from __future__ import annotations

import hashlib
import logging
import os
import secrets
import smtplib
import ssl
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from threading import Lock
from time import monotonic

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy.orm import Session

from ..auth import (
    access_token_ttl_minutes,
    cookie_samesite_value,
    cookie_secure_enabled,
    create_access_token,
    get_current_user,
    hash_password,
    verify_password,
)
from ..db import get_db
from ..portal_models import PasswordResetToken, User
from ..portal_schemas import (
    AuthLoginIn,
    AuthLoginOut,
    AuthLogoutOut,
    AuthPasswordResetConfirmIn,
    AuthPasswordResetConfirmOut,
    AuthPasswordResetRequestIn,
    AuthPasswordResetRequestOut,
    UserOut,
)

router = APIRouter(prefix="/auth", tags=["auth"])
logger = logging.getLogger("portal_backend.auth")

_LOGIN_ATTEMPTS: dict[str, deque[float]] = defaultdict(deque)
_LOGIN_ATTEMPTS_LOCK = Lock()


def _read_int_env(name: str, default: int, minimum: int) -> int:
    raw_value = (os.getenv(name) or str(default)).strip()
    try:
        parsed = int(raw_value)
    except ValueError:
        parsed = default
    return max(parsed, minimum)


def _login_rate_limit_window_seconds() -> int:
    return _read_int_env("AUTH_LOGIN_RATE_LIMIT_WINDOW_SECONDS", 300, 30)


def _login_rate_limit_max_attempts() -> int:
    return _read_int_env("AUTH_LOGIN_RATE_LIMIT_MAX_ATTEMPTS", 8, 1)


def _client_ip_key(raw_ip: str) -> str:
    cleaned = (raw_ip or "").strip()
    return cleaned or "unknown"


def _record_login_failure(rate_key: str) -> None:
    now = monotonic()
    window_seconds = _login_rate_limit_window_seconds()
    with _LOGIN_ATTEMPTS_LOCK:
        queue = _LOGIN_ATTEMPTS[rate_key]
        while queue and (now - queue[0]) > window_seconds:
            queue.popleft()
        queue.append(now)


def _clear_login_failures(rate_key: str) -> None:
    with _LOGIN_ATTEMPTS_LOCK:
        _LOGIN_ATTEMPTS.pop(rate_key, None)


def _enforce_login_rate_limit(rate_key: str) -> None:
    now = monotonic()
    window_seconds = _login_rate_limit_window_seconds()
    max_attempts = _login_rate_limit_max_attempts()

    with _LOGIN_ATTEMPTS_LOCK:
        queue = _LOGIN_ATTEMPTS[rate_key]
        while queue and (now - queue[0]) > window_seconds:
            queue.popleft()
        if len(queue) >= max_attempts:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many login attempts. Try again later.",
            )


def _request_ip(request: Request) -> str:
    forwarded_for = (request.headers.get("x-forwarded-for") or "").strip()
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    client = getattr(request, "client", None)
    return (getattr(client, "host", "") or "").strip()


def _password_reset_ttl_minutes() -> int:
    return _read_int_env("AUTH_PASSWORD_RESET_TOKEN_TTL_MINUTES", 60, 5)


def _password_reset_url_base_for_role(user_role: str) -> str:
    role = (user_role or "").strip().lower()
    admin_base = (os.getenv("AUTH_PASSWORD_RESET_URL_BASE_ADMIN") or "").strip()
    client_base = (os.getenv("AUTH_PASSWORD_RESET_URL_BASE_CLIENT") or "").strip()
    legacy_base = (os.getenv("AUTH_PASSWORD_RESET_URL_BASE") or "").strip()

    if role == "admin":
        if admin_base:
            return admin_base
        if legacy_base:
            return legacy_base
        raise RuntimeError("AUTH_PASSWORD_RESET_URL_BASE_ADMIN (or AUTH_PASSWORD_RESET_URL_BASE fallback) is not set.")

    if client_base:
        return client_base
    if legacy_base:
        return legacy_base
    raise RuntimeError("AUTH_PASSWORD_RESET_URL_BASE_CLIENT (or AUTH_PASSWORD_RESET_URL_BASE fallback) is not set.")


def _hash_reset_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def _smtp_port() -> int:
    return _read_int_env("SMTP_PORT", 587, 1)


def _smtp_use_tls() -> bool:
    return (os.getenv("SMTP_USE_TLS") or "true").strip().lower() in {"1", "true", "yes", "on"}


def _smtp_use_ssl() -> bool:
    return (os.getenv("SMTP_USE_SSL") or "false").strip().lower() in {"1", "true", "yes", "on"}


def _send_password_reset_email(*, to_email: str, reset_link: str) -> None:
    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    smtp_username = (os.getenv("SMTP_USERNAME") or "").strip()
    smtp_password = (os.getenv("SMTP_PASSWORD") or "").strip()
    from_email = (os.getenv("SMTP_FROM_EMAIL") or smtp_username).strip()
    from_name = (os.getenv("SMTP_FROM_NAME") or "Elci Solutions").strip()

    if not smtp_host or not from_email:
        raise RuntimeError("SMTP_HOST and SMTP_FROM_EMAIL (or SMTP_USERNAME) are required for password reset emails.")

    message = EmailMessage()
    message["Subject"] = "Reset your Elci Solutions Portal password"
    message["From"] = f"{from_name} <{from_email}>"
    message["To"] = to_email
    message.set_content(
        "We received a password reset request for your Elci Solutions Portal account.\n\n"
        f"Reset your password: {reset_link}\n\n"
        f"This link expires in {_password_reset_ttl_minutes()} minutes.\n"
        "If you did not request this, you can ignore this email."
    )

    port = _smtp_port()
    context = ssl.create_default_context()
    if _smtp_use_ssl():
        with smtplib.SMTP_SSL(smtp_host, port, timeout=20, context=context) as smtp:
            if smtp_username:
                smtp.login(smtp_username, smtp_password)
            smtp.send_message(message)
        return

    with smtplib.SMTP(smtp_host, port, timeout=20) as smtp:
        if _smtp_use_tls():
            smtp.starttls(context=context)
        if smtp_username:
            smtp.login(smtp_username, smtp_password)
        smtp.send_message(message)


def _user_to_out(user: User) -> UserOut:
    return UserOut(
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        role=user.role,
        is_active=user.is_active,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


@router.post("/login", response_model=AuthLoginOut)
def login(
    payload: AuthLoginIn,
    response: Response,
    request: Request,
    db: Session = Depends(get_db),
) -> AuthLoginOut:
    normalized_email = str(payload.email).strip().lower()
    ip = _client_ip_key(_request_ip(request))
    rate_key = f"{ip}:{normalized_email}"
    _enforce_login_rate_limit(rate_key)

    user = db.query(User).filter(User.email == normalized_email).first()
    if not user or not verify_password(payload.password, user.password_hash):
        _record_login_failure(rate_key)
        logger.warning("auth.login_failed email=%s ip=%s reason=invalid_credentials", normalized_email, ip)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password.")
    if not user.is_active:
        _record_login_failure(rate_key)
        logger.warning("auth.login_failed email=%s ip=%s reason=inactive", normalized_email, ip)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account is inactive.")

    token_ttl_minutes = access_token_ttl_minutes()
    access_token = create_access_token(
        user_id=user.id,
        email=user.email,
        role=user.role,
        ttl_minutes=token_ttl_minutes,
    )
    response.set_cookie(
        key="access_token",
        value=f"Bearer {access_token}",
        max_age=token_ttl_minutes * 60,
        httponly=True,
        secure=cookie_secure_enabled(),
        samesite=cookie_samesite_value(),
        path="/",
    )
    _clear_login_failures(rate_key)
    logger.info("auth.login_succeeded user_id=%s role=%s ip=%s", user.id, user.role, ip)
    return AuthLoginOut(access_token=access_token, token_type="bearer", user=_user_to_out(user))


@router.post("/logout", response_model=AuthLogoutOut)
def logout(response: Response) -> AuthLogoutOut:
    response.delete_cookie(key="access_token", path="/")
    return AuthLogoutOut()


@router.get("/me", response_model=UserOut)
def me(current_user: User = Depends(get_current_user)) -> UserOut:
    return _user_to_out(current_user)


@router.post("/password-reset/request", response_model=AuthPasswordResetRequestOut)
def request_password_reset(
    payload: AuthPasswordResetRequestIn,
    db: Session = Depends(get_db),
) -> AuthPasswordResetRequestOut:
    normalized_email = str(payload.email).strip().lower()
    user = db.query(User).filter(User.email == normalized_email).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="This email is not registered.")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account is inactive.")

    now = datetime.now(timezone.utc)
    db.query(PasswordResetToken).filter(
        PasswordResetToken.user_id == user.id,
        PasswordResetToken.used_at.is_(None),
        PasswordResetToken.expires_at > now,
    ).update(
        {
            PasswordResetToken.used_at: now,
            PasswordResetToken.updated_at: now,
        },
        synchronize_session=False,
    )

    raw_token = secrets.token_urlsafe(48)
    token_hash = _hash_reset_token(raw_token)
    expires_at = now + timedelta(minutes=_password_reset_ttl_minutes())
    reset_record = PasswordResetToken(
        user_id=user.id,
        token_hash=token_hash,
        expires_at=expires_at,
    )
    db.add(reset_record)
    db.flush()

    reset_link = f"{_password_reset_url_base_for_role(user.role)}?reset_token={raw_token}"
    try:
        _send_password_reset_email(to_email=user.email, reset_link=reset_link)
    except Exception:
        db.rollback()
        logger.exception("auth.password_reset_email_failed email=%s", normalized_email)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not send reset email. Please contact support.",
        )

    db.commit()
    logger.info("auth.password_reset_requested user_id=%s email=%s", user.id, normalized_email)
    return AuthPasswordResetRequestOut(message="Password reset link sent.")


@router.post("/password-reset/confirm", response_model=AuthPasswordResetConfirmOut)
def confirm_password_reset(
    payload: AuthPasswordResetConfirmIn,
    db: Session = Depends(get_db),
) -> AuthPasswordResetConfirmOut:
    now = datetime.now(timezone.utc)
    token_hash = _hash_reset_token(payload.token)

    token_record = (
        db.query(PasswordResetToken)
        .filter(
            PasswordResetToken.token_hash == token_hash,
            PasswordResetToken.used_at.is_(None),
            PasswordResetToken.expires_at > now,
        )
        .first()
    )
    if not token_record:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired reset token.")

    user = db.query(User).filter(User.id == token_record.user_id).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid reset token.")

    user.password_hash = hash_password(payload.new_password)
    user.updated_at = now
    token_record.used_at = now
    token_record.updated_at = now

    db.commit()
    logger.info("auth.password_reset_completed user_id=%s", user.id)
    return AuthPasswordResetConfirmOut(message="Password has been reset.")
