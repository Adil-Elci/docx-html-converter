from __future__ import annotations

import logging
import os
from collections import defaultdict, deque
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
    verify_password,
)
from ..db import get_db
from ..portal_models import User
from ..portal_schemas import AuthLoginIn, AuthLoginOut, AuthLogoutOut, UserOut

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


def _user_to_out(user: User) -> UserOut:
    return UserOut(
        id=user.id,
        email=user.email,
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
