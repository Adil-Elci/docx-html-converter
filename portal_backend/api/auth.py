from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set
from uuid import UUID

import jwt
from fastapi import Depends, HTTPException, Request, status
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from .db import get_db
from .portal_models import ClientSiteAccess, ClientUser, Site, User

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(raw_password: str) -> str:
    return _pwd_context.hash(raw_password)


def verify_password(raw_password: str, hashed_password: str) -> bool:
    return _pwd_context.verify(raw_password, hashed_password)


def _jwt_secret() -> str:
    secret = (os.getenv("AUTH_JWT_SECRET") or os.getenv("JWT_SECRET") or "").strip()
    if not secret:
        raise RuntimeError("AUTH_JWT_SECRET is not set.")
    return secret


def _jwt_algorithm() -> str:
    return (os.getenv("AUTH_JWT_ALGORITHM") or "HS256").strip()


def access_token_ttl_minutes() -> int:
    raw_value = (os.getenv("AUTH_ACCESS_TOKEN_TTL_MINUTES") or "10080").strip()
    try:
        ttl = int(raw_value)
    except ValueError:
        ttl = 10080
    return max(ttl, 15)


def cookie_secure_enabled() -> bool:
    raw_value = (os.getenv("AUTH_COOKIE_SECURE") or "false").strip().lower()
    return raw_value in {"1", "true", "yes", "on"}


def cookie_samesite_value() -> str:
    raw_value = (os.getenv("AUTH_COOKIE_SAMESITE") or "lax").strip().lower()
    if raw_value not in {"lax", "strict", "none"}:
        return "lax"
    return raw_value


def create_access_token(*, user_id: UUID, email: str, role: str, ttl_minutes: Optional[int] = None) -> str:
    now = datetime.now(timezone.utc)
    expires_in_minutes = ttl_minutes if ttl_minutes is not None else access_token_ttl_minutes()
    payload: Dict[str, Any] = {
        "sub": str(user_id),
        "email": email.strip().lower(),
        "role": role.strip().lower(),
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=expires_in_minutes)).timestamp()),
    }
    return jwt.encode(payload, _jwt_secret(), algorithm=_jwt_algorithm())


def decode_access_token(token: str) -> Dict[str, Any]:
    return jwt.decode(token, _jwt_secret(), algorithms=[_jwt_algorithm()])


def _extract_token(request: Request) -> Optional[str]:
    authorization = (request.headers.get("authorization") or "").strip()
    if authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
        if token:
            return token

    cookie_token = (request.cookies.get("access_token") or "").strip()
    if cookie_token.lower().startswith("bearer "):
        cookie_token = cookie_token[7:].strip()
    return cookie_token or None


def get_current_user(
    request: Request,
    db: Session = Depends(get_db),
) -> User:
    token = _extract_token(request)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")

    try:
        claims = decode_access_token(token)
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication token.") from exc

    subject = claims.get("sub")
    if not isinstance(subject, str):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication token.")
    try:
        user_id = UUID(subject)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication token.") from exc

    user = db.query(User).filter(User.id == user_id).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Account is not active.")
    return user


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required.")
    return current_user


def get_optional_current_user(
    request: Request,
    db: Session = Depends(get_db),
) -> Optional[User]:
    token = _extract_token(request)
    if not token:
        return None

    try:
        claims = decode_access_token(token)
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication token.") from exc

    subject = claims.get("sub")
    if not isinstance(subject, str):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication token.")
    try:
        user_id = UUID(subject)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication token.") from exc

    user = db.query(User).filter(User.id == user_id).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Account is not active.")
    return user


def user_client_ids(db: Session, user: User) -> Set[UUID]:
    if user.role == "admin":
        rows = db.query(ClientUser.client_id).all()
    else:
        rows = db.query(ClientUser.client_id).filter(ClientUser.user_id == user.id).all()
    return {row[0] for row in rows}


def user_accessible_site_ids(db: Session, user: User) -> Set[UUID]:
    enforce_client_site_access = (os.getenv("AUTOMATION_ENFORCE_CLIENT_SITE_ACCESS") or "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not enforce_client_site_access:
        rows = db.query(Site.id).all()
        return {row[0] for row in rows}
    if user.role == "admin":
        rows = db.query(ClientSiteAccess.site_id).all()
    else:
        rows = (
            db.query(ClientSiteAccess.site_id)
            .join(ClientUser, ClientUser.client_id == ClientSiteAccess.client_id)
            .filter(
                ClientUser.user_id == user.id,
                ClientSiteAccess.enabled.is_(True),
            )
            .all()
        )
    return {row[0] for row in rows}


def ensure_client_access(db: Session, user: User, client_id: UUID) -> None:
    if user.role == "admin":
        return
    allowed_client_ids = user_client_ids(db, user)
    if client_id not in allowed_client_ids:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Client access denied.")


def ensure_site_access(db: Session, user: User, site_id: UUID) -> None:
    enforce_client_site_access = (os.getenv("AUTOMATION_ENFORCE_CLIENT_SITE_ACCESS") or "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not enforce_client_site_access:
        return
    if user.role == "admin":
        return
    allowed_site_ids = user_accessible_site_ids(db, user)
    if site_id not in allowed_site_ids:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Site access denied.")
