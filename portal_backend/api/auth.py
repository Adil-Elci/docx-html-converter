from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID

import jwt
from fastapi import Cookie, Depends, Header, HTTPException, status
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from .db import get_db
from .portal_models import Client, User

PWD_CONTEXT = CryptContext(schemes=["bcrypt"], deprecated="auto")
JWT_ALGORITHM = "HS256"


def _get_jwt_secret() -> str:
    secret = os.getenv("JWT_SECRET")
    if not secret:
        raise RuntimeError("JWT_SECRET is not set.")
    return secret


def hash_password(password: str) -> str:
    return PWD_CONTEXT.hash(password)


def verify_password(plain_password: str, password_hash: str) -> bool:
    return PWD_CONTEXT.verify(plain_password, password_hash)


def create_access_token(user_id: UUID, role: str, expires_minutes: Optional[int] = None) -> str:
    minutes = expires_minutes or int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "4320"))
    expire = datetime.now(tz=timezone.utc) + timedelta(minutes=minutes)
    payload = {"sub": str(user_id), "role": role, "exp": expire}
    return jwt.encode(payload, _get_jwt_secret(), algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, _get_jwt_secret(), algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired.") from exc
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token.") from exc


def get_token_from_request(
    authorization: Optional[str],
    access_token: Optional[str],
) -> Optional[str]:
    if authorization and authorization.lower().startswith("bearer "):
        return authorization.split(" ", 1)[1].strip()
    if access_token:
        return access_token
    return None


def get_current_user(
    db: Session = Depends(get_db),
    authorization: Optional[str] = Header(default=None),
    access_token: Optional[str] = Cookie(default=None),
) -> User:
    token = get_token_from_request(authorization, access_token)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated.")
    payload = decode_token(token)
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token.")
    try:
        user_uuid = UUID(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token.") from exc
    user = db.query(User).filter(User.id == user_uuid).first()
    if not user or not user.active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User inactive.")
    if user.role == "client":
        client = db.query(Client).filter(Client.id == user.client_id, Client.active.is_(True)).first()
        if not client:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Client inactive.")
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required.")
    return user
