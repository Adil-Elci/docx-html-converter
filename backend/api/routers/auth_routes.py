from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session

from ..auth import create_access_token, hash_password, require_admin, verify_password
from ..db import get_db
from ..portal_models import Client, Invite, User
from ..portal_schemas import AuthResponse, InviteCreate, LoginRequest, RegisterRequest, UserOut

router = APIRouter(prefix="/auth", tags=["auth"])


def _cookie_settings() -> dict:
    secure = os.getenv("COOKIE_SECURE", "false").lower() == "true"
    return {
        "httponly": True,
        "secure": secure,
        "samesite": "lax",
        "path": "/",
    }


def _invite_expiry() -> datetime:
    days = int(os.getenv("INVITE_EXPIRE_DAYS", "7"))
    return datetime.now(tz=timezone.utc) + timedelta(days=days)


def _validate_invite(invite: Invite, email: str) -> None:
    if invite.used_at is not None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invite already used.")
    if invite.expires_at < datetime.now(tz=timezone.utc):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invite expired.")
    if invite.email.lower() != email.lower():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invite email mismatch.")


def _user_to_out(user: User) -> UserOut:
    return UserOut(
        id=user.id,
        email=user.email,
        role=user.role,
        client_id=user.client_id,
        ui_language=user.ui_language,
        active=user.active,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


@router.post("/invite")
def create_invite(
    payload: InviteCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> dict:
    client = db.query(Client).filter(Client.id == payload.client_id, Client.active.is_(True)).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found.")

    token = os.urandom(32).hex()
    invite = Invite(
        email=payload.email.lower(),
        client_id=payload.client_id,
        token=token,
        expires_at=_invite_expiry(),
    )
    db.add(invite)
    db.commit()
    db.refresh(invite)
    return {
        "ok": True,
        "invite_id": invite.id,
        "token": invite.token,
        "expires_at": invite.expires_at,
    }


@router.post("/register", response_model=AuthResponse)
def register(
    payload: RegisterRequest,
    response: Response,
    db: Session = Depends(get_db),
) -> AuthResponse:
    invite = db.query(Invite).filter(Invite.token == payload.token).first()
    if not invite:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid invite token.")
    _validate_invite(invite, payload.email)
    client = db.query(Client).filter(Client.id == invite.client_id, Client.active.is_(True)).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Client is not active.")

    existing = db.query(User).filter(User.email == payload.email.lower()).first()
    if existing:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User already exists.")

    user = User(
        email=payload.email.lower(),
        password_hash=hash_password(payload.password),
        role="client",
        client_id=invite.client_id,
        ui_language=payload.ui_language,
        active=True,
    )
    invite.used_at = datetime.now(tz=timezone.utc)
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_access_token(user.id, user.role)
    response.set_cookie("access_token", token, **_cookie_settings())
    return AuthResponse(user=_user_to_out(user))


@router.post("/login", response_model=AuthResponse)
def login(
    payload: LoginRequest,
    response: Response,
    db: Session = Depends(get_db),
) -> AuthResponse:
    user = db.query(User).filter(User.email == payload.email.lower()).first()
    if not user or not user.active or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials.")
    if user.role == "client":
        client = db.query(Client).filter(Client.id == user.client_id, Client.active.is_(True)).first()
        if not client:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Client inactive.")
    token = create_access_token(user.id, user.role)
    response.set_cookie("access_token", token, **_cookie_settings())
    return AuthResponse(user=_user_to_out(user))


@router.post("/logout")
def logout(response: Response) -> dict:
    response.delete_cookie("access_token", path="/")
    return {"ok": True}
