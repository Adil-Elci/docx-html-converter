from __future__ import annotations

from typing import List, Set
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..auth import hash_password, require_admin
from ..db import get_db
from ..portal_models import Client, ClientUser, User
from ..portal_schemas import AdminUserCreate, AdminUserOut, AdminUserUpdate

router = APIRouter(prefix="/admin/users", tags=["admin_users"], dependencies=[Depends(require_admin)])


def _active_admin_count(db: Session) -> int:
    return db.query(User).filter(User.role == "admin", User.is_active.is_(True)).count()


def _validated_client_ids(db: Session, client_ids: List[UUID]) -> List[UUID]:
    deduped: List[UUID] = []
    seen: Set[UUID] = set()
    for client_id in client_ids:
        if client_id in seen:
            continue
        seen.add(client_id)
        deduped.append(client_id)

    if not deduped:
        return deduped

    rows = db.query(Client.id).filter(Client.id.in_(deduped), Client.status == "active").all()
    found = {row[0] for row in rows}
    missing = [client_id for client_id in deduped if client_id not in found]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown/inactive client ids: {', '.join(str(item) for item in missing)}",
        )
    return deduped


def _replace_client_links(db: Session, user_id: UUID, client_ids: List[UUID]) -> None:
    db.query(ClientUser).filter(ClientUser.user_id == user_id).delete(synchronize_session=False)
    for client_id in client_ids:
        db.add(
            ClientUser(
                user_id=user_id,
                client_id=client_id,
            )
        )


def _user_to_out(db: Session, user: User) -> AdminUserOut:
    client_rows = db.query(ClientUser.client_id).filter(ClientUser.user_id == user.id).all()
    client_ids = [row[0] for row in client_rows]
    return AdminUserOut(
        id=user.id,
        email=user.email,
        role=user.role,
        is_active=user.is_active,
        created_at=user.created_at,
        updated_at=user.updated_at,
        client_ids=client_ids,
    )


@router.get("", response_model=List[AdminUserOut])
def list_admin_users(db: Session = Depends(get_db)) -> List[AdminUserOut]:
    users = db.query(User).order_by(User.created_at.desc()).all()
    return [_user_to_out(db, user) for user in users]


@router.post("", response_model=AdminUserOut, status_code=status.HTTP_201_CREATED)
def create_admin_user(payload: AdminUserCreate, db: Session = Depends(get_db)) -> AdminUserOut:
    client_ids = _validated_client_ids(db, payload.client_ids)

    user = User(
        email=str(payload.email).strip().lower(),
        password_hash=hash_password(payload.password),
        role=payload.role,
        is_active=payload.is_active,
    )
    db.add(user)
    db.flush()

    if payload.role == "client":
        for client_id in client_ids:
            db.add(
                ClientUser(
                    user_id=user.id,
                    client_id=client_id,
                )
            )

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already exists.") from exc

    db.refresh(user)
    return _user_to_out(db, user)


@router.patch("/{user_id}", response_model=AdminUserOut)
def update_admin_user(
    user_id: UUID,
    payload: AdminUserUpdate,
    db: Session = Depends(get_db),
) -> AdminUserOut:
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found.")

    new_role = payload.role if payload.role is not None else user.role
    new_is_active = payload.is_active if payload.is_active is not None else user.is_active

    if user.role == "admin" and user.is_active and (new_role != "admin" or not new_is_active):
        if _active_admin_count(db) <= 1:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Cannot demote/deactivate the last active admin.",
            )

    if payload.role is not None:
        user.role = payload.role
    if payload.is_active is not None:
        user.is_active = payload.is_active
    if payload.password is not None:
        user.password_hash = hash_password(payload.password)

    if payload.client_ids is not None:
        client_ids = _validated_client_ids(db, payload.client_ids)
        _replace_client_links(db, user.id, client_ids if user.role == "client" else [])
    elif payload.role == "admin":
        # Keep admin accounts unmapped by default for least confusion.
        _replace_client_links(db, user.id, [])

    db.add(user)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User update conflict.") from exc
    db.refresh(user)
    return _user_to_out(db, user)
