from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..auth import get_current_user
from ..db import get_db
from ..portal_models import User
from ..portal_schemas import UiLanguageUpdate, UserOut

router = APIRouter(tags=["me"])


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


@router.get("/me", response_model=UserOut)
def me(current_user: User = Depends(get_current_user)) -> UserOut:
    return _user_to_out(current_user)


@router.patch("/me/ui-language", response_model=UserOut)
def update_ui_language(
    payload: UiLanguageUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> UserOut:
    current_user.ui_language = payload.ui_language
    db.add(current_user)
    db.commit()
    db.refresh(current_user)
    return _user_to_out(current_user)
