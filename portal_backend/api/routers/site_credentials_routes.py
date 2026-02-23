from __future__ import annotations

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..auth import require_admin
from ..db import get_db
from ..portal_models import Site, SiteCredential
from ..portal_schemas import SiteCredentialCreate, SiteCredentialOut, SiteCredentialUpdate

router = APIRouter(prefix="/site-credentials", tags=["publishing_site_credentials"], dependencies=[Depends(require_admin)])


def _credential_to_out(credential: SiteCredential) -> SiteCredentialOut:
    return SiteCredentialOut(
        id=credential.id,
        site_id=credential.site_id,
        auth_type=credential.auth_type,
        wp_username=credential.wp_username,
        wp_app_password=credential.wp_app_password,
        author_name=credential.author_name,
        author_id=credential.author_id,
        enabled=credential.enabled,
        created_at=credential.created_at,
        updated_at=credential.updated_at,
    )


@router.get("", response_model=List[SiteCredentialOut])
def list_site_credentials(
    site_id: Optional[UUID] = Query(default=None),
    enabled: Optional[bool] = Query(default=None),
    db: Session = Depends(get_db),
) -> List[SiteCredentialOut]:
    query = db.query(SiteCredential)
    if site_id is not None:
        query = query.filter(SiteCredential.site_id == site_id)
    if enabled is not None:
        query = query.filter(SiteCredential.enabled.is_(enabled))
    credentials = query.order_by(SiteCredential.created_at.desc()).all()
    return [_credential_to_out(credential) for credential in credentials]


@router.post("", response_model=SiteCredentialOut, status_code=status.HTTP_201_CREATED)
def create_site_credential(
    payload: SiteCredentialCreate,
    db: Session = Depends(get_db),
) -> SiteCredentialOut:
    site = db.query(Site).filter(Site.id == payload.site_id).first()
    if not site:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Publishing site not found.")

    credential = SiteCredential(
        site_id=payload.site_id,
        auth_type=payload.auth_type,
        wp_username=payload.wp_username,
        wp_app_password=payload.wp_app_password,
        author_name=payload.author_name,
        author_id=payload.author_id,
        enabled=payload.enabled,
    )
    db.add(credential)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Credential for this username already exists on the publishing site.",
        ) from exc
    db.refresh(credential)
    return _credential_to_out(credential)


@router.patch("/{credential_id}", response_model=SiteCredentialOut)
def update_site_credential(
    credential_id: UUID,
    payload: SiteCredentialUpdate,
    db: Session = Depends(get_db),
) -> SiteCredentialOut:
    credential = db.query(SiteCredential).filter(SiteCredential.id == credential_id).first()
    if not credential:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Site credential not found.")

    if payload.auth_type is not None:
        credential.auth_type = payload.auth_type
    if payload.wp_username is not None:
        credential.wp_username = payload.wp_username
    if payload.wp_app_password is not None:
        credential.wp_app_password = payload.wp_app_password
    if "author_name" in payload.__fields_set__:
        credential.author_name = payload.author_name
    if "author_id" in payload.__fields_set__:
        credential.author_id = payload.author_id
    if payload.enabled is not None:
        credential.enabled = payload.enabled

    db.add(credential)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Credential for this username already exists on the publishing site.",
        ) from exc
    db.refresh(credential)
    return _credential_to_out(credential)
