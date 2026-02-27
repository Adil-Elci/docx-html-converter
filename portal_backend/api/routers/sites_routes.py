from __future__ import annotations

import os
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from ..auth import get_current_user, require_admin, user_accessible_site_ids
from ..db import get_db
from ..portal_models import Site, SiteCredential, User
from ..portal_schemas import SiteCreate, SiteOut, SiteUpdate

router = APIRouter(prefix="/sites", tags=["publishing_sites"])


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _site_to_out(site: Site, credential: Optional[SiteCredential] = None) -> SiteOut:
    return SiteOut(
        id=site.id,
        name=site.name,
        site_url=site.site_url,
        wp_rest_base=site.wp_rest_base,
        hosted_by=site.hosted_by,
        host_panel=site.host_panel,
        author_name=credential.author_name if credential else None,
        author_id=credential.author_id if credential else None,
        status=site.status,
        created_at=site.created_at,
        updated_at=site.updated_at,
    )


@router.get("", response_model=List[SiteOut])
def list_sites(
    status_filter: Optional[str] = Query(default=None, alias="status"),
    ready_only: bool = Query(default=False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[SiteOut]:
    query = db.query(Site)
    if current_user.role != "admin":
        enforce_client_site_access = _read_bool_env("AUTOMATION_ENFORCE_CLIENT_SITE_ACCESS", False)
        if enforce_client_site_access:
            allowed_site_ids = user_accessible_site_ids(db, current_user)
            if not allowed_site_ids:
                return []
            query = query.filter(Site.id.in_(allowed_site_ids))
    if status_filter:
        query = query.filter(Site.status == status_filter.strip().lower())
    if ready_only:
        query = query.filter(
            db.query(SiteCredential.id)
            .filter(
                SiteCredential.site_id == Site.id,
                SiteCredential.enabled.is_(True),
                SiteCredential.wp_username.isnot(None),
                SiteCredential.wp_app_password.isnot(None),
                func.length(func.btrim(SiteCredential.wp_username)) > 0,
                func.length(func.btrim(SiteCredential.wp_app_password)) > 0,
            )
            .exists()
        )
    sites = query.order_by(Site.created_at.desc()).all()
    if not sites:
        return []
    site_ids = [site.id for site in sites]
    credentials = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id.in_(site_ids), SiteCredential.enabled.is_(True))
        .order_by(SiteCredential.created_at.desc())
        .all()
    )
    credential_map = {cred.site_id: cred for cred in credentials}
    return [_site_to_out(site, credential_map.get(site.id)) for site in sites]


@router.post("", response_model=SiteOut, status_code=status.HTTP_201_CREATED)
def create_site(
    payload: SiteCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> SiteOut:
    site = Site(
        name=payload.name,
        site_url=payload.site_url,
        wp_rest_base=payload.wp_rest_base,
        hosted_by=payload.hosted_by,
        host_panel=payload.host_panel,
        status=payload.status,
    )
    db.add(site)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Publishing site URL already exists.") from exc
    db.refresh(site)
    return _site_to_out(site)


@router.patch("/{site_id}", response_model=SiteOut)
def update_site(
    site_id: UUID,
    payload: SiteUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> SiteOut:
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Publishing site not found.")

    if payload.name is not None:
        site.name = payload.name
    if payload.site_url is not None:
        site.site_url = payload.site_url
    if payload.wp_rest_base is not None:
        site.wp_rest_base = payload.wp_rest_base
    if payload.hosted_by is not None:
        site.hosted_by = payload.hosted_by
    if payload.host_panel is not None:
        site.host_panel = payload.host_panel
    if payload.status is not None:
        site.status = payload.status

    db.add(site)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Publishing site URL already exists.") from exc
    db.refresh(site)
    return _site_to_out(site)
