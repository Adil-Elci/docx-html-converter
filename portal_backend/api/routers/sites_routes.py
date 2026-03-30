from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable, List, Optional, Sequence, Tuple
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from ..automation_service import DEFAULT_TIMEOUT_SECONDS, AutomationError, wp_check_site_access
from ..auth import get_current_user, require_admin
from ..db import get_db
from ..portal_models import Site, SiteCredential, User
from ..portal_schemas import SiteCreate, SiteOut, SiteUpdate

router = APIRouter(prefix="/sites", tags=["publishing_sites"])
logger = logging.getLogger("portal_backend.site_access")


class SiteAccessCheckFailureOut(BaseModel):
    site_id: UUID
    site_name: str
    site_url: str
    error: str


class SiteAccessCheckSummaryOut(BaseModel):
    tested_count: int = 0
    accessible_count: int = 0
    failed_count: int = 0
    checked_at: datetime
    failures: List[SiteAccessCheckFailureOut] = Field(default_factory=list)


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


def _list_ready_sites_with_credentials(db: Session) -> List[Tuple[Site, SiteCredential]]:
    return (
        db.query(Site, SiteCredential)
        .join(SiteCredential, SiteCredential.site_id == Site.id)
        .filter(
            Site.status == "active",
            SiteCredential.enabled.is_(True),
            SiteCredential.wp_username.isnot(None),
            SiteCredential.wp_app_password.isnot(None),
            func.length(func.btrim(SiteCredential.wp_username)) > 0,
            func.length(func.btrim(SiteCredential.wp_app_password)) > 0,
        )
        .order_by(Site.name.asc())
        .all()
    )


def _run_site_access_checks(
    site_rows: Sequence[Tuple[Site, SiteCredential]],
    *,
    checker: Callable[..., dict] = wp_check_site_access,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> SiteAccessCheckSummaryOut:
    failures: List[SiteAccessCheckFailureOut] = []
    accessible_count = 0

    for site, credential in site_rows:
        try:
            checker(
                site_url=site.site_url,
                wp_rest_base=site.wp_rest_base,
                wp_username=credential.wp_username,
                wp_app_password=credential.wp_app_password,
                timeout_seconds=timeout_seconds,
            )
            accessible_count += 1
        except AutomationError as exc:
            failures.append(
                SiteAccessCheckFailureOut(
                    site_id=site.id,
                    site_name=site.name,
                    site_url=site.site_url,
                    error=str(exc),
                )
            )
        except Exception as exc:  # pragma: no cover - defensive guardrail for unexpected probe failures
            logger.exception("Unexpected publishing-site access check failure for %s", site.site_url)
            failures.append(
                SiteAccessCheckFailureOut(
                    site_id=site.id,
                    site_name=site.name,
                    site_url=site.site_url,
                    error=f"Unexpected error: {exc}",
                )
            )

    return SiteAccessCheckSummaryOut(
        tested_count=len(site_rows),
        accessible_count=accessible_count,
        failed_count=len(failures),
        checked_at=datetime.now(timezone.utc),
        failures=failures,
    )


@router.get("", response_model=List[SiteOut])
def list_sites(
    status_filter: Optional[str] = Query(default=None, alias="status"),
    ready_only: bool = Query(default=False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[SiteOut]:
    query = db.query(Site)
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


@router.post("/access-check", response_model=SiteAccessCheckSummaryOut)
def check_active_site_access(
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> SiteAccessCheckSummaryOut:
    return _run_site_access_checks(_list_ready_sites_with_credentials(db))


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
