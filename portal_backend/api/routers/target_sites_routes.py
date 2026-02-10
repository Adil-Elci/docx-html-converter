from __future__ import annotations

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ..auth import get_current_user, require_admin
from ..db import get_db
from ..portal_models import TargetSite, User
from ..portal_schemas import TargetSiteCreate, TargetSiteOut, TargetSiteUpdate

router = APIRouter(prefix="/target-sites", tags=["target_sites"])


def _site_to_out(site: TargetSite) -> TargetSiteOut:
    return TargetSiteOut(
        id=site.id,
        site_name=site.site_name,
        site_url=site.site_url,
        active=site.active,
        created_at=site.created_at,
        updated_at=site.updated_at,
    )


@router.get("", response_model=List[TargetSiteOut])
def list_sites(
    active: Optional[bool] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[TargetSiteOut]:
    query = db.query(TargetSite)
    if current_user.role != "admin":
        active = True
    if active is not None:
        query = query.filter(TargetSite.active.is_(active))
    sites = query.order_by(TargetSite.created_at.desc()).all()
    return [_site_to_out(site) for site in sites]


@router.post("", response_model=TargetSiteOut)
def create_site(
    payload: TargetSiteCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> TargetSiteOut:
    site = TargetSite(site_name=payload.site_name, site_url=payload.site_url, active=True)
    db.add(site)
    db.commit()
    db.refresh(site)
    return _site_to_out(site)


@router.patch("/{site_id}", response_model=TargetSiteOut)
def update_site(
    site_id: UUID,
    payload: TargetSiteUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> TargetSiteOut:
    site = db.query(TargetSite).filter(TargetSite.id == site_id).first()
    if not site:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target site not found.")
    if payload.site_name is not None:
        site.site_name = payload.site_name
    if payload.site_url is not None:
        site.site_url = payload.site_url
    if payload.active is not None:
        site.active = payload.active
    db.add(site)
    db.commit()
    db.refresh(site)
    return _site_to_out(site)
