from __future__ import annotations

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..auth import require_admin
from ..db import get_db
from ..portal_models import Client, ClientSiteAccess, Site
from ..portal_schemas import ClientSiteAccessCreate, ClientSiteAccessOut, ClientSiteAccessUpdate

router = APIRouter(prefix="/client-site-access", tags=["client_site_access"], dependencies=[Depends(require_admin)])


def _access_to_out(access: ClientSiteAccess) -> ClientSiteAccessOut:
    return ClientSiteAccessOut(
        id=access.id,
        client_id=access.client_id,
        site_id=access.site_id,
        enabled=access.enabled,
        created_at=access.created_at,
        updated_at=access.updated_at,
    )


@router.get("", response_model=List[ClientSiteAccessOut])
def list_client_site_access(
    client_id: Optional[UUID] = Query(default=None),
    site_id: Optional[UUID] = Query(default=None),
    enabled: Optional[bool] = Query(default=None),
    db: Session = Depends(get_db),
) -> List[ClientSiteAccessOut]:
    query = db.query(ClientSiteAccess)
    if client_id is not None:
        query = query.filter(ClientSiteAccess.client_id == client_id)
    if site_id is not None:
        query = query.filter(ClientSiteAccess.site_id == site_id)
    if enabled is not None:
        query = query.filter(ClientSiteAccess.enabled.is_(enabled))
    rows = query.order_by(ClientSiteAccess.created_at.desc()).all()
    return [_access_to_out(row) for row in rows]


@router.post("", response_model=ClientSiteAccessOut, status_code=status.HTTP_201_CREATED)
def create_client_site_access(
    payload: ClientSiteAccessCreate,
    db: Session = Depends(get_db),
) -> ClientSiteAccessOut:
    client = db.query(Client).filter(Client.id == payload.client_id).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found.")

    site = db.query(Site).filter(Site.id == payload.site_id).first()
    if not site:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Site not found.")

    row = ClientSiteAccess(
        client_id=payload.client_id,
        site_id=payload.site_id,
        enabled=payload.enabled,
    )
    db.add(row)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Client-site mapping already exists.") from exc
    db.refresh(row)
    return _access_to_out(row)


@router.patch("/{access_id}", response_model=ClientSiteAccessOut)
def update_client_site_access(
    access_id: UUID,
    payload: ClientSiteAccessUpdate,
    db: Session = Depends(get_db),
) -> ClientSiteAccessOut:
    row = db.query(ClientSiteAccess).filter(ClientSiteAccess.id == access_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client-site access row not found.")

    if payload.enabled is not None:
        row.enabled = payload.enabled

    db.add(row)
    db.commit()
    db.refresh(row)
    return _access_to_out(row)
