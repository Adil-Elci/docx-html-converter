from __future__ import annotations

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ..auth import require_admin
from ..db import get_db
from ..portal_models import Client, User
from ..portal_schemas import ClientOut, ClientUpdate

router = APIRouter(prefix="/clients", tags=["clients"])


def _client_to_out(client: Client) -> ClientOut:
    return ClientOut(
        id=client.id,
        name=client.name,
        website_domain=client.website_domain,
        active=client.active,
        created_at=client.created_at,
        updated_at=client.updated_at,
    )


@router.get("", response_model=List[ClientOut])
def list_clients(
    active: Optional[bool] = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> List[ClientOut]:
    query = db.query(Client)
    if active is not None:
        query = query.filter(Client.active.is_(active))
    clients = query.order_by(Client.created_at.desc()).all()
    return [_client_to_out(client) for client in clients]


@router.patch("/{client_id}", response_model=ClientOut)
def update_client(
    client_id: UUID,
    payload: ClientUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> ClientOut:
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found.")
    if payload.name is not None:
        client.name = payload.name
    if payload.website_domain is not None:
        client.website_domain = payload.website_domain
    if payload.active is not None:
        client.active = payload.active
    db.add(client)
    db.commit()
    db.refresh(client)
    return _client_to_out(client)
