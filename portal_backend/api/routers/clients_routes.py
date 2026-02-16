from __future__ import annotations

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..db import get_db
from ..portal_models import Client
from ..portal_schemas import ClientCreate, ClientOut, ClientUpdate

router = APIRouter(prefix="/clients", tags=["clients"])


def _client_to_out(client: Client) -> ClientOut:
    return ClientOut(
        id=client.id,
        name=client.name,
        primary_domain=client.primary_domain,
        backlink_url=client.backlink_url,
        email=client.email,
        phone_number=client.phone_number,
        status=client.status,
        created_at=client.created_at,
        updated_at=client.updated_at,
    )


@router.get("", response_model=List[ClientOut])
def list_clients(
    status_filter: Optional[str] = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
) -> List[ClientOut]:
    query = db.query(Client)
    if status_filter:
        query = query.filter(Client.status == status_filter.strip().lower())
    clients = query.order_by(Client.created_at.desc()).all()
    return [_client_to_out(client) for client in clients]


@router.post("", response_model=ClientOut, status_code=status.HTTP_201_CREATED)
def create_client(
    payload: ClientCreate,
    db: Session = Depends(get_db),
) -> ClientOut:
    client = Client(
        name=payload.name,
        primary_domain=payload.primary_domain,
        backlink_url=payload.backlink_url,
        email=payload.email,
        phone_number=payload.phone_number,
        status=payload.status,
    )
    db.add(client)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Client conflict.") from exc
    db.refresh(client)
    return _client_to_out(client)


@router.patch("/{client_id}", response_model=ClientOut)
def update_client(
    client_id: UUID,
    payload: ClientUpdate,
    db: Session = Depends(get_db),
) -> ClientOut:
    client = db.query(Client).filter(Client.id == client_id).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client not found.")

    if payload.name is not None:
        client.name = payload.name
    if payload.primary_domain is not None:
        client.primary_domain = payload.primary_domain
    if payload.backlink_url is not None:
        client.backlink_url = payload.backlink_url
    if payload.email is not None:
        client.email = payload.email
    if payload.phone_number is not None:
        client.phone_number = payload.phone_number
    if payload.status is not None:
        client.status = payload.status

    db.add(client)
    db.commit()
    db.refresh(client)
    return _client_to_out(client)
