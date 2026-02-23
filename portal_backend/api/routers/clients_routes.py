from __future__ import annotations

from typing import Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..auth import get_current_user, require_admin, user_client_ids
from ..db import get_db
from ..portal_models import Client, ClientTargetSite, User
from ..portal_schemas import ClientCreate, ClientOut, ClientTargetSiteIn, ClientTargetSiteOut, ClientUpdate

router = APIRouter(prefix="/clients", tags=["clients"])


def _normalize_target_site_payloads(
    target_sites: List[ClientTargetSiteIn],
    *,
    legacy_primary_domain: Optional[str],
    legacy_backlink_url: Optional[str],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    if target_sites:
        has_explicit_primary = any(bool(ts.is_primary) for ts in target_sites)
        primary_assigned = False
        for index, item in enumerate(target_sites):
            is_primary = False
            if has_explicit_primary:
                if bool(item.is_primary) and not primary_assigned:
                    is_primary = True
                    primary_assigned = True
            else:
                is_primary = index == 0
            rows.append(
                {
                    "target_site_domain": item.target_site_domain,
                    "target_site_url": item.target_site_url,
                    "is_primary": is_primary,
                }
            )
    else:
        has_legacy = bool((legacy_primary_domain or "").strip()) or bool((legacy_backlink_url or "").strip())
        if has_legacy:
            rows.append(
                {
                    "target_site_domain": (legacy_primary_domain or "").strip() or None,
                    "target_site_url": (legacy_backlink_url or "").strip() or None,
                    "is_primary": True,
                }
            )

    # De-duplicate exact domain/url pairs while preserving order.
    seen: set[tuple[Optional[str], Optional[str]]] = set()
    deduped: List[Dict[str, object]] = []
    for row in rows:
        key = (row["target_site_domain"], row["target_site_url"])  # type: ignore[index]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    if deduped and not any(bool(row.get("is_primary")) for row in deduped):
        deduped[0]["is_primary"] = True
    return deduped


def _set_client_legacy_target_fields(client: Client, target_sites: List[Dict[str, object]]) -> None:
    primary = next((row for row in target_sites if bool(row.get("is_primary"))), None)
    selected = primary or (target_sites[0] if target_sites else None)
    if not selected:
        client.primary_domain = None
        client.backlink_url = None
        return
    client.primary_domain = selected.get("target_site_domain")  # type: ignore[assignment]
    client.backlink_url = selected.get("target_site_url")  # type: ignore[assignment]


def _replace_client_target_sites(db: Session, client: Client, rows: List[Dict[str, object]]) -> None:
    db.query(ClientTargetSite).filter(ClientTargetSite.client_id == client.id).delete(synchronize_session=False)
    for row in rows:
        db.add(
            ClientTargetSite(
                client_id=client.id,
                target_site_domain=row.get("target_site_domain"),
                target_site_url=row.get("target_site_url"),
                is_primary=bool(row.get("is_primary")),
            )
        )


def _load_target_sites_map(db: Session, client_ids: List[UUID]) -> Dict[UUID, List[ClientTargetSite]]:
    if not client_ids:
        return {}
    rows = (
        db.query(ClientTargetSite)
        .filter(ClientTargetSite.client_id.in_(client_ids))
        .order_by(
            ClientTargetSite.client_id.asc(),
            ClientTargetSite.is_primary.desc(),
            ClientTargetSite.created_at.asc(),
        )
        .all()
    )
    out: Dict[UUID, List[ClientTargetSite]] = {}
    for row in rows:
        out.setdefault(row.client_id, []).append(row)
    return out


def _client_to_out(client: Client, target_sites: Optional[List[ClientTargetSite]] = None) -> ClientOut:
    rows = target_sites or []
    return ClientOut(
        id=client.id,
        name=client.name,
        primary_domain=client.primary_domain,
        backlink_url=client.backlink_url,
        target_sites=[
            ClientTargetSiteOut(
                id=row.id,
                client_id=row.client_id,
                target_site_domain=row.target_site_domain,
                target_site_url=row.target_site_url,
                is_primary=bool(row.is_primary),
                created_at=row.created_at,
                updated_at=row.updated_at,
            )
            for row in rows
        ],
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
    current_user: User = Depends(get_current_user),
) -> List[ClientOut]:
    query = db.query(Client)
    if current_user.role != "admin":
        allowed_ids = user_client_ids(db, current_user)
        if not allowed_ids:
            return []
        query = query.filter(Client.id.in_(allowed_ids))
    if status_filter:
        query = query.filter(Client.status == status_filter.strip().lower())
    clients = query.order_by(Client.created_at.desc()).all()
    target_sites_map = _load_target_sites_map(db, [client.id for client in clients])
    return [_client_to_out(client, target_sites_map.get(client.id, [])) for client in clients]


@router.post("", response_model=ClientOut, status_code=status.HTTP_201_CREATED)
def create_client(
    payload: ClientCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> ClientOut:
    client = Client(
        name=payload.name,
        email=payload.email,
        phone_number=payload.phone_number,
        status=payload.status,
    )
    normalized_target_sites = _normalize_target_site_payloads(
        payload.target_sites,
        legacy_primary_domain=payload.primary_domain,
        legacy_backlink_url=payload.backlink_url,
    )
    _set_client_legacy_target_fields(client, normalized_target_sites)
    db.add(client)
    try:
        db.flush()
        _replace_client_target_sites(db, client, normalized_target_sites)
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Client conflict.") from exc
    db.refresh(client)
    target_sites = _load_target_sites_map(db, [client.id]).get(client.id, [])
    return _client_to_out(client, target_sites)


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
    target_sites_explicit = "target_sites" in payload.__fields_set__

    if "primary_domain" in payload.__fields_set__:
        client.primary_domain = payload.primary_domain
    if "backlink_url" in payload.__fields_set__:
        client.backlink_url = payload.backlink_url
    if "email" in payload.__fields_set__:
        client.email = payload.email
    if "phone_number" in payload.__fields_set__:
        client.phone_number = payload.phone_number
    if payload.status is not None:
        client.status = payload.status

    if target_sites_explicit:
        normalized_target_sites = _normalize_target_site_payloads(
            payload.target_sites or [],
            legacy_primary_domain=payload.primary_domain if "primary_domain" in payload.__fields_set__ else client.primary_domain,
            legacy_backlink_url=payload.backlink_url if "backlink_url" in payload.__fields_set__ else client.backlink_url,
        )
        _set_client_legacy_target_fields(client, normalized_target_sites)
        _replace_client_target_sites(db, client, normalized_target_sites)
    elif "primary_domain" in payload.__fields_set__ or "backlink_url" in payload.__fields_set__:
        normalized_target_sites = _normalize_target_site_payloads(
            [],
            legacy_primary_domain=client.primary_domain,
            legacy_backlink_url=client.backlink_url,
        )
        _replace_client_target_sites(db, client, normalized_target_sites)

    db.add(client)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Client conflict.") from exc
    db.refresh(client)
    target_sites = _load_target_sites_map(db, [client.id]).get(client.id, [])
    return _client_to_out(client, target_sites)
