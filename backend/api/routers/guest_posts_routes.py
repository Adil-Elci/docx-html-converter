from __future__ import annotations

from datetime import datetime, timezone
from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ..auth import get_current_user
from ..db import get_db
from ..portal_models import Client, GuestPost, TargetSite, User
from ..portal_schemas import GuestPostCreate, GuestPostOut, GuestPostUpdate
from ..portal_utils import generate_markdown, validate_backlink_url

router = APIRouter(prefix="/guest-posts", tags=["guest_posts"])


def _require_client(user: User) -> User:
    if user.role != "client":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Client access required.")
    if not user.client_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Client account missing.")
    return user


def _guest_post_to_out(post: GuestPost) -> GuestPostOut:
    return GuestPostOut(
        id=post.id,
        client_id=post.client_id,
        target_site_id=post.target_site_id,
        status=post.status,
        title_h1=post.title_h1,
        backlink_url=post.backlink_url,
        backlink_placement=post.backlink_placement,
        auto_backlink=post.auto_backlink,
        content_json=post.content_json,
        content_markdown=post.content_markdown,
        created_at=post.created_at,
        updated_at=post.updated_at,
        submitted_at=post.submitted_at,
    )


def _validate_target_site(db: Session, target_site_id: UUID) -> TargetSite:
    site = db.query(TargetSite).filter(TargetSite.id == target_site_id, TargetSite.active.is_(True)).first()
    if not site:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Target site is not active.")
    return site


def _get_client(db: Session, client_id: UUID) -> Client:
    client = db.query(Client).filter(Client.id == client_id, Client.active.is_(True)).first()
    if not client:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Client is not active.")
    return client


@router.post("", response_model=GuestPostOut)
def create_guest_post(
    payload: GuestPostCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> GuestPostOut:
    user = _require_client(current_user)
    _validate_target_site(db, payload.target_site_id)
    client = _get_client(db, user.client_id)

    backlink_url = validate_backlink_url(payload.backlink_url, client.website_domain)
    content_markdown = generate_markdown(
        title_h1=payload.title_h1,
        content_json=payload.content_json,
        backlink_url=backlink_url,
        auto_backlink=payload.auto_backlink,
        backlink_placement=payload.backlink_placement,
    )
    post = GuestPost(
        client_id=user.client_id,
        target_site_id=payload.target_site_id,
        status="draft",
        title_h1=payload.title_h1,
        backlink_url=backlink_url,
        backlink_placement=payload.backlink_placement,
        auto_backlink=payload.auto_backlink,
        content_json=payload.content_json.dict(),
        content_markdown=content_markdown,
    )
    db.add(post)
    db.commit()
    db.refresh(post)
    return _guest_post_to_out(post)


@router.get("", response_model=List[GuestPostOut])
def list_guest_posts(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[GuestPostOut]:
    user = _require_client(current_user)
    posts = (
        db.query(GuestPost)
        .filter(GuestPost.client_id == user.client_id)
        .order_by(GuestPost.created_at.desc())
        .all()
    )
    return [_guest_post_to_out(post) for post in posts]


@router.get("/{post_id}", response_model=GuestPostOut)
def get_guest_post(
    post_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> GuestPostOut:
    user = _require_client(current_user)
    post = db.query(GuestPost).filter(GuestPost.id == post_id, GuestPost.client_id == user.client_id).first()
    if not post:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Guest post not found.")
    return _guest_post_to_out(post)


@router.patch("/{post_id}", response_model=GuestPostOut)
def update_guest_post(
    post_id: UUID,
    payload: GuestPostUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> GuestPostOut:
    user = _require_client(current_user)
    post = db.query(GuestPost).filter(GuestPost.id == post_id, GuestPost.client_id == user.client_id).first()
    if not post:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Guest post not found.")
    if post.status != "draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Submitted posts are locked.")

    if payload.target_site_id is not None:
        _validate_target_site(db, payload.target_site_id)
        post.target_site_id = payload.target_site_id
    if payload.title_h1 is not None:
        post.title_h1 = payload.title_h1
    if payload.backlink_url is not None:
        client = _get_client(db, user.client_id)
        post.backlink_url = validate_backlink_url(payload.backlink_url, client.website_domain)
    if payload.auto_backlink is not None:
        post.auto_backlink = payload.auto_backlink
    if payload.backlink_placement is not None or payload.auto_backlink is not None:
        placement = payload.backlink_placement if payload.backlink_placement is not None else post.backlink_placement
        if post.auto_backlink:
            post.backlink_placement = None
        else:
            if placement not in {"intro", "conclusion"}:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="backlink_placement must be 'intro' or 'conclusion'.",
                )
            post.backlink_placement = placement
    if payload.content_json is not None:
        post.content_json = payload.content_json.dict()

    content_markdown = generate_markdown(
        title_h1=post.title_h1,
        content_json=post.content_json,
        backlink_url=post.backlink_url,
        auto_backlink=post.auto_backlink,
        backlink_placement=post.backlink_placement,
    )
    post.content_markdown = content_markdown

    db.add(post)
    db.commit()
    db.refresh(post)
    return _guest_post_to_out(post)


@router.post("/{post_id}/submit", response_model=GuestPostOut)
def submit_guest_post(
    post_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> GuestPostOut:
    user = _require_client(current_user)
    post = db.query(GuestPost).filter(GuestPost.id == post_id, GuestPost.client_id == user.client_id).first()
    if not post:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Guest post not found.")
    if post.status != "draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Guest post already submitted.")

    client = _get_client(db, user.client_id)
    post.backlink_url = validate_backlink_url(post.backlink_url, client.website_domain)

    post.content_markdown = generate_markdown(
        title_h1=post.title_h1,
        content_json=post.content_json,
        backlink_url=post.backlink_url,
        auto_backlink=post.auto_backlink,
        backlink_placement=post.backlink_placement,
    )
    post.status = "submitted"
    post.submitted_at = datetime.now(tz=timezone.utc)

    db.add(post)
    db.commit()
    db.refresh(post)
    return _guest_post_to_out(post)
