from __future__ import annotations

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime, timezone

from ..auth import require_admin
from ..db import get_db
from ..portal_models import GuestPost, User
from ..portal_schemas import GuestPostOut, GuestPostStatusUpdate

router = APIRouter(prefix="/admin/guest-posts", tags=["admin_guest_posts"])


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


@router.get("", response_model=List[GuestPostOut])
def list_guest_posts(
    status_filter: Optional[str] = Query(default=None, alias="status"),
    target_site_id: Optional[UUID] = Query(default=None),
    client_id: Optional[UUID] = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> List[GuestPostOut]:
    query = db.query(GuestPost)
    if status_filter:
        query = query.filter(GuestPost.status == status_filter)
    if target_site_id:
        query = query.filter(GuestPost.target_site_id == target_site_id)
    if client_id:
        query = query.filter(GuestPost.client_id == client_id)
    posts = query.order_by(GuestPost.created_at.desc()).all()
    return [_guest_post_to_out(post) for post in posts]


@router.get("/{post_id}", response_model=GuestPostOut)
def get_guest_post(
    post_id: UUID,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> GuestPostOut:
    post = db.query(GuestPost).filter(GuestPost.id == post_id).first()
    if not post:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Guest post not found.")
    return _guest_post_to_out(post)


@router.patch("/{post_id}/status", response_model=GuestPostOut)
def update_status(
    post_id: UUID,
    payload: GuestPostStatusUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> GuestPostOut:
    post = db.query(GuestPost).filter(GuestPost.id == post_id).first()
    if not post:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Guest post not found.")
    post.status = payload.status
    if payload.status == "submitted" and post.submitted_at is None:
        post.submitted_at = datetime.now(tz=timezone.utc)
    if payload.status == "draft":
        post.submitted_at = None
    db.add(post)
    db.commit()
    db.refresh(post)
    return _guest_post_to_out(post)
