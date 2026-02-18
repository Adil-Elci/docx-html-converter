from __future__ import annotations

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ..auth import (
    ensure_client_access,
    ensure_site_access,
    get_current_user,
    require_admin,
    user_client_ids,
    user_accessible_site_ids,
)
from ..db import get_db
from ..portal_models import Client, ClientSiteAccess, Site, Submission, User
from ..portal_schemas import SubmissionCreate, SubmissionOut, SubmissionUpdate

router = APIRouter(prefix="/submissions", tags=["submissions"])


def _submission_to_out(submission: Submission) -> SubmissionOut:
    return SubmissionOut(
        id=submission.id,
        client_id=submission.client_id,
        site_id=submission.site_id,
        source_type=submission.source_type,
        doc_url=submission.doc_url,
        file_url=submission.file_url,
        backlink_placement=submission.backlink_placement,
        post_status=submission.post_status,
        title=submission.title,
        raw_text=submission.raw_text,
        notes=submission.notes,
        status=submission.status,
        rejection_reason=submission.rejection_reason,
        created_at=submission.created_at,
        updated_at=submission.updated_at,
    )


def _require_active_client(db: Session, client_id: UUID) -> None:
    client = db.query(Client).filter(Client.id == client_id, Client.status == "active").first()
    if not client:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Client is not active.")


def _require_active_site(db: Session, site_id: UUID) -> None:
    site = db.query(Site).filter(Site.id == site_id, Site.status == "active").first()
    if not site:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Site is not active.")


def _require_access(db: Session, client_id: UUID, site_id: UUID) -> None:
    access = (
        db.query(ClientSiteAccess)
        .filter(
            ClientSiteAccess.client_id == client_id,
            ClientSiteAccess.site_id == site_id,
            ClientSiteAccess.enabled.is_(True),
        )
        .first()
    )
    if not access:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Client does not have enabled access to this site.",
        )


def _validate_source_payload(source_type: str, doc_url: Optional[str], file_url: Optional[str]) -> None:
    if source_type == "google-doc":
        if doc_url is None or file_url is not None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="google-doc requires doc_url and forbids file_url.",
            )
        return
    if source_type == "docx-upload":
        if file_url is None or doc_url is not None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="docx-upload requires file_url and forbids doc_url.",
            )
        return
    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Unsupported source_type.")


@router.get("", response_model=List[SubmissionOut])
def list_submissions(
    client_id: Optional[UUID] = Query(default=None),
    site_id: Optional[UUID] = Query(default=None),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[SubmissionOut]:
    query = db.query(Submission)
    if current_user.role != "admin":
        allowed_client_ids = user_client_ids(db, current_user)
        if not allowed_client_ids:
            return []
        query = query.filter(Submission.client_id.in_(allowed_client_ids))

        allowed_site_ids = user_accessible_site_ids(db, current_user)
        if allowed_site_ids:
            query = query.filter(Submission.site_id.in_(allowed_site_ids))
        else:
            return []

    if client_id is not None:
        if current_user.role != "admin":
            ensure_client_access(db, current_user, client_id)
        query = query.filter(Submission.client_id == client_id)
    if site_id is not None:
        if current_user.role != "admin":
            ensure_site_access(db, current_user, site_id)
        query = query.filter(Submission.site_id == site_id)
    if status_filter:
        query = query.filter(Submission.status == status_filter.strip().lower())

    submissions = query.order_by(Submission.created_at.desc()).all()
    return [_submission_to_out(submission) for submission in submissions]


@router.post("", response_model=SubmissionOut, status_code=status.HTTP_201_CREATED)
def create_submission(
    payload: SubmissionCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SubmissionOut:
    if current_user.role != "admin":
        ensure_client_access(db, current_user, payload.client_id)
        ensure_site_access(db, current_user, payload.site_id)
    _require_active_client(db, payload.client_id)
    _require_active_site(db, payload.site_id)
    _require_access(db, payload.client_id, payload.site_id)
    _validate_source_payload(payload.source_type, payload.doc_url, payload.file_url)

    submission = Submission(
        client_id=payload.client_id,
        site_id=payload.site_id,
        source_type=payload.source_type,
        doc_url=payload.doc_url,
        file_url=payload.file_url,
        backlink_placement=payload.backlink_placement,
        post_status=payload.post_status,
        title=payload.title,
        raw_text=payload.raw_text,
        notes=payload.notes,
        status=payload.status,
        rejection_reason=payload.rejection_reason,
    )

    db.add(submission)
    db.commit()
    db.refresh(submission)
    return _submission_to_out(submission)


@router.get("/{submission_id}", response_model=SubmissionOut)
def get_submission(
    submission_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SubmissionOut:
    submission = db.query(Submission).filter(Submission.id == submission_id).first()
    if not submission:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found.")
    if current_user.role != "admin":
        ensure_client_access(db, current_user, submission.client_id)
        ensure_site_access(db, current_user, submission.site_id)
    return _submission_to_out(submission)


@router.patch("/{submission_id}", response_model=SubmissionOut)
def update_submission(
    submission_id: UUID,
    payload: SubmissionUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> SubmissionOut:
    submission = db.query(Submission).filter(Submission.id == submission_id).first()
    if not submission:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found.")
    if current_user.role != "admin":
        ensure_client_access(db, current_user, submission.client_id)
        ensure_site_access(db, current_user, submission.site_id)
        if payload.client_id is not None and payload.client_id != submission.client_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Client reassignment is not allowed.")
        if payload.site_id is not None and payload.site_id != submission.site_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Site reassignment is not allowed.")

    merged_source_type = payload.source_type if payload.source_type is not None else submission.source_type
    merged_doc_url = payload.doc_url if payload.doc_url is not None else submission.doc_url
    merged_file_url = payload.file_url if payload.file_url is not None else submission.file_url

    if payload.client_id is not None:
        submission.client_id = payload.client_id
    if payload.site_id is not None:
        submission.site_id = payload.site_id
    if payload.source_type is not None:
        submission.source_type = payload.source_type
    if payload.doc_url is not None:
        submission.doc_url = payload.doc_url
    if payload.file_url is not None:
        submission.file_url = payload.file_url
    if payload.backlink_placement is not None:
        submission.backlink_placement = payload.backlink_placement
    if payload.post_status is not None:
        submission.post_status = payload.post_status
    if payload.title is not None:
        submission.title = payload.title
    if payload.raw_text is not None:
        submission.raw_text = payload.raw_text
    if payload.notes is not None:
        submission.notes = payload.notes
    if payload.status is not None:
        submission.status = payload.status
    if payload.rejection_reason is not None:
        submission.rejection_reason = payload.rejection_reason

    _require_active_client(db, submission.client_id)
    _require_active_site(db, submission.site_id)
    _require_access(db, submission.client_id, submission.site_id)
    _validate_source_payload(merged_source_type, merged_doc_url, merged_file_url)

    db.add(submission)
    db.commit()
    db.refresh(submission)
    return _submission_to_out(submission)
