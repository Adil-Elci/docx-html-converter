from __future__ import annotations

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ..auth import ensure_client_access, ensure_site_access, get_current_user, require_admin, user_client_ids
from ..db import get_db
from ..portal_models import Asset, Job, JobEvent, Submission, User
from ..portal_schemas import (
    AssetCreate,
    AssetOut,
    JobCreate,
    JobEventCreate,
    JobEventOut,
    JobOut,
    JobUpdate,
)

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _job_to_out(job: Job) -> JobOut:
    return JobOut(
        id=job.id,
        submission_id=job.submission_id,
        client_id=job.client_id,
        site_id=job.site_id,
        job_status=job.job_status,
        attempt_count=job.attempt_count,
        last_error=job.last_error,
        wp_post_id=job.wp_post_id,
        wp_post_url=job.wp_post_url,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def _event_to_out(event: JobEvent) -> JobEventOut:
    return JobEventOut(
        id=event.id,
        job_id=event.job_id,
        event_type=event.event_type,
        payload=event.payload,
        created_at=event.created_at,
    )


def _asset_to_out(asset: Asset) -> AssetOut:
    return AssetOut(
        id=asset.id,
        job_id=asset.job_id,
        asset_type=asset.asset_type,
        provider=asset.provider,
        source_url=asset.source_url,
        storage_url=asset.storage_url,
        meta=asset.meta,
        created_at=asset.created_at,
    )


def _get_submission_or_404(db: Session, submission_id: UUID) -> Submission:
    submission = db.query(Submission).filter(Submission.id == submission_id).first()
    if not submission:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found.")
    return submission


def _get_job_or_404(db: Session, job_id: UUID) -> Job:
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found.")
    return job


@router.get("", response_model=List[JobOut])
def list_jobs(
    submission_id: Optional[UUID] = Query(default=None),
    client_id: Optional[UUID] = Query(default=None),
    site_id: Optional[UUID] = Query(default=None),
    job_status: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[JobOut]:
    query = db.query(Job)
    if current_user.role != "admin":
        allowed_client_ids = user_client_ids(db, current_user)
        if not allowed_client_ids:
            return []
        query = query.filter(Job.client_id.in_(allowed_client_ids))
    if submission_id is not None:
        query = query.filter(Job.submission_id == submission_id)
    if client_id is not None:
        if current_user.role != "admin":
            ensure_client_access(db, current_user, client_id)
        query = query.filter(Job.client_id == client_id)
    if site_id is not None:
        if current_user.role != "admin":
            ensure_site_access(db, current_user, site_id)
        query = query.filter(Job.site_id == site_id)
    if job_status:
        query = query.filter(Job.job_status == job_status.strip().lower())
    jobs = query.order_by(Job.created_at.desc()).all()
    return [_job_to_out(job) for job in jobs]


@router.post("", response_model=JobOut, status_code=status.HTTP_201_CREATED)
def create_job(
    payload: JobCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> JobOut:
    submission = _get_submission_or_404(db, payload.submission_id)

    client_id = payload.client_id if payload.client_id is not None else submission.client_id
    site_id = payload.site_id if payload.site_id is not None else submission.site_id

    if client_id != submission.client_id or site_id != submission.site_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Job client/site must match the referenced submission.",
        )

    job = Job(
        submission_id=payload.submission_id,
        client_id=client_id,
        site_id=site_id,
        job_status=payload.job_status,
        attempt_count=payload.attempt_count,
        last_error=payload.last_error,
        wp_post_id=payload.wp_post_id,
        wp_post_url=payload.wp_post_url,
    )

    db.add(job)
    db.commit()
    db.refresh(job)
    return _job_to_out(job)


@router.get("/{job_id}", response_model=JobOut)
def get_job(
    job_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> JobOut:
    job = _get_job_or_404(db, job_id)
    if current_user.role != "admin":
        ensure_client_access(db, current_user, job.client_id)
        ensure_site_access(db, current_user, job.site_id)
    return _job_to_out(job)


@router.patch("/{job_id}", response_model=JobOut)
def update_job(
    job_id: UUID,
    payload: JobUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> JobOut:
    job = _get_job_or_404(db, job_id)

    if payload.job_status is not None:
        job.job_status = payload.job_status
    if payload.attempt_count is not None:
        job.attempt_count = payload.attempt_count
    if payload.last_error is not None:
        job.last_error = payload.last_error
    if payload.wp_post_id is not None:
        job.wp_post_id = payload.wp_post_id
    if payload.wp_post_url is not None:
        job.wp_post_url = payload.wp_post_url

    db.add(job)
    db.commit()
    db.refresh(job)
    return _job_to_out(job)


@router.get("/{job_id}/events", response_model=List[JobEventOut])
def list_job_events(
    job_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[JobEventOut]:
    job = _get_job_or_404(db, job_id)
    if current_user.role != "admin":
        ensure_client_access(db, current_user, job.client_id)
        ensure_site_access(db, current_user, job.site_id)
    events = db.query(JobEvent).filter(JobEvent.job_id == job_id).order_by(JobEvent.created_at.asc()).all()
    return [_event_to_out(event) for event in events]


@router.post("/{job_id}/events", response_model=JobEventOut, status_code=status.HTTP_201_CREATED)
def create_job_event(
    job_id: UUID,
    payload: JobEventCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> JobEventOut:
    _get_job_or_404(db, job_id)

    event = JobEvent(
        job_id=job_id,
        event_type=payload.event_type,
        payload=payload.payload,
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return _event_to_out(event)


@router.get("/{job_id}/assets", response_model=List[AssetOut])
def list_job_assets(
    job_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> List[AssetOut]:
    job = _get_job_or_404(db, job_id)
    if current_user.role != "admin":
        ensure_client_access(db, current_user, job.client_id)
        ensure_site_access(db, current_user, job.site_id)
    assets = db.query(Asset).filter(Asset.job_id == job_id).order_by(Asset.created_at.asc()).all()
    return [_asset_to_out(asset) for asset in assets]


@router.post("/{job_id}/assets", response_model=AssetOut, status_code=status.HTTP_201_CREATED)
def create_job_asset(
    job_id: UUID,
    payload: AssetCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> AssetOut:
    _get_job_or_404(db, job_id)

    asset = Asset(
        job_id=job_id,
        asset_type=payload.asset_type,
        provider=payload.provider,
        source_url=payload.source_url,
        storage_url=payload.storage_url,
        meta=payload.meta,
    )
    db.add(asset)
    db.commit()
    db.refresh(asset)
    return _asset_to_out(asset)
