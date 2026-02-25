from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from ..db import get_db, get_sessionmaker
from ..portal_models import DbUpdaterSyncJob

try:
    from scripts.db_updater.run_master_site_sync import run_master_sync_for_file
except ImportError:  # pragma: no cover
    from portal_backend.scripts.db_updater.run_master_site_sync import run_master_sync_for_file

router = APIRouter(prefix="/db-updater", tags=["db_updater"])

_ALLOWED_EXTENSIONS = {".csv", ".xlsx"}
_MAX_UPLOAD_BYTES = int(os.getenv("DB_UPDATER_MAX_UPLOAD_BYTES", str(20 * 1024 * 1024)))
_UPLOAD_DIR = Path(os.getenv("DB_UPDATER_UPLOAD_DIR", "/tmp/db_updater_uploads")).resolve()


def _job_to_payload(job: DbUpdaterSyncJob) -> dict[str, Any]:
    return {
        "id": str(job.id),
        "status": job.status,
        "progress_percent": int(job.progress_percent or 0),
        "stage": job.stage or "",
        "message": job.message or "",
        "error": job.error or "",
        "dry_run": bool(job.dry_run),
        "file_name": job.file_name,
        "report": job.report,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
    }


def _update_job(job_id: str, **updates: Any) -> None:
    session = get_sessionmaker()()
    try:
        job_uuid = UUID(str(job_id))
        job = session.query(DbUpdaterSyncJob).filter(DbUpdaterSyncJob.id == job_uuid).first()
        if not job:
            return
        for key, value in updates.items():
            if hasattr(job, key):
                setattr(job, key, value)
        session.add(job)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _progress_cb(job_id: str):
    def inner(percent: int, stage: str, message: str | None) -> None:
        _update_job(
            job_id,
            status="running" if percent < 100 else "completed",
            progress_percent=int(percent),
            stage=stage,
            message=message or "",
        )

    return inner


def _run_job(job_id: str, upload_path: Path, dry_run: bool, delete_missing_sites: bool) -> None:
    try:
        report = run_master_sync_for_file(
            upload_path,
            dry_run=dry_run,
            delete_missing_sites=delete_missing_sites,
            progress_callback=_progress_cb(job_id),
        )
        _update_job(
            job_id,
            status="completed",
            progress_percent=100,
            stage="completed",
            message="Sync complete.",
            report=report,
        )
    except Exception as exc:
        _update_job(
            job_id,
            status="failed",
            stage="failed",
            message="Sync failed.",
            error=str(exc),
        )
    finally:
        try:
            upload_path.unlink(missing_ok=True)
        except Exception:
            pass


@router.post("/master-site-sync/jobs", status_code=status.HTTP_202_ACCEPTED)
async def create_master_site_sync_job(
    file: UploadFile = File(...),
    dry_run: bool = Form(False),
    delete_missing_sites: bool = Form(False),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    file_name = (file.filename or "").strip()
    if not file_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing file name.")

    suffix = Path(file_name).suffix.lower()
    if suffix not in _ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only CSV and XLSX files are supported.")

    _UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Uploaded file is empty.")
    if len(payload) > _MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="Uploaded file is too large.")

    with NamedTemporaryFile(prefix="master_site_sync_", suffix=suffix, dir=_UPLOAD_DIR, delete=False) as tmp:
        tmp.write(payload)
        upload_path = Path(tmp.name)

    job = DbUpdaterSyncJob(
        status="queued",
        progress_percent=2,
        stage="queued",
        error="",
        dry_run=bool(dry_run),
        file_name=file_name,
        report=None,
        message=("Queued delete+sync job." if delete_missing_sites else f"Queued {file_name}."),
        created_by_user_id=None,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    job_id = str(job.id)

    import threading
    thread = threading.Thread(
        target=_run_job,
        args=(job_id, upload_path, bool(dry_run), bool(delete_missing_sites)),
        daemon=True,
    )
    thread.start()
    return {"job_id": job_id, "status": "queued"}


@router.get("/master-site-sync/jobs/{job_id}")
def get_master_site_sync_job(job_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    try:
        job_uuid = UUID(str(job_id))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found.") from exc
    job = db.query(DbUpdaterSyncJob).filter(DbUpdaterSyncJob.id == job_uuid).first()
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found.")
    return _job_to_payload(job)


@router.get("/master-site-sync/jobs")
def list_master_site_sync_jobs(
    limit: int = 20,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit or 20), 100))
    jobs = (
        db.query(DbUpdaterSyncJob)
        .filter(DbUpdaterSyncJob.job_type == "master_site_sync")
        .order_by(DbUpdaterSyncJob.updated_at.desc(), DbUpdaterSyncJob.created_at.desc())
        .limit(safe_limit)
        .all()
    )
    return {"items": [_job_to_payload(job) for job in jobs]}
