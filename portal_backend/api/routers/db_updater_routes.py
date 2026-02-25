from __future__ import annotations

import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status

from ..auth import require_admin
from ..portal_models import User
from ...scripts.db_updater.run_master_site_sync import run_master_sync_for_file

router = APIRouter(prefix="/db-updater", tags=["db_updater"])

_ALLOWED_EXTENSIONS = {".csv", ".xlsx"}
_MAX_UPLOAD_BYTES = int(os.getenv("DB_UPDATER_MAX_UPLOAD_BYTES", str(20 * 1024 * 1024)))
_UPLOAD_DIR = Path(os.getenv("DB_UPDATER_UPLOAD_DIR", "/tmp/db_updater_uploads")).resolve()
_JOBS_LOCK = threading.Lock()
_JOBS: dict[str, dict[str, Any]] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _set_job(job_id: str, **updates: Any) -> None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id, {})
        job.update(updates)
        job["updated_at"] = _now_iso()
        _JOBS[job_id] = job


def _progress_cb(job_id: str):
    def inner(percent: int, stage: str, message: str | None) -> None:
        _set_job(
            job_id,
            status="running" if percent < 100 else "completed",
            progress_percent=int(percent),
            stage=stage,
            message=message or "",
        )

    return inner


def _run_job(job_id: str, upload_path: Path, dry_run: bool) -> None:
    try:
        report = run_master_sync_for_file(upload_path, dry_run=dry_run, progress_callback=_progress_cb(job_id))
        _set_job(
            job_id,
            status="completed",
            progress_percent=100,
            stage="completed",
            message="Sync complete.",
            report=report,
        )
    except Exception as exc:
        _set_job(
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
    _: User = Depends(require_admin),
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

    job_id = uuid4().hex
    _set_job(
        job_id,
        id=job_id,
        status="queued",
        progress_percent=2,
        stage="queued",
        message=f"Queued {file_name}.",
        error="",
        dry_run=bool(dry_run),
        file_name=file_name,
        report=None,
        created_at=_now_iso(),
    )

    thread = threading.Thread(target=_run_job, args=(job_id, upload_path, bool(dry_run)), daemon=True)
    thread.start()
    return {"job_id": job_id, "status": "queued"}


@router.get("/master-site-sync/jobs/{job_id}")
def get_master_site_sync_job(job_id: str, _: User = Depends(require_admin)) -> dict[str, Any]:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found.")
        return dict(job)

