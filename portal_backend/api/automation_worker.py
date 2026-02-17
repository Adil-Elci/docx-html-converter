from __future__ import annotations

import os
import logging
import threading
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from uuid import UUID

from sqlalchemy.orm import Session, sessionmaker

from .automation_service import (
    AutomationError,
    converter_target_from_site_url,
    get_runtime_config,
    run_guest_post_pipeline,
)
from .portal_models import Asset, Job, JobEvent, Site, SiteCategory, SiteCredential, SiteDefaultCategory, Submission

logger = logging.getLogger("portal_backend.automation")


class AutomationJobWorker:
    def __init__(self, db_sessionmaker: sessionmaker):
        self._sessionmaker = db_sessionmaker
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        logger.info("automation.worker.start")
        self._thread = threading.Thread(target=self._run, name="automation-job-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("automation.worker.stop")

    def _run(self) -> None:
        poll_interval = _read_int_env("AUTOMATION_WORKER_POLL_SECONDS", 2)
        while not self._stop_event.is_set():
            try:
                processed = self._process_next_job()
            except Exception:
                logger.exception("automation.worker.loop_error")
                processed = False
            if not processed:
                self._stop_event.wait(poll_interval)

    def _process_next_job(self) -> bool:
        with self._sessionmaker() as session:
            job = (
                session.query(Job)
                .filter(Job.job_status.in_(("queued", "retrying")))
                .order_by(Job.created_at.asc())
                .with_for_update(skip_locked=True)
                .first()
            )
            if not job:
                session.rollback()
                return False

            job.job_status = "processing"
            job.attempt_count = int(job.attempt_count or 0) + 1
            session.add(job)
            session.commit()
            job_id = job.id
            logger.info("automation.worker.claimed job_id=%s attempt=%s", job_id, job.attempt_count)

        self._process_claimed_job(job_id)
        return True

    def _process_claimed_job(self, job_id: UUID) -> None:
        max_attempts = _read_int_env("AUTOMATION_JOB_MAX_ATTEMPTS", 3)
        try:
            run_config = get_runtime_config()
        except AutomationError as exc:
            logger.warning("automation.worker.config_error job_id=%s error=%s", job_id, str(exc))
            self._mark_failed_or_retry(job_id, max_attempts=max_attempts, error_message=str(exc))
            return
        if not run_config["leonardo_api_key"]:
            logger.warning("automation.worker.missing_leonardo_key job_id=%s", job_id)
            self._mark_failed_or_retry(
                job_id,
                max_attempts=max_attempts,
                error_message="LEONARDO_API_KEY is not set.",
            )
            return

        try:
            payload = self._load_job_payload(job_id)
            self._append_event(
                job_id,
                "converter_called",
                {
                    "source_url": payload["source_url"],
                    "target_site": payload["converter_target_site"],
                    "attempt": payload["attempt_count"],
                },
            )
            pipeline_result = run_guest_post_pipeline(
                source_url=payload["source_url"],
                target_site=payload["converter_target_site"],
                site_url=payload["site_url"],
                wp_rest_base=payload["wp_rest_base"],
                wp_username=payload["wp_username"],
                wp_app_password=payload["wp_app_password"],
                existing_wp_post_id=payload["existing_wp_post_id"],
                post_status=payload["post_status"],
                author_id=payload["author_id"],
                category_ids=payload["category_ids"],
                category_candidates=payload["category_candidates"],
                converter_endpoint=run_config["converter_endpoint"],
                leonardo_api_key=run_config["leonardo_api_key"],
                leonardo_base_url=run_config["leonardo_base_url"],
                leonardo_model_id=run_config["leonardo_model_id"],
                timeout_seconds=run_config["timeout_seconds"],
                poll_timeout_seconds=run_config["poll_timeout_seconds"],
                poll_interval_seconds=run_config["poll_interval_seconds"],
                image_width=run_config["image_width"],
                image_height=run_config["image_height"],
                category_llm_enabled=run_config["category_llm_enabled"],
                category_llm_api_key=run_config["category_llm_api_key"],
                category_llm_base_url=run_config["category_llm_base_url"],
                category_llm_model=run_config["category_llm_model"],
                category_llm_max_categories=run_config["category_llm_max_categories"],
                category_llm_confidence_threshold=run_config["category_llm_confidence_threshold"],
            )
            self._mark_success(
                job_id,
                converted=pipeline_result["converted"],
                image_url=pipeline_result["image_url"],
                media_url=pipeline_result["media_url"],
                post_payload=pipeline_result["post_payload"],
                post_event_type=pipeline_result["post_event_type"],
                selected_category_ids=pipeline_result["selected_category_ids"],
                leonardo_model_id=run_config["leonardo_model_id"],
            )
            logger.info("automation.worker.succeeded job_id=%s", job_id)
        except (AutomationError, RuntimeError) as exc:
            logger.warning("automation.worker.failed job_id=%s error=%s", job_id, str(exc))
            self._mark_failed_or_retry(job_id, max_attempts=max_attempts, error_message=str(exc))
        except Exception as exc:
            logger.exception("automation.worker.unexpected_error job_id=%s", job_id)
            self._mark_failed_or_retry(
                job_id,
                max_attempts=max_attempts,
                error_message=f"Unexpected error ({exc.__class__.__name__}): {exc}",
            )

    def _load_job_payload(self, job_id: UUID) -> Dict[str, Any]:
        with self._sessionmaker() as session:
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                raise RuntimeError("Job not found.")
            submission = session.query(Submission).filter(Submission.id == job.submission_id).first()
            if not submission:
                raise RuntimeError("Submission not found.")
            site = session.query(Site).filter(Site.id == job.site_id, Site.status == "active").first()
            if not site:
                raise RuntimeError("Site is not active.")
            credential = (
                session.query(SiteCredential)
                .filter(SiteCredential.site_id == site.id, SiteCredential.enabled.is_(True))
                .order_by(SiteCredential.created_at.desc())
                .first()
            )
            if not credential:
                raise RuntimeError("No enabled site credential found for site.")

            if submission.source_type == "google-doc":
                source_url = (submission.doc_url or "").strip()
            else:
                source_url = (submission.file_url or "").strip()
            if not source_url:
                raise RuntimeError("Submission source URL is empty.")

            parsed_notes = _parse_notes(submission.notes)
            post_status = parsed_notes.get("post_status", "publish").strip().lower()
            if post_status not in {"draft", "publish"}:
                post_status = "publish"

            credential_author_id_raw = credential.author_id
            credential_author_id = None
            if credential_author_id_raw is not None:
                try:
                    parsed_credential_author_id = int(credential_author_id_raw)
                except (TypeError, ValueError):
                    parsed_credential_author_id = 0
                if parsed_credential_author_id > 0:
                    credential_author_id = parsed_credential_author_id
            default_author_id = credential_author_id or _read_int_env("AUTOMATION_POST_AUTHOR_ID", 4)
            author_id = _safe_int(parsed_notes.get("author_id"), default=default_author_id)
            if author_id <= 0:
                author_id = default_author_id

            converter_target_site = converter_target_from_site_url(site.site_url)
            if not converter_target_site:
                converter_target_site = (urlparse(site.site_url).hostname or "").strip().lower()
            if not converter_target_site:
                raise RuntimeError("Failed to resolve converter target_site from site_url.")
            category_ids = [
                int(row.wp_category_id)
                for row in (
                    session.query(SiteDefaultCategory)
                    .filter(
                        SiteDefaultCategory.site_id == site.id,
                        SiteDefaultCategory.enabled.is_(True),
                    )
                    .order_by(
                        SiteDefaultCategory.position.asc(),
                        SiteDefaultCategory.created_at.asc(),
                    )
                    .all()
                )
                if row.wp_category_id is not None and int(row.wp_category_id) > 0
            ]
            # Preserve order while removing duplicates.
            seen: set[int] = set()
            ordered_category_ids: list[int] = []
            for category_id in category_ids:
                if category_id in seen:
                    continue
                seen.add(category_id)
                ordered_category_ids.append(category_id)

            category_candidates: List[Dict[str, Any]] = []
            for row in (
                session.query(SiteCategory)
                .filter(
                    SiteCategory.site_id == site.id,
                    SiteCategory.enabled.is_(True),
                )
                .order_by(
                    SiteCategory.name.asc(),
                    SiteCategory.wp_category_id.asc(),
                )
                .all()
            ):
                raw_id = row.wp_category_id
                if raw_id is None:
                    continue
                category_id = int(raw_id)
                if category_id <= 0:
                    continue
                category_candidates.append(
                    {
                        "id": category_id,
                        "name": (row.name or "").strip(),
                        "slug": (row.slug or "").strip(),
                    }
                )

            return {
                "source_url": source_url,
                "converter_target_site": converter_target_site,
                "site_url": site.site_url,
                "wp_rest_base": site.wp_rest_base,
                "wp_username": credential.wp_username,
                "wp_app_password": credential.wp_app_password,
                "existing_wp_post_id": job.wp_post_id,
                "post_status": post_status,
                "author_id": author_id,
                "category_ids": ordered_category_ids,
                "category_candidates": category_candidates,
                "attempt_count": int(job.attempt_count or 0),
            }

    def _append_event(self, job_id: UUID, event_type: str, payload: Dict[str, Any]) -> None:
        with self._sessionmaker() as session:
            event = JobEvent(job_id=job_id, event_type=event_type, payload=payload)
            session.add(event)
            session.commit()

    def _mark_success(
        self,
        job_id: UUID,
        *,
        converted: Dict[str, Any],
        image_url: str,
        media_url: Optional[str],
        post_payload: Dict[str, Any],
        post_event_type: str,
        selected_category_ids: List[int],
        leonardo_model_id: str,
    ) -> None:
        with self._sessionmaker() as session:
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                return

            wp_post_id = post_payload.get("id")
            wp_post_url = post_payload.get("link")
            if wp_post_id is not None:
                job.wp_post_id = int(wp_post_id)
            if isinstance(wp_post_url, str) and wp_post_url.strip():
                job.wp_post_url = wp_post_url.strip()

            job.job_status = "succeeded"
            job.last_error = None
            session.add(job)

            session.add(
                JobEvent(
                    job_id=job_id,
                    event_type="converter_ok",
                    payload={
                        "title": converted.get("title"),
                        "slug": converted.get("slug"),
                    },
                )
            )
            session.add(
                JobEvent(
                    job_id=job_id,
                    event_type="image_prompt_ok",
                    payload={"image_prompt": converted.get("image_prompt")},
                )
            )
            session.add(
                JobEvent(
                    job_id=job_id,
                    event_type="image_generated",
                    payload={"source_url": image_url},
                )
            )
            session.add(
                Asset(
                    job_id=job_id,
                    asset_type="featured_image",
                    provider="leonardo",
                    source_url=image_url,
                    storage_url=media_url,
                    meta={"model_id": leonardo_model_id},
                )
            )
            session.add(
                JobEvent(
                    job_id=job_id,
                    event_type=post_event_type,
                    payload={
                        "wp_post_id": job.wp_post_id,
                        "wp_post_url": job.wp_post_url,
                        "category_ids": selected_category_ids,
                    },
                )
            )
            session.commit()

    def _mark_failed_or_retry(self, job_id: UUID, *, max_attempts: int, error_message: str) -> None:
        with self._sessionmaker() as session:
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                return

            attempts = int(job.attempt_count or 0)
            should_retry = attempts < max_attempts
            job.job_status = "retrying" if should_retry else "failed"
            job.last_error = error_message[:2000]
            session.add(job)
            session.add(
                JobEvent(
                    job_id=job_id,
                    event_type="failed",
                    payload={
                        "error": error_message[:2000],
                        "attempt": attempts,
                        "max_attempts": max_attempts,
                        "will_retry": should_retry,
                    },
                )
            )
            session.commit()
            logger.warning(
                "automation.worker.marked job_id=%s status=%s attempt=%s max_attempts=%s",
                job_id,
                job.job_status,
                attempts,
                max_attempts,
            )


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _safe_int(value: Optional[str], default: int) -> int:
    if value is None:
        return default
    try:
        return int(value.strip())
    except (ValueError, AttributeError):
        return default


def _parse_notes(notes: Optional[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not notes:
        return out
    for part in notes.split(";"):
        cleaned = part.strip()
        if "=" not in cleaned:
            continue
        key, value = cleaned.split("=", 1)
        key_clean = key.strip().lower()
        value_clean = value.strip()
        if key_clean:
            out[key_clean] = value_clean
    return out
