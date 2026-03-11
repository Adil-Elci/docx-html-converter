from __future__ import annotations

import datetime
import os
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse
from uuid import UUID

from sqlalchemy import func as sa_func
from sqlalchemy.orm import Session, sessionmaker

from .automation_service import (
    AutomationError,
    converter_publishing_site_from_site_url,
    get_runtime_config,
    run_submit_article_pipeline,
    run_create_article_pipeline,
)
from .internal_linking import build_creator_internal_link_inventory, upsert_publishing_site_article
from .internal_linking_sync import fetch_creator_internal_link_inventory_for_site, run_internal_link_inventory_sync
from .portal_models import (
    Asset,
    ClientTargetSite,
    CreatorOutput,
    Job,
    JobEvent,
    Site,
    SiteCategory,
    SiteCredential,
    SiteDefaultCategory,
    Submission,
)
from .site_profiles import (
    ensure_publishing_site_profile,
    get_combined_target_profile,
    get_latest_site_profile,
    normalize_site_profile_url,
)
from .site_analysis_cache import (
    PHASE1_TARGET_ANALYSIS_CACHE_KIND,
    get_latest_site_analysis_cache,
    normalize_site_analysis_url,
    upsert_site_analysis_cache,
)

logger = logging.getLogger("portal_backend.automation")


class AutomationJobWorker:
    """Queue worker that processes automation jobs using a thread pool.

    Uses PostgreSQL ``FOR UPDATE SKIP LOCKED`` to safely claim jobs, and a
    ``ThreadPoolExecutor`` so multiple jobs can run in parallel.

    Env vars:
        AUTOMATION_WORKER_CONCURRENCY – max parallel jobs (default 3).
        AUTOMATION_WORKER_POLL_SECONDS – seconds between poll cycles (default 2).
        AUTOMATION_WORKER_STALE_MINUTES – minutes before a "processing" job
            is considered stale and auto-requeued (default 30).
        AUTOMATION_WORKER_STALE_SWEEP_SECONDS – seconds between stale-job
            recovery sweeps (default 300 = 5 minutes).
    """

    def __init__(self, db_sessionmaker: sessionmaker):
        self._sessionmaker = db_sessionmaker
        self._stop_event = threading.Event()
        self._dispatcher_thread: Optional[threading.Thread] = None
        self._sweeper_thread: Optional[threading.Thread] = None

        self._concurrency = _read_int_env("AUTOMATION_WORKER_CONCURRENCY", 3)
        self._pool: Optional[ThreadPoolExecutor] = None

        # Track in-flight jobs so we never double-dispatch the same job_id.
        self._in_flight_lock = threading.Lock()
        self._in_flight: Set[UUID] = set()

        # Lightweight counters for the stats endpoint.
        self._total_processed = 0
        self._total_succeeded = 0
        self._total_failed = 0
        self._started_at: Optional[datetime.datetime] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._dispatcher_thread and self._dispatcher_thread.is_alive():
            return
        logger.info(
            "automation.worker.start concurrency=%s",
            self._concurrency,
        )
        self._started_at = datetime.datetime.utcnow()
        self._pool = ThreadPoolExecutor(
            max_workers=self._concurrency,
            thread_name_prefix="job-worker",
        )
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch_loop,
            name="job-dispatcher",
            daemon=True,
        )
        self._dispatcher_thread.start()

        self._sweeper_thread = threading.Thread(
            target=self._stale_sweep_loop,
            name="job-stale-sweeper",
            daemon=True,
        )
        self._sweeper_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._dispatcher_thread:
            self._dispatcher_thread.join(timeout=5)
        if self._sweeper_thread:
            self._sweeper_thread.join(timeout=5)
        if self._pool:
            self._pool.shutdown(wait=False)
        logger.info("automation.worker.stop")

    # ------------------------------------------------------------------
    # Stats (consumed by /queue/stats endpoint)
    # ------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Return a snapshot of queue / worker statistics."""
        with self._in_flight_lock:
            active = len(self._in_flight)
            active_ids = sorted(str(jid) for jid in self._in_flight)

        # Query queue depth directly from the DB.
        queued_count = 0
        try:
            with self._sessionmaker() as session:
                queued_count = (
                    session.query(sa_func.count(Job.id))
                    .filter(Job.job_status.in_(("queued", "retrying")))
                    .scalar()
                ) or 0
        except Exception:
            logger.debug("automation.worker.stats_query_error", exc_info=True)

        return {
            "worker_running": self._dispatcher_thread is not None
            and self._dispatcher_thread.is_alive(),
            "concurrency": self._concurrency,
            "active_jobs": active,
            "active_job_ids": active_ids,
            "queued_jobs": queued_count,
            "total_processed": self._total_processed,
            "total_succeeded": self._total_succeeded,
            "total_failed": self._total_failed,
            "started_at": self._started_at.isoformat() + "Z" if self._started_at else None,
        }

    # ------------------------------------------------------------------
    # Dispatcher loop — polls DB and submits jobs to the pool
    # ------------------------------------------------------------------

    def _dispatch_loop(self) -> None:
        poll_interval = _read_int_env("AUTOMATION_WORKER_POLL_SECONDS", 2)
        while not self._stop_event.is_set():
            try:
                dispatched = self._try_dispatch_jobs()
            except Exception:
                logger.exception("automation.worker.dispatch_loop_error")
                dispatched = False
            if not dispatched:
                self._stop_event.wait(poll_interval)

    def _available_slots(self) -> int:
        with self._in_flight_lock:
            return max(0, self._concurrency - len(self._in_flight))

    def _try_dispatch_jobs(self) -> bool:
        """Claim up to N available jobs and submit them to the thread pool.

        Returns True if at least one job was dispatched.
        """
        slots = self._available_slots()
        if slots <= 0:
            return False

        dispatched_any = False
        for _ in range(slots):
            job_id = self._claim_next_job()
            if job_id is None:
                break
            dispatched_any = True
            future = self._pool.submit(self._run_job_wrapper, job_id)  # type: ignore[union-attr]
            future.add_done_callback(lambda f, jid=job_id: self._on_job_done(jid, f))
        return dispatched_any

    def _claim_next_job(self) -> Optional[UUID]:
        """Atomically claim the oldest queued/retrying job and return its ID."""
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
                return None

            # Guard against dispatcher racing itself (shouldn't happen, but
            # defensive).
            with self._in_flight_lock:
                if job.id in self._in_flight:
                    session.rollback()
                    return None
                self._in_flight.add(job.id)

            job.job_status = "processing"
            job.attempt_count = int(job.attempt_count or 0) + 1
            job.updated_at = datetime.datetime.now(datetime.timezone.utc)
            session.add(job)
            session.commit()
            logger.info(
                "automation.worker.claimed job_id=%s attempt=%s",
                job.id,
                job.attempt_count,
            )
            return job.id

    def _run_job_wrapper(self, job_id: UUID) -> None:
        """Thin wrapper around _process_claimed_job for the thread pool."""
        try:
            self._process_claimed_job(job_id)
        except Exception:
            logger.exception("automation.worker.job_wrapper_error job_id=%s", job_id)

    def _on_job_done(self, job_id: UUID, future: Future) -> None:  # type: ignore[type-arg]
        """Callback executed when a job future completes — updates counters."""
        with self._in_flight_lock:
            self._in_flight.discard(job_id)

        self._total_processed += 1

        # Check final status to update succeeded/failed counters.
        try:
            with self._sessionmaker() as session:
                row = session.query(Job.job_status).filter(Job.id == job_id).first()
                if row:
                    status = row[0]
                    if status in ("succeeded", "pending_approval"):
                        self._total_succeeded += 1
                    elif status == "failed":
                        self._total_failed += 1
        except Exception:
            logger.debug("automation.worker.on_done_query_error job_id=%s", job_id, exc_info=True)

    # ------------------------------------------------------------------
    # Stale-job recovery sweep
    # ------------------------------------------------------------------

    def _stale_sweep_loop(self) -> None:
        sweep_interval = _read_int_env("AUTOMATION_WORKER_STALE_SWEEP_SECONDS", 300)
        stale_minutes = _read_int_env("AUTOMATION_WORKER_STALE_MINUTES", 30)
        # Wait one full interval before the first sweep so normal startup
        # jobs don't get immediately requeued.
        self._stop_event.wait(sweep_interval)
        while not self._stop_event.is_set():
            try:
                self._recover_stale_jobs(stale_minutes)
            except Exception:
                logger.exception("automation.worker.stale_sweep_error")
            self._stop_event.wait(sweep_interval)

    def _recover_stale_jobs(self, stale_minutes: int) -> None:
        """Requeue jobs stuck in 'processing' for longer than *stale_minutes*.

        Only requeues jobs that are NOT currently in-flight in this worker
        instance (they might be legitimately running).
        """
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(minutes=stale_minutes)
        with self._sessionmaker() as session:
            stale_jobs = (
                session.query(Job)
                .filter(
                    Job.job_status == "processing",
                    Job.updated_at < cutoff,
                )
                .with_for_update(skip_locked=True)
                .all()
            )
            if not stale_jobs:
                return

            with self._in_flight_lock:
                current_in_flight = set(self._in_flight)

            requeued = 0
            for job in stale_jobs:
                if job.id in current_in_flight:
                    continue
                max_attempts = _read_int_env("AUTOMATION_JOB_MAX_ATTEMPTS", 3)
                attempts = int(job.attempt_count or 0)
                if attempts >= max_attempts:
                    job.job_status = "failed"
                    job.last_error = f"Stale: stuck in processing for >{stale_minutes}m (attempts exhausted)"
                    session.add(
                        JobEvent(
                            job_id=job.id,
                            event_type="failed",
                            payload={
                                "error": job.last_error,
                                "attempt": attempts,
                                "max_attempts": max_attempts,
                                "stale_recovery": True,
                            },
                        )
                    )
                else:
                    job.job_status = "retrying"
                    job.last_error = f"Stale: stuck in processing for >{stale_minutes}m — auto-requeued"
                    session.add(
                        JobEvent(
                            job_id=job.id,
                            event_type="failed",
                            payload={
                                "error": job.last_error,
                                "attempt": attempts,
                                "max_attempts": max_attempts,
                                "will_retry": True,
                                "stale_recovery": True,
                            },
                        )
                    )
                session.add(job)
                requeued += 1

            if requeued:
                session.commit()
                logger.info(
                    "automation.worker.stale_sweep requeued=%s stale_minutes=%s",
                    requeued,
                    stale_minutes,
                )

    def _process_claimed_job(self, job_id: UUID) -> None:
        max_attempts = _read_int_env("AUTOMATION_JOB_MAX_ATTEMPTS", 3)
        try:
            run_config = get_runtime_config()
        except AutomationError as exc:
            logger.warning("automation.worker.config_error job_id=%s error=%s", job_id, str(exc))
            self._mark_failed_or_retry(job_id, max_attempts=max_attempts, error_message=str(exc))
            return
        try:
            if self._is_job_canceled(job_id):
                logger.info("automation.worker.canceled_before_start job_id=%s", job_id)
                return
            payload = self._load_job_payload(job_id)
            if self._is_job_canceled(job_id):
                logger.info("automation.worker.canceled_before_run job_id=%s", job_id)
                return
            if payload.get("creator_mode"):
                if not (payload.get("target_site_url") or "").strip():
                    raise AutomationError("Article creation requests require target_site_url.")
                if not run_config.get("creator_endpoint"):
                    raise AutomationError("Creator endpoint is not configured.")
                self._append_event(
                    job_id,
                    "converter_called",
                    {
                        "source": "creator",
                        "target_site_url": payload.get("target_site_url"),
                        "publishing_site": payload.get("site_url"),
                        "attempt": payload.get("attempt_count"),
                    },
                )
                def _on_phase(phase: int, label: str, percent: int) -> None:
                    try:
                        self._append_event(
                            job_id,
                            "creator_phase",
                            {"phase": phase, "label": label, "percent": percent},
                        )
                    except Exception:
                        logger.debug("automation.worker.phase_event_failed job_id=%s phase=%s", job_id, phase)

                def _should_cancel() -> bool:
                    return self._is_job_canceled(job_id)

                pipeline_result = run_create_article_pipeline(
                    creator_endpoint=run_config["creator_endpoint"],
                    target_site_url=payload.get("target_site_url") or "",
                    publishing_site_url=payload.get("site_url") or "",
                    publishing_site_id=payload.get("publishing_site_id"),
                    client_target_site_id=payload.get("target_site_id"),
                    anchor=payload.get("anchor"),
                    topic=payload.get("topic"),
                    exclude_topics=payload.get("exclude_topics") or [],
                    internal_link_inventory=payload.get("internal_link_inventory") or [],
                    phase1_cache_payload=payload.get("phase1_cache_payload"),
                    phase1_cache_content_hash=payload.get("phase1_cache_content_hash"),
                    phase2_cache_payload=payload.get("phase2_cache_payload"),
                    phase2_cache_content_hash=payload.get("phase2_cache_content_hash"),
                    target_profile_payload=payload.get("target_profile_payload"),
                    target_profile_content_hash=payload.get("target_profile_content_hash"),
                    publishing_profile_payload=payload.get("publishing_profile_payload"),
                    publishing_profile_content_hash=payload.get("publishing_profile_content_hash"),
                    on_phase=_on_phase,
                    site_url=payload["site_url"],
                    wp_rest_base=payload["wp_rest_base"],
                    wp_username=payload["wp_username"],
                    wp_app_password=payload["wp_app_password"],
                    existing_wp_post_id=payload["existing_wp_post_id"],
                    post_status=payload["post_status"],
                    author_id=payload["author_id"],
                    category_ids=payload["category_ids"],
                    category_candidates=payload["category_candidates"],
                    timeout_seconds=run_config["timeout_seconds"],
                    creator_timeout_seconds=run_config["creator_timeout_seconds"],
                    poll_timeout_seconds=run_config["poll_timeout_seconds"],
                    poll_interval_seconds=run_config["poll_interval_seconds"],
                    image_width=run_config["image_width"],
                    image_height=run_config["image_height"],
                    leonardo_api_key=run_config["leonardo_api_key"],
                    leonardo_base_url=run_config["leonardo_base_url"],
                    leonardo_model_id=run_config["leonardo_model_id"],
                    category_llm_enabled=run_config["category_llm_enabled"],
                    category_llm_api_key=run_config["category_llm_api_key"],
                    category_llm_base_url=run_config["category_llm_base_url"],
                    category_llm_model=run_config["category_llm_model"],
                    category_llm_max_categories=run_config["category_llm_max_categories"],
                    category_llm_confidence_threshold=run_config["category_llm_confidence_threshold"],
                    should_cancel=_should_cancel,
                )
                if self._is_job_canceled(job_id):
                    logger.info("automation.worker.canceled_after_creator job_id=%s", job_id)
                    return
                self._mark_creator_success(
                    job_id,
                    creator_output=pipeline_result["creator_output"],
                    image_url=pipeline_result["image_url"],
                    media_url=pipeline_result["media_url"],
                    post_payload=pipeline_result["post_payload"],
                    post_event_type=pipeline_result["post_event_type"],
                    selected_category_ids=pipeline_result["selected_category_ids"],
                    leonardo_model_id=run_config["leonardo_model_id"],
                )
                logger.info("automation.worker.creator_succeeded job_id=%s", job_id)
                return

            if not run_config["leonardo_api_key"]:
                logger.warning("automation.worker.missing_leonardo_key job_id=%s", job_id)
                self._mark_failed_or_retry(
                    job_id,
                    max_attempts=max_attempts,
                    error_message="LEONARDO_API_KEY is not set.",
                )
                return

            self._append_event(
                job_id,
                "converter_called",
                {
                    "source_url": payload["source_url"],
                    "publishing_site": payload["converter_publishing_site"],
                    "attempt": payload["attempt_count"],
                },
            )
            pipeline_result = run_submit_article_pipeline(
                source_url=payload["source_url"],
                publishing_site=payload["converter_publishing_site"],
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

    def _is_job_canceled(self, job_id: UUID) -> bool:
        with self._sessionmaker() as session:
            job = session.query(Job.job_status).filter(Job.id == job_id).first()
            if not job:
                return False
            return job[0] == "canceled"

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

            parsed_notes = _parse_notes(submission.notes)
            creator_mode = parsed_notes.get("creator_mode", "").lower() == "true"

            if submission.source_type == "google-doc":
                source_url = (submission.doc_url or "").strip()
            else:
                source_url = (submission.file_url or "").strip()
            if not source_url and not creator_mode:
                raise RuntimeError("Submission source URL is empty.")
            post_status = parsed_notes.get("post_status", "publish").strip().lower()
            if post_status not in {"draft", "publish"}:
                post_status = "publish"
            if bool(job.requires_admin_approval):
                post_status = "draft"

            target_site_url = parsed_notes.get("client_target_site_url", "")
            target_site_id: Optional[UUID] = None
            raw_target_site_id = (parsed_notes.get("client_target_site_id") or "").strip()
            if raw_target_site_id:
                try:
                    target_site_id = UUID(raw_target_site_id)
                except ValueError:
                    target_site_id = None
            anchor = parsed_notes.get("anchor")
            topic = parsed_notes.get("topic")

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

            converter_publishing_site = converter_publishing_site_from_site_url(site.site_url)
            if not converter_publishing_site:
                converter_publishing_site = (urlparse(site.site_url).hostname or "").strip().lower()
            if not converter_publishing_site:
                raise RuntimeError("Failed to resolve converter publishing_site from site_url.")
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

            # Gather topics from previous creator outputs for the same
            # client + publishing site + target site so the creator can
            # avoid generating duplicate articles.
            exclude_topics: List[str] = []
            if creator_mode and target_site_url:
                prev_outputs = (
                    session.query(CreatorOutput.payload)
                    .filter(
                        CreatorOutput.client_id == job.client_id,
                        CreatorOutput.site_id == job.site_id,
                        CreatorOutput.target_site_url == target_site_url,
                        CreatorOutput.job_id != job.id,
                    )
                    .order_by(CreatorOutput.created_at.desc())
                    .limit(50)
                    .all()
                )
                for (prev_payload,) in prev_outputs:
                    if not isinstance(prev_payload, dict):
                        continue
                    phase3_data = prev_payload.get("phase3") or {}
                    prev_topic = phase3_data.get("final_article_topic", "")
                    if isinstance(prev_topic, str) and prev_topic.strip():
                        exclude_topics.append(prev_topic.strip())

            phase1_cache_payload: Optional[Dict[str, Any]] = None
            phase1_cache_content_hash = ""
            phase2_cache_payload: Optional[Dict[str, Any]] = None
            phase2_cache_content_hash = ""
            target_profile_payload: Optional[Dict[str, Any]] = None
            target_profile_content_hash = ""
            publishing_profile_payload: Optional[Dict[str, Any]] = None
            publishing_profile_content_hash = ""
            if creator_mode:
                if target_site_url:
                    target_root_url = ""
                    if target_site_id:
                        target_row = session.query(ClientTargetSite).filter(ClientTargetSite.id == target_site_id).first()
                        if target_row is not None:
                            target_root_url = str(target_row.target_site_root_url or "").strip()
                    normalized_target_url = normalize_site_analysis_url(target_site_url)
                    latest_phase1_cache = get_latest_site_analysis_cache(
                        session,
                        site_role="target",
                        site_type="target_site",
                        normalized_url=normalized_target_url,
                        cache_kind=PHASE1_TARGET_ANALYSIS_CACHE_KIND,
                        client_target_site_id=target_site_id,
                    )
                    if latest_phase1_cache and isinstance(latest_phase1_cache.payload, dict):
                        phase1_cache_payload = latest_phase1_cache.payload
                        phase1_cache_content_hash = str(latest_phase1_cache.content_hash or "").strip()
                    try:
                        target_profile_payload, target_profile_content_hash, _, _ = get_combined_target_profile(
                            session,
                            target_site_url=target_site_url,
                            target_site_root_url=target_root_url or None,
                            client_target_site_id=target_site_id,
                            timeout_seconds=10,
                            max_pages=3,
                        )
                    except Exception:
                        logger.warning("automation.worker.target_profile_ensure_failed job_id=%s", job_id, exc_info=True)
                elif creator_mode:
                    raise RuntimeError("Target site profile is required before running Creator.")
                if not target_profile_payload:
                    raise RuntimeError("Target site profile is required before running Creator.")

                normalized_site_url = normalize_site_analysis_url(site.site_url)
                latest_phase2_cache = get_latest_site_analysis_cache(
                    session,
                    site_role="host",
                    site_type="publishing_site",
                    normalized_url=normalized_site_url,
                    publishing_site_id=site.id,
                )
                if latest_phase2_cache and isinstance(latest_phase2_cache.payload, dict):
                    phase2_cache_payload = latest_phase2_cache.payload
                    phase2_cache_content_hash = str(latest_phase2_cache.content_hash or "").strip()
                try:
                    ensure_publishing_site_profile(
                        session,
                        site=site,
                        timeout_seconds=10,
                        max_pages=3,
                    )
                except Exception:
                    logger.warning("automation.worker.publishing_profile_ensure_failed job_id=%s", job_id, exc_info=True)
                latest_publishing_profile = get_latest_site_profile(
                    session,
                    profile_kind="publishing_site",
                    normalized_url=normalize_site_profile_url(site.site_url),
                    publishing_site_id=site.id,
                )
                if latest_publishing_profile and isinstance(latest_publishing_profile.payload, dict):
                    publishing_profile_payload = latest_publishing_profile.payload
                    publishing_profile_content_hash = str(latest_publishing_profile.content_hash or "").strip()
                elif creator_mode:
                    raise RuntimeError("Publishing site profile is required before running Creator.")

            internal_link_inventory: List[Dict[str, Any]] = []
            if creator_mode:
                try:
                    run_internal_link_inventory_sync(
                        self._sessionmaker,
                        site_url_filter=site.site_url,
                        per_page=100,
                        timeout_seconds=10,
                    )
                except Exception:
                    logger.warning("automation.worker.internal_link_inventory_refresh_failed job_id=%s", job_id, exc_info=True)
                internal_link_inventory = build_creator_internal_link_inventory(
                    session,
                    site_id=site.id,
                    limit=max(50, _read_int_env("INTERNAL_LINK_INVENTORY_LIMIT", 250)),
                )
                try:
                    live_internal_link_inventory = fetch_creator_internal_link_inventory_for_site(
                        site_url=site.site_url,
                        wp_rest_base=site.wp_rest_base,
                        wp_username=credential.wp_username,
                        wp_app_password=credential.wp_app_password,
                        per_page=max(50, min(100, _read_int_env("INTERNAL_LINK_INVENTORY_LIMIT", 250))),
                        timeout_seconds=10,
                    )
                    if live_internal_link_inventory:
                        internal_link_inventory = live_internal_link_inventory
                except Exception:
                    logger.warning("automation.worker.internal_link_inventory_live_fetch_failed job_id=%s", job_id, exc_info=True)

            return {
                "source_url": source_url,
                "converter_publishing_site": converter_publishing_site,
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
                "creator_mode": creator_mode,
                "target_site_url": target_site_url,
                "anchor": anchor,
                "topic": topic,
                "exclude_topics": exclude_topics,
                "internal_link_inventory": internal_link_inventory,
                "target_site_id": str(target_site_id) if target_site_id else "",
                "publishing_site_id": str(site.id),
                "phase1_cache_payload": phase1_cache_payload,
                "phase1_cache_content_hash": phase1_cache_content_hash,
                "phase2_cache_payload": phase2_cache_payload,
                "phase2_cache_content_hash": phase2_cache_content_hash,
                "target_profile_payload": target_profile_payload,
                "target_profile_content_hash": target_profile_content_hash,
                "publishing_profile_payload": publishing_profile_payload,
                "publishing_profile_content_hash": publishing_profile_content_hash,
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

            article = upsert_publishing_site_article(
                session,
                site_id=job.site_id,
                post_payload=post_payload,
                source="job",
            )
            if article is not None:
                session.add(article)

            job.job_status = "pending_approval" if bool(job.requires_admin_approval) else "succeeded"
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
                        "pending_admin_approval": bool(job.requires_admin_approval),
                    },
                )
            )
            session.commit()

    def _mark_creator_success(
        self,
        job_id: UUID,
        *,
        creator_output: Dict[str, Any],
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

            submission = session.query(Submission).filter(Submission.id == job.submission_id).first()
            if not submission:
                return

            wp_post_id = post_payload.get("id")
            wp_post_url = post_payload.get("link")
            if wp_post_id is not None:
                job.wp_post_id = int(wp_post_id)
            if isinstance(wp_post_url, str) and wp_post_url.strip():
                job.wp_post_url = wp_post_url.strip()

            article = upsert_publishing_site_article(
                session,
                site_id=job.site_id,
                post_payload=post_payload,
                source="job",
            )
            if article is not None:
                session.add(article)

            job.job_status = "pending_approval" if bool(job.requires_admin_approval) else "succeeded"
            job.last_error = None
            session.add(job)

            phase5 = creator_output.get("phase5") or {}
            phase6 = creator_output.get("phase6") or {}
            featured_prompt = ""
            if isinstance(phase6, dict):
                featured_image = phase6.get("featured_image")
                if isinstance(featured_image, dict):
                    featured_prompt = str(featured_image.get("prompt") or "").strip()

            session.add(
                JobEvent(
                    job_id=job_id,
                    event_type="converter_ok",
                    payload={
                        "source": "creator",
                        "title": phase5.get("meta_title") or phase5.get("title"),
                        "slug": phase5.get("slug"),
                    },
                )
            )
            if featured_prompt:
                session.add(
                    JobEvent(
                        job_id=job_id,
                        event_type="image_prompt_ok",
                        payload={"image_prompt": featured_prompt, "source": "creator"},
                    )
                )
            if image_url:
                session.add(
                    JobEvent(
                        job_id=job_id,
                        event_type="image_generated",
                        payload={"source_url": image_url, "source": "creator"},
                    )
                )
                session.add(
                    Asset(
                        job_id=job_id,
                        asset_type="featured_image",
                        provider="leonardo",
                        source_url=image_url,
                        storage_url=media_url,
                        meta={"model_id": leonardo_model_id, "source": "creator"},
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
                        "pending_admin_approval": bool(job.requires_admin_approval),
                        "source": "creator",
                    },
                )
            )
            session.add(
                CreatorOutput(
                    submission_id=submission.id,
                    job_id=job.id,
                    client_id=submission.client_id,
                    site_id=submission.site_id,
                    target_site_url=str(creator_output.get("target_site_url") or ""),
                    host_site_url=str(creator_output.get("host_site_url") or ""),
                    payload=creator_output,
                )
            )
            parsed_notes = _parse_notes(submission.notes)
            target_site_id: Optional[UUID] = None
            raw_target_site_id = (parsed_notes.get("client_target_site_id") or "").strip()
            if raw_target_site_id:
                try:
                    target_site_id = UUID(raw_target_site_id)
                except ValueError:
                    target_site_id = None

            phase1 = creator_output.get("phase1")
            phase1_cache_meta = creator_output.get("phase1_cache_meta")
            if isinstance(phase1, dict) and isinstance(phase1_cache_meta, dict):
                normalized_url = str(phase1_cache_meta.get("normalized_url") or "").strip()
                content_hash = str(phase1_cache_meta.get("content_hash") or "").strip()
                prompt_version = str(phase1_cache_meta.get("prompt_version") or "").strip()
                generator_mode = str(phase1_cache_meta.get("generator_mode") or "").strip()
                model_name = str(phase1_cache_meta.get("model_name") or "").strip()
                if normalized_url and content_hash and prompt_version and generator_mode:
                    upsert_site_analysis_cache(
                        session,
                        site_role="target",
                        site_type="target_site",
                        normalized_url=normalized_url,
                        content_hash=content_hash,
                        generator_mode=generator_mode,
                        payload=phase1,
                        prompt_version=prompt_version,
                        model_name=model_name,
                        cache_kind=PHASE1_TARGET_ANALYSIS_CACHE_KIND,
                        client_target_site_id=target_site_id,
                    )

            phase2 = creator_output.get("phase2")
            phase2_cache_meta = creator_output.get("phase2_cache_meta")
            if isinstance(phase2, dict) and isinstance(phase2_cache_meta, dict):
                normalized_url = str(phase2_cache_meta.get("normalized_url") or "").strip()
                content_hash = str(phase2_cache_meta.get("content_hash") or "").strip()
                prompt_version = str(phase2_cache_meta.get("prompt_version") or "").strip()
                generator_mode = str(phase2_cache_meta.get("generator_mode") or "").strip()
                model_name = str(phase2_cache_meta.get("model_name") or "").strip()
                if normalized_url and content_hash and prompt_version and generator_mode:
                    upsert_site_analysis_cache(
                        session,
                        site_role="host",
                        site_type="publishing_site",
                        normalized_url=normalized_url,
                        content_hash=content_hash,
                        generator_mode=generator_mode,
                        payload=phase2,
                        prompt_version=prompt_version,
                        model_name=model_name,
                        publishing_site_id=submission.site_id,
                    )
            session.commit()

    def _mark_failed_or_retry(self, job_id: UUID, *, max_attempts: int, error_message: str) -> None:
        with self._sessionmaker() as session:
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                return
            if job.job_status == "canceled":
                logger.info("automation.worker.skip_fail_mark job_id=%s status=canceled", job_id)
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
