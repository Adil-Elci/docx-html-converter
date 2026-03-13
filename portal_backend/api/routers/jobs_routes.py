from __future__ import annotations

from datetime import datetime, timezone
from html import escape
import logging
import os
import re
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from ..auth import ensure_client_access, ensure_site_access, get_current_user, require_admin, user_client_ids
from ..automation_service import (
    AutomationError,
    download_binary_file,
    generate_image_via_leonardo,
    get_runtime_config,
    wp_create_media_item,
    wp_get_media,
    wp_get_post,
    wp_publish_post,
    wp_update_post_featured_media,
)
from ..db import get_db
from ..internal_linking import upsert_publishing_site_article
from ..publish_notification_hook import send_client_publish_notification
from ..portal_models import Asset, Client, CreatorOutput, Job, JobEvent, Site, SiteCredential, Submission, User
from ..portal_schemas import (
    AssetCreate,
    AssetOut,
    JobCreate,
    JobEventCreate,
    JobEventOut,
    JobOut,
    JobUpdate,
    PublishedArticleOut,
    PublishedArticlesPageOut,
    PendingJobOut,
    PendingJobPublishOut,
    PendingJobRegenerateImageOut,
    PendingJobRejectIn,
    PendingJobRejectOut,
    RejectedArticleOut,
    RejectedArticlesPageOut,
)

router = APIRouter(prefix="/jobs", tags=["jobs"])
logger = logging.getLogger("portal_backend.jobs")

REJECTION_REASON_LABELS = {
    "quality_below_standard": "Content quality below publishing standard",
    "policy_or_compliance_issue": "Policy or compliance issue",
    "seo_or_link_issue": "SEO or link placement issue",
    "format_or_structure_issue": "Formatting or structure issue",
    "other": "Other",
}


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _job_to_out(job: Job) -> JobOut:
    return JobOut(
        id=job.id,
        submission_id=job.submission_id,
        client_id=job.client_id,
        site_id=job.site_id,
        job_status=job.job_status,
        requires_admin_approval=bool(job.requires_admin_approval),
        approved_by=job.approved_by,
        approved_by_name_snapshot=job.approved_by_name_snapshot,
        approved_at=job.approved_at,
        attempt_count=job.attempt_count,
        last_error=job.last_error,
        wp_post_id=job.wp_post_id,
        wp_post_url=job.wp_post_url,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def _get_latest_creator_output_payload(db: Session, job_id: UUID) -> Dict[str, Any]:
    row = (
        db.query(CreatorOutput.payload, CreatorOutput.planner_trace, CreatorOutput.writer_prompt_trace)
        .filter(CreatorOutput.job_id == job_id)
        .order_by(CreatorOutput.created_at.desc())
        .first()
    )
    if not row:
        return {}
    payload = row[0] if isinstance(row[0], dict) else {}
    planner_trace = row[1] if isinstance(row[1], dict) else {}
    writer_prompt_trace = row[2] if isinstance(row[2], list) else []
    debug = payload.get("debug") if isinstance(payload.get("debug"), dict) else {}
    prompt_trace = debug.get("prompt_trace") if isinstance(debug.get("prompt_trace"), dict) else {}
    if planner_trace:
        prompt_trace["planner"] = planner_trace
    if writer_prompt_trace:
        prompt_trace["writer_attempts"] = writer_prompt_trace
    if prompt_trace:
        debug["prompt_trace"] = prompt_trace
        payload["debug"] = debug
    return payload


def _build_creator_debug_payload(job: Job, creator_output: Dict[str, Any]) -> Dict[str, Any]:
    debug = creator_output.get("debug") if isinstance(creator_output.get("debug"), dict) else {}
    phase3 = creator_output.get("phase3") if isinstance(creator_output.get("phase3"), dict) else {}
    phase4 = creator_output.get("phase4") if isinstance(creator_output.get("phase4"), dict) else {}
    prompt_trace = debug.get("prompt_trace") if isinstance(debug.get("prompt_trace"), dict) else {}
    planner_trace = prompt_trace.get("planner") if isinstance(prompt_trace.get("planner"), dict) else {}
    writer_attempts = prompt_trace.get("writer_attempts") if isinstance(prompt_trace.get("writer_attempts"), list) else []
    planning_quality = debug.get("planning_quality") if isinstance(debug.get("planning_quality"), dict) else {}
    keyword_selection = debug.get("keyword_selection") if isinstance(debug.get("keyword_selection"), dict) else {}
    internal_linking = debug.get("internal_linking") if isinstance(debug.get("internal_linking"), dict) else {}
    quality_scores = debug.get("quality_scores") if isinstance(debug.get("quality_scores"), dict) else {}

    if not planner_trace:
        planner_trace = {
            "mode": "deterministic",
            "attempts": [
                {
                    "attempt": 1,
                    "input_packet": {
                        "topic": str(phase3.get("final_article_topic") or "").strip(),
                        "primary_keyword": str(phase3.get("primary_keyword") or "").strip(),
                        "secondary_keywords": phase3.get("secondary_keywords") or [],
                        "intent_type": str(phase3.get("search_intent_type") or keyword_selection.get("intent_type") or "").strip(),
                        "article_angle": str(phase3.get("article_angle") or keyword_selection.get("article_angle") or "").strip(),
                        "topic_class": str(phase3.get("topic_class") or keyword_selection.get("topic_class") or "").strip(),
                        "style_profile": phase3.get("style_profile") or keyword_selection.get("style_profile") or {},
                        "specificity_profile": phase3.get("specificity_profile") or keyword_selection.get("specificity_profile") or {},
                        "title_package": phase3.get("title_package") or keyword_selection.get("title_package") or {},
                        "content_brief": phase3.get("content_brief") or keyword_selection.get("content_brief") or {},
                        "faq_candidates": phase3.get("faq_candidates") or keyword_selection.get("faq_candidates") or [],
                        "internal_link_candidates": internal_linking.get("candidates") or [],
                    },
                    "plan": phase4,
                    "planning_quality": planning_quality,
                }
            ],
        }

    return {
        "job_id": job.id,
        "job_status": job.job_status,
        "planner": planner_trace,
        "writer_attempts": writer_attempts,
        "quality_scores": quality_scores,
        "planning_quality": planning_quality,
        "internal_linking": internal_linking,
        "writer_prompt_recorded": bool(writer_attempts),
    }


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


def _pending_job_to_out(
    job: Job,
    submission: Submission,
    client: Client,
    site: Site,
    *,
    content_title: Optional[str] = None,
    target_site_url: Optional[str] = None,
) -> PendingJobOut:
    return PendingJobOut(
        job_id=job.id,
        submission_id=submission.id,
        request_kind=submission.request_kind,
        client_id=client.id,
        client_name=(client.name or "").strip(),
        site_id=site.id,
        site_name=(site.name or "").strip(),
        site_url=(site.site_url or "").strip(),
        target_site_url=(target_site_url or "").strip() or None,
        content_title=(content_title or "").strip() or None,
        job_status=job.job_status,
        wp_post_id=job.wp_post_id,
        wp_post_url=job.wp_post_url,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def _rejected_article_to_out(
    job: Job,
    submission: Submission,
    client: Client,
    site: Site,
    *,
    content_title: Optional[str] = None,
    target_site_url: Optional[str] = None,
    rejection_reason: Optional[str] = None,
    rejected_by: Optional[str] = None,
    rejected_at: Optional[datetime] = None,
) -> RejectedArticleOut:
    return RejectedArticleOut(
        job_id=job.id,
        submission_id=submission.id,
        request_kind=submission.request_kind,
        client_id=client.id,
        client_name=(client.name or "").strip(),
        site_id=site.id,
        site_name=(site.name or "").strip(),
        site_url=(site.site_url or "").strip(),
        target_site_url=(target_site_url or "").strip() or None,
        content_title=(content_title or "").strip() or None,
        rejection_reason=(rejection_reason or "").strip() or None,
        rejected_by=(rejected_by or "").strip() or None,
        rejected_at=rejected_at,
        status=job.job_status,
        wp_post_url=(job.wp_post_url or "").strip() or None,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def _extract_note_map(notes: Optional[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    if not notes:
        return out
    for part in notes.split(";"):
        item = part.strip()
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        out[key.strip().lower()] = value.strip()
    return out


def _get_content_titles_by_job_id(db: Session, job_ids: List[UUID]) -> dict[UUID, str]:
    if not job_ids:
        return {}
    event_rows = (
        db.query(JobEvent.job_id, JobEvent.payload)
        .filter(
            JobEvent.job_id.in_(job_ids),
            JobEvent.event_type == "converter_ok",
        )
        .order_by(JobEvent.created_at.desc())
        .all()
    )

    title_map: dict[UUID, str] = {}
    for job_id_value, payload in event_rows:
        if job_id_value in title_map or not isinstance(payload, dict):
            continue
        raw_title = payload.get("title")
        if isinstance(raw_title, str) and raw_title.strip():
            title_map[job_id_value] = raw_title.strip()
    return title_map


def _extract_rejection_event_metadata(payload: Any, *, fallback_created_at: Optional[datetime] = None) -> Optional[dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    if str(payload.get("action") or "").strip().lower() != "admin_reject":
        return None

    rejected_at = fallback_created_at
    raw_rejected_at = payload.get("rejected_at")
    if isinstance(raw_rejected_at, str) and raw_rejected_at.strip():
        try:
            rejected_at = datetime.fromisoformat(raw_rejected_at.strip().replace("Z", "+00:00"))
        except ValueError:
            rejected_at = fallback_created_at

    rejected_by = payload.get("rejected_by_email")
    rejection_reason = payload.get("reason_summary")
    return {
        "rejected_at": rejected_at,
        "rejected_by": rejected_by.strip() if isinstance(rejected_by, str) and rejected_by.strip() else None,
        "rejection_reason": rejection_reason.strip() if isinstance(rejection_reason, str) and rejection_reason.strip() else None,
    }


def _get_rejection_metadata_by_job_id(db: Session, job_ids: List[UUID]) -> dict[UUID, dict[str, Any]]:
    if not job_ids:
        return {}
    event_rows = (
        db.query(JobEvent.job_id, JobEvent.payload, JobEvent.created_at)
        .filter(
            JobEvent.job_id.in_(job_ids),
            JobEvent.event_type == "failed",
        )
        .order_by(JobEvent.created_at.desc())
        .all()
    )

    metadata_by_job: dict[UUID, dict[str, Any]] = {}
    for job_id_value, payload, created_at in event_rows:
        if job_id_value in metadata_by_job:
            continue
        metadata = _extract_rejection_event_metadata(payload, fallback_created_at=created_at)
        if metadata:
            metadata_by_job[job_id_value] = metadata
    return metadata_by_job


def _get_enabled_credential_for_site(db: Session, site_id: UUID) -> SiteCredential:
    credential = (
        db.query(SiteCredential)
        .filter(
            SiteCredential.site_id == site_id,
            SiteCredential.enabled.is_(True),
        )
        .order_by(SiteCredential.created_at.desc())
        .first()
    )
    if not credential:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No enabled site credential found for job site.")
    return credential


def _sanitize_html_for_preview(value: str) -> str:
    if not value:
        return ""
    without_scripts = re.sub(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", "", value, flags=re.IGNORECASE)
    return re.sub(r"\son\w+\s*=\s*(['\"]).*?\1", "", without_scripts, flags=re.IGNORECASE)


def _pick_featured_image_url(post_payload: dict) -> str:
    for key in ("jetpack_featured_media_url", "featured_media_url"):
        value = post_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    embedded = post_payload.get("_embedded")
    if isinstance(embedded, dict):
        featured = embedded.get("wp:featuredmedia")
        if isinstance(featured, list):
            for item in featured:
                if not isinstance(item, dict):
                    continue
                source_url = item.get("source_url")
                if isinstance(source_url, str) and source_url.strip():
                    return source_url.strip()
                guid = item.get("guid")
                if isinstance(guid, dict):
                    rendered = guid.get("rendered")
                    if isinstance(rendered, str) and rendered.strip():
                        return rendered.strip()
    return ""


def _extract_post_title(post_payload: dict, fallback: str = "") -> str:
    title_value = post_payload.get("title")
    if isinstance(title_value, dict):
        rendered = title_value.get("rendered")
        if isinstance(rendered, str) and rendered.strip():
            return rendered.strip()
    if isinstance(title_value, str) and title_value.strip():
        return title_value.strip()
    return fallback.strip()


def _resolve_job_image_prompt(db: Session, job_id: UUID, *, fallback_title: str = "") -> str:
    row = (
        db.query(JobEvent.payload)
        .filter(
            JobEvent.job_id == job_id,
            JobEvent.event_type == "image_prompt_ok",
        )
        .order_by(JobEvent.created_at.desc())
        .first()
    )
    payload = row[0] if row else None
    if isinstance(payload, dict):
        raw_prompt = payload.get("image_prompt")
        if isinstance(raw_prompt, str) and raw_prompt.strip():
            return raw_prompt.strip()

    fallback_title = fallback_title.strip()
    if fallback_title:
        return f"Featured image for article titled: {fallback_title}"

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="No image prompt found for this job. Re-run automation for this submission first.",
    )


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


@router.get("/published", response_model=PublishedArticlesPageOut)
def list_published_articles(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    q: Optional[str] = Query(default=None),
    client_id: Optional[UUID] = Query(default=None),
    site_id: Optional[UUID] = Query(default=None),
    sort: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> PublishedArticlesPageOut:
    query = (
        db.query(Job, Client, Site)
        .join(Client, Client.id == Job.client_id)
        .join(Site, Site.id == Job.site_id)
        .filter(
            Job.job_status == "succeeded",
            Job.wp_post_url.isnot(None),
            Job.wp_post_url != "",
        )
    )
    if client_id is not None:
        query = query.filter(Job.client_id == client_id)
    if site_id is not None:
        query = query.filter(Job.site_id == site_id)
    if q:
        cleaned = q.strip().lower()
        if cleaned:
            like = f"%{cleaned}%"
            query = query.filter(
                or_(
                    func.lower(Job.wp_post_url).like(like),
                    func.lower(func.coalesce(Job.approved_by_name_snapshot, "")).like(like),
                    func.lower(func.coalesce(Client.name, "")).like(like),
                    func.lower(func.coalesce(Site.name, "")).like(like),
                    func.lower(func.coalesce(Site.site_url, "")).like(like),
                )
            )

    total = query.count()
    normalized_sort = (sort or "").strip().lower()
    if normalized_sort and normalized_sort not in {"published_at", "url"}:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="sort must be published_at or url.")

    if normalized_sort == "url":
        ordering = (func.lower(Job.wp_post_url).asc(), Job.created_at.desc())
    else:
        ordering = (Job.approved_at.desc().nullslast(), Job.created_at.desc())

    rows = query.order_by(*ordering).offset(offset).limit(limit).all()
    if not rows:
        return PublishedArticlesPageOut(items=[], total=total, limit=limit, offset=offset)

    job_ids = [job.id for job, _, _ in rows]
    creator_output_rows = (
        db.query(CreatorOutput.job_id, CreatorOutput.payload)
        .filter(CreatorOutput.job_id.in_(job_ids))
        .order_by(CreatorOutput.created_at.desc())
        .all()
    )
    seo_score_by_job: dict[UUID, int] = {}
    seo_evaluation_by_job: dict[UUID, dict] = {}
    for creator_job_id, payload in creator_output_rows:
        if creator_job_id in seo_evaluation_by_job or not isinstance(payload, dict):
            continue
        seo_evaluation = payload.get("seo_evaluation") or (payload.get("debug") or {}).get("seo_evaluation") or {}
        if not isinstance(seo_evaluation, dict):
            continue
        seo_evaluation_by_job[creator_job_id] = seo_evaluation
        score = seo_evaluation.get("score")
        if isinstance(score, (int, float)):
            seo_score_by_job[creator_job_id] = int(score)

    event_rows = (
        db.query(JobEvent.job_id, JobEvent.created_at)
        .filter(
            JobEvent.job_id.in_(job_ids),
            JobEvent.event_type.in_(("wp_post_updated", "wp_post_created")),
        )
        .order_by(JobEvent.created_at.desc())
        .all()
    )

    published_at_map: dict[UUID, datetime] = {}
    for job_id_value, created_at in event_rows:
        if job_id_value in published_at_map:
            continue
        published_at_map[job_id_value] = created_at

    out: List[PublishedArticleOut] = []
    for job, client, site in rows:
        url = (job.wp_post_url or "").strip()
        if not url:
            continue
        published_by = (job.approved_by_name_snapshot or "").strip() or None
        published_at = job.approved_at or published_at_map.get(job.id)
        out.append(
            PublishedArticleOut(
                job_id=job.id,
                wp_post_url=url,
                published_by=published_by,
                published_at=published_at,
                seo_score=seo_score_by_job.get(job.id),
                seo_evaluation=seo_evaluation_by_job.get(job.id),
                status=job.job_status,
                client_id=client.id,
                client_name=(client.name or "").strip(),
                site_id=site.id,
                site_name=(site.name or "").strip(),
                site_url=(site.site_url or "").strip(),
            )
        )
    return PublishedArticlesPageOut(items=out, total=total, limit=limit, offset=offset)


@router.get("/rejected", response_model=RejectedArticlesPageOut)
def list_rejected_articles(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    q: Optional[str] = Query(default=None),
    client_id: Optional[UUID] = Query(default=None),
    site_id: Optional[UUID] = Query(default=None),
    sort: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> RejectedArticlesPageOut:
    query = (
        db.query(Job, Submission, Client, Site)
        .join(Submission, Submission.id == Job.submission_id)
        .join(Client, Client.id == Job.client_id)
        .join(Site, Site.id == Job.site_id)
        .filter(Job.job_status == "rejected")
    )
    if client_id is not None:
        query = query.filter(Job.client_id == client_id)
    if site_id is not None:
        query = query.filter(Job.site_id == site_id)
    if q:
        cleaned = q.strip().lower()
        if cleaned:
            like = f"%{cleaned}%"
            query = query.filter(
                or_(
                    func.lower(func.coalesce(Submission.title, "")).like(like),
                    func.lower(func.coalesce(Submission.rejection_reason, "")).like(like),
                    func.lower(func.coalesce(Job.last_error, "")).like(like),
                    func.lower(func.coalesce(Client.name, "")).like(like),
                    func.lower(func.coalesce(Site.name, "")).like(like),
                    func.lower(func.coalesce(Site.site_url, "")).like(like),
                )
            )

    total = query.count()
    normalized_sort = (sort or "").strip().lower()
    if normalized_sort and normalized_sort not in {"rejected_at", "title"}:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="sort must be rejected_at or title.")

    if normalized_sort == "title":
        ordering = (func.lower(func.coalesce(Submission.title, "")).asc(), Job.updated_at.desc())
    else:
        ordering = (Job.updated_at.desc(), Job.created_at.desc())

    rows = query.order_by(*ordering).offset(offset).limit(limit).all()
    if not rows:
        return RejectedArticlesPageOut(items=[], total=total, limit=limit, offset=offset)

    job_ids = [job.id for job, _, _, _ in rows]
    title_map = _get_content_titles_by_job_id(db, job_ids)
    rejection_metadata_by_job = _get_rejection_metadata_by_job_id(db, job_ids)

    out: List[RejectedArticleOut] = []
    for job, submission, client, site in rows:
        title_value = title_map.get(job.id)
        if not title_value and isinstance(submission.title, str):
            title_value = submission.title.strip() or None
        note_map = _extract_note_map(submission.notes)
        rejection_metadata = rejection_metadata_by_job.get(job.id) or {}
        rejection_reason = (
            rejection_metadata.get("rejection_reason")
            or ((submission.rejection_reason or "").strip() or None)
            or ((job.last_error or "").strip() or None)
        )
        rejected_by = rejection_metadata.get("rejected_by")
        rejected_at = rejection_metadata.get("rejected_at") or job.updated_at
        out.append(
            _rejected_article_to_out(
                job,
                submission,
                client,
                site,
                content_title=title_value,
                target_site_url=note_map.get("client_target_site_url"),
                rejection_reason=rejection_reason,
                rejected_by=rejected_by,
                rejected_at=rejected_at,
            )
        )
    return RejectedArticlesPageOut(items=out, total=total, limit=limit, offset=offset)


@router.get("/pending", response_model=List[PendingJobOut])
def list_pending_jobs(
    request_kind: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> List[PendingJobOut]:
    kind_filter: Optional[str] = None
    if request_kind is not None:
        cleaned = request_kind.strip().lower()
        if cleaned not in {"submit_article", "create_article"}:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="request_kind must be submit_article or create_article.")
        kind_filter = cleaned

    query = (
        db.query(Job, Submission, Client, Site)
        .join(Submission, Submission.id == Job.submission_id)
        .join(Client, Client.id == Job.client_id)
        .join(Site, Site.id == Job.site_id)
        .filter(
            Job.requires_admin_approval.is_(True),
            Job.job_status == "pending_approval",
            Job.wp_post_id.isnot(None),
        )
    )
    if kind_filter:
        query = query.filter(Submission.request_kind == kind_filter)

    rows = query.order_by(Job.updated_at.desc(), Job.created_at.desc()).all()
    if not rows:
        return []

    job_ids = [job.id for job, _, _, _ in rows]
    title_map = _get_content_titles_by_job_id(db, job_ids)

    out: List[PendingJobOut] = []
    for job, submission, client, site in rows:
        title_value = title_map.get(job.id)
        if not title_value and isinstance(submission.title, str):
            title_value = submission.title.strip() or None
        note_map = _extract_note_map(submission.notes)
        out.append(
            _pending_job_to_out(
                job,
                submission,
                client,
                site,
                content_title=title_value,
                target_site_url=note_map.get("client_target_site_url"),
            )
        )
    return out


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
        requires_admin_approval=payload.requires_admin_approval,
        approved_by=payload.approved_by,
        approved_by_name_snapshot=payload.approved_by_name_snapshot,
        approved_at=payload.approved_at,
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


@router.get("/{job_id}/creator-debug")
def get_job_creator_debug(
    job_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Dict[str, Any]:
    job = _get_job_or_404(db, job_id)
    if current_user.role != "admin":
        ensure_client_access(db, current_user, job.client_id)
        ensure_site_access(db, current_user, job.site_id)

    creator_output = _get_latest_creator_output_payload(db, job.id)
    if not creator_output:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No creator output found for this job.")

    return _build_creator_debug_payload(job, creator_output)


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
    if payload.requires_admin_approval is not None:
        job.requires_admin_approval = payload.requires_admin_approval
    if payload.approved_by is not None:
        job.approved_by = payload.approved_by
    if payload.approved_by_name_snapshot is not None:
        job.approved_by_name_snapshot = payload.approved_by_name_snapshot
    if payload.approved_at is not None:
        job.approved_at = payload.approved_at
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


@router.post("/{job_id}/publish", response_model=PendingJobPublishOut)
def publish_pending_job(
    job_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
) -> PendingJobPublishOut:
    job = _get_job_or_404(db, job_id)
    if not bool(job.requires_admin_approval):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="This job does not require admin approval.")
    if job.job_status != "pending_approval":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Job is not pending admin approval.")
    if job.wp_post_id is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Job has no WordPress draft post to publish.")

    site = db.query(Site).filter(Site.id == job.site_id).first()
    if not site:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Publishing site not found for job.")
    credential = _get_enabled_credential_for_site(db, site.id)

    try:
        config = get_runtime_config()
    except AutomationError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    try:
        post_payload = wp_publish_post(
            site_url=site.site_url,
            wp_rest_base=site.wp_rest_base,
            wp_username=credential.wp_username,
            wp_app_password=credential.wp_app_password,
            post_id=int(job.wp_post_id),
            timeout_seconds=config["timeout_seconds"],
        )
    except AutomationError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    now = datetime.now(timezone.utc)
    wp_post_url = post_payload.get("link")
    if isinstance(wp_post_url, str) and wp_post_url.strip():
        job.wp_post_url = wp_post_url.strip()
    article = upsert_publishing_site_article(
        db,
        site_id=site.id,
        post_payload=post_payload,
        source="job",
        synced_at=now,
    )
    if article is not None:
        db.add(article)
    submission = db.query(Submission).filter(Submission.id == job.submission_id).first()
    if submission:
        submission.post_status = "publish"
        submission.updated_at = now
        db.add(submission)
    job.job_status = "succeeded"
    job.approved_by = current_user.id
    job.approved_by_name_snapshot = (current_user.full_name or "").strip() or current_user.email
    job.approved_at = now
    job.updated_at = now
    db.add(job)
    db.add(
        JobEvent(
            job_id=job.id,
            event_type="wp_post_updated",
            payload={
                "action": "admin_publish",
                "wp_post_id": int(job.wp_post_id),
                "wp_post_url": job.wp_post_url,
                "approved_by": str(current_user.id),
                "approved_by_name": job.approved_by_name_snapshot,
                "approved_at": now.isoformat(),
            },
        )
    )
    db.commit()
    db.refresh(job)
    try:
        send_client_publish_notification(db, job_id=job.id, post_payload=post_payload)
    except Exception:
        logger.exception("jobs.publish_notification_failed job_id=%s", job.id)
    return PendingJobPublishOut(job=_job_to_out(job))


@router.post("/{job_id}/cancel", response_model=JobOut)
def cancel_job(
    job_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> JobOut:
    job = _get_job_or_404(db, job_id)
    if current_user.role != "admin":
        ensure_client_access(db, current_user, job.client_id)
        ensure_site_access(db, current_user, job.site_id)

    submission = db.query(Submission).filter(Submission.id == job.submission_id).first()
    if not submission:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found for job.")
    if submission.request_kind != "create_article":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Only created-article jobs can be canceled.")

    if job.job_status not in {"queued", "processing", "retrying"}:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Job cannot be canceled in its current state.")
    if job.wp_post_id is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Job already created a WordPress draft.")

    if current_user.role != "admin":
        max_cancels = _read_int_env("AUTOMATION_CLIENT_CANCELS_PER_DAY", 2)
        now = datetime.now(timezone.utc)
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        cancels_today = (
            db.query(JobEvent.id)
            .join(Job, Job.id == JobEvent.job_id)
            .filter(
                Job.client_id == job.client_id,
                JobEvent.event_type == "canceled",
                JobEvent.created_at >= start_of_day,
            )
            .count()
        )
        if cancels_today >= max_cancels:
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Daily cancel limit reached.")

    now = datetime.now(timezone.utc)
    job.job_status = "canceled"
    job.last_error = "canceled_by_client"
    job.updated_at = now
    db.add(job)
    db.add(
        JobEvent(
            job_id=job.id,
            event_type="canceled",
            payload={
                "by_user_id": str(current_user.id),
                "by_role": current_user.role,
                "reason": "client_requested",
            },
        )
    )
    db.commit()
    db.refresh(job)
    return _job_to_out(job)


@router.post("/{job_id}/regenerate-image", response_model=PendingJobRegenerateImageOut)
def regenerate_pending_job_image(
    job_id: UUID,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
) -> PendingJobRegenerateImageOut:
    job = _get_job_or_404(db, job_id)
    if not bool(job.requires_admin_approval):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="This job does not require admin approval.")
    if job.job_status != "pending_approval":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Job is not pending admin approval.")
    if job.wp_post_id is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Job has no WordPress draft post.")

    site = db.query(Site).filter(Site.id == job.site_id).first()
    if not site:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Publishing site not found for job.")
    credential = _get_enabled_credential_for_site(db, site.id)

    try:
        config = get_runtime_config()
    except AutomationError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    if not config.get("leonardo_api_key"):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="LEONARDO_API_KEY is not configured.")

    try:
        post_payload = wp_get_post(
            site_url=site.site_url,
            wp_rest_base=site.wp_rest_base,
            wp_username=credential.wp_username,
            wp_app_password=credential.wp_app_password,
            post_id=int(job.wp_post_id),
            timeout_seconds=config["timeout_seconds"],
        )
    except AutomationError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    post_title = _extract_post_title(post_payload, fallback=f"Draft #{int(job.wp_post_id)}")
    image_prompt = _resolve_job_image_prompt(db, job.id, fallback_title=post_title)
    previous_featured_media_id = post_payload.get("featured_media")
    if not isinstance(previous_featured_media_id, int):
        previous_featured_media_id = None

    sizes_to_try = [
        (max(256, int(config["image_width"])), max(256, int(config["image_height"]))),
        (768, 432),
        (640, 360),
        (512, 288),
    ]
    unique_sizes: list[tuple[int, int]] = []
    for size in sizes_to_try:
        if size not in unique_sizes:
            unique_sizes.append(size)

    image_url: str = ""
    media_payload: dict = {}
    last_upload_error: Optional[AutomationError] = None

    for idx, (width, height) in enumerate(unique_sizes):
        try:
            image_url = generate_image_via_leonardo(
                prompt=image_prompt,
                api_key=config["leonardo_api_key"],
                timeout_seconds=config["timeout_seconds"],
                poll_timeout_seconds=config["poll_timeout_seconds"],
                poll_interval_seconds=config["poll_interval_seconds"],
                model_id=config["leonardo_model_id"],
                width=width,
                height=height,
                base_url=config["leonardo_base_url"],
            )
            image_bytes, file_name, content_type = download_binary_file(
                image_url,
                timeout_seconds=config["timeout_seconds"],
            )
            media_payload = wp_create_media_item(
                site_url=site.site_url,
                wp_rest_base=site.wp_rest_base,
                wp_username=credential.wp_username,
                wp_app_password=credential.wp_app_password,
                data=image_bytes,
                file_name=file_name,
                content_type=content_type,
                title=post_title,
                timeout_seconds=config["timeout_seconds"],
            )
            break
        except AutomationError as exc:
            if "HTTP 413" in str(exc) and idx < len(unique_sizes) - 1:
                last_upload_error = exc
                continue
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    else:
        if last_upload_error is not None:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(last_upload_error)) from last_upload_error
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Failed to upload regenerated image.")

    new_featured_media_id = media_payload.get("id")
    if not isinstance(new_featured_media_id, int):
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="WordPress media upload did not return media ID.")

    try:
        updated_post_payload = wp_update_post_featured_media(
            site_url=site.site_url,
            wp_rest_base=site.wp_rest_base,
            wp_username=credential.wp_username,
            wp_app_password=credential.wp_app_password,
            post_id=int(job.wp_post_id),
            featured_media_id=new_featured_media_id,
            timeout_seconds=config["timeout_seconds"],
        )
    except AutomationError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    guid_value = media_payload.get("guid")
    media_url = media_payload.get("source_url")
    if not media_url and isinstance(guid_value, dict):
        rendered = guid_value.get("rendered")
        if isinstance(rendered, str):
            media_url = rendered
    wp_post_url = updated_post_payload.get("link")

    now = datetime.now(timezone.utc)
    if isinstance(wp_post_url, str) and wp_post_url.strip():
        job.wp_post_url = wp_post_url.strip()
    job.updated_at = now
    db.add(job)

    db.add(
        JobEvent(
            job_id=job.id,
            event_type="image_generated",
            payload={
                "action": "admin_regenerate_image",
                "source_url": image_url,
                "prompt": image_prompt,
                "previous_featured_media_id": previous_featured_media_id,
                "new_featured_media_id": new_featured_media_id,
                "regenerated_by": str(current_user.id),
                "regenerated_by_email": current_user.email,
                "regenerated_at": now.isoformat(),
            },
        )
    )
    db.add(
        Asset(
            job_id=job.id,
            asset_type="featured_image",
            provider="leonardo",
            source_url=image_url,
            storage_url=media_url if isinstance(media_url, str) else None,
            meta={
                "model_id": config["leonardo_model_id"],
                "action": "admin_regenerate_image",
                "regenerated_by": str(current_user.id),
            },
        )
    )
    db.add(
        JobEvent(
            job_id=job.id,
            event_type="wp_post_updated",
            payload={
                "action": "admin_regenerate_image",
                "wp_post_id": int(job.wp_post_id),
                "wp_post_url": job.wp_post_url,
                "previous_featured_media_id": previous_featured_media_id,
                "new_featured_media_id": new_featured_media_id,
                "regenerated_by": str(current_user.id),
                "regenerated_by_email": current_user.email,
                "regenerated_at": now.isoformat(),
            },
        )
    )

    db.commit()
    db.refresh(job)
    return PendingJobRegenerateImageOut(
        job=_job_to_out(job),
        wp_media_id=new_featured_media_id,
        wp_media_url=media_url if isinstance(media_url, str) else None,
    )


@router.get("/{job_id}/draft-preview", response_class=HTMLResponse)
def preview_pending_job_draft(
    job_id: UUID,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> HTMLResponse:
    job = _get_job_or_404(db, job_id)
    if job.wp_post_id is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Job has no WordPress draft post.")

    site = db.query(Site).filter(Site.id == job.site_id).first()
    if not site:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Publishing site not found for job.")
    credential = _get_enabled_credential_for_site(db, site.id)

    try:
        config = get_runtime_config()
    except AutomationError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    try:
        post_payload = wp_get_post(
            site_url=site.site_url,
            wp_rest_base=site.wp_rest_base,
            wp_username=credential.wp_username,
            wp_app_password=credential.wp_app_password,
            post_id=int(job.wp_post_id),
            timeout_seconds=config["timeout_seconds"],
        )
    except AutomationError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    title = ""
    if isinstance(post_payload.get("title"), dict):
        title = str(post_payload["title"].get("rendered") or "").strip()
    title = title or f"Draft #{job.wp_post_id}"

    content_html = ""
    if isinstance(post_payload.get("content"), dict):
        content_html = str(post_payload["content"].get("rendered") or "")
    content_html = _sanitize_html_for_preview(content_html)

    excerpt_html = ""
    if isinstance(post_payload.get("excerpt"), dict):
        excerpt_html = str(post_payload["excerpt"].get("rendered") or "")
    excerpt_html = _sanitize_html_for_preview(excerpt_html)

    featured_image_url = _pick_featured_image_url(post_payload)
    if not featured_image_url:
        featured_media_id = post_payload.get("featured_media")
        if isinstance(featured_media_id, int) and featured_media_id > 0:
            try:
                media_payload = wp_get_media(
                    site_url=site.site_url,
                    wp_rest_base=site.wp_rest_base,
                    wp_username=credential.wp_username,
                    wp_app_password=credential.wp_app_password,
                    media_id=featured_media_id,
                    timeout_seconds=config["timeout_seconds"],
                )
                maybe_url = media_payload.get("source_url")
                if isinstance(maybe_url, str) and maybe_url.strip():
                    featured_image_url = maybe_url.strip()
            except AutomationError:
                featured_image_url = ""

    status_value = str(post_payload.get("status") or "unknown")
    slug_value = str(post_payload.get("slug") or "")
    site_url = (site.site_url or "").strip()

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(title)} - Draft Preview</title>
  <style>
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f5f7fb;
      color: #0f172a;
      line-height: 1.6;
    }}
    .wrap {{
      max-width: 920px;
      margin: 24px auto;
      background: #fff;
      border: 1px solid #dbe2ef;
      border-radius: 12px;
      padding: 24px;
    }}
    .meta {{
      display: grid;
      gap: 4px;
      margin-bottom: 16px;
      color: #475569;
      font-size: 14px;
    }}
    .meta code {{
      background: #eef2ff;
      border: 1px solid #dbe2ef;
      border-radius: 6px;
      padding: 1px 6px;
    }}
    .actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      margin: 0 0 18px;
    }}
    .copy-btn {{
      appearance: none;
      border: 1px solid #2563eb;
      background: #2563eb;
      color: #fff;
      border-radius: 8px;
      padding: 10px 14px;
      font-size: 14px;
      font-weight: 600;
      cursor: pointer;
    }}
    .copy-btn.secondary {{
      background: #fff;
      color: #2563eb;
    }}
    .copy-btn:hover {{
      opacity: 0.94;
    }}
    .copy-status {{
      color: #475569;
      font-size: 14px;
      min-height: 20px;
    }}
    h1 {{
      margin-top: 0;
      margin-bottom: 12px;
      font-size: 32px;
      line-height: 1.15;
    }}
    .excerpt {{
      margin-bottom: 18px;
      padding: 12px;
      border-left: 4px solid #93c5fd;
      background: #f8fbff;
    }}
    .featured-image {{
      margin: 0 0 18px;
    }}
    .featured-image img {{
      max-width: 100%;
      height: auto;
      display: block;
      border-radius: 10px;
      border: 1px solid #dbe2ef;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="meta">
      <div>Site: <code>{escape(site_url)}</code></div>
      <div>Post ID: <code>{int(job.wp_post_id)}</code></div>
      <div>Status: <code>{escape(status_value)}</code></div>
      <div>Slug: <code>{escape(slug_value)}</code></div>
    </div>
    <div class="actions">
      <button id="copy-rich-draft" class="copy-btn" type="button">Copy draft with links</button>
      <button id="copy-html-draft" class="copy-btn secondary" type="button">Copy HTML</button>
      <span id="copy-status" class="copy-status" role="status" aria-live="polite"></span>
    </div>
    <h1>{escape(title)}</h1>
    {"<div class='featured-image'><img src='" + escape(featured_image_url) + "' alt='Featured image' /></div>" if featured_image_url else ""}
    {"<div class='excerpt'>" + excerpt_html + "</div>" if excerpt_html else ""}
    <article id="draft-article">{content_html}</article>
  </div>
  <script>
    (function () {{
      const copyRichButton = document.getElementById("copy-rich-draft");
      const copyHtmlButton = document.getElementById("copy-html-draft");
      const copyStatus = document.getElementById("copy-status");
      const article = document.getElementById("draft-article");
      if (!article) return;

      const getDraftHtml = () => {{
        return article.innerHTML;
      }};

      const getDraftPlainText = () => {{
        return (article.textContent || "").trim();
      }};

      const setCopyStatus = (message, isError = false) => {{
        if (!copyStatus) return;
        copyStatus.textContent = message;
        copyStatus.style.color = isError ? "#b91c1c" : "#475569";
      }};

      const fallbackCopyRichText = (html, plainText) => {{
        const container = document.createElement("div");
        container.setAttribute("contenteditable", "true");
        container.style.position = "fixed";
        container.style.left = "-9999px";
        container.style.top = "0";
        container.innerHTML = html;
        document.body.appendChild(container);

        const selection = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(container);
        selection.removeAllRanges();
        selection.addRange(range);

        let copied = false;
        try {{
          copied = document.execCommand("copy");
        }} catch {{
          copied = false;
        }}

        selection.removeAllRanges();
        document.body.removeChild(container);

        if (copied) {{
          return true;
        }}

        if (navigator.clipboard && navigator.clipboard.writeText) {{
          return navigator.clipboard.writeText(plainText).then(() => false);
        }}
        return false;
      }};

      const copyDraftRichText = async () => {{
        const html = getDraftHtml();
        const plainText = getDraftPlainText();
        try {{
          if (navigator.clipboard && window.ClipboardItem && navigator.clipboard.write) {{
            await navigator.clipboard.write([
              new ClipboardItem({{
                "text/html": new Blob([html], {{ type: "text/html" }}),
                "text/plain": new Blob([plainText], {{ type: "text/plain" }}),
              }}),
            ]);
            setCopyStatus("Draft copied with links.");
            return;
          }}
          const fallbackResult = await fallbackCopyRichText(html, plainText);
          setCopyStatus(
            fallbackResult ? "Draft copied with links." : "Copied plain text only. Use Copy HTML to preserve exact links.",
            !fallbackResult
          );
        }} catch {{
          setCopyStatus("Copy failed. Use Copy HTML as a fallback.", true);
        }}
      }};

      const copyDraftHtml = async () => {{
        try {{
          const html = getDraftHtml();
          if (navigator.clipboard && navigator.clipboard.writeText) {{
            await navigator.clipboard.writeText(html);
            setCopyStatus("Draft HTML copied.");
            return;
          }}
          setCopyStatus("Clipboard API unavailable in this browser.", true);
        }} catch {{
          setCopyStatus("Copy failed. Select the preview and copy manually.", true);
        }}
      }};

      if (copyRichButton) {{
        copyRichButton.addEventListener("click", copyDraftRichText);
      }}
      if (copyHtmlButton) {{
        copyHtmlButton.addEventListener("click", copyDraftHtml);
      }}

      const slugify = (value) => (value || "")
        .toLowerCase()
        .replace(/<[^>]*>/g, "")
        .replace(/[^a-z0-9\\u00c0-\\u024f\\u1e00-\\u1eff]+/g, "-")
        .replace(/^-+|-+$/g, "");

      const headingBySlug = new Map();
      const headings = Array.from(article.querySelectorAll("h1, h2, h3, h4, h5, h6"));
      headings.forEach((heading, index) => {{
        const textSlug = slugify(heading.textContent || "");
        if (!heading.id) {{
          heading.id = textSlug ? `section-${{textSlug}}` : `section-${{index + 1}}`;
        }}
        if (textSlug && !headingBySlug.has(textSlug)) {{
          headingBySlug.set(textSlug, heading);
        }}
      }});

      const normalizeAnchor = (raw) => decodeURIComponent((raw || "").replace(/^#/, "").trim()).toLowerCase();

      const isMailOrPhone = (href) => href.startsWith("mailto:") || href.startsWith("tel:");

      const resolveHeadingTarget = (link) => {{
        const href = (link.getAttribute("href") || "").trim();
        if (!href || isMailOrPhone(href)) return null;

        let hashTarget = "";
        try {{
          const parsed = new URL(href, window.location.href);
          hashTarget = normalizeAnchor(parsed.hash);
        }} catch {{
          hashTarget = href.includes("#") ? normalizeAnchor(href.split("#").pop()) : "";
        }}

        const textTarget = slugify(link.textContent || "");
        let headingTarget = null;

        if (hashTarget) {{
          headingTarget =
            document.getElementById(hashTarget) ||
            document.getElementById(`section-${{hashTarget}}`) ||
            headingBySlug.get(hashTarget) ||
            null;
        }}

        if (!headingTarget && textTarget) {{
          headingTarget =
            headingBySlug.get(textTarget) ||
            document.getElementById(textTarget) ||
            document.getElementById(`section-${{textTarget}}`) ||
            null;
        }}

        return headingTarget;
      }};

      const scrollToHeading = (headingTarget) => {{
        headingTarget.scrollIntoView({{ behavior: "smooth", block: "start" }});
        window.history.replaceState(null, "", `#${{headingTarget.id}}`);
      }};

      const links = Array.from(article.querySelectorAll("a[href]"));
      links.forEach((link) => {{
        const headingTarget = resolveHeadingTarget(link);
        const href = (link.getAttribute("href") || "").trim();
        if (!headingTarget) {{
          if (href.includes("#")) {{
            link.removeAttribute("href");
            link.style.textDecoration = "none";
            link.style.cursor = "default";
          }}
          return;
        }}
        link.setAttribute("href", `#${{headingTarget.id}}`);
      }});

      article.addEventListener("click", (event) => {{
        const link = event.target instanceof Element ? event.target.closest("a[href]") : null;
        if (!link || !article.contains(link)) return;
        const headingTarget = resolveHeadingTarget(link);
        if (!headingTarget) return;
        event.preventDefault();
        scrollToHeading(headingTarget);
      }});
    }})();
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html, status_code=status.HTTP_200_OK)


@router.post("/{job_id}/reject", response_model=PendingJobRejectOut)
def reject_pending_job(
    job_id: UUID,
    payload: PendingJobRejectIn,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
) -> PendingJobRejectOut:
    job = _get_job_or_404(db, job_id)
    if not bool(job.requires_admin_approval):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="This job does not require admin approval.")
    if job.job_status != "pending_approval":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Job is not pending admin approval.")

    submission = db.query(Submission).filter(Submission.id == job.submission_id).first()
    if not submission:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission not found for job.")

    reason_code = payload.reason_code.strip().lower()
    reason_label = REJECTION_REASON_LABELS.get(reason_code, "Other")
    reason_text = payload.other_reason.strip() if payload.other_reason else ""
    reason_summary = reason_label if not reason_text else f"{reason_label}: {reason_text}"

    now = datetime.now(timezone.utc)
    submission.status = "rejected"
    submission.rejection_reason = reason_summary
    submission.updated_at = now
    db.add(submission)

    job.job_status = "rejected"
    job.last_error = f"Rejected by admin ({current_user.email}): {reason_summary}"
    job.updated_at = now
    db.add(job)

    db.add(
        JobEvent(
            job_id=job.id,
            event_type="failed",
            payload={
                "action": "admin_reject",
                "reason_code": reason_code,
                "reason_label": reason_label,
                "reason_text": reason_text or None,
                "reason_summary": reason_summary,
                "rejected_by": str(current_user.id),
                "rejected_by_email": current_user.email,
                "rejected_at": now.isoformat(),
            },
        )
    )

    db.commit()
    db.refresh(job)
    return PendingJobRejectOut(job=_job_to_out(job))


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
