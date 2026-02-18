from __future__ import annotations

import os
import logging
from typing import Dict, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlparse
from uuid import UUID

import requests
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import ValidationError
from sqlalchemy.orm import Session

from ..automation_service import (
    AutomationError,
    converter_target_from_site_url,
    get_runtime_config,
    resolve_source_url,
    run_guest_post_pipeline,
)
from ..db import get_db
from ..portal_models import (
    Client,
    ClientSiteAccess,
    Job,
    JobEvent,
    Site,
    SiteCategory,
    SiteCredential,
    SiteDefaultCategory,
    Submission,
)
from ..portal_schemas import (
    AutomationGuestPostIn,
    AutomationGuestPostOut,
    AutomationGuestPostResultOut,
    AutomationStatusEventOut,
    AutomationStatusOut,
)

router = APIRouter(prefix="/automation", tags=["automation"])
logger = logging.getLogger("portal_backend.automation")


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _normalized_host(value: str) -> Optional[str]:
    raw = (value or "").strip()
    if not raw:
        return None
    with_scheme = raw if "://" in raw else f"https://{raw}"
    host = (urlparse(with_scheme).hostname or "").strip().lower().rstrip(".")
    return host or None


def _host_variants(value: str) -> Set[str]:
    host = _normalized_host(value)
    if not host:
        return set()
    variants = {host}
    if host.startswith("www."):
        variants.add(host[4:])
    else:
        variants.add(f"www.{host}")
    return variants


def _resolve_site_by_target(db: Session, target_site: str) -> Site:
    try:
        site_uuid = UUID(target_site.strip())
        site = db.query(Site).filter(Site.id == site_uuid, Site.status == "active").first()
        if site:
            return site
    except ValueError:
        pass

    target_variants = _host_variants(target_site)
    if not target_variants:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="target_site is invalid.")

    sites = db.query(Site).filter(Site.status == "active").all()
    for site in sites:
        if _host_variants(site.site_url) & target_variants:
            return site

    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active site matches target_site.")


def _resolve_enabled_credential(db: Session, site_id: UUID) -> SiteCredential:
    credential = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site_id, SiteCredential.enabled.is_(True))
        .order_by(SiteCredential.created_at.desc())
        .first()
    )
    if not credential:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No enabled site credential found for target site.",
        )
    return credential


def _resolve_effective_author_id(
    *,
    payload_author: Optional[int],
    credential_author_id: Optional[int],
    fallback_author_id: int,
) -> int:
    if payload_author is not None:
        return payload_author
    if credential_author_id is not None and credential_author_id > 0:
        return credential_author_id
    return fallback_author_id


def _resolve_default_category_ids(db: Session, site_id: UUID) -> list[int]:
    rows = (
        db.query(SiteDefaultCategory)
        .filter(
            SiteDefaultCategory.site_id == site_id,
            SiteDefaultCategory.enabled.is_(True),
        )
        .order_by(
            SiteDefaultCategory.position.asc(),
            SiteDefaultCategory.created_at.asc(),
        )
        .all()
    )
    seen: set[int] = set()
    ordered_ids: list[int] = []
    for row in rows:
        raw = row.wp_category_id
        if raw is None:
            continue
        category_id = int(raw)
        if category_id <= 0 or category_id in seen:
            continue
        seen.add(category_id)
        ordered_ids.append(category_id)
    return ordered_ids


def _resolve_category_candidates(db: Session, site_id: UUID) -> list[Dict[str, object]]:
    rows = (
        db.query(SiteCategory)
        .filter(
            SiteCategory.site_id == site_id,
            SiteCategory.enabled.is_(True),
        )
        .order_by(
            SiteCategory.name.asc(),
            SiteCategory.wp_category_id.asc(),
        )
        .all()
    )
    out: list[Dict[str, object]] = []
    for row in rows:
        raw = row.wp_category_id
        if raw is None:
            continue
        category_id = int(raw)
        if category_id <= 0:
            continue
        out.append(
            {
                "id": category_id,
                "name": (row.name or "").strip(),
                "slug": (row.slug or "").strip(),
            }
        )
    return out


def _resolve_client(db: Session, payload: AutomationGuestPostIn) -> Client:
    client_id = payload.client_id
    client_name = (payload.client_name or "").strip()

    if client_id is not None:
        client = db.query(Client).filter(Client.id == client_id, Client.status == "active").first()
        if not client:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Client is not active.")
        return client

    if client_name:
        candidates = (
            db.query(Client)
            .filter(Client.status == "active")
            .order_by(Client.created_at.asc())
            .all()
        )
        matches = [candidate for candidate in candidates if (candidate.name or "").strip().lower() == client_name.lower()]
        if not matches:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"No active client matches client_name '{client_name}'.",
            )
        if len(matches) > 1:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Multiple active clients match client_name '{client_name}'. Provide client_id instead.",
            )
        return matches[0]

    if client_id is None:
        fallback = os.getenv("AUTOMATION_DEFAULT_CLIENT_ID", "").strip()
        if not fallback:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="client_id or client_name is required for async/shadow mode, or set AUTOMATION_DEFAULT_CLIENT_ID.",
            )
        try:
            client_id = UUID(fallback)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="AUTOMATION_DEFAULT_CLIENT_ID is invalid.",
            ) from exc

    client = db.query(Client).filter(Client.id == client_id, Client.status == "active").first()
    if not client:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Client is not active.")
    return client


def _require_client_site_access(db: Session, client_id: UUID, site_id: UUID) -> None:
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


def _resolve_submission_source(source_type: str, source_url: str) -> Tuple[str, Optional[str], Optional[str]]:
    if source_type == "google-doc":
        return "google-doc", source_url, None
    return "docx-upload", None, source_url


def _resolve_converter_target(target_site: str, site_url: str) -> str:
    target_host = _normalized_host(target_site)
    if target_host:
        return target_host
    return converter_target_from_site_url(site_url)


def _compose_submission_notes(idempotency_key: str, post_status: str, author_id: int) -> str:
    return f"idempotency_key={idempotency_key};post_status={post_status};author_id={author_id}"


def _extract_note_map(notes: Optional[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not notes:
        return out
    for part in notes.split(";"):
        item = part.strip()
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        out[key.strip().lower()] = value.strip()
    return out


def _build_idempotency_key(
    *,
    explicit_key: Optional[str],
    client_id: UUID,
    site_id: UUID,
    source_type: str,
    source_url: str,
) -> str:
    if explicit_key:
        candidate = explicit_key.strip()
    else:
        candidate = f"{client_id}:{site_id}:{source_type}:{source_url}"
    return candidate.replace(";", "_").replace("=", "_")[:200]


def _find_existing_submission(
    db: Session,
    *,
    client_id: UUID,
    site_id: UUID,
    source_type: str,
    doc_url: Optional[str],
    file_url: Optional[str],
    idempotency_key: str,
) -> Optional[Submission]:
    query = db.query(Submission).filter(
        Submission.client_id == client_id,
        Submission.site_id == site_id,
        Submission.source_type == source_type,
    )
    if doc_url is None:
        query = query.filter(Submission.doc_url.is_(None))
    else:
        query = query.filter(Submission.doc_url == doc_url)
    if file_url is None:
        query = query.filter(Submission.file_url.is_(None))
    else:
        query = query.filter(Submission.file_url == file_url)

    for submission in query.order_by(Submission.created_at.desc()).limit(20).all():
        note_map = _extract_note_map(submission.notes)
        if note_map.get("idempotency_key") == idempotency_key:
            return submission
    return None


def _dispatch_shadow_webhook(payload: AutomationGuestPostIn) -> bool:
    webhook_url = os.getenv("AUTOMATION_SHADOW_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return False
    body = payload.dict()
    try:
        response = requests.post(webhook_url, json=body, timeout=10)
    except requests.RequestException:
        logger.exception("automation.shadow.dispatch_failed")
        return False
    return response.status_code < 400


def _enqueue_job(
    db: Session,
    *,
    payload: AutomationGuestPostIn,
    source_type: str,
    source_url: str,
    site: Site,
    client: Client,
    post_status: str,
    author_id: int,
) -> Tuple[Submission, Job, bool]:
    submission_source_type, doc_url, file_url = _resolve_submission_source(source_type, source_url)
    idempotency_key = _build_idempotency_key(
        explicit_key=payload.idempotency_key,
        client_id=client.id,
        site_id=site.id,
        source_type=submission_source_type,
        source_url=source_url,
    )
    notes = _compose_submission_notes(idempotency_key, post_status, author_id)

    existing_submission = _find_existing_submission(
        db,
        client_id=client.id,
        site_id=site.id,
        source_type=submission_source_type,
        doc_url=doc_url,
        file_url=file_url,
        idempotency_key=idempotency_key,
    )
    if existing_submission:
        existing_job = (
            db.query(Job)
            .filter(Job.submission_id == existing_submission.id)
            .order_by(Job.created_at.desc())
            .first()
        )
        if existing_job:
            if existing_job.job_status == "failed":
                existing_job.job_status = "retrying"
                existing_job.last_error = None
                db.add(existing_job)
                db.commit()
                db.refresh(existing_job)
            return existing_submission, existing_job, True
        job = Job(
            submission_id=existing_submission.id,
            client_id=client.id,
            site_id=site.id,
            job_status="queued",
            attempt_count=0,
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        return existing_submission, job, True

    submission = Submission(
        client_id=client.id,
        site_id=site.id,
        source_type=submission_source_type,
        doc_url=doc_url,
        file_url=file_url,
        backlink_placement=payload.backlink_placement,
        post_status=post_status,
        status="queued",
        notes=notes,
    )
    db.add(submission)
    db.commit()
    db.refresh(submission)

    job = Job(
        submission_id=submission.id,
        client_id=client.id,
        site_id=site.id,
        job_status="queued",
        attempt_count=0,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return submission, job, False


async def _parse_automation_payload(request: Request) -> AutomationGuestPostIn:
    content_type = (request.headers.get("content-type") or "").lower()
    data: Dict[str, object]

    if "application/json" in content_type:
        parsed_json = await request.json()
        if not isinstance(parsed_json, dict):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="JSON body must be an object.")
        data = dict(parsed_json)
    elif "application/x-www-form-urlencoded" in content_type:
        raw_body = (await request.body()).decode("utf-8", errors="replace")
        data = dict(parse_qsl(raw_body, keep_blank_values=True))
    elif "multipart/form-data" in content_type:
        try:
            form_data = await request.form()
        except AssertionError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="multipart parsing requires python-multipart to be installed.",
            ) from exc
        data = {key: value for key, value in form_data.items()}
    else:
        # Fallback attempt to support callers with missing/incorrect content-type.
        raw_body = await request.body()
        if raw_body.strip().startswith(b"{"):
            parsed_json = await request.json()
            if not isinstance(parsed_json, dict):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Request body must be a JSON object.",
                )
            data = dict(parsed_json)
        else:
            decoded_body = raw_body.decode("utf-8", errors="replace")
            data = dict(parse_qsl(decoded_body, keep_blank_values=True))

    try:
        return AutomationGuestPostIn(**data)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"error": "validation_error", "details": exc.errors()},
        ) from exc


@router.post("/guest-post-webhook", response_model=AutomationGuestPostOut, status_code=status.HTTP_200_OK)
async def process_guest_post_webhook(
    request: Request,
    db: Session = Depends(get_db),
) -> AutomationGuestPostOut:
    payload = await _parse_automation_payload(request)
    logger.info(
        "automation.webhook.received mode=%s source_type=%s target_site=%s idempotency_key=%s",
        payload.execution_mode,
        payload.source_type,
        payload.target_site,
        payload.idempotency_key,
    )
    try:
        config = get_runtime_config()
    except AutomationError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    if not config["leonardo_api_key"]:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="LEONARDO_API_KEY is not set.")

    post_status = payload.post_status or config["default_post_status"]
    if post_status not in {"draft", "publish"}:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="AUTOMATION_POST_STATUS must be draft or publish.",
        )

    try:
        normalized_source_type, source_url = resolve_source_url(
            payload.source_type,
            payload.doc_url,
            payload.docx_file,
        )
    except AutomationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    site = _resolve_site_by_target(db, payload.target_site)
    credential = _resolve_enabled_credential(db, site.id)
    default_category_ids = _resolve_default_category_ids(db, site.id)
    category_candidates = _resolve_category_candidates(db, site.id)
    author_id = _resolve_effective_author_id(
        payload_author=payload.author,
        credential_author_id=credential.author_id,
        fallback_author_id=config["default_author_id"],
    )
    if author_id <= 0:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No valid author_id found. Set site_credentials.author_id or AUTOMATION_POST_AUTHOR_ID.",
        )
    converter_target_site = _resolve_converter_target(payload.target_site, site.site_url)

    if payload.execution_mode in {"async", "shadow"}:
        client = _resolve_client(db, payload)
        enforce_client_site_access = _read_bool_env("AUTOMATION_ENFORCE_CLIENT_SITE_ACCESS", False)
        if enforce_client_site_access:
            _require_client_site_access(db, client.id, site.id)
        else:
            logger.info(
                "automation.webhook.client_site_access_check_skipped client_id=%s site_id=%s",
                client.id,
                site.id,
            )
        submission, job, deduplicated = _enqueue_job(
            db,
            payload=payload,
            source_type=normalized_source_type,
            source_url=source_url,
            site=site,
            client=client,
            post_status=post_status,
            author_id=author_id,
        )
        shadow_dispatched = False
        if payload.execution_mode == "shadow":
            shadow_dispatched = _dispatch_shadow_webhook(payload)
        logger.info(
            "automation.webhook.enqueued mode=%s submission_id=%s job_id=%s deduplicated=%s shadow_dispatched=%s",
            payload.execution_mode,
            submission.id,
            job.id,
            deduplicated,
            shadow_dispatched,
        )
        return AutomationGuestPostOut(
            ok=True,
            execution_mode=payload.execution_mode,
            deduplicated=deduplicated,
            submission_id=submission.id,
            job_id=job.id,
            job_status=job.job_status,
            shadow_dispatched=shadow_dispatched,
            result=None,
        )

    try:
        pipeline_result = run_guest_post_pipeline(
            source_url=source_url,
            target_site=converter_target_site,
            site_url=site.site_url,
            wp_rest_base=site.wp_rest_base,
            wp_username=credential.wp_username,
            wp_app_password=credential.wp_app_password,
            existing_wp_post_id=None,
            post_status=post_status,
            author_id=author_id,
            category_ids=default_category_ids,
            category_candidates=category_candidates,
            converter_endpoint=config["converter_endpoint"],
            leonardo_api_key=config["leonardo_api_key"],
            leonardo_base_url=config["leonardo_base_url"],
            leonardo_model_id=config["leonardo_model_id"],
            timeout_seconds=config["timeout_seconds"],
            poll_timeout_seconds=config["poll_timeout_seconds"],
            poll_interval_seconds=config["poll_interval_seconds"],
            image_width=config["image_width"],
            image_height=config["image_height"],
            category_llm_enabled=config["category_llm_enabled"],
            category_llm_api_key=config["category_llm_api_key"],
            category_llm_base_url=config["category_llm_base_url"],
            category_llm_model=config["category_llm_model"],
            category_llm_max_categories=config["category_llm_max_categories"],
            category_llm_confidence_threshold=config["category_llm_confidence_threshold"],
        )
    except AutomationError as exc:
        logger.warning("automation.webhook.sync_failed error=%s", str(exc))
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    result = AutomationGuestPostResultOut(
        source_type=normalized_source_type,
        target_site=payload.target_site,
        source_url=source_url,
        converter=pipeline_result["converted"],
        generated_image_url=pipeline_result["image_url"],
        wp_media_id=int(pipeline_result["media_payload"]["id"]),
        wp_media_url=pipeline_result["media_url"],
        wp_post_id=int(pipeline_result["post_payload"]["id"]),
        wp_post_url=pipeline_result["post_payload"].get("link"),
        site_id=site.id,
        site_credential_id=credential.id,
    )
    logger.info(
        "automation.webhook.sync_succeeded site_id=%s wp_post_id=%s",
        site.id,
        result.wp_post_id,
    )
    return AutomationGuestPostOut(
        ok=True,
        execution_mode="sync",
        deduplicated=False,
        shadow_dispatched=False,
        result=result,
    )


def _status_from_submission(
    db: Session,
    submission: Submission,
    *,
    idempotency_key: Optional[str],
) -> AutomationStatusOut:
    job = (
        db.query(Job)
        .filter(Job.submission_id == submission.id)
        .order_by(Job.created_at.desc())
        .first()
    )
    events: list[AutomationStatusEventOut] = []
    if job is not None:
        event_rows = (
            db.query(JobEvent)
            .filter(JobEvent.job_id == job.id)
            .order_by(JobEvent.created_at.asc())
            .all()
        )
        events = [
            AutomationStatusEventOut(
                event_type=row.event_type,
                payload=row.payload,
                created_at=row.created_at,
            )
            for row in event_rows
        ]

    return AutomationStatusOut(
        found=True,
        idempotency_key=idempotency_key,
        submission_id=submission.id,
        submission_status=submission.status,
        job_id=job.id if job else None,
        job_status=job.job_status if job else None,
        attempt_count=job.attempt_count if job else None,
        last_error=job.last_error if job else None,
        wp_post_id=job.wp_post_id if job else None,
        wp_post_url=job.wp_post_url if job else None,
        events=events,
    )


@router.get("/status", response_model=AutomationStatusOut, status_code=status.HTTP_200_OK)
def get_automation_status(
    idempotency_key: Optional[str] = Query(default=None),
    job_id: Optional[UUID] = Query(default=None),
    submission_id: Optional[UUID] = Query(default=None),
    db: Session = Depends(get_db),
) -> AutomationStatusOut:
    if not any([idempotency_key, job_id, submission_id]):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Provide at least one query parameter: idempotency_key, job_id, or submission_id.",
        )

    if job_id is not None:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job:
            return AutomationStatusOut(found=False, idempotency_key=idempotency_key)
        submission = db.query(Submission).filter(Submission.id == job.submission_id).first()
        if not submission:
            return AutomationStatusOut(found=False, idempotency_key=idempotency_key)
        return _status_from_submission(db, submission, idempotency_key=idempotency_key)

    if submission_id is not None:
        submission = db.query(Submission).filter(Submission.id == submission_id).first()
        if not submission:
            return AutomationStatusOut(found=False, idempotency_key=idempotency_key)
        return _status_from_submission(db, submission, idempotency_key=idempotency_key)

    cleaned_key = (idempotency_key or "").strip()
    if not cleaned_key:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="idempotency_key is empty.")

    marker = f"idempotency_key={cleaned_key}"
    candidates = (
        db.query(Submission)
        .filter(Submission.notes.contains(marker))
        .order_by(Submission.created_at.desc())
        .all()
    )
    for submission in candidates:
        note_map = _extract_note_map(submission.notes)
        if note_map.get("idempotency_key") == cleaned_key:
            return _status_from_submission(db, submission, idempotency_key=cleaned_key)

    return AutomationStatusOut(found=False, idempotency_key=cleaned_key)
