from __future__ import annotations

import json
import os
import logging
import mimetypes
import time
from pathlib import Path
from typing import Dict, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlparse
from uuid import UUID
from uuid import uuid4

import requests
from fastapi import APIRouter, Depends, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import FileResponse
from pydantic import ValidationError
from starlette.datastructures import UploadFile as StarletteUploadFile
from sqlalchemy.orm import Session

from ..auth import (
    ensure_client_access,
    ensure_site_access,
    get_current_user,
    get_optional_current_user,
)
from ..automation_service import (
    AutomationError,
    call_creator_pair_fit,
    check_creator_health,
    converter_publishing_site_from_site_url,
    get_runtime_config,
    resolve_source_url,
    run_submit_article_pipeline,
)
from ..db import get_db
from ..portal_models import (
    Client,
    ClientTargetSite,
    Job,
    JobEvent,
    Site,
    SiteCategory,
    SiteCredential,
    SiteDefaultCategory,
    Submission,
    User,
)
from ..portal_schemas import (
    AutomationSubmitArticleIn,
    AutomationSubmitArticleOut,
    AutomationSubmitArticleResultOut,
    AutomationStatusEventOut,
    AutomationStatusOut,
)
from ..site_profiles import (
    SPECIALIZED_SELECTION_CONTEXTS,
    candidate_target_context_strength,
    count_relevant_inventory_articles,
    derive_site_root_url,
    normalize_site_profile_url,
    top_ranked_publishing_sites_for_target,
)

router = APIRouter(prefix="/automation", tags=["automation"])
logger = logging.getLogger("portal_backend.automation")
_UPLOAD_DIR = Path(os.getenv("AUTOMATION_UPLOAD_DIR", "/tmp/automation_uploads")).resolve()
_UPLOAD_TTL_SECONDS = int(os.getenv("AUTOMATION_UPLOAD_TTL_SECONDS", str(48 * 3600)))
_UPLOAD_MAX_BYTES = int(os.getenv("AUTOMATION_UPLOAD_MAX_BYTES", str(30 * 1024 * 1024)))


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _request_origin_base_url(request: Request) -> str:
    forced = (os.getenv("AUTOMATION_PUBLIC_BASE_URL") or "").strip()
    if forced:
        return forced.rstrip("/")
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").strip()
    scheme = forwarded_proto.split(",")[0].strip() if forwarded_proto else request.url.scheme
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc or "").strip()
    if not host:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to resolve public upload URL base.")
    return f"{scheme}://{host}"


def _cleanup_stale_uploads() -> None:
    if not _UPLOAD_DIR.exists():
        return
    now = time.time()
    for path in _UPLOAD_DIR.glob("*"):
        if not path.is_file():
            continue
        try:
            if now - path.stat().st_mtime > _UPLOAD_TTL_SECONDS:
                path.unlink(missing_ok=True)
        except OSError:
            continue


async def _materialize_multipart_docx_file(data: Dict[str, object], request: Request) -> Dict[str, object]:
    raw_file = data.get("docx_file")
    if not isinstance(raw_file, (UploadFile, StarletteUploadFile)):
        return data

    file_name = (raw_file.filename or "").strip()
    extension = Path(file_name).suffix.lower()
    if extension not in {".doc", ".docx"}:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="docx_file must be a .doc or .docx file.")

    body = await raw_file.read()
    if not body:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="docx_file upload is empty.")
    if len(body) > _UPLOAD_MAX_BYTES:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="Uploaded file is too large.")

    _UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    _cleanup_stale_uploads()

    token_name = f"{uuid4().hex}{extension}"
    stored_path = (_UPLOAD_DIR / token_name).resolve()
    if stored_path.parent != _UPLOAD_DIR:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid upload path.")
    stored_path.write_bytes(body)

    public_base_url = _request_origin_base_url(request)
    data["docx_file"] = f"{public_base_url}/automation/uploads/{token_name}"
    return data


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


def _resolve_publishing_site(db: Session, publishing_site: str) -> Site:
    try:
        site_uuid = UUID(publishing_site.strip())
        site = db.query(Site).filter(Site.id == site_uuid, Site.status == "active").first()
        if site:
            return site
    except ValueError:
        pass

    publishing_site_variants = _host_variants(publishing_site)
    if not publishing_site_variants:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="publishing_site is invalid.")

    sites = db.query(Site).filter(Site.status == "active").all()
    for site in sites:
        if _host_variants(site.site_url) & publishing_site_variants:
            return site

    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active site matches publishing_site.")


def _candidate_publishing_sites(db: Session) -> list[Site]:
    return (
        db.query(Site)
        .filter(Site.status == "active")
        .order_by(Site.name.asc(), Site.created_at.asc())
        .all()
    )


def _parse_auto_site_priority_weights() -> Dict[str, int]:
    raw = (os.getenv("AUTOMATION_AUTO_SITE_PRIORITY_WEIGHTS") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError:
        logger.warning("automation.auto_site_priority_weights_invalid")
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: Dict[str, int] = {}
    for key, value in parsed.items():
        try:
            out[str(key).strip()] = int(value)
        except (TypeError, ValueError):
            continue
    return out


def _select_best_accepted_pair(
    *,
    creator_endpoint: str,
    target_site_url: str,
    target_profile_payload: Dict[str, object],
    target_profile_content_hash: str,
    client_target_site_id: Optional[UUID],
    candidate_rankings: list[Dict[str, object]],
    requested_topic: Optional[str],
    exclude_topics: Optional[list[str]],
    timeout_seconds: int,
) -> Tuple[Optional[Dict[str, object]], list[Dict[str, object]]]:
    allow_rejected_pairs = _read_bool_env("ALLOW_REJECTED_PAIRS_FOR_TESTING", False)
    target_primary_context = str(target_profile_payload.get("primary_context") or "").strip()

    def _candidate_context_strength(item: Dict[str, object]) -> int:
        return candidate_target_context_strength(item, target_primary_context)

    def _sort_accepted_pairs(items: list[Dict[str, object]]) -> list[Dict[str, object]]:
        sorted_items = list(items)
        sorted_items.sort(
            key=lambda item: (
                bool(item.get("override_selected")),
                not bool(item.get("target_context_strength")),
                not bool(item.get("topic_internal_support_count")),
                -int(item.get("pair_fit_score") or 0),
                -int(item.get("topic_internal_support_count") or 0),
                -int(item.get("target_context_strength") or 0),
                -int(item.get("score") or 0),
                -int((item.get("details") or {}).get("semantic_score") or 0),
                -int((item.get("details") or {}).get("internal_link_support") or 0),
            )
        )
        return sorted_items

    evaluated: list[Dict[str, object]] = []
    for candidate in candidate_rankings:
        try:
            pair_fit_result = call_creator_pair_fit(
                creator_endpoint=creator_endpoint,
                target_site_url=target_site_url,
                publishing_site_url=str(candidate.get("site_url") or ""),
                publishing_site_id=str(candidate.get("site_id") or ""),
                client_target_site_id=str(client_target_site_id) if client_target_site_id else None,
                requested_topic=requested_topic,
                exclude_topics=exclude_topics or [],
                target_profile_payload=target_profile_payload,
                target_profile_content_hash=target_profile_content_hash,
                publishing_profile_payload=dict(candidate.get("profile") or {}),
                publishing_profile_content_hash=str(candidate.get("content_hash") or ""),
                timeout_seconds=timeout_seconds,
            )
        except AutomationError as exc:
            result = {
                **candidate,
                "pair_fit_error": str(exc),
                "accepted": False,
                "override_selected": False,
                "specialized_context_match": _candidate_context_strength(candidate) > 0,
                "target_context_strength": _candidate_context_strength(candidate),
            }
            evaluated.append(result)
            continue
        pair_fit = dict(pair_fit_result.get("pair_fit") or {})
        final_match_decision = str(pair_fit.get("final_match_decision") or "").strip().lower()
        if not final_match_decision:
            final_match_decision = "accepted" if bool(pair_fit.get("backlink_fit_ok")) else "hard_reject"
        accepted = final_match_decision == "accepted" and bool(pair_fit.get("backlink_fit_ok"))
        override_selected = bool(allow_rejected_pairs and final_match_decision in {"weak_fit", "hard_reject"})
        combined_score = int(candidate.get("score") or 0) + int(pair_fit.get("fit_score") or 0)
        context_strength = _candidate_context_strength(candidate)
        topic_internal_support_count = count_relevant_inventory_articles(
            inventory_context=candidate.get("inventory_context") if isinstance(candidate.get("inventory_context"), dict) else {},
            target_profile=target_profile_payload,
            topic=str(pair_fit.get("final_article_topic") or ""),
        )
        result = {
            **candidate,
            "pair_fit": pair_fit,
            "pair_fit_cached": bool(pair_fit_result.get("cached")),
            "accepted": accepted or override_selected,
            "override_selected": override_selected and not accepted,
            "final_match_decision": final_match_decision,
            "pair_fit_score": int(pair_fit.get("fit_score") or 0),
            "combined_score": combined_score,
            "specialized_context_match": context_strength > 0,
            "target_context_strength": context_strength,
            "topic_internal_support_count": topic_internal_support_count,
        }
        evaluated.append(result)

    accepted_pairs = _sort_accepted_pairs([item for item in evaluated if item.get("accepted")])
    return (accepted_pairs[0] if accepted_pairs else None), evaluated


def _resolve_or_auto_select_publishing_site(
    db: Session,
    *,
    payload: AutomationSubmitArticleIn,
    client: Client,
    client_target_site: Optional[ClientTargetSite],
    creator_endpoint: str,
) -> Site:
    explicit_site = (payload.publishing_site or "").strip()
    target_url = (
        (client_target_site.target_site_url or "").strip()
        if client_target_site is not None
        else (payload.target_site_url or "").strip()
    )
    target_root_url = (
        (client_target_site.target_site_root_url or "").strip()
        if client_target_site is not None
        else derive_site_root_url(target_url)
    )
    if not target_url:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="target_site_url is required for creator article generation.",
        )
    priority_weights = _parse_auto_site_priority_weights()
    if explicit_site:
        site = _resolve_publishing_site(db, explicit_site)
        target_profile, target_profile_content_hash, candidate_rankings = top_ranked_publishing_sites_for_target(
            db,
            target_site_url=target_url,
            target_site_root_url=target_root_url or None,
            candidate_sites=[site],
            client_target_site_id=client_target_site.id if client_target_site is not None else None,
            timeout_seconds=10,
            max_pages=3,
            min_score=0,
            limit=1,
            business_priority_weights=priority_weights,
        )
        target_profile_payload = dict(target_profile or {})
        selected_pair, evaluated = _select_best_accepted_pair(
            creator_endpoint=creator_endpoint,
            target_site_url=target_url,
            target_profile_payload=target_profile_payload,
            target_profile_content_hash=target_profile_content_hash,
            client_target_site_id=client_target_site.id if client_target_site is not None else None,
            candidate_rankings=candidate_rankings,
            requested_topic=payload.topic,
            exclude_topics=[],
            timeout_seconds=90,
        )
        if selected_pair is None:
            logger.warning(
                "automation.creator_pair_rejected explicit_site=%s target_site_url=%s candidates=%s",
                site.site_url,
                target_url,
                evaluated[:3],
            )
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error": "no_natural_publishing_site_fit",
                    "target_site_url": target_url,
                    "target_primary_context": str(target_profile.get("primary_context") or ""),
                    "candidates": evaluated[:3],
                },
            )
        return site
    candidate_sites = _candidate_publishing_sites(db)
    if not candidate_sites:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No eligible publishing sites are available for auto-selection.",
        )
    target_profile, target_profile_content_hash, ranked = top_ranked_publishing_sites_for_target(
        db,
        target_site_url=target_url,
        target_site_root_url=target_root_url or None,
        candidate_sites=candidate_sites,
        client_target_site_id=client_target_site.id if client_target_site is not None else None,
        timeout_seconds=10,
        max_pages=3,
        min_score=max(10, int(os.getenv("AUTOMATION_AUTO_SITE_MIN_SCORE", "18"))),
        limit=max(1, int(os.getenv("AUTOMATION_AUTO_SITE_TOP_K", "5"))),
        business_priority_weights=priority_weights,
    )
    if not ranked:
        top_reason = {}
        logger.warning(
            "automation.auto_site_no_ranked_candidates target_site_url=%s target_primary_context=%s",
            target_url,
            str(target_profile.get("primary_context") or ""),
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "no_natural_publishing_site_fit",
                "target_site_url": target_url,
                "target_primary_context": str(target_profile.get("primary_context") or ""),
                "candidates": [],
                "details": top_reason,
            },
        )
    target_profile_payload = dict(target_profile or {})
    selected_pair, evaluated = _select_best_accepted_pair(
        creator_endpoint=creator_endpoint,
        target_site_url=target_url,
        target_profile_payload=target_profile_payload,
        target_profile_content_hash=target_profile_content_hash,
        client_target_site_id=client_target_site.id if client_target_site is not None else None,
        candidate_rankings=ranked,
        requested_topic=payload.topic,
        exclude_topics=[],
        timeout_seconds=90,
    )
    if selected_pair is None:
        top_reason = evaluated[0].get("details") if evaluated else {}
        logger.warning(
            "automation.auto_site_no_accepted_pair target_site_url=%s evaluated=%s",
            target_url,
            evaluated[:3],
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "no_accepted_publishing_site_pair",
                "target_site_url": target_url,
                "target_primary_context": str(target_profile.get("primary_context") or ""),
                "candidates": evaluated[:5],
                "details": top_reason,
            },
        )
    best_site = next((site for site in candidate_sites if str(site.id) == str(selected_pair.get("site_id"))), None)
    if best_site is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Auto-selected site could not be resolved.")
    logger.info(
        "automation.webhook.auto_selected_site client_id=%s site_id=%s score=%s pair_fit_score=%s target=%s combined_score=%s",
        client.id,
        best_site.id,
        selected_pair.get("score") or 0,
        selected_pair.get("pair_fit_score") or 0,
        target_url,
        selected_pair.get("combined_score") or 0,
    )
    return best_site


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
            detail="No enabled site credential found for publishing site.",
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


def _resolve_client(db: Session, payload: AutomationSubmitArticleIn) -> Client:
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


def _resolve_client_target_site(
    db: Session,
    *,
    client: Client,
    payload: AutomationSubmitArticleIn,
) -> Optional[ClientTargetSite]:
    rows = (
        db.query(ClientTargetSite)
        .filter(ClientTargetSite.client_id == client.id)
        .order_by(ClientTargetSite.is_primary.desc(), ClientTargetSite.created_at.asc())
        .all()
    )
    if not rows:
        return None

    if payload.target_site_id is not None:
        for row in rows:
            if row.id == payload.target_site_id:
                if payload.target_site_url:
                    requested_url = normalize_site_profile_url(payload.target_site_url)
                    expected_urls = {
                        normalize_site_profile_url(row.target_site_url or ""),
                        normalize_site_profile_url(row.target_site_root_url or ""),
                    }
                    if requested_url not in {value for value in expected_urls if value}:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="target_site_url does not match target_site_id.")
                return row
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="target_site_id is not assigned to this client.")

    if payload.target_site_url:
        requested_url = normalize_site_profile_url(payload.target_site_url)
        for row in rows:
            if requested_url in {
                normalize_site_profile_url(row.target_site_url or ""),
                normalize_site_profile_url(row.target_site_root_url or ""),
            }:
                return row
        return None

    primary = next((row for row in rows if bool(row.is_primary)), None)
    return primary or rows[0]


def _resolve_submission_source(source_type: str, source_url: str) -> Tuple[str, Optional[str], Optional[str]]:
    if source_type == "google-doc":
        return "google-doc", source_url, None
    return "docx-upload", None, source_url


def _payload_has_source_document(payload: AutomationSubmitArticleIn) -> bool:
    return bool((payload.doc_url or "").strip() or (payload.docx_file or "").strip())


def _resolve_converter_publishing_site(publishing_site: str, site_url: str) -> str:
    publishing_host = _normalized_host(publishing_site)
    if publishing_host:
        return publishing_host
    return converter_publishing_site_from_site_url(site_url)


def _safe_note_value(value: object) -> str:
    return str(value).replace(";", "_").replace("=", "_").strip()


def _compose_submission_notes(
    idempotency_key: str,
    post_status: str,
    author_id: int,
    *,
    client_target_site: Optional[ClientTargetSite] = None,
    custom_target_site_url: Optional[str] = None,
    anchor: Optional[str] = None,
    topic: Optional[str] = None,
    manual_create_article: bool = False,
    creator_mode: bool = False,
    auto_selected_site: bool = False,
) -> str:
    parts = [
        f"idempotency_key={_safe_note_value(idempotency_key)}",
        f"post_status={_safe_note_value(post_status)}",
        f"author_id={_safe_note_value(author_id)}",
    ]
    if client_target_site is not None:
        parts.append(f"client_target_site_id={_safe_note_value(client_target_site.id)}")
        if client_target_site.target_site_domain:
            parts.append(f"client_target_site_domain={_safe_note_value(client_target_site.target_site_domain)}")
        if client_target_site.target_site_url:
            parts.append(f"client_target_site_url={_safe_note_value(client_target_site.target_site_url)}")
        if client_target_site.target_site_root_url:
            parts.append(f"client_target_site_root_url={_safe_note_value(client_target_site.target_site_root_url)}")
    elif custom_target_site_url:
        parts.append(f"client_target_site_url={_safe_note_value(custom_target_site_url)}")
    if anchor:
        parts.append(f"anchor={_safe_note_value(anchor)}")
    if topic:
        parts.append(f"topic={_safe_note_value(topic)}")
    if manual_create_article:
        parts.append("manual_create_article=true")
    if creator_mode:
        parts.append("creator_mode=true")
    if auto_selected_site:
        parts.append("auto_selected_site=true")
    return ";".join(parts)


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
    request_kind: str,
    source_type: str,
    doc_url: Optional[str],
    file_url: Optional[str],
    idempotency_key: str,
) -> Optional[Submission]:
    query = db.query(Submission).filter(
        Submission.client_id == client_id,
        Submission.site_id == site_id,
        Submission.request_kind == request_kind,
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


def _dispatch_shadow_webhook(payload: AutomationSubmitArticleIn) -> bool:
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
    payload: AutomationSubmitArticleIn,
    request_kind: str,
    source_type: str,
    source_url: Optional[str],
    site: Site,
    client: Client,
    post_status: str,
    requires_admin_approval: bool,
    author_id: int,
    client_target_site: Optional[ClientTargetSite],
    creator_mode: bool,
    auto_selected_site: bool = False,
) -> Tuple[Submission, Job, bool]:
    manual_create_article = request_kind == "create_article" and not (source_url or "").strip()
    creator_create_article = manual_create_article and creator_mode
    if manual_create_article:
        submission_source_type = "google-doc"
        doc_url = None
        file_url = None
    else:
        if source_url is None:
            raise RuntimeError("source_url is required for non-manual submissions.")
        submission_source_type, doc_url, file_url = _resolve_submission_source(source_type, source_url)
    # For creator article-creation requests without an explicit idempotency key, generate a
    # unique key so that multiple creations with the same anchor/topic/site
    # each create their own submission + job instead of being deduplicated.
    if creator_create_article and not (payload.idempotency_key or "").strip():
        idempotency_source = f"create_article:{uuid4().hex}"
    else:
        idempotency_source = (source_url or "").strip() or f"create_article:{(payload.anchor or '').strip()}:{(payload.topic or '').strip()}"
    idempotency_key = _build_idempotency_key(
        explicit_key=payload.idempotency_key,
        client_id=client.id,
        site_id=site.id,
        source_type=submission_source_type,
        source_url=idempotency_source,
    )
    notes = _compose_submission_notes(
        idempotency_key,
        post_status,
        author_id,
        client_target_site=client_target_site,
        custom_target_site_url=payload.target_site_url,
        anchor=payload.anchor,
        topic=payload.topic,
        manual_create_article=manual_create_article,
        creator_mode=creator_mode,
        auto_selected_site=auto_selected_site,
    )

    existing_submission = _find_existing_submission(
        db,
        client_id=client.id,
        site_id=site.id,
        request_kind=request_kind,
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
            changed_existing_job = False
            existing_job.requires_admin_approval = requires_admin_approval
            changed_existing_job = True
            if requires_admin_approval:
                existing_job.approved_by = None
                existing_job.approved_by_name_snapshot = None
                existing_job.approved_at = None
            if manual_create_article and not creator_create_article and existing_job.job_status in {"queued", "retrying", "failed", "processing"}:
                existing_job.job_status = "pending_approval"
                existing_job.last_error = None
                changed_existing_job = True
            if creator_create_article and existing_job.job_status == "pending_approval":
                existing_job.job_status = "queued"
                existing_job.last_error = None
                changed_existing_job = True
            if existing_job.job_status == "failed":
                existing_job.job_status = "retrying"
                existing_job.last_error = None
                db.add(existing_job)
                db.commit()
                db.refresh(existing_job)
                return existing_submission, existing_job, True
            if changed_existing_job:
                db.add(existing_job)
                db.commit()
                db.refresh(existing_job)
            return existing_submission, existing_job, True
        job = Job(
            submission_id=existing_submission.id,
            client_id=client.id,
            site_id=site.id,
            job_status="pending_approval" if manual_create_article else "queued",
            requires_admin_approval=requires_admin_approval,
            approved_by=None,
            approved_by_name_snapshot=None,
            approved_at=None,
            attempt_count=0,
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        return existing_submission, job, True

    submission = Submission(
        client_id=client.id,
        site_id=site.id,
        request_kind=request_kind,
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
        job_status="queued" if creator_create_article else ("pending_approval" if manual_create_article else "queued"),
        requires_admin_approval=requires_admin_approval,
        approved_by=None,
        approved_by_name_snapshot=None,
        approved_at=None,
        attempt_count=0,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return submission, job, False


async def _parse_automation_payload(request: Request) -> AutomationSubmitArticleIn:
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
        data = await _materialize_multipart_docx_file(data, request)
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
        return AutomationSubmitArticleIn(**data)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"error": "validation_error", "details": exc.errors()},
        ) from exc


@router.get("/uploads/{file_name}")
def get_automation_upload(file_name: str) -> FileResponse:
    safe_name = Path(file_name).name
    if safe_name != file_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file name.")
    extension = Path(safe_name).suffix.lower()
    if extension not in {".doc", ".docx"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported file extension.")
    file_path = (_UPLOAD_DIR / safe_name).resolve()
    if file_path.parent != _UPLOAD_DIR or not file_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Uploaded file not found.")
    media_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
    return FileResponse(path=file_path, media_type=media_type, filename=safe_name)


@router.post("/submit-article-webhook", response_model=AutomationSubmitArticleOut, status_code=status.HTTP_200_OK)
async def process_submit_article_webhook(
    request: Request,
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_optional_current_user),
) -> AutomationSubmitArticleOut:
    payload = await _parse_automation_payload(request)
    logger.info(
        "automation.webhook.received mode=%s source_type=%s publishing_site=%s idempotency_key=%s",
        payload.execution_mode,
        payload.source_type,
        payload.publishing_site,
        payload.idempotency_key,
    )
    request_kind = payload.request_kind
    manual_create_article = request_kind == "create_article" and not _payload_has_source_document(payload) and not payload.creator_mode
    creator_create_article = request_kind == "create_article" and payload.creator_mode and not _payload_has_source_document(payload)
    if manual_create_article and payload.execution_mode == "sync":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Created-article requests without a document require async or shadow mode.",
        )
    if creator_create_article and payload.execution_mode == "sync":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Article-creation requests require async or shadow mode.",
        )

    site: Optional[Site] = None

    if creator_create_article and payload.execution_mode in {"async", "shadow"}:
        try:
            health = check_creator_health(
                creator_endpoint=get_runtime_config()["creator_endpoint"],
                timeout_seconds=10,
            )
        except AutomationError as exc:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
        if not health.get("ok"):
            detail = health or {"error": "creator_not_ready"}
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=detail)
        client = _resolve_client(db, payload)
        client_target_site = _resolve_client_target_site(db, client=client, payload=payload)
        site = _resolve_or_auto_select_publishing_site(
            db,
            payload=payload,
            client=client,
            client_target_site=client_target_site,
            creator_endpoint=get_runtime_config()["creator_endpoint"],
        )
        if current_user is not None and current_user.role != "admin":
            ensure_client_access(db, current_user, client.id)
            ensure_site_access(db, current_user, site.id)
        submission, job, deduplicated = _enqueue_job(
            db,
            payload=payload,
            request_kind=request_kind,
            source_type="google-doc",
            source_url=None,
            site=site,
            client=client,
            post_status="draft",
            requires_admin_approval=True,
            author_id=0,
            client_target_site=client_target_site,
            creator_mode=True,
            auto_selected_site=not bool((payload.publishing_site or "").strip()),
        )
        shadow_dispatched = False
        if payload.execution_mode == "shadow":
            shadow_dispatched = _dispatch_shadow_webhook(payload)
        return AutomationSubmitArticleOut(
            ok=True,
            execution_mode=payload.execution_mode,
            deduplicated=deduplicated,
            submission_id=submission.id,
            job_id=job.id,
            job_status=job.job_status,
            shadow_dispatched=shadow_dispatched,
            result=None,
        )

    if manual_create_article and payload.execution_mode in {"async", "shadow"}:
        site = _resolve_publishing_site(db, payload.publishing_site or "")
        client = _resolve_client(db, payload)
        client_target_site = _resolve_client_target_site(db, client=client, payload=payload)
        if current_user is not None and current_user.role != "admin":
            ensure_client_access(db, current_user, client.id)
            ensure_site_access(db, current_user, site.id)
        submission, job, deduplicated = _enqueue_job(
            db,
            payload=payload,
            request_kind=request_kind,
            source_type="google-doc",
            source_url=None,
            site=site,
            client=client,
            post_status="draft",
            requires_admin_approval=True,
            author_id=0,
            client_target_site=client_target_site,
            creator_mode=payload.creator_mode,
        )
        shadow_dispatched = False
        if payload.execution_mode == "shadow":
            shadow_dispatched = _dispatch_shadow_webhook(payload)
        return AutomationSubmitArticleOut(
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
        config = get_runtime_config()
    except AutomationError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    if site is None:
        site = _resolve_publishing_site(db, payload.publishing_site or "")

    if not config["leonardo_api_key"]:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="LEONARDO_API_KEY is not set.")

    post_status = payload.post_status or config["default_post_status"]
    if post_status not in {"draft", "publish"}:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="AUTOMATION_POST_STATUS must be draft or publish.",
        )
    requires_admin_approval = True
    if requires_admin_approval:
        post_status = "draft"

    try:
        normalized_source_type, source_url = resolve_source_url(
            payload.source_type,
            payload.doc_url,
            payload.docx_file,
        )
    except AutomationError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

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
    converter_publishing_site = _resolve_converter_publishing_site(payload.publishing_site, site.site_url)

    if payload.execution_mode in {"async", "shadow"}:
        client = _resolve_client(db, payload)
        client_target_site = _resolve_client_target_site(db, client=client, payload=payload)
        if current_user is not None and current_user.role != "admin":
            ensure_client_access(db, current_user, client.id)
            ensure_site_access(db, current_user, site.id)
        submission, job, deduplicated = _enqueue_job(
            db,
            payload=payload,
            request_kind=request_kind,
            source_type=normalized_source_type,
            source_url=source_url,
            site=site,
            client=client,
            post_status=post_status,
            requires_admin_approval=requires_admin_approval,
            author_id=author_id,
            client_target_site=client_target_site,
            creator_mode=payload.creator_mode,
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
        return AutomationSubmitArticleOut(
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
        if current_user is not None and current_user.role != "admin" and payload.execution_mode == "sync":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Clients cannot run sync execution mode.")
        pipeline_result = run_submit_article_pipeline(
            source_url=source_url,
            publishing_site=converter_publishing_site,
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

    result = AutomationSubmitArticleResultOut(
        source_type=normalized_source_type,
        publishing_site=payload.publishing_site,
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
    return AutomationSubmitArticleOut(
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
    current_user: User = Depends(get_current_user),
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
        if current_user.role != "admin":
            ensure_client_access(db, current_user, job.client_id)
            ensure_site_access(db, current_user, job.site_id)
        submission = db.query(Submission).filter(Submission.id == job.submission_id).first()
        if not submission:
            return AutomationStatusOut(found=False, idempotency_key=idempotency_key)
        return _status_from_submission(db, submission, idempotency_key=idempotency_key)

    if submission_id is not None:
        submission = db.query(Submission).filter(Submission.id == submission_id).first()
        if not submission:
            return AutomationStatusOut(found=False, idempotency_key=idempotency_key)
        if current_user.role != "admin":
            ensure_client_access(db, current_user, submission.client_id)
            ensure_site_access(db, current_user, submission.site_id)
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
            if current_user.role != "admin":
                ensure_client_access(db, current_user, submission.client_id)
                ensure_site_access(db, current_user, submission.site_id)
            return _status_from_submission(db, submission, idempotency_key=cleaned_key)

    return AutomationStatusOut(found=False, idempotency_key=cleaned_key)
