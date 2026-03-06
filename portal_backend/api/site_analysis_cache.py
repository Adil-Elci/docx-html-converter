from __future__ import annotations

import hashlib
from typing import Any, Optional
from urllib.parse import urlparse
from uuid import UUID

from sqlalchemy.orm import Session

from .portal_models import SiteAnalysisCache, utcnow

PHASE2_SITE_ANALYSIS_CACHE_KIND = "phase2_site_analysis"
DEFAULT_CACHE_PROMPT_VERSION = "v1"


def normalize_site_analysis_url(value: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    parsed = urlparse(cleaned if "://" in cleaned else f"https://{cleaned}")
    if not parsed.netloc:
        return cleaned.rstrip("/")
    scheme = (parsed.scheme or "https").lower()
    host = (parsed.netloc or "").strip().lower().rstrip("/")
    path = parsed.path or ""
    return f"{scheme}://{host}{path}".rstrip("/")


def build_site_analysis_content_hash(content: str) -> str:
    normalized = (content or "").strip().encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()


def get_site_analysis_cache(
    db: Session,
    *,
    site_role: str,
    normalized_url: str,
    content_hash: str,
    generator_mode: str,
    prompt_version: str = DEFAULT_CACHE_PROMPT_VERSION,
    model_name: str = "",
    cache_kind: str = PHASE2_SITE_ANALYSIS_CACHE_KIND,
    publishing_site_id: Optional[UUID] = None,
    client_target_site_id: Optional[UUID] = None,
) -> Optional[SiteAnalysisCache]:
    query = db.query(SiteAnalysisCache).filter(
        SiteAnalysisCache.cache_kind == cache_kind,
        SiteAnalysisCache.site_role == site_role,
        SiteAnalysisCache.normalized_url == normalized_url,
        SiteAnalysisCache.content_hash == content_hash,
        SiteAnalysisCache.generator_mode == generator_mode,
        SiteAnalysisCache.model_name == (model_name or ""),
        SiteAnalysisCache.prompt_version == prompt_version,
    )
    if publishing_site_id is None:
        query = query.filter(SiteAnalysisCache.publishing_site_id.is_(None))
    else:
        query = query.filter(SiteAnalysisCache.publishing_site_id == publishing_site_id)
    if client_target_site_id is None:
        query = query.filter(SiteAnalysisCache.client_target_site_id.is_(None))
    else:
        query = query.filter(SiteAnalysisCache.client_target_site_id == client_target_site_id)
    return query.order_by(SiteAnalysisCache.updated_at.desc(), SiteAnalysisCache.created_at.desc()).first()


def get_latest_site_analysis_cache(
    db: Session,
    *,
    site_role: str,
    normalized_url: str,
    cache_kind: str = PHASE2_SITE_ANALYSIS_CACHE_KIND,
    publishing_site_id: Optional[UUID] = None,
    client_target_site_id: Optional[UUID] = None,
) -> Optional[SiteAnalysisCache]:
    query = db.query(SiteAnalysisCache).filter(
        SiteAnalysisCache.cache_kind == cache_kind,
        SiteAnalysisCache.site_role == site_role,
        SiteAnalysisCache.normalized_url == normalized_url,
    )
    if publishing_site_id is None:
        query = query.filter(SiteAnalysisCache.publishing_site_id.is_(None))
    else:
        query = query.filter(SiteAnalysisCache.publishing_site_id == publishing_site_id)
    if client_target_site_id is None:
        query = query.filter(SiteAnalysisCache.client_target_site_id.is_(None))
    else:
        query = query.filter(SiteAnalysisCache.client_target_site_id == client_target_site_id)
    return query.order_by(SiteAnalysisCache.updated_at.desc(), SiteAnalysisCache.created_at.desc()).first()


def upsert_site_analysis_cache(
    db: Session,
    *,
    site_role: str,
    normalized_url: str,
    content_hash: str,
    generator_mode: str,
    payload: dict[str, Any],
    prompt_version: str = DEFAULT_CACHE_PROMPT_VERSION,
    model_name: str = "",
    cache_kind: str = PHASE2_SITE_ANALYSIS_CACHE_KIND,
    publishing_site_id: Optional[UUID] = None,
    client_target_site_id: Optional[UUID] = None,
) -> SiteAnalysisCache:
    record = get_site_analysis_cache(
        db,
        site_role=site_role,
        normalized_url=normalized_url,
        content_hash=content_hash,
        generator_mode=generator_mode,
        prompt_version=prompt_version,
        model_name=model_name,
        cache_kind=cache_kind,
        publishing_site_id=publishing_site_id,
        client_target_site_id=client_target_site_id,
    )
    if record is None:
        record = SiteAnalysisCache(
            cache_kind=cache_kind,
            site_role=site_role,
            publishing_site_id=publishing_site_id,
            client_target_site_id=client_target_site_id,
            normalized_url=normalized_url,
            content_hash=content_hash,
            generator_mode=generator_mode,
            model_name=model_name or "",
            prompt_version=prompt_version,
            payload=payload,
        )
    else:
        record.payload = payload
        record.updated_at = utcnow()
    db.add(record)
    return record
