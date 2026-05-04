"""Cache the full ``ResearchPayload`` to ``seo_research_cache`` so subsequent
runs of the same target_keyword skip the ~$0.05 DataForSEO + entity extraction
spend within the TTL window.

Owned by the portal_backend schema (migration 0049 widens the existing
seo_research_cache CHECK constraint to allow ``cache_kind='research_payload'``);
this module just declares the columns it needs as a typed ``sqlalchemy.Table``
so the creator service can read/write without owning the schema. Mirrors the
trend_cache.py pattern.

Read path returns the raw payload dict — caller is responsible for
hydrating dataclasses via ``research._payload_from_dict``. Write path takes
an already-jsonable dict (``research._payload_to_dict``).
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import Column, DateTime, MetaData, Table, Text, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine import Engine

logger = logging.getLogger("creator.research_cache")

PROVIDER = "dataforseo"
CACHE_KIND = "research_payload"
DEFAULT_TTL_SECONDS = 7 * 24 * 60 * 60  # 7 days

_ENGINE: Optional[Engine] = None
_METADATA = MetaData()
SEO_RESEARCH_CACHE = Table(
    "seo_research_cache",
    _METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("provider", Text, nullable=False),
    Column("cache_kind", Text, nullable=False),
    Column("lookup_key", Text, nullable=False),
    Column("locale", Text, nullable=True),
    Column("content_hash", Text, nullable=True),
    Column("payload", JSONB, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)


def _get_database_url() -> str:
    return (os.getenv("CREATOR_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()


def _get_engine() -> Optional[Engine]:
    global _ENGINE
    database_url = _get_database_url()
    if not database_url:
        return None
    if _ENGINE is None:
        _ENGINE = create_engine(database_url, pool_pre_ping=True)
    return _ENGINE


def build_lookup_key(
    *,
    target_keyword: str,
    skip_related_keywords: bool,
    skip_entity_extraction: bool,
    research_version: str,
) -> str:
    normalized_keyword = (target_keyword or "").strip().lower()
    return (
        f"{normalized_keyword}|skip_rel={skip_related_keywords}"
        f"|skip_ent={skip_entity_extraction}|ver={research_version}"
    )


def build_locale(language_code: str, location_code: int) -> str:
    return f"{(language_code or '').strip().lower()}-{int(location_code)}"


def get_cached_research_payload(
    *,
    lookup_key: str,
    locale: str,
) -> Optional[Dict[str, Any]]:
    """Return the cached payload dict if a fresh row exists; else None.

    Stale rows (``expires_at`` in the past) are treated as misses so callers
    re-fetch live data. Database errors are swallowed and logged — caching is
    a perf/cost optimisation, never a correctness gate.
    """

    engine = _get_engine()
    if engine is None or not lookup_key:
        return None
    now = datetime.now(timezone.utc)
    try:
        with engine.begin() as connection:
            row = connection.execute(
                select(SEO_RESEARCH_CACHE).where(
                    SEO_RESEARCH_CACHE.c.provider == PROVIDER,
                    SEO_RESEARCH_CACHE.c.cache_kind == CACHE_KIND,
                    SEO_RESEARCH_CACHE.c.lookup_key == lookup_key,
                    SEO_RESEARCH_CACHE.c.locale == locale,
                )
            ).mappings().first()
    except Exception:
        logger.warning(
            "creator.research_cache.lookup_failed key=%s locale=%s",
            lookup_key,
            locale,
            exc_info=True,
        )
        return None
    if not row:
        return None
    expires_at = row.get("expires_at")
    if expires_at is not None and expires_at < now:
        return None
    payload = row.get("payload")
    return dict(payload) if isinstance(payload, dict) else None


def upsert_research_payload(
    *,
    lookup_key: str,
    locale: str,
    payload: Dict[str, Any],
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> None:
    """Insert or replace a research payload row keyed on (provider, cache_kind, lookup_key, locale)."""

    engine = _get_engine()
    if engine is None or not lookup_key:
        return
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=max(60, int(ttl_seconds)))
    serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    try:
        with engine.begin() as connection:
            existing = connection.execute(
                select(SEO_RESEARCH_CACHE.c.id).where(
                    SEO_RESEARCH_CACHE.c.provider == PROVIDER,
                    SEO_RESEARCH_CACHE.c.cache_kind == CACHE_KIND,
                    SEO_RESEARCH_CACHE.c.lookup_key == lookup_key,
                    SEO_RESEARCH_CACHE.c.locale == locale,
                )
            ).scalar()
            if existing:
                connection.execute(
                    SEO_RESEARCH_CACHE.update()
                    .where(SEO_RESEARCH_CACHE.c.id == existing)
                    .values(
                        payload=payload,
                        content_hash=None,  # not used for this cache_kind
                        fetched_at=now,
                        expires_at=expires_at,
                        updated_at=now,
                    )
                )
            else:
                connection.execute(
                    SEO_RESEARCH_CACHE.insert().values(
                        id=uuid.uuid4(),
                        provider=PROVIDER,
                        cache_kind=CACHE_KIND,
                        lookup_key=lookup_key,
                        locale=locale,
                        content_hash=None,
                        payload=payload,
                        fetched_at=now,
                        expires_at=expires_at,
                        created_at=now,
                        updated_at=now,
                    )
                )
    except Exception:
        logger.warning(
            "creator.research_cache.upsert_failed key=%s locale=%s payload_bytes=%d",
            lookup_key,
            locale,
            len(serialized),
            exc_info=True,
        )
