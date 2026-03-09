from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import Column, DateTime, Integer, MetaData, Table, Text, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.engine import Engine

logger = logging.getLogger("creator.site_fit_cache")

_ENGINE: Optional[Engine] = None
_METADATA = MetaData()
SITE_FIT_CACHE = Table(
    "site_fit_cache",
    _METADATA,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("publishing_site_id", UUID(as_uuid=True), nullable=False),
    Column("client_target_site_id", UUID(as_uuid=True), nullable=True),
    Column("target_normalized_url", Text, nullable=False),
    Column("publishing_profile_hash", Text, nullable=False),
    Column("target_profile_hash", Text, nullable=False),
    Column("model_name", Text, nullable=False),
    Column("prompt_version", Text, nullable=False),
    Column("fit_score", Integer, nullable=False),
    Column("decision", Text, nullable=False),
    Column("payload", JSONB, nullable=False),
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


def _stable_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = json.loads(json.dumps(payload or {}, sort_keys=True, ensure_ascii=False))
    return normalized if isinstance(normalized, dict) else {}


def get_site_fit_cache_entry(
    *,
    publishing_site_id: str,
    target_normalized_url: str,
    publishing_profile_hash: str,
    target_profile_hash: str,
    prompt_version: str,
) -> Optional[Dict[str, Any]]:
    engine = _get_engine()
    if engine is None:
        return None
    if not all([publishing_site_id, target_normalized_url, publishing_profile_hash, target_profile_hash, prompt_version]):
        return None
    try:
        with engine.begin() as connection:
            row = connection.execute(
                select(SITE_FIT_CACHE).where(
                    SITE_FIT_CACHE.c.publishing_site_id == publishing_site_id,
                    SITE_FIT_CACHE.c.target_normalized_url == target_normalized_url,
                    SITE_FIT_CACHE.c.publishing_profile_hash == publishing_profile_hash,
                    SITE_FIT_CACHE.c.target_profile_hash == target_profile_hash,
                    SITE_FIT_CACHE.c.prompt_version == prompt_version,
                )
            ).mappings().first()
    except Exception:
        logger.warning("creator.site_fit_cache.lookup_failed", exc_info=True)
        return None
    return dict(row) if row else None


def upsert_site_fit_cache_entry(
    *,
    publishing_site_id: str,
    client_target_site_id: str = "",
    target_normalized_url: str,
    publishing_profile_hash: str,
    target_profile_hash: str,
    prompt_version: str,
    model_name: str,
    fit_score: int,
    decision: str,
    payload: Dict[str, Any],
) -> None:
    engine = _get_engine()
    if engine is None:
        return
    if not all([publishing_site_id, target_normalized_url, publishing_profile_hash, target_profile_hash, prompt_version]):
        return
    now = datetime.now(timezone.utc)
    decision_value = "accepted" if str(decision or "").strip().lower() != "rejected" else "rejected"
    stable_payload = _stable_payload(payload)
    try:
        with engine.begin() as connection:
            existing = connection.execute(
                select(SITE_FIT_CACHE.c.id).where(
                    SITE_FIT_CACHE.c.publishing_site_id == publishing_site_id,
                    SITE_FIT_CACHE.c.target_normalized_url == target_normalized_url,
                    SITE_FIT_CACHE.c.publishing_profile_hash == publishing_profile_hash,
                    SITE_FIT_CACHE.c.target_profile_hash == target_profile_hash,
                    SITE_FIT_CACHE.c.prompt_version == prompt_version,
                )
            ).scalar_one_or_none()
            values = {
                "publishing_site_id": publishing_site_id,
                "client_target_site_id": client_target_site_id or None,
                "target_normalized_url": target_normalized_url,
                "publishing_profile_hash": publishing_profile_hash,
                "target_profile_hash": target_profile_hash,
                "prompt_version": prompt_version,
                "model_name": model_name or "",
                "fit_score": max(0, min(100, int(fit_score or 0))),
                "decision": decision_value,
                "payload": stable_payload,
                "updated_at": now,
            }
            if existing is None:
                values["id"] = uuid.uuid4()
                values["created_at"] = now
                connection.execute(SITE_FIT_CACHE.insert().values(**values))
            else:
                connection.execute(
                    SITE_FIT_CACHE.update().where(SITE_FIT_CACHE.c.id == existing).values(**values)
                )
    except Exception:
        logger.warning("creator.site_fit_cache.upsert_failed", exc_info=True)
