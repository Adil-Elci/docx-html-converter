from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import UUID

from sqlalchemy.orm import Session

from .portal_models import CreatorOutput
from .site_profiles import derive_site_root_url, normalize_site_profile_url


def _normalize_history_value(value: Any) -> str:
    return " ".join(str(value or "").split()).strip().casefold()


def _clean_history_value(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _dedupe_history_values(values: Iterable[Any], *, limit: int) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw_value in values:
        cleaned = _clean_history_value(raw_value)
        if not cleaned:
            continue
        normalized = _normalize_history_value(cleaned)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(cleaned)
        if len(out) >= limit:
            break
    return out


def _extract_payload_history(payload: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    phase3 = payload.get("phase3") if isinstance(payload.get("phase3"), dict) else {}
    phase4 = payload.get("phase4") if isinstance(payload.get("phase4"), dict) else {}
    phase5 = payload.get("phase5") if isinstance(payload.get("phase5"), dict) else {}
    title_package = phase3.get("title_package") if isinstance(phase3.get("title_package"), dict) else {}

    topics = [
        phase3.get("final_article_topic"),
        phase3.get("primary_keyword"),
        title_package.get("h1"),
        title_package.get("title"),
    ]
    titles = [
        title_package.get("h1"),
        title_package.get("title"),
        phase4.get("h1"),
        phase5.get("meta_title"),
        phase5.get("title"),
    ]
    return (
        _dedupe_history_values(topics, limit=8),
        _dedupe_history_values(titles, limit=8),
    )


def collect_recent_creator_history(
    payloads: Iterable[Dict[str, Any]],
    *,
    max_topics: int = 12,
    max_titles: int = 12,
) -> Dict[str, List[str]]:
    topic_values: List[str] = []
    title_values: List[str] = []
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        payload_topics, payload_titles = _extract_payload_history(payload)
        topic_values.extend(payload_topics)
        topic_values.extend(payload_titles[:2])
        title_values.extend(payload_titles)
    return {
        "exclude_topics": _dedupe_history_values(topic_values, limit=max_topics),
        "recent_article_titles": _dedupe_history_values(title_values, limit=max_titles),
    }


def load_recent_creator_history(
    session: Session,
    *,
    client_id: UUID,
    target_site_url: str,
    exclude_job_id: Optional[UUID] = None,
    row_limit: int = 120,
    max_topics: int = 12,
    max_titles: int = 12,
) -> Dict[str, List[str]]:
    normalized_target_url = normalize_site_profile_url(target_site_url)
    normalized_target_root = derive_site_root_url(target_site_url)
    payloads: List[Dict[str, Any]] = []

    rows = (
        session.query(CreatorOutput.job_id, CreatorOutput.target_site_url, CreatorOutput.payload)
        .filter(CreatorOutput.client_id == client_id)
        .order_by(CreatorOutput.created_at.desc())
        .limit(max(20, row_limit))
        .all()
    )
    for row_job_id, row_target_site_url, payload in rows:
        if exclude_job_id is not None and row_job_id == exclude_job_id:
            continue
        row_normalized_url = normalize_site_profile_url(str(row_target_site_url or ""))
        row_normalized_root = derive_site_root_url(row_normalized_url)
        if normalized_target_root:
            if row_normalized_root != normalized_target_root:
                continue
        elif normalized_target_url and row_normalized_url != normalized_target_url:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)

    return collect_recent_creator_history(
        payloads,
        max_topics=max_topics,
        max_titles=max_titles,
    )
