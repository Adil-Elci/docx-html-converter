"""Extract topical entities from competitor article bodies.

A single Claude Haiku call reads the concatenated competitor texts and returns
the entities (people, organizations, laws, places, concepts, products) that
multiple competitors mention. Entities present in the majority of competitors
signal what an authoritative article on the topic must cover; the eval
harness uses these for the topical-coverage score.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from .llm import LLMError, call_llm_json
from .serp_scrape import ScrapedCompetitor

logger = logging.getLogger("creator.entity_extract")

DEFAULT_HAIKU_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_MAX_BODY_CHARS_PER_COMPETITOR = 3500
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_TOKENS = 1500
DEFAULT_TEMPERATURE = 0.0
ENTITY_LIMIT = 40

VALID_ENTITY_TYPES = {
    "PERSON",
    "ORGANIZATION",
    "LAW",
    "PLACE",
    "PRODUCT",
    "CONCEPT",
    "STATISTIC",
    "DATE",
}


@dataclass
class ExtractedEntity:
    name: str
    type: str
    n_competitors: int
    coverage: float = 0.0  # n_competitors / total_competitors


@dataclass
class EntityExtractionResult:
    entities: List[ExtractedEntity] = field(default_factory=list)
    competitor_count: int = 0
    raw_response: Optional[Dict[str, Any]] = None


SYSTEM_PROMPT = (
    "Du bist ein SEO-Analyst für deutsche Inhalte. Du extrahierst Entitäten und Schlüsselkonzepte "
    "aus mehreren Wettbewerber-Artikeln zum gleichen Thema. Antworte ausschließlich mit gültigem JSON, "
    "ohne Markdown-Codeblöcke und ohne erklärenden Vor- oder Nachtext."
)


def _build_user_prompt(topic: str, bodies: List[str]) -> str:
    sections = [f"[Wettbewerber {i + 1}]\n{body.strip()}" for i, body in enumerate(bodies)]
    competitor_block = "\n\n".join(sections)
    return (
        f"THEMA: {topic}\n\n"
        f"Anzahl Wettbewerber-Artikel: {len(bodies)}\n\n"
        "Identifiziere die wichtigsten Entitäten und Fachbegriffe, die ein vollständiger Artikel zu diesem "
        "Thema abdecken sollte. Konzentriere dich auf:\n"
        "- Personen (PERSON)\n"
        "- Organisationen / Behörden / Unternehmen (ORGANIZATION)\n"
        "- Gesetze und Verordnungen (LAW)\n"
        "- Orte und geografische Begriffe (PLACE)\n"
        "- Produkte und Dienstleistungen (PRODUCT)\n"
        "- Fachbegriffe und Konzepte (CONCEPT)\n"
        "- Konkrete Statistiken und Zahlenangaben (STATISTIC)\n"
        "- Wichtige Daten / Jahreszahlen (DATE)\n\n"
        "Wichtige Regeln:\n"
        "- Schließe NUR Entitäten ein, die in mindestens 2 von den oben aufgeführten Wettbewerbern vorkommen.\n"
        "- Schließe KEINE generischen Begriffe ein (z.B. 'Person', 'Unternehmen', 'Sache').\n"
        "- Verwende exakt die deutschen Schreibweisen, die in den Texten vorkommen.\n"
        "- Wenn ein Begriff in mehreren Schreibweisen vorkommt, gib die häufigste Variante zurück.\n"
        "- Maximal 40 Entitäten, sortiert nach n_competitors absteigend.\n\n"
        "Antworte exakt in diesem JSON-Format:\n"
        "{\n"
        '  "entities": [\n'
        '    {"name": "...", "type": "...", "n_competitors": <int>}\n'
        "  ]\n"
        "}\n\n"
        "WETTBEWERBER:\n"
        f"{competitor_block}\n"
    )


def _truncate_body(body: str, max_chars: int) -> str:
    if len(body) <= max_chars:
        return body
    return body[:max_chars].rsplit(" ", 1)[0]


def _validate_entity(payload: Dict[str, Any], total_competitors: int) -> Optional[ExtractedEntity]:
    name = str(payload.get("name") or "").strip()
    type_raw = str(payload.get("type") or "").strip().upper()
    if not name or len(name) < 2:
        return None
    if type_raw not in VALID_ENTITY_TYPES:
        # Coerce unknown types to CONCEPT rather than dropping; the LLM occasionally invents a type.
        type_raw = "CONCEPT"
    try:
        n_competitors = int(payload.get("n_competitors") or 0)
    except (TypeError, ValueError):
        n_competitors = 0
    if n_competitors < 2:
        return None
    n_competitors = min(n_competitors, total_competitors) if total_competitors > 0 else n_competitors
    coverage = (n_competitors / total_competitors) if total_competitors > 0 else 0.0
    return ExtractedEntity(name=name, type=type_raw, n_competitors=n_competitors, coverage=coverage)


def _verify_with_regex(entity: ExtractedEntity, bodies: List[str]) -> ExtractedEntity:
    """Cross-check the LLM's claimed competitor count against actual text matches.

    The LLM is mostly accurate but occasionally over-reports. If the regex finds
    fewer competitors than claimed, we trust the regex and adjust.
    """

    pattern = re.compile(rf"\b{re.escape(entity.name)}\b", re.IGNORECASE)
    actual = sum(1 for body in bodies if pattern.search(body))
    if actual == 0:
        # Hallucination — drop entirely. Caller filters None out.
        return ExtractedEntity(name=entity.name, type=entity.type, n_competitors=0, coverage=0.0)
    n_competitors = min(entity.n_competitors, actual)
    total = len(bodies)
    coverage = (n_competitors / total) if total > 0 else 0.0
    return ExtractedEntity(name=entity.name, type=entity.type, n_competitors=n_competitors, coverage=coverage)


def extract_entities_from_competitors(
    competitors: Sequence[ScrapedCompetitor],
    *,
    topic: str,
    api_key: Optional[str] = None,
    model: str = DEFAULT_HAIKU_MODEL,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_body_chars_per_competitor: int = DEFAULT_MAX_BODY_CHARS_PER_COMPETITOR,
) -> EntityExtractionResult:
    usable = [c for c in competitors if c.fetch_status == "ok" and c.body_text.strip()]
    if len(usable) < 2:
        logger.info("entity_extract.too_few_competitors usable=%s", len(usable))
        return EntityExtractionResult(entities=[], competitor_count=len(usable))

    bodies = [_truncate_body(c.body_text, max_body_chars_per_competitor) for c in usable]
    user_prompt = _build_user_prompt(topic=topic, bodies=bodies)

    resolved_api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not resolved_api_key:
        raise LLMError("Missing ANTHROPIC_API_KEY for entity extraction.")

    try:
        payload = call_llm_json(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            api_key=resolved_api_key,
            base_url=base_url,
            model=model,
            timeout_seconds=timeout_seconds,
            max_tokens=DEFAULT_MAX_TOKENS,
            temperature=DEFAULT_TEMPERATURE,
            request_label="entity_extraction",
        )
    except LLMError:
        raise

    raw_entities = payload.get("entities") if isinstance(payload, dict) else None
    if not isinstance(raw_entities, list):
        logger.warning("entity_extract.malformed_response missing_entities_list")
        return EntityExtractionResult(entities=[], competitor_count=len(usable), raw_response=payload)

    seen_names: set[str] = set()
    extracted: List[ExtractedEntity] = []
    for raw in raw_entities[:ENTITY_LIMIT * 2]:
        if not isinstance(raw, dict):
            continue
        candidate = _validate_entity(raw, total_competitors=len(usable))
        if candidate is None:
            continue
        normalized = candidate.name.lower()
        if normalized in seen_names:
            continue
        seen_names.add(normalized)
        verified = _verify_with_regex(candidate, bodies)
        if verified.n_competitors >= 2:
            extracted.append(verified)

    extracted.sort(key=lambda e: (-e.n_competitors, e.name.lower()))
    return EntityExtractionResult(
        entities=extracted[:ENTITY_LIMIT],
        competitor_count=len(usable),
        raw_response=payload,
    )
