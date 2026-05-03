"""Generate a ContentContract from a research payload via Opus 4.7 + extended thinking.

This is the highest-leverage call in the pipeline. The Contract is the
immutable per-article spec consumed by the section writer, voice pass, and
enforcer. Quality of the Contract directly determines quality of the article.

Cost target per call: ~$0.30 (Opus + thinking budget).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Callable, Dict, List, Optional

import requests

from .contract import ContentContract
from .llm import LLMError, _extract_json
from .prompt_registry import Prompt, load as load_prompt
from .research import ResearchPayload

logger = logging.getLogger("creator.contract_generator")

DEFAULT_OPUS_MODEL = "claude-opus-4-7"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_MAX_TOKENS = 8000
DEFAULT_THINKING_BUDGET_TOKENS = 4000
PROMPT_NAME = "contract_generator"

# Number of items we surface to the LLM. More than this is mostly noise.
MAX_ORGANIC_RESULTS = 5
MAX_PAA_QUESTIONS = 8
MAX_RELATED_SEARCHES = 10
MAX_RELATED_KEYWORDS = 12
MAX_COMMON_H2 = 12
MAX_HIGH_COVERAGE_ENTITIES = 25


# ---- prompt assembly --------------------------------------------------------


def _format_organics(payload: ResearchPayload) -> str:
    if not payload.organic:
        return "(keine organischen Ergebnisse verfügbar)"
    lines: List[str] = []
    for result in payload.organic[:MAX_ORGANIC_RESULTS]:
        lines.append(
            f"  #{result.rank}  {result.domain}\n    Titel: {result.title}\n    URL: {result.url}"
        )
    return "\n".join(lines)


def _format_simple_list(items: List[str], limit: int) -> str:
    cleaned = [item.strip() for item in items if item and item.strip()]
    if not cleaned:
        return "(keine)"
    return "\n".join(f"  - {item}" for item in cleaned[:limit])


def _format_related_keywords(payload: ResearchPayload) -> str:
    if not payload.related_keywords:
        return "(keine)"
    lines: List[str] = []
    for related in payload.related_keywords[:MAX_RELATED_KEYWORDS]:
        volume = related.search_volume if related.search_volume is not None else "?"
        lines.append(f"  - {related.keyword} (Volumen: {volume})")
    return "\n".join(lines)


def _format_entities(payload: ResearchPayload) -> str:
    if not payload.high_coverage_entities:
        return "(keine Pflicht-Entitäten — Wettbewerber-Forschung lieferte zu wenig Signal)"
    lines: List[str] = []
    for entity in payload.high_coverage_entities[:MAX_HIGH_COVERAGE_ENTITIES]:
        lines.append(
            f"  - {entity.name} [{entity.type}]  (in {entity.n_competitors} von {payload.successful_competitor_count} Wettbewerbern, "
            f"coverage={entity.coverage:.0%})"
        )
    return "\n".join(lines)


def _format_volume(payload: ResearchPayload) -> str:
    metric = payload.primary_volume
    if metric is None:
        return "(keine Volumen-Daten)"
    volume = metric.search_volume if metric.search_volume is not None else "?"
    competition = f"{metric.competition:.2f}" if metric.competition is not None else "?"
    cpc = f"{metric.cpc:.2f}€" if metric.cpc is not None else "?"
    return f"Volumen: {volume} / Wettbewerb: {competition} / CPC: {cpc}"


def build_user_prompt(
    payload: ResearchPayload,
    *,
    target_backlink_url: str,
    anchor_hint: Optional[str] = None,
) -> str:
    median = payload.competitor_word_count_median or "(unbekannt)"
    return (
        f"ZIEL-KEYWORD: {payload.target_keyword}\n"
        f"ZIEL-URL für Backlink: {target_backlink_url}\n"
        f"GEWÜNSCHTER ANKER-HINWEIS: {anchor_hint or '(frei wählbar)'}\n\n"
        f"WETTBEWERBER-FORSCHUNG\n"
        f"======================\n\n"
        f"Top-{MAX_ORGANIC_RESULTS} organische Ergebnisse (Standort: Deutschland, Sprache: Deutsch):\n"
        f"{_format_organics(payload)}\n\n"
        f"People-Also-Ask Fragen:\n"
        f"{_format_simple_list(payload.paa_questions, MAX_PAA_QUESTIONS)}\n\n"
        f"Verwandte Suchen:\n"
        f"{_format_simple_list(payload.related_searches, MAX_RELATED_SEARCHES)}\n\n"
        f"Keyword-Metriken (primär):\n"
        f"  {_format_volume(payload)}\n\n"
        f"Verwandte Keywords (mit Volumen):\n"
        f"{_format_related_keywords(payload)}\n\n"
        f"Wettbewerber-Wortzahl Median: {median} Wörter\n\n"
        f"Häufige H2-Themen (in 2+ Wettbewerbern):\n"
        f"{_format_simple_list(payload.common_h2_themes, MAX_COMMON_H2)}\n\n"
        f"Pflicht-Entitäten (in mind. 60% der Top-5 Wettbewerber):\n"
        f"{_format_entities(payload)}\n\n"
        f"Erstelle jetzt den ContentContract als JSON gemäß dem Schema im System-Prompt."
    )


def build_system_prompt(prompt: Prompt) -> str:
    schema_json = json.dumps(
        ContentContract.model_json_schema(),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    return f"{prompt.body}\n\n# JSON-Schema\n\n```json\n{schema_json}\n```\n"


# ---- Opus call with extended thinking --------------------------------------


def call_opus_with_thinking(
    *,
    system_prompt: str,
    user_prompt: str,
    api_key: str,
    model: str = DEFAULT_OPUS_MODEL,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    thinking_budget_tokens: int = DEFAULT_THINKING_BUDGET_TOKENS,
    request_label: str = "contract_generator",
) -> str:
    """Issue a single Opus request with extended thinking enabled.

    Returns the concatenated text-block content (skips ``thinking`` blocks).
    Anthropic requires temperature=1.0 when thinking is enabled.
    """

    url = base_url.rstrip("/") + "/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": 1.0,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_prompt}],
        "thinking": {"type": "enabled", "budget_tokens": thinking_budget_tokens},
    }
    try:
        response = requests.post(url, headers=headers, json=body, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise LLMError(f"Opus thinking request failed: {exc}") from exc
    if response.status_code >= 400:
        raise LLMError(f"Opus thinking HTTP {response.status_code}: {response.text[:400]}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise LLMError("Opus thinking response was not JSON.") from exc
    blocks = payload.get("content") or []
    text_chunks: List[str] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            text = block.get("text")
            if isinstance(text, str) and text.strip():
                text_chunks.append(text.strip())
    usage = payload.get("usage") or {}
    logger.info(
        "creator.contract_generator.usage label=%s input=%s output=%s",
        request_label,
        usage.get("input_tokens"),
        usage.get("output_tokens"),
    )
    if not text_chunks:
        raise LLMError("Opus thinking response missing text content.")
    return "\n".join(text_chunks)


# ---- public API -------------------------------------------------------------


def generate_contract(
    research: ResearchPayload,
    *,
    target_backlink_url: str,
    anchor_hint: Optional[str] = None,
    api_key: Optional[str] = None,
    model: str = DEFAULT_OPUS_MODEL,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    thinking_budget_tokens: int = DEFAULT_THINKING_BUDGET_TOKENS,
    prompt_version: Optional[str] = None,
    llm_caller: Optional[Callable[..., str]] = None,
) -> ContentContract:
    """Generate the ContentContract for a single article.

    ``llm_caller`` is injected for tests; in production it defaults to the
    extended-thinking Opus call above.
    """

    resolved_api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not resolved_api_key and llm_caller is None:
        raise LLMError("Missing ANTHROPIC_API_KEY for contract generation.")
    if not target_backlink_url.strip():
        raise ValueError("target_backlink_url is required.")

    prompt = load_prompt(PROMPT_NAME, prompt_version)
    system_prompt = build_system_prompt(prompt)
    user_prompt = build_user_prompt(
        research,
        target_backlink_url=target_backlink_url,
        anchor_hint=anchor_hint,
    )

    caller = llm_caller or call_opus_with_thinking
    raw_text = caller(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=resolved_api_key,
        model=model,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        max_tokens=max_tokens,
        thinking_budget_tokens=thinking_budget_tokens,
        request_label=f"contract_generator/{prompt.version}",
    )

    try:
        payload: Dict[str, object] = _extract_json(raw_text)
    except LLMError as exc:
        raise LLMError(f"Contract generator returned non-JSON: {exc}") from exc

    try:
        return ContentContract.model_validate(payload)
    except Exception as exc:
        raise LLMError(f"Contract generator output failed schema validation: {exc}") from exc
