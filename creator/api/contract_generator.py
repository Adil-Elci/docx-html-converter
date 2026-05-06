"""Generate a ContentContract from a research payload via Sonnet 4.6 + extended thinking.

This is the highest-leverage call in the pipeline. The Contract is the
immutable per-article spec consumed by the section writer, voice pass, and
enforcer. Quality of the Contract directly determines quality of the article.

Default model: Sonnet 4.6 with extended thinking (~$0.06/contract). Override
to Opus 4.7 (~$0.30/contract) by setting CREATOR_CONTRACT_MODEL or passing
``model=`` if quality on a tough keyword falls short.
"""

from __future__ import annotations

import html
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import requests

from .contract import ContentContract
from .llm import LLMError, _extract_json
from .prompt_registry import Prompt, load as load_prompt
from .research import ResearchPayload

logger = logging.getLogger("creator.contract_generator")

DEFAULT_CONTRACT_MODEL = "claude-sonnet-4-6"
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


def _format_numeric(value: object, *, fmt: str = ".2f", suffix: str = "") -> str:
    """Format numeric values with the given spec, but pass through strings as-is.

    DataForSEO sometimes returns ``competition`` as a string label
    ("LOW" / "MEDIUM" / "HIGH") instead of a 0-1 float, and an upstream
    ``f"{x:.2f}"`` blows up with ``ValueError: Unknown format code 'f'``.
    This helper preserves whatever shape the API returned without crashing.
    """

    if value is None:
        return "?"
    if isinstance(value, bool):
        return f"{value}{suffix}"
    if isinstance(value, (int, float)):
        return f"{value:{fmt}}{suffix}"
    return f"{value}{suffix}"


def _format_volume(payload: ResearchPayload) -> str:
    metric = payload.primary_volume
    if metric is None:
        return "(keine Volumen-Daten)"
    volume = metric.search_volume if metric.search_volume is not None else "?"
    competition = _format_numeric(metric.competition)
    cpc = _format_numeric(metric.cpc, suffix="€")
    return f"Volumen: {volume} / Wettbewerb: {competition} / CPC: {cpc}"


_LANGUAGE_USER_PROMPT_TEMPLATES: Dict[str, Dict[str, str]] = {
    "de": {
        "target_keyword_label": "ZIEL-KEYWORD",
        "backlink_label": "ZIEL-URL für Backlink",
        "anchor_label": "GEWÜNSCHTER ANKER-HINWEIS",
        "anchor_default": "(frei wählbar)",
        "current_year_label": "AKTUELLES JAHR",
        "research_header": "WETTBEWERBER-FORSCHUNG",
        "organic_header_template": "Top-{n} organische Ergebnisse (Standort/Sprache aus Locale: {locale_label}):",
        "paa_header": "People-Also-Ask Fragen:",
        "related_searches_header": "Verwandte Suchen:",
        "metrics_header": "Keyword-Metriken (primär):",
        "related_keywords_header": "Verwandte Keywords (mit Volumen):",
        "median_label": "Wettbewerber-Wortzahl Median",
        "median_words": "Wörter",
        "median_unknown": "(unbekannt)",
        "common_h2_header": "Häufige H2-Themen (in 2+ Wettbewerbern):",
        "entities_header": "Pflicht-Entitäten (in mind. 60% der Top-5 Wettbewerber):",
        "language_directive": "PFLICHT: Verfasse den gesamten Vertrag in deutscher Sprache. Setze `language` auf `\"de\"` und `tone` auf `\"sie\"`.",
        "tail": "Erstelle jetzt den ContentContract als JSON gemäß dem Schema im System-Prompt.",
    },
    "fr": {
        "target_keyword_label": "MOT-CLÉ CIBLE",
        "backlink_label": "URL CIBLE du backlink",
        "anchor_label": "SUGGESTION D'ANCRE",
        "anchor_default": "(libre)",
        "current_year_label": "ANNÉE EN COURS",
        "research_header": "RECHERCHE CONCURRENTIELLE",
        "organic_header_template": "Top-{n} résultats organiques (localisation/langue d'après le locale : {locale_label}) :",
        "paa_header": "Questions People-Also-Ask :",
        "related_searches_header": "Recherches associées :",
        "metrics_header": "Métriques mot-clé (principal) :",
        "related_keywords_header": "Mots-clés associés (avec volume) :",
        "median_label": "Médiane du nombre de mots des concurrents",
        "median_words": "mots",
        "median_unknown": "(inconnu)",
        "common_h2_header": "Thèmes H2 fréquents (chez 2+ concurrents) :",
        "entities_header": "Entités obligatoires (présentes chez ≥60 % du Top-5 concurrents) :",
        "language_directive": "OBLIGATOIRE : rédigez l'intégralité du contrat en français. Mettez `language` à `\"fr\"` et `tone` à `\"sie\"` (vouvoiement).",
        "tail": "Produisez maintenant le ContentContract au format JSON conforme au schéma du prompt système.",
    },
}


def _user_prompt_template(language: str) -> Dict[str, str]:
    return _LANGUAGE_USER_PROMPT_TEMPLATES.get(language.lower(), _LANGUAGE_USER_PROMPT_TEMPLATES["de"])


def _format_editorial_angle(angle: Optional[Dict[str, Any]], language: str) -> str:
    """Format an editorial angle block for the user prompt, if provided.

    The angle is a brainstormed slant (title + hook + rationale) the contract
    LLM should build the article around. When absent, returns "" so the
    block is skipped entirely.
    """

    if not isinstance(angle, dict):
        return ""
    title = str(angle.get("title") or "").strip()
    if not title:
        return ""
    hook = str(angle.get("hook") or "").strip()
    rationale = str(angle.get("rationale") or "").strip()
    if (language or "de").lower() == "fr":
        lines = [
            "ANGLE ÉDITORIAL (à utiliser comme cadre de l'article)",
            "=====================================================",
            f"- Titre proposé : {title}",
        ]
        if hook:
            lines.append(f"- Accroche : {hook}")
        if rationale:
            lines.append(f"- Pourquoi cet angle : {rationale}")
        lines.append(
            "Le H1 doit être ce titre (ou une variante très proche). Les sections doivent suivre cet "
            "angle journalistique, pas un cadre comparatif/tarifaire générique."
        )
        return "\n".join(lines) + "\n\n"
    lines = [
        "EDITORIAL ANGLE (als Rahmen für den Artikel verwenden)",
        "======================================================",
        f"- Vorgeschlagener Titel: {title}",
    ]
    if hook:
        lines.append(f"- Hook: {hook}")
    if rationale:
        lines.append(f"- Warum dieser Angle: {rationale}")
    lines.append(
        "Der H1 MUSS dieser Titel sein (oder eine sehr nahe Variante). Die Sektionen müssen dem "
        "redaktionellen Angle folgen, nicht einem generischen Vergleichs-/Preisrahmen."
    )
    return "\n".join(lines) + "\n\n"


def build_user_prompt(
    payload: ResearchPayload,
    *,
    target_backlink_url: str,
    anchor_hint: Optional[str] = None,
    language: str = "de",
    current_year: Optional[int] = None,
    editorial_angle: Optional[Dict[str, Any]] = None,
) -> str:
    t = _user_prompt_template(language)
    median = payload.competitor_word_count_median or t["median_unknown"]
    locale_label = f"location_code={payload.location_code}, language_code={payload.language_code}"
    year = int(current_year) if current_year is not None else datetime.now(timezone.utc).year
    angle_block = _format_editorial_angle(editorial_angle, language)
    return (
        f"{t['target_keyword_label']}: {payload.target_keyword}\n"
        f"{t['backlink_label']}: {target_backlink_url}\n"
        f"{t['anchor_label']}: {anchor_hint or t['anchor_default']}\n"
        f"{t['current_year_label']}: {year}\n\n"
        f"{angle_block}"
        f"{t['language_directive']}\n\n"
        f"{t['research_header']}\n"
        f"{'=' * len(t['research_header'])}\n\n"
        f"{t['organic_header_template'].format(n=MAX_ORGANIC_RESULTS, locale_label=locale_label)}\n"
        f"{_format_organics(payload)}\n\n"
        f"{t['paa_header']}\n"
        f"{_format_simple_list(payload.paa_questions, MAX_PAA_QUESTIONS)}\n\n"
        f"{t['related_searches_header']}\n"
        f"{_format_simple_list(payload.related_searches, MAX_RELATED_SEARCHES)}\n\n"
        f"{t['metrics_header']}\n"
        f"  {_format_volume(payload)}\n\n"
        f"{t['related_keywords_header']}\n"
        f"{_format_related_keywords(payload)}\n\n"
        f"{t['median_label']}: {median} {t['median_words']}\n\n"
        f"{t['common_h2_header']}\n"
        f"{_format_simple_list(payload.common_h2_themes, MAX_COMMON_H2)}\n\n"
        f"{t['entities_header']}\n"
        f"{_format_entities(payload)}\n\n"
        f"{t['tail']}"
    )


def build_system_prompt(prompt: Prompt) -> str:
    schema_json = json.dumps(
        ContentContract.model_json_schema(),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    return f"{prompt.body}\n\n# JSON-Schema\n\n```json\n{schema_json}\n```\n"


# ---- contract LLM call with extended thinking ------------------------------


# Transient HTTP codes worth retrying on the contract endpoint. 529 is
# Anthropic's "overloaded" status — equivalent to 503 from a normal API.
# 408 is request timeout, 429 is rate limit; the rest are upstream/server
# blips. 4xx outside this set are real client bugs (bad request, auth) and
# must fail fast.
_CONTRACT_RETRY_HTTP_CODES = frozenset({408, 429, 500, 502, 503, 504, 529})


def call_with_thinking(
    *,
    system_prompt: str,
    user_prompt: str,
    api_key: str,
    model: str = DEFAULT_CONTRACT_MODEL,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    thinking_budget_tokens: int = DEFAULT_THINKING_BUDGET_TOKENS,
    request_label: str = "contract_generator",
    max_attempts: int = 3,
    retry_backoff_seconds: float = 2.0,
) -> str:
    """Issue a single request to a Claude model with extended thinking enabled.

    Returns the concatenated text-block content (skips ``thinking`` blocks).
    Anthropic requires temperature=1.0 when thinking is enabled. Works with
    any Claude 4.x model that supports extended thinking (Sonnet 4.6 by
    default; Opus 4.7 is a drop-in upgrade).

    Retries on transient HTTP failures (408/429/500/502/503/504/529) and
    connection errors with exponential backoff. The contract step runs after
    ~$0.05 of research has already been spent; losing the run to a
    momentary Anthropic overload (HTTP 529) without a retry was the
    failure mode that motivated this loop.
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

    last_error: Optional[LLMError] = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(url, headers=headers, json=body, timeout=timeout_seconds)
        except requests.RequestException as exc:
            last_error = LLMError(f"contract thinking request failed ({model}): {exc}")
            if attempt >= max_attempts:
                raise last_error from exc
            sleep_for = retry_backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "creator.contract_generator.connection_retry label=%s attempt=%s/%s sleep=%.1fs error=%s",
                request_label, attempt, max_attempts, sleep_for, exc,
            )
            time.sleep(sleep_for)
            continue

        if response.status_code in _CONTRACT_RETRY_HTTP_CODES:
            if attempt < max_attempts:
                sleep_for = retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "creator.contract_generator.transient_retry label=%s status=%s attempt=%s/%s sleep=%.1fs",
                    request_label, response.status_code, attempt, max_attempts, sleep_for,
                )
                time.sleep(sleep_for)
                continue
            raise LLMError(
                f"contract thinking HTTP {response.status_code} ({model}) after {max_attempts} attempts: {response.text[:400]}"
            )

        if response.status_code >= 400:
            raise LLMError(
                f"contract thinking HTTP {response.status_code} ({model}): {response.text[:400]}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise LLMError(f"contract thinking response was not JSON ({model}).") from exc
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
            "creator.contract_generator.usage label=%s model=%s input=%s output=%s",
            request_label,
            model,
            usage.get("input_tokens"),
            usage.get("output_tokens"),
        )
        if not text_chunks:
            raise LLMError(f"contract thinking response missing text content ({model}).")
        return "\n".join(text_chunks)

    # Loop only exits via return / raise; this terminator is unreachable in
    # practice but keeps mypy happy and gives a clean message if the logic
    # ever drifts.
    raise LLMError(f"contract thinking ({model}) ended without a response after {max_attempts} attempts.")


# ---- public API -------------------------------------------------------------


def generate_contract(
    research: ResearchPayload,
    *,
    target_backlink_url: str,
    anchor_hint: Optional[str] = None,
    language: str = "de",
    editorial_angle: Optional[Dict[str, Any]] = None,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    thinking_budget_tokens: int = DEFAULT_THINKING_BUDGET_TOKENS,
    prompt_version: Optional[str] = None,
    llm_caller: Optional[Callable[..., str]] = None,
) -> ContentContract:
    """Generate the ContentContract for a single article.

    Default model is Sonnet 4.6 (DEFAULT_CONTRACT_MODEL). Override via the
    ``model`` argument or the ``CREATOR_CONTRACT_MODEL`` env var. ``llm_caller``
    is injected for tests; in production it defaults to ``call_with_thinking``.

    ``language`` (ISO 639-1) selects the prompt translation (``v1.de.md`` or
    ``v1.fr.md``) and the user-prompt template. The contract returned has
    ``language`` set to match.
    """

    resolved_api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not resolved_api_key and llm_caller is None:
        raise LLMError("Missing ANTHROPIC_API_KEY for contract generation.")
    if not target_backlink_url.strip():
        raise ValueError("target_backlink_url is required.")

    resolved_model = model or os.getenv("CREATOR_CONTRACT_MODEL", "").strip() or DEFAULT_CONTRACT_MODEL
    prompt = load_prompt(PROMPT_NAME, prompt_version, language=language)
    system_prompt = build_system_prompt(prompt)
    user_prompt = build_user_prompt(
        research,
        target_backlink_url=target_backlink_url,
        anchor_hint=anchor_hint,
        language=language,
        editorial_angle=editorial_angle,
    )

    caller = llm_caller or call_with_thinking
    raw_text = caller(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=resolved_api_key,
        model=resolved_model,
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

    # Force the contract's language field to match the requested language. The
    # LLM is instructed to set it correctly via the language_directive in the
    # user prompt, but we don't want a stray drift here to silently route
    # downstream prompts to the wrong locale.
    if isinstance(payload, dict):
        payload["language"] = language
        _heal_html_entities_in_contract_payload(payload)

    try:
        return ContentContract.model_validate(payload)
    except Exception as exc:
        raise LLMError(f"Contract generator output failed schema validation: {exc}") from exc


def _heal_html_entities_in_contract_payload(payload: Dict[str, Any]) -> None:
    """Decode HTML entities the LLM occasionally injects into title-shaped fields.

    Sonnet sometimes emits `&amp;` / `&quot;` etc. inside JSON strings because
    the field will end up in HTML — but the contract stores raw text and the
    assembler escapes it later. Fix in-place so downstream rendering doesn't
    show literal entities like `Kinderbrillen &amp; Jugendbrillen`.
    """

    for key in ("h1", "meta_title", "meta_description"):
        value = payload.get(key)
        if isinstance(value, str):
            payload[key] = html.unescape(value)
    sections = payload.get("sections")
    if isinstance(sections, list):
        for section in sections:
            if isinstance(section, dict) and isinstance(section.get("h2"), str):
                section["h2"] = html.unescape(section["h2"])
    faq_items = payload.get("faq_items")
    if isinstance(faq_items, list):
        for item in faq_items:
            if isinstance(item, dict) and isinstance(item.get("question"), str):
                item["question"] = html.unescape(item["question"])
