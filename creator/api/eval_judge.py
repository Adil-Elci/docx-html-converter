"""Single Haiku call producing the three LLM-judged eval axes.

Bundles intent_match / backlink_anchor_naturalness / eeat_signal_density into
one request to keep latency and cost low. Output is structured JSON with
integer scores 0-10 and one-sentence German rationales.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Callable, List, Optional

from .contract import ContentContract
from .llm import LLMError, call_llm_json
from .prompt_registry import load as load_prompt
from .research import ResearchPayload

logger = logging.getLogger("creator.eval_judge")

DEFAULT_HAIKU_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_TOKENS = 1200
DEFAULT_TEMPERATURE = 0.0
DEFAULT_ARTICLE_CHARS = 8000
PROMPT_NAME = "eval_judge"

INTENT_MATCH_MIN_SCORE = 7
BACKLINK_NATURALNESS_MIN_SCORE = 7
EEAT_DENSITY_MIN_SCORE = 6


@dataclass
class JudgeAxisResult:
    score: int
    rationale: str
    threshold: int

    @property
    def passed(self) -> bool:
        return self.score >= self.threshold


@dataclass
class JudgeScores:
    intent_match: JudgeAxisResult
    backlink_anchor_naturalness: JudgeAxisResult
    eeat_signal_density: JudgeAxisResult


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rsplit(" ", 1)[0] + " ..."


def _format_link_plan(contract: ContentContract) -> str:
    if not contract.link_plan:
        return "(kein Backlink im Vertrag definiert)"
    target = contract.link_plan[0]
    return (
        f"Ziel-URL: {target.target_url}\n"
        f"Anker-Strategie (gewünscht): {target.anchor_strategy}\n"
        f"Sektion-Index: {target.section_index}\n"
        f"Kontext-Anforderung: {target.surrounding_context_requirements}"
    )


def _format_competitors(research: Optional[ResearchPayload], limit: int = 3) -> str:
    if research is None or not research.organic:
        return "(keine Wettbewerber-Daten verfügbar)"
    lines: List[str] = []
    for organic in research.organic[:limit]:
        lines.append(f"  #{organic.rank}  {organic.title}  ({organic.domain})")
    return "\n".join(lines)


def build_user_prompt(
    *,
    article_html: str,
    contract: ContentContract,
    research: Optional[ResearchPayload] = None,
    article_max_chars: int = DEFAULT_ARTICLE_CHARS,
) -> str:
    article_excerpt = _truncate(article_html, article_max_chars)
    return (
        f"VERTRAG (was geliefert werden sollte)\n"
        f"=====================================\n"
        f"target_keyword: {contract.target_keyword}\n"
        f"intent: {contract.intent.value}\n"
        f"audience: {contract.target_audience}\n\n"
        f"BACKLINK\n"
        f"========\n"
        f"{_format_link_plan(contract)}\n\n"
        f"TOP-WETTBEWERBER (für Intent-Vergleich)\n"
        f"=======================================\n"
        f"{_format_competitors(research)}\n\n"
        f"ARTIKEL (HTML)\n"
        f"==============\n"
        f"{article_excerpt}\n\n"
        f"Bewerte den Artikel auf den drei Achsen und antworte mit dem geforderten JSON."
    )


def _coerce_score(raw: object) -> int:
    if isinstance(raw, bool):
        return 0
    if isinstance(raw, int):
        return max(0, min(10, raw))
    if isinstance(raw, float):
        return max(0, min(10, int(round(raw))))
    if isinstance(raw, str) and raw.strip().lstrip("-").isdigit():
        return max(0, min(10, int(raw.strip())))
    return 0


def _coerce_rationale(raw: object) -> str:
    return str(raw).strip() if raw is not None else ""


def judge_article(
    *,
    article_html: str,
    contract: ContentContract,
    research: Optional[ResearchPayload] = None,
    api_key: Optional[str] = None,
    model: str = DEFAULT_HAIKU_MODEL,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
    prompt_version: Optional[str] = None,
    llm_caller: Optional[Callable[..., dict]] = None,
) -> JudgeScores:
    """Run the three-axis judge against a finished article.

    ``llm_caller`` is injected for tests; it must return a parsed dict
    (matching ``call_llm_json`` semantics).
    """

    resolved_api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not resolved_api_key and llm_caller is None:
        raise LLMError("Missing ANTHROPIC_API_KEY for eval judge.")

    prompt = load_prompt(PROMPT_NAME, prompt_version)
    system_prompt = prompt.body
    user_prompt = build_user_prompt(
        article_html=article_html,
        contract=contract,
        research=research,
    )

    if llm_caller is None:
        def _default(**kwargs):
            return call_llm_json(**kwargs)
        llm_caller = _default

    payload = llm_caller(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=resolved_api_key,
        base_url=base_url,
        model=model,
        timeout_seconds=timeout_seconds,
        max_tokens=max_tokens,
        temperature=temperature,
        request_label=f"eval_judge/{prompt.version}",
    )

    if not isinstance(payload, dict):
        raise LLMError("Eval judge returned non-dict response.")

    return JudgeScores(
        intent_match=JudgeAxisResult(
            score=_coerce_score(payload.get("intent_match")),
            rationale=_coerce_rationale(payload.get("intent_match_rationale")),
            threshold=INTENT_MATCH_MIN_SCORE,
        ),
        backlink_anchor_naturalness=JudgeAxisResult(
            score=_coerce_score(payload.get("backlink_anchor_naturalness")),
            rationale=_coerce_rationale(payload.get("backlink_anchor_naturalness_rationale")),
            threshold=BACKLINK_NATURALNESS_MIN_SCORE,
        ),
        eeat_signal_density=JudgeAxisResult(
            score=_coerce_score(payload.get("eeat_signal_density")),
            rationale=_coerce_rationale(payload.get("eeat_signal_density_rationale")),
            threshold=EEAT_DENSITY_MIN_SCORE,
        ),
    )
