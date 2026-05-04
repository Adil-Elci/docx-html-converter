"""Voice & coherence pass — single Sonnet call that refines tone and removes AI tells.

Operates on the assembled article HTML (output of ``article_assembler``). The
system prompt enforces strict preservation rules: headings, links, structural
tags, numbers, and named entities must remain unchanged; only prose inside
``<p>`` tags is editable.

Returns the refined HTML string. By default validates that every ``href``
URL present in the input is still present in the output and raises if any
were dropped — voice-pass losing the backlink would be a silent regression
worth catching loudly.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Callable, List, Optional

from .contract import ContentContract
from .llm import LLMError, call_llm_text
from .prompt_registry import load as load_prompt

logger = logging.getLogger("creator.voice_pass")

DEFAULT_VOICE_MODEL = "claude-sonnet-4-6"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_TIMEOUT_SECONDS = 90
DEFAULT_MAX_TOKENS = 4000
DEFAULT_TEMPERATURE = 0.3
PROMPT_NAME = "voice_pass"


class VoicePassValidationError(LLMError):
    """Raised when the voice-pass output drops links that were in the input."""


# ---- helpers ---------------------------------------------------------------


_HREF_PATTERN = re.compile(r'href="([^"]+)"')
_CODEBLOCK_OPEN = re.compile(r"^```[a-zA-Z]*\s*")
_CODEBLOCK_CLOSE = re.compile(r"\s*```\s*$")


def _extract_urls(html: str) -> List[str]:
    return _HREF_PATTERN.findall(html or "")


def _strip_codeblock_wrapper(text: str) -> str:
    """Some Sonnet runs ignore the no-codeblock instruction. Strip if present."""

    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = _CODEBLOCK_OPEN.sub("", cleaned)
        cleaned = _CODEBLOCK_CLOSE.sub("", cleaned)
    return cleaned.strip()


def _format_blocklist(blocklist: List[str]) -> str:
    if not blocklist:
        return "(keine zusätzlichen vertragsspezifischen Floskeln)"
    return "\n".join(f"  - {phrase}" for phrase in blocklist)


def build_user_prompt(*, article_html: str, contract: ContentContract) -> str:
    return (
        f"KONTEXT (für stilistische Konsistenz)\n"
        f"=====================================\n"
        f"target_keyword : {contract.target_keyword}\n"
        f"intent         : {contract.intent.value}\n"
        f"audience       : {contract.target_audience}\n"
        f"tone           : Sie\n\n"
        f"AI-FLOSKEL-BLOCKLISTE (vertragsspezifisch — sofort ersetzen falls vorhanden)\n"
        f"============================================================================\n"
        f"{_format_blocklist(contract.ai_tell_blocklist)}\n\n"
        f"ZU ÜBERARBEITENDER ARTIKEL\n"
        f"==========================\n"
        f"{article_html}"
    )


# ---- public API -----------------------------------------------------------


def refine_voice(
    *,
    article_html: str,
    contract: ContentContract,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
    prompt_version: Optional[str] = None,
    llm_caller: Optional[Callable[..., str]] = None,
    validate_links: bool = True,
) -> str:
    """Run a Sonnet voice pass over the assembled article.

    Returns refined HTML. Raises ``VoicePassValidationError`` if any href URL
    from the input is missing from the output. Pass ``validate_links=False``
    to skip the check (e.g. when intentionally rewriting link structure).
    """

    if not article_html.strip():
        raise ValueError("article_html is empty.")

    resolved_api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not resolved_api_key and llm_caller is None:
        raise LLMError("Missing ANTHROPIC_API_KEY for voice pass.")
    resolved_model = model or os.getenv("CREATOR_VOICE_MODEL", "").strip() or DEFAULT_VOICE_MODEL

    prompt = load_prompt(PROMPT_NAME, prompt_version)
    system_prompt = prompt.body
    user_prompt = build_user_prompt(article_html=article_html, contract=contract)

    if llm_caller is None:
        def _default(**kwargs):
            return call_llm_text(**kwargs)
        llm_caller = _default

    refined_raw = llm_caller(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=resolved_api_key,
        base_url=base_url,
        model=resolved_model,
        timeout_seconds=timeout_seconds,
        max_tokens=max_tokens,
        temperature=temperature,
        request_label=f"voice_pass/{prompt.version}",
        cache_system=True,
    )

    if not isinstance(refined_raw, str) or not refined_raw.strip():
        raise LLMError("Voice pass returned empty content.")

    refined = _strip_codeblock_wrapper(refined_raw)

    if validate_links:
        original_urls = _extract_urls(article_html)
        refined_urls = set(_extract_urls(refined))
        missing = [url for url in original_urls if url not in refined_urls]
        if missing:
            raise VoicePassValidationError(
                f"Voice pass dropped {len(missing)} URL(s) from the article: {missing}"
            )

    return refined
