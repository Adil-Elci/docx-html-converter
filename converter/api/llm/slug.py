from __future__ import annotations

import logging
import os
import re
import unicodedata
from typing import Optional

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

try:
    from anthropic import Anthropic
except Exception:  # pragma: no cover
    Anthropic = None

logger = logging.getLogger("doc_converter.llm")

MODEL_NAME = "claude-sonnet-4-5-20250929"
TIMEOUT_SECONDS = 10
MAX_TOKENS = 60
TEMPERATURE = 0.2

SYSTEM_PROMPT = (
    "You generate a single SEO-friendly URL slug in the same language as the input. "
    "Output only the slug with no quotes or markdown. Rules: 3 to 6 words, lowercase, "
    "words separated by single hyphens, no numbers, no special characters, no accents "
    "(use ae/oe/ue/ss for German). Avoid duplicate words. Keep it as short as possible "
    "while meaningful. Prefer the primary keyword first. Avoid filler/stop words. "
    "Use only words that appear in the title and keep their original order. You may skip "
    "non-essential function words. If the title is a question, preserve the core question "
    "structure and include the leading auxiliary verb (e.g., ist/sind/kann/darf/muss/soll) "
    "when it carries meaning. Drop optional prepositional phrases unless they are crucial."
)


def generate_slug(
    title: str,
    *,
    min_words: int,
    max_words: int,
    max_length: int,
) -> str:
    if load_dotenv:
        loaded = load_dotenv(override=True)
        logger.info("dotenv_loaded=%s cwd=%s", bool(loaded), os.getcwd())
    else:
        logger.warning("python-dotenv is not installed; .env will not be loaded")

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY is missing")
        logger.info("slug_fallback reason=missing_api_key")
        return ""

    if Anthropic is None:
        logger.error("anthropic SDK is not available")
        logger.info("slug_fallback reason=missing_anthropic_sdk")
        return ""

    for attempt in range(1, 3):
        logger.info(
            "slug_attempt attempt=%s model=%s api_key_present=%s",
            attempt,
            MODEL_NAME,
            True,
        )
        try:
            result = _call_anthropic(api_key, title)
            logger.info(
                "slug_response received=%s length=%s",
                bool(result),
                len(result) if result else 0,
            )
            normalized = enforce_slug_constraints(result, min_words, max_words, max_length)
            if normalized:
                logger.info("slug_success attempt=%s", attempt)
                return normalized
            logger.info("slug_invalid attempt=%s", attempt)
        except Exception as exc:
            logger.error(
                "slug_error attempt=%s error_type=%s error=%s",
                attempt,
                type(exc).__name__,
                exc,
            )
            continue

    logger.info("slug_fallback reason=all_attempts_failed")
    return ""


def _call_anthropic(api_key: str, title: str) -> str:
    client = Anthropic(api_key=api_key, timeout=TIMEOUT_SECONDS)
    user_message = f"Title: {title}\n"

    response = client.messages.create(
        model=MODEL_NAME,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
    )

    content: Optional[str] = None
    blocks = getattr(response, "content", None) or []
    texts = []
    for block in blocks:
        block_type = getattr(block, "type", None)
        if block_type is None and isinstance(block, dict):
            block_type = block.get("type")
        if block_type != "text":
            continue
        text = getattr(block, "text", None)
        if text is None and isinstance(block, dict):
            text = block.get("text")
        if text:
            texts.append(text)
    if texts:
        content = "".join(texts)

    if not content:
        raise ValueError("Empty response")

    return " ".join(content.split()).strip()


def enforce_slug_constraints(text: str, min_words: int, max_words: int, max_length: int) -> str:
    normalized = normalize_slug_text(text)
    if not normalized:
        return ""

    words = [w for w in normalized.split("-") if w]
    deduped = []
    seen = set()
    for word in words:
        if word in seen:
            continue
        seen.add(word)
        deduped.append(word)

    if len(deduped) < min_words:
        return ""

    if len(deduped) > max_words:
        deduped = deduped[:max_words]

    slug = "-".join(deduped)
    if len(slug) > max_length:
        while len(deduped) > min_words and len("-".join(deduped)) > max_length:
            deduped = deduped[:-1]
        slug = "-".join(deduped)
        if len(slug) > max_length:
            slug = slug[:max_length].rstrip("-")

    if not slug:
        return ""

    if len([w for w in slug.split("-") if w]) < min_words:
        return ""

    return slug


def normalize_slug_text(text: str) -> str:
    cleaned = text.strip().lower()
    cleaned = cleaned.replace("&", " und ")
    cleaned = (
        cleaned.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    cleaned = unicodedata.normalize("NFKD", cleaned)
    cleaned = "".join(ch for ch in cleaned if not unicodedata.combining(ch))
    cleaned = re.sub(r"\d+", " ", cleaned)
    cleaned = re.sub(r"[^a-z\s-]+", " ", cleaned)
    cleaned = re.sub(r"[\s_-]+", "-", cleaned)
    cleaned = re.sub(r"-+", "-", cleaned).strip("-")
    return cleaned
