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
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None

logger = logging.getLogger("doc_converter.llm")

MODEL_NAME = "gpt-4o-mini"
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

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY is missing")
        logger.info("slug_fallback reason=missing_api_key")
        return ""

    if OpenAI is None:
        logger.error("openai SDK is not available")
        logger.info("slug_fallback reason=missing_openai_sdk")
        return ""

    for attempt in range(1, 3):
        logger.info(
            "slug_attempt attempt=%s model=%s api_key_present=%s",
            attempt,
            MODEL_NAME,
            True,
        )
        try:
            result = _call_openai(api_key, title)
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


def _call_openai(api_key: str, title: str) -> str:
    client = OpenAI(api_key=api_key, timeout=TIMEOUT_SECONDS)
    user_message = f"Title: {title}\n"

    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
    )

    content: Optional[str] = None
    choices = getattr(response, "choices", None) or []
    if choices:
        message = getattr(choices[0], "message", None)
        if message:
            content = getattr(message, "content", None)

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
