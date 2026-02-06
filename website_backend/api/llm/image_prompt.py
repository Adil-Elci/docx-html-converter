from __future__ import annotations

import logging
import os
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
MAX_TOKENS = 200
TEMPERATURE = 0.4

FALLBACK_PROMPT = (
    "Professional abstract editorial image, neutral background, modern minimal composition, "
    "soft diffused lighting, high detail. Negative: text, watermark, logo, low quality, blurry, deformed"
)

SYSTEM_PROMPT = (
    "You generate a single English image prompt. Output only the prompt with no quotes or markdown. "
    "Rules: English only. No text in the image. No logos or brand names. Professional hyper realistic "
    "editorial photography or illustration. Neutral, modern, minimal style. Avoid clichés. "
    "If the topic is a debate, controversial, or has opposing viewpoints, the image must be strictly neutral "
    "and balanced, avoiding any symbols, colors, objects, or composition that imply a position, judgment, "
    "or advocacy. Prefer abstract, symmetrical, or evenly weighted compositions in such cases. "
    "No people faces unless clearly appropriate. "
    "The prompt must end with: "
    "Negative: text, watermark, logo, low quality, blurry, deformed"
)


def generate_image_prompt(title: str, intro: str) -> str:
    if load_dotenv:
        loaded = load_dotenv(override=True)
        logger.info("dotenv_loaded=%s cwd=%s", bool(loaded), os.getcwd())
    else:
        logger.warning("python-dotenv is not installed; .env will not be loaded")

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY is missing")
        logger.info("image_prompt_fallback reason=missing_api_key")
        return FALLBACK_PROMPT

    if Anthropic is None:
        logger.error("anthropic SDK is not available")
        logger.info("image_prompt_fallback reason=missing_anthropic_sdk")
        return FALLBACK_PROMPT

    for attempt in range(1, 3):
        logger.info(
            "image_prompt_attempt attempt=%s model=%s api_key_present=%s",
            attempt,
            MODEL_NAME,
            True,
        )
        try:
            result = _call_anthropic(api_key, title, intro)
            logger.info(
                "image_prompt_response received=%s length=%s",
                bool(result),
                len(result) if result else 0,
            )
            if _is_valid_prompt(result):
                logger.info("image_prompt_success attempt=%s", attempt)
                return result
            logger.info("image_prompt_invalid attempt=%s", attempt)
        except Exception as exc:
            logger.error(
                "image_prompt_error attempt=%s error_type=%s error=%s",
                attempt,
                type(exc).__name__,
                exc,
            )
            continue

    logger.info("image_prompt_fallback reason=all_attempts_failed")
    return FALLBACK_PROMPT


def _call_anthropic(api_key: str, title: str, intro: str) -> str:
    client = Anthropic(api_key=api_key, timeout=TIMEOUT_SECONDS)
    user_message = f"Title (German): {title}\nIntro (German): {intro}\n"

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


def _is_valid_prompt(prompt: str) -> bool:
    if not prompt:
        return False
    return len(prompt) >= 20
