from __future__ import annotations

import ast
import json
import logging
import os
import re
import time
from typing import Any, Dict, Optional, Tuple

import requests

logger = logging.getLogger("creator.llm")


class LLMError(RuntimeError):
    pass


def _extract_json(text: str) -> Dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except ValueError:
        pass

    parsed = _try_literal_eval(cleaned)
    if parsed is not None:
        return parsed

    first = cleaned.find("{")
    last = cleaned.rfind("}")
    if first >= 0 and last > first:
        snippet = cleaned[first:last + 1]
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, dict):
                return parsed
        except ValueError:
            pass
        parsed = _try_literal_eval(snippet)
        if parsed is not None:
            return parsed

    raise LLMError("LLM returned invalid JSON.")


def _is_retryable_error(error: LLMError) -> bool:
    message = str(error).lower()
    if "llm request failed" in message:
        return True
    match = re.search(r"llm http (\d+)", message)
    if match:
        code = int(match.group(1))
        return code in {408, 409, 429, 500, 502, 503, 504}
    if "timed out" in message:
        return True
    return False


def _try_literal_eval(text: str) -> Optional[Dict[str, Any]]:
    normalized = re.sub(r'(?<![A-Za-z0-9_"])true(?![A-Za-z0-9_"])', "True", text, flags=re.IGNORECASE)
    normalized = re.sub(r'(?<![A-Za-z0-9_"])false(?![A-Za-z0-9_"])', "False", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'(?<![A-Za-z0-9_"])null(?![A-Za-z0-9_"])', "None", normalized, flags=re.IGNORECASE)
    try:
        parsed = ast.literal_eval(normalized)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _read_env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name, str(default))).strip())
    except Exception:
        return default


def _read_env_float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name, str(default))).strip())
    except Exception:
        return default


def _call_openai(
    *,
    system_prompt: str,
    user_prompt: str,
    api_key: str,
    base_url: str,
    model: str,
    timeout_seconds: int,
    max_tokens: int,
    temperature: float,
) -> str:
    url = base_url.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise LLMError(f"LLM request failed: {exc}") from exc

    if response.status_code >= 400:
        raise LLMError(f"LLM HTTP {response.status_code}: {response.text[:400]}")

    try:
        body = response.json()
    except ValueError as exc:
        raise LLMError("LLM returned non-JSON response.") from exc

    content: Optional[str] = None
    choices = body.get("choices")
    if isinstance(choices, list) and choices:
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        if isinstance(message, dict):
            content = message.get("content")
    if not content:
        raise LLMError("LLM response missing content.")
    return str(content)


def _call_anthropic(
    *,
    system_prompt: str,
    user_prompt: str,
    api_key: str,
    base_url: str,
    model: str,
    timeout_seconds: int,
    max_tokens: int,
    temperature: float,
) -> str:
    url = base_url.rstrip("/") + "/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_prompt}],
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise LLMError(f"LLM request failed: {exc}") from exc

    if response.status_code >= 400:
        raise LLMError(f"LLM HTTP {response.status_code}: {response.text[:400]}")

    try:
        body = response.json()
    except ValueError as exc:
        raise LLMError("LLM returned non-JSON response.") from exc

    content_blocks = body.get("content")
    if isinstance(content_blocks, list):
        texts = []
        for block in content_blocks:
            if isinstance(block, dict) and block.get("type") == "text":
                text = block.get("text")
                if isinstance(text, str) and text.strip():
                    texts.append(text.strip())
        if texts:
            return "\n".join(texts)
    raise LLMError("LLM response missing content.")


def call_llm_json(
    *,
    system_prompt: str,
    user_prompt: str,
    api_key: str,
    base_url: str,
    model: str,
    timeout_seconds: int,
    max_tokens: int = 1200,
    temperature: float = 0.3,
) -> Dict[str, Any]:
    if not api_key:
        raise LLMError("Missing LLM API key.")
    provider_is_anthropic = "anthropic" in (base_url or "").lower() or model.strip().lower().startswith("claude")
    retries = _read_env_int("CREATOR_LLM_RETRIES", 2)
    backoff_seconds = _read_env_float("CREATOR_LLM_RETRY_BACKOFF_SECONDS", 2.0)

    last_error: Optional[LLMError] = None
    for attempt in range(retries + 1):
        try:
            if provider_is_anthropic:
                raw = _call_anthropic(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    api_key=api_key,
                    base_url=base_url,
                    model=model,
                    timeout_seconds=timeout_seconds,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
            else:
                raw = _call_openai(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    api_key=api_key,
                    base_url=base_url,
                    model=model,
                    timeout_seconds=timeout_seconds,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
            return _extract_json(str(raw))
        except LLMError as exc:
            last_error = exc
            if attempt >= retries or not _is_retryable_error(exc):
                break
            sleep_seconds = backoff_seconds * (2 ** attempt)
            logger.warning(
                "creator.llm_retry attempt=%s/%s sleep=%.1fs error=%s",
                attempt + 1,
                retries + 1,
                sleep_seconds,
                exc,
            )
            time.sleep(sleep_seconds)

    raise last_error or LLMError("LLM request failed.")
