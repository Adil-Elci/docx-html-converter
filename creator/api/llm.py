from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Optional

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

    raise LLMError("LLM returned invalid JSON.")


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
        raise LLMError("Missing CREATOR_LLM_API_KEY.")
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
    return _extract_json(str(content))
