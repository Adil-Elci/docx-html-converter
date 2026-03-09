from __future__ import annotations

import ast
import json
import logging
import os
import re
import time
from typing import Any, Callable, Dict, Optional

import requests

logger = logging.getLogger("creator.llm")


class LLMError(RuntimeError):
    pass


_SMART_QUOTES = str.maketrans(
    {
        "“": '"',
        "”": '"',
        "„": '"',
        "‟": '"',
        "’": "'",
        "‘": "'",
        "‚": "'",
        "‛": "'",
    }
)


def _normalize_json_text(text: str) -> str:
    cleaned = (text or "").strip().translate(_SMART_QUOTES)
    cleaned = cleaned.lstrip("\ufeff")
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _extract_balanced_object(text: str) -> str:
    start = text.find("{")
    if start < 0:
        return ""
    depth = 0
    in_string = False
    escape = False
    quote_char = ""
    for index in range(start, len(text)):
        char = text[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote_char:
                in_string = False
            continue
        if char in {'"', "'"}:
            in_string = True
            quote_char = char
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return ""


def _repair_json_like_text(text: str) -> str:
    repaired = _normalize_json_text(text)
    repaired = _extract_balanced_object(repaired) or repaired
    repaired = re.sub(r",(\s*[}\]])", r"\1", repaired)
    repaired = re.sub(r"([{,]\s*)([A-Za-z_][A-Za-z0-9_\-]*)(\s*:)", r'\1"\2"\3', repaired)
    return repaired.strip()


def _extract_json(text: str) -> Dict[str, Any]:
    cleaned = _normalize_json_text(text)
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except ValueError:
        pass

    parsed = _try_literal_eval(cleaned)
    if parsed is not None:
        return parsed

    snippet = _extract_balanced_object(cleaned)
    if snippet:
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, dict):
                return parsed
        except ValueError:
            pass
        parsed = _try_literal_eval(snippet)
        if parsed is not None:
            return parsed

    repaired = _repair_json_like_text(cleaned)
    if repaired and repaired != cleaned:
        try:
            parsed = json.loads(repaired)
            if isinstance(parsed, dict):
                return parsed
        except ValueError:
            pass
        parsed = _try_literal_eval(repaired)
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


def _strip_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()


def _extract_html_blob(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:html)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    if "<" not in cleaned or ">" not in cleaned:
        return ""
    match = re.search(r"<(h1|html|article|section|div|p)\b", cleaned, flags=re.IGNORECASE)
    if match:
        cleaned = cleaned[match.start():]
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _infer_meta_title(html: str) -> str:
    match = re.search(r"<h1[^>]*>(.*?)</h1>", html, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return _strip_tags(match.group(1))[:160]
    return ""


def _infer_excerpt(html: str) -> str:
    match = re.search(r"<p[^>]*>(.*?)</p>", html, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return _strip_tags(match.group(1))[:200]
    return ""


def _coerce_html_payload(raw: str) -> Optional[Dict[str, Any]]:
    html = _extract_html_blob(raw)
    if not html:
        return None
    return {
        "_html_fallback": True,
        "meta_title": _infer_meta_title(html),
        "meta_description": "",
        "slug": "",
        "excerpt": _infer_excerpt(html),
        "article_html": html,
    }


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


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _log_usage(
    provider: str,
    request_label: str,
    model: str,
    usage: Any,
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> None:
    if not isinstance(usage, dict):
        return
    prompt_tokens = _to_int(usage.get("prompt_tokens", usage.get("input_tokens")))
    completion_tokens = _to_int(usage.get("completion_tokens", usage.get("output_tokens")))
    total_tokens = _to_int(usage.get("total_tokens"))
    cache_creation_input_tokens = _to_int(usage.get("cache_creation_input_tokens"))
    cache_read_input_tokens = _to_int(usage.get("cache_read_input_tokens"))
    if total_tokens is None and prompt_tokens is not None and completion_tokens is not None:
        total_tokens = prompt_tokens + completion_tokens

    label = request_label or "unspecified"
    logger.info(
        "creator.llm_usage provider=%s label=%s model=%s prompt_tokens=%s completion_tokens=%s total_tokens=%s cache_creation_input_tokens=%s cache_read_input_tokens=%s",
        provider,
        label,
        model,
        prompt_tokens,
        completion_tokens,
        total_tokens,
        cache_creation_input_tokens,
        cache_read_input_tokens,
    )
    if usage_collector is not None:
        try:
            usage_collector(
                {
                    "provider": provider,
                    "label": label,
                    "model": model,
                    "prompt_tokens": prompt_tokens or 0,
                    "completion_tokens": completion_tokens or 0,
                    "total_tokens": total_tokens or 0,
                    "cache_creation_input_tokens": cache_creation_input_tokens or 0,
                    "cache_read_input_tokens": cache_read_input_tokens or 0,
                }
            )
        except Exception:
            logger.warning("creator.llm_usage_collector_failed", exc_info=True)


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
    request_label: str,
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
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
    _log_usage("openai", request_label, model, body.get("usage"), usage_collector)

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
    request_label: str,
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
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
    _log_usage("anthropic", request_label, model, body.get("usage"), usage_collector)

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
    allow_html_fallback: bool = False,
    request_label: str = "",
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    if not api_key:
        raise LLMError("Missing LLM API key.")
    provider_is_anthropic = "anthropic" in (base_url or "").lower() or model.strip().lower().startswith("claude")
    retries = 0
    backoff_seconds = 0.0

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
                    request_label=request_label,
                    usage_collector=usage_collector,
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
                    request_label=request_label,
                    usage_collector=usage_collector,
                )
            raw_text = str(raw)
            try:
                return _extract_json(raw_text)
            except LLMError as exc:
                if allow_html_fallback:
                    payload = _coerce_html_payload(raw_text)
                    if payload:
                        logger.warning("creator.llm_html_fallback used")
                        return payload
                raise exc
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


def call_llm_text(
    *,
    system_prompt: str,
    user_prompt: str,
    api_key: str,
    base_url: str,
    model: str,
    timeout_seconds: int,
    max_tokens: int = 1200,
    temperature: float = 0.3,
    request_label: str = "",
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> str:
    if not api_key:
        raise LLMError("Missing LLM API key.")
    provider_is_anthropic = "anthropic" in (base_url or "").lower() or model.strip().lower().startswith("claude")
    retries = 0
    backoff_seconds = 0.0

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
                    request_label=request_label,
                    usage_collector=usage_collector,
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
                    request_label=request_label,
                    usage_collector=usage_collector,
                )
            return str(raw)
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
