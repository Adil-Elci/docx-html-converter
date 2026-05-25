from __future__ import annotations

import base64
import hashlib
import json
import logging
import mimetypes
import os
import re
import time
from datetime import datetime, timezone
from html import escape
from html import unescape
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from .creator_prompt_trace import ensure_prompt_trace_in_creator_output


DEFAULT_CONVERTER_ENDPOINT = "https://elci.live/convert"
DEFAULT_CREATOR_ENDPOINT = "http://localhost:8100"
DEFAULT_LEONARDO_BASE_URL = "https://cloud.leonardo.ai/api/rest/v1"
# Leonardo Flux Schnell
DEFAULT_LEONARDO_MODEL_ID = "1dd50843-d653-4516-a8e3-f0238ee453ff"
DEFAULT_IMAGE_WIDTH = 1024
DEFAULT_IMAGE_HEIGHT = 576
DEFAULT_IMAGE_COUNT = 1
DEFAULT_AUTHOR_ID = 4
DEFAULT_POST_STATUS = "publish"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_CREATOR_TIMEOUT_SECONDS = 300
DEFAULT_IMAGE_POLL_TIMEOUT_SECONDS = 90
DEFAULT_IMAGE_POLL_INTERVAL_SECONDS = 2
DEFAULT_CATEGORY_LLM_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_CATEGORY_LLM_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_CATEGORY_LLM_OPENAI_MODEL = "gpt-4.1-mini"
DEFAULT_CATEGORY_LLM_ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_CATEGORY_LLM_MAX_CATEGORIES = 2
DEFAULT_CATEGORY_LLM_CONFIDENCE_THRESHOLD = 0.55
DEFAULT_4LLM_WORD_COUNT = 1800
ACCESS_CHECK_IMAGE_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO7Z0x0AAAAASUVORK5CYII="
)

logger = logging.getLogger("portal_backend.automation")


class AutomationError(RuntimeError):
    def __init__(self, message: str, *, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


def _normalize_http_url(value: str, field_name: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        raise AutomationError(f"{field_name} is required.")
    parsed = urlparse(cleaned)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise AutomationError(f"{field_name} must be a valid http(s) URL.")
    return cleaned


def resolve_source_url(source_type: str, doc_url: Optional[str], docx_file: Optional[str]) -> Tuple[str, str]:
    cleaned_source_type = source_type.strip().lower()
    if cleaned_source_type == "google-doc":
        return cleaned_source_type, _normalize_http_url(doc_url or "", "doc_url")

    if cleaned_source_type in {"word-doc", "docx-upload"}:
        raw_value = (docx_file or "").strip()
        if not raw_value:
            raise AutomationError("docx_file is required for source_type word-doc/docx-upload.")

        unescaped = unescape(raw_value)
        match = re.search(r"""href\s*=\s*["']([^"']+)["']""", unescaped, flags=re.IGNORECASE)
        source_url = match.group(1).strip() if match else unescaped
        return cleaned_source_type, _normalize_http_url(source_url, "docx_file URL")

    raise AutomationError("source_type must be one of google-doc, word-doc, docx-upload.")


# Status codes worth retrying: shared-host blips (503), upstream gateway
# failures (502/504), and rate limits (429). 5xx without one of these is
# treated as a real server bug and not retried.
_TRANSIENT_RETRY_STATUS_CODES = frozenset({429, 502, 503, 504})


def _request_json(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    allow_redirects: bool = True,
    max_attempts: int = 3,
    retry_backoff_seconds: float = 2.0,
) -> Dict[str, Any]:
    """HTTP JSON request with retry on transient failures.

    Retries the call on connection errors and the transient HTTP status
    codes in ``_TRANSIENT_RETRY_STATUS_CODES`` (429/502/503/504) up to
    ``max_attempts`` total tries with exponential backoff. The brillenhaus
    test hit a 503 from a shared WP host AFTER the full ~$0.50 article
    generation budget was already spent -- losing the article to a
    transient host blip is the worst outcome we can have.

    4xx (other than 429) is treated as a permanent client error and
    never retried.
    """

    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=json_body,
                timeout=timeout_seconds,
                allow_redirects=allow_redirects,
            )
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= max_attempts:
                raise AutomationError(f"Request failed for {url}: {exc}") from exc
            sleep_for = retry_backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "automation.http.connection_retry url=%s attempt=%s/%s sleep=%.1fs error=%s",
                url, attempt, max_attempts, sleep_for, exc,
            )
            time.sleep(sleep_for)
            continue

        if 300 <= response.status_code < 400:
            location = response.headers.get("Location", "")
            raise AutomationError(f"Unexpected redirect from {url} to {location}.")

        if response.status_code in _TRANSIENT_RETRY_STATUS_CODES:
            if attempt < max_attempts:
                sleep_for = retry_backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "automation.http.transient_retry url=%s status=%s attempt=%s/%s sleep=%.1fs",
                    url, response.status_code, attempt, max_attempts, sleep_for,
                )
                time.sleep(sleep_for)
                continue
            body = response.text[:600]
            raise AutomationError(
                f"HTTP {response.status_code} from {url} after {max_attempts} attempts: {body}"
            )

        if response.status_code >= 400:
            body = response.text[:600]
            raise AutomationError(f"HTTP {response.status_code} from {url}: {body}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise AutomationError(f"Non-JSON response from {url}.") from exc
        if not isinstance(payload, dict):
            raise AutomationError(f"Expected JSON object from {url}, got {type(payload).__name__}.")
        return payload

    # Loop only exits via return/raise above; this is unreachable in practice
    # but mypy/pylint prefer an explicit terminator.
    raise AutomationError(f"Request to {url} ended without a response after {max_attempts} attempts.")


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _normalize_site_selection_url(value: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return ""
    parsed = urlparse(cleaned if "://" in cleaned else f"https://{cleaned}")
    scheme = parsed.scheme or "https"
    host = (parsed.netloc or parsed.path or "").strip().lower().rstrip("/")
    path = (parsed.path or "").strip()
    if host and path and path != "/":
        return f"{scheme}://{host}{path.rstrip('/')}"
    if host:
        return f"{scheme}://{host}"
    return cleaned.rstrip("/")


def _extract_creator_selected_site_url(creator_output: Dict[str, Any], fallback_site_url: str) -> str:
    direct = str(creator_output.get("host_site_url") or "").strip()
    if direct:
        return direct
    debug = creator_output.get("debug") if isinstance(creator_output.get("debug"), dict) else {}
    supervisor_master_plan = (
        debug.get("supervisor_master_plan")
        if isinstance(debug.get("supervisor_master_plan"), dict)
        else {}
    )
    publishing_site = (
        supervisor_master_plan.get("publishing_site")
        if isinstance(supervisor_master_plan.get("publishing_site"), dict)
        else {}
    )
    selected = str(publishing_site.get("site_url") or "").strip()
    return selected or fallback_site_url


def _select_publish_target(
    *,
    creator_output: Dict[str, Any],
    fallback_target: Dict[str, Any],
    publishing_candidates: Optional[List[Dict[str, Any]]],
) -> Dict[str, Any]:
    selected_site_url = _extract_creator_selected_site_url(
        creator_output,
        str(fallback_target.get("site_url") or ""),
    )
    selected_key = _normalize_site_selection_url(selected_site_url)
    candidate_map: Dict[str, Dict[str, Any]] = {}
    for candidate in publishing_candidates or []:
        if not isinstance(candidate, dict):
            continue
        site_url = str(candidate.get("site_url") or "").strip()
        if not site_url:
            continue
        candidate_map[_normalize_site_selection_url(site_url)] = candidate
    return dict(candidate_map.get(selected_key) or fallback_target)


def _strip_html_to_text(value: str) -> str:
    if not value:
        return ""
    without_scripts = re.sub(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", " ", value, flags=re.IGNORECASE)
    without_styles = re.sub(r"<style\b[^<]*(?:(?!</style>)<[^<]*)*</style>", " ", without_scripts, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", without_styles)
    compact = re.sub(r"\s+", " ", text).strip()
    return compact


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _build_category_selection_messages(
    *,
    title: str,
    excerpt: str,
    clean_html: str,
    category_candidates: List[Dict[str, Any]],
    max_categories: int,
) -> List[Dict[str, str]]:
    content_text = _strip_html_to_text(clean_html)
    if len(content_text) > 6000:
        content_text = content_text[:6000]

    candidates_lines = []
    for candidate in category_candidates:
        wp_id = candidate.get("id")
        name = str(candidate.get("name", "")).strip()
        slug = str(candidate.get("slug", "")).strip()
        if not isinstance(wp_id, int):
            continue
        candidates_lines.append(f'- id={wp_id}; name="{name}"; slug="{slug}"')

    system_prompt = (
        "You assign WordPress categories to posts. "
        "Return only JSON with key category_ids (array of integers) and confidence (0..1). "
        f"Select 1 to {max_categories} categories from the provided candidates only."
    )
    user_prompt = (
        f"Post title:\n{title}\n\n"
        f"Post excerpt:\n{excerpt}\n\n"
        f"Post content (plain text):\n{content_text}\n\n"
        f"Allowed categories:\n{chr(10).join(candidates_lines)}\n\n"
        "Response JSON schema: {\"category_ids\":[int],\"confidence\":0.0}"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def _extract_llm_json_text(payload: Dict[str, Any]) -> str:
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0]
        if isinstance(first_choice, dict):
            message = first_choice.get("message")
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str):
                    return content.strip()
    content = payload.get("content")
    if isinstance(content, list):
        parts: List[str] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") != "text":
                continue
            text = block.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
        if parts:
            return "\n".join(parts).strip()
    raise AutomationError("Category LLM response missing message content.")


def _parse_json_object_from_text(raw_text: str) -> Dict[str, Any]:
    cleaned = raw_text.strip()
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
        snippet = cleaned[first : last + 1]
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, dict):
                return parsed
        except ValueError:
            pass

    raise AutomationError(f"Category LLM returned invalid JSON content: {raw_text[:200]}")


def _select_categories_with_llm(
    *,
    title: str,
    excerpt: str,
    clean_html: str,
    category_candidates: List[Dict[str, Any]],
    api_key: str,
    base_url: str,
    model: str,
    max_categories: int,
    confidence_threshold: float,
    timeout_seconds: int,
) -> List[int]:
    messages = _build_category_selection_messages(
        title=title,
        excerpt=excerpt,
        clean_html=clean_html,
        category_candidates=category_candidates,
        max_categories=max_categories,
    )
    provider_is_anthropic = "anthropic" in (base_url or "").lower() or model.strip().lower().startswith("claude")
    if provider_is_anthropic:
        system_prompt = messages[0]["content"]
        user_prompt = messages[1]["content"]
        url = f"{base_url.rstrip('/')}/messages"
        body = {
            "model": model,
            "temperature": 0.1,
            "max_tokens": 512,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }
    else:
        url = f"{base_url.rstrip('/')}/chat/completions"
        body = {
            "model": model,
            "messages": messages,
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    try:
        response = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        raise AutomationError(f"Category LLM request failed: {exc}") from exc

    if response.status_code >= 400:
        raise AutomationError(f"Category LLM HTTP {response.status_code}: {response.text[:400]}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise AutomationError("Category LLM returned non-JSON response.") from exc
    if not isinstance(payload, dict):
        raise AutomationError("Category LLM returned unexpected payload type.")

    raw_text = _extract_llm_json_text(payload)
    parsed = _parse_json_object_from_text(raw_text)

    raw_ids = parsed.get("category_ids", [])
    raw_confidence = parsed.get("confidence")
    confidence = 1.0
    if isinstance(raw_confidence, (int, float)):
        confidence = float(raw_confidence)
    if confidence < confidence_threshold:
        raise AutomationError(
            f"Category LLM confidence too low ({confidence:.2f} < {confidence_threshold:.2f})."
        )

    if not isinstance(raw_ids, list):
        raise AutomationError("Category LLM category_ids must be an array.")
    allowed_ids = {
        int(candidate["id"])
        for candidate in category_candidates
        if isinstance(candidate.get("id"), int) and int(candidate["id"]) > 0
    }
    selected: List[int] = []
    seen: set[int] = set()
    for raw in raw_ids:
        if not isinstance(raw, int):
            continue
        if raw not in allowed_ids or raw in seen:
            continue
        seen.add(raw)
        selected.append(raw)
        if len(selected) >= max_categories:
            break

    if not selected:
        raise AutomationError("Category LLM did not return valid category IDs from allowed candidates.")
    return selected


def call_converter(source_url: str, publishing_site: str, converter_endpoint: str, timeout_seconds: int) -> Dict[str, Any]:
    response = _request_json(
        "POST",
        converter_endpoint,
        json_body={"source_url": source_url, "publishing_site": publishing_site},
        timeout_seconds=timeout_seconds,
    )
    required = ("title", "slug", "clean_html", "excerpt", "image_prompt")
    missing = [key for key in required if not response.get(key)]
    if missing:
        raise AutomationError(f"Converter response missing required field(s): {', '.join(missing)}.")
    return response


def _find_first_generated_image_url(payload: Any) -> Optional[str]:
    stack = [payload]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            generated_images = current.get("generated_images")
            if isinstance(generated_images, list):
                for item in generated_images:
                    if isinstance(item, dict):
                        url = item.get("url")
                        if isinstance(url, str) and url.strip():
                            return url.strip()
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return None


def _find_generation_id(payload: Any) -> Optional[str]:
    stack = [payload]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            for key, value in current.items():
                if key.lower() in {"generationid", "generation_id"} and isinstance(value, str) and value.strip():
                    return value.strip()
                stack.append(value)
        elif isinstance(current, list):
            stack.extend(current)
    return None


def generate_image_via_leonardo(
    prompt: str,
    *,
    api_key: str,
    timeout_seconds: int,
    poll_timeout_seconds: int,
    poll_interval_seconds: int,
    model_id: str = DEFAULT_LEONARDO_MODEL_ID,
    width: int = DEFAULT_IMAGE_WIDTH,
    height: int = DEFAULT_IMAGE_HEIGHT,
    num_images: int = DEFAULT_IMAGE_COUNT,
    base_url: str = DEFAULT_LEONARDO_BASE_URL,
) -> str:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    create_url = f"{base_url.rstrip('/')}/generations"
    create_payload = {
        "prompt": prompt,
        "modelId": model_id,
        "width": width,
        "height": height,
        "num_images": num_images,
    }
    created = _request_json(
        "POST",
        create_url,
        headers=headers,
        json_body=create_payload,
        timeout_seconds=timeout_seconds,
    )

    immediate = _find_first_generated_image_url(created)
    if immediate:
        return immediate

    generation_id = _find_generation_id(created)
    if not generation_id:
        raise AutomationError("Leonardo response did not include generation ID or image URL.")

    poll_url = f"{base_url.rstrip('/')}/generations/{generation_id}"
    deadline = time.monotonic() + poll_timeout_seconds
    while time.monotonic() < deadline:
        polled = _request_json("GET", poll_url, headers=headers, timeout_seconds=timeout_seconds)
        image_url = _find_first_generated_image_url(polled)
        if image_url:
            return image_url
        time.sleep(poll_interval_seconds)

    raise AutomationError(f"Timed out waiting for Leonardo generation {generation_id}.")


def download_binary_file(url: str, timeout_seconds: int) -> Tuple[bytes, str, str]:
    try:
        response = requests.get(url, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise AutomationError(f"Failed to download generated image: {exc}") from exc

    if response.status_code >= 400:
        raise AutomationError(f"Failed to download generated image, HTTP {response.status_code}.")

    content_type = response.headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
    path_name = Path(urlparse(url).path).name
    file_name = path_name if path_name else f"generated_image{mimetypes.guess_extension(content_type) or '.bin'}"
    return response.content, file_name, content_type


def _wp_auth_header(username: str, app_password: str) -> str:
    token = base64.b64encode(f"{username}:{app_password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def _wp_api_base(site_url: str, wp_rest_base: str) -> str:
    clean_site_url = site_url.rstrip("/")
    clean_rest_base = (wp_rest_base or "/wp-json/wp/v2").strip()
    if not clean_rest_base.startswith("/"):
        clean_rest_base = f"/{clean_rest_base}"
    return f"{clean_site_url}{clean_rest_base}"


def wp_create_media_item(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    data: bytes,
    file_name: str,
    content_type: str,
    title: str,
    alt_text: Optional[str] = None,
    timeout_seconds: int,
) -> Dict[str, Any]:
    media_url = f"{_wp_api_base(site_url, wp_rest_base)}/media"
    headers = {
        "Authorization": _wp_auth_header(wp_username, wp_app_password),
        "Content-Disposition": f'attachment; filename="{file_name}"',
        "Content-Type": content_type or "application/octet-stream",
    }

    try:
        response = requests.post(
            media_url,
            headers=headers,
            data=data,
            timeout=timeout_seconds,
            allow_redirects=False,
        )
    except requests.RequestException as exc:
        raise AutomationError(f"WordPress media upload failed: {exc}") from exc

    if 300 <= response.status_code < 400:
        location = response.headers.get("Location", "")
        raise AutomationError(
            "WordPress media upload was redirected. "
            f"Check site_url/wp_rest_base canonical host. redirect={location}"
        )

    if response.status_code >= 400:
        if response.status_code == 413:
            raise AutomationError(
                "WordPress media upload failed, HTTP 413 (Request Entity Too Large). "
                f"upload_bytes={len(data)} response={response.text[:300]}"
            )
        raise AutomationError(f"WordPress media upload failed, HTTP {response.status_code}: {response.text[:500]}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise AutomationError("WordPress media upload returned non-JSON response.") from exc
    if isinstance(payload, list):
        raise AutomationError(
            "WordPress media upload returned a JSON list instead of an object. "
            "This usually means the request hit a listing route after redirect or wrong endpoint."
        )
    if not isinstance(payload, dict):
        raise AutomationError(f"WordPress media upload returned unexpected payload type: {type(payload).__name__}.")

    media_id = payload.get("id")
    if not media_id:
        raise AutomationError("WordPress media upload succeeded but response did not include media ID.")

    # Keep parity with Make: uploaded media title follows generated post title.
    title_url = f"{_wp_api_base(site_url, wp_rest_base)}/media/{media_id}"
    update_payload = {"title": title}
    if alt_text:
        update_payload["alt_text"] = alt_text
    _request_json(
        "POST",
        title_url,
        headers={
            "Authorization": _wp_auth_header(wp_username, wp_app_password),
            "Content-Type": "application/json",
        },
        json_body=update_payload,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )
    return payload


def wp_create_post(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    title: str,
    clean_html: str,
    excerpt: str,
    slug: str,
    featured_media_id: Optional[int],
    post_status: str,
    author_id: int,
    category_ids: Optional[List[int]],
    timeout_seconds: int,
) -> Dict[str, Any]:
    posts_url = f"{_wp_api_base(site_url, wp_rest_base)}/posts"
    headers = {
        "Authorization": _wp_auth_header(wp_username, wp_app_password),
        "Content-Type": "application/json",
    }
    payload: Dict[str, Any] = {
        "title": title,
        "content": clean_html,
        "excerpt": excerpt,
        "slug": slug,
        "status": post_status,
        "author": author_id,
        "format": "standard",
        "date": datetime.now(timezone.utc).isoformat(),
    }
    if featured_media_id is not None:
        payload["featured_media"] = featured_media_id
    if category_ids:
        payload["categories"] = category_ids
    return _request_json(
        "POST",
        posts_url,
        headers=headers,
        json_body=payload,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def wp_update_post(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    post_id: int,
    title: str,
    clean_html: str,
    excerpt: str,
    slug: str,
    featured_media_id: Optional[int],
    post_status: str,
    author_id: int,
    category_ids: Optional[List[int]],
    timeout_seconds: int,
) -> Dict[str, Any]:
    post_url = f"{_wp_api_base(site_url, wp_rest_base)}/posts/{post_id}"
    headers = {
        "Authorization": _wp_auth_header(wp_username, wp_app_password),
        "Content-Type": "application/json",
    }
    payload: Dict[str, Any] = {
        "title": title,
        "content": clean_html,
        "excerpt": excerpt,
        "slug": slug,
        "status": post_status,
        "author": author_id,
        "format": "standard",
        "date": datetime.now(timezone.utc).isoformat(),
    }
    if featured_media_id is not None:
        payload["featured_media"] = featured_media_id
    if category_ids:
        payload["categories"] = category_ids
    return _request_json(
        "POST",
        post_url,
        headers=headers,
        json_body=payload,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def wp_publish_post(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    post_id: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    post_url = f"{_wp_api_base(site_url, wp_rest_base)}/posts/{post_id}"
    headers = {
        "Authorization": _wp_auth_header(wp_username, wp_app_password),
        "Content-Type": "application/json",
    }
    return _request_json(
        "POST",
        post_url,
        headers=headers,
        json_body={"status": "publish"},
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def wp_update_post_featured_media(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    post_id: int,
    featured_media_id: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    post_url = f"{_wp_api_base(site_url, wp_rest_base)}/posts/{post_id}"
    headers = {
        "Authorization": _wp_auth_header(wp_username, wp_app_password),
        "Content-Type": "application/json",
    }
    return _request_json(
        "POST",
        post_url,
        headers=headers,
        json_body={"featured_media": featured_media_id},
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def wp_patch_post_content(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    post_id: int,
    title: str,
    content_html: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    post_url = f"{_wp_api_base(site_url, wp_rest_base)}/posts/{post_id}"
    headers = {
        "Authorization": _wp_auth_header(wp_username, wp_app_password),
        "Content-Type": "application/json",
    }
    return _request_json(
        "POST",
        post_url,
        headers=headers,
        json_body={"title": title, "content": content_html},
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def wp_get_post(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    post_id: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    post_url = f"{_wp_api_base(site_url, wp_rest_base)}/posts/{post_id}?context=edit&_embed=1"
    headers = {
        "Authorization": _wp_auth_header(wp_username, wp_app_password),
        "Content-Type": "application/json",
    }
    return _request_json(
        "GET",
        post_url,
        headers=headers,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def wp_get_media(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    media_id: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    media_url = f"{_wp_api_base(site_url, wp_rest_base)}/media/{media_id}"
    headers = {
        "Authorization": _wp_auth_header(wp_username, wp_app_password),
        "Content-Type": "application/json",
    }
    return _request_json(
        "GET",
        media_url,
        headers=headers,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def _wp_delete_entity(
    *,
    resource: str,
    resource_id: int,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    entity_url = f"{_wp_api_base(site_url, wp_rest_base)}/{resource}/{resource_id}?force=true"
    headers = {
        "Authorization": _wp_auth_header(wp_username, wp_app_password),
        "Content-Type": "application/json",
    }
    return _request_json(
        "DELETE",
        entity_url,
        headers=headers,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def wp_delete_post(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    post_id: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    return _wp_delete_entity(
        resource="posts",
        resource_id=post_id,
        site_url=site_url,
        wp_rest_base=wp_rest_base,
        wp_username=wp_username,
        wp_app_password=wp_app_password,
        timeout_seconds=timeout_seconds,
    )


def wp_delete_media(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    media_id: int,
    timeout_seconds: int,
) -> Dict[str, Any]:
    return _wp_delete_entity(
        resource="media",
        resource_id=media_id,
        site_url=site_url,
        wp_rest_base=wp_rest_base,
        wp_username=wp_username,
        wp_app_password=wp_app_password,
        timeout_seconds=timeout_seconds,
    )


def wp_check_site_access(
    *,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    posts_url = f"{_wp_api_base(site_url, wp_rest_base)}/posts"
    headers = {
        "Authorization": _wp_auth_header(wp_username, wp_app_password),
        "Content-Type": "application/json",
    }
    check_token = f"{int(time.time())}-{int(time.monotonic() * 1000)}"
    slug = f"portal-access-check-{check_token}"
    title = f"Portal access check {check_token}"
    created_post_id: Optional[int] = None
    uploaded_media_id: Optional[int] = None
    cleanup_errors: List[str] = []

    try:
        created_post_payload = _request_json(
            "POST",
            posts_url,
            headers=headers,
            json_body={
                "title": title,
                "content": "<p>Automated access check draft. Safe to delete.</p>",
                "status": "draft",
                "slug": slug,
                "format": "standard",
            },
            timeout_seconds=timeout_seconds,
            allow_redirects=False,
        )
        raw_post_id = created_post_payload.get("id")
        if not isinstance(raw_post_id, int) or raw_post_id <= 0:
            raise AutomationError("WordPress post access check succeeded but response did not include a valid post ID.")
        created_post_id = raw_post_id

        uploaded_media_payload = wp_create_media_item(
            site_url=site_url,
            wp_rest_base=wp_rest_base,
            wp_username=wp_username,
            wp_app_password=wp_app_password,
            data=ACCESS_CHECK_IMAGE_BYTES,
            file_name=f"{slug}.png",
            content_type="image/png",
            title=title,
            alt_text=title,
            timeout_seconds=timeout_seconds,
        )
        raw_media_id = uploaded_media_payload.get("id")
        if not isinstance(raw_media_id, int) or raw_media_id <= 0:
            raise AutomationError("WordPress media access check succeeded but response did not include a valid media ID.")
        uploaded_media_id = raw_media_id
        return {
            "ok": True,
            "post_id": created_post_id,
            "media_id": uploaded_media_id,
        }
    finally:
        if uploaded_media_id is not None:
            try:
                wp_delete_media(
                    site_url=site_url,
                    wp_rest_base=wp_rest_base,
                    wp_username=wp_username,
                    wp_app_password=wp_app_password,
                    media_id=uploaded_media_id,
                    timeout_seconds=timeout_seconds,
                )
            except AutomationError as exc:
                cleanup_errors.append(f"media cleanup failed: {exc}")
        if created_post_id is not None:
            try:
                wp_delete_post(
                    site_url=site_url,
                    wp_rest_base=wp_rest_base,
                    wp_username=wp_username,
                    wp_app_password=wp_app_password,
                    post_id=created_post_id,
                    timeout_seconds=timeout_seconds,
                )
            except AutomationError as exc:
                cleanup_errors.append(f"post cleanup failed: {exc}")
        if cleanup_errors:
            logger.warning(
                "WordPress access check cleanup errors for %s: %s",
                site_url,
                "; ".join(cleanup_errors),
            )


def converter_publishing_site_from_site_url(site_url: str) -> str:
    parsed = urlparse(site_url.strip())
    return (parsed.netloc or parsed.path).strip().lower()


def _pick_creator_image(images: List[Dict[str, Any]], image_type: str) -> str:
    for item in images or []:
        if not isinstance(item, dict):
            continue
        if item.get("type") != image_type:
            continue
        value = item.get("id_or_url")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _insert_in_content_image(html: str, image_url: str, alt_text: str) -> str:
    if not image_url:
        return html
    alt = alt_text.replace('"', "'").strip()
    img_tag = f'<figure class="wp-block-image"><img src="{image_url}" alt="{alt}" /></figure>'
    if "</h2>" in html:
        return html.replace("</h2>", f"</h2>{img_tag}", 1)
    return f"{html}{img_tag}"


def _strip_leading_h1_from_article_html(html: str) -> str:
    cleaned = str(html or "").strip()
    return re.sub(r"^\s*<h1[^>]*>.*?</h1>\s*", "", cleaned, count=1, flags=re.IGNORECASE | re.DOTALL)


def _strip_jsonld_script_blocks(html: str) -> str:
    """Remove `<script type="application/ld+json">…</script>` blocks.

    WordPress firewalls (NinjaFirewall, Wordfence, etc.) block any POST to
    /wp-json/wp/v2/posts whose content contains a `<script>` tag — that's a
    classic XSS heuristic. Our v2 article assembler emits Article + FAQPage
    JSON-LD as inline `<script>` blocks; we strip them before publish so the
    body sails through the firewall. The schema-included HTML is still
    preserved upstream (in `article_html.final` and the creator_output) for
    review and for any future schema-injection plugin path.
    """

    return re.sub(
        r"<script\b[^>]*>.*?</script>\s*",
        "",
        str(html or ""),
        flags=re.IGNORECASE | re.DOTALL,
    )


def call_creator_pair_fit(
    *,
    creator_endpoint: str,
    target_site_url: str,
    publishing_site_url: str,
    publishing_site_id: Optional[str],
    client_target_site_id: Optional[str],
    requested_topic: Optional[str],
    exclude_topics: Optional[List[str]],
    target_profile_payload: Dict[str, Any],
    target_profile_content_hash: str,
    publishing_profile_payload: Dict[str, Any],
    publishing_profile_content_hash: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    if not creator_endpoint:
        raise AutomationError("Creator endpoint is not configured.")
    body: Dict[str, Any] = {
        "target_site_url": target_site_url,
        "publishing_site_url": publishing_site_url,
        "target_profile": {
            "content_hash": target_profile_content_hash,
            "payload": target_profile_payload,
        },
        "publishing_profile": {
            "content_hash": publishing_profile_content_hash,
            "payload": publishing_profile_payload,
        },
    }
    if publishing_site_id:
        body["publishing_site_id"] = publishing_site_id
    if client_target_site_id:
        body["client_target_site_id"] = client_target_site_id
    if requested_topic:
        body["requested_topic"] = requested_topic
    if exclude_topics:
        body["exclude_topics"] = exclude_topics
    url = creator_endpoint.rstrip("/") + "/pair-fit"
    return _request_json(
        "POST",
        url,
        json_body=body,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def call_creator_v2_derive_topic(
    *,
    creator_endpoint: str,
    target_url: str,
    timeout_seconds: int = 60,
) -> Dict[str, Any]:
    """POST to the creator service's /v2/derive-topic endpoint.

    Used when the webhook didn't provide an explicit topic so we can run
    the publisher-fit validation BEFORE spending the contract budget.
    Returns the full ``DerivedTopic`` payload as a dict; raises
    ``AutomationError`` with the stable derivation code on failure.
    """

    if not creator_endpoint:
        raise AutomationError("Creator endpoint is not configured.")
    body = {"target_url": target_url}
    url = creator_endpoint.rstrip("/") + "/v2/derive-topic"
    try:
        response = requests.post(url, json=body, timeout=timeout_seconds, allow_redirects=False)
    except requests.RequestException as exc:
        raise AutomationError(f"Creator derive-topic request failed: {exc}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise AutomationError(
            f"Creator derive-topic returned non-JSON (HTTP {response.status_code}): {response.text[:300]}"
        ) from exc

    if response.status_code == 422 and isinstance(payload, dict) and payload.get("error") == "topic_derivation_failed":
        code = str(payload.get("code") or "derivation_failed")
        message = str(payload.get("message") or "Topic derivation failed.")
        raise AutomationError(f"Creator topic derivation failed [{code}]: {message}")
    if response.status_code != 200 or not isinstance(payload, dict) or not payload.get("ok"):
        raise AutomationError(
            f"Creator derive-topic unexpected response (HTTP {response.status_code}): {str(payload)[:300]}"
        )
    return payload


def call_creator_v2_brainstorm_topics(
    *,
    creator_endpoint: str,
    target_url: str,
    target_keyword: str,
    publisher_url: Optional[str] = None,
    publishing_profile_payload: Optional[Dict[str, Any]] = None,
    language: str = "de",
    num_angles: int = 5,
    exclude_topics: Optional[List[str]] = None,
    use_cache: bool = True,
    timeout_seconds: int = 90,
) -> Dict[str, Any]:
    """POST to the creator service's /v2/brainstorm-topics endpoint.

    Returns the parsed response with up to ``num_angles`` editorial angles.
    On error or no API key, the creator service returns ``angles=[]``; we
    propagate that as an empty list so callers can fall back to the
    keyword-only flow gracefully.
    """

    if not creator_endpoint:
        raise AutomationError("Creator endpoint is not configured.")
    body: Dict[str, Any] = {
        "target_url": target_url,
        "target_keyword": target_keyword,
        "language": language,
        "num_angles": num_angles,
        "use_cache": use_cache,
    }
    if publisher_url:
        body["publisher_url"] = publisher_url
    if publishing_profile_payload:
        body["publishing_profile_payload"] = publishing_profile_payload
    if exclude_topics:
        body["exclude_topics"] = list(exclude_topics)
    url = creator_endpoint.rstrip("/") + "/v2/brainstorm-topics"
    try:
        response = requests.post(url, json=body, timeout=timeout_seconds, allow_redirects=False)
    except requests.RequestException as exc:
        raise AutomationError(f"Creator brainstorm-topics request failed: {exc}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise AutomationError(
            f"Creator brainstorm-topics returned non-JSON (HTTP {response.status_code}): {response.text[:300]}"
        ) from exc

    if response.status_code == 422 and isinstance(payload, dict) and payload.get("error") == "topic_brainstorm_failed":
        code = str(payload.get("code") or "brainstorm_failed")
        message = str(payload.get("message") or "Brainstorm failed.")
        raise AutomationError(f"Creator brainstorm failed [{code}]: {message}")
    if response.status_code != 200 or not isinstance(payload, dict) or not payload.get("ok"):
        raise AutomationError(
            f"Creator brainstorm-topics unexpected response (HTTP {response.status_code}): {str(payload)[:300]}"
        )
    return payload


def call_creator_v2_select_publisher(
    *,
    creator_endpoint: str,
    target_url: str,
    target_keyword: str,
    candidates: List[Dict[str, Any]],
    target_profile_payload: Optional[Dict[str, Any]] = None,
    language: str = "de",
    timeout_seconds: int = 60,
) -> Dict[str, Any]:
    """POST to the creator service's /v2/select-publisher endpoint.

    Returns the parsed selection dict (best_pick, ranking, no_fit, ...).
    The endpoint never raises on ``no_fit`` -- that's a verdict the caller
    handles by falling back to the Allgemein publisher. Raises
    ``AutomationError`` only on infra failure / unexpected HTTP responses.

    ``candidates`` is the deterministic shortlist (already top-K from the
    portal site-fit ranker). Each candidate must carry ``site_id``,
    ``site_url``, ``publishing_profile_payload``, and the ``is_general``
    flag.
    """

    if not creator_endpoint:
        raise AutomationError("Creator endpoint is not configured.")
    payload_candidates: List[Dict[str, Any]] = []
    for candidate in candidates:
        site_id = str(candidate.get("site_id") or "").strip()
        site_url = str(candidate.get("site_url") or "").strip()
        if not site_id or not site_url:
            continue
        payload_candidates.append(
            {
                "site_id": site_id,
                "site_url": site_url,
                "publishing_profile_payload": candidate.get("publishing_profile_payload") or {},
                "is_general": bool(candidate.get("is_general")),
            }
        )
    if not payload_candidates:
        raise AutomationError("No publishing candidates with site_id+site_url to select from.")

    body: Dict[str, Any] = {
        "target_url": target_url,
        "target_keyword": target_keyword,
        "candidates": payload_candidates,
        "language": language,
    }
    if target_profile_payload:
        body["target_profile_payload"] = target_profile_payload

    url = creator_endpoint.rstrip("/") + "/v2/select-publisher"
    try:
        response = requests.post(url, json=body, timeout=timeout_seconds, allow_redirects=False)
    except requests.RequestException as exc:
        raise AutomationError(f"Creator publisher-selector request failed: {exc}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise AutomationError(
            f"Creator publisher-selector returned non-JSON (HTTP {response.status_code}): {response.text[:300]}"
        ) from exc

    if (
        response.status_code == 422
        and isinstance(payload, dict)
        and payload.get("error") == "publisher_selection_failed"
    ):
        code = str(payload.get("code") or "selection_failed")
        message = str(payload.get("message") or "Publisher selection failed.")
        raise AutomationError(f"Creator publisher selector failed [{code}]: {message}")
    if response.status_code != 200 or not isinstance(payload, dict) or not payload.get("ok"):
        raise AutomationError(
            f"Creator publisher-selector unexpected response (HTTP {response.status_code}): {str(payload)[:300]}"
        )
    return payload


def call_creator_v2_pipeline(
    *,
    creator_endpoint: str,
    target_keyword: Optional[str],
    target_backlink_url: str,
    publishing_site_url: Optional[str],
    anchor_hint: Optional[str] = None,
    canonical_url: Optional[str] = None,
    language: Optional[str] = None,
    editorial_angle: Optional[Dict[str, Any]] = None,
    article_format: Optional[str] = None,
    service_type: Optional[str] = None,
    brand_name: Optional[str] = None,
    skip_voice_pass: bool = False,
    skip_judge: bool = False,
    skip_related_keywords: bool = False,
    skip_entity_extraction: bool = False,
    timeout_seconds: int = 300,
) -> Dict[str, Any]:
    """POST to the creator service's /v2/run-pipeline endpoint.

    Returns the full PipelineRun payload as a dict (research, contract,
    sections, article_html, judge_scores, quality_report, etc.). Raises
    AutomationError on network failure, non-2xx response, or pipeline
    failure (HTTP 422 with ``error="pipeline_failed"``). The pipeline
    failure path includes the failing phase name in the error message,
    so callers can log "[contract]" / "[voice_pass]" / etc. without
    parsing free-form messages.

    Pass ``target_keyword=None`` to let the creator service derive the
    keyword (and language) from ``target_backlink_url``. Pass
    ``publishing_site_url=None`` for late-binding (host-based eval check
    is skipped on the creator side).
    """

    if not creator_endpoint:
        raise AutomationError("Creator endpoint is not configured.")
    body: Dict[str, Any] = {
        "target_backlink_url": target_backlink_url,
        "anchor_hint": anchor_hint,
        "canonical_url": canonical_url,
        "skip_voice_pass": skip_voice_pass,
        "skip_judge": skip_judge,
        "skip_related_keywords": skip_related_keywords,
        "skip_entity_extraction": skip_entity_extraction,
    }
    if target_keyword:
        body["target_keyword"] = target_keyword
    if publishing_site_url:
        body["publishing_site_url"] = publishing_site_url
    if language:
        body["language"] = language
    if editorial_angle:
        body["editorial_angle"] = editorial_angle
    if article_format and article_format.strip().lower() in {"narrative", "listicle"}:
        body["article_format"] = article_format.strip().lower()
    if service_type and service_type.strip().lower() in {"article", "brand_mention"}:
        body["service_type"] = service_type.strip().lower()
    if brand_name and brand_name.strip():
        body["brand_name"] = brand_name.strip()
    url = creator_endpoint.rstrip("/") + "/v2/run-pipeline"
    try:
        response = requests.post(url, json=body, timeout=timeout_seconds, allow_redirects=False)
    except requests.RequestException as exc:
        raise AutomationError(f"Creator v2 pipeline request failed: {exc}") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise AutomationError(
            f"Creator v2 pipeline returned non-JSON (HTTP {response.status_code}): {response.text[:300]}"
        ) from exc

    if response.status_code == 422 and isinstance(payload, dict) and payload.get("error") == "pipeline_failed":
        phase = payload.get("phase") or "unknown"
        message = payload.get("message") or "no message"
        raise AutomationError(f"Creator v2 pipeline failed at phase [{phase}]: {message}")

    if response.status_code >= 400:
        raise AutomationError(
            f"Creator v2 pipeline HTTP {response.status_code}: {str(payload)[:300]}"
        )

    if not isinstance(payload, dict) or not payload.get("ok"):
        raise AutomationError(f"Creator v2 pipeline returned malformed payload: {str(payload)[:300]}")
    return payload


def _normalize_text_tokens(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", str(value or "").lower())
    cleaned = cleaned.translate(str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"}))
    cleaned = re.sub(r"[^\w\s-]", " ", cleaned)
    cleaned = re.sub(r"[_-]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


# Confidence floor for the selector's best_pick. A winner under this
# threshold is treated as effective no_fit and routed to Allgemein --
# the LLM tends to pick a "least-bad" option rather than declare
# no_fit=true even when its rationale is weak. 0.55 is one notch above
# the selector's "weak but possible overlap" band (0.4-0.6).
MIN_SELECTOR_CONFIDENCE = 0.55


def _summarise_selector_ranking(ranking: Any, *, top_n: int = 3) -> List[Dict[str, Any]]:
    """Extract the top-N entries from a selector ranking for trace events.

    Keeps only ``site_url`` + ``fit_score`` + a truncated ``rationale``;
    drops ``site_id`` (internal UUIDs aren't useful in admin logs).
    """

    if not isinstance(ranking, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in ranking[:top_n]:
        if not isinstance(item, dict):
            continue
        rationale = str(item.get("rationale") or "").strip()
        try:
            fit_score = float(item.get("fit_score") or 0.0)
        except (TypeError, ValueError):
            fit_score = 0.0
        out.append({
            "site_url": str(item.get("site_url") or "").strip(),
            "fit_score": fit_score,
            "rationale": rationale[:160],
        })
    return out


# A publisher is considered editorially broad when at least this many
# distinct context labels have comparable weight (>= 50% of the top
# context's score). Tuned empirically: a niche site usually has one
# dominant context and small noise; a generalist (lifestyle/news magazine)
# spreads roughly evenly across health, finance, family, real-estate, etc.
_DIVERSITY_HEURISTIC_MIN_CONTEXTS = 3
_DIVERSITY_HEURISTIC_RELATIVE_FLOOR = 0.5


def _profile_has_diverse_contexts(profile: Optional[Dict[str, Any]]) -> bool:
    """Auto-flag generalist publishers from their context-score spread.

    mysupr.de is the canonical case: title="Startseite",
    description="Aktuelles" -- meta tags give nothing. But its homepage
    H2s span skincare, real-estate, finance, relationships, food
    culture, energy, sleep. The site profiler's ``context_scores`` dict
    captures this spread; this heuristic returns True when 3+ contexts
    sit within 50% of the top context's score.
    """

    if not isinstance(profile, dict):
        return False
    scores = profile.get("context_scores")
    if not isinstance(scores, dict) or not scores:
        return False
    numeric: List[float] = []
    for value in scores.values():
        try:
            score = float(value)
        except (TypeError, ValueError):
            continue
        if score > 0:
            numeric.append(score)
    if len(numeric) < _DIVERSITY_HEURISTIC_MIN_CONTEXTS:
        return False
    numeric.sort(reverse=True)
    top_score = numeric[0]
    if top_score <= 0:
        return False
    floor = top_score * _DIVERSITY_HEURISTIC_RELATIVE_FLOOR
    qualifying = sum(1 for score in numeric if score >= floor)
    return qualifying >= _DIVERSITY_HEURISTIC_MIN_CONTEXTS


def _candidate_is_general(candidate: Dict[str, Any]) -> bool:
    """True when the site is flagged or detected as ``allgemein`` /
    general-topic.

    Three signals, in order of trust:
    1. Explicit ``is_general`` column on ``publishing_sites`` (admin sets this).
    2. Keyword heuristic on ``primary_context`` (contains
       ``allgemein`` / ``general`` / ``magazin``).
    3. Diversity heuristic: 3+ distinct context labels sit within 50% of
       the top context's score -- a generalist spreads evenly while a
       niche site has one dominant context.

    Any of the three is sufficient. Auto-detection (#2 + #3) catches
    sites we haven't manually flagged yet and is the safety net for the
    no-fit fallback path.
    """

    if candidate.get("is_general"):
        return True
    profile = candidate.get("publishing_profile_payload") or {}
    primary_context = str(profile.get("primary_context") or "").strip().lower()
    if any(token in primary_context for token in ("allgemein", "general", "magazin")):
        return True
    return _profile_has_diverse_contexts(profile)


def _slugify(value: str) -> str:
    normalized = _normalize_text_tokens(value)
    slug = re.sub(r"\s+", "-", normalized).strip("-")
    return slug[:90] or "artikel"


_KEYWORD_TITLE_MARKERS = (":", " — ", " – ", " - ", " | ", "?", "!")
_MAX_KEYWORD_WORDS = 8


def _looks_like_seo_keyword(value: str) -> bool:
    """Cheap shape check before we promote a string to ``target_keyword``.

    Rejects title-shaped strings (multi-clause separators, multi-sentence
    punctuation, > ~8 words). Used to gate brainstorm/selector outputs
    before they're forwarded to DataForSEO, which rejects long keywords
    with status 40501.
    """

    text = (value or "").strip()
    if not text:
        return False
    if any(marker in text for marker in _KEYWORD_TITLE_MARKERS):
        return False
    words = text.split()
    return len(words) <= _MAX_KEYWORD_WORDS


def _derive_keyword_from_target_profile(target_profile_payload: Optional[Dict[str, Any]]) -> str:
    """Pull a clean SEO keyword candidate from the target profile.

    Skips ``page_title`` deliberately: a raw page title like
    ``'Brillenhaus24.de - Ihr Onlineshop fuer guenstige Brillen & Komplettbril'``
    is junk as a search keyword and produced the brillenhaus -> Klimaschutz
    mismatch (no publisher could plausibly fit a string like that). When
    the profile only carries page-title-grade signal, returning ``""``
    forces the caller to fall through to ``/v2/derive-topic``, which uses
    DataForSEO to produce a real keyword.
    """

    if not isinstance(target_profile_payload, dict):
        return ""
    for key in ("domain_level_topic", "primary_context"):
        candidate = str(target_profile_payload.get(key) or "").strip()
        if candidate:
            return candidate
    topics = target_profile_payload.get("topics")
    if isinstance(topics, list) and topics:
        first = topics[0]
        if isinstance(first, dict):
            label = str(first.get("label") or first.get("topic") or "").strip()
            if label:
                return label
        elif isinstance(first, str) and first.strip():
            return first.strip()
    return ""


def _build_creator_output_for_v2(
    *,
    v2_response: Dict[str, Any],
    target_site_url: str,
    selected_site_url: str,
    selected_site_id: Optional[str],
    target_keyword: str,
    article_html: str,
) -> Dict[str, Any]:
    """Adapt the v2 PipelineRun response to the legacy creator_output dict shape.

    Downstream worker code (``_mark_creator_success``,
    ``_persist_failed_creator_output``) reads phase1..phase6 keys, so we
    populate the keys it actually consumes and leave the rest as empty
    placeholders. Source-of-truth fields live under ``pipeline_state.v2``
    and ``debug`` for observability.
    """

    contract = v2_response.get("contract") if isinstance(v2_response.get("contract"), dict) else {}
    quality_report = v2_response.get("quality_report") if isinstance(v2_response.get("quality_report"), dict) else {}
    sections = v2_response.get("sections") if isinstance(v2_response.get("sections"), list) else []
    research = v2_response.get("research") if isinstance(v2_response.get("research"), dict) else {}
    judge_scores = v2_response.get("judge_scores")
    title = str(contract.get("h1") or "").strip()
    meta_title = str(contract.get("meta_title") or title).strip()
    meta_description = str(contract.get("meta_description") or "").strip()
    slug_source = str(contract.get("slug") or meta_title or title).strip()
    excerpt = meta_description[:220] if meta_description else ""
    competitor_top_urls = contract.get("competitor_top_urls") if isinstance(contract.get("competitor_top_urls"), list) else []

    return {
        "ok": True,
        "target_site_url": target_site_url,
        "host_site_url": selected_site_url,
        "host_site_id": selected_site_id,
        "phase1": {},
        "phase2": {
            "selected_publishing_site_url": selected_site_url,
            "selected_publishing_site_id": selected_site_id,
        },
        "phase3": {
            "target_keyword": {"keyword": target_keyword},
            "competitor_references": [{"url": str(url)} for url in competitor_top_urls if str(url).strip()],
            "internal_link_candidates": [],
            "external_link_candidates": [],
        },
        "phase4": {
            "content_brief": {
                "target_keyword": target_keyword,
                "suggested_title": title,
            },
        },
        "phase5": {
            "title": title,
            "meta_title": meta_title,
            "meta_description": meta_description,
            "slug": _slugify(slug_source) if slug_source else "",
            "excerpt": excerpt,
            "article_markdown": "",
            "linked_markdown": "",
            "article_html": article_html,
            "quality_report": quality_report,
            "sections": sections,
        },
        "debug": {
            "quality_report": quality_report,
            "judge_scores": judge_scores,
            "v2_notes": list(v2_response.get("notes") or []),
            "v2_skipped_voice_pass": bool(v2_response.get("skipped_voice_pass")),
            "v2_skipped_judge": bool(v2_response.get("skipped_judge")),
        },
        "pipeline_state": {
            "v2": True,
            "research": research,
            "contract": contract,
            "quality_report": quality_report,
            "judge_scores": judge_scores,
            "selected_publishing_site": {
                "site_url": selected_site_url,
                "site_id": selected_site_id,
            },
        },
    }


IMAGE_STYLE_DIRECTIVES = (
    "Hyperrealistic editorial photograph, professional photography, "
    "natural lighting, shallow depth of field, ultra-detailed, photorealistic, 8k."
)
IMAGE_NEGATIVE_DIRECTIVES = (
    "No text, no letters, no words, no captions, no signage, no watermarks, "
    "no logos, no typography of any kind, no UI elements, no overlays."
)
# Marker we emit at the top of every directive block. Used to detect
# already-styled prompts and avoid double-appending.
_IMAGE_STYLE_MARKER = "Hyperrealistic editorial photograph"


def apply_image_style_directives(prompt: str) -> str:
    """Append the Flux Schnell style + negative directives to a subject prompt.

    Idempotent: if the prompt already contains the style marker (because it
    was produced by ``_build_image_prompt_from_contract`` on a v2 job), it's
    returned unchanged. This lets the regenerate-image endpoint reuse a
    stored prompt from a JobEvent without double-appending directives, while
    still upgrading old prompts (or the generic fallback) to the new style.
    """

    text = (prompt or "").strip()
    if not text:
        return f"Editorial photo.\n\nStyle: {IMAGE_STYLE_DIRECTIVES}\n\nMust not contain: {IMAGE_NEGATIVE_DIRECTIVES}"
    if _IMAGE_STYLE_MARKER in text:
        return text
    return f"{text}\n\nStyle: {IMAGE_STYLE_DIRECTIVES}\n\nMust not contain: {IMAGE_NEGATIVE_DIRECTIVES}"


def _build_image_prompt_from_contract(contract: Dict[str, Any]) -> str:
    """Templated featured-image prompt for v2 articles.

    No LLM call: the contract already has h1 + meta_description. Adding an LLM
    prompt-rewrite step would burn tokens for marginal gain.

    Two style/negative directive blocks are appended so Flux Schnell
    consistently produces hyperrealistic photography and avoids hallucinated
    text/typography artifacts (Flux is much better than older models at
    *not* generating text when explicitly told not to).
    """

    h1 = str(contract.get("h1") or contract.get("meta_title") or contract.get("target_keyword") or "").strip()
    meta_description = str(contract.get("meta_description") or "").strip()
    if h1 and meta_description:
        subject = f"Editorial photo illustrating: {h1}. Context: {meta_description}"
    elif h1:
        subject = f"Editorial photo illustrating: {h1}"
    else:
        subject = f"Editorial photo illustrating: {contract.get('target_keyword') or 'business article'}"
    return apply_image_style_directives(subject)


def _generate_featured_image_for_v2(
    *,
    prompt: str,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    title: str,
    leonardo_api_key: str,
    leonardo_base_url: str,
    leonardo_model_id: str,
    image_width: int,
    image_height: int,
    timeout_seconds: int,
    poll_timeout_seconds: int,
    poll_interval_seconds: int,
) -> Tuple[str, Dict[str, Any]]:
    """Generate a Leonardo image and upload it to WordPress.

    Returns ``(image_url, media_payload)`` on success. Caller decides what
    to do on AutomationError (the v2 path treats it as a soft failure and
    publishes without a featured image).
    """

    sizes_to_try = [
        (max(256, image_width), max(256, image_height)),
        (768, 432),
        (640, 360),
        (512, 288),
    ]
    unique_sizes: list[tuple[int, int]] = []
    for size in sizes_to_try:
        if size not in unique_sizes:
            unique_sizes.append(size)

    last_upload_error: Optional[AutomationError] = None
    for idx, (width, height) in enumerate(unique_sizes):
        image_url = generate_image_via_leonardo(
            prompt=prompt,
            api_key=leonardo_api_key,
            timeout_seconds=timeout_seconds,
            poll_timeout_seconds=poll_timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            model_id=leonardo_model_id,
            width=width,
            height=height,
            base_url=leonardo_base_url,
        )
        image_bytes, file_name, content_type = download_binary_file(
            image_url,
            timeout_seconds=timeout_seconds,
        )
        try:
            media_payload = wp_create_media_item(
                site_url=site_url,
                wp_rest_base=wp_rest_base,
                wp_username=wp_username,
                wp_app_password=wp_app_password,
                data=image_bytes,
                file_name=file_name,
                content_type=content_type,
                title=title,
                timeout_seconds=timeout_seconds,
            )
            return image_url, media_payload
        except AutomationError as exc:
            if "HTTP 413" in str(exc) and idx < len(unique_sizes) - 1:
                last_upload_error = exc
                continue
            raise
    if last_upload_error:
        raise last_upload_error
    raise AutomationError("WordPress media upload failed for all image size attempts.")


def _run_create_article_pipeline_v2(
    *,
    creator_endpoint: str,
    target_site_url: str,
    publishing_site_url: str,
    publishing_site_id: Optional[str],
    publishing_candidates: Optional[List[Dict[str, Any]]],
    internal_link_inventory: Optional[List[Dict[str, Any]]],
    target_profile_payload: Optional[Dict[str, Any]],
    publishing_profile_payload: Optional[Dict[str, Any]] = None,
    anchor: Optional[str] = None,
    topic: Optional[str] = None,
    article_format: str = "narrative",
    service_type: str = "article",
    exclude_topics: Optional[List[str]] = None,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    existing_wp_post_id: Optional[int],
    post_status: str,
    author_id: int,
    category_ids: Optional[List[int]],
    category_candidates: Optional[List[Dict[str, Any]]],
    timeout_seconds: int,
    creator_timeout_seconds: int,
    category_llm_enabled: bool,
    category_llm_api_key: str,
    category_llm_base_url: str,
    category_llm_model: str,
    category_llm_max_categories: int,
    category_llm_confidence_threshold: float,
    leonardo_api_key: str = "",
    leonardo_base_url: str = DEFAULT_LEONARDO_BASE_URL,
    leonardo_model_id: str = DEFAULT_LEONARDO_MODEL_ID,
    image_width: int = DEFAULT_IMAGE_WIDTH,
    image_height: int = DEFAULT_IMAGE_HEIGHT,
    poll_timeout_seconds: int = DEFAULT_IMAGE_POLL_TIMEOUT_SECONDS,
    poll_interval_seconds: int = DEFAULT_IMAGE_POLL_INTERVAL_SECONDS,
    skip_image: bool = False,
    trace_event: Optional[Callable[[str, str, str, str, Optional[Dict[str, Any]]], None]] = None,
) -> Dict[str, Any]:
    def _trace(level: str, phase: str, event: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        if trace_event is not None:
            trace_event(level, phase, event, message, details)

    if not (target_site_url or "").strip():
        raise AutomationError("Creator v2 pipeline requires target_site_url for the backlink.")

    # Topic is optional in Phase C: if the webhook didn't provide it and the
    # target profile doesn't carry it, fall back to the creator's
    # /v2/derive-topic endpoint (returns keyword + language). We derive
    # upfront so the fit validator can run BEFORE the contract spend.
    profile_topic = (topic or "").strip() or _derive_keyword_from_target_profile(target_profile_payload)
    upfront_target_keyword: Optional[str] = profile_topic or None
    upfront_language: Optional[str] = None
    derivation_payload: Optional[Dict[str, Any]] = None

    if not upfront_target_keyword:
        try:
            derivation_payload = call_creator_v2_derive_topic(
                creator_endpoint=creator_endpoint,
                target_url=target_site_url,
                timeout_seconds=min(60, creator_timeout_seconds),
            )
        except AutomationError as exc:
            _trace(
                "warning",
                "topic_derivation",
                "failed",
                "Creator topic derivation failed; proceeding without upfront topic.",
                {"error": str(exc)[:300]},
            )
            derivation_payload = None
        if derivation_payload:
            upfront_target_keyword = str(derivation_payload.get("target_keyword") or "").strip() or None
            upfront_language = str(derivation_payload.get("language_code") or "").strip().lower() or None
            _trace(
                "info",
                "topic_derivation",
                "complete",
                "Topic derived from target URL.",
                {
                    "target_keyword": upfront_target_keyword,
                    "language_code": upfront_language,
                    "cache_hit": bool(derivation_payload.get("cache_hit")),
                },
            )

    # The user can either let the worker auto-discover candidates from
    # publishing sites associated with the client target, OR pick a specific
    # publishing site explicitly (passed via publishing_site_url +
    # publishing_site_id + WP credentials). When the explicit path is used,
    # publishing_candidates is empty -- so we synthesise a single candidate
    # from the explicit selection. The synthesised candidate has no language
    # field, which the late-binding selector treats as "passes the language
    # filter" -- explicit user choice wins, period.
    effective_candidates: List[Dict[str, Any]] = list(publishing_candidates or [])
    if not effective_candidates and (publishing_site_url or "").strip():
        effective_candidates = [
            {
                "site_url": publishing_site_url,
                "site_id": publishing_site_id,
                "fit_score": 50,  # neutral; explicit selection bypasses topical fit anyway
                "notes": ["explicit_user_selection"],
                "internal_link_inventory": list(internal_link_inventory or []),
                # Populate the profile payload from the webhook context if it
                # was passed in (worker loads it from SiteProfileCache for the
                # selected publishing site). Without this, fit-validation has
                # no signal and silently passes mismatched topic/publisher
                # pairs (the brillenhaus24 -> kidsblatt regression).
                "publishing_profile_payload": dict(publishing_profile_payload or {}),
                "wp_rest_base": wp_rest_base,
                "wp_username": wp_username,
                "wp_app_password": wp_app_password,
                "category_ids": list(category_ids or []),
                "category_candidates": list(category_candidates or []),
                "is_general": False,
            }
        ]

    # Pre-flight: if neither auto-discovered candidates nor an explicit
    # publishing-site selection are available, fail before spending the
    # contract budget. We don't yet know the article's language so we can't
    # filter by it here -- the post-pipeline selector enforces the language
    # match after the contract returns.
    if not effective_candidates:
        raise AutomationError(
            "Creator v2 pipeline requires at least one publishing candidate. "
            "Either pick a publishing site explicitly or associate the client "
            "target site with at least one publishing site."
        )

    # Publisher selection: pick the best-fit publisher from the shortlist
    # in ONE Haiku call BEFORE spending the contract budget. The selector
    # ranks all candidates relatively (so it can pick the best fit even
    # when no candidate is a perfect topical match), refines the article
    # topic to suit the chosen publisher's audience (e.g. brillen ->
    # kinderbrillen on a family magazine), and returns ``no_fit=true`` only
    # when literally none of the candidates have editorial overlap. In that
    # case we fall back to an Allgemein / general publisher; if no general
    # publisher is in the shortlist either, we hard-fail with a clear
    # admin-facing message.
    fit_language = upfront_language or "de"
    selector_payload: Optional[Dict[str, Any]] = None
    chosen_candidate: Optional[Dict[str, Any]] = None
    if upfront_target_keyword:
        try:
            selector_payload = call_creator_v2_select_publisher(
                creator_endpoint=creator_endpoint,
                target_url=target_site_url,
                target_keyword=upfront_target_keyword,
                candidates=effective_candidates,
                target_profile_payload=target_profile_payload,
                language=fit_language,
                timeout_seconds=min(60, creator_timeout_seconds),
            )
        except AutomationError as exc:
            # Selector infra failure is logged but doesn't block: fall back
            # to the deterministic top candidate so the run can complete.
            _trace(
                "warning",
                "publisher_selector",
                "failed",
                "Publisher selector call failed; falling back to deterministic top candidate.",
                {"error": str(exc)[:300]},
            )
            selector_payload = None

    if selector_payload:
        best_pick = selector_payload.get("best_pick") if isinstance(selector_payload.get("best_pick"), dict) else {}
        ranking_summary = _summarise_selector_ranking(selector_payload.get("ranking"))
        no_fit_verdict = bool(selector_payload.get("no_fit"))
        try:
            confidence = float(best_pick.get("confidence") or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0

        # Confidence floor: a "best of bad" winner with confidence < 0.55
        # gets treated as an effective no_fit. The LLM tends to pick a
        # least-bad option rather than declare no_fit=true, but a sub-0.55
        # winner means it didn't actually find a good fit -- routing to
        # Allgemein is strictly better than shipping a misfit article.
        below_floor = confidence < MIN_SELECTOR_CONFIDENCE
        effective_no_fit = no_fit_verdict or below_floor

        if effective_no_fit:
            general_candidates = [c for c in effective_candidates if _candidate_is_general(c)]
            reason = "no_fit_verdict" if no_fit_verdict else "confidence_below_floor"
            if not general_candidates:
                raise AutomationError(
                    f"Creator publisher selector did not find a fit "
                    f"({reason}; confidence={confidence:.2f}) and no Allgemein / "
                    f"general publisher is available as a fallback. Pick a "
                    f"different target or flag a general site. "
                    f"Rationale: {best_pick.get('rationale') or 'no overlap'}"
                )
            chosen_candidate = general_candidates[0]
            _trace(
                "info",
                "publisher_selector",
                "allgemein_fallback",
                f"No editorial fit ({reason}); routing to Allgemein publisher.",
                {
                    "selected_site_url": chosen_candidate.get("site_url"),
                    "fallback_reason": reason,
                    "confidence": confidence,
                    "rationale": best_pick.get("rationale"),
                    "ranking": ranking_summary,
                },
            )
        else:
            picked_id = str(best_pick.get("site_id") or "").strip()
            if picked_id:
                chosen_candidate = next(
                    (c for c in effective_candidates if str(c.get("site_id") or "").strip() == picked_id),
                    None,
                )
            if chosen_candidate is None:
                # LLM picked an id that doesn't match anything we sent (defensive).
                chosen_candidate = effective_candidates[0]
            refined = str(best_pick.get("refined_topic") or "").strip()
            if refined and refined != upfront_target_keyword and _looks_like_seo_keyword(refined):
                _trace(
                    "info",
                    "publisher_selector",
                    "refined",
                    "Topic refined to fit the chosen publisher's audience.",
                    {
                        "original": upfront_target_keyword,
                        "refined": refined,
                        "selected_site_url": chosen_candidate.get("site_url"),
                        "confidence": confidence,
                        "rationale": best_pick.get("rationale"),
                        "ranking": ranking_summary,
                    },
                )
                upfront_target_keyword = refined
            elif refined and not _looks_like_seo_keyword(refined):
                _trace(
                    "warning",
                    "publisher_selector",
                    "refined_topic_rejected",
                    "Selector refined_topic is title-shaped; keeping upfront keyword.",
                    {"rejected_topic": refined[:160], "kept_keyword": upfront_target_keyword},
                )
            else:
                _trace(
                    "info",
                    "publisher_selector",
                    "selected",
                    "Publisher chosen by Haiku rerank.",
                    {
                        "selected_site_url": chosen_candidate.get("site_url"),
                        "confidence": confidence,
                        "rationale": best_pick.get("rationale"),
                        "soft_passed": bool(selector_payload.get("soft_passed")),
                        "ranking": ranking_summary,
                    },
                )

    if chosen_candidate is None:
        # Selector skipped (no upfront keyword) or its call failed: fall back to
        # the first candidate, which is the deterministic top of the shortlist.
        chosen_candidate = effective_candidates[0]
        _trace(
            "info",
            "publisher_selector",
            "deterministic_fallback",
            "Using deterministic top candidate (selector skipped or unavailable).",
            {"selected_site_url": chosen_candidate.get("site_url")},
        )

    chosen_profile_payload = chosen_candidate.get("publishing_profile_payload") or {}
    chosen_site_url = str(chosen_candidate.get("site_url") or "").strip()
    selected_publish_site_url = chosen_site_url or site_url
    selected_publish_site_id = str(chosen_candidate.get("site_id") or publishing_site_id or "").strip() or None
    selected_wp_rest_base = str(chosen_candidate.get("wp_rest_base") or wp_rest_base).strip() or wp_rest_base
    selected_wp_username = str(chosen_candidate.get("wp_username") or wp_username).strip() or wp_username
    selected_wp_app_password = str(chosen_candidate.get("wp_app_password") or wp_app_password).strip() or wp_app_password
    selected_category_ids = list(chosen_candidate.get("category_ids") or category_ids or [])
    selected_category_candidates = list(chosen_candidate.get("category_candidates") or category_candidates or [])
    # Each WP site has its own user list, so author_id must follow the chosen
    # publisher. Falling back to the caller's author_id only matches the
    # originally-associated site -- using it on a different chosen publisher
    # gets us an HTTP 400 rest_invalid_author from WP.
    candidate_author_id_raw = chosen_candidate.get("author_id")
    try:
        candidate_author_id = int(candidate_author_id_raw) if candidate_author_id_raw is not None else 0
    except (TypeError, ValueError):
        candidate_author_id = 0
    selected_author_id = candidate_author_id if candidate_author_id > 0 else author_id

    # Brainstorm an editorial angle for the article. Only runs when the
    # webhook didn't pin an explicit topic -- if the admin chose a topic
    # we respect it. Sonnet 4.6 single shot (~$0.02). The auto-pick is the
    # first angle in the LLM's ranked output; full slate is recorded in
    # trace events for transparency / future admin-side override UI.
    editorial_angle: Optional[Dict[str, Any]] = None
    explicit_topic_present = bool((topic or "").strip() or profile_topic)
    is_listicle_request = (article_format or "narrative").lower() == "listicle"
    if upfront_target_keyword and not explicit_topic_present:
        try:
            brainstorm_payload = call_creator_v2_brainstorm_topics(
                creator_endpoint=creator_endpoint,
                target_url=target_site_url,
                target_keyword=upfront_target_keyword,
                publisher_url=chosen_site_url or None,
                publishing_profile_payload=chosen_profile_payload or None,
                language=fit_language,
                num_angles=5,
                exclude_topics=list(exclude_topics or []),
                use_cache=True,
                prefer_listicle=is_listicle_request,
                timeout_seconds=min(120, creator_timeout_seconds),
            )
        except AutomationError as exc:
            _trace(
                "warning",
                "brainstorm",
                "failed",
                "Topic brainstorm failed; falling back to keyword-only contract.",
                {"error": str(exc)[:300]},
            )
            brainstorm_payload = None
        if brainstorm_payload:
            angles = brainstorm_payload.get("angles") if isinstance(brainstorm_payload, dict) else None
            if isinstance(angles, list) and angles and isinstance(angles[0], dict):
                top = angles[0]
                editorial_angle = {
                    "title": str(top.get("title") or "").strip(),
                    "hook": str(top.get("hook") or "").strip(),
                    "rationale": str(top.get("rationale") or "").strip(),
                }
                if is_listicle_request:
                    editorial_angle["format"] = "listicle"
                top_keyword = str(top.get("target_keyword") or "").strip()
                # Brainstorm sometimes returns the article TITLE in the
                # target_keyword field (LLM drift). DataForSEO rejects
                # multi-clause / colon-bearing strings with status 40501,
                # so we keep the cleaner upfront keyword whenever the
                # brainstorm output looks title-shaped.
                if top_keyword and top_keyword != upfront_target_keyword and _looks_like_seo_keyword(top_keyword):
                    upfront_target_keyword = top_keyword
                elif top_keyword and not _looks_like_seo_keyword(top_keyword):
                    _trace(
                        "warning",
                        "brainstorm",
                        "keyword_rejected",
                        "Brainstorm target_keyword is title-shaped; keeping upfront keyword.",
                        {"rejected_keyword": top_keyword[:160], "kept_keyword": upfront_target_keyword},
                    )
                _trace(
                    "info",
                    "brainstorm",
                    "selected",
                    "Editorial angle selected for the article.",
                    {
                        "title": editorial_angle["title"],
                        "target_keyword": upfront_target_keyword,
                        "cache_hit": bool(brainstorm_payload.get("cache_hit")),
                        "excluded_count": int(brainstorm_payload.get("excluded_count") or 0),
                        "alternates": [
                            {"title": str(a.get("title") or ""), "target_keyword": str(a.get("target_keyword") or "")}
                            for a in angles[1:]
                            if isinstance(a, dict)
                        ],
                    },
                )

    # Listicle path needs an editorial_angle marker even when brainstorm was
    # skipped (explicit topic) -- the creator's contract_generator picks v2
    # listicle prompt only when editorial_angle.format == "listicle".
    if is_listicle_request:
        if editorial_angle is None:
            editorial_angle = {
                "title": "",
                "hook": "",
                "rationale": "",
                "format": "listicle",
            }
        else:
            editorial_angle.setdefault("format", "listicle")

    _trace(
        "info",
        "creator_v2",
        "start",
        "Calling creator /v2/run-pipeline with the chosen publisher locked in.",
        {
            "target_keyword": upfront_target_keyword,
            "publishing_site_url": selected_publish_site_url,
            "topic_will_be_derived": upfront_target_keyword is None,
            "article_format": article_format,
            "service_type": service_type,
        },
    )
    v2_response = call_creator_v2_pipeline(
        creator_endpoint=creator_endpoint,
        target_keyword=upfront_target_keyword,
        target_backlink_url=target_site_url,
        publishing_site_url=selected_publish_site_url or None,
        language=upfront_language,
        editorial_angle=editorial_angle,
        article_format=article_format,
        service_type=service_type,
        anchor_hint=anchor,
        timeout_seconds=creator_timeout_seconds,
    )

    # Resolve the keyword and language from whatever the creator decided. If
    # we sent a keyword, it comes back unchanged; if we sent None, the creator
    # derived one and put it in derived_topic.target_keyword.
    contract_for_select = (
        v2_response.get("contract") if isinstance(v2_response.get("contract"), dict) else {}
    )
    target_keyword = str(
        contract_for_select.get("target_keyword") or upfront_target_keyword or ""
    ).strip()
    article_language = str(
        contract_for_select.get("language") or v2_response.get("language") or "de"
    ).strip().lower()

    # Format-pin verification + telemetry. The creator pipeline already
    # hard-fails on format drift (PipelineError surfaces as AutomationError)
    # but we also emit a JobEvent so admins can see in the trace what the
    # contract actually returned and which item names were picked.
    returned_format = str(contract_for_select.get("format") or "narrative").strip().lower()
    listicle_plan = contract_for_select.get("listicle_plan") if isinstance(contract_for_select, dict) else None
    listicle_items = (
        list(listicle_plan.get("items") or []) if isinstance(listicle_plan, dict) else []
    )
    item_count = len(listicle_items)
    _trace(
        "info" if returned_format == article_format else "warning",
        "format_pin",
        "verified" if returned_format == article_format else "drift",
        f"Contract format={returned_format} (requested={article_format}); items={item_count}.",
        {
            "requested_format": article_format,
            "returned_format": returned_format,
            "item_count": item_count,
            "item_preview": [str(name)[:80] for name in listicle_items[:5]],
            "h1": str(contract_for_select.get("h1") or "")[:200],
        },
    )
    if is_listicle_request and returned_format != "listicle":
        # Belt-and-suspenders: the creator side already raises on this, but
        # if a stray response slips through with format mismatch we hard-fail
        # here too rather than ship a narrative article under a listicle job.
        raise AutomationError(
            f"Listicle was requested but contract returned format={returned_format!r}. "
            "Refusing to publish as a regular article."
        )

    if is_listicle_request:
        # Per-item summary so admins can see at a glance whether items have
        # the expected structure (verdict tag + bullets). Helps spot writer
        # regressions without cracking open the full pipeline payload.
        items_payload = v2_response.get("items") if isinstance(v2_response.get("items"), list) else []
        item_summary: List[Dict[str, Any]] = []
        for entry in items_payload:
            if not isinstance(entry, dict):
                continue
            body = str(entry.get("body_html") or "")
            item_summary.append({
                "rank": entry.get("rank"),
                "name": str(entry.get("name") or "")[:120],
                "word_count": entry.get("word_count"),
                "has_verdict": ("class=\"verdict\"" in body) or ("class='verdict'" in body),
                "li_count": body.count("<li"),
                "h3_count": body.count("<h3"),
            })
        _trace(
            "info",
            "listicle",
            "items_written",
            f"Listicle items written: {len(item_summary)}.",
            {"items": item_summary},
        )

    _trace(
        "info",
        "creator_v2",
        "complete",
        "Creator v2 pipeline returned.",
        {
            "skipped_voice_pass": bool(v2_response.get("skipped_voice_pass")),
            "skipped_judge": bool(v2_response.get("skipped_judge")),
        },
    )

    article_html_v2 = ""
    article_html_for_publish = ""
    article_html_block = v2_response.get("article_html")
    if isinstance(article_html_block, dict):
        article_html_v2 = str(article_html_block.get("final") or article_html_block.get("refined_body") or article_html_block.get("assembled") or "").strip()
        # Schema-free body for WP publish: NinjaFirewall (and similar WP
        # firewalls) blocks any POST whose content contains <script> tags,
        # so we strip the JSON-LD blocks. The schema-included `final` HTML
        # is still preserved in creator_output for the review surface and
        # for downstream re-injection if a sitewide schema plugin wants it.
        article_html_for_publish = str(article_html_block.get("refined_body") or article_html_block.get("assembled") or article_html_v2 or "").strip()
        article_html_for_publish = _strip_jsonld_script_blocks(article_html_for_publish)
    if not article_html_v2:
        raise AutomationError("Creator v2 pipeline returned no article HTML.")
    if not article_html_for_publish:
        article_html_for_publish = _strip_jsonld_script_blocks(article_html_v2)

    contract = v2_response.get("contract") if isinstance(v2_response.get("contract"), dict) else {}
    title = str(contract.get("h1") or "").strip() or target_keyword
    meta_title = str(contract.get("meta_title") or title).strip()
    meta_description = str(contract.get("meta_description") or "").strip()
    excerpt = meta_description[:220] if meta_description else ""
    slug = _slugify(str(contract.get("slug") or meta_title or title))

    if category_llm_enabled and selected_category_candidates and category_llm_api_key:
        try:
            llm_selected_ids = _select_categories_with_llm(
                title=title,
                excerpt=excerpt,
                clean_html=article_html_v2,
                category_candidates=selected_category_candidates,
                api_key=category_llm_api_key,
                base_url=category_llm_base_url,
                model=category_llm_model,
                max_categories=max(1, category_llm_max_categories),
                confidence_threshold=max(0.0, min(1.0, category_llm_confidence_threshold)),
                timeout_seconds=timeout_seconds,
            )
            selected_category_ids = llm_selected_ids
        except AutomationError as exc:
            _trace("warning", "categories", "llm_fallback", "Category LLM selection failed; using defaults.", {"error": str(exc)})

    image_prompt = _build_image_prompt_from_contract(contract)
    image_url: str = ""
    media_payload: Dict[str, Any] = {}
    if skip_image:
        _trace("info", "image", "skipped", "Featured image skipped (skip_image=True).")
    elif not leonardo_api_key:
        _trace(
            "warning",
            "image",
            "no_api_key",
            "LEONARDO_API_KEY not configured; publishing without a featured image.",
        )
    else:
        _trace("info", "image", "start", "Generating featured image via Leonardo.", {"prompt_chars": len(image_prompt)})
        try:
            image_url, media_payload = _generate_featured_image_for_v2(
                prompt=image_prompt,
                site_url=selected_publish_site_url,
                wp_rest_base=selected_wp_rest_base,
                wp_username=selected_wp_username,
                wp_app_password=selected_wp_app_password,
                title=title,
                leonardo_api_key=leonardo_api_key,
                leonardo_base_url=leonardo_base_url,
                leonardo_model_id=leonardo_model_id,
                image_width=image_width,
                image_height=image_height,
                timeout_seconds=timeout_seconds,
                poll_timeout_seconds=poll_timeout_seconds,
                poll_interval_seconds=poll_interval_seconds,
            )
            _trace(
                "info",
                "image",
                "ready",
                "Featured image generated and uploaded to WordPress.",
                {"media_id": media_payload.get("id")},
            )
        except AutomationError as exc:
            # Image generation is treated as a soft failure: the article still
            # publishes (matching legacy 4llm behaviour, where there was never
            # an image at all). The error is logged so we can spot Leonardo /
            # WP-media regressions without breaking the publish path.
            _trace(
                "warning",
                "image",
                "failed",
                "Featured image generation failed; publishing without an image.",
                {"error": str(exc)[:300]},
            )
            image_url = ""
            media_payload = {}

    featured_media_id: Optional[int] = 0 if existing_wp_post_id else None
    if media_payload.get("id") is not None:
        try:
            featured_media_id = int(media_payload["id"])
        except (TypeError, ValueError):
            featured_media_id = 0 if existing_wp_post_id else None

    clean_html = _strip_leading_h1_from_article_html(article_html_for_publish)
    if existing_wp_post_id:
        post_payload = wp_update_post(
            site_url=selected_publish_site_url,
            wp_rest_base=selected_wp_rest_base,
            wp_username=selected_wp_username,
            wp_app_password=selected_wp_app_password,
            post_id=existing_wp_post_id,
            title=title,
            clean_html=clean_html,
            excerpt=excerpt,
            slug=slug,
            featured_media_id=featured_media_id,
            post_status=post_status,
            author_id=selected_author_id,
            category_ids=selected_category_ids,
            timeout_seconds=timeout_seconds,
        )
        post_event_type = "wp_post_updated"
    else:
        post_payload = wp_create_post(
            site_url=selected_publish_site_url,
            wp_rest_base=selected_wp_rest_base,
            wp_username=selected_wp_username,
            wp_app_password=selected_wp_app_password,
            title=title,
            clean_html=clean_html,
            excerpt=excerpt,
            slug=slug,
            featured_media_id=featured_media_id,
            post_status=post_status,
            author_id=selected_author_id,
            category_ids=selected_category_ids,
            timeout_seconds=timeout_seconds,
        )
        post_event_type = "wp_post_created"

    creator_output = _build_creator_output_for_v2(
        v2_response=v2_response,
        target_site_url=target_site_url,
        selected_site_url=selected_publish_site_url,
        selected_site_id=selected_publish_site_id,
        target_keyword=target_keyword,
        article_html=article_html_v2,
    )
    # Expose the image prompt + result on creator_output so the worker fires
    # the same image_prompt_ok / image_generated JobEvents the converter flow
    # does. The worker reads phase6.featured_image.prompt for image_prompt_ok
    # and the top-level image_url for image_generated + Asset row.
    creator_output["phase6"] = {
        "featured_image": {
            "prompt": image_prompt,
            "alt_text": title,
            "image_url": image_url,
            "media_id": media_payload.get("id"),
        }
    }

    media_url: Optional[str] = None
    if media_payload:
        guid_value = media_payload.get("guid")
        media_url = media_payload.get("source_url")
        if not media_url and isinstance(guid_value, dict):
            media_url = guid_value.get("rendered")

    return {
        "creator_output": creator_output,
        "image_url": image_url,
        "media_payload": media_payload,
        "media_url": media_url,
        "post_payload": post_payload,
        "post_event_type": post_event_type,
        "selected_category_ids": selected_category_ids,
        "selected_site_id": selected_publish_site_id,
        "selected_site_url": selected_publish_site_url,
        "article_format": article_format,
        "service_type": service_type,
    }


def run_create_article_pipeline(
    *,
    creator_endpoint: str,
    target_site_url: str,
    publishing_site_url: str,
    publishing_site_id: Optional[str],
    client_target_site_id: Optional[str],
    anchor: Optional[str],
    topic: Optional[str],
    article_format: str = "narrative",
    service_type: str = "article",
    exclude_topics: Optional[List[str]] = None,
    recent_article_titles: Optional[List[str]] = None,
    internal_link_inventory: Optional[List[Dict[str, Any]]] = None,
    publishing_candidates: Optional[List[Dict[str, Any]]] = None,
    phase1_cache_payload: Optional[Dict[str, Any]] = None,
    phase1_cache_content_hash: Optional[str] = None,
    phase2_cache_payload: Optional[Dict[str, Any]] = None,
    phase2_cache_content_hash: Optional[str] = None,
    target_profile_payload: Optional[Dict[str, Any]] = None,
    target_profile_content_hash: Optional[str] = None,
    publishing_profile_payload: Optional[Dict[str, Any]] = None,
    publishing_profile_content_hash: Optional[str] = None,
    on_phase: Optional[Callable[[int, str, int], None]] = None,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    existing_wp_post_id: Optional[int],
    post_status: str,
    author_id: int,
    category_ids: Optional[List[int]],
    category_candidates: Optional[List[Dict[str, Any]]],
    timeout_seconds: int,
    creator_timeout_seconds: int = DEFAULT_CREATOR_TIMEOUT_SECONDS,
    poll_timeout_seconds: int,
    poll_interval_seconds: int,
    image_width: int,
    image_height: int,
    leonardo_api_key: str = "",
    leonardo_base_url: str = DEFAULT_LEONARDO_BASE_URL,
    leonardo_model_id: str = DEFAULT_LEONARDO_MODEL_ID,
    category_llm_enabled: bool,
    category_llm_api_key: str,
    category_llm_base_url: str,
    category_llm_model: str,
    category_llm_max_categories: int,
    category_llm_confidence_threshold: float,
    trace_event: Optional[Callable[[str, str, str, str, Optional[Dict[str, Any]]], None]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> Dict[str, Any]:
    return _run_create_article_pipeline_v2(
        creator_endpoint=creator_endpoint,
        target_site_url=target_site_url,
        publishing_site_url=publishing_site_url,
        publishing_site_id=publishing_site_id,
        publishing_candidates=publishing_candidates,
        internal_link_inventory=internal_link_inventory,
        target_profile_payload=target_profile_payload,
        publishing_profile_payload=publishing_profile_payload,
        anchor=anchor,
        topic=topic,
        article_format=article_format,
        service_type=service_type,
        exclude_topics=exclude_topics,
        site_url=site_url,
        wp_rest_base=wp_rest_base,
        wp_username=wp_username,
        wp_app_password=wp_app_password,
        existing_wp_post_id=existing_wp_post_id,
        post_status=post_status,
        author_id=author_id,
        category_ids=category_ids,
        category_candidates=category_candidates,
        timeout_seconds=timeout_seconds,
        creator_timeout_seconds=creator_timeout_seconds,
        category_llm_enabled=category_llm_enabled,
        category_llm_api_key=category_llm_api_key,
        category_llm_base_url=category_llm_base_url,
        category_llm_model=category_llm_model,
        category_llm_max_categories=category_llm_max_categories,
        category_llm_confidence_threshold=category_llm_confidence_threshold,
        leonardo_api_key=leonardo_api_key,
        leonardo_base_url=leonardo_base_url,
        leonardo_model_id=leonardo_model_id,
        image_width=image_width,
        image_height=image_height,
        poll_timeout_seconds=poll_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        trace_event=trace_event,
    )


def run_submit_article_pipeline(
    *,
    source_url: str,
    publishing_site: str,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    existing_wp_post_id: Optional[int],
    post_status: str,
    author_id: int,
    category_ids: Optional[List[int]],
    category_candidates: Optional[List[Dict[str, Any]]],
    converter_endpoint: str,
    leonardo_api_key: str,
    leonardo_base_url: str,
    leonardo_model_id: str,
    timeout_seconds: int,
    poll_timeout_seconds: int,
    poll_interval_seconds: int,
    image_width: int,
    image_height: int,
    category_llm_enabled: bool,
    category_llm_api_key: str,
    category_llm_base_url: str,
    category_llm_model: str,
    category_llm_max_categories: int,
    category_llm_confidence_threshold: float,
) -> Dict[str, Any]:
    converted = call_converter(
        source_url=source_url,
        publishing_site=publishing_site,
        converter_endpoint=converter_endpoint,
        timeout_seconds=timeout_seconds,
    )

    sizes_to_try = [
        (max(256, image_width), max(256, image_height)),
        (768, 432),
        (640, 360),
        (512, 288),
    ]
    unique_sizes: list[tuple[int, int]] = []
    for size in sizes_to_try:
        if size not in unique_sizes:
            unique_sizes.append(size)

    image_url: str = ""
    media_payload: Dict[str, Any] = {}
    image_bytes: bytes = b""
    file_name = ""
    content_type = "application/octet-stream"
    last_upload_error: Optional[AutomationError] = None

    for idx, (width, height) in enumerate(unique_sizes):
        image_url = generate_image_via_leonardo(
            prompt=converted["image_prompt"],
            api_key=leonardo_api_key,
            timeout_seconds=timeout_seconds,
            poll_timeout_seconds=poll_timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            model_id=leonardo_model_id,
            width=width,
            height=height,
            base_url=leonardo_base_url,
        )
        image_bytes, file_name, content_type = download_binary_file(
            image_url,
            timeout_seconds=timeout_seconds,
        )
        try:
            media_payload = wp_create_media_item(
                site_url=site_url,
                wp_rest_base=wp_rest_base,
                wp_username=wp_username,
                wp_app_password=wp_app_password,
                data=image_bytes,
                file_name=file_name,
                content_type=content_type,
                title=converted["title"],
                timeout_seconds=timeout_seconds,
            )
            break
        except AutomationError as exc:
            error_text = str(exc)
            if "HTTP 413" in error_text and idx < len(unique_sizes) - 1:
                last_upload_error = exc
                continue
            raise
    else:
        if last_upload_error:
            raise last_upload_error
        raise AutomationError("WordPress media upload failed for all image size attempts.")

    selected_category_ids = list(category_ids or [])
    if category_llm_enabled and category_candidates:
        if category_llm_api_key:
            try:
                llm_selected_ids = _select_categories_with_llm(
                    title=converted["title"],
                    excerpt=converted["excerpt"],
                    clean_html=converted["clean_html"],
                    category_candidates=category_candidates,
                    api_key=category_llm_api_key,
                    base_url=category_llm_base_url,
                    model=category_llm_model,
                    max_categories=max(1, category_llm_max_categories),
                    confidence_threshold=max(0.0, min(1.0, category_llm_confidence_threshold)),
                    timeout_seconds=timeout_seconds,
                )
                selected_category_ids = llm_selected_ids
            except AutomationError as exc:
                logger.warning(
                    "automation.category_llm.fallback reason=%s defaults_count=%s",
                    str(exc),
                    len(selected_category_ids),
                )
        else:
            logger.warning(
                "automation.category_llm.fallback reason=missing_api_key defaults_count=%s",
                len(selected_category_ids),
            )

    if existing_wp_post_id:
        post_payload = wp_update_post(
            site_url=site_url,
            wp_rest_base=wp_rest_base,
            wp_username=wp_username,
            wp_app_password=wp_app_password,
            post_id=existing_wp_post_id,
            title=converted["title"],
            clean_html=converted["clean_html"],
            excerpt=converted["excerpt"],
            slug=converted["slug"],
            featured_media_id=int(media_payload["id"]),
            post_status=post_status,
            author_id=author_id,
            category_ids=selected_category_ids,
            timeout_seconds=timeout_seconds,
        )
        post_event_type = "wp_post_updated"
    else:
        post_payload = wp_create_post(
            site_url=site_url,
            wp_rest_base=wp_rest_base,
            wp_username=wp_username,
            wp_app_password=wp_app_password,
            title=converted["title"],
            clean_html=converted["clean_html"],
            excerpt=converted["excerpt"],
            slug=converted["slug"],
            featured_media_id=int(media_payload["id"]),
            post_status=post_status,
            author_id=author_id,
            category_ids=selected_category_ids,
            timeout_seconds=timeout_seconds,
        )
        post_event_type = "wp_post_created"

    guid_value = media_payload.get("guid")
    media_url = media_payload.get("source_url")
    if not media_url and isinstance(guid_value, dict):
        media_url = guid_value.get("rendered")

    return {
        "converted": converted,
        "image_url": image_url,
        "media_payload": media_payload,
        "media_url": media_url,
        "post_payload": post_payload,
        "post_event_type": post_event_type,
        "selected_category_ids": selected_category_ids,
    }


def check_creator_health(*, creator_endpoint: str, timeout_seconds: int) -> Dict[str, Any]:
    url = creator_endpoint.rstrip("/") + "/health"
    return _request_json(
        "GET",
        url,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def get_runtime_config() -> Dict[str, Any]:
    def read_int(name: str, default: int) -> int:
        raw = os.getenv(name, str(default)).strip()
        try:
            return int(raw)
        except ValueError as exc:
            raise AutomationError(f"{name} must be an integer, got '{raw}'.") from exc

    def read_float(name: str, default: float) -> float:
        raw = os.getenv(name, str(default)).strip()
        try:
            return float(raw)
        except ValueError as exc:
            raise AutomationError(f"{name} must be a number, got '{raw}'.") from exc

    explicit_category_llm_key = os.getenv("AUTOMATION_CATEGORY_LLM_API_KEY", "").strip()
    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
    anthropic_api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    category_llm_api_key = explicit_category_llm_key or openai_api_key or anthropic_api_key

    explicit_base_url = os.getenv("AUTOMATION_CATEGORY_LLM_BASE_URL", "").strip()
    if explicit_base_url:
        category_llm_base_url = explicit_base_url
    elif anthropic_api_key and not openai_api_key:
        category_llm_base_url = DEFAULT_CATEGORY_LLM_ANTHROPIC_BASE_URL
    else:
        category_llm_base_url = DEFAULT_CATEGORY_LLM_OPENAI_BASE_URL

    explicit_model = os.getenv("AUTOMATION_CATEGORY_LLM_MODEL", "").strip()
    if explicit_model:
        category_llm_model = explicit_model
    elif "anthropic" in category_llm_base_url.lower():
        category_llm_model = DEFAULT_CATEGORY_LLM_ANTHROPIC_MODEL
    else:
        category_llm_model = DEFAULT_CATEGORY_LLM_OPENAI_MODEL

    return {
        "converter_endpoint": os.getenv("AUTOMATION_CONVERTER_ENDPOINT", DEFAULT_CONVERTER_ENDPOINT).strip(),
        "creator_endpoint": os.getenv("AUTOMATION_CREATOR_ENDPOINT", DEFAULT_CREATOR_ENDPOINT).strip(),
        "leonardo_api_key": os.getenv("LEONARDO_API_KEY", "").strip(),
        "leonardo_base_url": os.getenv("LEONARDO_BASE_URL", DEFAULT_LEONARDO_BASE_URL).strip(),
        "leonardo_model_id": os.getenv("LEONARDO_MODEL_ID", DEFAULT_LEONARDO_MODEL_ID).strip(),
        "image_width": read_int("AUTOMATION_IMAGE_WIDTH", DEFAULT_IMAGE_WIDTH),
        "image_height": read_int("AUTOMATION_IMAGE_HEIGHT", DEFAULT_IMAGE_HEIGHT),
        "timeout_seconds": read_int("AUTOMATION_REQUEST_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS),
        "creator_timeout_seconds": read_int("AUTOMATION_CREATOR_TIMEOUT_SECONDS", DEFAULT_CREATOR_TIMEOUT_SECONDS),
        "poll_timeout_seconds": read_int("AUTOMATION_IMAGE_POLL_TIMEOUT_SECONDS", DEFAULT_IMAGE_POLL_TIMEOUT_SECONDS),
        "poll_interval_seconds": read_int(
            "AUTOMATION_IMAGE_POLL_INTERVAL_SECONDS",
            DEFAULT_IMAGE_POLL_INTERVAL_SECONDS,
        ),
        "category_llm_enabled": _read_bool_env("AUTOMATION_CATEGORY_LLM_ENABLED", True),
        "category_llm_api_key": category_llm_api_key,
        "category_llm_base_url": category_llm_base_url,
        "category_llm_model": category_llm_model,
        "category_llm_max_categories": read_int(
            "AUTOMATION_CATEGORY_LLM_MAX_CATEGORIES",
            DEFAULT_CATEGORY_LLM_MAX_CATEGORIES,
        ),
        "category_llm_confidence_threshold": read_float(
            "AUTOMATION_CATEGORY_LLM_CONFIDENCE_THRESHOLD",
            DEFAULT_CATEGORY_LLM_CONFIDENCE_THRESHOLD,
        ),
        "default_author_id": read_int("AUTOMATION_POST_AUTHOR_ID", DEFAULT_AUTHOR_ID),
        "default_post_status": os.getenv("AUTOMATION_POST_STATUS", DEFAULT_POST_STATUS).strip().lower(),
    }
