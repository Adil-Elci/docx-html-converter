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
from bs4 import BeautifulSoup

from .creator_prompt_trace import ensure_prompt_trace_in_creator_output
from .four_llm_schemas import CompetitorReference, ContentBrief, KeywordMetric, LinkCandidate, QualityCheckResult, QualityReport


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


def _request_json(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    allow_redirects: bool = True,
) -> Dict[str, Any]:
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
        raise AutomationError(f"Request failed for {url}: {exc}") from exc

    if 300 <= response.status_code < 400:
        location = response.headers.get("Location", "")
        raise AutomationError(f"Unexpected redirect from {url} to {location}.")

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


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _read_pipeline_mode() -> str:
    raw = os.getenv("CREATOR_PIPELINE_MODE", "legacy").strip().lower()
    return raw if raw in {"legacy", "supervisor", "4llm"} else "legacy"


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


def call_creator_service(
    *,
    creator_endpoint: str,
    target_site_url: str,
    publishing_site_url: str,
    publishing_site_id: Optional[str],
    client_target_site_id: Optional[str],
    anchor: Optional[str],
    topic: Optional[str],
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
    timeout_seconds: int,
    on_phase: Optional[Callable[[int, str, int], None]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> Dict[str, Any]:
    if not creator_endpoint:
        raise AutomationError("Creator endpoint is not configured.")
    body: Dict[str, Any] = {
        "target_site_url": target_site_url,
        "publishing_site_url": publishing_site_url,
    }
    if publishing_site_id:
        body["publishing_site_id"] = publishing_site_id
    if client_target_site_id:
        body["client_target_site_id"] = client_target_site_id
    if anchor:
        body["anchor"] = anchor
    if topic:
        body["topic"] = topic
    if exclude_topics:
        body["exclude_topics"] = exclude_topics
    if recent_article_titles:
        body["recent_article_titles"] = recent_article_titles
    if internal_link_inventory:
        body["internal_link_inventory"] = internal_link_inventory
    if publishing_candidates:
        body["publishing_candidates"] = [
            {
                "site_url": str(candidate.get("site_url") or "").strip(),
                "site_id": str(candidate.get("site_id") or "").strip() or None,
                "fit_score": candidate.get("fit_score"),
                "notes": [
                    str(item).strip()
                    for item in (candidate.get("notes") or [])
                    if str(item).strip()
                ],
                "internal_link_inventory": list(candidate.get("internal_link_inventory") or []),
                "publishing_profile": {
                    "content_hash": str(candidate.get("publishing_profile_content_hash") or "").strip(),
                    "payload": dict(candidate.get("publishing_profile_payload") or {}),
                },
            }
            for candidate in publishing_candidates
            if isinstance(candidate, dict)
            and str(candidate.get("site_url") or "").strip()
            and isinstance(candidate.get("publishing_profile_payload"), dict)
            and str(candidate.get("publishing_profile_content_hash") or "").strip()
        ]
    if phase1_cache_payload and phase1_cache_content_hash:
        body["phase1_cache"] = {
            "content_hash": phase1_cache_content_hash,
            "payload": phase1_cache_payload,
        }
    if phase2_cache_payload and phase2_cache_content_hash:
        body["phase2_cache"] = {
            "content_hash": phase2_cache_content_hash,
            "payload": phase2_cache_payload,
        }
    if target_profile_payload and target_profile_content_hash:
        body["target_profile"] = {
            "content_hash": target_profile_content_hash,
            "payload": target_profile_payload,
        }
    if publishing_profile_payload and publishing_profile_content_hash:
        body["publishing_profile"] = {
            "content_hash": publishing_profile_content_hash,
            "payload": publishing_profile_payload,
        }

    if on_phase is not None:
        return _call_creator_stream(creator_endpoint, body, timeout_seconds, on_phase, should_cancel)

    url = creator_endpoint.rstrip("/") + "/create"
    return _request_json(
        "POST",
        url,
        json_body=body,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
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


def _call_creator_stream(
    creator_endpoint: str,
    body: Dict[str, Any],
    timeout_seconds: int,
    on_phase: Callable[[int, str, int], None],
    should_cancel: Optional[Callable[[], bool]] = None,
) -> Dict[str, Any]:
    """Call the creator /create-stream SSE endpoint and forward phase events."""
    url = creator_endpoint.rstrip("/") + "/create-stream"
    try:
        resp = requests.post(
            url,
            json=body,
            timeout=timeout_seconds,
            stream=True,
            headers={"Accept": "text/event-stream"},
        )
    except requests.RequestException as exc:
        raise AutomationError(f"Request failed for {url}: {exc}") from exc
    if resp.status_code >= 400:
        resp_body = resp.text[:600]
        raise AutomationError(f"HTTP {resp.status_code} from {url}: {resp_body}")

    result: Optional[Dict[str, Any]] = None
    error_msg: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    current_event = "message"
    current_data_lines: List[str] = []

    for raw_line in resp.iter_lines(decode_unicode=True):
        if should_cancel is not None and should_cancel():
            resp.close()
            raise AutomationError("Job canceled by client.")
        if raw_line is None:
            raw_line = ""
        line = raw_line

        if line.startswith("event:"):
            current_event = line[len("event:"):].strip()
            continue
        if line.startswith("data:"):
            current_data_lines.append(line[len("data:"):].strip())
            continue
        if line == "" and current_data_lines:
            data_str = "\n".join(current_data_lines)
            current_data_lines = []
            try:
                data = json.loads(data_str)
            except (json.JSONDecodeError, ValueError):
                current_event = "message"
                continue
            if current_event == "progress":
                phase = data.get("phase", 0)
                label = data.get("label", "")
                percent = data.get("percent", 0)
                try:
                    on_phase(phase, label, percent)
                except Exception:
                    logger.warning("creator.stream.on_phase_error phase=%s", phase, exc_info=True)
            elif current_event == "complete":
                result = data.get("data") if isinstance(data.get("data"), dict) else data
            elif current_event == "error":
                error_msg = data.get("error", "creator_stream_error")
                error_details = data.get("details") if isinstance(data.get("details"), dict) else None
            current_event = "message"

    resp.close()

    if error_msg:
        raise AutomationError(f"Creator pipeline failed: {error_msg}", details=error_details)
    if result is None:
        raise AutomationError("Creator stream ended without a result.")
    if not isinstance(result, dict):
        raise AutomationError(f"Expected JSON object from creator stream, got {type(result).__name__}.")
    return result


def _call_creator_4llm_endpoint(
    *,
    creator_endpoint: str,
    path: str,
    body: Dict[str, Any],
    timeout_seconds: int,
) -> Dict[str, Any]:
    if not creator_endpoint:
        raise AutomationError("Creator endpoint is not configured.")
    return _request_json(
        "POST",
        creator_endpoint.rstrip("/") + path,
        json_body=body,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def call_creator_site_understanding(
    *,
    creator_endpoint: str,
    target_site_url: str,
    timeout_seconds: int,
    max_pages: int = 10,
) -> Dict[str, Any]:
    return _call_creator_4llm_endpoint(
        creator_endpoint=creator_endpoint,
        path="/site-understanding",
        body={"target_site_url": target_site_url, "max_pages": max_pages},
        timeout_seconds=timeout_seconds,
    )


def call_creator_draft_article(
    *,
    creator_endpoint: str,
    content_brief: Dict[str, Any],
    timeout_seconds: int,
) -> Dict[str, Any]:
    return _call_creator_4llm_endpoint(
        creator_endpoint=creator_endpoint,
        path="/draft-article",
        body={"content_brief": content_brief},
        timeout_seconds=timeout_seconds,
    )


def call_creator_integrate_links(
    *,
    creator_endpoint: str,
    article_markdown: str,
    internal_links: List[Dict[str, Any]],
    external_links: List[Dict[str, Any]],
    timeout_seconds: int,
) -> Dict[str, Any]:
    return _call_creator_4llm_endpoint(
        creator_endpoint=creator_endpoint,
        path="/integrate-links",
        body={
            "article_markdown": article_markdown,
            "internal_links": internal_links,
            "external_links": external_links,
        },
        timeout_seconds=timeout_seconds,
    )


def call_creator_generate_meta(
    *,
    creator_endpoint: str,
    target_keyword: str,
    article_title: str,
    article_intro: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    return _call_creator_4llm_endpoint(
        creator_endpoint=creator_endpoint,
        path="/generate-meta",
        body={
            "target_keyword": target_keyword,
            "article_title": article_title,
            "article_intro": article_intro,
        },
        timeout_seconds=timeout_seconds,
    )


def _normalize_text_tokens(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", str(value or "").lower())
    cleaned = cleaned.translate(str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"}))
    cleaned = re.sub(r"[^\w\s-]", " ", cleaned)
    cleaned = re.sub(r"[_-]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _token_set(value: str) -> set[str]:
    return {token for token in re.findall(r"\b[a-z0-9]{3,}\b", _normalize_text_tokens(value))}


def _similarity_score(text: str, reference: str) -> float:
    left = _token_set(text)
    right = _token_set(reference)
    if not left or not right:
        return 0.0
    overlap = len(left & right)
    return overlap / float(max(len(right), 1))


def _flatten_candidate_text(candidate: Dict[str, Any]) -> str:
    profile = candidate.get("publishing_profile_payload") or {}
    notes = " ".join(str(item).strip() for item in (candidate.get("notes") or []) if str(item).strip())
    profile_text = " ".join(
        str(profile.get(key) or "").strip()
        for key in ("primary_context", "secondary_contexts", "taxonomy_terms", "editorial_terms")
    )
    inventory_titles = " ".join(
        str(item.get("title") or "").strip()
        for item in (candidate.get("internal_link_inventory") or [])[:8]
        if isinstance(item, dict)
    )
    return " ".join([notes, profile_text, inventory_titles]).strip()


def _select_publish_target_for_4llm(
    *,
    site_understanding: Dict[str, Any],
    fallback_target: Dict[str, Any],
    publishing_candidates: Optional[List[Dict[str, Any]]],
) -> Dict[str, Any]:
    topic_text = " ".join(
        [
            str(site_understanding.get("primary_niche") or ""),
            str(site_understanding.get("main_topic") or ""),
            " ".join(str(item) for item in (site_understanding.get("seed_keywords") or [])),
        ]
    ).strip()
    best = dict(fallback_target)
    best_score = -1.0
    for candidate in publishing_candidates or []:
        candidate_text = _flatten_candidate_text(candidate)
        score = float(candidate.get("fit_score") or 0) / 100.0
        score += _similarity_score(candidate_text, topic_text) * 3.0
        language = str((candidate.get("publishing_profile_payload") or {}).get("language") or "").strip().lower()
        target_language = str(site_understanding.get("language") or "").strip().lower()
        if language and target_language and language == target_language:
            score += 1.0
        if score > best_score:
            best_score = score
            best = dict(candidate)
    return best


def _safe_get(url: str, timeout_seconds: int) -> requests.Response:
    try:
        response = requests.get(
            url,
            timeout=timeout_seconds,
            headers={"User-Agent": "portal-backend-4llm/1.0"},
        )
    except requests.RequestException as exc:
        raise AutomationError(f"Request failed for {url}: {exc}") from exc
    if response.status_code >= 400:
        raise AutomationError(f"HTTP {response.status_code} from {url}: {response.text[:200]}")
    return response


def _dataforseo_auth() -> tuple[str, str]:
    login = os.getenv("DATAFORSEO_LOGIN", "").strip()
    password = os.getenv("DATAFORSEO_PASSWORD", "").strip()
    if not login or not password:
        raise AutomationError("DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD are required in 4llm mode.")
    return login, password


def _call_dataforseo_keyword(
    *,
    keyword: str,
    timeout_seconds: int,
) -> KeywordMetric:
    login, password = _dataforseo_auth()
    endpoint = os.getenv(
        "DATAFORSEO_SERP_ENDPOINT",
        "https://api.dataforseo.com/v3/serp/google/organic/live/advanced",
    ).strip()
    body = [
        {
            "keyword": keyword,
            "language_code": os.getenv("DATAFORSEO_LANGUAGE_CODE", "de"),
            "location_code": int(os.getenv("DATAFORSEO_LOCATION_CODE", "2276")),
            "device": "desktop",
            "os": "windows",
            "depth": 10,
        }
    ]
    try:
        response = requests.post(endpoint, auth=(login, password), json=body, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise AutomationError(f"DataForSEO request failed: {exc}") from exc
    if response.status_code >= 400:
        raise AutomationError(f"DataForSEO HTTP {response.status_code}: {response.text[:300]}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise AutomationError("DataForSEO returned non-JSON response.") from exc
    tasks = payload.get("tasks") if isinstance(payload, dict) else None
    result = ((tasks or [{}])[0] or {}).get("result") if isinstance((tasks or [{}])[0], dict) else None
    first_result = (result or [{}])[0] if isinstance(result, list) and result else {}
    items = first_result.get("items") if isinstance(first_result, dict) else []
    top_urls: List[str] = []
    if isinstance(items, list):
        for item in items[:10]:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "").strip()
            if url:
                top_urls.append(url)
    search_volume = int(first_result.get("search_volume") or 0) if isinstance(first_result, dict) else 0
    keyword_difficulty = float(first_result.get("keyword_difficulty") or 0.0) if isinstance(first_result, dict) else 0.0
    score = float(search_volume) / float(keyword_difficulty + 1.0)
    return KeywordMetric(
        keyword=keyword,
        search_volume=search_volume,
        keyword_difficulty=keyword_difficulty,
        score=score,
        top_urls=top_urls,
    )


def _derive_content_format(title: str, h2s: List[str]) -> str:
    normalized = _normalize_text_tokens(" ".join([title] + h2s))
    if any(token in normalized for token in ("vergleich", "vs", "oder", "unterschied")):
        return "comparison"
    if any(token in normalized for token in ("schritt", "anleitung", "so geht", "how to")):
        return "how-to"
    if any(token in normalized for token in ("liste", "tipps", "ideen", "checkliste")):
        return "listicle"
    return "guide"


def _scrape_competitor_reference(url: str, *, timeout_seconds: int) -> CompetitorReference:
    response = _safe_get(url, timeout_seconds)
    soup = BeautifulSoup(response.text, "lxml")
    title = _normalize_text_tokens(soup.title.get_text(" ", strip=True)) if soup.title else ""
    h1_node = soup.find("h1")
    h1 = h1_node.get_text(" ", strip=True) if h1_node else ""
    h2s = [_normalize_whitespace(node.get_text(" ", strip=True)) for node in soup.find_all("h2")][:10]
    h3s = [_normalize_whitespace(node.get_text(" ", strip=True)) for node in soup.find_all("h3")][:12]
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = _normalize_whitespace(soup.get_text(" ", strip=True))
    key_topics = [token for token in list(_token_set(" ".join(h2s + h3s)))[:8]]
    return CompetitorReference(
        url=url,
        title=title or h1 or url,
        h1=h1,
        h2s=h2s,
        h3s=h3s,
        word_count=len(text.split()),
        content_format=_derive_content_format(title or h1, h2s),
        key_topics=key_topics,
    )


def _select_target_keyword(site_understanding: Dict[str, Any], *, timeout_seconds: int) -> tuple[KeywordMetric, List[CompetitorReference]]:
    seed_keywords = [str(item).strip() for item in (site_understanding.get("seed_keywords") or []) if str(item).strip()]
    if not seed_keywords:
        seed_keywords = [
            str(site_understanding.get("main_topic") or "").strip(),
            str(site_understanding.get("primary_niche") or "").strip(),
        ]
    metrics: List[KeywordMetric] = []
    for keyword in seed_keywords[:10]:
        metric = _call_dataforseo_keyword(keyword=keyword, timeout_seconds=timeout_seconds)
        metrics.append(metric)
    if not metrics:
        raise AutomationError("No keyword research results were returned.")
    selected = max(metrics, key=lambda item: item.score)
    competitor_references: List[CompetitorReference] = []
    for url in list(selected.top_urls)[:5]:
        try:
            competitor_references.append(_scrape_competitor_reference(url, timeout_seconds=timeout_seconds))
        except AutomationError:
            logger.warning("automation.4llm.competitor_scrape_failed url=%s", url)
    return selected, competitor_references


def _infer_search_intent(keyword: str, references: List[CompetitorReference]) -> str:
    normalized = _normalize_text_tokens(keyword + " " + " ".join(ref.title for ref in references))
    if any(token in normalized for token in ("kaufen", "preis", "kosten", "angebot")):
        return "transactional"
    if any(token in normalized for token in ("vergleich", "beste", "test", "bewertung")):
        return "commercial"
    if any(token in normalized for token in ("login", "konto", "kontakt")):
        return "navigational"
    return "informational"


def _infer_recommended_format(keyword: str, references: List[CompetitorReference]) -> str:
    if references:
        formats = [ref.content_format for ref in references if ref.content_format]
        if formats:
            return max(set(formats), key=formats.count)
    return _derive_content_format(keyword, [])


def _select_internal_link_candidates(
    *,
    inventory: List[Dict[str, Any]],
    topic_text: str,
    limit: int = 5,
) -> List[LinkCandidate]:
    ranked: List[tuple[float, Dict[str, Any]]] = []
    for item in inventory:
        title = str(item.get("title") or "")
        excerpt = str(item.get("excerpt") or "")
        score = _similarity_score(f"{title} {excerpt}", topic_text)
        if score <= 0:
            continue
        ranked.append((score, item))
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    out: List[LinkCandidate] = []
    for score, item in ranked[:limit]:
        out.append(
            LinkCandidate(
                url=str(item.get("url") or ""),
                title=str(item.get("title") or ""),
                excerpt=str(item.get("excerpt") or ""),
                relevance_score=min(1.0, round(score, 4)),
                link_type="internal",
                target_kind="owned_network",
            )
        )
    return out


def _select_target_page_candidates(
    *,
    site_understanding: Dict[str, Any],
    topic_text: str,
    limit: int = 3,
) -> List[LinkCandidate]:
    ranked: List[tuple[float, Dict[str, Any]]] = []
    for page in site_understanding.get("scraped_pages") or []:
        if not isinstance(page, dict):
            continue
        text = " ".join(
            [
                str(page.get("title") or ""),
                str(page.get("h1") or ""),
                str(page.get("text_excerpt") or ""),
            ]
        )
        score = _similarity_score(text, topic_text)
        if score <= 0:
            continue
        ranked.append((score, page))
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    out: List[LinkCandidate] = []
    for score, page in ranked[:limit]:
        out.append(
            LinkCandidate(
                url=str(page.get("url") or ""),
                title=str(page.get("title") or page.get("h1") or ""),
                excerpt=str(page.get("text_excerpt") or ""),
                relevance_score=min(1.0, round(score, 4)),
                link_type="target_backlink",
                target_kind="target_site",
            )
        )
    return out


def _select_cross_network_candidates(
    *,
    publishing_candidates: List[Dict[str, Any]],
    selected_site_url: str,
    topic_text: str,
    limit: int = 2,
) -> List[LinkCandidate]:
    ranked: List[tuple[float, Dict[str, Any]]] = []
    for candidate in publishing_candidates:
        if _normalize_site_selection_url(str(candidate.get("site_url") or "")) == _normalize_site_selection_url(selected_site_url):
            continue
        for item in candidate.get("internal_link_inventory") or []:
            if not isinstance(item, dict):
                continue
            text = f"{item.get('title') or ''} {item.get('excerpt') or ''}"
            score = _similarity_score(text, topic_text)
            if score <= 0:
                continue
            ranked.append((score, item))
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    out: List[LinkCandidate] = []
    seen: set[str] = set()
    for score, item in ranked:
        url = str(item.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(
            LinkCandidate(
                url=url,
                title=str(item.get("title") or ""),
                excerpt=str(item.get("excerpt") or ""),
                relevance_score=min(1.0, round(score, 4)),
                link_type="external",
                target_kind="owned_network",
            )
        )
        if len(out) >= limit:
            break
    return out


def _build_outline(keyword: str, references: List[CompetitorReference]) -> List[str]:
    topic = keyword.strip()
    reference_topics: List[str] = []
    for reference in references[:3]:
        for heading in reference.h2s[:3]:
            if heading and heading not in reference_topics:
                reference_topics.append(heading)
    outline = [
        "Einleitung",
        f"{topic}: Wichtige Grundlagen",
        reference_topics[0] if len(reference_topics) > 0 else f"{topic}: Praktische Kriterien",
        reference_topics[1] if len(reference_topics) > 1 else f"{topic}: Häufige Fehler vermeiden",
        "FAQ",
        "Fazit",
    ]
    return outline[:6]


def _recommended_word_count(references: List[CompetitorReference]) -> int:
    counts = [ref.word_count for ref in references if ref.word_count > 0]
    if not counts:
        return DEFAULT_4LLM_WORD_COUNT
    average = sum(counts) / float(len(counts))
    bounded = max(900, min(2600, int(round(average / 50.0) * 50)))
    return bounded


def _build_content_brief_4llm(
    *,
    target_site_url: str,
    selected_target: Dict[str, Any],
    site_understanding: Dict[str, Any],
    keyword_metric: KeywordMetric,
    competitor_references: List[CompetitorReference],
    internal_links: List[LinkCandidate],
    external_links: List[LinkCandidate],
) -> ContentBrief:
    keyword = keyword_metric.keyword
    topic = str(site_understanding.get("main_topic") or keyword).strip()
    intent = _infer_search_intent(keyword, competitor_references)
    recommended_format = _infer_recommended_format(keyword, competitor_references)
    outline = _build_outline(keyword, competitor_references)
    title = f"{topic}: {keyword} sinnvoll einordnen"
    key_topics = list(dict.fromkeys(
        [topic, keyword] + [topic for ref in competitor_references for topic in ref.key_topics[:2]]
    ))
    return ContentBrief(
        target_keyword=keyword,
        secondary_keywords=[str(item).strip() for item in (site_understanding.get("seed_keywords") or []) if str(item).strip() and str(item).strip() != keyword][:5],
        search_intent=intent,
        recommended_format=recommended_format,
        recommended_word_count=_recommended_word_count(competitor_references),
        tone=str(site_understanding.get("content_tone") or "informativ").strip() or "informativ",
        target_audience=str(site_understanding.get("target_audience") or "Leserinnen und Leser").strip(),
        suggested_title=title,
        outline=outline,
        key_topics_to_cover=key_topics[:8],
        internal_link_candidates=internal_links,
        external_link_candidates=external_links,
        competitor_references=competitor_references,
        target_site_url=target_site_url,
        publishing_site_url=str(selected_target.get("site_url") or ""),
        publishing_site_id=str(selected_target.get("site_id") or "").strip() or None,
        target_site_language=str(site_understanding.get("language") or "de").strip() or "de",
        seed_keywords=[str(item).strip() for item in (site_understanding.get("seed_keywords") or []) if str(item).strip()][:10],
        chosen_topic=topic,
        notes=[
            f"Primary niche: {str(site_understanding.get('primary_niche') or '').strip()}",
            f"Main topic: {topic}",
        ],
    )


def _extract_markdown_title(markdown: str) -> str:
    for line in (markdown or "").splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    return ""


def _extract_markdown_intro(markdown: str) -> str:
    blocks = [block.strip() for block in re.split(r"\n\s*\n", markdown or "") if block.strip()]
    for block in blocks:
        if block.startswith("#"):
            continue
        return re.sub(r"\s+", " ", block).strip()
    return ""


def _extract_markdown_h2s(markdown: str) -> List[str]:
    return [line.strip()[3:].strip() for line in (markdown or "").splitlines() if line.strip().startswith("## ")]


def _extract_plain_text(markdown: str) -> str:
    text = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", markdown or "")
    text = re.sub(r"^#+\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"`{1,3}", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _keyword_density(text: str, keyword: str) -> float:
    normalized_text = _normalize_text_tokens(text)
    normalized_keyword = _normalize_text_tokens(keyword)
    if not normalized_text or not normalized_keyword:
        return 0.0
    occurrences = normalized_text.count(normalized_keyword)
    words = max(1, len(normalized_text.split()))
    return occurrences / float(words)


def _extract_markdown_link_targets(markdown: str) -> List[str]:
    return [url.strip() for _, url in re.findall(r"\[([^\]]+)\]\((https?://[^)]+)\)", markdown or "")]


def _classify_link(url: str, target_site_url: str, publishing_site_url: str) -> str:
    normalized = _normalize_site_selection_url(url)
    if normalized.startswith(_normalize_site_selection_url(target_site_url)):
        return "target_backlink"
    if normalized.startswith(_normalize_site_selection_url(publishing_site_url)):
        return "internal"
    return "external"


def _validate_links(urls: List[str], timeout_seconds: int) -> tuple[bool, List[str]]:
    invalid: List[str] = []
    for url in urls:
        try:
            response = requests.get(url, timeout=timeout_seconds, allow_redirects=True, headers={"User-Agent": "portal-backend-4llm/1.0"})
            if response.status_code >= 400:
                invalid.append(url)
        except requests.RequestException:
            invalid.append(url)
    return (not invalid, invalid)


def _copyscape_credentials() -> tuple[str, str]:
    username = os.getenv("COPYSCAPE_USERNAME", "").strip()
    api_key = os.getenv("COPYSCAPE_API_KEY", "").strip()
    if not username or not api_key:
        raise AutomationError("COPYSCAPE_USERNAME and COPYSCAPE_API_KEY are required in 4llm mode.")
    return username, api_key


def _run_copyscape_check(content_text: str, timeout_seconds: int) -> float:
    username, api_key = _copyscape_credentials()
    endpoint = os.getenv("COPYSCAPE_API_ENDPOINT", "https://www.copyscape.com/api/").strip()
    try:
        response = requests.post(
            endpoint,
            data={
                "u": username,
                "k": api_key,
                "o": "csearch",
                "e": "UTF-8",
                "t": content_text[:8000],
            },
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        raise AutomationError(f"Copyscape request failed: {exc}") from exc
    if response.status_code >= 400:
        raise AutomationError(f"Copyscape HTTP {response.status_code}: {response.text[:300]}")
    match = re.search(r"<percent>([\d.]+)</percent>", response.text, flags=re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return 0.0
    return 0.0


def _render_inline_markdown(text: str) -> str:
    escaped = escape(text)
    escaped = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r'<a href="\2">\1</a>', escaped)
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"\*(.+?)\*", r"<em>\1</em>", escaped)
    return escaped


def markdown_to_html(markdown: str) -> str:
    lines = (markdown or "").splitlines()
    parts: List[str] = []
    in_list = False
    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            if in_list:
                parts.append("</ul>")
                in_list = False
            continue
        if stripped.startswith("# "):
            if in_list:
                parts.append("</ul>")
                in_list = False
            parts.append(f"<h1>{_render_inline_markdown(stripped[2:].strip())}</h1>")
            continue
        if stripped.startswith("## "):
            if in_list:
                parts.append("</ul>")
                in_list = False
            parts.append(f"<h2>{_render_inline_markdown(stripped[3:].strip())}</h2>")
            continue
        if stripped.startswith("### "):
            if in_list:
                parts.append("</ul>")
                in_list = False
            parts.append(f"<h3>{_render_inline_markdown(stripped[4:].strip())}</h3>")
            continue
        if stripped.startswith("- "):
            if not in_list:
                parts.append("<ul>")
                in_list = True
            parts.append(f"<li>{_render_inline_markdown(stripped[2:].strip())}</li>")
            continue
        if in_list:
            parts.append("</ul>")
            in_list = False
        parts.append(f"<p>{_render_inline_markdown(stripped)}</p>")
    if in_list:
        parts.append("</ul>")
    return "\n".join(parts).strip()


def _build_quality_report(
    *,
    linked_markdown: str,
    content_brief: ContentBrief,
    meta_preview: Dict[str, Any],
    timeout_seconds: int,
) -> QualityReport:
    plain_text = _extract_plain_text(linked_markdown)
    word_count = len(plain_text.split())
    title = _extract_markdown_title(linked_markdown)
    intro = _extract_markdown_intro(linked_markdown)
    h2s = _extract_markdown_h2s(linked_markdown)
    keyword = content_brief.target_keyword
    normalized_keyword = _normalize_text_tokens(keyword)
    density = _keyword_density(plain_text, keyword)
    urls = _extract_markdown_link_targets(linked_markdown)
    internal_count = 0
    external_count = 0
    for url in urls:
        link_type = _classify_link(url, str(content_brief.target_site_url), str(content_brief.publishing_site_url))
        if link_type == "internal":
            internal_count += 1
        else:
            external_count += 1
    links_ok, invalid_links = _validate_links(urls, timeout_seconds)
    duplicate_percent = _run_copyscape_check(plain_text, timeout_seconds)
    html_body = markdown_to_html(linked_markdown)

    checks: List[QualityCheckResult] = []
    blockers: List[str] = []
    warnings: List[str] = []

    def add_check(name: str, passed: bool, severity: str, details: Dict[str, object]) -> None:
        checks.append(QualityCheckResult(name=name, passed=passed, severity=severity, details=details))
        if not passed:
            if severity == "critical":
                blockers.append(name)
            else:
                warnings.append(name)

    title_has_keyword = normalized_keyword in _normalize_text_tokens(title)
    intro_has_keyword = normalized_keyword in _normalize_text_tokens(" ".join(intro.split()[:100]))
    h2_keyword_hits = sum(1 for heading in h2s if normalized_keyword in _normalize_text_tokens(heading))
    target_min = int(content_brief.recommended_word_count * 0.9)
    target_max = int(content_brief.recommended_word_count * 1.1)

    add_check("word_count", target_min <= word_count <= target_max, "warning", {"word_count": word_count, "target_min": target_min, "target_max": target_max})
    add_check("keyword_in_title", title_has_keyword, "critical", {"title": title, "keyword": keyword})
    add_check("keyword_in_first_100_words", intro_has_keyword, "critical", {"intro": intro[:220], "keyword": keyword})
    add_check("keyword_in_h2s", h2_keyword_hits >= 2, "warning", {"matches": h2_keyword_hits, "keyword": keyword})
    add_check("keyword_density", 0.005 <= density <= 0.025, "warning", {"density": density})
    add_check("meta_description_length", 120 <= len(str(meta_preview.get("meta_description") or "")) <= 160, "warning", {"length": len(str(meta_preview.get("meta_description") or ""))})
    add_check("internal_links_count", 2 <= internal_count <= 5, "warning", {"count": internal_count})
    add_check("external_links_count", 1 <= external_count <= 3, "warning", {"count": external_count})
    add_check("link_validity", links_ok, "critical", {"invalid_links": invalid_links})
    add_check("duplicate_content", duplicate_percent < 20.0, "critical", {"duplicate_percent": duplicate_percent})
    add_check("html_conversion", bool(html_body and "<h1>" in html_body), "critical", {"html_length": len(html_body)})
    return QualityReport(passed=not blockers, blockers=blockers, warnings=warnings, checks=checks)


def _slugify(value: str) -> str:
    normalized = _normalize_text_tokens(value)
    slug = re.sub(r"\s+", "-", normalized).strip("-")
    return slug[:90] or "artikel"


def _build_creator_output_for_4llm(
    *,
    target_site_url: str,
    selected_site_url: str,
    selected_site_id: Optional[str],
    site_understanding: Dict[str, Any],
    keyword_metric: KeywordMetric,
    competitor_references: List[CompetitorReference],
    content_brief: ContentBrief,
    draft_markdown: str,
    linked_markdown: str,
    meta_preview: Dict[str, Any],
    quality_report: QualityReport,
    article_html: str,
) -> Dict[str, Any]:
    title = _extract_markdown_title(linked_markdown) or content_brief.suggested_title
    excerpt = _extract_markdown_intro(linked_markdown)[:220]
    return {
        "ok": True,
        "pipeline_mode": "4llm",
        "target_site_url": target_site_url,
        "host_site_url": selected_site_url,
        "host_site_id": selected_site_id,
        "phase1": site_understanding,
        "phase2": {
            "selected_publishing_site_url": selected_site_url,
            "selected_publishing_site_id": selected_site_id,
        },
        "phase3": {
            "target_keyword": keyword_metric.model_dump(),
            "competitor_references": [item.model_dump() for item in competitor_references],
            "internal_link_candidates": [item.model_dump() for item in content_brief.internal_link_candidates],
            "external_link_candidates": [item.model_dump() for item in content_brief.external_link_candidates],
        },
        "phase4": {"content_brief": content_brief.model_dump()},
        "phase5": {
            "title": title,
            "meta_title": str(meta_preview.get("meta_title") or title),
            "meta_description": str(meta_preview.get("meta_description") or ""),
            "slug": _slugify(str(meta_preview.get("meta_title") or title)),
            "excerpt": excerpt,
            "article_markdown": draft_markdown,
            "linked_markdown": linked_markdown,
            "article_html": article_html,
        },
        "debug": {
            "quality_report": quality_report.model_dump(),
            "meta_preview": meta_preview,
        },
    }


def _run_create_article_pipeline_4llm(
    *,
    creator_endpoint: str,
    target_site_url: str,
    publishing_site_url: str,
    publishing_site_id: Optional[str],
    publishing_candidates: Optional[List[Dict[str, Any]]],
    internal_link_inventory: Optional[List[Dict[str, Any]]],
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
    trace_event: Optional[Callable[[str, str, str, str, Optional[Dict[str, Any]]], None]] = None,
) -> Dict[str, Any]:
    def _trace(level: str, phase: str, event: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        if trace_event is not None:
            trace_event(level, phase, event, message, details)

    _trace("info", "site_understanding", "start", "Calling creator site-understanding endpoint.")
    site_understanding = call_creator_site_understanding(
        creator_endpoint=creator_endpoint,
        target_site_url=target_site_url,
        timeout_seconds=creator_timeout_seconds,
    )
    _trace(
        "info",
        "site_understanding",
        "complete",
        "Target site understanding generated.",
        {"primary_niche": site_understanding.get("primary_niche"), "main_topic": site_understanding.get("main_topic")},
    )

    selected_target = _select_publish_target_for_4llm(
        site_understanding=site_understanding,
        fallback_target={
            "site_url": site_url,
            "site_id": publishing_site_id,
            "wp_rest_base": wp_rest_base,
            "wp_username": wp_username,
            "wp_app_password": wp_app_password,
            "category_ids": list(category_ids or []),
            "category_candidates": list(category_candidates or []),
            "internal_link_inventory": list(internal_link_inventory or []),
        },
        publishing_candidates=publishing_candidates,
    )
    selected_publish_site_url = str(selected_target.get("site_url") or site_url).strip() or site_url
    selected_publish_site_id = str(selected_target.get("site_id") or publishing_site_id or "").strip() or None
    selected_wp_rest_base = str(selected_target.get("wp_rest_base") or wp_rest_base).strip() or wp_rest_base
    selected_wp_username = str(selected_target.get("wp_username") or wp_username).strip() or wp_username
    selected_wp_app_password = str(selected_target.get("wp_app_password") or wp_app_password).strip() or wp_app_password
    selected_category_ids = list(selected_target.get("category_ids") or category_ids or [])
    selected_category_candidates = list(selected_target.get("category_candidates") or category_candidates or [])
    selected_inventory = list(selected_target.get("internal_link_inventory") or internal_link_inventory or [])
    _trace(
        "info",
        "site_match",
        "selected",
        "Deterministic publishing-site match selected.",
        {"selected_site_url": selected_publish_site_url, "selected_site_id": selected_publish_site_id},
    )

    keyword_metric, competitor_references = _select_target_keyword(
        site_understanding,
        timeout_seconds=timeout_seconds,
    )
    topic_text = " ".join(
        [
            keyword_metric.keyword,
            str(site_understanding.get("main_topic") or ""),
            str(site_understanding.get("primary_niche") or ""),
        ]
    ).strip()
    internal_candidates = _select_internal_link_candidates(
        inventory=selected_inventory,
        topic_text=topic_text,
        limit=5,
    )
    target_page_candidates = _select_target_page_candidates(
        site_understanding=site_understanding,
        topic_text=topic_text,
        limit=3,
    )
    cross_network_candidates = _select_cross_network_candidates(
        publishing_candidates=list(publishing_candidates or []),
        selected_site_url=selected_publish_site_url,
        topic_text=topic_text,
        limit=2,
    )
    external_candidates = target_page_candidates + cross_network_candidates
    content_brief = _build_content_brief_4llm(
        target_site_url=target_site_url,
        selected_target=selected_target,
        site_understanding=site_understanding,
        keyword_metric=keyword_metric,
        competitor_references=competitor_references,
        internal_links=internal_candidates,
        external_links=external_candidates,
    )
    _trace(
        "info",
        "content_brief",
        "ready",
        "Deterministic content brief assembled.",
        {"target_keyword": content_brief.target_keyword, "internal_links": len(internal_candidates), "external_links": len(external_candidates)},
    )

    attempts: List[Dict[str, Any]] = []
    final_markdown = ""
    final_meta: Dict[str, Any] = {}
    final_quality: Optional[QualityReport] = None
    for attempt_index in range(2):
        _trace("info", "draft", "start", "Calling creator draft-article endpoint.", {"attempt": attempt_index + 1})
        draft_payload = call_creator_draft_article(
            creator_endpoint=creator_endpoint,
            content_brief=content_brief.model_dump(),
            timeout_seconds=creator_timeout_seconds,
        )
        linked_payload = call_creator_integrate_links(
            creator_endpoint=creator_endpoint,
            article_markdown=str(draft_payload.get("markdown") or ""),
            internal_links=[item.model_dump() for item in internal_candidates],
            external_links=[item.model_dump() for item in external_candidates],
            timeout_seconds=creator_timeout_seconds,
        )
        linked_markdown = str(linked_payload.get("markdown") or "").strip()
        title = _extract_markdown_title(linked_markdown) or content_brief.suggested_title
        intro = _extract_markdown_intro(linked_markdown)
        meta_payload = call_creator_generate_meta(
            creator_endpoint=creator_endpoint,
            target_keyword=content_brief.target_keyword,
            article_title=title,
            article_intro=intro,
            timeout_seconds=creator_timeout_seconds,
        )
        quality_report = _build_quality_report(
            linked_markdown=linked_markdown,
            content_brief=content_brief,
            meta_preview=meta_payload,
            timeout_seconds=timeout_seconds,
        )
        attempts.append(
            {
                "attempt": attempt_index + 1,
                "draft_markdown": str(draft_payload.get("markdown") or ""),
                "linked_markdown": linked_markdown,
                "meta_preview": meta_payload,
                "quality_report": quality_report.model_dump(),
            }
        )
        if quality_report.passed:
            final_markdown = linked_markdown
            final_meta = meta_payload
            final_quality = quality_report
            break
        _trace(
            "warning",
            "quality",
            "retry",
            "4llm quality gate requested another draft attempt.",
            {"attempt": attempt_index + 1, "blockers": list(quality_report.blockers)},
        )

    if not final_quality:
        latest = attempts[-1]
        failed_html = markdown_to_html(str(latest.get("linked_markdown") or ""))
        failed_quality = QualityReport.model_validate(latest["quality_report"])
        creator_output = _build_creator_output_for_4llm(
            target_site_url=target_site_url,
            selected_site_url=selected_publish_site_url,
            selected_site_id=selected_publish_site_id,
            site_understanding=site_understanding,
            keyword_metric=keyword_metric,
            competitor_references=competitor_references,
            content_brief=content_brief,
            draft_markdown=str(latest.get("draft_markdown") or ""),
            linked_markdown=str(latest.get("linked_markdown") or ""),
            meta_preview=dict(latest.get("meta_preview") or {}),
            quality_report=failed_quality,
            article_html=failed_html,
        )
        raise AutomationError(
            "4llm quality checks failed.",
            details={
                "creator_output": creator_output,
                "quality_report": failed_quality.model_dump(),
            },
        )

    article_html = markdown_to_html(final_markdown)
    if category_llm_enabled and selected_category_candidates and category_llm_api_key:
        try:
            llm_selected_ids = _select_categories_with_llm(
                title=_extract_markdown_title(final_markdown) or content_brief.suggested_title,
                excerpt=_extract_markdown_intro(final_markdown),
                clean_html=article_html,
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

    featured_media_id = 0 if existing_wp_post_id else None
    title = _extract_markdown_title(final_markdown) or content_brief.suggested_title
    excerpt = _extract_markdown_intro(final_markdown)[:220]
    slug = _slugify(str(final_meta.get("meta_title") or title))
    if existing_wp_post_id:
        post_payload = wp_update_post(
            site_url=selected_publish_site_url,
            wp_rest_base=selected_wp_rest_base,
            wp_username=selected_wp_username,
            wp_app_password=selected_wp_app_password,
            post_id=existing_wp_post_id,
            title=title,
            clean_html=_strip_leading_h1_from_article_html(article_html),
            excerpt=excerpt,
            slug=slug,
            featured_media_id=featured_media_id,
            post_status=post_status,
            author_id=author_id,
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
            clean_html=_strip_leading_h1_from_article_html(article_html),
            excerpt=excerpt,
            slug=slug,
            featured_media_id=featured_media_id,
            post_status=post_status,
            author_id=author_id,
            category_ids=selected_category_ids,
            timeout_seconds=timeout_seconds,
        )
        post_event_type = "wp_post_created"

    creator_output = _build_creator_output_for_4llm(
        target_site_url=target_site_url,
        selected_site_url=selected_publish_site_url,
        selected_site_id=selected_publish_site_id,
        site_understanding=site_understanding,
        keyword_metric=keyword_metric,
        competitor_references=competitor_references,
        content_brief=content_brief,
        draft_markdown=str(attempts[-1].get("draft_markdown") or ""),
        linked_markdown=final_markdown,
        meta_preview=final_meta,
        quality_report=final_quality,
        article_html=article_html,
    )
    creator_output["pipeline_state"] = {
        "site_understanding": site_understanding,
        "selected_publishing_site": {
            "site_url": selected_publish_site_url,
            "site_id": selected_publish_site_id,
        },
        "keyword_research": keyword_metric.model_dump(),
        "content_brief": content_brief.model_dump(),
        "quality_report": final_quality.model_dump(),
        "meta_preview": final_meta,
    }
    creator_output["phase5"]["quality_report"] = final_quality.model_dump()
    return {
        "creator_output": creator_output,
        "image_url": "",
        "media_payload": {},
        "media_url": None,
        "post_payload": post_payload,
        "post_event_type": post_event_type,
        "selected_category_ids": selected_category_ids,
        "selected_site_id": selected_publish_site_id,
        "selected_site_url": selected_publish_site_url,
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
    if _read_pipeline_mode() == "4llm":
        return _run_create_article_pipeline_4llm(
            creator_endpoint=creator_endpoint,
            target_site_url=target_site_url,
            publishing_site_url=publishing_site_url,
            publishing_site_id=publishing_site_id,
            publishing_candidates=publishing_candidates,
            internal_link_inventory=internal_link_inventory,
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
            trace_event=trace_event,
        )

    def _trace(
        level: str,
        phase: str,
        event: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        if trace_event is not None:
            trace_event(level, phase, event, message, details)

    _trace(
        "info",
        "creator",
        "request_started",
        "Calling creator service.",
        {
            "target_site_url": target_site_url,
            "publishing_site_url": publishing_site_url,
            "publishing_candidates_count": len(publishing_candidates or []),
        },
    )
    creator_output = call_creator_service(
        creator_endpoint=creator_endpoint,
        target_site_url=target_site_url,
        publishing_site_url=publishing_site_url,
        publishing_site_id=publishing_site_id,
        client_target_site_id=client_target_site_id,
        anchor=anchor,
        topic=topic,
        exclude_topics=exclude_topics,
        recent_article_titles=recent_article_titles,
        internal_link_inventory=internal_link_inventory,
        publishing_candidates=publishing_candidates,
        phase1_cache_payload=phase1_cache_payload,
        phase1_cache_content_hash=phase1_cache_content_hash,
        phase2_cache_payload=phase2_cache_payload,
        phase2_cache_content_hash=phase2_cache_content_hash,
        target_profile_payload=target_profile_payload,
        target_profile_content_hash=target_profile_content_hash,
        publishing_profile_payload=publishing_profile_payload,
        publishing_profile_content_hash=publishing_profile_content_hash,
        timeout_seconds=creator_timeout_seconds,
        on_phase=on_phase,
        should_cancel=should_cancel,
    )
    _trace("info", "creator", "response_received", "Creator service returned a response.")
    creator_output = ensure_prompt_trace_in_creator_output(creator_output)
    selected_publish_target = _select_publish_target(
        creator_output=creator_output,
        fallback_target={
            "site_url": site_url,
            "site_id": publishing_site_id,
            "wp_rest_base": wp_rest_base,
            "wp_username": wp_username,
            "wp_app_password": wp_app_password,
            "category_ids": list(category_ids or []),
            "category_candidates": list(category_candidates or []),
        },
        publishing_candidates=publishing_candidates,
    )
    selected_publish_site_url = str(selected_publish_target.get("site_url") or site_url).strip() or site_url
    selected_publish_site_id = str(selected_publish_target.get("site_id") or publishing_site_id or "").strip() or None
    selected_wp_rest_base = str(selected_publish_target.get("wp_rest_base") or wp_rest_base).strip() or wp_rest_base
    selected_wp_username = str(selected_publish_target.get("wp_username") or wp_username).strip() or wp_username
    selected_wp_app_password = str(selected_publish_target.get("wp_app_password") or wp_app_password).strip() or wp_app_password
    selected_category_ids = list(selected_publish_target.get("category_ids") or category_ids or [])
    selected_category_candidates = list(selected_publish_target.get("category_candidates") or category_candidates or [])
    if _normalize_site_selection_url(selected_publish_site_url) != _normalize_site_selection_url(site_url):
        _trace(
            "info",
            "creator",
            "publishing_site_switched",
            "Creator selected a different publishing site from the candidate shortlist.",
            {
                "provisional_site_url": site_url,
                "selected_site_url": selected_publish_site_url,
                "selected_site_id": selected_publish_site_id,
            },
        )
    phase5 = creator_output.get("phase5") or {}
    phase6 = creator_output.get("phase6") or {}
    images = creator_output.get("images") or []

    title = str(phase5.get("meta_title") or phase5.get("title") or "").strip()
    if title:
        title = unescape(title)
    excerpt = str(phase5.get("excerpt") or "").strip()
    slug = str(phase5.get("slug") or "").strip()
    article_html = str(phase5.get("article_html") or "").strip()
    if not article_html:
        _trace("error", "creator", "article_html_missing", "Creator output missing article_html.")
        raise AutomationError("Creator output missing article_html.")
    article_html = _strip_leading_h1_from_article_html(article_html)

    if category_llm_enabled and selected_category_candidates:
        if category_llm_api_key:
            try:
                llm_selected_ids = _select_categories_with_llm(
                    title=title or "Generated Draft",
                    excerpt=excerpt,
                    clean_html=article_html,
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
                logger.warning(
                    "automation.creator.category_llm.fallback reason=%s defaults_count=%s",
                    str(exc),
                    len(selected_category_ids),
                )
                _trace(
                    "warning",
                    "categories",
                    "llm_fallback",
                    "Category LLM selection failed; using default categories.",
                    {"error": str(exc), "defaults_count": len(selected_category_ids)},
                )
        else:
            logger.warning(
                "automation.creator.category_llm.fallback reason=missing_api_key defaults_count=%s",
                len(selected_category_ids),
            )
            _trace(
                "warning",
                "categories",
                "llm_missing_api_key",
                "Category LLM selection skipped because the API key is missing.",
                {"defaults_count": len(selected_category_ids)},
            )

    featured_url = _pick_creator_image(images, "featured")
    featured_alt = ""
    featured_meta = phase6.get("featured_image") if isinstance(phase6.get("featured_image"), dict) else {}
    if isinstance(featured_meta, dict):
        featured_alt = str(featured_meta.get("alt_text") or "").strip()

    media_payload: Dict[str, Any] = {}
    if featured_url:
        image_bytes, file_name, content_type = download_binary_file(
            featured_url,
            timeout_seconds=timeout_seconds,
        )
        media_payload = wp_create_media_item(
            site_url=selected_publish_site_url,
            wp_rest_base=selected_wp_rest_base,
            wp_username=selected_wp_username,
            wp_app_password=selected_wp_app_password,
            data=image_bytes,
            file_name=file_name,
            content_type=content_type,
            title=title or "Generated Draft",
            alt_text=featured_alt or None,
            timeout_seconds=timeout_seconds,
        )

    in_content_url = _pick_creator_image(images, "in_content")
    if in_content_url:
        in_meta = phase6.get("in_content_image") if isinstance(phase6.get("in_content_image"), dict) else {}
        in_alt = str(in_meta.get("alt_text") or "").strip() if isinstance(in_meta, dict) else ""
        try:
            in_bytes, in_name, in_type = download_binary_file(
                in_content_url,
                timeout_seconds=timeout_seconds,
            )
            in_media_payload = wp_create_media_item(
                site_url=selected_publish_site_url,
                wp_rest_base=selected_wp_rest_base,
                wp_username=selected_wp_username,
                wp_app_password=selected_wp_app_password,
                data=in_bytes,
                file_name=in_name,
                content_type=in_type,
                title=title or "Generated Draft",
                alt_text=in_alt or None,
                timeout_seconds=timeout_seconds,
            )
            in_media_url = in_media_payload.get("source_url") or in_media_payload.get("guid", {}).get("rendered")
            if isinstance(in_media_url, str) and in_media_url.strip():
                article_html = _insert_in_content_image(article_html, in_media_url.strip(), in_alt)
        except AutomationError:
            logger.warning("automation.creator.in_content_upload_failed")
            _trace(
                "warning",
                "media",
                "in_content_upload_failed",
                "Uploading the in-content image failed; continuing without it.",
            )

    featured_media_id: Optional[int] = None
    raw_featured_media_id = media_payload.get("id")
    if isinstance(raw_featured_media_id, int):
        featured_media_id = raw_featured_media_id
    elif existing_wp_post_id:
        # Clear stale featured media on draft updates when creator intentionally returned no image.
        featured_media_id = 0
    if existing_wp_post_id:
        post_payload = wp_update_post(
            site_url=selected_publish_site_url,
            wp_rest_base=selected_wp_rest_base,
            wp_username=selected_wp_username,
            wp_app_password=selected_wp_app_password,
            post_id=existing_wp_post_id,
            title=title,
            clean_html=article_html,
            excerpt=excerpt,
            slug=slug,
            featured_media_id=featured_media_id,
            post_status=post_status,
            author_id=author_id,
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
            clean_html=article_html,
            excerpt=excerpt,
            slug=slug,
            featured_media_id=featured_media_id,
            post_status=post_status,
            author_id=author_id,
            category_ids=selected_category_ids,
            timeout_seconds=timeout_seconds,
        )
        post_event_type = "wp_post_created"
    _trace(
        "info",
        "publish",
        post_event_type,
        "WordPress draft/post persisted.",
        {"post_id": post_payload.get("id"), "post_status": post_payload.get("status") or post_status},
    )

    guid_value = media_payload.get("guid")
    media_url = media_payload.get("source_url")
    if not media_url and isinstance(guid_value, dict):
        media_url = guid_value.get("rendered")

    return {
        "creator_output": creator_output,
        "image_url": featured_url,
        "media_payload": media_payload,
        "media_url": media_url,
        "post_payload": post_payload,
        "post_event_type": post_event_type,
        "selected_category_ids": selected_category_ids,
        "selected_site_id": selected_publish_site_id,
        "selected_site_url": selected_publish_site_url,
    }


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
        "creator_pipeline_mode": _read_pipeline_mode(),
        "converter_endpoint": os.getenv("AUTOMATION_CONVERTER_ENDPOINT", DEFAULT_CONVERTER_ENDPOINT).strip(),
        "creator_endpoint": os.getenv("AUTOMATION_CREATOR_ENDPOINT", DEFAULT_CREATOR_ENDPOINT).strip(),
        "leonardo_api_key": os.getenv("LEONARDO_API_KEY", "").strip(),
        "leonardo_base_url": os.getenv("LEONARDO_BASE_URL", DEFAULT_LEONARDO_BASE_URL).strip(),
        "leonardo_model_id": DEFAULT_LEONARDO_MODEL_ID,
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
