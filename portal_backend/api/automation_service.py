from __future__ import annotations

import base64
import json
import logging
import mimetypes
import os
import re
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests


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
DEFAULT_CATEGORY_LLM_ANTHROPIC_MODEL = "claude-haiku-4-20250414"
DEFAULT_CATEGORY_LLM_MAX_CATEGORIES = 2
DEFAULT_CATEGORY_LLM_CONFIDENCE_THRESHOLD = 0.55

logger = logging.getLogger("portal_backend.automation")


class AutomationError(RuntimeError):
    pass


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


def _strip_html_to_text(value: str) -> str:
    if not value:
        return ""
    without_scripts = re.sub(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", " ", value, flags=re.IGNORECASE)
    without_styles = re.sub(r"<style\b[^<]*(?:(?!</style>)<[^<]*)*</style>", " ", without_scripts, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", without_styles)
    compact = re.sub(r"\s+", " ", text).strip()
    return compact


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
    featured_media_id: int,
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
        "featured_media": featured_media_id,
        "format": "standard",
        "date": datetime.now(timezone.utc).isoformat(),
    }
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
    featured_media_id: int,
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
        "featured_media": featured_media_id,
        "format": "standard",
        "date": datetime.now(timezone.utc).isoformat(),
    }
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


def call_creator_service(
    *,
    creator_endpoint: str,
    target_site_url: str,
    publishing_site_url: str,
    anchor: Optional[str],
    topic: Optional[str],
    timeout_seconds: int,
) -> Dict[str, Any]:
    if not creator_endpoint:
        raise AutomationError("Creator endpoint is not configured.")
    url = creator_endpoint.rstrip("/") + "/create"
    body: Dict[str, Any] = {
        "target_site_url": target_site_url,
        "publishing_site_url": publishing_site_url,
    }
    if anchor:
        body["anchor"] = anchor
    if topic:
        body["topic"] = topic
    return _request_json(
        "POST",
        url,
        json_body=body,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def run_creator_order_pipeline(
    *,
    creator_endpoint: str,
    target_site_url: str,
    publishing_site_url: str,
    anchor: Optional[str],
    topic: Optional[str],
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
) -> Dict[str, Any]:
    creator_output = call_creator_service(
        creator_endpoint=creator_endpoint,
        target_site_url=target_site_url,
        publishing_site_url=publishing_site_url,
        anchor=anchor,
        topic=topic,
        timeout_seconds=creator_timeout_seconds,
    )
    phase5 = creator_output.get("phase5") or {}
    phase6 = creator_output.get("phase6") or {}
    images = creator_output.get("images") or []

    title = str(phase5.get("meta_title") or phase5.get("title") or "").strip()
    excerpt = str(phase5.get("excerpt") or "").strip()
    slug = str(phase5.get("slug") or "").strip()
    article_html = str(phase5.get("article_html") or "").strip()
    if not article_html:
        raise AutomationError("Creator output missing article_html.")

    selected_category_ids = list(category_ids or [])
    if category_llm_enabled and category_candidates:
        if category_llm_api_key:
            try:
                llm_selected_ids = _select_categories_with_llm(
                    title=title or "Generated Draft",
                    excerpt=excerpt,
                    clean_html=article_html,
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
                    "automation.creator.category_llm.fallback reason=%s defaults_count=%s",
                    str(exc),
                    len(selected_category_ids),
                )
        else:
            logger.warning(
                "automation.creator.category_llm.fallback reason=missing_api_key defaults_count=%s",
                len(selected_category_ids),
            )

    featured_url = _pick_creator_image(images, "featured")
    featured_alt = ""
    featured_meta = phase6.get("featured_image") if isinstance(phase6.get("featured_image"), dict) else {}
    if isinstance(featured_meta, dict):
        featured_alt = str(featured_meta.get("alt_text") or "").strip()
    if not featured_url:
        featured_prompt = ""
        if isinstance(featured_meta, dict):
            featured_prompt = str(featured_meta.get("prompt") or "").strip()
        if not featured_prompt:
            featured_prompt = f"Editorial photo illustrating: {title or 'blog post'}"
        if not leonardo_api_key:
            raise AutomationError("Creator output missing featured image URL and LEONARDO_API_KEY is not set for fallback.")
        logger.warning("automation.creator.image_fallback generating featured image on portal side")
        featured_url = generate_image_via_leonardo(
            prompt=featured_prompt,
            api_key=leonardo_api_key,
            timeout_seconds=timeout_seconds,
            poll_timeout_seconds=poll_timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            model_id=leonardo_model_id,
            width=image_width,
            height=image_height,
            base_url=leonardo_base_url,
        )

    image_bytes, file_name, content_type = download_binary_file(
        featured_url,
        timeout_seconds=timeout_seconds,
    )
    media_payload = wp_create_media_item(
        site_url=site_url,
        wp_rest_base=wp_rest_base,
        wp_username=wp_username,
        wp_app_password=wp_app_password,
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
                site_url=site_url,
                wp_rest_base=wp_rest_base,
                wp_username=wp_username,
                wp_app_password=wp_app_password,
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

    featured_media_id = int(media_payload.get("id"))
    if existing_wp_post_id:
        post_payload = wp_update_post(
            site_url=site_url,
            wp_rest_base=wp_rest_base,
            wp_username=wp_username,
            wp_app_password=wp_app_password,
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
            site_url=site_url,
            wp_rest_base=wp_rest_base,
            wp_username=wp_username,
            wp_app_password=wp_app_password,
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
    }


def run_guest_post_pipeline(
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
