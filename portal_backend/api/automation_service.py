from __future__ import annotations

import base64
import mimetypes
import os
import re
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import requests


DEFAULT_CONVERTER_ENDPOINT = "https://elci.live/convert"
DEFAULT_LEONARDO_BASE_URL = "https://cloud.leonardo.ai/api/rest/v1"
DEFAULT_LEONARDO_MODEL_ID = "1dd50843-d653-4516-a8e3-f0238ee453ff"
DEFAULT_IMAGE_WIDTH = 1536
DEFAULT_IMAGE_HEIGHT = 864
DEFAULT_IMAGE_COUNT = 1
DEFAULT_AUTHOR_ID = 4
DEFAULT_POST_STATUS = "publish"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_IMAGE_POLL_TIMEOUT_SECONDS = 90
DEFAULT_IMAGE_POLL_INTERVAL_SECONDS = 2


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


def call_converter(source_url: str, target_site: str, converter_endpoint: str, timeout_seconds: int) -> Dict[str, Any]:
    response = _request_json(
        "POST",
        converter_endpoint,
        json_body={"source_url": source_url, "target_site": target_site},
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
    _request_json(
        "POST",
        title_url,
        headers={
            "Authorization": _wp_auth_header(wp_username, wp_app_password),
            "Content-Type": "application/json",
        },
        json_body={"title": title},
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
    return _request_json(
        "POST",
        post_url,
        headers=headers,
        json_body=payload,
        timeout_seconds=timeout_seconds,
        allow_redirects=False,
    )


def converter_target_from_site_url(site_url: str) -> str:
    parsed = urlparse(site_url.strip())
    return (parsed.netloc or parsed.path).strip().lower()


def run_guest_post_pipeline(
    *,
    source_url: str,
    target_site: str,
    site_url: str,
    wp_rest_base: str,
    wp_username: str,
    wp_app_password: str,
    existing_wp_post_id: Optional[int],
    post_status: str,
    author_id: int,
    converter_endpoint: str,
    leonardo_api_key: str,
    leonardo_base_url: str,
    leonardo_model_id: str,
    timeout_seconds: int,
    poll_timeout_seconds: int,
    poll_interval_seconds: int,
) -> Dict[str, Any]:
    converted = call_converter(
        source_url=source_url,
        target_site=target_site,
        converter_endpoint=converter_endpoint,
        timeout_seconds=timeout_seconds,
    )

    image_url = generate_image_via_leonardo(
        prompt=converted["image_prompt"],
        api_key=leonardo_api_key,
        timeout_seconds=timeout_seconds,
        poll_timeout_seconds=poll_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        model_id=leonardo_model_id,
        base_url=leonardo_base_url,
    )
    image_bytes, file_name, content_type = download_binary_file(
        image_url,
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
        title=converted["title"],
        timeout_seconds=timeout_seconds,
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
    }


def get_runtime_config() -> Dict[str, Any]:
    def read_int(name: str, default: int) -> int:
        raw = os.getenv(name, str(default)).strip()
        try:
            return int(raw)
        except ValueError as exc:
            raise AutomationError(f"{name} must be an integer, got '{raw}'.") from exc

    return {
        "converter_endpoint": os.getenv("AUTOMATION_CONVERTER_ENDPOINT", DEFAULT_CONVERTER_ENDPOINT).strip(),
        "leonardo_api_key": os.getenv("LEONARDO_API_KEY", "").strip(),
        "leonardo_base_url": os.getenv("LEONARDO_BASE_URL", DEFAULT_LEONARDO_BASE_URL).strip(),
        "leonardo_model_id": os.getenv("LEONARDO_MODEL_ID", DEFAULT_LEONARDO_MODEL_ID).strip(),
        "timeout_seconds": read_int("AUTOMATION_REQUEST_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS),
        "poll_timeout_seconds": read_int("AUTOMATION_IMAGE_POLL_TIMEOUT_SECONDS", DEFAULT_IMAGE_POLL_TIMEOUT_SECONDS),
        "poll_interval_seconds": read_int(
            "AUTOMATION_IMAGE_POLL_INTERVAL_SECONDS",
            DEFAULT_IMAGE_POLL_INTERVAL_SECONDS,
        ),
        "default_author_id": read_int("AUTOMATION_POST_AUTHOR_ID", DEFAULT_AUTHOR_ID),
        "default_post_status": os.getenv("AUTOMATION_POST_STATUS", DEFAULT_POST_STATUS).strip().lower(),
    }
