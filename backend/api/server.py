"""
Local Document Conversion Service

Run locally:
python -m uvicorn api.server:app --host 0.0.0.0 --port 8000 --reload

Request example (JSON):
{
  "target_site": "audit-net.de",
  "source_url": "https://docs.google.com/document/d/GOOGLE_DOC_ID/edit",
  "post_status": "draft",
  "client_id": "client-123",
  "post_id": "post-456",
  "client_url": "https://client.example.com",
  "language": "de",
  "options": {
    "remove_images": true,
    "fix_headings": true,
    "max_slug_length": 80,
    "max_meta_length": 155,
    "max_excerpt_length": 180
  }
}

Response example:
{
  "ok": true,
  "target_site": "audit-net.de",
  "source_url": "https://docs.google.com/document/d/GOOGLE_DOC_ID/edit",
  "source_type": "google_doc",
  "source_filename": "google_doc_GOOGLE_DOC_ID.docx",
  "title": "Beispieltitel",
  "slug": "beispieltitel",
  "excerpt": "Kurzer deutscher Auszug...",
  "meta_description": "Kurze deutsche Meta-Beschreibung...",
  "clean_html": "<h2>...</h2><p>...</p>",
  "image_prompt": "Professional editorial photo... Negative: text, watermark, logo, low quality, blurry, deformed",
  "warnings": [],
  "debug": {
    "download_ms": 120,
    "convert_ms": 80,
    "sanitize_ms": 25,
    "total_ms": 260
  }
}

Example curl (JSON):
curl -X POST http://localhost:8000/convert \
  -H "Content-Type: application/json" \
  -d '{"target_site":"audit-net.de","source_url":"https://docs.google.com/document/d/GOOGLE_DOC_ID/edit"}'

Example curl (multipart):
curl -X POST http://localhost:8000/convert \
  -F "target_site=audit-net.de" \
  -F "source_url=https://docs.google.com/document/d/GOOGLE_DOC_ID/edit" \
  -F 'options={"remove_images":true,"fix_headings":true}'

Health check:
curl http://localhost:8000/health
"""

from __future__ import annotations

import ipaddress
import json
import logging
import re
import time
import unicodedata
import html
import os
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import mammoth
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import ValidationError

from .llm.image_prompt import generate_image_prompt
from .llm.slug import generate_slug
from .models import ConvertDebug, ConvertOptions, ConvertRequest, ConvertResponse, ErrorResponse
from .routers import (
    admin_guest_posts_router,
    auth_router,
    clients_router,
    guest_posts_router,
    target_sites_router,
    user_router,
)

load_dotenv()

APP_NAME = "doc_converter"

MAX_DOCX_SIZE = 25 * 1024 * 1024
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(APP_NAME)

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "doc-converter/1.0",
        "Accept": "application/vnd.openxmlformats-officedocument.wordprocessingml.document,application/octet-stream,*/*",
    }
)

app = FastAPI(title="Local Document Conversion Service")

cors_origins = [origin.strip() for origin in os.getenv("CORS_ORIGINS", "").split(",") if origin.strip()]
if cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(auth_router)
app.include_router(clients_router)
app.include_router(target_sites_router)
app.include_router(guest_posts_router)
app.include_router(admin_guest_posts_router)
app.include_router(user_router)


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        return error_response(
            str(exc.detail.get("error")),
            exc.status_code,
            details=exc.detail.get("details"),
        )
    return error_response(str(exc.detail), exc.status_code)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    details = format_validation_errors(exc.errors())
    return error_response("validation_error", 422, details=details)


@app.exception_handler(Exception)
async def generic_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled exception", extra={"event": "unhandled_exception"})
    return error_response("Internal server error.", 500)


@app.get("/health")
async def health() -> Dict[str, bool]:
    return {"ok": True}


@app.post("/convert")
async def convert(request: Request) -> JSONResponse:
    parsed = await parse_request(request)
    response = convert_core(parsed)
    return JSONResponse(status_code=200, content=model_to_dict(response))


@app.get("/preview")
async def preview(request: Request) -> HTMLResponse:
    query = request.query_params
    data: Dict[str, Any] = {
        "target_site": query.get("target_site", ""),
        "source_url": query.get("source_url", ""),
        "language": query.get("language", "de"),
        "post_status": "draft",
    }

    try:
        parsed = ConvertRequest(**data)
        parsed = normalize_request(parsed)
        response = convert_core(parsed)
        return HTMLResponse(status_code=200, content=render_preview_html(response))
    except HTTPException as exc:
        error_payload = exc.detail if isinstance(exc.detail, dict) else {"message": str(exc.detail)}
        error_type = error_payload.get("error", "http_error")
        message = error_payload.get("message") or error_payload.get("error") or str(exc.detail)
        return HTMLResponse(
            status_code=exc.status_code,
            content=render_error_html(
                error_type,
                message,
                error_payload.get("details"),
            ),
        )
    except ValidationError as exc:
        details = format_validation_errors(exc.errors())
        return HTMLResponse(
            status_code=422,
            content=render_error_html("validation_error", "Validation failed.", details),
        )
    except Exception as exc:
        logger.exception("preview_error", extra={"event": "preview_error"})
        return HTMLResponse(
            status_code=500,
            content=render_error_html("internal_error", str(exc)),
        )


def convert_core(parsed: ConvertRequest) -> ConvertResponse:
    start_total = time.monotonic()
    validate_source_url(str(parsed.source_url))

    source_type, source_filename, download_url = detect_source(str(parsed.source_url))

    logger.info(
        "convert_start",
        extra={
            "event": "convert_start",
            "target_site": parsed.target_site,
            "source_type": source_type,
            "source_url": str(parsed.source_url),
            "client_id": parsed.client_id,
            "post_id": parsed.post_id,
        },
    )

    warnings: List[str] = []
    timing: Dict[str, int] = {}

    start_download = time.monotonic()
    docx_bytes = download_docx(download_url, source_type == "google_doc")
    timing["download_ms"] = ms_since(start_download)

    start_convert = time.monotonic()
    try:
        html, convert_warnings = convert_docx_to_html(docx_bytes)
    except Exception:
        raise HTTPException(status_code=500, detail="Conversion failed.")
    if convert_warnings:
        warnings.extend(convert_warnings)
    timing["convert_ms"] = ms_since(start_convert)

    start_sanitize = time.monotonic()
    clean_html = sanitize_html(html, parsed.options)
    timing["sanitize_ms"] = ms_since(start_sanitize)

    title, title_warning = extract_title(clean_html)
    if title_warning:
        warnings.append(title_warning)

    clean_html = remove_duplicate_title_heading(clean_html, title)

    slug = generate_slug(
        title,
        min_words=3,
        max_words=6,
        max_length=parsed.options.max_slug_length,
    )
    slug_warning = None
    if not slug:
        slug, slug_warning = slugify(title, parsed.options.max_slug_length)
        warnings.append("LLM slug generation failed; used deterministic slug.")
        if slug_warning:
            warnings.append(slug_warning)

    excerpt, excerpt_warning = generate_excerpt(clean_html, parsed.options.max_excerpt_length)
    if excerpt_warning:
        warnings.append(excerpt_warning)

    meta_description, meta_warning = generate_meta_description(
        clean_html, parsed.options.max_meta_length
    )
    if meta_warning:
        warnings.append(meta_warning)

    excerpt = adjust_excerpt_if_duplicate(
        clean_html,
        excerpt,
        meta_description,
        parsed.options.max_excerpt_length,
    )

    intro_paragraph = extract_intro_paragraph(clean_html)
    image_prompt = generate_image_prompt(title, intro_paragraph)

    timing["total_ms"] = ms_since(start_total)
    debug = ConvertDebug(**timing)

    response = ConvertResponse(
        ok=True,
        target_site=parsed.target_site,
        source_url=str(parsed.source_url),
        source_type=source_type,
        source_filename=source_filename,
        title=title,
        slug=slug,
        excerpt=excerpt,
        meta_description=meta_description,
        clean_html=clean_html,
        image_prompt=image_prompt,
        warnings=warnings,
        debug=debug,
    )

    logger.info(
        "convert_success",
        extra={
            "event": "convert_success",
            "target_site": parsed.target_site,
            "source_type": source_type,
            "download_ms": timing["download_ms"],
            "convert_ms": timing["convert_ms"],
            "sanitize_ms": timing["sanitize_ms"],
            "total_ms": timing["total_ms"],
            "warnings": len(warnings),
        },
    )

    return response


async def parse_request(request: Request) -> ConvertRequest:
    content_type = request.headers.get("content-type", "")
    data: Dict[str, Any]

    if "application/json" in content_type:
        try:
            data = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body.")

        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="Invalid JSON body.")

        req = build_request_from_json(data)
    else:
        form = await request.form()
        data = {key: value for key, value in form.items()}
        req = build_request_from_form(data)

    return normalize_request(req)


def build_request_from_json(data: Dict[str, Any]) -> ConvertRequest:
    if isinstance(data.get("options"), str):
        try:
            parsed_options = json.loads(data["options"])
            if isinstance(parsed_options, dict):
                data["options"] = parsed_options
        except Exception:
            pass

    try:
        return ConvertRequest(**data)
    except ValidationError as exc:
        details = format_validation_errors(exc.errors())
        raise HTTPException(
            status_code=422,
            detail={"error": "validation_error", "details": details},
        ) from exc


def build_request_from_form(data: Dict[str, Any]) -> ConvertRequest:
    cleaned: Dict[str, Any] = {}
    for key, value in data.items():
        if value is None:
            cleaned[key] = None
        elif isinstance(value, str):
            cleaned[key] = value
        else:
            cleaned[key] = str(value)

    options_data = parse_options_from_form(cleaned)

    req_data: Dict[str, Any] = {
        "target_site": cleaned.get("target_site", ""),
        "source_url": cleaned.get("source_url", ""),
        "post_status": cleaned.get("post_status", "draft"),
        "language": cleaned.get("language", "de"),
        "client_id": cleaned.get("client_id"),
        "post_id": cleaned.get("post_id"),
        "client_url": cleaned.get("client_url"),
    }

    if options_data:
        req_data["options"] = options_data

    try:
        return ConvertRequest(**req_data)
    except ValidationError as exc:
        details = format_validation_errors(exc.errors())
        raise HTTPException(
            status_code=422,
            detail={"error": "validation_error", "details": details},
        ) from exc


def parse_options_from_form(data: Dict[str, Any]) -> Dict[str, Any]:
    options_data: Dict[str, Any] = {}
    raw_options = data.get("options")
    if raw_options:
        if isinstance(raw_options, str):
            try:
                parsed_options = json.loads(raw_options)
                if isinstance(parsed_options, dict):
                    options_data.update(parsed_options)
            except Exception:
                pass
        elif isinstance(raw_options, dict):
            options_data.update(raw_options)

    for key in option_fields():
        for form_key in (key, f"options.{key}", f"options[{key}]"):
            if form_key in data:
                options_data[key] = data[form_key]

    return options_data


def option_fields() -> List[str]:
    if hasattr(ConvertOptions, "model_fields"):
        return list(ConvertOptions.model_fields.keys())
    return list(ConvertOptions.__fields__.keys())


def normalize_request(req: ConvertRequest) -> ConvertRequest:
    target_site = req.target_site.strip()
    source_url = str(req.source_url).strip()

    if not target_site:
        raise HTTPException(status_code=400, detail="target_site is required.")
    if not source_url:
        raise HTTPException(status_code=400, detail="source_url is required.")

    post_status = req.post_status.strip() or "draft"
    language = req.language.strip() or "de"

    updates = {
        "target_site": target_site,
        "source_url": source_url,
        "post_status": post_status,
        "language": language,
        "client_id": normalize_optional_str(req.client_id),
        "post_id": normalize_optional_str(req.post_id),
        "client_url": normalize_optional_str(req.client_url),
    }

    return update_model(req, updates)


def update_model(model: ConvertRequest, updates: Dict[str, Any]) -> ConvertRequest:
    if hasattr(model, "model_copy"):
        return model.model_copy(update=updates)
    return model.copy(update=updates)


def model_to_dict(model: Any) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def error_response(message: str, status_code: int, details: Optional[Any] = None) -> JSONResponse:
    payload = ErrorResponse(error=message, details=details)
    return JSONResponse(status_code=status_code, content=model_to_dict(payload))


def render_preview_html(response: ConvertResponse) -> str:
    debug_payload = {
        "debug": model_to_dict(response.debug),
        "warnings": response.warnings,
    }
    debug_json = json.dumps(debug_payload, ensure_ascii=False, indent=2)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Preview - {html.escape(response.title)}</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      color: #111;
      margin: 32px auto;
      max-width: 900px;
      line-height: 1.6;
      padding: 0 24px 80px;
    }}
    .meta {{
      color: #666;
      font-size: 0.9rem;
      margin: 4px 0 24px;
    }}
    section {{
      margin: 24px 0;
      padding: 16px 0;
      border-top: 1px solid #eee;
    }}
    h1 {{
      margin-bottom: 6px;
    }}
    h2 {{
      margin: 0 0 8px;
      font-size: 1.1rem;
      color: #222;
    }}
    pre {{
      background: #f6f6f6;
      padding: 12px;
      overflow-x: auto;
      font-size: 0.9rem;
    }}
  </style>
</head>
<body>
  <h1>{html.escape(response.title)}</h1>
  <div class="meta">
    Slug: {html.escape(response.slug)} |
    Source type: {html.escape(response.source_type)} |
    Source filename: {html.escape(response.source_filename)}
  </div>

  <section>
    <h2>Excerpt</h2>
    <p>{html.escape(response.excerpt)}</p>
  </section>

  <section>
    <h2>Meta Description</h2>
    <p>{html.escape(response.meta_description)}</p>
  </section>

  <section>
    <h2>Image Prompt</h2>
    <pre>{html.escape(response.image_prompt)}</pre>
  </section>

  <section>
    <h2>Content</h2>
    {response.clean_html}
  </section>

  <section>
    <h2>Debug</h2>
    <pre>{html.escape(debug_json)}</pre>
  </section>
</body>
</html>
"""


def render_error_html(error_type: str, message: str, details: Optional[Any] = None) -> str:
    details_json = json.dumps(details, ensure_ascii=False, indent=2) if details else ""
    details_block = (
        f"<pre>{html.escape(details_json)}</pre>" if details_json else "<p>No details.</p>"
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Preview Error</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      color: #111;
      margin: 32px auto;
      max-width: 800px;
      line-height: 1.6;
      padding: 0 24px 80px;
    }}
    pre {{
      background: #f6f6f6;
      padding: 12px;
      overflow-x: auto;
      font-size: 0.9rem;
    }}
  </style>
</head>
<body>
  <h1>Preview Error</h1>
  <p><strong>Type:</strong> {html.escape(str(error_type))}</p>
  <p><strong>Message:</strong> {html.escape(str(message))}</p>
  <section>
    <h2>Details</h2>
    {details_block}
  </section>
</body>
</html>
"""


def format_validation_errors(errors: List[Dict[str, Any]]) -> Any:
    cleaned: List[Dict[str, str]] = []
    for err in errors:
        loc = [str(part) for part in err.get("loc", []) if part not in {"body", "query", "path"}]
        field = ".".join(loc) if loc else "request"
        msg = err.get("msg", "Invalid value")
        if field == "source_url":
            msg = "must be http/https and not private or localhost"
        cleaned.append({"field": field, "message": msg})
    if len(cleaned) == 1:
        return cleaned[0]
    return cleaned


def normalize_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    val = str(value).strip()
    return val or None


def validate_source_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="source_url must be http or https.")
    if not parsed.netloc:
        raise HTTPException(status_code=400, detail="source_url is invalid.")

    hostname = parsed.hostname
    if not hostname:
        raise HTTPException(status_code=400, detail="source_url is invalid.")

    lowered = hostname.lower()
    if lowered in {"localhost", "127.0.0.1", "::1"} or lowered.endswith(".local"):
        raise HTTPException(status_code=400, detail="source_url points to a blocked host.")

    try:
        ip = ipaddress.ip_address(lowered)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            raise HTTPException(status_code=400, detail="source_url points to a blocked IP.")
    except ValueError:
        pass


def detect_source(source_url: str) -> Tuple[str, str, str]:
    doc_id = extract_google_doc_id(source_url)
    if doc_id:
        export_url = f"https://docs.google.com/document/d/{doc_id}/export?format=docx"
        filename = f"google_doc_{doc_id}.docx"
        return "google_doc", filename, export_url

    parsed = urlparse(source_url)
    filename = Path(parsed.path).name or "document.docx"
    return "docx_url", filename, source_url


def extract_google_doc_id(url: str) -> Optional[str]:
    parsed = urlparse(url)
    if not parsed.hostname:
        return None
    if "docs.google.com" not in parsed.hostname:
        return None
    match = re.search(r"/document/d/([a-zA-Z0-9_-]+)", parsed.path)
    if not match:
        return None
    return match.group(1)


def download_docx(url: str, is_google: bool) -> bytes:
    try:
        response = SESSION.get(
            url,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            allow_redirects=True,
            stream=True,
        )
    except requests.RequestException:
        raise HTTPException(status_code=422, detail="Failed to fetch source_url.")

    try:
        if response.status_code != 200:
            if is_google:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "Google Doc is not accessible. Please share it as \"anyone with the link can view\" "
                        "or share with the service account in the future."
                    ),
                )
            raise HTTPException(status_code=422, detail="Failed to fetch source_url.")

        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                if int(content_length) > MAX_DOCX_SIZE:
                    raise HTTPException(status_code=413, detail="DOCX is larger than 25 MB.")
            except ValueError:
                pass

        buffer = BytesIO()
        total = 0
        for chunk in response.iter_content(chunk_size=65536):
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_DOCX_SIZE:
                raise HTTPException(status_code=413, detail="DOCX is larger than 25 MB.")
            buffer.write(chunk)

        data = buffer.getvalue()
        if not data.startswith(b"PK"):
            if is_google:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "Google Doc is not accessible. Please share it as \"anyone with the link can view\" "
                        "or share with the service account in the future."
                    ),
                )
            raise HTTPException(status_code=422, detail="Downloaded file is not a DOCX.")

        return data
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=422, detail="Failed to download source_url.")
    finally:
        response.close()


def convert_docx_to_html(docx_bytes: bytes) -> Tuple[str, List[str]]:
    result = mammoth.convert_to_html(BytesIO(docx_bytes))
    warnings = []
    for msg in result.messages or []:
        warnings.append(f"mammoth: {msg}")
    return result.value, warnings


def sanitize_html(html: str, options: ConvertOptions) -> str:
    soup = BeautifulSoup(html, "lxml")

    if options.remove_images:
        for img in soup.find_all("img"):
            img.decompose()

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if href.startswith("https://www.google.com/url") or href.startswith("http://www.google.com/url"):
            parsed = urlparse(href)
            query = parse_qs(parsed.query)
            real_url = query.get("q", query.get("url", [""]))[0]
            if real_url:
                link["href"] = real_url

    if options.fix_headings:
        last_level = 2
        for heading in soup.find_all(re.compile(r"^h[1-6]$")):
            level = int(heading.name[1])
            if level == 1:
                level = 2
            if level < 2:
                level = 2
            if level > 4:
                level = 4
            if level > last_level + 1:
                level = last_level + 1
            heading.name = f"h{level}"
            last_level = level

    for tag in soup.find_all(True):
        if "style" in tag.attrs:
            del tag.attrs["style"]
        if "class" in tag.attrs:
            del tag.attrs["class"]

    for tag in soup.find_all(["p", "span"]):
        if tag.get_text(strip=True):
            continue
        if tag.find(["img", "table", "ul", "ol", "li", "strong", "em", "a", "br"]):
            continue
        tag.decompose()

    return html_from_soup(soup)


def extract_title(clean_html: str) -> Tuple[str, Optional[str]]:
    soup = BeautifulSoup(clean_html, "lxml")
    heading = soup.find(re.compile(r"^h[1-6]$"))
    if heading and heading.get_text(strip=True):
        title = heading.get_text(" ", strip=True)
        return normalize_title(title), None

    first_paragraph = soup.find("p")
    if first_paragraph and first_paragraph.get_text(strip=True):
        text = first_paragraph.get_text(" ", strip=True)
        sentence = first_sentence(text)
        return normalize_title(sentence), "No heading found; title derived from first paragraph."

    fallback_title = "Beitrag"
    return fallback_title, "No heading or paragraph found; using fallback title."


def remove_duplicate_title_heading(clean_html: str, title: str) -> str:
    if not clean_html or not title:
        return clean_html

    soup = BeautifulSoup(clean_html, "lxml")
    heading = soup.find(["h1", "h2", "h3"])
    if not heading:
        return clean_html

    if heading.name in {"h1", "h2"}:
        heading_text = normalize_text_for_match(heading.get_text(" ", strip=True))
        title_text = normalize_text_for_match(title)
        if heading_text and title_text and heading_text == title_text:
            heading.decompose()
            return html_from_soup(soup)

    return clean_html


def normalize_text_for_match(text: str) -> str:
    cleaned = text.strip().lower()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"[\s\.,;:!\?\-–—]+$", "", cleaned)
    return cleaned


def normalize_title(title: str) -> str:
    cleaned = title.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if cleaned.endswith((".", ";", ":", ",")):
        cleaned = cleaned[:-1].strip()
    return cleaned


def slugify(title: str, max_length: int) -> Tuple[str, Optional[str]]:
    warning = None
    text = title.lower()
    text = text.replace("&", " und ")
    text = (
        text.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text)
    text = text.strip("-")

    if not text:
        timestamp = datetime.utcnow().strftime("%y%m%d%H%M%S")
        text = f"post-{timestamp}"
        warning = "Slug was empty; used fallback slug with timestamp."

    words = [w for w in text.split("-") if w]
    if len(words) < 3:
        padding = ["beitrag", "artikel", "post"]
        for pad in padding:
            if len(words) >= 3:
                break
            words.append(pad)
    if len(words) > 6:
        words = words[:6]

    text = "-".join(words)

    if len(text) > max_length:
        while len(text) > max_length and "-" in text:
            text = "-".join(text.split("-")[:-1])
        if len(text) > max_length:
            text = text[:max_length].rstrip("-")
        text = re.sub(r"-+", "-", text).strip("-")

    return text, warning


def generate_excerpt(clean_html: str, max_length: int) -> Tuple[str, Optional[str]]:
    soup = BeautifulSoup(clean_html, "lxml")
    paragraph = None
    for p in soup.find_all("p"):
        if p.get_text(strip=True):
            paragraph = p
            break

    if paragraph:
        text = paragraph.get_text(" ", strip=True)
        text = normalize_whitespace(text)
        sentences = split_sentences(text)
        excerpt_source = " ".join(sentences[:3]).strip() if sentences else text
        excerpt_source = normalize_whitespace(excerpt_source)
        return truncate_excerpt(excerpt_source, max_length), None

    full_text = normalize_whitespace(soup.get_text(" ", strip=True))
    if full_text:
        return truncate_excerpt(full_text, max_length), "No paragraph found; excerpt derived from full text."

    return "", "No text found for excerpt."


def generate_meta_description(clean_html: str, max_length: int) -> Tuple[str, Optional[str]]:
    soup = BeautifulSoup(clean_html, "lxml")
    paragraph = soup.find("p")
    intro_text = paragraph.get_text(" ", strip=True) if paragraph else soup.get_text(" ", strip=True)
    intro_text = normalize_whitespace(intro_text)

    if not intro_text:
        return "", "No text found for meta description."

    sentences = split_sentences(intro_text)
    description = " ".join(sentences[:2]).strip() if sentences else intro_text
    description = normalize_whitespace(description)
    description = remove_quotes(description)
    description = remove_hashtags(description)
    description = remove_emoji(description)
    description = finalize_text_snippet(description, max_length)

    return description, None


def finalize_text_snippet(text: str, max_length: int) -> str:
    if len(text) <= max_length:
        return trim_trailing_punct(text)
    truncated = truncate_at_word_boundary(text, max_length)
    return trim_trailing_punct(truncated)


def truncate_at_word_boundary(text: str, max_length: int) -> str:
    if len(text) <= max_length:
        return text
    cutoff = text[: max_length + 1]
    if " " in cutoff:
        cutoff = cutoff[: cutoff.rfind(" ")]
    else:
        cutoff = text[:max_length]
    return cutoff.strip()


def trim_trailing_punct(text: str) -> str:
    return re.sub(r"[\s\.,;:!\?\-–—]+$", "", text).strip()


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def split_sentences(text: str) -> List[str]:
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [p.strip() for p in parts if p.strip()]


def first_sentence(text: str) -> str:
    sentences = split_sentences(text)
    return sentences[0] if sentences else text


def remove_quotes(text: str) -> str:
    return text.strip('"“”')


def remove_hashtags(text: str) -> str:
    return re.sub(r"#\S+", "", text).strip()


def remove_emoji(text: str) -> str:
    return re.sub(
        r"[\U0001F300-\U0001F6FF\U0001F700-\U0001F9FF\U0001FA00-\U0001FAFF\u2600-\u26FF\u2700-\u27BF]",
        "",
        text,
    ).strip()


def extract_intro_paragraph(clean_html: str) -> str:
    if not clean_html:
        return ""
    soup = BeautifulSoup(clean_html, "lxml")
    for p in soup.find_all("p"):
        if p.get_text(strip=True):
            return normalize_whitespace(p.get_text(" ", strip=True))
    return ""


def adjust_excerpt_if_duplicate(
    clean_html: str,
    excerpt: str,
    meta_description: str,
    max_length: int,
) -> str:
    if not excerpt or excerpt != meta_description:
        return excerpt

    soup = BeautifulSoup(clean_html, "lxml")
    paragraph = None
    for p in soup.find_all("p"):
        if p.get_text(strip=True):
            paragraph = p
            break

    if not paragraph:
        return excerpt

    full_text = normalize_whitespace(paragraph.get_text(" ", strip=True))
    if not full_text or len(full_text) <= len(excerpt):
        return excerpt

    extended = truncate_excerpt(full_text, max_length)
    if extended and extended != excerpt:
        return extended

    sentences = split_sentences(full_text)
    if len(sentences) > 1:
        combined = normalize_whitespace(" ".join(sentences[:4]))
        extended = truncate_excerpt(combined, max_length)
        if extended and extended != excerpt:
            return extended

    return excerpt


def truncate_excerpt(text: str, max_length: int) -> str:
    if not text:
        return ""
    text = normalize_whitespace(text)
    if len(text) <= max_length:
        return trim_trailing_punct(text)

    boundary = find_sentence_boundary(text, max_length)
    if boundary is None:
        boundary = find_last_comma(text, max_length)
    if boundary is None:
        boundary = find_last_whitespace(text, max_length)
    if boundary is None:
        boundary = max_length

    excerpt = text[:boundary].strip()
    excerpt = trim_trailing_punct(excerpt)
    excerpt = remove_dangling_function_words(excerpt)

    if not excerpt:
        excerpt = truncate_at_word_boundary(text, max_length)
        excerpt = trim_trailing_punct(excerpt)

    return excerpt


def find_sentence_boundary(text: str, max_length: int) -> Optional[int]:
    for match in re.finditer(r"[.!?]", text):
        end = match.end()
        if end <= max_length:
            last_end = end
        else:
            break
    return locals().get("last_end")


def find_last_comma(text: str, max_length: int) -> Optional[int]:
    idx = text.rfind(",", 0, max_length + 1)
    return idx + 1 if idx != -1 else None


def find_last_whitespace(text: str, max_length: int) -> Optional[int]:
    idx = text.rfind(" ", 0, max_length + 1)
    return idx if idx != -1 else None


def remove_dangling_function_words(text: str) -> str:
    bad_words = {
        "und",
        "oder",
        "aber",
        "weil",
        "dass",
        "wobei",
        "zwar",
        "eine",
        "einen",
        "einem",
        "einer",
        "ein",
        "eines",
        "einerseits",
        "andererseits",
    }
    cleaned = text.strip()
    while cleaned:
        last_token = re.sub(r"[\\s\\.,;:!\\?\\-–—]+$", "", cleaned).split(" ")[-1].lower()
        if last_token in bad_words:
            cut = cleaned.rfind(" ")
            if cut == -1:
                cleaned = ""
            else:
                cleaned = cleaned[:cut].strip()
            continue
        break
    return cleaned


def html_from_soup(soup: BeautifulSoup) -> str:
    if soup.body:
        cleaned = "".join(str(child) for child in soup.body.contents)
    else:
        cleaned = "".join(str(child) for child in soup.contents)
    return cleaned.strip()


def ms_since(start: float) -> int:
    return int((time.monotonic() - start) * 1000)
