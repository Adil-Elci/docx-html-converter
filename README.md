# Elci Services

This repo now contains four separate services:
- `converter/` (document conversion API)
- `portal_backend/` (client portal API)
- `portal_frontend/` (client portal UI)
- `portal_backend/alembic/` (deploy-time schema migrations)

Migration ownership:
- Only `portal_backend` runs migrations on deploy.
- Converter never runs Alembic migrations.
- Database container never runs Alembic migrations.
- Backend deploys must run from `portal_backend/` Dockerfile entrypoint to execute `alembic upgrade head`.

## Portal Backend (quick run)
```bash
cd portal_backend
pip install -r requirements.txt
export DATABASE_URL="postgresql://<prod-user>:<prod-password>@<prod-host>:5432/<prod-db>"
alembic upgrade head
uvicorn api.server:app --reload --port 8001
```

Database policy:
- `DATABASE_URL` must always target the live/production Postgres instance.
- Localhost/loopback database URLs are not allowed.

## Portal Frontend (quick run)
```bash
cd portal_frontend
npm install
npm run dev
```

# Local Guest Post Conversion Service

## What it does
This service runs locally and converts guest post documents into WordPress-ready HTML and metadata. It accepts a `source_url`, downloads a DOCX (or Google Docs export), converts and sanitizes the HTML, and returns German title/slug/excerpt/meta plus an English image prompt.

## Supported inputs
- Google Docs share links
- Direct DOCX file URLs

### Google Docs permission requirement
The Google Doc must be publicly accessible ("Anyone with the link can view"). If it is not, the service returns a clear 422 error explaining the permission issue.

## Validation rules
- `publishing_site` must be `audit-net.de`.
- `post_status` must be `draft` or `publish`.
- `language` must be `de`.
- `source_url` must be a valid `http://` or `https://` URL and must not point to localhost or private IP ranges.
- Option bounds:
  - `max_slug_length`: 20..120
  - `max_meta_length`: 80..200
  - `max_excerpt_length`: 80..300
- Output constraints enforced in the response model:
  - `title` non-empty, max 200 chars
  - `slug` non-empty, max 120 chars
  - `meta_description` length <= 200
  - `excerpt` length <= 300
  - `clean_html` non-empty

Validation errors return HTTP 422 with an `ErrorResponse` payload.

## Endpoints

### `GET /health`
Returns `{"ok": true}`.

### `POST /convert`
Accepts JSON or multipart form data.

#### Example: JSON
```bash
curl -X POST http://localhost:8000/convert \
  -H "Content-Type: application/json" \
  -d '{"publishing_site":"audit-net.de","source_url":"https://docs.google.com/document/d/GOOGLE_DOC_ID/edit"}'
```

#### Example: multipart
```bash
curl -X POST http://localhost:8000/convert \
  -F "publishing_site=audit-net.de" \
  -F "source_url=https://docs.google.com/document/d/GOOGLE_DOC_ID/edit" \
  -F 'options={"remove_images":true,"fix_headings":true}'
```

#### Example response
```json
{
  "ok": true,
  "publishing_site": "audit-net.de",
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
```

## How to run locally
```bash
cd converter
python -m uvicorn api.server:app --host 0.0.0.0 --port 8000 --reload
```

## LLM configuration
Set `ANTHROPIC_API_KEY` in your environment (or `converter/.env`) to enable slug and image prompt generation.

## How to run tests
```bash
cd converter
pytest
```

## Make.com mapping notes
**Make sends:**
- `publishing_site` (required)
- `source_url` (required)
- `post_status` (optional)
- `language` (optional)
- `client_id` (optional)
- `post_id` (optional)
- `client_url` (optional)
- `options` (optional object or JSON string)

**Make receives:**
- `ok`, `publishing_site`, `source_url`, `source_type`, `source_filename`
- `title`, `slug`, `excerpt`, `meta_description`, `clean_html`
- `image_prompt`, `warnings`, `debug`

## Security notes
- Only `http` and `https` URLs are allowed.
- Localhost and private IP ranges are blocked to reduce SSRF risk.
- DOCX downloads are limited to 25 MB.

## Known limitations and next steps
- No authentication or rate limiting (local use only).
- No database or job queue for large files.
- Limited HTML sanitization rules; more aggressive cleanup may be needed for complex documents.
- Optional spaCy support is not bundled; install `de_core_news_sm` if you want NLP noun extraction.
- Potential next steps: add auth, caching, per-site HTML tuning, and background task processing.
