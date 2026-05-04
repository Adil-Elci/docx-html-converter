# Elci Services

---

## Session Notes: DB Tunnel Fix + Author Sync Integration (2026-04-30)

### What was done

**1. Fixed SSH tunnel authentication to the remote Dokploy PostgreSQL database**

The root issues were:
- `portal_backend/.env.live` had a wrong database name and port — fixed to match `.env`.
- The macOS LaunchAgent (`~/Library/LaunchAgents/com.adil.postgres-tunnel.plist`) was pointing to a stale Docker internal IP (`172.19.0.2`) instead of the stable external host (`localhost:9876`). Fixed the plist and reloaded it.
- The PostgreSQL user password had been set with a base64-generated string containing `/` and `=` characters which broke URL parsing. Regenerated with `openssl rand -hex 32` and applied via `ALTER USER`.

After fixes, `sync_wp_authors.py --only-missing --dry-run` connected successfully and reported 10 author records that needed updating.

**2. Merged WordPress author sync into `run_master_site_sync.py`**

When new publishing sites are added via the master site Excel sync, their WordPress `author_id` and `author_name` are now automatically fetched from the WP REST API (`/wp-json/wp/v2/users/me`) and written to `publishing_site_credentials`.

Functions added to `portal_backend/scripts/db_updater/run_master_site_sync.py`:
- `_fetch_wp_author()` — calls the WP REST API with basic auth and returns `(author_id, author_name)`
- `_sync_author_info_for_credentials()` — iterates over newly written credential rows, calls `_fetch_wp_author`, updates the DB

The author sync runs automatically as step 92 in `run_master_sync_for_file()`, after the credentials upsert, and only when `not dry_run and credential_rows_to_write`.

---

### SSH tunnel architecture

The production database lives on a Dokploy server at `76.13.143.101:9876`. Local scripts connect via an SSH tunnel.

**Always-on tunnel (LaunchAgent)**
```
local:5432 → SSH → 76.13.143.101 → localhost:9876 (Postgres container)
```
Managed by `/Users/Adil/Library/LaunchAgents/com.adil.postgres-tunnel.plist`. Starts automatically on login and restarts on failure (`KeepAlive: true`). To reload after editing:
```bash
launchctl unload ~/Library/LaunchAgents/com.adil.postgres-tunnel.plist
launchctl load  ~/Library/LaunchAgents/com.adil.postgres-tunnel.plist
```

**DATABASE_URL format (in `.env` and `.env.live`)**
```
postgresql://Adil:<hex-password>@76.13.143.101:9876/article-automation-database
```

`ssh_tunnel_helper.py` rewrites this URL to `localhost:5432` when establishing a tunnel inside a script.

---

### Running the master site sync

```bash
cd portal_backend
# dry run (no writes, author sync skipped)
python scripts/db_updater/run_master_site_sync.py --dry-run

# live run (writes sites, credentials, and fetches WP author info for new credentials)
python scripts/db_updater/run_master_site_sync.py
```

The script reads `portal_backend/scripts/db_updater/master_site_info/master_site_file.xlsx` and syncs it into `master_site_info`, `publishing_sites`, `publishing_site_credentials`, and `publishing_site_admin_credentials`.

### Running the standalone author sync (backfill)

```bash
cd portal_backend
# dry run — shows what would be updated
python scripts/sync_wp_authors.py --only-missing --dry-run

# live run — updates author_id and author_name for credentials missing them
python scripts/sync_wp_authors.py --only-missing
```

---

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

Dokploy exec command (inside backend container):
```bash
cd /app
/opt/venv/bin/alembic -c /app/alembic.ini upgrade head
```

## Security

### Measures in place

**1. Hostinger VPS firewall — port 9876 blocked externally**

Port 9876 (the PostgreSQL external port published by Docker) is not reachable from the internet. The Hostinger firewall (`dokploy-ports`) allows only ports 22, 80, 443, and 3000. The catch-all `Drop / Any / Any` rule blocks everything else, including 9876.

Why Docker's own UFW doesn't help: Docker bypasses UFW by writing iptables rules directly. The Hostinger network-level firewall operates before traffic reaches the VPS, so it blocks Docker-published ports that UFW cannot.

Live services connect via the internal Docker hostname (`article-automation-article-automation-database-ilvbzx:5432`) and are unaffected. Local scripts connect via the SSH tunnel (see below).

**2. WordPress credential encryption at rest**

`wp_app_password` (in `publishing_site_credentials`) and `wp_admin_username` / `wp_admin_password` (in `publishing_site_admin_credentials`) are encrypted in the database using Fernet (AES-128-CBC + HMAC-SHA256). The encryption key is stored only as an environment variable in Dokploy — never in the codebase.

The ORM decrypts values automatically on read via a `TypeDecorator` ([api/credential_crypto.py](portal_backend/api/credential_crypto.py)), so application code and API responses always receive plaintext. TablePlus shows the raw Fernet tokens.

The master sync script ([scripts/db_updater/run_master_site_sync.py](portal_backend/scripts/db_updater/run_master_site_sync.py)) also encrypts before writing and decrypts when comparing existing values, so the sync remains idempotent.

Required Dokploy environment variable (backend service):
```
WP_CREDENTIAL_ENCRYPTION_KEY=<44-char Fernet key>
```

Generate a new key:
```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

If the encryption key is ever rotated, run the migration script again after updating the env var (it will re-encrypt all rows with the new key):
```bash
cd portal_backend
WP_CREDENTIAL_ENCRYPTION_KEY=<new-key> python scripts/encrypt_wp_credentials.py
```

**3. Login rate limiter — X-Forwarded-For spoofing prevention**

The login rate limiter keys on the client IP. Previously it trusted the `X-Forwarded-For` header unconditionally, allowing an attacker to rotate that header and bypass the lockout window.

Fixed in [api/routers/auth_routes.py](portal_backend/api/routers/auth_routes.py): `X-Forwarded-For` is now only trusted when the direct TCP connection comes from a known proxy IP (Traefik). All other connections use `request.client.host` instead.

Required Dokploy environment variable (backend service):
```
TRUSTED_PROXY_IPS=<Traefik's internal Docker IP>
```

To find Traefik's IP on the backend network:
```bash
# On the VPS
docker network inspect dokploy-network --format '{{range .Containers}}{{.Name}}: {{.IPv4Address}}{{"\n"}}{{end}}'
```

Look for `dokploy-traefik` in the output and use that IP (without the `/24` suffix).

**4. Password reset — user enumeration prevented**

Previously `/auth/password-reset/request` returned HTTP 404 if the email was not registered, allowing an attacker to enumerate valid accounts. It now always returns HTTP 200 with a neutral message regardless of whether the email exists, is inactive, or is valid. The actual outcome is still logged server-side.

---

### Reading credentials in plaintext

TablePlus shows raw Fernet tokens, not plaintext. To view decrypted credentials use the audit script (reads via ORM, so the TypeDecorator decrypts automatically):

```bash
cd portal_backend

# Application credentials (wp_username / wp_app_password)
WP_CREDENTIAL_ENCRYPTION_KEY=<key> python scripts/show_credentials.py

# Admin credentials (wp_admin_username / wp_admin_password)
WP_CREDENTIAL_ENCRYPTION_KEY=<key> python scripts/show_credentials.py --admin
```

---

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

# Local Submitted Article Conversion Service

## What it does
This service runs locally and converts submitted article documents into WordPress-ready HTML and metadata. It accepts a `source_url`, downloads a DOCX (or Google Docs export), converts and sanitizes the HTML, and returns German title/slug/excerpt/meta plus an English image prompt.

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
