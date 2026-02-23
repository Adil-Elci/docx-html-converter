# Portal Backend

## Database Policy
- `DATABASE_URL` is required and must point to the live/production Postgres database.
- Localhost/loopback database URLs are intentionally rejected (`localhost`, `127.0.0.1`, `::1`, `0.0.0.0`).

## Run
```bash
cd portal_backend
pip install -r requirements.txt
export DATABASE_URL="postgresql://<prod-user>:<prod-password>@<prod-host>:5432/<prod-db>"
alembic upgrade head
uvicorn api.server:app --reload --host 0.0.0.0 --port 8000
```

## Auth Foundation (Phase 1)
New endpoints:
- `POST /auth/login`
- `POST /auth/logout`
- `GET /auth/me`
- `POST /auth/password-reset/request`
- `POST /auth/password-reset/confirm`

Required auth env:
- `AUTH_JWT_SECRET` (required, long random secret)
- `AUTH_JWT_ALGORITHM` (default: `HS256`)
- `AUTH_ACCESS_TOKEN_TTL_MINUTES` (default: `10080`)
- `AUTH_COOKIE_SECURE` (default: `false`)
- `AUTH_COOKIE_SAMESITE` (default: `lax`)
- `AUTH_LOGIN_RATE_LIMIT_WINDOW_SECONDS` (default: `300`)
- `AUTH_LOGIN_RATE_LIMIT_MAX_ATTEMPTS` (default: `8`)
- `AUTH_PASSWORD_RESET_TOKEN_TTL_MINUTES` (default: `60`)
- `AUTH_PASSWORD_RESET_URL_BASE` (required for reset links, e.g. frontend domain)
- SMTP settings for reset emails:
  - `SMTP_HOST`
  - `SMTP_PORT` (default `587`)
  - `SMTP_USE_TLS` (default `true`)
  - `SMTP_USE_SSL` (default `false`)
  - `SMTP_USERNAME`
  - `SMTP_PASSWORD`
  - `SMTP_FROM_EMAIL`
  - `SMTP_FROM_NAME` (default `Elci Solutions`)

Bootstrap first admin user:
```bash
cd portal_backend
python scripts/create_admin_user.py --email admin@example.com --password "replace_me_123"
```

## RBAC Enforcement (Phase 2)
Role model:
- `admin`: full access to management routes
- `client`: scoped access only to mapped clients/publishing sites (`client_users` + `client_publishing_site_access`)

Enforced behavior:
- Admin-only routes:
  - `POST/PATCH /clients`
  - `POST/PATCH /sites`
  - `GET/POST/PATCH /site-credentials*`
  - `GET/POST/PATCH /client-site-access*`
  - `POST/PATCH /jobs*` and `POST /jobs/{id}/events`, `POST /jobs/{id}/assets`
- Client-scoped routes:
  - `GET /clients`, `GET /sites`
  - `GET/POST/PATCH /submissions*`
  - `GET /jobs*`, `GET /jobs/{id}*`
  - `GET /automation/status`
- `POST /automation/guest-post-webhook` remains compatible for unauthenticated external callers (Ninja/Make), but authenticated client users are scoped to their allowed client/site mappings and cannot run `execution_mode=sync`.

## Admin User Management + Hardening (Phase 4)
Admin-only endpoints:
- `GET /admin/users`
- `POST /admin/users`
- `PATCH /admin/users/{user_id}`

Notes:
- `POST/PATCH /auth/login` now has in-memory rate limiting by `IP + email` key.
- Last active admin cannot be demoted/deactivated (`409` guard).
- Client-role users can be mapped to clients via `client_ids` in admin user payloads.

## Migration Ownership
- `portal_backend` is the only service that runs migrations.
- Deploy startup runs: wait for Postgres, advisory lock, `alembic upgrade head`, unlock, start API.
- Converter and database containers do not run Alembic.

## Docker / Dokploy
- Container startup is handled by `entrypoint.sh`.
- Docker image uses `ENTRYPOINT ["/app/entrypoint.sh"]`.
- For Dokploy, use the Dockerfile and do not override the run command.
- Keep `REQUIRE_DB_AT_HEAD=true` (default) so API startup fails if DB is not at Alembic head.

Dokploy settings that enforce migrations on every backend deploy:
- Service root: `portal_backend/` (or Dockerfile path `portal_backend/Dockerfile` from repo root).
- Build type: Dockerfile.
- Dockerfile: `portal_backend/Dockerfile`.
- Start command override: empty.
- Entrypoint override: empty.
- Required env: `DATABASE_URL` (live DB), optional `MIGRATION_LOCK_KEY`, `DB_WAIT_TIMEOUT_SECONDS`.
- Verification: check deploy logs for `alembic upgrade head` and `Postgres is reachable`.

## One-way Google Sheets Sync (DB -> Sheets only)
Use `scripts/sync_db_to_sheets.py` to export selected safe columns from Postgres to Google Sheets.

Security behavior:
- Export is one-way: Postgres -> Google Sheets.
- No Sheet data is read back into the DB.
- Sensitive credential data is excluded by allowlist.
- `publishing_site_credentials` is not exported.

Setup:
```bash
cd portal_backend
pip install -r requirements.txt
export DATABASE_URL="postgresql://<prod-user>:<prod-password>@<prod-host>:5432/<prod-db>"
export GOOGLE_SHEETS_SPREADSHEET_ID="<spreadsheet_id>"
export GOOGLE_SERVICE_ACCOUNT_FILE="/path/to/google-service-account.json"
python scripts/sync_db_to_sheets.py
```

Optional:
- `GOOGLE_SERVICE_ACCOUNT_JSON` instead of `GOOGLE_SERVICE_ACCOUNT_FILE`
- `GOOGLE_SHEETS_EXPORT_TABLES=clients,publishing_sites,jobs` to export only a subset

Recommended operations:
- Schedule this script as a cron/worker job in Dokploy.
- Share the Google Sheet with your team as viewer-only.

## Sync WordPress Authors To DB
Use `scripts/sync_wp_authors.py` to populate `publishing_site_credentials.author_id` and `publishing_site_credentials.author_name` from each publishing site's WordPress `/users/me` endpoint.

Setup:
```bash
cd portal_backend
pip install -r requirements.txt
export DATABASE_URL="postgresql://<prod-user>:<prod-password>@<prod-host>:5432/<prod-db>"
python scripts/sync_wp_authors.py --dry-run
python scripts/sync_wp_authors.py
```

Optional filters:
- `--site-url https://eintragnews.de` to sync one site (with/without `www` match).
- `--credential-id <publishing_site_credentials_uuid>` to sync one credential.
- `--only-missing` to skip rows that already have both author fields.
- `--include-inactive-sites` if needed.

## Sync WordPress Categories To DB
Use `scripts/sync_wp_categories.py` to populate `publishing_site_categories` from each publishing site's WordPress categories API.

Setup:
```bash
cd portal_backend
pip install -r requirements.txt
export DATABASE_URL="postgresql://<prod-user>:<prod-password>@<prod-host>:5432/<prod-db>"
python scripts/sync_wp_categories.py --dry-run
python scripts/sync_wp_categories.py
```

Optional:
- `--site-url https://eintragnews.de` to sync one site.
- `--default-slugs guest-post,news,allgemein` to seed `publishing_site_default_categories` in slug order.
- `--replace-defaults` (with `--default-slugs`) to disable existing defaults not in the matched list.

## Make.com Replacement Webhook
Endpoint:
- `POST /automation/guest-post-webhook`
- Content types supported: `application/json`, `application/x-www-form-urlencoded`, `multipart/form-data`

Supported payload fields:
- `source_type`: `google-doc`, `word-doc`, or `docx-upload`
- `publishing_site`: domain/URL/site-id that maps to an active row in `publishing_sites`
- `execution_mode`: `sync`, `async`, `shadow` (default: `async`)
- `doc_url`: required for `google-doc`
- `docx_file`: required for `word-doc`/`docx-upload` (raw URL or HTML anchor snippet with `href=...`)
- `client_id` (preferred for `async`/`shadow`)
- `client_name` (supported for `async`/`shadow`; must uniquely match one active client)
- `client_id` or `client_name` is required for `async`/`shadow`, unless `AUTOMATION_DEFAULT_CLIENT_ID` is set
- `idempotency_key` (optional but recommended for deduplication in async mode)
- `backlink_placement` (optional, default: `intro`)
- `post_status` (optional): `draft` or `publish`
- `author` (optional): WordPress author ID override

Execution behavior:
- `sync`: processes immediately in request/response cycle.
- `async`: queues in DB (`submissions` + `jobs`) and returns `job_id`.
- `shadow`: same as `async`, and also forwards payload to `AUTOMATION_SHADOW_WEBHOOK_URL` if configured.

Runtime env vars:
- `AUTOMATION_CONVERTER_ENDPOINT` (default: `https://elci.live/convert`)
- `LEONARDO_API_KEY` (required)
- `LEONARDO_BASE_URL` (default: `https://cloud.leonardo.ai/api/rest/v1`)
- Leonardo model is fixed to Flux Schnell (`1dd50843-d653-4516-a8e3-f0238ee453ff`)
- `AUTOMATION_IMAGE_WIDTH` (default: `1024`)
- `AUTOMATION_IMAGE_HEIGHT` (default: `576`)
- `AUTOMATION_POST_AUTHOR_ID` (default: `4`)
- `AUTOMATION_POST_STATUS` (default: `publish`)
- `AUTOMATION_REQUEST_TIMEOUT_SECONDS` (default: `60`)
- `AUTOMATION_IMAGE_POLL_TIMEOUT_SECONDS` (default: `90`)
- `AUTOMATION_IMAGE_POLL_INTERVAL_SECONDS` (default: `2`)
- `AUTOMATION_CATEGORY_LLM_ENABLED` (default: `true`)
- `AUTOMATION_CATEGORY_LLM_API_KEY` (optional; if empty, fallback is `OPENAI_API_KEY` then `ANTHROPIC_API_KEY`)
- `AUTOMATION_CATEGORY_LLM_BASE_URL` (optional; auto-selects OpenAI/Anthropic default by available key)
- `AUTOMATION_CATEGORY_LLM_MODEL` (optional; auto-selects provider default by base URL)
- `AUTOMATION_CATEGORY_LLM_MAX_CATEGORIES` (default: `2`)
- `AUTOMATION_CATEGORY_LLM_CONFIDENCE_THRESHOLD` (default: `0.55`)
- `AUTOMATION_DEFAULT_CLIENT_ID` (optional fallback client for async/shadow)
- `AUTOMATION_ENFORCE_CLIENT_SITE_ACCESS` (default: `false`; set `true` to require `client_publishing_site_access` mapping)
- `AUTOMATION_WORKER_ENABLED` (default: `true`)
- `AUTOMATION_WORKER_POLL_SECONDS` (default: `2`)
- `AUTOMATION_JOB_MAX_ATTEMPTS` (default: `3`)
- `AUTOMATION_SHADOW_WEBHOOK_URL` (optional Make webhook URL for shadow mode)
- `AUTOMATION_LOG_LEVEL` (default: `INFO`)

Image upload behavior:
- If WordPress returns HTTP `413` during media upload, the pipeline retries with progressively smaller generated image sizes (`768x432`, `640x360`, `512x288`) before failing.

Author selection precedence:
- Webhook `author` field (if provided)
- `publishing_site_credentials.author_id` (new per-publishing-site default)
- `AUTOMATION_POST_AUTHOR_ID` env fallback

Category selection:
- If `AUTOMATION_CATEGORY_LLM_ENABLED=true` and `publishing_site_categories` are available for the publishing site, the backend asks the LLM to pick category IDs from that allowed list.
- Supported providers: OpenAI (`/chat/completions`) and Anthropic (`/messages`).
- If LLM selection fails/invalid/low confidence, it falls back to enabled `publishing_site_default_categories` in configured order.
- If no defaults exist, category is left to WordPress/site defaults.

Debugging workflow (recommended):
- Keep `execution_mode=async` in production; webhook returns `job_id` and `submission_id`.
- Query job status by idempotency key / job / submission:
  - `GET /automation/status?idempotency_key=<your_submission_key>`
  - `GET /automation/status?job_id=<job_uuid>`
  - `GET /automation/status?submission_id=<submission_uuid>`
- Inspect raw job events:
  - `GET /jobs/<job_uuid>/events`
- For immediate error feedback while testing, temporarily use `execution_mode=sync` so API returns upstream errors directly.
