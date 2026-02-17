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
- `site_credentials` is not exported.

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
- `GOOGLE_SHEETS_EXPORT_TABLES=clients,sites,jobs` to export only a subset

Recommended operations:
- Schedule this script as a cron/worker job in Dokploy.
- Share the Google Sheet with your team as viewer-only.

## Sync WordPress Authors To DB
Use `scripts/sync_wp_authors.py` to populate `site_credentials.author_id` and `site_credentials.author_name` from each site's WordPress `/users/me` endpoint.

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
- `--credential-id <site_credentials_uuid>` to sync one credential.
- `--only-missing` to skip rows that already have both author fields.
- `--include-inactive-sites` if needed.

## Make.com Replacement Webhook
Endpoint:
- `POST /automation/guest-post-webhook`
- Content types supported: `application/json`, `application/x-www-form-urlencoded`, `multipart/form-data`

Supported payload fields:
- `source_type`: `google-doc`, `word-doc`, or `docx-upload`
- `target_site`: domain/URL/site-id that maps to an active row in `sites`
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
- `LEONARDO_MODEL_ID` (default: `1dd50843-d653-4516-a8e3-f0238ee453ff`)
- `AUTOMATION_IMAGE_WIDTH` (default: `1024`)
- `AUTOMATION_IMAGE_HEIGHT` (default: `576`)
- `AUTOMATION_POST_AUTHOR_ID` (default: `4`)
- `AUTOMATION_POST_STATUS` (default: `publish`)
- `AUTOMATION_REQUEST_TIMEOUT_SECONDS` (default: `60`)
- `AUTOMATION_IMAGE_POLL_TIMEOUT_SECONDS` (default: `90`)
- `AUTOMATION_IMAGE_POLL_INTERVAL_SECONDS` (default: `2`)
- `AUTOMATION_DEFAULT_CLIENT_ID` (optional fallback client for async/shadow)
- `AUTOMATION_WORKER_ENABLED` (default: `true`)
- `AUTOMATION_WORKER_POLL_SECONDS` (default: `2`)
- `AUTOMATION_JOB_MAX_ATTEMPTS` (default: `3`)
- `AUTOMATION_SHADOW_WEBHOOK_URL` (optional Make webhook URL for shadow mode)
- `AUTOMATION_LOG_LEVEL` (default: `INFO`)

Image upload behavior:
- If WordPress returns HTTP `413` during media upload, the pipeline retries with progressively smaller generated image sizes (`768x432`, `640x360`, `512x288`) before failing.

Author selection precedence:
- Webhook `author` field (if provided)
- `site_credentials.author_id` (new per-site default)
- `AUTOMATION_POST_AUTHOR_ID` env fallback

Debugging workflow (recommended):
- Keep `execution_mode=async` in production; webhook returns `job_id` and `submission_id`.
- Query job status by idempotency key / job / submission:
  - `GET /automation/status?idempotency_key=<your_submission_key>`
  - `GET /automation/status?job_id=<job_uuid>`
  - `GET /automation/status?submission_id=<submission_uuid>`
- Inspect raw job events:
  - `GET /jobs/<job_uuid>/events`
- For immediate error feedback while testing, temporarily use `execution_mode=sync` so API returns upstream errors directly.
