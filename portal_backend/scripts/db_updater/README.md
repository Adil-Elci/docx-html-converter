# DB Updater

This folder contains:

- `run_master_site_sync.py` (recommended): sync one master site file into multiple DB tables
- `import_tabular_to_db.py`: generic config-driven tabular importer/updater (CSV/XLSX/Google Sheets)

## How to Update the Live Database

### Step 1 — Set the DATABASE_URL in portal_backend/.env

The live database runs on a Dokploy-managed VPS (Hostinger). The DB port is exposed externally.

Set `DATABASE_URL` in `portal_backend/.env` to:

```
DATABASE_URL=postgresql://Adil:4OIiID4x00y9XQ4J8OKL@76.13.143.101:9876/article-automation-database
```

- `76.13.143.101` — the VPS public IP
- `9876` — the host port that Dokploy maps to the DB container's internal port 5432
- `article-automation-database` — the database name

> The internal Docker hostname (`article-automation-article-automation-database-ilvbzx`) only resolves inside the Docker network on the server. Always use the public IP + exposed port when connecting from a local machine.

### Step 2 — Prepare the master file

Close the file in Excel if it is open (Excel creates a lockfile `~$master_site_file.xlsx` that will cause the script to skip the real file).

Make sure exactly one `.xlsx` or `.csv` file is in:

```
portal_backend/scripts/db_updater/master_site_info/
```

### Step 3 — Load the env and do a dry run

From the repo root:

```bash
cd portal_backend
set -a; source .env; set +a
python scripts/db_updater/run_master_site_sync.py --dry-run
```

Review the output — it shows how many rows would be written to each table and lists any issues.

### Step 4 — Apply for real

```bash
python scripts/db_updater/run_master_site_sync.py
```

A JSON report is written to `portal_backend/scripts/db_updater/reports/`.

### Step 5 — Revert .env after you're done

The `.env` file is used by the local dev server too. After syncing, restore `DATABASE_URL` to the local dev DB (or leave it pointing at live if intentional).

---

## Recommended Workflow (Master Site File)

Put exactly one CSV/XLSX file into:

- `portal_backend/scripts/db_updater/master_site_info/`

Then run:

```bash
python3 portal_backend/scripts/db_updater/run_master_site_sync.py --dry-run
python3 portal_backend/scripts/db_updater/run_master_site_sync.py
```

### What `run_master_site_sync.py` does

- reads one master file from `master_site_info/`
- applies default/derived rules:
  - `name` = site URL without `https://`
  - `wp_rest_base` = `/wp-json/wp/v2`
  - `status` defaults to `active`
  - `enabled` defaults to `true`
- syncs `master_site_info` (new source snapshot table)
- syncs `publishing_sites`
- syncs `publishing_site_credentials` (one credential row per site)
- **automatically fetches `author_id` and `author_name`** from each site's `/wp-json/wp/v2/users/me` endpoint using the stored credentials, and writes them to `publishing_site_credentials`. No separate step is needed for new sites.
- writes reports to `portal_backend/scripts/db_updater/reports/`
- leaves the master file in place (you keep updating the same file)

> Note: `portal_backend/scripts/sync_wp_authors.py` exists as a standalone tool for re-syncing author info (e.g. after a WP user rename), but is **not required** when adding new sites — `run_master_site_sync.py` already handles it.

### Expected Master File Columns

Required:

- `publishing_site_url` or `site_url`

Optional:

- `hosted_by`
- `host_panel`
- `status`
- `auth_type`
- `wp_username`
- `wp_app_password`
- `wp_admin_login_url` or `admin_login_url`
- `wp_admin_username` or `admin_username`
- `wp_admin_password` or `admin_password`
- `enabled`

If credentials are provided, both `wp_username` and `wp_app_password` must be present.

## Generic Importer (Optional)

Use when you want a one-off or non-site-related import with a JSON config:

```bash
python3 portal_backend/scripts/db_updater/import_tabular_to_db.py --config portal_backend/scripts/db_updater/examples/publishing_site_credentials_by_url.example.json --dry-run
python3 portal_backend/scripts/db_updater/import_tabular_to_db.py --config portal_backend/scripts/db_updater/examples/publishing_site_credentials_by_url.example.json
```

## Input Types

- `csv` via `input.path`
- `xlsx` via `input.path` (requires `openpyxl`)
- `google_sheet` via `input.url` (public/shareable sheet) or `input.sheet_id` + `input.gid`

## What It Does

- Reads tabular rows
- Maps source columns to target DB columns
- Supports lookup mapping (for example `site_url` -> `publishing_site_id`)
- Upserts rows using `match_columns`
- Outputs skipped-row reasons (and optional JSONL issue log)
- Supports recurring file-driven updates

## Config Notes

- `table`: target DB table name
- `match_columns`: unique key columns used for upsert
- `column_map`: mapping rules for each target column
- `database_url_env`: env var containing DB URL (defaults to `DATABASE_URL`)
- `allow_issues`: if `false`, exits non-zero when rows are skipped
- `issues_output_jsonl`: optional path for skipped-row details

The example config in `portal_backend/scripts/db_updater/examples/` is still available for direct credential imports by site URL.
