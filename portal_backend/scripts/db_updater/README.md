# DB Updater

This folder contains:

- `run_master_site_sync.py` (recommended): sync one master site file into multiple DB tables
- `import_tabular_to_db.py`: generic config-driven tabular importer/updater (CSV/XLSX/Google Sheets)

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
- writes reports to `portal_backend/scripts/db_updater/reports/`
- leaves the master file in place (you keep updating the same file)

### Expected Master File Columns

Required:

- `publishing_site_url`

Optional:

- `hosted_by`
- `host_panel`
- `status`
- `auth_type`
- `wp_username`
- `wp_app_password`
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
