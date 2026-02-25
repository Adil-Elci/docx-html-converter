# Import Utility

`import_tabular_to_db.py` imports CSV / XLSX / Google Sheets data into a database table using a JSON config.

## Usage

```bash
python3 portal_backend/scripts/db_updater/import_tabular_to_db.py --config portal_backend/scripts/db_updater/examples/publishing_site_credentials_by_url.example.json --dry-run
python3 portal_backend/scripts/db_updater/import_tabular_to_db.py --config portal_backend/scripts/db_updater/examples/publishing_site_credentials_by_url.example.json
```

## Auto Inbox Mode (Recommended for recurring CSV patches)

Drop exactly one file into:

- `portal_backend/scripts/db_updater/inbox/`

Then run:

```bash
python3 portal_backend/scripts/db_updater/run_inbox_update.py --dry-run
python3 portal_backend/scripts/db_updater/run_inbox_update.py
```

What it does automatically:

- reads the single file in `inbox/`
- detects the target table from headers / filename
- uses DB unique keys/PKs for upsert when possible
- applies a built-in adapter for `publishing_site_credentials` (supports `publishing_site_url` -> `publishing_site_id` lookup)
- writes a report to `portal_backend/scripts/db_updater/reports/`
- moves the file to `processed/` or `failed/` (non-dry-run)

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
- Supports automatic inbox processing for recurring patches

## Config Notes

- `table`: target DB table name
- `match_columns`: unique key columns used for upsert
- `column_map`: mapping rules for each target column
- `database_url_env`: env var containing DB URL (defaults to `DATABASE_URL`)
- `allow_issues`: if `false`, exits non-zero when rows are skipped
- `issues_output_jsonl`: optional path for skipped-row details

The example config in `portal_backend/scripts/db_updater/examples/` is ready for importing WordPress credentials by site URL.
