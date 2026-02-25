# Import Utility

`import_tabular_to_db.py` imports CSV / XLSX / Google Sheets data into a database table using a JSON config.

## Usage

```bash
python3 "scripts/db updater/import_tabular_to_db.py" --config "scripts/db updater/examples/publishing_site_credentials_by_url.example.json" --dry-run
python3 "scripts/db updater/import_tabular_to_db.py" --config "scripts/db updater/examples/publishing_site_credentials_by_url.example.json"
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

## Config Notes

- `table`: target DB table name
- `match_columns`: unique key columns used for upsert
- `column_map`: mapping rules for each target column
- `database_url_env`: env var containing DB URL (defaults to `DATABASE_URL`)
- `allow_issues`: if `false`, exits non-zero when rows are skipped
- `issues_output_jsonl`: optional path for skipped-row details

The example config in `scripts/db updater/examples/` is ready for importing WordPress credentials by site URL.
