#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is required" >&2
  exit 1
fi

python - <<'PY'
import os
import time
import psycopg2

database_url = os.environ["DATABASE_URL"]
max_wait_seconds = int(os.getenv("DB_WAIT_TIMEOUT_SECONDS", "60"))
deadline = time.time() + max_wait_seconds

while True:
    try:
        conn = psycopg2.connect(database_url)
        conn.close()
        print("Postgres is reachable")
        break
    except Exception as exc:
        if time.time() >= deadline:
            raise RuntimeError(f"Postgres did not become reachable in {max_wait_seconds}s") from exc
        time.sleep(2)
PY

python - <<'PY'
import os
import subprocess
import psycopg2

lock_key = int(os.getenv("MIGRATION_LOCK_KEY", "823746192345678901"))
database_url = os.environ["DATABASE_URL"]

conn = psycopg2.connect(database_url)
conn.autocommit = True

try:
    with conn.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_lock(%s)", (lock_key,))
    try:
        subprocess.run(["alembic", "upgrade", "head"], check=True)
    finally:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
finally:
    conn.close()
PY

exec uvicorn api.server:app --host 0.0.0.0 --port 8000
