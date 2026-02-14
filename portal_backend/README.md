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
