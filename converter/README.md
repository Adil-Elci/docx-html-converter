# Converter Service

## Run locally
```bash
pip install -r requirements.txt
uvicorn api.server:app --reload --port 8000
```

## Database usage
- The converter may read/write tables using `DATABASE_URL`.
- `DATABASE_URL` must point to the live/production database (never localhost/loopback).
- The converter must not run migrations.
- Alembic migrations are owned by `portal_backend` only.

## Tests
```bash
pytest
```
