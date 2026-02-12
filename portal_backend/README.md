# Portal Backend

## Local Dev
```bash
cd portal_backend
pip install -r requirements.txt
export DATABASE_URL="postgresql://user:password@host:5432/dbname"
alembic upgrade head
uvicorn api.server:app --reload --host 0.0.0.0 --port 8000
```

## Docker / Dokploy
- Container startup is handled by `entrypoint.sh`.
- Startup flow: wait for Postgres, acquire advisory lock, run `alembic upgrade head`, release lock, start API.
- Docker image uses `ENTRYPOINT ["/app/entrypoint.sh"]`.
- For Dokploy, use the Dockerfile and do not override the run command.
