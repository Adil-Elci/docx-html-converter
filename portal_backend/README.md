# Portal Backend

## Run locally
```bash
pip install -r requirements.txt
uvicorn api.server:app --reload --port 8001
```

## Env
Create `portal_backend/.env` with:
```env
DATABASE_URL=postgresql://user:password@host:5432/dbname
JWT_SECRET=change-me
CORS_ORIGINS=http://localhost:5173
COOKIE_SECURE=false
```
