# Database (Postgres)

This folder contains SQL migrations for the portal database.

## Run migration
```bash
psql "$DATABASE_URL" -f db/migrations/001_init.sql
```
