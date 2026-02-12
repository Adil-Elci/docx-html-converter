# Postgres Schema Setup

Set `DATABASE_URL` first:

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/doc_converter_service"
```

Initialize a fresh database from the full schema:

```bash
make db-init
```

Run ordered migrations (safe to run after `db-init` because files are idempotent):

```bash
make db-migrate
```

Run only local dev seed data:

```bash
make db-seed
```
