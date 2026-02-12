PSQL ?= psql
DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/doc_converter_service

.PHONY: db-init db-migrate db-seed

db-init:
	$(PSQL) "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f init.sql

db-migrate:
	@set -e; \
	for file in $$(ls -1 migrations/*.sql | sort); do \
		echo "Applying $$file"; \
		$(PSQL) "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f $$file; \
	done

db-seed:
	$(PSQL) "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f migrations/006_seed_dev.sql
