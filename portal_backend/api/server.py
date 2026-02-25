from __future__ import annotations

import os
import logging

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .automation_worker import AutomationJobWorker
from .db import get_sessionmaker
from .migration_guard import should_verify_db_head_on_startup, verify_db_is_at_head
from .routers import (
    admin_users_router,
    auth_router,
    automation_router,
    client_site_access_router,
    clients_router,
    db_updater_router,
    jobs_router,
    site_credentials_router,
    sites_router,
    submissions_router,
)

load_dotenv()

app = FastAPI(title="Client Portal API")
_automation_worker: AutomationJobWorker | None = None

cors_origins = [origin.strip() for origin in os.getenv("CORS_ORIGINS", "").split(",") if origin.strip()]
if cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(clients_router)
app.include_router(auth_router)
app.include_router(admin_users_router)
app.include_router(db_updater_router)
app.include_router(automation_router)
app.include_router(sites_router)
app.include_router(site_credentials_router)
app.include_router(client_site_access_router)
app.include_router(submissions_router)
app.include_router(jobs_router)


def _configure_automation_logger() -> None:
    level_name = os.getenv("AUTOMATION_LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.getLogger("portal_backend.automation").setLevel(level)


@app.on_event("startup")
def verify_schema_state_on_startup() -> None:
    global _automation_worker
    _configure_automation_logger()
    if should_verify_db_head_on_startup():
        verify_db_is_at_head()
    worker_enabled = os.getenv("AUTOMATION_WORKER_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    if worker_enabled:
        _automation_worker = AutomationJobWorker(get_sessionmaker())
        _automation_worker.start()


@app.on_event("shutdown")
def stop_automation_worker() -> None:
    global _automation_worker
    if _automation_worker is not None:
        _automation_worker.stop()
        _automation_worker = None


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"ok": False, "error": str(exc.detail)})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    sanitized_errors = []
    for item in exc.errors():
        if not isinstance(item, dict):
            sanitized_errors.append(str(item))
            continue
        safe_item = dict(item)
        raw_input = safe_item.get("input")
        if isinstance(raw_input, (bytes, bytearray)):
            safe_item["input"] = raw_input.decode("utf-8", errors="replace")
        sanitized_errors.append(jsonable_encoder(safe_item))
    return JSONResponse(status_code=422, content={"ok": False, "error": "validation_error", "details": sanitized_errors})


@app.get("/health")
async def health() -> dict:
    return {"ok": True}
