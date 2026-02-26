from __future__ import annotations

import logging
import os
from typing import Dict

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .llm import LLMError
from .models import CreatorRequest, ErrorResponse
from .pipeline import CreatorError, run_creator_pipeline

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("creator")

app = FastAPI(title="Creator Service")

cors_origins = [origin.strip() for origin in os.getenv("CORS_ORIGINS", "").split(",") if origin.strip()]
if cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content=ErrorResponse(error="validation_error", details={"errors": exc.errors()}).dict(),
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
    payload = ErrorResponse(error=str(exc.detail))
    return JSONResponse(status_code=exc.status_code, content=payload.dict())


@app.exception_handler(Exception)
async def unhandled_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("creator.unhandled_error")
    payload = ErrorResponse(error="internal_error")
    return JSONResponse(status_code=500, content=payload.dict())


@app.get("/health")
async def health() -> Dict[str, bool]:
    return {"ok": True}


@app.post("/create")
async def create(request: Request) -> JSONResponse:
    try:
        data = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON body.") from exc

    payload = CreatorRequest(**data)

    try:
        result = run_creator_pipeline(
            target_site_url=str(payload.target_site_url),
            publishing_site_url=str(payload.publishing_site_url),
            anchor=payload.anchor,
            topic=payload.topic,
            dry_run=payload.dry_run,
        )
    except (CreatorError, LLMError) as exc:
        logger.warning("creator.pipeline_failed error=%s", str(exc))
        response = ErrorResponse(error="pipeline_failed", details={"message": str(exc)})
        return JSONResponse(status_code=422, content=response.dict())

    return JSONResponse(status_code=200, content=result)
