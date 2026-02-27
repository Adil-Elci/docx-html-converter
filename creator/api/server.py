from __future__ import annotations

import json
import logging
import os
import queue
import threading
from typing import Dict

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

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
async def health() -> JSONResponse:
    llm_ready = bool(
        os.getenv("CREATOR_LLM_API_KEY")
        or os.getenv("OPENAI_API_KEY")
        or os.getenv("ANTHROPIC_API_KEY")
    )
    image_ready = bool(os.getenv("LEONARDO_API_KEY"))
    ok = llm_ready and image_ready
    payload = {"ok": ok, "llm_ready": llm_ready, "image_ready": image_ready}
    return JSONResponse(status_code=200 if ok else 503, content=payload)


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
            exclude_topics=payload.exclude_topics,
            dry_run=payload.dry_run,
        )
    except (CreatorError, LLMError) as exc:
        logger.warning("creator.pipeline_failed error=%s", str(exc))
        response = ErrorResponse(error="pipeline_failed", details={"message": str(exc)})
        return JSONResponse(status_code=422, content=response.dict())

    return JSONResponse(status_code=200, content=result)


@app.post("/create-stream")
async def create_stream(request: Request) -> EventSourceResponse:
    """SSE endpoint that streams phase progress events, then the final result."""
    try:
        data = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON body.") from exc

    payload = CreatorRequest(**data)

    progress_queue: queue.Queue = queue.Queue()

    def on_progress(phase: int, label: str, percent: int) -> None:
        progress_queue.put({"event": "progress", "phase": phase, "label": label, "percent": percent})

    def run_pipeline() -> None:
        try:
            result = run_creator_pipeline(
                target_site_url=str(payload.target_site_url),
                publishing_site_url=str(payload.publishing_site_url),
                anchor=payload.anchor,
                topic=payload.topic,
                exclude_topics=payload.exclude_topics,
                dry_run=payload.dry_run,
                on_progress=on_progress,
            )
            progress_queue.put({"event": "complete", "data": result})
        except (CreatorError, LLMError) as exc:
            logger.warning("creator.pipeline_failed error=%s", str(exc))
            progress_queue.put({"event": "error", "error": str(exc)})
        except Exception as exc:
            logger.exception("creator.stream.unhandled_error")
            progress_queue.put({"event": "error", "error": "internal_error"})

    thread = threading.Thread(target=run_pipeline, daemon=True)
    thread.start()

    async def event_generator():
        while True:
            try:
                msg = progress_queue.get(timeout=300)
            except queue.Empty:
                break
            event_type = msg.pop("event", "message")
            yield {"event": event_type, "data": json.dumps(msg)}
            if event_type in ("complete", "error"):
                break

    return EventSourceResponse(event_generator())
