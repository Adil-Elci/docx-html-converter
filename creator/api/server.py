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

from .four_llm import draft_article, generate_meta, integrate_links, understand_target_site
from .four_llm_schemas import (
    DraftArticleRequest,
    IntegrateLinksRequest,
    MetaTagsRequest,
    SiteUnderstandingRequest,
)
from .llm import LLMError
from .models import CreatorRequest, ErrorResponse, PairFitRequest
from .pipeline import CreatorError, run_creator_pipeline, run_pair_fit_pipeline

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
    pipeline_mode = os.getenv("CREATOR_PIPELINE_MODE", "legacy").strip().lower()
    llm_ready = bool(
        os.getenv("CREATOR_LLM_API_KEY")
        or os.getenv("OPENAI_API_KEY")
        or os.getenv("ANTHROPIC_API_KEY")
    )
    image_ready = bool(os.getenv("LEONARDO_API_KEY"))
    ok = llm_ready if pipeline_mode == "4llm" else (llm_ready and image_ready)
    payload = {"ok": ok, "llm_ready": llm_ready, "image_ready": image_ready, "pipeline_mode": pipeline_mode}
    return JSONResponse(status_code=200 if ok else 503, content=payload)


@app.post("/site-understanding")
async def site_understanding(payload: SiteUnderstandingRequest) -> JSONResponse:
    try:
        result = understand_target_site(payload)
    except LLMError as exc:
        logger.warning("creator.site_understanding_failed error=%s", str(exc))
        response = ErrorResponse(error="site_understanding_failed", details={"message": str(exc)})
        return JSONResponse(status_code=422, content=response.dict())
    return JSONResponse(status_code=200, content=result.model_dump())


@app.post("/draft-article")
async def draft_article_endpoint(payload: DraftArticleRequest) -> JSONResponse:
    try:
        result = draft_article(payload)
    except LLMError as exc:
        logger.warning("creator.draft_article_failed error=%s", str(exc))
        response = ErrorResponse(error="draft_article_failed", details={"message": str(exc)})
        return JSONResponse(status_code=422, content=response.dict())
    return JSONResponse(status_code=200, content=result.model_dump())


@app.post("/integrate-links")
async def integrate_links_endpoint(payload: IntegrateLinksRequest) -> JSONResponse:
    try:
        result = integrate_links(payload)
    except LLMError as exc:
        logger.warning("creator.integrate_links_failed error=%s", str(exc))
        response = ErrorResponse(error="integrate_links_failed", details={"message": str(exc)})
        return JSONResponse(status_code=422, content=response.dict())
    return JSONResponse(status_code=200, content=result.model_dump())


@app.post("/generate-meta")
async def generate_meta_endpoint(payload: MetaTagsRequest) -> JSONResponse:
    try:
        result = generate_meta(payload)
    except LLMError as exc:
        logger.warning("creator.generate_meta_failed error=%s", str(exc))
        response = ErrorResponse(error="generate_meta_failed", details={"message": str(exc)})
        return JSONResponse(status_code=422, content=response.dict())
    return JSONResponse(status_code=200, content=result.model_dump())


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
            publishing_site_url=str(payload.publishing_site_url) if payload.publishing_site_url else "",
            publishing_site_id=payload.publishing_site_id,
            publishing_candidates=[
                {
                    "site_url": str(candidate.site_url),
                    "site_id": candidate.site_id,
                    "fit_score": candidate.fit_score,
                    "notes": list(candidate.notes),
                    "internal_link_inventory": [item.dict() for item in candidate.internal_link_inventory],
                    "publishing_profile_payload": candidate.publishing_profile.payload,
                    "publishing_profile_content_hash": candidate.publishing_profile.content_hash,
                }
                for candidate in payload.publishing_candidates
            ],
            client_target_site_id=payload.client_target_site_id,
            anchor=payload.anchor,
            topic=payload.topic,
            exclude_topics=payload.exclude_topics,
            recent_article_titles=payload.recent_article_titles,
            internal_link_inventory=[item.dict() for item in payload.internal_link_inventory],
            phase1_cache_payload=payload.phase1_cache.payload if payload.phase1_cache else None,
            phase1_cache_content_hash=payload.phase1_cache.content_hash if payload.phase1_cache else None,
            phase2_cache_payload=payload.phase2_cache.payload if payload.phase2_cache else None,
            phase2_cache_content_hash=payload.phase2_cache.content_hash if payload.phase2_cache else None,
            target_profile_payload=payload.target_profile.payload if payload.target_profile else None,
            target_profile_content_hash=payload.target_profile.content_hash if payload.target_profile else None,
            publishing_profile_payload=payload.publishing_profile.payload if payload.publishing_profile else None,
            publishing_profile_content_hash=payload.publishing_profile.content_hash if payload.publishing_profile else None,
            dry_run=payload.dry_run,
        )
    except (CreatorError, LLMError) as exc:
        logger.warning("creator.pipeline_failed error=%s", str(exc))
        error_details = {"message": str(exc)}
        if isinstance(exc, CreatorError) and exc.details:
            error_details["details"] = exc.details
        response = ErrorResponse(error="pipeline_failed", details=error_details)
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
                publishing_site_url=str(payload.publishing_site_url) if payload.publishing_site_url else "",
                publishing_site_id=payload.publishing_site_id,
                publishing_candidates=[
                    {
                        "site_url": str(candidate.site_url),
                        "site_id": candidate.site_id,
                        "fit_score": candidate.fit_score,
                        "notes": list(candidate.notes),
                        "internal_link_inventory": [item.dict() for item in candidate.internal_link_inventory],
                        "publishing_profile_payload": candidate.publishing_profile.payload,
                        "publishing_profile_content_hash": candidate.publishing_profile.content_hash,
                    }
                    for candidate in payload.publishing_candidates
                ],
                client_target_site_id=payload.client_target_site_id,
                anchor=payload.anchor,
                topic=payload.topic,
                exclude_topics=payload.exclude_topics,
                recent_article_titles=payload.recent_article_titles,
                internal_link_inventory=[item.dict() for item in payload.internal_link_inventory],
                phase1_cache_payload=payload.phase1_cache.payload if payload.phase1_cache else None,
                phase1_cache_content_hash=payload.phase1_cache.content_hash if payload.phase1_cache else None,
                phase2_cache_payload=payload.phase2_cache.payload if payload.phase2_cache else None,
                phase2_cache_content_hash=payload.phase2_cache.content_hash if payload.phase2_cache else None,
                target_profile_payload=payload.target_profile.payload if payload.target_profile else None,
                target_profile_content_hash=payload.target_profile.content_hash if payload.target_profile else None,
                publishing_profile_payload=payload.publishing_profile.payload if payload.publishing_profile else None,
                publishing_profile_content_hash=payload.publishing_profile.content_hash if payload.publishing_profile else None,
                dry_run=payload.dry_run,
                on_progress=on_progress,
            )
            progress_queue.put({"event": "complete", "data": result})
        except (CreatorError, LLMError) as exc:
            logger.warning("creator.pipeline_failed error=%s", str(exc))
            error_payload = {"error": str(exc)}
            if isinstance(exc, CreatorError) and exc.details:
                error_payload["details"] = exc.details
            progress_queue.put({"event": "error", **error_payload})
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


@app.post("/pair-fit")
async def pair_fit(request: Request) -> JSONResponse:
    try:
        data = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON body.") from exc

    payload = PairFitRequest(**data)
    try:
        result = run_pair_fit_pipeline(
            target_site_url=str(payload.target_site_url),
            publishing_site_url=str(payload.publishing_site_url),
            publishing_site_id=payload.publishing_site_id,
            client_target_site_id=payload.client_target_site_id,
            requested_topic=payload.requested_topic,
            exclude_topics=payload.exclude_topics,
            target_profile_payload=payload.target_profile.payload,
            target_profile_content_hash=payload.target_profile.content_hash,
            publishing_profile_payload=payload.publishing_profile.payload,
            publishing_profile_content_hash=payload.publishing_profile.content_hash,
        )
    except (CreatorError, LLMError) as exc:
        logger.warning("creator.pair_fit_failed error=%s", str(exc))
        response = ErrorResponse(error="pair_fit_failed", details={"message": str(exc)})
        return JSONResponse(status_code=422, content=response.dict())
    return JSONResponse(status_code=200, content=result)
