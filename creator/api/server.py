from __future__ import annotations

import dataclasses
import logging
import os
from typing import Any, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .four_llm import draft_article, generate_meta, integrate_links, understand_target_site
from .four_llm_schemas import (
    DraftArticleRequest,
    IntegrateLinksRequest,
    MetaTagsRequest,
    SiteUnderstandingRequest,
)
from .llm import LLMError
from .models import ErrorResponse
from .pipeline_runner import PipelineError, PipelineRun, run_pipeline

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
        or os.getenv("ANTHROPIC_API_KEY")
        or os.getenv("OPENAI_API_KEY")
    )
    payload = {"ok": llm_ready, "llm_ready": llm_ready}
    return JSONResponse(status_code=200 if llm_ready else 503, content=payload)


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


# ---- v2: end-to-end pipeline ----------------------------------------------


class V2RunPipelineRequest(BaseModel):
    target_keyword: str = Field(..., min_length=2)
    target_backlink_url: str = Field(..., min_length=8)
    publishing_site_url: str = Field(..., min_length=4)
    anchor_hint: Optional[str] = None
    canonical_url: Optional[str] = None
    skip_voice_pass: bool = False
    skip_judge: bool = False
    skip_related_keywords: bool = False
    skip_entity_extraction: bool = False


def _to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    return value


def _serialize_run(run: PipelineRun) -> dict:
    return {
        "ok": True,
        "target_keyword": run.target_keyword,
        "target_backlink_url": run.target_backlink_url,
        "publishing_site_host": run.publishing_site_host,
        "research": _to_jsonable(run.research),
        "contract": run.contract.model_dump(mode="json"),
        "sections": [s.model_dump(mode="json") for s in run.sections],
        "article_html": {
            "assembled": run.assembled.full_html,
            "refined_body": run.refined_article_html,
            "final": run.final_html,
        },
        "judge_scores": _to_jsonable(run.judge_scores),
        "quality_report": run.quality_report.to_dict(),
        "skipped_voice_pass": run.skipped_voice_pass,
        "skipped_judge": run.skipped_judge,
        "notes": run.notes,
    }


@app.post("/v2/run-pipeline")
async def v2_run_pipeline(payload: V2RunPipelineRequest) -> JSONResponse:
    """End-to-end pipeline: research -> contract -> sections -> assemble -> voice -> judge -> eval.

    Returns the full PipelineRun serialized as JSON. Long-running (~30s end to
    end). Callers must use a long timeout (>=120s).
    """

    try:
        run = run_pipeline(
            target_keyword=payload.target_keyword,
            target_backlink_url=payload.target_backlink_url,
            publishing_site_url=payload.publishing_site_url,
            anchor_hint=payload.anchor_hint,
            canonical_url=payload.canonical_url,
            skip_voice_pass=payload.skip_voice_pass,
            skip_judge=payload.skip_judge,
            skip_related_keywords=payload.skip_related_keywords,
            skip_entity_extraction=payload.skip_entity_extraction,
        )
    except PipelineError as exc:
        logger.warning(
            "creator.v2_pipeline_failed phase=%s message=%s",
            exc.phase,
            str(exc),
        )
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": "pipeline_failed",
                "phase": exc.phase,
                "message": str(exc),
            },
        )

    return JSONResponse(status_code=200, content=_serialize_run(run))
