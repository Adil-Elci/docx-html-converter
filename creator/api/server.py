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

from .models import ErrorResponse
from .pipeline_runner import PipelineError, PipelineRun, run_pipeline
from .publisher_fit import (
    FitVerdict,
    PublisherFitError,
    validate_or_refine_topic_for_publisher,
)
from .topic_brainstorm import (
    BrainstormResult,
    EditorialAngle,
    TopicBrainstormError,
    brainstorm_editorial_angles,
)
from .topic_derivation import (
    DEFAULT_ALLOWED_LANGUAGES,
    DerivedTopic,
    TopicDerivationError,
    derive_topic,
)

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


# ---- v2: validate/refine topic for a publishing site ---------------------


class V2RefineTopicForPublisherRequest(BaseModel):
    target_keyword: str = Field(..., min_length=2)
    publishing_profile_payload: Optional[dict] = None
    language: str = "de"
    min_confidence: float = 0.4


def _serialize_fit_verdict(v: FitVerdict) -> dict:
    return {
        "ok": True,
        "refined_keyword": v.refined_keyword,
        "original_keyword": v.original_keyword,
        "changed": v.changed,
        "confidence": v.confidence,
        "rationale": v.rationale,
        "cost_usd": v.cost_usd,
    }


@app.post("/v2/refine-topic-for-publisher")
async def v2_refine_topic_for_publisher(payload: V2RefineTopicForPublisherRequest) -> JSONResponse:
    """Validate that a target keyword has an editorial intersection with the
    publishing site's audience; refine it if needed; hard-fail when no fit.

    Used by portal_backend before the contract step on the v2 path so we
    don't waste the contract budget on a topic the publisher would reject.
    Returns ``422`` with stable codes ``no_editorial_fit`` /
    ``fit_below_threshold`` when the topic and publisher are a poor match.
    """

    try:
        verdict = validate_or_refine_topic_for_publisher(
            target_keyword=payload.target_keyword,
            publishing_profile_payload=payload.publishing_profile_payload,
            language=payload.language,
            min_confidence=payload.min_confidence,
        )
    except PublisherFitError as exc:
        logger.warning(
            "creator.refine_topic_failed code=%s message=%s",
            exc.code,
            str(exc),
        )
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": "publisher_fit_failed",
                "code": exc.code,
                "message": str(exc),
            },
        )

    return JSONResponse(status_code=200, content=_serialize_fit_verdict(verdict))


# ---- v2: brainstorm editorial angles --------------------------------------


class V2BrainstormTopicsRequest(BaseModel):
    target_url: str = Field(..., min_length=4)
    target_keyword: str = Field(..., min_length=2)
    publishing_profile_payload: Optional[dict] = None
    language: str = "de"
    current_year: Optional[int] = None
    num_angles: int = Field(default=5, ge=1, le=8)


def _serialize_brainstorm(result: BrainstormResult) -> dict:
    return {
        "ok": True,
        "angles": [
            {
                "title": a.title,
                "target_keyword": a.target_keyword,
                "hook": a.hook,
                "rationale": a.rationale,
            }
            for a in result.angles
        ],
        "cost_usd": result.cost_usd,
    }


@app.post("/v2/brainstorm-topics")
async def v2_brainstorm_topics(payload: V2BrainstormTopicsRequest) -> JSONResponse:
    """Generate editorial topic angles for a (target site, publisher) pair.

    Used by portal_backend after the topic has been derived/refined so the
    contract step can build an article around an EDITORIAL ANGLE rather
    than a SEO-formula buying-guide. Returns up to ``num_angles`` angles
    ordered by the LLM's preference (caller takes the first as auto-pick).
    """

    try:
        result = brainstorm_editorial_angles(
            target_url=payload.target_url,
            target_keyword=payload.target_keyword,
            publishing_profile_payload=payload.publishing_profile_payload,
            language=payload.language,
            current_year=payload.current_year,
            num_angles=payload.num_angles,
        )
    except TopicBrainstormError as exc:
        logger.warning("creator.brainstorm_failed code=%s message=%s", exc.code, str(exc))
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": "topic_brainstorm_failed",
                "code": exc.code,
                "message": str(exc),
            },
        )

    return JSONResponse(status_code=200, content=_serialize_brainstorm(result))


# ---- v2: end-to-end pipeline ----------------------------------------------


class V2RunPipelineRequest(BaseModel):
    target_backlink_url: str = Field(..., min_length=8)
    target_keyword: Optional[str] = Field(default=None, min_length=2)
    publishing_site_url: Optional[str] = Field(default=None, min_length=4)
    language: Optional[str] = None  # ISO 639-1; auto-detected when absent
    editorial_angle: Optional[dict] = None  # brainstormed slant: {title, hook, rationale}
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


def _serialize_derived_topic(derived) -> Optional[dict]:
    if derived is None:
        return None
    return {
        "target_url": derived.target_url,
        "target_keyword": derived.target_keyword,
        "language_code": derived.language_code,
        "location_code": derived.location_code,
        "alternates": list(derived.alternates),
        "candidates": [
            {
                "keyword": c.keyword,
                "source": c.source,
                "search_volume": c.search_volume,
                "trend_ratio": c.trend_ratio,
                "score": c.score,
            }
            for c in derived.candidates
        ],
        "confidence": derived.confidence,
        "notes": list(derived.notes),
        "cost_usd": derived.cost_usd,
        "cache_hit": derived.cache_hit,
    }


def _serialize_run(run: PipelineRun) -> dict:
    return {
        "ok": True,
        "target_keyword": run.target_keyword,
        "target_backlink_url": run.target_backlink_url,
        "publishing_site_host": run.publishing_site_host,
        "language": run.language,
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
        "derived_topic": _serialize_derived_topic(run.derived_topic),
        "skipped_voice_pass": run.skipped_voice_pass,
        "skipped_judge": run.skipped_judge,
        "notes": run.notes,
    }


# ---- v2: derive topic from target URL -------------------------------------


class V2DeriveTopicRequest(BaseModel):
    target_url: str = Field(..., min_length=4)
    allowed_languages: Optional[list[str]] = None  # default: ("de", "fr")
    language_override: Optional[str] = None
    use_cache: bool = True


def _serialize_derived(result: DerivedTopic) -> dict:
    return {
        "ok": True,
        "target_url": result.target_url,
        "target_keyword": result.target_keyword,
        "language_code": result.language_code,
        "location_code": result.location_code,
        "alternates": list(result.alternates),
        "candidates": [
            {
                "keyword": c.keyword,
                "source": c.source,
                "search_volume": c.search_volume,
                "trend_ratio": c.trend_ratio,
                "score": c.score,
            }
            for c in result.candidates
        ],
        "confidence": result.confidence,
        "notes": list(result.notes),
        "cost_usd": result.cost_usd,
        "cache_hit": result.cache_hit,
    }


@app.post("/v2/derive-topic")
async def v2_derive_topic(payload: V2DeriveTopicRequest) -> JSONResponse:
    """Derive ``target_keyword`` + locale from a backlink target URL.

    Returns 422 with ``error: "topic_derivation_failed"`` and a stable
    ``code`` (``url_missing`` / ``fetch_failed`` / ``language_not_allowed`` /
    ``no_candidates``) when derivation cannot produce a usable result. Portal
    backend renders ``message`` verbatim in the admin error UI.
    """

    allowed_languages = tuple(payload.allowed_languages) if payload.allowed_languages else DEFAULT_ALLOWED_LANGUAGES

    try:
        result = derive_topic(
            payload.target_url,
            allowed_languages=allowed_languages,
            language_override=payload.language_override,
            use_cache=payload.use_cache,
        )
    except TopicDerivationError as exc:
        logger.warning(
            "creator.derive_topic_failed code=%s message=%s",
            exc.code,
            str(exc),
        )
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": "topic_derivation_failed",
                "code": exc.code,
                "message": str(exc),
            },
        )

    return JSONResponse(status_code=200, content=_serialize_derived(result))


@app.post("/v2/run-pipeline")
async def v2_run_pipeline(payload: V2RunPipelineRequest) -> JSONResponse:
    """End-to-end pipeline: research -> contract -> sections -> assemble -> voice -> judge -> eval.

    Returns the full PipelineRun serialized as JSON. Long-running (~30s end to
    end). Callers must use a long timeout (>=120s).
    """

    try:
        run = run_pipeline(
            target_backlink_url=payload.target_backlink_url,
            target_keyword=payload.target_keyword,
            publishing_site_url=payload.publishing_site_url,
            language=payload.language,
            editorial_angle=payload.editorial_angle,
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
