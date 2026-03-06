from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, validator


class Phase2CacheEnvelope(BaseModel):
    content_hash: str
    payload: Dict[str, Any] = Field(default_factory=dict)

    @validator("content_hash")
    def _trim_content_hash(cls, value: str) -> str:
        return (value or "").strip()

    @validator("payload", pre=True, always=True)
    def _validate_payload(cls, value: Any) -> Dict[str, Any]:
        return value if isinstance(value, dict) else {}


class CreatorRequest(BaseModel):
    target_site_url: HttpUrl
    publishing_site_url: HttpUrl = Field(..., description="Publishing site URL (required for analysis)")
    anchor: Optional[str] = None
    topic: Optional[str] = None
    exclude_topics: List[str] = Field(default_factory=list, description="Previously used topics to avoid duplicating")
    phase1_cache: Optional[Phase2CacheEnvelope] = None
    phase2_cache: Optional[Phase2CacheEnvelope] = None
    dry_run: bool = False

    @validator("anchor", "topic")
    def _trim_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None

    @validator("exclude_topics", pre=True, always=True)
    def _clean_exclude_topics(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]


class ErrorResponse(BaseModel):
    ok: bool = False
    error: str
    details: Optional[Dict[str, Any]] = None
    warnings: List[str] = Field(default_factory=list)
