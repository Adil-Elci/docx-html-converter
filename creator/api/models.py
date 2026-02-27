from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, validator


class CreatorRequest(BaseModel):
    target_site_url: HttpUrl
    publishing_site_url: HttpUrl = Field(..., description="Publishing site URL (required for analysis)")
    anchor: Optional[str] = None
    topic: Optional[str] = None
    exclude_topics: List[str] = Field(default_factory=list, description="Previously used topics to avoid duplicating")
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
