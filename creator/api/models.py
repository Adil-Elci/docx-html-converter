from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, validator


class CreatorRequest(BaseModel):
    target_site_url: HttpUrl
    publishing_site_url: HttpUrl = Field(..., description="Publishing site URL (required for analysis)")
    anchor: Optional[str] = None
    topic: Optional[str] = None
    dry_run: bool = False

    @validator("anchor", "topic")
    def _trim_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None


class ErrorResponse(BaseModel):
    ok: bool = False
    error: str
    details: Optional[Dict[str, Any]] = None
    warnings: List[str] = Field(default_factory=list)
