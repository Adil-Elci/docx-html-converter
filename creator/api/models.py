from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator


class Phase2CacheEnvelope(BaseModel):
    content_hash: str
    payload: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("content_hash")
    @classmethod
    def _trim_content_hash(cls, value: str) -> str:
        return (value or "").strip()

    @field_validator("payload", mode="before")
    @classmethod
    def _validate_payload(cls, value: Any) -> Dict[str, Any]:
        return value if isinstance(value, dict) else {}


class SiteProfileEnvelope(BaseModel):
    content_hash: str
    payload: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("content_hash")
    @classmethod
    def _trim_content_hash(cls, value: str) -> str:
        return (value or "").strip()

    @field_validator("payload", mode="before")
    @classmethod
    def _validate_payload(cls, value: Any) -> Dict[str, Any]:
        return value if isinstance(value, dict) else {}


class InternalLinkInventoryItem(BaseModel):
    url: HttpUrl
    title: str = ""
    excerpt: str = ""
    slug: str = ""
    categories: List[str] = Field(default_factory=list)
    published_at: str = ""

    @field_validator("title", "excerpt", "slug", "published_at", mode="before")
    @classmethod
    def _trim_text(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("categories", mode="before")
    @classmethod
    def _clean_categories(cls, value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]


class PublishingCandidateRequest(BaseModel):
    site_url: HttpUrl
    site_id: Optional[str] = None
    fit_score: Optional[float] = None
    notes: List[str] = Field(default_factory=list)
    internal_link_inventory: List[InternalLinkInventoryItem] = Field(default_factory=list)
    publishing_profile: SiteProfileEnvelope

    @field_validator("site_id")
    @classmethod
    def _trim_site_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None

    @field_validator("notes", mode="before")
    @classmethod
    def _clean_notes(cls, value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]


class CreatorRequest(BaseModel):
    target_site_url: HttpUrl
    publishing_site_url: Optional[HttpUrl] = Field(default=None, description="Publishing site URL")
    publishing_site_id: Optional[str] = None
    publishing_candidates: List[PublishingCandidateRequest] = Field(default_factory=list)
    client_target_site_id: Optional[str] = None
    anchor: Optional[str] = None
    topic: Optional[str] = None
    exclude_topics: List[str] = Field(default_factory=list, description="Previously used topics to avoid duplicating")
    recent_article_titles: List[str] = Field(default_factory=list, description="Recent titles for novelty checks")
    internal_link_inventory: List[InternalLinkInventoryItem] = Field(default_factory=list)
    phase1_cache: Optional[Phase2CacheEnvelope] = None
    phase2_cache: Optional[Phase2CacheEnvelope] = None
    target_profile: Optional[SiteProfileEnvelope] = None
    publishing_profile: Optional[SiteProfileEnvelope] = None
    dry_run: bool = False

    @field_validator("anchor", "topic", "publishing_site_id", "client_target_site_id")
    @classmethod
    def _trim_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None

    @field_validator("exclude_topics", mode="before")
    @classmethod
    def _clean_exclude_topics(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    @field_validator("recent_article_titles", mode="before")
    @classmethod
    def _clean_recent_article_titles(cls, value: Any) -> List[str]:
        if value is None or not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]


class ErrorResponse(BaseModel):
    ok: bool = False
    error: str
    details: Optional[Dict[str, Any]] = None
    warnings: List[str] = Field(default_factory=list)


class PairFitRequest(BaseModel):
    target_site_url: HttpUrl
    publishing_site_url: HttpUrl
    publishing_site_id: Optional[str] = None
    client_target_site_id: Optional[str] = None
    requested_topic: Optional[str] = None
    exclude_topics: List[str] = Field(default_factory=list)
    target_profile: SiteProfileEnvelope
    publishing_profile: SiteProfileEnvelope

    @field_validator("publishing_site_id", "client_target_site_id", "requested_topic")
    @classmethod
    def _trim_pair_fit_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        cleaned = value.strip()
        return cleaned or None

    @field_validator("exclude_topics", mode="before")
    @classmethod
    def _clean_pair_fit_exclude_topics(cls, value: Any) -> List[str]:
        if value is None or not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]
