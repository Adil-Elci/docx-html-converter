from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field, HttpUrl


class KeywordMetric(BaseModel):
    keyword: str = Field(..., min_length=2)
    search_volume: int = Field(default=0, ge=0)
    keyword_difficulty: float = Field(default=0.0, ge=0.0)
    score: float = Field(default=0.0, ge=0.0)
    top_urls: List[HttpUrl] = Field(default_factory=list)


class CompetitorReference(BaseModel):
    url: HttpUrl
    title: str = ""
    h1: str = ""
    h2s: List[str] = Field(default_factory=list)
    h3s: List[str] = Field(default_factory=list)
    word_count: int = Field(default=0, ge=0)
    content_format: str = ""
    key_topics: List[str] = Field(default_factory=list)


class LinkCandidate(BaseModel):
    url: HttpUrl
    title: str = ""
    relevance_score: float = Field(default=0.0, ge=0.0, le=1.0)
    link_type: Literal["internal", "external", "target_backlink"]
    target_kind: Literal["owned_network", "target_site"]
    excerpt: str = ""
    site_id: Optional[str] = None


class ContentBrief(BaseModel):
    target_keyword: str = Field(..., min_length=2)
    secondary_keywords: List[str] = Field(default_factory=list)
    search_intent: Literal["informational", "commercial", "transactional", "navigational"]
    recommended_format: Literal["listicle", "guide", "comparison", "how-to"]
    recommended_word_count: int = Field(..., ge=600, le=4000)
    tone: str = Field(..., min_length=2)
    target_audience: str = Field(..., min_length=2)
    suggested_title: str = Field(..., min_length=8)
    outline: List[str] = Field(..., min_length=4, max_length=8)
    key_topics_to_cover: List[str] = Field(default_factory=list)
    internal_link_candidates: List[LinkCandidate] = Field(default_factory=list)
    external_link_candidates: List[LinkCandidate] = Field(default_factory=list)
    competitor_references: List[CompetitorReference] = Field(default_factory=list)
    target_site_url: HttpUrl
    publishing_site_url: HttpUrl
    publishing_site_id: Optional[str] = None
    target_site_language: str = "de"
    seed_keywords: List[str] = Field(default_factory=list)
    chosen_topic: str = Field(..., min_length=3)
    notes: List[str] = Field(default_factory=list)


class QualityCheckResult(BaseModel):
    name: str
    passed: bool
    severity: Literal["critical", "warning", "info"] = "info"
    details: Dict[str, object] = Field(default_factory=dict)


class QualityReport(BaseModel):
    passed: bool
    blockers: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    checks: List[QualityCheckResult] = Field(default_factory=list)

