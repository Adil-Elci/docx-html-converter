from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator


class ScrapedPage(BaseModel):
    url: HttpUrl
    title: str = ""
    meta_title: str = ""
    meta_description: str = ""
    h1: str = ""
    h2s: List[str] = Field(default_factory=list)
    text_excerpt: str = ""

    @field_validator("title", "meta_title", "meta_description", "h1", "text_excerpt", mode="before")
    @classmethod
    def _trim_text(cls, value: object) -> str:
        return str(value or "").strip()


class TargetSiteUnderstanding(BaseModel):
    primary_niche: str = Field(..., min_length=2)
    main_topic: str = Field(..., min_length=2)
    target_audience: str = Field(..., min_length=2)
    seed_keywords: List[str] = Field(default_factory=list, min_length=3, max_length=10)
    content_tone: str = Field(..., min_length=2)
    site_type: str = Field(..., min_length=2)
    language: str = Field(..., min_length=2, max_length=12)
    scraped_pages: List[ScrapedPage] = Field(default_factory=list)


class SiteUnderstandingRequest(BaseModel):
    target_site_url: HttpUrl
    max_pages: int = Field(default=10, ge=1, le=20)


class BriefLinkCandidate(BaseModel):
    url: HttpUrl
    title: str = ""
    relevance_score: float = Field(default=0.0, ge=0.0, le=1.0)
    link_type: Literal["internal", "external", "target_backlink"]
    target_kind: Literal["owned_network", "target_site"]
    excerpt: str = ""


class CompetitorReferenceInput(BaseModel):
    url: HttpUrl
    title: str = ""
    h1: str = ""
    h2s: List[str] = Field(default_factory=list)
    h3s: List[str] = Field(default_factory=list)
    word_count: int = Field(default=0, ge=0)
    content_format: str = ""
    key_topics: List[str] = Field(default_factory=list)


class ContentBriefInput(BaseModel):
    target_keyword: str = Field(..., min_length=2)
    secondary_keywords: List[str] = Field(default_factory=list)
    search_intent: str = Field(..., min_length=2)
    recommended_format: str = Field(..., min_length=2)
    recommended_word_count: int = Field(..., ge=600, le=4000)
    tone: str = Field(..., min_length=2)
    target_audience: str = Field(..., min_length=2)
    suggested_title: str = Field(..., min_length=8)
    outline: List[str] = Field(..., min_length=4, max_length=8)
    key_topics_to_cover: List[str] = Field(default_factory=list)
    internal_link_candidates: List[BriefLinkCandidate] = Field(default_factory=list)
    external_link_candidates: List[BriefLinkCandidate] = Field(default_factory=list)
    competitor_references: List[CompetitorReferenceInput] = Field(default_factory=list)
    target_site_url: HttpUrl
    publishing_site_url: HttpUrl
    publishing_site_id: Optional[str] = None
    target_site_language: str = "de"
    seed_keywords: List[str] = Field(default_factory=list)
    chosen_topic: str = Field(..., min_length=3)
    notes: List[str] = Field(default_factory=list)


class DraftArticleRequest(BaseModel):
    content_brief: ContentBriefInput


class ArticleDraftWithPlaceholders(BaseModel):
    markdown: str = Field(..., min_length=300)


class IntegrateLinksRequest(BaseModel):
    article_markdown: str = Field(..., min_length=300)
    internal_links: List[BriefLinkCandidate] = Field(default_factory=list)
    external_links: List[BriefLinkCandidate] = Field(default_factory=list)


class LinkedArticleDraft(BaseModel):
    markdown: str = Field(..., min_length=300)
    placed_links: List[Dict[str, str]] = Field(default_factory=list)


class MetaTagsRequest(BaseModel):
    target_keyword: str = Field(..., min_length=2)
    article_title: str = Field(..., min_length=8)
    article_intro: str = Field(..., min_length=40)


class MetaTagsPayload(BaseModel):
    meta_title: str = Field(..., min_length=35, max_length=80)
    meta_description: str = Field(..., min_length=120, max_length=160)
    tags: List[str] = Field(default_factory=list, min_length=3, max_length=5)
