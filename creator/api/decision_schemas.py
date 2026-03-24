from __future__ import annotations

import json
from enum import Enum
from typing import Dict, List, Optional, Type

from pydantic import BaseModel, Field


class SchemaBackedModel(BaseModel):
    @classmethod
    def json_schema_dict(cls) -> Dict[str, object]:
        return cls.model_json_schema()

    @classmethod
    def json_schema_text(cls) -> str:
        return json.dumps(cls.json_schema_dict(), ensure_ascii=False, indent=2, sort_keys=True)


class ArticleIntentType(str, Enum):
    INFORMATIONAL = "informational"
    COMMERCIAL_INVESTIGATION = "commercial_investigation"
    TRANSACTIONAL = "transactional"
    NAVIGATIONAL = "navigational"


class ArticleAngle(str, Enum):
    DECISION_CRITERIA = "decision_criteria"
    PROCESS_AND_DECISION_FACTORS = "process_and_decision_factors"
    PROCESS_AND_NEXT_STEPS = "process_and_next_steps"
    RECOGNITION_AND_NEXT_STEPS = "recognition_and_next_steps"
    COMPARISON_AND_EVALUATION = "comparison_and_evaluation"
    EXPLAINER = "explainer"


class CriticVerdict(str, Enum):
    PASS = "pass"
    REPAIR_NEEDED = "repair_needed"
    FAIL = "fail"


class BacklinkStrategy(str, Enum):
    SECONDARY_RESOURCE = "secondary_resource"
    SUPPORTING_CONTEXT = "supporting_context"
    EVIDENCE_SUPPORT = "evidence_support"


class PublishingSiteDecision(SchemaBackedModel):
    site_id: Optional[str] = Field(default=None, description="Internal publishing site identifier when available.")
    site_url: str = Field(..., description="Canonical publishing site URL selected by the supervisor.")
    fit_reason: str = Field(..., min_length=12, description="Short explanation of why this site is the best host.")
    inventory_rationale: str = Field(
        ...,
        min_length=12,
        description="Why the site's existing content inventory supports the chosen article.",
    )
    confidence: float = Field(..., ge=0.0, le=1.0, description="Supervisor confidence in this site choice.")


class KeywordStrategy(SchemaBackedModel):
    primary_keyword: str = Field(..., min_length=3, description="Main query phrase for the article.")
    secondary_keywords: List[str] = Field(
        default_factory=list,
        max_length=6,
        description="Supporting query phrases that broaden the topic without duplicating the primary keyword.",
    )
    semantic_entities: List[str] = Field(
        default_factory=list,
        max_length=10,
        description="Named concepts, objects, or terms that should naturally appear in the article.",
    )
    keyword_intent_note: str = Field(
        ...,
        min_length=12,
        description="Why this keyword set matches the target audience and search intent.",
    )


class TitlePackage(SchemaBackedModel):
    h1: str = Field(..., min_length=12, description="Visible H1 title.")
    meta_title: str = Field(..., min_length=35, description="SEO title for metadata.")
    slug: str = Field(..., min_length=3, description="URL slug.")


class BacklinkPlan(SchemaBackedModel):
    strategy: BacklinkStrategy = Field(..., description="How the backlink should function inside the article.")
    anchor_text: str = Field(..., min_length=2, description="Final anchor text to use.")
    placement_hint: str = Field(
        ...,
        min_length=4,
        description="Where the backlink should appear, such as intro or a section identifier.",
    )
    rationale: str = Field(..., min_length=12, description="Why this backlink placement feels natural.")


class SectionPlan(SchemaBackedModel):
    section_id: str = Field(..., min_length=3, description="Stable section identifier.")
    kind: str = Field(..., min_length=3, description="Section type such as body, fazit, or faq.")
    h2: str = Field(..., min_length=3, description="Section H2 heading.")
    goal: str = Field(..., min_length=12, description="What the section must achieve.")
    key_points: List[str] = Field(
        default_factory=list,
        max_length=6,
        description="Concrete points the writer should cover in this section.",
    )
    required_terms: List[str] = Field(
        default_factory=list,
        max_length=8,
        description="Terms that should naturally appear in the section.",
    )
    target_min_words: Optional[int] = Field(default=None, ge=20, le=400, description="Minimum target word count.")
    target_max_words: Optional[int] = Field(default=None, ge=20, le=500, description="Maximum target word count.")


class MasterArticlePlan(SchemaBackedModel):
    publishing_site: PublishingSiteDecision
    topic: str = Field(..., min_length=8, description="Final article topic selected by the supervisor.")
    intent_type: ArticleIntentType
    article_angle: ArticleAngle
    audience: str = Field(..., min_length=3, description="Target audience for the article.")
    tone: str = Field(..., min_length=3, description="Writing tone for the article.")
    differentiator: str = Field(
        ...,
        min_length=12,
        description="What makes this article angle distinct from recent or adjacent pieces.",
    )
    title_package: TitlePackage
    keyword_strategy: KeywordStrategy
    backlink_plan: BacklinkPlan
    faq_questions: List[str] = Field(
        default_factory=list,
        min_length=3,
        max_length=5,
        description="Questions the FAQ must answer directly.",
    )
    internal_link_titles: List[str] = Field(
        default_factory=list,
        max_length=8,
        description="Candidate internal-link article titles the writer should support naturally if relevant.",
    )
    sections: List[SectionPlan] = Field(
        default_factory=list,
        min_length=3,
        max_length=8,
        description="Strict article structure the writer must follow.",
    )
    risk_notes: List[str] = Field(
        default_factory=list,
        max_length=6,
        description="Known risks the writer and critic should watch for.",
    )


class DraftArticlePayload(SchemaBackedModel):
    article_html: str = Field(..., min_length=120, description="Final article HTML body including headings.")
    meta_title: str = Field(..., min_length=35, description="SEO meta title.")
    meta_description: str = Field(..., min_length=80, description="SEO meta description.")
    slug: str = Field(..., min_length=3, description="URL slug.")
    excerpt: str = Field(..., min_length=40, description="Short excerpt or standfirst.")


class CriticIssue(SchemaBackedModel):
    code: str = Field(..., min_length=3, description="Stable issue code.")
    severity: str = Field(..., min_length=3, description="Issue severity such as low, medium, or high.")
    summary: str = Field(..., min_length=8, description="Short explanation of the issue.")
    location_hint: str = Field(default="", description="Section or metadata area where the issue occurs.")
    recommended_fix: str = Field(..., min_length=8, description="What should be corrected.")


class CriticReview(SchemaBackedModel):
    verdict: CriticVerdict
    overall_score: int = Field(..., ge=0, le=100, description="Overall quality score for the draft.")
    plan_alignment_score: int = Field(..., ge=0, le=100, description="How well the draft follows the master plan.")
    editorial_quality_score: int = Field(..., ge=0, le=100, description="Naturalness and usefulness of the writing.")
    seo_quality_score: int = Field(..., ge=0, le=100, description="Quality of titles, headings, and keyword handling.")
    strengths: List[str] = Field(default_factory=list, max_length=6, description="Strong points in the article.")
    issues: List[CriticIssue] = Field(default_factory=list, max_length=10, description="Problems that need attention.")
    repair_instructions: List[str] = Field(
        default_factory=list,
        max_length=8,
        description="Concrete rewrite instructions when verdict is repair_needed.",
    )
    final_recommendation: str = Field(..., min_length=8, description="Short final recommendation.")


SCHEMA_REGISTRY: Dict[str, Type[SchemaBackedModel]] = {
    "master_article_plan": MasterArticlePlan,
    "draft_article_payload": DraftArticlePayload,
    "critic_review": CriticReview,
}


def get_schema_model(name: str) -> Type[SchemaBackedModel]:
    normalized = str(name or "").strip()
    if normalized not in SCHEMA_REGISTRY:
        raise KeyError(f"Unknown schema: {normalized}")
    return SCHEMA_REGISTRY[normalized]


def get_schema_dict(name: str) -> Dict[str, object]:
    return get_schema_model(name).json_schema_dict()


def get_schema_text(name: str) -> str:
    return get_schema_model(name).json_schema_text()
