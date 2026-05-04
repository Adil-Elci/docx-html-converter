from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class SearchIntent(str, Enum):
    INFORMATIONAL = "informational"
    COMMERCIAL = "commercial"
    TRANSACTIONAL = "transactional"
    NAVIGATIONAL = "navigational"


class GermanTone(str, Enum):
    SIE = "sie"
    DU = "du"


class LinkTarget(BaseModel):
    target_url: str = Field(..., description="Absolute URL the anchor must point to.")
    anchor_strategy: str = Field(
        ...,
        description="How the anchor should be phrased: exact_match, partial_match, branded, generic, contextual.",
    )
    section_index: int = Field(..., ge=0, description="Index of the H2 section that hosts this link.")
    surrounding_context_requirements: str = Field(
        ...,
        description="What the paragraph around the anchor must communicate so the link feels natural.",
    )
    link_type: str = Field(..., description="backlink | internal | external_reference")


class EntityRequirement(BaseModel):
    name: str = Field(..., description="Entity name as it should appear in the article (German).")
    placement_hint: Optional[str] = Field(
        default=None,
        description="Optional hint about where the entity belongs (e.g. 'in section 2', 'in FAQ').",
    )


class SectionPlan(BaseModel):
    h2: str = Field(..., min_length=8, description="The H2 heading text.")
    mandate: str = Field(..., min_length=20, description="What the section must cover and why.")
    target_word_count: int = Field(..., ge=80, le=400)
    required_subheadings: List[str] = Field(default_factory=list, description="Optional H3s.")
    required_elements: List[str] = Field(
        default_factory=list,
        description="Structural elements like 'list', 'table', 'example', 'statistic'.",
    )


class FAQItem(BaseModel):
    question: str
    answer_outline: str = Field(..., description="Bullet-point outline the writer expands into the answer.")


class SchemaSpec(BaseModel):
    article: bool = True
    faq_page: bool = True


class ContentContract(BaseModel):
    """Immutable specification for a single article.

    Generated once per order by the contract step. Every downstream call
    (section writer, voice pass, enforcer) treats this as ground truth.
    """

    target_keyword: str = Field(..., min_length=3)
    secondary_keywords: List[str] = Field(default_factory=list, max_length=8)
    intent: SearchIntent
    tone: GermanTone = GermanTone.SIE
    target_audience: str = Field(..., min_length=10)
    word_count_target: int = Field(..., ge=600, le=3000)

    # Contract bounds are intentionally lenient — they catch only output that
    # would break downstream code. SEO-quality bands (50-60 title, 140-160
    # description) live in eval_harness, where they get flagged for human
    # review rather than rejecting the whole contract on a 44-char title.
    h1: str = Field(..., min_length=12, max_length=120)
    meta_title: str = Field(..., min_length=25, max_length=75)
    meta_description: str = Field(..., min_length=70, max_length=170)
    slug: str = Field(..., min_length=3, max_length=90)

    sections: List[SectionPlan] = Field(..., min_length=3, max_length=10)
    faq_items: List[FAQItem] = Field(default_factory=list, max_length=8)

    required_entities: List[EntityRequirement] = Field(default_factory=list)
    link_plan: List[LinkTarget] = Field(default_factory=list)
    schema_spec: SchemaSpec = Field(default_factory=SchemaSpec)

    ai_tell_blocklist: List[str] = Field(
        default_factory=list,
        description="German phrases the writer must never use.",
    )

    competitor_top_urls: List[str] = Field(default_factory=list, max_length=5)
    contract_version: str = Field(default="v1")
