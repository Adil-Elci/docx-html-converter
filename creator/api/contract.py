from __future__ import annotations

from enum import Enum
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


class SearchIntent(str, Enum):
    INFORMATIONAL = "informational"
    COMMERCIAL = "commercial"
    TRANSACTIONAL = "transactional"
    NAVIGATIONAL = "navigational"


class GermanTone(str, Enum):
    SIE = "sie"
    DU = "du"


class ArticleLanguage(str, Enum):
    DE = "de"
    FR = "fr"


class ArticleFormat(str, Enum):
    NARRATIVE = "narrative"
    LISTICLE = "listicle"


class ServiceType(str, Enum):
    """Which guest-post product this article is.

    ``ARTICLE`` (default): the article carries a backlink to the target site,
    but the target is NEVER named openly — the anchor is a natural/contextual
    or keyword phrase, never the brand/domain. The reader cannot tell which
    site is being promoted.

    ``BRAND_MENTION``: the target brand is name-dropped openly for SEO entity
    association + PR reach, but the article carries ZERO outbound links to the
    target (``link_plan`` is empty).
    """

    ARTICLE = "article"
    BRAND_MENTION = "brand_mention"


# Anchor strategies that name the target site outright. Forbidden in ARTICLE
# mode, where the backlink must stay hidden behind a neutral anchor.
OPEN_ANCHOR_STRATEGIES = frozenset({"branded"})


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
    # Lenient bounds — tight quality constraints belong in eval_harness, not
    # here. Schema-level rejection of a 19-char mandate would just burn the
    # contract call without producing a usable artifact.
    h2: str = Field(..., min_length=4, description="The H2 heading text.")
    mandate: str = Field(..., min_length=10, description="What the section must cover and why.")
    target_word_count: int = Field(..., ge=50, le=600)
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
    item_list: bool = True


class ListiclePlan(BaseModel):
    """Listicle-format plan. Only populated when ``ContentContract.format=LISTICLE``.

    Each ranked item becomes one ``<h2>`` block in the final article. Item count
    bounded so eval can deterministically gate the structure (5..15).
    """

    item_count: int = Field(..., ge=5, le=15)
    ranking_basis: Literal["score", "alphabetical", "unranked"] = "score"
    item_template: List[str] = Field(
        default_factory=lambda: ["name", "hook", "pros", "cons", "verdict"],
        description="Required fields per item; the writer renders each as the matching HTML element.",
    )
    items: List[str] = Field(
        default_factory=list,
        description="Item names in their final ranked order. Length must equal item_count when populated.",
    )


class ContentContract(BaseModel):
    """Immutable specification for a single article.

    Generated once per order by the contract step. Every downstream call
    (section writer, voice pass, enforcer) treats this as ground truth.
    """

    target_keyword: str = Field(..., min_length=2)
    secondary_keywords: List[str] = Field(default_factory=list, max_length=12)
    intent: SearchIntent
    language: ArticleLanguage = ArticleLanguage.DE
    tone: GermanTone = GermanTone.SIE
    target_audience: str = Field(..., min_length=5)
    word_count_target: int = Field(..., ge=200, le=5000)
    format: ArticleFormat = ArticleFormat.NARRATIVE
    listicle_plan: Optional[ListiclePlan] = None

    service_type: ServiceType = ServiceType.ARTICLE
    brand_name: Optional[str] = Field(
        default=None,
        description=(
            "Human-readable brand/site name of the backlink target. Required for "
            "service_type=brand_mention (the name to drop in the body); ignored for "
            "service_type=article, where the target is never named."
        ),
    )

    # Contract bounds catch only output that would break downstream code.
    # SEO-quality bands (50-60 title, 140-160 description, 800-1500 words,
    # etc.) live in eval_harness, where they surface as flagged checks for
    # human review rather than rejecting the whole contract on a 44-char
    # title or a 510-word target.
    h1: str = Field(..., min_length=5, max_length=200)
    meta_title: str = Field(..., min_length=10, max_length=100)
    meta_description: str = Field(..., min_length=30, max_length=250)
    slug: str = Field(..., min_length=3, max_length=120)

    sections: List[SectionPlan] = Field(..., min_length=2, max_length=12)
    faq_items: List[FAQItem] = Field(default_factory=list, max_length=10)

    required_entities: List[EntityRequirement] = Field(default_factory=list)
    link_plan: List[LinkTarget] = Field(default_factory=list)
    schema_spec: SchemaSpec = Field(default_factory=SchemaSpec)

    ai_tell_blocklist: List[str] = Field(
        default_factory=list,
        description="German phrases the writer must never use.",
    )

    competitor_top_urls: List[str] = Field(default_factory=list, max_length=5)
    contract_version: str = Field(default="v1")

    @model_validator(mode="after")
    def _validate_format_plan(self) -> "ContentContract":
        if self.format == ArticleFormat.LISTICLE:
            if self.listicle_plan is None:
                raise ValueError("listicle_plan is required when format=listicle.")
            items = self.listicle_plan.items
            if items and len(items) != self.listicle_plan.item_count:
                raise ValueError(
                    f"listicle_plan.items length ({len(items)}) must match item_count ({self.listicle_plan.item_count})."
                )
        elif self.listicle_plan is not None:
            # Narrative articles must not carry a listicle plan; reject silently
            # rather than auto-clearing so the LLM doesn't ship a half-formed
            # contract that's easy to misread downstream.
            raise ValueError("listicle_plan must be omitted when format=narrative.")
        return self

    @model_validator(mode="after")
    def _validate_service_type(self) -> "ContentContract":
        # Brand-mention articles never link to the target — a stray link_plan
        # entry would make the section writer insert a backlink, defeating the
        # whole "unlinked mention" product. Article-mode links must stay hidden:
        # the 'branded' anchor names the target outright, which is exactly what
        # this mode forbids.
        if self.service_type == ServiceType.BRAND_MENTION:
            if self.link_plan:
                raise ValueError(
                    "service_type=brand_mention must have an empty link_plan "
                    "(no outbound link to the target site)."
                )
        else:
            for link in self.link_plan:
                if link.anchor_strategy in OPEN_ANCHOR_STRATEGIES:
                    raise ValueError(
                        f"service_type=article forbids the {link.anchor_strategy!r} anchor "
                        "strategy — the target site must never be named openly."
                    )
        return self
