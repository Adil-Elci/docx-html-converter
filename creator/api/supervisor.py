from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator
from pydantic import ValidationError

from .decision_schemas import MasterArticlePlan
from .llm import LLMError
from .llm_provider import CreatorLLMProvider, LLMRole, build_provider


def _trim_text(value: Any, *, max_chars: int = 220) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def _trim_string_list(values: Any, *, max_items: int, max_chars: int = 120) -> List[str]:
    if not isinstance(values, list):
        return []
    cleaned: List[str] = []
    for item in values:
        text = _trim_text(item, max_chars=max_chars)
        if text:
            cleaned.append(text)
        if len(cleaned) >= max_items:
            break
    return cleaned


def _compact_profile_payload(profile: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(profile, dict):
        return {}
    compact: Dict[str, Any] = {}
    for key in (
        "normalized_url",
        "page_title",
        "meta_description",
        "primary_context",
        "business_type",
        "business_intent",
        "content_tone",
    ):
        text = _trim_text(profile.get(key), max_chars=160)
        if text:
            compact[key] = text
    for key in (
        "topics",
        "contexts",
        "topic_clusters",
        "site_categories",
        "services_or_products",
        "repeated_keywords",
        "content_style",
        "visible_headings",
    ):
        values = _trim_string_list(profile.get(key), max_items=5, max_chars=90)
        if values:
            compact[key] = values
    return compact


class PublishingCandidateInput(BaseModel):
    site_url: HttpUrl
    site_id: Optional[str] = None
    fit_score: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    inventory_count: int = Field(default=0, ge=0)
    internal_link_titles: List[str] = Field(default_factory=list, max_length=5)
    profile: Dict[str, Any] = Field(default_factory=dict)
    notes: List[str] = Field(default_factory=list, max_length=8)

    @field_validator("site_id", mode="before")
    @classmethod
    def _trim_site_id(cls, value: Any) -> Optional[str]:
        cleaned = str(value or "").strip()
        return cleaned or None

    @field_validator("internal_link_titles", "notes", mode="before")
    @classmethod
    def _clean_string_lists(cls, value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    def prompt_payload(self) -> Dict[str, Any]:
        return {
            "site_url": str(self.site_url),
            "site_id": self.site_id,
            "fit_score": self.fit_score,
            "inventory_count": self.inventory_count,
            "internal_link_titles": self.internal_link_titles[:4],
            "profile": _compact_profile_payload(self.profile),
            "notes": self.notes,
        }


class SupervisorContext(BaseModel):
    target_site_url: HttpUrl
    target_profile: Dict[str, Any] = Field(default_factory=dict)
    publishing_candidates: List[PublishingCandidateInput] = Field(default_factory=list, min_length=1)
    requested_topic: Optional[str] = None
    anchor_text: Optional[str] = None
    exclude_topics: List[str] = Field(default_factory=list)
    recent_article_titles: List[str] = Field(default_factory=list)
    target_keyword_hints: List[str] = Field(default_factory=list)
    target_context_notes: List[str] = Field(default_factory=list)

    @field_validator("requested_topic", "anchor_text", mode="before")
    @classmethod
    def _trim_optional_text(cls, value: Any) -> Optional[str]:
        cleaned = str(value or "").strip()
        return cleaned or None

    @field_validator(
        "exclude_topics",
        "recent_article_titles",
        "target_keyword_hints",
        "target_context_notes",
        mode="before",
    )
    @classmethod
    def _clean_string_lists(cls, value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    def prompt_payload(self) -> Dict[str, Any]:
        return {
            "target_site_url": str(self.target_site_url),
            "target_profile": _compact_profile_payload(self.target_profile),
            "requested_topic": self.requested_topic,
            "anchor_text": self.anchor_text,
            "exclude_topics": self.exclude_topics[:4],
            "recent_article_titles": self.recent_article_titles[:4],
            "target_keyword_hints": self.target_keyword_hints[:6],
            "target_context_notes": self.target_context_notes[:5],
            "publishing_candidates": [candidate.prompt_payload() for candidate in self.publishing_candidates],
        }


SUPERVISOR_SYSTEM_PROMPT = """You are the creator supervisor for German SEO guest posts.

Your job is to make the highest-quality global article decision before writing starts.
You must choose the best publishing site candidate and return one complete master article plan.

Requirements:
- Think globally across target fit, publishing-site fit, inventory depth, internal-link potential, duplicate risk, search intent, and editorial coherence.
- Prefer plans that are specific, publishable, and naturally support internal links.
- Avoid generic lifestyle framing when the topic can be made more concrete.
- Avoid repeating excluded topics or recent article-title angles.
- If a requested topic is provided, respect it unless it is clearly weak or duplicative; in that case, keep the same user need but choose a sharper angle.
- Keep the article practical, specific, and non-promotional.
- Set image_strategy so the featured image supports the article editorially instead of as generic stock art.
- Use forbidden_phrases and quality_requirements to keep the downstream draft sharp, non-spammy, and publishable.
- Do not output markdown or commentary.
- Return only valid JSON that matches the schema exactly.
"""


SUPERVISOR_PLAN_CONTRACT = """Return one JSON object with these keys:
- publishing_site: {site_id|null, site_url, fit_reason, inventory_rationale, confidence}
- topic
- intent_type
- article_angle
- audience
- tone
- differentiator
- title_package: {h1, meta_title, slug}
- keyword_strategy: {primary_keyword, secondary_keywords[], semantic_entities[], keyword_intent_note}
- backlink_plan: {strategy, anchor_text, placement_hint, rationale}
- image_strategy: {featured_prompt, featured_alt, include_in_content, in_content_prompt, in_content_alt}
- faq_questions: exactly 3 concise user-facing questions
- internal_link_titles: 0 to 5 titles taken from the provided candidate inventory
- sections: 5 or 6 sections total
  - body sections first
  - then Fazit
  - then FAQ
  - every section needs section_id, kind, h2, goal, key_points, required_terms, target_min_words, target_max_words
- forbidden_phrases: short phrases to avoid
- quality_requirements: short editorial requirements to enforce
- risk_notes
- warnings

Rules:
- Keep sections lean and concrete.
- Never return null for any section h2, goal, or section_id.
- Keep strings concise.
- Choose exactly one publishing candidate from the input.
- Return JSON only."""


def build_supervisor_system_prompt() -> str:
    return SUPERVISOR_SYSTEM_PROMPT.strip() + "\n\n" + SUPERVISOR_PLAN_CONTRACT


def build_supervisor_user_prompt(context: SupervisorContext) -> str:
    payload = json.dumps(context.prompt_payload(), ensure_ascii=False, indent=2, sort_keys=True)
    return (
        "Create one master_article_plan for this target and candidate set.\n"
        "Choose exactly one publishing site from publishing_candidates.\n"
        "The plan must be coherent enough that a writer can follow it without inventing a new angle.\n"
        "Keep the output compact and concrete.\n\n"
        f"{payload}"
    )


class CreatorSupervisor:
    def __init__(self, provider: Optional[CreatorLLMProvider] = None) -> None:
        self.provider = provider or build_provider(LLMRole.SUPERVISOR)

    def create_master_article_plan(
        self,
        context: SupervisorContext,
        *,
        request_label: str = "supervisor_master_article_plan",
    ) -> MasterArticlePlan:
        system_prompt = build_supervisor_system_prompt()
        user_prompt = build_supervisor_user_prompt(context)
        try:
            payload = self.provider.call_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                request_label=request_label,
            )
            return MasterArticlePlan.model_validate(payload)
        except (LLMError, ValidationError) as exc:
            repair_prompt = (
                user_prompt
                + "\n\nYour previous response was invalid or incomplete."
                + f"\nValidation issue: {_trim_text(exc, max_chars=300)}"
                + "\nReturn a smaller, fully valid JSON object now."
            )
            payload = self.provider.call_json(
                system_prompt=system_prompt,
                user_prompt=repair_prompt,
                request_label=f"{request_label}_retry",
            )
            return MasterArticlePlan.model_validate(payload)
