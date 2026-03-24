from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator

from .decision_schemas import MasterArticlePlan
from .llm_provider import CreatorLLMProvider, LLMRole, build_provider, schema_prompt_block


class PublishingCandidateInput(BaseModel):
    site_url: HttpUrl
    site_id: Optional[str] = None
    fit_score: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    inventory_count: int = Field(default=0, ge=0)
    internal_link_titles: List[str] = Field(default_factory=list, max_length=12)
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
            "internal_link_titles": self.internal_link_titles,
            "profile": self.profile,
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
            "target_profile": self.target_profile,
            "requested_topic": self.requested_topic,
            "anchor_text": self.anchor_text,
            "exclude_topics": self.exclude_topics,
            "recent_article_titles": self.recent_article_titles,
            "target_keyword_hints": self.target_keyword_hints,
            "target_context_notes": self.target_context_notes,
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
- Do not output markdown or commentary.
- Return only valid JSON that matches the schema exactly.
"""


def build_supervisor_system_prompt() -> str:
    return (
        SUPERVISOR_SYSTEM_PROMPT.strip()
        + "\n\nReturn JSON that matches this schema:\n"
        + schema_prompt_block(MasterArticlePlan)
    )


def build_supervisor_user_prompt(context: SupervisorContext) -> str:
    payload = json.dumps(context.prompt_payload(), ensure_ascii=False, indent=2, sort_keys=True)
    return (
        "Create one master_article_plan for this target and candidate set.\n"
        "Choose exactly one publishing site from publishing_candidates.\n"
        "The plan must be coherent enough that a writer can follow it without inventing a new angle.\n\n"
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
        return self.provider.call_schema(
            schema_model=MasterArticlePlan,
            system_prompt=build_supervisor_system_prompt(),
            user_prompt=build_supervisor_user_prompt(context),
            request_label=request_label,
        )
