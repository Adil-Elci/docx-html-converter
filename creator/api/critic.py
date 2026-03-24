from __future__ import annotations

import json
from typing import List, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator

from .decision_schemas import CriticReview, DraftArticlePayload, MasterArticlePlan
from .llm_provider import CreatorLLMProvider, LLMRole, build_provider, schema_prompt_block


class CriticContext(BaseModel):
    target_site_url: HttpUrl
    publishing_site_url: HttpUrl
    master_plan: MasterArticlePlan
    draft_article: DraftArticlePayload
    deterministic_validation_errors: List[str] = Field(default_factory=list)
    content_brief: str = ""
    internal_link_titles: List[str] = Field(default_factory=list)

    @field_validator("deterministic_validation_errors", "internal_link_titles", mode="before")
    @classmethod
    def _clean_string_lists(cls, value):  # type: ignore[no-untyped-def]
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    @field_validator("content_brief", mode="before")
    @classmethod
    def _clean_content_brief(cls, value):  # type: ignore[no-untyped-def]
        return str(value or "").strip()

    def prompt_payload(self) -> dict[str, object]:
        return {
            "target_site_url": str(self.target_site_url),
            "publishing_site_url": str(self.publishing_site_url),
            "content_brief": self.content_brief,
            "deterministic_validation_errors": self.deterministic_validation_errors,
            "internal_link_titles": self.internal_link_titles,
            "master_plan": self.master_plan.model_dump(mode="json"),
            "draft_article": self.draft_article.model_dump(mode="json"),
        }


CRITIC_SYSTEM_PROMPT = """You are the critic for German SEO guest posts.

Your task is to review a finished draft against the approved master_article_plan and return only structured scoring.

Requirements:
- Evaluate whether the draft follows the plan, reads naturally, and is publishable.
- Consider title quality, heading naturalness, FAQ usefulness, specificity, internal-link readiness, and overall coherence.
- Score title quality, heading quality, intent consistency, backlink naturalness, specificity, spam risk, and coherence explicitly.
- Respect deterministic_validation_errors as hard evidence, but still score the draft globally.
- Use verdict:
  - pass: publishable without further LLM rewriting
  - repair_needed: close, but needs a constrained rewrite
  - fail: the draft is too weak or too off-plan to repair cheaply
- Return only valid JSON that matches the schema exactly.
"""


def build_critic_system_prompt() -> str:
    return (
        CRITIC_SYSTEM_PROMPT.strip()
        + "\n\nReturn JSON that matches this schema:\n"
        + schema_prompt_block(CriticReview)
    )


def build_critic_user_prompt(context: CriticContext) -> str:
    payload = json.dumps(context.prompt_payload(), ensure_ascii=False, indent=2, sort_keys=True)
    return (
        "Review this draft against the approved master_article_plan.\n"
        "Do not rewrite the article. Score it and return the structured critic_review only.\n\n"
        f"{payload}"
    )


class CreatorCritic:
    def __init__(self, provider: Optional[CreatorLLMProvider] = None) -> None:
        self.provider = provider or build_provider(LLMRole.CRITIC)

    def review_article(
        self,
        context: CriticContext,
        *,
        request_label: str = "critic_review_article",
    ) -> CriticReview:
        return self.provider.call_schema(
            schema_model=CriticReview,
            system_prompt=build_critic_system_prompt(),
            user_prompt=build_critic_user_prompt(context),
            request_label=request_label,
        )
