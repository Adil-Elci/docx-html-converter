from __future__ import annotations

import json
from typing import List, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator

from .decision_schemas import CriticReview, DraftArticlePayload, MasterArticlePlan
from .llm_provider import CreatorLLMProvider, LLMRole, build_provider, schema_prompt_block


class RepairContext(BaseModel):
    target_site_url: HttpUrl
    publishing_site_url: HttpUrl
    master_plan: MasterArticlePlan
    draft_article: DraftArticlePayload
    critic_review: CriticReview
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
            "critic_review": self.critic_review.model_dump(mode="json"),
            "draft_article": self.draft_article.model_dump(mode="json"),
        }


REPAIR_SYSTEM_PROMPT = """You are the repair writer for German SEO guest posts.

Your task is to repair an existing draft that is already close to publishable.

Requirements:
- Rewrite only what is needed to satisfy the critic review and deterministic validation errors.
- Keep the same article topic, publishing-site choice, and overall section order from the master_article_plan.
- Preserve exactly one H1.
- The final two H2 sections must remain Fazit and FAQ.
- FAQ must answer the listed questions directly using H3 question headings.
- Do not add hyperlinks. The application inserts them later.
- Improve heading naturalness, FAQ usefulness, specificity, metadata alignment, and plan adherence.
- Return only valid JSON that matches the schema exactly.
"""


def build_repair_system_prompt() -> str:
    return (
        REPAIR_SYSTEM_PROMPT.strip()
        + "\n\nReturn JSON that matches this schema:\n"
        + schema_prompt_block(DraftArticlePayload)
    )


def build_repair_user_prompt(context: RepairContext) -> str:
    payload = json.dumps(context.prompt_payload(), ensure_ascii=False, indent=2, sort_keys=True)
    return (
        "Repair this draft strictly against the master_article_plan and critic_review.\n"
        "Do not change the topic or publishing site choice.\n"
        "article_html must be full HTML that includes one H1 and all planned H2/H3 sections.\n"
        "Do not add links.\n\n"
        f"{payload}"
    )


class CreatorRepair:
    def __init__(self, provider: Optional[CreatorLLMProvider] = None) -> None:
        self.provider = provider or build_provider(LLMRole.REPAIR)

    def repair_article(
        self,
        context: RepairContext,
        *,
        request_label: str = "repair_rewrite_article",
    ) -> DraftArticlePayload:
        return self.provider.call_schema(
            schema_model=DraftArticlePayload,
            system_prompt=build_repair_system_prompt(),
            user_prompt=build_repair_user_prompt(context),
            request_label=request_label,
        )
