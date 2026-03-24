from __future__ import annotations

import json
from typing import List, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator

from .decision_schemas import DraftArticlePayload, MasterArticlePlan
from .llm_provider import CreatorLLMProvider, LLMRole, build_provider, schema_prompt_block


class WriterContext(BaseModel):
    target_site_url: HttpUrl
    publishing_site_url: HttpUrl
    master_plan: MasterArticlePlan
    validation_feedback: List[str] = Field(default_factory=list)
    content_brief: str = ""
    internal_link_titles: List[str] = Field(default_factory=list)

    @field_validator("validation_feedback", "internal_link_titles", mode="before")
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
            "validation_feedback": self.validation_feedback,
            "internal_link_titles": self.internal_link_titles,
            "master_plan": self.master_plan.model_dump(mode="json"),
        }


WRITER_SYSTEM_PROMPT = """You are the writer for German SEO guest posts.

You must write only from the approved master_article_plan.
Do not invent a new angle, do not change the publishing site choice, and do not add sections that are not in the plan.

Requirements:
- Write natural, publishable German (de-DE).
- Produce one coherent HTML article with exactly one H1.
- Follow the plan's section order exactly.
- The final two H2 sections must be Fazit and FAQ.
- FAQ must answer the listed questions directly using H3 question headings.
- Do not include hyperlinks. The application inserts them later.
- Avoid advertorial phrasing, generic filler, and repeated keyword stuffing.
- Keep metadata aligned with the plan.
- Return only valid JSON that matches the schema exactly.
"""


def build_writer_system_prompt() -> str:
    return (
        WRITER_SYSTEM_PROMPT.strip()
        + "\n\nReturn JSON that matches this schema:\n"
        + schema_prompt_block(DraftArticlePayload)
    )


def build_writer_user_prompt(context: WriterContext) -> str:
    payload = json.dumps(context.prompt_payload(), ensure_ascii=False, indent=2, sort_keys=True)
    return (
        "Write the article strictly from this master_article_plan.\n"
        "article_html must be full HTML that includes one H1 and all planned H2/H3 sections.\n"
        "Do not add links.\n\n"
        f"{payload}"
    )


class CreatorWriter:
    def __init__(self, provider: Optional[CreatorLLMProvider] = None) -> None:
        self.provider = provider or build_provider(LLMRole.WRITER)

    def write_article(
        self,
        context: WriterContext,
        *,
        request_label: str = "writer_draft_article",
    ) -> DraftArticlePayload:
        return self.provider.call_schema(
            schema_model=DraftArticlePayload,
            system_prompt=build_writer_system_prompt(),
            user_prompt=build_writer_user_prompt(context),
            request_label=request_label,
        )
