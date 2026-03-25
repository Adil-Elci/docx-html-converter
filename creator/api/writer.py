from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, field_validator
from pydantic import ValidationError

from .decision_schemas import DraftArticleSlotsPayload, MasterArticlePlan
from .llm import LLMError
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


def _strip_html_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value or "").strip()


def _extract_first_paragraph_text(value: str) -> str:
    match = re.search(r"<p[^>]*>(.*?)</p>", value or "", flags=re.IGNORECASE | re.DOTALL)
    if match:
        return re.sub(r"\s+", " ", _strip_html_tags(match.group(1))).strip()
    return re.sub(r"\s+", " ", _strip_html_tags(value)).strip()


def _normalize_heading_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _strip_html_tags(value).lower()).strip()


def _extract_article_intro_and_h2_sections(article_html: str) -> tuple[str, List[Dict[str, str]]]:
    html = str(article_html or "").strip()
    if not html:
        return "", []
    h1_match = re.search(r"<h1[^>]*>.*?</h1>", html, flags=re.IGNORECASE | re.DOTALL)
    content_start = h1_match.end() if h1_match else 0
    h2_matches = list(re.finditer(r"<h2[^>]*>(.*?)</h2>", html, flags=re.IGNORECASE | re.DOTALL))
    intro_end = h2_matches[0].start() if h2_matches else len(html)
    intro_html = html[content_start:intro_end].strip()
    sections: List[Dict[str, str]] = []
    for index, match in enumerate(h2_matches):
        start = match.end()
        end = h2_matches[index + 1].start() if index + 1 < len(h2_matches) else len(html)
        sections.append(
            {
                "heading": _strip_html_tags(match.group(1)).strip(),
                "body_html": html[start:end].strip(),
            }
        )
    return intro_html, sections


def _extract_faq_items_from_section_html(section_html: str) -> List[Dict[str, str]]:
    html = str(section_html or "").strip()
    if not html:
        return []
    matches = list(re.finditer(r"<h3[^>]*>(.*?)</h3>", html, flags=re.IGNORECASE | re.DOTALL))
    items: List[Dict[str, str]] = []
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(html)
        question = _strip_html_tags(match.group(1)).strip()
        answer_html = str(html[start:end] or "").strip()
        if question:
            items.append({"question": question, "answer_html": answer_html})
    return items


def _extract_slots_from_article_html(article_html: str, context: WriterContext) -> Dict[str, object]:
    intro_html, extracted_sections = _extract_article_intro_and_h2_sections(article_html)
    planned_sections = context.master_plan.sections
    body_sections: List[Dict[str, str]] = []
    faq_answers: List[Dict[str, str]] = []
    remaining_sections = list(extracted_sections)
    fazit_consumed = False

    for section in planned_sections:
        section_id = str(section.section_id or "").strip()
        kind = str(section.kind or "body").strip()
        if not section_id:
            continue
        if kind == "faq":
            faq_section_index = next(
                (idx for idx, candidate in enumerate(remaining_sections) if _normalize_heading_key(candidate.get("heading") or "") == "faq"),
                None,
            )
            faq_section = remaining_sections.pop(faq_section_index) if faq_section_index is not None else None
            parsed_items = _extract_faq_items_from_section_html(str((faq_section or {}).get("body_html") or ""))
            if parsed_items:
                for index, question in enumerate(context.master_plan.faq_questions):
                    answer_html = ""
                    if index < len(parsed_items):
                        answer_html = str(parsed_items[index].get("answer_html") or "").strip()
                    faq_answers.append({"question": question, "answer_html": answer_html})
            continue
        selected_html = ""
        if kind == "fazit":
            fazit_index = next(
                (idx for idx, candidate in enumerate(remaining_sections) if _normalize_heading_key(candidate.get("heading") or "") == "fazit"),
                None,
            )
            if fazit_index is not None:
                selected_html = str(remaining_sections.pop(fazit_index).get("body_html") or "").strip()
                fazit_consumed = True
        if not selected_html and remaining_sections:
            if kind == "fazit" and not fazit_consumed:
                selected_html = str(remaining_sections.pop(-1).get("body_html") or "").strip()
            else:
                selected_html = str(remaining_sections.pop(0).get("body_html") or "").strip()
        body_sections.append({"section_id": section_id, "body_html": selected_html})
    return {
        "intro_html": intro_html,
        "section_bodies": body_sections,
        "faq_answers": faq_answers,
    }


def _normalize_meta_description(value: str, *, fallback_text: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip()).strip()
    if len(text) >= 80:
        return text[:160].rstrip()
    fallback = re.sub(r"\s+", " ", str(fallback_text or "").strip()).strip()
    combined = f"{text} {fallback}".strip() if text else fallback
    combined = re.sub(r"\s+", " ", combined).strip()
    if len(combined) < 80:
        combined = (combined + " Konkrete Kriterien, alltagsnahe Einordnung und klare nächste Schritte für Leserinnen und Leser.").strip()
    return combined[:160].rstrip()


def _coerce_section_bodies(value: object) -> List[Dict[str, str]]:
    if isinstance(value, dict):
        return [
            {"section_id": str(section_id).strip(), "body_html": str(body_html or "").strip()}
            for section_id, body_html in value.items()
            if str(section_id).strip()
        ]
    if not isinstance(value, list):
        return []
    normalized: List[Dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        section_id = str(item.get("section_id") or item.get("id") or "").strip()
        body_html = str(item.get("body_html") or item.get("html") or "").strip()
        if section_id:
            normalized.append({"section_id": section_id, "body_html": body_html})
    return normalized


def _coerce_faq_answers(value: object, questions: List[str]) -> List[Dict[str, str]]:
    if isinstance(value, dict):
        return [
            {"question": str(question).strip(), "answer_html": str(answer_html or "").strip()}
            for question, answer_html in value.items()
            if str(question).strip()
        ]
    if not isinstance(value, list):
        return []
    normalized: List[Dict[str, str]] = []
    for index, item in enumerate(value):
        if isinstance(item, dict):
            question = str(item.get("question") or "").strip()
            answer_html = str(item.get("answer_html") or item.get("html") or "").strip()
        else:
            question = questions[index] if index < len(questions) else ""
            answer_html = str(item or "").strip()
        if question:
            normalized.append({"question": question, "answer_html": answer_html})
    return normalized


def _normalize_slot_payload(payload: dict[str, object], context: WriterContext) -> dict[str, object]:
    normalized = dict(payload or {})
    title_package = context.master_plan.title_package
    article_html = str(normalized.get("article_html") or "").strip()
    if article_html and not normalized.get("intro_html") and not normalized.get("section_bodies"):
        normalized.update(_extract_slots_from_article_html(article_html, context))
    intro_html = str(normalized.get("intro_html") or "").strip()
    if not intro_html and article_html:
        intro_html, _ = _extract_article_intro_and_h2_sections(article_html)
    excerpt = re.sub(r"\s+", " ", str(normalized.get("excerpt") or "").strip()).strip()
    if len(excerpt) < 40:
        excerpt = _extract_first_paragraph_text(intro_html or article_html)[:200]
    normalized["intro_html"] = intro_html
    normalized["section_bodies"] = _coerce_section_bodies(normalized.get("section_bodies"))
    normalized["faq_answers"] = _coerce_faq_answers(normalized.get("faq_answers"), list(context.master_plan.faq_questions))
    normalized["meta_title"] = str(normalized.get("meta_title") or "").strip() or title_package.meta_title
    normalized["slug"] = str(normalized.get("slug") or "").strip() or title_package.slug
    normalized["excerpt"] = excerpt
    fallback_meta_text = " ".join(
        part for part in [
            excerpt,
            _extract_first_paragraph_text(intro_html or article_html),
            context.master_plan.topic,
        ] if part
    )
    normalized["meta_description"] = _normalize_meta_description(
        str(normalized.get("meta_description") or "").strip(),
        fallback_text=fallback_meta_text,
    )
    return normalized


WRITER_SYSTEM_PROMPT = """You are the writer for German SEO guest posts.

You must write only from the approved master_article_plan.
Do not invent a new angle, do not change the publishing site choice, and do not add sections that are not in the plan.

Requirements:
- Write natural, publishable German (de-DE).
- Do not return a full article HTML document.
- Return content slots only: intro_html, section_bodies, faq_answers, and metadata.
- section_bodies must contain only body HTML keyed by approved section_id values. Do not include H2 headings.
- faq_answers must contain only answer HTML keyed by the approved FAQ questions. Do not include H3 headings.
- In each body section, use at least two concrete specifics from that section's key_points or required_terms when they are available.
- Respect forbidden_phrases and quality_requirements from the master_article_plan.
- Do not include hyperlinks. The application inserts them later.
- Do not repeat target brand names, domain names, or anchor text in prose unless the approved plan explicitly requires it.
- Avoid advertorial phrasing, generic filler, and repeated keyword stuffing.
- Keep metadata aligned with the plan.
- Keep JSON compact. Minify HTML fragments inside the JSON string and do not add commentary outside the JSON object.
- Return only valid JSON that matches the schema exactly.
"""


def build_writer_system_prompt() -> str:
    return (
        WRITER_SYSTEM_PROMPT.strip()
        + "\n\nReturn JSON that matches this schema:\n"
        + schema_prompt_block(DraftArticleSlotsPayload)
    )


def build_writer_user_prompt(context: WriterContext) -> str:
    payload = json.dumps(context.prompt_payload(), ensure_ascii=False, indent=2, sort_keys=True)
    return (
        "Write the article strictly from this master_article_plan.\n"
        "Do not emit a full article_html field.\n"
        "Emit only intro_html, section_bodies, faq_answers, and metadata.\n"
        "Each section_bodies item must use a section_id from the plan and contain only that section's body HTML.\n"
        "Each faq_answers item must answer one listed FAQ question and contain only the answer HTML.\n"
        "For each body section, cover at least two of that section's concrete key_points/required_terms when provided.\n"
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
    ) -> DraftArticleSlotsPayload:
        system_prompt = build_writer_system_prompt()
        user_prompt = build_writer_user_prompt(context)
        try:
            return self.provider.call_schema(
                schema_model=DraftArticleSlotsPayload,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                request_label=request_label,
            )
        except (LLMError, ValidationError) as exc:
            if not hasattr(self.provider, "call_json"):
                raise
            retry_prompt = (
                user_prompt
                + "\n\nYour previous response was invalid or incomplete."
                + "\nReturn a smaller but still complete JSON object now."
                + "\nRules for the retry:"
                + "\n- Return JSON only."
                + "\n- Keep HTML fragments compact on a single line."
                + "\n- Do not include any explanation before or after the JSON."
            )
            payload = self.provider.call_json(
                system_prompt=system_prompt,
                user_prompt=retry_prompt,
                request_label=f"{request_label}_retry",
                allow_html_fallback=True,
            )
            return DraftArticleSlotsPayload.model_validate(_normalize_slot_payload(payload, context))
