from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, ValidationInfo, field_validator
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


def _ensure_string_list(value: Any, *, max_items: int = 8) -> List[str]:
    if isinstance(value, list):
        return _trim_string_list(value, max_items=max_items, max_chars=160)
    text = _trim_text(value, max_chars=160)
    return [text] if text else []


def _normalize_article_angle(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {
        "decision_criteria",
        "process_and_decision_factors",
        "process_and_next_steps",
        "recognition_and_next_steps",
        "comparison_and_evaluation",
        "explainer",
    }:
        return text
    if any(token in text for token in ("schritt", "ablauf", "next", "naechst")):
        return "process_and_next_steps"
    if any(token in text for token in ("vergleich", "vergleichend", "bewertung", "evaluation")):
        return "comparison_and_evaluation"
    if any(token in text for token in ("erkennen", "warnzeichen", "hinweis")):
        return "recognition_and_next_steps"
    if any(token in text for token in ("kriter", "auswahl", "worauf")):
        return "decision_criteria"
    if any(token in text for token in ("faktor", "entscheidung")):
        return "process_and_decision_factors"
    return "explainer"


def _normalize_backlink_strategy(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"secondary_resource", "supporting_context", "evidence_support"}:
        return text
    if any(token in text for token in ("evidence", "beleg", "quelle", "studie")):
        return "evidence_support"
    if any(token in text for token in ("secondary", "sekund", "weiterf", "resource")):
        return "secondary_resource"
    return "supporting_context"


def _normalize_section_id(value: Any, index: int) -> str:
    text = str(value or "").strip().lower()
    match = re.fullmatch(r"s(?:ection)?[_-]?(\d+)", text)
    if match:
        return f"section_{int(match.group(1))}"
    if len(text) >= 3:
        return text
    return f"section_{index}"


def _normalize_master_plan_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    normalized = dict(payload)
    normalized["article_angle"] = _normalize_article_angle(payload.get("article_angle"))

    publishing_site = payload.get("publishing_site")
    if isinstance(publishing_site, dict):
        normalized_publishing_site = dict(publishing_site)
        fit_reason = _trim_text(publishing_site.get("fit_reason"), max_chars=220)
        inventory_rationale = _trim_text(publishing_site.get("inventory_rationale"), max_chars=220)
        if len(fit_reason) < 12:
            fit_reason = "Strong topical and editorial fit for the requested article."
        if len(inventory_rationale) < 12:
            inventory_rationale = "Existing inventory supports relevant internal-link opportunities."
        normalized_publishing_site["fit_reason"] = fit_reason
        normalized_publishing_site["inventory_rationale"] = inventory_rationale
        normalized["publishing_site"] = normalized_publishing_site

    backlink_plan = payload.get("backlink_plan")
    if isinstance(backlink_plan, dict):
        normalized_backlink = dict(backlink_plan)
        normalized_backlink["strategy"] = _normalize_backlink_strategy(backlink_plan.get("strategy"))
        normalized["backlink_plan"] = normalized_backlink

    sections = payload.get("sections")
    if isinstance(sections, list):
        normalized_sections: List[Dict[str, Any]] = []
        for index, section in enumerate(sections, start=1):
            if not isinstance(section, dict):
                continue
            item = dict(section)
            item["section_id"] = _normalize_section_id(section.get("section_id"), index)
            h2 = _trim_text(section.get("h2"), max_chars=140)
            if not h2:
                h2 = "FAQ" if str(section.get("kind") or "").strip().lower() == "faq" else f"Abschnitt {index}"
            item["h2"] = h2
            goal = _trim_text(section.get("goal"), max_chars=220)
            item["goal"] = goal or f"Behandle den Abschnitt {index} konkret und nutzerorientiert."
            item["key_points"] = _ensure_string_list(section.get("key_points"), max_items=6)
            item["required_terms"] = _ensure_string_list(section.get("required_terms"), max_items=8)
            normalized_sections.append(item)
        normalized["sections"] = normalized_sections

    for key, max_items in (
        ("risk_notes", 6),
        ("warnings", 8),
        ("quality_requirements", 10),
        ("forbidden_phrases", 12),
        ("internal_link_titles", 5),
    ):
        if key in payload:
            normalized[key] = _ensure_string_list(payload.get(key), max_items=max_items)

    faq_questions = _ensure_string_list(payload.get("faq_questions"), max_items=5)
    while len(faq_questions) < 3:
        fallback_index = len(faq_questions) + 1
        faq_questions.append(f"Welche praktischen Punkte sind bei diesem Thema besonders wichtig {fallback_index}?")
    normalized["faq_questions"] = faq_questions[:5]

    keyword_strategy = payload.get("keyword_strategy")
    if isinstance(keyword_strategy, dict):
        normalized_keyword_strategy = dict(keyword_strategy)
        for key, max_items in (
            ("secondary_keywords", 6),
            ("semantic_entities", 10),
        ):
            normalized_keyword_strategy[key] = _ensure_string_list(keyword_strategy.get(key), max_items=max_items)
        normalized["keyword_strategy"] = normalized_keyword_strategy

    return normalized


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
    def _clean_string_lists(cls, value: Any, info: ValidationInfo) -> List[str]:
        if not isinstance(value, list):
            return []
        cleaned = [str(item).strip() for item in value if str(item).strip()]
        if info.field_name == "internal_link_titles":
            return cleaned[:5]
        if info.field_name == "notes":
            return cleaned[:8]
        return cleaned

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
        last_error: Optional[Exception] = None
        try:
            payload = self.provider.call_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                request_label=request_label,
            )
            payload = _normalize_master_plan_payload(payload)
            return MasterArticlePlan.model_validate(payload)
        except (LLMError, ValidationError) as exc:
            last_error = exc
            repair_prompt = (
                user_prompt
                + "\n\nYour previous response was invalid or incomplete."
                + f"\nValidation issue: {_trim_text(exc, max_chars=300)}"
                + "\nReturn a smaller, fully valid JSON object now."
            )
        try:
            payload = self.provider.call_json(
                system_prompt=system_prompt,
                user_prompt=repair_prompt,
                request_label=f"{request_label}_retry",
            )
            payload = _normalize_master_plan_payload(payload)
            return MasterArticlePlan.model_validate(payload)
        except (LLMError, ValidationError) as exc:
            message = _trim_text(exc, max_chars=500)
            if last_error is not None:
                message = _trim_text(f"first_error={last_error}; retry_error={exc}", max_chars=500)
            raise LLMError(f"Supervisor returned invalid master_article_plan: {message}") from exc
