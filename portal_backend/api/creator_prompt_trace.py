from __future__ import annotations

import copy
import json
from typing import Any, Dict, List, Optional, Tuple


def _format_content_brief_prompt_text(content_brief: Dict[str, Any]) -> str:
    if not content_brief:
        return ""

    lines: List[str] = []
    must_cover = [str(item).strip() for item in (content_brief.get("must_cover") or []) if str(item).strip()]
    if must_cover:
        lines.append(f"Must cover: {', '.join(must_cover)}")
    avoid = [str(item).strip() for item in (content_brief.get("avoid") or []) if str(item).strip()]
    if avoid:
        lines.append(f"Avoid: {', '.join(avoid)}")
    evidence = [str(item).strip() for item in (content_brief.get("evidence") or []) if str(item).strip()]
    if evidence:
        lines.append(f"Evidence: {', '.join(evidence)}")
    angle = str(content_brief.get("angle") or "").strip()
    if angle:
        lines.append(f"Angle: {angle}")
    audience = str(content_brief.get("audience") or "").strip()
    if audience:
        lines.append(f"Audience: {audience}")
    return " | ".join(lines)


def _build_writer_prompt_request(
    *,
    article_plan: Dict[str, Any],
    phase3: Dict[str, Any],
    llm_model: str = "",
    max_tokens: int = 0,
    validation_feedback: Optional[List[str]] = None,
) -> Dict[str, Any]:
    content_brief_text = _format_content_brief_prompt_text(phase3.get("content_brief") or {})
    style_profile = phase3.get("style_profile") or {}
    specificity_profile = phase3.get("specificity_profile") or {}
    plan_payload = {
        "h1": article_plan.get("h1"),
        "intent_type": article_plan.get("intent_type") or phase3.get("search_intent_type"),
        "article_angle": article_plan.get("article_angle") or phase3.get("article_angle"),
        "topic_class": article_plan.get("topic_class") or phase3.get("topic_class"),
        "structured_mode": article_plan.get("structured_mode"),
        "keyword_guardrails": {
            "intro_exact_primary_required": True,
            "body_exact_primary_max": 1,
            "body_exact_secondary_max": 1,
            "faq_exact_secondary_allowed": False,
            "fazit_must_use_required_terms": True,
        },
        "style_profile": style_profile,
        "specificity_profile": specificity_profile,
        "sections": article_plan.get("sections"),
        "faq_questions": article_plan.get("faq_questions"),
    }
    system_prompt = (
        "Write a German (de-DE) SEO article for a fixed deterministic plan. "
        "The structure is owned by the application, so you must only fill the approved content slots. "
        "Do not add or remove sections. Do not add hyperlinks. Do not include H1/H2 wrappers inside section bodies. "
        "Do not repeat domain names, site slogans, navigation labels, or unrelated article titles as prose. "
        "Do not use stock openers such as 'Herzlich willkommen', 'Willkommen', or similar greeting filler. "
        "Return only the tagged slot format requested by the application."
    )
    slot_lines = [
        "[[INTRO_HTML]]",
        "<p>...</p>",
        "[[/INTRO_HTML]]",
    ]
    for section in article_plan.get("sections") or []:
        if str(section.get("kind") or "body").strip() == "faq":
            continue
        section_id = str(section.get("section_id") or "").strip()
        if not section_id:
            continue
        slot_lines.extend(
            [
                f"[[SECTION:{section_id}]]",
                "<p>...</p>",
                "[[/SECTION]]",
            ]
        )
    for index, _question in enumerate(article_plan.get("faq_questions") or [], start=1):
        slot_lines.extend(
            [
                f"[[FAQ_{index}]]",
                "<p>...</p>",
                f"[[/FAQ_{index}]]",
            ]
        )
    slot_lines.extend(
        [
            "[[EXCERPT]]",
            "Ein kurzer Auszug.",
            "[[/EXCERPT]]",
        ]
    )
    user_prompt = (
        f"Topic: {phase3.get('final_article_topic', '')}\n"
        f"Primary keyword: {phase3.get('primary_keyword', '')}\n"
        f"Secondary keywords: {phase3.get('secondary_keywords') or []}\n"
        f"Intent type: {phase3.get('search_intent_type', 'informational')}\n"
        f"Article angle: {phase3.get('article_angle', 'practical_guidance')}\n"
        f"Specificity minimum: {(specificity_profile or {}).get('min_specifics', 2)} concrete specifics in the body.\n"
        f"Editorial brief: {content_brief_text}\n"
        f"Plan:\n{json.dumps(plan_payload, ensure_ascii=False, sort_keys=True, indent=2)}\n\n"
        "Output format:\n"
        f"{chr(10).join(slot_lines)}\n"
        "Rules:\n"
        "- Return every slot exactly once using the same markers and section ids.\n"
        "- INTRO_HTML: exactly one opening paragraph, 80-120 words, include the primary keyword naturally.\n"
        "- For each SECTION block, return only body HTML with 1-2 substantial paragraphs and any required list/table.\n"
        "- For each section, answer its subquestion directly and naturally include its required_keywords and required_terms.\n"
        "- Keep one clear search intent and one article angle across the full article. Do not introduce adjacent but different intents.\n"
        "- After INTRO_HTML, avoid repeating the exact primary keyword across multiple sections; use natural variants instead.\n"
        "- Use each exact secondary keyword in at most one non-FAQ section, and do not force exact secondary keywords into FAQ answers.\n"
        "- Use concrete criteria, examples, risks, comparisons, process details, or next steps. Avoid generic filler.\n"
        "- Add at least the required amount of concrete specificity for the topic class and intent. Prefer norms, ranges, examples, use cases, process details, age groups, standards, or market/process facts where relevant.\n"
        "- Do not use greeting-style intros or stock openers such as 'Herzlich willkommen'.\n"
        "- Do not write advertorial copy, sales copy, or partner claims.\n"
        "- Never place brands in headings. A backlink sentence must read like a supporting resource, not a promotion.\n"
        "- The Fazit section body must be topic-specific, concrete, non-generic, and explicitly use at least one of its required_terms.\n"
        "- FAQ answers must answer the question directly without repeating the same keyword phrase across multiple answers.\n"
        "- Each FAQ_n block must answer FAQ question n directly, 35-55 words, with no links.\n"
        "- EXCERPT must be plain text, one sentence, max 160 characters.\n"
        "- Do not output JSON, markdown fences, explanations, or any text outside the requested markers.\n"
        "- Keep language strictly German (de-DE)."
    )
    if validation_feedback:
        user_prompt += f"\nPrevious validation issues to fix exactly: {validation_feedback}"

    return {
        "request_label": "phase5_writer_attempt_1" if not validation_feedback else "phase5_writer_retry",
        "model": llm_model,
        "max_tokens": max_tokens,
        "validation_feedback": list(validation_feedback or []),
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
    }


def _build_planner_prompt_trace_entry(
    *,
    phase3: Dict[str, Any],
    phase4: Dict[str, Any],
    planning_quality: Dict[str, Any],
    internal_link_candidates: Optional[List[str]] = None,
    attempt: int = 1,
) -> Dict[str, Any]:
    return {
        "attempt": attempt,
        "input_packet": {
            "topic": phase3.get("final_article_topic", ""),
            "primary_keyword": phase3.get("primary_keyword", ""),
            "secondary_keywords": phase3.get("secondary_keywords") or [],
            "intent_type": phase3.get("search_intent_type", ""),
            "article_angle": phase3.get("article_angle", ""),
            "topic_class": phase3.get("topic_class", ""),
            "style_profile": phase3.get("style_profile") or {},
            "specificity_profile": phase3.get("specificity_profile") or {},
            "title_package": phase3.get("title_package") or {},
            "content_brief": phase3.get("content_brief") or {},
            "faq_candidates": phase3.get("faq_candidates") or [],
            "internal_link_candidates": list(internal_link_candidates or [])[:8],
        },
        "plan": phase4,
        "planning_quality": planning_quality,
    }


def ensure_prompt_trace_in_creator_output(creator_output: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(creator_output, dict):
        return creator_output

    phase3 = creator_output.get("phase3") if isinstance(creator_output.get("phase3"), dict) else {}
    phase4 = creator_output.get("phase4") if isinstance(creator_output.get("phase4"), dict) else {}
    debug = creator_output.get("debug") if isinstance(creator_output.get("debug"), dict) else {}
    if not debug:
        debug = {}
        creator_output["debug"] = debug

    prompt_trace = debug.get("prompt_trace") if isinstance(debug.get("prompt_trace"), dict) else {}
    if not prompt_trace:
        prompt_trace = {
            "planner": {
                "mode": "deterministic",
                "attempts": [],
            },
            "writer_attempts": [],
        }
        debug["prompt_trace"] = prompt_trace

    planner_trace = prompt_trace.get("planner") if isinstance(prompt_trace.get("planner"), dict) else None
    if planner_trace is None:
        planner_trace = {"mode": "deterministic", "attempts": []}
        prompt_trace["planner"] = planner_trace
    planner_attempts = planner_trace.get("attempts") if isinstance(planner_trace.get("attempts"), list) else []
    if not planner_attempts and phase3 and phase4:
        planning_quality = debug.get("planning_quality") if isinstance(debug.get("planning_quality"), dict) else {}
        internal_linking = debug.get("internal_linking") if isinstance(debug.get("internal_linking"), dict) else {}
        planner_trace["attempts"] = [
            _build_planner_prompt_trace_entry(
                phase3=phase3,
                phase4=phase4,
                planning_quality=planning_quality,
                internal_link_candidates=internal_linking.get("candidates") or [],
                attempt=1,
            )
        ]

    writer_attempts = prompt_trace.get("writer_attempts") if isinstance(prompt_trace.get("writer_attempts"), list) else []
    if not writer_attempts and phase3 and phase4:
        prompt_trace["writer_attempts"] = [
            _build_writer_prompt_request(
                article_plan=phase4,
                phase3=phase3,
                llm_model="",
                max_tokens=0,
                validation_feedback=None,
            )
        ]

    return creator_output


def normalize_prompt_trace_payload(
    creator_output: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    working_payload = copy.deepcopy(creator_output) if isinstance(creator_output, dict) else {}
    normalized_payload = ensure_prompt_trace_in_creator_output(working_payload)
    debug = normalized_payload.get("debug") if isinstance(normalized_payload.get("debug"), dict) else {}
    prompt_trace = debug.get("prompt_trace") if isinstance(debug.get("prompt_trace"), dict) else {}
    planner_trace = prompt_trace.get("planner") if isinstance(prompt_trace.get("planner"), dict) else {}
    writer_prompt_trace = prompt_trace.get("writer_attempts") if isinstance(prompt_trace.get("writer_attempts"), list) else []
    return normalized_payload, planner_trace, writer_prompt_trace
