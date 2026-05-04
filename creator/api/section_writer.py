"""Write the body of one ContentContract H2 section via Sonnet 4.6.

Each section is one independent Sonnet call. ``write_all_sections`` runs the
calls in parallel via ThreadPoolExecutor — for a 4–6 section article this
cuts wall-clock time from ~90s to ~25s on a typical run while staying well
under Anthropic Tier 2 rate limits.

Output is a per-section ``SectionDraft`` with HTML body + reported inserted
links + word count. The article assembler (Phase 4b) stitches drafts together
into the final article.
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, Field

from .contract import ContentContract, EntityRequirement, LinkTarget, SectionPlan
from .llm import LLMError, call_llm_json
from .prompt_registry import load as load_prompt

logger = logging.getLogger("creator.section_writer")

DEFAULT_SECTION_MODEL = "claude-sonnet-4-6"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_TIMEOUT_SECONDS = 90
DEFAULT_MAX_TOKENS = 4000
DEFAULT_TEMPERATURE = 0.4
DEFAULT_PARALLEL_WORKERS = 4
PROMPT_NAME = "section_writer"


class InsertedLink(BaseModel):
    anchor_text: str
    target_url: str
    link_type: str = Field(default="backlink", description="backlink | internal | external_reference")


class SectionDraft(BaseModel):
    section_index: int
    h2: str
    body_html: str
    links_inserted: List[InsertedLink] = Field(default_factory=list)
    word_count: int = 0


# ---- prompt assembly --------------------------------------------------------


def _entities_for_section(contract: ContentContract, section_index: int) -> List[EntityRequirement]:
    needle = f"section {section_index}"
    return [
        entity
        for entity in contract.required_entities
        if entity.placement_hint and needle in entity.placement_hint.lower()
    ]


def _link_for_section(contract: ContentContract, section_index: int) -> Optional[LinkTarget]:
    for link in contract.link_plan:
        if link.section_index == section_index:
            return link
    return None


def _format_entities(entities: List[EntityRequirement]) -> str:
    if not entities:
        return "(keine Pflicht-Entitäten für diesen Abschnitt)"
    return "\n".join(f"  - {entity.name}" for entity in entities)


def _format_link(link: Optional[LinkTarget], target_keyword: str) -> str:
    if link is None:
        return "(kein Backlink in diesem Abschnitt)"
    return (
        f"  Ziel-URL: {link.target_url}\n"
        f"  Link-Typ: {link.link_type}\n"
        f"  Anker-Strategie: {link.anchor_strategy}\n"
        f"  Vertragliches Keyword (für exact/partial Anker): {target_keyword}\n"
        f"  Kontext-Anforderung: {link.surrounding_context_requirements}"
    )


def _format_required_elements(elements: List[str]) -> str:
    if not elements:
        return "(keine zwingenden Strukturelemente)"
    return ", ".join(elements)


def _format_subheadings(subheadings: List[str]) -> str:
    if not subheadings:
        return "(frei wählbar)"
    return "\n".join(f"  - {h}" for h in subheadings)


def build_user_prompt(
    *,
    contract: ContentContract,
    section_index: int,
) -> str:
    if not 0 <= section_index < len(contract.sections):
        raise IndexError(f"section_index {section_index} out of range for contract with {len(contract.sections)} sections")
    section: SectionPlan = contract.sections[section_index]
    prev_h2 = contract.sections[section_index - 1].h2 if section_index > 0 else "(Einleitung des Artikels)"
    next_h2 = (
        contract.sections[section_index + 1].h2
        if section_index < len(contract.sections) - 1
        else "(FAQ-Block / Fazit am Ende)"
    )
    entities = _entities_for_section(contract, section_index)
    link = _link_for_section(contract, section_index)
    return (
        f"GLOBALER VERTRAG-KONTEXT\n"
        f"========================\n"
        f"target_keyword : {contract.target_keyword}\n"
        f"intent         : {contract.intent.value}\n"
        f"audience       : {contract.target_audience}\n"
        f"tone           : Sie\n\n"
        f"DIESER ABSCHNITT (Index {section_index} von {len(contract.sections)})\n"
        f"==================================================\n"
        f"H2             : {section.h2}\n"
        f"Mandat         : {section.mandate}\n"
        f"Ziel-Wortzahl  : {section.target_word_count}\n"
        f"Pflicht-Subheadings:\n{_format_subheadings(section.required_subheadings)}\n"
        f"Pflicht-Elemente: {_format_required_elements(section.required_elements)}\n\n"
        f"BENACHBARTE ABSCHNITTE (Kohärenz)\n"
        f"=================================\n"
        f"Vorheriger H2  : {prev_h2}\n"
        f"Nächster H2    : {next_h2}\n\n"
        f"PFLICHT-ENTITÄTEN für diesen Abschnitt\n"
        f"========================================\n"
        f"{_format_entities(entities)}\n\n"
        f"BACKLINK\n"
        f"========\n"
        f"{_format_link(link, contract.target_keyword)}\n\n"
        f"VERTRAGSWEITE AI-FLOSKEL-BLOCKLISTE (zusätzlich zur System-Prompt-Liste)\n"
        f"========================================================================\n"
        + ("\n".join(f"  - {phrase}" for phrase in contract.ai_tell_blocklist) or "  (leer)")
        + f"\n\n"
        f"Schreibe JETZT den Abschnitt als JSON gemäß dem System-Schema."
    )


# ---- single section call ---------------------------------------------------


def write_section(
    *,
    contract: ContentContract,
    section_index: int,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
    prompt_version: Optional[str] = None,
    llm_caller: Optional[Callable[..., Dict[str, Any]]] = None,
) -> SectionDraft:
    resolved_api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not resolved_api_key and llm_caller is None:
        raise LLMError("Missing ANTHROPIC_API_KEY for section writer.")

    resolved_model = model or os.getenv("CREATOR_SECTION_MODEL", "").strip() or DEFAULT_SECTION_MODEL
    prompt = load_prompt(PROMPT_NAME, prompt_version)
    system_prompt = prompt.body
    user_prompt = build_user_prompt(contract=contract, section_index=section_index)

    if llm_caller is None:
        def _default(**kwargs):
            return call_llm_json(**kwargs)
        llm_caller = _default

    try:
        payload = llm_caller(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            api_key=resolved_api_key,
            base_url=base_url,
            model=resolved_model,
            timeout_seconds=timeout_seconds,
            max_tokens=max_tokens,
            temperature=temperature,
            request_label=f"section_writer/{prompt.version}/section_{section_index}",
            # Sections share a long stable system prompt; ephemeral cache means
            # only the first section in a batch pays full input-token price.
            cache_system=True,
        )
    except LLMError:
        raise

    if not isinstance(payload, dict):
        raise LLMError(f"Section writer returned non-dict for section {section_index}.")

    section_h2 = contract.sections[section_index].h2
    payload_with_meta = {**payload, "section_index": section_index, "h2": section_h2}
    try:
        return SectionDraft.model_validate(payload_with_meta)
    except Exception as exc:
        raise LLMError(f"Section {section_index} draft failed schema validation: {exc}") from exc


# ---- parallel orchestrator -------------------------------------------------


def write_all_sections(
    *,
    contract: ContentContract,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
    prompt_version: Optional[str] = None,
    parallel: bool = True,
    max_workers: int = DEFAULT_PARALLEL_WORKERS,
    llm_caller: Optional[Callable[..., Dict[str, Any]]] = None,
) -> List[SectionDraft]:
    """Write every section of the contract; raise on any failure."""

    indices = list(range(len(contract.sections)))
    drafts: Dict[int, SectionDraft] = {}

    def _one(section_index: int) -> SectionDraft:
        return write_section(
            contract=contract,
            section_index=section_index,
            api_key=api_key,
            model=model,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            max_tokens=max_tokens,
            temperature=temperature,
            prompt_version=prompt_version,
            llm_caller=llm_caller,
        )

    if not parallel or len(indices) <= 1:
        for i in indices:
            drafts[i] = _one(i)
    else:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(indices))) as executor:
            future_to_index = {executor.submit(_one, i): i for i in indices}
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                drafts[index] = future.result()

    return [drafts[i] for i in indices]
