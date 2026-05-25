"""Write one ranked listicle item via Sonnet 4.6.

Sibling of ``section_writer.py`` for ``ContentContract.format=LISTICLE``. Each
item is one independent Sonnet call; ``write_all_items`` runs them in parallel
via ThreadPoolExecutor with ``cache_system=True`` so only the first item in a
batch pays the full system-prompt input-token price.

Output is a per-item ``ItemDraft`` with HTML body + reported inserted links +
word count. The article assembler stitches the intro section + ranked items +
outro section + FAQ together.
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, Field

from .contract import ContentContract, EntityRequirement, LinkTarget, ListiclePlan, ServiceType
from .llm import LLMError, call_llm_json
from .prompt_registry import load as load_prompt
from .section_writer import InsertedLink

logger = logging.getLogger("creator.listicle_writer")

DEFAULT_MODEL = "claude-sonnet-4-6"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_MAX_TOKENS = 4000
DEFAULT_TEMPERATURE = 0.4
DEFAULT_PARALLEL_WORKERS = 4
DEFAULT_RETRIES = 4
PROMPT_NAME = "listicle_writer"


class ItemDraft(BaseModel):
    rank: int
    name: str
    body_html: str
    links_inserted: List[InsertedLink] = Field(default_factory=list)
    word_count: int = 0


def _entities_for_item(contract: ContentContract, rank: int) -> List[EntityRequirement]:
    needle = f"item {rank}"
    return [
        entity
        for entity in contract.required_entities
        if entity.placement_hint and needle in entity.placement_hint.lower()
    ]


def _link_for_item(contract: ContentContract, rank: int) -> Optional[LinkTarget]:
    # Listicle backlink targets reuse ``LinkTarget.section_index`` to mean rank.
    for link in contract.link_plan:
        if link.section_index == rank:
            return link
    return None


def _format_entities(entities: List[EntityRequirement], language: str) -> str:
    if not entities:
        return "(no required entities for this item)" if language == "fr" else "(keine Pflicht-Entitäten für diesen Eintrag)"
    return "\n".join(f"  - {entity.name}" for entity in entities)


def _format_link(link: Optional[LinkTarget], target_keyword: str, language: str) -> str:
    if link is None:
        return "(pas de backlink dans cet item)" if language == "fr" else "(kein Backlink in diesem Eintrag)"
    return (
        f"  Ziel-URL: {link.target_url}\n"
        f"  Link-Typ: {link.link_type}\n"
        f"  Anker-Strategie: {link.anchor_strategy}\n"
        f"  Vertragliches Keyword: {target_keyword}\n"
        f"  Kontext-Anforderung: {link.surrounding_context_requirements}"
    )


def _format_service_directive(contract: ContentContract) -> str:
    is_fr = contract.language.value == "fr"
    if contract.service_type == ServiceType.BRAND_MENTION:
        brand = (contract.brand_name or "").strip()
        if is_fr:
            return (
                "MODE MENTION DE MARQUE : n'insérez AUCUN lien (`<a href>`) ni URL dans cet item. "
                + (f"Si la marque « {brand} » figure dans les entités obligatoires, citez-la en texte brut, naturellement." if brand else "")
            )
        return (
            "SERVICE-MODUS MARKENERWÄHNUNG: Füge in diesem Eintrag KEINEN Link (`<a href>`) und KEINE URL ein. "
            + (f"Wenn die Marke „{brand}\" unter den Pflicht-Entitäten steht, erwähne sie als Klartext, natürlich im Satz." if brand else "")
        )
    if is_fr:
        return (
            "MODE ARTICLE : ne nommez JAMAIS le site cible (marque, domaine, nom d'entreprise) — ni dans le texte, "
            "ni dans le texte d'ancre. Le backlink reste dissimulé derrière une ancre contextuelle/mot-clé."
        )
    return (
        "SERVICE-MODUS ARTIKEL: Nenne die Ziel-Website NIEMALS offen (kein Markenname, keine Domain, kein Firmenname) "
        "— weder im Fließtext noch im Anker-Text. Der Backlink bleibt hinter einem kontextuellen/Keyword-Anker verborgen."
    )


def _format_peer_items(plan: "ListiclePlan", rank: int, language: str) -> str:
    """Render the full peer-item list so the writer can match parallel form
    and avoid overlapping with content that belongs to other items."""

    if not plan.items:
        return "(keine Peer-Items übergeben)" if language != "fr" else "(aucun peer-item fourni)"
    lines: List[str] = []
    for index, item in enumerate(plan.items, start=1):
        marker = " ← DIESES ITEM" if index == rank and language != "fr" else (" ← CET ITEM" if index == rank else "")
        lines.append(f"  {index}. {item}{marker}")
    return "\n".join(lines)


def build_user_prompt(*, contract: ContentContract, rank: int) -> str:
    plan = contract.listicle_plan
    if plan is None:
        raise ValueError("ContentContract.listicle_plan is required for listicle writers.")
    if not 1 <= rank <= plan.item_count:
        raise IndexError(f"rank {rank} out of range for {plan.item_count}-item listicle")
    name = plan.items[rank - 1] if plan.items and len(plan.items) >= rank else f"Eintrag {rank}"
    language = contract.language.value
    entities = _entities_for_item(contract, rank)
    link = _link_for_item(contract, rank)
    template_str = ", ".join(plan.item_template) or "name, hook, pros, cons, verdict"
    avg_words = max(120, contract.word_count_target // max(1, plan.item_count + 2))
    peer_block = _format_peer_items(plan, rank, language)
    if language == "fr":
        return (
            f"CONTEXTE GLOBAL DU CONTRAT\n"
            f"==========================\n"
            f"target_keyword : {contract.target_keyword}\n"
            f"intent         : {contract.intent.value}\n"
            f"audience       : {contract.target_audience}\n"
            f"format         : listicle ({plan.item_count} items, basis={plan.ranking_basis})\n\n"
            f"CET ITEM (rang {rank} sur {plan.item_count})\n"
            f"================================================\n"
            f"Énoncé de l'item : {name}\n"
            f"Mots cible       : {avg_words} mots\n"
            f"Champs obligatoires : {template_str}\n\n"
            f"TOUS LES PEER-ITEMS DU LISTICLE (pour parallélisme + éviter recoupements)\n"
            f"========================================================================\n"
            f"{peer_block}\n\n"
            f"ENTITÉS OBLIGATOIRES pour cet item\n"
            f"==================================\n"
            f"{_format_entities(entities, language)}\n\n"
            f"BACKLINK / MODE DE SERVICE\n"
            f"==========================\n"
            f"{_format_service_directive(contract)}\n"
            f"{_format_link(link, contract.target_keyword, language)}\n\n"
            f"LISTE NOIRE IA SPÉCIFIQUE AU CONTRAT\n"
            f"====================================\n"
            + ("\n".join(f"  - {phrase}" for phrase in contract.ai_tell_blocklist) or "  (vide)")
            + "\n\nRédigez maintenant cet item au format JSON selon le schéma du system prompt."
        )
    return (
        f"GLOBALER VERTRAG-KONTEXT\n"
        f"========================\n"
        f"target_keyword : {contract.target_keyword}\n"
        f"intent         : {contract.intent.value}\n"
        f"audience       : {contract.target_audience}\n"
        f"format         : listicle ({plan.item_count} ranked items, basis={plan.ranking_basis})\n\n"
        f"DIESER EINTRAG (Rang {rank} von {plan.item_count})\n"
        f"==================================================\n"
        f"Item-Statement : {name}\n"
        f"Ziel-Wortzahl  : {avg_words} Wörter\n"
        f"Pflicht-Felder : {template_str}\n\n"
        f"ALLE PEER-ITEMS DES LISTICLES (für Parallelität + Vermeidung von Überlappungen)\n"
        f"==============================================================================\n"
        f"{peer_block}\n\n"
        f"PFLICHT-ENTITÄTEN für diesen Eintrag\n"
        f"========================================\n"
        f"{_format_entities(entities, language)}\n\n"
        f"BACKLINK / SERVICE-MODUS\n"
        f"========================\n"
        f"{_format_service_directive(contract)}\n"
        f"{_format_link(link, contract.target_keyword, language)}\n\n"
        f"VERTRAGSWEITE AI-FLOSKEL-BLOCKLISTE\n"
        f"===================================\n"
        + ("\n".join(f"  - {phrase}" for phrase in contract.ai_tell_blocklist) or "  (leer)")
        + "\n\nSchreibe JETZT diesen Listen-Eintrag als JSON gemäß dem System-Schema."
    )


def write_item(
    *,
    contract: ContentContract,
    rank: int,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
    prompt_version: Optional[str] = None,
    llm_caller: Optional[Callable[..., Dict[str, Any]]] = None,
) -> ItemDraft:
    plan: Optional[ListiclePlan] = contract.listicle_plan
    if plan is None:
        raise ValueError("ContentContract.listicle_plan is required for listicle writers.")

    resolved_api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not resolved_api_key and llm_caller is None:
        raise LLMError("Missing ANTHROPIC_API_KEY for listicle writer.")

    resolved_model = model or os.getenv("CREATOR_LISTICLE_MODEL", "").strip() or DEFAULT_MODEL
    prompt = load_prompt(PROMPT_NAME, prompt_version, language=contract.language.value)
    system_prompt = prompt.body
    user_prompt = build_user_prompt(contract=contract, rank=rank)

    if llm_caller is None:
        def _default(**kwargs):
            return call_llm_json(**kwargs)
        llm_caller = _default

    payload = llm_caller(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=resolved_api_key,
        base_url=base_url,
        model=resolved_model,
        timeout_seconds=timeout_seconds,
        max_tokens=max_tokens,
        temperature=temperature,
        request_label=f"listicle_writer/{prompt.version}/item_{rank}",
        cache_system=True,
        retries=DEFAULT_RETRIES,
    )

    if not isinstance(payload, dict):
        raise LLMError(f"Listicle writer returned non-dict for item {rank}.")

    # The LLM is told to emit name + body_html; we trust the contract's
    # canonical ranked-item name when present (ranking_basis=score) so the
    # assembler can render <h2>{rank}. {name}</h2> consistently.
    canonical_name = plan.items[rank - 1] if plan.items and len(plan.items) >= rank else None
    payload_with_meta = {
        **payload,
        "rank": rank,
        "name": canonical_name or payload.get("name") or f"Eintrag {rank}",
    }
    try:
        return ItemDraft.model_validate(payload_with_meta)
    except Exception as exc:
        raise LLMError(f"Listicle item {rank} failed schema validation: {exc}") from exc


def write_all_items(
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
) -> List[ItemDraft]:
    plan: Optional[ListiclePlan] = contract.listicle_plan
    if plan is None:
        raise ValueError("ContentContract.listicle_plan is required for listicle writers.")
    ranks = list(range(1, plan.item_count + 1))
    drafts: Dict[int, ItemDraft] = {}

    def _one(rank: int) -> ItemDraft:
        return write_item(
            contract=contract,
            rank=rank,
            api_key=api_key,
            model=model,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            max_tokens=max_tokens,
            temperature=temperature,
            prompt_version=prompt_version,
            llm_caller=llm_caller,
        )

    if not parallel or len(ranks) <= 1:
        for r in ranks:
            drafts[r] = _one(r)
    else:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(ranks))) as executor:
            future_to_rank = {executor.submit(_one, r): r for r in ranks}
            for future in as_completed(future_to_rank):
                rank = future_to_rank[future]
                drafts[rank] = future.result()

    return [drafts[r] for r in ranks]
