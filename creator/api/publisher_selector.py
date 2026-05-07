"""Pick the best-fit publishing site for a target backlink in one LLM call.

Replaces the old two-step "rank deterministically -> validate the top-1 with
Haiku -> hope it fits" flow that hard-failed when the deterministic top pick
had no editorial overlap with the target (the brillenhaus24 -> Klimaschutz
collision). The deterministic ranker still runs upstream as a *recall*
mechanism that produces a shortlist; this module is the *rerank* that
chooses among the shortlisted candidates.

The LLM sees all K candidates at once -- so it can pick the best one
RELATIVELY, even when no candidate has a perfect topical match. This is
much more stable than asking Haiku per-candidate "does this fit?", which
swings on threshold tuning and discards comparative information.

Output also carries a refined topic for the chosen publisher (eyewear ->
kids' eyewear if a family magazine wins). One Haiku call regardless of
candidate count -- ~$0.005 at K=8. Cheaper net than the old flow because
it replaces both the per-candidate fit gate AND the post-contract
reselection step.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .llm import LLMError, call_llm_json

logger = logging.getLogger("creator.publisher_selector")

DEFAULT_HAIKU_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_TIMEOUT_SECONDS = 45
DEFAULT_MAX_TOKENS = 1500
MAX_CANDIDATES_IN_PROMPT = 12  # hard cap; deterministic shortlist is usually 5-8


class PublisherSelectorError(RuntimeError):
    """Raised on input/infra errors. ``no_fit`` is NOT raised -- it's a verdict."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class CandidateRanking:
    site_id: str
    site_url: str
    fit_score: float  # 0.0 - 1.0
    rationale: str


@dataclass
class SelectionResult:
    best_site_id: str
    best_site_url: str
    refined_topic: str
    confidence: float  # 0.0 - 1.0; how strong the editorial fit is for the winner
    rationale: str
    no_fit: bool  # True when no candidate has genuine editorial overlap
    ranking: List[CandidateRanking] = field(default_factory=list)
    cost_usd: float = 0.0
    soft_passed: bool = False  # True if the LLM was unreachable and we fell back to candidate[0]


# ---- prompts --------------------------------------------------------------


_LANGUAGE_INSTRUCTIONS: Dict[str, Dict[str, str]] = {
    "de": {
        "system": (
            "Du bist ein erfahrener deutscher Content-Stratege fuer ein Gastartikel-"
            "Netzwerk. Aufgabe: Zu einem ZIEL-SITE (Backlink-Ziel) bewertest du eine "
            "Auswahl moeglicher PUBLISHER-SITES und entscheidest, welche Publisher-"
            "Site fuer einen Gastartikel mit Backlink zur Ziel-Site editorial am besten "
            "passt. Gleichzeitig schlaegst du fuer die gewaehlte Publisher-Site ein "
            "verfeinertes Article-Thema vor (z.B. 'Brillen' -> 'Kinderbrillen', wenn "
            "der Gewinner ein Familien-Magazin ist).\n\n"
            "Antworte AUSSCHLIESSLICH mit gueltigem JSON nach diesem Schema:\n"
            "{\n"
            "  \"ranking\": [\n"
            "    {\n"
            "      \"site_id\": <string>,\n"
            "      \"fit_score\": <float 0.0-1.0>,\n"
            "      \"rationale\": <string, max. 25 Woerter, warum dieser Publisher passt oder nicht>\n"
            "    }\n"
            "  ],\n"
            "  \"best_pick\": {\n"
            "    \"site_id\": <string, das beste Match -- leer wenn no_fit=true>,\n"
            "    \"refined_topic\": <string, KURZES SEO-Suchkeyword (1-4 Woerter, Kleinschreibung) -- KEIN Artikel-Titel>,\n"
            "    \"confidence\": <float 0.0-1.0>,\n"
            "    \"rationale\": <string, max. 30 Woerter>\n"
            "  },\n"
            "  \"no_fit\": <bool, true NUR wenn buchstaeblich kein Kandidat eine editorial Ueberschneidung hat>\n"
            "}\n\n"
            "Bewertungsregeln:\n"
            "- fit_score 0.9-1.0: perfekte Audience-Ueberschneidung, der Editor wuerde den Artikel sofort akzeptieren.\n"
            "- fit_score 0.6-0.8: gute Ueberschneidung mit angepasstem Thema (z.B. Brillen -> Kinderbrillen auf Familien-Site).\n"
            "- fit_score 0.4-0.6: schwache, aber moegliche Ueberschneidung (allgemeines Lifestyle-Magazin akzeptiert breit gefaecherte Themen).\n"
            "- fit_score < 0.4: keine echte Ueberschneidung (z.B. Adult-Brillen auf reiner Klimaschutz/Solar-Site).\n"
            "- Sortiere ranking ABSTEIGEND nach fit_score. best_pick.site_id MUSS dem Top-Eintrag entsprechen, wenn no_fit=false.\n"
            "- refined_topic ist ein SEO-Suchkeyword fuer DataForSEO -- KEIN Artikel-Titel.\n"
            "  Format: 1-4 Woerter, Kleinschreibung, KEINE Doppelpunkte, KEINE Fragezeichen,\n"
            "  KEINE Bindestriche zwischen Klauseln, KEINE Pipes.\n"
            "  RICHTIG: 'kinderbrillen', 'kinderbrillen kaufen', 'sehhilfen kinder', 'brille kind ratgeber'.\n"
            "  FALSCH: 'Augengesundheit und Sehhilfen: Wie die richtige Brille zu deinem Lifestyle passt',\n"
            "  'Was Eltern wissen muessen', 'Brille fuer Kinder - der grosse Ratgeber'.\n"
            "- no_fit=true ist die Ausnahme: setze es nur, wenn ALLE Kandidaten fit_score < 0.4 haben UND keine "
            "Audience-Bruecke konstruierbar ist. Ein 'Allgemein'-Magazin (Lifestyle/News) zaehlt fast immer als fit_score >= 0.5.\n"
        ),
        "user_template": (
            "ZIEL-SITE\n=========\n{target_summary}\n\n"
            "ZIEL-KEYWORD (Vorschlag, darf verfeinert werden): {target_keyword}\n\n"
            "PUBLISHER-KANDIDATEN ({n_candidates})\n"
            "===============================\n"
            "{candidates_block}\n\n"
            "Bewerte und liefere das JSON."
        ),
    },
    "fr": {
        "system": (
            "Vous etes un strategiste de contenu francophone pour un reseau d'articles "
            "invites. Tache : etant donne un SITE CIBLE (cible du backlink), evaluez "
            "une selection de SITES EDITEURS et decidez lequel convient le mieux "
            "editorialement pour un article invite avec un lien vers le site cible. "
            "Proposez egalement un sujet d'article affine pour l'editeur retenu "
            "(ex. 'lunettes' -> 'lunettes pour enfants' si le gagnant est un "
            "magazine famille).\n\n"
            "Repondez UNIQUEMENT avec un JSON valide selon ce schema :\n"
            "{\n"
            "  \"ranking\": [\n"
            "    {\n"
            "      \"site_id\": <string>,\n"
            "      \"fit_score\": <float 0.0-1.0>,\n"
            "      \"rationale\": <string, max. 25 mots>\n"
            "    }\n"
            "  ],\n"
            "  \"best_pick\": {\n"
            "    \"site_id\": <string -- vide si no_fit=true>,\n"
            "    \"refined_topic\": <string, COURT mot-cle de recherche SEO (1-4 mots, minuscules) -- PAS un titre d'article>,\n"
            "    \"confidence\": <float 0.0-1.0>,\n"
            "    \"rationale\": <string, max. 30 mots>\n"
            "  },\n"
            "  \"no_fit\": <bool>\n"
            "}\n\n"
            "Regles :\n"
            "- fit_score 0.9-1.0 : recoupement d'audience parfait.\n"
            "- fit_score 0.6-0.8 : bon recoupement avec sujet ajuste.\n"
            "- fit_score 0.4-0.6 : recoupement faible mais possible (magazine lifestyle generaliste).\n"
            "- fit_score < 0.4 : pas de recoupement editorial reel.\n"
            "- Triez ranking par fit_score decroissant. best_pick.site_id correspond au top quand no_fit=false.\n"
            "- refined_topic = mot-cle SEO pour DataForSEO -- PAS un titre.\n"
            "  Format : 1-4 mots, minuscules, AUCUN deux-points, AUCUN point d'interrogation, AUCUN tiret de clause, AUCUN pipe.\n"
            "  CORRECT : 'lunettes enfants', 'lunettes pour enfants', 'lunettes vue enfant'.\n"
            "  FAUX : 'Sante des yeux : comment choisir les bonnes lunettes', 'Ce que les parents doivent savoir'.\n"
            "- no_fit=true est l'exception ; un magazine generaliste compte presque toujours comme fit_score >= 0.5.\n"
        ),
        "user_template": (
            "SITE CIBLE\n==========\n{target_summary}\n\n"
            "MOT-CLE CIBLE (proposition, peut etre affinee) : {target_keyword}\n\n"
            "CANDIDATS EDITEURS ({n_candidates})\n"
            "===========================\n"
            "{candidates_block}\n\n"
            "Evaluez et renvoyez le JSON."
        ),
    },
}


# ---- profile summarisation -----------------------------------------------


def _coerce_string_list(value: Any, *, limit: int = 8) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    if isinstance(value, (list, tuple)):
        out: List[str] = []
        for item in value:
            if isinstance(item, str) and item.strip():
                out.append(item.strip())
            elif isinstance(item, dict):
                label = item.get("label") or item.get("topic") or item.get("name")
                if isinstance(label, str) and label.strip():
                    out.append(label.strip())
            if len(out) >= limit:
                break
        return out
    return []


def _summarise_target_profile(profile: Optional[Dict[str, Any]], target_url: str) -> str:
    """Compact summary of the target site so the LLM can reason about its
    business context. Falls through gracefully if profile is sparse.

    ``visible_headings`` (H1/H2/H3 scraped from the homepage and linked
    pages) is the highest-signal field for "what does this site actually
    do" -- much more reliable than meta tags, which are often empty or
    boilerplate. ``meta_description`` is included as a secondary signal.
    """

    lines: List[str] = [f"- url: {target_url}"]
    if not isinstance(profile, dict):
        lines.append("- profile: (not available)")
        return "\n".join(lines)

    fields_in_order: List[tuple[str, str]] = [
        ("domain_level_topic", "domain topic"),
        ("primary_context", "primary context"),
        ("page_title", "page title"),
        ("meta_description", "meta description"),
        ("visible_headings", "homepage headings"),
        ("topics", "topics"),
        ("topic_clusters", "topic clusters"),
        ("services_or_products", "services / products"),
        ("audience", "audience"),
        ("target_audience", "target audience"),
    ]
    for key, label in fields_in_order:
        raw = profile.get(key)
        if isinstance(raw, str) and raw.strip():
            lines.append(f"- {label}: {raw.strip()}")
            continue
        # visible_headings can be long -- bump the limit so the LLM sees
        # enough headings to identify the business clearly.
        limit = 12 if key == "visible_headings" else 8
        items = _coerce_string_list(raw, limit=limit)
        if items:
            lines.append(f"- {label}: {', '.join(items)}")
    if len(lines) == 1:
        lines.append("- profile: (empty)")
    return "\n".join(lines)


def _summarise_candidate(candidate: Dict[str, Any], index: int) -> str:
    """One block per candidate. ``site_id`` is verbatim from the input so the
    LLM can echo it back in ``ranking`` / ``best_pick``."""

    site_id = str(candidate.get("site_id") or f"candidate_{index}").strip()
    site_url = str(candidate.get("site_url") or "").strip()
    is_general = bool(candidate.get("is_general"))
    profile = candidate.get("publishing_profile_payload") or candidate.get("profile") or {}
    if not isinstance(profile, dict):
        profile = {}

    lines: List[str] = [f"[{index + 1}] site_id: {site_id}"]
    if site_url:
        lines.append(f"    site_url: {site_url}")
    if is_general:
        lines.append("    note: general-purpose / Allgemein publisher (broad lifestyle)")

    # ``visible_headings`` (H1/H2/H3 from the publisher's homepage + a few
    # linked pages) and ``prominent_titles`` (recent article titles from the
    # WP REST inventory) are the highest-signal fields for "what does this
    # publisher actually publish". A meta-tag-only profile fooled the
    # selector into picking generalist sites with empty meta as no-fit
    # filler -- once headings are surfaced, the LLM sees the actual
    # editorial mix.
    candidate_fields: List[tuple[str, str]] = [
        ("primary_context", "primary context"),
        ("topics", "topics"),
        ("topic_clusters", "topic clusters"),
        ("audience", "audience"),
        ("target_audience", "target audience"),
        ("editorial_terms", "editorial terms"),
        ("repeated_keywords", "repeated keywords"),
        ("visible_headings", "homepage headings"),
        ("prominent_titles", "recent article titles"),
        ("sample_page_titles", "sample page titles"),
    ]
    headings_keys = {"visible_headings", "prominent_titles"}
    for key, label in candidate_fields:
        raw = profile.get(key)
        if isinstance(raw, str) and raw.strip():
            lines.append(f"    - {label}: {raw.strip()}")
            continue
        # Headings/titles get a higher cap because identifying a publisher's
        # editorial mix needs more than a handful of samples.
        limit = 12 if key in headings_keys else 6
        items = _coerce_string_list(raw, limit=limit)
        if items:
            lines.append(f"    - {label}: {', '.join(items)}")
    return "\n".join(lines)


# ---- response parsing -----------------------------------------------------


def _clamp_unit(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return 0.0


def _parse_ranking(payload: Any, candidates: List[Dict[str, Any]]) -> List[CandidateRanking]:
    if not isinstance(payload, list):
        return []
    by_id: Dict[str, Dict[str, Any]] = {
        str(c.get("site_id") or "").strip(): c for c in candidates if c.get("site_id")
    }
    out: List[CandidateRanking] = []
    seen: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        raw_id = str(item.get("site_id") or "").strip()
        if not raw_id or raw_id in seen:
            continue
        seen.add(raw_id)
        candidate = by_id.get(raw_id)
        site_url = str((candidate or {}).get("site_url") or "").strip()
        out.append(
            CandidateRanking(
                site_id=raw_id,
                site_url=site_url,
                fit_score=_clamp_unit(item.get("fit_score")),
                rationale=str(item.get("rationale") or "").strip(),
            )
        )
    return out


def _resolve_best_pick(
    *,
    payload: Dict[str, Any],
    ranking: List[CandidateRanking],
    candidates: List[Dict[str, Any]],
    fallback_topic: str,
) -> tuple[str, str, str, float, str, bool]:
    """Resolve (site_id, site_url, refined_topic, confidence, rationale, no_fit).

    Rules:
    - If ``no_fit`` is true OR ranking is empty OR best_pick is missing, surface
      the no-fit verdict and let the caller decide on the Allgemein fallback.
    - Otherwise: trust the LLM's best_pick when its site_id is in the ranking,
      else fall back to the top-ranked entry by fit_score.
    """

    no_fit = bool(payload.get("no_fit"))
    best_pick = payload.get("best_pick") if isinstance(payload.get("best_pick"), dict) else {}

    if no_fit or not ranking:
        rationale = str(best_pick.get("rationale") or "").strip() or "no editorial overlap with any candidate"
        return "", "", fallback_topic, 0.0, rationale, True

    refined_topic = str(best_pick.get("refined_topic") or "").strip() or fallback_topic
    confidence = _clamp_unit(best_pick.get("confidence"))
    rationale = str(best_pick.get("rationale") or "").strip()

    pick_id = str(best_pick.get("site_id") or "").strip()
    chosen: Optional[CandidateRanking] = None
    if pick_id:
        chosen = next((r for r in ranking if r.site_id == pick_id), None)
    if chosen is None:
        chosen = max(ranking, key=lambda r: r.fit_score)
        rationale = rationale or chosen.rationale

    if confidence <= 0.0:
        confidence = chosen.fit_score
    if not rationale:
        rationale = chosen.rationale

    site_url = chosen.site_url
    if not site_url:
        # Last-resort lookup against the input candidates by id.
        match = next(
            (c for c in candidates if str(c.get("site_id") or "").strip() == chosen.site_id),
            None,
        )
        site_url = str((match or {}).get("site_url") or "").strip()

    return chosen.site_id, site_url, refined_topic, confidence, rationale, False


# ---- public API -----------------------------------------------------------


def select_best_publisher(
    *,
    target_url: str,
    target_keyword: str,
    candidates: List[Dict[str, Any]],
    target_profile: Optional[Dict[str, Any]] = None,
    language: str = "de",
    api_key: Optional[str] = None,
    model: str = DEFAULT_HAIKU_MODEL,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> SelectionResult:
    """Pick the best publishing-site candidate for a target backlink.

    ``candidates`` is the deterministic shortlist (already top-K from the
    portal's site-fit ranker). Each candidate must carry ``site_id``,
    ``site_url``, and ``publishing_profile_payload``. The function tolerates
    missing profile fields but a fully-empty candidate list raises.

    Behaviour on infra failure (no API key / LLM error / timeout):
    soft-pass with ``soft_passed=True`` and the first candidate as the
    winner, confidence 0.5. Caller should treat this as a degraded but
    non-blocking outcome -- selection still happens, just deterministically.
    """

    cleaned_keyword = (target_keyword or "").strip()
    if not cleaned_keyword:
        raise PublisherSelectorError(
            "missing_keyword",
            "target_keyword is required for publisher selection.",
        )
    if not candidates:
        raise PublisherSelectorError(
            "missing_candidates",
            "At least one publishing-site candidate is required.",
        )

    trimmed_candidates = candidates[:MAX_CANDIDATES_IN_PROMPT]

    lang = (language or "de").lower()
    instructions = _LANGUAGE_INSTRUCTIONS.get(lang, _LANGUAGE_INSTRUCTIONS["de"])

    target_summary = _summarise_target_profile(target_profile, target_url)
    candidates_block = "\n\n".join(
        _summarise_candidate(c, index=i) for i, c in enumerate(trimmed_candidates)
    )
    user_prompt = instructions["user_template"].format(
        target_summary=target_summary,
        target_keyword=cleaned_keyword,
        n_candidates=len(trimmed_candidates),
        candidates_block=candidates_block,
    )

    resolved_api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not resolved_api_key:
        return _soft_pass(trimmed_candidates, cleaned_keyword, "anthropic_api_key_not_configured")

    try:
        payload = call_llm_json(
            system_prompt=instructions["system"],
            user_prompt=user_prompt,
            api_key=resolved_api_key,
            base_url=base_url,
            model=model,
            timeout_seconds=timeout_seconds,
            max_tokens=DEFAULT_MAX_TOKENS,
            temperature=0.1,
            request_label="publisher_selector",
        )
    except LLMError as exc:
        logger.warning("publisher_selector.llm_failed err=%s", exc)
        return _soft_pass(trimmed_candidates, cleaned_keyword, f"llm_unavailable:{str(exc)[:80]}")

    ranking = _parse_ranking(payload.get("ranking"), trimmed_candidates)
    site_id, site_url, refined_topic, confidence, rationale, no_fit = _resolve_best_pick(
        payload=payload,
        ranking=ranking,
        candidates=trimmed_candidates,
        fallback_topic=cleaned_keyword,
    )

    cost = 0.005  # rough Haiku upper bound at K=8 with this prompt size
    return SelectionResult(
        best_site_id=site_id,
        best_site_url=site_url,
        refined_topic=refined_topic,
        confidence=confidence,
        rationale=rationale,
        no_fit=no_fit,
        ranking=ranking,
        cost_usd=cost,
        soft_passed=False,
    )


def _soft_pass(
    candidates: List[Dict[str, Any]],
    target_keyword: str,
    rationale: str,
) -> SelectionResult:
    """Degraded path: LLM unavailable. Pick the deterministic top candidate."""

    top = candidates[0]
    site_id = str(top.get("site_id") or "").strip()
    site_url = str(top.get("site_url") or "").strip()
    return SelectionResult(
        best_site_id=site_id,
        best_site_url=site_url,
        refined_topic=target_keyword,
        confidence=0.5,
        rationale=rationale,
        no_fit=False,
        ranking=[
            CandidateRanking(
                site_id=site_id,
                site_url=site_url,
                fit_score=0.5,
                rationale=rationale,
            )
        ],
        cost_usd=0.0,
        soft_passed=True,
    )
