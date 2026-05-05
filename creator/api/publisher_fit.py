"""Validate or refine a target keyword so it fits the publishing site's audience.

Phase D follow-up: when a webhook comes in with target_site=brillenhaus24.de
(eyewear) and publishing_site=kidsblatt.de (kids/family magazine), the
auto-derived topic is "günstige brillen online kaufen" -- which is a
perfectly good search keyword for the target site, but a terrible
editorial fit for a kids magazine. The article ends up advertising
adult eyewear on a parenting publisher.

This module finds the *intersection*: a keyword that's relevant to
BOTH the target site's offering AND the publisher's audience (e.g.
"kinderbrillen online kaufen"). If no genuine overlap exists, it
hard-fails so the admin gets a clear "these two sites don't fit; pick
a different publisher" error.

One Haiku 4.5 call per validation -- ~$0.001. Soft-skipped when the
publisher profile is empty (no signal to validate against).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .llm import LLMError, call_llm_json

logger = logging.getLogger("creator.publisher_fit")

DEFAULT_HAIKU_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_TIMEOUT_SECONDS = 30


class PublisherFitError(RuntimeError):
    """Raised when no editorial intersection exists between target + publisher.

    Carries a stable ``code`` so the portal can render a clean admin error.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class FitVerdict:
    refined_keyword: str  # the keyword to use (may equal the input)
    original_keyword: str
    changed: bool
    confidence: float  # 0.0 - 1.0; how strong the editorial fit is
    rationale: str
    cost_usd: float = 0.0


_LANGUAGE_INSTRUCTIONS: Dict[str, Dict[str, str]] = {
    "de": {
        "system": (
            "Du bist ein erfahrener deutscher Content-Stratege. Deine Aufgabe: "
            "prüfe, ob ein Such-Keyword zu der redaktionellen Linie einer "
            "Publisher-Website passt, und schlage bei Bedarf eine Verfeinerung "
            "des Keywords vor, die für BEIDE Seiten relevant ist (Ziel-Seite "
            "UND Publisher-Audience).\n\n"
            "Antworte AUSSCHLIESSLICH mit gültigem JSON nach diesem Schema:\n"
            "{\n"
            "  \"verdict\": \"fits\" | \"refine\" | \"no_fit\",\n"
            "  \"refined_keyword\": <string>,  // bei \"refine\": das verfeinerte Keyword; bei \"fits\": das ursprüngliche; bei \"no_fit\": leer\n"
            "  \"confidence\": <float 0.0-1.0>,\n"
            "  \"rationale\": <string, max. 25 Wörter>\n"
            "}\n\n"
            "Regeln:\n"
            "- \"fits\": ursprüngliches Keyword passt direkt — keine Anpassung nötig.\n"
            "- \"refine\": es gibt eine echte Überschneidung, aber das Keyword muss "
            "auf die Publisher-Audience zugeschnitten werden (z.B. Brillen → Kinderbrillen, "
            "wenn Publisher = Kindermagazin).\n"
            "- \"no_fit\": keine echte redaktionelle Überschneidung möglich (z.B. Glücksspiel-"
            "Inhalte auf einer Kinderseite, Adult-Produkte auf einer Familienseite).\n"
            "- Erfundene Verfeinerungen, die zwar grammatikalisch passen, aber für die "
            "Ziel-Seite NICHT relevant sind, gelten als \"no_fit\".\n"
            "- Confidence: 1.0 = perfekte Überschneidung; 0.6 = ausreichend für einen "
            "Gastartikel; <0.4 = zu schwach.\n"
        ),
        "user_template": (
            "ZIEL-KEYWORD (vom Backlink-Ziel abgeleitet): {keyword}\n\n"
            "PUBLISHER-PROFIL\n"
            "================\n"
            "{publisher_summary}\n\n"
            "Bewerte den Fit und gib JSON zurück."
        ),
    },
    "fr": {
        "system": (
            "Vous êtes un stratège de contenu francophone expérimenté. Votre tâche : "
            "vérifier qu'un mot-clé de recherche convient à la ligne éditoriale d'un "
            "site éditeur, et au besoin proposer une variante affinée du mot-clé "
            "pertinente à la fois pour le site cible ET pour l'audience de l'éditeur.\n\n"
            "Répondez UNIQUEMENT avec un JSON valide selon ce schéma :\n"
            "{\n"
            "  \"verdict\": \"fits\" | \"refine\" | \"no_fit\",\n"
            "  \"refined_keyword\": <string>,\n"
            "  \"confidence\": <float 0.0-1.0>,\n"
            "  \"rationale\": <string, max. 25 mots>\n"
            "}\n\n"
            "Règles :\n"
            "- \"fits\" : le mot-clé d'origine convient directement — aucune adaptation.\n"
            "- \"refine\" : il existe un véritable recoupement, mais le mot-clé doit être "
            "ajusté à l'audience de l'éditeur (ex. lunettes → lunettes pour enfants, si "
            "l'éditeur est un magazine familial).\n"
            "- \"no_fit\" : aucun recoupement éditorial possible (ex. contenus de jeu "
            "d'argent sur un site enfants, produits adultes sur un site famille).\n"
            "- Les affinements inventés grammaticalement corrects mais NON pertinents "
            "pour le site cible comptent comme \"no_fit\".\n"
            "- Confidence : 1.0 = parfait ; 0.6 = suffisant pour un article invité ; "
            "<0.4 = trop faible.\n"
        ),
        "user_template": (
            "MOT-CLÉ CIBLE (dérivé de la cible du backlink) : {keyword}\n\n"
            "PROFIL ÉDITEUR\n"
            "==============\n"
            "{publisher_summary}\n\n"
            "Évaluez le fit et renvoyez le JSON."
        ),
    },
}


def _summarise_publisher_profile(profile: Dict[str, Any]) -> str:
    """Compact, language-neutral summary of the publisher's profile payload.

    Pulls the high-signal fields the JSONB blob carries and skips empty ones
    so the LLM sees a focused snapshot rather than a wall of nulls.
    """

    if not isinstance(profile, dict):
        return "(no publisher profile available)"

    fields_in_order: List[tuple[str, str]] = [
        ("primary_context", "primary context"),
        ("secondary_contexts", "secondary contexts"),
        ("topics", "topics"),
        ("topic_clusters", "topic clusters"),
        ("audience", "audience"),
        ("target_audience", "target audience"),
        ("editorial_terms", "editorial terms"),
        ("taxonomy_terms", "taxonomy terms"),
        ("repeated_keywords", "repeated keywords"),
        ("language", "language"),
    ]

    def _fmt_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (list, tuple)):
            parts: List[str] = []
            for item in value:
                if isinstance(item, str) and item.strip():
                    parts.append(item.strip())
                elif isinstance(item, dict):
                    label = item.get("label") or item.get("topic") or item.get("name")
                    if isinstance(label, str) and label.strip():
                        parts.append(label.strip())
            return ", ".join(parts[:8])
        return str(value).strip()

    lines: List[str] = []
    for key, label in fields_in_order:
        formatted = _fmt_value(profile.get(key))
        if formatted:
            lines.append(f"- {label}: {formatted}")
    if not lines:
        return "(publisher profile is empty)"
    return "\n".join(lines)


def _parse_verdict(payload: Dict[str, Any], original_keyword: str) -> tuple[str, str, float, str]:
    """Returns (verdict, refined_keyword, confidence, rationale)."""

    verdict = str(payload.get("verdict") or "").strip().lower()
    if verdict not in {"fits", "refine", "no_fit"}:
        verdict = "fits"  # safest default if the LLM mangles the schema
    raw_refined = str(payload.get("refined_keyword") or "").strip().lower()
    if verdict == "fits" or not raw_refined:
        refined = original_keyword
    else:
        refined = raw_refined
    try:
        confidence = max(0.0, min(1.0, float(payload.get("confidence") or 0.0)))
    except (TypeError, ValueError):
        confidence = 0.0
    rationale = str(payload.get("rationale") or "").strip()
    return verdict, refined, confidence, rationale


def validate_or_refine_topic_for_publisher(
    *,
    target_keyword: str,
    publishing_profile_payload: Optional[Dict[str, Any]],
    language: str = "de",
    api_key: Optional[str] = None,
    model: str = DEFAULT_HAIKU_MODEL,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    min_confidence: float = 0.4,
) -> FitVerdict:
    """Check the topic against the publisher profile; refine or hard-fail.

    Returns a ``FitVerdict`` with the keyword to use downstream. The caller
    is expected to feed ``refined_keyword`` into the contract step in place
    of the original.

    Soft-skip: if ``publishing_profile_payload`` is empty/None, returns the
    original keyword with confidence=0.5 and rationale="profile_unavailable".
    Caller can decide to enforce a profile-required policy elsewhere.

    Hard-fail (raises ``PublisherFitError``):
    - LLM returns ``verdict="no_fit"``, OR
    - LLM returns a refined keyword with confidence < ``min_confidence``.
    """

    keyword = (target_keyword or "").strip()
    if not keyword:
        raise PublisherFitError("missing_keyword", "target_keyword is required for fit validation.")

    summary = _summarise_publisher_profile(publishing_profile_payload or {})
    if summary.startswith("(") and summary.endswith(")"):
        # No publisher profile signal -> soft pass-through.
        return FitVerdict(
            refined_keyword=keyword,
            original_keyword=keyword,
            changed=False,
            confidence=0.5,
            rationale="publisher_profile_unavailable",
            cost_usd=0.0,
        )

    lang = language.lower() if language else "de"
    instructions = _LANGUAGE_INSTRUCTIONS.get(lang, _LANGUAGE_INSTRUCTIONS["de"])

    resolved_api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not resolved_api_key:
        # No way to validate without the API key. Soft-pass with low confidence
        # so the rest of the pipeline can still log/decide.
        return FitVerdict(
            refined_keyword=keyword,
            original_keyword=keyword,
            changed=False,
            confidence=0.5,
            rationale="anthropic_api_key_not_configured",
            cost_usd=0.0,
        )

    user_prompt = instructions["user_template"].format(keyword=keyword, publisher_summary=summary)

    try:
        payload = call_llm_json(
            system_prompt=instructions["system"],
            user_prompt=user_prompt,
            api_key=resolved_api_key,
            base_url=base_url,
            model=model,
            timeout_seconds=timeout_seconds,
            max_tokens=400,
            temperature=0.1,
            request_label="publisher_fit",
        )
    except LLMError as exc:
        # Don't block on infra hiccups -- soft-pass and let downstream eval
        # checks catch any real mismatch.
        logger.warning("publisher_fit.llm_failed err=%s", exc)
        return FitVerdict(
            refined_keyword=keyword,
            original_keyword=keyword,
            changed=False,
            confidence=0.5,
            rationale=f"llm_unavailable:{str(exc)[:80]}",
            cost_usd=0.0,
        )

    verdict, refined, confidence, rationale = _parse_verdict(payload, keyword)
    cost = 0.001  # rough Haiku cost upper bound for this prompt size

    if verdict == "no_fit":
        raise PublisherFitError(
            "no_editorial_fit",
            (
                f"No editorial intersection between target keyword {keyword!r} and the "
                f"publishing site's audience. {rationale or 'Pick a different publisher or topic.'}"
            ),
        )

    if confidence < min_confidence:
        raise PublisherFitError(
            "fit_below_threshold",
            (
                f"Fit confidence {confidence:.2f} below threshold {min_confidence:.2f} for "
                f"target keyword {keyword!r} on this publisher. {rationale or ''}"
            ),
        )

    return FitVerdict(
        refined_keyword=refined,
        original_keyword=keyword,
        changed=(verdict == "refine" and refined != keyword),
        confidence=confidence,
        rationale=rationale,
        cost_usd=cost,
    )
