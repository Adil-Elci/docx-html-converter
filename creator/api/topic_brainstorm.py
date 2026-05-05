"""Brainstorm 3-5 editorial topic angles for a guest post.

Why this exists: the keyword-derivation step produces a search keyword
(e.g. "günstige brillen online kaufen"), and the contract generator
turns that into a comparison/buying-guide article. That's fine for
ranking but produces formulaic articles that read like product pages.

A human editor pitching a guest post on the same brief would propose
ideas like "Kurzsichtigkeit bei Kindern: Warum immer mehr Grundschüler
eine Brille brauchen" or "Bildschirmzeit & Augengesundheit". Trend-led,
journalistic, and far more publishable. This module produces that kind
of slate so we can pick the strongest editorial angle BEFORE the
contract step locks in the article's frame.

Costs ~$0.02 per call (Sonnet 4.6, single shot, no thinking tokens).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .llm import LLMError, call_llm_json

logger = logging.getLogger("creator.topic_brainstorm")

DEFAULT_MODEL = "claude-sonnet-4-6"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_NUM_ANGLES = 5


class TopicBrainstormError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class EditorialAngle:
    title: str             # proposed H1
    target_keyword: str    # SEO keyword that fits this angle
    hook: str              # one-sentence summary of the editorial slant
    rationale: str         # why this angle works for target + publisher + 2026 trends


@dataclass
class BrainstormResult:
    angles: List[EditorialAngle]
    cost_usd: float = 0.0
    cache_hit: bool = False
    excluded_count: int = 0  # how many cached angles were dropped because they matched exclude_topics


# ---- prompts --------------------------------------------------------------


_LANGUAGE_SYSTEM_PROMPTS: Dict[str, str] = {
    "de": (
        "Du bist ein erfahrener deutscher Magazin-Redakteur und SEO-Stratege. "
        "Deine Aufgabe: schlage {n} editorial starke Themen-Angles für einen "
        "Gastartikel vor, der auf einer Drittseite veröffentlicht wird.\n\n"
        "Kontext, den du bekommst:\n"
        "- ZIEL-WEBSITE: das Unternehmen / Produkt, das den Backlink bekommt.\n"
        "- ZIEL-KEYWORD: das Such-Keyword, das aus der Ziel-URL abgeleitet wurde.\n"
        "- PUBLISHER-PROFIL: redaktionelle Linie der Veröffentlichungsseite.\n"
        "- AKTUELLES JAHR: für zeitliche Relevanz.\n"
        "- BEREITS VERWENDETE THEMEN (optional): Themen, die der Kunde dieses Jahr "
        "  auf dieser Ziel-Seite bereits veröffentlicht hat. NIEMALS neu vorschlagen.\n\n"
        "Antworte AUSSCHLIESSLICH mit gültigem JSON nach diesem Schema:\n"
        "{{\n"
        "  \"angles\": [\n"
        "    {{\n"
        "      \"title\": <string, der vorgeschlagene Artikel-Titel / H1>,\n"
        "      \"target_keyword\": <string, das passende deutsche Such-Keyword (2-5 Wörter, Kleinschreibung)>,\n"
        "      \"hook\": <string, ein Satz: was den Artikel interessant macht>,\n"
        "      \"rationale\": <string, ein Satz: warum dieser Angle für Ziel + Publisher + 2026 funktioniert>\n"
        "    }}\n"
        "  ]\n"
        "}}\n\n"
        "Regeln für gute Angles:\n"
        "- **Editorial first**: news-getrieben, ratgeberhaft, erklärend, oder trendbasiert. KEINE reinen Vergleichs-/Preisartikel als Hauptangle.\n"
        "- **Publisher-passend**: das Thema muss für die Audience der Veröffentlichungsseite relevant sein, nicht für die Audience der Ziel-Website.\n"
        "- **Aktualität**: bevorzuge Angles mit zeitlichem Bezug (Studien, Trends, Gesetzesänderungen, Saison) über zeitlose \"Was ist X\"-Themen.\n"
        "- **Vielfalt**: liefere unterschiedliche Angles. Nicht 5x \"Vergleich von X\" mit Variationen.\n"
        "- **Konkret**: Titel müssen konkrete Aussagen oder Fragen enthalten, keine Floskeln.\n"
        "- **Keine Marken in Titeln**: der Titel darf nicht den Markennamen / die Domain der Ziel-Website enthalten — der Backlink wird natürlich im Artikelkörper platziert.\n"
        "- **Such-Keyword**: realistisch, das Nutzer in Deutschland 2026 in Google eingeben würden.\n"
        "- **Duplikate vermeiden**: wenn BEREITS VERWENDETE THEMEN gelistet sind, generiere KEINE Angles, deren Such-Keyword oder Titel inhaltlich (nicht nur wörtlich) einem dieser Themen entspricht.\n"
    ),
    "fr": (
        "Vous êtes un rédacteur en chef de magazine francophone expérimenté et stratège SEO. "
        "Votre tâche : proposez {n} angles éditoriaux forts pour un article invité publié sur un site tiers.\n\n"
        "Contexte fourni :\n"
        "- SITE CIBLE : l'entreprise / le produit qui reçoit le backlink.\n"
        "- MOT-CLÉ CIBLE : le mot-clé de recherche dérivé de l'URL cible.\n"
        "- PROFIL ÉDITEUR : la ligne éditoriale du site de publication.\n"
        "- ANNÉE EN COURS : pour la pertinence temporelle.\n"
        "- SUJETS DÉJÀ UTILISÉS (optionnel) : sujets que ce client a déjà publiés "
        "  cette année sur ce site cible. NE PAS reproposer.\n\n"
        "Répondez UNIQUEMENT avec un JSON valide selon ce schéma :\n"
        "{{\n"
        "  \"angles\": [\n"
        "    {{\n"
        "      \"title\": <string, le titre / H1 proposé>,\n"
        "      \"target_keyword\": <string, le mot-clé SEO français correspondant (2-5 mots, minuscules)>,\n"
        "      \"hook\": <string, une phrase : ce qui rend l'article intéressant>,\n"
        "      \"rationale\": <string, une phrase : pourquoi cet angle marche pour cible + éditeur + 2026>\n"
        "    }}\n"
        "  ]\n"
        "}}\n\n"
        "Règles pour de bons angles :\n"
        "- **Éditorial d'abord** : actualité, conseil, explicatif, ou tendance. PAS d'articles purement comparatifs/tarifs comme angle principal.\n"
        "- **Adapté à l'éditeur** : le sujet doit intéresser l'audience du site de publication, pas celle du site cible.\n"
        "- **Actualité** : privilégiez les angles temporels (études, tendances, changements réglementaires, saisons) aux sujets atemporels \"Qu'est-ce que X\".\n"
        "- **Variété** : proposez des angles différents. Pas 5x \"Comparatif de X\" avec variations.\n"
        "- **Concret** : les titres doivent contenir des affirmations ou questions concrètes, pas des formules creuses.\n"
        "- **Pas de marques dans les titres** : le titre ne doit pas contenir le nom de marque / domaine du site cible — le backlink sera placé naturellement dans le corps.\n"
        "- **Mot-clé** : réaliste, ce que des utilisateurs en France saisiraient sur Google en 2026.\n"
        "- **Éviter les doublons** : si SUJETS DÉJÀ UTILISÉS sont listés, ne générez PAS d'angles dont le mot-clé ou le titre recoupe l'un de ces sujets (substantiellement, pas seulement à la lettre).\n"
    ),
}


def _summarise_publisher_profile(profile: Dict[str, Any]) -> str:
    if not isinstance(profile, dict) or not profile:
        return "(no publisher profile available)"
    fields_in_order: List[tuple[str, str]] = [
        ("primary_context", "primary context"),
        ("topics", "topics"),
        ("audience", "audience"),
        ("target_audience", "target audience"),
        ("editorial_terms", "editorial terms"),
        ("repeated_keywords", "repeated keywords"),
        ("language", "language"),
    ]

    def _fmt(value: Any) -> str:
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
        formatted = _fmt(profile.get(key))
        if formatted:
            lines.append(f"- {label}: {formatted}")
    return "\n".join(lines) if lines else "(publisher profile is empty)"


def _format_exclude_topics(values: Optional[List[str]], language: str) -> str:
    cleaned = [v.strip() for v in (values or []) if v and v.strip()]
    if not cleaned:
        return ""
    bullet_lines = "\n".join(f"  - {v}" for v in cleaned[:20])
    if language == "fr":
        return f"\nSUJETS DÉJÀ UTILISÉS (à éviter)\n================================\n{bullet_lines}\n"
    return f"\nBEREITS VERWENDETE THEMEN (vermeiden)\n=====================================\n{bullet_lines}\n"


def _build_user_prompt(
    *,
    target_url: str,
    target_keyword: str,
    publishing_profile_payload: Optional[Dict[str, Any]],
    language: str,
    current_year: int,
    exclude_topics: Optional[List[str]] = None,
) -> str:
    publisher_summary = _summarise_publisher_profile(publishing_profile_payload or {})
    exclude_block = _format_exclude_topics(exclude_topics, language)
    if language == "fr":
        return (
            f"SITE CIBLE : {target_url}\n"
            f"MOT-CLÉ CIBLE : {target_keyword}\n"
            f"ANNÉE EN COURS : {current_year}\n\n"
            f"PROFIL ÉDITEUR\n==============\n{publisher_summary}\n"
            f"{exclude_block}\n"
            f"Proposez les angles maintenant."
        )
    # default DE
    return (
        f"ZIEL-WEBSITE: {target_url}\n"
        f"ZIEL-KEYWORD: {target_keyword}\n"
        f"AKTUELLES JAHR: {current_year}\n\n"
        f"PUBLISHER-PROFIL\n================\n{publisher_summary}\n"
        f"{exclude_block}\n"
        f"Schlage jetzt die Angles vor."
    )


def _normalize_for_dedup(value: str) -> str:
    return " ".join((value or "").lower().split())


def _filter_against_excludes(
    angles: List[EditorialAngle], exclude_topics: Optional[List[str]]
) -> Tuple[List[EditorialAngle], int]:
    """Returns (kept_angles, dropped_count). Compares lowercased,
    whitespace-collapsed forms; substring containment counts as a match
    (so "kinderbrillen" excludes "kinderbrillen kaufen 2026")."""

    if not exclude_topics:
        return list(angles), 0
    normalized_excludes = [_normalize_for_dedup(t) for t in exclude_topics if t and t.strip()]
    if not normalized_excludes:
        return list(angles), 0

    kept: List[EditorialAngle] = []
    dropped = 0
    for angle in angles:
        haystacks = [_normalize_for_dedup(angle.target_keyword), _normalize_for_dedup(angle.title)]
        # Unidirectional: the previously-used topic must appear inside the
        # candidate angle. The reverse (haystack inside exclude) would
        # false-positive on short strings -- e.g. a one-letter title "B"
        # would match any exclude containing the letter "b".
        is_dup = any(
            ex and any(ex in h for h in haystacks if h)
            for ex in normalized_excludes
        )
        if is_dup:
            dropped += 1
            continue
        kept.append(angle)
    return kept, dropped


def _parse_angles(payload: Dict[str, Any]) -> List[EditorialAngle]:
    raw_angles = payload.get("angles") if isinstance(payload, dict) else None
    if not isinstance(raw_angles, list):
        return []
    out: List[EditorialAngle] = []
    for item in raw_angles:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        keyword = str(item.get("target_keyword") or "").strip().lower()
        hook = str(item.get("hook") or "").strip()
        rationale = str(item.get("rationale") or "").strip()
        if title and keyword:
            out.append(EditorialAngle(
                title=title,
                target_keyword=keyword,
                hook=hook,
                rationale=rationale,
            ))
    return out


def _serialize_angles_for_cache(angles: List[EditorialAngle]) -> Dict[str, Any]:
    return {
        "angles": [
            {
                "title": a.title,
                "target_keyword": a.target_keyword,
                "hook": a.hook,
                "rationale": a.rationale,
            }
            for a in angles
        ],
    }


def _hydrate_angles_from_cache(payload: Dict[str, Any]) -> List[EditorialAngle]:
    raw = payload.get("angles") if isinstance(payload, dict) else None
    if not isinstance(raw, list):
        return []
    out: List[EditorialAngle] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        keyword = str(item.get("target_keyword") or "").strip().lower()
        if title and keyword:
            out.append(EditorialAngle(
                title=title,
                target_keyword=keyword,
                hook=str(item.get("hook") or "").strip(),
                rationale=str(item.get("rationale") or "").strip(),
            ))
    return out


def brainstorm_editorial_angles(
    *,
    target_url: str,
    target_keyword: str,
    publishing_profile_payload: Optional[Dict[str, Any]] = None,
    publisher_url: Optional[str] = None,
    language: str = "de",
    current_year: Optional[int] = None,
    num_angles: int = DEFAULT_NUM_ANGLES,
    exclude_topics: Optional[List[str]] = None,
    use_cache: bool = True,
    min_angles_after_filter: int = 1,
    api_key: Optional[str] = None,
    model: str = DEFAULT_MODEL,
    base_url: str = DEFAULT_ANTHROPIC_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> BrainstormResult:
    """Generate editorial angle suggestions for a (target site, publisher) pair.

    Returns up to ``num_angles`` angles ordered by the LLM's own preference
    (caller can take the first one as the auto-pick). Cost: ~$0.02 per fresh call.

    Caching: when ``use_cache=True`` and ``publisher_url`` is set, results are
    cached for 90 days keyed on (target_url, publisher_url, language, year).
    Cache hits cost $0. The exclude_topics list is applied AT READ TIME so
    a different exclude list doesn't fragment the cache; if filtering
    leaves fewer than ``min_angles_after_filter`` angles, the cache is
    bypassed and a fresh brainstorm runs (with exclude_topics passed to
    the LLM).

    Soft-fails to an empty result on missing API key or LLM error.
    Hard-fails (raises ``TopicBrainstormError``) only on missing inputs.
    """

    keyword = (target_keyword or "").strip()
    if not keyword:
        raise TopicBrainstormError("missing_keyword", "target_keyword is required for brainstorm.")
    if not (target_url or "").strip():
        raise TopicBrainstormError("missing_target_url", "target_url is required for brainstorm.")

    lang = (language or "de").lower()
    year = int(current_year) if current_year is not None else datetime.now(timezone.utc).year
    cache_lookup_key: Optional[str] = None
    cache_locale: Optional[str] = None

    # Cache lookup -- skip when no publisher_url (we'd cache too broadly).
    if use_cache and (publisher_url or "").strip():
        try:
            from .topic_brainstorm_cache import build_locale, build_lookup_key, get_cached_brainstorm
        except ImportError:
            get_cached_brainstorm = None  # type: ignore
            build_lookup_key = None  # type: ignore
            build_locale = None  # type: ignore
        if get_cached_brainstorm is not None:
            cache_lookup_key = build_lookup_key(
                target_url=target_url,
                publisher_url=publisher_url,
                language=lang,
                current_year=year,
            )
            cache_locale = build_locale(lang, year)
            cached = get_cached_brainstorm(lookup_key=cache_lookup_key, locale=cache_locale)
            if cached:
                cached_angles = _hydrate_angles_from_cache(cached)
                kept, dropped = _filter_against_excludes(cached_angles, exclude_topics)
                if len(kept) >= max(1, min_angles_after_filter):
                    logger.info(
                        "topic_brainstorm.cache_hit pair=(%s,%s) total=%s kept=%s",
                        target_url, publisher_url, len(cached_angles), len(kept),
                    )
                    return BrainstormResult(
                        angles=kept[:num_angles],
                        cost_usd=0.0,
                        cache_hit=True,
                        excluded_count=dropped,
                    )
                logger.info(
                    "topic_brainstorm.cache_exhausted pair=(%s,%s) cached=%s after_filter=%s — refreshing",
                    target_url, publisher_url, len(cached_angles), len(kept),
                )

    resolved_api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not resolved_api_key:
        logger.warning("topic_brainstorm.no_api_key — returning empty angles")
        return BrainstormResult(angles=[], cost_usd=0.0)

    system_template = _LANGUAGE_SYSTEM_PROMPTS.get(lang) or _LANGUAGE_SYSTEM_PROMPTS["de"]
    system_prompt = system_template.format(n=num_angles)
    user_prompt = _build_user_prompt(
        target_url=target_url,
        target_keyword=keyword,
        publishing_profile_payload=publishing_profile_payload,
        language=lang,
        current_year=year,
        exclude_topics=exclude_topics,
    )

    try:
        payload = call_llm_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            api_key=resolved_api_key,
            base_url=base_url,
            model=model,
            timeout_seconds=timeout_seconds,
            max_tokens=2000,
            temperature=0.9,  # higher for more creative variety
            request_label="topic_brainstorm",
        )
    except LLMError as exc:
        logger.warning("topic_brainstorm.llm_failed err=%s", exc)
        return BrainstormResult(angles=[], cost_usd=0.0)

    parsed = _parse_angles(payload)
    # Defensive post-filter: if Haiku ignored the exclude list in the
    # prompt, drop dupes here so the caller never sees them.
    filtered, dropped = _filter_against_excludes(parsed, exclude_topics)
    final_angles = filtered[:num_angles]

    # Write-back cache: store the FRESH angles (full list, not the filtered
    # one) so the next call can reuse them with a different exclude list.
    if cache_lookup_key and cache_locale and parsed:
        try:
            from .topic_brainstorm_cache import upsert_brainstorm
            upsert_brainstorm(
                lookup_key=cache_lookup_key,
                locale=cache_locale,
                payload=_serialize_angles_for_cache(parsed[:num_angles]),
            )
        except ImportError:
            pass

    return BrainstormResult(
        angles=final_angles,
        cost_usd=0.02,
        cache_hit=False,
        excluded_count=dropped,
    )
