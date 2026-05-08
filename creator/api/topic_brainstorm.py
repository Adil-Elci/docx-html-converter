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

import html
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .llm import LLMError, call_llm_json

logger = logging.getLogger("creator.topic_brainstorm")

DEFAULT_MODEL = "claude-sonnet-4-6"
DEFAULT_ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
DEFAULT_TIMEOUT_SECONDS = 90
DEFAULT_NUM_ANGLES = 5
# Number of angles to brainstorm and cache on a fresh call. Sequential
# consumption: portal_backend asks for num_angles=1 each time and gets the
# next-unused (highest-ranked still-unpublished) angle from the cached batch.
# Cache holds for 90 days OR until all 45 are excluded by the published-list
# filter, whichever comes first. Per-article cost amortises to ~$0.002 once
# the batch is reused across multiple orders.
BRAINSTORM_BATCH_SIZE = 45
DEFAULT_MAX_TOKENS = 8000  # ~45 angles x ~150 tokens each + JSON envelope


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
        "Du bist ein erfahrener deutscher SEO-Stratege UND Magazin-Redakteur. "
        "Du denkst in zwei Schichten gleichzeitig: was Leser:innen bei Google "
        "suchen (SEO) und wie ein Editor die Geschichte erzählen würde, damit "
        "ein Publisher sie tatsächlich veröffentlicht.\n\n"
        "Deine Aufgabe: schlage GENAU {n} Themen-Angles für einen Gastbeitrag "
        "auf einer fremden Publisher-Website vor. Sortiere sie nach "
        "PUBLIZIERBARKEIT — nicht nach SEO-Volumen. Der Top-Angle (Index 0) "
        "muss von einem Editor einer großen deutschen Magazin-Site OHNE "
        "Zögern angenommen werden.\n\n"
        "Kontext, den du bekommst:\n"
        "- ZIEL-WEBSITE: das Unternehmen / Produkt, das den Backlink bekommt.\n"
        "- ZIEL-KEYWORD: das Such-Keyword, oft kommerziell-transaktional "
        "  (z.B. \"kinderbrillen günstig\", \"steuerberater hamburg\").\n"
        "- PUBLISHER-PROFIL: redaktionelle Linie der Veröffentlichungsseite.\n"
        "- AKTUELLES JAHR: für zeitliche Relevanz.\n"
        "- BEREITS VERWENDETE THEMEN (optional): nicht erneut vorschlagen.\n\n"
        "WICHTIGSTES PRINZIP — editoriale Hülle für kommerzielle Intention:\n\n"
        "Das ZIEL-KEYWORD ist oft kommerziell. Du sollst die kommerzielle "
        "Intention NICHT verstecken, sondern sie in ein editoriales Format "
        "einbetten, das ein Publisher veröffentlicht. Beide Beispiele führen "
        "zum gleichen kommerziellen Ziel — aber nur das zweite würde als "
        "Gastbeitrag akzeptiert:\n"
        "  Falsch (Verkaufsseite): \"Kinderbrillen günstig: Top-Anbieter im "
        "Vergleich 2026\"\n"
        "  Richtig (Magazin-Stück): \"Kinderbrille auf Rezept: Wie viel zahlt "
        "die Krankenkasse — und wo lohnt sich der Online-Kauf?\"\n\n"
        "Antworte AUSSCHLIESSLICH mit gültigem JSON nach diesem Schema:\n"
        "{{\n"
        "  \"angles\": [\n"
        "    {{\n"
        "      \"title\": <string, redaktioneller H1 des Artikels>,\n"
        "      \"target_keyword\": <string, das SEO-Keyword (2-5 Wörter, Kleinschreibung)>,\n"
        "      \"hook\": <string, ein Satz: was den Artikel interessant macht>,\n"
        "      \"rationale\": <string, ein Satz: warum dieser Angle für Publisher UND Ranking funktioniert>\n"
        "    }}\n"
        "  ]\n"
        "}}\n\n"
        "Liefere die volle Anzahl ({n}). Wenn du nur 10 starke Angles hast, "
        "fülle den Rest mit Variationen unterschiedlicher Editorial-Frames "
        "auf — keine Lücken, keine Wiederholungen, keine Filler-Phrasen.\n\n"
        "Sortier-Regel (KRITISCH — Index 0 = am publizierbarsten):\n"
        "- **Top ~12**: Angles, die ein Magazin-Editor sofort annimmt — "
        "  konkrete Frage, neue Studie, regulatorische Änderung, "
        "  journalistische Reportage, Experten-Erklärung. SEO-Volumen ist "
        "  NUR Tiebreaker, nicht primäres Sortierkriterium.\n"
        "- **Mitte ~13-28**: solide Ratgeber- / Erklär-Stücke ohne "
        "  News-Aufhänger (zeitlose Themen mit klarem Leser-Nutzen).\n"
        "- **Tail ~29-{n}**: Long-Tail-Frage-/Anwendungs-Keywords, "
        "  redaktionell schwächer, aber als Quick-Win rankbar.\n\n"
        "TITEL-REGELN (HARD):\n"
        "- KEINE kommerziellen Qualifizierer im TITEL: \"günstig\", "
        "  \"billig\", \"preiswert\", \"kostenlos\", \"Top X\", \"Beste X\", "
        "  \"Bestseller\", \"kaufen\", \"bestellen\", \"Test\", "
        "  \"Vergleichssieger\", \"% Rabatt\", \"% sparen\", \"Angebot\", "
        "  \"Aktion\", \"Schnäppchen\", \"Deal\". Diese gehören (wenn überhaupt) "
        "  ins target_keyword, NIEMALS in den Titel.\n"
        "- Der Titel MUSS einer dieser Editorial-Strukturen folgen:\n"
        "  1. **Frage**: \"Wie viel zahlt die Krankenkasse für …?\", "
        "     \"Wann lohnt sich …?\", \"Worauf sollten Eltern beim … achten?\"\n"
        "  2. **Studie/Daten**: \"Studie zeigt: X Prozent der …\", "
        "     \"Neue Zahlen belegen: …\"\n"
        "  3. **News/Regulatorisch**: \"Neue Regelung 2026 — was Familien "
        "     jetzt wissen müssen\", \"Ab Januar gilt: …\"\n"
        "  4. **Erklärer**: \"X auf Rezept: Was die Kasse zahlt, was Sie "
        "     selbst tragen\", \"Worauf es bei … wirklich ankommt\"\n"
        "  5. **Reportage/Trend**: \"Warum immer mehr Kinder eine Brille "
        "     brauchen — Eltern berichten\", \"Der wahre Preis von …\"\n"
        "  6. **Faktencheck**: \"Apotheke vs. Online-Anbieter — der "
        "     ehrliche Vergleich\", \"Mythos … — was wirklich stimmt\"\n"
        "- Konkret: Titel müssen konkrete Aussagen oder Fragen enthalten, "
        "  keine Floskeln.\n"
        "- Keine Marken: der Titel darf nicht den Markennamen / die Domain "
        "  der Ziel-Website enthalten — der Backlink wird natürlich im "
        "  Artikelkörper platziert.\n"
        "- ASCII-Zeichen für Bindestriche und Anführungszeichen verwenden, "
        "  KEINE HTML-Entities (\"&\" statt \"&amp;\").\n\n"
        "KEYWORD-REGELN:\n"
        "- target_keyword darf das ZIEL-KEYWORD oder eine nahe Long-Tail-"
        "  Variante sein. Die kommerzielle Intention bleibt im Keyword "
        "  erhalten — nur der TITEL wird editorial gerahmt.\n"
        "- Realistisch: was Nutzer in Deutschland im aktuellen Jahr in "
        "  Google eingeben — keine erfundenen Begriffe.\n\n"
        "PUBLISHER-FIT:\n"
        "- Lesertest: würde der Editor des Publishers diesen Artikel auf "
        "  seiner Startseite featuren? Wenn nein, ist der Angle ungeeignet.\n"
        "- Das Thema muss für die Audience der Veröffentlichungsseite "
        "  relevant sein, nicht für die Audience der Ziel-Website.\n\n"
        "VIELFALT:\n"
        "- Alle {n} Angles müssen unterschiedlich sein — verschiedene "
        "  Editorial-Frames, nicht nur Wort-Variationen desselben Themas.\n\n"
        "DUPLIKATE VERMEIDEN:\n"
        "- Wenn BEREITS VERWENDETE THEMEN gelistet sind, generiere KEINE "
        "  Angles, deren Such-Keyword oder Titel inhaltlich (nicht nur "
        "  wörtlich) einem dieser Themen entspricht.\n"
    ),
    "fr": (
        "Vous êtes stratège SEO francophone ET rédacteur en chef de magazine. "
        "Vous pensez en deux couches : ce que les lecteurs cherchent sur "
        "Google (SEO) et comment un éditeur de magazine raconterait l'histoire "
        "pour qu'un publisher la publie réellement.\n\n"
        "Votre tâche : proposez EXACTEMENT {n} angles pour un article invité "
        "publié sur un site tiers. Triez-les par PUBLIABILITÉ — pas par "
        "volume SEO. L'angle de tête (index 0) doit pouvoir être accepté SANS "
        "hésitation par un rédacteur de grand magazine français.\n\n"
        "Contexte fourni :\n"
        "- SITE CIBLE : l'entreprise / le produit qui reçoit le backlink.\n"
        "- MOT-CLÉ CIBLE : le mot-clé de recherche, souvent commercial-"
        "  transactionnel (ex. \"lunettes enfant pas cher\", \"avocat lyon\").\n"
        "- PROFIL ÉDITEUR : la ligne éditoriale du site de publication.\n"
        "- ANNÉE EN COURS : pour la pertinence temporelle.\n"
        "- SUJETS DÉJÀ UTILISÉS (optionnel) : ne pas reproposer.\n\n"
        "PRINCIPE CENTRAL — habillage éditorial de l'intention commerciale :\n\n"
        "Le MOT-CLÉ CIBLE est souvent commercial. Vous ne devez PAS cacher "
        "l'intention commerciale, mais l'emballer dans un format éditorial "
        "qu'un éditeur publierait. Les deux exemples mènent au même but "
        "commercial — mais seul le second serait accepté comme article "
        "invité :\n"
        "  Faux (page de vente) : \"Lunettes enfants pas cher : top "
        "fournisseurs comparés 2026\"\n"
        "  Bon (article magazine) : \"Lunettes pour enfants sur ordonnance : "
        "combien rembourse la Sécu — et où l'achat en ligne vaut le coup ?\"\n\n"
        "Répondez UNIQUEMENT avec un JSON valide selon ce schéma :\n"
        "{{\n"
        "  \"angles\": [\n"
        "    {{\n"
        "      \"title\": <string, le H1 éditorial de l'article>,\n"
        "      \"target_keyword\": <string, le mot-clé SEO français (2-5 mots, minuscules)>,\n"
        "      \"hook\": <string, une phrase : ce qui rend l'article intéressant>,\n"
        "      \"rationale\": <string, une phrase : pourquoi cet angle marche pour l'éditeur ET le ranking>\n"
        "    }}\n"
        "  ]\n"
        "}}\n\n"
        "Fournissez le nombre complet ({n}). S'il n'existe que 10 angles "
        "forts, complétez avec des variantes de cadres éditoriaux différents "
        "— pas de trous, pas de répétitions, pas de phrases creuses.\n\n"
        "Règle de tri (CRITIQUE — index 0 = le plus publiable) :\n"
        "- **Top ~12** : angles qu'un éditeur accepterait immédiatement — "
        "  question concrète, étude récente, changement réglementaire, "
        "  reportage, explication d'expert. Le volume SEO est UNIQUEMENT "
        "  un critère de départage, pas le tri principal.\n"
        "- **Milieu ~13-28** : conseils / explicatifs solides sans accroche "
        "  d'actualité (sujets atemporels avec utilité claire pour le lecteur).\n"
        "- **Queue ~29-{n}** : mots-clés longue traîne (questions, usage), "
        "  rapides à classer mais éditorialement plus faibles.\n\n"
        "RÈGLES DE TITRE (DURES) :\n"
        "- AUCUN qualificatif commercial dans le TITRE : \"pas cher\", "
        "  \"bon marché\", \"gratuit\", \"top X\", \"meilleur X\", \"best-of\", "
        "  \"acheter\", \"commander\", \"test\", \"vainqueur du comparatif\", "
        "  \"% de réduction\", \"% d'économie\", \"promo\", \"soldes\", "
        "  \"bon plan\", \"deal\". Ces termes vont (si besoin) dans le "
        "  mot-clé cible, JAMAIS dans le titre.\n"
        "- Le titre DOIT suivre l'une de ces structures éditoriales :\n"
        "  1. **Question** : \"Combien rembourse la Sécu pour … ?\", "
        "     \"Quand l'achat en ligne vaut-il le coup ?\", \"Que doivent "
        "     vérifier les parents avant … ?\"\n"
        "  2. **Étude/Données** : \"Étude : X % des …\", \"Les nouveaux "
        "     chiffres montrent : …\"\n"
        "  3. **Actualité/Réglementation** : \"Nouvelle règle 2026 — ce que "
        "     les familles doivent savoir\", \"À partir de janvier : …\"\n"
        "  4. **Explicatif** : \"X sur ordonnance : ce que la Sécu paie, ce "
        "     qui reste à votre charge\", \"Ce qui compte vraiment quand …\"\n"
        "  5. **Reportage/Tendance** : \"Pourquoi de plus en plus de … : "
        "     des parents témoignent\", \"Le vrai prix de …\"\n"
        "  6. **Vérification des faits** : \"Pharmacie vs. boutique en "
        "     ligne — le comparatif honnête\", \"Mythe : … — ce qui est "
        "     réellement vrai\"\n"
        "- Concret : titres avec affirmations ou questions concrètes, pas "
        "  de formules creuses.\n"
        "- Pas de marques : le titre ne doit pas contenir le nom de marque "
        "  / domaine du site cible — le backlink sera placé naturellement "
        "  dans le corps.\n"
        "- Caractères ASCII pour les tirets et apostrophes, JAMAIS d'entités "
        "  HTML (\"&\" pas \"&amp;\").\n\n"
        "RÈGLES DE MOT-CLÉ :\n"
        "- target_keyword peut être le MOT-CLÉ CIBLE ou une variante longue "
        "  traîne proche. L'intention commerciale reste dans le mot-clé — "
        "  seul le TITRE est habillé en éditorial.\n"
        "- Réaliste : ce que des utilisateurs en France saisiraient sur "
        "  Google dans l'année en cours — pas de termes inventés.\n\n"
        "ADAPTATION ÉDITEUR :\n"
        "- Test du lecteur : l'éditeur du site mettrait-il cet article en "
        "  une ? Sinon, l'angle ne convient pas.\n"
        "- Le sujet doit intéresser l'audience du site de publication, pas "
        "  celle du site cible.\n\n"
        "VARIÉTÉ :\n"
        "- Tous les {n} angles doivent être différents — différents cadres "
        "  éditoriaux, pas seulement variations de mots du même sujet.\n\n"
        "ÉVITER LES DOUBLONS :\n"
        "- Si SUJETS DÉJÀ UTILISÉS sont listés, ne générez PAS d'angles "
        "  dont le mot-clé ou le titre recoupe l'un de ces sujets "
        "  (substantiellement, pas seulement à la lettre).\n"
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


_LISTICLE_DIRECTIVE_DE = (
    "FORMAT-PRÄFERENZ: FRAMEWORK-LISTICLE\n"
    "=====================================\n"
    "Generiere in dieser Runde AUSSCHLIESSLICH Framework-Listicle-Angles aus den\n"
    "folgenden sechs erlaubten Frames. Items sind handlungsorientierte Statements\n"
    "(Fehler, Fragen, Anzeichen, Tipps, Kriterien, Schritte) — KEINE realen\n"
    "Marken, Produkte oder Anbieter, weil wir keine Recherche zu echten\n"
    "Entitäten haben.\n\n"
    "ERLAUBTE Frames (mindestens 80% der Angles MÜSSEN einem davon folgen):\n"
    "  1. 'N Fehler bei …' — z.B. '7 Fehler bei der Steuerberater-Auswahl'\n"
    "  2. 'N Fragen, die Sie … stellen sollten' — z.B. '10 Fragen an Ihren Steuerberater'\n"
    "  3. 'N Anzeichen, dass …' — z.B. '5 Anzeichen, dass Sie einen neuen Steuerberater brauchen'\n"
    "  4. 'N Tipps für …' — z.B. '8 Tipps für die Wahl des Steuerberaters'\n"
    "  5. 'N Kriterien / Funktionen für …' — z.B. '6 Kriterien für die Auswahl eines Steuerberaters'\n"
    "  6. 'N Schritte zu …' — z.B. '5 Schritte zum richtigen Steuerberater'\n\n"
    "VERBOTEN — diese Frames bitte NICHT generieren:\n"
    "  ✗ 'Die N besten X' / 'Top N X' / 'N beste Anbieter / Kanzleien / Produkte'\n"
    "  ✗ Listen, deren Items konkrete Marken oder Unternehmen sein müssten\n"
    "  ✗ 'Vergleich der besten …' (impliziert Anbieter-Recherche)\n\n"
    "Die Zahl N (zwischen 5 und 12) gehört in den Titel ('7', '10' usw.).\n"
    "Die kommerziellen Qualifizierer aus den allgemeinen Titel-Regeln bleiben\n"
    "verboten — der Listicle-Frame ist redaktionell, nicht promotional.\n"
)

_LISTICLE_DIRECTIVE_FR = (
    "PRÉFÉRENCE FORMAT : LISTICLE-FRAMEWORK\n"
    "=======================================\n"
    "Générez UNIQUEMENT des angles de listicle « framework » selon les six cadres\n"
    "ci-dessous. Les items sont des énoncés actionnables (erreurs, questions,\n"
    "signaux, conseils, critères, étapes) — JAMAIS des marques ou fournisseurs\n"
    "réels (nous n'avons pas de recherche sur les entités réelles).\n\n"
    "Cadres AUTORISÉS (au moins 80 % des angles doivent suivre l'un d'eux) :\n"
    "  1. 'N erreurs à éviter quand …' — ex. '7 erreurs à éviter en choisissant un avocat'\n"
    "  2. 'N questions à poser à …' — ex. '10 questions à poser à votre avocat'\n"
    "  3. 'N signes que …' — ex. '5 signes qu'il vous faut changer d'avocat'\n"
    "  4. 'N conseils pour …' — ex. '8 conseils pour bien choisir son avocat'\n"
    "  5. 'N critères / fonctionnalités pour …' — ex. '6 critères pour choisir un avocat'\n"
    "  6. 'N étapes pour …' — ex. '5 étapes pour trouver le bon avocat'\n\n"
    "INTERDIT — ne générez PAS ces cadres :\n"
    "  ✗ 'Les N meilleurs X' / 'Top N X' / 'N meilleurs fournisseurs / cabinets / produits'\n"
    "  ✗ Listes dont les items doivent être des marques ou entreprises réelles\n"
    "  ✗ 'Comparatif des meilleurs …' (implique de la recherche fournisseurs)\n\n"
    "Le chiffre N (entre 5 et 12) figure dans le titre ('7', '10', etc.).\n"
    "Les qualificatifs commerciaux interdits par les règles de titre restent\n"
    "interdits — le format listicle est éditorial, pas promotionnel.\n"
)


def _build_user_prompt(
    *,
    target_url: str,
    target_keyword: str,
    publishing_profile_payload: Optional[Dict[str, Any]],
    language: str,
    current_year: int,
    exclude_topics: Optional[List[str]] = None,
    prefer_listicle: bool = False,
) -> str:
    publisher_summary = _summarise_publisher_profile(publishing_profile_payload or {})
    exclude_block = _format_exclude_topics(exclude_topics, language)
    listicle_block = ""
    if prefer_listicle:
        listicle_block = (_LISTICLE_DIRECTIVE_FR if language == "fr" else _LISTICLE_DIRECTIVE_DE) + "\n"
    if language == "fr":
        return (
            f"SITE CIBLE : {target_url}\n"
            f"MOT-CLÉ CIBLE : {target_keyword}\n"
            f"ANNÉE EN COURS : {current_year}\n\n"
            f"PROFIL ÉDITEUR\n==============\n{publisher_summary}\n"
            f"{exclude_block}\n"
            f"{listicle_block}"
            f"Proposez les angles maintenant."
        )
    # default DE
    return (
        f"ZIEL-WEBSITE: {target_url}\n"
        f"ZIEL-KEYWORD: {target_keyword}\n"
        f"AKTUELLES JAHR: {current_year}\n\n"
        f"PUBLISHER-PROFIL\n================\n{publisher_summary}\n"
        f"{exclude_block}\n"
        f"{listicle_block}"
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


def _clean_text_field(value: Any) -> str:
    """Strip + decode HTML entities the LLM sometimes injects into JSON strings.
    Sonnet occasionally emits `&amp;` / `&lt;` etc. in titles because the field
    will end up in HTML — we want raw text here and re-escape later if needed.
    """

    return html.unescape(str(value or "").strip())


def _parse_angles(payload: Dict[str, Any]) -> List[EditorialAngle]:
    raw_angles = payload.get("angles") if isinstance(payload, dict) else None
    if not isinstance(raw_angles, list):
        return []
    out: List[EditorialAngle] = []
    for item in raw_angles:
        if not isinstance(item, dict):
            continue
        title = _clean_text_field(item.get("title"))
        keyword = _clean_text_field(item.get("target_keyword")).lower()
        hook = _clean_text_field(item.get("hook"))
        rationale = _clean_text_field(item.get("rationale"))
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
        title = _clean_text_field(item.get("title"))
        keyword = _clean_text_field(item.get("target_keyword")).lower()
        if title and keyword:
            out.append(EditorialAngle(
                title=title,
                target_keyword=keyword,
                hook=_clean_text_field(item.get("hook")),
                rationale=_clean_text_field(item.get("rationale")),
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
    prefer_listicle: bool = False,
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

    # On a cache miss we ALWAYS generate the full batch (BRAINSTORM_BATCH_SIZE)
    # and cache it. The caller's `num_angles` only controls how many filtered
    # angles get returned to them on this call -- subsequent calls draw the
    # remaining ranked angles from the cache without paying the LLM cost again.
    batch_size = max(num_angles, BRAINSTORM_BATCH_SIZE)
    system_template = _LANGUAGE_SYSTEM_PROMPTS.get(lang) or _LANGUAGE_SYSTEM_PROMPTS["de"]
    system_prompt = system_template.format(n=batch_size)
    user_prompt = _build_user_prompt(
        target_url=target_url,
        target_keyword=keyword,
        publishing_profile_payload=publishing_profile_payload,
        language=lang,
        current_year=year,
        exclude_topics=exclude_topics,
        prefer_listicle=prefer_listicle,
    )

    try:
        payload = call_llm_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            api_key=resolved_api_key,
            base_url=base_url,
            model=model,
            timeout_seconds=timeout_seconds,
            max_tokens=DEFAULT_MAX_TOKENS,
            temperature=0.9,  # higher for more creative variety
            request_label="topic_brainstorm",
        )
    except LLMError as exc:
        logger.warning("topic_brainstorm.llm_failed err=%s", exc)
        return BrainstormResult(angles=[], cost_usd=0.0)

    parsed = _parse_angles(payload)
    # Defensive post-filter: if the LLM ignored the exclude list in the
    # prompt, drop dupes here so the caller never sees them.
    filtered, dropped = _filter_against_excludes(parsed, exclude_topics)
    final_angles = filtered[:num_angles]

    # Write-back cache: store the FULL ranked batch (unfiltered) so the next
    # call can pull the next-unused angle for the SAME site pair without
    # re-running the LLM. Cache TTL is 90 days; a different exclude list
    # at read time just slides further down the same ranked list.
    if cache_lookup_key and cache_locale and parsed:
        try:
            from .topic_brainstorm_cache import upsert_brainstorm
            upsert_brainstorm(
                lookup_key=cache_lookup_key,
                locale=cache_locale,
                payload=_serialize_angles_for_cache(parsed[:batch_size]),
            )
        except ImportError:
            pass

    # ~$0.06 for a 35-angle Sonnet 4.6 call (≈1.5K tokens in, ≈3.5K out).
    # Per-article cost amortises down as the batch is reused across orders.
    return BrainstormResult(
        angles=final_angles,
        cost_usd=0.06,
        cache_hit=False,
        excluded_count=dropped,
    )
