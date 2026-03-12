from __future__ import annotations

import datetime
import html
import hashlib
import json
import logging
import math
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from .llm import LLMError, call_llm_json, call_llm_text
from .trend_cache import (
    get_keyword_trend_cache_entry,
    get_keyword_trend_cache_family_entries,
    record_keyword_trend_cache_hit,
    upsert_keyword_trend_cache_entry,
)
from .validators import (
    count_h2,
    validate_backlink_placement,
    validate_word_count,
    word_count_from_html,
)
from .web import (
    extract_internal_links,
    extract_canonical_link,
    extract_meta_content,
    extract_page_title,
    fetch_url,
    sanitize_html,
)

logger = logging.getLogger("creator.pipeline")

DEFAULT_LLM_BASE_URL = "https://api.openai.com/v1"
DEFAULT_OPENAI_LLM_MODEL = "gpt-4.1-mini"
DEFAULT_ANTHROPIC_PLANNING_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_ANTHROPIC_WRITING_MODEL = "claude-sonnet-4-6"
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_HTTP_RETRIES = 0
DEFAULT_LEONARDO_BASE_URL = "https://cloud.leonardo.ai/api/rest/v1"
DEFAULT_LEONARDO_MODEL_ID = "1dd50843-d653-4516-a8e3-f0238ee453ff"
DEFAULT_IMAGE_WIDTH = 1024
DEFAULT_IMAGE_HEIGHT = 576
DEFAULT_POLL_SECONDS = 2
DEFAULT_POLL_TIMEOUT_SECONDS = 90
PHASE1_CACHE_PROMPT_VERSION = "v3"
PHASE2_CACHE_PROMPT_VERSION = "v3"
PAIR_FIT_PROMPT_VERSION = "v4"
DEFAULT_SITE_ANALYSIS_MAX_PAGES = 4
DEFAULT_SITE_ANALYSIS_PAGE_TEXT_CHARS = 1400
SEO_TITLE_MIN_CHARS = 45
SEO_TITLE_MAX_CHARS = 68
SEO_H1_MAX_CHARS = 96
SEO_DESCRIPTION_MIN_CHARS = 120
SEO_DESCRIPTION_MAX_CHARS = 160
SEO_SLUG_MAX_CHARS = 80
INTERNAL_LINK_ANCHOR_MIN_UNIQUE = 2

NEGATIVE_PROMPT = "text, watermark, logo, letters, UI, low quality, blurry, deformed"

STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "into", "your", "you", "our", "are", "was", "were",
    "what", "when", "where", "why", "how", "who", "about", "into", "out", "over", "under", "between", "while",
    "their", "they", "them", "these", "those", "also", "but", "not", "can", "will", "just", "than", "then",
    "such", "use", "using", "used", "more", "most", "some", "any", "each", "other", "its", "it's", "our", "we",
}

GERMAN_FUNCTION_WORDS = {
    "der", "die", "das", "und", "ist", "nicht", "mit", "auf", "von", "für", "im", "in", "den", "dem", "ein",
    "eine", "als", "auch", "bei", "zu", "des", "sich", "dass", "zum", "zur", "am", "an", "oder", "wie", "wird",
}

ENGLISH_FUNCTION_WORDS = {
    "the", "and", "is", "are", "with", "for", "this", "that", "to", "of", "in", "on", "as", "be", "by", "an",
    "or", "from", "it", "at", "we", "you", "your", "our", "has", "have", "was", "were", "will", "can",
}
PAIR_FIT_EXTRA_STOPWORDS = {
    "alle", "alles", "beim", "bereits", "beste", "besten", "diese", "diesem", "dieser", "dieses", "durch",
    "einfach", "einen", "einem", "einer", "eines", "erste", "erstes", "etwa", "fuer", "gegen", "genau",
    "haben", "hilfreiche", "ihr", "ihre", "ihren", "ihres", "jede", "jeder", "jedes", "jetzt", "kein", "keine",
    "mehr", "muss", "mussen", "nachdem", "naechste", "noch", "rund", "sehr", "sich", "sollte", "sollten",
    "thema", "themen", "unsere", "unter", "viele", "vielen", "vom", "warum", "was", "welche", "welcher",
    "welches", "wenn", "weiter", "weiterlesen", "wird", "wurden", "zeigt", "zeigen", "zwischen",
}
PAIR_FIT_BOILERPLATE_TOKENS = {
    "artikel", "beitrag", "beitraege", "blog", "cookie", "datenschutz", "entdecken", "forum", "hilfe", "home",
    "impressum", "jetzt", "kategorie", "kategorien", "login", "magazin", "mehr", "menu", "navigation", "news",
    "onlineshop",
    "online", "portal", "registrieren", "service", "shop", "start", "startseite", "suche", "tag", "tags",
    "uebersicht", "weiterlesen",
}
PAIR_FIT_PROMO_TOKENS = {
    "angebot", "angebote", "bestellen", "guenstig", "guenstige", "günstig", "günstige", "kaufen",
    "bestseller", "kollektion", "kollektionen", "komplettbrille", "komplettbrillen", "marke", "marken",
    "neu", "neuheit", "neuheiten", "preis", "preise", "rabatt", "sale", "shop", "sortiment",
    "sofort", "versand",
}
PAIR_FIT_INFORMATIONAL_CUES = {
    "alltag", "antworten", "anleitung", "aufpassen", "beachten", "checkliste", "einordnung", "erklaert", "hilfe",
    "hinweise", "orientierung", "praevention", "praxis", "ratgeber", "sicherheit", "schutz", "tipps", "wissen",
    "worauf",
}
PAIR_FIT_CONTEXT_KEYWORDS = {
    "health": {"arzt", "augen", "behandlung", "ernaehrung", "gesundheit", "koerper", "medizin", "praevention", "schutz", "sicht", "symptome", "therapie", "vorsorge"},
    "safety": {"absicherung", "sicherheit", "schutz", "uv", "vorsicht", "warnung", "warnzeichen", "praevention", "risiko"},
    "lifestyle": {"alltag", "ideen", "leben", "lifestyle", "mode", "ratgeber", "stil", "trends"},
    "family_life": {"alltag", "baby", "eltern", "familie", "familien", "haushalt", "kinder", "partnerschaft", "schwangerschaft"},
    "parenting": {"baby", "eltern", "erziehung", "familie", "kinder", "kleinkind", "schule", "schwangerschaft"},
    "home": {"garten", "haus", "haushalt", "wohnen", "wohnung"},
    "finance": {"budget", "finanzierung", "kosten", "preis", "preise", "sparen", "versicherung"},
    "education": {"bildung", "kita", "lernen", "schule", "wissen"},
    "wellbeing": {"balance", "entspannung", "mental", "ruhe", "stress", "wohlbefinden"},
    "mobility": {"auto", "fahrt", "mobil", "mobilitaet", "reise", "reisen", "unterwegs", "verkehr"},
    "outdoor": {"ausflug", "draussen", "freizeit", "natur", "outdoor", "reise", "reisen", "sommer", "sonne", "urlaub"},
    "productivity": {"organisation", "planung", "produktiv", "routine", "workflow"},
    "beauty": {"beauty", "haut", "kosmetik", "pflege", "stil"},
    "shopping": {"angebot", "bestellen", "kaufen", "marke", "preis", "preise", "produkt", "produkte", "shop", "vergleich"},
}
PAIR_FIT_CONTEXT_LABELS = {
    "health": "gesundheitlichen Fragen",
    "safety": "Sicherheit im Alltag",
    "lifestyle": "alltagsnahen Entscheidungen",
    "family_life": "Familienalltag",
    "parenting": "Elternalltag",
    "home": "Zuhause",
    "finance": "Kosten und Entscheidungen",
    "education": "Orientierung und Lernen",
    "wellbeing": "Wohlbefinden",
    "mobility": "unterwegs",
    "outdoor": "Aktivitaeten im Freien",
    "productivity": "Organisation im Alltag",
    "beauty": "Pflege und Stil",
    "shopping": "Auswahl und Orientierung",
}
PAIR_FIT_CONTEXT_AUDIENCES = {
    "family_life": "Eltern und Familien",
    "parenting": "Eltern und Bezugspersonen",
    "education": "Lernende und Familien",
    "health": "gesundheitsbewusste Leserinnen und Leser",
    "wellbeing": "achtsame Leserinnen und Leser",
    "home": "Haushalte",
    "finance": "preisbewusste Leserinnen und Leser",
    "mobility": "Menschen unterwegs",
    "outdoor": "aktive Leserinnen und Leser",
    "beauty": "pflegebewusste Leserinnen und Leser",
    "productivity": "organisierte Teams und Einzelpersonen",
}
PAIR_FIT_AUDIENCE_TOKENS = {
    "baby", "babys", "eltern", "familie", "familien", "kinder", "kundinnen", "kunden", "leser",
    "leserinnen", "menschen", "nutzer", "patienten", "schueler", "schwangere", "teams",
}
PAIR_FIT_CANDIDATE_COUNT = 5

GENERIC_CONCLUSION_PHRASES = (
    "this article has examined the key factors",
    "the evidence presented demonstrates",
    "further investigation and analysis remain necessary",
    "moving forward, stakeholders must prioritize",
    "ultimately, a multifaceted approach",
    "addressing the challenges and opportunities presented by this subject matter",
)
GENERIC_BODY_PHRASES = (
    "in der heutigen zeit",
    "spielt eine wichtige rolle",
    "es ist wichtig zu beachten",
    "laesst sich festhalten",
    "ganzheitlicher ansatz",
    "verschiedene aspekte",
    "zahlreiche moeglichkeiten",
    "im fokus steht",
    "abschliessend laesst sich sagen",
)

TITLE_FILLER_PHRASES = {
    "vergleich und orientierung",
    "tipps und orientierung",
    "wichtige hinweise und orientierung",
    "vergleich kosten und tipps",
    "checkliste und tipps",
}
TITLE_WEAK_TAIL_PHRASES = {
    "wichtige hinweise",
    "erste orientierung",
    "mehr orientierung",
    "kompakte orientierung",
}
HEADING_FILLER_PHRASES = {
    "wichtige kriterien unterschiede und qualitaetsmerkmale",
    "wichtige hintergruende kriterien und einordnung",
    "praktische tipps und alltagsnahe orientierung",
    "vergleich und ueberblick",
}
PROMOTIONAL_BACKLINK_PHRASES = (
    "ist der richtige partner",
    "ist ihr partner",
    "bietet genau das",
    "bietet genau diese",
    "ist ihr anbieter",
    "vereinbaren sie jetzt",
    "jetzt vereinbaren",
    "onlineshop",
    "shop fuer",
    "ihr onlineshop",
)
COMMERCIAL_INVESTIGATION_CUES = {
    "auswahl",
    "bewertung",
    "checkliste",
    "kriterien",
    "kosten",
    "lohnt",
    "preis",
    "preise",
    "vergleich",
    "unterschiede",
}
TRANSACTIONAL_CUES = {
    "anfrage",
    "beantragen",
    "beauftragen",
    "bestellen",
    "buchen",
    "kauf",
    "kaufen",
    "kuendigen",
    "mieten",
    "reservieren",
    "verkauf",
    "verkaufen",
}
NAVIGATIONAL_CUES = {
    "adresse",
    "anfahrt",
    "kontakt",
    "login",
    "oeffnungszeiten",
    "portal",
    "standort",
}
PROCESS_ACTION_TOKENS = {
    "beantragen",
    "beauftragen",
    "checkliste",
    "kuendigen",
    "organisieren",
    "planen",
    "pruefen",
    "schritte",
    "umbauen",
    "umsetzen",
    "verkaufen",
    "vorbereiten",
}
TOPIC_CLASS_KEYWORDS = {
    "real_estate": {"immobilie", "immobilien", "haus", "makler", "miete", "mieten", "verkauf", "verkaufen", "wohnung"},
    "health_parenting": {"arzt", "augen", "baby", "eltern", "familie", "familien", "gesundheit", "kinder", "schutz", "vorsorge"},
    "nutrition_supplements": {
        "aminosaeure", "collagen", "dosierung", "eiweiss", "ernaehrung", "greens", "inhaltsstoffe",
        "kollagen", "kollagenpraeparate", "kollagenpräparate", "kreatin", "mineralstoff",
        "nahrungsergaenzungsmittel", "nahrungsergänzungsmittel", "omega", "protein", "pulver",
        "shake", "superfood", "supplement", "supplements", "vegan", "vitamin",
    },
    "product_service": {"auswahl", "kategorie", "material", "modell", "modelle", "produkt", "produkte", "qualitaet", "vergleich"},
    "finance_legal": {"finanzierung", "frist", "gesetz", "kosten", "provision", "recht", "steuer", "vertrag", "zins"},
}
SPECIFICITY_SIGNAL_BUCKETS = {
    "real_estate": {
        "market_context": {"lage", "markt", "marktwert", "nachfrage", "preis", "preise", "stadtteil", "zins"},
        "documents_process": {"besichtigung", "energieausweis", "expose", "grundbuch", "notar", "unterlagen", "vertrag", "wertermittlung"},
        "buyer_context": {"familien", "interessenten", "kaeufer", "kapitalanleger", "verkaeufer"},
    },
    "health_parenting": {
        "standards_safety": {"ce", "iso", "kategorie", "klasse", "norm", "risiko", "schutzklasse", "uv", "uv400"},
        "age_use_case": {"alltag", "babys", "gebirge", "kinder", "kleinkinder", "schulkinder", "sommer", "strand", "urlaub"},
        "decision_criteria": {"bruchsicher", "groesse", "komfort", "material", "passform", "sitz", "schutz", "gewicht"},
    },
    "nutrition_supplements": {
        "product_formulation": {
            "aminosaeure", "bioverfuegbarkeit", "darreichungsform", "dosierung", "inhaltsstoffe", "portion",
            "protein", "rohstoffe", "taegliche", "tagesdosis", "vitamin", "wirkstoff",
        },
        "quality_signals": {
            "allergen", "bio", "deklaration", "gmp", "labor", "labortest", "reinheit", "siegel", "vegan", "zertifizierung",
        },
        "cost_use_cases": {
            "abo", "alltag", "kapseln", "kosten", "monat", "preis", "preise", "pulver", "sticks", "vergleich",
        },
    },
    "product_service": {
        "criteria_specs": {"kategorie", "klasse", "material", "modell", "passform", "preis", "preise", "qualitaet", "schutz"},
        "use_cases": {"alltag", "einsatz", "ferien", "praxis", "sport", "strand", "urlaub", "vergleich"},
        "standards_rules": {"ce", "iso", "klasse", "norm", "prozent", "uv", "uv400"},
    },
    "finance_legal": {
        "rules_terms": {"frist", "gesetz", "laufzeit", "nachweis", "regel", "steuer", "vertrag"},
        "costs_conditions": {"finanzierung", "kosten", "preis", "preise", "provision", "zins"},
        "scenarios": {"alltag", "familien", "haushalte", "kaeufer", "verkaeufer"},
    },
    "general": {
        "criteria": {"auswahl", "entscheidung", "kriterien", "pruefen", "vergleich"},
        "use_cases": {"alltag", "beispiel", "einsatz", "praxis", "situation"},
        "next_steps": {"hilfe", "naechste", "schritte", "umsetzen", "vorbereiten"},
    },
}
PLAN_QUALITY_MIN_SCORE = 72
ARTICLE_QUALITY_MIN_SCORE = 70
ARTICLE_MAX_SPAM_RISK_SCORE = 35

KEYWORD_MIN_SECONDARY = 3
KEYWORD_MAX_SECONDARY = 6
KEYWORD_MAX_SEMANTIC_ENTITIES = 8
KEYWORD_MAX_SUPPORT_TOPICS = 6
KEYWORD_MIN_WORDS = 2
KEYWORD_MAX_WORDS = 8
KEYWORD_QUERY_MAX_WORDS = 6
KEYWORD_OVERUSE_MIN_OCCURRENCES = 12
DEFAULT_INTERNAL_LINK_MIN = 2
DEFAULT_INTERNAL_LINK_MAX = 4
KEYWORD_MAX_FAQ = 5
ARTICLE_MIN_H2 = 4
ARTICLE_MAX_H2 = 6
ARTICLE_SECTION_MIN_WORDS = 55
GOOGLE_SUGGEST_CACHE_TTL_SECONDS = 6 * 60 * 60
GOOGLE_SUGGEST_CACHE_MAX_ENTRIES = 256
DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
FAQ_MIN_QUESTIONS = 3
FAQ_MIN_WORDS = 80
ARTICLE_MIN_WORDS = 500
ARTICLE_MAX_WORDS = 1200
KEYWORD_LOW_SIGNAL_TOKENS = {
    "aktuell", "aktuelle", "aktuellen", "allgemein", "beitrag", "beitraege", "beliebt", "beliebte",
    "entdecken", "ganze", "hilfe", "hilfreich", "infos", "magazin", "mehr", "ratgeber", "spannend", "spannende",
    "thema", "themen", "tipps", "wissen", "wertvolle", "amp", "ideen", "richtig", "fuer", "jeden", "wirklich",
}

GERMAN_KEYWORD_MODIFIERS = (
    "tipps",
    "ratgeber",
    "checkliste",
    "hilfe",
    "erfahrungen",
    "ursachen",
    "auswirkungen",
)
TRAILING_TITLE_STOPWORDS = {
    "am", "an", "auf", "aus", "bei", "das", "dem", "den", "der", "des", "die", "ein", "eine", "einem",
    "einen", "einer", "eines", "fuer", "im", "in", "mit", "oder", "ueber", "und", "von", "wie", "zu",
    "zum", "zur",
}
GENERIC_UI_CHROME_TOKENS = {
    "anmelden", "checkout", "filtern", "filter", "konto", "kasse", "merkzettel", "sortieren",
    "sortierung", "suche", "warenkorb", "wunschliste",
}
EDITORIAL_ACTION_TOKENS = {
    "achten", "auswaehlen", "erkennen", "gelingt", "hilft", "kaufen", "lohnt", "nutzen", "pruefen",
    "reagieren", "steigert", "vergleichen", "verstehen", "zaehlt",
}
ABSTRACT_QUERY_TOKENS = {
    "anzeichen", "aspekte", "ausloeser", "chancen", "fehler", "fragen", "hinweise", "kriterien",
    "orientierung", "signale", "schritte", "ueberblick", "ursachen", "warnzeichen",
}
GENERIC_TOPIC_CATEGORY_TOKENS = {
    "beauty", "familie", "family", "finance", "gesundheit", "health", "lifestyle", "mode", "shopping",
    "stil", "produkt", "produkte", "wellness", "wohnen",
}
QUESTION_LEAD_HELPER_TOKENS = {
    "am", "an", "auf", "bei", "das", "dem", "den", "der", "des", "die", "ein", "eine", "einem", "einen",
    "einer", "eines", "fuer", "für", "ihr", "ihre", "im", "in", "man", "mein", "meine", "meinem", "meinen",
    "meiner", "mit", "sich", "und", "unser", "unsere", "vom", "von", "zum", "zur",
}
QUESTION_LEAD_VERB_TOKENS = {
    "bleibt", "braucht", "brauchen", "empfiehlt", "empfehlen", "erkennt", "erkennen", "gelingt", "helfen", "hilft",
    "ist", "sind", "kann", "koennen", "können", "kostet", "kosten", "laesst", "lässt", "lassen", "lohnt", "lohnen",
    "macht", "machen", "passt", "prueft", "prüft", "soll", "sollte", "sollten", "waehlt", "wählt", "wird", "werden",
}
QUESTION_TRAILING_LOW_SIGNAL_TOKENS = {"eigentlich", "heute", "jetzt", "noch", "richtig", "sinnvoll", "wirklich"}
QUESTION_FOCUS_PRIORITY_TOKENS = {"kosten", "kostet", "preis", "preise", "vergleich", "dosierung", "dosis", "tagesdosis"}
EDITORIAL_NOISE_TOKENS = {"amp"}
EDITORIAL_GREETING_PREFIXES = (
    "herzlich willkommen",
    "willkommen",
)
SELF_ASSESSMENT_QUESTION_FRAGMENTS = (
    " bin ich",
    " passt zu mir",
    " zu mir passt",
    " steht mir",
    " ist mein typ",
    " passt am besten zu mir",
)
OUTLINE_PROBLEM_TOKENS = {
    "anzeichen", "behandlung", "diagnose", "erkennen", "hilfe", "problem", "probleme", "risiko",
    "risiken", "symptome", "therapie", "ursachen", "warnzeichen", "wann",
}
OUTLINE_DECISION_TOKENS = {
    "auswahl", "checkliste", "kauf", "kaufen", "kriterien", "material", "modell", "passform",
    "preis", "preise", "qualitaet", "schutzklasse", "test", "vergleich",
}

GERMAN_QUESTION_PREFIXES = (
    "was",
    "was ist",
    "wie",
    "wann",
    "warum",
    "welche",
    "welcher",
    "welches",
    "wo",
    "woran",
    "kann",
    "darf",
)

GOOGLE_SUGGEST_CACHE: Dict[str, Dict[str, Any]] = {}
STRUCTURED_LIST_HINTS = {"tipps", "checkliste", "schritte", "anleitung", "symptome", "ursachen"}
STRUCTURED_TABLE_HINTS = {"vergleich", "kosten", "unterschied", "vs", "tabelle"}
TOPIC_SUFFIX_MODIFIERS = set(GERMAN_KEYWORD_MODIFIERS) | {"ideen", "fragen"}
TOPIC_SIGNATURE_EXCLUDED_TOKENS = set(GERMAN_QUESTION_PREFIXES) | {
    "braucht", "mein", "meine", "meinem", "meinen", "rund", "sein", "seine", "seinem", "seinen",
    "um", "unser", "unsere", "unserem", "unseren", "verstehen",
}
INTERNAL_LINK_GENERIC_TOKENS = {
    "alltag", "eltern", "familie", "familien", "gesundheit", "ideen", "kind", "kinder", "kindern", "leben",
    "ratgeber", "tipps", "zuhause", "familien4leben", "glueck", "teilen",
}
INTERNAL_LINK_WEAK_MATCH_TOKENS = {
    "alltag", "auswahl", "beratung", "checkliste", "familie", "familien", "finden", "gesundheit", "hilfe",
    "ideen", "kauf", "kaufen", "kosten", "online", "praxis", "ratgeber", "schritte", "schutz", "service",
    "sommer", "strand", "tipps", "urlaub", "vergleich", "wahl",
}


class CreatorError(RuntimeError):
    def __init__(self, message: str, *, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _read_non_negative_int_env(name: str, default: int) -> int:
    return max(0, _read_int_env(name, default))


def _read_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        return float(raw)
    except ValueError:
        return default


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _build_pipeline_execution_policy() -> Dict[str, Any]:
    strict_failure_mode = _read_bool_env("CREATOR_STRICT_FAILURE_MODE", False)
    phase5_max_attempts = 1 if strict_failure_mode else max(1, _read_int_env("CREATOR_PHASE5_MAX_ATTEMPTS", 2))
    phase7_repair_attempts = 0 if strict_failure_mode else max(0, _read_int_env("CREATOR_PHASE7_REPAIR_ATTEMPTS", 1))
    return {
        "strict_failure_mode": strict_failure_mode,
        "phase4_outline_fallback_enabled": not strict_failure_mode,
        "phase5_max_attempts": phase5_max_attempts,
        "phase5_expand_passes": max(0, phase5_max_attempts - 1),
        "phase5_faq_enrichment_soft_fail": not strict_failure_mode,
        "phase6_image_soft_fail": not strict_failure_mode,
        "phase7_keyword_context_repair_enabled": not strict_failure_mode,
        "phase7_repair_attempts": phase7_repair_attempts,
    }


def _normalize_url(url: str) -> str:
    cleaned = (url or "").strip()
    parsed = urlparse(cleaned)
    if not parsed.scheme or not parsed.netloc:
        return cleaned
    normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path or ''}"
    return normalized.rstrip("/")


def _hash_text(text: str) -> str:
    return hashlib.sha256((text or "").strip().encode("utf-8")).hexdigest()


def _limit_text(value: str, max_chars: int) -> str:
    cleaned = re.sub(r"\s+", " ", (value or "").strip())
    if max_chars <= 0 or len(cleaned) <= max_chars:
        return cleaned
    clipped = cleaned[:max_chars].rsplit(" ", 1)[0].strip()
    return clipped or cleaned[:max_chars].strip()


def _merge_string_lists(*value_lists: List[str], max_items: int) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for values in value_lists:
        for value in values:
            cleaned = re.sub(r"\s+", " ", str(value or "").strip())
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(cleaned)
            if len(out) >= max_items:
                return out
    return out


def _extract_inventory_categories(items: List[Dict[str, Any]], *, max_items: int = 10) -> List[str]:
    categories: List[str] = []
    for item in items:
        raw = item.get("categories")
        if not isinstance(raw, list):
            continue
        categories.extend(str(category).strip() for category in raw if str(category).strip())
    return _merge_string_lists(categories, max_items=max_items)


def _extract_inventory_topic_clusters(items: List[Dict[str, Any]], *, max_items: int = 8) -> List[str]:
    scores: Dict[str, int] = {}
    for item in items:
        title = _normalize_keyword_phrase(str(item.get("title") or ""))
        categories = [str(value).strip() for value in (item.get("categories") or []) if str(value).strip()]
        for category in categories:
            normalized_category = _normalize_keyword_phrase(category)
            if normalized_category:
                scores[normalized_category] = scores.get(normalized_category, 0) + 4
        words = title.split()
        for size in (2, 3):
            for index in range(0, max(0, len(words) - size + 1)):
                phrase = " ".join(words[index : index + size]).strip()
                if not _is_valid_keyword_phrase(phrase):
                    continue
                scores[phrase] = scores.get(phrase, 0) + (3 if size == 2 else 2)
    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    return [phrase for phrase, _score in ranked[:max_items]]


def _build_inventory_topic_insights(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    categories = _extract_inventory_categories(items)
    topic_clusters = _extract_inventory_topic_clusters(items)
    prominent_titles = _merge_string_lists(
        [str(item.get("title") or "").strip() for item in items if str(item.get("title") or "").strip()],
        max_items=8,
    )
    internal_linking_opportunities = []
    for item in items[:8]:
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        categories_for_item = [str(value).strip() for value in (item.get("categories") or []) if str(value).strip()]
        if categories_for_item:
            internal_linking_opportunities.append(f"{categories_for_item[0]} -> {title}")
        else:
            internal_linking_opportunities.append(title)
    return {
        "site_categories": categories,
        "topic_clusters": topic_clusters,
        "prominent_titles": prominent_titles,
        "internal_linking_opportunities": _merge_string_lists(internal_linking_opportunities, max_items=10),
    }


def _structured_content_mode(topic: str, primary_keyword: str, search_intent_type: str) -> str:
    normalized = _normalize_keyword_phrase(f"{topic} {primary_keyword}")
    tokens = set(normalized.split())
    if tokens & STRUCTURED_TABLE_HINTS or (search_intent_type or "").strip().lower() == "commercial_investigation":
        return "table"
    if tokens & STRUCTURED_LIST_HINTS:
        return "list"
    return "none"


def _tokens_have_problem_signal(tokens: set[str]) -> bool:
    return bool(
        tokens & OUTLINE_PROBLEM_TOKENS
        or any(token.endswith(("probleme", "symptome", "warnzeichen", "ursachen")) for token in tokens)
    )


def _tokens_have_decision_signal(tokens: set[str]) -> bool:
    return bool(
        tokens & OUTLINE_DECISION_TOKENS
        or any(token.endswith(("schutzklasse", "vergleich", "preise")) for token in tokens)
    )


def _infer_search_intent_type(*, topic: str, target_profile: Dict[str, Any]) -> str:
    normalized_topic = _normalize_keyword_phrase(topic)
    tokens = _keyword_token_set(
        " ".join(
            [
                normalized_topic,
                " ".join(str(item).strip() for item in (target_profile.get("services_or_products") or []) if str(item).strip()),
                str(target_profile.get("page_title") or "").strip(),
            ]
        )
    )
    if _looks_like_question_phrase(normalized_topic) or _tokens_have_problem_signal(tokens):
        return "informational"
    navigational_score = len(tokens & NAVIGATIONAL_CUES)
    transactional_score = len(tokens & TRANSACTIONAL_CUES)
    commercial_score = len(tokens & COMMERCIAL_INVESTIGATION_CUES)
    if navigational_score >= 2:
        return "navigational"
    if transactional_score >= 2 and commercial_score == 0:
        return "transactional"
    if _tokens_have_decision_signal(tokens) or commercial_score >= 2:
        return "commercial_investigation"
    return "informational"


def _infer_topic_class(
    *,
    topic: str,
    target_profile: Optional[Dict[str, Any]] = None,
    publishing_profile: Optional[Dict[str, Any]] = None,
    content_brief: Optional[Dict[str, Any]] = None,
) -> str:
    combined = " ".join(
        [
            str(topic or "").strip(),
            " ".join(str(item).strip() for item in ((target_profile or {}).get("topics") or []) if str(item).strip()),
            " ".join(str(item).strip() for item in ((target_profile or {}).get("services_or_products") or []) if str(item).strip()),
            " ".join(str(item).strip() for item in ((publishing_profile or {}).get("topics") or []) if str(item).strip()),
            " ".join(str(item).strip() for item in ((content_brief or {}).get("target_signals") or []) if str(item).strip()),
            " ".join(str(item).strip() for item in ((content_brief or {}).get("publishing_signals") or []) if str(item).strip()),
        ]
    )
    tokens = _keyword_focus_tokens(combined)
    scored = sorted(
        (
            len(tokens & keywords),
            topic_class,
        )
        for topic_class, keywords in TOPIC_CLASS_KEYWORDS.items()
    )
    best_score, best_class = max(scored, key=lambda item: (item[0], item[1])) if scored else (0, "general")
    return best_class if best_score > 0 else "general"


def _infer_article_angle(
    *,
    topic: str,
    intent_type: str,
    structured_mode: str,
    topic_class: str,
    topic_signature: Optional[Dict[str, Any]] = None,
) -> str:
    tokens = _keyword_focus_tokens(
        " ".join(
            [
                str(topic or "").strip(),
                " ".join(str(item).strip() for item in ((topic_signature or {}).get("support_phrases") or []) if str(item).strip()),
                " ".join(str(item).strip() for item in ((topic_signature or {}).get("target_terms") or []) if str(item).strip()),
            ]
        )
    )
    if intent_type == "navigational":
        return "resource_navigation"
    if intent_type == "transactional" or tokens & PROCESS_ACTION_TOKENS:
        return "process_and_next_steps" if topic_class != "real_estate" else "process_and_decision_factors"
    if _tokens_have_problem_signal(tokens) or _looks_like_question_phrase(topic):
        return "recognition_and_next_steps"
    if structured_mode == "table" or intent_type == "commercial_investigation" or _tokens_have_decision_signal(tokens):
        return "decision_criteria"
    return "practical_guidance"


def _build_style_profile(
    *,
    topic: str,
    topic_class: str,
    intent_type: str,
    article_angle: str,
    content_brief: Optional[Dict[str, Any]] = None,
    publishing_profile: Optional[Dict[str, Any]] = None,
    target_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    audience = str((content_brief or {}).get("audience") or "").strip()
    tone = str((publishing_profile or {}).get("content_tone") or "").strip() or "sachlich"
    style_cues = _merge_string_lists(
        [str(item).strip() for item in ((content_brief or {}).get("style_cues") or []) if str(item).strip()],
        [str(item).strip() for item in ((publishing_profile or {}).get("content_style") or []) if str(item).strip()],
        max_items=4,
    )
    topic_focus_tokens = _filter_topic_signature_tokens(
        _keyword_focus_tokens(f"{_build_topic_phrase(topic)} {_extract_topic_question_phrase(topic)}")
    )
    target_context: List[str] = []
    for raw_value in _merge_string_lists(
        [str(item).strip() for item in ((content_brief or {}).get("target_signals") or []) if str(item).strip()],
        [str(item).strip() for item in ((target_profile or {}).get("topics") or []) if str(item).strip()],
        max_items=8,
    ):
        normalized = _sanitize_editorial_phrase(raw_value, allow_single_token=True)
        if not normalized:
            continue
        normalized_tokens = _keyword_focus_tokens(normalized)
        if not normalized_tokens:
            continue
        if len(normalized_tokens & (PAIR_FIT_BOILERPLATE_TOKENS | PAIR_FIT_PROMO_TOKENS | GENERIC_UI_CHROME_TOKENS)) >= max(1, len(normalized_tokens) - 1):
            continue
        if _phrase_has_same_family_duplicate_tokens(normalized):
            continue
        family_topic_overlap = {
            token
            for token in normalized_tokens
            if _token_matches_reference_family(token, topic_focus_tokens, allow_prefix_match=False)
        }
        aligned_topic_tokens = (normalized_tokens & topic_focus_tokens) | family_topic_overlap
        off_topic_tokens = normalized_tokens - aligned_topic_tokens - PAIR_FIT_INFORMATIONAL_CUES
        if len(normalized_tokens) >= 3 and len(off_topic_tokens) >= max(2, len(normalized_tokens) - 1):
            continue
        if topic_focus_tokens and not (
            aligned_topic_tokens
            or _keyword_similarity(normalized, topic) >= 0.32
        ):
            continue
        target_context.append(normalized)
        if len(target_context) >= 4:
            break
    return {
        "topic_class": topic_class,
        "intent_type": intent_type,
        "article_angle": article_angle,
        "audience": audience,
        "tone": tone,
        "style_cues": style_cues,
        "target_context": target_context,
    }


def _build_specificity_profile(
    *,
    topic: str,
    topic_class: str,
    intent_type: str,
) -> Dict[str, Any]:
    raw_buckets = SPECIFICITY_SIGNAL_BUCKETS.get(topic_class) or SPECIFICITY_SIGNAL_BUCKETS["general"]
    buckets = {
        str(bucket_name): sorted(str(token).strip() for token in bucket_tokens if str(token).strip())
        for bucket_name, bucket_tokens in raw_buckets.items()
    }
    min_specifics = 3 if topic_class in {"real_estate", "health_parenting", "nutrition_supplements", "product_service", "finance_legal"} else 2
    if intent_type in {"transactional", "commercial_investigation"}:
        min_specifics = max(min_specifics, 3)
    return {
        "topic": topic,
        "topic_class": topic_class,
        "intent_type": intent_type,
        "min_specifics": min(4, max(2, min_specifics)),
        "buckets": buckets,
    }


def _count_repeated_two_word_fragments(value: str) -> int:
    tokens = _normalize_keyword_phrase(value).split()
    counts: Dict[str, int] = {}
    for index in range(len(tokens) - 1):
        fragment = f"{tokens[index]} {tokens[index + 1]}".strip()
        if not fragment:
            continue
        counts[fragment] = counts.get(fragment, 0) + 1
    return max(counts.values(), default=0)


def _derive_title_support_clause(
    *,
    topic: str,
    intent_type: str,
    article_angle: str,
    topic_class: str,
    structured_mode: str,
) -> str:
    normalized_topic = _normalize_keyword_phrase(topic)
    if _looks_like_question_phrase(normalized_topic):
        return ""
    if article_angle == "recognition_and_next_steps":
        return "Woran man erste Hinweise erkennt"
    if article_angle in {"process_and_decision_factors", "process_and_next_steps"}:
        return "Welche Schritte und Fehler wirklich zählen"
    if article_angle == "decision_criteria":
        if topic_class in {"product_service", "real_estate"} or structured_mode == "table":
            return "Welche Kriterien bei der Auswahl wirklich zählen"
        return "Welche Kriterien wirklich zählen"
    if intent_type == "navigational":
        return "Die wichtigsten Informationen auf einen Blick"
    return "Welche Kriterien und nächsten Schritte wirklich zählen"


def _evaluate_title_quality(
    *,
    title: str,
    primary_keyword: str,
    topic: str,
    max_chars: int = SEO_TITLE_MAX_CHARS,
) -> Dict[str, Any]:
    normalized_title = _normalize_keyword_phrase(title)
    normalized_primary = _normalize_keyword_phrase(primary_keyword)
    normalized_topic = _normalize_keyword_phrase(topic)
    errors: List[str] = []
    score = 100
    if not normalized_title:
        return {"score": 0, "errors": ["title_missing"]}
    if len(title.strip()) < SEO_TITLE_MIN_CHARS or len(title.strip()) > max_chars:
        score -= 12
        errors.append("title_length_invalid")
    if any(phrase in normalized_title for phrase in TITLE_FILLER_PHRASES):
        score -= 28
        errors.append("title_filler_detected")
    if any(normalized_title.endswith(phrase) for phrase in TITLE_WEAK_TAIL_PHRASES):
        score -= 14
        errors.append("title_weak_tail")
    if normalized_primary and _count_keyword_occurrences(normalized_title, normalized_primary) > 1:
        score -= 24
        errors.append("title_keyword_stuffed")
    if _phrase_has_same_family_duplicate_tokens(normalized_title):
        score -= 28
        errors.append("title_family_duplicate_tokens")
    if _count_repeated_two_word_fragments(normalized_title) > 1:
        score -= 18
        errors.append("title_fragment_repeated")
    if normalized_primary and normalized_topic and _keyword_similarity(normalized_title, normalized_primary) >= 0.92 and len(normalized_title.split()) > 5:
        score -= 14
        errors.append("title_too_close_to_primary_keyword")
    if re.search(r"\b(?:und|oder)\b.*\b(?:und|oder)\b", normalized_title):
        score -= 6
    return {"score": max(0, score), "errors": _dedupe_string_values(errors)}


def _dedupe_recent_title_values(values: Optional[List[str]]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw_value in values or []:
        cleaned = " ".join(str(raw_value or "").split()).strip()
        if not cleaned:
            continue
        normalized = _normalize_keyword_phrase(cleaned)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(cleaned)
    return out


def _evaluate_title_history_novelty(
    *,
    title: str,
    recent_titles: Optional[List[str]],
) -> Dict[str, Any]:
    normalized_title = _normalize_keyword_phrase(title)
    if not normalized_title:
        return {"score_penalty": 0, "errors": [], "max_similarity": 0.0}
    history = _dedupe_recent_title_values(recent_titles)
    max_similarity = max(
        (_keyword_similarity(normalized_title, _normalize_keyword_phrase(item)) for item in history),
        default=0.0,
    )
    errors: List[str] = []
    score_penalty = 0
    title_word_count = len(normalized_title.split())
    if max_similarity >= 0.96:
        score_penalty = 65
        errors.append("title_duplicate_history")
    elif max_similarity >= 0.88 and title_word_count >= 4:
        score_penalty = 36
        errors.append("title_too_similar_to_history")
    elif max_similarity >= 0.82 and title_word_count >= 5:
        score_penalty = 16
    return {
        "score_penalty": score_penalty,
        "errors": _dedupe_string_values(errors),
        "max_similarity": round(max_similarity, 3),
    }


def _derive_title_support_clause_variants(
    *,
    topic: str,
    intent_type: str,
    article_angle: str,
    topic_class: str,
    structured_mode: str,
) -> List[str]:
    base_clause = _derive_title_support_clause(
        topic=topic,
        intent_type=intent_type,
        article_angle=article_angle,
        topic_class=topic_class,
        structured_mode=structured_mode,
    )
    candidates: List[str] = [base_clause]
    if article_angle == "recognition_and_next_steps":
        candidates.extend(
            [
                "Wie man Chancen und Risiken frueh erkennt",
                "Welche Signale in der Praxis wirklich zaehlen",
            ]
        )
    elif article_angle in {"process_and_decision_factors", "process_and_next_steps"}:
        candidates.extend(
            [
                "Welche Schritte zuerst wichtig sind",
                "Wie der Ablauf in der Praxis gelingt",
            ]
        )
    elif article_angle == "decision_criteria":
        candidates.extend(
            [
                "Worauf man bei der Auswahl achten sollte",
                "Was in der Praxis den Unterschied macht",
            ]
        )
    else:
        candidates.extend(
            [
                "Worauf es in der Praxis ankommt",
                "Was man konkret beachten sollte",
            ]
        )
    return _dedupe_string_values([item for item in candidates if str(item or "").strip()])


def _heading_generic_penalty(heading: str) -> int:
    normalized = _normalize_keyword_phrase(heading)
    penalty = 0
    if any(phrase in normalized for phrase in HEADING_FILLER_PHRASES):
        penalty += 24
    if normalized.startswith("worauf es bei ") and normalized.endswith(" im alltag ankommt"):
        penalty += 12
    if normalized.startswith("praktische tipps ") or normalized.startswith("wichtige kriterien "):
        penalty += 10
    if normalized.count(" und ") >= 2:
        penalty += 8
    return penalty


def _heading_is_natural_core_question(heading: str) -> bool:
    normalized = _normalize_keyword_phrase(heading)
    return any(
        normalized.startswith(prefix)
        for prefix in (
            "welche anzeichen",
            "welche warnzeichen",
            "welche symptome",
            "welche hinweise",
            "welche signale",
            "welche kriterien",
            "welche unterlagen",
            "welche unterschiede",
            "welche fehler",
            "welche ursachen",
            "welche schritte",
            "welche naechsten schritte",
            "welche weiteren aspekte",
            "woran erkennt man",
            "wann ist fachlicher rat",
            "wann lohnt sich",
            "wie laesst sich",
            "worauf kommt es",
        )
    )


def _evaluate_heading_quality(
    *,
    headings: List[str],
    topic_signature: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    errors: List[str] = []
    score = 100
    normalized_headings = [
        _normalize_keyword_phrase(heading)
        for heading in headings
        if _normalize_keyword_phrase(heading) not in {"fazit", "faq"}
    ]
    for heading in normalized_headings:
        penalty = _heading_generic_penalty(heading)
        if penalty:
            score -= penalty
            errors.append(f"heading_generic:{heading}")
        if _phrase_has_same_family_duplicate_tokens(heading):
            score -= 22
            errors.append(f"heading_family_duplicate_tokens:{heading}")
        if _phrase_has_editorial_noise(heading) and not _heading_is_natural_core_question(heading):
            score -= 18
            errors.append(f"heading_invalid:{heading}")
        if _keyword_candidate_is_support_topic_noise(heading, topic_signature) and not _heading_is_natural_core_question(heading):
            score -= 18
            errors.append(f"heading_support_topic_noise:{heading}")
        if not _topic_signature_candidate_has_relevance(heading, topic_signature) and not _heading_is_natural_core_question(heading):
            score -= 22
            errors.append(f"heading_topic_drift:{heading}")
    for index, heading in enumerate(normalized_headings):
        repeated = sum(
            1
            for other in normalized_headings[index + 1 :]
            if _keyword_similarity(heading, other) >= 0.82
        )
        if repeated:
            score -= 14 * repeated
            errors.append(f"heading_repetition:{heading}")
    return {"score": max(0, score), "errors": _dedupe_string_values(errors)}


def _evaluate_plan_intent_consistency(
    *,
    headings: List[str],
    intent_type: str,
    article_angle: str,
    topic_signature: Optional[Dict[str, Any]],
    specificity_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    errors: List[str] = []
    score = 100
    intent_family_map = {
        "navigational": NAVIGATIONAL_CUES,
        "transactional": TRANSACTIONAL_CUES,
        "commercial_investigation": COMMERCIAL_INVESTIGATION_CUES,
    }
    dominant_tokens = intent_family_map.get(intent_type, set())
    specificity_tokens = {
        token
        for bucket_tokens in ((specificity_profile or {}).get("buckets") or {}).values()
        if isinstance(bucket_tokens, (list, tuple, set))
        for token in bucket_tokens
        if str(token).strip()
    }
    conflicting_hits = 0
    for heading in headings:
        normalized = _normalize_keyword_phrase(heading)
        if normalized in {"fazit", "faq"}:
            continue
        heading_tokens = _keyword_token_set(normalized)
        if dominant_tokens and not (heading_tokens & dominant_tokens) and intent_type != "informational":
            has_topic_relevance = _topic_signature_candidate_has_relevance(normalized, topic_signature)
            has_specificity_support = bool(heading_tokens & specificity_tokens)
            has_process_support = article_angle in {"process_and_decision_factors", "process_and_next_steps"} and bool(
                heading_tokens & PROCESS_ACTION_TOKENS
            )
            is_natural_question = _heading_is_natural_core_question(normalized)
            if not (
                has_topic_relevance
                or has_specificity_support
                or has_process_support
                or (is_natural_question and (has_specificity_support or has_process_support))
            ):
                conflicting_hits += 1
        if article_angle == "process_and_decision_factors" and len(heading_tokens & {"mieten", "miete"}) >= 1:
            if len(_keyword_token_set(normalized) & {"verkaufen", "verkauf"}) == 0:
                conflicting_hits += 1
    if conflicting_hits:
        score -= conflicting_hits * 18
        errors.append("outline_mixed_intent_or_angle")
    return {"score": max(0, score), "errors": _dedupe_string_values(errors)}


def _evaluate_plan_quality(
    *,
    title: str,
    headings: List[str],
    primary_keyword: str,
    topic: str,
    intent_type: str,
    article_angle: str,
    topic_signature: Optional[Dict[str, Any]],
    specificity_profile: Optional[Dict[str, Any]] = None,
    recent_titles: Optional[List[str]] = None,
) -> Dict[str, Any]:
    title_eval = _evaluate_title_quality(
        title=title,
        primary_keyword=primary_keyword,
        topic=topic,
        max_chars=SEO_H1_MAX_CHARS,
    )
    title_history_eval = _evaluate_title_history_novelty(
        title=title,
        recent_titles=recent_titles,
    )
    adjusted_title_score = max(0, int(title_eval["score"]) - int(title_history_eval["score_penalty"]))
    heading_eval = _evaluate_heading_quality(headings=headings, topic_signature=topic_signature)
    intent_eval = _evaluate_plan_intent_consistency(
        headings=headings,
        intent_type=intent_type,
        article_angle=article_angle,
        topic_signature=topic_signature,
        specificity_profile=specificity_profile,
    )
    coherence_score = max(
        0,
        int(round((adjusted_title_score * 0.25) + (heading_eval["score"] * 0.45) + (intent_eval["score"] * 0.30))),
    )
    errors = _dedupe_string_values(
        list(title_eval["errors"])
        + list(title_history_eval["errors"])
        + list(heading_eval["errors"])
        + list(intent_eval["errors"])
    )
    if coherence_score < PLAN_QUALITY_MIN_SCORE:
        errors.append("plan_quality_below_threshold")
    return {
        "title_quality_score": adjusted_title_score,
        "heading_quality_score": heading_eval["score"],
        "intent_consistency_score": intent_eval["score"],
        "coherence_score": coherence_score,
        "title_history_similarity": title_history_eval["max_similarity"],
        "errors": _dedupe_string_values(errors),
    }


def _format_title_case(value: str) -> str:
    words = [word for word in re.split(r"\s+", (value or "").strip()) if word]
    out: List[str] = []
    for word in words:
        if word.isupper():
            out.append(word)
        else:
            out.append(word[:1].upper() + word[1:])
    return " ".join(out)


def _truncate_title(value: str, *, max_chars: int = SEO_TITLE_MAX_CHARS) -> str:
    def _clean_trailing_fragment(fragment: str) -> str:
        cleaned_fragment = fragment.strip(" -,:;/")
        words = cleaned_fragment.split()
        while len(words) > 2 and _normalize_keyword_phrase(words[-1]) in TRAILING_TITLE_STOPWORDS:
            words.pop()
        trimmed = " ".join(words).strip(" -,:;/")
        return trimmed or cleaned_fragment

    cleaned = re.sub(r"\s+", " ", (value or "").strip())
    if len(cleaned) <= max_chars:
        return _clean_trailing_fragment(cleaned)
    clipped = cleaned[:max_chars].rsplit(" ", 1)[0].strip()
    return _clean_trailing_fragment(clipped or cleaned[:max_chars].strip())


def _split_topic_segments(topic: str) -> List[str]:
    raw = re.sub(r"\s+", " ", str(topic or "").strip())
    if not raw:
        return []
    return [
        segment.strip(" -")
        for segment in re.split(r"\s*[:|]\s*", raw)
        if segment.strip(" -")
    ]


def _strip_trailing_topic_modifiers(words: List[str]) -> List[str]:
    trimmed = list(words)
    while len(trimmed) > 2 and trimmed[-1] in TOPIC_SUFFIX_MODIFIERS:
        trimmed.pop()
    return trimmed


def _clean_topic_segment_raw(segment: str, *, preserve_question: bool = False) -> str:
    raw = re.sub(r"\s+", " ", str(segment or "").strip()).strip(" -")
    if not raw:
        return ""
    question_mark = raw.endswith("?")
    raw = raw.rstrip("?").strip()
    words = raw.split()
    while len(words) > 2 and _normalize_keyword_phrase(words[-1]) in TOPIC_SUFFIX_MODIFIERS:
        words.pop()
    while len(words) > 2 and _normalize_keyword_phrase(words[-1]) in TRAILING_TITLE_STOPWORDS:
        words.pop()
    cleaned = " ".join(words).strip(" -")
    if not cleaned:
        return ""
    if preserve_question and (question_mark or _looks_like_question_phrase(cleaned)):
        return cleaned.rstrip("?").strip() + "?"
    return cleaned


def _extract_topic_subject_phrase(topic: str) -> str:
    for segment in _split_topic_segments(topic):
        cleaned = _clean_topic_segment_raw(segment)
        if cleaned and not _looks_like_question_phrase(cleaned):
            return cleaned
    normalized = _normalize_keyword_phrase(topic)
    words = _strip_trailing_topic_modifiers(normalized.split())
    if len(words) > KEYWORD_MAX_WORDS:
        words = words[:KEYWORD_MAX_WORDS]
    candidate = " ".join(words).strip()
    return candidate if candidate and not _looks_like_question_phrase(candidate) else ""


def _extract_topic_question_focus_phrase(topic: str) -> str:
    normalized_question = _normalize_keyword_phrase(_extract_topic_question_phrase(topic))
    if not normalized_question:
        return ""
    words = normalized_question.split()
    if not words:
        return ""
    if words[0] in GERMAN_QUESTION_PREFIXES:
        words = words[1:]
    leading_signal = ""
    while words and words[0] in (QUESTION_LEAD_HELPER_TOKENS | QUESTION_LEAD_VERB_TOKENS):
        if not leading_signal and words[0] in QUESTION_FOCUS_PRIORITY_TOKENS:
            leading_signal = "kosten" if words[0] in {"kostet", "kosten"} else words[0]
        words = words[1:]
    filtered_words = [
        word
        for word in words
        if word not in QUESTION_LEAD_HELPER_TOKENS
        and word not in QUESTION_TRAILING_LOW_SIGNAL_TOKENS
        and word not in GERMAN_QUESTION_PREFIXES
    ]
    while len(filtered_words) > 2 and filtered_words[-1] in (QUESTION_TRAILING_LOW_SIGNAL_TOKENS | TOPIC_SUFFIX_MODIFIERS | TRAILING_TITLE_STOPWORDS):
        filtered_words.pop()
    if leading_signal and leading_signal not in filtered_words:
        filtered_words.append(leading_signal)
    if len(filtered_words) > KEYWORD_QUERY_MAX_WORDS:
        if leading_signal and filtered_words[-1] == leading_signal:
            filtered_words = filtered_words[: KEYWORD_QUERY_MAX_WORDS - 1] + [leading_signal]
        else:
            filtered_words = filtered_words[:KEYWORD_QUERY_MAX_WORDS]
    candidate = " ".join(filtered_words).strip()
    return candidate if _is_valid_keyword_phrase(candidate) else ""


def _topic_phrase_is_generic(value: str) -> bool:
    focus_tokens = _filter_keyword_focus_tokens(_keyword_focus_tokens(value))
    if not focus_tokens:
        return True
    domain_tokens = focus_tokens - ABSTRACT_QUERY_TOKENS - EDITORIAL_ACTION_TOKENS - GENERIC_TOPIC_CATEGORY_TOKENS
    return not domain_tokens and bool(focus_tokens & GENERIC_TOPIC_CATEGORY_TOKENS)


def _topic_phrase_specificity_score(value: str) -> tuple[int, int, int, int, int]:
    focus_tokens = _filter_keyword_focus_tokens(_keyword_focus_tokens(value))
    domain_tokens = focus_tokens - ABSTRACT_QUERY_TOKENS - EDITORIAL_ACTION_TOKENS - GENERIC_TOPIC_CATEGORY_TOKENS
    generic_tokens = focus_tokens & GENERIC_TOPIC_CATEGORY_TOKENS
    priority_tokens = focus_tokens & QUESTION_FOCUS_PRIORITY_TOKENS
    return (
        len(domain_tokens),
        len(priority_tokens),
        -len(generic_tokens),
        len(focus_tokens),
        -len(_normalize_keyword_phrase(value).split()),
    )


def _topic_phrase_domain_token_count(value: str) -> int:
    focus_tokens = _filter_keyword_focus_tokens(_keyword_focus_tokens(value))
    return len(focus_tokens - ABSTRACT_QUERY_TOKENS - EDITORIAL_ACTION_TOKENS - GENERIC_TOPIC_CATEGORY_TOKENS)


def _extract_topic_question_phrase(topic: str) -> str:
    segments = _split_topic_segments(topic)
    for segment in segments[1:]:
        cleaned = _clean_topic_segment_raw(segment, preserve_question=True)
        if cleaned and _looks_like_question_phrase(cleaned):
            return cleaned
    fallback = _clean_topic_segment_raw(topic, preserve_question=True)
    if fallback and _looks_like_question_phrase(fallback):
        return fallback
    normalized = _normalize_keyword_phrase(topic)
    if _looks_like_question_phrase(normalized):
        words = _strip_trailing_topic_modifiers(normalized.split())
        return " ".join(words).strip() + "?"
    return ""


def _format_sentence_start(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if not cleaned:
        return ""
    return cleaned[:1].upper() + cleaned[1:]


def _append_sentence_with_limit(current: str, sentence: str, *, max_chars: int) -> str:
    cleaned_sentence = re.sub(r"\s+", " ", str(sentence or "").strip())
    if not cleaned_sentence:
        return current
    if not current:
        return _truncate_title(cleaned_sentence, max_chars=max_chars)
    candidate = f"{current} {cleaned_sentence}".strip()
    if len(candidate) <= max_chars:
        return candidate
    return current


def _looks_like_self_assessment_question(value: str) -> bool:
    normalized = _normalize_keyword_phrase(_fold_keyword_text(value))
    if not normalized or not _looks_like_question_phrase(normalized):
        return False
    return any(fragment in normalized for fragment in SELF_ASSESSMENT_QUESTION_FRAGMENTS)


def _phrase_has_editorial_noise(value: str) -> bool:
    normalized = _normalize_keyword_phrase(value)
    if not normalized:
        return True
    folded = _normalize_keyword_phrase(_fold_keyword_text(normalized))
    tokens = folded.split()
    if not tokens:
        return True
    if any(folded.startswith(prefix) for prefix in EDITORIAL_GREETING_PREFIXES):
        return True
    if any(token in EDITORIAL_NOISE_TOKENS for token in tokens):
        return True
    if len(tokens) >= 2 and tokens[-1] in TRAILING_TITLE_STOPWORDS:
        return True
    if _looks_like_self_assessment_question(folded):
        return True
    return False


def _ensure_primary_keyword_in_title(
    *,
    title: str,
    primary_title: str,
    question_title: str,
    subject_title: str,
    suffix: str,
    max_chars: int = SEO_TITLE_MAX_CHARS,
) -> str:
    if not primary_title or _keyword_present_relaxed(title, primary_title):
        return title
    reference_text = _normalize_keyword_phrase(" ".join([question_title, subject_title, title]))
    primary_tokens = _keyword_token_set(primary_title)
    if len(primary_tokens) > 4 and _keyword_similarity(primary_title, reference_text) < 0.2:
        return title

    question_title = _format_sentence_start(question_title)
    subject_title = _format_title_case(subject_title)
    candidates: List[str] = []
    if question_title:
        candidates.extend(
            [
                f"{primary_title}: {question_title}",
                f"{question_title}: {primary_title}",
            ]
        )
    if subject_title:
        candidates.extend(
            [
                f"{primary_title}: {subject_title}",
                f"{subject_title}: {primary_title}",
            ]
        )
    candidates.extend(
        [
            f"{primary_title}: {suffix}",
            primary_title,
        ]
    )

    normalized_original = _normalize_keyword_phrase(title)
    for candidate in candidates:
        normalized_candidate = _truncate_title(candidate, max_chars=max_chars)
        if not _keyword_present_relaxed(normalized_candidate, primary_title):
            continue
        if len(normalized_candidate) > max_chars:
            continue
        if len(normalized_candidate) >= SEO_TITLE_MIN_CHARS:
            return normalized_candidate
        if normalized_original and _keyword_similarity(normalized_candidate, normalized_original) >= 0.35:
            return normalized_candidate
    return title


def _build_deterministic_title_package(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    search_intent_type: str,
    structured_mode: str,
    current_year: int,
    article_angle: str = "",
    topic_class: str = "general",
    recent_titles: Optional[List[str]] = None,
) -> Dict[str, str]:
    subject_title = _extract_topic_subject_phrase(topic)
    question_title = _extract_topic_question_phrase(topic)
    primary_title = _format_title_case(_sanitize_editorial_phrase(primary_keyword))
    topic_title = (
        _format_sentence_start(question_title)
        if question_title
        else _format_title_case(subject_title or topic or primary_keyword or "Ratgeber")
    )
    if (
        not question_title
        and primary_title
        and not _keyword_present_relaxed(topic_title, primary_title)
        and _keyword_similarity(topic_title, primary_title) < 0.5
    ):
        for candidate in (
            f"{primary_title}: {topic_title}",
            f"{topic_title}: {primary_title}",
        ):
            if len(candidate) <= SEO_TITLE_MAX_CHARS:
                topic_title = candidate
                break
    normalized_topic = _normalize_keyword_phrase(topic)
    include_year = "checkliste" in normalized_topic or "trend" in normalized_topic
    suffix = _derive_title_support_clause(
        topic=topic,
        intent_type=search_intent_type,
        article_angle=article_angle,
        topic_class=topic_class,
        structured_mode=structured_mode,
    )
    if question_title:
        h1 = _truncate_title(topic_title, max_chars=SEO_H1_MAX_CHARS)
    else:
        h1 = _truncate_title(topic_title, max_chars=SEO_H1_MAX_CHARS)
    if suffix and ":" not in h1 and len(h1) < SEO_TITLE_MIN_CHARS:
        h1 = _truncate_title(f"{h1}: {suffix}", max_chars=SEO_H1_MAX_CHARS)
    if len(h1) < SEO_TITLE_MIN_CHARS:
        fallback_suffix = "Worauf es in der Praxis ankommt"
        if suffix and _keyword_similarity(suffix, fallback_suffix) < 0.6:
            h1 = _truncate_title(f"{h1}: {fallback_suffix}", max_chars=SEO_H1_MAX_CHARS)
        else:
            h1 = _truncate_title(f"{h1}: Wichtige Fragen und naechste Schritte", max_chars=SEO_H1_MAX_CHARS)
    support_clause_variants = _derive_title_support_clause_variants(
        topic=topic,
        intent_type=search_intent_type,
        article_angle=article_angle,
        topic_class=topic_class,
        structured_mode=structured_mode,
    )

    raw_candidates: List[str] = []

    def _push_candidate(value: str) -> None:
        cleaned = re.sub(r"\s+", " ", str(value or "").strip())
        if not cleaned:
            return
        if cleaned in raw_candidates:
            return
        raw_candidates.append(cleaned)

    _push_candidate(h1)
    if question_title:
        _push_candidate(_format_sentence_start(question_title))
    if subject_title and question_title:
        _push_candidate(f"{_format_title_case(subject_title)}: {_format_sentence_start(question_title)}")
    if primary_title and question_title:
        _push_candidate(f"{primary_title}: {_format_sentence_start(question_title)}")
    for clause in support_clause_variants:
        if subject_title:
            _push_candidate(f"{_format_title_case(subject_title)}: {clause}")
        if primary_title and primary_title != _format_title_case(subject_title):
            _push_candidate(f"{primary_title}: {clause}")
    if primary_title:
        _push_candidate(primary_title)

    best_h1 = h1
    best_score = float("-inf")
    recent_title_values = _dedupe_recent_title_values(recent_titles)
    for index, raw_candidate in enumerate(raw_candidates):
        candidate_h1 = _ensure_primary_keyword_in_title(
            title=_truncate_title(raw_candidate, max_chars=SEO_H1_MAX_CHARS),
            primary_title=primary_title,
            question_title=question_title,
            subject_title=subject_title or topic or primary_keyword,
            suffix=suffix,
            max_chars=SEO_H1_MAX_CHARS,
        )
        if include_year and str(current_year) not in candidate_h1:
            candidate_h1 = _truncate_title(f"{candidate_h1} {current_year}", max_chars=SEO_H1_MAX_CHARS)
        title_quality = _evaluate_title_quality(
            title=candidate_h1,
            primary_keyword=primary_keyword,
            topic=topic,
            max_chars=SEO_H1_MAX_CHARS,
        )
        history_eval = _evaluate_title_history_novelty(
            title=candidate_h1,
            recent_titles=recent_title_values,
        )
        candidate_score = float(title_quality["score"] - history_eval["score_penalty"]) - (index * 0.2)
        if question_title and _keyword_similarity(candidate_h1, question_title) >= 0.85:
            candidate_score += 3.0
        if history_eval["max_similarity"] <= 0.45:
            candidate_score += 1.0
        if candidate_score > best_score:
            best_h1 = candidate_h1
            best_score = candidate_score
    h1 = best_h1

    title_quality = _evaluate_title_quality(
        title=h1,
        primary_keyword=primary_keyword,
        topic=topic,
        max_chars=SEO_H1_MAX_CHARS,
    )
    if title_quality["errors"]:
        natural_fallback = _truncate_title(
            f"{_format_title_case(subject_title or primary_keyword or topic)}: {_derive_title_support_clause(topic=topic, intent_type=search_intent_type, article_angle=article_angle or 'practical_guidance', topic_class=topic_class, structured_mode=structured_mode) or 'Worauf es wirklich ankommt'}",
            max_chars=SEO_H1_MAX_CHARS,
        )
        fallback_quality = _evaluate_title_quality(
            title=natural_fallback,
            primary_keyword=primary_keyword,
            topic=topic,
            max_chars=SEO_H1_MAX_CHARS,
        )
        fallback_history_eval = _evaluate_title_history_novelty(
            title=natural_fallback,
            recent_titles=recent_title_values,
        )
        fallback_score = fallback_quality["score"] - fallback_history_eval["score_penalty"]
        current_score = title_quality["score"] - _evaluate_title_history_novelty(
            title=h1,
            recent_titles=recent_title_values,
        )["score_penalty"]
        if fallback_score >= current_score:
            h1 = natural_fallback
    meta_title = _truncate_title(h1)
    slug_seed = primary_keyword or topic
    slug = _derive_slug(slug_seed)[:SEO_SLUG_MAX_CHARS]
    return {"h1": h1, "meta_title": meta_title, "slug": slug}


def _build_deterministic_meta_description(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    structured_mode: str,
) -> str:
    opening_source = _format_title_case(topic or primary_keyword or "Ratgeber")
    normalized_secondaries = _dedupe_keyword_phrases(secondary_keywords)[:3]
    descriptions: List[str] = []

    for opening_max_chars in (60, 52, 44):
        opening = _truncate_title(opening_source, max_chars=opening_max_chars)
        if structured_mode == "table":
            description = f"{opening}: Vergleich, Einordnung und klare Unterschiede."
        elif structured_mode == "list":
            description = f"{opening}: Checkliste, konkrete Schritte und hilfreiche Tipps."
        else:
            description = f"{opening}: kompakte Einordnung, klare Kriterien und konkrete Tipps."

        support_sentences: List[str] = []
        if normalized_secondaries:
            support_sentences.append(f"Fokus auf {_format_title_case(normalized_secondaries[0])}.")
        if normalized_secondaries[1:2]:
            support_sentences.append(f"Auch {_format_title_case(normalized_secondaries[1])} wird praxisnah erklaert.")
        elif primary_keyword and not _keyword_present_relaxed(opening, primary_keyword):
            support_sentences.append(f"{_format_title_case(primary_keyword)} wird praxisnah erklaert.")
        if normalized_secondaries[2:3]:
            support_sentences.append(f"Zusätzlich geht es um {_format_title_case(normalized_secondaries[2])}.")
        support_sentences.append("Hilfreich fuer schnelle Entscheidungen im Alltag.")

        for sentence in support_sentences:
            description = _append_sentence_with_limit(
                description,
                sentence,
                max_chars=SEO_DESCRIPTION_MAX_CHARS,
            )

        descriptions.append(description)
        if SEO_DESCRIPTION_MIN_CHARS <= len(description) <= SEO_DESCRIPTION_MAX_CHARS:
            return description

    fallback_opening = _truncate_title(opening_source, max_chars=44)
    fallback = f"{fallback_opening}: kompakte Einordnung, klare Kriterien und konkrete Tipps. Hilfreich fuer schnelle Entscheidungen im Alltag."
    fallback = _truncate_title(fallback, max_chars=SEO_DESCRIPTION_MAX_CHARS)
    descriptions.append(fallback)
    return max(descriptions, key=len) if descriptions else fallback


def _serialize_site_snapshot_pages(pages: List[Dict[str, str]]) -> str:
    lines: List[str] = []
    for page in pages:
        title = str(page.get("title") or "").strip()
        text = _limit_text(str(page.get("text") or "").strip(), DEFAULT_SITE_ANALYSIS_PAGE_TEXT_CHARS)
        if not text:
            continue
        lines.append(f"URL: {str(page.get('url') or '').strip()}")
        if title:
            lines.append(f"Titel: {title}")
        lines.append(f"Inhalt: {text}")
    return "\n".join(lines).strip()


def _build_site_snapshot(
    *,
    site_url: str,
    homepage_html: str,
    candidate_urls: List[str],
    purpose_prefix: str,
    warnings: List[str],
    debug: Dict[str, Any],
    timeout_seconds: int,
    retries: int,
    max_pages: int,
) -> Dict[str, Any]:
    pages: List[Dict[str, str]] = []
    normalized_site_url = _normalize_url(site_url)
    homepage_text = sanitize_html(homepage_html)
    if homepage_text:
        pages.append(
            {
                "url": normalized_site_url,
                "title": extract_page_title(homepage_html),
                "text": _limit_text(homepage_text, DEFAULT_SITE_ANALYSIS_PAGE_TEXT_CHARS),
            }
        )

    normalized_candidates = _normalize_internal_link_candidates(
        candidate_urls,
        publishing_site_url=site_url,
        backlink_url="",
        max_items=max(0, max_pages - len(pages)),
    )
    for index, candidate_url in enumerate(normalized_candidates, start=1):
        if len(pages) >= max_pages:
            break
        if _normalize_url(candidate_url) == normalized_site_url:
            continue
        candidate_html = fetch_url(
            candidate_url,
            purpose=f"{purpose_prefix}_{index}",
            warnings=warnings,
            debug=debug,
            timeout_seconds=timeout_seconds,
            retries=retries,
        )
        candidate_text = sanitize_html(candidate_html)
        if not candidate_text:
            continue
        pages.append(
            {
                "url": _normalize_url(candidate_url),
                "title": extract_page_title(candidate_html),
                "text": _limit_text(candidate_text, DEFAULT_SITE_ANALYSIS_PAGE_TEXT_CHARS),
            }
        )

    serialized = _serialize_site_snapshot_pages(pages)
    page_titles = [str(page.get("title") or "").strip() for page in pages if str(page.get("title") or "").strip()]
    sample_urls = [str(page.get("url") or "").strip() for page in pages if str(page.get("url") or "").strip()]
    summary = " | ".join(page_titles[:4])
    return {
        "pages": pages,
        "content_hash": _hash_text(serialized),
        "combined_text": serialized,
        "site_summary": summary,
        "sample_page_titles": page_titles[:8],
        "sample_urls": sample_urls[:8],
    }


def _merge_phase2_analysis(
    current: Dict[str, Any],
    cached: Optional[Dict[str, Any]],
    *,
    inventory_categories: List[str],
) -> Dict[str, Any]:
    if not cached:
        current["allowed_topics"] = _merge_string_lists(current.get("allowed_topics") or [], max_items=12)
        current["content_style_constraints"] = _merge_string_lists(
            current.get("content_style_constraints") or [],
            max_items=6,
        )
        current["internal_linking_opportunities"] = _merge_string_lists(
            current.get("internal_linking_opportunities") or [],
            max_items=10,
        )
        current["site_categories"] = _merge_string_lists(
            current.get("site_categories") or [],
            inventory_categories,
            max_items=10,
        )
        current["topic_clusters"] = _merge_string_lists(current.get("topic_clusters") or [], max_items=8)
        current["prominent_titles"] = _merge_string_lists(current.get("prominent_titles") or [], max_items=8)
        current["sample_page_titles"] = _merge_string_lists(current.get("sample_page_titles") or [], max_items=8)
        current["sample_urls"] = _merge_string_lists(current.get("sample_urls") or [], max_items=8)
        current["site_summary"] = str(current.get("site_summary") or "").strip()
        return current

    current["allowed_topics"] = _merge_string_lists(
        current.get("allowed_topics") or [],
        cached.get("allowed_topics") or [],
        max_items=12,
    )
    current["content_style_constraints"] = _merge_string_lists(
        current.get("content_style_constraints") or [],
        cached.get("content_style_constraints") or [],
        max_items=6,
    )
    current["internal_linking_opportunities"] = _merge_string_lists(
        current.get("internal_linking_opportunities") or [],
        cached.get("internal_linking_opportunities") or [],
        max_items=10,
    )
    current["site_categories"] = _merge_string_lists(
        current.get("site_categories") or [],
        cached.get("site_categories") or [],
        inventory_categories,
        max_items=10,
    )
    current["topic_clusters"] = _merge_string_lists(
        current.get("topic_clusters") or [],
        cached.get("topic_clusters") or [],
        max_items=8,
    )
    current["prominent_titles"] = _merge_string_lists(
        current.get("prominent_titles") or [],
        cached.get("prominent_titles") or [],
        max_items=8,
    )
    current["sample_page_titles"] = _merge_string_lists(
        current.get("sample_page_titles") or [],
        cached.get("sample_page_titles") or [],
        max_items=8,
    )
    current["sample_urls"] = _merge_string_lists(
        current.get("sample_urls") or [],
        cached.get("sample_urls") or [],
        max_items=8,
    )
    current["site_summary"] = str(current.get("site_summary") or "").strip() or str(cached.get("site_summary") or "").strip()
    return current


def _model_prefers_anthropic(*models: str) -> bool:
    return any((model or "").strip().lower().startswith("claude") for model in models)


def _coerce_phase2_payload(value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    allowed_topics = value.get("allowed_topics")
    content_style_constraints = value.get("content_style_constraints")
    internal_linking_opportunities = value.get("internal_linking_opportunities")
    if not isinstance(allowed_topics, list) or not isinstance(content_style_constraints, list):
        return None
    return {
        "allowed_topics": [str(item).strip() for item in allowed_topics if str(item).strip()],
        "content_style_constraints": [str(item).strip() for item in content_style_constraints if str(item).strip()],
        "internal_linking_opportunities": [
            str(item).strip() for item in (internal_linking_opportunities or []) if str(item).strip()
        ],
        "site_summary": str(value.get("site_summary") or "").strip(),
        "site_categories": [str(item).strip() for item in (value.get("site_categories") or []) if str(item).strip()],
        "topic_clusters": [str(item).strip() for item in (value.get("topic_clusters") or []) if str(item).strip()],
        "prominent_titles": [str(item).strip() for item in (value.get("prominent_titles") or []) if str(item).strip()],
        "sample_page_titles": [str(item).strip() for item in (value.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (value.get("sample_urls") or []) if str(item).strip()],
    }


def _coerce_phase1_payload(value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    brand_name = str(value.get("brand_name") or "").strip()
    backlink_url = str(value.get("backlink_url") or "").strip()
    anchor_type = str(value.get("anchor_type") or "").strip()
    keyword_cluster = value.get("keyword_cluster")
    if anchor_type not in {"brand", "contextual_generic", "partial_match"}:
        return None
    if not isinstance(keyword_cluster, list):
        return None
    return {
        "brand_name": brand_name,
        "backlink_url": backlink_url,
        "anchor_type": anchor_type,
        "keyword_cluster": [str(item).strip() for item in keyword_cluster if str(item).strip()],
        "site_summary": str(value.get("site_summary") or "").strip(),
        "sample_page_titles": [str(item).strip() for item in (value.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (value.get("sample_urls") or []) if str(item).strip()],
    }


def _coerce_site_profile_payload(value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    normalized_url = str(value.get("normalized_url") or "").strip()
    topics = [str(item).strip() for item in (value.get("topics") or []) if str(item).strip()]
    contexts = [str(item).strip() for item in (value.get("contexts") or []) if str(item).strip()]
    if not normalized_url and not topics and not contexts:
        return None
    return {
        "normalized_url": normalized_url,
        "page_title": str(value.get("page_title") or "").strip(),
        "meta_description": str(value.get("meta_description") or "").strip(),
        "visible_headings": [str(item).strip() for item in (value.get("visible_headings") or []) if str(item).strip()],
        "repeated_keywords": [str(item).strip() for item in (value.get("repeated_keywords") or []) if str(item).strip()],
        "sample_page_titles": [str(item).strip() for item in (value.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (value.get("sample_urls") or []) if str(item).strip()],
        "domain_level_topic": str(value.get("domain_level_topic") or "").strip(),
        "primary_context": str(value.get("primary_context") or "").strip(),
        "topics": topics,
        "contexts": contexts,
        "content_tone": str(value.get("content_tone") or "").strip(),
        "content_style": [str(item).strip() for item in (value.get("content_style") or []) if str(item).strip()],
        "site_categories": [str(item).strip() for item in (value.get("site_categories") or []) if str(item).strip()],
        "topic_clusters": [str(item).strip() for item in (value.get("topic_clusters") or []) if str(item).strip()],
        "prominent_titles": [str(item).strip() for item in (value.get("prominent_titles") or []) if str(item).strip()],
        "business_type": str(value.get("business_type") or "").strip(),
        "services_or_products": [str(item).strip() for item in (value.get("services_or_products") or []) if str(item).strip()],
        "business_intent": str(value.get("business_intent") or "").strip(),
        "commerciality": int(value.get("commerciality") or 0),
    }


def _coerce_pair_fit_topic_candidates(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    candidates: List[Dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            topic = str(item.get("topic") or "").strip()
            if not topic:
                continue
            candidates.append(
                {
                    "topic": topic,
                    "publishing_site_relevance": max(0, min(10, int(item.get("publishing_site_relevance") or 0))),
                    "backlink_naturalness": max(0, min(10, int(item.get("backlink_naturalness") or 0))),
                    "informational_value": max(0, min(10, int(item.get("informational_value") or 0))),
                    "seo_plausibility": max(0, min(10, int(item.get("seo_plausibility") or 0))),
                    "non_spamminess": max(0, min(10, int(item.get("non_spamminess") or 0))),
                    "target_site_relevance": max(0, min(10, int(item.get("target_site_relevance") or 0))),
                    "spam_risk": max(0, min(10, int(item.get("spam_risk") or 0))),
                    "total_score": max(0, min(50, int(item.get("total_score") or 0))),
                    "backlink_angle": str(item.get("backlink_angle") or "").strip(),
                    "score_breakdown": item.get("score_breakdown") if isinstance(item.get("score_breakdown"), dict) else {},
                }
            )
            continue
        topic = str(item or "").strip()
        if topic:
            candidates.append(
                {
                    "topic": topic,
                    "publishing_site_relevance": 0,
                    "backlink_naturalness": 0,
                    "informational_value": 0,
                    "seo_plausibility": 0,
                    "non_spamminess": 0,
                    "target_site_relevance": 0,
                    "spam_risk": 10,
                    "total_score": 0,
                    "backlink_angle": "",
                    "score_breakdown": {},
                }
            )
    return candidates[:5]


def _pair_fit_candidate_topics(candidates: List[Dict[str, Any]]) -> List[str]:
    return [str(item.get("topic") or "").strip() for item in candidates if str(item.get("topic") or "").strip()]


def _compact_pair_fit_profile(profile: Dict[str, Any], *, site_kind: str) -> Dict[str, Any]:
    compact = {
        "normalized_url": str(profile.get("normalized_url") or "").strip(),
        "page_title": str(profile.get("page_title") or "").strip(),
        "meta_description": str(profile.get("meta_description") or "").strip(),
        "domain_level_topic": str(profile.get("domain_level_topic") or "").strip(),
        "primary_context": str(profile.get("primary_context") or "").strip(),
        "topics": [str(item).strip() for item in (profile.get("topics") or []) if str(item).strip()][:8],
        "contexts": [str(item).strip() for item in (profile.get("contexts") or []) if str(item).strip()][:6],
        "visible_headings": [str(item).strip() for item in (profile.get("visible_headings") or []) if str(item).strip()][:6],
        "repeated_keywords": [str(item).strip() for item in (profile.get("repeated_keywords") or []) if str(item).strip()][:8],
    }
    if site_kind == "publishing":
        compact["content_style"] = [str(item).strip() for item in (profile.get("content_style") or []) if str(item).strip()][:5]
        compact["site_categories"] = [str(item).strip() for item in (profile.get("site_categories") or []) if str(item).strip()][:6]
        compact["topic_clusters"] = [str(item).strip() for item in (profile.get("topic_clusters") or []) if str(item).strip()][:6]
        compact["prominent_titles"] = [str(item).strip() for item in (profile.get("prominent_titles") or []) if str(item).strip()][:5]
    else:
        compact["business_type"] = str(profile.get("business_type") or "").strip()
        compact["services_or_products"] = [
            str(item).strip() for item in (profile.get("services_or_products") or []) if str(item).strip()
        ][:8]
        compact["business_intent"] = str(profile.get("business_intent") or "").strip()
        compact["site_root_url"] = str(profile.get("site_root_url") or "").strip()
    return compact


def _phase1_from_target_profile(profile: Dict[str, Any], *, target_site_url: str) -> Dict[str, Any]:
    brand_name = _normalize_brand_name(str(profile.get("page_title") or "").strip())
    if not brand_name:
        brand_name = _guess_brand_name(target_site_url, "")
    keyword_cluster = _merge_string_lists(
        profile.get("repeated_keywords") or [],
        profile.get("topics") or [],
        profile.get("services_or_products") or [],
        max_items=10,
    )
    anchor_type = "brand" if brand_name else ("partial_match" if keyword_cluster else "contextual_generic")
    return {
        "brand_name": brand_name,
        "backlink_url": target_site_url,
        "anchor_type": anchor_type,
        "keyword_cluster": keyword_cluster,
        "site_summary": str(profile.get("meta_description") or profile.get("domain_level_topic") or "").strip(),
        "sample_page_titles": [str(item).strip() for item in (profile.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (profile.get("sample_urls") or []) if str(item).strip()],
    }


def _phase2_from_publishing_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    allowed_topics = _merge_string_lists(
        profile.get("topics") or [],
        profile.get("topic_clusters") or [],
        profile.get("site_categories") or [],
        max_items=12,
    )
    return {
        "allowed_topics": allowed_topics,
        "content_style_constraints": _merge_string_lists(
            profile.get("content_style") or [],
            [str(profile.get("content_tone") or "").strip()],
            max_items=6,
        ),
        "internal_linking_opportunities": [],
        "site_summary": str(profile.get("meta_description") or profile.get("domain_level_topic") or "").strip(),
        "site_categories": [str(item).strip() for item in (profile.get("site_categories") or []) if str(item).strip()],
        "topic_clusters": [str(item).strip() for item in (profile.get("topic_clusters") or []) if str(item).strip()],
        "prominent_titles": [str(item).strip() for item in (profile.get("prominent_titles") or []) if str(item).strip()],
        "sample_page_titles": [str(item).strip() for item in (profile.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (profile.get("sample_urls") or []) if str(item).strip()],
    }


def _deterministic_style_constraints_from_titles(values: List[str]) -> List[str]:
    joined = " ".join(values).lower()
    constraints = ["Sachlicher, hilfreicher Ton", "Natuerliche Sprache ohne werbliche Uebertreibung"]
    if any(term in joined for term in {"tipps", "ratgeber", "checkliste"}):
        constraints.append("Praxisnahe Tipps und klare Struktur")
    if any(term in joined for term in {"vergleich", "vs", "oder"}):
        constraints.append("Vergleichende Einordnung mit konkreten Kriterien")
    if any(term in joined for term in {"faq", "fragen", "antworten"}):
        constraints.append("Direkte Antworten auf typische Leserfragen")
    return _merge_string_lists(constraints, max_items=6)


def _derive_deterministic_phase2_analysis(
    *,
    publishing_snapshot: Dict[str, Any],
    inventory_topic_insights: Dict[str, Any],
) -> Dict[str, Any]:
    sample_titles = [str(item).strip() for item in (publishing_snapshot.get("sample_page_titles") or []) if str(item).strip()]
    sample_urls = [str(item).strip() for item in (publishing_snapshot.get("sample_urls") or []) if str(item).strip()]
    combined_text = str(publishing_snapshot.get("combined_text") or "")
    keyword_terms = _extract_keywords(combined_text, max_terms=10)
    allowed_topics = _merge_string_lists(
        sample_titles,
        inventory_topic_insights.get("topic_clusters") or [],
        inventory_topic_insights.get("site_categories") or [],
        keyword_terms,
        max_items=12,
    )
    return {
        "allowed_topics": allowed_topics,
        "content_style_constraints": _deterministic_style_constraints_from_titles(sample_titles + keyword_terms),
        "internal_linking_opportunities": inventory_topic_insights.get("internal_linking_opportunities") or [],
        "site_summary": str(publishing_snapshot.get("site_summary") or "").strip(),
        "site_categories": inventory_topic_insights.get("site_categories") or [],
        "topic_clusters": _merge_string_lists(
            inventory_topic_insights.get("topic_clusters") or [],
            keyword_terms,
            max_items=10,
        ),
        "prominent_titles": inventory_topic_insights.get("prominent_titles") or [],
        "sample_page_titles": sample_titles,
        "sample_urls": sample_urls,
    }


def _pair_fit_cache_payload_is_usable(payload: Dict[str, Any]) -> bool:
    decision = str(payload.get("decision") or "").strip().lower()
    final_match_decision = str(payload.get("final_match_decision") or "").strip().lower()
    if decision == "rejected" or final_match_decision in {"weak_fit", "hard_reject"}:
        return True
    candidates = _coerce_pair_fit_topic_candidates(payload.get("topic_candidates"))
    pair_fit_mode = str(payload.get("pair_fit_mode") or "selection").strip().lower()
    required_candidate_count = 1 if pair_fit_mode == "validation" else 5
    return bool(
        str(payload.get("final_article_topic") or "").strip()
        and len(candidates) == required_candidate_count
        and isinstance(payload.get("intersection_contexts"), list)
        and bool(payload.get("why_this_topic_was_chosen"))
        and final_match_decision in {"accepted", ""}
    )


def _pair_fit_tokens_from_text(value: str) -> List[str]:
    tokens = re.findall(r"\b[a-zA-ZäöüÄÖÜß]{3,}\b", _normalize_keyword_phrase(value))
    out: List[str] = []
    for token in tokens:
        if len(token) < 4:
            continue
        if token in STOPWORDS or token in GERMAN_FUNCTION_WORDS or token in ENGLISH_FUNCTION_WORDS:
            continue
        if token in PAIR_FIT_EXTRA_STOPWORDS or token in PAIR_FIT_BOILERPLATE_TOKENS:
            continue
        out.append(token)
    return out


def _pair_fit_term_weights(profile: Dict[str, Any], *, site_kind: str) -> Dict[str, float]:
    weighted_fields: List[tuple[Any, float]] = [
        (profile.get("topics"), 4.0),
        (profile.get("site_categories"), 3.5),
        (profile.get("topic_clusters"), 3.0),
        (profile.get("services_or_products"), 4.0 if site_kind == "target" else 2.0),
        (profile.get("prominent_titles"), 2.5),
        (profile.get("sample_page_titles"), 2.0),
        (profile.get("visible_headings"), 1.8),
        (profile.get("repeated_keywords"), 1.2),
        ([profile.get("page_title")], 2.4),
        ([profile.get("domain_level_topic")], 2.0),
        ([profile.get("meta_description")], 1.0),
    ]
    scores: Dict[str, float] = {}
    for raw_values, weight in weighted_fields:
        values = raw_values if isinstance(raw_values, list) else [raw_values]
        for raw_value in values:
            cleaned = _sanitize_editorial_phrase(raw_value, allow_single_token=True)
            if not cleaned:
                continue
            tokens = _pair_fit_tokens_from_text(cleaned)
            if not tokens:
                continue
            phrase = " ".join(tokens[:6]).strip()
            if len(phrase) < 4:
                continue
            scores[phrase] = scores.get(phrase, 0.0) + weight
    return scores


def _pair_fit_ranked_terms(profile: Dict[str, Any], *, site_kind: str, max_items: int = 10) -> List[str]:
    weighted = _pair_fit_term_weights(profile, site_kind=site_kind)
    ranked = sorted(weighted.items(), key=lambda item: (-item[1], item[0]))
    return [term for term, _score in ranked[:max_items]]


def _pair_fit_overlap_terms(publishing_terms: List[str], target_terms: List[str], *, max_items: int = 12) -> List[str]:
    publishing_tokens = {token for term in publishing_terms for token in _pair_fit_tokens_from_text(term)}
    target_tokens = {token for term in target_terms for token in _pair_fit_tokens_from_text(term)}
    ranked = sorted(publishing_tokens & target_tokens)
    return ranked[:max_items]


def _pair_fit_expand_contexts(profile: Dict[str, Any], *, ranked_terms: List[str]) -> List[str]:
    contexts = _dedupe_preserve_order([str(item).strip() for item in (profile.get("contexts") or []) if str(item).strip()])
    text = " ".join(
        ranked_terms
        + [str(profile.get("page_title") or "").strip(), str(profile.get("domain_level_topic") or "").strip()]
    ).lower()
    for context, keywords in PAIR_FIT_CONTEXT_KEYWORDS.items():
        matches = sum(1 for keyword in keywords if keyword in text)
        if matches > 0:
            contexts.append(context)
    return _dedupe_preserve_order(contexts)[:8]


def _pair_fit_context_label(contexts: List[str]) -> str:
    for context in contexts:
        label = PAIR_FIT_CONTEXT_LABELS.get(context)
        if label:
            return label
    return "den Alltag"


def _pair_fit_focus_term(terms: List[str], *, fallback: str) -> str:
    for term in terms:
        tokens = _pair_fit_tokens_from_text(term)
        if 1 <= len(tokens) <= 5:
            return _format_title_case(" ".join(tokens))
    return _format_title_case(" ".join(_pair_fit_tokens_from_text(fallback)[:5]) or fallback or "Hilfreiche Orientierung")


def _pair_fit_audience_term(terms: List[str], contexts: List[str]) -> str:
    for context in contexts:
        audience = PAIR_FIT_CONTEXT_AUDIENCES.get(context)
        if audience:
            return audience
    for term in terms:
        tokens = _pair_fit_tokens_from_text(term)
        audience_tokens = [token for token in tokens if token in PAIR_FIT_AUDIENCE_TOKENS]
        if audience_tokens:
            return _format_title_case(audience_tokens[0])
    return "Leserinnen und Leser"


def _pair_fit_generate_bridge_topics(
    *,
    requested_topic: str,
    exclude_topics: List[str],
    publishing_terms: List[str],
    target_terms: List[str],
    publishing_contexts: List[str],
    target_contexts: List[str],
) -> List[str]:
    target_focus = _pair_fit_focus_term(target_terms, fallback=requested_topic or (target_terms[0] if target_terms else "Thema"))
    publishing_audience = _pair_fit_audience_term(publishing_terms, publishing_contexts)
    shared_contexts = _dedupe_preserve_order(publishing_contexts + target_contexts)
    context_label = _pair_fit_context_label(shared_contexts)
    templates = [
        requested_topic.strip(),
        f"{target_focus}: worauf {publishing_audience} achten sollten",
        f"{target_focus} im {context_label}: praktische Orientierung",
        f"Alltag und {target_focus}: hilfreiche Hinweise fuer {publishing_audience}",
        f"{target_focus}: sinnvolle Kriterien fuer {publishing_audience}",
        f"{target_focus} ohne Werbedruck: was im {context_label} wirklich wichtig ist",
        f"{target_focus} im Alltag: typische Fehler und bessere Entscheidungen",
    ]
    candidates: List[str] = []
    normalized_excluded = [_normalize_keyword_phrase(item) for item in exclude_topics if _normalize_keyword_phrase(item)]
    for template in templates:
        cleaned = re.sub(r"\s+", " ", str(template or "").strip())
        if not cleaned:
            continue
        normalized = _normalize_keyword_phrase(cleaned)
        if not normalized or any(_keyword_similarity(normalized, excluded) >= 0.8 for excluded in normalized_excluded):
            continue
        if any(_keyword_similarity(normalized, _normalize_keyword_phrase(existing)) >= 0.82 for existing in candidates):
            continue
        candidates.append(cleaned)
        if len(candidates) >= PAIR_FIT_CANDIDATE_COUNT:
            break
    while len(candidates) < PAIR_FIT_CANDIDATE_COUNT:
        fallback = f"{target_focus}: hilfreiche Orientierung fuer {publishing_audience}"
        if not any(_keyword_similarity(_normalize_keyword_phrase(fallback), _normalize_keyword_phrase(existing)) >= 0.82 for existing in candidates):
            candidates.append(fallback)
        else:
            candidates.append(f"{target_focus}: alltagsnahe Einordnung fuer {publishing_audience}")
    return candidates[:PAIR_FIT_CANDIDATE_COUNT]


def _pair_fit_score_candidate(
    topic: str,
    *,
    publishing_terms: List[str],
    target_terms: List[str],
    publishing_contexts: List[str],
    target_contexts: List[str],
    overlap_terms: List[str],
    target_business_intent: str,
) -> Dict[str, Any]:
    topic_tokens = set(_pair_fit_tokens_from_text(topic))
    publishing_tokens = {token for term in publishing_terms for token in _pair_fit_tokens_from_text(term)}
    target_tokens = {token for term in target_terms for token in _pair_fit_tokens_from_text(term)}
    shared_contexts = set(publishing_contexts) & set(target_contexts)
    publishing_overlap = len(topic_tokens & publishing_tokens)
    target_overlap = len(topic_tokens & target_tokens)
    overlap_bonus = len(topic_tokens & set(overlap_terms))
    shared_context_count = len(shared_contexts)
    bridge_evidence_score = shared_context_count * 3 + overlap_bonus * 2
    if publishing_overlap >= 2:
        bridge_evidence_score += 1
    if target_overlap >= 2:
        bridge_evidence_score += 1
    publishing_site_relevance = 2 + publishing_overlap * 2 + min(2, shared_context_count)
    target_site_relevance = 2 + target_overlap * 2 + min(2, shared_context_count) + min(1, overlap_bonus)
    if shared_context_count == 0 and overlap_bonus == 0:
        publishing_site_relevance -= 1
        target_site_relevance -= 1
    publishing_site_relevance = max(1, min(10, publishing_site_relevance))
    target_site_relevance = max(1, min(10, target_site_relevance))
    informational_value = 5 + sum(1 for cue in PAIR_FIT_INFORMATIONAL_CUES if cue in _normalize_keyword_phrase(topic))
    if "vergleich" in _normalize_keyword_phrase(topic) or "kriterien" in _normalize_keyword_phrase(topic):
        informational_value += 1
    informational_value = max(1, min(10, informational_value))
    backlink_naturalness = 2 + shared_context_count * 2 + min(2, overlap_bonus) + min(1, publishing_overlap) + min(1, target_overlap)
    if target_business_intent == "commercial" and shared_context_count == 0:
        backlink_naturalness -= 1
    if shared_context_count == 0 and overlap_bonus == 0:
        backlink_naturalness -= 2
    backlink_naturalness = max(1, min(10, backlink_naturalness))
    spam_risk = 1 + sum(1 for token in topic_tokens if token in PAIR_FIT_PROMO_TOKENS)
    if target_business_intent == "commercial":
        spam_risk += 1
    if shared_context_count == 0:
        spam_risk += 2
    if overlap_bonus == 0:
        spam_risk += 1
    if publishing_overlap == 0:
        spam_risk += 2
    if target_overlap == 0:
        spam_risk += 1
    if publishing_overlap <= 1 and target_overlap <= 1 and shared_context_count == 0:
        spam_risk += 1
    spam_risk = max(0, min(10, spam_risk))
    non_spamminess = max(1, min(10, 10 - spam_risk))
    seo_plausibility = 2 + publishing_overlap + target_overlap + min(3, shared_context_count + overlap_bonus)
    if shared_context_count == 0 and overlap_bonus == 0:
        seo_plausibility -= 1
    seo_plausibility = max(1, min(10, seo_plausibility))
    total_score = publishing_site_relevance + backlink_naturalness + informational_value + seo_plausibility + non_spamminess
    backlink_angle = (
        "Die Zielseite dient als weiterfuehrende Ressource innerhalb eines informativen Hauptthemas."
        if backlink_naturalness >= 6
        else "Die Zielseite kann nur vorsichtig und klar nachrangig als Zusatzquelle eingebunden werden."
    )
    return {
        "topic": topic,
        "publishing_site_relevance": publishing_site_relevance,
        "target_site_relevance": target_site_relevance,
        "backlink_naturalness": backlink_naturalness,
        "informational_value": informational_value,
        "seo_plausibility": seo_plausibility,
        "non_spamminess": non_spamminess,
        "spam_risk": spam_risk,
        "total_score": max(0, min(50, total_score)),
        "backlink_angle": backlink_angle,
        "score_breakdown": {
            "publishing_site_relevance": publishing_site_relevance,
            "target_site_relevance": target_site_relevance,
            "informational_value": informational_value,
            "backlink_naturalness": backlink_naturalness,
            "spam_risk": spam_risk,
            "publishing_overlap": publishing_overlap,
            "target_overlap": target_overlap,
            "shared_context_count": shared_context_count,
            "overlap_term_matches": overlap_bonus,
            "bridge_evidence_score": bridge_evidence_score,
        },
    }


def _pair_fit_candidate_decision(candidate: Dict[str, Any]) -> str:
    publishing_relevance = int(candidate.get("publishing_site_relevance") or 0)
    target_relevance = int(candidate.get("target_site_relevance") or 0)
    informational_value = int(candidate.get("informational_value") or 0)
    backlink_naturalness = int(candidate.get("backlink_naturalness") or 0)
    spam_risk = int(candidate.get("spam_risk") or 10)
    total_score = int(candidate.get("total_score") or 0)
    if (
        publishing_relevance >= 6
        and target_relevance >= 6
        and informational_value >= 6
        and backlink_naturalness >= 5
        and spam_risk <= 5
        and total_score >= 30
    ):
        return "accepted"
    if (
        publishing_relevance >= 4
        and target_relevance >= 4
        and informational_value >= 4
        and backlink_naturalness >= 3
        and spam_risk <= 7
        and total_score >= 20
    ):
        return "weak_fit"
    return "hard_reject"


def _pair_fit_reject_reason(final_match_decision: str, best_candidate: Dict[str, Any], overlap_terms: List[str]) -> str:
    if final_match_decision == "accepted":
        return ""
    if final_match_decision == "weak_fit":
        if overlap_terms:
            return "Es gibt eine grundsaetzlich denkbare Verbindung, aber sie bleibt noch schwach und braucht eine sehr vorsichtige redaktionelle Einbettung."
        return "Die Verbindung ist nur lose erkennbar und wirkt ohne starke redaktionelle Fuehrung schnell erzwungen."
    if overlap_terms:
        return "Zwischen beiden Seiten fehlt ein ausreichend natuerlicher Informationsraum; gemeinsame Signale reichen nicht fuer einen glaubwuerdigen Hauptartikel mit sinnvoller Zusatzressource."
    return "Die beiden Seiten teilen keinen tragfaehigen inhaltlichen Kontext fuer einen natuerlichen informativen Brueckenartikel."


def _pair_fit_overlap_reason(final_match_decision: str, overlap_terms: List[str], shared_contexts: List[str]) -> str:
    overlap_text = ", ".join(overlap_terms[:4])
    context_text = ", ".join(shared_contexts[:3])
    if final_match_decision == "accepted":
        return f"Gemeinsame Kontexte ({context_text}) und passende Signale ({overlap_text}) erlauben einen informativen Hauptartikel ohne werbliche Schwerpunktverschiebung.".strip()
    if final_match_decision == "weak_fit":
        return f"Es gibt gewisse Beruehrungspunkte ueber {context_text or 'einzelne Kontexte'}, aber die Verbindung bleibt redaktionell empfindlich.".strip()
    return f"Die Kontexte ueberlappen zu wenig ({context_text or 'keine tragfaehigen Kontexte'}) und die gemeinsamen Signale ({overlap_text or 'kaum relevante Signale'}) tragen keinen natuerlichen Artikel.".strip()


def _pair_fit_llm_input_payload(
    *,
    mode: str,
    requested_topic: str,
    exclude_topics: List[str],
    target_site_url: str,
    publishing_site_url: str,
    target_profile: Dict[str, Any],
    publishing_profile: Dict[str, Any],
    publishing_terms: List[str],
    target_terms: List[str],
    publishing_contexts: List[str],
    target_contexts: List[str],
    overlap_terms: List[str],
    heuristic_candidates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    shared_contexts = _dedupe_preserve_order([context for context in publishing_contexts if context in set(target_contexts)])
    payload = {
        "mode": mode,
        "requested_topic": requested_topic,
        "publishing_site_url": publishing_site_url,
        "target_site_url": target_site_url,
        "publishing_profile": _compact_pair_fit_profile(publishing_profile, site_kind="publishing"),
        "target_profile": _compact_pair_fit_profile(target_profile, site_kind="target"),
        "derived_signals": {
            "publishing_topics": publishing_terms[:8],
            "target_topics": target_terms[:8],
            "publishing_contexts": publishing_contexts[:8],
            "target_contexts": target_contexts[:8],
            "shared_contexts": shared_contexts,
            "overlap_terms": overlap_terms[:12],
        },
    }
    if mode == "selection":
        payload["exclude_topics"] = exclude_topics[:8]
        payload["derived_signals"]["seed_bridge_topics"] = [
            {
                "topic": str(item.get("topic") or "").strip(),
                "heuristic_total_score": int(item.get("total_score") or 0),
                "heuristic_breakdown": dict(item.get("score_breakdown") or {}),
            }
            for item in heuristic_candidates[:PAIR_FIT_CANDIDATE_COUNT]
        ]
    return payload


def _pair_fit_llm_prompts(input_payload: Dict[str, Any]) -> tuple[str, str]:
    mode = str(input_payload.get("mode") or "selection").strip().lower()
    system_prompt = (
        "Du bist ein redaktioneller Match-Analyst fuer Backlink-geeignete Informationsartikel. "
        "Beurteile, ob auf der Publishing-Seite ein natuerlicher informativer Artikel entstehen kann, "
        "der genau einen kontextuellen Link zur Zielseite enthaelt. "
        "Nutze semantische Kontexte, Zielgruppe, redaktionelle Glaubwuerdigkeit und Informationswert. "
        "Lehne kommerzielle Ziele nicht automatisch ab. "
        "Antworte ausschliesslich mit gueltigem JSON."
    )
    if mode == "validation":
        user_prompt = (
            "Bewerte ausschliesslich das angegebene requested_topic auf Basis der strukturierten Profildaten.\n"
            "Arbeitsregeln:\n"
            "- Pruefe nur dieses eine Thema. Erfinde keine Alternativen und schlage keine Ersatzthemen vor.\n"
            "- Das Hauptthema muss zuerst natuerlich zur Publishing-Seite passen.\n"
            "- Der Link zur Zielseite darf nur eine nachrangige, kontextuelle Ressource sein.\n"
            "- Match auf Kontext- und Zielgruppenebene, nicht nur ueber exakte Keywords.\n"
            "- final_match_decision ist genau eines von: accepted, weak_fit, hard_reject.\n"
            "- accepted: das requested_topic ist klar natuerlich und redaktionell glaubwuerdig.\n"
            "- weak_fit: das requested_topic ist denkbar, aber empfindlich.\n"
            "- hard_reject: das requested_topic wirkt nicht natuerlich.\n"
            "- Gib nur JSON mit diesem Schema zurueck:\n"
            "{\n"
            '  "publishing_site_relevance": 0,\n'
            '  "target_site_relevance": 0,\n'
            '  "informational_value": 0,\n'
            '  "backlink_naturalness": 0,\n'
            '  "spam_risk": 0,\n'
            '  "total_score": 0,\n'
            '  "backlink_angle": "string",\n'
            '  "final_match_decision": "accepted|weak_fit|hard_reject",\n'
            '  "why_this_topic_was_chosen": "string",\n'
            '  "best_overlap_reason": "string",\n'
            '  "reject_reason": "string",\n'
            '  "fit_score": 0\n'
            "}\n\n"
            "Input:\n"
            f"{json.dumps(input_payload, ensure_ascii=False, sort_keys=True, indent=2)}"
        )
        return system_prompt, user_prompt
    user_prompt = (
        "Bewerte die Passung zwischen Publishing-Seite und Zielseite auf Basis der strukturierten Profildaten.\n"
        "Arbeitsregeln:\n"
        "- Das Hauptthema muss zuerst natuerlich zur Publishing-Seite passen.\n"
        "- Der Link zur Zielseite darf nur eine nachrangige, kontextuelle Ressource sein.\n"
        "- Match auf Kontext- und Zielgruppenebene, nicht nur ueber exakte Keywords.\n"
        "- Vermeide zu breite Oberbegriffe, wenn die Zielseite ein konkretes Themenfeld erkennen laesst.\n"
        "- Bevorzuge Themen, die Publishing-Kontext und konkretes Zielseiten-Feld gleichzeitig sichtbar machen.\n"
        "- Nutze die Seed-Topics als Startpunkt, darfst sie aber verbessern oder ersetzen.\n"
        "- Wenn requested_topic gesetzt ist, musst du dieses Thema als einen der 5 Kandidaten explizit bewerten. Du darfst es sprachlich glätten, aber nicht in ein anderes Thema umdeuten.\n"
        "- Erzeuge genau 5 Kandidaten in deutscher Sprache.\n"
        "- Kandidaten bewerten auf einer Skala 0-10 fuer publishing_site_relevance, target_site_relevance, informational_value, backlink_naturalness, spam_risk.\n"
        "- total_score ist 0-50.\n"
        "- final_match_decision ist genau eines von: accepted, weak_fit, hard_reject.\n"
        "- accepted: mindestens ein Kandidat ist klar natuerlich und redaktionell glaubwuerdig.\n"
        "- weak_fit: eine Verbindung ist denkbar, aber empfindlich.\n"
        "- hard_reject: kein natuerlicher Brueckenartikel erkennbar.\n"
        "- Gib nur JSON mit diesem Schema zurueck:\n"
        "{\n"
        '  "topic_candidates": [\n'
        "    {\n"
        '      "topic": "string",\n'
        '      "publishing_site_relevance": 0,\n'
        '      "target_site_relevance": 0,\n'
        '      "informational_value": 0,\n'
        '      "backlink_naturalness": 0,\n'
        '      "spam_risk": 0,\n'
        '      "total_score": 0,\n'
        '      "backlink_angle": "string"\n'
        "    }\n"
        "  ],\n"
        '  "final_article_topic": "string",\n'
        '  "final_match_decision": "accepted|weak_fit|hard_reject",\n'
        '  "why_this_topic_was_chosen": "string",\n'
        '  "best_overlap_reason": "string",\n'
        '  "reject_reason": "string",\n'
        '  "fit_score": 0\n'
        "}\n\n"
        "Input:\n"
        f"{json.dumps(input_payload, ensure_ascii=False, sort_keys=True, indent=2)}"
    )
    return system_prompt, user_prompt


def _pair_fit_normalize_requested_topic_payload(
    *,
    llm_payload: Dict[str, Any],
    requested_topic: str,
    publishing_terms: List[str],
    target_terms: List[str],
    publishing_contexts: List[str],
    target_contexts: List[str],
    overlap_terms: List[str],
    target_business_intent: str,
) -> Dict[str, Any]:
    requested_clean = requested_topic.strip()
    if not requested_clean:
        raise CreatorError("Pair fit requested-topic normalization requires requested_topic.")
    fallback_candidate = _pair_fit_score_candidate(
        requested_clean,
        publishing_terms=publishing_terms,
        target_terms=target_terms,
        publishing_contexts=publishing_contexts,
        target_contexts=target_contexts,
        overlap_terms=overlap_terms,
        target_business_intent=target_business_intent,
    )
    candidates = _coerce_pair_fit_topic_candidates(llm_payload.get("topic_candidates"))
    requested_candidate = next(
        (
            item
            for item in candidates
            if _keyword_similarity(
                _normalize_keyword_phrase(str(item.get("topic") or "")),
                _normalize_keyword_phrase(requested_clean),
            ) >= 0.88
        ),
        None,
    )
    if requested_candidate is None:
        requested_candidate = {
            **fallback_candidate,
            "publishing_site_relevance": int(llm_payload.get("publishing_site_relevance") or fallback_candidate["publishing_site_relevance"]),
            "target_site_relevance": int(llm_payload.get("target_site_relevance") or fallback_candidate["target_site_relevance"]),
            "informational_value": int(llm_payload.get("informational_value") or fallback_candidate["informational_value"]),
            "backlink_naturalness": int(llm_payload.get("backlink_naturalness") or fallback_candidate["backlink_naturalness"]),
            "spam_risk": int(llm_payload.get("spam_risk") or fallback_candidate["spam_risk"]),
            "total_score": int(llm_payload.get("total_score") or fallback_candidate["total_score"]),
            "backlink_angle": str(llm_payload.get("backlink_angle") or fallback_candidate["backlink_angle"]).strip(),
        }
    requested_candidate["topic"] = requested_clean
    requested_candidate["seo_plausibility"] = max(
        1,
        min(
            10,
            int(requested_candidate.get("publishing_site_relevance") or 0)
            + int(requested_candidate.get("target_site_relevance") or 0)
            + int(requested_candidate.get("informational_value") or 0)
            - int(requested_candidate.get("spam_risk") or 0),
        ),
    )
    requested_candidate["non_spamminess"] = max(1, min(10, 10 - int(requested_candidate.get("spam_risk") or 10)))
    requested_candidate["score_breakdown"] = {
        "publishing_site_relevance": int(requested_candidate.get("publishing_site_relevance") or 0),
        "target_site_relevance": int(requested_candidate.get("target_site_relevance") or 0),
        "informational_value": int(requested_candidate.get("informational_value") or 0),
        "backlink_naturalness": int(requested_candidate.get("backlink_naturalness") or 0),
        "spam_risk": int(requested_candidate.get("spam_risk") or 0),
    }
    final_match_decision = str(llm_payload.get("final_match_decision") or "").strip().lower()
    if final_match_decision not in {"accepted", "weak_fit", "hard_reject"}:
        final_match_decision = _pair_fit_candidate_decision(requested_candidate)
    shared_contexts = _dedupe_preserve_order([context for context in publishing_contexts if context in set(target_contexts)])
    why_this_topic_was_chosen = str(llm_payload.get("why_this_topic_was_chosen") or "").strip()
    if not why_this_topic_was_chosen:
        why_this_topic_was_chosen = (
            "Das angefragte Thema passt inhaltlich zur Publishing-Seite und laesst die Zielseite nur als nachrangige Zusatzressource zu."
            if final_match_decision == "accepted"
            else "Das angefragte Thema ist nur eingeschraenkt tragfaehig und braucht besondere redaktionelle Vorsicht."
        )
    best_overlap_reason = str(llm_payload.get("best_overlap_reason") or "").strip()
    if not best_overlap_reason:
        best_overlap_reason = _pair_fit_overlap_reason(final_match_decision, overlap_terms, shared_contexts)
    reject_reason = str(llm_payload.get("reject_reason") or llm_payload.get("rejection_reason") or "").strip()
    if final_match_decision == "accepted":
        reject_reason = ""
    elif not reject_reason:
        reject_reason = _pair_fit_reject_reason(final_match_decision, requested_candidate, overlap_terms)
    fit_score = int(llm_payload.get("fit_score") or 0)
    if fit_score <= 0:
        fit_score = max(0, min(100, int(requested_candidate.get("total_score") or 0) * 2))
    return {
        "pair_fit_mode": "validation",
        "publishing_site_topics": publishing_terms[:8],
        "target_site_topics": target_terms[:8],
        "publishing_site_contexts": publishing_contexts,
        "target_site_contexts": target_contexts,
        "intersection_contexts": shared_contexts,
        "overlap_terms": overlap_terms,
        "generated_bridge_topics": [dict(requested_candidate)],
        "score_breakdown": {
            "best_candidate": dict(requested_candidate),
            "shared_context_count": len(shared_contexts),
            "overlap_term_count": len(overlap_terms),
            "match_engine": "llm_validation",
        },
        "best_overlap_reason": best_overlap_reason,
        "topic_candidates": [dict(requested_candidate)],
        "final_article_topic": requested_clean,
        "requested_topic_evaluation": {
            **dict(requested_candidate),
            "decision": final_match_decision,
        },
        "why_this_topic_was_chosen": why_this_topic_was_chosen,
        "backlink_fit_ok": final_match_decision == "accepted",
        "fit_score": max(0, min(100, fit_score)),
        "decision": "accepted" if final_match_decision == "accepted" else "rejected",
        "final_match_decision": final_match_decision,
        "rejection_reason": reject_reason,
        "reject_reason": reject_reason,
    }


def _pair_fit_normalize_llm_payload(
    *,
    llm_payload: Dict[str, Any],
    publishing_terms: List[str],
    target_terms: List[str],
    publishing_contexts: List[str],
    target_contexts: List[str],
    overlap_terms: List[str],
    requested_topic: str,
) -> Dict[str, Any]:
    candidates = _coerce_pair_fit_topic_candidates(llm_payload.get("topic_candidates"))
    if len(candidates) != PAIR_FIT_CANDIDATE_COUNT:
        raise CreatorError(f"Pair fit returned invalid candidate count:{len(candidates)}")
    for candidate in candidates:
        seo_plausibility = max(
            1,
            min(
                10,
                candidate["publishing_site_relevance"]
                + candidate["target_site_relevance"]
                + candidate["informational_value"]
                - candidate["spam_risk"],
            ),
        )
        candidate["seo_plausibility"] = seo_plausibility
        candidate["non_spamminess"] = max(1, min(10, 10 - candidate["spam_risk"]))
        score_breakdown = candidate.get("score_breakdown") if isinstance(candidate.get("score_breakdown"), dict) else {}
        candidate["score_breakdown"] = {
            **score_breakdown,
            "publishing_site_relevance": candidate["publishing_site_relevance"],
            "target_site_relevance": candidate["target_site_relevance"],
            "informational_value": candidate["informational_value"],
            "backlink_naturalness": candidate["backlink_naturalness"],
            "spam_risk": candidate["spam_risk"],
        }
    final_match_decision = str(llm_payload.get("final_match_decision") or "").strip().lower()
    if final_match_decision not in {"accepted", "weak_fit", "hard_reject"}:
        raise CreatorError(f"Pair fit returned invalid final_match_decision:{final_match_decision or 'missing'}")
    final_topic = str(llm_payload.get("final_article_topic") or "").strip()
    if not final_topic:
        final_topic = str(candidates[0].get("topic") or "").strip()
    if not final_topic and requested_topic.strip():
        final_topic = requested_topic.strip()
    if not final_topic:
        raise CreatorError("Pair fit returned no final_article_topic.")
    requested_normalized = _normalize_keyword_phrase(requested_topic)
    requested_topic_evaluation: Optional[Dict[str, Any]] = None
    if requested_normalized:
        requested_candidate = next(
            (item for item in candidates if _keyword_similarity(_normalize_keyword_phrase(str(item.get("topic") or "")), requested_normalized) >= 0.88),
            None,
        )
        if requested_candidate is None:
            requested_candidate = _pair_fit_score_candidate(
                requested_topic.strip(),
                publishing_terms=publishing_terms,
                target_terms=target_terms,
                publishing_contexts=publishing_contexts,
                target_contexts=target_contexts,
                overlap_terms=overlap_terms,
                target_business_intent="informational",
            )
        requested_decision = _pair_fit_candidate_decision(requested_candidate)
        requested_topic_evaluation = {
            **dict(requested_candidate),
            "decision": requested_decision,
        }
        final_topic = str(requested_candidate.get("topic") or "").strip() or requested_topic.strip() or final_topic
        final_match_decision = requested_decision
    selected_candidate = next(
        (item for item in candidates if _keyword_similarity(_normalize_keyword_phrase(str(item.get("topic") or "")), _normalize_keyword_phrase(final_topic)) >= 0.88),
        candidates[0],
    )
    best_candidate = max(
        candidates,
        key=lambda item: (
            int(item.get("total_score") or 0),
            int(item.get("publishing_site_relevance") or 0),
            int(item.get("target_site_relevance") or 0),
            int(item.get("backlink_naturalness") or 0),
            -int(item.get("spam_risk") or 10),
        ),
    )
    if requested_normalized:
        selected_candidate = requested_topic_evaluation or selected_candidate
        best_candidate = selected_candidate
    else:
        final_topic_score = _pair_fit_score_candidate(
            final_topic,
            publishing_terms=publishing_terms,
            target_terms=target_terms,
            publishing_contexts=publishing_contexts,
            target_contexts=target_contexts,
            overlap_terms=overlap_terms,
            target_business_intent="informational",
        )
        if (
            int(final_topic_score.get("publishing_site_relevance") or 0) < 5
            or int(final_topic_score.get("target_site_relevance") or 0) < 5
            or int(best_candidate.get("total_score") or 0) >= int(final_topic_score.get("total_score") or 0) + 3
        ):
            final_topic = str(best_candidate.get("topic") or "").strip() or final_topic
            selected_candidate = next(
                (
                    item
                    for item in candidates
                    if _keyword_similarity(
                        _normalize_keyword_phrase(str(item.get("topic") or "")),
                        _normalize_keyword_phrase(final_topic),
                    )
                    >= 0.88
                ),
                best_candidate,
            )
        else:
            best_candidate = selected_candidate
    shared_contexts = _dedupe_preserve_order([context for context in publishing_contexts if context in set(target_contexts)])
    why_this_topic_was_chosen = str(llm_payload.get("why_this_topic_was_chosen") or "").strip()
    if not why_this_topic_was_chosen:
        why_this_topic_was_chosen = (
            "Das Thema bleibt klar informativ, passt zum Publishing-Kontext und nutzt die Zielseite nur als nachrangige Zusatzressource."
            if final_match_decision == "accepted"
            else "Das Thema ist als Bruecke denkbar, braucht aber besondere redaktionelle Vorsicht, damit der Verweis nicht werblich wirkt."
        )
    best_overlap_reason = str(llm_payload.get("best_overlap_reason") or "").strip()
    if not best_overlap_reason:
        best_overlap_reason = _pair_fit_overlap_reason(final_match_decision, overlap_terms, shared_contexts)
    reject_reason = str(llm_payload.get("reject_reason") or llm_payload.get("rejection_reason") or "").strip()
    if final_match_decision == "accepted":
        reject_reason = ""
    elif not reject_reason:
        reject_reason = _pair_fit_reject_reason(final_match_decision, best_candidate, overlap_terms)
    fit_score = int(llm_payload.get("fit_score") or 0)
    if requested_normalized:
        fit_score = max(0, min(100, int(best_candidate.get("total_score") or 0) * 2))
    elif fit_score <= 0:
        fit_score = max(0, min(100, int(best_candidate.get("total_score") or 0) * 2))
    return {
        "pair_fit_mode": "selection",
        "publishing_site_topics": publishing_terms[:8],
        "target_site_topics": target_terms[:8],
        "publishing_site_contexts": publishing_contexts,
        "target_site_contexts": target_contexts,
        "intersection_contexts": shared_contexts,
        "overlap_terms": overlap_terms,
        "generated_bridge_topics": [dict(item) for item in candidates],
        "score_breakdown": {
            "best_candidate": dict(best_candidate),
            "shared_context_count": len(shared_contexts),
            "overlap_term_count": len(overlap_terms),
            "match_engine": "llm_hybrid",
        },
        "best_overlap_reason": best_overlap_reason,
        "topic_candidates": candidates,
        "final_article_topic": final_topic,
        "requested_topic_evaluation": requested_topic_evaluation or {},
        "why_this_topic_was_chosen": why_this_topic_was_chosen,
        "backlink_fit_ok": final_match_decision == "accepted",
        "fit_score": max(0, min(100, fit_score)),
        "decision": "accepted" if final_match_decision == "accepted" else "rejected",
        "final_match_decision": final_match_decision,
        "rejection_reason": reject_reason,
        "reject_reason": reject_reason,
    }


def _run_pair_fit_reasoning(
    *,
    requested_topic: str,
    exclude_topics: List[str],
    target_site_url: str,
    publishing_site_url: str,
    target_profile: Dict[str, Any],
    publishing_profile: Dict[str, Any],
    llm_api_key: str,
    llm_base_url: str,
    planning_model: str,
    timeout_seconds: int,
    usage_collector: Optional[Callable[[Dict[str, Any]], None]],
) -> Dict[str, Any]:
    publishing_terms = _pair_fit_ranked_terms(publishing_profile, site_kind="publishing")
    target_terms = _pair_fit_ranked_terms(target_profile, site_kind="target")
    publishing_contexts = _pair_fit_expand_contexts(publishing_profile, ranked_terms=publishing_terms)
    target_contexts = _pair_fit_expand_contexts(target_profile, ranked_terms=target_terms)
    overlap_terms = _pair_fit_overlap_terms(publishing_terms, target_terms)
    generated_topics = _pair_fit_generate_bridge_topics(
        requested_topic=requested_topic,
        exclude_topics=exclude_topics,
        publishing_terms=publishing_terms,
        target_terms=target_terms,
        publishing_contexts=publishing_contexts,
        target_contexts=target_contexts,
    )
    heuristic_candidates = [
        _pair_fit_score_candidate(
            topic,
            publishing_terms=publishing_terms,
            target_terms=target_terms,
            publishing_contexts=publishing_contexts,
            target_contexts=target_contexts,
            overlap_terms=overlap_terms,
            target_business_intent=str(target_profile.get("business_intent") or "").strip().lower(),
        )
        for topic in generated_topics
    ]
    heuristic_candidates = sorted(
        heuristic_candidates,
        key=lambda item: (
            -int(item.get("total_score") or 0),
            -int(item.get("publishing_site_relevance") or 0),
            -int(item.get("target_site_relevance") or 0),
            int(item.get("spam_risk") or 10),
            str(item.get("topic") or ""),
        ),
    )[:PAIR_FIT_CANDIDATE_COUNT]
    if len(heuristic_candidates) < PAIR_FIT_CANDIDATE_COUNT:
        raise CreatorError(f"Pair fit returned invalid candidate count:{len(heuristic_candidates)}")
    pair_fit_mode = "validation" if requested_topic.strip() else "selection"
    input_payload = _pair_fit_llm_input_payload(
        mode=pair_fit_mode,
        requested_topic=requested_topic,
        exclude_topics=exclude_topics,
        target_site_url=target_site_url,
        publishing_site_url=publishing_site_url,
        target_profile=target_profile,
        publishing_profile=publishing_profile,
        publishing_terms=publishing_terms,
        target_terms=target_terms,
        publishing_contexts=publishing_contexts,
        target_contexts=target_contexts,
        overlap_terms=overlap_terms,
        heuristic_candidates=heuristic_candidates,
    )
    system_prompt, user_prompt = _pair_fit_llm_prompts(input_payload)
    llm_payload = call_llm_json(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=llm_api_key,
        base_url=llm_base_url,
        model=planning_model,
        timeout_seconds=timeout_seconds,
        max_tokens=1800 if pair_fit_mode == "validation" else 3000,
        temperature=0.1,
        request_label="phase3_pair_fit_validate" if pair_fit_mode == "validation" else "phase3_pair_fit_select",
        usage_collector=usage_collector,
    )
    if pair_fit_mode == "validation":
        return _pair_fit_normalize_requested_topic_payload(
            llm_payload=llm_payload,
            requested_topic=requested_topic,
            publishing_terms=publishing_terms,
            target_terms=target_terms,
            publishing_contexts=publishing_contexts,
            target_contexts=target_contexts,
            overlap_terms=overlap_terms,
            target_business_intent=str(target_profile.get("business_intent") or "").strip().lower(),
        )
    return _pair_fit_normalize_llm_payload(
        llm_payload=llm_payload,
        publishing_terms=publishing_terms,
        target_terms=target_terms,
        publishing_contexts=publishing_contexts,
        target_contexts=target_contexts,
        overlap_terms=overlap_terms,
        requested_topic=requested_topic,
    )


def run_pair_fit_pipeline(
    *,
    target_site_url: str,
    publishing_site_url: str,
    publishing_site_id: Optional[str],
    client_target_site_id: Optional[str],
    requested_topic: Optional[str],
    exclude_topics: Optional[List[str]],
    target_profile_payload: Optional[Dict[str, Any]],
    target_profile_content_hash: Optional[str],
    publishing_profile_payload: Optional[Dict[str, Any]],
    publishing_profile_content_hash: Optional[str],
) -> Dict[str, Any]:
    target_profile = _coerce_site_profile_payload(target_profile_payload)
    publishing_profile = _coerce_site_profile_payload(publishing_profile_payload)
    if not target_profile or not publishing_profile:
        raise CreatorError("Pair fit requires target_profile and publishing_profile.")

    explicit_llm_key = os.getenv("CREATOR_LLM_API_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    explicit_shared_model = os.getenv("CREATOR_LLM_MODEL", "").strip()
    explicit_planning_model = os.getenv("CREATOR_LLM_MODEL_PLANNING", "").strip()
    explicit_writing_model = os.getenv("CREATOR_LLM_MODEL_WRITING", "").strip()
    planning_model = explicit_planning_model or explicit_shared_model
    writing_model = explicit_writing_model or explicit_shared_model
    explicit_base_url = os.getenv("CREATOR_LLM_BASE_URL", "").strip()
    if not planning_model:
        planning_model = DEFAULT_ANTHROPIC_PLANNING_MODEL if anthropic_key else DEFAULT_OPENAI_LLM_MODEL
    if not writing_model:
        writing_model = DEFAULT_ANTHROPIC_WRITING_MODEL if anthropic_key else DEFAULT_OPENAI_LLM_MODEL
    if explicit_base_url:
        llm_base_url = explicit_base_url
    elif anthropic_key and _model_prefers_anthropic(planning_model, writing_model):
        llm_base_url = "https://api.anthropic.com/v1"
    elif anthropic_key and not openai_key:
        llm_base_url = "https://api.anthropic.com/v1"
    else:
        llm_base_url = DEFAULT_LLM_BASE_URL
    if "anthropic" in llm_base_url.lower():
        llm_api_key = explicit_llm_key or anthropic_key or openai_key
    else:
        llm_api_key = explicit_llm_key or openai_key or anthropic_key
    if not llm_api_key:
        raise CreatorError("No LLM API key configured for pair fit.")

    target_profile_hash = (target_profile_content_hash or "").strip() or _hash_text(
        json.dumps(target_profile, sort_keys=True, ensure_ascii=False)
    )
    publishing_profile_hash = (publishing_profile_content_hash or "").strip() or _hash_text(
        json.dumps(publishing_profile, sort_keys=True, ensure_ascii=False)
    )
    pair_fit = _run_pair_fit_reasoning(
        requested_topic=(requested_topic or "").strip(),
        exclude_topics=list(exclude_topics or []),
        target_site_url=target_site_url,
        publishing_site_url=publishing_site_url,
        target_profile=target_profile,
        publishing_profile=publishing_profile,
        llm_api_key=llm_api_key,
        llm_base_url=llm_base_url,
        planning_model=planning_model,
        timeout_seconds=max(1, _read_int_env("CREATOR_HTTP_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)),
        usage_collector=None,
    )
    return {
        "ok": True,
        "cached": False,
        "pair_fit": pair_fit,
        "publishing_profile_hash": publishing_profile_hash,
        "target_profile_hash": target_profile_hash,
        "prompt_version": PAIR_FIT_PROMPT_VERSION,
        "model_name": planning_model,
    }


def _infer_meta_description(html: str) -> str:
    excerpt = ""
    match = re.search(r"<p[^>]*>(.*?)</p>", html or "", flags=re.IGNORECASE | re.DOTALL)
    if match:
        excerpt = re.sub(r"<[^>]+>", "", match.group(1)).strip()
    return excerpt[:160]


def _derive_slug(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", "", value or "").strip().lower()
    cleaned = (
        cleaned.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    cleaned = re.sub(r"[^a-z0-9]+", "-", cleaned)
    return cleaned.strip("-")[:80]


def _fill_article_metadata(article_payload: Dict[str, Any], fallback_title: str) -> Dict[str, Any]:
    html = str(article_payload.get("article_html") or "").strip()
    excerpt = str(article_payload.get("excerpt") or "").strip()
    if not excerpt:
        excerpt = _infer_meta_description(html)[:200]
    meta_title = str(article_payload.get("meta_title") or "").strip() or fallback_title
    meta_description = str(article_payload.get("meta_description") or "").strip() or _infer_meta_description(html)
    slug = str(article_payload.get("slug") or "").strip() or _derive_slug(meta_title or fallback_title)
    article_payload["meta_title"] = meta_title
    article_payload["meta_description"] = meta_description
    article_payload["slug"] = slug
    article_payload["excerpt"] = excerpt
    return article_payload


def _apply_deterministic_article_metadata(
    article_payload: Dict[str, Any],
    *,
    phase3: Dict[str, Any],
    phase4: Dict[str, Any],
    reset_excerpt: bool = False,
) -> Dict[str, Any]:
    if reset_excerpt:
        article_payload["excerpt"] = ""
    article_payload["meta_title"] = phase3["title_package"]["meta_title"]
    article_payload["slug"] = phase3["title_package"]["slug"]
    article_payload["meta_description"] = _build_deterministic_meta_description(
        topic=phase3["final_article_topic"],
        primary_keyword=phase3["primary_keyword"],
        secondary_keywords=phase3.get("secondary_keywords") or [],
        structured_mode=phase3.get("structured_content_mode", "none"),
    )
    return _fill_article_metadata(article_payload, phase4["h1"])


def _build_deterministic_image_prompts(topic: str) -> Dict[str, Any]:
    cleaned_topic = (topic or "").strip() or "Industry insights"
    return {
        "featured_prompt": f"Editorial hero image illustrating: {cleaned_topic}",
        "featured_alt": cleaned_topic,
        "include_in_content": False,
        "in_content_prompt": "",
        "in_content_alt": "",
    }


def _extract_keywords(text: str, max_terms: int = 10) -> List[str]:
    words = re.findall(r"\b[a-zA-Z][a-zA-Z-]{2,}\b", (text or "").lower())
    counts: Dict[str, int] = {}
    for word in words:
        normalized = _normalize_keyword_phrase(word)
        if not normalized:
            continue
        if normalized in STOPWORDS or normalized in GERMAN_FUNCTION_WORDS or normalized in ENGLISH_FUNCTION_WORDS:
            continue
        if normalized in KEYWORD_LOW_SIGNAL_TOKENS or normalized in GENERIC_UI_CHROME_TOKENS:
            continue
        if normalized in PAIR_FIT_BOILERPLATE_TOKENS:
            continue
        counts[normalized] = counts.get(normalized, 0) + 1
    sorted_terms = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [term for term, _ in sorted_terms[:max_terms]]


def _guess_brand_name(target_url: str, html: str) -> str:
    meta = extract_meta_content(html, [("property", "og:site_name"), ("name", "application-name")])
    if meta:
        return meta
    title = extract_page_title(html)
    if title:
        for sep in ["|", "-", "–", ":"]:
            if sep in title:
                return title.split(sep)[0].strip()
        return title.strip()
    host = urlparse(target_url).netloc
    return host.replace("www.", "").split(".")[0].replace("-", " ").title()


def _normalize_brand_name(value: str) -> str:
    cleaned = html.unescape(str(value or "")).strip()
    if not cleaned:
        return ""
    domain_match = re.search(r"\b([\w-]+(?:\.[\w-]+)+)\b", cleaned)
    if domain_match:
        return domain_match.group(1).strip()
    for sep in ("|", "–", ":", " - "):
        if sep in cleaned:
            cleaned = cleaned.split(sep, 1)[0].strip()
            break
    return re.sub(r"\s+", " ", cleaned).strip()


def _pick_backlink_url(target_url: str, html: str) -> str:
    canonical = extract_canonical_link(html)
    if canonical:
        try:
            target = urlparse(target_url)
            canon = urlparse(canonical)
            if target.netloc and canon.netloc and target.netloc == canon.netloc and target.path == canon.path:
                return canonical
        except Exception:
            pass
    return target_url


def _is_anchor_safe(anchor: Optional[str]) -> bool:
    if not anchor:
        return False
    cleaned = _normalize_brand_name(anchor) or anchor.strip()
    if len(cleaned) < 2 or len(cleaned) > 80:
        return False
    if re.search(r"https?://", cleaned):
        return False
    if _anchor_text_has_promotional_noise(cleaned):
        return False
    lowered = cleaned.lower()
    if any(term in lowered for term in ["visit our", "buy now", "click here", "limited time"]):
        return False
    return True


def _anchor_text_has_promotional_noise(value: str) -> bool:
    normalized = _normalize_keyword_phrase(html.unescape(str(value or "")).strip())
    if not normalized:
        return True
    tokens = set(normalized.split())
    promo_hits = tokens & PAIR_FIT_PROMO_TOKENS
    ui_hits = tokens & GENERIC_UI_CHROME_TOKENS
    if promo_hits or {"onlineshop", "shop"} & tokens:
        return True
    if ui_hits and len(tokens) <= len(ui_hits) + 1:
        return True
    return False


def _build_anchor_text(anchor_type: str, brand_name: str, keyword_cluster: List[str]) -> str:
    normalized_brand = _normalize_brand_name(brand_name)
    if anchor_type == "brand" and normalized_brand and not _anchor_text_has_promotional_noise(normalized_brand):
        return normalized_brand
    if anchor_type == "partial_match" and keyword_cluster:
        return " ".join(keyword_cluster[:3]).title()
    return "Weitere Informationen"


def _strip_code_fences(text: str) -> str:
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:html)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _strip_html_tags(value: str) -> str:
    stripped = re.sub(r"<[^>]+>", " ", value or "", flags=re.IGNORECASE | re.DOTALL)
    return html.unescape(stripped).replace("\xa0", " ")


def _tokenize_words(text: str) -> List[str]:
    return re.findall(r"\b[a-zA-ZäöüÄÖÜß]{2,}\b", (text or "").lower())


def _looks_english_heavy(text: str) -> bool:
    words = _tokenize_words(text)
    if len(words) < 120:
        return False
    de_hits = sum(1 for word in words if word in GERMAN_FUNCTION_WORDS)
    en_hits = sum(1 for word in words if word in ENGLISH_FUNCTION_WORDS)
    return en_hits >= 10 and en_hits > int(de_hits * 1.2)


def _extract_h2_headings(html: str) -> List[str]:
    headings: List[str] = []
    for match in re.finditer(r"<h2[^>]*>(.*?)</h2>", html or "", flags=re.IGNORECASE | re.DOTALL):
        heading = _strip_html_tags(match.group(1)).strip()
        if heading:
            headings.append(heading)
    return headings


def _extract_h2_section_html(html: str, heading_name: str) -> str:
    matches = list(re.finditer(r"<h2[^>]*>(.*?)</h2>", html or "", flags=re.IGNORECASE | re.DOTALL))
    if not matches:
        return ""
    normalized_heading = _normalize_keyword_phrase(heading_name)
    document = html or ""
    for index, match in enumerate(matches):
        heading = _normalize_keyword_phrase(_strip_html_tags(match.group(1)))
        if heading != normalized_heading:
            continue
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(document)
        return document[start:end].strip()
    return ""


def _extract_h2_section_text(html: str, heading_name: str) -> str:
    return _strip_html_tags(_extract_h2_section_html(html, heading_name)).strip()


def _normalize_faq_section_questions(article_html: str) -> str:
    html = article_html or ""
    if not html:
        return html
    soup = BeautifulSoup(html, "lxml")
    body = soup.body or soup
    faq_h2 = next(
        (
            heading
            for heading in body.find_all("h2")
            if _normalize_keyword_phrase(heading.get_text(" ", strip=True)) == "faq"
        ),
        None,
    )
    if faq_h2 is None:
        return html

    current = faq_h2.next_sibling
    while current is not None:
        next_node = current.next_sibling
        if getattr(current, "name", None) == "h2":
            break
        if getattr(current, "name", None) == "h3":
            question_text = current.get_text(" ", strip=True)
            formatted_question = _format_faq_question(question_text) or question_text
            if formatted_question and not formatted_question.endswith("?"):
                formatted_question = formatted_question.rstrip(".! ") + "?"
            current.clear()
            current.append(formatted_question)
        current = next_node

    return body.decode_contents() if getattr(body, "decode_contents", None) else str(body)


def _topic_keywords(topic: str, *, max_terms: int = 5) -> List[str]:
    words = _tokenize_words(topic)
    out: List[str] = []
    seen: set[str] = set()
    for word in words:
        if len(word) < 4:
            continue
        if word in STOPWORDS or word in GERMAN_FUNCTION_WORDS or word in ENGLISH_FUNCTION_WORDS:
            continue
        if word in seen:
            continue
        seen.add(word)
        out.append(word)
        if len(out) >= max_terms:
            break
    return out


def _topic_focus_terms(topic: str, *, max_terms: int = 2) -> List[str]:
    out: List[str] = []
    for term in _topic_keywords(topic, max_terms=max_terms * 3):
        if term in PAIR_FIT_AUDIENCE_TOKENS or term in KEYWORD_LOW_SIGNAL_TOKENS:
            continue
        if term in GERMAN_KEYWORD_MODIFIERS:
            continue
        if term in out:
            continue
        out.append(term)
        if len(out) >= max_terms:
            break
    if out:
        return out
    fallback = _normalize_keyword_phrase(_extract_topic_subject_phrase(topic) or topic)
    if not fallback:
        return []
    return [" ".join(fallback.split()[: max(1, max_terms)])]


def _normalize_keyword_phrase(value: str) -> str:
    cleaned = re.sub(r"[^\wäöüÄÖÜß\s-]", " ", (value or "").strip().lower())
    cleaned = re.sub(r"[_-]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _fold_keyword_text(value: str) -> str:
    return (
        str(value or "")
        .replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )


def _keyword_token_set(value: str) -> set[str]:
    tokens = _tokenize_words(_normalize_keyword_phrase(value))
    return {
        token
        for token in tokens
        if token not in STOPWORDS
        and token not in GERMAN_FUNCTION_WORDS
        and token not in ENGLISH_FUNCTION_WORDS
    }


def _keyword_focus_tokens(value: str) -> set[str]:
    return {
        token
        for token in _keyword_token_set(value)
        if token not in KEYWORD_LOW_SIGNAL_TOKENS and token not in GERMAN_KEYWORD_MODIFIERS
    }


def _filter_keyword_focus_tokens(tokens: set[str]) -> set[str]:
    return {
        token
        for token in tokens
        if token not in KEYWORD_LOW_SIGNAL_TOKENS and token not in GERMAN_KEYWORD_MODIFIERS
    }


def _is_valid_keyword_phrase(value: str) -> bool:
    normalized = _normalize_keyword_phrase(value)
    if not normalized:
        return False
    words = normalized.split()
    if not (KEYWORD_MIN_WORDS <= len(words) <= KEYWORD_MAX_WORDS):
        return False
    if len(normalized) < 6 or len(normalized) > 90:
        return False
    if any(token.isdigit() for token in words):
        return False
    if any(any(char.isdigit() for char in token) and any(char.isalpha() for char in token) for token in words):
        return False
    return len(_keyword_token_set(normalized)) >= 2


def _is_low_signal_keyword_phrase(value: str) -> bool:
    normalized = _normalize_keyword_phrase(value)
    if not normalized:
        return True
    tokens = [token for token in _keyword_token_set(normalized) if token]
    if not tokens:
        return True
    low_signal_hits = sum(1 for token in tokens if token in KEYWORD_LOW_SIGNAL_TOKENS)
    if low_signal_hits >= len(tokens):
        return True
    if len(tokens) >= 3 and low_signal_hits >= len(tokens) - 1:
        return True
    return False


def _sanitize_editorial_phrase(value: str, *, allow_single_token: bool = False) -> str:
    cleaned = re.sub(r"\([^)]*\d[^)]*\)", " ", str(value or "").strip())
    cleaned = re.sub(r"\[[^\]]*\d[^\]]*\]", " ", cleaned)
    cleaned = re.sub(r"\b[\w.-]+\.(?:de|com|net|org|at|ch)\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[>|»•]+", " ", cleaned)
    cleaned = re.sub(r"(?<!&)\bamp\b(?!;)", " ", cleaned, flags=re.IGNORECASE)
    normalized = _normalize_keyword_phrase(cleaned)
    if not normalized:
        return ""
    if _phrase_has_editorial_noise(normalized):
        return ""
    tokens = _keyword_token_set(normalized)
    if not tokens:
        return ""
    folded_tokens = set(_normalize_keyword_phrase(_fold_keyword_text(normalized)).split())
    ui_hits = sum(
        1
        for token in folded_tokens
        if token in GENERIC_UI_CHROME_TOKENS or token in PAIR_FIT_BOILERPLATE_TOKENS
    )
    promo_hits = sum(1 for token in folded_tokens if token in PAIR_FIT_PROMO_TOKENS)
    if ui_hits >= max(1, len(tokens) - 1):
        return ""
    if len(tokens) >= 3 and (ui_hits + promo_hits) >= len(tokens) - 1:
        return ""
    if "onlineshop" in folded_tokens and promo_hits >= 1:
        return ""
    if promo_hits >= max(1, len(tokens) - 1) and len(folded_tokens - PAIR_FIT_PROMO_TOKENS) <= 1:
        return ""
    if {"brillen", "komplettbrillen"} <= folded_tokens and {"guenstig", "guenstige"} & folded_tokens:
        return ""
    if folded_tokens and all(
        token in PAIR_FIT_AUDIENCE_TOKENS
        or token in INTERNAL_LINK_GENERIC_TOKENS
        or token in KEYWORD_LOW_SIGNAL_TOKENS
        for token in folded_tokens
    ):
        return ""
    if len(tokens) < KEYWORD_MIN_WORDS:
        if allow_single_token and len(tokens) == 1:
            return normalized
        return ""
    return normalized if _is_valid_keyword_phrase(normalized) else ""


def _is_brand_identity_phrase(value: str, brand_name: str) -> bool:
    normalized_value = _sanitize_editorial_phrase(value, allow_single_token=True)
    normalized_brand = _normalize_brand_name(brand_name) or _sanitize_editorial_phrase(brand_name, allow_single_token=True)
    if not normalized_value or not normalized_brand:
        return False
    brand_tokens = _filter_keyword_focus_tokens(_keyword_focus_tokens(normalized_brand))
    value_tokens = _filter_keyword_focus_tokens(_keyword_focus_tokens(normalized_value))
    if not brand_tokens or not value_tokens:
        return False
    if not (brand_tokens & value_tokens):
        return False
    if _keyword_similarity(normalized_value, normalized_brand) >= 0.72:
        return True
    if brand_tokens <= value_tokens and len(value_tokens - brand_tokens) <= 2:
        return True
    return False


def _keyword_similarity(a: str, b: str) -> float:
    ta = _keyword_token_set(a)
    tb = _keyword_token_set(b)
    if not ta or not tb:
        return 0.0
    intersection = len(ta & tb)
    union = len(ta | tb)
    if union == 0:
        return 0.0
    return intersection / union


def _tokens_share_same_family_variant(first: str, second: str) -> bool:
    first_normalized = _normalize_keyword_phrase(first)
    second_normalized = _normalize_keyword_phrase(second)
    if not first_normalized or not second_normalized or first_normalized == second_normalized:
        return first_normalized == second_normalized and bool(first_normalized)
    shorter, longer = (
        (first_normalized, second_normalized)
        if len(first_normalized) <= len(second_normalized)
        else (second_normalized, first_normalized)
    )
    if len(shorter) < 5:
        return False
    if shorter in KEYWORD_LOW_SIGNAL_TOKENS or shorter in ABSTRACT_QUERY_TOKENS:
        return False
    if len(longer) - len(shorter) > 3:
        return False
    return longer.startswith(shorter)


def _phrase_has_same_family_duplicate_tokens(value: str) -> bool:
    focus_tokens = [
        token
        for token in _filter_keyword_focus_tokens(_keyword_focus_tokens(value))
        if token not in ABSTRACT_QUERY_TOKENS
        and token not in EDITORIAL_ACTION_TOKENS
    ]
    if len(focus_tokens) < 2:
        return False
    unique_tokens = sorted(set(focus_tokens))
    for index, token in enumerate(unique_tokens):
        for other in unique_tokens[index + 1 :]:
            if _tokens_share_same_family_variant(token, other):
                return True
    return False


def _dedupe_keyword_phrases(values: List[str]) -> List[str]:
    out: List[str] = []
    for item in values:
        normalized = _sanitize_editorial_phrase(item)
        if not normalized:
            continue
        if _is_low_signal_keyword_phrase(normalized):
            continue
        if _phrase_has_same_family_duplicate_tokens(normalized):
            continue
        if any(_keyword_similarity(normalized, existing) >= 0.75 for existing in out):
            continue
        out.append(normalized)
    return out


def _sanitize_semantic_term(value: str, *, allow_single_token: bool = True) -> str:
    normalized = _sanitize_editorial_phrase(value, allow_single_token=allow_single_token)
    if not normalized or _looks_like_question_phrase(normalized):
        return ""
    words = normalized.split()
    if len(words) > 4:
        return ""
    if sum(1 for word in words if word in INTERNAL_LINK_GENERIC_TOKENS) >= len(words):
        return ""
    return normalized


def _dedupe_semantic_terms(values: List[str], *, max_items: int = KEYWORD_MAX_SEMANTIC_ENTITIES) -> List[str]:
    out: List[str] = []
    for item in values:
        normalized = _sanitize_semantic_term(item, allow_single_token=True)
        if not normalized:
            continue
        if any(
            normalized == existing
            or _keyword_similarity(normalized, existing) >= 0.75
            for existing in out
        ):
            continue
        out.append(normalized)
        if len(out) >= max_items:
            break
    return out


def _keyword_candidate_is_query_like(
    candidate: str,
    *,
    topic: str,
    primary_keyword: str,
    topic_signature: Optional[Dict[str, Any]] = None,
) -> bool:
    normalized = _sanitize_editorial_phrase(candidate)
    if not normalized or _keyword_candidate_has_question_noise(normalized):
        return False
    if _phrase_has_same_family_duplicate_tokens(normalized):
        return False
    words = normalized.split()
    if not (KEYWORD_MIN_WORDS <= len(words) <= KEYWORD_QUERY_MAX_WORDS):
        return False
    reference_phrase = str((topic_signature or {}).get("subject_phrase") or primary_keyword or topic).strip()
    similarity = max(
        _keyword_similarity(normalized, topic),
        _keyword_similarity(normalized, primary_keyword),
        _keyword_similarity(normalized, reference_phrase),
    )
    stats = _topic_signature_candidate_stats(normalized, topic_signature)
    reference_tokens = _filter_topic_signature_tokens(_keyword_focus_tokens(f"{topic} {primary_keyword} {reference_phrase}"))
    reference_domain_tokens = {
        token
        for token in reference_tokens
        if token not in EDITORIAL_ACTION_TOKENS and token not in ABSTRACT_QUERY_TOKENS
    }
    candidate_domain_tokens = {
        token
        for token in _filter_topic_signature_tokens(_keyword_focus_tokens(normalized))
        if token not in EDITORIAL_ACTION_TOKENS and token not in ABSTRACT_QUERY_TOKENS
    }
    if not candidate_domain_tokens:
        return False
    family_domain_overlap = {
        token
        for token in candidate_domain_tokens
        if _token_matches_reference_family(token, reference_domain_tokens, allow_prefix_match=False)
    }
    if (
        reference_domain_tokens
        and not (candidate_domain_tokens & reference_domain_tokens)
        and not family_domain_overlap
        and not stats["specific_overlap"]
        and len(stats["broad_overlap"]) < 2
        and similarity < 0.7
    ):
        return False
    off_topic_action_hits = {
        word for word in words
        if word in EDITORIAL_ACTION_TOKENS and word not in reference_tokens
    }
    if _keyword_candidate_is_support_topic_noise(normalized, topic_signature) and similarity < 0.62:
        return False
    if " als " in f" {normalized} " and similarity < 0.68:
        return False
    if len(words) >= 4 and off_topic_action_hits and similarity < 0.68:
        return False
    if len(words) >= 5 and similarity < 0.72:
        return False
    if not stats["specific_overlap"] and not stats["broad_overlap"] and similarity < 0.55:
        return False
    return True


def _select_semantic_entities(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    keyword_cluster: List[str],
    trend_candidates: List[str],
    target_terms: List[str],
    overlap_terms: List[str],
    topic_signature: Optional[Dict[str, Any]],
    max_items: int = KEYWORD_MAX_SEMANTIC_ENTITIES,
) -> List[str]:
    secondary_pool = _dedupe_keyword_phrases(secondary_keywords)
    primary_tokens = _filter_topic_signature_tokens(_keyword_focus_tokens(primary_keyword))
    phrase_scores: Dict[str, float] = {}
    token_scores: Dict[str, float] = {}

    def _add_candidates(values: List[str], *, weight: float) -> None:
        for raw_value in values:
            normalized = _sanitize_semantic_term(raw_value, allow_single_token=True)
            if not normalized:
                continue
            if _keyword_similarity(normalized, primary_keyword) >= 0.82:
                continue
            if any(_keyword_similarity(normalized, existing) >= 0.82 for existing in secondary_pool):
                continue
            stats = _topic_signature_candidate_stats(normalized, topic_signature)
            if (
                not stats["specific_overlap"]
                and len(stats["broad_overlap"]) < 1
                and _keyword_similarity(normalized, topic) < 0.35
            ):
                continue
            if len(normalized.split()) >= 4 and _keyword_candidate_is_support_topic_noise(normalized, topic_signature):
                continue
            score = (
                weight
                + (2.8 * len(stats["specific_overlap"]))
                + (1.3 * len(stats["broad_overlap"]))
                + _keyword_similarity(normalized, primary_keyword)
            )
            if len(normalized.split()) <= 3:
                phrase_scores[normalized] = max(phrase_scores.get(normalized, 0.0), score)
            for token in _filter_topic_signature_tokens(_keyword_focus_tokens(normalized)):
                if len(token) < 6 or token in primary_tokens:
                    continue
                token_scores[token] = max(token_scores.get(token, 0.0), score - 0.4)

    _add_candidates(overlap_terms, weight=4.6)
    _add_candidates(keyword_cluster, weight=4.2)
    _add_candidates(target_terms, weight=2.6)
    _add_candidates(secondary_keywords, weight=2.4)

    ranked_terms = [
        term
        for term, _score in sorted(phrase_scores.items(), key=lambda item: (-item[1], item[0]))
    ]
    ranked_tokens = [
        token
        for token, _score in sorted(token_scores.items(), key=lambda item: (-item[1], item[0]))
    ]
    return _dedupe_semantic_terms(ranked_terms + ranked_tokens, max_items=max_items)


def _keyword_source_match(
    candidate: str,
    value: str,
    *,
    allow_single_token: bool = False,
) -> tuple[float, str]:
    normalized_candidate = _normalize_keyword_phrase(candidate)
    normalized_value = _sanitize_editorial_phrase(value, allow_single_token=allow_single_token)
    if not normalized_candidate or not normalized_value:
        return 0.0, ""
    candidate_tokens = _filter_topic_signature_tokens(_keyword_focus_tokens(normalized_candidate))
    value_tokens = _filter_topic_signature_tokens(_keyword_focus_tokens(normalized_value))
    if not candidate_tokens or not value_tokens:
        return 0.0, normalized_value
    overlap = len(candidate_tokens & value_tokens)
    token_score = overlap / max(1, min(len(candidate_tokens), len(value_tokens)))
    similarity = _keyword_similarity(normalized_candidate, normalized_value)
    return max(token_score, similarity), normalized_value


def _describe_keyword_bucket_entries(
    values: List[str],
    *,
    role: str,
    source_map: Dict[str, List[str]],
    topic_signature: Optional[Dict[str, Any]],
    topic: str,
    primary_keyword: str,
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw_value in values:
        normalized = _normalize_keyword_phrase(raw_value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        role_score = _topic_signature_candidate_score(normalized, topic_signature)
        is_query_like = _keyword_candidate_is_query_like(
            normalized,
            topic=topic,
            primary_keyword=primary_keyword,
            topic_signature=topic_signature,
        )
        if role in {"primary_query", "secondary_query"}:
            role_score += 1.4 if is_query_like else -2.2
        elif role == "semantic_entity":
            role_score += 0.8 if len(normalized.split()) <= 3 else 0.0
        elif role == "support_topic":
            role_score += 0.6 if not is_query_like else -1.8
        sources: List[Dict[str, Any]] = []
        for label, pool in source_map.items():
            best_score = 0.0
            best_match = ""
            allow_single = role in {"semantic_entity", "support_topic"}
            for item in pool:
                score, matched_value = _keyword_source_match(
                    normalized,
                    item,
                    allow_single_token=allow_single,
                )
                if score > best_score:
                    best_score = score
                    best_match = matched_value
            if best_score >= 0.45 and best_match:
                sources.append(
                    {
                        "source": label,
                        "matched_value": best_match,
                        "score": round(best_score, 3),
                    }
                )
        entries.append(
            {
                "value": normalized,
                "role": role,
                "score": round(role_score, 3),
                "query_like": is_query_like,
                "sources": sorted(sources, key=lambda item: (-float(item["score"]), item["source"])),
            }
        )
    return entries


def _build_keyword_provenance(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    semantic_entities: List[str],
    support_topics: List[str],
    keyword_cluster: List[str],
    trend_candidates: List[str],
    overlap_terms: List[str],
    target_terms: List[str],
    allowed_topics: List[str],
    internal_titles: List[str],
    topic_signature: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "primary_query": _describe_keyword_bucket_entries(
            [primary_keyword],
            role="primary_query",
            source_map={
                "topic": [topic],
                "keyword_cluster": keyword_cluster,
                "trend_candidates": trend_candidates,
            },
            topic_signature=topic_signature,
            topic=topic,
            primary_keyword=primary_keyword,
        ),
        "secondary_queries": _describe_keyword_bucket_entries(
            secondary_keywords,
            role="secondary_query",
            source_map={
                "topic": [topic],
                "keyword_cluster": keyword_cluster,
                "trend_candidates": trend_candidates,
            },
            topic_signature=topic_signature,
            topic=topic,
            primary_keyword=primary_keyword,
        ),
        "semantic_entities": _describe_keyword_bucket_entries(
            semantic_entities,
            role="semantic_entity",
            source_map={
                "overlap_terms": overlap_terms,
                "target_terms": target_terms,
                "keyword_cluster": keyword_cluster,
                "secondary_queries": secondary_keywords,
            },
            topic_signature=topic_signature,
            topic=topic,
            primary_keyword=primary_keyword,
        ),
        "support_topics_for_internal_links": _describe_keyword_bucket_entries(
            support_topics,
            role="support_topic",
            source_map={
                "publishing_topics": allowed_topics,
                "internal_titles": internal_titles,
            },
            topic_signature=topic_signature,
            topic=topic,
            primary_keyword=primary_keyword,
        ),
    }


def _select_support_topic_candidates(
    *,
    topic: str,
    primary_keyword: str,
    candidates: List[str],
    topic_signature: Optional[Dict[str, Any]],
    max_items: int = KEYWORD_MAX_SUPPORT_TOPICS,
) -> List[str]:
    out: List[str] = []
    for raw_value in candidates:
        normalized = _sanitize_editorial_phrase(raw_value)
        if not normalized:
            continue
        if _keyword_candidate_is_query_like(
            normalized,
            topic=topic,
            primary_keyword=primary_keyword,
            topic_signature=topic_signature,
        ):
            continue
        if not _topic_signature_candidate_has_relevance(normalized, topic_signature):
            continue
        if any(
            normalized == existing
            or _keyword_similarity(normalized, existing) >= 0.78
            for existing in out
        ):
            continue
        out.append(normalized)
        if len(out) >= max_items:
            break
    return out


def _build_keyword_buckets(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    keyword_cluster: List[str],
    trend_candidates: List[str],
    allowed_topics: List[str],
    target_terms: List[str],
    overlap_terms: List[str],
    topic_signature: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    relevant_allowed_topics = [
        candidate
        for candidate in _extract_candidate_phrases_from_topics(allowed_topics, max_phrases=10)
        if _sanitize_editorial_phrase(candidate)
        and _topic_signature_candidate_has_relevance(candidate, topic_signature)
    ]
    internal_support_titles = [
        str(item).strip()
        for item in ((topic_signature or {}).get("high_confidence_internal_titles") or [])
        if str(item).strip()
    ]
    semantic_entities = _select_semantic_entities(
        topic=topic,
        primary_keyword=primary_keyword,
        secondary_keywords=secondary_keywords,
        keyword_cluster=keyword_cluster,
        trend_candidates=trend_candidates,
        target_terms=target_terms,
        overlap_terms=overlap_terms,
        topic_signature=topic_signature,
        max_items=KEYWORD_MAX_SEMANTIC_ENTITIES,
    )
    support_topics = _select_support_topic_candidates(
        topic=topic,
        primary_keyword=primary_keyword,
        candidates=relevant_allowed_topics + internal_support_titles,
        topic_signature=topic_signature,
        max_items=KEYWORD_MAX_SUPPORT_TOPICS,
    )
    provenance = _build_keyword_provenance(
        topic=topic,
        primary_keyword=primary_keyword,
        secondary_keywords=secondary_keywords,
        semantic_entities=semantic_entities,
        support_topics=support_topics,
        keyword_cluster=keyword_cluster,
        trend_candidates=trend_candidates,
        overlap_terms=overlap_terms,
        target_terms=target_terms,
        allowed_topics=relevant_allowed_topics,
        internal_titles=internal_support_titles,
        topic_signature=topic_signature,
    )
    return {
        "primary_query": _normalize_keyword_phrase(primary_keyword),
        "secondary_queries": _dedupe_keyword_phrases(secondary_keywords)[:KEYWORD_MAX_SECONDARY],
        "semantic_entities": semantic_entities,
        "support_topics_for_internal_links": support_topics,
        "provenance": provenance,
    }


def _build_topic_phrase(topic: str) -> str:
    subject_phrase = _normalize_keyword_phrase(_extract_topic_subject_phrase(topic))
    question_focus_phrase = _normalize_keyword_phrase(_extract_topic_question_focus_phrase(topic))
    question_phrase = _normalize_keyword_phrase(_extract_topic_question_phrase(topic))
    preferred = subject_phrase or question_focus_phrase or question_phrase or _normalize_keyword_phrase(topic)
    if question_focus_phrase:
        subject_specificity = _topic_phrase_specificity_score(subject_phrase) if subject_phrase else (0, 0, 0, 0, 0)
        question_specificity = _topic_phrase_specificity_score(question_focus_phrase)
        if not subject_phrase or _topic_phrase_is_generic(subject_phrase):
            preferred = question_focus_phrase
        elif (
            _topic_phrase_domain_token_count(subject_phrase) < 2
            and question_specificity > subject_specificity
        ):
            preferred = question_focus_phrase
        elif (
            question_specificity[0] >= subject_specificity[0] + 1
            and question_specificity > subject_specificity
        ):
            preferred = question_focus_phrase
    elif not preferred:
        preferred = _normalize_keyword_phrase(topic)
    words = _strip_query_year_tokens(_strip_trailing_topic_modifiers(preferred.split()))
    if len(words) > KEYWORD_MAX_WORDS:
        normalized = " ".join(words[:KEYWORD_MAX_WORDS])
    else:
        normalized = " ".join(words)
    return normalized.strip()


def _strip_query_year_tokens(words: List[str]) -> List[str]:
    filtered = [word for word in words if not re.fullmatch(r"(?:19|20)\d{2}", str(word).strip())]
    return filtered if len(filtered) >= KEYWORD_MIN_WORDS else words


def _extract_candidate_phrases_from_topics(topics: List[str], *, max_phrases: int = 16) -> List[str]:
    out: List[str] = []
    for topic in topics:
        normalized = _normalize_keyword_phrase(topic)
        if not normalized:
            continue
        words = normalized.split()
        if len(words) > KEYWORD_MAX_WORDS:
            out.append(" ".join(words[:KEYWORD_MAX_WORDS]))
        out.append(normalized)
        if len(out) >= max_phrases:
            break
    return out[:max_phrases]


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in values:
        normalized = _normalize_keyword_phrase(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _dedupe_string_values(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in values:
        cleaned = str(item).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def _select_topic_relevant_signals(
    *,
    topic: str,
    values: List[str],
    overlap_terms: List[str],
    max_items: int,
) -> List[str]:
    topic_tokens = _keyword_token_set(topic)
    overlap_tokens = _keyword_token_set(" ".join(overlap_terms))
    candidates = _dedupe_keyword_phrases(_extract_candidate_phrases_from_topics(values, max_phrases=max_items * 6))
    scored: List[tuple[float, str]] = []
    for candidate in candidates:
        candidate = _sanitize_editorial_phrase(candidate)
        if not candidate:
            continue
        candidate_tokens = _keyword_token_set(candidate)
        if not candidate_tokens:
            continue
        relevance = (
            3.0 * len(candidate_tokens & topic_tokens)
            + 1.5 * len(candidate_tokens & overlap_tokens)
            + _keyword_similarity(candidate, topic)
        )
        drift = candidate_tokens - (topic_tokens | overlap_tokens)
        if relevance < 1.15 and not (candidate_tokens & overlap_tokens):
            continue
        if drift and len(drift) >= len(candidate_tokens & (topic_tokens | overlap_tokens)) + 1:
            continue
        scored.append((relevance, candidate))
    ranked = sorted(scored, key=lambda item: (-item[0], item[1]))
    return [_format_sentence_start(value) for _score, value in ranked[:max_items]]


def _token_matches_reference_family(token: str, reference_tokens: set[str], *, allow_prefix_match: bool = True) -> bool:
    normalized = str(token).strip()
    if len(normalized) < 4:
        return False
    for reference in reference_tokens:
        reference = str(reference).strip()
        if len(reference) < 4:
            continue
        if normalized == reference:
            return True
        shorter, longer = (normalized, reference) if len(normalized) <= len(reference) else (reference, normalized)
        if shorter in INTERNAL_LINK_WEAK_MATCH_TOKENS:
            continue
        if normalized in reference or reference in normalized:
            return True
        if (
            allow_prefix_match
            and len(normalized) >= 6
            and len(reference) >= 6
            and normalized[:3] == reference[:3]
            and not any(
                normalized.startswith(prefix) or reference.startswith(prefix)
                for prefix in (PAIR_FIT_AUDIENCE_TOKENS | INTERNAL_LINK_GENERIC_TOKENS)
                if len(prefix) >= 4
            )
        ):
            return True
    return False


def _token_matches_internal_link_reference(token: str, reference_tokens: set[str]) -> bool:
    normalized = str(token).strip()
    if len(normalized) < 4:
        return False
    for reference in reference_tokens:
        candidate = str(reference).strip()
        if len(candidate) < 4:
            continue
        if normalized == candidate:
            return True
        shorter, longer = (normalized, candidate) if len(normalized) <= len(candidate) else (candidate, normalized)
        if shorter in INTERNAL_LINK_GENERIC_TOKENS or shorter in KEYWORD_LOW_SIGNAL_TOKENS or shorter in INTERNAL_LINK_WEAK_MATCH_TOKENS:
            continue
        if len(shorter) < 5:
            continue
        if (longer.startswith(shorter) or longer.endswith(shorter)) and len(shorter) * 2 >= len(longer):
            return True
        if (
            len(normalized) >= 8
            and len(candidate) >= 8
            and normalized[:3] == candidate[:3]
            and not any(
                normalized.startswith(prefix) or candidate.startswith(prefix)
                for prefix in (PAIR_FIT_AUDIENCE_TOKENS | INTERNAL_LINK_GENERIC_TOKENS)
                if len(prefix) >= 4
            )
        ):
            return True
    return False


def _select_relevant_keyword_cluster_terms(
    *,
    topic: str,
    keyword_cluster: List[str],
    target_terms: Optional[List[str]] = None,
    overlap_terms: Optional[List[str]] = None,
    max_items: int = 8,
) -> List[str]:
    subject_phrase = _build_topic_phrase(topic) or _normalize_keyword_phrase(topic)
    reference_tokens = _filter_topic_signature_tokens(
        _keyword_focus_tokens(
            " ".join(
                [
                    subject_phrase,
                    " ".join(str(item).strip() for item in (target_terms or []) if str(item).strip()),
                    " ".join(str(item).strip() for item in (overlap_terms or []) if str(item).strip()),
                ]
            )
        )
    )
    scored: List[tuple[float, str]] = []
    candidates = _dedupe_preserve_order(
        [
            cleaned
            for cleaned in (_sanitize_editorial_phrase(item, allow_single_token=True) for item in keyword_cluster)
            if cleaned
        ]
    )[: max_items * 4]
    for candidate in candidates:
        candidate_tokens = _filter_topic_signature_tokens(_keyword_focus_tokens(candidate))
        if not candidate_tokens:
            continue
        family_overlap = {token for token in candidate_tokens if _token_matches_reference_family(token, reference_tokens)}
        overlap = (candidate_tokens & reference_tokens) | family_overlap
        similarity = max(_keyword_similarity(candidate, subject_phrase), _keyword_similarity(candidate, topic))
        drift = candidate_tokens - (reference_tokens | family_overlap)
        if not overlap and similarity < 0.28:
            continue
        if len(overlap) < 1 and drift and len(drift) >= len(overlap) + 1 and similarity < 0.45:
            continue
        if len(overlap) < 2 and drift and len(drift) > len(overlap) + 1 and similarity < 0.4:
            continue
        score = 4.0 * len(overlap) + 1.2 * similarity - 1.6 * len(drift)
        scored.append((score, candidate))
    ranked = sorted(scored, key=lambda item: (-item[0], item[1]))
    return [candidate for _score, candidate in ranked[:max_items]]


def _build_content_brief(
    *,
    topic: str,
    phase2: Dict[str, Any],
    pair_fit: Dict[str, Any],
    target_profile: Dict[str, Any],
    publishing_profile: Dict[str, Any],
    brand_name: str = "",
) -> Dict[str, Any]:
    overlap_terms = [str(item).strip() for item in (pair_fit.get("overlap_terms") or []) if str(item).strip()][:4]
    publishing_terms = _pair_fit_ranked_terms(publishing_profile, site_kind="publishing")
    publishing_contexts = [str(item).strip() for item in (pair_fit.get("publishing_site_contexts") or []) if str(item).strip()]
    audience = _pair_fit_audience_term(publishing_terms, publishing_contexts)
    publishing_signals = _select_topic_relevant_signals(
        topic=topic,
        values=_merge_string_lists(
            phase2.get("allowed_topics") or [],
            phase2.get("site_categories") or [],
            publishing_profile.get("topics") or [],
            publishing_profile.get("topic_clusters") or [],
            max_items=18,
        ),
        overlap_terms=overlap_terms,
        max_items=3,
    )
    target_signals = [
        signal
        for signal in _select_topic_relevant_signals(
            topic=topic,
            values=_merge_string_lists(
                target_profile.get("topics") or [],
                target_profile.get("services_or_products") or [],
                max_items=18,
            ),
            overlap_terms=overlap_terms,
            max_items=4,
        )
        if _keyword_similarity(signal, topic) < 0.85
        and not _is_brand_identity_phrase(signal, brand_name)
    ]
    style_cues = _merge_string_lists(
        phase2.get("content_style_constraints") or [],
        publishing_profile.get("content_style") or [],
        [str(publishing_profile.get("content_tone") or "").strip()],
        max_items=3,
    )
    fit_reason = _limit_text(
        str(pair_fit.get("why_this_topic_was_chosen") or pair_fit.get("best_overlap_reason") or "").strip(),
        180,
    )
    return {
        "audience": audience,
        "publishing_signals": publishing_signals,
        "target_signals": target_signals,
        "overlap_terms": overlap_terms,
        "style_cues": style_cues,
        "fit_reason": fit_reason,
    }


def _select_signature_target_terms(
    *,
    topic: str,
    target_profile: Dict[str, Any],
    content_brief: Optional[Dict[str, Any]],
    overlap_terms: List[str],
    brand_name: str = "",
    max_items: int = 8,
) -> List[str]:
    relevant_profile_terms = _select_topic_relevant_signals(
        topic=topic,
        values=_merge_string_lists(
            target_profile.get("services_or_products") or [],
            target_profile.get("topics") or [],
            max_items=20,
        ),
        overlap_terms=overlap_terms,
        max_items=max_items,
    )
    filtered_terms = [
        term
        for term in _merge_string_lists(
            [str(item).strip() for item in ((content_brief or {}).get("target_signals") or []) if str(item).strip()],
            relevant_profile_terms,
            max_items=max_items * 2,
        )
        if not _is_brand_identity_phrase(term, brand_name)
    ]
    return _merge_string_lists(filtered_terms, max_items=max_items)


def _format_content_brief_prompt_text(content_brief: Dict[str, Any]) -> str:
    if not isinstance(content_brief, dict) or not content_brief:
        return ""
    parts: List[str] = []
    audience = str(content_brief.get("audience") or "").strip()
    publishing_signals = [str(item).strip() for item in (content_brief.get("publishing_signals") or []) if str(item).strip()]
    target_signals = [str(item).strip() for item in (content_brief.get("target_signals") or []) if str(item).strip()]
    style_cues = [str(item).strip() for item in (content_brief.get("style_cues") or []) if str(item).strip()]
    fit_reason = str(content_brief.get("fit_reason") or "").strip()
    if audience:
        parts.append(f"audience={audience}")
    if publishing_signals:
        parts.append(f"publishing={', '.join(publishing_signals[:3])}")
    if target_signals:
        parts.append(f"target={', '.join(target_signals[:3])}")
    if style_cues:
        parts.append(f"style={', '.join(style_cues[:3])}")
    if fit_reason:
        parts.append(f"fit={fit_reason}")
    parts.append("backlink=secondary resource only")
    return "Editorial brief: " + " | ".join(parts)


def _estimate_html_max_tokens(target_words: int, *, floor: int, ceiling: int) -> int:
    estimated = int(target_words * 2.4)
    return max(floor, min(ceiling, estimated))


def _build_keyword_query_variants(
    *,
    topic: str,
    primary_hint: str,
    allowed_topics: List[str],
    max_queries: int = 8,
) -> List[str]:
    seed_tokens = _keyword_token_set(f"{topic} {primary_hint}")
    relevant_allowed_topics = [
        item
        for item in allowed_topics
        if _keyword_candidate_has_relevance(
            item,
            topic_tokens=seed_tokens,
            cluster_tokens=seed_tokens,
            trend_tokens=set(),
        )
    ]
    base_phrases = _dedupe_preserve_order(
        [topic, primary_hint] + _extract_candidate_phrases_from_topics(relevant_allowed_topics, max_phrases=2)
    )
    if not base_phrases:
        return []

    queries: List[str] = []
    primary_base = base_phrases[0]
    queries.append(primary_base)
    for modifier in GERMAN_KEYWORD_MODIFIERS[:2]:
        queries.append(f"{primary_base} {modifier}")
    queries.extend(
        [
            f"was ist {primary_base}",
            f"wie {primary_base}",
            f"wann {primary_base}",
            f"warum {primary_base}",
        ]
    )
    for base in base_phrases[1:2]:
        queries.append(base)
        for modifier in GERMAN_KEYWORD_MODIFIERS[:2]:
            queries.append(f"{base} {modifier}")
    for modifier in GERMAN_KEYWORD_MODIFIERS[2:4]:
        queries.append(f"{primary_base} {modifier}")

    deduped = _dedupe_preserve_order(queries)
    return deduped[:max_queries]


def _derive_trend_query_family(value: str) -> str:
    normalized = _normalize_keyword_phrase(value)
    if not normalized:
        return ""
    words = normalized.split()
    if len(words) >= 2 and any(normalized.startswith(f"{prefix} ") for prefix in GERMAN_QUESTION_PREFIXES):
        words = words[2:] if words[0] in {"was", "wie", "wann", "warum", "welche", "welcher", "welches", "wo", "woran", "kann", "darf"} else words[1:]
    words = [word for word in words if word not in GERMAN_KEYWORD_MODIFIERS]
    if len(words) > 4:
        words = words[:4]
    return " ".join(words).strip()


def _derive_repeated_trend_tokens(
    values: List[str],
    *,
    focus_tokens: set[str],
    min_count: int = 2,
) -> set[str]:
    counts: Dict[str, int] = {}
    for value in values:
        for token in _keyword_focus_tokens(value):
            counts[token] = counts.get(token, 0) + 1
    repeated = {token for token, count in counts.items() if count >= min_count}
    if focus_tokens:
        focused = repeated & focus_tokens
        if focused:
            return focused
    return repeated


def _get_cached_google_suggestions(query: str) -> Optional[List[str]]:
    normalized_query = _normalize_keyword_phrase(query)
    if not normalized_query:
        return None
    cached = GOOGLE_SUGGEST_CACHE.get(normalized_query)
    if not cached:
        return None
    expires_at = float(cached.get("expires_at") or 0)
    if expires_at <= time.time():
        GOOGLE_SUGGEST_CACHE.pop(normalized_query, None)
        return None
    results = cached.get("results")
    if not isinstance(results, list):
        return None
    return [str(item).strip() for item in results if str(item).strip()]


def _set_cached_google_suggestions(query: str, results: List[str]) -> None:
    normalized_query = _normalize_keyword_phrase(query)
    if not normalized_query:
        return
    if len(GOOGLE_SUGGEST_CACHE) >= GOOGLE_SUGGEST_CACHE_MAX_ENTRIES:
        oldest_key = min(
            GOOGLE_SUGGEST_CACHE,
            key=lambda key: float(GOOGLE_SUGGEST_CACHE[key].get("stored_at") or 0),
        )
        GOOGLE_SUGGEST_CACHE.pop(oldest_key, None)
    GOOGLE_SUGGEST_CACHE[normalized_query] = {
        "results": [str(item).strip() for item in results if str(item).strip()],
        "stored_at": time.time(),
        "expires_at": time.time() + GOOGLE_SUGGEST_CACHE_TTL_SECONDS,
    }


def _trend_entry_is_fresh(entry: Dict[str, Any]) -> bool:
    fetched_at_raw = str(entry.get("fetched_at") or "").strip()
    expires_at_raw = str(entry.get("expires_at") or "").strip()
    if expires_at_raw:
        try:
            expires_at = datetime.datetime.fromisoformat(expires_at_raw.replace("Z", "+00:00"))
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=datetime.timezone.utc)
            return expires_at > datetime.datetime.now(datetime.timezone.utc)
        except ValueError:
            return False
    if fetched_at_raw:
        try:
            fetched_at = datetime.datetime.fromisoformat(fetched_at_raw.replace("Z", "+00:00"))
            if fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=datetime.timezone.utc)
            age = datetime.datetime.now(datetime.timezone.utc) - fetched_at
            return age.total_seconds() <= DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS
        except ValueError:
            return False
    return False


def _fetch_google_de_suggestions_live(query: str, *, timeout_seconds: int) -> List[str]:
    cleaned = _normalize_keyword_phrase(query)
    if not cleaned:
        return []
    url = "https://suggestqueries.google.com/complete/search"
    try:
        response = requests.get(
            url,
            params={"client": "firefox", "hl": "de", "gl": "de", "q": cleaned},
            headers={"User-Agent": "creator-service/1.0"},
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return []

    if not isinstance(payload, list) or len(payload) < 2:
        return []
    suggestions_raw = payload[1]
    if not isinstance(suggestions_raw, list):
        return []
    out: List[str] = []
    for item in suggestions_raw:
        if isinstance(item, str) and item.strip():
            out.append(_normalize_keyword_phrase(item))
    return out


def _fetch_google_de_suggestions(
    query: str,
    *,
    timeout_seconds: int,
    trend_cache_ttl_seconds: int,
    cache_metadata_collector: Optional[List[Dict[str, Any]]] = None,
) -> List[str]:
    cleaned = _normalize_keyword_phrase(query)
    if not cleaned:
        return []
    query_family = _derive_trend_query_family(cleaned)
    cached = _get_cached_google_suggestions(cleaned)
    if cached is not None:
        if cache_metadata_collector is not None:
            cache_metadata_collector.append({"query": cleaned, "source": "memory", "status": "fresh"})
        return cached

    db_entry = get_keyword_trend_cache_entry(cleaned)
    db_payload = db_entry.get("payload") if isinstance(db_entry, dict) else None
    db_results = []
    if isinstance(db_payload, dict):
        raw = db_payload.get("suggestions")
        if isinstance(raw, list):
            db_results = [str(item).strip() for item in raw if str(item).strip()]
    db_is_fresh = isinstance(db_entry, dict) and _trend_entry_is_fresh(db_entry)
    if db_results and db_is_fresh:
        _set_cached_google_suggestions(cleaned, db_results)
        record_keyword_trend_cache_hit(cleaned)
        if cache_metadata_collector is not None:
            cache_metadata_collector.append(
                {
                    "query": cleaned,
                    "source": "db",
                    "status": "fresh",
                    "fetched_at": str(db_entry.get("fetched_at") or ""),
                }
            )
        return db_results

    family_entries = get_keyword_trend_cache_family_entries(query_family) if query_family else []
    family_suggestions = _merge_string_lists(
        *[
            [
                str(item).strip()
                for item in ((entry.get("payload") or {}).get("suggestions") or [])
                if str(item).strip()
            ]
            for entry in family_entries
            if _trend_entry_is_fresh(entry)
        ],
        max_items=12,
    )
    if family_suggestions:
        if cache_metadata_collector is not None:
            cache_metadata_collector.append(
                {
                    "query": cleaned,
                    "source": "db_family",
                    "status": "fresh_family_support",
                    "query_family": query_family,
                }
            )

    live_results = _fetch_google_de_suggestions_live(cleaned, timeout_seconds=timeout_seconds)
    if live_results:
        merged_live_results = _merge_string_lists(live_results, family_suggestions, max_items=12)
        _set_cached_google_suggestions(cleaned, merged_live_results)
        upsert_keyword_trend_cache_entry(
            seed_query=query,
            normalized_seed_query=cleaned,
            query_family=query_family,
            payload={"suggestions": merged_live_results},
            ttl_seconds=trend_cache_ttl_seconds,
        )
        if cache_metadata_collector is not None:
            cache_metadata_collector.append(
                {
                    "query": cleaned,
                    "source": "live",
                    "status": "refreshed" if db_results else "miss_refreshed",
                }
            )
        return merged_live_results

    if db_results:
        _set_cached_google_suggestions(cleaned, db_results)
        record_keyword_trend_cache_hit(cleaned)
        if cache_metadata_collector is not None:
            cache_metadata_collector.append(
                {
                    "query": cleaned,
                    "source": "db",
                    "status": "stale_fallback",
                    "fetched_at": str(db_entry.get("fetched_at") or ""),
                }
            )
        return db_results

    if family_suggestions:
        _set_cached_google_suggestions(cleaned, family_suggestions)
        if cache_metadata_collector is not None:
            cache_metadata_collector.append(
                {
                    "query": cleaned,
                    "source": "db_family",
                    "status": "fresh_family_fallback",
                    "query_family": query_family,
                }
            )
        return family_suggestions

    if cache_metadata_collector is not None:
        cache_metadata_collector.append({"query": cleaned, "source": "none", "status": "empty"})
    return []


def _looks_like_question_phrase(value: str) -> bool:
    normalized = _normalize_keyword_phrase(value)
    if not normalized:
        return False
    return any(normalized.startswith(f"{prefix} ") for prefix in GERMAN_QUESTION_PREFIXES)


def _rank_keyword_candidates(
    candidates: List[str],
    *,
    topic_tokens: set[str],
    cluster_tokens: set[str],
    allowed_tokens: set[str],
    trend_tokens: set[str],
    max_items: int,
) -> List[str]:
    ranked = sorted(
        _dedupe_keyword_phrases(candidates),
        key=lambda item: _score_keyword_candidate(
            item,
            topic_tokens=topic_tokens,
            cluster_tokens=cluster_tokens,
            allowed_tokens=allowed_tokens,
            trend_tokens=trend_tokens,
        ),
        reverse=True,
    )
    return ranked[:max_items]


def _discover_keyword_candidates(
    *,
    topic: str,
    primary_hint: str,
    keyword_cluster: List[str],
    allowed_topics: List[str],
    timeout_seconds: int,
    max_terms: int,
    trend_cache_ttl_seconds: int = DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS,
) -> Dict[str, Any]:
    query_variants = _build_keyword_query_variants(
        topic=topic,
        primary_hint=primary_hint,
        allowed_topics=allowed_topics,
    )
    raw_suggestions: List[str] = []
    trend_cache_events: List[Dict[str, Any]] = []
    for query in query_variants:
        raw_suggestions.extend(
            _fetch_google_de_suggestions(
                query,
                timeout_seconds=timeout_seconds,
                trend_cache_ttl_seconds=trend_cache_ttl_seconds,
                cache_metadata_collector=trend_cache_events,
            )
        )

    suggestion_pool = _dedupe_preserve_order(raw_suggestions)
    keyword_candidates = [item for item in suggestion_pool if not _looks_like_question_phrase(item)]
    faq_candidates = [item for item in suggestion_pool if _looks_like_question_phrase(item)]
    if len(faq_candidates) < 3:
        faq_candidates.extend(item for item in query_variants if _looks_like_question_phrase(item))

    topic_tokens = _keyword_token_set(topic)
    cluster_tokens = _keyword_token_set(" ".join(keyword_cluster))
    allowed_tokens = _keyword_token_set(" ".join(allowed_topics))
    trend_tokens = _derive_repeated_trend_tokens(
        keyword_candidates,
        focus_tokens=_filter_keyword_focus_tokens(topic_tokens | allowed_tokens | cluster_tokens),
    )

    ranked_keywords = _rank_keyword_candidates(
        keyword_candidates or query_variants,
        topic_tokens=topic_tokens,
        cluster_tokens=cluster_tokens,
        allowed_tokens=allowed_tokens,
        trend_tokens=trend_tokens,
        max_items=max_terms,
    )
    ranked_faqs = _rank_keyword_candidates(
        faq_candidates,
        topic_tokens=topic_tokens,
        cluster_tokens=cluster_tokens,
        allowed_tokens=allowed_tokens,
        trend_tokens=trend_tokens,
        max_items=KEYWORD_MAX_FAQ,
    )
    return {
        "query_variants": query_variants,
        "trend_candidates": ranked_keywords,
        "faq_candidates": ranked_faqs,
        "trend_cache_events": trend_cache_events,
    }


def _fetch_keyword_trend_candidates(
    *,
    topic: str,
    primary_hint: str,
    keyword_cluster: List[str],
    allowed_topics: List[str],
    timeout_seconds: int,
    max_terms: int,
) -> List[str]:
    discovery = _discover_keyword_candidates(
        topic=topic,
        primary_hint=primary_hint,
        keyword_cluster=keyword_cluster,
        allowed_topics=allowed_topics,
        timeout_seconds=timeout_seconds,
        max_terms=max_terms,
    )
    return discovery["trend_candidates"]


def _score_keyword_candidate(
    candidate: str,
    *,
    topic_tokens: set[str],
    cluster_tokens: set[str],
    allowed_tokens: set[str],
    trend_tokens: set[str],
) -> float:
    candidate_tokens = _keyword_focus_tokens(candidate)
    if not candidate_tokens:
        return -1.0
    topic_focus = _filter_keyword_focus_tokens(topic_tokens)
    cluster_focus = _filter_keyword_focus_tokens(cluster_tokens)
    allowed_focus = _filter_keyword_focus_tokens(allowed_tokens)
    trend_focus = _filter_keyword_focus_tokens(trend_tokens)
    topic_overlap = candidate_tokens & topic_focus
    cluster_overlap = candidate_tokens & cluster_focus
    trend_overlap = candidate_tokens & trend_focus
    if not topic_overlap and not cluster_overlap and len(trend_overlap) < 2:
        return -1.0
    score = 0.0
    score += 4.0 * len(topic_overlap)
    score += 2.0 * len(cluster_overlap)
    score += 0.8 * len(candidate_tokens & allowed_focus)
    score += 0.6 * len(trend_overlap)
    score += 1.4 * _keyword_similarity(candidate, " ".join(topic_focus))
    score += min(1.5, len(candidate_tokens) * 0.3)
    return score


def _keyword_candidate_has_relevance(
    candidate: str,
    *,
    topic_tokens: set[str],
    cluster_tokens: set[str],
    trend_tokens: set[str],
) -> bool:
    candidate_tokens = _keyword_focus_tokens(candidate)
    if not candidate_tokens:
        return False
    topic_focus = _filter_keyword_focus_tokens(topic_tokens)
    cluster_focus = _filter_keyword_focus_tokens(cluster_tokens)
    trend_focus = _filter_keyword_focus_tokens(trend_tokens)
    if candidate_tokens & (topic_focus | cluster_focus):
        return True
    return len(candidate_tokens & trend_focus) >= 2


def _keyword_is_strict_token_subset(candidate: str, reference: str) -> bool:
    candidate_tokens = _keyword_token_set(candidate)
    reference_tokens = _keyword_token_set(reference)
    return bool(candidate_tokens) and bool(reference_tokens) and candidate_tokens < reference_tokens


def _to_keyword_dative_phrase(value: str) -> str:
    normalized = _normalize_keyword_phrase(value)
    if not normalized:
        return ""
    words = normalized.split()
    if not words:
        return ""
    last = words[-1]
    if last.endswith("e") and not last.endswith("ee"):
        words[-1] = f"{last}n"
    return " ".join(words)


def _audience_dative_term(audience_term: str) -> str:
    return {
        "baby": "babys",
        "babys": "babys",
        "eltern": "eltern",
        "familie": "familien",
        "familien": "familien",
        "kinder": "kindern",
        "kunden": "kunden",
        "kundinnen": "kundinnen",
        "leser": "lesern",
        "leserinnen": "leserinnen",
        "menschen": "menschen",
        "nutzer": "nutzern",
        "patienten": "patienten",
        "schueler": "schuelern",
        "schwangere": "schwangeren",
        "teams": "teams",
    }.get(audience_term, audience_term)


def _detect_topic_audience_term(*values: str) -> str:
    for value in values:
        for token in _normalize_keyword_phrase(value).split():
            if token == "kindern":
                return "kinder"
            if token == "familien":
                return "familien"
            if token in PAIR_FIT_AUDIENCE_TOKENS:
                return token
    return ""


def _filter_topic_signature_tokens(tokens: set[str]) -> set[str]:
    return {
        token
        for token in tokens
        if token not in INTERNAL_LINK_GENERIC_TOKENS
        and token not in TOPIC_SIGNATURE_EXCLUDED_TOKENS
        and token not in KEYWORD_LOW_SIGNAL_TOKENS
    }


def _build_target_term_support_phrases(target_terms: List[str]) -> List[str]:
    candidates: List[str] = []
    for raw_value in target_terms[:4]:
        normalized = _sanitize_editorial_phrase(raw_value, allow_single_token=True)
        if not normalized:
            continue
        words = normalized.split()
        if _is_valid_keyword_phrase(normalized):
            candidates.append(normalized)
            candidates.append(f"{normalized} im alltag")
            continue
        if len(words) == 1:
            if not normalized.endswith(("heit", "keit", "ung", "schaft", "ion")):
                candidates.append(f"{normalized} richtig auswaehlen")
            candidates.append(f"{normalized} im alltag")
    return _dedupe_keyword_phrases(candidates)


def _build_keyword_cluster_support_phrases(keyword_cluster: List[str], *, audience_term: str, subject_phrase: str = "") -> List[str]:
    candidates: List[str] = []
    subject_tokens = _filter_keyword_focus_tokens(_keyword_focus_tokens(subject_phrase))
    normalized_subject = _normalize_keyword_phrase(subject_phrase)
    if audience_term and normalized_subject.startswith(f"{audience_term} "):
        normalized_subject = normalized_subject[len(audience_term) + 1 :].strip()
    for raw_candidate in _dedupe_preserve_order(
        [
            cleaned
            for cleaned in (_sanitize_editorial_phrase(item, allow_single_token=True) for item in keyword_cluster)
            if cleaned
        ]
    )[:8]:
        focus_tokens = _keyword_focus_tokens(raw_candidate)
        if len(focus_tokens) >= 2:
            candidates.append(raw_candidate)
            if len(candidates) >= 4:
                break
            continue
        if len(focus_tokens) == 1 and normalized_subject:
            token = next(iter(focus_tokens))
            if (
                token in _filter_keyword_focus_tokens(_keyword_focus_tokens(normalized_subject))
                and normalized_subject != token
                and _is_valid_keyword_phrase(normalized_subject)
            ):
                candidates.append(normalized_subject)
                if len(candidates) >= 4:
                    break
    audience_dative = _audience_dative_term(audience_term) if audience_term else ""
    if audience_dative and audience_term not in {"eltern", "familie", "familien"}:
        seen: set[str] = set(_normalize_keyword_phrase(item) for item in candidates)
        for token in _filter_keyword_focus_tokens(_keyword_focus_tokens(" ".join(keyword_cluster))):
            if len(token) < 7 or audience_term in token:
                continue
            if token in subject_tokens:
                continue
            candidate = f"{token} bei {audience_dative}"
            normalized = _normalize_keyword_phrase(candidate)
            if normalized in seen or not _is_valid_keyword_phrase(normalized):
                continue
            candidates.append(candidate)
            seen.add(normalized)
            if len(candidates) >= 4:
                break
    return _dedupe_keyword_phrases(candidates)


def _select_high_confidence_internal_titles(
    items: List[Dict[str, Any]],
    *,
    specific_tokens: set[str],
    topic: str,
    primary_keyword: str,
    max_items: int = 3,
) -> List[str]:
    scored: List[tuple[float, str]] = []
    signature_tokens = {token for token in specific_tokens if token not in KEYWORD_LOW_SIGNAL_TOKENS}
    seed_tokens = signature_tokens | _keyword_focus_tokens(f"{topic} {primary_keyword}")
    for item in _coerce_internal_link_inventory(items):
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        title_tokens = {token for token in _keyword_focus_tokens(title) if token not in INTERNAL_LINK_GENERIC_TOKENS}
        if not title_tokens:
            continue
        overlap = title_tokens & signature_tokens
        if not overlap:
            continue
        primary_similarity = _keyword_similarity(title, primary_keyword)
        topic_similarity = _keyword_similarity(title, topic)
        combined_similarity = max(primary_similarity, topic_similarity)
        if len(overlap) < 2 and combined_similarity < 0.4:
            continue
        drift = title_tokens - seed_tokens
        if len(drift) > len(overlap) and combined_similarity < 0.46:
            continue
        score = (
            4.5 * len(overlap)
            + 1.2 * primary_similarity
            + 0.8 * topic_similarity
            - 1.3 * len(drift)
        )
        if score < 2.5:
            continue
        scored.append((score, title))
    ranked = sorted(scored, key=lambda item: (-item[0], item[1]))
    return _merge_string_lists([title for _score, title in ranked], max_items=max_items)


def _build_topic_signature(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    target_terms: List[str],
    overlap_terms: List[str],
    trend_candidates: List[str],
    keyword_cluster: List[str],
    internal_link_inventory: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    subject_phrase = _build_topic_phrase(topic)
    question_phrase = _extract_topic_question_phrase(topic)
    target_terms = [
        cleaned
        for cleaned in (_sanitize_editorial_phrase(item, allow_single_token=True) for item in target_terms)
        if cleaned
    ]
    relevant_keyword_cluster = _select_relevant_keyword_cluster_terms(
        topic=topic,
        keyword_cluster=keyword_cluster,
        target_terms=target_terms,
        overlap_terms=overlap_terms,
        max_items=8,
    )
    audience_term = _detect_topic_audience_term(
        subject_phrase,
        primary_keyword,
        " ".join(target_terms),
        " ".join(overlap_terms),
    )
    target_support_phrases = _build_target_term_support_phrases(target_terms)
    cluster_support_phrases = _build_keyword_cluster_support_phrases(
        relevant_keyword_cluster,
        audience_term=audience_term,
        subject_phrase=subject_phrase,
    )
    base_reference_tokens = _filter_topic_signature_tokens(
        _keyword_focus_tokens(
            f"{subject_phrase} {primary_keyword} {' '.join(target_terms)} {' '.join(overlap_terms)} {' '.join(cluster_support_phrases)}"
        )
    )
    filtered_trend_candidates = [
        candidate
        for candidate in trend_candidates
        if (
            _filter_topic_signature_tokens(_keyword_focus_tokens(candidate)) & base_reference_tokens
            or _keyword_similarity(candidate, subject_phrase or primary_keyword) >= 0.25
        )
    ]

    token_weights: Dict[str, float] = {}
    phrase_weights: Dict[str, float] = {}

    def _add_signature_values(values: List[str], weight: float) -> None:
        for value in values:
            normalized = _normalize_keyword_phrase(value)
            if not normalized:
                continue
            if _is_valid_keyword_phrase(normalized):
                phrase_weights[normalized] = phrase_weights.get(normalized, 0.0) + weight
            for token in _filter_topic_signature_tokens(_keyword_focus_tokens(normalized)):
                token_weights[token] = token_weights.get(token, 0.0) + weight

    _add_signature_values([subject_phrase], 5.0)
    _add_signature_values([question_phrase], 4.6)
    _add_signature_values([primary_keyword], 5.0)
    _add_signature_values(secondary_keywords[:KEYWORD_MAX_SECONDARY], 3.5)
    _add_signature_values(target_terms[:4], 4.0)
    _add_signature_values(target_support_phrases, 3.8)
    _add_signature_values(overlap_terms[:4], 4.2)
    _add_signature_values(cluster_support_phrases, 3.2)
    _add_signature_values(filtered_trend_candidates[:8], 2.4)
    core_tokens = _filter_topic_signature_tokens(
        _keyword_focus_tokens(f"{subject_phrase} {primary_keyword} {' '.join(target_terms)} {' '.join(target_support_phrases)}")
    )

    base_specific_tokens = {
        token
        for token in (
            _filter_topic_signature_tokens(_keyword_focus_tokens(subject_phrase))
            | _filter_topic_signature_tokens(_keyword_focus_tokens(primary_keyword))
            | _filter_topic_signature_tokens(_keyword_focus_tokens(" ".join(target_terms)))
            | _filter_topic_signature_tokens(_keyword_focus_tokens(" ".join(overlap_terms)))
        )
    }
    seed_specific_tokens = set(base_specific_tokens)
    seed_all_tokens = seed_specific_tokens | _filter_topic_signature_tokens(_keyword_focus_tokens(" ".join(secondary_keywords)))
    ranked_base_tokens = sorted(token_weights.items(), key=lambda item: (-item[1], item[0]))
    specific_tokens = {
        token
        for token, weight in ranked_base_tokens
        if weight >= 3.0 or token in base_specific_tokens
    }
    if not specific_tokens:
        specific_tokens = {token for token, _weight in ranked_base_tokens[:6]}

    # Keep the topic signature independent from the publishing-site inventory.
    # Feeding candidate internal titles back into the signature creates a
    # circular bias where one weak match can dominate future ranking.
    high_confidence_internal_titles = _select_high_confidence_internal_titles(
        internal_link_inventory or [],
        specific_tokens=base_specific_tokens,
        topic=subject_phrase or topic,
        primary_keyword=primary_keyword,
    )

    ranked_tokens = sorted(token_weights.items(), key=lambda item: (-item[1], item[0]))
    all_tokens = [token for token, _weight in ranked_tokens[:16]]
    specific_tokens = {
        token
        for token, weight in ranked_tokens
        if weight >= 3.0 or token in base_specific_tokens
    }
    if not specific_tokens:
        specific_tokens = set(all_tokens[:8])

    support_candidates = _dedupe_keyword_phrases(
        [subject_phrase, primary_keyword]
        + secondary_keywords
        + target_support_phrases
        + cluster_support_phrases
        + filtered_trend_candidates[:6]
    )
    support_scored: List[tuple[float, str]] = []
    all_tokens_set = set(all_tokens)
    for candidate in support_candidates:
        candidate_tokens = _filter_topic_signature_tokens(_keyword_focus_tokens(candidate))
        if not candidate_tokens:
            continue
        specific_overlap = candidate_tokens & specific_tokens
        broad_overlap = candidate_tokens & all_tokens_set
        drift = candidate_tokens - all_tokens_set
        if not specific_overlap and len(broad_overlap) < 2:
            continue
        score = (
            4.0 * len(specific_overlap)
            + 1.8 * len(broad_overlap)
            + phrase_weights.get(candidate, 0.0)
            + 1.0 * _keyword_similarity(candidate, subject_phrase or primary_keyword)
            - 1.4 * len(drift)
        )
        support_scored.append((score, candidate))
    support_phrases = _merge_string_lists(
        [subject_phrase],
        [candidate for _score, candidate in sorted(support_scored, key=lambda item: (-item[0], item[1]))],
        max_items=6,
    )

    return {
        "subject_phrase": subject_phrase,
        "question_phrase": question_phrase,
        "audience_term": audience_term,
        "target_terms": _merge_string_lists(target_terms, max_items=4),
        "target_support_phrases": target_support_phrases[:4],
        "support_phrases": support_phrases,
        "core_tokens": sorted(core_tokens),
        "seed_specific_tokens": sorted(seed_specific_tokens),
        "seed_all_tokens": sorted(seed_all_tokens),
        "specific_tokens": sorted(specific_tokens),
        "all_tokens": all_tokens,
        "high_confidence_internal_titles": high_confidence_internal_titles,
        "keyword_cluster_phrases": cluster_support_phrases,
        "primary_keyword": _normalize_keyword_phrase(primary_keyword),
    }


def _topic_signature_token_sets(signature: Optional[Dict[str, Any]]) -> tuple[set[str], set[str]]:
    if not isinstance(signature, dict):
        return set(), set()
    specific_tokens = {str(item).strip() for item in (signature.get("specific_tokens") or []) if str(item).strip()}
    all_tokens = {str(item).strip() for item in (signature.get("all_tokens") or []) if str(item).strip()}
    if not all_tokens:
        all_tokens = set(specific_tokens)
    return specific_tokens, all_tokens


def _internal_link_domain_gate_tokens(signature: Optional[Dict[str, Any]]) -> set[str]:
    if not isinstance(signature, dict):
        return set()
    candidates = {
        str(item).strip()
        for item in (
            (signature.get("seed_specific_tokens") or [])
            + (signature.get("specific_tokens") or [])
            + (signature.get("core_tokens") or [])
        )
        if str(item).strip()
    }
    return {
        token
        for token in candidates
        if len(token) >= 4
        and token not in INTERNAL_LINK_GENERIC_TOKENS
        and token not in KEYWORD_LOW_SIGNAL_TOKENS
        and token not in GERMAN_KEYWORD_MODIFIERS
        and token not in INTERNAL_LINK_WEAK_MATCH_TOKENS
    }


def _internal_link_context_gate_tokens(signature: Optional[Dict[str, Any]]) -> set[str]:
    if not isinstance(signature, dict):
        return set()
    domain_tokens = _internal_link_domain_gate_tokens(signature)
    signature_tokens = set(domain_tokens)
    if not signature_tokens:
        signature_tokens = {
            token
            for token in (
                set(str(item).strip() for item in (signature.get("seed_all_tokens") or []) if str(item).strip())
                | set(str(item).strip() for item in (signature.get("all_tokens") or []) if str(item).strip())
            )
            if token
            and token not in INTERNAL_LINK_GENERIC_TOKENS
            and token not in KEYWORD_LOW_SIGNAL_TOKENS
            and token not in INTERNAL_LINK_WEAK_MATCH_TOKENS
        }
    scored_contexts = sorted(
        (
            len(signature_tokens & keywords),
            label,
        )
        for label, keywords in PAIR_FIT_CONTEXT_KEYWORDS.items()
    )
    selected_labels = [
        label
        for score, label in sorted(scored_contexts, key=lambda item: (-item[0], item[1]))
        if score > 0
    ][:2]
    if not selected_labels:
        return set()
    return set().union(*(PAIR_FIT_CONTEXT_KEYWORDS[label] for label in selected_labels)) - domain_tokens


def _token_matches_context_gate_token(token: str, context_tokens: set[str]) -> bool:
    normalized = str(token).strip()
    if len(normalized) < 6:
        return False
    for reference in context_tokens:
        reference = str(reference).strip()
        if len(reference) < 4:
            continue
        if normalized == reference:
            return True
        shorter, longer = (normalized, reference) if len(normalized) <= len(reference) else (reference, normalized)
        if len(shorter) < 5:
            continue
        if shorter in longer and len(longer) - len(shorter) >= 3:
            return True
    return False


def _token_matches_domain_support_token(token: str, domain_tokens: set[str]) -> bool:
    normalized = str(token).strip()
    if len(normalized) < 6:
        return False
    for reference in domain_tokens:
        reference = str(reference).strip()
        if len(reference) < 4:
            continue
        if normalized == reference:
            return True
        shorter, longer = (normalized, reference) if len(normalized) <= len(reference) else (reference, normalized)
        if len(shorter) < 5:
            continue
        if shorter in longer and len(longer) - len(shorter) >= 3:
            return True
    return False


def _topic_signature_candidate_stats(candidate: str, topic_signature: Optional[Dict[str, Any]]) -> Dict[str, set[str]]:
    candidate_tokens = _filter_topic_signature_tokens(_keyword_focus_tokens(candidate))
    specific_tokens, all_tokens = _topic_signature_token_sets(topic_signature)
    non_generic_tokens = set(candidate_tokens)
    specific_family_overlap = {
        token for token in non_generic_tokens if _token_matches_reference_family(token, specific_tokens, allow_prefix_match=False)
    }
    broad_family_overlap = {
        token for token in non_generic_tokens if _token_matches_reference_family(token, all_tokens, allow_prefix_match=False)
    }
    return {
        "candidate_tokens": candidate_tokens,
        "non_generic_tokens": non_generic_tokens,
        "specific_overlap": (non_generic_tokens & specific_tokens) | specific_family_overlap,
        "broad_overlap": (non_generic_tokens & all_tokens) | broad_family_overlap,
        "drift": non_generic_tokens - (all_tokens | broad_family_overlap),
    }


def _topic_signature_candidate_has_relevance(candidate: str, topic_signature: Optional[Dict[str, Any]]) -> bool:
    stats = _topic_signature_candidate_stats(candidate, topic_signature)
    return bool(stats["specific_overlap"]) or len(stats["broad_overlap"]) >= 2


def _topic_signature_candidate_score(candidate: str, topic_signature: Optional[Dict[str, Any]]) -> float:
    stats = _topic_signature_candidate_stats(candidate, topic_signature)
    if not stats["non_generic_tokens"]:
        return -1.0
    if not stats["specific_overlap"] and len(stats["broad_overlap"]) < 2:
        return -1.6 * len(stats["drift"])
    reference_phrase = ""
    if isinstance(topic_signature, dict):
        reference_phrase = str(topic_signature.get("subject_phrase") or topic_signature.get("primary_keyword") or "").strip()
    return (
        4.0 * len(stats["specific_overlap"])
        + 1.8 * len(stats["broad_overlap"])
        + 0.8 * _keyword_similarity(candidate, reference_phrase)
        - 1.5 * len(stats["drift"])
    )


def _keyword_candidate_is_support_topic_noise(candidate: str, topic_signature: Optional[Dict[str, Any]]) -> bool:
    normalized = _sanitize_editorial_phrase(candidate)
    if not normalized:
        return True
    stats = _topic_signature_candidate_stats(normalized, topic_signature)
    words = normalized.split()
    if len(words) < 4:
        return False
    reference_phrase = ""
    if isinstance(topic_signature, dict):
        reference_phrase = str(topic_signature.get("subject_phrase") or topic_signature.get("primary_keyword") or "").strip()
    reference_similarity = _keyword_similarity(normalized, reference_phrase)
    specific_overlap = len(stats["specific_overlap"])
    broad_overlap = len(stats["broad_overlap"])
    drift = len(stats["drift"])
    if any(word in EDITORIAL_ACTION_TOKENS for word in words) and specific_overlap < 2 and reference_similarity < 0.55:
        return True
    if len(words) >= 5 and drift >= (specific_overlap + broad_overlap + 2):
        return True
    if " als " in f" {normalized} " and specific_overlap < 2 and reference_similarity < 0.5:
        return True
    return False


def _keyword_candidate_has_editorial_quality(candidate: str, topic_signature: Optional[Dict[str, Any]]) -> bool:
    normalized = _sanitize_editorial_phrase(candidate)
    if not normalized:
        return False
    if _keyword_candidate_is_support_topic_noise(normalized, topic_signature):
        return False
    stats = _topic_signature_candidate_stats(normalized, topic_signature)
    if len(stats["non_generic_tokens"]) >= 2:
        return True
    reference_phrase = ""
    if isinstance(topic_signature, dict):
        reference_phrase = str(topic_signature.get("subject_phrase") or topic_signature.get("primary_keyword") or "").strip()
    return _keyword_similarity(normalized, reference_phrase) >= 0.55


def _keyword_candidate_has_question_noise(candidate: str) -> bool:
    normalized = _normalize_keyword_phrase(candidate)
    if _looks_like_self_assessment_question(normalized):
        return True
    words = normalized.split()
    if len(words) <= 4:
        return False
    return any(word in TOPIC_SIGNATURE_EXCLUDED_TOKENS for word in words)


def _keyword_redundant_with_topic(candidate: str, topic: str) -> bool:
    normalized_candidate = _normalize_keyword_phrase(candidate)
    topic_phrase = _build_topic_phrase(topic) or _normalize_keyword_phrase(topic)
    if not normalized_candidate or not topic_phrase:
        return False
    if normalized_candidate == topic_phrase:
        return True
    if _keyword_present(normalized_candidate, topic_phrase):
        return True
    return False


def _is_heading_support_phrase_usable(candidate: str) -> bool:
    normalized = _sanitize_editorial_phrase(candidate)
    if not normalized:
        return False
    if any(fragment in normalized for fragment in ("richtig auswaehlen", "rund um", "tipps zu", "im alltag")):
        return False
    return not re.search(
        r"\bbei (?:eltern|familien|haushalten|kundinnen|kunden|leserinnen|lesern|menschen|schwangeren|teams)\b",
        normalized,
    )


def _pick_outline_target_focus_phrase(
    topic_signature: Optional[Dict[str, Any]],
    *,
    exclude_phrases: Optional[List[str]] = None,
) -> str:
    excluded = [str(item).strip() for item in (exclude_phrases or []) if str(item).strip()]
    candidate_lists = [
        [str(item).strip() for item in ((topic_signature or {}).get("target_support_phrases") or []) if str(item).strip()],
        [str(item).strip() for item in ((topic_signature or {}).get("support_phrases") or []) if str(item).strip()],
        [str(item).strip() for item in ((topic_signature or {}).get("target_terms") or []) if str(item).strip()],
    ]
    for candidates in candidate_lists:
        for candidate in candidates:
            normalized = _sanitize_editorial_phrase(candidate)
            if not normalized or not _is_heading_support_phrase_usable(normalized):
                continue
            if any(_keyword_similarity(normalized, existing) >= 0.78 for existing in excluded):
                continue
            return normalized
    return ""


def _pick_topic_signature_support_phrase(
    topic_signature: Optional[Dict[str, Any]],
    *,
    exclude_phrases: Optional[List[str]] = None,
) -> str:
    excluded = [str(item).strip() for item in (exclude_phrases or []) if str(item).strip()]
    for candidate in [str(item).strip() for item in ((topic_signature or {}).get("support_phrases") or []) if str(item).strip()]:
        normalized = _sanitize_editorial_phrase(candidate)
        if not normalized or not _is_heading_support_phrase_usable(normalized):
            continue
        if any(_keyword_similarity(normalized, existing) >= 0.78 for existing in excluded):
            continue
        return normalized
    if isinstance(topic_signature, dict):
        return _sanitize_editorial_phrase(
            str(topic_signature.get("subject_phrase") or topic_signature.get("primary_keyword") or "").strip()
        )
    return ""


def _build_secondary_keyword_fallbacks(
    *,
    topic: str,
    primary_keyword: str,
    keyword_cluster: List[str],
    allowed_topics: List[str],
    topic_signature: Optional[Dict[str, Any]] = None,
) -> List[str]:
    signature = topic_signature or _build_topic_signature(
        topic=topic,
        primary_keyword=primary_keyword,
        secondary_keywords=[],
        target_terms=[],
        overlap_terms=[],
        trend_candidates=[],
        keyword_cluster=keyword_cluster,
        internal_link_inventory=[],
    )
    subject_phrase = str(signature.get("subject_phrase") or _build_topic_phrase(topic) or _topic_head_keyword(primary_keyword)).strip()
    signature_support = _pick_topic_signature_support_phrase(signature, exclude_phrases=[primary_keyword, subject_phrase])
    target_support_phrases = [str(item).strip() for item in (signature.get("target_support_phrases") or []) if str(item).strip()]
    cluster_support_phrases = [str(item).strip() for item in (signature.get("keyword_cluster_phrases") or []) if str(item).strip()]
    question_phrase = _normalize_keyword_phrase(str(signature.get("question_phrase") or ""))
    question_tokens = _filter_keyword_focus_tokens(_keyword_focus_tokens(question_phrase))
    topic_signal_tokens = _keyword_focus_tokens(f"{topic} {primary_keyword} {subject_phrase}")
    decision_like_topic = _tokens_have_decision_signal(topic_signal_tokens)
    problem_like_topic = bool(question_phrase) or _tokens_have_problem_signal(_keyword_token_set(subject_phrase))
    process_like_topic = bool(topic_signal_tokens & PROCESS_ACTION_TOKENS)
    audience_term = str(signature.get("audience_term") or "").strip()
    audience_dative = _audience_dative_term(audience_term) if audience_term else ""
    subject_focus_tokens = [
        token
        for token in _filter_keyword_focus_tokens(_keyword_focus_tokens(subject_phrase))
        if token not in PAIR_FIT_AUDIENCE_TOKENS
        and token not in EDITORIAL_ACTION_TOKENS
    ]
    timing_support_token = next(
        (
            token
            for token in [
                *sorted(_filter_keyword_focus_tokens(_keyword_focus_tokens(" ".join(cluster_support_phrases)))),
                *sorted(_filter_keyword_focus_tokens(_keyword_focus_tokens(" ".join(keyword_cluster)))),
            ]
            if len(token) >= 7
            and token not in ABSTRACT_QUERY_TOKENS
            and token not in EDITORIAL_ACTION_TOKENS
            and token not in subject_focus_tokens
        ),
        "",
    )
    candidates = [
        signature_support,
        cluster_support_phrases[0] if cluster_support_phrases else "",
        cluster_support_phrases[1] if len(cluster_support_phrases) > 1 else "",
        cluster_support_phrases[2] if len(cluster_support_phrases) > 2 else "",
        target_support_phrases[0] if target_support_phrases else "",
        target_support_phrases[1] if len(target_support_phrases) > 1 else "",
        f"warnzeichen {signature_support}" if signature_support and problem_like_topic else "",
        f"warnzeichen {cluster_support_phrases[0]}" if cluster_support_phrases and problem_like_topic else "",
        f"{subject_phrase} checkliste" if subject_phrase and process_like_topic else "",
        f"{subject_phrase} kriterien" if subject_phrase and decision_like_topic else "",
        f"{subject_phrase} unterlagen" if subject_phrase and process_like_topic else "",
        f"{subject_focus_tokens[0]} bei {audience_dative}" if subject_focus_tokens and audience_dative else "",
        f"{subject_focus_tokens[0]} zeitpunkt" if subject_focus_tokens and "zeitpunkt" in question_tokens else "",
        f"{timing_support_token} zeitpunkt" if timing_support_token and "zeitpunkt" in question_tokens else "",
        f"{subject_focus_tokens[0]} kauf oder verkauf" if subject_focus_tokens and {"kauf", "verkauf"} <= question_tokens else "",
    ]
    return [
        candidate
        for candidate in _dedupe_keyword_phrases(candidates)
        if _keyword_candidate_is_query_like(
            candidate,
            topic=topic,
            primary_keyword=primary_keyword,
            topic_signature=signature,
        )
    ]


def _finalize_secondary_keywords(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    keyword_cluster: List[str],
    allowed_topics: List[str],
    topic_signature: Optional[Dict[str, Any]] = None,
) -> List[str]:
    finalized = [
        candidate
        for candidate in _dedupe_keyword_phrases(secondary_keywords)
        if _keyword_similarity(candidate, primary_keyword) < 0.75
        and not _keyword_redundant_with_topic(candidate, topic)
        and _keyword_candidate_is_query_like(
            candidate,
            topic=topic,
            primary_keyword=primary_keyword,
            topic_signature=topic_signature,
        )
    ]
    if len(finalized) >= KEYWORD_MIN_SECONDARY:
        return finalized[:KEYWORD_MAX_SECONDARY]

    for candidate in _build_secondary_keyword_fallbacks(
        topic=topic,
        primary_keyword=primary_keyword,
        keyword_cluster=keyword_cluster,
        allowed_topics=allowed_topics,
        topic_signature=topic_signature,
    ):
        if len(finalized) >= KEYWORD_MIN_SECONDARY:
            break
        if _keyword_similarity(candidate, primary_keyword) >= 0.75:
            continue
        if _keyword_redundant_with_topic(candidate, topic):
            continue
        if not _keyword_candidate_is_query_like(
            candidate,
            topic=topic,
            primary_keyword=primary_keyword,
            topic_signature=topic_signature,
        ):
            continue
        if any(_keyword_similarity(candidate, existing) >= 0.75 for existing in finalized):
            continue
        finalized.append(candidate)
    return finalized[:KEYWORD_MAX_SECONDARY]


def _faq_candidate_has_planning_noise(
    question: str,
    *,
    topic_signature: Optional[Dict[str, Any]],
    brand_name: str = "",
) -> bool:
    normalized = _normalize_keyword_phrase(question)
    if not normalized:
        return True
    focus_phrase = re.sub(
        r"^(?:was ist|wie|wann|warum|welche|welcher|welches|wo|woran|worauf|kann|darf)\s+",
        "",
        normalized,
    )
    focus_phrase = re.sub(
        r"\b(?:bei|fuer|für|im|alltag|wichtig|achten|hilft|helfen|naechsten|nächsten|schritte|sollte|sollten|man|dann)\b",
        " ",
        focus_phrase,
    )
    focus_phrase = re.sub(r"\s+", " ", focus_phrase).strip()
    if not focus_phrase:
        return False
    if _phrase_has_same_family_duplicate_tokens(focus_phrase):
        return True
    if _is_brand_identity_phrase(focus_phrase, brand_name):
        return True
    return _keyword_candidate_is_support_topic_noise(focus_phrase, topic_signature)


def _topic_head_keyword(topic: str) -> str:
    subject_phrase = _normalize_keyword_phrase(_extract_topic_subject_phrase(topic))
    normalized = (subject_phrase if subject_phrase and not _topic_phrase_is_generic(subject_phrase) else "") or _build_topic_phrase(topic)
    words = _strip_query_year_tokens(_strip_trailing_topic_modifiers(normalized.split()))
    compressed_words = [
        word
        for word in words
        if word not in KEYWORD_LOW_SIGNAL_TOKENS
        and word not in GERMAN_KEYWORD_MODIFIERS
        and word not in {"und", "oder"}
    ]
    if len(compressed_words) >= 2:
        words = compressed_words
    if len(words) > 4:
        normalized = " ".join(words[:4])
    else:
        normalized = " ".join(words)
    return normalized if _is_valid_keyword_phrase(normalized) else ""


def _align_primary_keyword_to_topic(
    *,
    topic: str,
    current_primary: str,
    trend_candidates: List[str],
    keyword_cluster: List[str],
) -> str:
    normalized_topic = _normalize_keyword_phrase(topic)
    if not normalized_topic:
        return current_primary

    topic_head = _topic_head_keyword(topic)
    current_primary_normalized = _normalize_keyword_phrase(current_primary)
    topic_tokens = _keyword_token_set(normalized_topic)
    head_tokens = _keyword_token_set(topic_head)
    cluster_candidates = _dedupe_keyword_phrases(
        [
            " ".join(keyword_cluster[:2]),
            " ".join(keyword_cluster[:3]),
            " ".join(keyword_cluster[:4]),
        ]
    )
    candidate_pool = _dedupe_keyword_phrases(
        [topic_head, current_primary] + trend_candidates + cluster_candidates + [normalized_topic]
    )
    candidate_pool = [
        item
        for item in candidate_pool
        if not _phrase_has_same_family_duplicate_tokens(item)
        and (
            item == topic_head
            or item == normalized_topic
            or _keyword_candidate_is_query_like(
                item,
                topic=topic,
                primary_keyword=current_primary or topic_head or normalized_topic,
                topic_signature=None,
            )
        )
    ]
    if not candidate_pool:
        return current_primary or topic_head or normalized_topic

    if (
        current_primary_normalized
        and len(current_primary_normalized.split()) > 4
        and _keyword_similarity(current_primary_normalized, normalized_topic) >= 0.88
    ):
        refined_candidates = [
            item
            for item in candidate_pool
            if 2 <= len(_keyword_token_set(item)) <= 4
            and _keyword_similarity(item, normalized_topic) >= 0.15
            and not _keyword_is_strict_token_subset(item, normalized_topic)
        ]
        if refined_candidates:
            return max(
                refined_candidates,
                key=lambda item: (
                    6.0 * len(_keyword_token_set(item) & head_tokens)
                    + 2.5 * len(_keyword_token_set(item) & topic_tokens)
                    - 2.0 * len(_keyword_token_set(item) - topic_tokens),
                    -len(_keyword_token_set(item)),
                ),
            )
        if topic_head:
            return topic_head

    if current_primary and _keyword_present_relaxed(normalized_topic, current_primary):
        current_tokens = _keyword_token_set(current_primary)
        if not head_tokens or len(current_tokens & head_tokens) >= max(1, len(head_tokens) - 1):
            return current_primary

    if topic_head and topic_head in candidate_pool:
        return topic_head

    def _score(item: str) -> tuple[float, int]:
        item_tokens = _keyword_token_set(item)
        return (
            6.0 * len(item_tokens & head_tokens)
            + 2.0 * len(item_tokens & topic_tokens)
            - 1.5 * max(0, len(item_tokens) - 3),
            -len(item_tokens),
        )

    best = max(candidate_pool, key=_score)
    return best or current_primary or topic_head or normalized_topic


def _select_keywords(
    *,
    topic: str,
    llm_primary: str,
    llm_secondary: List[str],
    keyword_cluster: List[str],
    allowed_topics: List[str],
    trend_candidates: List[str],
    faq_candidates: List[str],
    target_terms: Optional[List[str]] = None,
    overlap_terms: Optional[List[str]] = None,
    internal_link_inventory: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    topic_phrase = _build_topic_phrase(topic)
    topic_tokens = _keyword_token_set(topic_phrase)
    relevant_keyword_cluster = _select_relevant_keyword_cluster_terms(
        topic=topic,
        keyword_cluster=keyword_cluster,
        target_terms=[str(item).strip() for item in (target_terms or []) if str(item).strip()],
        overlap_terms=[str(item).strip() for item in (overlap_terms or []) if str(item).strip()],
        max_items=8,
    )
    cluster_tokens = _keyword_token_set(" ".join(relevant_keyword_cluster))
    allowed_tokens = _keyword_token_set(" ".join(allowed_topics))
    trend_tokens = _derive_repeated_trend_tokens(
        trend_candidates,
        focus_tokens=_filter_keyword_focus_tokens(topic_tokens | cluster_tokens | allowed_tokens),
    )
    topic_signature = _build_topic_signature(
        topic=topic,
        primary_keyword=_sanitize_editorial_phrase(llm_primary) or topic_phrase,
        secondary_keywords=[],
        target_terms=[str(item).strip() for item in (target_terms or []) if str(item).strip()],
        overlap_terms=[str(item).strip() for item in (overlap_terms or []) if str(item).strip()],
        trend_candidates=trend_candidates,
        keyword_cluster=keyword_cluster,
        internal_link_inventory=internal_link_inventory or [],
    )
    relevant_allowed_topics = [
        item
        for item in allowed_topics
        if _sanitize_editorial_phrase(item)
        and (
            _keyword_candidate_has_relevance(
                item,
                topic_tokens=topic_tokens,
                cluster_tokens=cluster_tokens,
                trend_tokens=trend_tokens,
            )
            or _topic_signature_candidate_has_relevance(item, topic_signature)
        )
        and _topic_signature_candidate_score(item, topic_signature) >= 0.5
    ]
    target_term_candidates = [str(item).strip() for item in (topic_signature.get("target_terms") or []) if str(item).strip()]
    relevant_keyword_cluster = _select_relevant_keyword_cluster_terms(
        topic=topic,
        keyword_cluster=_merge_string_lists(relevant_keyword_cluster, keyword_cluster, max_items=12),
        target_terms=target_term_candidates,
        overlap_terms=[str(item).strip() for item in (overlap_terms or []) if str(item).strip()],
        max_items=8,
    )
    cluster_phrase_candidates = _dedupe_keyword_phrases(
        [
            " ".join(relevant_keyword_cluster[:2]),
            " ".join(relevant_keyword_cluster[:3]),
            " ".join(relevant_keyword_cluster[:4]),
        ]
    )

    primary_pool = _dedupe_keyword_phrases(
        [llm_primary, topic_phrase, _topic_head_keyword(topic)]
        + trend_candidates
        + cluster_phrase_candidates
        + _extract_candidate_phrases_from_topics(relevant_keyword_cluster, max_phrases=6)
    )
    primary_pool = [
        candidate
        for candidate in primary_pool
        if not _phrase_has_same_family_duplicate_tokens(candidate)
        and (
            _keyword_candidate_has_relevance(
                candidate,
                topic_tokens=topic_tokens,
                cluster_tokens=cluster_tokens,
                trend_tokens=trend_tokens,
            )
            or _topic_signature_candidate_has_relevance(candidate, topic_signature)
        )
        and (
            candidate == topic_phrase
            or candidate == _topic_head_keyword(topic)
            or _keyword_candidate_is_query_like(
                candidate,
                topic=topic,
                primary_keyword=_sanitize_editorial_phrase(llm_primary) or topic_phrase,
                topic_signature=topic_signature,
            )
        )
        and _topic_signature_candidate_score(candidate, topic_signature) >= 0.0
    ]
    if not primary_pool and _is_valid_keyword_phrase(topic_phrase):
        primary_pool = [topic_phrase]
    if not primary_pool:
        fallback = _normalize_keyword_phrase(topic) or "branchen einblicke"
        primary_pool = [fallback]
    topic_head_phrase = _topic_head_keyword(topic)
    topic_focus_tokens = _filter_keyword_focus_tokens(topic_tokens)
    primary_ranked = sorted(
        primary_pool,
        key=lambda item: (
            _score_keyword_candidate(
                item,
                topic_tokens=topic_tokens,
                cluster_tokens=cluster_tokens,
                allowed_tokens=allowed_tokens,
                trend_tokens=trend_tokens,
            )
            + _topic_signature_candidate_score(item, topic_signature)
            + (2.2 * _keyword_similarity(item, topic_phrase or topic))
            + (0.8 * _keyword_similarity(item, topic_head_phrase))
            - (1.2 * len(_filter_keyword_focus_tokens(_keyword_focus_tokens(item)) - topic_focus_tokens))
        ),
        reverse=True,
    )
    primary_keyword = primary_ranked[0]
    if (
        topic_phrase
        and _is_valid_keyword_phrase(topic_phrase)
        and len(_filter_keyword_focus_tokens(topic_tokens)) >= 2
        and _keyword_similarity(primary_keyword, topic_phrase) < 0.7
    ):
        primary_keyword = topic_phrase

    secondary_pool = _dedupe_keyword_phrases(
        llm_secondary
        + trend_candidates
        + _extract_candidate_phrases_from_topics(relevant_keyword_cluster)
        + [_topic_head_keyword(topic)]
        + [topic_phrase]
    )
    support_topic_pool = _dedupe_keyword_phrases(
        _extract_candidate_phrases_from_topics(relevant_allowed_topics, max_phrases=8)
        + [str(item).strip() for item in (topic_signature.get("high_confidence_internal_titles") or []) if str(item).strip()]
    )
    ranked_secondary = sorted(
        [
            candidate
            for candidate in secondary_pool
            if _keyword_similarity(candidate, primary_keyword) < 0.8
            and not _keyword_redundant_with_topic(candidate, topic)
            and not _keyword_is_strict_token_subset(candidate, primary_keyword)
            and not _keyword_candidate_has_question_noise(candidate)
            and _keyword_candidate_is_query_like(
                candidate,
                topic=topic,
                primary_keyword=primary_keyword,
                topic_signature=topic_signature,
            )
            and (
                _keyword_candidate_has_relevance(
                    candidate,
                    topic_tokens=topic_tokens,
                    cluster_tokens=cluster_tokens,
                    trend_tokens=trend_tokens,
                )
                or _topic_signature_candidate_has_relevance(candidate, topic_signature)
            )
            and _topic_signature_candidate_score(candidate, topic_signature) >= 0.4
        ],
        key=lambda item: _score_keyword_candidate(
            item,
            topic_tokens=topic_tokens,
            cluster_tokens=cluster_tokens,
            allowed_tokens=allowed_tokens,
            trend_tokens=trend_tokens,
        ) + _topic_signature_candidate_score(item, topic_signature),
        reverse=True,
    )
    secondary_keywords = ranked_secondary[:KEYWORD_MAX_SECONDARY]
    supplemental_secondary = sorted(
        [
            candidate
            for candidate in secondary_pool
            if candidate not in secondary_keywords
            and _keyword_similarity(candidate, primary_keyword) < 0.8
            and not _keyword_redundant_with_topic(candidate, topic)
            and not _keyword_is_strict_token_subset(candidate, primary_keyword)
            and not _keyword_candidate_has_question_noise(candidate)
            and _keyword_candidate_is_query_like(
                candidate,
                topic=topic,
                primary_keyword=primary_keyword,
                topic_signature=topic_signature,
            )
            and (
                _keyword_candidate_has_relevance(
                    candidate,
                    topic_tokens=topic_tokens,
                    cluster_tokens=cluster_tokens,
                    trend_tokens=trend_tokens,
                )
                or _topic_signature_candidate_has_relevance(candidate, topic_signature)
            )
            and _score_keyword_candidate(
                candidate,
                topic_tokens=topic_tokens,
                cluster_tokens=cluster_tokens,
                allowed_tokens=allowed_tokens,
                trend_tokens=trend_tokens,
            ) >= 2.0
        ],
        key=lambda item: _score_keyword_candidate(
            item,
            topic_tokens=topic_tokens,
            cluster_tokens=cluster_tokens,
            allowed_tokens=allowed_tokens,
            trend_tokens=trend_tokens,
        ),
        reverse=True,
    )
    for candidate in supplemental_secondary:
        if len(secondary_keywords) >= KEYWORD_MIN_SECONDARY:
            break
        if any(_keyword_similarity(candidate, existing) >= 0.75 for existing in secondary_keywords):
            continue
        secondary_keywords.append(candidate)
    if len(secondary_keywords) < KEYWORD_MIN_SECONDARY:
        fallback_secondary = _build_secondary_keyword_fallbacks(
            topic=topic,
            primary_keyword=primary_keyword,
            keyword_cluster=keyword_cluster,
            allowed_topics=[],
            topic_signature=topic_signature,
        )
        for candidate in fallback_secondary:
            if len(secondary_keywords) >= KEYWORD_MIN_SECONDARY:
                break
            if any(_keyword_similarity(candidate, existing) >= 0.75 for existing in secondary_keywords):
                continue
            if _keyword_similarity(candidate, primary_keyword) >= 0.8:
                continue
            if _keyword_redundant_with_topic(candidate, topic):
                continue
            if _keyword_is_strict_token_subset(candidate, primary_keyword):
                continue
            if not _keyword_candidate_is_query_like(
                candidate,
                topic=topic,
                primary_keyword=primary_keyword,
                topic_signature=topic_signature,
            ):
                continue
            secondary_keywords.append(candidate)
    secondary_keywords = [candidate for candidate in secondary_keywords if not _keyword_candidate_has_question_noise(candidate)]
    if len(secondary_keywords) < KEYWORD_MIN_SECONDARY:
        for candidate in _build_secondary_keyword_fallbacks(
            topic=topic,
            primary_keyword=primary_keyword,
            keyword_cluster=keyword_cluster,
            allowed_topics=[],
            topic_signature=topic_signature,
        ):
            if len(secondary_keywords) >= KEYWORD_MIN_SECONDARY:
                break
            if _keyword_candidate_has_question_noise(candidate):
                continue
            if any(_keyword_similarity(candidate, existing) >= 0.75 for existing in secondary_keywords):
                continue
            if _keyword_similarity(candidate, primary_keyword) >= 0.8:
                continue
            if _keyword_redundant_with_topic(candidate, topic):
                continue
            if not _keyword_candidate_is_query_like(
                candidate,
                topic=topic,
                primary_keyword=primary_keyword,
                topic_signature=topic_signature,
            ):
                continue
            secondary_keywords.append(candidate)

    final_signature = _build_topic_signature(
        topic=topic,
        primary_keyword=primary_keyword,
        secondary_keywords=secondary_keywords,
        target_terms=[str(item).strip() for item in (target_terms or []) if str(item).strip()],
        overlap_terms=[str(item).strip() for item in (overlap_terms or []) if str(item).strip()],
        trend_candidates=trend_candidates,
        keyword_cluster=keyword_cluster,
        internal_link_inventory=internal_link_inventory or [],
    )
    keyword_buckets = _build_keyword_buckets(
        topic=topic,
        primary_keyword=primary_keyword,
        secondary_keywords=secondary_keywords,
        keyword_cluster=relevant_keyword_cluster,
        trend_candidates=trend_candidates,
        allowed_topics=support_topic_pool,
        target_terms=target_term_candidates,
        overlap_terms=[str(item).strip() for item in (overlap_terms or []) if str(item).strip()],
        topic_signature=final_signature,
    )

    return {
        "primary_keyword": primary_keyword,
        "secondary_keywords": secondary_keywords[:KEYWORD_MAX_SECONDARY],
        "trend_candidates": trend_candidates,
        "topic_signature": final_signature,
        "keyword_buckets": keyword_buckets,
        "faq_candidates": _rank_keyword_candidates(
            faq_candidates,
            topic_tokens=topic_tokens,
            cluster_tokens=cluster_tokens,
            allowed_tokens=allowed_tokens,
            trend_tokens=trend_tokens,
            max_items=KEYWORD_MAX_FAQ,
        ),
    }


def _coerce_internal_link_inventory(items: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if not isinstance(items, list):
        return []
    out: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        categories = item.get("categories")
        out.append(
            {
                "url": url,
                "title": str(item.get("title") or "").strip(),
                "excerpt": str(item.get("excerpt") or "").strip(),
                "slug": str(item.get("slug") or "").strip(),
                "categories": [str(value).strip() for value in categories if str(value).strip()] if isinstance(categories, list) else [],
                "published_at": str(item.get("published_at") or "").strip(),
            }
        )
    return out


def _score_internal_link_inventory_item(
    item: Dict[str, Any],
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    topic_signature: Optional[Dict[str, Any]] = None,
) -> float:
    signature = topic_signature or _build_topic_signature(
        topic=topic,
        primary_keyword=primary_keyword,
        secondary_keywords=secondary_keywords,
        target_terms=[],
        overlap_terms=[],
        trend_candidates=[],
        keyword_cluster=[],
        internal_link_inventory=[],
    )
    topic_tokens = _keyword_focus_tokens(topic)
    primary_tokens = _keyword_focus_tokens(primary_keyword)
    secondary_tokens = _keyword_focus_tokens(" ".join(secondary_keywords))
    title_tokens = _keyword_focus_tokens(str(item.get("title") or ""))
    excerpt_tokens = _keyword_focus_tokens(str(item.get("excerpt") or ""))
    slug_tokens = _keyword_focus_tokens(str(item.get("slug") or ""))
    category_tokens = _keyword_focus_tokens(" ".join(item.get("categories") or []))
    combined = title_tokens | excerpt_tokens | slug_tokens | category_tokens
    if not combined:
        return 0.0

    combined_text = " ".join(
        [
            str(item.get("title") or "").strip(),
            str(item.get("excerpt") or "").strip(),
            str(item.get("slug") or "").strip(),
            " ".join(item.get("categories") or []),
        ]
    )
    stats = _topic_signature_candidate_stats(combined_text, signature)
    candidate_specific_overlap = set(stats["specific_overlap"])
    candidate_broad_overlap = set(stats["broad_overlap"])
    item_focus = _filter_keyword_focus_tokens(combined)
    core_tokens = {str(item).strip() for item in (signature.get("core_tokens") or []) if str(item).strip()}
    seed_specific_tokens = {
        str(item).strip() for item in (signature.get("seed_specific_tokens") or signature.get("specific_tokens") or []) if str(item).strip()
    }
    seed_all_tokens = {
        str(item).strip() for item in (signature.get("seed_all_tokens") or signature.get("all_tokens") or []) if str(item).strip()
    }
    if not seed_all_tokens:
        seed_all_tokens = set(seed_specific_tokens)
    core_overlap = item_focus & core_tokens
    title_focus = _filter_keyword_focus_tokens(title_tokens | slug_tokens)
    domain_gate_tokens = _internal_link_domain_gate_tokens(signature)
    core_family_overlap = {
        token for token in title_focus if token not in core_overlap and _token_matches_internal_link_reference(token, core_tokens)
    }
    seed_specific_overlap = item_focus & seed_specific_tokens
    seed_specific_family_overlap = {
        token
        for token in title_focus
        if token not in seed_specific_overlap and _token_matches_internal_link_reference(token, seed_specific_tokens)
    }
    seed_broad_overlap = item_focus & seed_all_tokens
    seed_broad_family_overlap = {
        token
        for token in title_focus
        if token not in seed_broad_overlap and _token_matches_internal_link_reference(token, seed_all_tokens)
    }
    title_seed_overlap = title_focus & seed_specific_tokens
    title_broad_overlap = title_focus & seed_all_tokens
    title_domain_overlap = title_focus & domain_gate_tokens
    context_gate_tokens = _internal_link_context_gate_tokens(signature)
    support_focus = item_focus - title_focus
    candidate_domain_overlap = {
        token
        for token in stats["candidate_tokens"]
        if domain_gate_tokens and _token_matches_domain_support_token(token, domain_gate_tokens)
    }
    candidate_context_overlap = {
        token
        for token in support_focus
        if context_gate_tokens
        and domain_gate_tokens
        and _token_matches_domain_support_token(token, domain_gate_tokens)
        and _token_matches_context_gate_token(token, context_gate_tokens)
    }
    title_domain_family_overlap = {
        token
        for token in title_focus
        if token not in title_domain_overlap and _token_matches_internal_link_reference(token, domain_gate_tokens)
    }
    seed_drift = item_focus - (seed_all_tokens | seed_broad_family_overlap)
    secondary_similarity = max((_keyword_similarity(combined_text, keyword) for keyword in secondary_keywords), default=0.0)
    title_secondary_similarity = max((_keyword_similarity(str(item.get("title") or ""), keyword) for keyword in secondary_keywords), default=0.0)
    if not stats["non_generic_tokens"] or not candidate_specific_overlap:
        return 0.0

    title_similarity = max(
        _keyword_similarity(str(item.get("title") or ""), primary_keyword),
        _keyword_similarity(str(item.get("title") or ""), topic),
    )
    combined_similarity = max(
        _keyword_similarity(combined_text, primary_keyword),
        _keyword_similarity(combined_text, topic),
    )
    if (
        domain_gate_tokens
        and not (title_domain_overlap or title_domain_family_overlap)
        and (
            not candidate_domain_overlap
            or (context_gate_tokens and not candidate_context_overlap and max(title_similarity, title_secondary_similarity, secondary_similarity) < 0.2)
        )
    ):
        return 0.0
    if (
        len(core_overlap | core_family_overlap) <= 1
        and len(candidate_specific_overlap) < 2
        and max(title_similarity, combined_similarity, secondary_similarity) < 0.28
        and not (title_broad_overlap or len(candidate_broad_overlap) >= 2)
    ):
        return 0.0
    if (
        len(candidate_specific_overlap) == 1
        and not (title_seed_overlap or title_broad_overlap or core_overlap or core_family_overlap)
        and secondary_similarity < 0.12
    ):
        return 0.0

    candidate_specific_support_overlap = candidate_specific_overlap - seed_specific_overlap - seed_specific_family_overlap
    candidate_broad_support_overlap = candidate_broad_overlap - seed_broad_overlap - seed_broad_family_overlap
    score = 0.0
    score += 3.6 * len(core_overlap)
    score += 2.2 * len(core_family_overlap)
    score += 5.4 * len(seed_specific_overlap)
    score += 2.8 * len(seed_specific_family_overlap)
    score += 2.6 * len(candidate_specific_support_overlap)
    score += 2.4 * len(title_seed_overlap)
    score += 1.2 * len(title_broad_overlap - title_seed_overlap)
    score += 1.8 * len(seed_broad_overlap)
    score += 1.0 * len(seed_broad_family_overlap)
    score += 0.9 * len(candidate_broad_support_overlap - candidate_specific_support_overlap)
    score += 2.5 * len(item_focus & primary_tokens)
    score += 1.5 * len(item_focus & secondary_tokens)
    score += 2.0 * secondary_similarity
    score += 1.0 * title_secondary_similarity
    score += 1.0 * title_similarity
    score += 0.8 * _keyword_similarity(str(item.get("title") or ""), topic)
    score += 0.5 * combined_similarity
    score += min(1.0, len(title_tokens) * 0.2)
    score -= min(3.0, 0.2 * len(seed_drift))
    score -= 0.4 * len({token for token in item_focus if token in INTERNAL_LINK_GENERIC_TOKENS and token not in seed_specific_overlap})
    if str(item.get("published_at") or "").strip():
        score += 0.3
    return score


def _rank_internal_link_inventory(
    items: List[Dict[str, Any]],
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    publishing_site_url: str,
    backlink_url: str,
    max_items: int,
    topic_signature: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    normalized_items = _coerce_internal_link_inventory(items)
    scored: List[tuple[float, Dict[str, Any]]] = []
    for item in normalized_items:
        url = str(item.get("url") or "").strip()
        if not _is_internal_href(url, publishing_site_url):
            continue
        if _normalize_url(url) == _normalize_url(backlink_url):
            continue
        score = _score_internal_link_inventory_item(
            item,
            topic=topic,
            primary_keyword=primary_keyword,
            secondary_keywords=secondary_keywords,
            topic_signature=topic_signature,
        )
        if score < 2.5:
            continue
        scored.append((score, item))
    ranked = sorted(scored, key=lambda item: item[0], reverse=True)
    if not ranked:
        return []
    top_score = ranked[0][0]
    minimum_score = max(3.5, top_score - 12.0)
    return [item for score, item in ranked if score >= minimum_score][:max_items]


def _normalize_text_for_keyword_search(value: str) -> str:
    normalized = _normalize_keyword_phrase(value)
    return f" {normalized} " if normalized else " "


def _keyword_present(text: str, keyword_phrase: str) -> bool:
    normalized_text = _normalize_text_for_keyword_search(text)
    normalized_keyword = _normalize_keyword_phrase(keyword_phrase)
    if not normalized_keyword:
        return False
    return f" {normalized_keyword} " in normalized_text


def _keyword_token_approx_match(a: str, b: str) -> bool:
    if a == b:
        return True
    if min(len(a), len(b)) < 5:
        return False
    prefix = 0
    for left, right in zip(a, b):
        if left != right:
            break
        prefix += 1
    return prefix >= (min(len(a), len(b)) - 2)


def _keyword_present_relaxed(text: str, keyword_phrase: str) -> bool:
    if _keyword_present(text, keyword_phrase):
        return True
    keyword_tokens = list(_keyword_token_set(keyword_phrase))
    text_tokens = list(_keyword_token_set(text))
    if not keyword_tokens or not text_tokens:
        return False
    matched = 0
    for keyword_token in keyword_tokens:
        if any(_keyword_token_approx_match(keyword_token, text_token) for text_token in text_tokens):
            matched += 1
    return matched >= max(1, len(keyword_tokens) - 1)


def _count_keyword_occurrences(text: str, keyword_phrase: str) -> int:
    normalized_text = _normalize_keyword_phrase(text)
    normalized_keyword = _normalize_keyword_phrase(keyword_phrase)
    if not normalized_text or not normalized_keyword:
        return 0
    pattern = r"(?<!\w)" + r"\s+".join(re.escape(token) for token in normalized_keyword.split()) + r"(?!\w)"
    return len(re.findall(pattern, normalized_text))


def _extract_h1_text(html: str) -> str:
    match = re.search(r"<h1[^>]*>(.*?)</h1>", html or "", flags=re.IGNORECASE | re.DOTALL)
    return _strip_html_tags(match.group(1)).strip() if match else ""


def _extract_first_paragraph_text(html: str) -> str:
    match = re.search(r"<p[^>]*>(.*?)</p>", html or "", flags=re.IGNORECASE | re.DOTALL)
    return _strip_html_tags(match.group(1)).strip() if match else ""


def _ensure_primary_keyword_in_intro(html: str, primary_keyword: str) -> str:
    keyword = _normalize_keyword_phrase(primary_keyword)
    if not keyword:
        return html or ""
    article_html = html or ""
    intro_text = _extract_first_paragraph_text(article_html)
    if _keyword_present(intro_text, keyword):
        return article_html

    sentence = f"{_format_title_case(primary_keyword)} ist dabei ein zentraler Aspekt."

    def _inject(match: re.Match[str]) -> str:
        inner = match.group(1)
        stripped = _strip_html_tags(inner).strip()
        if not stripped:
            return f"<p>{sentence}</p>"
        return f"<p>{sentence} {inner.strip()}</p>"

    if re.search(r"<p[^>]*>.*?</p>", article_html, flags=re.IGNORECASE | re.DOTALL):
        return re.sub(r"<p[^>]*>(.*?)</p>", _inject, article_html, count=1, flags=re.IGNORECASE | re.DOTALL)
    return f"<p>{sentence}</p>{article_html}"


def _trim_article_to_word_limit(html: str, max_words: int) -> str:
    article_html = html or ""
    if word_count_from_html(article_html) <= max_words:
        return article_html

    soup = BeautifulSoup(article_html, "lxml")
    body = soup.body or soup
    paragraphs = body.find_all("p")
    if not paragraphs:
        return article_html

    first_paragraph = paragraphs[0] if paragraphs else None
    current_count = word_count_from_html(str(body))
    candidates = [p for p in paragraphs if p is not first_paragraph and not p.find("a")]
    if not candidates:
        candidates = [p for p in paragraphs if p is not first_paragraph]

    for paragraph in reversed(candidates):
        if current_count <= max_words:
            break
        text = re.sub(r"\s+", " ", paragraph.get_text(" ")).strip()
        words = re.findall(r"\b\w+\b", text)
        if len(words) <= 28:
            continue
        overflow = current_count - max_words
        target_words = max(28, len(words) - overflow)
        trimmed_text = " ".join(words[:target_words]).strip()
        if not trimmed_text:
            continue
        if trimmed_text[-1] not in ".!?":
            trimmed_text += "."
        paragraph.clear()
        paragraph.append(trimmed_text)
        current_count = word_count_from_html(str(body))

    result = body.decode_contents() if getattr(body, "decode_contents", None) else str(body)
    return result


def _validate_keyword_coverage(article_html: str, primary_keyword: str, secondary_keywords: List[str]) -> List[str]:
    errors: List[str] = []
    primary = _normalize_keyword_phrase(primary_keyword)
    secondaries = _dedupe_keyword_phrases(secondary_keywords)[:KEYWORD_MAX_SECONDARY]
    if not _is_valid_keyword_phrase(primary):
        errors.append("primary_keyword_invalid")
        return errors

    h1_text = _extract_h1_text(article_html)
    intro_text = _extract_first_paragraph_text(article_html)
    h2_headings = _extract_h2_headings(article_html)
    h2_text = " ".join(h2_headings)
    plain_text = _strip_html_tags(article_html)

    if not _keyword_present_relaxed(h1_text, primary):
        errors.append("primary_keyword_missing_h1")
    if not _keyword_present_relaxed(intro_text, primary):
        errors.append("primary_keyword_missing_intro")
    natural_question_h2_count = sum(1 for heading in h2_headings if _heading_is_natural_core_question(heading))
    if not _keyword_present_relaxed(h2_text, primary) and natural_question_h2_count < 2:
        errors.append("primary_keyword_missing_h2")

    required_secondaries = secondaries[:KEYWORD_MIN_SECONDARY]
    missing_secondaries = [kw for kw in required_secondaries if not _keyword_present_relaxed(plain_text, kw)]
    if missing_secondaries:
        errors.append("secondary_keywords_missing:" + ",".join(missing_secondaries[:3]))

    words = max(1, word_count_from_html(article_html))
    max_occurrences = max(KEYWORD_OVERUSE_MIN_OCCURRENCES, int((words / 300.0) * 3))
    for keyword in [primary] + required_secondaries:
        occurrences = _count_keyword_occurrences(plain_text, keyword)
        if occurrences > max_occurrences:
            errors.append(f"keyword_overused:{keyword}:{occurrences}")

    return errors


def _extract_internal_anchor_texts(article_html: str, *, backlink_url: str, publishing_site_url: str) -> List[str]:
    anchors: List[str] = []
    for href, inner in re.findall(
        r"<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        article_html or "",
        flags=re.IGNORECASE | re.DOTALL,
    ):
        absolute = _absolutize_url(href, publishing_site_url)
        if _is_backlink_href(absolute, backlink_url):
            continue
        if not _is_internal_href(absolute, publishing_site_url):
            continue
        anchor_text = _strip_html_tags(inner).strip()
        if anchor_text:
            anchors.append(anchor_text)
    return anchors


def _validate_internal_anchor_texts(article_html: str, *, backlink_url: str, publishing_site_url: str) -> List[str]:
    anchors = _extract_internal_anchor_texts(article_html, backlink_url=backlink_url, publishing_site_url=publishing_site_url)
    if not anchors:
        return []
    errors: List[str] = []
    unique_normalized = {_normalize_keyword_phrase(anchor) for anchor in anchors if _normalize_keyword_phrase(anchor)}
    if len(unique_normalized) < min(INTERNAL_LINK_ANCHOR_MIN_UNIQUE, len(anchors)):
        errors.append("internal_anchor_diversity_too_low")
    generic_count = sum(1 for anchor in anchors if _normalize_keyword_phrase(anchor) in {"mehr dazu", "siehe auch", "passend dazu", "weiterfuehrende informationen"})
    if generic_count >= len(anchors):
        errors.append("internal_anchor_too_generic")
    return errors


def _validate_structured_content(article_html: str, structured_mode: str) -> List[str]:
    mode = (structured_mode or "").strip().lower()
    if mode == "list":
        if not re.search(r"<(?:ul|ol)\b", article_html or "", flags=re.IGNORECASE):
            return ["structured_list_missing"]
    if mode == "table":
        if not re.search(r"<table\b", article_html or "", flags=re.IGNORECASE):
            return ["structured_table_missing"]
    return []


def _validate_seo_metadata(
    *,
    article_html: str,
    primary_keyword: str,
    required_h1: str,
    meta_title: str,
    meta_description: str,
    slug: str,
    structured_mode: str,
) -> List[str]:
    errors: List[str] = []
    if len((meta_title or "").strip()) < SEO_TITLE_MIN_CHARS or len((meta_title or "").strip()) > SEO_TITLE_MAX_CHARS:
        errors.append(f"meta_title_length_invalid:{len((meta_title or '').strip())}")
    if len((meta_description or "").strip()) < SEO_DESCRIPTION_MIN_CHARS or len((meta_description or "").strip()) > SEO_DESCRIPTION_MAX_CHARS:
        errors.append(f"meta_description_length_invalid:{len((meta_description or '').strip())}")
    if not _keyword_present_relaxed(meta_title, primary_keyword):
        errors.append("meta_title_missing_primary_keyword")
    normalized_slug = _derive_slug(slug or "")
    if normalized_slug != (slug or "").strip():
        errors.append("slug_format_invalid")
    slug_tokens = _keyword_token_set(slug or "")
    primary_tokens = _keyword_token_set(primary_keyword)
    if len(slug_tokens & primary_tokens) < min(2, max(1, len(primary_tokens))):
        errors.append("slug_missing_primary_keyword")
    h1_text = _extract_h1_text(article_html)
    if _normalize_keyword_phrase(h1_text) != _normalize_keyword_phrase(required_h1):
        errors.append("h1_not_required_title")
    errors.extend(_validate_structured_content(article_html, structured_mode))
    return errors


def _score_seo_output(
    *,
    article_html: str,
    meta_title: str,
    meta_description: str,
    slug: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    required_h1: str,
    structured_mode: str,
    backlink_url: str,
    publishing_site_url: str,
    min_internal_links: int,
    max_internal_links: int,
    topic: str,
    content_brief: Optional[Dict[str, Any]] = None,
    intent_type: str = "informational",
    article_angle: str = "practical_guidance",
    topic_signature: Optional[Dict[str, Any]] = None,
    specificity_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    checks = {
        "keyword_coverage": _validate_keyword_coverage(article_html, primary_keyword, secondary_keywords),
        "language_conclusion": _validate_language_and_conclusion(article_html, topic),
        "section_substance": _validate_section_substance(article_html),
        "phrase_integrity": _validate_phrase_integrity(article_html),
        "contextual_alignment": _validate_contextual_alignment(article_html, content_brief),
        "link_strategy": _validate_link_strategy(
            article_html,
            backlink_url=backlink_url,
            publishing_site_url=publishing_site_url,
            min_internal_links=min_internal_links,
            max_internal_links=max_internal_links,
        ),
        "anchor_quality": _validate_internal_anchor_texts(
            article_html,
            backlink_url=backlink_url,
            publishing_site_url=publishing_site_url,
        ),
        "metadata": _validate_seo_metadata(
            article_html=article_html,
            primary_keyword=primary_keyword,
            required_h1=required_h1,
            meta_title=meta_title,
            meta_description=meta_description,
            slug=slug,
            structured_mode=structured_mode,
        ),
    }
    title_eval = _evaluate_title_quality(title=meta_title or required_h1, primary_keyword=primary_keyword, topic=topic)
    heading_eval = _evaluate_heading_quality(headings=_extract_h2_headings(article_html), topic_signature=topic_signature)
    backlink_eval = _evaluate_backlink_naturalness(
        article_html=article_html,
        backlink_url=backlink_url,
        publishing_site_url=publishing_site_url,
        topic_signature=topic_signature,
    )
    specificity_eval = _evaluate_specificity(
        article_html=article_html,
        specificity_profile=specificity_profile,
    )
    spam_eval = _evaluate_spam_risk(
        article_html=article_html,
        primary_keyword=primary_keyword,
        backlink_url=backlink_url,
    )
    coherence_eval = _evaluate_article_coherence(
        article_html=article_html,
        topic_signature=topic_signature,
        intent_type=intent_type,
        article_angle=article_angle,
    )
    error_count = sum(len(value) for value in checks.values())
    overall = max(
        0,
        int(
            round(
                (
                    title_eval["score"] * 0.14
                    + heading_eval["score"] * 0.18
                    + backlink_eval["score"] * 0.18
                    + specificity_eval["score"] * 0.20
                    + coherence_eval["score"] * 0.20
                    + max(0, 100 - spam_eval["score"]) * 0.10
                )
            )
        )
        - (error_count * 2)
    )
    return {
        "score": overall,
        "checks": checks,
        "title_quality_score": title_eval["score"],
        "heading_quality_score": heading_eval["score"],
        "intent_type": intent_type,
        "backlink_naturalness_score": backlink_eval["score"],
        "specificity_score": specificity_eval["score"],
        "spam_risk_score": spam_eval["score"],
        "coherence_score": coherence_eval["score"],
        "quality_errors": _dedupe_string_values(
            title_eval["errors"]
            + heading_eval["errors"]
            + backlink_eval["errors"]
            + specificity_eval["errors"]
            + spam_eval["errors"]
            + coherence_eval["errors"]
        ),
    }


def _collect_article_validation_errors(
    *,
    article_html: str,
    meta_title: str,
    meta_description: str,
    slug: str,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    required_h1: str,
    structured_mode: str,
    backlink_url: str,
    backlink_placement: str,
    publishing_site_url: str,
    min_internal_links: int,
    max_internal_links: int,
    content_brief: Optional[Dict[str, Any]] = None,
    intent_type: str = "informational",
    article_angle: str = "practical_guidance",
    topic_signature: Optional[Dict[str, Any]] = None,
    specificity_profile: Optional[Dict[str, Any]] = None,
) -> List[str]:
    errors: List[str] = []
    for check in (
        validate_word_count(article_html, ARTICLE_MIN_WORDS, ARTICLE_MAX_WORDS),
        validate_backlink_placement(article_html, backlink_url, backlink_placement),
    ):
        if check:
            errors.append(check)
    errors.extend(
        _validate_link_strategy(
            article_html,
            backlink_url=backlink_url,
            publishing_site_url=publishing_site_url,
            min_internal_links=min_internal_links,
            max_internal_links=max_internal_links,
        )
    )
    errors.extend(
        _validate_internal_anchor_texts(
            article_html,
            backlink_url=backlink_url,
            publishing_site_url=publishing_site_url,
        )
    )
    if not (ARTICLE_MIN_H2 <= count_h2(article_html) <= ARTICLE_MAX_H2):
        errors.append("h2_count_invalid")
    errors.extend(_validate_language_and_conclusion(article_html, topic))
    errors.extend(_validate_section_substance(article_html))
    errors.extend(_validate_phrase_integrity(article_html))
    errors.extend(_validate_contextual_alignment(article_html, content_brief))
    errors.extend(_validate_keyword_coverage(article_html, primary_keyword, secondary_keywords))
    title_eval = _evaluate_title_quality(title=meta_title or required_h1, primary_keyword=primary_keyword, topic=topic)
    if title_eval["score"] < 70:
        errors.extend(title_eval["errors"] or ["title_quality_low"])
    heading_eval = _evaluate_heading_quality(headings=_extract_h2_headings(article_html), topic_signature=topic_signature)
    if heading_eval["score"] < 70:
        errors.extend(heading_eval["errors"] or ["heading_quality_low"])
    backlink_eval = _evaluate_backlink_naturalness(
        article_html=article_html,
        backlink_url=backlink_url,
        publishing_site_url=publishing_site_url,
        topic_signature=topic_signature,
    )
    if backlink_eval["score"] < 65:
        errors.extend(backlink_eval["errors"] or ["backlink_naturalness_low"])
    specificity_eval = _evaluate_specificity(
        article_html=article_html,
        specificity_profile=specificity_profile,
    )
    if specificity_eval["score"] < 65:
        errors.extend(specificity_eval["errors"] or ["specificity_low"])
    spam_eval = _evaluate_spam_risk(
        article_html=article_html,
        primary_keyword=primary_keyword,
        backlink_url=backlink_url,
    )
    if spam_eval["score"] > ARTICLE_MAX_SPAM_RISK_SCORE:
        errors.extend(spam_eval["errors"] or ["spam_risk_high"])
    coherence_eval = _evaluate_article_coherence(
        article_html=article_html,
        topic_signature=topic_signature,
        intent_type=intent_type,
        article_angle=article_angle,
    )
    if coherence_eval["score"] < ARTICLE_QUALITY_MIN_SCORE:
        errors.extend(coherence_eval["errors"] or ["coherence_low"])
    errors.extend(
        _validate_seo_metadata(
            article_html=article_html,
            primary_keyword=primary_keyword,
            required_h1=required_h1,
            meta_title=meta_title,
            meta_description=meta_description,
            slug=slug,
            structured_mode=structured_mode,
        )
    )
    return errors


def _contains_generic_conclusion(text: str) -> bool:
    lowered = (text or "").lower()
    return any(phrase in lowered for phrase in GENERIC_CONCLUSION_PHRASES)


def _format_faq_question(question: str) -> str:
    raw = re.sub(r"\s+", " ", str(question or "").strip())
    normalized = _normalize_keyword_phrase(raw)
    if not normalized:
        return ""
    if _phrase_has_editorial_noise(normalized):
        return ""
    formatted = raw.rstrip("?").strip() if any(char.isupper() for char in raw[1:]) else normalized[:1].upper() + normalized[1:]
    if (raw.endswith("?") or _looks_like_question_phrase(normalized)) and not formatted.endswith("?"):
        formatted += "?"
    formatted_tokens = _normalize_keyword_phrase(formatted.rstrip("?")).split()
    if formatted_tokens and formatted_tokens[-1] in TRAILING_TITLE_STOPWORDS:
        return ""
    return formatted


def _faq_question_intent(question: str) -> str:
    normalized = _normalize_keyword_phrase(question)
    if not normalized:
        return ""
    for prefix in sorted(GERMAN_QUESTION_PREFIXES, key=len, reverse=True):
        if normalized == prefix or normalized.startswith(f"{prefix} "):
            return prefix
    return normalized.split()[0]


def _dedupe_faq_questions(values: List[str], *, max_items: int = FAQ_MIN_QUESTIONS) -> List[str]:
    out: List[str] = []
    for item in values:
        formatted = _format_faq_question(item)
        if not formatted:
            continue
        normalized = _normalize_keyword_phrase(formatted)
        intent = _faq_question_intent(formatted)
        if any(
            _keyword_similarity(normalized, _normalize_keyword_phrase(existing)) >= 0.7
            and _faq_question_intent(existing) == intent
            for existing in out
        ):
            continue
        out.append(formatted)
        if len(out) >= max_items:
            break
    return out


def _build_faq_fallback_questions(topic: str, *, topic_signature: Optional[Dict[str, Any]] = None) -> List[str]:
    signature = topic_signature or _build_topic_signature(
        topic=topic,
        primary_keyword=_build_topic_phrase(topic),
        secondary_keywords=[],
        target_terms=[],
        overlap_terms=[],
        trend_candidates=[],
        keyword_cluster=[],
        internal_link_inventory=[],
    )
    subject_phrase = str(signature.get("subject_phrase") or _build_topic_phrase(topic) or "dieses thema").strip()
    question_phrase = _format_sentence_start(str(signature.get("question_phrase") or _extract_topic_question_phrase(topic)).strip())
    cluster_support = next(
        (str(item).strip() for item in (signature.get("keyword_cluster_phrases") or []) if str(item).strip()),
        "",
    )
    support_phrase = cluster_support or _pick_topic_signature_support_phrase(signature, exclude_phrases=[subject_phrase])
    raw_target_term_value = ""
    for candidate in [
        *[str(item).strip() for item in (signature.get("target_terms") or []) if str(item).strip()],
        *[str(item).strip() for item in (signature.get("target_support_phrases") or []) if str(item).strip()],
        _pick_outline_target_focus_phrase(signature, exclude_phrases=[subject_phrase, support_phrase]),
    ]:
        normalized = _sanitize_editorial_phrase(candidate, allow_single_token=True)
        if normalized:
            raw_target_term_value = normalized
            break
    raw_target_term = _format_title_case(raw_target_term_value)
    support_question_focus = ""
    if support_phrase and _keyword_candidate_is_query_like(
        support_phrase,
        topic=topic,
        primary_keyword=str(signature.get("primary_keyword") or subject_phrase or topic),
        topic_signature=signature,
    ):
        support_question_focus = _format_sentence_start(support_phrase)
    target_question_focus = raw_target_term if raw_target_term_value else ""
    if question_phrase:
        direct_question = question_phrase if question_phrase.endswith("?") else f"{question_phrase}?"
        questions = [
            direct_question,
            (
                f"Woran erkennt man fruehzeitig Hinweise auf {support_question_focus}?"
                if support_question_focus
                else f"Welche Hinweise sind bei {_format_sentence_start(subject_phrase)} besonders wichtig?"
            ),
            f"Welche nächsten Schritte helfen bei {_format_sentence_start(subject_phrase)}?",
        ]
        if raw_target_term:
            questions[-1] = f"Worauf sollte man bei {raw_target_term} achten?"
        return questions
    questions = [
        f"Was ist bei {subject_phrase} wichtig?",
        f"Worauf sollte man bei {target_question_focus or support_question_focus or 'der Auswahl'} achten?",
        "Welche nächsten Schritte helfen dann im Alltag?",
    ]
    return questions


def _ensure_faq_candidates(
    topic: str,
    faq_candidates: List[str],
    *,
    topic_signature: Optional[Dict[str, Any]] = None,
    brand_name: str = "",
) -> List[str]:
    normalized_faqs = _dedupe_faq_questions(
        [
            question
            for question in faq_candidates
            if not _faq_candidate_has_planning_noise(
                question,
                topic_signature=topic_signature,
                brand_name=brand_name,
            )
        ],
        max_items=FAQ_MIN_QUESTIONS,
    )
    if len(normalized_faqs) >= FAQ_MIN_QUESTIONS:
        return normalized_faqs[:FAQ_MIN_QUESTIONS]

    topic_phrase = _build_topic_phrase(topic) or "dieses thema"
    fallback_questions = _build_faq_fallback_questions(topic, topic_signature=topic_signature)
    normalized_faqs = _dedupe_faq_questions(normalized_faqs + fallback_questions, max_items=FAQ_MIN_QUESTIONS)
    if len(normalized_faqs) >= FAQ_MIN_QUESTIONS:
        return normalized_faqs[:FAQ_MIN_QUESTIONS]

    compact_topic = _topic_head_keyword(topic) or topic_phrase or "diesem thema"
    backup_questions = [
        f"Was bedeutet {compact_topic} im Alltag?",
        f"Woran erkennt man {compact_topic} fruehzeitig?",
        f"Wann ist fachlicher Rat bei {compact_topic} sinnvoll?",
    ]
    normalized_faqs = _dedupe_faq_questions(
        normalized_faqs + backup_questions,
        max_items=max(FAQ_MIN_QUESTIONS, len(normalized_faqs) + len(backup_questions)),
    )
    return normalized_faqs[:FAQ_MIN_QUESTIONS]


def _build_article_faq_queries(
    *,
    topic: str,
    primary_keyword: str,
    article_html: str,
    max_queries: int = 6,
) -> List[str]:
    h2_headings = [
        heading for heading in _extract_h2_headings(article_html)
        if _normalize_keyword_phrase(heading) not in {"fazit", "faq"}
    ]
    return _build_keyword_query_variants(
        topic=topic,
        primary_hint=primary_keyword,
        allowed_topics=h2_headings[:2],
        max_queries=max_queries,
    )


def _coerce_generated_faqs(value: Any) -> List[Dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: List[Dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        question = str(item.get("question") or "").strip()
        answer_html = str(item.get("answer_html") or "").strip()
        search_reason = str(item.get("search_reason") or "").strip()
        if not question or not answer_html:
            continue
        if not question.endswith("?"):
            question = f"{question.rstrip('.!?')}?"
        answer_html = _strip_code_fences(answer_html)
        answer_html = re.sub(r"<h[1-6][^>]*>.*?</h[1-6]>", "", answer_html, flags=re.IGNORECASE | re.DOTALL)
        answer_html = re.sub(r"<a[^>]*>(.*?)</a>", r"\1", answer_html, flags=re.IGNORECASE | re.DOTALL)
        if not re.search(r"<(?:p|ul|ol|table)\b", answer_html, flags=re.IGNORECASE):
            answer_html = _wrap_paragraphs(answer_html)
        if not answer_html.strip():
            continue
        out.append(
            {
                "question": question,
                "answer_html": answer_html.strip(),
                "search_reason": search_reason,
            }
        )
    return out[:KEYWORD_MAX_FAQ]


def _render_faq_section_html(faqs: List[Dict[str, str]]) -> str:
    normalized_questions = _dedupe_faq_questions([item.get("question") or "" for item in faqs], max_items=len(faqs))
    rendered: List[str] = []
    for question in normalized_questions:
        item = next((faq for faq in faqs if _keyword_similarity(faq.get("question", ""), question) >= 0.8), None)
        if item is None:
            continue
        rendered.append(f"<h3>{question}</h3>{item['answer_html']}")
    return "".join(rendered)


def _replace_faq_section(article_html: str, faq_html: str) -> str:
    html = article_html or ""
    match = re.search(r"(<h2[^>]*>\s*FAQ\s*</h2>)(.*)$", html, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return html[:match.start()] + match.group(1) + faq_html
    return f"{html}<h2>FAQ</h2>{faq_html}"


def _generate_search_informed_faqs(
    *,
    article_html: str,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    current_faq_candidates: List[str],
    llm_api_key: str,
    llm_base_url: str,
    llm_model: str,
    timeout_seconds: int,
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    queries = _build_article_faq_queries(
        topic=topic,
        primary_keyword=primary_keyword,
        article_html=article_html,
    )
    search_questions: List[str] = []
    for query in queries:
        suggestions = _fetch_google_de_suggestions(
            query,
            timeout_seconds=timeout_seconds,
            trend_cache_ttl_seconds=DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS,
        )
        search_questions.extend(item for item in suggestions if _looks_like_question_phrase(item))
    normalized_search_questions = _ensure_faq_candidates(
        topic,
        _dedupe_faq_questions(search_questions + current_faq_candidates, max_items=KEYWORD_MAX_FAQ),
    )
    system_prompt = (
        "Create a German FAQ section for a finished SEO article. "
        "Use the article, the primary/secondary keywords, and Germany-focused search questions. "
        "Questions must be natural, specific, and useful for readers. "
        "Answers must be concise HTML, 35-60 words each, with no links and no markdown. "
        "Return JSON only."
    )
    article_text = _strip_html_tags(article_html)
    article_text = re.sub(r"\s+", " ", article_text).strip()[:6000]
    user_prompt = (
        f"Topic: {topic}\n"
        f"Primary keyword: {primary_keyword}\n"
        f"Secondary keywords: {secondary_keywords}\n"
        f"Germany search questions: {normalized_search_questions[:KEYWORD_MAX_FAQ]}\n"
        f"Existing FAQ candidates: {current_faq_candidates[:KEYWORD_MAX_FAQ]}\n"
        f"Article text: {article_text}\n"
        "Return JSON: "
        "{\"faqs\":[{\"question\":\"...?\",\"answer_html\":\"<p>...</p>\",\"search_reason\":\"...\"}]}"
    )
    llm_out = call_llm_json(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=llm_api_key,
        base_url=llm_base_url,
        model=llm_model,
        timeout_seconds=timeout_seconds,
        max_tokens=1800,
        temperature=0.2,
        request_label="phase5_faq_enrichment",
        usage_collector=usage_collector,
    )
    faqs = _coerce_generated_faqs(llm_out.get("faqs"))
    if len(faqs) < FAQ_MIN_QUESTIONS:
        raise CreatorError(f"FAQ enrichment returned too few items:{len(faqs)}")
    faq_html = _render_faq_section_html(faqs)
    if not faq_html.strip():
        raise CreatorError("FAQ enrichment returned empty FAQ html.")
    return {
        "faqs": faqs[:KEYWORD_MAX_FAQ],
        "faq_html": faq_html,
        "search_questions": normalized_search_questions[:KEYWORD_MAX_FAQ],
        "queries": queries,
    }


def _inject_faq_section(
    outline_items: List[Any],
    faq_candidates: List[str],
    topic: str,
    *,
    topic_signature: Optional[Dict[str, Any]] = None,
) -> List[Any]:
    if not isinstance(outline_items, list):
        return outline_items

    faq_section: Optional[Dict[str, Any]] = None
    fazit_section: Optional[Dict[str, Any]] = None
    core_sections: List[Dict[str, Any]] = []
    normalized_faqs = _ensure_faq_candidates(topic, faq_candidates, topic_signature=topic_signature)

    for item in outline_items:
        section = item if isinstance(item, dict) else {"h2": str(item), "h3": []}
        h2_value = str(section.get("h2") or "").strip()
        heading_normalized = _normalize_keyword_phrase(h2_value)
        if heading_normalized == "faq":
            faq_section = {"h2": "FAQ", "h3": normalized_faqs}
            continue
        if heading_normalized == "fazit":
            fazit_section = {"h2": "Fazit", "h3": []}
            continue
        core_sections.append({"h2": h2_value, "h3": section.get("h3") or []})

    if fazit_section is None:
        fazit_section = {"h2": "Fazit", "h3": []}
    if faq_section is None:
        faq_section = {"h2": "FAQ", "h3": normalized_faqs}

    trimmed_core = core_sections[: max(0, ARTICLE_MAX_H2 - 2)]
    return trimmed_core + [fazit_section, faq_section]


def _format_outline_heading(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if not cleaned:
        return ""
    return cleaned[:1].upper() + cleaned[1:]


def _determine_outline_heading_mode(
    *,
    topic: str,
    primary_keyword: str,
    structured_mode: str,
    topic_signature: Optional[Dict[str, Any]] = None,
) -> str:
    signature = topic_signature or {}
    subject_phrase = str(signature.get("subject_phrase") or _build_topic_phrase(topic)).strip()
    subject_tokens = _keyword_token_set(subject_phrase)
    primary_tokens = _keyword_token_set(primary_keyword)
    target_tokens = _keyword_token_set(
        " ".join(
            [
                " ".join(signature.get("target_terms") or []),
                " ".join(signature.get("target_support_phrases") or []),
            ]
        )
    )
    normalized = _normalize_keyword_phrase(
        " ".join(
            [
                topic,
                primary_keyword,
                " ".join(signature.get("target_support_phrases") or []),
                " ".join(signature.get("target_terms") or []),
            ]
        )
    )
    tokens = _keyword_token_set(normalized)
    if structured_mode == "table" or _tokens_have_decision_signal(tokens):
        return "decision"
    if _tokens_have_problem_signal(tokens) and (signature.get("question_phrase") or not target_tokens):
        return "problem"
    if target_tokens and len((primary_tokens | target_tokens) - subject_tokens) >= 2:
        return "decision"
    if _tokens_have_problem_signal(tokens):
        return "problem"
    return "guidance"


def _build_question_topic_outline_headings(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    structured_mode: str,
    intent_type: str = "informational",
    article_angle: str = "",
    topic_class: str = "general",
    topic_signature: Optional[Dict[str, Any]] = None,
) -> List[str]:
    signature = topic_signature or _build_topic_signature(
        topic=topic,
        primary_keyword=primary_keyword,
        secondary_keywords=secondary_keywords,
        target_terms=[],
        overlap_terms=[],
        trend_candidates=[],
        keyword_cluster=[],
        internal_link_inventory=[],
    )
    question_phrase = _format_sentence_start(str(signature.get("question_phrase") or _extract_topic_question_phrase(topic)).strip())
    subject_phrase = str(
        signature.get("subject_phrase") or _build_topic_phrase(topic) or _build_topic_phrase(primary_keyword) or "dieses thema"
    ).strip()
    subject_heading = _format_outline_heading(subject_phrase)
    resolved_angle = article_angle or _infer_article_angle(
        topic=topic,
        intent_type=intent_type,
        structured_mode=structured_mode,
        topic_class=topic_class,
        topic_signature=signature,
    )

    if resolved_angle == "recognition_and_next_steps":
        return [
            f"Woran erkennt man erste Hinweise auf {subject_heading}?",
            "Welche Ursachen oder Ausloeser sind haeufig?",
            "Welche Schritte helfen im Alltag zuerst?",
            f"Wann ist fachlicher Rat bei {subject_heading} sinnvoll?",
        ]
    if resolved_angle in {"process_and_decision_factors", "process_and_next_steps"}:
        return [
            "Welche Unterlagen und Kennzahlen zaehlen zuerst?",
            "Welche Fehler kosten dabei Zeit oder Geld?",
            "Wann lohnt sich professionelle Unterstuetzung?",
            "Wie laesst sich der Ablauf realistisch vorbereiten?",
        ]
    if resolved_angle == "decision_criteria":
        return [
            f"Welche Kriterien entscheiden bei {subject_heading}?",
            "Welche Unterschiede sind in der Praxis relevant?",
            "Welche Fehler sind bei der Auswahl haeufig?",
            f"Wie laesst sich {subject_heading} im Alltag sinnvoll pruefen?",
        ]
    if intent_type == "navigational":
        return [
            f"Welche Informationen zu {subject_heading} braucht man zuerst?",
            "Welche Unterlagen oder Angaben sollte man bereithalten?",
            "Welche Fehler fuehren am haeufigsten zu Rueckfragen?",
            "Wie findet man die wichtigsten Naechsten Schritte schnell?",
        ]
    return [
        f"Worauf kommt es bei {subject_heading} wirklich an?",
        "Welche Fehler sind im Alltag haeufig?",
        f"Welche Kriterien helfen bei {subject_heading} weiter?",
        "Welche naechsten Schritte bringen in der Praxis am meisten?",
    ]


def _build_deterministic_outline(
    *,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    faq_candidates: List[str],
    structured_mode: str,
    anchor_text_final: str,
    intent_type: str = "informational",
    article_angle: str = "",
    topic_class: str = "general",
    topic_signature: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    topic_phrase = _build_topic_phrase(topic) or _build_topic_phrase(primary_keyword) or "dieses Thema"
    raw_headings = _build_question_topic_outline_headings(
        topic=topic,
        primary_keyword=primary_keyword,
        secondary_keywords=secondary_keywords,
        structured_mode=structured_mode,
        intent_type=intent_type,
        article_angle=article_angle,
        topic_class=topic_class,
        topic_signature=topic_signature,
    )
    if primary_keyword and not any(
        _keyword_present(heading, primary_keyword) or _keyword_similarity(heading, primary_keyword) >= 0.78
        for heading in raw_headings
    ):
        primary_focus = _format_outline_heading(_sanitize_editorial_phrase(primary_keyword) or primary_keyword)
        if primary_focus:
            if article_angle == "recognition_and_next_steps":
                raw_headings[0] = f"Woran erkennt man erste Hinweise auf {primary_focus}?"
            elif article_angle in {"process_and_decision_factors", "process_and_next_steps"}:
                raw_headings[0] = f"Welche Schritte sind bei {primary_focus} zuerst wichtig?"
            else:
                raw_headings[0] = f"Welche Kriterien entscheiden bei {primary_focus}?"
    core_sections: List[Dict[str, Any]] = []
    for heading in raw_headings:
        if len(core_sections) >= ARTICLE_MAX_H2 - 2:
            break
        normalized = _normalize_keyword_phrase(heading)
        if any(_keyword_similarity(_normalize_keyword_phrase(item.get("h2") or ""), normalized) >= 0.8 for item in core_sections):
            continue
        core_sections.append({"h2": heading, "h3": []})

    while len(core_sections) < ARTICLE_MIN_H2 - 2:
        core_sections.append({"h2": f"Welche weiteren Aspekte sind bei {topic_phrase} relevant?", "h3": []})

    outline_items = _inject_faq_section(
        core_sections,
        faq_candidates,
        topic,
        topic_signature=topic_signature,
    )
    return {
        "outline": outline_items,
        "backlink_placement": "intro",
        "anchor_text_final": anchor_text_final,
    }


def _build_phase4_fallback_outline(
    *,
    h1: str,
    topic: str,
    primary_keyword: str,
    secondary_keywords: List[str],
    faq_candidates: List[str],
    structured_mode: str,
    anchor: str,
    anchor_safe: bool,
    anchor_type: str,
    brand_name: str,
    keyword_cluster: List[str],
    llm_out: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    anchor_text_final = str((llm_out or {}).get("anchor_text_final") or "").strip()
    if not anchor_text_final:
        anchor_text_final = anchor if anchor_safe else _build_anchor_text(anchor_type, brand_name, keyword_cluster)
    outline = _build_deterministic_outline(
        topic=topic,
        primary_keyword=primary_keyword,
        secondary_keywords=secondary_keywords,
        faq_candidates=faq_candidates,
        structured_mode=structured_mode,
        anchor_text_final=anchor_text_final,
    )
    backlink_placement = str((llm_out or {}).get("backlink_placement") or "").strip()
    if backlink_placement in {"intro", "section_2", "section_3", "section_4", "section_5"}:
        outline["backlink_placement"] = backlink_placement
    return {
        "h1": h1,
        "outline": outline["outline"],
        "backlink_placement": outline["backlink_placement"],
        "anchor_text_final": outline["anchor_text_final"],
    }


def _section_goal_from_heading(heading: str, *, section_kind: str, topic: str) -> str:
    normalized = _normalize_keyword_phrase(heading)
    topic_phrase = _format_title_case(topic or heading or "das Thema")
    if section_kind == "fazit":
        return f"Verdichte die wichtigsten Entscheidungen und nächsten Schritte zu {topic_phrase} in einer klaren, konkreten Einordnung."
    if section_kind == "faq":
        return f"Beantworte die häufigsten Rückfragen zu {topic_phrase} knapp, konkret und ohne Wiederholungen."
    if any(term in normalized for term in {"ueberblick", "wichtigste", "vergleich", "checkliste"}):
        return f"Gib einen konkreten Einstieg in {topic_phrase} und leite die wichtigsten Kriterien fuer Leserinnen und Leser her."
    if any(term in normalized for term in {"ursachen", "hintergruende", "ausloeser"}):
        return f"Erklaere Ursachen, Zusammenhaenge und typische Ausloeser zu {topic_phrase} mit alltagsnahen Beispielen."
    if any(term in normalized for term in {"auswirkungen", "herausforderungen", "risiken"}):
        return f"Zeige konkrete Folgen, Risiken und typische Herausforderungen zu {topic_phrase} auf."
    if any(term in normalized for term in {"tipps", "schritte", "hilfe", "unterstuetzung", "fehler"}):
        return f"Leite konkrete Handlungsschritte und Entscheidungshilfen zu {topic_phrase} ab."
    return f"Erklaere den Abschnittsfokus {heading} mit konkreten Kriterien, Beispielen und praktischer Orientierung zu {topic_phrase}."


def _build_deterministic_article_plan(
    *,
    phase1: Dict[str, Any],
    phase3: Dict[str, Any],
    anchor: str,
    anchor_safe: bool,
) -> Dict[str, Any]:
    topic = str(phase3.get("final_article_topic") or "").strip()
    primary_keyword = str(phase3.get("primary_keyword") or "").strip()
    topic_signature = phase3.get("topic_signature") if isinstance(phase3.get("topic_signature"), dict) else None
    secondary_keywords = _dedupe_keyword_phrases(phase3.get("secondary_keywords") or [])[:KEYWORD_MAX_SECONDARY]
    faq_questions = _ensure_faq_candidates(
        topic,
        phase3.get("faq_candidates") or [],
        topic_signature=topic_signature,
        brand_name=str(phase3.get("target_brand_name") or "").strip(),
    )
    structured_mode = str(phase3.get("structured_content_mode") or "none").strip().lower()
    content_brief = phase3.get("content_brief") or {}
    intent_type = str(phase3.get("search_intent_type") or "informational").strip() or "informational"
    article_angle = str(phase3.get("article_angle") or "practical_guidance").strip() or "practical_guidance"
    topic_class = str(phase3.get("topic_class") or "general").strip() or "general"
    keyword_buckets = phase3.get("keyword_buckets") if isinstance(phase3.get("keyword_buckets"), dict) else {}
    anchor_text_final = anchor if anchor_safe else _build_anchor_text(
        str(phase1.get("anchor_type") or "").strip(),
        str(phase1.get("brand_name") or "").strip(),
        [str(item).strip() for item in (phase1.get("keyword_cluster") or []) if str(item).strip()],
    )
    outline_package = _build_deterministic_outline(
        topic=topic,
        primary_keyword=primary_keyword,
        secondary_keywords=secondary_keywords,
        faq_candidates=faq_questions,
        structured_mode=structured_mode,
        anchor_text_final=anchor_text_final,
        intent_type=intent_type,
        article_angle=article_angle,
        topic_class=topic_class,
        topic_signature=topic_signature,
    )
    outline_items = outline_package["outline"]
    topic_focus_terms = _topic_focus_terms(topic, max_terms=2)
    semantic_support_terms = _merge_string_lists(
        [str(item).strip() for item in (keyword_buckets.get("semantic_entities") or []) if str(item).strip()],
        topic_focus_terms,
        [str(item).strip() for item in (content_brief.get("overlap_terms") or []) if str(item).strip()],
        max_items=8,
    )
    publishing_signals = _merge_string_lists(
        [str(item).strip() for item in (content_brief.get("publishing_signals") or []) if str(item).strip()],
        max_items=4,
    )
    core_positions = [
        index
        for index, item in enumerate(outline_items)
        if _normalize_keyword_phrase(str(item.get("h2") or "")) not in {"fazit", "faq"}
    ]
    core_count = max(1, len(core_positions))
    secondary_assignments: List[List[str]] = [[] for _ in range(core_count)]
    for index, keyword in enumerate(secondary_keywords):
        secondary_assignments[index % core_count].append(keyword)
    semantic_assignments: List[List[str]] = [[] for _ in range(core_count)]
    for index, term in enumerate(semantic_support_terms):
        semantic_assignments[index % core_count].append(term)
    publishing_assignments: List[List[str]] = [[] for _ in range(core_count)]
    for index, term in enumerate(publishing_signals):
        publishing_assignments[index % core_count].append(term)

    intro_words = 95
    fazit_words = 75
    faq_answer_words = 45
    target_total = 760
    core_target = max(
        110,
        min(
            170,
            int((target_total - intro_words - fazit_words - (len(faq_questions) * faq_answer_words)) / core_count),
        ),
    )
    backlink_placement = "section_2" if len(outline_items) >= 4 else "intro"
    if len(outline_items) < 2:
        backlink_placement = "intro"

    sections: List[Dict[str, Any]] = []
    core_cursor = 0
    for index, item in enumerate(outline_items, start=1):
        h2 = str(item.get("h2") or "").strip()
        normalized_h2 = _normalize_keyword_phrase(h2)
        h3_items = [str(value).strip() for value in (item.get("h3") or []) if str(value).strip()]
        section_id = f"section_{index}"
        if normalized_h2 == "faq":
            sections.append(
                {
                    "section_id": section_id,
                    "kind": "faq",
                    "h2": "FAQ",
                    "subquestion": "Welche Rueckfragen bleiben offen?",
                    "h3": faq_questions[:FAQ_MIN_QUESTIONS],
                    "goal": _section_goal_from_heading(h2, section_kind="faq", topic=topic),
                    "required_keywords": [],
                    "required_terms": [],
                    "required_elements": [],
                    "target_words": {"per_answer_min": 35, "per_answer_max": 55},
                }
            )
            continue
        if normalized_h2 == "fazit":
            sections.append(
                {
                    "section_id": section_id,
                    "kind": "fazit",
                    "h2": "Fazit",
                    "subquestion": "Was sind die wichtigsten Entscheidungen und naechsten Schritte?",
                    "h3": [],
                    "goal": _section_goal_from_heading(h2, section_kind="fazit", topic=topic),
                    "required_keywords": [],
                    "required_terms": _merge_string_lists(topic_focus_terms, semantic_support_terms[:1], max_items=3),
                    "required_elements": [],
                    "target_words": {"min": 65, "max": 95},
                }
            )
            continue

        assigned_secondaries = secondary_assignments[core_cursor] if core_cursor < len(secondary_assignments) else []
        assigned_semantic = semantic_assignments[core_cursor][:2] if core_cursor < len(semantic_assignments) else []
        assigned_publishing = publishing_assignments[core_cursor][:1] if core_cursor < len(publishing_assignments) else []
        required_keywords = _dedupe_keyword_phrases(assigned_secondaries)
        required_terms = _merge_string_lists(assigned_semantic, assigned_publishing, max_items=3)
        required_elements: List[str] = []
        if core_cursor == 0 and structured_mode == "list":
            required_elements.append("list")
        if core_cursor == 0 and structured_mode == "table":
            required_elements.append("table")
        sections.append(
            {
                "section_id": section_id,
                "kind": "body",
                "h2": h2,
                "subquestion": h2,
                "h3": h3_items,
                "goal": _section_goal_from_heading(h2, section_kind="body", topic=topic),
                "required_keywords": required_keywords,
                "required_terms": required_terms,
                "required_elements": required_elements,
                "target_words": {"min": max(90, core_target - 20), "max": min(185, core_target + 20)},
            }
        )
        core_cursor += 1

    return {
        "plan_version": "deterministic_v3",
        "h1": str(phase3.get("title_package", {}).get("h1") or "").strip(),
        "outline": outline_items,
        "sections": sections,
        "faq_questions": faq_questions[:FAQ_MIN_QUESTIONS],
        "backlink_placement": backlink_placement,
        "anchor_text_final": anchor_text_final,
        "structured_mode": structured_mode,
        "intent_type": intent_type,
        "article_angle": article_angle,
        "topic_class": topic_class,
        "style_profile": phase3.get("style_profile") or {},
        "specificity_profile": phase3.get("specificity_profile") or {},
    }


def _select_phase4_repair_topic(
    *,
    requested_topic: str,
    current_topic: str,
    primary_keyword: str,
    topic_signature: Optional[Dict[str, Any]],
) -> str:
    candidates = [
        _sanitize_editorial_phrase(requested_topic),
        str((topic_signature or {}).get("subject_phrase") or "").strip(),
        _sanitize_editorial_phrase(primary_keyword),
        _sanitize_editorial_phrase(current_topic),
    ]
    for candidate in candidates:
        cleaned = str(candidate or "").strip()
        if not cleaned:
            continue
        if not _keyword_candidate_is_support_topic_noise(cleaned, topic_signature):
            return cleaned
    return str(primary_keyword or current_topic or requested_topic or "").strip()


def _looks_like_promotional_text_block(value: str) -> bool:
    raw = str(value or "").strip()
    normalized = _normalize_keyword_phrase(_strip_html_tags(raw))
    if not normalized:
        return False
    words = normalized.split()
    has_domain = bool(re.search(r"\b[\w-]+\.(?:de|com|net|org)\b", raw, flags=re.IGNORECASE))
    has_alpha_numeric_brand = any(any(char.isalpha() for char in word) and any(char.isdigit() for char in word) for word in words)
    generic_hits = sum(1 for word in words if word in INTERNAL_LINK_GENERIC_TOKENS)
    promo_hits = sum(1 for word in words if word in {"onlineshop", "shop", "komplettbrille", "komplettbrillen", "guenstig", "guenstige"})
    return (has_domain and promo_hits >= 1) or (has_alpha_numeric_brand and generic_hits >= 3)


def _sanitize_generated_fragment_html(value: str) -> str:
    cleaned = html.unescape(str(value or ""))

    def _drop_noisy_block(match: re.Match[str]) -> str:
        block = match.group(0) or ""
        return "" if _looks_like_promotional_text_block(block) else block

    cleaned = re.sub(r"<p[^>]*>.*?</p>", _drop_noisy_block, cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<li[^>]*>.*?</li>", _drop_noisy_block, cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(
        r"\b[\w-]+\.(?:de|com|net|org)\b\s*[–-]\s*[^<]{0,120}",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"(?<!&)\bamp\b(?!;)", "und", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"(?i)\bherzlich willkommen(?:[^<\n]{0,140}?)(?:[:.!?])\s*",
        "",
        cleaned,
    )
    return cleaned.strip()


def _normalize_writer_html_fragment(value: str) -> str:
    cleaned = _strip_code_fences(value or "")
    if not cleaned.strip():
        return ""
    cleaned = re.sub(r"</?(?:html|body)[^>]*>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<h[1-6][^>]*>.*?</h[1-6]>", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<a[^>]*>(.*?)</a>", r"\1", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = _sanitize_generated_fragment_html(cleaned)
    if not re.search(r"<(?:p|ul|ol|table)\b", cleaned, flags=re.IGNORECASE):
        cleaned = _wrap_paragraphs(cleaned)
    return _strip_empty_blocks(cleaned).strip()


def _coerce_writer_section_bodies(value: Any) -> Dict[str, str]:
    if not isinstance(value, list):
        return {}
    out: Dict[str, str] = {}
    for item in value:
        if not isinstance(item, dict):
            continue
        section_id = str(item.get("section_id") or item.get("id") or "").strip()
        body_html = _normalize_writer_html_fragment(
            str(
                item.get("body_html")
                or item.get("html")
                or item.get("content_html")
                or item.get("content")
                or ""
            ).strip()
        )
        if section_id and body_html:
            out[section_id] = body_html
    return out


def _extract_writer_tagged_block(raw_text: str, tag: str) -> str:
    pattern = rf"\[\[{re.escape(tag)}\]\](.*?)\[\[/{re.escape(tag)}\]\]"
    match = re.search(pattern, raw_text or "", flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def _extract_writer_section_blocks(raw_text: str) -> Dict[str, str]:
    sections: Dict[str, str] = {}
    for section_id, body in re.findall(
        r"\[\[SECTION:([A-Za-z0-9_\-]+)\]\](.*?)\[\[/SECTION\]\]",
        raw_text or "",
        flags=re.IGNORECASE | re.DOTALL,
    ):
        normalized_body = _normalize_writer_html_fragment(body)
        if normalized_body:
            sections[str(section_id).strip()] = normalized_body
    return sections


def _extract_writer_faq_answers(raw_text: str) -> Dict[int, str]:
    answers: Dict[int, str] = {}
    for raw_index, body in re.findall(
        r"\[\[FAQ_(\d+)\]\](.*?)\[\[/FAQ_\1\]\]",
        raw_text or "",
        flags=re.IGNORECASE | re.DOTALL,
    ):
        try:
            index = int(raw_index)
        except ValueError:
            continue
        normalized_body = _normalize_writer_html_fragment(body)
        if normalized_body:
            answers[index] = normalized_body
    return answers


def _parse_writer_tagged_response(
    *,
    raw_text: str,
    article_plan: Dict[str, Any],
) -> Dict[str, Any]:
    intro_html = _normalize_writer_html_fragment(_extract_writer_tagged_block(raw_text, "INTRO_HTML"))
    if not intro_html:
        raise LLMError("Writer output missing INTRO_HTML block.")

    section_bodies = _extract_writer_section_blocks(raw_text)
    expected_section_ids = [
        str(section.get("section_id") or "").strip()
        for section in (article_plan.get("sections") or [])
        if str(section.get("kind") or "body").strip() != "faq"
    ]
    missing_sections = [section_id for section_id in expected_section_ids if section_id and section_id not in section_bodies]
    if missing_sections:
        raise LLMError("Writer output missing section blocks: " + ",".join(missing_sections[:4]))

    faq_questions = [str(item).strip() for item in (article_plan.get("faq_questions") or []) if str(item).strip()]
    faq_answers = _extract_writer_faq_answers(raw_text)
    missing_faq_indexes = [index for index in range(1, len(faq_questions) + 1) if index not in faq_answers]
    if missing_faq_indexes:
        raise LLMError(
            "Writer output missing FAQ answer blocks: " + ",".join(str(index) for index in missing_faq_indexes[:4])
        )

    faq_items = [
        {"question": question, "answer_html": faq_answers[index]}
        for index, question in enumerate(faq_questions, start=1)
        if faq_answers.get(index)
    ]
    excerpt = re.sub(r"\s+", " ", _strip_html_tags(_extract_writer_tagged_block(raw_text, "EXCERPT"))).strip()
    if not excerpt:
        excerpt = _extract_first_paragraph_text(intro_html)[:200]
    return {
        "intro_html": intro_html,
        "section_bodies": section_bodies,
        "faq_items": faq_items,
        "excerpt": excerpt,
    }


def _render_article_from_plan(
    *,
    article_plan: Dict[str, Any],
    intro_html: str,
    section_bodies: Dict[str, str],
    faq_items: List[Dict[str, str]],
) -> str:
    parts: List[str] = [f"<h1>{article_plan['h1']}</h1>"]
    if intro_html:
        parts.append(intro_html)
    used_faq_indexes: set[int] = set()
    fallback_faq_answers = [item for item in faq_items if item.get("answer_html")]

    for section in article_plan.get("sections") or []:
        h2 = str(section.get("h2") or "").strip()
        if not h2:
            continue
        section_id = str(section.get("section_id") or "").strip()
        section_kind = str(section.get("kind") or "body").strip()
        parts.append(f"<h2>{h2}</h2>")
        if section_kind == "faq":
            questions = [str(item).strip() for item in (section.get("h3") or []) if str(item).strip()]
            for question in questions:
                formatted_question = _format_faq_question(question) or question.strip()
                if formatted_question and not formatted_question.endswith("?"):
                    formatted_question = formatted_question.rstrip(".! ") + "?"
                answer_html = ""
                for idx, item in enumerate(faq_items):
                    if idx in used_faq_indexes:
                        continue
                    if _keyword_similarity(str(item.get("question") or ""), formatted_question) >= 0.75:
                        answer_html = str(item.get("answer_html") or "").strip()
                        used_faq_indexes.add(idx)
                        break
                if not answer_html and fallback_faq_answers:
                    for idx, item in enumerate(fallback_faq_answers):
                        if idx in used_faq_indexes:
                            continue
                        answer_html = str(item.get("answer_html") or "").strip()
                        used_faq_indexes.add(idx)
                        break
                if not answer_html:
                    answer_html = "<p></p>"
                parts.append(f"<h3>{formatted_question}</h3>{answer_html}")
            continue
        body_html = section_bodies.get(section_id, "")
        parts.append(body_html)

    return "".join(parts)


def _split_plain_text_sentences(value: str) -> List[str]:
    return [
        part.strip()
        for part in re.split(r"(?<=[.!?])\s+", re.sub(r"\s+", " ", str(value or "")).strip())
        if part.strip()
    ]


def _build_article_context_text(
    *,
    article_plan: Dict[str, Any],
    intro_html: str,
    section_bodies: Dict[str, str],
) -> str:
    parts: List[str] = []
    intro_text = _strip_html_tags(intro_html).strip()
    if intro_text:
        parts.append(intro_text)
    for section in article_plan.get("sections") or []:
        if str(section.get("kind") or "body").strip() == "faq":
            continue
        heading = str(section.get("h2") or "").strip()
        if heading:
            parts.append(heading)
        section_id = str(section.get("section_id") or "").strip()
        if not section_id:
            continue
        body_text = _strip_html_tags(section_bodies.get(section_id, "")).strip()
        if body_text:
            parts.append(body_text)
    return " ".join(parts).strip()


def _select_faq_support_sentences(
    *,
    context_text: str,
    question: str,
    topic: str,
    semantic_terms: List[str],
    max_sentences: int = 2,
) -> List[str]:
    reference_tokens = _keyword_focus_tokens(
        " ".join(
            [
                str(question or "").strip(),
                str(topic or "").strip(),
                " ".join(str(item).strip() for item in semantic_terms if str(item).strip()),
            ]
        )
    )
    scored: List[tuple[float, str]] = []
    for sentence in _split_plain_text_sentences(context_text):
        sentence_tokens = _keyword_focus_tokens(sentence)
        if not sentence_tokens:
            continue
        overlap = len(sentence_tokens & reference_tokens)
        family_overlap = sum(
            1 for token in sentence_tokens
            if token not in reference_tokens and _token_matches_reference_family(token, reference_tokens)
        )
        score = (overlap * 4.0) + (family_overlap * 1.4) + (_keyword_similarity(sentence, question) * 2.0)
        if score <= 0:
            continue
        scored.append((score, sentence))
    ranked = sorted(scored, key=lambda item: (-item[0], item[1]))
    selected: List[str] = []
    for _score, sentence in ranked:
        if any(_keyword_similarity(sentence, existing) >= 0.88 for existing in selected):
            continue
        selected.append(sentence)
        if len(selected) >= max_sentences:
            break
    return selected


def _build_faq_support_sentence(
    *,
    question: str,
    topic: str,
    primary_keyword: str,
    semantic_terms: List[str],
) -> str:
    normalized_question = _normalize_keyword_phrase(question)
    topic_phrase = _format_title_case(_build_topic_phrase(topic) or primary_keyword or "dieses Thema")
    support_terms = [_format_title_case(term) for term in semantic_terms if str(term).strip()]
    if len(support_terms) >= 2:
        support_text = f"{support_terms[0]} und {support_terms[1]}"
    elif support_terms:
        support_text = support_terms[0]
    else:
        support_text = topic_phrase
    if normalized_question.startswith("was ist"):
        return (
            f"Entscheidend ist dabei vor allem {support_text}, weil sich daran Nutzen, Risiken "
            "und sinnvolle Entscheidungen im konkreten Kontext besser einordnen lassen."
        )
    if normalized_question.startswith("worauf"):
        return (
            f"Wichtig sind vor allem {support_text}, weil diese Punkte Qualität, Relevanz, "
            "Aufwand oder Alltagstauglichkeit belastbarer bewertbar machen."
        )
    if "naechsten schritte" in normalized_question or "nächsten schritte" in normalized_question:
        return (
            f"Sinnvoll sind klare nächste Schritte rund um {topic_phrase}, etwa die Prüfung von "
            f"{support_text}, damit Entscheidungen nicht nur schnell, sondern auch tragfähig ausfallen."
        )
    return (
        f"Für {topic_phrase} helfen konkrete Kriterien wie {support_text}, weil sich daraus "
        "praktische Unterschiede und sinnvolle nächste Schritte ableiten lassen."
    )


def _build_deterministic_faq_answer_html(
    *,
    question: str,
    topic: str,
    primary_keyword: str,
    context_text: str,
    semantic_terms: List[str],
    min_words: int,
) -> str:
    answer_parts = _select_faq_support_sentences(
        context_text=context_text,
        question=question,
        topic=topic,
        semantic_terms=semantic_terms,
        max_sentences=2,
    )
    fallback_sentence = _build_faq_support_sentence(
        question=question,
        topic=topic,
        primary_keyword=primary_keyword,
        semantic_terms=semantic_terms,
    )
    if not answer_parts:
        answer_parts.append(fallback_sentence)
    joined = " ".join(answer_parts).strip()
    if fallback_sentence and word_count_from_html(f"<p>{joined}</p>") < min_words:
        joined = f"{joined} {fallback_sentence}".strip()
    if word_count_from_html(f"<p>{joined}</p>") < min_words:
        topic_sentence = (
            f"Gerade bei {_format_title_case(_build_topic_phrase(topic) or primary_keyword or 'diesem Thema')} "
            "kommt es deshalb auf eine konkrete, alltagsnahe Einordnung an."
        )
        joined = f"{joined} {topic_sentence}".strip()
    joined = re.sub(r"\s+", " ", joined).strip()
    if joined and joined[-1] not in ".!?":
        joined += "."
    return _wrap_paragraphs(joined) or "<p></p>"


def _ensure_faq_items_complete(
    *,
    article_plan: Dict[str, Any],
    phase3: Dict[str, Any],
    intro_html: str,
    section_bodies: Dict[str, str],
    faq_items: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    faq_questions = [str(item).strip() for item in (article_plan.get("faq_questions") or []) if str(item).strip()]
    if not faq_questions:
        return faq_items
    context_text = _build_article_context_text(
        article_plan=article_plan,
        intro_html=intro_html,
        section_bodies=section_bodies,
    )
    keyword_buckets = phase3.get("keyword_buckets") if isinstance(phase3.get("keyword_buckets"), dict) else {}
    content_brief = phase3.get("content_brief") if isinstance(phase3.get("content_brief"), dict) else {}
    semantic_terms = _merge_string_lists(
        [str(item).strip() for item in (keyword_buckets.get("semantic_entities") or []) if str(item).strip()],
        [str(item).strip() for item in ((content_brief or {}).get("target_signals") or []) if str(item).strip()],
        [str(item).strip() for item in ((content_brief or {}).get("publishing_signals") or []) if str(item).strip()],
        [str(item).strip() for item in ((content_brief or {}).get("overlap_terms") or []) if str(item).strip()],
        _topic_focus_terms(str(phase3.get("final_article_topic") or ""), max_terms=2),
        max_items=8,
    )
    faq_target_words = article_plan.get("sections") or []
    faq_section = next(
        (section for section in faq_target_words if str(section.get("kind") or "").strip() == "faq"),
        {},
    )
    per_answer_min = int(((faq_section or {}).get("target_words") or {}).get("per_answer_min") or 35)
    ensured: List[Dict[str, str]] = []
    used_indexes: set[int] = set()
    for index, question in enumerate(faq_questions, start=1):
        formatted_question = _format_faq_question(question) or question
        matched_answer_html = ""
        for item_index, item in enumerate(faq_items):
            if item_index in used_indexes:
                continue
            if _keyword_similarity(str(item.get("question") or ""), formatted_question) >= 0.75:
                matched_answer_html = str(item.get("answer_html") or "").strip()
                used_indexes.add(item_index)
                break
        if not matched_answer_html and index - 1 < len(faq_items):
            candidate = faq_items[index - 1]
            matched_answer_html = str(candidate.get("answer_html") or "").strip()
            if matched_answer_html:
                used_indexes.add(index - 1)
        if not matched_answer_html or word_count_from_html(matched_answer_html) < per_answer_min:
            matched_answer_html = _build_deterministic_faq_answer_html(
                question=formatted_question,
                topic=str(phase3.get("final_article_topic") or ""),
                primary_keyword=str(phase3.get("primary_keyword") or ""),
                context_text=context_text,
                semantic_terms=semantic_terms,
                min_words=per_answer_min,
            )
        ensured.append(
            {
                "question": formatted_question,
                "answer_html": matched_answer_html,
            }
        )
    while ensured and word_count_from_html(_render_faq_section_html(ensured)) < FAQ_MIN_WORDS:
        for item in ensured:
            answer_html = str(item.get("answer_html") or "").strip()
            expanded = _build_deterministic_faq_answer_html(
                question=str(item.get("question") or ""),
                topic=str(phase3.get("final_article_topic") or ""),
                primary_keyword=str(phase3.get("primary_keyword") or ""),
                context_text=context_text,
                semantic_terms=semantic_terms,
                min_words=max(per_answer_min + 8, 42),
            )
            if word_count_from_html(expanded) > word_count_from_html(answer_html):
                item["answer_html"] = expanded
            if word_count_from_html(_render_faq_section_html(ensured)) >= FAQ_MIN_WORDS:
                break
        else:
            break
    return ensured[:KEYWORD_MAX_FAQ]


def _build_writer_prompt_request(
    *,
    article_plan: Dict[str, Any],
    phase3: Dict[str, Any],
    llm_model: str,
    max_tokens: int,
    validation_feedback: Optional[List[str]] = None,
) -> Dict[str, Any]:
    content_brief_text = _format_content_brief_prompt_text(phase3.get("content_brief") or {})
    style_profile = phase3.get("style_profile") or {}
    specificity_profile = phase3.get("specificity_profile") or {}
    keyword_buckets = phase3.get("keyword_buckets") if isinstance(phase3.get("keyword_buckets"), dict) else {}
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
        "keyword_buckets": keyword_buckets,
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
        f"Primary query: {phase3.get('primary_keyword', '')}\n"
        f"Secondary queries: {(keyword_buckets.get('secondary_queries') or phase3.get('secondary_keywords') or [])}\n"
        f"Semantic entities: {(keyword_buckets.get('semantic_entities') or [])}\n"
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
            "keyword_buckets": phase3.get("keyword_buckets") or {},
            "keyword_provenance": phase3.get("keyword_provenance") or {},
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


def _build_partial_creator_output(
    *,
    target_site_url: str,
    publishing_site_url: str,
    phase1: Dict[str, Any],
    phase1_cache_meta: Dict[str, Any],
    phase2: Dict[str, Any],
    phase2_cache_meta: Dict[str, Any],
    phase3: Dict[str, Any],
    phase4: Dict[str, Any],
    warnings: List[str],
    debug: Dict[str, Any],
    rejection_reason: List[str],
    phase5: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "ok": False,
        "target_site_url": target_site_url,
        "host_site_url": publishing_site_url,
        "phase1": phase1,
        "phase1_cache_meta": phase1_cache_meta,
        "phase2": phase2,
        "phase2_cache_meta": phase2_cache_meta,
        "phase3": phase3,
        "phase4": phase4,
        "warnings": warnings,
        "rejection_reason": rejection_reason,
        "debug": {
            **debug,
            "rejection_reason": rejection_reason,
        },
    }
    if isinstance(phase5, dict) and phase5:
        payload["phase5"] = phase5
    return ensure_prompt_trace_in_creator_output(payload)


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
            "repair_attempts": [],
        }
        debug["prompt_trace"] = prompt_trace

    planner_trace = prompt_trace.get("planner") if isinstance(prompt_trace.get("planner"), dict) else None
    if planner_trace is None:
        planner_trace = {"mode": "deterministic", "attempts": []}
        prompt_trace["planner"] = planner_trace
    if not isinstance(prompt_trace.get("repair_attempts"), list):
        prompt_trace["repair_attempts"] = []
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


def _generate_article_from_plan(
    *,
    article_plan: Dict[str, Any],
    phase3: Dict[str, Any],
    backlink_url: str,
    publishing_site_url: str,
    internal_link_candidates: List[str],
    internal_link_anchor_map: Optional[Dict[str, str]],
    min_internal_links: int,
    max_internal_links: int,
    llm_api_key: str,
    llm_base_url: str,
    llm_model: str,
    http_timeout: int,
    max_tokens: int,
    validation_feedback: Optional[List[str]] = None,
    prompt_trace: Optional[List[Dict[str, Any]]] = None,
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    prompt_request = _build_writer_prompt_request(
        article_plan=article_plan,
        phase3=phase3,
        llm_model=llm_model,
        max_tokens=max_tokens,
        validation_feedback=validation_feedback,
    )
    system_prompt = str(prompt_request.get("system_prompt") or "")
    user_prompt = str(prompt_request.get("user_prompt") or "")
    request_label = str(prompt_request.get("request_label") or "phase5_writer_attempt_1")
    if prompt_trace is not None:
        prompt_trace.append(
            {
                "attempt": len(prompt_trace) + 1,
                "request_label": request_label,
                "model": llm_model,
                "max_tokens": max_tokens,
                "validation_feedback": list(validation_feedback or []),
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
            }
        )

    raw_text = call_llm_text(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=llm_api_key,
        base_url=llm_base_url,
        model=llm_model,
        timeout_seconds=http_timeout,
        max_tokens=max_tokens,
        temperature=0.2,
        request_label=request_label,
        usage_collector=usage_collector,
    )
    llm_out = _parse_writer_tagged_response(raw_text=raw_text, article_plan=article_plan)
    intro_html = str(llm_out.get("intro_html") or "").strip()
    section_bodies = dict(llm_out.get("section_bodies") or {})
    faq_items = _ensure_faq_items_complete(
        article_plan=article_plan,
        phase3=phase3,
        intro_html=intro_html,
        section_bodies=section_bodies,
        faq_items=_coerce_generated_faqs(llm_out.get("faq_items") or []),
    )
    article_html = _render_article_from_plan(
        article_plan=article_plan,
        intro_html=intro_html,
        section_bodies=section_bodies,
        faq_items=faq_items,
    )
    article_html = _sanitize_generated_fragment_html(article_html)
    article_html = _ensure_primary_keyword_in_intro(article_html, phase3.get("primary_keyword", ""))
    article_html = _repair_link_constraints(
        article_html=article_html,
        backlink_url=backlink_url,
        publishing_site_url=publishing_site_url,
        internal_links=internal_link_candidates,
        internal_link_anchor_map=internal_link_anchor_map,
        min_internal_links=min_internal_links,
        max_internal_links=max_internal_links,
        backlink_placement=str(article_plan.get("backlink_placement") or "intro"),
        anchor_text=str(article_plan.get("anchor_text_final") or "Weitere Informationen"),
        required_h1=str(article_plan.get("h1") or ""),
        primary_keyword=phase3.get("primary_keyword", ""),
    )
    article_html = _normalize_faq_section_questions(article_html)
    article_html = _strip_empty_blocks(article_html)
    article_html = _strip_leading_empty_blocks(article_html)
    article_html = _trim_article_to_word_limit(article_html, ARTICLE_MAX_WORDS)
    article_html = _normalize_faq_section_questions(article_html)
    excerpt = str(llm_out.get("excerpt") or "").strip()
    if not excerpt:
        excerpt = _extract_first_paragraph_text(article_html)[:200]
    return {
        "meta_title": str(article_plan.get("h1") or "").strip(),
        "meta_description": "",
        "slug": "",
        "excerpt": excerpt,
        "article_html": article_html,
    }


def _strip_all_links(html: str) -> str:
    return re.sub(
        r"<a[^>]*>(.*?)</a>",
        r"\1",
        html or "",
        flags=re.IGNORECASE | re.DOTALL,
    )


def _is_editorial_llm_repairable_error(error: str) -> bool:
    value = (error or "").strip()
    if not value or _is_link_only_error(value):
        return False
    return (
        _is_keyword_context_repairable_error(value)
        or value.startswith("word_count_too_short:")
        or value.startswith("heading_")
        or value.startswith("section_topic_drift:")
        or value.startswith("generic_filler_excessive:")
        or value.startswith("specificity_too_low:")
        or value.startswith("faq_answers_too_thin:")
        or value.startswith("faq_question_integrity_invalid:")
        or value.startswith("keyword_overused:")
        or value.startswith("backlink_promotional")
        or value.startswith("backlink_sentence_templated")
        or value.startswith("backlink_sentence_too_thin")
        or value.startswith("backlink_context_misaligned")
        or value.startswith("conclusion_generic")
        or value.startswith("conclusion_not_topic_specific")
        or value.startswith("faq_question_format_invalid")
        or value.startswith("faq_questions_not_unique")
        or value.startswith("publishing_context_missing")
        or value.startswith("entity_noise_detected")
        or value.startswith("greeting_noise_detected")
        or value.startswith("spam_")
    )


def _normalize_repaired_article_html(value: str) -> str:
    cleaned = _strip_code_fences(value or "")
    cleaned = re.sub(r"</?(?:html|body)[^>]*>", "", cleaned, flags=re.IGNORECASE)
    cleaned = _strip_all_links(cleaned)
    cleaned = _sanitize_generated_fragment_html(cleaned)
    cleaned = _strip_empty_blocks(cleaned)
    cleaned = _strip_leading_empty_blocks(cleaned)
    return cleaned.strip()


def _build_repair_prompt_request(
    *,
    article_html: str,
    article_plan: Dict[str, Any],
    phase3: Dict[str, Any],
    repair_errors: List[str],
    llm_model: str,
    max_tokens: int,
    attempt: int,
) -> Dict[str, Any]:
    content_brief_text = _format_content_brief_prompt_text(phase3.get("content_brief") or {})
    keyword_buckets = phase3.get("keyword_buckets") if isinstance(phase3.get("keyword_buckets"), dict) else {}
    repair_plan = {
        "required_h1": article_plan.get("h1"),
        "section_order": [
            {
                "section_id": section.get("section_id"),
                "kind": section.get("kind"),
                "h2": section.get("h2"),
                "h3": section.get("h3") or [],
                "goal": section.get("goal") or "",
                "required_keywords": section.get("required_keywords") or [],
                "required_terms": section.get("required_terms") or [],
            }
            for section in (article_plan.get("sections") or [])
        ],
        "intent_type": article_plan.get("intent_type") or phase3.get("search_intent_type"),
        "article_angle": article_plan.get("article_angle") or phase3.get("article_angle"),
        "topic_class": article_plan.get("topic_class") or phase3.get("topic_class"),
        "specificity_profile": article_plan.get("specificity_profile") or phase3.get("specificity_profile") or {},
        "keyword_buckets": keyword_buckets,
        "repair_errors": repair_errors[:10],
    }
    system_prompt = (
        "Revise a German (de-DE) HTML article that already has a fixed section structure. "
        "Fix only the editorial quality issues listed by the application. "
        "Keep the article informational, topic-focused, and non-promotional. "
        "Do not add hyperlinks. Return HTML only."
    )
    user_prompt = (
        f"Topic: {phase3.get('final_article_topic', '')}\n"
        f"Primary query: {phase3.get('primary_keyword', '')}\n"
        f"Secondary queries: {(keyword_buckets.get('secondary_queries') or phase3.get('secondary_keywords') or [])}\n"
        f"Semantic entities: {(keyword_buckets.get('semantic_entities') or [])}\n"
        f"Intent type: {phase3.get('search_intent_type', 'informational')}\n"
        f"Article angle: {phase3.get('article_angle', 'practical_guidance')}\n"
        f"Editorial brief: {content_brief_text}\n"
        f"Repair plan:\n{json.dumps(repair_plan, ensure_ascii=False, sort_keys=True, indent=2)}\n\n"
        "Current HTML draft (links already stripped, keep HTML structure coherent):\n"
        f"{article_html}\n\n"
        "Rules:\n"
        "- Return one full HTML article only. No markdown, no JSON, no explanations.\n"
        "- Keep exactly one <h1> and preserve the overall section order.\n"
        "- Keep the FAQ and Fazit sections at the end.\n"
        "- You may rewrite H2/H3 wording only if needed to fix heading quality or topical drift.\n"
        "- Do not add, remove, or move major sections.\n"
        "- Do not add hyperlinks. The application will reinsert links after repair.\n"
        "- Remove generic filler, advertorial phrasing, and repeated keyword stuffing.\n"
        "- Strengthen concrete specifics, topic focus, and useful decision guidance.\n"
        f"- Ensure the repaired article reaches at least {ARTICLE_MIN_WORDS} words unless the draft is already longer.\n"
        "- Keep language strictly German (de-DE)."
    )
    return {
        "request_label": f"phase7_repair_attempt_{attempt}",
        "model": llm_model,
        "max_tokens": max_tokens,
        "repair_errors": list(repair_errors),
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
    }


def _repair_article_with_llm(
    *,
    article_html: str,
    article_plan: Dict[str, Any],
    phase3: Dict[str, Any],
    repair_errors: List[str],
    backlink_url: str,
    publishing_site_url: str,
    internal_link_candidates: List[str],
    internal_link_anchor_map: Optional[Dict[str, str]],
    min_internal_links: int,
    max_internal_links: int,
    llm_api_key: str,
    llm_base_url: str,
    llm_model: str,
    http_timeout: int,
    max_tokens: int,
    attempt: int,
    prompt_trace: Optional[List[Dict[str, Any]]] = None,
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    prompt_request = _build_repair_prompt_request(
        article_html=_strip_all_links(article_html),
        article_plan=article_plan,
        phase3=phase3,
        repair_errors=repair_errors,
        llm_model=llm_model,
        max_tokens=max_tokens,
        attempt=attempt,
    )
    system_prompt = str(prompt_request.get("system_prompt") or "")
    user_prompt = str(prompt_request.get("user_prompt") or "")
    request_label = str(prompt_request.get("request_label") or f"phase7_repair_attempt_{attempt}")
    if prompt_trace is not None:
        prompt_trace.append(
            {
                "attempt": len(prompt_trace) + 1,
                "request_label": request_label,
                "model": llm_model,
                "max_tokens": max_tokens,
                "repair_errors": list(repair_errors or []),
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
            }
        )
    raw_text = call_llm_text(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        api_key=llm_api_key,
        base_url=llm_base_url,
        model=llm_model,
        timeout_seconds=http_timeout,
        max_tokens=max_tokens,
        temperature=0.2,
        request_label=request_label,
        usage_collector=usage_collector,
    )
    repaired_html = _normalize_repaired_article_html(raw_text)
    if not repaired_html:
        raise LLMError("Repair output empty.")
    repaired_html = _ensure_required_h1(repaired_html, str(article_plan.get("h1") or ""))
    repaired_html = _ensure_primary_keyword_in_intro(repaired_html, phase3.get("primary_keyword", ""))
    repaired_html = _repair_link_constraints(
        article_html=repaired_html,
        backlink_url=backlink_url,
        publishing_site_url=publishing_site_url,
        internal_links=internal_link_candidates,
        internal_link_anchor_map=internal_link_anchor_map,
        min_internal_links=min_internal_links,
        max_internal_links=max_internal_links,
        backlink_placement=str(article_plan.get("backlink_placement") or "intro"),
        anchor_text=str(article_plan.get("anchor_text_final") or "Weitere Informationen"),
        required_h1=str(article_plan.get("h1") or ""),
        primary_keyword=phase3.get("primary_keyword", ""),
    )
    repaired_html = _normalize_faq_section_questions(repaired_html)
    repaired_html = _strip_empty_blocks(repaired_html)
    repaired_html = _strip_leading_empty_blocks(repaired_html)
    repaired_html = _trim_article_to_word_limit(repaired_html, ARTICLE_MAX_WORDS)
    excerpt = _extract_first_paragraph_text(repaired_html)[:200]
    return {
        "meta_title": str(article_plan.get("h1") or "").strip(),
        "meta_description": "",
        "slug": "",
        "excerpt": excerpt,
        "article_html": repaired_html,
    }


def _validate_language_and_conclusion(article_html: str, topic: str) -> List[str]:
    errors: List[str] = []
    plain_text = _strip_html_tags(article_html)
    if _looks_english_heavy(plain_text):
        errors.append("language_not_german")

    headings = _extract_h2_headings(article_html)
    normalized_headings = [_normalize_keyword_phrase(item) for item in headings]
    if len(normalized_headings) < ARTICLE_MIN_H2 or len(normalized_headings) > ARTICLE_MAX_H2:
        errors.append("h2_count_invalid")
    if not normalized_headings or normalized_headings[-1] != "faq":
        errors.append("final_h2_not_faq")
    if len(normalized_headings) < 2 or normalized_headings[-2] != "fazit":
        errors.append("penultimate_h2_not_fazit")

    conclusion_text = _extract_h2_section_text(article_html, "Fazit")
    if conclusion_text:
        if _contains_generic_conclusion(conclusion_text):
            errors.append("conclusion_generic")
        topic_terms = _topic_keywords(topic)
        if topic_terms:
            lowered = conclusion_text.lower()
            if not any(term in lowered for term in topic_terms):
                errors.append("conclusion_not_topic_specific")

    faq_html = _extract_h2_section_html(article_html, "FAQ")
    faq_text = _strip_html_tags(faq_html).strip()
    if not faq_text:
        errors.append("faq_missing")
    else:
        faq_questions = [
            _strip_html_tags(match.group(1)).strip()
            for match in re.finditer(r"<h3[^>]*>(.*?)</h3>", faq_html, flags=re.IGNORECASE | re.DOTALL)
            if _strip_html_tags(match.group(1)).strip()
        ]
        if len(faq_questions) < FAQ_MIN_QUESTIONS:
            errors.append(f"faq_question_count_too_low:{len(faq_questions)}")
        unique_questions = _dedupe_faq_questions(faq_questions, max_items=max(FAQ_MIN_QUESTIONS, len(faq_questions)))
        if len(unique_questions) < len(faq_questions):
            errors.append("faq_questions_not_unique")
        if any(not question.endswith("?") for question in faq_questions):
            errors.append("faq_question_format_invalid")
        if word_count_from_html(faq_html) < FAQ_MIN_WORDS:
            errors.append(f"faq_answers_too_thin:{word_count_from_html(faq_html)}")

    return errors


def _validate_section_substance(article_html: str) -> List[str]:
    errors: List[str] = []
    for heading in _extract_h2_headings(article_html):
        normalized = _normalize_keyword_phrase(heading)
        if normalized in {"fazit", "faq"}:
            continue
        section_html = _extract_h2_section_html(article_html, heading)
        section_words = word_count_from_html(section_html)
        has_structure = bool(re.search(r"<(?:ul|ol|table)\b", section_html or "", flags=re.IGNORECASE))
        if section_words < ARTICLE_SECTION_MIN_WORDS and not has_structure:
            errors.append(f"section_too_thin:{normalized}")
    return errors


def _validate_phrase_integrity(article_html: str) -> List[str]:
    errors: List[str] = []
    plain_text = _normalize_keyword_phrase(_strip_html_tags(article_html))
    if any(prefix in plain_text for prefix in EDITORIAL_GREETING_PREFIXES):
        errors.append("greeting_noise_detected")
    if re.search(r"(?<!&)\bamp\b(?!;)", plain_text, flags=re.IGNORECASE):
        errors.append("entity_noise_detected")

    for heading in _extract_h2_headings(article_html):
        normalized = _normalize_keyword_phrase(heading)
        if normalized in {"fazit", "faq"}:
            continue
        if _phrase_has_editorial_noise(normalized) and not _heading_is_natural_core_question(normalized):
            errors.append(f"heading_phrase_invalid:{normalized}")
            break

    faq_html = _extract_h2_section_html(article_html, "FAQ")
    for question in [
        _strip_html_tags(match.group(1)).strip()
        for match in re.finditer(r"<h3[^>]*>(.*?)</h3>", faq_html or "", flags=re.IGNORECASE | re.DOTALL)
    ]:
        normalized_question = _normalize_keyword_phrase(question)
        if not normalized_question:
            continue
        if _phrase_has_editorial_noise(normalized_question):
            errors.append(f"faq_question_integrity_invalid:{normalized_question}")
            break
    return errors


def _extract_contextual_validation_cues(content_brief: Optional[Dict[str, Any]]) -> Dict[str, List[str]]:
    if not isinstance(content_brief, dict) or not content_brief:
        return {"publishing": [], "target": []}
    publishing_cues = _merge_string_lists(
        [str(item).strip() for item in (content_brief.get("publishing_signals") or []) if str(item).strip()],
        max_items=4,
    )
    target_cues = _merge_string_lists(
        [str(item).strip() for item in (content_brief.get("target_signals") or []) if str(item).strip()],
        [str(item).strip() for item in (content_brief.get("overlap_terms") or []) if str(item).strip()],
        max_items=4,
    )
    return {
        "publishing": publishing_cues,
        "target": target_cues,
    }


def _validate_contextual_alignment(article_html: str, content_brief: Optional[Dict[str, Any]]) -> List[str]:
    if not isinstance(content_brief, dict) or not content_brief:
        return []
    body_html = re.sub(r"<h1[^>]*>.*?</h1>", "", article_html or "", flags=re.IGNORECASE | re.DOTALL)
    plain_text = _strip_html_tags(body_html)
    normalized_text = _normalize_keyword_phrase(plain_text)
    errors: List[str] = []
    contextual_cues = _extract_contextual_validation_cues(content_brief)
    publishing_cues = contextual_cues["publishing"]
    target_cues = contextual_cues["target"]
    if publishing_cues and not any(_keyword_present_relaxed(plain_text, cue) for cue in publishing_cues):
        errors.append("publishing_context_missing")
    if target_cues and not any(_keyword_present_relaxed(plain_text, cue) for cue in target_cues):
        errors.append("target_specificity_missing")
    filler_hits = sum(1 for phrase in GENERIC_BODY_PHRASES if phrase in normalized_text)
    if filler_hits >= 3:
        errors.append(f"generic_filler_excessive:{filler_hits}")
    return errors


def _extract_backlink_context(
    article_html: str,
    *,
    backlink_url: str,
    publishing_site_url: str,
) -> Dict[str, str]:
    soup = BeautifulSoup(article_html or "", "html.parser")
    backlink_norm = _normalize_url(backlink_url)
    for anchor in soup.find_all("a", href=True):
        href = _absolutize_url(str(anchor.get("href") or "").strip(), publishing_site_url)
        if _normalize_url(href) != backlink_norm:
            continue
        container = anchor.find_parent(["p", "li"]) or anchor
        sentence = re.sub(r"\s+", " ", container.get_text(" ")).strip()
        section_heading = ""
        previous_h2 = container.find_previous("h2")
        if previous_h2 is not None:
            section_heading = re.sub(r"\s+", " ", previous_h2.get_text(" ")).strip()
        return {
            "sentence": sentence,
            "section_heading": section_heading,
            "anchor_text": re.sub(r"\s+", " ", anchor.get_text(" ")).strip(),
        }
    return {"sentence": "", "section_heading": "", "anchor_text": ""}


def _evaluate_backlink_naturalness(
    *,
    article_html: str,
    backlink_url: str,
    publishing_site_url: str,
    topic_signature: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    context = _extract_backlink_context(
        article_html,
        backlink_url=backlink_url,
        publishing_site_url=publishing_site_url,
    )
    sentence = str(context.get("sentence") or "").strip()
    anchor_text = str(context.get("anchor_text") or "").strip()
    errors: List[str] = []
    if not sentence:
        return {"score": 0, "errors": ["backlink_context_missing"], "context": context}
    normalized_sentence = _normalize_keyword_phrase(sentence)
    score = 100
    if any(phrase in normalized_sentence for phrase in PROMOTIONAL_BACKLINK_PHRASES):
        score -= 40
        errors.append("backlink_promotional")
    if any(
        phrase in normalized_sentence
        for phrase in (
            "weitere informationen bietet",
            "wer sich weiter informieren moechte",
            "wer sich zu",
            "findet beispielsweise bei",
        )
    ):
        score -= 20
        errors.append("backlink_sentence_templated")
    if len(sentence.split()) < 8:
        score -= 14
        errors.append("backlink_sentence_too_thin")
    if anchor_text and normalized_sentence.count(_normalize_keyword_phrase(anchor_text)) > 1:
        score -= 10
        errors.append("backlink_anchor_repeated")
    section_context = " ".join(filter(None, [context.get("section_heading") or "", sentence]))
    if not _topic_signature_candidate_has_relevance(section_context, topic_signature):
        score -= 22
        errors.append("backlink_context_misaligned")
    heading_text = " ".join(_extract_h2_headings(article_html))
    if anchor_text and _keyword_present_relaxed(heading_text, anchor_text):
        score -= 18
        errors.append("backlink_brand_in_heading")
    return {"score": max(0, score), "errors": _dedupe_string_values(errors), "context": context}


def _evaluate_specificity(
    *,
    article_html: str,
    specificity_profile: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    profile = specificity_profile or {}
    buckets = profile.get("buckets") if isinstance(profile.get("buckets"), dict) else {}
    min_specifics = max(2, int(profile.get("min_specifics") or 2))
    plain_text = _strip_html_tags(article_html)
    normalized_text = _normalize_keyword_phrase(plain_text)
    tokens = set(normalized_text.split())
    hits: List[str] = []
    for bucket_name, bucket_tokens in buckets.items():
        bucket_token_set = {str(token).strip() for token in bucket_tokens if str(token).strip()}
        overlap = tokens & bucket_token_set
        if len(overlap) >= 2 or (overlap and re.search(r"\d", plain_text)):
            hits.append(str(bucket_name))
    if re.search(r"\b\d+(?:[.,]\d+)?\s*(?:%|prozent|euro|eur|jahre|jahr|tage|tage|wochen|monate|qm)\b", normalized_text):
        hits.append("numeric_detail")
    if re.search(r"\b(?:uv ?400|ce|en iso|iso|din|kategorie \d|klasse \d)\b", normalized_text):
        hits.append("standard_detail")
    if re.search(r"\b(?:zum beispiel|beispielsweise|etwa|unter drei|zwischen \d+ und \d+)\b", normalized_text):
        hits.append("example_or_segment")
    unique_hits = _dedupe_preserve_order(hits)
    ratio = min(1.0, len(unique_hits) / float(max(1, min_specifics)))
    score = min(100, int(round((ratio * 70) + min(30, len(unique_hits) * 7))))
    errors: List[str] = []
    if len(unique_hits) < min_specifics:
        errors.append(f"specificity_too_low:{len(unique_hits)}")
    return {"score": score, "errors": errors, "hits": unique_hits}


def _extract_domain_label_from_url(value: str) -> str:
    try:
        netloc = urlparse(value or "").netloc.lower()
    except Exception:
        netloc = ""
    netloc = netloc.replace("www.", "")
    return netloc.split(":", 1)[0]


def _evaluate_spam_risk(
    *,
    article_html: str,
    primary_keyword: str,
    backlink_url: str,
) -> Dict[str, Any]:
    plain_text = _strip_html_tags(article_html)
    normalized_text = _normalize_keyword_phrase(plain_text)
    risk = 0
    errors: List[str] = []
    filler_hits = sum(1 for phrase in GENERIC_BODY_PHRASES if phrase in normalized_text)
    risk += filler_hits * 8
    if filler_hits >= 3:
        errors.append("spam_generic_filler")
    promo_hits = sum(1 for phrase in PROMOTIONAL_BACKLINK_PHRASES if phrase in normalized_text)
    risk += promo_hits * 16
    if promo_hits:
        errors.append("spam_promotional_language")
    exact_primary_occurrences = _count_keyword_occurrences(normalized_text, primary_keyword)
    primary_soft_cap = max(6, int(word_count_from_html(article_html) / 120))
    if exact_primary_occurrences > primary_soft_cap:
        risk += min(32, (exact_primary_occurrences - primary_soft_cap) * 6)
        errors.append("spam_primary_repetition")
    domain_label = _extract_domain_label_from_url(backlink_url)
    if domain_label:
        bare_mentions = normalized_text.count(_normalize_keyword_phrase(domain_label))
        if bare_mentions > 1:
            risk += min(20, (bare_mentions - 1) * 10)
            errors.append("spam_brand_repetition")
    return {"score": min(100, risk), "errors": _dedupe_string_values(errors)}


def _evaluate_article_coherence(
    *,
    article_html: str,
    topic_signature: Optional[Dict[str, Any]],
    intent_type: str,
    article_angle: str,
) -> Dict[str, Any]:
    score = 100
    errors: List[str] = []
    headings = _extract_h2_headings(article_html)
    for heading in headings:
        normalized = _normalize_keyword_phrase(heading)
        if normalized in {"fazit", "faq"}:
            continue
        section_text = _extract_h2_section_text(article_html, heading)
        if not _topic_signature_candidate_has_relevance(f"{heading} {section_text}", topic_signature):
            score -= 20
            errors.append(f"section_topic_drift:{normalized}")
    plan_intent = _evaluate_plan_intent_consistency(
        headings=headings,
        intent_type=intent_type,
        article_angle=article_angle,
        topic_signature=topic_signature,
    )
    if plan_intent["errors"]:
        score -= max(12, 100 - plan_intent["score"])
        errors.extend(plan_intent["errors"])
    return {"score": max(0, score), "errors": _dedupe_string_values(errors)}


def _parse_missing_secondary_keywords(errors: List[str]) -> List[str]:
    values: List[str] = []
    for error in errors:
        if not str(error).startswith("secondary_keywords_missing:"):
            continue
        raw = str(error).split(":", 1)[1]
        values.extend(part.strip() for part in raw.split(",") if part.strip())
    return _dedupe_keyword_phrases(values)


def _parse_thin_section_headings(errors: List[str]) -> List[str]:
    headings: List[str] = []
    for error in errors:
        if not str(error).startswith("section_too_thin:"):
            continue
        heading = str(error).split(":", 1)[1].strip()
        if heading:
            headings.append(heading)
    return headings


def _build_keyword_support_paragraph(
    *,
    topic: str,
    primary_keyword: str,
    publishing_signals: List[str],
    target_signals: List[str],
    secondary_keywords: List[str],
) -> str:
    sentences: List[str] = []
    topic_phrase = _format_title_case(_build_topic_phrase(topic) or primary_keyword or "dieses Thema")
    if primary_keyword:
        sentences.append(
            f"Bei {_format_title_case(primary_keyword)} helfen konkrete Kriterien, typische Einsatzsituationen und ein realistischer Blick auf Aufwand, Nutzen und Grenzen."
        )
    else:
        sentences.append(
            f"Bei {topic_phrase} helfen konkrete Kriterien, typische Einsatzsituationen und ein realistischer Blick auf Aufwand, Nutzen und Grenzen."
        )
    if publishing_signals:
        sentences.append(
            f"Gerade {_format_title_case(publishing_signals[0])} zeigt, wie sich das Thema alltagsnah und im passenden redaktionellen Kontext einordnen lässt."
        )
    if target_signals:
        sentences.append(
            f"Wichtig ist dabei auch {_format_title_case(target_signals[0])}, weil sich Qualität, Nutzen und konkrete Entscheidungskriterien daran besser einordnen lassen."
        )
    if secondary_keywords:
        sentences.append(
            f"Ebenso sollte {_format_title_case(secondary_keywords[0])} in die Entscheidung einfliessen, damit Leserinnen und Leser nicht nur oberflaechliche Tipps, sondern belastbare Orientierung erhalten."
        )
    if len(target_signals) > 1:
        sentences.append(f"Gerade {_format_title_case(target_signals[1])} zeigt, worauf es im konkreten Einsatz wirklich ankommt.")
    elif len(publishing_signals) > 1:
        sentences.append(
            f"Auch {_format_title_case(publishing_signals[1])} hilft dabei, Unterschiede, Risiken und sinnvolle nächste Schritte konkreter zu bewerten."
        )
    elif len(secondary_keywords) > 1:
        sentences.append(f"Auch {_format_title_case(secondary_keywords[1])} verdient einen kurzen Blick, weil daraus praxisnahe Unterschiede sichtbar werden.")
    return " ".join(sentences[:3]).strip()


def _find_section_end_node(h2_tag: Any) -> Any:
    current = h2_tag
    last = h2_tag
    while current is not None:
        current = current.next_sibling
        if current is None:
            break
        if getattr(current, "name", None) == "h2":
            break
        last = current
    return last


def _repair_keyword_context_gaps(
    *,
    article_html: str,
    errors: List[str],
    topic: str,
    primary_keyword: str,
    content_brief: Optional[Dict[str, Any]],
) -> str:
    if not article_html.strip():
        return article_html
    soup = BeautifulSoup(article_html, "lxml")
    body = soup.body or soup
    h2_tags = [tag for tag in body.find_all("h2") if _normalize_keyword_phrase(tag.get_text(" ")) not in {"fazit", "faq"}]
    if not h2_tags:
        return article_html

    main_h2 = h2_tags[0]
    if "primary_keyword_missing_h2" in errors and primary_keyword and not _keyword_present_relaxed(main_h2.get_text(" "), primary_keyword):
        current_heading = re.sub(r"\s+", " ", main_h2.get_text(" ")).strip()
        main_h2.string = f"{_format_title_case(primary_keyword)}: {current_heading}"

    plain_text = _strip_html_tags(str(body))
    contextual_cues = _extract_contextual_validation_cues(content_brief)
    publishing_signals = [
        signal
        for signal in contextual_cues["publishing"]
        if not _keyword_present_relaxed(plain_text, signal)
    ]
    target_signals = [
        signal
        for signal in contextual_cues["target"]
        if not _keyword_present_relaxed(plain_text, signal)
    ]
    missing_secondaries = [
        keyword for keyword in _parse_missing_secondary_keywords(errors) if not _keyword_present_relaxed(plain_text, keyword)
    ]
    thin_sections = _parse_thin_section_headings(errors)

    paragraph_targets: List[Any] = []
    for thin_heading in thin_sections:
        match = next((tag for tag in h2_tags if _normalize_keyword_phrase(tag.get_text(" ")) == thin_heading), None)
        if match is not None:
            paragraph_targets.append(match)
    if not paragraph_targets:
        paragraph_targets.append(main_h2)

    if publishing_signals or target_signals or missing_secondaries or thin_sections or "primary_keyword_missing_h2" in errors:
        for target_h2 in paragraph_targets[:2]:
            paragraph_text = _build_keyword_support_paragraph(
                topic=topic,
                primary_keyword=primary_keyword,
                publishing_signals=publishing_signals[:2],
                target_signals=target_signals[:2],
                secondary_keywords=missing_secondaries[:2],
            )
            if not paragraph_text:
                continue
            new_paragraph = soup.new_tag("p")
            new_paragraph.string = paragraph_text
            insert_after = _find_section_end_node(target_h2)
            insert_after.insert_after(new_paragraph)
            plain_text = _strip_html_tags(str(body))
            publishing_signals = [signal for signal in publishing_signals if not _keyword_present_relaxed(plain_text, signal)]
            target_signals = [signal for signal in target_signals if not _keyword_present_relaxed(plain_text, signal)]
            missing_secondaries = [keyword for keyword in missing_secondaries if not _keyword_present_relaxed(plain_text, keyword)]
            if not publishing_signals and not target_signals and not missing_secondaries:
                break

    result = body.decode_contents() if getattr(body, "decode_contents", None) else str(body)
    return _strip_empty_blocks(result)


def _wrap_paragraphs(text: str) -> str:
    cleaned = _strip_code_fences(text)
    if "<p" in cleaned or "<h2" in cleaned:
        return cleaned
    parts = [part.strip() for part in re.split(r"\n{2,}", cleaned) if part.strip()]
    if not parts:
        return ""
    return "".join(f"<p>{part}</p>" for part in parts)


def _normalize_section_html(h2: str, h3s: List[str], raw: str) -> str:
    cleaned = _strip_code_fences(raw)
    existing_h3_blocks = list(
        re.finditer(
            r"<h3[^>]*>(.*?)</h3>(.*?)(?=<h3[^>]*>|$)",
            cleaned,
            flags=re.IGNORECASE | re.DOTALL,
        )
    )
    cleaned = re.sub(r"<h[1-2][^>]*>.*?</h[1-2]>", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
    body = cleaned if re.search(r"<(?:p|ul|ol|table)\b", cleaned, flags=re.IGNORECASE) else _wrap_paragraphs(cleaned)
    html = f"<h2>{h2}</h2>"
    if h3s:
        if existing_h3_blocks:
            rendered = []
            for block in existing_h3_blocks:
                heading = _strip_html_tags(block.group(1)).strip()
                content = (block.group(2) or "").strip()
                if not heading:
                    continue
                if content and not re.search(r"<(?:p|ul|ol|table)\b", content, flags=re.IGNORECASE):
                    content = _wrap_paragraphs(content)
                rendered.append((heading, content))
            for idx, h3 in enumerate(h3s):
                html += f"<h3>{h3}</h3>"
                matched = next((content for heading, content in rendered if _keyword_similarity(heading, h3) >= 0.75), "")
                if not matched and idx < len(rendered):
                    matched = rendered[idx][1]
                html += matched or "<p></p>"
            return html

        plain_text = re.sub(r"\s+", " ", _strip_html_tags(body)).strip()
        sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", plain_text) if part.strip()]
        chunks: List[str] = []
        if sentences:
            chunk_size = max(1, math.ceil(len(sentences) / max(1, len(h3s))))
            for idx in range(len(h3s)):
                chunk = " ".join(sentences[idx * chunk_size:(idx + 1) * chunk_size]).strip()
                chunks.append(chunk)
        while len(chunks) < len(h3s):
            chunks.append("")
        for idx, h3 in enumerate(h3s):
            html += f"<h3>{h3}</h3>"
            answer = chunks[idx].strip()
            html += _wrap_paragraphs(answer) or "<p></p>"
        return html

    if body:
        html += body
    return html


def _strip_h1_tags(html: str) -> str:
    return re.sub(r"<h1[^>]*>.*?</h1>", "", html, flags=re.IGNORECASE | re.DOTALL)


def _ensure_required_h1(html: str, required_h1: str) -> str:
    body = _strip_h1_tags(html or "").strip()
    heading = _strip_html_tags(required_h1 or "").strip()
    if not heading:
        return body
    return f"<h1>{heading}</h1>{body}"


def _strip_empty_blocks(html: str) -> str:
    cleaned = html or ""
    empty_block = r"(?:\s|&nbsp;|&#160;|<br\s*/?>)*"
    for tag in ("p", "h1", "h2", "h3"):
        cleaned = re.sub(
            rf"<{tag}[^>]*>{empty_block}</{tag}>",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
    return cleaned


def _strip_leading_empty_blocks(html: str) -> str:
    cleaned = (html or "").lstrip()
    pattern = (
        r"^(?:\s*(?:"
        r"<p[^>]*>(?:\s|&nbsp;|&#160;|<br\s*/?>)*</p>"
        r"|<br\s*/?>"
        r"|<h1[^>]*>(?:\s|&nbsp;|&#160;)*</h1>"
        r"))+"
    )
    return re.sub(pattern, "", cleaned, flags=re.IGNORECASE)


def _host_variants(url: str) -> set[str]:
    parsed = urlparse((url or "").strip())
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return set()
    variants = {host}
    if host.startswith("www."):
        variants.add(host[4:])
    else:
        variants.add(f"www.{host}")
    return variants


def _absolutize_url(href: str, base_url: str) -> str:
    raw = (href or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    parsed_base = urlparse((base_url or "").strip())
    host = (parsed_base.netloc or "").strip()
    scheme = (parsed_base.scheme or "https").strip()
    if not host:
        return raw
    if raw.startswith("/"):
        return f"{scheme}://{host}{raw}"
    return raw


def _is_internal_href(href: str, publishing_site_url: str) -> bool:
    absolute = _absolutize_url(href, publishing_site_url)
    parsed = urlparse(absolute)
    if absolute.startswith("/") and not parsed.netloc:
        return True
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return False
    return host in _host_variants(publishing_site_url)


def _is_backlink_href(href: str, backlink_url: str) -> bool:
    if not backlink_url:
        return False
    return _normalize_url(_absolutize_url(href, backlink_url)) == _normalize_url(backlink_url)


def _normalize_internal_link_candidates(
    links: List[str],
    *,
    publishing_site_url: str,
    backlink_url: str,
    max_items: int,
) -> List[str]:
    out: List[str] = []
    backlink_norm = _normalize_url(backlink_url)
    for href in links:
        absolute = _absolutize_url(str(href), publishing_site_url)
        if not absolute:
            continue
        if not _is_internal_href(absolute, publishing_site_url):
            continue
        if _normalize_url(absolute) == backlink_norm:
            continue
        parsed = urlparse(absolute)
        if not parsed.scheme or not parsed.netloc:
            continue
        cleaned = absolute.strip()
        if cleaned in out:
            continue
        out.append(cleaned)
        if len(out) >= max_items:
            break
    return out


def _internal_anchor_text(url: str, anchor_map: Optional[Dict[str, str]] = None) -> str:
    normalized_url = _normalize_url(url)
    if anchor_map and normalized_url in anchor_map:
        raw_value = anchor_map[normalized_url]
        if isinstance(raw_value, (list, tuple)):
            for candidate in raw_value:
                preferred = re.sub(r"\s+", " ", str(candidate or "").strip())
                if preferred:
                    words = preferred.split()
                    return " ".join(words[:8])
        preferred = re.sub(r"\s+", " ", str(raw_value or "").strip())
        if preferred:
            words = preferred.split()
            return " ".join(words[:8])
    parsed = urlparse((url or "").strip())
    tail = (parsed.path or "").strip("/").split("/")[-1] if parsed.path else ""
    cleaned = re.sub(r"[-_]+", " ", tail).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if len(cleaned) >= 3:
        return cleaned.capitalize()
    return "Weiterfuehrende Informationen"


def _build_internal_anchor_variants(item: Dict[str, Any]) -> List[str]:
    variants: List[str] = []
    title = str(item.get("title") or "").strip()
    slug = str(item.get("slug") or "").strip()
    categories = [str(value).strip() for value in (item.get("categories") or []) if str(value).strip()]
    if title:
        variants.append(title)
        title_words = title.split()
        if len(title_words) > 4:
            variants.append(" ".join(title_words[:4]))
    if categories and title:
        variants.append(f"{categories[0]}: {title}")
    if slug:
        slug_variant = re.sub(r"[-_]+", " ", slug).strip()
        if slug_variant:
            variants.append(_format_title_case(slug_variant))
    return _merge_string_lists(variants, max_items=4)


def _extract_link_stats(article_html: str, *, backlink_url: str, publishing_site_url: str) -> Dict[str, Any]:
    links = re.findall(r"<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", article_html or "", flags=re.IGNORECASE | re.DOTALL)
    backlink_count = 0
    internal_count = 0
    external_count = 0
    internal_urls: List[str] = []
    for href, _inner in links:
        absolute = _absolutize_url(href, publishing_site_url)
        if _is_backlink_href(absolute, backlink_url):
            backlink_count += 1
            continue
        if _is_internal_href(absolute, publishing_site_url):
            internal_count += 1
            norm = _normalize_url(absolute)
            if norm and norm not in internal_urls:
                internal_urls.append(norm)
            continue
        external_count += 1
    return {
        "backlink_count": backlink_count,
        "internal_count": internal_count,
        "external_count": external_count,
        "internal_unique_count": len(internal_urls),
    }


def _validate_link_strategy(
    article_html: str,
    *,
    backlink_url: str,
    publishing_site_url: str,
    min_internal_links: int,
    max_internal_links: int,
) -> List[str]:
    stats = _extract_link_stats(article_html, backlink_url=backlink_url, publishing_site_url=publishing_site_url)
    errors: List[str] = []
    if stats["backlink_count"] != 1:
        errors.append(f"backlink_count_invalid:{stats['backlink_count']}")
    if stats["external_count"] != 0:
        errors.append(f"external_link_count_invalid:{stats['external_count']}")
    if stats["internal_count"] < min_internal_links:
        errors.append(f"internal_link_count_too_low:{stats['internal_count']}")
    if stats["internal_count"] > max_internal_links:
        errors.append(f"internal_link_count_too_high:{stats['internal_count']}")
    if stats["internal_unique_count"] < min_internal_links:
        errors.append(f"internal_link_uniqueness_too_low:{stats['internal_unique_count']}")
    return errors


def _strip_disallowed_links(html: str, *, backlink_url: str, publishing_site_url: str) -> str:
    def replacer(match: re.Match[str]) -> str:
        href = match.group(1) or ""
        inner = match.group(2) or ""
        absolute = _absolutize_url(href, publishing_site_url)
        if _is_backlink_href(absolute, backlink_url):
            return match.group(0)
        if _is_internal_href(absolute, publishing_site_url):
            return match.group(0)
        return inner

    return re.sub(
        r"<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        replacer,
        html or "",
        flags=re.IGNORECASE | re.DOTALL,
    )


def _backlink_focus_phrase(required_h1: str, primary_keyword: str) -> str:
    for candidate in (
        _extract_topic_subject_phrase(required_h1),
        _sanitize_editorial_phrase(primary_keyword),
        _normalize_keyword_phrase(primary_keyword),
    ):
        cleaned = re.sub(r"\s+", " ", str(candidate or "").strip())
        if cleaned and len(cleaned.split()) <= 6:
            return cleaned
    return ""


def _build_backlink_sentence(
    *,
    backlink_url: str,
    anchor_text: str,
    focus_phrase: str = "",
    context_text: str = "",
) -> str:
    anchor_html = f'<a href="{backlink_url}">{anchor_text}</a>'
    cleaned_focus = re.sub(r"\s+", " ", str(focus_phrase or "").strip())
    context_tokens = _keyword_focus_tokens(context_text)
    if {"auswahl", "kriterien", "vergleich", "modell", "modelle", "preis", "preise"} & context_tokens:
        if cleaned_focus:
            return f"Konkrete Beispiele und Auswahlkriterien zu {cleaned_focus} lassen sich beispielsweise bei {anchor_html} einsehen."
        return f"Konkrete Beispiele und Auswahlkriterien lassen sich beispielsweise bei {anchor_html} einsehen."
    if {"ablauf", "unterlagen", "vertrag", "schritte", "vorbereiten", "prozess"} & context_tokens:
        if cleaned_focus:
            return f"Weiterfuehrende Informationen zu {cleaned_focus} und den naechsten Schritten lassen sich beispielsweise ueber {anchor_html} einholen."
        return f"Weiterfuehrende Informationen zu den naechsten Schritten lassen sich beispielsweise ueber {anchor_html} einholen."
    if cleaned_focus:
        return f"Ergaenzende Informationen und praktische Beispiele zu {cleaned_focus} finden sich beispielsweise bei {anchor_html}."
    return f"Ergaenzende Informationen und praktische Beispiele finden sich beispielsweise bei {anchor_html}."


def _insert_backlink(
    html: str,
    backlink_url: str,
    anchor_text: str,
    placement: str,
    *,
    focus_phrase: str = "",
) -> str:
    if placement == "intro":
        match = re.search(r"</p>", html, flags=re.IGNORECASE)
        if match:
            paragraph_html = html[:match.end()]
            backlink_html = _build_backlink_sentence(
                backlink_url=backlink_url,
                anchor_text=anchor_text,
                focus_phrase=focus_phrase,
                context_text=_strip_html_tags(paragraph_html),
            )
            return html[:match.start()] + f" {backlink_html}" + html[match.start():]
        backlink_html = _build_backlink_sentence(
            backlink_url=backlink_url,
            anchor_text=anchor_text,
            focus_phrase=focus_phrase,
            context_text=_strip_html_tags(html[:240]),
        )
        return f"<p>{backlink_html}</p>" + html

    index = 0
    try:
        index = max(0, int(placement.split("_")[1]) - 1)
    except Exception:
        index = 0

    matches = list(re.finditer(r"<h2[^>]*>", html, flags=re.IGNORECASE))
    if not matches:
        backlink_html = _build_backlink_sentence(
            backlink_url=backlink_url,
            anchor_text=anchor_text,
            focus_phrase=focus_phrase,
            context_text=_strip_html_tags(html[-240:]),
        )
        return html + f"<p>{backlink_html}</p>"

    if index >= len(matches):
        index = len(matches) - 1

    start = matches[index].end()
    after = html[start:]
    p_match = re.search(r"</p>", after, flags=re.IGNORECASE)
    if p_match:
        insert_at = start + p_match.start()
        backlink_html = _build_backlink_sentence(
            backlink_url=backlink_url,
            anchor_text=anchor_text,
            focus_phrase=focus_phrase,
            context_text=_strip_html_tags(after[: p_match.end()]),
        )
        return html[:insert_at] + f" {backlink_html}" + html[insert_at:]
    backlink_html = _build_backlink_sentence(
        backlink_url=backlink_url,
        anchor_text=anchor_text,
        focus_phrase=focus_phrase,
        context_text=_strip_html_tags(after[:240]),
    )
    return html[:start] + f"<p>{backlink_html}</p>" + html[start:]


def _insert_internal_links(
    html: str,
    *,
    internal_links: List[str],
    target_internal_count: int,
    anchor_map: Optional[Dict[str, str]] = None,
) -> str:
    if target_internal_count <= 0 or not internal_links:
        return html
    working = html or ""
    h2_matches = list(re.finditer(r"<h2[^>]*>(.*?)</h2>", working, flags=re.IGNORECASE | re.DOTALL))
    usable_section_indexes = []
    for idx, match in enumerate(h2_matches):
        heading_text = _strip_html_tags(match.group(1)).strip().lower()
        if "fazit" in heading_text or "faq" in heading_text:
            continue
        usable_section_indexes.append(idx)
    if not usable_section_indexes:
        usable_section_indexes = [0]

    for idx in range(min(target_internal_count, len(internal_links))):
        href = internal_links[idx]
        anchor_text = _internal_anchor_text(href, anchor_map=anchor_map)
        link_html = f'<a href="{href}">{anchor_text}</a>'
        section_idx = usable_section_indexes[idx % len(usable_section_indexes)]
        lead = ["Siehe auch", "Passend dazu", "Vertiefend", "Hilfreich ist auch"][idx % 4]

        section_starts = list(re.finditer(r"<h2[^>]*>", working, flags=re.IGNORECASE))
        if section_starts:
            section_start = section_starts[min(section_idx, len(section_starts) - 1)].end()
            tail = working[section_start:]
            p_match = re.search(r"</p>", tail, flags=re.IGNORECASE)
            if p_match:
                insert_at = section_start + p_match.start()
                working = working[:insert_at] + f" {lead}: {link_html}." + working[insert_at:]
                continue

        match = re.search(r"</p>", working, flags=re.IGNORECASE)
        if match:
            working = working[:match.start()] + f" {lead}: {link_html}." + working[match.start():]
        else:
            working += f"<p>{lead}: {link_html}.</p>"

    return working


def _new_token_bucket() -> Dict[str, int]:
    return {
        "calls": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
    }


def _as_non_negative_int(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def _phase_from_request_label(label: str) -> str:
    cleaned = (label or "").strip().lower()
    if cleaned.startswith("phase2"):
        return "phase2"
    if cleaned.startswith("phase3"):
        return "phase3"
    if cleaned.startswith("phase4"):
        return "phase4"
    if cleaned.startswith("phase5"):
        return "phase5"
    if cleaned.startswith("phase7"):
        return "phase7"
    return "unknown"


def _is_link_only_error(error: str) -> bool:
    value = (error or "").strip()
    return (
        value.startswith("backlink_missing")
        or value.startswith("backlink_wrong_placement")
        or value.startswith("backlink_count_invalid")
        or value.startswith("internal_link_count_too_low")
        or value.startswith("internal_link_count_too_high")
        or value.startswith("internal_link_uniqueness_too_low")
        or value.startswith("external_link_count_invalid")
    )


def _is_keyword_context_repairable_error(error: str) -> bool:
    value = (error or "").strip()
    return (
        value.startswith("publishing_context_missing")
        or value.startswith("target_specificity_missing")
        or value.startswith("primary_keyword_missing_h2")
        or value.startswith("secondary_keywords_missing:")
        or value.startswith("section_too_thin:")
    )


def _repair_link_constraints(
    *,
    article_html: str,
    backlink_url: str,
    publishing_site_url: str,
    internal_links: List[str],
    internal_link_anchor_map: Optional[Dict[str, str]] = None,
    min_internal_links: int,
    max_internal_links: int,
    backlink_placement: str,
    anchor_text: str,
    required_h1: str = "",
    primary_keyword: str = "",
) -> str:
    # Remove all hyperlinks and then insert the required backlink + internal links.
    repaired = re.sub(r"<a[^>]*>(.*?)</a>", r"\1", article_html or "", flags=re.IGNORECASE | re.DOTALL)
    if backlink_url and anchor_text:
        repaired = _insert_backlink(
            repaired,
            backlink_url,
            anchor_text,
            backlink_placement,
            focus_phrase=_backlink_focus_phrase(required_h1, primary_keyword),
        )
    normalized_internal = _normalize_internal_link_candidates(
        internal_links,
        publishing_site_url=publishing_site_url,
        backlink_url=backlink_url,
        max_items=max_internal_links,
    )
    target_internal_count = min(max_internal_links, len(normalized_internal))
    if target_internal_count < min_internal_links:
        target_internal_count = len(normalized_internal)
    repaired = _insert_internal_links(
        repaired,
        internal_links=normalized_internal,
        target_internal_count=target_internal_count,
        anchor_map=internal_link_anchor_map,
    )
    repaired = _strip_disallowed_links(repaired, backlink_url=backlink_url, publishing_site_url=publishing_site_url)
    repaired = _strip_empty_blocks(repaired)
    repaired = _strip_leading_empty_blocks(repaired)
    repaired = _ensure_required_h1(repaired, required_h1)
    return repaired


def _generate_article_by_sections(
    *,
    phase4: Dict[str, Any],
    phase3: Dict[str, Any],
    backlink_url: str,
    publishing_site_url: str,
    internal_link_candidates: List[str],
    internal_link_anchor_map: Optional[Dict[str, str]],
    min_internal_links: int,
    max_internal_links: int,
    faq_candidates: List[str],
    structured_mode: str,
    llm_api_key: str,
    llm_base_url: str,
    llm_model: str,
    http_timeout: int,
    expand_passes: int,
    section_max_tokens: int,
    expand_max_tokens: int,
    usage_collector: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Optional[Dict[str, Any]]:
    outline_items = phase4.get("outline") or []
    if not isinstance(outline_items, list) or not outline_items:
        return None

    h2_count = len(outline_items)
    intro_target = 100
    target_total = 700
    per_section = max(85, int((target_total - intro_target) / max(1, h2_count)))
    per_min = max(70, per_section - 15)
    per_max = min(150, per_section + 15)

    backlink_placement = phase4.get("backlink_placement") or "intro"
    anchor_text = phase4.get("anchor_text_final") or "this resource"
    internal_links_prompt = internal_link_candidates[:max_internal_links]
    content_brief_text = _format_content_brief_prompt_text(phase3.get("content_brief") or {})
    intro_max_tokens = min(section_max_tokens, _estimate_html_max_tokens(intro_target + 20, floor=180, ceiling=420))
    body_section_max_tokens = min(section_max_tokens, _estimate_html_max_tokens(per_max + 20, floor=220, ceiling=560))
    expansion_max_tokens = min(expand_max_tokens, _estimate_html_max_tokens(130, floor=180, ceiling=360))

    intro_system = "Write a short introduction paragraph in German (de-DE) in HTML. Return only HTML."
    intro_user = (
        f"Topic: {phase3.get('final_article_topic','')}\n"
        f"H1: {phase4.get('h1','')}\n"
        f"Primary keyword: {phase3.get('primary_keyword','')}\n"
        f"Length: {intro_target - 15}-{intro_target + 15} words.\n"
        f"{content_brief_text}\n"
        "Do not include links unless explicitly requested. Language: German (de-DE). "
        "Open with a concrete reader problem or decision, not generic scene-setting. "
        "Include the primary keyword naturally in this first paragraph."
    )
    if backlink_placement == "intro":
        intro_user += f"\nInclude exactly one hyperlink to {backlink_url} with anchor text: {anchor_text}."

    try:
        intro_raw = call_llm_text(
            system_prompt=intro_system,
            user_prompt=intro_user,
            api_key=llm_api_key,
            base_url=llm_base_url,
            model=llm_model,
            timeout_seconds=http_timeout,
            max_tokens=intro_max_tokens,
            temperature=0.2,
            request_label="phase5_section_intro",
            usage_collector=usage_collector,
        )
    except LLMError:
        intro_raw = ""
    intro_html = _wrap_paragraphs(intro_raw) or "<p></p>"
    intro_html = _ensure_primary_keyword_in_intro(intro_html, phase3.get("primary_keyword", ""))
    intro_html = _ensure_required_h1(intro_html, phase4.get("h1", ""))

    sections_html: List[str] = []
    for index, item in enumerate(outline_items, start=1):
        h2 = (item.get("h2") or "").strip() if isinstance(item, dict) else str(item)
        h3s = item.get("h3") if isinstance(item, dict) else []
        h3s_list = [str(h3).strip() for h3 in (h3s or []) if str(h3).strip()]
        placement_index = None
        if backlink_placement.startswith("section_"):
            try:
                placement_index = int(backlink_placement.split("_")[1]) - 1
            except Exception:
                placement_index = None
        include_backlink = placement_index == (index - 1)

        section_system = "Write HTML for a single H2 section of a submitted article in German (de-DE). Return only HTML."
        section_user = (
            f"H2: {h2}\n"
            f"H3s: {h3s_list}\n"
            f"Primary keyword: {phase3.get('primary_keyword','')}\n"
            f"Secondary keywords: {phase3.get('secondary_keywords') or []}\n"
            f"Length: {per_min}-{per_max} words.\n"
            f"{content_brief_text}\n"
            "Write in a neutral authoritative tone in German (de-DE). "
            "Each section must add at least one concrete criterion, example, risk, checklist point, or decision aid. "
            "Avoid generic filler and repeated framing. Do not use bullet lists unless necessary."
            "\nDo not include links unless explicitly requested."
        )
        if "faq" in h2.lower():
            section_user += (
                f"\nThis is the FAQ section. Answer these questions clearly and directly: {faq_candidates[:3]}. "
                "Use the H3 questions as subheadings, avoid duplicate questions, and write 35-55 words per answer in German."
            )
        elif structured_mode == "list" and index == 1:
            section_user += "\nInclude a meaningful HTML list (<ul> or <ol>) in this section."
        elif structured_mode == "table" and index == 1:
            section_user += "\nInclude a meaningful HTML table in this section."
        if "fazit" in h2.lower():
            section_user += (
                f"\nThis is the final 'Fazit' section. Summarize concrete takeaways for topic: "
                f"{phase3.get('final_article_topic','')}. Avoid generic statements."
            )
        if include_backlink:
            section_user += f"\nInclude exactly one hyperlink to {backlink_url} with anchor text: {anchor_text}."
        if internal_links_prompt:
            section_user += (
                f"\nUse up to one internal link from this allowed list when contextually relevant: {internal_links_prompt}. "
                "Do not use external links."
            )

        try:
            raw = call_llm_text(
                system_prompt=section_system,
                user_prompt=section_user,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=llm_model,
                timeout_seconds=http_timeout,
                max_tokens=body_section_max_tokens,
                temperature=0.2,
                request_label=f"phase5_section_{index}",
                usage_collector=usage_collector,
            )
        except LLMError:
            raw = ""

        sections_html.append(_normalize_section_html(h2, h3s_list, raw))

    article_html = intro_html + "".join(sections_html)
    article_html = _repair_link_constraints(
        article_html=article_html,
        backlink_url=backlink_url,
        publishing_site_url=publishing_site_url,
        internal_links=internal_link_candidates,
        internal_link_anchor_map=internal_link_anchor_map,
        min_internal_links=min_internal_links,
        max_internal_links=max_internal_links,
        backlink_placement=backlink_placement,
        anchor_text=anchor_text,
        required_h1=phase4.get("h1", ""),
        primary_keyword=phase3.get("primary_keyword", ""),
    )
    article_html = _strip_empty_blocks(article_html)
    article_html = _strip_leading_empty_blocks(article_html)
    article_html = _ensure_required_h1(article_html, phase4.get("h1", ""))
    article_html = _trim_article_to_word_limit(article_html, ARTICLE_MAX_WORDS)

    word_count = word_count_from_html(article_html)
    for _expand_pass in range(max(0, expand_passes)):
        if word_count >= ARTICLE_MIN_WORDS:
            break
        expand_system = "Write an additional paragraph for a German (de-DE) blog post in HTML. Return only HTML."
        expand_user = (
            f"Topic: {phase3.get('final_article_topic','')}\n"
            f"Primary keyword: {phase3.get('primary_keyword','')}\n"
            f"Secondary keywords: {phase3.get('secondary_keywords') or []}\n"
            f"Current word count: {word_count}. Need at least {ARTICLE_MIN_WORDS} words.\n"
            f"{content_brief_text}\n"
            f"Write one additional paragraph of 80-120 words that fits the article. "
            "Add concrete detail rather than summary or filler. No hyperlinks. Language: German (de-DE)."
        )
        try:
            extra = call_llm_text(
                system_prompt=expand_system,
                user_prompt=expand_user,
                api_key=llm_api_key,
                base_url=llm_base_url,
                model=llm_model,
                timeout_seconds=http_timeout,
                max_tokens=expansion_max_tokens,
                temperature=0.2,
                request_label="phase5_section_expand",
                usage_collector=usage_collector,
            )
            article_html += _wrap_paragraphs(extra)
            word_count = word_count_from_html(article_html)
        except LLMError:
            break
    article_html = _ensure_required_h1(article_html, phase4.get("h1", ""))
    article_html = _trim_article_to_word_limit(article_html, ARTICLE_MAX_WORDS)

    meta_title = phase4.get("h1") or ""
    excerpt = ""
    match = re.search(r"<p[^>]*>(.*?)</p>", article_html, flags=re.IGNORECASE | re.DOTALL)
    if match:
        excerpt = re.sub(r"<[^>]+>", "", match.group(1)).strip()[:200]

    return {
        "meta_title": meta_title,
        "meta_description": "",
        "slug": "",
        "excerpt": excerpt,
        "article_html": article_html,
    }


def _call_leonardo(
    *,
    prompt: str,
    api_key: str,
    base_url: str,
    model_id: str,
    width: int,
    height: int,
    timeout_seconds: int,
    poll_timeout_seconds: int,
    poll_interval_seconds: int,
) -> str:
    if not api_key:
        raise CreatorError("LEONARDO_API_KEY is required for image generation.")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    create_url = base_url.rstrip("/") + "/generations"
    payload = {
        "prompt": prompt,
        "modelId": model_id,
        "width": width,
        "height": height,
        "num_images": 1,
        "negative_prompt": NEGATIVE_PROMPT,
        "guidance_scale": 6,
    }
    try:
        response = requests.post(create_url, headers=headers, json=payload, timeout=timeout_seconds)
    except Exception as exc:  # pragma: no cover - network
        raise CreatorError(f"Leonardo request failed: {exc}") from exc
    if response.status_code >= 400:
        raise CreatorError(f"Leonardo HTTP {response.status_code}: {response.text[:300]}")
    body = response.json()
    generation_id = body.get("sdGenerationJob", {}).get("generationId") or body.get("generationId")
    if not generation_id:
        raise CreatorError("Leonardo response missing generationId.")

    poll_url = base_url.rstrip("/") + f"/generations/{generation_id}"
    deadline = time.time() + poll_timeout_seconds
    while time.time() < deadline:
        try:
            poll = requests.get(poll_url, headers=headers, timeout=timeout_seconds)
        except Exception as exc:  # pragma: no cover - network
            raise CreatorError(f"Leonardo poll failed: {exc}") from exc
        if poll.status_code >= 400:
            raise CreatorError(f"Leonardo poll HTTP {poll.status_code}: {poll.text[:300]}")
        data = poll.json()
        generations = data.get("generations") or data.get("sdGenerationJob", {}).get("generations") or []
        if generations:
            url = generations[0].get("url") or generations[0].get("imageUrl")
            if url:
                return url
        time.sleep(poll_interval_seconds)
    raise CreatorError("Leonardo generation timed out.")


PHASE_LABELS: List[str] = [
    "",                          # index 0 unused
    "Analyzing target site",     # phase 1
    "Analyzing publishing site", # phase 2
    "Selecting topic",           # phase 3
    "Creating outline",          # phase 4
    "Writing article",           # phase 5
    "Generating images",         # phase 6
    "Final SEO checks",          # phase 7
]

def _noop_progress(phase: int, label: str, percent: int) -> None:
    pass


def run_creator_pipeline(
    *,
    target_site_url: str,
    publishing_site_url: str,
    publishing_site_id: Optional[str],
    client_target_site_id: Optional[str],
    anchor: Optional[str],
    topic: Optional[str],
    exclude_topics: Optional[List[str]] = None,
    recent_article_titles: Optional[List[str]] = None,
    internal_link_inventory: Optional[List[Dict[str, Any]]] = None,
    phase1_cache_payload: Optional[Dict[str, Any]] = None,
    phase1_cache_content_hash: Optional[str] = None,
    phase2_cache_payload: Optional[Dict[str, Any]] = None,
    phase2_cache_content_hash: Optional[str] = None,
    target_profile_payload: Optional[Dict[str, Any]] = None,
    target_profile_content_hash: Optional[str] = None,
    publishing_profile_payload: Optional[Dict[str, Any]] = None,
    publishing_profile_content_hash: Optional[str] = None,
    dry_run: bool,
    on_progress: Optional[Callable[[int, str, int], None]] = None,
) -> Dict[str, Any]:
    progress = on_progress or _noop_progress
    warnings: List[str] = []
    phase_names = [f"phase{i}" for i in range(1, 8)]
    tokens_by_phase: Dict[str, Dict[str, int]] = {phase: _new_token_bucket() for phase in phase_names}
    tokens_by_label: Dict[str, Dict[str, int]] = {}
    debug: Dict[str, Any] = {
        "dry_run": dry_run,
        "timings_ms": {},
        "fetched_pages": [],
        "tokens_by_phase": tokens_by_phase,
        "tokens_by_label": tokens_by_label,
        "prompt_trace": {
            "planner": {
                "mode": "deterministic",
                "attempts": [],
            },
            "writer_attempts": [],
            "repair_attempts": [],
        },
    }
    current_year = datetime.datetime.now().year

    execution_policy = _build_pipeline_execution_policy()
    strict_failure_mode = bool(execution_policy["strict_failure_mode"])
    http_timeout = _read_int_env("CREATOR_HTTP_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
    http_retries = _read_non_negative_int_env("CREATOR_HTTP_RETRIES", DEFAULT_HTTP_RETRIES)
    site_analysis_max_pages = max(1, _read_int_env("CREATOR_SITE_ANALYSIS_MAX_PAGES", DEFAULT_SITE_ANALYSIS_MAX_PAGES))
    phase2_prompt_chars = _read_int_env("CREATOR_PHASE2_PROMPT_CHARS", 2500)
    phase2_max_tokens = _read_int_env("CREATOR_PHASE2_MAX_TOKENS", 3000)
    phase4_max_attempts = 1
    phase4_max_tokens = _read_int_env("CREATOR_PHASE4_MAX_TOKENS", 3000)
    phase5_max_attempts = int(execution_policy["phase5_max_attempts"])
    phase5_max_tokens_attempt1 = max(320, _read_int_env("CREATOR_PHASE5_MAX_TOKENS_ATTEMPT1", 900))
    phase5_max_tokens_retry = max(800, _read_int_env("CREATOR_PHASE5_MAX_TOKENS_RETRY", 1800))
    phase7_repair_attempts = int(execution_policy["phase7_repair_attempts"])
    phase7_repair_max_tokens = max(1200, _read_int_env("CREATOR_PHASE7_REPAIR_MAX_TOKENS", 2200))
    internal_link_min = max(0, _read_int_env("CREATOR_INTERNAL_LINK_MIN", DEFAULT_INTERNAL_LINK_MIN))
    internal_link_max = max(internal_link_min, _read_int_env("CREATOR_INTERNAL_LINK_MAX", DEFAULT_INTERNAL_LINK_MAX))
    internal_link_candidates_max = max(internal_link_max, _read_int_env("CREATOR_INTERNAL_LINK_CANDIDATES_MAX", 10))
    keyword_trends_enabled = _read_bool_env("CREATOR_KEYWORD_TRENDS_ENABLED", True)
    keyword_trends_timeout = max(1, _read_int_env("CREATOR_KEYWORD_TRENDS_TIMEOUT_SECONDS", 4))
    keyword_trends_max_terms = max(4, min(20, _read_int_env("CREATOR_KEYWORD_TRENDS_MAX_TERMS", 10)))
    keyword_trend_cache_ttl_seconds = max(
        3600,
        _read_int_env("CREATOR_KEYWORD_TREND_CACHE_TTL_SECONDS", DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS),
    )
    explicit_llm_key = os.getenv("CREATOR_LLM_API_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    explicit_shared_model = os.getenv("CREATOR_LLM_MODEL", "").strip()
    explicit_planning_model = os.getenv("CREATOR_LLM_MODEL_PLANNING", "").strip()
    explicit_writing_model = os.getenv("CREATOR_LLM_MODEL_WRITING", "").strip()
    planning_model = explicit_planning_model or explicit_shared_model
    writing_model = explicit_writing_model or explicit_shared_model
    explicit_base_url = os.getenv("CREATOR_LLM_BASE_URL", "").strip()
    if not planning_model:
        planning_model = DEFAULT_ANTHROPIC_PLANNING_MODEL if anthropic_key else DEFAULT_OPENAI_LLM_MODEL
    if not writing_model:
        writing_model = DEFAULT_ANTHROPIC_WRITING_MODEL if anthropic_key else DEFAULT_OPENAI_LLM_MODEL

    if explicit_base_url:
        llm_base_url = explicit_base_url
    elif anthropic_key and _model_prefers_anthropic(planning_model, writing_model):
        llm_base_url = "https://api.anthropic.com/v1"
    elif anthropic_key and not openai_key:
        llm_base_url = "https://api.anthropic.com/v1"
    else:
        llm_base_url = DEFAULT_LLM_BASE_URL

    if "anthropic" in llm_base_url.lower():
        llm_api_key = explicit_llm_key or anthropic_key or openai_key
    else:
        llm_api_key = explicit_llm_key or openai_key or anthropic_key

    debug["keyword_trends_enabled"] = keyword_trends_enabled
    debug["keyword_trend_cache_ttl_seconds"] = keyword_trend_cache_ttl_seconds
    debug["strict_failure_mode"] = strict_failure_mode
    provided_internal_link_inventory = _coerce_internal_link_inventory(internal_link_inventory)
    target_profile = _coerce_site_profile_payload(target_profile_payload)
    publishing_profile = _coerce_site_profile_payload(publishing_profile_payload)
    if not target_profile or not publishing_profile:
        raise CreatorError(
            "Creator requires deterministic target_profile and publishing_profile for profile-first topic selection."
        )
    debug["internal_link_inventory_count"] = len(provided_internal_link_inventory)
    safe_recent_titles = _dedupe_recent_title_values(recent_article_titles)
    debug["recent_article_titles_count"] = len(safe_recent_titles)

    def _collect_llm_usage(record: Dict[str, Any]) -> None:
        label = str(record.get("label") or "unspecified")
        phase_key = _phase_from_request_label(label)
        bucket = tokens_by_phase.get(phase_key)
        if bucket is None:
            bucket = _new_token_bucket()
            tokens_by_phase[phase_key] = bucket

        label_bucket = tokens_by_label.get(label)
        if label_bucket is None:
            label_bucket = _new_token_bucket()
            tokens_by_label[label] = label_bucket

        for target_bucket in (bucket, label_bucket):
            target_bucket["calls"] += 1
            target_bucket["prompt_tokens"] += _as_non_negative_int(record.get("prompt_tokens"))
            target_bucket["completion_tokens"] += _as_non_negative_int(record.get("completion_tokens"))
            target_bucket["total_tokens"] += _as_non_negative_int(record.get("total_tokens"))
            target_bucket["cache_creation_input_tokens"] += _as_non_negative_int(record.get("cache_creation_input_tokens"))
            target_bucket["cache_read_input_tokens"] += _as_non_negative_int(record.get("cache_read_input_tokens"))

    progress(1, PHASE_LABELS[1], 0)
    phase_start = time.time()
    logger.info("creator.phase1.start target=%s", target_site_url)
    normalized_target_url = _normalize_url(target_site_url)
    target_content_hash = (target_profile_content_hash or "").strip() or _hash_text(
        json.dumps(target_profile, sort_keys=True, ensure_ascii=False)
    )
    target_snapshot = {
        "pages": [],
        "sample_urls": list(target_profile.get("sample_urls") or []),
        "sample_page_titles": list(target_profile.get("sample_page_titles") or []),
        "combined_text": " ".join(
            _merge_string_lists(
                target_profile.get("topics") or [],
                target_profile.get("repeated_keywords") or [],
                target_profile.get("visible_headings") or [],
                max_items=24,
            )
        ),
        "content_hash": target_content_hash,
    }
    phase1 = _phase1_from_target_profile(target_profile, target_site_url=target_site_url)
    phase1_cache_hit = False
    phase1_cache_warm = False
    phase1_cache_meta = {
        "normalized_url": normalized_target_url,
        "content_hash": target_content_hash,
        "prompt_version": PHASE1_CACHE_PROMPT_VERSION,
        "generator_mode": "deterministic",
        "model_name": "",
        "cache_hit": False,
        "cacheable": True,
        "snapshot_page_count": len(target_snapshot.get("pages") or []),
        "sample_urls": list(target_snapshot.get("sample_urls") or []),
    }

    brand_name = str(phase1.get("brand_name") or "").strip()
    backlink_url = str(phase1.get("backlink_url") or "").strip() or (target_site_url or "")
    anchor_type = str(phase1.get("anchor_type") or "").strip()
    keyword_cluster = [str(item).strip() for item in (phase1.get("keyword_cluster") or []) if str(item).strip()]
    if anchor_type not in {"brand", "contextual_generic", "partial_match"}:
        anchor_type = "partial_match" if (not brand_name and keyword_cluster) else ("brand" if brand_name else "contextual_generic")
    phase1 = {
        "brand_name": brand_name,
        "backlink_url": backlink_url,
        "anchor_type": anchor_type,
        "keyword_cluster": keyword_cluster,
        "site_summary": str(phase1.get("site_summary") or "").strip(),
        "sample_page_titles": [str(item).strip() for item in (phase1.get("sample_page_titles") or []) if str(item).strip()],
        "sample_urls": [str(item).strip() for item in (phase1.get("sample_urls") or []) if str(item).strip()],
    }

    debug["phase1_cache_hit"] = phase1_cache_hit
    debug["phase1_snapshot"] = {
        "page_count": len(target_snapshot.get("pages") or []),
        "sample_urls": phase1.get("sample_urls") or [],
        "cache_fallback_used": phase1_cache_warm,
    }
    debug["timings_ms"]["phase1"] = int((time.time() - phase_start) * 1000)
    progress(1, PHASE_LABELS[1], 14)

    progress(2, PHASE_LABELS[2], 14)
    phase_start = time.time()
    logger.info("creator.phase2.start publishing=%s", publishing_site_url)
    normalized_publishing_url = _normalize_url(publishing_site_url)
    inventory_topic_insights = _build_inventory_topic_insights(provided_internal_link_inventory)
    publishing_content_hash = (publishing_profile_content_hash or "").strip() or _hash_text(
        json.dumps(publishing_profile, sort_keys=True, ensure_ascii=False)
    )
    publishing_snapshot = {
        "pages": [],
        "sample_urls": list(publishing_profile.get("sample_urls") or []),
        "sample_page_titles": list(publishing_profile.get("sample_page_titles") or []),
        "combined_text": " ".join(
            _merge_string_lists(
                publishing_profile.get("topics") or [],
                publishing_profile.get("topic_clusters") or [],
                publishing_profile.get("visible_headings") or [],
                max_items=24,
            )
        ),
        "content_hash": publishing_content_hash,
    }
    internal_link_candidates: List[str] = []
    effective_internal_min = 0
    effective_internal_max = 0
    phase2 = _phase2_from_publishing_profile(publishing_profile)
    phase2_cache_hit = False
    phase2_cache_warm = False
    phase2_cache_meta = {
        "normalized_url": normalized_publishing_url,
        "content_hash": publishing_content_hash,
        "prompt_version": PHASE2_CACHE_PROMPT_VERSION,
        "generator_mode": "deterministic",
        "model_name": "",
        "cache_hit": False,
        "cacheable": True,
        "snapshot_page_count": len(publishing_snapshot.get("pages") or []),
        "sample_urls": list(publishing_snapshot.get("sample_urls") or []),
    }
    phase2 = _merge_phase2_analysis(
        phase2,
        None,
        inventory_categories=inventory_topic_insights.get("site_categories") or [],
    )
    debug["phase2_cache_hit"] = phase2_cache_hit
    debug["phase2_snapshot"] = {
        "page_count": len(publishing_snapshot.get("pages") or []),
        "sample_urls": phase2.get("sample_urls") or [],
        "inventory_category_count": len(phase2.get("site_categories") or []),
        "cache_fallback_used": phase2_cache_warm,
    }
    debug["timings_ms"]["phase2"] = int((time.time() - phase_start) * 1000)
    progress(2, PHASE_LABELS[2], 28)

    progress(3, PHASE_LABELS[3], 28)
    phase_start = time.time()
    logger.info("creator.phase3.start")
    safe_exclude = list(exclude_topics or [])
    requested_topic = (topic or "").strip()
    target_profile_for_fit = target_profile
    publishing_profile_for_fit = publishing_profile
    target_profile_hash = (target_profile_content_hash or "").strip() or _hash_text(
        json.dumps(target_profile_for_fit, sort_keys=True, ensure_ascii=False)
    )
    publishing_profile_hash = (publishing_profile_content_hash or "").strip() or _hash_text(
        json.dumps(publishing_profile_for_fit, sort_keys=True, ensure_ascii=False)
    )
    pair_fit = None
    try:
        pair_fit = _run_pair_fit_reasoning(
            requested_topic=requested_topic,
            exclude_topics=safe_exclude,
            target_site_url=target_site_url,
            publishing_site_url=publishing_site_url,
            target_profile=target_profile_for_fit,
            publishing_profile=publishing_profile_for_fit,
            llm_api_key=llm_api_key,
            llm_base_url=llm_base_url,
            planning_model=planning_model,
            timeout_seconds=http_timeout,
            usage_collector=_collect_llm_usage,
        )
    except LLMError as exc:
        raise CreatorError(f"Pair fit reasoning failed: {exc}") from exc
    final_match_decision = str(pair_fit.get("final_match_decision") or "").strip().lower() or (
        "accepted" if bool(pair_fit.get("backlink_fit_ok")) else "hard_reject"
    )
    allow_rejected_pairs_for_testing = _read_bool_env("ALLOW_REJECTED_PAIRS_FOR_TESTING", False)
    if final_match_decision != "accepted":
        rejection_reason = str(
            pair_fit.get("reject_reason")
            or pair_fit.get("rejection_reason")
            or pair_fit.get("best_overlap_reason")
            or "no_natural_semantic_fit"
        ).strip()
        if not allow_rejected_pairs_for_testing:
            raise CreatorError(f"Pair fit rejected: {rejection_reason}")
        warnings.append(f"pair_fit_override_enabled:{final_match_decision}")
    elif final_match_decision == "accepted":
        pair_fit["backlink_fit_ok"] = True

    resolved_topic = str(pair_fit.get("final_article_topic") or requested_topic or "").strip()
    if not resolved_topic:
        raise CreatorError("Pair fit returned no final_article_topic.")
    phase3 = {
        "final_article_topic": resolved_topic,
        "search_intent_type": _infer_search_intent_type(topic=resolved_topic, target_profile=target_profile),
        "primary_keyword": requested_topic or resolved_topic,
        "secondary_keywords": keyword_cluster[1:3] if len(keyword_cluster) > 1 else [],
        "pair_fit": pair_fit,
        "target_brand_name": str(phase1.get("brand_name") or "").strip(),
    }

    keyword_discovery: Dict[str, Any] = {"query_variants": [], "trend_candidates": [], "faq_candidates": []}
    if keyword_trends_enabled and phase3.get("final_article_topic"):
        keyword_discovery = _discover_keyword_candidates(
            topic=phase3.get("final_article_topic", ""),
            primary_hint=phase3.get("primary_keyword", ""),
            keyword_cluster=keyword_cluster,
            allowed_topics=phase2.get("allowed_topics") or [],
            timeout_seconds=keyword_trends_timeout,
            max_terms=keyword_trends_max_terms,
            trend_cache_ttl_seconds=keyword_trend_cache_ttl_seconds,
        )
    phase3["content_brief"] = _build_content_brief(
        topic=phase3.get("final_article_topic", ""),
        phase2=phase2,
        pair_fit=pair_fit,
        target_profile=target_profile,
        publishing_profile=publishing_profile,
        brand_name=phase3.get("target_brand_name", ""),
    )
    signature_overlap_terms = [str(item).strip() for item in (pair_fit.get("overlap_terms") or []) if str(item).strip()]
    signature_target_terms = _select_signature_target_terms(
        topic=phase3.get("final_article_topic", ""),
        target_profile=target_profile,
        content_brief=phase3.get("content_brief") or {},
        overlap_terms=signature_overlap_terms,
        brand_name=phase3.get("target_brand_name", ""),
        max_items=8,
    )
    keyword_selection = _select_keywords(
        topic=phase3.get("final_article_topic", ""),
        llm_primary=phase3.get("primary_keyword", ""),
        llm_secondary=phase3.get("secondary_keywords") or [],
        keyword_cluster=keyword_cluster,
        allowed_topics=phase2.get("allowed_topics") or [],
        trend_candidates=keyword_discovery.get("trend_candidates") or [],
        faq_candidates=keyword_discovery.get("faq_candidates") or [],
        target_terms=signature_target_terms,
        overlap_terms=signature_overlap_terms,
        internal_link_inventory=provided_internal_link_inventory,
    )
    phase3["primary_keyword"] = _align_primary_keyword_to_topic(
        topic=phase3.get("final_article_topic", ""),
        current_primary=keyword_selection["primary_keyword"],
        trend_candidates=_dedupe_keyword_phrases(
            (keyword_selection.get("secondary_keywords") or []) + (keyword_selection.get("trend_candidates") or [])
        ),
        keyword_cluster=keyword_cluster,
    )
    phase3["topic_signature"] = _build_topic_signature(
        topic=phase3.get("final_article_topic", ""),
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=keyword_selection.get("secondary_keywords") or [],
        target_terms=signature_target_terms,
        overlap_terms=signature_overlap_terms,
        trend_candidates=keyword_discovery.get("trend_candidates") or [],
        keyword_cluster=keyword_cluster,
        internal_link_inventory=provided_internal_link_inventory,
    )
    phase3["secondary_keywords"] = _finalize_secondary_keywords(
        topic=phase3.get("final_article_topic", ""),
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=keyword_selection.get("secondary_keywords") or [],
        keyword_cluster=keyword_cluster,
        allowed_topics=phase2.get("allowed_topics") or [],
        topic_signature=phase3.get("topic_signature"),
    )
    phase3["topic_signature"] = _build_topic_signature(
        topic=phase3.get("final_article_topic", ""),
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=phase3.get("secondary_keywords") or [],
        target_terms=signature_target_terms,
        overlap_terms=signature_overlap_terms,
        trend_candidates=keyword_discovery.get("trend_candidates") or [],
        keyword_cluster=keyword_cluster,
        internal_link_inventory=provided_internal_link_inventory,
    )
    phase3["faq_candidates"] = _ensure_faq_candidates(
        phase3.get("final_article_topic", ""),
        keyword_selection.get("faq_candidates") or [],
        topic_signature=phase3.get("topic_signature"),
        brand_name=str(phase3.get("target_brand_name") or "").strip(),
    )
    phase3["structured_content_mode"] = _structured_content_mode(
        phase3.get("final_article_topic", ""),
        phase3.get("primary_keyword", ""),
        phase3.get("search_intent_type", ""),
    )
    phase3["topic_class"] = _infer_topic_class(
        topic=phase3.get("final_article_topic", ""),
        target_profile=target_profile,
        publishing_profile=publishing_profile,
        content_brief=phase3.get("content_brief") or {},
    )
    phase3["article_angle"] = _infer_article_angle(
        topic=phase3.get("final_article_topic", ""),
        intent_type=phase3.get("search_intent_type", ""),
        structured_mode=phase3.get("structured_content_mode", "none"),
        topic_class=phase3.get("topic_class", "general"),
        topic_signature=phase3.get("topic_signature"),
    )
    phase3["style_profile"] = _build_style_profile(
        topic=phase3.get("final_article_topic", ""),
        topic_class=phase3.get("topic_class", "general"),
        intent_type=phase3.get("search_intent_type", ""),
        article_angle=phase3.get("article_angle", "practical_guidance"),
        content_brief=phase3.get("content_brief") or {},
        publishing_profile=publishing_profile,
        target_profile=target_profile,
    )
    phase3["specificity_profile"] = _build_specificity_profile(
        topic=phase3.get("final_article_topic", ""),
        topic_class=phase3.get("topic_class", "general"),
        intent_type=phase3.get("search_intent_type", ""),
    )
    title_package = _build_deterministic_title_package(
        topic=phase3.get("final_article_topic", ""),
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=phase3.get("secondary_keywords") or [],
        search_intent_type=phase3.get("search_intent_type", ""),
        structured_mode=phase3.get("structured_content_mode", "none"),
        current_year=current_year,
        article_angle=phase3.get("article_angle", ""),
        topic_class=phase3.get("topic_class", "general"),
        recent_titles=safe_recent_titles,
    )
    phase3["title_package"] = title_package
    phase3["keyword_buckets"] = _build_keyword_buckets(
        topic=phase3.get("final_article_topic", ""),
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=phase3.get("secondary_keywords") or [],
        keyword_cluster=keyword_cluster,
        trend_candidates=keyword_discovery.get("trend_candidates") or [],
        allowed_topics=phase2.get("allowed_topics") or [],
        target_terms=signature_target_terms,
        overlap_terms=signature_overlap_terms,
        topic_signature=phase3.get("topic_signature"),
    )
    phase3["keyword_provenance"] = phase3.get("keyword_buckets", {}).get("provenance") or {}
    phase3["keyword_buckets"] = {
        key: value
        for key, value in (phase3.get("keyword_buckets") or {}).items()
        if key != "provenance"
    }
    phase3["secondary_keywords"] = (
        phase3.get("keyword_buckets", {}).get("secondary_queries")
        or phase3.get("secondary_keywords")
        or []
    )
    if isinstance(phase3.get("topic_signature"), dict):
        phase3["topic_signature"] = {
            **phase3["topic_signature"],
            "semantic_entities": phase3.get("keyword_buckets", {}).get("semantic_entities") or [],
            "support_topics_for_internal_links": (
                phase3.get("keyword_buckets", {}).get("support_topics_for_internal_links") or []
            ),
        }
    ranked_internal_link_inventory = _rank_internal_link_inventory(
        provided_internal_link_inventory,
        topic=phase3.get("final_article_topic", ""),
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=phase3.get("secondary_keywords") or [],
        publishing_site_url=publishing_site_url,
        backlink_url=backlink_url,
        max_items=internal_link_candidates_max,
        topic_signature=phase3.get("topic_signature"),
    )
    if ranked_internal_link_inventory:
        internal_link_candidates = _normalize_internal_link_candidates(
            [str(item.get("url") or "").strip() for item in ranked_internal_link_inventory],
            publishing_site_url=publishing_site_url,
            backlink_url=backlink_url,
            max_items=internal_link_candidates_max,
        )
        internal_link_source = "inventory"
        internal_link_anchor_map = {
            _normalize_url(str(item.get("url") or "").strip()): _build_internal_anchor_variants(item)
            for item in ranked_internal_link_inventory
            if str(item.get("url") or "").strip()
        }
        internal_links_prompt_entries = [
            f"{(str(item.get('title') or '').strip() or _internal_anchor_text(str(item.get('url') or '')))} -> {str(item.get('url') or '').strip()}"
            for item in ranked_internal_link_inventory
            if str(item.get("url") or "").strip()
        ]
    else:
        internal_link_candidates = []
        internal_link_source = "none"
        internal_link_anchor_map = {}
        internal_links_prompt_entries = []
    effective_internal_min = min(internal_link_min, len(internal_link_candidates))
    effective_internal_max = min(internal_link_max, len(internal_link_candidates))
    if effective_internal_max < effective_internal_min:
        effective_internal_max = effective_internal_min
    debug["keyword_selection"] = {
        "primary_keyword": phase3["primary_keyword"],
        "secondary_keywords": phase3["secondary_keywords"],
        "keyword_buckets": phase3.get("keyword_buckets") or {},
        "keyword_provenance": phase3.get("keyword_provenance") or {},
        "trend_candidates": keyword_selection.get("trend_candidates") or [],
        "faq_candidates": keyword_selection.get("faq_candidates") or [],
        "query_variants": keyword_discovery.get("query_variants") or [],
        "trend_cache_events": keyword_discovery.get("trend_cache_events") or [],
        "structured_content_mode": phase3.get("structured_content_mode", "none"),
        "title_package": title_package,
        "pair_fit": pair_fit,
        "content_brief": phase3.get("content_brief") or {},
        "topic_signature": phase3.get("topic_signature") or {},
        "intent_type": phase3.get("search_intent_type", ""),
        "article_angle": phase3.get("article_angle", ""),
        "topic_class": phase3.get("topic_class", ""),
        "style_profile": phase3.get("style_profile") or {},
        "specificity_profile": phase3.get("specificity_profile") or {},
    }
    debug["internal_linking"] = {
        "configured_min": internal_link_min,
        "configured_max": internal_link_max,
        "effective_min": effective_internal_min,
        "effective_max": effective_internal_max,
        "candidate_count": len(internal_link_candidates),
        "candidate_source": internal_link_source,
        "candidates": internal_link_candidates[:8],
        "inventory_matches": ranked_internal_link_inventory[:5],
    }
    debug["timings_ms"]["phase3"] = int((time.time() - phase_start) * 1000)
    progress(3, PHASE_LABELS[3], 42)

    progress(4, PHASE_LABELS[4], 42)
    phase_start = time.time()
    logger.info("creator.phase4.start")
    anchor_safe = _is_anchor_safe(anchor)
    phase4 = _build_deterministic_article_plan(
        phase1=phase1,
        phase3=phase3,
        anchor=anchor or "",
        anchor_safe=anchor_safe,
    )
    plan_quality = _evaluate_plan_quality(
        title=str(phase4.get("h1") or "").strip(),
        headings=[str(item.get("h2") or "").strip() for item in (phase4.get("outline") or []) if str(item.get("h2") or "").strip()],
        primary_keyword=phase3.get("primary_keyword", ""),
        topic=phase3.get("final_article_topic", ""),
        intent_type=phase3.get("search_intent_type", ""),
        article_angle=phase3.get("article_angle", ""),
        topic_signature=phase3.get("topic_signature"),
        specificity_profile=phase3.get("specificity_profile"),
        recent_titles=safe_recent_titles,
    )
    planner_prompt_trace = (
        (debug.get("prompt_trace") or {}).get("planner")
        if isinstance(debug.get("prompt_trace"), dict)
        else None
    )
    if isinstance(planner_prompt_trace, dict):
        planner_attempts = planner_prompt_trace.setdefault("attempts", [])
        if isinstance(planner_attempts, list):
            planner_attempts.append(
                {
                    "attempt": len(planner_attempts) + 1,
                    "input_packet": {
                        "topic": phase3.get("final_article_topic", ""),
                        "primary_keyword": phase3.get("primary_keyword", ""),
                        "secondary_keywords": phase3.get("secondary_keywords") or [],
                        "keyword_buckets": phase3.get("keyword_buckets") or {},
                        "keyword_provenance": phase3.get("keyword_provenance") or {},
                        "intent_type": phase3.get("search_intent_type", ""),
                        "article_angle": phase3.get("article_angle", ""),
                        "topic_class": phase3.get("topic_class", ""),
                        "style_profile": phase3.get("style_profile") or {},
                        "specificity_profile": phase3.get("specificity_profile") or {},
                        "title_package": phase3.get("title_package") or {},
                        "content_brief": phase3.get("content_brief") or {},
                        "faq_candidates": phase3.get("faq_candidates") or [],
                        "recent_article_titles": safe_recent_titles[:8],
                        "internal_link_candidates": internal_links_prompt_entries[:8],
                    },
                    "plan": phase4,
                    "planning_quality": plan_quality,
                }
            )
    if plan_quality["errors"]:
        warnings.append("phase4_plan_regenerated")
        debug["rejection_reason"] = plan_quality["errors"]
        repaired_topic = _select_phase4_repair_topic(
            requested_topic=topic or "",
            current_topic=phase3.get("final_article_topic", ""),
            primary_keyword=phase3.get("primary_keyword", ""),
            topic_signature=phase3.get("topic_signature"),
        )
        if repaired_topic and repaired_topic != str(phase3.get("final_article_topic") or "").strip():
            phase3["final_article_topic"] = repaired_topic
            repaired_topic_signature = _build_topic_signature(
                topic=phase3.get("final_article_topic", ""),
                primary_keyword=phase3.get("primary_keyword", ""),
                secondary_keywords=phase3.get("secondary_keywords") or [],
                target_terms=[str(item).strip() for item in ((phase3.get("topic_signature") or {}).get("target_terms") or []) if str(item).strip()],
                overlap_terms=[str(item).strip() for item in ((phase3.get("content_brief") or {}).get("overlap_terms") or []) if str(item).strip()],
                trend_candidates=keyword_discovery.get("trend_candidates") or [],
                keyword_cluster=keyword_cluster,
                internal_link_inventory=provided_internal_link_inventory,
            )
            if isinstance(phase3.get("topic_signature"), dict):
                repaired_topic_signature = {
                    **repaired_topic_signature,
                    "semantic_entities": phase3.get("keyword_buckets", {}).get("semantic_entities") or [],
                    "support_topics_for_internal_links": (
                        phase3.get("keyword_buckets", {}).get("support_topics_for_internal_links") or []
                    ),
                }
            phase3["topic_signature"] = repaired_topic_signature
            phase3["faq_candidates"] = _ensure_faq_candidates(
                phase3.get("final_article_topic", ""),
                phase3.get("faq_candidates") or [],
                topic_signature=phase3.get("topic_signature"),
                brand_name=str(phase3.get("target_brand_name") or "").strip(),
            )
            phase3["search_intent_type"] = _infer_search_intent_type(
                topic=phase3.get("final_article_topic", ""),
                target_profile=target_profile,
            )
            phase3["structured_content_mode"] = _structured_content_mode(
                phase3.get("final_article_topic", ""),
                phase3.get("primary_keyword", ""),
                phase3.get("search_intent_type", ""),
            )
            phase3["article_angle"] = _infer_article_angle(
                topic=phase3.get("final_article_topic", ""),
                intent_type=phase3.get("search_intent_type", ""),
                structured_mode=phase3.get("structured_content_mode", "none"),
                topic_class=phase3.get("topic_class", "general"),
                topic_signature=phase3.get("topic_signature"),
            )
            phase3["style_profile"] = _build_style_profile(
                topic=phase3.get("final_article_topic", ""),
                topic_class=phase3.get("topic_class", "general"),
                intent_type=phase3.get("search_intent_type", ""),
                article_angle=phase3.get("article_angle", "practical_guidance"),
                content_brief=phase3.get("content_brief") or {},
                publishing_profile=publishing_profile,
                target_profile=target_profile,
            )
            phase3["specificity_profile"] = _build_specificity_profile(
                topic=phase3.get("final_article_topic", ""),
                topic_class=phase3.get("topic_class", "general"),
                intent_type=phase3.get("search_intent_type", ""),
            )
        phase3["title_package"] = _build_deterministic_title_package(
            topic=phase3.get("final_article_topic", ""),
            primary_keyword=phase3.get("primary_keyword", ""),
            secondary_keywords=phase3.get("secondary_keywords") or [],
            search_intent_type=phase3.get("search_intent_type", ""),
            structured_mode=phase3.get("structured_content_mode", "none"),
            current_year=current_year,
            article_angle=phase3.get("article_angle", "practical_guidance"),
            topic_class=phase3.get("topic_class", "general"),
            recent_titles=safe_recent_titles,
        )
        phase4 = _build_deterministic_article_plan(
            phase1=phase1,
            phase3=phase3,
            anchor=anchor or "",
            anchor_safe=anchor_safe,
        )
        plan_quality = _evaluate_plan_quality(
            title=str(phase4.get("h1") or "").strip(),
            headings=[str(item.get("h2") or "").strip() for item in (phase4.get("outline") or []) if str(item.get("h2") or "").strip()],
            primary_keyword=phase3.get("primary_keyword", ""),
            topic=phase3.get("final_article_topic", ""),
            intent_type=phase3.get("search_intent_type", ""),
            article_angle=phase3.get("article_angle", ""),
            topic_signature=phase3.get("topic_signature"),
            specificity_profile=phase3.get("specificity_profile"),
            recent_titles=safe_recent_titles,
        )
        if isinstance(planner_prompt_trace, dict):
            planner_attempts = planner_prompt_trace.setdefault("attempts", [])
            if isinstance(planner_attempts, list):
                planner_attempts.append(
                    {
                        "attempt": len(planner_attempts) + 1,
                        "input_packet": {
                            "topic": phase3.get("final_article_topic", ""),
                            "primary_keyword": phase3.get("primary_keyword", ""),
                            "secondary_keywords": phase3.get("secondary_keywords") or [],
                            "keyword_buckets": phase3.get("keyword_buckets") or {},
                            "keyword_provenance": phase3.get("keyword_provenance") or {},
                            "intent_type": phase3.get("search_intent_type", ""),
                            "article_angle": phase3.get("article_angle", ""),
                            "topic_class": phase3.get("topic_class", ""),
                            "style_profile": phase3.get("style_profile") or {},
                            "specificity_profile": phase3.get("specificity_profile") or {},
                            "title_package": phase3.get("title_package") or {},
                            "content_brief": phase3.get("content_brief") or {},
                            "faq_candidates": phase3.get("faq_candidates") or [],
                            "recent_article_titles": safe_recent_titles[:8],
                            "internal_link_candidates": internal_links_prompt_entries[:8],
                        },
                        "plan": phase4,
                        "planning_quality": plan_quality,
                    }
                )
    if plan_quality["errors"]:
        partial_output = _build_partial_creator_output(
            target_site_url=target_site_url,
            publishing_site_url=publishing_site_url,
            phase1=phase1,
            phase1_cache_meta=phase1_cache_meta,
            phase2=phase2,
            phase2_cache_meta=phase2_cache_meta,
            phase3=phase3,
            phase4=phase4,
            warnings=warnings,
            debug={
                **debug,
                "planning_quality": plan_quality,
            },
            rejection_reason=plan_quality["errors"],
        )
        raise CreatorError(
            f"Phase 4 plan invalid: {plan_quality['errors']}",
            details={"creator_output": partial_output},
        )
    faq_candidates = phase4.get("faq_questions") or []
    debug["faq_generation"] = {
        "faq_enabled": True,
        "faq_candidates": faq_candidates[:3],
        "faq_in_outline": True,
        "generation_mode": "deterministic_plan",
    }
    debug["article_plan"] = phase4
    debug["planning_quality"] = plan_quality
    debug["timings_ms"]["phase4"] = int((time.time() - phase_start) * 1000)
    progress(4, PHASE_LABELS[4], 56)

    progress(5, PHASE_LABELS[5], 56)
    phase_start = time.time()
    logger.info("creator.phase5.start")
    article_payload = None
    last_phase5_candidate: Optional[Dict[str, Any]] = None
    errors: List[str] = []
    backlink_url = phase1["backlink_url"]
    writer_feedback: List[str] = []
    writer_token_floor = _estimate_html_max_tokens(ARTICLE_MAX_WORDS, floor=2600, ceiling=3800)
    repair_prompt_trace = (
        (debug.get("prompt_trace") or {}).get("repair_attempts")
        if isinstance(debug.get("prompt_trace"), dict)
        else None
    )
    for attempt in range(1, phase5_max_attempts + 1):
        try:
            writer_max_tokens = max(
                phase5_max_tokens_attempt1 if attempt == 1 else phase5_max_tokens_retry,
                writer_token_floor,
            )
            article_payload = _generate_article_from_plan(
                article_plan=phase4,
                phase3=phase3,
                backlink_url=backlink_url,
                publishing_site_url=publishing_site_url,
                internal_link_candidates=internal_link_candidates,
                internal_link_anchor_map=internal_link_anchor_map,
                min_internal_links=effective_internal_min,
                max_internal_links=effective_internal_max,
                llm_api_key=llm_api_key,
                llm_base_url=llm_base_url,
                llm_model=writing_model,
                http_timeout=http_timeout,
                max_tokens=writer_max_tokens,
                validation_feedback=writer_feedback if writer_feedback else None,
                prompt_trace=((debug.get("prompt_trace") or {}).get("writer_attempts") if isinstance(debug.get("prompt_trace"), dict) else None),
                usage_collector=_collect_llm_usage,
            )
        except LLMError as exc:
            if strict_failure_mode:
                partial_output = _build_partial_creator_output(
                    target_site_url=target_site_url,
                    publishing_site_url=publishing_site_url,
                    phase1=phase1,
                    phase1_cache_meta=phase1_cache_meta,
                    phase2=phase2,
                    phase2_cache_meta=phase2_cache_meta,
                    phase3=phase3,
                    phase4=phase4,
                    phase5=last_phase5_candidate,
                    warnings=warnings,
                    debug={
                        **debug,
                        "writer_validation_errors": [str(exc)],
                    },
                    rejection_reason=[str(exc)],
                )
                raise CreatorError(
                    f"Phase 5 writer attempt {attempt} failed: {exc}",
                    details={"creator_output": partial_output},
                ) from exc
            errors.append(str(exc))
            continue

        phase5_candidate = _apply_deterministic_article_metadata(
            article_payload,
            phase3=phase3,
            phase4=phase4,
        )
        last_phase5_candidate = dict(phase5_candidate)
        wc = word_count_from_html(phase5_candidate["article_html"])
        logger.info("creator.phase5.attempt attempt=%s mode=planned word_count=%s", attempt, wc)

        validation_errors = _collect_article_validation_errors(
            article_html=phase5_candidate["article_html"],
            meta_title=phase5_candidate.get("meta_title") or phase3["title_package"]["meta_title"],
            meta_description=phase5_candidate.get("meta_description") or "",
            slug=phase5_candidate.get("slug") or phase3["title_package"]["slug"],
            topic=phase3["final_article_topic"],
            primary_keyword=phase3.get("primary_keyword", ""),
            secondary_keywords=phase3.get("secondary_keywords") or [],
            required_h1=phase4["h1"],
            structured_mode=phase3.get("structured_content_mode", "none"),
            backlink_url=backlink_url,
            backlink_placement=phase4["backlink_placement"],
            publishing_site_url=publishing_site_url,
            min_internal_links=effective_internal_min,
            max_internal_links=effective_internal_max,
            content_brief=phase3.get("content_brief") or {},
            intent_type=phase3.get("search_intent_type", ""),
            article_angle=phase3.get("article_angle", ""),
            topic_signature=phase3.get("topic_signature"),
            specificity_profile=phase3.get("specificity_profile"),
        )

        if validation_errors:
            repaired_html = phase5_candidate["article_html"]
            if any(_is_keyword_context_repairable_error(error) for error in validation_errors):
                repaired_html = _repair_keyword_context_gaps(
                    article_html=repaired_html,
                    errors=validation_errors,
                    topic=phase3["final_article_topic"],
                    primary_keyword=phase3.get("primary_keyword", ""),
                    content_brief=phase3.get("content_brief") or {},
                )
                repaired_html = _repair_link_constraints(
                    article_html=repaired_html,
                    backlink_url=backlink_url,
                    publishing_site_url=publishing_site_url,
                    internal_links=internal_link_candidates,
                    internal_link_anchor_map=internal_link_anchor_map,
                    min_internal_links=effective_internal_min,
                    max_internal_links=effective_internal_max,
                    backlink_placement=phase4["backlink_placement"],
                    anchor_text=phase4["anchor_text_final"],
                    required_h1=phase4["h1"],
                    primary_keyword=phase3.get("primary_keyword", ""),
                )
                repaired_html = _normalize_faq_section_questions(repaired_html)
                repaired_html = _strip_empty_blocks(repaired_html)
                repaired_html = _strip_leading_empty_blocks(repaired_html)
                repaired_html = _trim_article_to_word_limit(repaired_html, ARTICLE_MAX_WORDS)
                if repaired_html != phase5_candidate["article_html"]:
                    phase5_candidate = {
                        **phase5_candidate,
                        "article_html": repaired_html,
                        "excerpt": _extract_first_paragraph_text(repaired_html)[:200] or phase5_candidate.get("excerpt") or "",
                    }
                    last_phase5_candidate = dict(phase5_candidate)
                    validation_errors = _collect_article_validation_errors(
                        article_html=phase5_candidate["article_html"],
                        meta_title=phase5_candidate.get("meta_title") or phase3["title_package"]["meta_title"],
                        meta_description=phase5_candidate.get("meta_description") or "",
                        slug=phase5_candidate.get("slug") or phase3["title_package"]["slug"],
                        topic=phase3["final_article_topic"],
                        primary_keyword=phase3.get("primary_keyword", ""),
                        secondary_keywords=phase3.get("secondary_keywords") or [],
                        required_h1=phase4["h1"],
                        structured_mode=phase3.get("structured_content_mode", "none"),
                        backlink_url=backlink_url,
                        backlink_placement=phase4["backlink_placement"],
                        publishing_site_url=publishing_site_url,
                        min_internal_links=effective_internal_min,
                        max_internal_links=effective_internal_max,
                        content_brief=phase3.get("content_brief") or {},
                        intent_type=phase3.get("search_intent_type", ""),
                        article_angle=phase3.get("article_angle", ""),
                        topic_signature=phase3.get("topic_signature"),
                        specificity_profile=phase3.get("specificity_profile"),
                    )
            repairable_errors = _dedupe_string_values(
                [error for error in validation_errors if _is_editorial_llm_repairable_error(error)]
            )
            if validation_errors and repairable_errors and phase7_repair_attempts > 0:
                latest_errors = list(validation_errors)
                repaired_success_payload: Optional[Dict[str, Any]] = None
                for repair_attempt in range(1, phase7_repair_attempts + 1):
                    try:
                        repaired_payload = _repair_article_with_llm(
                            article_html=phase5_candidate["article_html"],
                            article_plan=phase4,
                            phase3=phase3,
                            repair_errors=repairable_errors,
                            backlink_url=backlink_url,
                            publishing_site_url=publishing_site_url,
                            internal_link_candidates=internal_link_candidates,
                            internal_link_anchor_map=internal_link_anchor_map,
                            min_internal_links=effective_internal_min,
                            max_internal_links=effective_internal_max,
                            llm_api_key=llm_api_key,
                            llm_base_url=llm_base_url,
                            llm_model=writing_model,
                            http_timeout=http_timeout,
                            max_tokens=phase7_repair_max_tokens,
                            attempt=repair_attempt,
                            prompt_trace=repair_prompt_trace,
                            usage_collector=_collect_llm_usage,
                        )
                    except LLMError as exc:
                        latest_errors = _dedupe_string_values(latest_errors + [f"repair_call_failed:{exc}"])
                        continue
                    repaired_candidate = _apply_deterministic_article_metadata(
                        repaired_payload,
                        phase3=phase3,
                        phase4=phase4,
                    )
                    latest_errors = _collect_article_validation_errors(
                        article_html=repaired_candidate["article_html"],
                        meta_title=repaired_candidate.get("meta_title") or phase3["title_package"]["meta_title"],
                        meta_description=repaired_candidate.get("meta_description") or "",
                        slug=repaired_candidate.get("slug") or phase3["title_package"]["slug"],
                        topic=phase3["final_article_topic"],
                        primary_keyword=phase3.get("primary_keyword", ""),
                        secondary_keywords=phase3.get("secondary_keywords") or [],
                        required_h1=phase4["h1"],
                        structured_mode=phase3.get("structured_content_mode", "none"),
                        backlink_url=backlink_url,
                        backlink_placement=phase4["backlink_placement"],
                        publishing_site_url=publishing_site_url,
                        min_internal_links=effective_internal_min,
                        max_internal_links=effective_internal_max,
                        content_brief=phase3.get("content_brief") or {},
                        intent_type=phase3.get("search_intent_type", ""),
                        article_angle=phase3.get("article_angle", ""),
                        topic_signature=phase3.get("topic_signature"),
                        specificity_profile=phase3.get("specificity_profile"),
                    )
                    if not latest_errors:
                        repaired_success_payload = repaired_candidate
                        break
                    repairable_errors = _dedupe_string_values(
                        [error for error in latest_errors if _is_editorial_llm_repairable_error(error)]
                    )
                if repaired_success_payload is not None:
                    article_payload = repaired_success_payload
                    break
                validation_errors = latest_errors
            if not validation_errors:
                article_payload = phase5_candidate
                break
            if strict_failure_mode:
                partial_output = _build_partial_creator_output(
                    target_site_url=target_site_url,
                    publishing_site_url=publishing_site_url,
                    phase1=phase1,
                    phase1_cache_meta=phase1_cache_meta,
                    phase2=phase2,
                    phase2_cache_meta=phase2_cache_meta,
                    phase3=phase3,
                    phase4=phase4,
                    phase5=phase5_candidate,
                    warnings=warnings,
                    debug={
                        **debug,
                        "writer_validation_errors": validation_errors,
                    },
                    rejection_reason=validation_errors,
                )
                raise CreatorError(
                    f"Phase 5 writer attempt {attempt} validation failed: {validation_errors}",
                    details={"creator_output": partial_output},
                )
            errors.extend(validation_errors)
            writer_feedback = validation_errors
            article_payload = None
            continue

        article_payload = phase5_candidate
        break

    if not article_payload:
        phase5_errors = _dedupe_string_values(errors)
        partial_output = _build_partial_creator_output(
            target_site_url=target_site_url,
            publishing_site_url=publishing_site_url,
            phase1=phase1,
            phase1_cache_meta=phase1_cache_meta,
            phase2=phase2,
            phase2_cache_meta=phase2_cache_meta,
            phase3=phase3,
            phase4=phase4,
            phase5=last_phase5_candidate,
            warnings=warnings,
            debug={
                **debug,
                "writer_validation_errors": phase5_errors,
            },
            rejection_reason=phase5_errors,
        )
        raise CreatorError(
            f"Phase 5 writer failed: {phase5_errors}",
            details={"creator_output": partial_output},
        )

    art_html = (article_payload.get("article_html") or "").strip()
    art_html = _strip_empty_blocks(art_html)
    art_html = _strip_leading_empty_blocks(art_html)
    article_payload["article_html"] = art_html
    article_payload = _apply_deterministic_article_metadata(
        article_payload,
        phase3=phase3,
        phase4=phase4,
    )
    phase5 = article_payload
    debug["faq_enrichment"] = {"enabled": False, "applied": False, "generation_mode": "writer_inline"}
    debug["timings_ms"]["phase5"] = int((time.time() - phase_start) * 1000)
    progress(5, PHASE_LABELS[5], 70)

    progress(6, PHASE_LABELS[6], 70)
    phase_start = time.time()
    logger.info("creator.phase6.start")
    phase6 = {
        "image_model": "Leonardo Flux Schnell",
        "featured_image": {},
        "in_content_image": {},
    }

    image_prompts = _build_deterministic_image_prompts(phase3["final_article_topic"])

    featured_prompt = (image_prompts.get("featured_prompt") or "").strip()
    featured_alt = (image_prompts.get("featured_alt") or "").strip()
    include_in_content = bool(image_prompts.get("include_in_content"))
    in_content_prompt = (image_prompts.get("in_content_prompt") or "").strip()
    in_content_alt = (image_prompts.get("in_content_alt") or "").strip()

    if not featured_prompt:
        featured_prompt = f"Editorial photo illustrating: {phase3['final_article_topic']}"
    if not featured_alt:
        featured_alt = phase3["final_article_topic"]

    leonardo_api_key = os.getenv("LEONARDO_API_KEY", "").strip()
    leonardo_base_url = os.getenv("LEONARDO_BASE_URL", DEFAULT_LEONARDO_BASE_URL).strip()
    width = _read_int_env("CREATOR_IMAGE_WIDTH", DEFAULT_IMAGE_WIDTH)
    height = _read_int_env("CREATOR_IMAGE_HEIGHT", DEFAULT_IMAGE_HEIGHT)
    poll_timeout = _read_int_env("CREATOR_IMAGE_POLL_TIMEOUT_SECONDS", DEFAULT_POLL_TIMEOUT_SECONDS)
    poll_interval = _read_int_env("CREATOR_IMAGE_POLL_INTERVAL_SECONDS", DEFAULT_POLL_SECONDS)
    image_generation_enabled = _read_bool_env("CREATOR_IMAGE_GENERATION_ENABLED", False)
    image_required = _read_bool_env("CREATOR_IMAGE_REQUIRED", False)

    featured_image_url = ""
    in_content_image_url = ""
    if not image_generation_enabled:
        warnings.append("phase6_image_generation_disabled")
        logger.info("creator.phase6.skip reason=image_generation_disabled")
        include_in_content = False
    elif not dry_run:
        try:
            featured_image_url = _call_leonardo(
                prompt=featured_prompt,
                api_key=leonardo_api_key,
                base_url=leonardo_base_url,
                model_id=DEFAULT_LEONARDO_MODEL_ID,
                width=width,
                height=height,
                timeout_seconds=http_timeout,
                poll_timeout_seconds=poll_timeout,
                poll_interval_seconds=poll_interval,
            )
        except CreatorError as exc:
            if not execution_policy["phase6_image_soft_fail"]:
                raise CreatorError(f"Phase 6 featured image generation failed: {exc}") from exc
            warnings.append(f"phase6_featured_image_failed:{exc}")
            featured_image_url = ""
            include_in_content = False

        if include_in_content and in_content_prompt:
            try:
                in_content_image_url = _call_leonardo(
                    prompt=in_content_prompt,
                    api_key=leonardo_api_key,
                    base_url=leonardo_base_url,
                    model_id=DEFAULT_LEONARDO_MODEL_ID,
                    width=width,
                    height=height,
                    timeout_seconds=http_timeout,
                    poll_timeout_seconds=poll_timeout,
                    poll_interval_seconds=poll_interval,
                )
            except CreatorError as exc:
                if not execution_policy["phase6_image_soft_fail"]:
                    raise CreatorError(f"Phase 6 in-content image generation failed: {exc}") from exc
                warnings.append(f"phase6_in_content_image_failed:{exc}")
                in_content_image_url = ""

    if image_required and image_generation_enabled and not featured_image_url and not dry_run:
        raise CreatorError("Featured image generation failed.")

    phase6["featured_image"] = {
        "prompt": featured_prompt,
        "negative_prompt": NEGATIVE_PROMPT,
        "alt_text": featured_alt,
    }
    phase6["in_content_image"] = {
        "prompt": in_content_prompt,
        "negative_prompt": NEGATIVE_PROMPT,
        "alt_text": in_content_alt,
    }
    debug["timings_ms"]["phase6"] = int((time.time() - phase_start) * 1000)
    progress(6, PHASE_LABELS[6], 85)

    progress(7, PHASE_LABELS[7], 85)
    phase_start = time.time()
    p7_wc = word_count_from_html(phase5["article_html"])
    logger.info("creator.phase7.start word_count=%s", p7_wc)
    phase7_errors: List[str] = []
    phase5 = _apply_deterministic_article_metadata(
        phase5,
        phase3=phase3,
        phase4=phase4,
    )
    allowed_topics = [t.lower() for t in phase2.get("allowed_topics") or [] if isinstance(t, str)]
    if allowed_topics:
        topic_lower = (phase3["final_article_topic"] or "").lower()
        if not any(topic in topic_lower for topic in allowed_topics):
            warnings.append("topic_not_in_allowed_topics")
    phase7_errors = _collect_article_validation_errors(
        article_html=phase5["article_html"],
        meta_title=phase5.get("meta_title") or phase3["title_package"]["meta_title"],
        meta_description=phase5.get("meta_description") or _build_deterministic_meta_description(
            topic=phase3["final_article_topic"],
            primary_keyword=phase3["primary_keyword"],
            secondary_keywords=phase3.get("secondary_keywords") or [],
            structured_mode=phase3.get("structured_content_mode", "none"),
        ),
        slug=phase5.get("slug") or phase3["title_package"]["slug"],
        topic=phase3["final_article_topic"],
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=phase3.get("secondary_keywords") or [],
        required_h1=phase4["h1"],
        structured_mode=phase3.get("structured_content_mode", "none"),
        backlink_url=backlink_url,
        backlink_placement=phase4["backlink_placement"],
        publishing_site_url=publishing_site_url,
        min_internal_links=effective_internal_min,
        max_internal_links=effective_internal_max,
        content_brief=phase3.get("content_brief") or {},
        intent_type=phase3.get("search_intent_type", ""),
        article_angle=phase3.get("article_angle", ""),
        topic_signature=phase3.get("topic_signature"),
        specificity_profile=phase3.get("specificity_profile"),
    )

    if phase7_errors:
        current_wc = word_count_from_html(phase5["article_html"])
        logger.info("creator.phase7.issues errors=%s word_count=%s", phase7_errors, current_wc)
        phase7_errors = _dedupe_string_values(phase7_errors)

    if phase7_errors:
        raise CreatorError(f"Final SEO checks failed: {phase7_errors}")

    seo_evaluation = _score_seo_output(
        article_html=phase5["article_html"],
        meta_title=phase5.get("meta_title") or "",
        meta_description=phase5.get("meta_description") or "",
        slug=phase5.get("slug") or "",
        primary_keyword=phase3.get("primary_keyword", ""),
        secondary_keywords=phase3.get("secondary_keywords") or [],
        required_h1=phase4["h1"],
        structured_mode=phase3.get("structured_content_mode", "none"),
        backlink_url=backlink_url,
        publishing_site_url=publishing_site_url,
        min_internal_links=effective_internal_min,
        max_internal_links=effective_internal_max,
        topic=phase3["final_article_topic"],
        content_brief=phase3.get("content_brief") or {},
        intent_type=phase3.get("search_intent_type", ""),
        article_angle=phase3.get("article_angle", ""),
        topic_signature=phase3.get("topic_signature"),
        specificity_profile=phase3.get("specificity_profile"),
    )
    debug["seo_evaluation"] = seo_evaluation
    debug["quality_scores"] = {
        "title_quality_score": seo_evaluation.get("title_quality_score", 0),
        "heading_quality_score": seo_evaluation.get("heading_quality_score", 0),
        "intent_type": seo_evaluation.get("intent_type") or phase3.get("search_intent_type", ""),
        "backlink_naturalness_score": seo_evaluation.get("backlink_naturalness_score", 0),
        "specificity_score": seo_evaluation.get("specificity_score", 0),
        "spam_risk_score": seo_evaluation.get("spam_risk_score", 0),
        "coherence_score": seo_evaluation.get("coherence_score", 0),
        "rejection_reason": debug.get("rejection_reason") or [],
    }

    debug["timings_ms"]["phase7"] = int((time.time() - phase_start) * 1000)
    progress(7, PHASE_LABELS[7], 100)

    images: List[Dict[str, str]] = []
    if featured_image_url:
        images.append({"type": "featured", "id_or_url": featured_image_url})
    if in_content_image_url:
        images.append({"type": "in_content", "id_or_url": in_content_image_url})

    debug["token_phase_ranking"] = sorted(
        [{"phase": phase_name, **tokens_by_phase.get(phase_name, _new_token_bucket())} for phase_name in phase_names],
        key=lambda item: (
            -item["total_tokens"],
            -item["prompt_tokens"],
            -item["completion_tokens"],
            -item["calls"],
        ),
    )
    debug["llm_tokens_total"] = {
        "calls": sum(bucket.get("calls", 0) for bucket in tokens_by_phase.values()),
        "prompt_tokens": sum(bucket.get("prompt_tokens", 0) for bucket in tokens_by_phase.values()),
        "completion_tokens": sum(bucket.get("completion_tokens", 0) for bucket in tokens_by_phase.values()),
        "total_tokens": sum(bucket.get("total_tokens", 0) for bucket in tokens_by_phase.values()),
        "cache_creation_input_tokens": sum(bucket.get("cache_creation_input_tokens", 0) for bucket in tokens_by_phase.values()),
        "cache_read_input_tokens": sum(bucket.get("cache_read_input_tokens", 0) for bucket in tokens_by_phase.values()),
    }

    return ensure_prompt_trace_in_creator_output(
        {
        "ok": True,
        "target_site_url": target_site_url,
        "host_site_url": publishing_site_url,
        "phase1": phase1,
        "phase1_cache_meta": phase1_cache_meta,
        "phase2": phase2,
        "phase2_cache_meta": phase2_cache_meta,
        "phase3": phase3,
        "phase4": phase4,
        "phase5": phase5,
        "phase6": phase6,
        "seo_evaluation": seo_evaluation,
        "title_quality_score": seo_evaluation.get("title_quality_score", 0),
        "heading_quality_score": seo_evaluation.get("heading_quality_score", 0),
        "intent_type": seo_evaluation.get("intent_type") or phase3.get("search_intent_type", ""),
        "backlink_naturalness_score": seo_evaluation.get("backlink_naturalness_score", 0),
        "specificity_score": seo_evaluation.get("specificity_score", 0),
        "spam_risk_score": seo_evaluation.get("spam_risk_score", 0),
        "coherence_score": seo_evaluation.get("coherence_score", 0),
        "rejection_reason": debug.get("rejection_reason") or [],
        "images": images,
        "warnings": warnings,
        "debug": debug,
        }
    )
