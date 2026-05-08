"""Objective evaluation harness for generated articles.

Scores any article against a fixed rubric grounded in published SEO guidelines:
deterministic measurements (word count, keyword density, schema, anchor
diversity, AI-tell density, German readability) plus LLM-judged measurements
that require competitor data (entity coverage, intent match, PAA coverage,
backlink anchor naturalness, E-E-A-T signal density).

LLM-judged scores are stubbed until the research layer is in place; the
deterministic scores are usable today.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Dict, List, Optional

from .contract import ArticleFormat, ContentContract
from .entity_extract import ExtractedEntity
from .eval_judge import JudgeScores
from .research import ResearchPayload


# ---- Deterministic thresholds -----------------------------------------------

KEYWORD_DENSITY_MIN = 0.005  # 0.5%
KEYWORD_DENSITY_MAX = 0.015  # 1.5%
KEYWORD_DENSITY_MAX_LISTICLE = 0.040  # 4.0% — listicles repeat item names; honest density runs higher.
WORD_COUNT_TOLERANCE = 0.20
META_TITLE_MIN = 50
META_TITLE_MAX = 60
META_DESCRIPTION_MIN = 140
META_DESCRIPTION_MAX = 160
INTERNAL_LINKS_MIN = 2
INTERNAL_LINKS_MAX = 5
EXTERNAL_LINKS_MIN = 1
EXTERNAL_LINKS_MAX = 3
ANCHOR_DIVERSITY_MIN = 0.6
WIENER_GRADE_MIN = 8
WIENER_GRADE_MAX = 12
FRENCH_GRADE_MIN = 6   # Kandel-Moles equivalent: easy-but-not-childish
FRENCH_GRADE_MAX = 14  # ceiling: anything above this is too dense for SEO body
LANGUAGE_CONSISTENCY_FOREIGN_TOKEN_MAX_RATIO = 0.005  # 0.5% of content tokens
ENTITY_COVERAGE_MIN = 0.70
PAA_QUESTION_TERM_MATCH_MIN = 0.6  # fraction of a question's content words that must appear in the article
PAA_COVERAGE_MIN = 0.80
BACKLINK_ANCHOR_NATURALNESS_MIN = 7
EEAT_DENSITY_MIN = 6


# Minimal stopword sets used for PAA-coverage scoring and language consistency.
# Not exhaustive linguistically — only common closed-class words that
# contribute no topical signal when matching question terms against article body.
_GERMAN_STOPWORDS = frozenset({
    "aber", "alle", "als", "am", "an", "auch", "auf", "aus",
    "bei", "bin", "bis", "bist",
    "da", "dann", "das", "dass", "dein", "dem", "den", "der", "des", "die", "diese", "dieser", "doch", "dort",
    "ein", "eine", "einem", "einen", "einer", "eines", "er", "es", "etwas",
    "fuer", "für",
    "ganz", "gar",
    "hat", "hatte", "hatten", "haben",
    "ich", "ihm", "ihr", "ihre", "im", "in", "ist",
    "ja", "jetzt",
    "kann", "kein", "keine", "können",
    "macht",
    "man", "mehr", "mein", "meine", "mit", "muss", "müssen",
    "nicht", "noch", "nur",
    "ob", "oder", "ohne",
    "schon", "sehr", "sein", "seine", "sich", "sie", "sind", "soll", "sollen", "sondern",
    "über",
    "um", "und", "uns", "unser", "unsere", "unter",
    "viel", "viele", "vom", "von", "vor",
    "war", "waren", "warum", "was", "wenn", "wer", "werden", "wie", "wir", "wird", "wo", "wofür", "woher", "wohin",
    "zu", "zum", "zur",
})

_FRENCH_STOPWORDS = frozenset({
    "à", "afin", "ainsi", "alors", "après", "au", "aucun", "aussi", "autre", "aux", "avant", "avec", "avoir",
    "bien",
    "ça", "car", "ce", "ceci", "cela", "celle", "celui", "ces", "cet", "cette", "ceux", "chez", "comme", "contre",
    "dans", "de", "des", "du", "donc", "dont",
    "elle", "elles", "en", "encore", "entre", "est", "et", "été", "être", "eux",
    "faire", "fait",
    "il", "ils",
    "je", "juste",
    "la", "le", "les", "leur", "leurs", "lui",
    "ma", "mais", "même", "mes", "moi", "mon",
    "ne", "ni", "non", "nos", "notre", "nous",
    "on", "ont", "ou", "où",
    "par", "parce", "pas", "peu", "plus", "pour", "puis",
    "quand", "que", "quel", "quelle", "qui", "quoi",
    "sa", "sans", "se", "ses", "si", "sien", "son", "sont", "sous", "sur",
    "ta", "tandis", "tant", "te", "tes", "toi", "ton", "tous", "tout", "très", "tu",
    "un", "une",
    "voici", "voilà", "vos", "votre", "vous",
    "y",
})

# High-signal stopwords used for cross-language consistency detection. We pick
# closed-class words that ONE language has and the other DOES NOT — this gives
# a low false-positive rate against names of laws / brands / numbers that
# might legitimately appear cross-lingually.
_GERMAN_DETECTION_TOKENS = frozenset({
    "der", "die", "das", "und", "ist", "nicht", "ein", "eine", "auch",
    "sich", "auf", "mit", "für", "von", "bei", "wir", "sie", "den", "dem",
})

_FRENCH_DETECTION_TOKENS = frozenset({
    "le", "la", "les", "des", "qui", "que", "pour", "avec", "dans",
    "vous", "nous", "est", "sont", "cette", "leur", "plus", "ainsi",
})

_LANGUAGE_STOPWORDS = {
    "de": _GERMAN_STOPWORDS,
    "fr": _FRENCH_STOPWORDS,
}


def _content_words(text: str, language: str = "de") -> List[str]:
    stopwords = _LANGUAGE_STOPWORDS.get(language, _GERMAN_STOPWORDS)
    tokens = re.findall(r"\b[\wäöüÄÖÜßàâçéèêëîïôûùüÿœæ]+\b", (text or "").lower())
    return [t for t in tokens if t not in stopwords and len(t) > 2]


@dataclass
class CheckResult:
    name: str
    passed: bool
    value: Optional[float] = None
    detail: str = ""


@dataclass
class QualityReport:
    contract_target_keyword: str
    deterministic: List[CheckResult] = field(default_factory=list)
    llm_judged: List[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.deterministic + self.llm_judged)

    def to_dict(self) -> Dict[str, object]:
        return {
            "target_keyword": self.contract_target_keyword,
            "passed": self.passed,
            "deterministic": [r.__dict__ for r in self.deterministic],
            "llm_judged": [r.__dict__ for r in self.llm_judged],
        }


# ---- HTML helpers -----------------------------------------------------------

class _ArticleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.h1: List[str] = []
        self.h2: List[str] = []
        self.h3: List[str] = []
        self.h4: List[str] = []
        self.text_buffer: List[str] = []
        self.links: List[Dict[str, str]] = []
        self.images: List[Dict[str, str]] = []
        self._in_anchor: Optional[str] = None
        self._anchor_text: List[str] = []
        self._heading_buffer: Optional[str] = None
        self._heading_text: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple]) -> None:
        attr_dict = dict(attrs)
        if tag in ("h1", "h2", "h3", "h4"):
            self._heading_buffer = tag
            self._heading_text = []
        if tag == "a":
            self._in_anchor = attr_dict.get("href", "")
            self._anchor_text = []
        if tag == "img":
            self.images.append({"src": attr_dict.get("src", ""), "alt": attr_dict.get("alt", "")})

    def handle_endtag(self, tag: str) -> None:
        if tag in ("h1", "h2", "h3", "h4") and self._heading_buffer == tag:
            heading_text = "".join(self._heading_text).strip()
            getattr(self, tag).append(heading_text)
            self._heading_buffer = None
            self._heading_text = []
        if tag == "a" and self._in_anchor is not None:
            self.links.append({"href": self._in_anchor, "anchor": "".join(self._anchor_text).strip()})
            self._in_anchor = None
            self._anchor_text = []

    def handle_data(self, data: str) -> None:
        if self._heading_buffer is not None:
            self._heading_text.append(data)
        if self._in_anchor is not None:
            self._anchor_text.append(data)
        self.text_buffer.append(data)

    @property
    def plain_text(self) -> str:
        return re.sub(r"\s+", " ", "".join(self.text_buffer)).strip()


def _parse(html: str) -> _ArticleParser:
    parser = _ArticleParser()
    parser.feed(html)
    return parser


def _word_count(text: str) -> int:
    return len([w for w in re.split(r"\s+", text) if w.strip()])


def _wiener_grade(text: str) -> float:
    """Wiener Sachtextformel (German readability). Lower = easier."""

    sentences = max(1, len(re.findall(r"[.!?]+", text)))
    words = re.findall(r"\b[\wäöüÄÖÜß]+\b", text)
    word_count = max(1, len(words))
    long_words = sum(1 for w in words if len(w) > 6)
    polysyllabic = sum(1 for w in words if len(re.findall(r"[aeiouäöü]", w.lower())) >= 3)
    monosyllabic = sum(1 for w in words if len(re.findall(r"[aeiouäöü]", w.lower())) == 1)
    ms = polysyllabic / word_count * 100
    sl = word_count / sentences
    iw = long_words / word_count * 100
    es = monosyllabic / word_count * 100
    return 0.1935 * ms + 0.1672 * sl + 0.1297 * iw - 0.0327 * es - 0.875


def _kandel_moles_grade(text: str) -> float:
    """Kandel-Moles formula (French readability, adapted Flesch). Lower = easier.

    Returns a grade-level estimate analogous to Wiener for German: ~6 = très
    facile, ~10 = standard, ~14+ = technique. Bands picked to match the
    German FRENCH_GRADE_MIN/MAX defaults below.
    """

    sentences = max(1, len(re.findall(r"[.!?]+", text)))
    words = re.findall(r"\b[\wàâçéèêëîïôûùüÿœæ]+\b", text, flags=re.IGNORECASE)
    word_count = max(1, len(words))
    syllables_total = 0
    for word in words:
        groups = re.findall(r"[aeiouyàâéèêëîïôûùüÿœæ]+", word.lower())
        syllables_total += max(1, len(groups))
    asl = word_count / sentences          # average sentence length
    asw = syllables_total / word_count    # average syllables per word
    # Kandel-Moles (1958): 209 - (1.015 * ASL) - (73.6 * ASW). Higher = easier.
    flesch_like = 209 - (1.015 * asl) - (73.6 * asw)
    # Translate to a grade band where lower = easier, comparable to Wiener.
    # 90+ flesch ~ grade 6, 60 ~ grade 9, 30 ~ grade 13.
    return max(0.0, (100 - flesch_like) / 10)


# ---- Deterministic checks ---------------------------------------------------

def check_keyword_in_h1(text_h1: str, keyword: str) -> CheckResult:
    passed = keyword.lower() in (text_h1 or "").lower()
    return CheckResult("keyword_in_h1", passed, detail=f"h1='{text_h1}' keyword='{keyword}'")


def check_keyword_in_first_100_words(plain_text: str, keyword: str) -> CheckResult:
    first = " ".join(plain_text.split()[:100]).lower()
    passed = keyword.lower() in first
    return CheckResult("keyword_in_first_100_words", passed)


def check_keyword_density(plain_text: str, keyword: str, *, density_max: float = KEYWORD_DENSITY_MAX) -> CheckResult:
    words = _word_count(plain_text)
    if words == 0:
        return CheckResult("keyword_density", False, detail="empty article")
    pattern = re.compile(re.escape(keyword), re.IGNORECASE)
    occurrences = len(pattern.findall(plain_text))
    density = occurrences / words
    passed = KEYWORD_DENSITY_MIN <= density <= density_max
    return CheckResult("keyword_density", passed, value=density)


def check_secondary_keyword_coverage(plain_text: str, secondaries: List[str]) -> CheckResult:
    if not secondaries:
        return CheckResult("secondary_keyword_coverage", True, value=1.0)
    lowered = plain_text.lower()
    present = sum(1 for k in secondaries if k.lower() in lowered)
    coverage = present / len(secondaries)
    return CheckResult("secondary_keyword_coverage", coverage >= 0.8, value=coverage)


def check_word_count_band(plain_text: str, target: int, competitor_median: Optional[int] = None) -> CheckResult:
    actual = _word_count(plain_text)
    reference = competitor_median or target
    lower = int(reference * (1 - WORD_COUNT_TOLERANCE))
    upper = int(reference * (1 + WORD_COUNT_TOLERANCE))
    return CheckResult("word_count_band", lower <= actual <= upper, value=float(actual))


def check_heading_hierarchy(parser: _ArticleParser) -> CheckResult:
    if len(parser.h1) != 1:
        return CheckResult("heading_hierarchy", False, detail=f"expected 1 h1, got {len(parser.h1)}")
    if parser.h4 and not parser.h3:
        return CheckResult("heading_hierarchy", False, detail="h4 used without h3")
    return CheckResult("heading_hierarchy", True)


def check_link_counts(parser: _ArticleParser, host_domain: str) -> CheckResult:
    if not host_domain:
        # Late-binding mode: publishing site not chosen yet, so we can't tell
        # internal from external. Just verify the link is non-zero so a
        # totally link-less article still fails the check.
        total = len(parser.links)
        return CheckResult(
            "link_counts",
            total > 0,
            detail=f"late-bind: total={total} (host unknown)",
        )
    internal = sum(1 for link in parser.links if host_domain.lower() in (link["href"] or "").lower())
    external = len(parser.links) - internal
    internal_ok = INTERNAL_LINKS_MIN <= internal <= INTERNAL_LINKS_MAX
    external_ok = EXTERNAL_LINKS_MIN <= external <= EXTERNAL_LINKS_MAX
    return CheckResult(
        "link_counts",
        internal_ok and external_ok,
        detail=f"internal={internal} external={external}",
    )


def check_anchor_diversity(parser: _ArticleParser) -> CheckResult:
    anchors = [link["anchor"] for link in parser.links if link["anchor"]]
    if not anchors:
        return CheckResult("anchor_diversity", False, detail="no anchors")
    diversity = len(set(a.lower() for a in anchors)) / len(anchors)
    return CheckResult("anchor_diversity", diversity >= ANCHOR_DIVERSITY_MIN, value=diversity)


def check_image_alt_text(parser: _ArticleParser) -> CheckResult:
    if not parser.images:
        return CheckResult("image_alt_text", True, value=1.0)
    with_alt = sum(1 for img in parser.images if img.get("alt", "").strip())
    coverage = with_alt / len(parser.images)
    return CheckResult("image_alt_text", coverage == 1.0, value=coverage)


def check_meta_lengths(meta_title: str, meta_description: str) -> CheckResult:
    title_ok = META_TITLE_MIN <= len(meta_title) <= META_TITLE_MAX
    desc_ok = META_DESCRIPTION_MIN <= len(meta_description) <= META_DESCRIPTION_MAX
    return CheckResult(
        "meta_lengths",
        title_ok and desc_ok,
        detail=f"title={len(meta_title)} desc={len(meta_description)}",
    )


def check_ai_tell_blocklist(plain_text: str, blocklist: List[str]) -> CheckResult:
    if not blocklist:
        return CheckResult("ai_tell_blocklist", True)
    lowered = plain_text.lower()
    hits = [phrase for phrase in blocklist if phrase.lower() in lowered]
    return CheckResult(
        "ai_tell_blocklist",
        not hits,
        value=float(len(hits)),
        detail=", ".join(hits) if hits else "",
    )


def check_german_readability(plain_text: str) -> CheckResult:
    grade = _wiener_grade(plain_text)
    return CheckResult(
        "german_readability_wiener",
        WIENER_GRADE_MIN <= grade <= WIENER_GRADE_MAX,
        value=grade,
    )


def check_french_readability(plain_text: str) -> CheckResult:
    grade = _kandel_moles_grade(plain_text)
    return CheckResult(
        "french_readability_kandel_moles",
        FRENCH_GRADE_MIN <= grade <= FRENCH_GRADE_MAX,
        value=grade,
    )


def check_language_consistency(plain_text: str, *, language: str) -> CheckResult:
    """Catch silent prompt-mismatch regressions.

    Counts how many times the *other* supported language's high-signal stop
    words appear in the article. A real article should have effectively zero
    foreign stop-word density (even brand names and laws don't usually
    overlap with closed-class function words). If the density exceeds
    ``LANGUAGE_CONSISTENCY_FOREIGN_TOKEN_MAX_RATIO``, we likely loaded the
    wrong prompt translation.
    """

    tokens = re.findall(r"\b[\wäöüÄÖÜßàâçéèêëîïôûùüÿœæ]+\b", (plain_text or "").lower())
    if not tokens:
        return CheckResult("language_consistency", True, value=0.0, detail="empty article")
    if language == "de":
        foreign = _FRENCH_DETECTION_TOKENS
    elif language == "fr":
        foreign = _GERMAN_DETECTION_TOKENS
    else:
        return CheckResult(
            "language_consistency",
            True,
            value=0.0,
            detail=f"unsupported language {language!r}; check skipped",
        )
    foreign_hits = sum(1 for t in tokens if t in foreign)
    ratio = foreign_hits / len(tokens)
    return CheckResult(
        "language_consistency",
        ratio <= LANGUAGE_CONSISTENCY_FOREIGN_TOKEN_MAX_RATIO,
        value=ratio,
        detail=f"{foreign_hits}/{len(tokens)} tokens are stopwords of the wrong language",
    )


# ---- Research-driven deterministic checks ----------------------------------

def check_topical_entity_coverage(
    plain_text: str,
    high_coverage_entities: List[ExtractedEntity],
) -> CheckResult:
    if not high_coverage_entities:
        return CheckResult(
            "topical_entity_coverage",
            True,
            value=1.0,
            detail="no high-coverage entities to enforce",
        )
    lowered = plain_text.lower()
    present = sum(1 for entity in high_coverage_entities if entity.name.lower() in lowered)
    coverage = present / len(high_coverage_entities)
    return CheckResult(
        "topical_entity_coverage",
        coverage >= ENTITY_COVERAGE_MIN,
        value=coverage,
        detail=f"{present}/{len(high_coverage_entities)} high-coverage entities present",
    )


def check_paa_coverage(
    plain_text: str,
    paa_questions: List[str],
    *,
    language: str = "de",
) -> CheckResult:
    if not paa_questions:
        return CheckResult("paa_coverage", True, value=1.0, detail="no PAA questions")
    article_words = set(_content_words(plain_text, language))
    covered = 0
    for question in paa_questions:
        question_words = _content_words(question, language)
        if not question_words:
            continue
        matches = sum(1 for word in question_words if word in article_words)
        if matches / len(question_words) >= PAA_QUESTION_TERM_MATCH_MIN:
            covered += 1
    coverage = covered / len(paa_questions)
    return CheckResult(
        "paa_coverage",
        coverage >= PAA_COVERAGE_MIN,
        value=coverage,
        detail=f"{covered}/{len(paa_questions)} questions addressed",
    )


# ---- Listicle-structure check ----------------------------------------------


_LISTICLE_RANK_HEADING_PATTERN = re.compile(r"^\s*(\d+)\.\s*(.+)$")
_LISTICLE_VERDICT_PATTERN = re.compile(r"<p[^>]*class\s*=\s*[\"'][^\"']*\bverdict\b", re.IGNORECASE)
_LISTICLE_PROS_HEADING_PATTERN = re.compile(r"<h3[^>]*>\s*(Vorteile|Avantages)\s*</h3>", re.IGNORECASE)
_LISTICLE_CONS_HEADING_PATTERN = re.compile(r"<h3[^>]*>\s*(Nachteile|Inconv[eé]nients)\s*</h3>", re.IGNORECASE)


def check_listicle_structure(article_html: str, contract: ContentContract) -> CheckResult:
    """Verify ranked-listicle structural invariants.

    Passes only when:
    1. Number of rank-prefixed `<h2>`s equals ``contract.listicle_plan.item_count``.
    2. When ``ranking_basis="score"``, rank numerals are consecutive 1..N.
    3. Each item carries a ``<p class="verdict">`` paragraph and a pros + cons heading.

    Narrative articles bypass the check by returning a passed=True stub (the
    caller is expected to skip the call entirely; this is a defensive fallback).
    """

    plan = contract.listicle_plan
    if contract.format != ArticleFormat.LISTICLE or plan is None:
        return CheckResult("listicle_structure", True, detail="not a listicle")

    parser = _parse(article_html)
    rank_headings: List[tuple[int, str]] = []
    for heading in parser.h2:
        match = _LISTICLE_RANK_HEADING_PATTERN.match(heading or "")
        if match:
            try:
                rank_headings.append((int(match.group(1)), match.group(2).strip()))
            except (TypeError, ValueError):
                continue

    expected = plan.item_count
    if len(rank_headings) != expected:
        return CheckResult(
            "listicle_structure",
            False,
            value=float(len(rank_headings)),
            detail=f"found {len(rank_headings)} ranked items, expected {expected}",
        )

    if plan.ranking_basis == "score":
        ranks = [r for r, _ in rank_headings]
        if ranks != list(range(1, expected + 1)):
            return CheckResult(
                "listicle_structure",
                False,
                value=float(len(rank_headings)),
                detail=f"rank numerals not 1..{expected} consecutively: {ranks}",
            )

    template = {field.lower() for field in plan.item_template}
    verdict_required = "verdict" in template
    pros_required = "pros" in template
    cons_required = "cons" in template

    verdict_count = len(_LISTICLE_VERDICT_PATTERN.findall(article_html))
    pros_count = len(_LISTICLE_PROS_HEADING_PATTERN.findall(article_html))
    cons_count = len(_LISTICLE_CONS_HEADING_PATTERN.findall(article_html))

    failures: List[str] = []
    if verdict_required and verdict_count < expected:
        failures.append(f"verdict tags {verdict_count}/{expected}")
    if pros_required and pros_count < expected:
        failures.append(f"pros headings {pros_count}/{expected}")
    if cons_required and cons_count < expected:
        failures.append(f"cons headings {cons_count}/{expected}")
    if failures:
        return CheckResult(
            "listicle_structure",
            False,
            value=float(len(rank_headings)),
            detail="; ".join(failures),
        )

    return CheckResult(
        "listicle_structure",
        True,
        value=float(len(rank_headings)),
        detail=f"{expected} items, structure intact",
    )


# ---- LLM-judged checks driven by JudgeScores (Phase 3b) --------------------

def _judge_axis_to_check(name: str, axis) -> CheckResult:
    return CheckResult(
        name,
        axis.passed,
        value=float(axis.score),
        detail=axis.rationale or f"score {axis.score}/{axis.threshold} threshold",
    )


def llm_judged_checks_from_scores(scores: JudgeScores) -> List[CheckResult]:
    return [
        _judge_axis_to_check("intent_match", scores.intent_match),
        _judge_axis_to_check("backlink_anchor_naturalness", scores.backlink_anchor_naturalness),
        _judge_axis_to_check("eeat_signal_density", scores.eeat_signal_density),
    ]


def stub_intent_match() -> CheckResult:
    return CheckResult("intent_match", True, detail="STUB: pass JudgeScores to evaluate() to enable")


def stub_backlink_anchor_naturalness() -> CheckResult:
    return CheckResult("backlink_anchor_naturalness", True, detail="STUB: pass JudgeScores to evaluate() to enable")


def stub_eeat_density() -> CheckResult:
    return CheckResult("eeat_signal_density", True, detail="STUB: pass JudgeScores to evaluate() to enable")


# ---- Orchestrator -----------------------------------------------------------

def evaluate(
    *,
    article_html: str,
    contract: ContentContract,
    host_domain: str,
    meta_title: str,
    meta_description: str,
    research: Optional[ResearchPayload] = None,
    judge_scores: Optional[JudgeScores] = None,
    language: Optional[str] = None,
) -> QualityReport:
    parser = _parse(article_html)
    plain = parser.plain_text
    primary = contract.target_keyword
    h1 = parser.h1[0] if parser.h1 else ""

    resolved_language = (language or contract.language.value).lower()

    competitor_word_count_median = research.competitor_word_count_median if research else None
    high_coverage_entities = research.high_coverage_entities if research else []
    paa_questions = research.paa_questions if research else []

    if resolved_language == "fr":
        readability_check = check_french_readability(plain)
    else:
        readability_check = check_german_readability(plain)

    is_listicle = contract.format == ArticleFormat.LISTICLE
    density_cap = KEYWORD_DENSITY_MAX_LISTICLE if is_listicle else KEYWORD_DENSITY_MAX

    deterministic = [
        check_keyword_in_h1(h1, primary),
        check_keyword_in_first_100_words(plain, primary),
        check_keyword_density(plain, primary, density_max=density_cap),
        check_secondary_keyword_coverage(plain, contract.secondary_keywords),
        check_word_count_band(plain, contract.word_count_target, competitor_word_count_median),
        check_heading_hierarchy(parser),
        check_link_counts(parser, host_domain),
        check_anchor_diversity(parser),
        check_image_alt_text(parser),
        check_meta_lengths(meta_title, meta_description),
        check_ai_tell_blocklist(plain, contract.ai_tell_blocklist),
        readability_check,
        check_language_consistency(plain, language=resolved_language),
        check_topical_entity_coverage(plain, high_coverage_entities),
        check_paa_coverage(plain, paa_questions, language=resolved_language),
    ]
    if is_listicle:
        deterministic.append(check_listicle_structure(article_html, contract))
    if judge_scores is not None:
        llm_judged = llm_judged_checks_from_scores(judge_scores)
    else:
        llm_judged = [
            stub_intent_match(),
            stub_backlink_anchor_naturalness(),
            stub_eeat_density(),
        ]
    return QualityReport(
        contract_target_keyword=primary,
        deterministic=deterministic,
        llm_judged=llm_judged,
    )
