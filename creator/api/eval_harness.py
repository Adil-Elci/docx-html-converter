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

from .contract import ContentContract
from .entity_extract import ExtractedEntity
from .eval_judge import JudgeScores
from .research import ResearchPayload


# ---- Deterministic thresholds -----------------------------------------------

KEYWORD_DENSITY_MIN = 0.005  # 0.5%
KEYWORD_DENSITY_MAX = 0.015  # 1.5%
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
ENTITY_COVERAGE_MIN = 0.70
PAA_QUESTION_TERM_MATCH_MIN = 0.6  # fraction of a question's content words that must appear in the article
PAA_COVERAGE_MIN = 0.80
BACKLINK_ANCHOR_NATURALNESS_MIN = 7
EEAT_DENSITY_MIN = 6


# Minimal German stopword set used for PAA-coverage scoring. Not exhaustive
# linguistically — only common closed-class words that contribute no topical
# signal when matching question terms against article body.
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


def _content_words(text: str) -> List[str]:
    tokens = re.findall(r"\b[\wäöüÄÖÜß]+\b", (text or "").lower())
    return [t for t in tokens if t not in _GERMAN_STOPWORDS and len(t) > 2]


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


# ---- Deterministic checks ---------------------------------------------------

def check_keyword_in_h1(text_h1: str, keyword: str) -> CheckResult:
    passed = keyword.lower() in (text_h1 or "").lower()
    return CheckResult("keyword_in_h1", passed, detail=f"h1='{text_h1}' keyword='{keyword}'")


def check_keyword_in_first_100_words(plain_text: str, keyword: str) -> CheckResult:
    first = " ".join(plain_text.split()[:100]).lower()
    passed = keyword.lower() in first
    return CheckResult("keyword_in_first_100_words", passed)


def check_keyword_density(plain_text: str, keyword: str) -> CheckResult:
    words = _word_count(plain_text)
    if words == 0:
        return CheckResult("keyword_density", False, detail="empty article")
    pattern = re.compile(re.escape(keyword), re.IGNORECASE)
    occurrences = len(pattern.findall(plain_text))
    density = occurrences / words
    passed = KEYWORD_DENSITY_MIN <= density <= KEYWORD_DENSITY_MAX
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


def check_paa_coverage(plain_text: str, paa_questions: List[str]) -> CheckResult:
    if not paa_questions:
        return CheckResult("paa_coverage", True, value=1.0, detail="no PAA questions")
    article_words = set(_content_words(plain_text))
    covered = 0
    for question in paa_questions:
        question_words = _content_words(question)
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
) -> QualityReport:
    parser = _parse(article_html)
    plain = parser.plain_text
    primary = contract.target_keyword
    h1 = parser.h1[0] if parser.h1 else ""

    competitor_word_count_median = research.competitor_word_count_median if research else None
    high_coverage_entities = research.high_coverage_entities if research else []
    paa_questions = research.paa_questions if research else []

    deterministic = [
        check_keyword_in_h1(h1, primary),
        check_keyword_in_first_100_words(plain, primary),
        check_keyword_density(plain, primary),
        check_secondary_keyword_coverage(plain, contract.secondary_keywords),
        check_word_count_band(plain, contract.word_count_target, competitor_word_count_median),
        check_heading_hierarchy(parser),
        check_link_counts(parser, host_domain),
        check_anchor_diversity(parser),
        check_image_alt_text(parser),
        check_meta_lengths(meta_title, meta_description),
        check_ai_tell_blocklist(plain, contract.ai_tell_blocklist),
        check_german_readability(plain),
        check_topical_entity_coverage(plain, high_coverage_entities),
        check_paa_coverage(plain, paa_questions),
    ]
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
