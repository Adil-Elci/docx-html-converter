"""End-to-end pipeline runner: keyword + target URL -> reviewable article + QualityReport.

Chains the seven phases:
1. research      (DataForSEO + scrape + Haiku entities)
2. contract      (Sonnet 4.6 + extended thinking)
3. sections      (parallel Sonnet 4.6 with prompt caching)
4. assemble      (deterministic stitch + FAQ + JSON-LD)
5. voice pass    (Sonnet 4.6 — preserves links + entities + headings)
6. judge         (Haiku 4.5 — intent / anchor naturalness / E-E-A-T)
7. eval harness  (deterministic + research-driven + judge axes)

Output is a single ``PipelineRun`` dataclass that downstream code (portal_backend,
review surface, smoke scripts) can serialize and inspect. Each step is wrapped
in a try/except so a single failure surfaces with phase context rather than a
bare traceback through the whole chain.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import urlparse

from .article_assembler import AssembledArticle, assemble_article, assemble_listicle
from .contract import ArticleFormat, ContentContract
from .contract_generator import generate_contract
from .eval_harness import QualityReport, evaluate
from .eval_judge import JudgeScores, judge_article
from .listicle_writer import ItemDraft, write_all_items
from .research import ResearchPayload, run_research
from .section_writer import SectionDraft, write_all_sections, write_section
from .topic_derivation import DerivedTopic, TopicDerivationError, derive_topic
from .voice_pass import refine_voice

logger = logging.getLogger("creator.pipeline_runner")


class PipelineError(RuntimeError):
    """Raised when a pipeline step fails. ``phase`` identifies where."""

    def __init__(self, phase: str, message: str) -> None:
        super().__init__(f"[{phase}] {message}")
        self.phase = phase


@dataclass
class PipelineRun:
    target_keyword: str
    target_backlink_url: str
    publishing_site_host: str  # empty string when site is to be late-bound
    language: str

    research: ResearchPayload
    contract: ContentContract
    sections: List[SectionDraft]
    assembled: AssembledArticle
    refined_article_html: str  # body HTML after voice pass (no schema blocks)
    final_html: str  # refined_article_html + schema blocks
    judge_scores: Optional[JudgeScores]
    quality_report: QualityReport

    derived_topic: Optional[DerivedTopic] = None
    items: List[ItemDraft] = field(default_factory=list)
    skipped_voice_pass: bool = False
    skipped_judge: bool = False
    notes: List[str] = field(default_factory=list)


def _host_from_url(url: str) -> str:
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = (parsed.netloc or parsed.path or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


_LANGUAGE_TO_LOCATION: dict = {
    "de": (2276, "de"),
    "fr": (2250, "fr"),
}


def run_pipeline(
    *,
    target_backlink_url: str,
    target_keyword: Optional[str] = None,
    publishing_site_url: Optional[str] = None,
    anchor_hint: Optional[str] = None,
    canonical_url: Optional[str] = None,
    language: Optional[str] = None,
    editorial_angle: Optional[dict] = None,
    article_format: Optional[str] = None,
    skip_voice_pass: bool = False,
    skip_judge: bool = False,
    skip_related_keywords: bool = False,
    skip_entity_extraction: bool = False,
) -> PipelineRun:
    """Run every phase end-to-end. Raises ``PipelineError`` on any step failure.

    ``target_keyword`` is optional: when omitted, ``derive_topic`` is called
    against ``target_backlink_url`` to derive both the keyword and the
    language/locale. The derived topic is preserved on the returned run.

    ``publishing_site_url`` is optional: when omitted, the eval harness's
    internal/external link split runs in "all-external" mode (the host
    isn't known yet because the publishing site will be late-bound by the
    portal backend after this pipeline returns).

    ``language`` (ISO 639-1) overrides the derived language. When neither
    is provided, derivation supplies it. Currently supported: ``de``, ``fr``.
    """

    notes: List[str] = []

    derived_topic: Optional[DerivedTopic] = None
    resolved_keyword = (target_keyword or "").strip()
    resolved_language = (language or "").strip().lower() or None

    if not resolved_keyword:
        try:
            derived_topic = derive_topic(target_backlink_url)
        except TopicDerivationError as exc:
            raise PipelineError(
                "topic_derivation",
                f"[{exc.code}] {exc}",
            ) from exc
        except Exception as exc:
            raise PipelineError("topic_derivation", str(exc)) from exc
        resolved_keyword = derived_topic.target_keyword
        if not resolved_language:
            resolved_language = derived_topic.language_code
        notes.append(f"derived_keyword={resolved_keyword}")

    if not resolved_keyword:
        raise PipelineError("topic_derivation", "Could not determine target keyword.")

    normalized_language = (resolved_language or "de").lower()
    if normalized_language not in _LANGUAGE_TO_LOCATION:
        raise PipelineError(
            "language",
            f"Unsupported language {normalized_language!r}; supported: {sorted(_LANGUAGE_TO_LOCATION.keys())}.",
        )
    location_code, language_code = _LANGUAGE_TO_LOCATION[normalized_language]

    publishing_site_host = _host_from_url(publishing_site_url) if publishing_site_url else ""
    if not publishing_site_host:
        notes.append("publishing_site late-bind: host-based eval check skipped")

    # -- 1. Research ---------------------------------------------------------
    try:
        research = run_research(
            target_keyword=resolved_keyword,
            location_code=location_code,
            language_code=language_code,
            skip_related_keywords=skip_related_keywords,
            skip_entity_extraction=skip_entity_extraction,
        )
    except Exception as exc:
        raise PipelineError("research", str(exc)) from exc
    logger.info(
        "pipeline.research_done keyword=%s competitors_ok=%s entities=%s cost=$%.4f",
        resolved_keyword,
        research.successful_competitor_count,
        len(research.entities),
        research.total_cost_usd,
    )

    # -- 2. Contract ---------------------------------------------------------
    # Caller can pin the article format via either the top-level
    # ``article_format`` param OR via ``editorial_angle.format``. The top-level
    # param wins; we mirror it onto editorial_angle so prompt selection +
    # format-pin both fire consistently.
    requested_format = (article_format or "").strip().lower() or None
    if requested_format not in (None, "narrative", "listicle"):
        raise PipelineError("contract", f"Unsupported article_format {requested_format!r}; expected narrative or listicle.")
    if requested_format == "listicle":
        if editorial_angle is None:
            editorial_angle = {"format": "listicle"}
        elif isinstance(editorial_angle, dict):
            editorial_angle.setdefault("format", "listicle")
            # Force-override; defends against caller threading a stale value.
            editorial_angle["format"] = "listicle"
    try:
        contract = generate_contract(
            research,
            target_backlink_url=target_backlink_url,
            anchor_hint=anchor_hint,
            language=normalized_language,
            editorial_angle=editorial_angle,
        )
    except Exception as exc:
        raise PipelineError("contract", str(exc)) from exc
    logger.info(
        "pipeline.contract_done sections=%s entities=%s ai_tells=%s",
        len(contract.sections),
        len(contract.required_entities),
        len(contract.ai_tell_blocklist),
    )

    # -- 3. Sections / Items (parallel) -------------------------------------
    is_listicle = contract.format == ArticleFormat.LISTICLE
    sections: List[SectionDraft] = []
    items: List[ItemDraft] = []
    if is_listicle:
        # Listicle: contract.sections holds [intro, outro] (rendered via the
        # narrative section_writer, single calls each). The listicle_writer
        # emits one ItemDraft per ranked item, parallelized just like sections.
        try:
            if len(contract.sections) >= 1:
                sections.append(write_section(contract=contract, section_index=0))
            if len(contract.sections) >= 2:
                sections.append(write_section(contract=contract, section_index=1))
        except Exception as exc:
            raise PipelineError("sections", str(exc)) from exc
        try:
            items = write_all_items(contract=contract, parallel=True)
        except Exception as exc:
            raise PipelineError("items", str(exc)) from exc
        logger.info("pipeline.listicle_done items=%s sections=%s", len(items), len(sections))
    else:
        try:
            sections = write_all_sections(contract=contract, parallel=True)
        except Exception as exc:
            raise PipelineError("sections", str(exc)) from exc
        logger.info("pipeline.sections_done count=%s", len(sections))

    # -- 4. Assemble --------------------------------------------------------
    try:
        if is_listicle:
            intro = sections[0] if sections else SectionDraft(section_index=0, h2="", body_html="")
            outro = sections[1] if len(sections) >= 2 else None
            assembled = assemble_listicle(
                contract=contract,
                intro=intro,
                items=items,
                outro=outro,
                canonical_url=canonical_url,
            )
        else:
            assembled = assemble_article(
                contract=contract,
                sections=sections,
                canonical_url=canonical_url,
            )
    except Exception as exc:
        raise PipelineError("assemble", str(exc)) from exc

    # -- 5. Voice pass (optional) -------------------------------------------
    if skip_voice_pass:
        refined_body = assembled.article_html
        notes.append("voice_pass skipped by caller")
    else:
        try:
            refined_body = refine_voice(article_html=assembled.article_html, contract=contract)
        except Exception as exc:
            raise PipelineError("voice_pass", str(exc)) from exc
    final_html = "\n".join([refined_body, *assembled.schema_blocks])
    logger.info("pipeline.voice_pass_done skipped=%s", skip_voice_pass)

    # -- 6. Judge (optional) ------------------------------------------------
    judge_scores: Optional[JudgeScores] = None
    if skip_judge:
        notes.append("judge skipped by caller")
    else:
        try:
            judge_scores = judge_article(article_html=final_html, contract=contract, research=research)
        except Exception as exc:
            raise PipelineError("judge", str(exc)) from exc
    logger.info("pipeline.judge_done skipped=%s", skip_judge)

    # -- 7. Eval harness ----------------------------------------------------
    try:
        quality_report = evaluate(
            article_html=final_html,
            contract=contract,
            host_domain=publishing_site_host,
            meta_title=contract.meta_title,
            meta_description=contract.meta_description,
            research=research,
            judge_scores=judge_scores,
            language=normalized_language,
        )
    except Exception as exc:
        raise PipelineError("eval", str(exc)) from exc
    logger.info(
        "pipeline.eval_done passed=%s deterministic_failures=%s judge_failures=%s",
        quality_report.passed,
        sum(1 for r in quality_report.deterministic if not r.passed),
        sum(1 for r in quality_report.llm_judged if not r.passed),
    )

    return PipelineRun(
        target_keyword=resolved_keyword,
        target_backlink_url=target_backlink_url,
        publishing_site_host=publishing_site_host,
        language=normalized_language,
        research=research,
        contract=contract,
        sections=sections,
        assembled=assembled,
        refined_article_html=refined_body,
        final_html=final_html,
        judge_scores=judge_scores,
        quality_report=quality_report,
        derived_topic=derived_topic,
        items=items,
        skipped_voice_pass=skip_voice_pass,
        skipped_judge=skip_judge,
        notes=notes,
    )
