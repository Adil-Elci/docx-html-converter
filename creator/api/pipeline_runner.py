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

from .article_assembler import AssembledArticle, assemble_article
from .contract import ContentContract
from .contract_generator import generate_contract
from .eval_harness import QualityReport, evaluate
from .eval_judge import JudgeScores, judge_article
from .research import ResearchPayload, run_research
from .section_writer import SectionDraft, write_all_sections
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
    publishing_site_host: str

    research: ResearchPayload
    contract: ContentContract
    sections: List[SectionDraft]
    assembled: AssembledArticle
    refined_article_html: str  # body HTML after voice pass (no schema blocks)
    final_html: str  # refined_article_html + schema blocks
    judge_scores: Optional[JudgeScores]
    quality_report: QualityReport

    skipped_voice_pass: bool = False
    skipped_judge: bool = False
    notes: List[str] = field(default_factory=list)


def _host_from_url(url: str) -> str:
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = (parsed.netloc or parsed.path or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def run_pipeline(
    *,
    target_keyword: str,
    target_backlink_url: str,
    publishing_site_url: str,
    anchor_hint: Optional[str] = None,
    canonical_url: Optional[str] = None,
    skip_voice_pass: bool = False,
    skip_judge: bool = False,
    skip_related_keywords: bool = False,
    skip_entity_extraction: bool = False,
) -> PipelineRun:
    """Run every phase end-to-end. Raises ``PipelineError`` on any step failure."""

    publishing_site_host = _host_from_url(publishing_site_url)
    notes: List[str] = []

    # -- 1. Research ---------------------------------------------------------
    try:
        research = run_research(
            target_keyword=target_keyword,
            skip_related_keywords=skip_related_keywords,
            skip_entity_extraction=skip_entity_extraction,
        )
    except Exception as exc:
        raise PipelineError("research", str(exc)) from exc
    logger.info(
        "pipeline.research_done keyword=%s competitors_ok=%s entities=%s cost=$%.4f",
        target_keyword,
        research.successful_competitor_count,
        len(research.entities),
        research.total_cost_usd,
    )

    # -- 2. Contract ---------------------------------------------------------
    try:
        contract = generate_contract(
            research,
            target_backlink_url=target_backlink_url,
            anchor_hint=anchor_hint,
        )
    except Exception as exc:
        raise PipelineError("contract", str(exc)) from exc
    logger.info(
        "pipeline.contract_done sections=%s entities=%s ai_tells=%s",
        len(contract.sections),
        len(contract.required_entities),
        len(contract.ai_tell_blocklist),
    )

    # -- 3. Sections (parallel) ---------------------------------------------
    try:
        sections = write_all_sections(contract=contract, parallel=True)
    except Exception as exc:
        raise PipelineError("sections", str(exc)) from exc
    logger.info("pipeline.sections_done count=%s", len(sections))

    # -- 4. Assemble --------------------------------------------------------
    try:
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
        target_keyword=target_keyword,
        target_backlink_url=target_backlink_url,
        publishing_site_host=publishing_site_host,
        research=research,
        contract=contract,
        sections=sections,
        assembled=assembled,
        refined_article_html=refined_body,
        final_html=final_html,
        judge_scores=judge_scores,
        quality_report=quality_report,
        skipped_voice_pass=skip_voice_pass,
        skipped_judge=skip_judge,
        notes=notes,
    )
