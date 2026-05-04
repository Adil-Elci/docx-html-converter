"""Stitch SectionDraft outputs + FAQ + schema.org JSON-LD into a final article.

Pure deterministic: no LLM calls. The article HTML is a string of concatenated
section bodies with H2 headings restored, an FAQ block when ``contract.faq_items``
is non-empty, and JSON-LD ``<script>`` blocks for ``Article`` and ``FAQPage``
schemas based on ``contract.schema_spec``.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import List, Optional, Sequence

from .contract import ContentContract, FAQItem
from .section_writer import SectionDraft

DEFAULT_FAQ_HEADING = "Häufig gestellte Fragen"


@dataclass
class AssembledArticle:
    article_html: str  # the article body HTML (h1 + sections + FAQ block)
    schema_blocks: List[str] = field(default_factory=list)  # raw <script type="application/ld+json"> strings

    @property
    def full_html(self) -> str:
        return "\n".join([self.article_html, *self.schema_blocks])


# ---- HTML escape -----------------------------------------------------------


_ESCAPE_TABLE = str.maketrans({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"})


def _escape(text: str) -> str:
    return (text or "").translate(_ESCAPE_TABLE)


def _strip_tags(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", text or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # Tag replacement leaves a stray space before terminal punctuation
    # (e.g. "<strong>Markup</strong>." -> "Markup ."). Tighten it.
    return re.sub(r"\s+([.,;:!?])", r"\1", cleaned)


# ---- assembly --------------------------------------------------------------


def _render_faq_block(faq_items: Sequence[FAQItem], heading: str) -> str:
    if not faq_items:
        return ""
    parts: List[str] = [f"<h2>{_escape(heading)}</h2>"]
    for item in faq_items:
        parts.append(f"<h3>{_escape(item.question)}</h3>")
        parts.append(f"<p>{_escape(item.answer_outline)}</p>")
    return "\n".join(parts)


def _build_article_schema(contract: ContentContract, canonical_url: Optional[str]) -> dict:
    schema: dict = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": contract.h1,
        "description": contract.meta_description,
        "inLanguage": "de",
    }
    if canonical_url:
        schema["mainEntityOfPage"] = canonical_url
    return schema


def _build_faqpage_schema(faq_items: Sequence[FAQItem]) -> dict:
    return {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {
                "@type": "Question",
                "name": item.question,
                "acceptedAnswer": {
                    "@type": "Answer",
                    "text": _strip_tags(item.answer_outline),
                },
            }
            for item in faq_items
        ],
    }


def _jsonld(schema: dict) -> str:
    payload = json.dumps(schema, ensure_ascii=False, indent=2)
    return f'<script type="application/ld+json">\n{payload}\n</script>'


def assemble_article(
    *,
    contract: ContentContract,
    sections: Sequence[SectionDraft],
    faq_heading: str = DEFAULT_FAQ_HEADING,
    canonical_url: Optional[str] = None,
) -> AssembledArticle:
    """Stitch sections + FAQ + JSON-LD into a final article.

    Sections may be passed in any order; they are sorted by ``section_index``.
    Sections with indices outside the contract's range are silently dropped to
    keep the assembler robust against partial outputs.
    """

    valid_indices = set(range(len(contract.sections)))
    filtered = [draft for draft in sections if draft.section_index in valid_indices]
    sorted_drafts = sorted(filtered, key=lambda d: d.section_index)

    parts: List[str] = [f"<h1>{_escape(contract.h1)}</h1>"]
    for draft in sorted_drafts:
        parts.append(f"<h2>{_escape(draft.h2)}</h2>")
        if draft.body_html.strip():
            parts.append(draft.body_html.strip())

    faq_block = _render_faq_block(contract.faq_items, faq_heading)
    if faq_block:
        parts.append(faq_block)

    article_html = "\n".join(parts)

    schema_blocks: List[str] = []
    if contract.schema_spec.article:
        schema_blocks.append(_jsonld(_build_article_schema(contract, canonical_url)))
    if contract.schema_spec.faq_page and contract.faq_items:
        schema_blocks.append(_jsonld(_build_faqpage_schema(contract.faq_items)))

    return AssembledArticle(article_html=article_html, schema_blocks=schema_blocks)
