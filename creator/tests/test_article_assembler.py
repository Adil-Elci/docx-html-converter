from __future__ import annotations

import json

import pytest

from creator.api.article_assembler import (
    DEFAULT_FAQ_HEADING,
    AssembledArticle,
    assemble_article,
)
from creator.api.contract import (
    ContentContract,
    FAQItem,
    GermanTone,
    LinkTarget,
    SchemaSpec,
    SearchIntent,
    SectionPlan,
)
from creator.api.section_writer import SectionDraft


def _contract(*, with_faq: bool = True, schema: SchemaSpec = None) -> ContentContract:
    sections = [
        SectionPlan(h2="Warum Hamburg", mandate="Vorteile fuer Hamburger Unternehmer ausfuehrlich.", target_word_count=200),
        SectionPlan(h2="Auswahlkriterien", mandate="Liste relevante Kriterien fuer die Auswahl auf.", target_word_count=300),
        SectionPlan(h2="Kosten und Honorare", mandate="Erklaere typische Honorarstrukturen.", target_word_count=200),
    ]
    return ContentContract(
        target_keyword="steuerberater hamburg",
        intent=SearchIntent.TRANSACTIONAL,
        target_audience="Hamburger Unternehmer und Selbstständige",
        word_count_target=900,
        h1="Steuerberater Hamburg: Wie Sie den richtigen Berater finden",
        meta_title="Steuerberater Hamburg finden: Tipps für Unternehmer 2026",
        meta_description="Sie suchen einen Steuerberater in Hamburg? Vergleichen Sie Leistungen, Honorare und Spezialisierungen mit unserem Leitfaden zur Auswahl.",
        slug="steuerberater-hamburg",
        sections=sections,
        faq_items=(
            [
                FAQItem(question="Was kostet ein Steuerberater?", answer_outline="100-300 Euro pro Stunde, je nach Leistung."),
                FAQItem(question="Wie finde ich einen Steuerberater?", answer_outline="Empfehlungen und Online-Recherche."),
            ]
            if with_faq
            else []
        ),
        schema_spec=schema or SchemaSpec(article=True, faq_page=True),
    )


def _draft(index: int, h2: str, body: str = "<p>Inhalt</p>", word_count: int = 200) -> SectionDraft:
    return SectionDraft(section_index=index, h2=h2, body_html=body, word_count=word_count)


def _drafts() -> list[SectionDraft]:
    return [
        _draft(0, "Warum Hamburg", "<p>Hamburg ist der zweitgrößte Wirtschaftsstandort.</p>"),
        _draft(1, "Auswahlkriterien", "<p>Spezialisierung, Honorar, Erreichbarkeit.</p>"),
        _draft(2, "Kosten und Honorare", "<p>Honorare folgen der Steuerberatergebührenverordnung.</p>"),
    ]


# ---- happy path ------------------------------------------------------------


def test_assemble_emits_h1_then_sections_in_index_order():
    result = assemble_article(contract=_contract(with_faq=False), sections=_drafts())
    html = result.article_html
    assert html.startswith("<h1>Steuerberater Hamburg")
    pos_h1 = html.find("<h1>")
    pos_warum = html.find("Warum Hamburg")
    pos_auswahl = html.find("Auswahlkriterien")
    pos_kosten = html.find("Kosten und Honorare")
    assert pos_h1 < pos_warum < pos_auswahl < pos_kosten


def test_assemble_sorts_drafts_by_section_index_when_passed_unsorted():
    contract = _contract(with_faq=False)
    drafts = list(reversed(_drafts()))
    result = assemble_article(contract=contract, sections=drafts)
    pos_first = result.article_html.find("Hamburg ist der zweitgrößte")
    pos_last = result.article_html.find("Steuerberatergebührenverordnung")
    assert 0 < pos_first < pos_last


def test_assemble_drops_sections_with_out_of_range_index():
    contract = _contract(with_faq=False)
    drafts = _drafts() + [_draft(99, "Phantom", "<p>nicht im Vertrag</p>")]
    result = assemble_article(contract=contract, sections=drafts)
    assert "Phantom" not in result.article_html
    assert "nicht im Vertrag" not in result.article_html


def test_assemble_appends_faq_block_when_items_present():
    result = assemble_article(contract=_contract(with_faq=True), sections=_drafts())
    html = result.article_html
    assert DEFAULT_FAQ_HEADING in html
    assert "<h3>Was kostet ein Steuerberater?</h3>" in html
    assert "<p>100-300 Euro pro Stunde, je nach Leistung.</p>" in html
    assert html.find(DEFAULT_FAQ_HEADING) > html.find("Kosten und Honorare")


def test_assemble_omits_faq_block_when_no_items():
    result = assemble_article(contract=_contract(with_faq=False), sections=_drafts())
    assert DEFAULT_FAQ_HEADING not in result.article_html


def test_assemble_uses_custom_faq_heading():
    result = assemble_article(
        contract=_contract(with_faq=True),
        sections=_drafts(),
        faq_heading="FAQ",
    )
    assert "<h2>FAQ</h2>" in result.article_html
    assert DEFAULT_FAQ_HEADING not in result.article_html


# ---- schema.org JSON-LD ----------------------------------------------------


def test_assemble_emits_article_jsonld_when_enabled():
    result = assemble_article(contract=_contract(with_faq=False), sections=_drafts())
    article_block = next(b for b in result.schema_blocks if '"@type": "Article"' in b)
    payload = json.loads(article_block.split(">", 1)[1].rsplit("<", 1)[0])
    assert payload["@type"] == "Article"
    assert payload["headline"].startswith("Steuerberater Hamburg")
    assert payload["inLanguage"] == "de"
    assert "mainEntityOfPage" not in payload


def test_assemble_includes_canonical_url_in_article_schema_when_provided():
    result = assemble_article(
        contract=_contract(with_faq=False),
        sections=_drafts(),
        canonical_url="https://example.de/steuerberater-hamburg",
    )
    article_block = next(b for b in result.schema_blocks if '"@type": "Article"' in b)
    payload = json.loads(article_block.split(">", 1)[1].rsplit("<", 1)[0])
    assert payload["mainEntityOfPage"] == "https://example.de/steuerberater-hamburg"


def test_assemble_emits_faqpage_jsonld_when_faq_present():
    result = assemble_article(contract=_contract(with_faq=True), sections=_drafts())
    faq_block = next(b for b in result.schema_blocks if '"@type": "FAQPage"' in b)
    payload = json.loads(faq_block.split(">", 1)[1].rsplit("<", 1)[0])
    assert payload["@type"] == "FAQPage"
    questions = payload["mainEntity"]
    assert len(questions) == 2
    assert questions[0]["name"] == "Was kostet ein Steuerberater?"
    assert questions[0]["acceptedAnswer"]["text"].startswith("100-300 Euro")


def test_assemble_omits_faqpage_jsonld_when_no_items():
    result = assemble_article(contract=_contract(with_faq=False), sections=_drafts())
    assert all('"@type": "FAQPage"' not in block for block in result.schema_blocks)


def test_assemble_omits_article_schema_when_disabled():
    contract = _contract(schema=SchemaSpec(article=False, faq_page=True))
    result = assemble_article(contract=contract, sections=_drafts())
    assert all('"@type": "Article"' not in block for block in result.schema_blocks)


def test_assemble_omits_faqpage_schema_when_disabled():
    contract = _contract(schema=SchemaSpec(article=True, faq_page=False))
    result = assemble_article(contract=contract, sections=_drafts())
    assert all('"@type": "FAQPage"' not in block for block in result.schema_blocks)


def test_assemble_strips_html_tags_from_faq_answer_in_schema():
    """JSON-LD FAQ answers must be plain text, not HTML."""

    contract = _contract(with_faq=True)
    contract.faq_items[0] = FAQItem(
        question="Frage?",
        answer_outline="<p>Antwort mit <strong>Markup</strong>.</p>",
    )
    result = assemble_article(contract=contract, sections=_drafts())
    faq_block = next(b for b in result.schema_blocks if '"@type": "FAQPage"' in b)
    payload = json.loads(faq_block.split(">", 1)[1].rsplit("<", 1)[0])
    answer_text = payload["mainEntity"][0]["acceptedAnswer"]["text"]
    assert answer_text == "Antwort mit Markup."
    assert "<" not in answer_text


# ---- escaping --------------------------------------------------------------


def test_assemble_escapes_h1_special_chars():
    contract = _contract(with_faq=False)
    contract.h1 = "Steuerberater & Co: <Hamburg>"
    result = assemble_article(contract=contract, sections=_drafts())
    assert "<h1>Steuerberater &amp; Co: &lt;Hamburg&gt;</h1>" in result.article_html


def test_assemble_escapes_faq_question_special_chars():
    contract = _contract(with_faq=True)
    contract.faq_items[0] = FAQItem(question="Was & wo?", answer_outline="Antwort mit <Sternchen>")
    result = assemble_article(contract=contract, sections=_drafts())
    assert "<h3>Was &amp; wo?</h3>" in result.article_html
    assert "<p>Antwort mit &lt;Sternchen&gt;</p>" in result.article_html


# ---- full_html shape -------------------------------------------------------


def test_full_html_concatenates_article_and_schema_blocks():
    result = assemble_article(contract=_contract(with_faq=True), sections=_drafts())
    assert result.article_html in result.full_html
    for block in result.schema_blocks:
        assert block in result.full_html
    assert result.full_html.endswith("</script>")


def test_assemble_skips_section_with_empty_body_html():
    drafts = [
        _draft(0, "Warum Hamburg", body=""),
        _draft(1, "Auswahlkriterien", body="<p>Inhalt.</p>"),
        _draft(2, "Kosten und Honorare", body="<p>Inhalt.</p>"),
    ]
    result = assemble_article(contract=_contract(with_faq=False), sections=drafts)
    # H2 is still emitted; just no body paragraph.
    assert "<h2>Warum Hamburg</h2>" in result.article_html
    # Empty body should not produce a stray empty <p> or duplicate whitespace artifact.
    pos_first_h2 = result.article_html.find("<h2>Warum Hamburg</h2>")
    pos_second_h2 = result.article_html.find("<h2>Auswahlkriterien</h2>")
    between = result.article_html[pos_first_h2:pos_second_h2]
    # Only the H2 line + a newline, no body paragraph between them.
    assert "<p>" not in between
