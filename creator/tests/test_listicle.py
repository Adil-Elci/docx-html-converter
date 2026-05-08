from __future__ import annotations

import pytest

from creator.api.article_assembler import assemble_listicle
from creator.api.contract import (
    ArticleFormat,
    ContentContract,
    EntityRequirement,
    GermanTone,
    LinkTarget,
    ListiclePlan,
    SchemaSpec,
    SearchIntent,
    SectionPlan,
)
from creator.api.eval_harness import (
    KEYWORD_DENSITY_MAX,
    KEYWORD_DENSITY_MAX_LISTICLE,
    check_keyword_density,
    check_listicle_structure,
)
from creator.api.listicle_writer import (
    ItemDraft,
    build_user_prompt as build_item_prompt,
    write_all_items,
    write_item,
)
from creator.api.section_writer import SectionDraft


def _make_listicle_contract(item_count: int = 5) -> ContentContract:
    if item_count < 5:
        raise ValueError("ListiclePlan.item_count must be >= 5; tests should bound accordingly.")
    return ContentContract(
        target_keyword="steuerberater hamburg",
        intent=SearchIntent.COMMERCIAL,
        target_audience="Hamburger Unternehmer und Selbstständige",
        word_count_target=1500,
        h1="Die 5 besten Steuerberater in Hamburg 2026",
        meta_title="Steuerberater Hamburg 2026: Top 5 Anbieter im Vergleich",
        meta_description="Welche Steuerberater in Hamburg lohnen sich 2026? Die 5 besten Kanzleien im redaktionellen Vergleich mit Stärken, Schwächen und Honoraren.",
        slug="steuerberater-hamburg-top-5-2026",
        format=ArticleFormat.LISTICLE,
        listicle_plan=ListiclePlan(
            item_count=item_count,
            ranking_basis="score",
            item_template=["name", "hook", "pros", "cons", "verdict"],
            items=[f"Kanzlei {i}" for i in range(1, item_count + 1)],
        ),
        sections=[
            SectionPlan(h2="Einleitung", mandate="Kontext und Auswahlkriterien", target_word_count=200),
            SectionPlan(h2="Fazit", mandate="Zusammenfassung und Empfehlung", target_word_count=200),
        ],
        required_entities=[EntityRequirement(name="DATEV", placement_hint="in item 3")],
        link_plan=[
            LinkTarget(
                target_url="https://example.de/steuerberater",
                anchor_strategy="branded",
                section_index=3,
                surrounding_context_requirements="Empfehlung als konkretes Beispiel auf Rang 3.",
                link_type="backlink",
            )
        ],
        ai_tell_blocklist=["Darüber hinaus"],
        schema_spec=SchemaSpec(article=True, faq_page=False, item_list=True),
        tone=GermanTone.SIE,
    )


def test_contract_listicle_round_trip() -> None:
    contract = _make_listicle_contract()
    assert contract.format == ArticleFormat.LISTICLE
    assert contract.listicle_plan is not None
    assert len(contract.listicle_plan.items) == 5


def test_contract_rejects_listicle_plan_on_narrative() -> None:
    with pytest.raises(Exception):
        ContentContract(
            target_keyword="kw",
            intent=SearchIntent.INFORMATIONAL,
            target_audience="audience description",
            word_count_target=500,
            h1="Some headline",
            meta_title="Some headline meta title for SEO",
            meta_description="A description with enough characters to satisfy the min-length constraint on this field.",
            slug="some-headline",
            format=ArticleFormat.NARRATIVE,
            listicle_plan=ListiclePlan(item_count=5),
            sections=[
                SectionPlan(h2="One", mandate="Cover topic A in detail.", target_word_count=200),
                SectionPlan(h2="Two", mandate="Cover topic B in detail.", target_word_count=200),
            ],
        )


def test_contract_rejects_listicle_without_plan() -> None:
    with pytest.raises(Exception):
        ContentContract(
            target_keyword="kw",
            intent=SearchIntent.INFORMATIONAL,
            target_audience="audience description",
            word_count_target=500,
            h1="Some headline",
            meta_title="Some headline meta title for SEO",
            meta_description="A description with enough characters to satisfy the min-length constraint on this field.",
            slug="some-headline",
            format=ArticleFormat.LISTICLE,
            sections=[
                SectionPlan(h2="One", mandate="Cover topic A in detail.", target_word_count=200),
                SectionPlan(h2="Two", mandate="Cover topic B in detail.", target_word_count=200),
            ],
        )


def test_contract_rejects_item_count_mismatch() -> None:
    with pytest.raises(Exception):
        ContentContract(
            target_keyword="kw",
            intent=SearchIntent.INFORMATIONAL,
            target_audience="audience description",
            word_count_target=500,
            h1="Some headline 5",
            meta_title="Some headline 5 meta title for SEO",
            meta_description="A description with enough characters to satisfy the min-length constraint on this field.",
            slug="some-headline-5",
            format=ArticleFormat.LISTICLE,
            listicle_plan=ListiclePlan(item_count=5, items=["a", "b", "c"]),
            sections=[
                SectionPlan(h2="One", mandate="Cover topic A in detail.", target_word_count=200),
                SectionPlan(h2="Two", mandate="Cover topic B in detail.", target_word_count=200),
            ],
        )


def test_listicle_writer_user_prompt_mentions_rank_and_template() -> None:
    contract = _make_listicle_contract()
    prompt = build_item_prompt(contract=contract, rank=3)
    assert "Rang 3 von 5" in prompt
    assert "DATEV" in prompt
    assert "https://example.de/steuerberater" in prompt
    assert "name, hook, pros, cons, verdict" in prompt


def _fake_item_call(rank: int):
    def _call(**kwargs):
        return {
            "body_html": (
                f"<p>Eintrag Nr. {rank} im Überblick.</p>"
                f"<h3>Vorteile</h3><ul><li>Stärke A</li><li>Stärke B</li><li>Stärke C</li></ul>"
                f"<h3>Nachteile</h3><ul><li>Schwäche X</li><li>Schwäche Y</li></ul>"
                f"<p class=\"verdict\">Empfehlung für Rang {rank}.</p>"
            ),
            "links_inserted": [],
            "word_count": 25,
        }
    return _call


def test_write_item_returns_validated_draft() -> None:
    contract = _make_listicle_contract(item_count=5)
    fake = _fake_item_call(2)

    def llm_caller(**kwargs):
        return fake(**kwargs)

    draft = write_item(contract=contract, rank=2, api_key="x", llm_caller=llm_caller)
    assert isinstance(draft, ItemDraft)
    assert draft.rank == 2
    assert draft.name == "Kanzlei 2"
    assert "Rang 2" in draft.body_html


def test_write_all_items_serial() -> None:
    contract = _make_listicle_contract(item_count=5)

    def llm_caller(**kwargs):
        label = kwargs.get("request_label", "")
        rank = int(label.rsplit("_", 1)[-1])
        return _fake_item_call(rank)()

    drafts = write_all_items(contract=contract, api_key="x", llm_caller=llm_caller, parallel=False)
    assert [d.rank for d in drafts] == [1, 2, 3, 4, 5]
    assert all(d.body_html.startswith("<p>") for d in drafts)


def _verdict_block(rank: int) -> str:
    return (
        f"<h2>{rank}. Kanzlei {rank}</h2>"
        f"<h3>Vorteile</h3><ul><li>a</li></ul>"
        f"<h3>Nachteile</h3><ul><li>b</li></ul>"
        f"<p class=\"verdict\">v</p>"
    )


def test_assemble_listicle_renders_ranks_and_jsonld() -> None:
    contract = _make_listicle_contract(item_count=5)
    intro = SectionDraft(section_index=0, h2="Einleitung", body_html="<p>Auswahlkriterien.</p>")
    outro = SectionDraft(section_index=1, h2="Fazit", body_html="<p>Zusammenfassung.</p>")
    items = [
        ItemDraft(rank=r, name=f"Kanzlei {r}", body_html=f"<p>Item {r}</p><p class=\"verdict\">v{r}</p>", word_count=10)
        for r in range(1, 6)
    ]
    assembled = assemble_listicle(contract=contract, intro=intro, items=items, outro=outro)
    html = assembled.full_html
    assert "<h1>Die 5 besten Steuerberater in Hamburg 2026</h1>" in html
    assert "<h2>1. Kanzlei 1</h2>" in html
    assert "<h2>5. Kanzlei 5</h2>" in html
    assert "<h2>Fazit</h2>" in html
    assert any('"@type": "ItemList"' in block for block in assembled.schema_blocks)


def test_check_listicle_structure_passes_on_well_formed_html() -> None:
    contract = _make_listicle_contract(item_count=5)
    html = "<h1>Top 5</h1>" + "".join(_verdict_block(r) for r in range(1, 6))
    result = check_listicle_structure(html, contract)
    assert result.passed, result.detail


def test_check_listicle_structure_fails_on_missing_item() -> None:
    contract = _make_listicle_contract(item_count=5)
    html = "<h1>Top 5</h1>" + "".join(_verdict_block(r) for r in range(1, 4))
    result = check_listicle_structure(html, contract)
    assert not result.passed
    assert "expected 5" in result.detail


def test_check_listicle_structure_fails_on_non_consecutive_ranks() -> None:
    contract = _make_listicle_contract(item_count=5)
    html = "<h1>Top 5</h1>" + "".join(_verdict_block(r) for r in (1, 3, 4, 5, 6))
    result = check_listicle_structure(html, contract)
    assert not result.passed
    assert "consecutively" in result.detail


def test_keyword_density_uses_listicle_cap() -> None:
    # 4% density should fail under narrative cap (1.5%) but pass with listicle cap (4%).
    text = " ".join(["foo"] * 4 + ["filler"] * 96)
    narrative = check_keyword_density(text, "foo", density_max=KEYWORD_DENSITY_MAX)
    listicle = check_keyword_density(text, "foo", density_max=KEYWORD_DENSITY_MAX_LISTICLE)
    assert not narrative.passed
    assert listicle.passed


def test_brainstorm_listicle_directive_only_when_flag_set() -> None:
    from creator.api.topic_brainstorm import _build_user_prompt

    base = _build_user_prompt(
        target_url="https://example.de",
        target_keyword="steuerberater hamburg",
        publishing_profile_payload=None,
        language="de",
        current_year=2026,
    )
    listicle = _build_user_prompt(
        target_url="https://example.de",
        target_keyword="steuerberater hamburg",
        publishing_profile_payload=None,
        language="de",
        current_year=2026,
        prefer_listicle=True,
    )
    assert "FORMAT-PRÄFERENZ: LISTICLE" not in base
    assert "FORMAT-PRÄFERENZ: LISTICLE" in listicle


def test_contract_generator_synthesises_listicle_plan_when_llm_drifts(monkeypatch) -> None:
    """Catches the regression where the LLM ignores v2 prompt and emits a
    narrative-shaped contract for a listicle request — Pydantic's default
    format=narrative would silently win and the pipeline would render a
    normal article.
    """
    from creator.api import contract_generator
    from creator.api.research import ResearchPayload

    def fake_load_prompt(name, version, language=None):
        return contract_generator.Prompt(
            name=name, version=version or "v1", language=language, body="STUB", metadata={}
        )

    narrative_shaped_response = (
        '{"target_keyword": "steuerberater hamburg", "intent": "commercial",'
        ' "target_audience": "Hamburger Unternehmer und Selbststaendige",'
        ' "word_count_target": 1500,'
        ' "h1": "Die 7 besten Steuerberater in Hamburg 2026",'
        ' "meta_title": "Steuerberater Hamburg 2026: Die Top 7 im Vergleich",'
        ' "meta_description": "Welche Steuerberater in Hamburg lohnen sich 2026? Die sieben besten Kanzleien im redaktionellen Vergleich auf einen Blick.",'
        ' "slug": "steuerberater-hamburg-top-7-2026",'
        ' "sections": [{"h2": "Einleitung und Auswahlkriterien", "mandate": "Kontextueller Aufhaenger und Auswahlkriterien.", "target_word_count": 200},'
        '   {"h2": "Mueller and Partner", "mandate": "Item 1 description here", "target_word_count": 200},'
        '   {"h2": "Schmidt Steuer GmbH", "mandate": "Item 2 description here", "target_word_count": 200},'
        '   {"h2": "Weber Kanzlei", "mandate": "Item 3 description here", "target_word_count": 200},'
        '   {"h2": "Fischer Steuern", "mandate": "Item 4 description here", "target_word_count": 200},'
        '   {"h2": "Becker und Co", "mandate": "Item 5 description here", "target_word_count": 200},'
        '   {"h2": "Hoffmann KG", "mandate": "Item 6 description here", "target_word_count": 200},'
        '   {"h2": "Wagner Steuerberatung", "mandate": "Item 7 description here", "target_word_count": 200},'
        '   {"h2": "Fazit und Empfehlung", "mandate": "Outro section", "target_word_count": 200}],'
        ' "faq_items": [{"question": "Wie viel kostet ein Steuerberater?", "answer_outline": "Stundensatz ab 80 EUR aufwaerts."}, '
        '   {"question": "Wann lohnt sich Steuerberatung?", "answer_outline": "Komplexe Faelle ab Selbstaendigkeit."}, '
        '   {"question": "Welche Qualifikationen?", "answer_outline": "Diplom plus Pruefungen."}],'
        ' "ai_tell_blocklist": ["Darueber hinaus", "Es ist wichtig zu beachten", "Zusammenfassend",'
        '   "In der heutigen Zeit", "Letztendlich", "Abschliessend", "Im Folgenden", "wie bereits erwaehnt",'
        '   "ohne Zweifel", "selbstverstaendlich", "essenziell", "ausserdem"],'
        ' "secondary_keywords": ["steuerberater hamburg vergleich", "steuerberater hamburg kosten", "kanzlei hamburg",'
        '   "steuerberatung hamburg", "buchhaltung hamburg"],'
        ' "link_plan": [{"target_url": "https://example.de/leistungen", "anchor_strategy": "branded",'
        '   "section_index": 4, "surrounding_context_requirements": "Empfehlung als Beispiel.",'
        '   "link_type": "backlink"}]}'
    )

    def fake_caller(**kwargs):
        return narrative_shaped_response

    monkeypatch.setattr(contract_generator, "load_prompt", fake_load_prompt)
    research = ResearchPayload(target_keyword="steuerberater hamburg", location_code=2276, language_code="de")
    contract = contract_generator.generate_contract(
        research,
        target_backlink_url="https://example.de/leistungen",
        editorial_angle={"format": "listicle", "title": "Die 7 besten Steuerberater in Hamburg 2026"},
        llm_caller=fake_caller,
        api_key="x",
    )
    # The defensive enforcer should have flipped this to a real listicle
    # contract: format=listicle, listicle_plan with the 7 middle sections as
    # ranked items, sections collapsed to [intro, outro].
    assert contract.format.value == "listicle"
    assert contract.listicle_plan is not None
    assert contract.listicle_plan.item_count == 7
    assert "Mueller and Partner" in contract.listicle_plan.items
    assert "Wagner Steuerberatung" in contract.listicle_plan.items
    assert len(contract.sections) == 2
    assert contract.sections[0].h2.startswith("Einleitung")
    assert contract.sections[1].h2.startswith("Fazit")
    assert contract.schema_spec.item_list is True


def test_contract_generator_picks_v2_prompt_for_listicle_angle(monkeypatch) -> None:
    from creator.api import contract_generator
    from creator.api.research import ResearchPayload

    captured = {}

    def fake_load_prompt(name, version, language=None):
        captured["version"] = version
        return contract_generator.Prompt(
            name=name,
            version=version or "v1",
            language=language,
            body="STUB BODY",
            metadata={},
        )

    def fake_caller(**kwargs):
        captured["called"] = True
        # Returning empty JSON forces the contract step to fail validation,
        # but we only care about prompt selection.
        raise contract_generator.LLMError("stub-stop")

    monkeypatch.setattr(contract_generator, "load_prompt", fake_load_prompt)
    research = ResearchPayload(
        target_keyword="kw",
        location_code=2276,
        language_code="de",
    )
    with pytest.raises(contract_generator.LLMError):
        contract_generator.generate_contract(
            research,
            target_backlink_url="https://example.de/page",
            editorial_angle={"format": "listicle", "title": "X"},
            llm_caller=fake_caller,
            api_key="x",
        )
    assert captured.get("version") == "v2"
