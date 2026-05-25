from __future__ import annotations

import pytest

from creator.api import contract_generator
from creator.api.contract import (
    ArticleFormat,
    ContentContract,
    EntityRequirement,
    GermanTone,
    LinkTarget,
    SearchIntent,
    SectionPlan,
    ServiceType,
)
from creator.api.contract_generator import (
    brand_name_from_url,
    build_user_prompt,
    generate_contract,
)
from creator.api import eval_harness
from creator.api.eval_harness import evaluate
from creator.api.prompt_registry import Prompt
from creator.api.research import ResearchPayload
from creator.api.section_writer import build_user_prompt as section_user_prompt


# ---- helpers ----------------------------------------------------------------


def _research() -> ResearchPayload:
    return ResearchPayload(
        target_keyword="kinderbrille",
        location_code=2276,
        language_code="de",
    )


def _article_contract(**overrides) -> ContentContract:
    base = dict(
        target_keyword="kinderbrille",
        intent=SearchIntent.COMMERCIAL,
        target_audience="Eltern von Schulkindern",
        word_count_target=1200,
        h1="Kinderbrille auf Rezept: Was Eltern wissen müssen",
        meta_title="Kinderbrille auf Rezept: Was zahlt die Krankenkasse 2026",
        meta_description="Kinderbrille auf Rezept: Wann die Krankenkasse zahlt, welche Kosten bleiben und worauf Eltern beim Kauf achten sollten — der kompakte Ratgeber.",
        slug="kinderbrille-auf-rezept",
        sections=[
            SectionPlan(h2="Einleitung", mandate="Kontext und Problem", target_word_count=200),
            SectionPlan(h2="Worauf achten", mandate="Auswahlkriterien", target_word_count=300),
            SectionPlan(h2="Fazit", mandate="Zusammenfassung", target_word_count=200),
        ],
        tone=GermanTone.SIE,
    )
    base.update(overrides)
    return ContentContract(**base)


def _valid_llm_payload() -> dict:
    return {
        "target_keyword": "kinderbrille",
        "secondary_keywords": ["kinderbrille rezept", "brille kind kasse"],
        "intent": "commercial",
        "tone": "sie",
        "target_audience": "Eltern von Schulkindern",
        "word_count_target": 1200,
        "h1": "Kinderbrille auf Rezept: Was Eltern wissen müssen",
        "meta_title": "Kinderbrille auf Rezept: Was zahlt die Kasse 2026 wirklich",
        "meta_description": "Kinderbrille auf Rezept: Wann die Krankenkasse zahlt, welche Kosten bleiben und worauf Eltern beim Kauf achten sollten — der kompakte Ratgeber dazu.",
        "slug": "kinderbrille-auf-rezept",
        "sections": [
            {"h2": "Einleitung", "mandate": "Kontext und Problem aufzeigen.", "target_word_count": 200},
            {"h2": "Worauf achten", "mandate": "Auswahlkriterien detailliert.", "target_word_count": 300},
            {"h2": "Fazit", "mandate": "Zusammenfassung und Empfehlung.", "target_word_count": 200},
        ],
        "faq_items": [
            {"question": "Zahlt die Kasse?", "answer_outline": "Ja, bis 18 Jahre anteilig."},
        ],
        "required_entities": [],
        "link_plan": [
            {
                "target_url": "https://brillenhaus24.de/kinder",
                "anchor_strategy": "branded",
                "section_index": 1,
                "surrounding_context_requirements": "Beispiel im Auswahl-Abschnitt.",
                "link_type": "backlink",
            }
        ],
        "schema_spec": {"article": True, "faq_page": True},
        "ai_tell_blocklist": ["Darüber hinaus", "Letztendlich"],
        "contract_version": "v1",
    }


def _fake_load_prompt(name, version=None, language=None):
    return Prompt(name=name, version=version or "v1", language=language, body="STUB", metadata={})


# ---- contract validators ----------------------------------------------------


def test_brand_mention_rejects_nonempty_link_plan():
    with pytest.raises(ValueError, match="empty link_plan"):
        _article_contract(
            service_type=ServiceType.BRAND_MENTION,
            brand_name="Brillenhaus24",
            link_plan=[
                LinkTarget(
                    target_url="https://brillenhaus24.de",
                    anchor_strategy="contextual",
                    section_index=1,
                    surrounding_context_requirements="x",
                    link_type="backlink",
                )
            ],
        )


def test_article_rejects_branded_anchor():
    with pytest.raises(ValueError, match="branded"):
        _article_contract(
            link_plan=[
                LinkTarget(
                    target_url="https://brillenhaus24.de",
                    anchor_strategy="branded",
                    section_index=1,
                    surrounding_context_requirements="x",
                    link_type="backlink",
                )
            ],
        )


def test_brand_mention_empty_link_plan_is_valid():
    contract = _article_contract(service_type=ServiceType.BRAND_MENTION, brand_name="Brillenhaus24")
    assert contract.service_type == ServiceType.BRAND_MENTION
    assert contract.link_plan == []
    assert contract.brand_name == "Brillenhaus24"


# ---- brand_name_from_url ----------------------------------------------------


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://www.brillenhaus24.de/kinder", "Brillenhaus24"),
        ("https://berlin-immobilien.de", "Berlin Immobilien"),
        ("steuerkanzlei-mueller.de", "Steuerkanzlei Mueller"),
    ],
)
def test_brand_name_from_url(url, expected):
    assert brand_name_from_url(url) == expected


# ---- service directive injection (contract user prompt) ---------------------


def test_contract_prompt_article_directive_de():
    prompt = build_user_prompt(
        _research(),
        target_backlink_url="https://brillenhaus24.de/kinder",
        service_type=ServiceType.ARTICLE,
    )
    assert "SERVICE-MODUS: ARTIKEL" in prompt
    assert "VERBOTEN" in prompt
    assert "branded" in prompt


def test_contract_prompt_brand_mention_directive_de():
    prompt = build_user_prompt(
        _research(),
        target_backlink_url="https://brillenhaus24.de/kinder",
        service_type=ServiceType.BRAND_MENTION,
        brand_name="Brillenhaus24",
    )
    assert "MARKENERWÄHNUNG" in prompt
    assert "Brillenhaus24" in prompt
    assert "link_plan` MUSS leer" in prompt


def test_contract_prompt_brand_mention_directive_fr():
    prompt = build_user_prompt(
        _research(),
        target_backlink_url="https://brillenhaus24.de/kinder",
        language="fr",
        service_type=ServiceType.BRAND_MENTION,
        brand_name="Brillenhaus24",
    )
    assert "MENTION DE MARQUE" in prompt
    assert "AUCUN lien" in prompt


# ---- generate_contract healing ---------------------------------------------


def test_generate_contract_brand_mention_strips_link_and_adds_entity(monkeypatch):
    monkeypatch.setattr(contract_generator, "load_prompt", _fake_load_prompt)
    contract = generate_contract(
        _research(),
        target_backlink_url="https://brillenhaus24.de/kinder",
        service_type=ServiceType.BRAND_MENTION,
        brand_name="Brillenhaus24",
        llm_caller=lambda **kw: __import__("json").dumps(_valid_llm_payload()),
        api_key="x",
    )
    assert contract.service_type == ServiceType.BRAND_MENTION
    assert contract.link_plan == []
    assert contract.brand_name == "Brillenhaus24"
    assert any(e.name == "Brillenhaus24" for e in contract.required_entities)


def test_generate_contract_article_coerces_branded_anchor(monkeypatch):
    monkeypatch.setattr(contract_generator, "load_prompt", _fake_load_prompt)
    contract = generate_contract(
        _research(),
        target_backlink_url="https://brillenhaus24.de/kinder",
        service_type=ServiceType.ARTICLE,
        llm_caller=lambda **kw: __import__("json").dumps(_valid_llm_payload()),
        api_key="x",
    )
    assert contract.service_type == ServiceType.ARTICLE
    assert contract.brand_name is None
    assert len(contract.link_plan) == 1
    assert contract.link_plan[0].anchor_strategy == "contextual"


def test_generate_contract_brand_mention_derives_brand_when_absent(monkeypatch):
    monkeypatch.setattr(contract_generator, "load_prompt", _fake_load_prompt)
    contract = generate_contract(
        _research(),
        target_backlink_url="https://brillenhaus24.de/kinder",
        service_type=ServiceType.BRAND_MENTION,
        llm_caller=lambda **kw: __import__("json").dumps(_valid_llm_payload()),
        api_key="x",
    )
    assert contract.brand_name == "Brillenhaus24"


# ---- writer service directive ----------------------------------------------


def test_section_writer_article_directive():
    contract = _article_contract(
        link_plan=[
            LinkTarget(
                target_url="https://brillenhaus24.de/kinder",
                anchor_strategy="contextual",
                section_index=1,
                surrounding_context_requirements="x",
                link_type="backlink",
            )
        ]
    )
    prompt = section_user_prompt(contract=contract, section_index=1)
    assert "SERVICE-MODUS ARTIKEL" in prompt
    assert "NIEMALS offen" in prompt


def test_section_writer_brand_mention_directive():
    contract = _article_contract(
        service_type=ServiceType.BRAND_MENTION,
        brand_name="Brillenhaus24",
        required_entities=[EntityRequirement(name="Brillenhaus24", placement_hint="in section 1")],
    )
    prompt = section_user_prompt(contract=contract, section_index=1)
    assert "MARKENERWÄHNUNG" in prompt
    assert "Brillenhaus24" in prompt


# ---- eval branching ---------------------------------------------------------


_ARTICLE_HIDDEN_HTML = (
    "<h1>Kinderbrille auf Rezept</h1>"
    "<p>Kinderbrille kaufen lohnt sich. Mehr dazu in diesem "
    '<a href="https://brillenhaus24.de/kinder">ausführlichen Ratgeber</a> zum Thema.</p>'
)
_ARTICLE_NAMED_HTML = (
    "<h1>Kinderbrille auf Rezept</h1>"
    "<p>Anbieter wie Brillenhaus24 verkaufen Kinderbrille günstig. "
    '<a href="https://brillenhaus24.de/kinder">Brillenhaus24</a> ist bekannt.</p>'
)


def test_eval_article_hidden_backlink_passes():
    contract = _article_contract(
        link_plan=[
            LinkTarget(
                target_url="https://brillenhaus24.de/kinder",
                anchor_strategy="contextual",
                section_index=1,
                surrounding_context_requirements="x",
                link_type="backlink",
            )
        ]
    )
    report = evaluate(
        article_html=_ARTICLE_HIDDEN_HTML,
        contract=contract,
        host_domain="",
        meta_title=contract.meta_title,
        meta_description=contract.meta_description,
        target_url="https://brillenhaus24.de/kinder",
    )
    names = {c.name: c for c in report.deterministic}
    assert "hidden_backlink" in names
    assert names["hidden_backlink"].passed
    assert "brand_mention" not in names


def test_eval_article_hidden_backlink_fails_when_target_named():
    contract = _article_contract(
        link_plan=[
            LinkTarget(
                target_url="https://brillenhaus24.de/kinder",
                anchor_strategy="contextual",
                section_index=1,
                surrounding_context_requirements="x",
                link_type="backlink",
            )
        ]
    )
    report = evaluate(
        article_html=_ARTICLE_NAMED_HTML,
        contract=contract,
        host_domain="",
        meta_title=contract.meta_title,
        meta_description=contract.meta_description,
        target_url="https://brillenhaus24.de/kinder",
    )
    names = {c.name: c for c in report.deterministic}
    assert names["hidden_backlink"].passed is False


_BRAND_OK_HTML = (
    "<h1>Kinderbrille auf Rezept</h1>"
    "<p>Anbieter wie Brillenhaus24 zeigen, dass eine Kinderbrille gut sein kann. Mehr Kontext folgt.</p>"
)
_BRAND_LINKED_HTML = (
    "<h1>Kinderbrille auf Rezept</h1>"
    '<p>Bei <a href="https://brillenhaus24.de/kinder">Brillenhaus24</a> gibt es eine Kinderbrille.</p>'
)


def test_eval_brand_mention_passes_when_named_and_unlinked():
    contract = _article_contract(service_type=ServiceType.BRAND_MENTION, brand_name="Brillenhaus24")
    report = evaluate(
        article_html=_BRAND_OK_HTML,
        contract=contract,
        host_domain="",
        meta_title=contract.meta_title,
        meta_description=contract.meta_description,
        target_url="https://brillenhaus24.de/kinder",
    )
    names = {c.name: c for c in report.deterministic}
    assert "brand_mention" in names
    assert names["brand_mention"].passed
    assert "link_counts" not in names
    assert "hidden_backlink" not in names


def test_strip_links_to_host_unwraps_target_anchor():
    from creator.api.pipeline_runner import _strip_links_to_host

    html = (
        '<p>Bei <a href="https://brillenhaus24.de/kinder">Brillenhaus24</a> gibt es Brillen. '
        'Siehe auch <a href="https://example.org/ratgeber">Ratgeber</a>.</p>'
    )
    cleaned, count = _strip_links_to_host(html, "brillenhaus24.de")
    assert count == 1
    assert "<a href=\"https://brillenhaus24.de" not in cleaned
    assert "Brillenhaus24" in cleaned  # inner text preserved
    assert 'href="https://example.org/ratgeber"' in cleaned  # unrelated link kept


def test_eval_brand_mention_fails_when_target_linked():
    contract = _article_contract(service_type=ServiceType.BRAND_MENTION, brand_name="Brillenhaus24")
    report = evaluate(
        article_html=_BRAND_LINKED_HTML,
        contract=contract,
        host_domain="",
        meta_title=contract.meta_title,
        meta_description=contract.meta_description,
        target_url="https://brillenhaus24.de/kinder",
    )
    names = {c.name: c for c in report.deterministic}
    assert names["brand_mention"].passed is False
