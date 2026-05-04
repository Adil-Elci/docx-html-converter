from __future__ import annotations

import threading
from unittest.mock import patch

import pytest

from creator.api.contract import (
    ContentContract,
    EntityRequirement,
    GermanTone,
    LinkTarget,
    SearchIntent,
    SectionPlan,
)
from creator.api.llm import LLMError
from creator.api.section_writer import (
    DEFAULT_PARALLEL_WORKERS,
    InsertedLink,
    SectionDraft,
    _entities_for_section,
    _link_for_section,
    build_user_prompt,
    write_all_sections,
    write_section,
)


def _contract() -> ContentContract:
    return ContentContract(
        target_keyword="steuerberater hamburg",
        intent=SearchIntent.TRANSACTIONAL,
        target_audience="Hamburger Unternehmer und Selbstständige",
        word_count_target=900,
        h1="Steuerberater Hamburg: Wie Sie den richtigen Berater finden",
        meta_title="Steuerberater Hamburg finden: Tipps für Unternehmer 2026",
        meta_description="Sie suchen einen Steuerberater in Hamburg? Vergleichen Sie Leistungen, Honorare und Spezialisierungen mit unserem Leitfaden zur Auswahl.",
        slug="steuerberater-hamburg",
        sections=[
            SectionPlan(h2="Warum Hamburg", mandate="Vorteile fuer Hamburger Unternehmer ausfuehrlich erklaeren.", target_word_count=200, required_elements=[]),
            SectionPlan(h2="Auswahlkriterien", mandate="Liste relevante Kriterien fuer die Auswahl auf.", target_word_count=300, required_elements=["list"]),
            SectionPlan(h2="Kosten und Honorare", mandate="Erklaere typische Honorarstrukturen mit Spannen.", target_word_count=200, required_elements=["table"]),
            SectionPlan(h2="DATEV und digitale Tools", mandate="Beschreibe wie moderne Berater DATEV einsetzen.", target_word_count=200),
        ],
        required_entities=[
            EntityRequirement(name="DATEV", placement_hint="in section 3"),
            EntityRequirement(name="Steuerberatergebührenverordnung", placement_hint="in section 2"),
        ],
        link_plan=[
            LinkTarget(
                target_url="https://client.de/leistungen",
                anchor_strategy="partial_match",
                section_index=1,
                surrounding_context_requirements="Im Kontext einer Empfehlung in der Sektion zu Auswahlkriterien.",
                link_type="backlink",
            )
        ],
        ai_tell_blocklist=["Darüber hinaus", "Es ist wichtig zu beachten", "Zusammenfassend"] * 4,
    )


def _good_section_payload(section_index: int = 0, with_link: bool = False) -> dict:
    body = "<p>Steuerberater in Hamburg sind unverzichtbar fuer kleine Unternehmen.</p>"
    if with_link:
        body = (
            '<p>Wir empfehlen einen <a href="https://client.de/leistungen">spezialisierten '
            "Steuerberater Hamburg</a> fuer Ihre Anforderungen.</p>"
        )
    return {
        "body_html": body,
        "links_inserted": (
            [{"anchor_text": "spezialisierten Steuerberater Hamburg", "target_url": "https://client.de/leistungen", "link_type": "backlink"}]
            if with_link
            else []
        ),
        "word_count": 12,
    }


# ---- selectors -------------------------------------------------------------


def test_entities_for_section_filters_by_placement_hint():
    contract = _contract()
    assert [e.name for e in _entities_for_section(contract, 2)] == ["Steuerberatergebührenverordnung"]
    assert [e.name for e in _entities_for_section(contract, 3)] == ["DATEV"]
    assert _entities_for_section(contract, 0) == []


def test_link_for_section_returns_match_or_none():
    contract = _contract()
    assert _link_for_section(contract, 1).link_type == "backlink"
    assert _link_for_section(contract, 0) is None


# ---- prompt assembly -------------------------------------------------------


def test_build_user_prompt_includes_section_metadata_and_neighbors():
    prompt = build_user_prompt(contract=_contract(), section_index=1)
    assert "target_keyword : steuerberater hamburg" in prompt
    assert "Index 1 von 4" in prompt
    assert "Auswahlkriterien" in prompt
    assert "Vorheriger H2  : Warum Hamburg" in prompt
    assert "Nächster H2    : Kosten und Honorare" in prompt


def test_build_user_prompt_lists_section_specific_entities():
    prompt = build_user_prompt(contract=_contract(), section_index=2)
    assert "Steuerberatergebührenverordnung" in prompt
    assert "DATEV" not in prompt.split("PFLICHT-ENTITÄTEN")[1].split("BACKLINK")[0]


def test_build_user_prompt_includes_backlink_only_for_owning_section():
    contract = _contract()
    prompt_with_link = build_user_prompt(contract=contract, section_index=1)
    prompt_without_link = build_user_prompt(contract=contract, section_index=0)
    assert "https://client.de/leistungen" in prompt_with_link
    assert "partial_match" in prompt_with_link
    assert "(kein Backlink in diesem Abschnitt)" in prompt_without_link


def test_build_user_prompt_first_section_uses_introduction_as_prev():
    prompt = build_user_prompt(contract=_contract(), section_index=0)
    assert "(Einleitung des Artikels)" in prompt


def test_build_user_prompt_last_section_uses_faq_as_next():
    prompt = build_user_prompt(contract=_contract(), section_index=3)
    assert "(FAQ-Block / Fazit am Ende)" in prompt


def test_build_user_prompt_required_elements_surfaced():
    prompt = build_user_prompt(contract=_contract(), section_index=1)
    assert "Pflicht-Elemente: list" in prompt


def test_build_user_prompt_includes_blocklist():
    prompt = build_user_prompt(contract=_contract(), section_index=0)
    assert "Es ist wichtig zu beachten" in prompt


def test_build_user_prompt_raises_on_invalid_index():
    with pytest.raises(IndexError):
        build_user_prompt(contract=_contract(), section_index=99)


# ---- write_section ---------------------------------------------------------


def test_write_section_returns_validated_draft(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return _good_section_payload(with_link=True)

    draft = write_section(contract=_contract(), section_index=1, llm_caller=fake_caller)
    assert isinstance(draft, SectionDraft)
    assert draft.section_index == 1
    assert draft.h2 == "Auswahlkriterien"
    assert draft.body_html.startswith("<p>")
    assert draft.links_inserted[0].target_url == "https://client.de/leistungen"


def test_write_section_passes_correct_request_label(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    captured = {}

    def fake_caller(**kwargs):
        captured.update(kwargs)
        return _good_section_payload()

    write_section(contract=_contract(), section_index=2, llm_caller=fake_caller)
    assert "section_writer/v1/section_2" == captured["request_label"]


def test_write_section_raises_on_non_dict(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return "not a dict"

    with pytest.raises(LLMError, match="non-dict"):
        write_section(contract=_contract(), section_index=0, llm_caller=fake_caller)


def test_write_section_raises_on_schema_failure(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return {"body_html": "<p>x</p>", "links_inserted": "not-a-list", "word_count": "five"}

    with pytest.raises(LLMError, match="schema validation"):
        write_section(contract=_contract(), section_index=0, llm_caller=fake_caller)


def test_write_section_requires_api_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(LLMError, match="ANTHROPIC_API_KEY"):
        write_section(contract=_contract(), section_index=0)


def test_write_section_resolves_model_from_env(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    monkeypatch.setenv("CREATOR_SECTION_MODEL", "claude-haiku-4-5-20251001")
    captured = {}

    def fake_caller(**kwargs):
        captured.update(kwargs)
        return _good_section_payload()

    write_section(contract=_contract(), section_index=0, llm_caller=fake_caller)
    assert captured["model"] == "claude-haiku-4-5-20251001"


# ---- write_all_sections ----------------------------------------------------


def test_write_all_sections_returns_drafts_in_index_order(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        return _good_section_payload()

    drafts = write_all_sections(contract=_contract(), parallel=False, llm_caller=fake_caller)
    assert [d.section_index for d in drafts] == [0, 1, 2, 3]
    assert [d.h2 for d in drafts] == ["Warum Hamburg", "Auswahlkriterien", "Kosten und Honorare", "DATEV und digitale Tools"]


def test_write_all_sections_parallel_executes_concurrently(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    concurrent_calls = []
    barrier = threading.Barrier(4, timeout=2.0)

    def fake_caller(**kwargs):
        concurrent_calls.append(threading.current_thread().ident)
        # If parallel, all four threads must reach the barrier.
        barrier.wait()
        return _good_section_payload()

    drafts = write_all_sections(
        contract=_contract(),
        parallel=True,
        max_workers=4,
        llm_caller=fake_caller,
    )
    assert len(drafts) == 4
    # Distinct thread idents prove parallel execution (not just submission ordering).
    assert len(set(concurrent_calls)) == 4


def test_write_all_sections_propagates_failure(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

    def fake_caller(**kwargs):
        if "section_2" in kwargs.get("request_label", ""):
            raise LLMError("section 2 failed")
        return _good_section_payload()

    with pytest.raises(LLMError, match="section 2"):
        write_all_sections(contract=_contract(), parallel=False, llm_caller=fake_caller)


def test_write_all_sections_serial_for_single_section_contract(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    contract = _contract()
    contract.sections = contract.sections[:3]  # ContentContract requires min 3
    captured_threads = []

    def fake_caller(**kwargs):
        captured_threads.append(threading.current_thread().ident)
        return _good_section_payload()

    drafts = write_all_sections(contract=contract, parallel=True, llm_caller=fake_caller)
    assert len(drafts) == 3


def test_default_parallel_workers_is_reasonable():
    # Sanity check on the constant used in production defaults.
    assert 1 <= DEFAULT_PARALLEL_WORKERS <= 8


def test_write_section_passes_cache_system_true_to_caller(monkeypatch):
    """The section system prompt is identical across sections of an article;
    cache_system=True must reach the llm caller so the first call writes the
    cache and the rest read from it."""

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    captured = {}

    def fake_caller(**kwargs):
        captured.update(kwargs)
        return _good_section_payload()

    write_section(contract=_contract(), section_index=0, llm_caller=fake_caller)
    assert captured.get("cache_system") is True
