import json

import pytest

from creator.api.pipeline import (
    CreatorError,
    DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS,
    GOOGLE_SUGGEST_CACHE,
    KEYWORD_MIN_SECONDARY,
    _align_primary_keyword_to_topic,
    _build_deterministic_article_plan,
    _build_deterministic_title_package,
    _build_deterministic_meta_description,
    _build_deterministic_outline,
    _build_pipeline_execution_policy,
    _build_phase4_fallback_outline,
    _build_site_snapshot,
    _compact_pair_fit_profile,
    _build_keyword_query_variants,
    _ensure_primary_keyword_in_intro,
    _format_faq_question,
    _insert_backlink,
    _trim_article_to_word_limit,
    _discover_keyword_candidates,
    _derive_trend_query_family,
    _ensure_faq_candidates,
    _fetch_google_de_suggestions,
    _generate_search_informed_faqs,
    _generate_article_by_sections,
    _inject_faq_section,
    _merge_phase2_analysis,
    _normalize_section_html,
    _normalize_writer_html_fragment,
    _format_content_brief_prompt_text,
    _pair_fit_normalize_llm_payload,
    _pair_fit_cache_payload_is_usable,
    _repair_keyword_context_gaps,
    _render_article_from_plan,
    _run_pair_fit_reasoning,
    _sanitize_editorial_phrase,
    _normalize_faq_section_questions,
    run_creator_pipeline,
    _select_keywords,
    _structured_content_mode,
    _trend_entry_is_fresh,
    _validate_contextual_alignment,
    _validate_phrase_integrity,
    _validate_seo_metadata,
    _validate_language_and_conclusion,
    _validate_keyword_coverage,
    _validate_section_substance,
)
from creator.api.validators import word_count_from_html


def test_build_keyword_query_variants_contract():
    queries = _build_keyword_query_variants(
        topic="Baby vorbereiten Checkliste",
        primary_hint="Kliniktasche Checkliste",
        allowed_topics=["Geburt vorbereiten und Organisation"],
        max_queries=8,
    )

    assert queries
    assert len(queries) <= 8
    assert queries[0] == "baby vorbereiten checkliste"
    assert "baby vorbereiten checkliste tipps" in queries
    assert any(item.startswith("was ist ") for item in queries)


def test_select_keywords_contract():
    result = _select_keywords(
        topic="Eltern-Sucht in der Schwangerschaft",
        llm_primary="Eltern-Sucht Schwangerschaft",
        llm_secondary=[
            "Auswirkungen auf Familienbeziehungen",
            "Unterstuetzung fuer betroffene Familien",
            "Praevention bei Suchterkrankung",
        ],
        keyword_cluster=["eltern", "schwangerschaft", "familie", "sucht"],
        allowed_topics=[
            "Hilfsangebote fuer Familien in Krisensituationen",
            "Psychische Gesundheit waehrend der Schwangerschaft",
        ],
        trend_candidates=[
            "eltern sucht schwangerschaft",
            "hilfe fuer suchtbelastete familien",
            "auswirkungen eltern sucht kinder",
            "beratung bei suchterkrankung in der familie",
        ],
        faq_candidates=[
            "was ist eltern sucht in der schwangerschaft",
            "wie wirkt sich eltern sucht auf kinder aus",
            "wann brauchen familien professionelle hilfe",
        ],
    )

    assert isinstance(result["primary_keyword"], str) and result["primary_keyword"].strip()
    assert KEYWORD_MIN_SECONDARY <= len(result["secondary_keywords"]) <= 6
    assert len(set(result["secondary_keywords"])) == len(result["secondary_keywords"])
    assert 1 <= len(result["faq_candidates"]) <= 5
    assert all(isinstance(item, str) and item.strip() for item in result["faq_candidates"])


def test_select_keywords_filters_low_signal_secondary_phrases():
    result = _select_keywords(
        topic="Kinder Sonnenbrillen im Sommer",
        llm_primary="kinder sonnenbrillen",
        llm_secondary=[
            "spannende und aktuelle themen",
            "wertvolle infos fuer eltern",
            "uv schutz fuer kinderaugen",
            "sonnenbrillen fuer familienausfluege",
        ],
        keyword_cluster=["kinder", "sonnenbrillen", "uv schutz", "sommer"],
        allowed_topics=[
            "Spannende und aktuelle Themen",
            "Kinderaugen vor UV Strahlung schuetzen",
            "Familienalltag im Sommer",
        ],
        trend_candidates=[
            "kinder sonnenbrillen uv schutz",
            "uv schutz fuer kinderaugen",
            "sonnenbrillen fuer familienausfluege",
            "kinderaugen sommer schutz",
        ],
        faq_candidates=[
            "was ist bei sonnenbrillen fuer kinder wichtig",
            "wie schuetzt man kinderaugen im sommer",
            "wann brauchen kinder uv schutz",
        ],
    )

    assert "spannende und aktuelle themen" not in result["secondary_keywords"]
    assert "wertvolle infos fuer eltern" not in result["secondary_keywords"]


def test_select_keywords_filters_unrelated_secondary_topics():
    result = _select_keywords(
        topic="Kinder Sonnenbrillen im Sommer",
        llm_primary="kinder sonnenbrillen",
        llm_secondary=[
            "die haarbalance nach der schwangerschaft wiederfinden",
            "du hast fragen oder interesse an einer zusammenarbeit",
            "uv schutz fuer kinderaugen",
            "kindersonnenbrillen fuer den alltag",
        ],
        keyword_cluster=["kinder", "sonnenbrillen", "uv schutz", "sommer"],
        allowed_topics=[
            "Die Haarbalance nach der Schwangerschaft wiederfinden",
            "Du hast Fragen oder Interesse an einer Zusammenarbeit",
            "Kinderaugen vor UV Strahlung schuetzen",
        ],
        trend_candidates=[
            "uv schutz fuer kinderaugen",
            "kindersonnenbrillen fuer den alltag",
            "kinder sonnenbrillen sommer",
            "kinderaugen sonne schutz",
        ],
        faq_candidates=[
            "was ist bei sonnenbrillen fuer kinder wichtig",
            "wie schuetzt man kinderaugen im sommer",
            "wann brauchen kinder uv schutz",
        ],
    )

    assert "die haarbalance nach der schwangerschaft wiederfinden" not in result["secondary_keywords"]
    assert "du hast fragen oder interesse an einer zusammenarbeit" not in result["secondary_keywords"]


def test_select_keywords_keeps_topic_focused_primary_keyword():
    result = _select_keywords(
        topic="Kinder Sonnenbrillen und UV Schutz",
        llm_primary="eltern sucht ratgeber erziehung familie kinder liebe",
        llm_secondary=[
            "uv schutz fuer kinderaugen",
            "kindersonnenbrillen im sommer",
        ],
        keyword_cluster=["kinder", "sonnenbrillen", "uv schutz", "kinderaugen"],
        allowed_topics=[
            "Eltern Sucht Ratgeber Erziehung Familie Kinder Liebe",
            "Magazin fuer Familie und Alltag",
            "Kinderaugen vor UV Strahlung schuetzen",
        ],
        trend_candidates=[
            "kinder sonnenbrillen uv schutz",
            "uv schutz fuer kinderaugen",
            "kindersonnenbrillen im sommer",
        ],
        faq_candidates=[
            "was ist bei kindersonnenbrillen wichtig",
            "wie schuetzt man kinderaugen im sommer",
        ],
    )

    assert result["primary_keyword"] != "eltern sucht ratgeber erziehung familie kinder liebe"
    assert "kinder" in result["primary_keyword"]
    assert "sonnen" in result["primary_keyword"]


def test_select_keywords_prefers_target_product_phrase_for_broad_family_topic():
    result = _select_keywords(
        topic="Sonnenschutz fuer die ganze Familie",
        llm_primary="sonnenschutz fuer die ganze familie",
        llm_secondary=[],
        keyword_cluster=["kinder", "sonnenbrillen", "uv schutz", "kinderaugen"],
        allowed_topics=[
            "Familienalltag im Sommer",
            "Kinderaugen vor UV Strahlung schuetzen",
        ],
        trend_candidates=[],
        faq_candidates=[],
        target_terms=["Warenkorb (0 Artikel)", "Kinder Sonnenbrillen", "UV Schutz fuer Kinderaugen"],
        overlap_terms=["familie", "sommer"],
    )

    title_package = _build_deterministic_title_package(
        topic="Sonnenschutz fuer die ganze Familie",
        primary_keyword=result["primary_keyword"],
        secondary_keywords=result["secondary_keywords"],
        search_intent_type="commercial",
        structured_mode="none",
        current_year=2026,
    )

    assert result["primary_keyword"] == "kinder sonnenbrillen"
    assert "Warenkorb" not in " ".join(result["secondary_keywords"])
    assert title_package["h1"].startswith("Kinder Sonnenbrillen:")
    assert "Sonnenschutz Fuer Die Ganze Familie" in title_package["h1"]


def test_select_keywords_rejects_self_assessment_page_labels_as_primary_keyword():
    result = _select_keywords(
        topic="Augenschutz im Familienalltag",
        llm_primary="Augenschutz im Familienalltag",
        llm_secondary=[],
        keyword_cluster=["augen", "sonnenbrillen", "uv schutz", "familie", "kinder"],
        allowed_topics=[
            "Welcher Sonnenbrillen Typ Bin Ich",
            "Familienalltag im Sommer",
            "Gesunde Augen im Alltag",
        ],
        trend_candidates=[],
        faq_candidates=[],
        target_terms=[
            "Welcher Sonnenbrillen Typ Bin Ich",
            "Ihr Onlineshop fuer guenstige Brillen & Komplettbrillen",
            "Sonnenbrillen fuer die ganze Familie",
        ],
        overlap_terms=["familie", "alltag"],
    )

    title_package = _build_deterministic_title_package(
        topic="Augenschutz im Familienalltag",
        primary_keyword=result["primary_keyword"],
        secondary_keywords=result["secondary_keywords"],
        search_intent_type="informational",
        structured_mode="none",
        current_year=2026,
    )
    faqs = _ensure_faq_candidates("Augenschutz im Familienalltag", [], topic_signature=result["topic_signature"])
    outline = _build_deterministic_outline(
        topic="Augenschutz im Familienalltag",
        primary_keyword=result["primary_keyword"],
        secondary_keywords=result["secondary_keywords"],
        faq_candidates=result["faq_candidates"],
        structured_mode="none",
        anchor_text_final="Mehr erfahren",
        topic_signature=result["topic_signature"],
    )

    assert result["primary_keyword"] != "welcher sonnenbrillen typ bin ich"
    assert all("welcher sonnenbrillen typ bin ich" not in item for item in result["secondary_keywords"])
    assert "welcher sonnenbrillen typ bin ich" not in title_package["h1"].lower()
    assert all("welcher sonnenbrillen typ bin ich" not in question.lower() for question in faqs)
    assert all("welcher sonnenbrillen typ bin ich" not in item["h2"].lower() for item in outline["outline"])


def test_select_keywords_builds_secondary_fallbacks_without_trends():
    result = _select_keywords(
        topic="Kinder Sehprobleme erkennen und richtig reagieren",
        llm_primary="Kinder Sehprobleme erkennen und richtig reagieren",
        llm_secondary=[],
        keyword_cluster=["kinder", "sehprobleme", "augen", "kinder sehprobleme", "kinderbrillen"],
        allowed_topics=["Familienalltag", "Kindergesundheit"],
        trend_candidates=[],
        faq_candidates=[],
    )

    assert result["primary_keyword"] == "kinder sehprobleme erkennen und richtig reagieren"
    assert len(result["secondary_keywords"]) >= KEYWORD_MIN_SECONDARY
    assert any("sehprobleme" in item for item in result["secondary_keywords"])
    assert any("warnzeichen" in item or "erkennen" in item or "augenarzt" in item for item in result["secondary_keywords"])


def test_question_topic_builds_natural_title_keywords_outline_and_faq():
    topic = "Sehstärke bei Kindern: Wann braucht mein Kind eine Brille? Tipps"
    result = _select_keywords(
        topic=topic,
        llm_primary="Sehstärke bei Kindern wann braucht mein Kind eine",
        llm_secondary=[],
        keyword_cluster=["kinder", "sehprobleme", "augen", "kinderbrillen"],
        allowed_topics=[
            "Familienurlaub auf Inseln Tipps Ideen",
            "Jeans richtig kombinieren Tipps fuer jeden Stil",
            "Kinder Augen Gesundheit verstehen",
        ],
        trend_candidates=[],
        faq_candidates=[],
        target_terms=["Kinderbrillen", "Augengesundheit"],
        overlap_terms=["kinder", "gesundheit"],
        internal_link_inventory=[
            {
                "url": "https://familien4leben.com/kinderaugen-warnzeichen",
                "title": "Kinderaugen verstehen und Warnzeichen erkennen",
                "excerpt": "Welche Anzeichen fuer Sehprobleme Eltern kennen sollten",
                "categories": ["Gesundheit", "Kinder"],
            }
        ],
    )

    title_package = _build_deterministic_title_package(
        topic=topic,
        primary_keyword=result["primary_keyword"],
        secondary_keywords=result["secondary_keywords"],
        search_intent_type="informational",
        structured_mode="none",
        current_year=2026,
    )
    faqs = _ensure_faq_candidates(topic, [], topic_signature=result["topic_signature"])
    outline = _build_deterministic_outline(
        topic=topic,
        primary_keyword=result["primary_keyword"],
        secondary_keywords=result["secondary_keywords"],
        faq_candidates=result["faq_candidates"],
        structured_mode="none",
        anchor_text_final="Mehr erfahren",
        topic_signature=result["topic_signature"],
    )

    assert result["primary_keyword"] == "sehstärke bei kindern"
    assert "familienurlaub auf inseln tipps ideen" not in result["secondary_keywords"]
    assert "jeans richtig kombinieren tipps fuer jeden stil" not in result["secondary_keywords"]
    assert any(item == "sehprobleme bei kindern" for item in result["secondary_keywords"])
    assert any("kinderbrillen" in item for item in result["secondary_keywords"])
    assert title_package["h1"] == "Sehstärke Bei Kindern: Wann braucht mein Kind eine Brille?"
    assert "sehstärke bei kindern" in title_package["meta_title"].lower()
    assert title_package["slug"] == "sehstaerke-bei-kindern"
    assert faqs == [
        "Wann braucht mein Kind eine Brille?",
        "Woran erkennt man fruehzeitig Hinweise auf Sehprobleme bei kindern?",
        "Worauf sollte man bei Kinderbrillen achten?",
    ]
    assert outline["outline"][0]["h2"] == "Wann braucht mein Kind eine Brille? Einordnung und erste Schritte"
    assert "sehstärke bei kindern" in outline["outline"][1]["h2"].lower()
    assert "warnzeichen" in outline["outline"][1]["h2"].lower()
    assert outline["outline"][-2]["h2"] == "Fazit"
    assert outline["outline"][-1]["h2"] == "FAQ"


def test_normalize_writer_html_fragment_strips_promo_and_tagline_noise():
    normalized = _normalize_writer_html_fragment(
        "<p>Brillenhaus24.de – Ihr Onlineshop fuer guenstige Brillen und Komplettbrillen.</p>"
        "<p>Familien4leben zuhause gestalten glueck teilen im alltag.</p>"
        "<p>Ein sauberer, hilfreicher Absatz bleibt erhalten.</p>"
    )

    assert "Onlineshop" not in normalized
    assert "Familien4leben" not in normalized
    assert "Ein sauberer, hilfreicher Absatz bleibt erhalten." in normalized


def test_sanitize_editorial_phrase_drops_shop_promo_noise():
    assert _sanitize_editorial_phrase(
        "Brillenhaus24.de – Ihr Onlineshop fuer guenstige Brillen & Komplettbrillen",
        allow_single_token=True,
    ) == ""
    assert _sanitize_editorial_phrase(
        "Brillenhaus24.de – Ihr Onlineshop für günstige Brillen & Komplettbrillen",
        allow_single_token=True,
    ) == ""
    assert _sanitize_editorial_phrase("Welcher Sonnenbrillen Typ Bin Ich", allow_single_token=True) == ""
    assert _sanitize_editorial_phrase("Herzlich willkommen", allow_single_token=True) == ""
    assert _sanitize_editorial_phrase("Familie Amp Kinder", allow_single_token=True) == ""


def test_normalize_writer_html_fragment_strips_greeting_filler():
    normalized = _normalize_writer_html_fragment(
        "<p>Herzlich willkommen zu einem Thema, das viele Eltern beschaeftigt: passender UV-Schutz fuer Kinderaugen.</p>"
        "<p>Kinder brauchen alltagstaugliche Sonnenbrillen mit verlaesslichem UV-Schutz und stabilem Sitz.</p>"
    )

    assert "Herzlich willkommen" not in normalized
    assert "Kinder brauchen alltagstaugliche Sonnenbrillen" in normalized


def test_render_article_from_plan_formats_faq_questions_as_questions():
    article_html = _render_article_from_plan(
        article_plan={
            "h1": "Kinder Sonnenbrillen: Orientierung",
            "sections": [
                {"section_id": "section_1", "kind": "body", "h2": "Kinder sonnenbrillen: wichtige Kriterien", "h3": []},
                {"section_id": "section_2", "kind": "faq", "h2": "FAQ", "h3": ["Was ist wichtig", "Worauf sollte man achten"]},
            ],
        },
        intro_html="<p>Intro mit Kinder Sonnenbrillen.</p>",
        section_bodies={"section_1": "<p>Abschnitt.</p>"},
        faq_items=[
            {"question": "Was ist wichtig?", "answer_html": "<p>Antwort eins.</p>"},
            {"question": "Worauf sollte man achten?", "answer_html": "<p>Antwort zwei.</p>"},
        ],
    )

    assert "<h3>Was ist wichtig?</h3>" in article_html
    assert "<h3>Worauf sollte man achten?</h3>" in article_html


def test_normalize_faq_section_questions_repairs_missing_question_marks():
    html = (
        "<h1>Titel</h1><p>Einleitung.</p><h2>FAQ</h2>"
        "<h3>Was ist wichtig</h3><p>Antwort eins.</p>"
        "<h3>Worauf sollte man bei Kinder Sonnenbrillen achten</h3><p>Antwort zwei.</p>"
    )

    normalized = _normalize_faq_section_questions(html)

    assert "<h3>Was ist wichtig?</h3>" in normalized
    assert "<h3>Worauf sollte man bei Kinder Sonnenbrillen achten?</h3>" in normalized


def test_validate_phrase_integrity_rejects_greeting_entity_and_self_assessment_noise():
    html = (
        "<h1>Titel</h1>"
        "<p>Herzlich willkommen zu einem allgemeinen Einstieg fuer Familien.</p>"
        "<h2>Welcher Sonnenbrillen Typ Bin Ich</h2>"
        "<p>Familie Amp Kinder brauchen Orientierung.</p>"
        "<h2>Fazit</h2><p>Konkreter Abschluss zum Augenschutz im Familienalltag.</p>"
        "<h2>FAQ</h2>"
        "<h3>Worauf sollte man bei?</h3><p>Antwort mit ausreichend Woertern fuer eine stabile Validierung im FAQ Bereich und mehr Kontext.</p>"
        "<h3>Was ist wichtig?</h3><p>Noch eine laengere Antwort mit ausreichend Woertern und ohne weitere Stoerung.</p>"
        "<h3>Welche Ursachen sind haeufig?</h3><p>Eine weitere laengere Antwort mit ausreichend Woertern fuer die FAQ Validierung.</p>"
    )

    errors = _validate_phrase_integrity(html)

    assert "greeting_noise_detected" in errors
    assert "entity_noise_detected" in errors
    assert any(error.startswith("heading_phrase_invalid:") for error in errors)
    assert any(error.startswith("faq_question_integrity_invalid:") for error in errors)


def test_select_keywords_rejects_noisy_trend_and_allowed_topic_pollution():
    result = _select_keywords(
        topic="Kinder Sehprobleme erkennen und richtig reagieren",
        llm_primary="kinder sehprobleme erkennen",
        llm_secondary=[
            "symptome von sehproblemen bei kindern",
            "augenarzt termin mit kind vorbereiten",
        ],
        keyword_cluster=["kinder", "sehprobleme", "augen", "vorsorge"],
        allowed_topics=[
            "Familienurlaub auf Inseln Tipps Ideen",
            "Jeans richtig kombinieren Tipps fuer jeden Stil",
            "Kinder Augen Gesundheit verstehen",
        ],
        trend_candidates=[
            "kinder sehprobleme erkennen",
            "symptome von sehproblemen bei kindern",
            "augenarzt termin mit kind vorbereiten",
            "familienurlaub auf inseln tipps ideen",
            "jeans richtig kombinieren tipps fuer jeden stil",
            "wissen amp ideen",
        ],
        faq_candidates=[
            "wann sollte ein kind zum augenarzt",
            "wie erkennt man sehprobleme bei kindern",
            "was hilft bei auffaelligen sehzeichen",
        ],
    )

    assert "familienurlaub auf inseln tipps ideen" not in result["secondary_keywords"]
    assert "jeans richtig kombinieren tipps fuer jeden stil" not in result["secondary_keywords"]
    assert "wissen amp ideen" not in result["secondary_keywords"]


def test_sanitize_editorial_phrase_rejects_catalog_chrome_phrases():
    assert _sanitize_editorial_phrase("Neu im Sortiment") == ""
    assert _sanitize_editorial_phrase("Unsere Bestseller") == ""


def test_align_primary_keyword_to_topic_prefers_topic_head():
    aligned = _align_primary_keyword_to_topic(
        topic="Kinder Sonnenbrillen: Worauf Eltern beim UV Schutz achten sollten",
        current_primary="uv schutz fuer kinderaugen",
        trend_candidates=[
            "kinder sonnenbrillen",
            "kindersonnenbrillen uv schutz",
            "uv schutz fuer kinderaugen",
        ],
        keyword_cluster=["kinder", "sonnenbrillen", "uv schutz", "kinderaugen"],
    )

    assert aligned == "kinder sonnenbrillen"


def test_discover_keyword_candidates_extracts_faqs(monkeypatch):
    def fake_suggest(query, *, timeout_seconds, trend_cache_ttl_seconds, cache_metadata_collector=None):
        if query.startswith("was ist"):
            return [
                "was ist eltern sucht in der schwangerschaft",
                "was hilft bei sucht in der familie",
            ]
        return [
            "eltern sucht schwangerschaft",
            "hilfe fuer suchtbelastete familien",
            "auswirkungen eltern sucht kinder",
        ]

    monkeypatch.setattr("creator.api.pipeline._fetch_google_de_suggestions", fake_suggest)

    result = _discover_keyword_candidates(
        topic="Eltern-Sucht in der Schwangerschaft",
        primary_hint="Eltern Sucht Schwangerschaft",
        keyword_cluster=["eltern", "schwangerschaft", "familie", "sucht"],
        allowed_topics=["Hilfsangebote fuer Familien in Krisensituationen"],
        timeout_seconds=2,
        max_terms=10,
    )

    assert result["query_variants"]
    assert "eltern sucht schwangerschaft" in result["trend_candidates"]
    assert any(item.startswith("was ist ") for item in result["faq_candidates"])


def test_ensure_faq_candidates_falls_back_to_topic_questions():
    faqs = _ensure_faq_candidates("Baby vorbereiten Checkliste", [])

    assert len(faqs) == 3
    assert faqs[0].startswith("Was ist ")


def test_ensure_faq_candidates_dedupes_similar_questions():
    faqs = _ensure_faq_candidates(
        "Baby vorbereiten Checkliste",
        [
            "was ist baby vorbereiten checkliste",
            "was ist baby vorbereiten checkliste",
            "welche ursachen hat baby vorbereiten checkliste",
            "wann ist hilfe bei baby vorbereiten checkliste sinnvoll",
        ],
    )

    assert len(faqs) == 3
    assert len(set(faqs)) == 3


def test_ensure_faq_candidates_keeps_three_questions_for_question_like_topics():
    faqs = _ensure_faq_candidates("Wann ist Hilfe bei Sehproblemen sinnvoll", [])

    assert len(faqs) == 3
    assert len(set(faqs)) == 3


def test_ensure_faq_candidates_does_not_repeat_exact_topic_phrase_in_every_fallback_question():
    faqs = _ensure_faq_candidates(
        "Augenschutz im Sommerurlaub",
        [],
        topic_signature={
            "subject_phrase": "augenschutz im sommerurlaub",
            "target_terms": ["kinder sonnenbrillen"],
            "target_support_phrases": ["kinder sonnenbrillen", "uv schutz"],
            "support_phrases": ["uv schutz", "kinder sonnenbrillen"],
            "keyword_cluster_phrases": ["uv schutz"],
        },
    )

    assert len(faqs) == 3
    assert sum("augenschutz im sommerurlaub" in question.lower() for question in faqs) == 1
    assert any("kinder sonnenbrillen" in question.lower() or "uv schutz" in question.lower() for question in faqs[1:])


def test_format_faq_question_preserves_explicit_question_mark():
    assert _format_faq_question("Kindersonnenbrillen richtig waehlen?") == "Kindersonnenbrillen richtig waehlen?"


def test_inject_faq_section_after_fazit():
    outline = [
        {"h2": "Ursachen", "h3": []},
        {"h2": "Hilfen im Alltag", "h3": []},
        {"h2": "Fazit", "h3": []},
    ]
    updated = _inject_faq_section(
        outline,
        [
            "was ist eltern sucht in der schwangerschaft",
            "wie wirkt sich eltern sucht auf kinder aus",
            "wann brauchen familien professionelle hilfe",
        ],
        "Eltern-Sucht in der Schwangerschaft",
    )

    assert len(updated) == 4
    assert updated[-2]["h2"] == "Fazit"
    assert updated[-1]["h2"] == "FAQ"
    assert len(updated[-1]["h3"]) >= 2


def test_normalize_section_html_preserves_all_faq_questions():
    html = _normalize_section_html(
        "FAQ",
        [
            "Was ist wichtig?",
            "Wie finden Eltern die passende Groesse?",
            "Wann ist UV Schutz besonders wichtig?",
        ],
        "<p>Kinderaugen sind empfindlich und brauchen im Alltag guten Schutz. Eltern sollten auf Sitz, UV Filter und Einsatzbereich achten. Gerade im Sommer ist ein konsequenter Schutz wichtig.</p>",
    )

    assert html.count("<h3>") == 3
    assert word_count_from_html(html) >= 12


def test_generate_search_informed_faqs_uses_search_questions(monkeypatch):
    def fake_suggestions(query, *, timeout_seconds, trend_cache_ttl_seconds, cache_metadata_collector=None):
        return [
            "was ist kindersonnenbrillen wichtig",
            "wie schuetzt man kinderaugen im sommer",
            "wann brauchen kinder uv schutz",
        ]

    def fake_call_llm_json(**kwargs):
        prompt = str(kwargs.get("user_prompt") or "")
        assert "Germany search questions" in prompt
        assert "Article text" in prompt
        return {
            "faqs": [
                {
                    "question": "Was ist bei Kindersonnenbrillen wichtig?",
                    "answer_html": "<p>Wichtig sind UV Schutz, passender Sitz und bruchsichere Materialien, damit Kinderaugen im Alltag und bei Ausfluegen verlaesslich geschuetzt bleiben.</p>",
                    "search_reason": "haeufige Grundlagenfrage",
                },
                {
                    "question": "Wie schuetzt man Kinderaugen im Sommer?",
                    "answer_html": "<p>Eltern sollten direkte Mittagssonne meiden, Kappen nutzen und Sonnenbrillen mit hohem UV Schutz waehlen, damit die Belastung fuer empfindliche Augen sinkt.</p>",
                    "search_reason": "starker saisonaler Suchbezug",
                },
                {
                    "question": "Wann brauchen Kinder UV Schutz?",
                    "answer_html": "<p>Besonders wichtig ist UV Schutz bei intensiver Sonne, auf dem Spielplatz, im Urlaub und bei reflektierenden Flaechen wie Wasser oder hellem Boden.</p>",
                    "search_reason": "handlungsorientierte Suchintention",
                },
            ]
        }

    monkeypatch.setattr("creator.api.pipeline._fetch_google_de_suggestions", fake_suggestions)
    monkeypatch.setattr("creator.api.pipeline.call_llm_json", fake_call_llm_json)

    result = _generate_search_informed_faqs(
        article_html=(
            "<h1>Kinder Sonnenbrillen</h1><p>Kinder brauchen guten UV Schutz im Alltag.</p>"
            "<h2>Kinder Sonnenbrillen: Das Wichtigste im Ueberblick</h2><p>Text.</p>"
            "<h2>Fazit</h2><p>Text.</p><h2>FAQ</h2><p>Alt.</p>"
        ),
        topic="Kinder Sonnenbrillen im Sommer",
        primary_keyword="kinder sonnenbrillen",
        secondary_keywords=["uv schutz fuer kinderaugen", "sommer mit kindern"],
        current_faq_candidates=["was ist kindersonnenbrillen wichtig"],
        llm_api_key="test-key",
        llm_base_url="https://api.openai.com/v1",
        llm_model="gpt-4.1-mini",
        timeout_seconds=2,
        usage_collector=None,
    )

    assert len(result["faqs"]) == 3
    assert len(result["search_questions"]) >= 3
    assert result["faq_html"].count("<h3>") == 3


def test_build_deterministic_outline_produces_valid_structure():
    outline = _build_deterministic_outline(
        topic="Wie Eltern-Sucht die Schwangerschaft und Familienbeziehungen beeinflusst",
        primary_keyword="eltern sucht schwangerschaft",
        secondary_keywords=[
            "auswirkungen auf familienbeziehungen",
            "hilfe fuer betroffene familien",
        ],
        faq_candidates=[
            "Was ist Eltern-Sucht in der Schwangerschaft?",
            "Wie wirkt sich Eltern-Sucht auf Familien aus?",
            "Wann ist professionelle Hilfe sinnvoll?",
        ],
        structured_mode="none",
        anchor_text_final="Mehr erfahren",
    )

    assert outline["backlink_placement"] == "intro"
    assert outline["outline"][-2]["h2"] == "Fazit"
    assert outline["outline"][-1]["h2"] == "FAQ"
    assert 4 <= len(outline["outline"]) <= 6
    assert "wie eltern-sucht die schwangerschaft und familienbeziehungen beeinflusst" in outline["outline"][0]["h2"].lower()


def test_build_phase4_fallback_outline_recovers_invalid_llm_outline():
    outline = _build_phase4_fallback_outline(
        h1="Kinder Sehprobleme erkennen: Orientierung fuer Eltern",
        topic="Kinder Sehprobleme erkennen und richtig reagieren",
        primary_keyword="kinder sehprobleme erkennen",
        secondary_keywords=[
            "symptome von sehproblemen bei kindern",
            "kinder augen gesundheit verstehen",
            "augenarzt termin mit kind vorbereiten",
            "sehprobleme bei kindern alltag",
        ],
        faq_candidates=[
            "Wann sollte ein Kind zum Augenarzt?",
            "Wie erkennt man Sehprobleme bei Kindern?",
            "Was hilft bei auffaelligen Sehzeichen?",
        ],
        structured_mode="none",
        anchor="",
        anchor_safe=False,
        anchor_type="partial_match",
        brand_name="Brillenhaus24",
        keyword_cluster=["kinder", "sehprobleme", "augen", "vorsorge"],
        llm_out={
            "outline": [{"h2": "Nur ein Abschnitt", "h3": []}],
            "backlink_placement": "section_2",
            "anchor_text_final": "Mehr zur Kinderbrille",
        },
    )

    assert outline["backlink_placement"] == "section_2"
    assert outline["anchor_text_final"] == "Mehr zur Kinderbrille"
    assert outline["outline"][-2]["h2"] == "Fazit"
    assert outline["outline"][-1]["h2"] == "FAQ"
    assert 4 <= len(outline["outline"]) <= 6


def test_build_pipeline_execution_policy_honors_strict_failure_mode(monkeypatch):
    monkeypatch.delenv("CREATOR_STRICT_FAILURE_MODE", raising=False)
    monkeypatch.delenv("CREATOR_PHASE5_MAX_ATTEMPTS", raising=False)
    monkeypatch.delenv("CREATOR_PHASE7_REPAIR_ATTEMPTS", raising=False)

    default_policy = _build_pipeline_execution_policy()

    assert default_policy["strict_failure_mode"] is False
    assert default_policy["phase4_outline_fallback_enabled"] is True
    assert default_policy["phase5_faq_enrichment_soft_fail"] is True
    assert default_policy["phase7_keyword_context_repair_enabled"] is True

    monkeypatch.setenv("CREATOR_STRICT_FAILURE_MODE", "true")
    monkeypatch.setenv("CREATOR_PHASE5_MAX_ATTEMPTS", "5")
    monkeypatch.setenv("CREATOR_PHASE7_REPAIR_ATTEMPTS", "3")

    strict_policy = _build_pipeline_execution_policy()

    assert strict_policy["strict_failure_mode"] is True
    assert strict_policy["phase4_outline_fallback_enabled"] is False
    assert strict_policy["phase5_max_attempts"] == 1
    assert strict_policy["phase5_expand_passes"] == 0
    assert strict_policy["phase5_faq_enrichment_soft_fail"] is False
    assert strict_policy["phase6_image_soft_fail"] is False
    assert strict_policy["phase7_keyword_context_repair_enabled"] is False
    assert strict_policy["phase7_repair_attempts"] == 0


def test_build_deterministic_article_plan_assigns_structure_and_keyword_coverage():
    plan = _build_deterministic_article_plan(
        phase1={
            "brand_name": "Brillenhaus24",
            "anchor_type": "partial_match",
            "keyword_cluster": ["kinder", "sehprobleme", "augen"],
        },
        phase3={
            "final_article_topic": "Kinder Sehprobleme erkennen und richtig reagieren",
            "primary_keyword": "kinder sehprobleme erkennen",
            "secondary_keywords": [
                "symptome von sehproblemen bei kindern",
                "augenarzt termin mit kind vorbereiten",
                "kinder augen gesundheit verstehen",
                "sehprobleme bei kindern alltag",
            ],
            "faq_candidates": [
                "Wann sollte ein Kind zum Augenarzt?",
                "Wie erkennt man Sehprobleme bei Kindern?",
                "Was hilft bei auffaelligen Sehzeichen?",
            ],
            "structured_content_mode": "none",
            "title_package": {"h1": "Kinder Sehprobleme erkennen: Orientierung fuer Eltern"},
            "content_brief": {
                "audience": "Eltern und Familien",
                "publishing_signals": ["Familienalltag"],
                "target_signals": ["Kinderbrillen", "Augengesundheit"],
                "overlap_terms": ["kinder"],
            },
        },
        anchor="",
        anchor_safe=False,
    )

    assert plan["outline"][-2]["h2"] == "Fazit"
    assert plan["outline"][-1]["h2"] == "FAQ"
    assert plan["sections"][0]["kind"] == "body"
    assert plan["sections"][0]["required_keywords"]
    assert "kinder sehprobleme erkennen" not in plan["sections"][0]["required_keywords"]
    assert plan["sections"][-2]["kind"] == "fazit"
    assert "sehprobleme" in plan["sections"][-2]["required_terms"]
    assert plan["sections"][-1]["kind"] == "faq"
    assert plan["sections"][-1]["required_keywords"] == []
    assert plan["sections"][-1]["required_terms"] == []
    assert len(plan["faq_questions"]) == 3


def test_run_creator_pipeline_uses_deterministic_plan_and_single_writer_call(monkeypatch):
    monkeypatch.setenv("CREATOR_KEYWORD_TRENDS_ENABLED", "false")

    monkeypatch.setattr(
        "creator.api.pipeline._run_pair_fit_reasoning",
        lambda **kwargs: {
            "final_match_decision": "accepted",
            "backlink_fit_ok": True,
            "final_article_topic": "Kinder Sehprobleme erkennen und richtig reagieren",
            "why_this_topic_was_chosen": "Passt zum Familien- und Gesundheitskontext.",
            "best_overlap_reason": "Familienalltag und Kindergesundheit ueberlappen sinnvoll.",
            "overlap_terms": ["kinder", "gesundheit"],
            "publishing_site_contexts": ["Familienalltag"],
            "target_site_contexts": ["Kinderbrillen"],
        },
    )
    captured_labels: list[str] = []

    def fake_call_llm_text(**kwargs):
        label = str(kwargs.get("request_label") or "")
        captured_labels.append(label)
        assert label.startswith("phase5_writer")
        prompt = str(kwargs.get("user_prompt") or "")
        assert "do not force exact secondary keywords into FAQ answers" in prompt
        assert "The Fazit section body must be topic-specific, concrete, non-generic, and explicitly use at least one of its required_terms." in prompt
        plan_json = prompt.split("Plan:\n", 1)[1].split("\n\nOutput format:", 1)[0]
        plan = json.loads(plan_json)
        assert plan["sections"][-1]["required_keywords"] == []
        assert plan["sections"][-2]["kind"] == "fazit"
        assert "sehprobleme" in plan["sections"][-2]["required_terms"]
        parts = [
            "[[INTRO_HTML]]",
            (
                "<p>Auffaellige Sehzeichen bei Kindern frueh zu erkennen hilft Eltern, Beobachtungen sicher "
                "einzuordnen, klare Unterschiede festzuhalten und passende naechste Schritte ohne Alarmismus "
                "abzuleiten.</p>"
            ),
            "[[/INTRO_HTML]]",
        ]
        for section in plan["sections"]:
            if section["kind"] == "faq":
                for index, _question in enumerate(section["h3"], start=1):
                    parts.extend(
                        [
                            f"[[FAQ_{index}]]",
                                (
                                    "<p>Die Antwort erklaert den konkreten Alltag, nennt klare Beobachtungen, "
                                    "ordnet Risiken ein und zeigt Eltern, wann praktische Unterstuetzung oder "
                                    "ein Termin zur weiteren Abklaerung sinnvoll wird. Zusaetzlich werden "
                                    "naechste Schritte, typische Warnzeichen und sinnvolle Fragen fuer den "
                                    "Augenarzttermin knapp und verstaendlich zusammengefasst.</p>"
                                ),
                                f"[[/FAQ_{index}]]",
                            ]
                            )
                    continue
            required_keywords = " und ".join((section.get("required_keywords") or ["praxisnahe einordnung"])[:1])
            required_terms = " und ".join(section.get("required_terms") or ["Familienalltag", "Kinderbrillen"])
            body_html = (
                f"<p>Dieser Abschnitt gibt Eltern zu {required_keywords} konkrete Kriterien, "
                f"alltagsnahe Beobachtungen und klare Unterschiede. Gerade {required_terms} hilft dabei, "
                "nicht bei allgemeinen Aussagen zu bleiben, sondern belastbare Orientierung fuer die naechsten "
                "Schritte im Familienalltag zu gewinnen.</p>"
                "<p>Darueber hinaus zeigt der Abschnitt, welche Signale wirklich wichtig sind, wie sich "
                "das Thema praktisch einordnen laesst und warum "
                f"{required_terms} fuer eine "
                "sichere Entscheidung im Alltag relevant bleibt.</p>"
            )
            if "list" in (section.get("required_elements") or []):
                body_html += "<ul><li>Signal beobachten</li><li>Alltag dokumentieren</li></ul>"
            if "table" in (section.get("required_elements") or []):
                body_html += "<table><tr><th>Signal</th><th>Bedeutung</th></tr><tr><td>Blinzeln</td><td>Abklaeren</td></tr></table>"
            parts.extend([f"[[SECTION:{section['section_id']}]]", body_html, "[[/SECTION]]"])
        parts.extend(
            [
                "[[EXCERPT]]",
                "Konkrete Orientierung fuer Eltern bei ersten Anzeichen von Sehproblemen.",
                "[[/EXCERPT]]",
            ]
        )
        return "\n".join(parts)

    monkeypatch.setattr("creator.api.pipeline.call_llm_text", fake_call_llm_text)

    result = run_creator_pipeline(
        target_site_url="https://www.brillenhaus24.de/",
        publishing_site_url="https://familien4leben.com/",
        publishing_site_id=None,
        client_target_site_id=None,
        anchor=None,
        topic="Kinder Sehprobleme erkennen und richtig reagieren",
        exclude_topics=[],
        internal_link_inventory=[
            {
                "url": "https://familien4leben.com/gesundheit/kinderaugen-warnzeichen",
                "title": "Kinderaugen verstehen und Warnzeichen erkennen",
                "slug": "kinderaugen-warnzeichen",
                "excerpt": "Welche Anzeichen fuer Sehprobleme Eltern kennen sollten",
                "categories": ["Gesundheit", "Kinder"],
            },
            {
                "url": "https://familien4leben.com/familie/arzttermine-mit-kind",
                "title": "Arzttermine mit Kind vorbereiten",
                "slug": "arzttermine-mit-kind",
                "excerpt": "So bereiten Familien den Augenarzt Termin mit Kind vor",
                "categories": ["Familie", "Gesundheit"],
            },
        ],
        target_profile_payload={
            "normalized_url": "https://www.brillenhaus24.de/",
            "page_title": "Brillenhaus24",
            "meta_description": "Kinderbrillen und alltagstaugliche Sehhilfen.",
            "topics": ["Kinder Sehprobleme", "Kinderbrillen"],
            "contexts": ["Augengesundheit"],
            "repeated_keywords": ["kinder", "sehprobleme", "augen"],
            "services_or_products": ["Kinderbrillen"],
            "business_type": "Optiker",
            "business_intent": "commercial",
        },
        publishing_profile_payload={
            "normalized_url": "https://familien4leben.com/",
            "page_title": "Familien4Leben",
            "meta_description": "Ratgeber fuer Familien und Gesundheit im Alltag.",
            "topics": ["Familienalltag", "Kindergesundheit"],
            "contexts": ["Familienalltag"],
            "site_categories": ["Familie"],
            "topic_clusters": ["Gesundheit", "Elternratgeber"],
            "content_style": ["sachlich"],
            "content_tone": "hilfreich",
        },
        dry_run=True,
    )

    assert captured_labels == ["phase5_writer_attempt_1"]
    assert result["phase4"]["outline"][-2]["h2"] == "Fazit"
    assert result["phase4"]["outline"][-1]["h2"] == "FAQ"
    assert "<h2>FAQ</h2>" in result["phase5"]["article_html"]
    assert "<h2>Fazit</h2>" in result["phase5"]["article_html"]
    assert 'href="https://www.brillenhaus24.de/"' in result["phase5"]["article_html"]


def test_run_creator_pipeline_does_not_force_internal_links_when_inventory_has_no_relevant_matches(monkeypatch):
    monkeypatch.setenv("CREATOR_KEYWORD_TRENDS_ENABLED", "false")

    monkeypatch.setattr(
        "creator.api.pipeline._run_pair_fit_reasoning",
        lambda **kwargs: {
            "final_match_decision": "accepted",
            "backlink_fit_ok": True,
            "final_article_topic": "Sonnenschutz fuer die ganze Familie",
            "why_this_topic_was_chosen": "Familienkontext und Sonnenschutz passen grundsaetzlich zusammen.",
            "best_overlap_reason": "Sonnenschutz und Familienalltag ueberlappen.",
            "overlap_terms": ["familie", "schutz"],
            "publishing_site_contexts": ["Familienalltag"],
            "target_site_contexts": ["shopping", "outdoor"],
        },
    )

    def fake_call_llm_text(**kwargs):
        prompt = str(kwargs.get("user_prompt") or "")
        plan_json = prompt.split("Plan:\n", 1)[1].split("\n\nOutput format:", 1)[0]
        plan = json.loads(plan_json)
        parts = [
            "[[INTRO_HTML]]",
            (
                "<p>Eltern achten bei Kinder Sonnenbrillen auf UV Schutz, Passform und alltagstaugliche Materialien. "
                "Gerade an langen Sommertagen hilft eine klare Orientierung dazu, Schutzklassen, Sitz und Material "
                "nicht nur oberflaechlich zu vergleichen, sondern wirklich passend fuer Kinderaugen einzuordnen.</p>"
            ),
            "[[/INTRO_HTML]]",
        ]
        for section in plan["sections"]:
            if section["kind"] == "faq":
                for index, _question in enumerate(section["h3"], start=1):
                    parts.extend(
                        [
                            f"[[FAQ_{index}]]",
                            (
                                "<p>Kinderaugen brauchen im Freien verlaesslichen UV Schutz, eine stabile Passform "
                                "und eine Fassung, die auch beim Spielen bequem sitzt. Eltern sollten deshalb "
                                "Schutzklasse, Material, Sitz und Alltagstauglichkeit gemeinsam bewerten.</p>"
                            ),
                            f"[[/FAQ_{index}]]",
                        ]
                    )
                continue
            body_html = (
                "<p>Gute Sonnenbrillen fuer Kinder brauchen UV Schutz, bequemen Sitz und robuste Materialien fuer den Familienalltag. "
                "Eltern sollten auf klare Kennzeichnungen, eine stabile Passform beim Spielen und eine leichte Fassung achten, "
                "damit die Brille draussen wirklich getragen wird und Schutz nicht nur auf dem Etikett steht.</p>"
                "<p>Praktisch relevant sind ausserdem Schutzklasse, Materialqualitaet, seitlicher Lichtschutz und die Frage, "
                "wie gut die Brille auf Nase und Ohren sitzt. So entstehen konkrete Entscheidungskriterien statt allgemeiner "
                "Sommertipps, und Familien koennen den Kauf alltagstauglich einordnen.</p>"
            )
            if "table" in (section.get("required_elements") or []):
                body_html += (
                    "<table><tr><th>Kriterium</th><th>Worauf Eltern achten</th></tr>"
                    "<tr><td>UV Schutz</td><td>UV 400 und klare Herstellerangaben</td></tr></table>"
                )
            parts.extend(
                [
                    f"[[SECTION:{section['section_id']}]]",
                    body_html,
                    "[[/SECTION]]",
                ]
            )
        parts.extend(["[[EXCERPT]]", "Konkrete Orientierung fuer Eltern beim Kauf von Kinder Sonnenbrillen.", "[[/EXCERPT]]"])
        return "\n".join(parts)

    monkeypatch.setattr("creator.api.pipeline.call_llm_text", fake_call_llm_text)

    result = run_creator_pipeline(
        target_site_url="https://www.brillenhaus24.de/Sonnenbrille_1",
        publishing_site_url="https://familien4leben.com/",
        publishing_site_id=None,
        client_target_site_id=None,
        anchor=None,
        topic=None,
        exclude_topics=[],
        internal_link_inventory=[
            {
                "url": "https://familien4leben.com/lieferoptionen",
                "title": "Lieferoptionen fuer Familien vergleichen und sparen",
                "slug": "lieferoptionen-vergleichen",
                "excerpt": "Tipps zum Sparen beim Onlinekauf",
            },
            {
                "url": "https://familien4leben.com/hautpflege-routinen",
                "title": "Hautpflege-Routinen fuer die ganze Familie",
                "slug": "hautpflege-routinen-familie",
                "excerpt": "Pflegeideen fuer den Sommer",
            },
        ],
        target_profile_payload={
            "normalized_url": "https://www.brillenhaus24.de/Sonnenbrille_1",
            "page_title": "Sonnenbrillen fuer Kinder",
            "meta_description": "Kinder Sonnenbrillen mit UV Schutz und robusten Materialien.",
            "topics": ["Kinder Sonnenbrillen", "UV Schutz fuer Kinderaugen"],
            "contexts": ["shopping", "outdoor"],
            "repeated_keywords": ["sonnenbrillen", "kinder", "uv", "schutz"],
            "services_or_products": ["Kinder Sonnenbrillen", "Kindersonnenbrillen"],
            "business_type": "Optiker",
            "business_intent": "commercial",
        },
        publishing_profile_payload={
            "normalized_url": "https://familien4leben.com/",
            "page_title": "Familien4Leben",
            "meta_description": "Ratgeber fuer Familien und Gesundheit im Alltag.",
            "topics": ["Familienalltag", "Familienleben im Sommer"],
            "contexts": ["Familienalltag"],
            "site_categories": ["Familie"],
            "topic_clusters": ["Familienratgeber", "Sommer"],
            "content_style": ["sachlich"],
            "content_tone": "hilfreich",
        },
        dry_run=True,
    )

    assert result["debug"]["internal_linking"]["candidate_count"] == 0
    assert 'href="https://familien4leben.com/' not in result["phase5"]["article_html"]


def test_run_creator_pipeline_strict_mode_raises_phase5_writer_validation_error(monkeypatch):
    monkeypatch.setenv("CREATOR_STRICT_FAILURE_MODE", "true")
    monkeypatch.setenv("CREATOR_KEYWORD_TRENDS_ENABLED", "false")

    monkeypatch.setattr(
        "creator.api.pipeline._run_pair_fit_reasoning",
        lambda **kwargs: {
            "final_match_decision": "accepted",
            "backlink_fit_ok": True,
            "final_article_topic": "Kinder Sehprobleme erkennen und richtig reagieren",
            "why_this_topic_was_chosen": "Passt zum Familien- und Gesundheitskontext.",
            "best_overlap_reason": "Familienalltag und Kindergesundheit ueberlappen sinnvoll.",
            "overlap_terms": ["kinder", "gesundheit"],
            "publishing_site_contexts": ["Familienalltag"],
            "target_site_contexts": ["Kinderbrillen"],
        },
    )

    def fake_call_llm_text(**kwargs):
        prompt = str(kwargs.get("user_prompt") or "")
        plan_json = prompt.split("Plan:\n", 1)[1].split("\n\nOutput format:", 1)[0]
        plan = json.loads(plan_json)
        parts = [
            "[[INTRO_HTML]]",
            "<p>Kinder sehprobleme erkennen.</p>",
            "[[/INTRO_HTML]]",
        ]
        for section in plan["sections"]:
            if section["kind"] == "faq":
                for index, _question in enumerate(section["h3"], start=1):
                    parts.extend([f"[[FAQ_{index}]]", "<p>Kurz.</p>", f"[[/FAQ_{index}]]"])
                continue
            parts.extend([f"[[SECTION:{section['section_id']}]]", "<p>Kurz.</p>", "[[/SECTION]]"])
        parts.extend(["[[EXCERPT]]", "Kurz.", "[[/EXCERPT]]"])
        return "\n".join(parts)

    monkeypatch.setattr("creator.api.pipeline.call_llm_text", fake_call_llm_text)

    with pytest.raises(CreatorError, match=r"Phase 5 writer attempt 1 validation failed:"):
        run_creator_pipeline(
            target_site_url="https://www.brillenhaus24.de/",
            publishing_site_url="https://familien4leben.com/",
            publishing_site_id=None,
            client_target_site_id=None,
            anchor=None,
            topic="Kinder Sehprobleme erkennen und richtig reagieren",
            exclude_topics=[],
            internal_link_inventory=[
                {
                    "url": "https://familien4leben.com/gesundheit/kinderaugen",
                    "title": "Kinderaugen verstehen und Warnzeichen erkennen",
                    "slug": "kinderaugen-warnzeichen",
                    "excerpt": "Welche Anzeichen fuer Sehprobleme Eltern kennen sollten",
                },
                {
                    "url": "https://familien4leben.com/familie/arzttermine-mit-kind",
                    "title": "Arzttermine mit Kind vorbereiten",
                    "slug": "arzttermine-mit-kind",
                    "excerpt": "So bereiten Familien medizinische Termine mit Kindern vor",
                },
            ],
            target_profile_payload={
                "normalized_url": "https://www.brillenhaus24.de/",
                "page_title": "Brillenhaus24",
                "meta_description": "Kinderbrillen und alltagstaugliche Sehhilfen.",
                "topics": ["Kinder Sehprobleme", "Kinderbrillen"],
                "contexts": ["Augengesundheit"],
                "repeated_keywords": ["kinder", "sehprobleme", "augen"],
                "services_or_products": ["Kinderbrillen"],
                "business_type": "Optiker",
                "business_intent": "commercial",
            },
            publishing_profile_payload={
                "normalized_url": "https://familien4leben.com/",
                "page_title": "Familien4Leben",
                "meta_description": "Ratgeber fuer Familien und Gesundheit im Alltag.",
                "topics": ["Familienalltag", "Kindergesundheit"],
                "contexts": ["Familienalltag"],
                "site_categories": ["Familie"],
                "topic_clusters": ["Gesundheit", "Elternratgeber"],
                "content_style": ["sachlich"],
                "content_tone": "hilfreich",
            },
            dry_run=True,
        )


def test_ensure_primary_keyword_in_intro_injects_missing_keyword():
    html = "<h1>Titel</h1><p>Ein sachlicher Einstieg ohne exakten Suchbegriff.</p><h2>Abschnitt</h2><p>Text.</p>"

    updated = _ensure_primary_keyword_in_intro(html, "eltern sucht schwangerschaft")

    assert "Eltern Sucht Schwangerschaft ist dabei ein zentraler Aspekt." in updated
    assert "primary_keyword_missing_intro" not in _validate_keyword_coverage(
        updated,
        "eltern sucht schwangerschaft",
        ["hilfe fuer familien", "schwangerschaft belastung", "unterstuetzung", "beratung"],
    )


def test_trim_article_to_word_limit_reduces_overflow():
    html = (
        "<h1>Titel</h1>"
        "<p>Einleitung mit primaerem begriff und genug woertern fuer den einstieg ohne links.</p>"
        "<h2>Abschnitt</h2>"
        f"<p>{'wort ' * 220}</p>"
        "<h2>Fazit</h2>"
        f"<p>{'schluss ' * 120}</p>"
        "<h2>FAQ</h2>"
        "<h3>Was ist wichtig?</h3>"
        f"<p>{'antwort ' * 60}</p>"
    )

    trimmed = _trim_article_to_word_limit(html, 180)

    assert word_count_from_html(trimmed) <= 180
    assert "<h1>Titel</h1>" in trimmed


def test_insert_backlink_maps_section_placement_correctly():
    html = (
        "<h1>Titel</h1><p>Intro.</p>"
        "<h2>Erster Abschnitt</h2><p>Text eins.</p>"
        "<h2>Zweiter Abschnitt</h2><p>Text zwei.</p>"
    )

    updated = _insert_backlink(
        html,
        backlink_url="https://target.example.com",
        anchor_text="Quelle",
        placement="section_2",
    )

    assert 'href="https://target.example.com"' in updated
    first_section, second_section = updated.split("<h2>Zweiter Abschnitt</h2>")
    assert 'href="https://target.example.com"' not in first_section
    assert 'href="https://target.example.com"' in second_section


def test_fetch_google_de_suggestions_uses_cache(monkeypatch):
    GOOGLE_SUGGEST_CACHE.clear()
    calls = {"count": 0}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return ["query", ["baby vorbereiten checkliste", "kliniktasche checkliste"]]

    def fake_get(url, params, headers, timeout):
        calls["count"] += 1
        return FakeResponse()

    monkeypatch.setattr("creator.api.pipeline.requests.get", fake_get)

    first = _fetch_google_de_suggestions(
        "baby vorbereiten checkliste",
        timeout_seconds=2,
        trend_cache_ttl_seconds=DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS,
    )
    second = _fetch_google_de_suggestions(
        "baby vorbereiten checkliste",
        timeout_seconds=2,
        trend_cache_ttl_seconds=DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS,
    )

    assert first == second
    assert calls["count"] == 1


def test_fetch_google_de_suggestions_uses_fresh_db_entry(monkeypatch):
    GOOGLE_SUGGEST_CACHE.clear()
    monkeypatch.setattr(
        "creator.api.pipeline.get_keyword_trend_cache_entry",
        lambda _query: {
            "payload": {"suggestions": ["baby vorbereiten checkliste", "kliniktasche checkliste"]},
            "fetched_at": "2026-03-08T10:00:00+00:00",
            "expires_at": "2026-03-15T10:00:00+00:00",
        },
    )
    monkeypatch.setattr("creator.api.pipeline.requests.get", lambda *args, **kwargs: None)

    result = _fetch_google_de_suggestions(
        "baby vorbereiten checkliste",
        timeout_seconds=2,
        trend_cache_ttl_seconds=DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS,
    )

    assert "baby vorbereiten checkliste" in result


def test_fetch_google_de_suggestions_refreshes_stale_db_entry(monkeypatch):
    GOOGLE_SUGGEST_CACHE.clear()
    refreshed = {}

    monkeypatch.setattr(
        "creator.api.pipeline.get_keyword_trend_cache_entry",
        lambda _query: {
            "payload": {"suggestions": ["alte suchanfrage"]},
            "fetched_at": "2026-02-01T10:00:00+00:00",
            "expires_at": "2026-02-08T10:00:00+00:00",
        },
    )

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return ["query", ["neue suchanfrage", "weitere frage"]]

    monkeypatch.setattr("creator.api.pipeline.requests.get", lambda *args, **kwargs: FakeResponse())
    monkeypatch.setattr(
        "creator.api.pipeline.upsert_keyword_trend_cache_entry",
        lambda **kwargs: refreshed.update(kwargs),
    )

    result = _fetch_google_de_suggestions(
        "baby vorbereiten checkliste",
        timeout_seconds=2,
        trend_cache_ttl_seconds=DEFAULT_KEYWORD_TREND_CACHE_TTL_SECONDS,
    )

    assert result[0] == "neue suchanfrage"
    assert refreshed["normalized_seed_query"] == "baby vorbereiten checkliste"


def test_trend_entry_is_fresh_with_future_expiry():
    assert _trend_entry_is_fresh({"expires_at": "2099-01-01T00:00:00+00:00"}) is True
    assert _trend_entry_is_fresh({"expires_at": "2000-01-01T00:00:00+00:00"}) is False


def test_build_site_snapshot_aggregates_multiple_pages(monkeypatch):
    def fake_fetch_url(url, *, purpose, warnings, debug, timeout_seconds, retries):
        if url.endswith("/ratgeber"):
            return "<html><head><title>Ratgeber</title></head><body><p>Ausfuehrlicher Ratgeber zur Vorbereitung auf die Geburt.</p></body></html>"
        if url.endswith("/checkliste"):
            return "<html><head><title>Checkliste</title></head><body><p>Praktische Checkliste fuer Kliniktasche und erste Tage.</p></body></html>"
        return ""

    monkeypatch.setattr("creator.api.pipeline.fetch_url", fake_fetch_url)

    snapshot = _build_site_snapshot(
        site_url="https://publisher.example.com",
        homepage_html="<html><head><title>Startseite</title></head><body><p>Familienmagazin mit Ratgebern und alltagsnahen Tipps.</p></body></html>",
        candidate_urls=[
            "https://publisher.example.com/ratgeber",
            "https://publisher.example.com/checkliste",
        ],
        purpose_prefix="publishing_snapshot",
        warnings=[],
        debug={},
        timeout_seconds=2,
        retries=1,
        max_pages=3,
    )

    assert snapshot["content_hash"]
    assert len(snapshot["pages"]) == 3
    assert "Startseite" in snapshot["combined_text"]
    assert "Ratgeber" in snapshot["combined_text"]
    assert snapshot["sample_urls"][0] == "https://publisher.example.com"


def test_merge_phase2_analysis_keeps_cached_context_and_inventory_categories():
    merged = _merge_phase2_analysis(
        {
            "allowed_topics": ["Geburt vorbereiten"],
            "content_style_constraints": ["Sachlich und klar"],
            "internal_linking_opportunities": ["Geburtsvorbereitung -> Kliniktasche"],
            "site_summary": "Magazin fuer junge Familien",
            "site_categories": ["Familie"],
            "sample_page_titles": ["Geburt vorbereiten leicht gemacht"],
            "sample_urls": ["https://publisher.example.com/geburt-vorbereiten"],
        },
        {
            "allowed_topics": ["Kliniktasche Checkliste"],
            "content_style_constraints": ["Alltagsnah schreiben"],
            "internal_linking_opportunities": ["Kliniktasche -> Baby Erstausstattung"],
            "site_summary": "Cached summary",
            "site_categories": ["Schwangerschaft"],
            "sample_page_titles": ["Baby Erstausstattung"],
            "sample_urls": ["https://publisher.example.com/baby-erstausstattung"],
        },
        inventory_categories=["Baby"],
    )

    assert "Geburt vorbereiten" in merged["allowed_topics"]
    assert "Kliniktasche Checkliste" in merged["allowed_topics"]
    assert "Baby" in merged["site_categories"]
    assert len(merged["sample_page_titles"]) >= 2


def test_build_deterministic_title_package_targets_seo_length():
    title_package = _build_deterministic_title_package(
        topic="Baby vorbereiten Checkliste",
        primary_keyword="baby vorbereiten checkliste",
        secondary_keywords=["kliniktasche checkliste"],
        search_intent_type="informational",
        structured_mode="list",
        current_year=2026,
    )

    assert 45 <= len(title_package["meta_title"]) <= 68
    assert title_package["slug"] == "baby-vorbereiten-checkliste"


def test_build_deterministic_title_package_uses_topic_over_site_identity_keyword():
    title_package = _build_deterministic_title_package(
        topic="Kinder Sonnenbrillen: Worauf Eltern beim UV Schutz achten sollten",
        primary_keyword="eltern sucht ratgeber erziehung familie kinder liebe",
        secondary_keywords=["uv schutz fuer kinderaugen"],
        search_intent_type="informational",
        structured_mode="none",
        current_year=2026,
    )

    assert title_package["h1"].startswith("Kinder Sonnenbrillen")
    assert "Eltern Sucht Ratgeber" not in title_package["h1"]


def test_build_deterministic_title_package_avoids_dangling_truncation_and_uses_specific_primary_keyword():
    title_package = _build_deterministic_title_package(
        topic="Sonnenschutz fuer die ganze Familie",
        primary_keyword="kinder sonnenbrillen",
        secondary_keywords=["uv schutz fuer kinderaugen"],
        search_intent_type="commercial",
        structured_mode="table",
        current_year=2026,
    )

    assert "Kinder Sonnenbrillen" in title_package["h1"]
    assert not title_package["h1"].endswith(" und")
    assert not title_package["h1"].endswith(":")


def test_build_deterministic_meta_description_meets_length_contract():
    meta_description = _build_deterministic_meta_description(
        topic="Kinder Sonnenbrillen: worauf Eltern achten sollten",
        primary_keyword="kinder sonnenbrillen",
        secondary_keywords=["uv schutz fuer kinderaugen"],
        structured_mode="none",
    )

    assert 120 <= len(meta_description) <= 160
    assert "schutz fuer kinderaugen" in meta_description.lower()


def test_build_deterministic_outline_filters_noisy_target_terms_and_uses_decision_headings():
    outline = _build_deterministic_outline(
        topic="Sonnenschutz fuer die ganze Familie",
        primary_keyword="kinder sonnenbrillen",
        secondary_keywords=[
            "uv schutz fuer kinderaugen",
            "passform fuer kinder sonnenbrillen",
        ],
        faq_candidates=["Was ist wichtig?", "Welche Schutzklasse passt?", "Worauf sollten Eltern achten?"],
        structured_mode="table",
        anchor_text_final="Mehr zu Kinder Sonnenbrillen",
        topic_signature={
            "subject_phrase": "sonnenschutz fuer die ganze familie",
            "question_phrase": "",
            "target_terms": [
                "Warenkorb (0 Artikel)",
                "Brillenhaus24.de – Ihr Onlineshop fuer guenstige Brillen & Komplettbrillen",
                "Kinder Sonnenbrillen",
            ],
            "target_support_phrases": ["kinder sonnenbrillen", "uv schutz fuer kinderaugen"],
            "support_phrases": ["sonnenschutz fuer die ganze familie", "kinder sonnenbrillen"],
            "keyword_cluster_phrases": ["kinder sonnenbrillen", "uv schutz fuer kinderaugen"],
            "primary_keyword": "kinder sonnenbrillen",
        },
    )

    headings = [item["h2"] for item in outline["outline"]]
    assert all("Warenkorb" not in heading for heading in headings)
    assert all("Onlineshop" not in heading for heading in headings)
    assert any("Qualitaetsmerkmale" in heading for heading in headings)
    assert any("Kinder sonnenbrillen" in heading for heading in headings)
    assert not any("Anzeichen, Ursachen" in heading for heading in headings)


def test_build_deterministic_outline_forces_primary_keyword_into_heading_when_needed():
    outline = _build_deterministic_outline(
        topic="Sonnenschutz fuer die ganze Familie",
        primary_keyword="kinder sonnenbrillen",
        secondary_keywords=["uv schutz fuer kinderaugen"],
        faq_candidates=["Was ist wichtig?", "Welche Ursachen sind haeufig?", "Worauf sollte man achten?"],
        structured_mode="none",
        anchor_text_final="Mehr erfahren",
    )

    headings = [item["h2"] for item in outline["outline"]]
    assert any("kinder sonnenbrillen" in heading.lower() for heading in headings)


def test_structured_content_mode_detects_list_and_table_topics():
    assert _structured_content_mode("Baby vorbereiten Checkliste", "baby vorbereiten checkliste", "informational") == "list"
    assert _structured_content_mode("Geburtskosten Vergleich", "geburtskosten vergleich", "commercial") == "table"


def test_validate_seo_metadata_requires_exact_h1_and_metadata_quality():
    meta_description = _build_deterministic_meta_description(
        topic="Baby vorbereiten Checkliste",
        primary_keyword="baby vorbereiten checkliste",
        secondary_keywords=["kliniktasche checkliste", "geburt vorbereiten tipps"],
        structured_mode="list",
    )
    errors = _validate_seo_metadata(
        article_html="""
        <h1>Baby Vorbereiten Checkliste: Checkliste und Tipps fuer Betroffene und Familien</h1>
        <p>Einleitung.</p>
        <h2>Baby vorbereiten checkliste im ueberblick</h2>
        <p>Text.</p>
        <ul><li>Punkt</li></ul>
        <h2>Fazit</h2>
        <p>Fazit.</p>
        <h2>FAQ</h2>
        <h3>Was ist wichtig?</h3>
        <p>Antwort mit ausreichend Woertern fuer die Validierung des FAQ Blocks und klare Hinweise fuer Familien.</p>
        """,
        primary_keyword="baby vorbereiten checkliste",
        required_h1="Baby Vorbereiten Checkliste: Checkliste und Tipps fuer Betroffene und Familien",
        meta_title="Baby Vorbereiten Checkliste: Checkliste und Tipps fuer Familien",
        meta_description=meta_description,
        slug="baby-vorbereiten-checkliste",
        structured_mode="list",
    )

    assert errors == []


def test_derive_trend_query_family_groups_question_variant():
    assert _derive_trend_query_family("was ist baby vorbereiten checkliste") == "baby vorbereiten"


def test_validate_keyword_coverage_missing_primary_locations():
    html = """
    <h1>Auswirkungen auf Familienbeziehungen</h1>
    <p>Dieser Beitrag beleuchtet zentrale Aspekte fuer betroffene Familien.</p>
    <h2>Ursachen und Hintergruende</h2>
    <p>Viele Faktoren wirken zusammen.</p>
    <h2>Fazit</h2>
    <p>Ein guter Abschluss mit klaren Schritten.</p>
    """
    errors = _validate_keyword_coverage(
        html,
        primary_keyword="eltern sucht schwangerschaft",
        secondary_keywords=[
            "auswirkungen auf familienbeziehungen",
            "unterstuetzung fuer betroffene familien",
            "praevention bei suchterkrankung",
            "hilfsangebote fuer familien",
        ],
    )
    assert "primary_keyword_missing_h1" in errors
    assert "primary_keyword_missing_intro" in errors


def test_validate_keyword_coverage_ok():
    html = """
    <h1>Eltern Sucht Schwangerschaft: Auswirkungen und Hilfe</h1>
    <p>Eltern sucht schwangerschaft betrifft viele Familien und erfordert fruehe Hilfe.</p>
    <h2>Eltern Sucht Schwangerschaft im Alltag</h2>
    <p>Auswirkungen auf familienbeziehungen sind deutlich sichtbar.</p>
    <p>Unterstuetzung fuer betroffene familien ist zentral.</p>
    <h2>Praevention und Hilfsangebote</h2>
    <p>Praevention bei suchterkrankung gelingt besser mit lokalen Hilfsangeboten fuer familien.</p>
    <h2>Fazit</h2>
    <p>Die Lage ist herausfordernd, aber mit frueher Hilfe verbessert sich die Perspektive.</p>
    """
    errors = _validate_keyword_coverage(
        html,
        primary_keyword="eltern sucht schwangerschaft",
        secondary_keywords=[
            "auswirkungen auf familienbeziehungen",
            "unterstuetzung fuer betroffene familien",
            "praevention bei suchterkrankung",
            "hilfsangebote fuer familien",
        ],
    )
    assert not errors


def test_validate_language_and_conclusion_requires_fazit_then_faq():
    html = """
    <h1>Eltern Sucht Schwangerschaft: Auswirkungen und Hilfe</h1>
    <p>Eltern sucht schwangerschaft betrifft viele Familien und erfordert fruehe Hilfe.</p>
    <h2>Eltern Sucht Schwangerschaft im Alltag</h2>
    <p>Auswirkungen auf familienbeziehungen sind deutlich sichtbar.</p>
    <h2>Unterstuetzung</h2>
    <p>Unterstuetzung fuer betroffene familien ist zentral.</p>
    <h2>Fazit</h2>
    <p>Bei eltern sucht in der schwangerschaft sind fruehe hilfen, klare absprachen und stabile bezugspersonen besonders wichtig.</p>
    <h2>FAQ</h2>
    <h3>Was ist eltern sucht in der schwangerschaft?</h3>
    <p>Damit ist gemeint, dass eine suchtbelastung der eltern die gesundheit, den alltag und die beziehungen in der familie waehrend der schwangerschaft deutlich beeinflusst und deshalb fruehe unterstuetzung wichtig wird.</p>
    <h3>Welche hilfen sind frueh sinnvoll?</h3>
    <p>Sinnvoll sind fruehe gespraeche mit hebamme, suchtberatung, frauenarztpraxis und vertrauten bezugspersonen, damit belastungen sichtbar werden und familien schnell zu stabilen hilfen vor ort finden koennen.</p>
    <h3>Warum ist schnelle unterstuetzung wichtig?</h3>
    <p>Schnelle unterstuetzung hilft, konflikte zu reduzieren, alltagsstrukturen zu sichern und mutter, kind und weitere familienmitglieder besser zu entlasten, bevor sich gesundheitliche und soziale probleme weiter verstaerken.</p>
    """
    errors = _validate_language_and_conclusion(html, "Eltern-Sucht in der Schwangerschaft")
    assert not errors


def test_validate_language_and_conclusion_rejects_thin_faq():
    html = """
    <h1>Eltern Sucht Schwangerschaft: Auswirkungen und Hilfe</h1>
    <p>Eltern sucht schwangerschaft betrifft viele Familien und erfordert fruehe Hilfe.</p>
    <h2>Alltag</h2>
    <p>Auswirkungen auf familienbeziehungen sind deutlich sichtbar.</p>
    <h2>Unterstuetzung</h2>
    <p>Unterstuetzung fuer betroffene familien ist zentral und sollte frueh beginnen.</p>
    <h2>Fazit</h2>
    <p>Bei eltern sucht in der schwangerschaft sind fruehe hilfen, klare absprachen und stabile bezugspersonen besonders wichtig.</p>
    <h2>FAQ</h2>
    <h3>Was ist eltern sucht in der schwangerschaft?</h3>
    <p>Kurz.</p>
    <h3>Wann ist hilfe sinnvoll?</h3>
    <p>Sehr frueh.</p>
    </p>
    """
    errors = _validate_language_and_conclusion(html, "Eltern-Sucht in der Schwangerschaft")
    assert any(error.startswith("faq_question_count_too_low") or error.startswith("faq_answers_too_thin") for error in errors)


def test_validate_contextual_alignment_rejects_generic_off_context_copy():
    html = """
    <h1>Kinder Sonnenbrillen: worauf Eltern achten sollten</h1>
    <p>In der heutigen Zeit spielt das Thema eine wichtige Rolle und es ist wichtig zu beachten, dass verschiedene Aspekte relevant sind.</p>
    <h2>Das Wichtigste im Ueberblick</h2>
    <p>Abschliessend laesst sich sagen, dass zahlreiche Moeglichkeiten betrachtet werden koennen.</p>
    """

    errors = _validate_contextual_alignment(
        html,
        {
            "audience": "Eltern und Familien",
            "publishing_signals": ["Familienalltag", "Gesundheit"],
            "target_signals": ["UV Schutz fuer Kinderaugen", "Kinderbrillen"],
            "overlap_terms": ["schutz"],
            "style_cues": ["hilfreich", "sachlich"],
            "fit_reason": "Familie, Gesundheit und Schutz ergeben einen natuerlichen Kontext.",
        },
    )

    assert "publishing_context_missing" in errors
    assert "target_specificity_missing" in errors
    assert any(error.startswith("generic_filler_excessive") for error in errors)


def test_validate_section_substance_flags_thin_main_sections():
    html = """
    <h1>Kinder Sonnenbrillen: worauf Eltern achten sollten</h1>
    <p>Kinder sonnenbrillen helfen Familien im Alltag.</p>
    <h2>Passform</h2>
    <p>Kurz erklaert.</p>
    <h2>Material</h2>
    <p>Noch kuerzer.</p>
    <h2>Fazit</h2>
    <p>Bei kinder sonnenbrillen helfen klare Kriterien fuer Familien im Alltag.</p>
    <h2>FAQ</h2>
    <h3>Was ist wichtig?</h3>
    <p>Antwort mit genug Woertern fuer den Test und etwas Kontext zu Familien im Alltag.</p>
    <h3>Wann hilft UV Schutz?</h3>
    <p>Antwort mit genug Woertern fuer den Test und etwas Kontext zu Familien im Alltag.</p>
    <h3>Wie prueft man die Passform?</h3>
    <p>Antwort mit genug Woertern fuer den Test und etwas Kontext zu Familien im Alltag.</p>
    """

    errors = _validate_section_substance(html)
    assert any(error.startswith("section_too_thin:passform") for error in errors)
    assert any(error.startswith("section_too_thin:material") for error in errors)


def test_generate_article_by_sections_uses_editorial_brief_and_bounded_tokens(monkeypatch):
    captured: list[dict[str, object]] = []
    content_brief = {
        "audience": "Eltern und Familien",
        "publishing_signals": ["Familienalltag", "Gesundheit"],
        "target_signals": ["UV Schutz fuer Kinderaugen", "Kinderbrillen"],
        "overlap_terms": ["schutz"],
        "style_cues": ["hilfreich", "sachlich"],
        "fit_reason": "Familie, Gesundheit und Schutz ergeben einen natuerlichen Ratgeber-Kontext.",
    }

    def fake_call_llm_text(**kwargs):
        captured.append(
            {
                "label": kwargs.get("request_label"),
                "prompt": kwargs.get("user_prompt"),
                "max_tokens": kwargs.get("max_tokens"),
            }
        )
        if kwargs.get("request_label") == "phase5_section_intro":
            return (
                "<p>Eltern und Familien achten bei Kinder Sonnenbrillen auf UV Schutz, gute Passform und "
                "alltagstaugliche Materialien, damit Kinderaugen im Alltag besser geschuetzt bleiben.</p>"
            )
        return (
            "<p>Eltern und Familien pruefen UV Schutz, Passform, Haltbarkeit und Alltagseinsatz. "
            "Eine Kinderbrille sollte bequem sitzen, beim Spielen stabil bleiben und Kinderaugen vor "
            "Blendung schuetzen. So entstehen konkrete Kriterien statt allgemeiner Aussagen.</p>"
        )

    monkeypatch.setattr("creator.api.pipeline.call_llm_text", fake_call_llm_text)

    payload = _generate_article_by_sections(
        phase4={
            "h1": "Kinder Sonnenbrillen: worauf Eltern achten sollten",
            "outline": [
                {"h2": "Passform im Familienalltag", "h3": []},
                {"h2": "Material und UV Schutz", "h3": []},
                {"h2": "Fazit", "h3": []},
                {"h2": "FAQ", "h3": ["Was ist wichtig?", "Wie prueft man die Passform?", "Wann hilft UV Schutz?"]},
            ],
            "backlink_placement": "intro",
            "anchor_text_final": "Kinderbrillen Auswahlhilfe",
        },
        phase3={
            "final_article_topic": "Kinder Sonnenbrillen: worauf Eltern achten sollten",
            "primary_keyword": "kinder sonnenbrillen",
            "secondary_keywords": [
                "uv schutz fuer kinderaugen",
                "kinderbrillen im alltag",
                "passform fuer kinder",
                "sonnenbrillen fuer familien",
            ],
            "content_brief": content_brief,
        },
        backlink_url="https://target.example.com/kinderbrillen",
        publishing_site_url="https://publisher.example.com",
        internal_link_candidates=[
            "https://publisher.example.com/familienalltag",
            "https://publisher.example.com/gesundheit/kinderaugen",
        ],
        internal_link_anchor_map=None,
        min_internal_links=1,
        max_internal_links=2,
        faq_candidates=["Was ist wichtig?", "Wie prueft man die Passform?", "Wann hilft UV Schutz?"],
        structured_mode="none",
        llm_api_key="test-key",
        llm_base_url="https://api.openai.com/v1",
        llm_model="gpt-4.1-mini",
        http_timeout=2,
        expand_passes=0,
        section_max_tokens=900,
        expand_max_tokens=1800,
        usage_collector=None,
    )

    assert payload is not None
    brief_text = _format_content_brief_prompt_text(content_brief)
    section_prompts = [item for item in captured if str(item["label"]).startswith("phase5_section")]
    assert section_prompts
    assert all(brief_text in str(item["prompt"]) for item in section_prompts)
    assert all(int(item["max_tokens"]) < 600 for item in section_prompts)


def test_repair_keyword_context_gaps_preserves_fazit_and_faq_structure():
    html = """
    <h1>Kinder Sehprobleme erkennen und richtig reagieren</h1>
    <p>Einleitung fuer Eltern und Familien.</p>
    <h2>Praktische Tipps fuer Eltern im Umgang mit Sehproblemen</h2>
    <p>Kurz erklaert.</p>
    <h2>Fazit</h2>
    <p>Bei kinder sehproblemen helfen fruehe Beobachtung und klare naechste Schritte.</p>
    <h2>FAQ</h2>
    <h3>Wann sollte ein Kind zum Augenarzt?</h3>
    <p>Antwort mit ausreichend Woertern fuer den Test und etwas Kontext zu Familien im Alltag.</p>
    <h3>Wie erkennt man Sehprobleme?</h3>
    <p>Antwort mit ausreichend Woertern fuer den Test und etwas Kontext zu Familien im Alltag.</p>
    <h3>Was hilft im Alltag?</h3>
    <p>Antwort mit ausreichend Woertern fuer den Test und etwas Kontext zu Familien im Alltag.</p>
    """

    repaired = _repair_keyword_context_gaps(
        article_html=html,
        errors=[
            "target_specificity_missing",
            "primary_keyword_missing_h2",
            "secondary_keywords_missing:symptome von sehproblemen bei kindern,augenarzt termin mit kind vorbereiten",
            "section_too_thin:praktische tipps fuer eltern im umgang mit sehproblemen",
        ],
        topic="Kinder Sehprobleme erkennen und richtig reagieren",
        primary_keyword="kinder sehprobleme erkennen",
        content_brief={
            "target_signals": ["Augenarzt Termin Mit Kind Vorbereiten", "Symptome Von Sehproblemen Bei Kindern"],
        },
    )

    assert repaired.count("<h2>FAQ</h2>") == 1
    assert repaired.count("<h2>Fazit</h2>") == 1
    assert "Kinder Sehprobleme Erkennen:" in repaired
    assert "augenarzt termin mit kind vorbereiten" in repaired.lower()


def test_pair_fit_reasoning_builds_bridge_topics_for_commercial_target():
    captured: dict[str, object] = {}

    def fake_call_llm_json(**kwargs):
        captured["request_label"] = kwargs.get("request_label")
        prompt = str(kwargs.get("user_prompt") or "")
        input_json = prompt.split("Input:\n", 1)[1]
        captured["input_payload"] = json.loads(input_json)
        return {
            "topic_candidates": [
                {
                    "topic": "Kinder Sonnenbrillen: worauf Eltern achten sollten",
                    "publishing_site_relevance": 8,
                    "target_site_relevance": 8,
                    "informational_value": 8,
                    "backlink_naturalness": 7,
                    "spam_risk": 2,
                    "total_score": 40,
                    "backlink_angle": "Die Zielseite wird als nachrangige Ressource fuer konkrete Auswahlkriterien verlinkt.",
                },
                {
                    "topic": "UV Schutz fuer Kinder unterwegs: praktische Orientierung",
                    "publishing_site_relevance": 8,
                    "target_site_relevance": 7,
                    "informational_value": 8,
                    "backlink_naturalness": 7,
                    "spam_risk": 2,
                    "total_score": 39,
                    "backlink_angle": "Der Link vertieft geeignete Produkte als Zusatzressource.",
                },
                {
                    "topic": "Familienalltag im Sommer: Augenschutz fuer Kinder",
                    "publishing_site_relevance": 7,
                    "target_site_relevance": 7,
                    "informational_value": 8,
                    "backlink_naturalness": 6,
                    "spam_risk": 2,
                    "total_score": 37,
                    "backlink_angle": "Die Zielseite ergaenzt den Ratgeber mit weiterfuehrender Produktauswahl.",
                },
                {
                    "topic": "Kinderaugen draussen schuetzen: sinnvolle Kriterien",
                    "publishing_site_relevance": 7,
                    "target_site_relevance": 7,
                    "informational_value": 7,
                    "backlink_naturalness": 6,
                    "spam_risk": 3,
                    "total_score": 34,
                    "backlink_angle": "Die Zielseite dient als Beispiel fuer passende Loesungen.",
                },
                {
                    "topic": "Outdoor mit Kindern: Sonnenbrillen ohne Werbedruck einordnen",
                    "publishing_site_relevance": 6,
                    "target_site_relevance": 6,
                    "informational_value": 7,
                    "backlink_naturalness": 5,
                    "spam_risk": 3,
                    "total_score": 31,
                    "backlink_angle": "Die Zielseite wird rein kontextuell als Zusatzhinweis genannt.",
                },
            ],
            "final_article_topic": "Kinder Sonnenbrillen: worauf Eltern achten sollten",
            "final_match_decision": "accepted",
            "why_this_topic_was_chosen": "Das Thema passt klar zu Elternratgebern und bindet die Zielseite nur als Zusatzressource ein.",
            "best_overlap_reason": "Familie, Gesundheit und Schutz ergeben einen natuerlichen Kontext fuer einen Ratgeber.",
            "reject_reason": "",
            "fit_score": 82,
        }

    from creator.api import pipeline as pipeline_module

    original = pipeline_module.call_llm_json
    pipeline_module.call_llm_json = fake_call_llm_json
    try:
        result = _run_pair_fit_reasoning(
            requested_topic="",
            exclude_topics=[],
            target_site_url="https://target.example.com",
            publishing_site_url="https://publisher.example.com",
            target_profile={
                "normalized_url": "https://target.example.com",
                "topics": ["Sonnenbrillen", "Augenschutz fuer Kinder", "UV Schutz unterwegs"],
                "contexts": ["shopping", "safety"],
                "business_type": "E-Commerce",
                "services_or_products": ["Sonnenbrillen", "Kinderbrillen"],
                "repeated_keywords": ["sonnenbrillen", "uv", "schutz", "kinder"],
                "visible_headings": ["Kinderaugen vor UV Strahlung schuetzen"],
                "business_intent": "commercial",
            },
            publishing_profile={
                "normalized_url": "https://publisher.example.com",
                "topics": ["Familie", "Elternratgeber", "Gesunder Familienalltag"],
                "contexts": ["family_life", "health"],
                "site_categories": ["Familie", "Kinder", "Gesundheit"],
                "repeated_keywords": ["kinder", "familie", "schutz", "alltag"],
                "content_style": ["hilfreich", "sachlich"],
            },
            llm_api_key="test-key",
            llm_base_url="https://api.openai.com/v1",
            planning_model="gpt-4.1-mini",
            timeout_seconds=2,
            usage_collector=None,
        )
    finally:
        pipeline_module.call_llm_json = original

    assert captured["request_label"] == "phase3_pair_fit"
    input_payload = captured["input_payload"]
    assert input_payload["derived_signals"]["publishing_contexts"]
    assert input_payload["derived_signals"]["seed_bridge_topics"]
    assert input_payload["target_profile"]["services_or_products"]
    assert result["final_match_decision"] == "accepted"
    assert len(result["topic_candidates"]) == 5
    assert result["generated_bridge_topics"]
    assert result["final_article_topic"]
    assert "safety" in result["target_site_contexts"]
    assert any(item in result["publishing_site_contexts"] for item in ["family_life", "health", "parenting"])


def test_compact_pair_fit_profile_limits_prompt_fields() -> None:
    compact = _compact_pair_fit_profile(
        {
            "normalized_url": "https://target.example.com/path",
            "page_title": "Sehr lange Zielseite",
            "meta_description": "Meta",
            "domain_level_topic": "Optik",
            "primary_context": "family_life",
            "topics": [f"topic {idx}" for idx in range(12)],
            "contexts": [f"context {idx}" for idx in range(10)],
            "visible_headings": [f"heading {idx}" for idx in range(10)],
            "repeated_keywords": [f"keyword {idx}" for idx in range(12)],
            "services_or_products": [f"service {idx}" for idx in range(12)],
            "business_type": "shop",
            "business_intent": "commercial",
            "site_root_url": "https://target.example.com",
        },
        site_kind="target",
    )

    assert len(compact["topics"]) == 8
    assert len(compact["contexts"]) == 6
    assert len(compact["visible_headings"]) == 6
    assert len(compact["repeated_keywords"]) == 8
    assert len(compact["services_or_products"]) == 8


def test_pair_fit_reasoning_distinguishes_hard_reject():
    def fake_call_llm_json(**_kwargs):
        return {
            "topic_candidates": [
                {
                    "topic": "Beschaffung im Alltag: lose Einordnung",
                    "publishing_site_relevance": 2,
                    "target_site_relevance": 4,
                    "informational_value": 5,
                    "backlink_naturalness": 2,
                    "spam_risk": 8,
                    "total_score": 14,
                    "backlink_angle": "Der Verweis waere nur schwer natuerlich einzubetten.",
                },
                {
                    "topic": "Ersatzteile ohne Werbedruck erklaert",
                    "publishing_site_relevance": 2,
                    "target_site_relevance": 4,
                    "informational_value": 5,
                    "backlink_naturalness": 2,
                    "spam_risk": 8,
                    "total_score": 13,
                    "backlink_angle": "Der Link waere redaktionell fremd.",
                },
                {
                    "topic": "B2B Beschaffung vorsichtig eingeordnet",
                    "publishing_site_relevance": 1,
                    "target_site_relevance": 4,
                    "informational_value": 4,
                    "backlink_naturalness": 1,
                    "spam_risk": 9,
                    "total_score": 11,
                    "backlink_angle": "Der Link wuerde den Artikel inhaltlich kippen.",
                },
                {
                    "topic": "Grossbestellungen sachlich erklaert",
                    "publishing_site_relevance": 1,
                    "target_site_relevance": 4,
                    "informational_value": 4,
                    "backlink_naturalness": 1,
                    "spam_risk": 9,
                    "total_score": 10,
                    "backlink_angle": "Die Zielseite passt nicht zur Leserintention.",
                },
                {
                    "topic": "Industrie Ersatzteile knapp eingeordnet",
                    "publishing_site_relevance": 1,
                    "target_site_relevance": 3,
                    "informational_value": 4,
                    "backlink_naturalness": 1,
                    "spam_risk": 9,
                    "total_score": 9,
                    "backlink_angle": "Der Verweis bleibt fachfremd.",
                },
            ],
            "final_article_topic": "Beschaffung im Alltag: lose Einordnung",
            "final_match_decision": "hard_reject",
            "why_this_topic_was_chosen": "Kein Thema erreicht eine glaubwuerdige redaktionelle Passung.",
            "best_overlap_reason": "Die Kontexte liegen zu weit auseinander.",
            "reject_reason": "Zwischen Achtsamkeit und industrieller Beschaffung entsteht kein natuerlicher Informationsartikel.",
            "fit_score": 24,
        }

    from creator.api import pipeline as pipeline_module

    original = pipeline_module.call_llm_json
    pipeline_module.call_llm_json = fake_call_llm_json
    try:
        result = _run_pair_fit_reasoning(
            requested_topic="",
            exclude_topics=[],
            target_site_url="https://target.example.com",
            publishing_site_url="https://publisher.example.com",
            target_profile={
                "normalized_url": "https://target.example.com",
                "topics": ["Industrie Ersatzteile", "B2B Beschaffung"],
                "contexts": ["shopping", "productivity"],
                "services_or_products": ["Ersatzteile", "Grossbestellungen"],
                "business_intent": "commercial",
            },
            publishing_profile={
                "normalized_url": "https://publisher.example.com",
                "topics": ["Meditation", "Achtsamkeit", "Wellbeing"],
                "contexts": ["wellbeing"],
                "site_categories": ["Entspannung"],
            },
            llm_api_key="test-key",
            llm_base_url="https://api.openai.com/v1",
            planning_model="gpt-4.1-mini",
            timeout_seconds=2,
            usage_collector=None,
        )
    finally:
        pipeline_module.call_llm_json = original

    assert result["final_match_decision"] == "hard_reject"
    assert result["decision"] == "rejected"
    assert result["reject_reason"]


def test_pair_fit_normalize_prefers_balanced_bridge_topic():
    result = _pair_fit_normalize_llm_payload(
        llm_payload={
            "topic_candidates": [
                {
                    "topic": "Kinder Sonnenbrillen: worauf Eltern achten sollten",
                    "publishing_site_relevance": 8,
                    "target_site_relevance": 8,
                    "informational_value": 8,
                    "backlink_naturalness": 7,
                    "spam_risk": 2,
                    "total_score": 39,
                    "backlink_angle": "Nachrangige Ressource.",
                },
                {
                    "topic": "UV Schutz fuer Kinderaugen im Alltag",
                    "publishing_site_relevance": 7,
                    "target_site_relevance": 7,
                    "informational_value": 8,
                    "backlink_naturalness": 7,
                    "spam_risk": 2,
                    "total_score": 37,
                    "backlink_angle": "Nachrangige Ressource.",
                },
                {
                    "topic": "Sonnenschutz fuer Familien im Sommer",
                    "publishing_site_relevance": 6,
                    "target_site_relevance": 3,
                    "informational_value": 7,
                    "backlink_naturalness": 4,
                    "spam_risk": 4,
                    "total_score": 24,
                    "backlink_angle": "Nur schwach passend.",
                },
                {
                    "topic": "Sommer Alltag mit Kindern",
                    "publishing_site_relevance": 5,
                    "target_site_relevance": 2,
                    "informational_value": 6,
                    "backlink_naturalness": 3,
                    "spam_risk": 5,
                    "total_score": 18,
                    "backlink_angle": "Sehr lose Verbindung.",
                },
                {
                    "topic": "Outdoor Orientierung fuer Eltern",
                    "publishing_site_relevance": 5,
                    "target_site_relevance": 2,
                    "informational_value": 5,
                    "backlink_naturalness": 3,
                    "spam_risk": 5,
                    "total_score": 17,
                    "backlink_angle": "Sehr lose Verbindung.",
                },
            ],
            "final_article_topic": "Sonnenschutz fuer Familien im Sommer",
            "final_match_decision": "accepted",
            "why_this_topic_was_chosen": "generic",
            "best_overlap_reason": "shared",
            "fit_score": 70,
        },
        publishing_terms=["familie", "kinder", "gesundheit", "elternratgeber"],
        target_terms=["sonnenbrillen", "uv schutz", "kinderaugen"],
        publishing_contexts=["family_life", "health", "parenting"],
        target_contexts=["shopping", "health", "safety", "outdoor"],
        overlap_terms=["kinder", "schutz"],
        requested_topic="",
    )

    assert result["final_article_topic"] == "Kinder Sonnenbrillen: worauf Eltern achten sollten"


def test_pair_fit_cache_payload_is_usable_requires_complete_accepted_payload():
    accepted_payload = {
        "final_article_topic": "Kandidat 1",
        "topic_candidates": [
            {"topic": f"Kandidat {idx}", "total_score": 30}
            for idx in range(1, 6)
        ],
        "intersection_contexts": ["family_life"],
        "why_this_topic_was_chosen": "Passt zum Hauptkontext.",
        "backlink_fit_ok": True,
        "decision": "accepted",
    }
    rejected_payload = {
        "decision": "rejected",
        "rejection_reason": "Kein natuerlicher Fit.",
    }

    assert _pair_fit_cache_payload_is_usable(accepted_payload) is True
    assert _pair_fit_cache_payload_is_usable(rejected_payload) is True
    assert _pair_fit_cache_payload_is_usable({**accepted_payload, "topic_candidates": [{"topic": "Nur einer"}]}) is False
