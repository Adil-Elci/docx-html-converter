from creator.api.pipeline import (
    GOOGLE_SUGGEST_CACHE,
    KEYWORD_MIN_SECONDARY,
    _build_site_snapshot,
    _build_keyword_query_variants,
    _discover_keyword_candidates,
    _ensure_faq_candidates,
    _fetch_google_de_suggestions,
    _inject_faq_section,
    _merge_phase2_analysis,
    _select_keywords,
    _validate_language_and_conclusion,
    _validate_keyword_coverage,
)


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


def test_discover_keyword_candidates_extracts_faqs(monkeypatch):
    def fake_suggest(query, *, timeout_seconds):
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

    first = _fetch_google_de_suggestions("baby vorbereiten checkliste", timeout_seconds=2)
    second = _fetch_google_de_suggestions("baby vorbereiten checkliste", timeout_seconds=2)

    assert first == second
    assert calls["count"] == 1


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
