from creator.api.pipeline import _repair_link_constraints, _validate_link_strategy


def test_validate_link_strategy_ok():
    html = """
    <h1>Titel</h1>
    <p>Einleitung mit <a href="https://target.example.com/page">Backlink</a>.</p>
    <h2>Abschnitt 1</h2>
    <p>Siehe auch <a href="https://publisher.example.com/a">A</a>.</p>
    <h2>Abschnitt 2</h2>
    <p>Weiterfuehrend <a href="https://publisher.example.com/b">B</a>.</p>
    <h2>Fazit</h2>
    <p>Zusammenfassung.</p>
    """
    errors = _validate_link_strategy(
        html,
        backlink_url="https://target.example.com/page",
        publishing_site_url="https://publisher.example.com",
        min_internal_links=2,
        max_internal_links=4,
    )
    assert errors == []


def test_validate_link_strategy_rejects_external():
    html = """
    <h1>Titel</h1>
    <p>Einleitung mit <a href="https://target.example.com/page">Backlink</a>.</p>
    <h2>Abschnitt 1</h2>
    <p>Fremdlink <a href="https://external.example.com/x">X</a>.</p>
    <h2>Fazit</h2>
    <p>Zusammenfassung.</p>
    """
    errors = _validate_link_strategy(
        html,
        backlink_url="https://target.example.com/page",
        publishing_site_url="https://publisher.example.com",
        min_internal_links=1,
        max_internal_links=3,
    )
    assert any(error.startswith("external_link_count_invalid") for error in errors)


def test_repair_link_constraints_inserts_backlink_and_internal_links():
    html = """
    <h1>Titel</h1>
    <p>Einleitung ohne Links.</p>
    <h2>Abschnitt 1</h2>
    <p>Text.</p>
    <h2>Abschnitt 2</h2>
    <p>Mehr Text.</p>
    <h2>Fazit</h2>
    <p>Ende.</p>
    """
    repaired = _repair_link_constraints(
        article_html=html,
        backlink_url="https://target.example.com/page",
        publishing_site_url="https://publisher.example.com",
        internal_links=[
            "https://publisher.example.com/a",
            "https://publisher.example.com/b",
            "https://publisher.example.com/c",
        ],
        min_internal_links=2,
        max_internal_links=4,
        backlink_placement="intro",
        anchor_text="Backlink",
    )
    errors = _validate_link_strategy(
        repaired,
        backlink_url="https://target.example.com/page",
        publishing_site_url="https://publisher.example.com",
        min_internal_links=2,
        max_internal_links=4,
    )
    assert errors == []
