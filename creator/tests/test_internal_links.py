from creator.api.pipeline import _rank_internal_link_inventory, _repair_link_constraints, _validate_link_strategy


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
    <h2>FAQ</h2>
    <h3>Was ist wichtig?</h3>
    <p>Antwort mit genug Woertern fuer die FAQ Bewertung und klare Hinweise fuer Leserinnen und Leser.</p>
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
        internal_link_anchor_map={
            "https://publisher.example.com/a": "Relevanter Artikel A",
            "https://publisher.example.com/b": "Relevanter Artikel B",
            "https://publisher.example.com/c": "Relevanter Artikel C",
        },
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
    assert repaired.count('href="https://publisher.example.com/') == 3
    assert "Relevanter Artikel A" in repaired
    assert "Mehr dazu" not in repaired


def test_rank_internal_link_inventory_prefers_relevant_same_site_articles():
    ranked = _rank_internal_link_inventory(
        [
            {
                "url": "https://publisher.example.com/baby-schlaf-tipps",
                "title": "Baby Schlaf Tipps fuer die ersten Monate",
                "excerpt": "Hilfen fuer Eltern bei Schlafproblemen im Alltag",
                "categories": ["Baby", "Familie"],
            },
            {
                "url": "https://publisher.example.com/immobilien-finanzierung",
                "title": "Immobilien Finanzierung einfach erklaert",
                "excerpt": "Ein Leitfaden fuer Kaeufer",
                "categories": ["Immobilien"],
            },
            {
                "url": "https://other.example.com/baby-tipps",
                "title": "Externes Baby Thema",
                "excerpt": "Sollte ignoriert werden",
                "categories": ["Baby"],
            },
        ],
        topic="Baby Schlafprobleme in den ersten Monaten",
        primary_keyword="baby schlaf tipps",
        secondary_keywords=["schlafprobleme baby", "hilfe fuer eltern"],
        publishing_site_url="https://publisher.example.com",
        backlink_url="https://target.example.com",
        max_items=3,
    )

    assert ranked
    assert ranked[0]["url"] == "https://publisher.example.com/baby-schlaf-tipps"
    assert all(item["url"].startswith("https://publisher.example.com") for item in ranked)
