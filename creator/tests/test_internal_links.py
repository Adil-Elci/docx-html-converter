from creator.api.pipeline import (
    _normalize_section_html,
    _rank_internal_link_inventory,
    _repair_link_constraints,
    _validate_link_strategy,
)


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
        required_h1="Titel",
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
    assert repaired.startswith("<h1>Titel</h1>")


def test_normalize_section_html_preserves_required_heading_structure():
    html = _normalize_section_html(
        "FAQ",
        ["Was ist wichtig?", "Wann ist Hilfe sinnvoll?"],
        "<h2>Falsche Ueberschrift</h2><p>Antwort eins.</p><h3>Andere Frage</h3><p>Antwort zwei.</p>",
    )

    assert html.startswith("<h2>FAQ</h2>")
    assert "<h3>Was ist wichtig?</h3>" in html
    assert "<h3>Wann ist Hilfe sinnvoll?</h3>" in html
    assert "Falsche Ueberschrift" not in html


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


def test_rank_internal_link_inventory_rejects_generic_family_articles_for_specific_topic():
    ranked = _rank_internal_link_inventory(
        [
            {
                "url": "https://publisher.example.com/familienurlaub-inseln",
                "title": "Familienurlaub auf Inseln Tipps Ideen",
                "excerpt": "Inspiration fuer Eltern und Kinder im Sommer",
                "categories": ["Familie", "Reisen"],
            },
            {
                "url": "https://publisher.example.com/jeans-kombinieren",
                "title": "Jeans richtig kombinieren Tipps fuer jeden Stil",
                "excerpt": "Modeideen fuer den Alltag",
                "categories": ["Mode"],
            },
            {
                "url": "https://publisher.example.com/kinderaugen-warnzeichen",
                "title": "Kinderaugen verstehen und Warnzeichen erkennen",
                "excerpt": "Welche Anzeichen fuer Sehprobleme Eltern kennen sollten",
                "categories": ["Gesundheit", "Kinder"],
            },
        ],
        topic="Sehstärke bei Kindern: Wann braucht mein Kind eine Brille? Tipps",
        primary_keyword="sehprobleme bei kindern",
        secondary_keywords=[
            "warnzeichen fuer sehprobleme bei kindern",
            "augenarzt termin mit kind",
        ],
        publishing_site_url="https://publisher.example.com",
        backlink_url="https://target.example.com",
        max_items=4,
    )

    assert [item["url"] for item in ranked] == ["https://publisher.example.com/kinderaugen-warnzeichen"]


def test_rank_internal_link_inventory_returns_only_high_confidence_matches_in_rank_order():
    ranked = _rank_internal_link_inventory(
        [
            {
                "url": "https://publisher.example.com/kinderaugen-uv",
                "title": "Kinderaugen im Sommer: UV-Schutz richtig einordnen",
                "excerpt": "Warum Kinderaugen Schutz brauchen und worauf Eltern achten sollten",
                "categories": ["Gesundheit", "Kinder"],
            },
            {
                "url": "https://publisher.example.com/kinder-sonnenbrillen-passform",
                "title": "Kinder Sonnenbrillen: Passform und Schutzklassen im Alltag",
                "excerpt": "Worauf Eltern bei Sitz, Schutz und Material achten sollten",
                "categories": ["Familie", "Gesundheit"],
            },
            {
                "url": "https://publisher.example.com/hautpflege-familie",
                "title": "Hautpflege-Routinen fuer die ganze Familie",
                "excerpt": "Pflegeideen fuer sonnige Tage",
                "categories": ["Familie", "Pflege"],
            },
        ],
        topic="Sonnenschutz fuer die ganze Familie",
        primary_keyword="kinder sonnenbrillen",
        secondary_keywords=[
            "uv schutz fuer kinderaugen",
            "passform fuer kinder sonnenbrillen",
            "schutzklasse fuer sonnenbrillen",
        ],
        publishing_site_url="https://publisher.example.com",
        backlink_url="https://target.example.com",
        max_items=4,
        topic_signature={
            "subject_phrase": "sonnenschutz fuer die ganze familie",
            "primary_keyword": "kinder sonnenbrillen",
            "target_terms": ["Kinder Sonnenbrillen", "UV Schutz fuer Kinderaugen"],
            "target_support_phrases": ["kinder sonnenbrillen", "uv schutz fuer kinderaugen"],
            "core_tokens": ["kinderaugen", "schutz", "sonnenbrillen", "sonnenschutz", "uv"],
            "specific_tokens": ["kinderaugen", "schutz", "sonnenbrillen", "sonnenschutz", "uv"],
            "all_tokens": ["kinderaugen", "schutz", "sonnenbrillen", "sonnenschutz", "uv", "passform", "schutzklasse"],
        },
    )

    assert {item["url"] for item in ranked} == {
        "https://publisher.example.com/kinderaugen-uv",
        "https://publisher.example.com/kinder-sonnenbrillen-passform",
    }


def test_rank_internal_link_inventory_keeps_support_articles_without_exact_product_token():
    ranked = _rank_internal_link_inventory(
        [
            {
                "url": "https://publisher.example.com/kinderaugen-uv",
                "title": "Kinderaugen im Sommer: UV-Schutz richtig einordnen",
                "excerpt": "Warum Kinderaugen Schutz brauchen und worauf Eltern achten sollten",
                "categories": ["Gesundheit", "Kinder"],
            },
            {
                "url": "https://publisher.example.com/familienurlaub-inseln",
                "title": "Familienurlaub auf Inseln: Tipps und Ideen",
                "excerpt": "Inspiration fuer sonnige Ferientage",
                "categories": ["Familie", "Reisen"],
            },
        ],
        topic="Sonnenbrillen fuer Kinder",
        primary_keyword="sonnenbrillen kinder",
        secondary_keywords=[
            "uv schutz fuer kinderaugen",
            "passform fuer kindersonnenbrillen",
        ],
        publishing_site_url="https://publisher.example.com",
        backlink_url="https://target.example.com",
        max_items=4,
        topic_signature={
            "subject_phrase": "sonnenbrillen fuer kinder",
            "primary_keyword": "sonnenbrillen kinder",
            "target_terms": ["Sonnenbrillen fuer Kinder", "UV Schutz fuer Kinderaugen"],
            "target_support_phrases": ["sonnenbrillen fuer kinder", "uv schutz fuer kinderaugen"],
            "core_tokens": ["sonnenbrillen"],
            "specific_tokens": ["sonnenbrillen", "uv", "schutz", "kinderaugen"],
            "all_tokens": ["sonnenbrillen", "uv", "schutz", "kinderaugen", "passform"],
        },
    )

    assert [item["url"] for item in ranked] == ["https://publisher.example.com/kinderaugen-uv"]


def test_rank_internal_link_inventory_requires_title_or_slug_signal_for_fuzzy_support_matches():
    ranked = _rank_internal_link_inventory(
        [
            {
                "url": "https://publisher.example.com/sehstaerke-vaeter",
                "title": "Sehstaerke im Alltag: Tipps fuer Vaeter ab 40",
                "excerpt": "Die Augen verlieren an Anpassungsfaehigkeit und gute Augengesundheit wird wichtiger.",
                "slug": "sehstaerke-tipps-vaeter-40",
                "categories": ["Wissen"],
            },
            {
                "url": "https://publisher.example.com/helgoland-ausflug",
                "title": "Familienausflug nach Helgoland: Ein unvergesslicher Tag",
                "excerpt": "Ob kleine Kinder mit grossen Augen oder Grosseltern mit maritimer Sehnsucht.",
                "slug": "familienausflug-nach-helgoland",
                "categories": ["Reisen"],
            },
        ],
        topic="Sonnenbrillen fuer Kinder",
        primary_keyword="sonnenbrillen fuer kinder",
        secondary_keywords=[
            "uv schutz kinder augen",
            "kindersonnenbrillen kaufen",
        ],
        publishing_site_url="https://publisher.example.com",
        backlink_url="https://target.example.com",
        max_items=4,
        topic_signature={
            "subject_phrase": "sonnenbrillen fuer kinder",
            "primary_keyword": "sonnenbrillen fuer kinder",
            "core_tokens": ["sonnenbrillen", "schutz", "uv"],
            "seed_specific_tokens": ["augen", "schutz", "sehgesundheit", "sonnenbrillen", "uv"],
            "seed_all_tokens": ["augen", "kindersonnenbrillen", "kaufen", "schutz", "sehgesundheit", "sonnenbrillen", "uv"],
            "specific_tokens": ["augen", "schutz", "sehgesundheit", "sonnenbrillen", "uv"],
            "all_tokens": ["augen", "kindersonnenbrillen", "kaufen", "schutz", "sehgesundheit", "sonnenbrillen", "uv"],
        },
    )

    assert [item["url"] for item in ranked] == ["https://publisher.example.com/sehstaerke-vaeter"]


def test_rank_internal_link_inventory_rejects_generic_support_token_matches_without_domain_alignment():
    ranked = _rank_internal_link_inventory(
        [
            {
                "url": "https://publisher.example.com/sehstaerke-vaeter",
                "title": "Sehstaerke im Alltag: Tipps fuer Vaeter ab 40",
                "excerpt": "Die Augen verlieren an Anpassungsfaehigkeit und gute Augengesundheit wird wichtiger.",
                "slug": "sehstaerke-tipps-vaeter-40",
                "categories": ["Wissen"],
            },
            {
                "url": "https://publisher.example.com/keller-schutz",
                "title": "Hochwertige Strategien fuer Feuchtigkeitsschutz im Keller",
                "excerpt": "Ohne wirksame Vorbeugung entstehen erhebliche Schaeden an Gebaeuden.",
                "slug": "keller-schutz-massnahmen",
                "categories": ["Wohnen"],
            },
        ],
        topic="Sonnenbrillen fuer Kinder",
        primary_keyword="sonnenbrillen fuer kinder",
        secondary_keywords=[
            "uv schutz kinder augen",
            "kindersonnenbrillen kaufen",
        ],
        publishing_site_url="https://publisher.example.com",
        backlink_url="https://target.example.com",
        max_items=4,
        topic_signature={
            "subject_phrase": "sonnenbrillen fuer kinder",
            "primary_keyword": "sonnenbrillen fuer kinder",
            "core_tokens": ["sonnenbrillen", "schutz", "uv"],
            "seed_specific_tokens": ["augen", "schutz", "sehgesundheit", "sonnenbrillen", "uv"],
            "seed_all_tokens": ["augen", "kindersonnenbrillen", "kaufen", "schutz", "sehgesundheit", "sonnenbrillen", "uv"],
            "specific_tokens": ["augen", "augenschutz", "kaufen", "kindersonnenbrillen", "schutz", "sehgesundheit", "sonnenbrillen", "strand", "uv"],
            "all_tokens": ["augen", "augenschutz", "kaufen", "kindersonnenbrillen", "schutz", "sehgesundheit", "sonnenbrillen", "strand", "uv"],
        },
    )

    assert [item["url"] for item in ranked] == ["https://publisher.example.com/sehstaerke-vaeter"]


def test_rank_internal_link_inventory_keeps_support_articles_with_excerpt_level_domain_signal():
    ranked = _rank_internal_link_inventory(
        [
            {
                "url": "https://publisher.example.com/sehstaerke-vaeter",
                "title": "Sehstaerke im Alltag: Tipps fuer Vaeter ab 40",
                "excerpt": (
                    "Die Augen verlieren an Anpassungsfaehigkeit und kleine Schrift wird anstrengender. "
                    "Mit dem richtigen Wissen laesst sich die Augengesundheit gezielt unterstuetzen."
                ),
                "slug": "sehstaerke-tipps-vaeter-40",
                "categories": ["Wissen"],
            },
            {
                "url": "https://publisher.example.com/helgoland-ausflug",
                "title": "Familienausflug nach Helgoland: Ein unvergesslicher Tag",
                "excerpt": "Ob kleine Kinder mit grossen Augen oder Grosseltern mit maritimer Sehnsucht.",
                "slug": "familienausflug-nach-helgoland",
                "categories": ["Reisen"],
            },
        ],
        topic="Sonnenbrillen fuer Kinder",
        primary_keyword="sonnenbrillen fuer kinder",
        secondary_keywords=[
            "kindersonnenbrillen kaufen",
            "uv schutz kinder augen",
            "ce kennzeichen sonnenbrille kinder",
        ],
        publishing_site_url="https://publisher.example.com",
        backlink_url="https://target.example.com",
        max_items=4,
        topic_signature={
            "subject_phrase": "sonnenbrillen fuer kinder",
            "primary_keyword": "sonnenbrillen fuer kinder",
            "core_tokens": ["sonnenbrillen"],
            "seed_specific_tokens": ["sonnenbrillen"],
            "seed_all_tokens": ["augen", "ce", "kaufen", "kennzeichen", "kindersonnenbrillen", "schutz", "sonnenbrille", "sonnenbrillen", "uv"],
            "specific_tokens": ["augen", "ce", "kaufen", "kennzeichen", "kindersonnenbrillen", "schutz", "sonnenbrille", "sonnenbrillen", "uv"],
            "all_tokens": ["sonnenbrillen", "augen", "ce", "kaufen", "kennzeichen", "kindersonnenbrillen", "schutz", "sonnenbrille", "uv"],
        },
    )

    assert [item["url"] for item in ranked] == ["https://publisher.example.com/sehstaerke-vaeter"]


def test_rank_internal_link_inventory_rejects_visual_mentions_and_generic_buying_posts():
    ranked = _rank_internal_link_inventory(
        [
            {
                "url": "https://publisher.example.com/sehstaerke-vaeter",
                "title": "Sehstaerke im Alltag: Tipps fuer Vaeter ab 40",
                "excerpt": (
                    "Die Augen verlieren an Anpassungsfaehigkeit und kleine Schrift wird anstrengender. "
                    "Mit dem richtigen Wissen laesst sich die Augengesundheit gezielt unterstuetzen."
                ),
                "slug": "sehstaerke-tipps-vaeter-40",
                "categories": ["Wissen"],
            },
            {
                "url": "https://publisher.example.com/babyfotos-bindung",
                "title": "Mit Papa auf dem Arm, mit Mama im Blick: Wie emotionale Bindungen auf Babyfotos sichtbar werden",
                "excerpt": (
                    "Die Augen des Neugeborenen suchen den vertrauten Ausdruck, waehrend die Mutter laechelt. "
                    "Genau diese Augenblicke machen Babyfotos so besonders."
                ),
                "slug": "emotionale-bindungen-auf-babyfotos",
                "categories": ["Familie"],
            },
            {
                "url": "https://publisher.example.com/iphone-kaufen",
                "title": "iPhone mieten oder kaufen - Was lohnt sich?",
                "excerpt": "Viele Verbraucher stehen vor der Frage, ob sie kaufen oder mieten sollen.",
                "slug": "iphone-mieten-oder-kaufen",
                "categories": ["Technik"],
            },
        ],
        topic="Sonnenbrillen fuer Kinder",
        primary_keyword="sonnenbrillen fuer kinder",
        secondary_keywords=[
            "kindersonnenbrillen kaufen",
            "uv schutz kinder augen",
            "ce kennzeichen sonnenbrille kinder",
        ],
        publishing_site_url="https://publisher.example.com",
        backlink_url="https://target.example.com",
        max_items=4,
        topic_signature={
            "subject_phrase": "sonnenbrillen fuer kinder",
            "primary_keyword": "sonnenbrillen fuer kinder",
            "core_tokens": ["sonnenbrillen"],
            "seed_specific_tokens": ["sonnenbrillen"],
            "seed_all_tokens": ["augen", "ce", "kaufen", "kennzeichen", "kindersonnenbrillen", "schutz", "sonnenbrille", "sonnenbrillen", "uv"],
            "specific_tokens": ["augen", "ce", "kaufen", "kennzeichen", "kindersonnenbrillen", "schutz", "sonnenbrille", "sonnenbrillen", "uv"],
            "all_tokens": ["sonnenbrillen", "augen", "ce", "kaufen", "kennzeichen", "kindersonnenbrillen", "schutz", "sonnenbrille", "uv"],
        },
    )

    assert [item["url"] for item in ranked] == ["https://publisher.example.com/sehstaerke-vaeter"]
