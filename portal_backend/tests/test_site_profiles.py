from portal_backend.api.site_profiles import (
    _extract_internal_links,
    _extract_keywords,
    _extract_page_signals,
    _shortlist_ranked_publishing_candidates,
    build_combined_target_profile,
    compute_site_selection_score,
    fetch_site_profile_payload,
    score_publishing_site_fit,
)


def test_score_publishing_site_fit_rewards_semantic_overlap() -> None:
    publishing_profile = {
        "topics": ["Elternratgeber", "Schwangerschaft", "Familienalltag"],
        "site_categories": ["Familie", "Gesundheit"],
        "topic_clusters": ["schwangerschaft", "familienalltag", "kinder"],
        "repeated_keywords": ["eltern", "kinder", "ratgeber"],
        "visible_headings": ["Tipps fuer Familien", "Schwangerschaft begleiten"],
        "contexts": ["family_life", "health"],
        "primary_context": "family_life",
    }
    target_profile = {
        "topics": ["Schwimmbrille fuer Kinder", "Sicher schwimmen"],
        "contexts": ["family_life", "safety"],
        "repeated_keywords": ["kinder", "schwimmen", "sicherheit"],
        "services_or_products": ["Schwimmbrille", "Kinderbrillen"],
        "visible_headings": ["Kinder beim Schwimmen schuetzen"],
        "primary_context": "family_life",
        "business_intent": "commercial",
    }

    score, details = score_publishing_site_fit(publishing_profile, target_profile)

    assert score >= 20
    assert "family_life" in details["context_overlap"]
    assert "kinder" in details["topic_overlap_terms"]


def test_compute_site_selection_score_prefers_real_estate_specialist_over_broad_magazine() -> None:
    target_profile = {
        "topics": ["Immobilie verkaufen", "Hausverkauf Hamburg", "Wertermittlung Immobilie"],
        "contexts": ["real_estate", "home", "finance"],
        "repeated_keywords": ["immobilien", "verkauf", "makler", "hausverkauf"],
        "services_or_products": ["Immobilienverkauf", "Immobilienmakler Hamburg"],
        "visible_headings": ["Immobilie verkaufen in Hamburg"],
        "primary_context": "real_estate",
        "business_intent": "commercial",
    }
    specialist_profile = {
        "topics": ["Immobilie verkaufen", "Hausverkauf Hamburg", "Wertermittlung"],
        "site_categories": ["Immobilien", "Hausverkauf"],
        "topic_clusters": ["immobilienverkauf", "wertermittlung", "grundbuch"],
        "repeated_keywords": ["immobilien", "verkauf", "makler"],
        "visible_headings": ["Immobilienmakler in Hamburg"],
        "contexts": ["real_estate", "home", "finance"],
        "primary_context": "real_estate",
    }
    broad_profile = {
        "topics": ["Lifestyle Trends", "Wohnen", "Einrichten", "Immobilien Tipps"],
        "site_categories": ["Lifestyle", "Ratgeber"],
        "topic_clusters": ["ideen", "wohnen", "immobilien"],
        "repeated_keywords": ["ideen", "alltag", "wohnen", "immobilien"],
        "visible_headings": ["Tipps fuer den Alltag", "Immobilien kaufen fuer Einsteiger"],
        "contexts": ["lifestyle", "home"],
        "primary_context": "lifestyle",
    }

    specialist_score, specialist_details = compute_site_selection_score(
        publishing_profile=specialist_profile,
        target_profile=target_profile,
        inventory_context={
            "prominent_titles": ["Immobilie verkaufen in Hamburg", "Wertermittlung vor dem Notartermin"],
            "site_categories": ["Immobilien", "Hausverkauf"],
            "topic_clusters": ["immobilienverkauf", "wertermittlung", "notar"],
        },
    )
    broad_score, broad_details = compute_site_selection_score(
        publishing_profile=broad_profile,
        target_profile=target_profile,
        inventory_context={
            "prominent_titles": ["Immobilie verkaufen Tipps", "Immobilien kaufen fuer Einsteiger", "Wohnideen fuer Familien"],
            "site_categories": ["Lifestyle", "Immobilien"],
            "topic_clusters": ["immobilien", "wohnen", "ideen"],
        },
    )

    assert specialist_score > broad_score
    assert specialist_details["primary_context_mismatch"] is False
    assert broad_details["primary_context_mismatch"] is True


def test_compute_site_selection_score_rewards_relevant_inventory_depth() -> None:
    target_profile = {
        "topics": ["Nahrungsergänzungsmittel Kosten", "Omega 3 Preisvergleich"],
        "contexts": ["health", "lifestyle"],
        "repeated_keywords": ["nahrungsergänzungsmittel", "kosten", "omega"],
        "services_or_products": ["Omega 3", "Vitamin D"],
        "visible_headings": ["Preisvergleich für Supplements"],
        "primary_context": "health",
        "business_intent": "informational",
    }
    publishing_profile = {
        "topics": ["Gesundheit", "Ernährung", "Wellness"],
        "site_categories": ["Gesundheit", "Wellness"],
        "topic_clusters": ["gesundheit", "supplements", "ernährung"],
        "repeated_keywords": ["gesundheit", "vitamin", "omega"],
        "visible_headings": ["Ratgeber für Nahrungsergänzungsmittel"],
        "contexts": ["health", "lifestyle"],
        "primary_context": "health",
    }

    shallow_score, shallow_details = compute_site_selection_score(
        publishing_profile=publishing_profile,
        target_profile=target_profile,
        inventory_context={
            "article_titles": ["Gesund leben im Alltag", "Mehr Energie im Büro"],
            "prominent_titles": ["Gesund leben im Alltag", "Mehr Energie im Büro"],
            "site_categories": ["Gesundheit"],
            "topic_clusters": ["gesundheit", "alltag"],
        },
    )
    deep_score, deep_details = compute_site_selection_score(
        publishing_profile=publishing_profile,
        target_profile=target_profile,
        inventory_context={
            "article_titles": [
                "Nahrungsergänzungsmittel Kosten im Vergleich",
                "Omega 3 Preisvergleich nach Tagesdosis",
                "Vitamin D kaufen: Preis und Dosierung prüfen",
            ],
            "prominent_titles": [
                "Nahrungsergänzungsmittel Kosten im Vergleich",
                "Omega 3 Preisvergleich nach Tagesdosis",
            ],
            "site_categories": ["Gesundheit", "Supplements"],
            "topic_clusters": ["nahrungsergänzungsmittel", "kosten", "omega", "vitamin"],
        },
    )

    assert deep_details["relevant_inventory_count"] > shallow_details["relevant_inventory_count"]
    assert deep_details["relevant_inventory_bonus"] > shallow_details["relevant_inventory_bonus"]
    assert deep_details["relevant_inventory_count"] >= 1


def test_compute_site_selection_score_prefers_nutrition_context_for_supplement_targets() -> None:
    target_profile = {
        "topics": ["Nahrungsergänzungsmittel Kosten", "Protein Pulver Preisvergleich"],
        "contexts": ["nutrition", "health", "shopping"],
        "repeated_keywords": ["nahrungsergänzungsmittel", "protein", "kosten", "vitamin"],
        "services_or_products": ["Protein Pulver", "Omega 3", "Vitamine"],
        "visible_headings": ["Preisvergleich für Supplements"],
        "primary_context": "nutrition",
        "business_intent": "informational",
    }
    generic_price_profile = {
        "topics": ["Kosten im Alltag", "Preise vergleichen", "Gesundheit"],
        "site_categories": ["Kosten", "Gesundheit"],
        "topic_clusters": ["kosten", "vergleich", "gesundheit"],
        "repeated_keywords": ["kosten", "preis", "vergleich"],
        "visible_headings": ["Was kostet das wirklich"],
        "contexts": ["finance", "health", "shopping"],
        "primary_context": "finance",
    }
    nutrition_profile = {
        "topics": ["Nahrungsergänzungsmittel", "Proteine", "Vitamine"],
        "site_categories": ["Gesundheit", "Supplements"],
        "topic_clusters": ["nahrungsergänzungsmittel", "protein", "vitamin", "omega"],
        "repeated_keywords": ["protein", "vitamin", "supplements"],
        "visible_headings": ["Ratgeber für Nahrungsergänzungsmittel"],
        "contexts": ["nutrition", "health", "shopping"],
        "primary_context": "nutrition",
    }

    generic_score, generic_details = compute_site_selection_score(
        publishing_profile=generic_price_profile,
        target_profile=target_profile,
        inventory_context={
            "article_titles": ["Was kostet Strom", "Gesund leben im Alltag"],
            "prominent_titles": ["Was kostet Strom"],
            "site_categories": ["Kosten"],
            "topic_clusters": ["kosten", "vergleich"],
        },
    )
    nutrition_score, nutrition_details = compute_site_selection_score(
        publishing_profile=nutrition_profile,
        target_profile=target_profile,
        inventory_context={
            "article_titles": [
                "Nahrungsergänzungsmittel Kosten im Vergleich",
                "Omega 3 Preis pro Tagesdosis",
                "Protein Pulver Dosierung und Preis",
            ],
            "prominent_titles": ["Nahrungsergänzungsmittel Kosten im Vergleich"],
            "site_categories": ["Gesundheit", "Supplements"],
            "topic_clusters": ["nahrungsergänzungsmittel", "protein", "omega", "kosten"],
        },
    )

    assert nutrition_score > generic_score
    assert nutrition_details["primary_context_mismatch"] is False
    assert generic_details["primary_context_mismatch"] is True


def test_fetch_site_profile_payload_prefers_snapshot_primary_context_over_inventory_titles(monkeypatch) -> None:
    monkeypatch.setattr(
        "portal_backend.api.site_profiles._build_snapshot_pages",
        lambda *_args, **_kwargs: [
            {
                "url": "https://publisher.example.com",
                "title": "Familienalltag mit Ideen fuer Zuhause",
                "meta_description": "Praktische Ideen fuer Eltern, Kinder und den Familienalltag.",
                "headings": ["Ideen fuer den Familienalltag", "Alltag mit Kindern leichter organisieren"],
                "text": "Familien, Eltern und Kinder finden hier Ideen fuer den Alltag, Organisation und Routinen.",
            }
        ],
    )

    payload = fetch_site_profile_payload(
        site_url="https://publisher.example.com",
        profile_kind="publishing_site",
        inventory_context={
            "site_categories": ["Immobilien", "Hausverkauf"],
            "prominent_titles": ["Immobilie verkaufen in Hamburg", "Wertermittlung vor dem Notartermin"],
            "topic_clusters": ["immobilienverkauf", "wertermittlung", "notar"],
        },
    )

    assert payload["primary_context"] == "family_life"
    assert "family_life" in payload["snapshot_contexts"]
    assert "real_estate" in payload["inventory_contexts"]
    assert "real_estate" in payload["contexts"]


def test_shortlist_ranked_publishing_candidates_prioritizes_stronger_target_context() -> None:
    ranked = [
        {
            "site_url": "https://broad.example.com",
            "site_name": "Broad",
            "score": 92,
            "profile": {
                "primary_context": "lifestyle",
                "contexts": ["lifestyle", "home", "real_estate"],
                "snapshot_contexts": ["real_estate"],
                "inventory_contexts": ["real_estate"],
            },
            "details": {"publishing_primary_context": "lifestyle", "semantic_score": 42, "internal_link_support": 15},
        },
        {
            "site_url": "https://specialist-a.example.com",
            "site_name": "Specialist A",
            "score": 60,
            "profile": {"primary_context": "real_estate", "contexts": ["real_estate", "finance"]},
            "details": {"publishing_primary_context": "real_estate", "semantic_score": 54, "internal_link_support": 9},
        },
        {
            "site_url": "https://specialist-b.example.com",
            "site_name": "Specialist B",
            "score": 58,
            "profile": {"primary_context": "real_estate", "contexts": ["real_estate", "home"]},
            "details": {"publishing_primary_context": "real_estate", "semantic_score": 50, "internal_link_support": 8},
        },
    ]

    shortlisted = _shortlist_ranked_publishing_candidates(
        ranked,
        target_profile={"primary_context": "real_estate"},
        limit=2,
    )

    assert [item["site_url"] for item in shortlisted] == [
        "https://broad.example.com",
        "https://specialist-a.example.com",
    ]


def test_shortlist_ranked_publishing_candidates_keeps_specialists_with_relaxed_floor() -> None:
    ranked = [
        {
            "site_url": "https://broad.example.com",
            "site_name": "Broad",
            "score": 91,
            "profile": {"primary_context": "lifestyle", "contexts": ["lifestyle", "home"]},
            "details": {"publishing_primary_context": "lifestyle", "semantic_score": 48, "internal_link_support": 15},
        },
        {
            "site_url": "https://specialist-low.example.com",
            "site_name": "Specialist Low",
            "score": 12,
            "profile": {"primary_context": "real_estate", "contexts": ["real_estate", "finance"]},
            "details": {"publishing_primary_context": "real_estate", "semantic_score": 14, "internal_link_support": 3},
        },
    ]

    shortlisted = _shortlist_ranked_publishing_candidates(
        ranked,
        target_profile={"primary_context": "real_estate"},
        limit=2,
        min_score=18,
    )

    assert [item["site_url"] for item in shortlisted] == [
        "https://specialist-low.example.com",
        "https://broad.example.com",
    ]


def test_build_combined_target_profile_merges_page_and_root_context() -> None:
    exact_profile = {
        "page_title": "Schwimmbrille fuer Kinder",
        "meta_description": "Tipps zur Auswahl der passenden Schwimmbrille.",
        "visible_headings": ["Kinder beim Schwimmen schuetzen"],
        "repeated_keywords": ["schwimmbrille", "kinder", "schutz"],
        "sample_page_titles": ["Schwimmbrille fuer Kinder"],
        "sample_urls": ["https://shop.example.com/schwimmbrille"],
        "topics": ["Schwimmbrille fuer Kinder"],
        "contexts": ["family_life", "safety"],
        "content_tone": "practical",
        "content_style": "ratgeber",
        "services_or_products": ["Schwimmbrille"],
        "business_intent": "commercial",
        "commerciality": 0.9,
        "page_count": 1,
    }
    root_profile = {
        "page_title": "Brillenhaus24",
        "domain_level_topic": "Brillen und Sehhilfen",
        "primary_context": "family_life",
        "topics": ["Brillen fuer Kinder", "Sehhilfen"],
        "contexts": ["family_life", "shopping"],
        "repeated_keywords": ["brillen", "kinder"],
        "visible_headings": ["Kinderbrillen im Vergleich"],
        "site_categories": ["Kinderbrillen", "Sportbrillen"],
        "topic_clusters": ["kinderbrillen", "sportbrillen"],
        "prominent_titles": ["Kinderbrillen im Alltag"],
        "business_type": "ecommerce_optics",
        "services_or_products": ["Kinderbrillen", "Sportbrillen"],
        "business_intent": "commercial",
        "commerciality": 0.8,
        "page_count": 2,
    }

    combined = build_combined_target_profile(
        target_site_url="https://shop.example.com/schwimmbrille",
        target_site_root_url="https://shop.example.com",
        exact_profile=exact_profile,
        root_profile=root_profile,
    )

    assert combined["source_url"] == "https://shop.example.com/schwimmbrille"
    assert combined["site_root_url"] == "https://shop.example.com"
    assert combined["page_title"] == "Schwimmbrille fuer Kinder"
    assert combined["primary_context"] == "family_life"
    assert "Schwimmbrille fuer Kinder" in combined["topics"]
    assert "Brillen fuer Kinder" in combined["topics"]
    assert "Schwimmbrille" in combined["services_or_products"]
    assert "Kinderbrillen" in combined["services_or_products"]


def test_extract_keywords_filters_low_signal_terms() -> None:
    keywords = _extract_keywords(
        "Was man wissen sollte weiterlesen familie sonne schutz welche man sich navigation kinderschutz alltag",
        limit=10,
    )

    assert "man" not in keywords
    assert "welche" not in keywords
    assert "weiterlesen" not in keywords
    assert "schutz" in keywords


def test_extract_page_signals_prefers_content_region_over_form_and_nav_chrome() -> None:
    html = """
    <html>
      <body>
        <header>
          <nav>
            <a href="/kontakt">Kontakt</a>
            <a href="/impressum">Impressum</a>
          </nav>
        </header>
        <main>
          <article>
            <h1>Augenschutz im Sommerurlaub</h1>
            <h2>UV Schutz fuer Kinderaugen</h2>
            <p>Eltern achten im Sommerurlaub auf UV Schutz, Passform und Alltagstauglichkeit.</p>
            <a href="/augenschutz-kinder">Augenschutz fuer Kinder im Sommer</a>
          </article>
          <section>
            <form>
              <h2>Kontaktformular</h2>
              <label>E-Mail</label>
              <input type="email" />
            </form>
          </section>
        </main>
        <footer>
          <a href="/datenschutz">Datenschutz</a>
        </footer>
      </body>
    </html>
    """

    signals = _extract_page_signals("https://publisher.example.com", html)

    assert "Augenschutz im Sommerurlaub" in signals["headings"]
    assert "UV Schutz fuer Kinderaugen" in signals["headings"]
    assert "Kontaktformular" not in signals["headings"]
    assert "Datenschutz" not in signals["text"]


def test_extract_internal_links_prefers_content_links_over_boilerplate_links() -> None:
    html = """
    <html>
      <body>
        <header>
          <nav>
            <a href="/kontakt">Kontakt</a>
            <a href="/login">Login</a>
          </nav>
        </header>
        <main>
          <article>
            <h1>Sommerurlaub mit Kindern</h1>
            <a href="/augenschutz-kinder">Augenschutz fuer Kinder im Sommer</a>
            <a href="/uv-schutz-strand">UV Schutz am Strand richtig einordnen</a>
          </article>
        </main>
        <footer>
          <a href="/impressum">Impressum</a>
        </footer>
      </body>
    </html>
    """

    links = _extract_internal_links("https://publisher.example.com", html, limit=4)

    assert links == [
        "https://publisher.example.com/augenschutz-kinder",
        "https://publisher.example.com/uv-schutz-strand",
    ]
