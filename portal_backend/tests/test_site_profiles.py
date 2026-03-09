from portal_backend.api.site_profiles import _extract_keywords, build_combined_target_profile, score_publishing_site_fit


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
