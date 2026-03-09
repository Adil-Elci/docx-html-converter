from portal_backend.api.site_profiles import score_publishing_site_fit


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
