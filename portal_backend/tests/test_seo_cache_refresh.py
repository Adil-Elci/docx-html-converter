from portal_backend.api.seo_cache_refresh import _build_publishing_site_cache_payload


def test_build_publishing_site_cache_payload_uses_inventory_clusters():
    payload = _build_publishing_site_cache_payload(
        [
            {
                "url": "https://publisher.example.com/baby-schlaf-tipps",
                "title": "Baby Schlaf Tipps fuer die ersten Monate",
                "categories": ["Baby", "Familie"],
            },
            {
                "url": "https://publisher.example.com/kliniktasche-checkliste",
                "title": "Kliniktasche Checkliste fuer die Geburt",
                "categories": ["Schwangerschaft"],
            },
        ]
    )

    assert payload["allowed_topics"]
    assert "Baby" in payload["site_categories"]
    assert payload["topic_clusters"]
    assert payload["sample_urls"]
