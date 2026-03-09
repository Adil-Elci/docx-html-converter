from portal_backend.api.site_analysis_cache import build_site_analysis_content_hash


def test_build_site_analysis_content_hash_supports_structured_payloads():
    payload = {
        "sample_urls": ["https://example.com", "https://example.com/a"],
        "allowed_topics": ["Familie", "Schwangerschaft"],
    }

    first = build_site_analysis_content_hash(payload)
    second = build_site_analysis_content_hash(
        {
            "allowed_topics": ["Familie", "Schwangerschaft"],
            "sample_urls": ["https://example.com", "https://example.com/a"],
        }
    )

    assert first
    assert first == second
