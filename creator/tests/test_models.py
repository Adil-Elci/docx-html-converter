from creator.api.models import CreatorRequest


def test_creator_request_allows_optional_publishing_site() -> None:
    payload = CreatorRequest(
        target_site_url="https://target.example.com",
        publishing_site_id="site-123",
        client_target_site_id="target-123",
        target_profile={"content_hash": "abc", "payload": {"normalized_url": "https://target.example.com", "topics": ["familie"]}},
        publishing_profile={"content_hash": "def", "payload": {"normalized_url": "https://publisher.example.com", "topics": ["familie"]}},
    )

    assert payload.publishing_site_url is None
    assert payload.publishing_site_id == "site-123"
