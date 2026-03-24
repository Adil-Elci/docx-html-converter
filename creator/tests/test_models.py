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


def test_creator_request_accepts_publishing_candidates() -> None:
    payload = CreatorRequest(
        target_site_url="https://target.example.com",
        publishing_candidates=[
            {
                "site_url": "https://publisher-one.example.com",
                "site_id": "site-1",
                "fit_score": 0.81,
                "notes": ["Strong home-planning inventory."],
                "internal_link_inventory": [
                    {
                        "url": "https://publisher-one.example.com/article-1",
                        "title": "Stauraum im Flur richtig planen",
                    }
                ],
                "publishing_profile": {
                    "content_hash": "def",
                    "payload": {
                        "normalized_url": "https://publisher-one.example.com",
                        "topics": ["wohnen", "haus"],
                    },
                },
            }
        ],
        target_profile={"content_hash": "abc", "payload": {"normalized_url": "https://target.example.com", "topics": ["familie"]}},
    )

    assert payload.publishing_candidates[0].site_id == "site-1"
    assert str(payload.publishing_candidates[0].site_url) == "https://publisher-one.example.com/"
    assert payload.publishing_candidates[0].internal_link_inventory[0].title == "Stauraum im Flur richtig planen"
