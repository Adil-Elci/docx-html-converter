from portal_backend.api.internal_linking import (
    _derive_inventory_excerpt,
    build_creator_internal_link_inventory_from_post_payloads,
)


def test_derive_inventory_excerpt_uses_content_when_wp_excerpt_is_meta_stub():
    excerpt = _derive_inventory_excerpt(
        {
            "excerpt": {
                "rendered": "<p>Here is a meta description with a maximum of 100 characters:</p><p>Option 1</p>",
            },
            "content": {
                "rendered": (
                    "<p>Mit dem 40. Lebensjahr verändert sich die Sehkraft spürbar. "
                    "Die Augen verlieren an Anpassungsfähigkeit und kleine Schrift wird anstrengender.</p>"
                ),
            },
        }
    )

    assert "Sehkraft" in excerpt
    assert "meta description" not in excerpt.lower()


def test_derive_inventory_excerpt_keeps_real_excerpt():
    excerpt = _derive_inventory_excerpt(
        {
            "excerpt": {
                "rendered": (
                    "<p>Eltern achten auf UV400-Schutz, robuste Materialien und eine kindgerechte Passform.</p>"
                ),
            },
            "content": {
                "rendered": "<p>Langform.</p>",
            },
        }
    )

    assert excerpt == "Eltern achten auf UV400-Schutz, robuste Materialien und eine kindgerechte Passform."


def test_derive_inventory_excerpt_collects_multiple_paragraphs_for_topic_signal():
    excerpt = _derive_inventory_excerpt(
        {
            "excerpt": {
                "rendered": "<p>Meta-Beschreibung:</p><p>Option 1</p>",
            },
            "content": {
                "rendered": (
                    "<p>Die Augen verlieren an Anpassungsfaehigkeit und kleine Schrift wird anstrengender.</p>"
                    "<p>Mit dem richtigen Wissen laesst sich die Augengesundheit gezielt unterstuetzen.</p>"
                ),
            },
        }
    )

    assert "Die Augen verlieren" in excerpt
    assert "Augengesundheit" in excerpt


def test_build_creator_internal_link_inventory_from_post_payloads_uses_content_excerpt():
    inventory = build_creator_internal_link_inventory_from_post_payloads(
        [
            {
                "link": "https://publisher.example.com/sehstaerke",
                "slug": "sehstaerke",
                "date_gmt": "2026-03-01T10:00:00",
                "title": {"rendered": "Sehstaerke im Alltag"},
                "excerpt": {"rendered": "<p>Meta-Beschreibung:</p><p>Option 1</p>"},
                "content": {
                    "rendered": (
                        "<p>Die Augen verlieren an Anpassungsfaehigkeit und kleine Schrift wird anstrengender.</p>"
                        "<p>Mit dem richtigen Wissen laesst sich die Augengesundheit gezielt unterstuetzen.</p>"
                    )
                },
            }
        ]
    )

    assert inventory == [
        {
            "url": "https://publisher.example.com/sehstaerke",
            "title": "Sehstaerke im Alltag",
            "excerpt": (
                "Die Augen verlieren an Anpassungsfaehigkeit und kleine Schrift wird anstrengender. "
                "Mit dem richtigen Wissen laesst sich die Augengesundheit gezielt unterstuetzen."
            ),
            "slug": "sehstaerke",
            "categories": [],
            "published_at": "2026-03-01T10:00:00",
        }
    ]
