from portal_backend.api.internal_linking import _derive_inventory_excerpt


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
