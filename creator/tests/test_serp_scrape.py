from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from creator.api import serp_scrape
from creator.api.dataforseo import OrganicResult
from creator.api.serp_scrape import (
    ScrapedCompetitor,
    scrape_competitor,
    scrape_top_results,
)


SAMPLE_HTML = """
<!doctype html>
<html lang="de">
<head>
    <title>Steuerberater Hamburg finden — Beispielseite</title>
    <script type="application/ld+json">
      {"@context":"https://schema.org","@type":"Article","headline":"Test"}
    </script>
    <script type="application/ld+json">
      {"@context":"https://schema.org","@type":"FAQPage","mainEntity":[]}
    </script>
</head>
<body>
    <header><nav><a href="/about">Über uns</a></nav></header>
    <main>
        <article>
            <h1>Steuerberater in Hamburg: So finden Sie den richtigen Berater</h1>
            <p>Wer in Hamburg einen Steuerberater sucht, sollte folgende Kriterien beachten.</p>
            <h2>Auswahlkriterien</h2>
            <p>Spezialisierung, Honorar, Erreichbarkeit.</p>
            <h3>Spezialisierung</h3>
            <p>Branchen-Know-how ist entscheidend.</p>
            <h2>Kosten und Honorare</h2>
            <p>Honorare richten sich nach der Steuerberatergebührenverordnung.</p>
            <a href="https://example.de/honorare">Honorare</a>
            <a href="https://other-site.de/preisliste">Preisliste extern</a>
            <a href="/kontakt">Kontakt</a>
        </article>
    </main>
    <footer><a href="/impressum">Impressum</a></footer>
</body>
</html>
"""


def _mock_response(*, status_code: int = 200, text: str = SAMPLE_HTML, final_url: str = "https://example.de/steuerberater") -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    response.url = final_url
    return response


def test_scrape_competitor_extracts_title_headings_body():
    with patch("creator.api.serp_scrape.requests.get", return_value=_mock_response()):
        result = scrape_competitor("https://example.de/steuerberater")
    assert result.fetch_status == "ok"
    assert result.title == "Steuerberater Hamburg finden — Beispielseite"
    assert result.h1 == "Steuerberater in Hamburg: So finden Sie den richtigen Berater"
    assert result.h2s == ["Auswahlkriterien", "Kosten und Honorare"]
    assert result.h3s == ["Spezialisierung"]
    assert "Branchen-Know-how" in result.body_text
    assert result.word_count > 10


def test_scrape_competitor_counts_links_correctly():
    with patch("creator.api.serp_scrape.requests.get", return_value=_mock_response()):
        result = scrape_competitor("https://example.de/steuerberater")
    # internal: /honorare, /kontakt, plus the dropped header/footer anchors are ignored.
    # The article body has /honorare (internal) and /kontakt (internal).
    # Note: links from <header> and <footer> are dropped during body extraction
    # but link counting walks the FULL document, so /about and /impressum count too.
    assert result.internal_link_count >= 2
    assert result.external_link_count == 1


def test_scrape_competitor_extracts_schema_types():
    with patch("creator.api.serp_scrape.requests.get", return_value=_mock_response()):
        result = scrape_competitor("https://example.de/steuerberater")
    assert "Article" in result.schema_types
    assert "FAQPage" in result.schema_types
    assert result.has_article_schema is True
    assert result.has_faq_schema is True


def test_scrape_competitor_handles_403():
    with patch("creator.api.serp_scrape.requests.get", return_value=_mock_response(status_code=403, text="")):
        result = scrape_competitor("https://blocked.de/foo")
    assert result.fetch_status == "forbidden"
    assert result.http_status == 403
    assert result.title == ""


def test_scrape_competitor_handles_timeout():
    with patch("creator.api.serp_scrape.requests.get", side_effect=requests.Timeout()):
        result = scrape_competitor("https://slow.de/foo")
    assert result.fetch_status == "timeout"
    assert result.http_status is None


def test_scrape_competitor_handles_network_error():
    with patch("creator.api.serp_scrape.requests.get", side_effect=requests.ConnectionError("dns")):
        result = scrape_competitor("https://gone.de/foo")
    assert result.fetch_status == "network_error"


def test_scrape_competitor_handles_empty_body():
    with patch("creator.api.serp_scrape.requests.get", return_value=_mock_response(text="   ")):
        result = scrape_competitor("https://blank.de/foo")
    assert result.fetch_status == "empty"


def test_scrape_competitor_captures_redirect_final_url():
    with patch(
        "creator.api.serp_scrape.requests.get",
        return_value=_mock_response(final_url="https://canonical.de/steuerberater"),
    ):
        result = scrape_competitor("https://example.de/steuerberater")
    assert result.final_url == "https://canonical.de/steuerberater"


def test_scrape_competitor_handles_invalid_schema_json():
    html_with_bad_json = SAMPLE_HTML.replace(
        '"@type":"FAQPage"', '"@type":INVALID_JSON'
    )
    with patch("creator.api.serp_scrape.requests.get", return_value=_mock_response(text=html_with_bad_json)):
        result = scrape_competitor("https://example.de/steuerberater")
    assert result.fetch_status == "ok"
    assert "Article" in result.schema_types  # the valid one survives


def test_scrape_competitor_dedupes_schema_types_case_insensitive():
    html = """
    <html><body>
    <script type="application/ld+json">{"@type":"Article"}</script>
    <script type="application/ld+json">{"@type":"article"}</script>
    <script type="application/ld+json">{"@type":["Article","NewsArticle"]}</script>
    </body></html>
    """
    with patch("creator.api.serp_scrape.requests.get", return_value=_mock_response(text=html)):
        result = scrape_competitor("https://x.de")
    assert len([t for t in result.schema_types if t.lower() == "article"]) == 1
    assert "NewsArticle" in result.schema_types


def test_scrape_top_results_processes_each_url():
    organics = [
        OrganicResult(rank=1, url="https://a.de", title="A", description="", domain="a.de"),
        OrganicResult(rank=2, url="https://b.de", title="B", description="", domain="b.de"),
        OrganicResult(rank=3, url="https://c.de", title="C", description="", domain="c.de"),
    ]
    call_urls = []

    def fake_get(url, headers, timeout, allow_redirects):
        call_urls.append(url)
        return _mock_response(final_url=url)

    with patch("creator.api.serp_scrape.requests.get", side_effect=fake_get):
        results = scrape_top_results(organics, top_n=2)

    assert len(results) == 2
    assert call_urls == ["https://a.de", "https://b.de"]


def test_scrape_top_results_top_n_zero_returns_empty():
    organics = [OrganicResult(rank=1, url="https://a.de", title="A", description="", domain="a.de")]
    with patch("creator.api.serp_scrape.requests.get") as mocked:
        results = scrape_top_results(organics, top_n=0)
    assert results == []
    mocked.assert_not_called()


def test_scrape_competitor_skips_anchor_and_protocol_only_links():
    html = """
    <html><body><article>
    <h1>X</h1>
    <a href="#section">jump</a>
    <a href="mailto:foo@example.de">email</a>
    <a href="tel:+49123">call</a>
    <a href="javascript:void(0)">js</a>
    <a href="https://example.de/real">real internal</a>
    </article></body></html>
    """
    with patch("creator.api.serp_scrape.requests.get", return_value=_mock_response(text=html)):
        result = scrape_competitor("https://example.de/foo")
    assert result.internal_link_count == 1
    assert result.external_link_count == 0
