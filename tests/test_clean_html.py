import sys
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.append(str(Path(__file__).resolve().parents[1] / "website_backend"))

from api.models import ConvertOptions
from api.server import sanitize_html


def test_clean_html_rules():
    html = (
        "<h1>Title</h1>"
        "<p class='lead' style='color:red'>Intro</p>"
        "<img src='x' />"
        "<h3>Sub</h3>"
        "<a href='https://www.google.com/url?q=https%3A%2F%2Fexample.com&sa=D'>X</a>"
    )
    options = ConvertOptions(remove_images=True, fix_headings=True)
    cleaned = sanitize_html(html, options)

    assert "<h1" not in cleaned
    assert "class=" not in cleaned
    assert "style=" not in cleaned

    soup = BeautifulSoup(cleaned, "lxml")
    assert soup.find("img") is None

    link = soup.find("a")
    assert link is not None
    assert link.get("href") == "https://example.com"
