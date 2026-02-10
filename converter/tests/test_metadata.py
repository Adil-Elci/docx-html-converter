import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.server import generate_excerpt, generate_meta_description


def _assert_truncated_at_word_boundary(original: str, truncated: str) -> None:
    assert original.startswith(truncated)
    if len(truncated) < len(original):
        assert original[len(truncated)] == " "


def test_excerpt_truncation_word_boundary():
    text = "Dies ist ein einfacher Beispielsatz fuer einen kurzen Test ohne Punkt"
    html = f"<p>{text}</p>"
    excerpt, _ = generate_excerpt(html, 30)
    assert len(excerpt) <= 30
    _assert_truncated_at_word_boundary(text, excerpt)


def test_meta_truncation_word_boundary():
    text = (
        "Dies ist ein erster Satz. "
        "Dies ist ein zweiter Satz mit genug Laenge fuer einen Test."
    )
    html = f"<p>{text}</p>"
    meta, _ = generate_meta_description(html, 40)
    assert len(meta) <= 40
    _assert_truncated_at_word_boundary(text, meta)
