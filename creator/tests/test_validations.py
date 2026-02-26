from creator.api.validators import (
    validate_backlink_placement,
    validate_hyperlink_count,
    validate_word_count,
    word_count_from_html,
)


def test_validate_hyperlink_count():
    html = "<h1>Title</h1><p>Intro <a href='https://example.com'>link</a></p>"
    assert validate_hyperlink_count(html, 1) is None
    html2 = html + "<p><a href='https://example.com/2'>second</a></p>"
    assert validate_hyperlink_count(html2, 1) is not None


def test_validate_word_count():
    html = "<p>one two three four five</p>"
    assert word_count_from_html(html) == 5
    assert validate_word_count(html, 3, 6) is None
    assert validate_word_count(html, 6, 10) is not None


def test_validate_backlink_placement_intro():
    html = """
    <h1>Title</h1>
    <p>Intro <a href='https://target.com'>Target</a></p>
    <h2>Section One</h2>
    <p>Body</p>
    """
    assert validate_backlink_placement(html, "https://target.com", "intro") is None
    assert validate_backlink_placement(html, "https://target.com", "section_1") is not None


def test_validate_backlink_placement_section():
    html = """
    <h1>Title</h1>
    <p>Intro paragraph.</p>
    <h2>Section One</h2>
    <p>Body <a href='https://target.com'>Target</a></p>
    <h2>Section Two</h2>
    <p>More</p>
    """
    assert validate_backlink_placement(html, "https://target.com", "section_1") is None
    assert validate_backlink_placement(html, "https://target.com", "intro") is not None
