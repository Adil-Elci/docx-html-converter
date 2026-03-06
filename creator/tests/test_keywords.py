from creator.api.pipeline import (
    KEYWORD_MIN_SECONDARY,
    _select_keywords,
    _validate_keyword_coverage,
)


def test_select_keywords_contract():
    result = _select_keywords(
        topic="Eltern-Sucht in der Schwangerschaft",
        llm_primary="Eltern-Sucht Schwangerschaft",
        llm_secondary=[
            "Auswirkungen auf Familienbeziehungen",
            "Unterstuetzung fuer betroffene Familien",
            "Praevention bei Suchterkrankung",
        ],
        keyword_cluster=["eltern", "schwangerschaft", "familie", "sucht"],
        allowed_topics=[
            "Hilfsangebote fuer Familien in Krisensituationen",
            "Psychische Gesundheit waehrend der Schwangerschaft",
        ],
        trend_candidates=[
            "eltern sucht schwangerschaft",
            "hilfe fuer suchtbelastete familien",
            "auswirkungen eltern sucht kinder",
            "beratung bei suchterkrankung in der familie",
        ],
    )

    assert isinstance(result["primary_keyword"], str) and result["primary_keyword"].strip()
    assert KEYWORD_MIN_SECONDARY <= len(result["secondary_keywords"]) <= 6
    assert len(set(result["secondary_keywords"])) == len(result["secondary_keywords"])


def test_validate_keyword_coverage_missing_primary_locations():
    html = """
    <h1>Auswirkungen auf Familienbeziehungen</h1>
    <p>Dieser Beitrag beleuchtet zentrale Aspekte fuer betroffene Familien.</p>
    <h2>Ursachen und Hintergruende</h2>
    <p>Viele Faktoren wirken zusammen.</p>
    <h2>Fazit</h2>
    <p>Ein guter Abschluss mit klaren Schritten.</p>
    """
    errors = _validate_keyword_coverage(
        html,
        primary_keyword="eltern sucht schwangerschaft",
        secondary_keywords=[
            "auswirkungen auf familienbeziehungen",
            "unterstuetzung fuer betroffene familien",
            "praevention bei suchterkrankung",
            "hilfsangebote fuer familien",
        ],
    )
    assert "primary_keyword_missing_h1" in errors
    assert "primary_keyword_missing_intro" in errors


def test_validate_keyword_coverage_ok():
    html = """
    <h1>Eltern Sucht Schwangerschaft: Auswirkungen und Hilfe</h1>
    <p>Eltern sucht schwangerschaft betrifft viele Familien und erfordert fruehe Hilfe.</p>
    <h2>Eltern Sucht Schwangerschaft im Alltag</h2>
    <p>Auswirkungen auf familienbeziehungen sind deutlich sichtbar.</p>
    <p>Unterstuetzung fuer betroffene familien ist zentral.</p>
    <h2>Praevention und Hilfsangebote</h2>
    <p>Praevention bei suchterkrankung gelingt besser mit lokalen Hilfsangeboten fuer familien.</p>
    <h2>Fazit</h2>
    <p>Die Lage ist herausfordernd, aber mit frueher Hilfe verbessert sich die Perspektive.</p>
    """
    errors = _validate_keyword_coverage(
        html,
        primary_keyword="eltern sucht schwangerschaft",
        secondary_keywords=[
            "auswirkungen auf familienbeziehungen",
            "unterstuetzung fuer betroffene familien",
            "praevention bei suchterkrankung",
            "hilfsangebote fuer familien",
        ],
    )
    assert not errors
