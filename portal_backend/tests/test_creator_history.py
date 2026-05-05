from __future__ import annotations

from portal_backend.api.creator_history import collect_recent_creator_history


def test_collect_recent_creator_history_reads_v2_contract_fields() -> None:
    """v2 jobs store the topic under phase3.target_keyword.keyword and
    pipeline_state.contract.target_keyword. Earlier the collector only read
    legacy 4llm fields, so v2 jobs silently contributed nothing to dedup.
    This test pins the v2 path."""

    history = collect_recent_creator_history(
        [
            {
                "phase3": {
                    "target_keyword": {"keyword": "kurzsichtigkeit kinder"},
                },
                "phase5": {
                    "title": "Kurzsichtigkeit bei Kindern: Warum mehr Grundschüler eine Brille brauchen",
                    "meta_title": "Kurzsichtigkeit bei Kindern 2026 — was Eltern wissen sollten",
                },
                "pipeline_state": {
                    "contract": {
                        "target_keyword": "kurzsichtigkeit kinder",
                        "h1": "Kurzsichtigkeit bei Kindern: Warum mehr Grundschüler eine Brille brauchen",
                    },
                },
            },
        ],
    )
    assert "kurzsichtigkeit kinder" in history["exclude_topics"]
    assert any("Kurzsichtigkeit" in title for title in history["recent_article_titles"])


def test_collect_recent_creator_history_merges_topics_and_titles_without_duplicates() -> None:
    history = collect_recent_creator_history(
        [
            {
                "phase3": {
                    "final_article_topic": "Was kosten hochwertige Nahrungsergaenzungsmittel wirklich?",
                    "primary_keyword": "hochwertige nahrungsergaenzungsmittel kosten",
                    "title_package": {"h1": "Was kosten hochwertige Nahrungsergaenzungsmittel wirklich?"},
                },
                "phase4": {"h1": "Was kosten hochwertige Nahrungsergaenzungsmittel wirklich?"},
                "phase5": {"meta_title": "Was kosten hochwertige Nahrungsergaenzungsmittel wirklich?"},
            },
            {
                "phase3": {
                    "final_article_topic": "Veganes Protein im Vergleich",
                    "primary_keyword": "veganes protein vergleich",
                    "title_package": {"h1": "Veganes Protein im Vergleich: Worauf man achten sollte"},
                },
                "phase4": {"h1": "Veganes Protein im Vergleich: Worauf man achten sollte"},
                "phase5": {"meta_title": "Veganes Protein im Vergleich"},
            },
        ],
        max_topics=8,
        max_titles=8,
    )

    assert history["exclude_topics"][0] == "Was kosten hochwertige Nahrungsergaenzungsmittel wirklich?"
    assert "hochwertige nahrungsergaenzungsmittel kosten" in history["exclude_topics"]
    assert "Veganes Protein im Vergleich: Worauf man achten sollte" in history["recent_article_titles"]
    assert len(history["recent_article_titles"]) == len(set(history["recent_article_titles"]))
