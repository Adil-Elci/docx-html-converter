from __future__ import annotations

from portal_backend.api.creator_history import collect_recent_creator_history


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
