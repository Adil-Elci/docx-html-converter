from creator.api.decision_schemas import MasterArticlePlan
from creator.api.supervisor import (
    CreatorSupervisor,
    PublishingCandidateInput,
    SupervisorContext,
    build_supervisor_system_prompt,
    build_supervisor_user_prompt,
)


class _StubProvider:
    def __init__(self) -> None:
        self.calls = []

    def call_schema(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
        return MasterArticlePlan.model_validate(
            {
                "publishing_site": {
                    "site_id": "site-2",
                    "site_url": "https://publisher-two.example.com",
                    "fit_reason": "The site has stronger home-planning relevance and cleaner internal-link support.",
                    "inventory_rationale": "Its existing articles cover layout, storage, and room-planning topics that support the draft naturally.",
                    "confidence": 0.87,
                },
                "topic": "Kleine Räume optimal nutzen: So gelingt clevere Wohnraumplanung",
                "intent_type": "commercial_investigation",
                "article_angle": "decision_criteria",
                "audience": "Hausbesitzerinnen und Hausbesitzer",
                "tone": "practical_informational",
                "differentiator": "The plan focuses on layout tradeoffs, storage constraints, and lighting instead of generic inspiration.",
                "title_package": {
                    "h1": "Kleine Räume optimal nutzen: Worauf es bei der Planung ankommt",
                    "meta_title": "Kleine Räume optimal nutzen: Planung, Licht und Stauraum",
                    "slug": "kleine-raeume-optimal-nutzen",
                },
                "keyword_strategy": {
                    "primary_keyword": "kleine räume optimal nutzen",
                    "secondary_keywords": ["wohnraumplanung stauraum", "kleine räume beleuchtung"],
                    "semantic_entities": ["grundriss", "laufbreite", "regal"],
                    "keyword_intent_note": "The keyword strategy serves readers who want concrete planning criteria before making layout choices.",
                },
                "backlink_plan": {
                    "strategy": "supporting_context",
                    "anchor_text": "mehr zur Raumplanung",
                    "placement_hint": "section_2",
                    "rationale": "The backlink supports a comparison point and stays editorial instead of promotional.",
                },
                "faq_questions": [
                    "Welche Möbel sparen in kleinen Räumen wirklich Platz?",
                    "Wie viel Laufbreite sollte zwischen Möbeln bleiben?",
                    "Welche Beleuchtung hilft kleinen Räumen?",
                ],
                "internal_link_titles": ["Stauraum im Flur richtig planen"],
                "sections": [
                    {
                        "section_id": "section_1",
                        "kind": "body",
                        "h2": "Welche Kriterien sind bei kleinen Räumen entscheidend?",
                        "goal": "Explain the main layout criteria with concrete homeowner examples.",
                        "key_points": ["Laufbreite", "Stauraum", "Licht"],
                        "required_terms": ["grundriss", "stauraum"],
                        "target_min_words": 100,
                        "target_max_words": 140,
                    },
                    {
                        "section_id": "section_2",
                        "kind": "body",
                        "h2": "Welche Möbel und Stauraumlösungen funktionieren wirklich?",
                        "goal": "Compare furniture choices and explain tradeoffs.",
                        "key_points": ["Tiefe", "Klappmechanik"],
                        "required_terms": ["regal"],
                        "target_min_words": 100,
                        "target_max_words": 140,
                    },
                    {
                        "section_id": "section_3",
                        "kind": "faq",
                        "h2": "FAQ",
                        "goal": "Answer the top follow-up questions directly.",
                        "key_points": ["Kurz", "Präzise"],
                        "required_terms": [],
                        "target_min_words": 90,
                        "target_max_words": 140,
                    },
                ],
                "risk_notes": ["Avoid generic lifestyle phrasing."],
            }
        )


def _sample_context() -> SupervisorContext:
    return SupervisorContext(
        target_site_url="https://www.eigenheim-blog.com/",
        target_profile={"primary_context": "home", "topics": ["wohnen", "haus", "raumplanung"]},
        publishing_candidates=[
            PublishingCandidateInput(
                site_url="https://publisher-one.example.com",
                site_id="site-1",
                fit_score=0.72,
                inventory_count=18,
                internal_link_titles=["Besser wohnen mit Licht"],
                profile={"primary_context": "home"},
                notes=["Solid general home coverage."],
            ),
            PublishingCandidateInput(
                site_url="https://publisher-two.example.com",
                site_id="site-2",
                fit_score=0.81,
                inventory_count=34,
                internal_link_titles=["Stauraum im Flur richtig planen", "Wohnzimmer besser zonieren"],
                profile={"primary_context": "home"},
                notes=["Stronger room-planning inventory."],
            ),
        ],
        exclude_topics=["Wohnräume gestalten: Praktische Tipps für Hausbesitzer"],
        recent_article_titles=["Wohnräume gestalten: Was man konkret beachten sollte"],
        target_keyword_hints=["kleine räume", "wohnraumplanung", "stauraum"],
        target_context_notes=["Audience is homeowners looking for practical planning advice."],
    )


def test_build_supervisor_system_prompt_mentions_master_plan_schema() -> None:
    prompt = build_supervisor_system_prompt()

    assert "master article plan" in prompt.lower()
    assert "publishing_site" in prompt
    assert "keyword_strategy" in prompt


def test_build_supervisor_user_prompt_embeds_candidate_context() -> None:
    prompt = build_supervisor_user_prompt(_sample_context())

    assert "publisher-two.example.com" in prompt
    assert "wohnraumplanung" in prompt
    assert "recent_article_titles" in prompt


def test_create_master_article_plan_calls_provider_with_expected_schema() -> None:
    provider = _StubProvider()
    supervisor = CreatorSupervisor(provider=provider)

    result = supervisor.create_master_article_plan(_sample_context(), request_label="test_supervisor")

    assert result.publishing_site.site_url == "https://publisher-two.example.com"
    assert provider.calls[0]["request_label"] == "test_supervisor"
    assert provider.calls[0]["schema_model"] is MasterArticlePlan
