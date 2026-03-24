from creator.api.critic import CreatorCritic, CriticContext, build_critic_system_prompt, build_critic_user_prompt
from creator.api.decision_schemas import CriticReview, DraftArticlePayload


class _StubCriticProvider:
    def __init__(self) -> None:
        self.calls = []

    def call_schema(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
        return CriticReview.model_validate(
            {
                "verdict": "repair_needed",
                "overall_score": 74,
                "plan_alignment_score": 82,
                "editorial_quality_score": 71,
                "seo_quality_score": 68,
                "strengths": ["The body contains concrete homeowner examples."],
                "issues": [
                    {
                        "code": "heading_phrase_invalid",
                        "severity": "medium",
                        "summary": "The first H2 is still too scaffold-like.",
                        "location_hint": "section_1",
                        "recommended_fix": "Rewrite the H2 as a natural homeowner question.",
                    }
                ],
                "repair_instructions": ["Rewrite the first H2 and strengthen FAQ answers."],
                "final_recommendation": "Repair before approval.",
            }
        )


def _sample_master_plan():  # type: ignore[no-untyped-def]
    from creator.api.decision_schemas import MasterArticlePlan

    return MasterArticlePlan.model_validate(
        {
            "publishing_site": {
                "site_id": "site-2",
                "site_url": "https://publisher-two.example.com",
                "fit_reason": "Strong topical fit for homeowners and room-planning topics.",
                "inventory_rationale": "The site already has several home-planning articles that support internal links.",
                "confidence": 0.84,
            },
            "topic": "Kleine Räume optimal nutzen: Tipps für clevere Wohnraumplanung",
            "intent_type": "commercial_investigation",
            "article_angle": "decision_criteria",
            "audience": "Hausbesitzerinnen und Hausbesitzer",
            "tone": "practical_informational",
            "differentiator": "Combines layout tradeoffs with concrete storage and lighting decisions.",
            "title_package": {
                "h1": "Kleine Räume optimal nutzen: Worauf es bei der Planung ankommt",
                "meta_title": "Kleine Räume optimal nutzen: Planung, Licht und Stauraum",
                "slug": "kleine-raeume-optimal-nutzen",
            },
            "keyword_strategy": {
                "primary_keyword": "kleine räume optimal nutzen",
                "secondary_keywords": ["wohnraumplanung stauraum", "kleine räume beleuchtung"],
                "semantic_entities": ["grundriss", "laufbreite", "regal"],
                "keyword_intent_note": "The keyword strategy supports planning and evaluation intent.",
            },
            "backlink_plan": {
                "strategy": "supporting_context",
                "anchor_text": "mehr zur Raumplanung",
                "placement_hint": "section_2",
                "rationale": "The backlink supports a planning comparison naturally.",
            },
            "faq_questions": [
                "Welche Möbel sparen Platz?",
                "Wie viel Laufbreite sollte bleiben?",
                "Welche Beleuchtung hilft kleinen Räumen?",
            ],
            "internal_link_titles": ["Stauraum im Flur richtig planen"],
            "sections": [
                {
                    "section_id": "section_1",
                    "kind": "body",
                    "h2": "Welche Kriterien sind bei kleinen Räumen entscheidend?",
                    "goal": "Explain the main layout criteria.",
                    "key_points": ["Laufbreite", "Stauraum", "Licht"],
                    "required_terms": ["grundriss", "stauraum"],
                    "target_min_words": 100,
                    "target_max_words": 140,
                },
                {
                    "section_id": "section_2",
                    "kind": "fazit",
                    "h2": "Fazit",
                    "goal": "Summarize the practical takeaway.",
                    "key_points": ["Klarheit"],
                    "required_terms": [],
                    "target_min_words": 70,
                    "target_max_words": 100,
                },
                {
                    "section_id": "section_3",
                    "kind": "faq",
                    "h2": "FAQ",
                    "goal": "Answer follow-up questions directly.",
                    "key_points": ["Kurz", "Präzise"],
                    "required_terms": [],
                    "target_min_words": 90,
                    "target_max_words": 140,
                },
            ],
            "risk_notes": ["Avoid generic lifestyle phrasing."],
        }
    )


def _sample_draft():  # type: ignore[no-untyped-def]
    return DraftArticlePayload.model_validate(
        {
            "article_html": (
                "<h1>Kleine Räume optimal nutzen: Worauf es bei der Planung ankommt</h1>"
                "<p>Kleine Räume profitieren von klaren Laufwegen, wandhohem Stauraum und gezielter Beleuchtung.</p>"
                "<h2>Welche Kriterien sind bei kleinen Räumen entscheidend?</h2>"
                "<p>Eine Laufbreite von rund 90 cm, helle Lichtquellen und Regale bis knapp unter die Decke helfen bei der Raumplanung.</p>"
                "<h2>Fazit</h2><p>Mit klaren Maßen und Stauraumplanung wirken kleine Räume ruhiger und funktionaler.</p>"
                "<h2>FAQ</h2><h3>Welche Möbel sparen Platz?</h3><p>Klapp- und Mehrzweckmöbel helfen besonders in kleinen Grundrissen.</p>"
            ),
            "meta_title": "Kleine Räume optimal nutzen: Planung, Licht und Stauraum",
            "meta_description": "Konkrete Tipps zu Stauraum, Laufbreite, Licht und Möbelwahl für kleine Räume mit praxisnaher Wohnraumplanung.",
            "slug": "kleine-raeume-optimal-nutzen",
            "excerpt": "Konkrete Planungstipps für kleine Räume mit Fokus auf Stauraum, Licht und Laufwegen.",
        }
    )


def test_build_critic_system_prompt_mentions_review_schema() -> None:
    prompt = build_critic_system_prompt()

    assert "criticreview" in prompt.lower()
    assert "repair_needed" in prompt


def test_build_critic_user_prompt_embeds_plan_and_draft() -> None:
    context = CriticContext(
        target_site_url="https://www.eigenheim-blog.com/",
        publishing_site_url="https://publisher-two.example.com",
        master_plan=_sample_master_plan(),
        draft_article=_sample_draft(),
        deterministic_validation_errors=["specificity_too_low:2"],
        content_brief="Practical homeowner advice with concrete layout examples.",
        internal_link_titles=["Stauraum im Flur richtig planen"],
    )

    prompt = build_critic_user_prompt(context)

    assert "specificity_too_low:2" in prompt
    assert "publisher-two.example.com" in prompt
    assert "Kleine Räume optimal nutzen" in prompt


def test_critic_calls_provider_with_review_schema() -> None:
    provider = _StubCriticProvider()
    critic = CreatorCritic(provider=provider)
    context = CriticContext(
        target_site_url="https://www.eigenheim-blog.com/",
        publishing_site_url="https://publisher-two.example.com",
        master_plan=_sample_master_plan(),
        draft_article=_sample_draft(),
        deterministic_validation_errors=[],
        content_brief="Practical homeowner advice with concrete layout examples.",
        internal_link_titles=["Stauraum im Flur richtig planen"],
    )

    result = critic.review_article(context, request_label="critic_test")

    assert result.verdict == "repair_needed"
    assert provider.calls[0]["request_label"] == "critic_test"
    assert provider.calls[0]["schema_model"] is CriticReview
