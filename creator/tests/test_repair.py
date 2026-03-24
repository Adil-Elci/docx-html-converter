from creator.api.decision_schemas import CriticReview, DraftArticleSlotsPayload
from creator.api.repair import CreatorRepair, RepairContext, build_repair_system_prompt, build_repair_user_prompt
from creator.tests.test_critic import _sample_draft, _sample_master_plan


class _StubRepairProvider:
    def __init__(self) -> None:
        self.calls = []

    def call_schema(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
        return DraftArticleSlotsPayload.model_validate(
            {
                "intro_html": "<p>Konkrete Planungshinweise mit Laufbreite, Licht und Stauraum.</p>",
                "section_bodies": [
                    {
                        "section_id": "section_1",
                        "body_html": "<p>Wandhohe Regale, klare Laufwege und mehrere Lichtquellen verbessern die Nutzung.</p>",
                    },
                    {
                        "section_id": "section_2",
                        "body_html": "<p>Mit klaren Maßen und Stauraumplanung wirken kleine Räume ruhiger.</p>",
                    },
                ],
                "faq_answers": [
                    {
                        "question": "Welche Möbel sparen Platz?",
                        "answer_html": "<p>Klapp- und Mehrzweckmöbel helfen.</p>",
                    }
                ],
                "meta_title": "Kleine Räume optimal nutzen: Planung, Licht und Stauraum",
                "meta_description": "Konkrete Tipps zu Stauraum, Laufbreite, Licht und Möbelwahl für kleine Räume mit klaren Planungsbeispielen.",
                "slug": "kleine-raeume-optimal-nutzen",
                "excerpt": "Konkrete Planungstipps für kleine Räume.",
            }
        )


def _sample_review() -> CriticReview:
    return CriticReview.model_validate(
        {
            "verdict": "repair_needed",
            "overall_score": 74,
            "plan_alignment_score": 81,
            "editorial_quality_score": 72,
            "seo_quality_score": 69,
            "title_quality_score": 71,
            "heading_quality_score": 67,
            "intent_consistency_score": 78,
            "backlink_naturalness_score": 74,
            "specificity_score": 66,
            "spam_risk_score": 17,
            "coherence_score": 79,
            "strengths": ["Useful body details."],
            "issues": [
                {
                    "code": "heading_phrase_invalid",
                    "severity": "medium",
                    "summary": "The first H2 is too scaffold-like.",
                    "location_hint": "section_1",
                    "recommended_fix": "Rewrite the first H2 as a natural question.",
                }
            ],
            "repair_instructions": ["Rewrite the first H2 as a natural homeowner question."],
            "final_recommendation": "Repair before approval.",
        }
    )


def test_build_repair_system_prompt_mentions_draft_schema() -> None:
    prompt = build_repair_system_prompt()

    assert "draftarticleslotspayload" in prompt.lower()
    assert "Do not add hyperlinks" in prompt


def test_build_repair_user_prompt_embeds_critic_review() -> None:
    context = RepairContext(
        target_site_url="https://www.eigenheim-blog.com/",
        publishing_site_url="https://publisher-two.example.com",
        master_plan=_sample_master_plan(),
        draft_article=_sample_draft(),
        critic_review=_sample_review(),
        deterministic_validation_errors=["heading_phrase_invalid"],
        content_brief="Practical homeowner advice with concrete layout examples.",
        internal_link_titles=["Stauraum im Flur richtig planen"],
    )

    prompt = build_repair_user_prompt(context)

    assert "heading_phrase_invalid" in prompt
    assert "repair_needed" in prompt
    assert "publisher-two.example.com" in prompt


def test_repair_calls_provider_with_draft_schema() -> None:
    provider = _StubRepairProvider()
    repair = CreatorRepair(provider=provider)
    context = RepairContext(
        target_site_url="https://www.eigenheim-blog.com/",
        publishing_site_url="https://publisher-two.example.com",
        master_plan=_sample_master_plan(),
        draft_article=_sample_draft(),
        critic_review=_sample_review(),
        deterministic_validation_errors=[],
        content_brief="Practical homeowner advice with concrete layout examples.",
        internal_link_titles=["Stauraum im Flur richtig planen"],
    )

    result = repair.repair_article(context, request_label="repair_test")

    assert result.slug == "kleine-raeume-optimal-nutzen"
    assert provider.calls[0]["request_label"] == "repair_test"
    assert provider.calls[0]["schema_model"] is DraftArticleSlotsPayload
