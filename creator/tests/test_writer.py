from creator.api.decision_schemas import DraftArticlePayload
from creator.api.llm import LLMError
from creator.api.supervisor import PublishingCandidateInput
from creator.api.writer import CreatorWriter, WriterContext, build_writer_system_prompt, build_writer_user_prompt


class _StubWriterProvider:
    def __init__(self) -> None:
        self.calls = []

    def call_schema(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
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


class _RetryingWriterProvider:
    def __init__(self) -> None:
        self.schema_calls = []
        self.json_calls = []

    def call_schema(self, **kwargs):  # type: ignore[no-untyped-def]
        self.schema_calls.append(kwargs)
        raise LLMError("LLM returned invalid JSON.")

    def call_json(self, **kwargs):  # type: ignore[no-untyped-def]
        self.json_calls.append(kwargs)
        return {
            "article_html": (
                "<h1>Kleine Räume optimal nutzen: Worauf es bei der Planung ankommt</h1>"
                "<p>Kleine Räume profitieren von klaren Laufwegen, wandhohem Stauraum und gezielter Beleuchtung.</p>"
                "<h2>Welche Kriterien sind bei kleinen Räumen entscheidend?</h2>"
                "<p>Eine Laufbreite von rund 90 cm, helle Lichtquellen und Regale bis knapp unter die Decke helfen bei der Raumplanung.</p>"
                "<h2>Fazit</h2><p>Mit klaren Maßen und Stauraumplanung wirken kleine Räume ruhiger und funktionaler.</p>"
                "<h2>FAQ</h2><h3>Welche Möbel sparen Platz?</h3><p>Klapp- und Mehrzweckmöbel helfen besonders in kleinen Grundrissen.</p>"
            ),
            "meta_title": "",
            "meta_description": "",
            "slug": "",
            "excerpt": "",
        }


def _sample_master_plan():  # type: ignore[no-untyped-def]
    from creator.api.decision_schemas import MasterArticlePlan

    return MasterArticlePlan.model_validate(
        {
            "publishing_site": {
                "site_id": "site-2",
                "site_url": "https://publisher-two.example.com",
                "fit_reason": "Strong topical fit for homeowners and room-planning topics.",
                "inventory_rationale": "The site already has several home-planning articles that can support internal links.",
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
            "image_strategy": {
                "featured_prompt": "Editorial image of a compact room with layered lighting and space-saving storage.",
                "featured_alt": "Kleiner Raum mit guter Stauraumplanung",
                "include_in_content": False,
                "in_content_prompt": "",
                "in_content_alt": "",
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
            "forbidden_phrases": ["hier erfahren Sie alles"],
            "quality_requirements": ["Use concrete measures where relevant.", "Keep headings natural and useful."],
            "risk_notes": ["Avoid generic lifestyle phrasing."],
            "warnings": [],
        }
    )


def test_build_writer_system_prompt_mentions_draft_schema() -> None:
    prompt = build_writer_system_prompt()

    assert "draftarticlepayload" in prompt.lower()
    assert "article_html" in prompt


def test_build_writer_user_prompt_embeds_master_plan() -> None:
    context = WriterContext(
        target_site_url="https://www.eigenheim-blog.com/",
        publishing_site_url="https://publisher-two.example.com",
        master_plan=_sample_master_plan(),
        validation_feedback=["specificity_too_low:2"],
        content_brief="Practical homeowner advice with concrete layout examples.",
        internal_link_titles=["Stauraum im Flur richtig planen"],
    )

    prompt = build_writer_user_prompt(context)

    assert "specificity_too_low:2" in prompt
    assert "publisher-two.example.com" in prompt
    assert "Kleine Räume optimal nutzen" in prompt
    assert "forbidden_phrases" in prompt


def test_writer_calls_provider_with_draft_schema() -> None:
    provider = _StubWriterProvider()
    writer = CreatorWriter(provider=provider)
    context = WriterContext(
        target_site_url="https://www.eigenheim-blog.com/",
        publishing_site_url="https://publisher-two.example.com",
        master_plan=_sample_master_plan(),
        validation_feedback=[],
        content_brief="Practical homeowner advice with concrete layout examples.",
        internal_link_titles=["Stauraum im Flur richtig planen"],
    )

    result = writer.write_article(context, request_label="writer_test")

    assert result.slug == "kleine-raeume-optimal-nutzen"
    assert provider.calls[0]["request_label"] == "writer_test"
    assert provider.calls[0]["schema_model"] is DraftArticlePayload


def test_writer_retries_with_compact_json_prompt_after_invalid_json() -> None:
    provider = _RetryingWriterProvider()
    writer = CreatorWriter(provider=provider)
    context = WriterContext(
        target_site_url="https://www.eigenheim-blog.com/",
        publishing_site_url="https://publisher-two.example.com",
        master_plan=_sample_master_plan(),
        validation_feedback=[],
        content_brief="Practical homeowner advice with concrete layout examples.",
        internal_link_titles=["Stauraum im Flur richtig planen"],
    )

    result = writer.write_article(context, request_label="writer_retry_test")

    assert result.slug == "kleine-raeume-optimal-nutzen"
    assert result.meta_title == "Kleine Räume optimal nutzen: Planung, Licht und Stauraum"
    assert len(result.meta_description) >= 80
    assert len(result.excerpt) >= 40
    assert provider.schema_calls[0]["request_label"] == "writer_retry_test"
    assert provider.json_calls[0]["request_label"] == "writer_retry_test_retry"
    assert provider.json_calls[0]["allow_html_fallback"] is True
    assert "previous response was invalid or incomplete" in provider.json_calls[0]["user_prompt"]
