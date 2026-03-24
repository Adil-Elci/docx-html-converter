from creator.api.decision_schemas import CriticReview, MasterArticlePlan, get_schema_dict, get_schema_text


def test_master_article_plan_schema_exports_sections_and_keywords() -> None:
    schema = get_schema_dict("master_article_plan")
    definitions = schema.get("$defs") or {}

    assert "KeywordStrategy" in definitions
    assert "SectionPlan" in definitions
    assert schema["properties"]["sections"]["minItems"] == 3


def test_critic_review_schema_text_mentions_verdict() -> None:
    schema_text = get_schema_text("critic_review")

    assert "verdict" in schema_text
    assert "repair_needed" in schema_text


def test_schema_models_validate_expected_payloads() -> None:
    plan = MasterArticlePlan.model_validate(
        {
            "publishing_site": {
                "site_id": "site-1",
                "site_url": "https://publisher.example.com",
                "fit_reason": "Strong topical fit for homeowners and room-planning topics.",
                "inventory_rationale": "The site already has several home-planning articles that can support internal links.",
                "confidence": 0.84,
            },
            "topic": "Kleine Räume optimal nutzen: Tipps für clevere Wohnraumplanung",
            "intent_type": "commercial_investigation",
            "article_angle": "decision_criteria",
            "audience": "Hausbesitzerinnen und Hausbesitzer",
            "tone": "practical_informational",
            "differentiator": "Combines space-planning criteria with concrete layout constraints and storage decisions.",
            "title_package": {
                "h1": "Kleine Räume optimal nutzen: Worauf es bei der Planung ankommt",
                "meta_title": "Kleine Räume optimal nutzen: Planung, Stauraum und Licht",
                "slug": "kleine-raeume-optimal-nutzen",
            },
            "keyword_strategy": {
                "primary_keyword": "kleine räume optimal nutzen",
                "secondary_keywords": ["wohnraumplanung stauraum", "kleine räume beleuchtung"],
                "semantic_entities": ["grundriss", "laufbreite", "regal"],
                "keyword_intent_note": "The keyword set reflects planning and evaluation intent instead of generic inspiration content.",
            },
            "backlink_plan": {
                "strategy": "supporting_context",
                "anchor_text": "mehr zur Raumplanung",
                "placement_hint": "section_2",
                "rationale": "The backlink supports a comparison point without turning the section promotional.",
            },
            "faq_questions": [
                "Welche Möbel funktionieren in kleinen Räumen am besten?",
                "Wie viel Laufbreite sollte zwischen Möbeln bleiben?",
                "Welche Beleuchtung hilft kleinen Räumen?",
            ],
            "internal_link_titles": ["Stauraum im Flur richtig planen"],
            "sections": [
                {
                    "section_id": "section_1",
                    "kind": "body",
                    "h2": "Welche Kriterien sind bei kleinen Räumen entscheidend?",
                    "goal": "Explain the main space-planning criteria with concrete examples.",
                    "key_points": ["Laufbreite", "Stauraum", "Licht"],
                    "required_terms": ["grundriss", "stauraum"],
                    "target_min_words": 100,
                    "target_max_words": 140,
                },
                {
                    "section_id": "section_2",
                    "kind": "body",
                    "h2": "Welche Möbel sparen wirklich Platz?",
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
    review = CriticReview.model_validate(
        {
            "verdict": "repair_needed",
            "overall_score": 74,
            "plan_alignment_score": 82,
            "editorial_quality_score": 70,
            "seo_quality_score": 69,
            "strengths": ["Concrete space-planning examples."],
            "issues": [
                {
                    "code": "heading_phrase_invalid",
                    "severity": "medium",
                    "summary": "The first H2 still sounds too scaffolded.",
                    "location_hint": "section_1",
                    "recommended_fix": "Rewrite the H2 as a natural homeowner question.",
                }
            ],
            "repair_instructions": ["Rewrite the first H2 and tighten the meta title."],
            "final_recommendation": "Repair before approval.",
        }
    )

    assert plan.keyword_strategy.primary_keyword == "kleine räume optimal nutzen"
    assert review.verdict == "repair_needed"
