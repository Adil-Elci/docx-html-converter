from creator.api.decision_schemas import MasterArticlePlan
from creator.api.supervisor import (
    CreatorSupervisor,
    PublishingCandidateInput,
    SupervisorContext,
    build_supervisor_system_prompt,
    build_supervisor_user_prompt,
)
from creator.api.pipeline import _apply_master_article_plan_to_phase_state


class _StubProvider:
    def __init__(self) -> None:
        self.calls = []

    def call_json(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
        return {
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
            "image_strategy": {
                "featured_prompt": "Editorial image of a compact living room with layered lighting and custom storage.",
                "featured_alt": "Kleiner Wohnraum mit Stauraum und guter Lichtplanung",
                "include_in_content": False,
                "in_content_prompt": "",
                "in_content_alt": "",
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
            "forbidden_phrases": ["hier erfahren Sie alles"],
            "quality_requirements": ["Use concrete homeowner examples.", "Keep headings natural and non-promotional."],
            "risk_notes": ["Avoid generic lifestyle phrasing."],
            "warnings": ["Avoid repeating the recent room-planning angle too closely."],
        }


class _LooseSupervisorProvider:
    def __init__(self) -> None:
        self.calls = []

    def call_json(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
        return {
            "publishing_site": {
                "site_id": "site-2",
                "site_url": "https://publisher-two.example.com",
                "fit_reason": "Strong fit.",
                "inventory_rationale": "Good inventory support.",
                "confidence": 0.8,
            },
            "topic": "Hausbau planen: Welche Schritte zuerst wichtig sind",
            "intent_type": "informational",
            "article_angle": "Schritt-für-Schritt-Leitfaden für Eigentümer",
            "audience": "Bauherren",
            "tone": "practical_informational",
            "differentiator": "Focuses on sequence and practical decisions.",
            "title_package": {
                "h1": "Hausbau planen: Welche Schritte zuerst wichtig sind",
                "meta_title": "Hausbau planen: Die wichtigsten ersten Schritte",
                "slug": "hausbau-planen-erste-schritte",
            },
            "keyword_strategy": {
                "primary_keyword": "hausbau planen",
                "secondary_keywords": "hausbau schritte",
                "semantic_entities": "bauantrag",
                "keyword_intent_note": "Matches readers planning their first decisions.",
            },
            "backlink_plan": {
                "strategy": "Contextual editorial link in the first body section",
                "anchor_text": "mehr zum Hausbau",
                "placement_hint": "section_2",
                "rationale": "Natural support link.",
            },
            "image_strategy": {
                "featured_prompt": "Editorial image of early-stage house planning with documents and site plan.",
                "featured_alt": "Unterlagen für die Hausbauplanung",
                "include_in_content": False,
                "in_content_prompt": "",
                "in_content_alt": "",
            },
            "faq_questions": "Was kommt beim Hausbau zuerst?",
            "internal_link_titles": "Hausbaukosten richtig planen",
            "sections": [
                {
                    "section_id": "s1",
                    "kind": "body",
                    "h2": "Welche Schritte kommen zuerst?",
                    "goal": "Explain the first steps.",
                    "key_points": "Grundstück, Budget",
                    "required_terms": "budget",
                    "target_min_words": 100,
                    "target_max_words": 140,
                },
                {
                    "section_id": "s2",
                    "kind": "faq",
                    "h2": "FAQ",
                    "goal": "Answer follow-up questions.",
                    "key_points": [],
                    "required_terms": [],
                    "target_min_words": 90,
                    "target_max_words": 140,
                },
                {
                    "section_id": "s3",
                    "kind": "fazit",
                    "h2": "Fazit",
                    "goal": "Summarize the takeaway.",
                    "key_points": [],
                    "required_terms": [],
                    "target_min_words": 70,
                    "target_max_words": 100,
                },
            ],
            "forbidden_phrases": "hier erfahren Sie alles",
            "quality_requirements": "use practical steps",
            "risk_notes": "Do not overpromise timelines.",
            "warnings": "Local regulations may vary.",
        }


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
    assert "image_strategy" in prompt


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
    assert "concrete homeowner examples" in " ".join(result.quality_requirements).lower()
    assert provider.calls[0]["request_label"] == "test_supervisor"
    assert "publishing_candidates" in provider.calls[0]["user_prompt"]


def test_publishing_candidate_input_trims_internal_link_titles_to_limit() -> None:
    candidate = PublishingCandidateInput(
        site_url="https://publisher-three.example.com",
        internal_link_titles=[
            "Titel 1",
            "Titel 2",
            "Titel 3",
            "Titel 4",
            "Titel 5",
            "Titel 6",
        ],
    )

    assert len(candidate.internal_link_titles) == 5
    assert candidate.internal_link_titles[-1] == "Titel 5"


def test_apply_master_article_plan_normalizes_keyword_structure_and_backlink_defaults() -> None:
    phase3 = {
        "final_article_topic": "Hausbau vorbereiten: Was vor dem Baustart konkret zu klären ist",
        "search_intent_type": "commercial_investigation",
        "article_angle": "process_and_decision_factors",
        "primary_keyword": "welche methode passt wirklich worauf kommt es wirklich an",
        "secondary_keywords": ["hausbau eigenheim blog", "baukosten"],
        "structured_content_mode": "none",
        "topic_class": "home",
        "style_profile": {"tone": "practical_informational", "audience": "Bauherren"},
        "specificity_profile": {"buckets": {"planning": ["unterlagen", "kostenplan"]}, "min_specifics": 2},
        "title_package": {"h1": "", "meta_title": "", "slug": ""},
        "keyword_buckets": {"semantic_entities": ["hausbau", "unterlagen", "kostenplan"]},
        "content_brief": {"overlap_terms": ["rohbau", "hausanschlüsse"]},
        "topic_signature": {
            "topic_class": "home",
            "subject_phrase": "hausbau vorbereiten",
            "target_terms": ["hausbau", "unterlagen", "kostenplan"],
            "target_support_phrases": ["baukosten planen"],
            "semantic_entities": ["hausbau", "unterlagen", "kostenplan"],
        },
        "faq_candidates": [
            "Welche Unterlagen sind zuerst wichtig?",
            "Wie plant man die wichtigsten Kosten realistisch?",
            "Wann lohnt sich fachliche Unterstützung?",
        ],
    }
    master_plan = {
        "topic": "Hausbau vorbereiten: Was vor dem Baustart konkret zu klären ist",
        "intent_type": "commercial_investigation",
        "article_angle": "process_and_decision_factors",
        "audience": "Bauherren",
        "tone": "practical_informational",
        "title_package": {
            "h1": "Hausbau vorbereiten: Was vor dem Baustart konkret zu klären ist",
            "meta_title": "Hausbau vorbereiten: Schritte, Unterlagen und Kostenplan",
            "slug": "hausbau-vorbereiten-schritte",
        },
        "keyword_strategy": {
            "primary_keyword": "welche methode passt wirklich worauf kommt es wirklich an",
            "secondary_keywords": ["eigenheim blog", "baukosten hausbau"],
            "semantic_entities": ["hausbau", "unterlagen", "kostenplan"],
            "keyword_intent_note": "Practical query set for early planning.",
        },
        "backlink_plan": {
            "strategy": "supporting_context",
            "anchor_text": "Eigenheim-Blog",
            "placement_hint": "section_5",
            "rationale": "Contextual support link.",
        },
        "faq_questions": [
            "Welche Unterlagen sind zuerst wichtig?",
            "Wie plant man die wichtigsten Kosten realistisch?",
            "Wann lohnt sich fachliche Unterstützung?",
        ],
        "sections": [
            {
                "section_id": "section_1",
                "kind": "body",
                "h2": "Irgendeine freie Überschrift",
                "goal": "Erkläre die ersten Schritte.",
                "required_terms": ["unterlagen"],
                "target_min_words": 90,
                "target_max_words": 200,
            }
        ],
        "forbidden_phrases": [],
        "quality_requirements": [],
        "warnings": [],
    }

    phase4 = _apply_master_article_plan_to_phase_state(master_plan=master_plan, phase3=phase3)

    assert phase3["primary_keyword"] == "hausbau vorbereiten"
    assert all("eigenheim blog" not in keyword for keyword in phase3["secondary_keywords"])
    assert len(phase4["sections"]) == 4
    assert phase4["sections"][-2]["kind"] == "fazit"
    assert phase4["sections"][-1]["kind"] == "faq"
    assert phase4["backlink_placement"] == "section_2"


def test_supervisor_normalizes_loose_plan_fields() -> None:
    provider = _LooseSupervisorProvider()
    supervisor = CreatorSupervisor(provider=provider)

    result = supervisor.create_master_article_plan(_sample_context(), request_label="normalize_supervisor")

    assert result.article_angle == "process_and_next_steps"
    assert result.backlink_plan.strategy == "supporting_context"
    assert result.sections[0].section_id == "section_1"
    assert result.risk_notes == ["Do not overpromise timelines."]
    assert result.warnings == ["Local regulations may vary."]


def test_apply_master_article_plan_rewrites_noisy_body_headings() -> None:
    phase3 = {
        "final_article_topic": "Rohgrundstück kaufen: Welche Schritte vor dem Hausbau wirklich wichtig sind",
        "search_intent_type": "informational",
        "article_angle": "process_and_next_steps",
        "primary_keyword": "rohgrundstück kaufen",
        "secondary_keywords": ["hausbau grundstück", "erschließungskosten grundstück"],
        "structured_content_mode": "none",
        "topic_class": "real_estate",
        "topic_signature": {
            "subject_phrase": "rohgrundstück kaufen",
            "question_phrase": "welche schritte vor dem hausbau wirklich wichtig sind",
            "semantic_entities": ["grundstück", "hausbau", "erschließung"],
            "support_topics_for_internal_links": [],
        },
        "specificity_profile": {
            "topic": "Rohgrundstück kaufen",
            "topic_class": "real_estate",
            "intent_type": "informational",
            "min_specifics": 3,
            "buckets": {
                "documents_process": ["grundbuch", "notar", "erschließung"],
            },
        },
        "style_profile": {},
        "keyword_buckets": {},
    }
    master_plan = {
        "topic": phase3["final_article_topic"],
        "intent_type": phase3["search_intent_type"],
        "article_angle": phase3["article_angle"],
        "tone": "practical_informational",
        "audience": "Hauskäufer",
        "keyword_strategy": {
            "primary_keyword": phase3["primary_keyword"],
            "secondary_keywords": phase3["secondary_keywords"],
            "semantic_entities": ["grundstück", "hausbau"],
        },
        "title_package": {
            "h1": "Rohgrundstück kaufen: Welche Schritte",
            "meta_title": "Rohgrundstück kaufen: Welche Schritte",
            "slug": "rohgrundstueck-kaufen-schritte",
        },
        "backlink_plan": {
            "placement_hint": "section_2",
            "anchor_text": "mehr zum Hausbau",
        },
        "faq_questions": [
            "Welche Unterlagen braucht man zuerst?",
            "Wann entstehen Erschließungskosten?",
            "Wann lohnt sich fachliche Unterstützung?",
        ],
        "sections": [
            {
                "section_id": "section_1",
                "kind": "body",
                "h2": "Grundstück prüfen: Was vor dem Kauf eines Rohgrundstücks zu klären ist",
                "goal": "Explain the first checks.",
                "required_terms": ["grundstück"],
                "target_min_words": 100,
                "target_max_words": 140,
            },
            {
                "section_id": "section_2",
                "kind": "body",
                "h2": "Fertighaus oder Massivbau: Welche Bauweise passt zu Ihrem Grundstück",
                "goal": "Compare options.",
                "required_terms": ["hausbau"],
                "target_min_words": 100,
                "target_max_words": 140,
            },
            {
                "section_id": "section_3",
                "kind": "body",
                "h2": "Bauphasen koordinieren: Von der Grundsteinlegung bis zur Schlüsselübergabe",
                "goal": "Explain preparation.",
                "required_terms": ["erschließung"],
                "target_min_words": 100,
                "target_max_words": 140,
            },
            {
                "section_id": "section_4",
                "kind": "body",
                "h2": "Schritt für Schritt: So erstellen Sie Ihren persönlichen Kostenplan",
                "goal": "Explain cost planning.",
                "required_terms": ["kosten"],
                "target_min_words": 100,
                "target_max_words": 140,
            },
            {
                "section_id": "section_5",
                "kind": "fazit",
                "h2": "Zusammenfassung",
                "goal": "Summarize.",
                "required_terms": [],
                "target_min_words": 70,
                "target_max_words": 100,
            },
            {
                "section_id": "section_6",
                "kind": "faq",
                "h2": "Häufige Fragen",
                "goal": "Answer follow-up questions.",
                "required_terms": [],
                "target_min_words": 90,
                "target_max_words": 140,
            },
        ],
    }

    phase4 = _apply_master_article_plan_to_phase_state(master_plan=master_plan, phase3=phase3)

    body_h2s = [section["h2"] for section in phase4["sections"] if section["kind"] == "body"]
    assert body_h2s[0] != "Grundstück prüfen: Was vor dem Kauf eines Rohgrundstücks zu klären ist"
    assert body_h2s[1] != "Fertighaus oder Massivbau: Welche Bauweise passt zu Ihrem Grundstück"
    assert body_h2s[2] != "Bauphasen koordinieren: Von der Grundsteinlegung bis zur Schlüsselübergabe"
    assert body_h2s[3] != "Schritt für Schritt: So erstellen Sie Ihren persönlichen Kostenplan"
    assert "lässt" in body_h2s[3] or "häufig" in body_h2s[3] or "früh" in body_h2s[3] or "nächsten" in body_h2s[3]
    assert "haeufig" not in body_h2s[3]
    assert not phase4["h1"].endswith(": Welche Schritte")
    assert phase4["sections"][-2]["h2"] == "Fazit"
    assert phase4["sections"][-1]["h2"] == "FAQ"


def test_apply_master_article_plan_replaces_short_supervisor_titles() -> None:
    master_plan = {
        "topic": "Hausbau vorbereiten: Was vor dem Baustart konkret zu klären ist",
        "intent_type": "informational",
        "article_angle": "process_and_next_steps",
        "audience": "Bauherren",
        "tone": "practical_informational",
        "title_package": {
            "h1": "Zu kurz",
            "meta_title": "Kurz",
            "slug": "hausbau-vorbereiten",
        },
        "keyword_strategy": {
            "primary_keyword": "hausbau vorbereiten",
            "secondary_keywords": ["baustart unterlagen", "hausbau kosten planen"],
            "semantic_entities": ["unterlagen", "kostenplan"],
        },
        "sections": [
            {
                "section_id": "section_1",
                "kind": "body",
                "h2": "Welche Unterlagen sind wichtig?",
                "goal": "Explain the first steps.",
                "required_terms": ["unterlagen"],
                "target_min_words": 100,
                "target_max_words": 140,
            },
            {
                "section_id": "section_2",
                "kind": "fazit",
                "h2": "Fazit",
                "goal": "Summarize.",
                "required_terms": ["hausbau"],
                "target_min_words": 70,
                "target_max_words": 100,
            },
            {
                "section_id": "section_3",
                "kind": "faq",
                "h2": "FAQ",
                "goal": "Answer follow-up questions.",
                "required_terms": [],
                "target_min_words": 90,
                "target_max_words": 140,
            },
        ],
        "faq_questions": [
            "Welche Unterlagen sind zuerst wichtig?",
            "Wie plant man die wichtigsten Kosten realistisch?",
            "Wann lohnt sich fachliche Unterstützung?",
        ],
    }
    phase3 = {
        "final_article_topic": "Hausbau vorbereiten: Was vor dem Baustart konkret zu klären ist",
        "search_intent_type": "informational",
        "primary_keyword": "hausbau vorbereiten",
        "secondary_keywords": ["baustart unterlagen", "hausbau kosten planen"],
        "structured_content_mode": "none",
        "article_angle": "process_and_next_steps",
        "topic_class": "home",
        "title_package": {"h1": "", "meta_title": "", "slug": ""},
    }

    phase4 = _apply_master_article_plan_to_phase_state(master_plan=master_plan, phase3=phase3)

    assert len(phase4["h1"]) >= 45
    assert len(phase3["title_package"]["meta_title"]) >= 45
    assert phase4["h1"] != "Zu kurz"
    assert phase3["title_package"]["meta_title"] != "Kurz"


def test_apply_master_article_plan_rebuilds_signature_after_primary_cleanup() -> None:
    phase3 = {
        "final_article_topic": "Rasenpflege im Garten: Wie sich kahle Stellen, Hanglagen und Schattenrasen gezielt behandeln lassen",
        "search_intent_type": "informational",
        "article_angle": "decision_criteria",
        "primary_keyword": "welche methode passt wirklich worauf kommt es wirklich an",
        "secondary_keywords": ["rasen pflege", "schattenrasen tipps"],
        "structured_content_mode": "none",
        "topic_class": "home",
        "style_profile": {},
        "title_package": {"h1": "", "meta_title": "", "slug": ""},
        "keyword_buckets": {"semantic_entities": ["rasen", "hanglagen", "schattenrasen"]},
        "content_brief": {"overlap_terms": ["boden", "drainage"]},
        "pair_fit": {"overlap_terms": ["boden", "drainage"]},
        "topic_signature": {
            "subject_phrase": "kahle stellen hanglagen und schattenrasen",
            "target_terms": ["rasen", "boden", "drainage"],
        },
    }
    master_plan = {
        "topic": phase3["final_article_topic"],
        "intent_type": "informational",
        "article_angle": "decision_criteria",
        "audience": "Hausbesitzer",
        "tone": "practical_informational",
        "title_package": {
            "h1": "Kahle Stellen, Hanglagen und Schattenrasen",
            "meta_title": "Kahle Stellen, Hanglagen und Schattenrasen",
            "slug": "rasenpflege-garten",
        },
        "keyword_strategy": {
            "primary_keyword": "kahle stellen hanglagen schattenrasen worauf kommt es wirklich an",
            "secondary_keywords": ["rasen pflege", "schattenrasen tipps"],
            "semantic_entities": ["rasen", "hanglagen", "schattenrasen"],
        },
        "backlink_plan": {"placement_hint": "section_2", "anchor_text": "mehr zur Rasenpflege"},
        "faq_questions": [
            "Woran erkennt man Bodenprobleme früh?",
            "Wann hilft Nachsaat statt kompletter Neuanlage?",
            "Wie lässt sich Schattenrasen sinnvoll pflegen?",
        ],
        "sections": [
            {
                "section_id": "section_1",
                "kind": "body",
                "h2": "Kahle Stellen, Hanglagen und Schattenrasen: Worauf kommt es wirklich an?",
                "goal": "Erkläre die wichtigsten Kriterien.",
                "required_terms": ["boden", "drainage"],
                "target_min_words": 100,
                "target_max_words": 140,
            }
        ],
    }

    phase4 = _apply_master_article_plan_to_phase_state(master_plan=master_plan, phase3=phase3)

    assert phase3["primary_keyword"] == "rasenpflege im garten"
    assert phase3["topic_signature"]["subject_phrase"] == "rasenpflege im garten"
    first_h2 = next(section["h2"] for section in phase4["sections"] if section["kind"] == "body")
    assert "kahle stellen hanglagen" not in first_h2.lower()
