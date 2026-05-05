"""Phase B — language parameterization tests.

Covers:
- prompt_registry resolution (.de.md / .fr.md / fallback to .md)
- ContentContract.language field
- contract_generator user_prompt switches per-language
- eval_harness language-aware checks (readability + language_consistency)
- pipeline_runner language plumbing (DataForSEO locale)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from creator.api import prompt_registry
from creator.api.contract import ArticleLanguage, ContentContract, SearchIntent, SectionPlan
from creator.api.contract_generator import (
    _user_prompt_template,
    build_user_prompt,
    generate_contract,
)
from creator.api.eval_harness import (
    FRENCH_GRADE_MAX,
    FRENCH_GRADE_MIN,
    LANGUAGE_CONSISTENCY_FOREIGN_TOKEN_MAX_RATIO,
    WIENER_GRADE_MAX,
    WIENER_GRADE_MIN,
    check_french_readability,
    check_german_readability,
    check_language_consistency,
    evaluate,
)
from creator.api.research import ResearchPayload


# ---- prompt_registry --------------------------------------------------------


class TestPromptRegistry:
    def test_loads_german_variant_when_language_de(self):
        prompt = prompt_registry.load("contract_generator", language="de")
        assert prompt.language == "de"
        assert "deutscher SEO-Content-Architekt" in prompt.body

    def test_loads_french_variant_when_language_fr(self):
        prompt = prompt_registry.load("contract_generator", language="fr")
        assert prompt.language == "fr"
        assert "architecte SEO francophone" in prompt.body

    def test_falls_back_to_german_when_no_language_specified(self):
        prompt = prompt_registry.load("contract_generator")
        assert prompt.language == "de"

    def test_unknown_language_falls_back_to_german_default(self):
        # Italian variant doesn't exist; should not crash, should fall back.
        prompt = prompt_registry.load("contract_generator", language="it")
        assert prompt.language == "de"

    def test_section_writer_prompts_exist_in_both_languages(self):
        de = prompt_registry.load("section_writer", language="de")
        fr = prompt_registry.load("section_writer", language="fr")
        assert de.language == "de"
        assert fr.language == "fr"
        assert "Sie (deutsche Geschäftskommunikation)" in de.body or "Sie" in de.body
        assert "vouvoiement" in fr.body

    def test_voice_pass_prompts_exist_in_both_languages(self):
        de = prompt_registry.load("voice_pass", language="de")
        fr = prompt_registry.load("voice_pass", language="fr")
        assert "Lektor" in de.body
        assert "correcteur" in fr.body

    def test_eval_judge_prompts_exist_in_both_languages(self):
        de = prompt_registry.load("eval_judge", language="de")
        fr = prompt_registry.load("eval_judge", language="fr")
        assert "Reviewer" in de.body
        assert "relecteur" in fr.body


# ---- ContentContract.language ----------------------------------------------


class TestContentContractLanguage:
    def test_default_is_german(self):
        contract = ContentContract(
            target_keyword="kw",
            intent=SearchIntent.INFORMATIONAL,
            target_audience="audience",
            word_count_target=900,
            h1="A test heading H1",
            meta_title="A test heading meta title okok",
            meta_description="A description that is long enough to satisfy the contract validator easily here.",
            slug="a-test",
            sections=[
                SectionPlan(h2="Eins zwei", mandate="Erste Sektion mandate.", target_word_count=300),
                SectionPlan(h2="Drei vier", mandate="Zweite Sektion mandate.", target_word_count=300),
            ],
        )
        assert contract.language == ArticleLanguage.DE

    def test_french_value_accepted(self):
        contract = ContentContract(
            target_keyword="kw",
            intent=SearchIntent.INFORMATIONAL,
            language=ArticleLanguage.FR,
            target_audience="audience",
            word_count_target=900,
            h1="Un titre H1 valable",
            meta_title="Un titre meta de longueur suffisante",
            meta_description="Une description suffisamment longue pour satisfaire le validateur de contrat facilement.",
            slug="un-test",
            sections=[
                SectionPlan(h2="Section A", mandate="Mandat de la section A.", target_word_count=300),
                SectionPlan(h2="Section B", mandate="Mandat de la section B.", target_word_count=300),
            ],
        )
        assert contract.language == ArticleLanguage.FR


# ---- contract_generator language routing -----------------------------------


class TestContractGeneratorLanguage:
    def test_user_prompt_template_de(self):
        t = _user_prompt_template("de")
        assert t["target_keyword_label"] == "ZIEL-KEYWORD"
        assert "deutscher Sprache" in t["language_directive"]

    def test_user_prompt_template_fr(self):
        t = _user_prompt_template("fr")
        assert t["target_keyword_label"] == "MOT-CLÉ CIBLE"
        assert "français" in t["language_directive"]

    def test_user_prompt_template_falls_back_to_de(self):
        t = _user_prompt_template("xx")
        assert t["target_keyword_label"] == "ZIEL-KEYWORD"

    def test_build_user_prompt_uses_french_labels(self):
        payload = ResearchPayload(
            target_keyword="expert-comptable paris",
            location_code=2250,
            language_code="fr",
        )
        prompt = build_user_prompt(payload, target_backlink_url="https://x.fr/y", language="fr")
        assert "MOT-CLÉ CIBLE" in prompt
        assert "URL CIBLE" in prompt
        assert "ZIEL-KEYWORD" not in prompt
        assert "français" in prompt

    def test_build_user_prompt_uses_german_labels_by_default(self):
        payload = ResearchPayload(
            target_keyword="steuerberater hamburg",
            location_code=2276,
            language_code="de",
        )
        prompt = build_user_prompt(payload, target_backlink_url="https://x.de/y")
        assert "ZIEL-KEYWORD" in prompt
        assert "MOT-CLÉ CIBLE" not in prompt

    def test_build_user_prompt_includes_current_year_de(self):
        payload = ResearchPayload(
            target_keyword="steuerberater hamburg",
            location_code=2276,
            language_code="de",
        )
        prompt = build_user_prompt(
            payload, target_backlink_url="https://x.de/y", current_year=2026
        )
        assert "AKTUELLES JAHR: 2026" in prompt

    def test_build_user_prompt_includes_current_year_fr(self):
        payload = ResearchPayload(
            target_keyword="expert-comptable paris",
            location_code=2250,
            language_code="fr",
        )
        prompt = build_user_prompt(
            payload,
            target_backlink_url="https://x.fr/y",
            language="fr",
            current_year=2026,
        )
        # NB: matches the existing FR-template convention (no thin space before
        # colon for the static label/value lines; the headers do use it).
        assert "ANNÉE EN COURS: 2026" in prompt

    def test_build_user_prompt_defaults_year_to_today(self):
        from datetime import datetime, timezone

        payload = ResearchPayload(
            target_keyword="kw",
            location_code=2276,
            language_code="de",
        )
        prompt = build_user_prompt(payload, target_backlink_url="https://x.de/y")
        # No current_year passed -> uses today's UTC year
        assert f"AKTUELLES JAHR: {datetime.now(timezone.utc).year}" in prompt

    def test_generate_contract_routes_french_prompt(self):
        captured: dict = {}

        def fake_caller(**kwargs):
            captured.update(kwargs)
            # Return a minimum-valid contract JSON. ``language`` is overwritten by
            # generate_contract regardless of what the LLM returns, so we leave
            # it out to verify the override path.
            return (
                '{"target_keyword":"expert-comptable paris","intent":"commercial",'
                '"target_audience":"PME francophones","word_count_target":900,'
                '"h1":"Expert-comptable Paris : guide pratique",'
                '"meta_title":"Expert-comptable Paris : guide pratique long",'
                '"meta_description":"Trouvez le bon expert-comptable a Paris en suivant ce guide pratique pour PME et independants en France.",'
                '"slug":"expert-comptable-paris",'
                '"sections":[{"h2":"Choisir","mandate":"Comment choisir.","target_word_count":300},'
                '{"h2":"Comparer","mandate":"Comparer les offres.","target_word_count":300}],'
                '"ai_tell_blocklist":["De plus","En conclusion"]}'
            )

        payload = ResearchPayload(
            target_keyword="expert-comptable paris",
            location_code=2250,
            language_code="fr",
        )
        contract = generate_contract(
            payload,
            target_backlink_url="https://client.fr/x",
            language="fr",
            llm_caller=fake_caller,
        )
        # The system prompt the caller saw must be the French one.
        assert "architecte SEO francophone" in captured["system_prompt"]
        # The contract.language field is forced to match the requested language
        # even if the LLM omitted it.
        assert contract.language == ArticleLanguage.FR


# ---- eval_harness language gating -----------------------------------------


class TestEvalReadabilityGating:
    def test_french_readability_in_band_for_simple_french(self):
        text = (
            "L'expert-comptable accompagne les entreprises. Il prepare la comptabilite annuelle "
            "et conseille sur la fiscalite. Les PME apprecient ce soutien quotidien. "
            "Un cabinet local connait les regles francaises et propose des tarifs clairs. "
            "Choisir un cabinet pres de chez vous facilite les rendez-vous reguliers."
        ) * 2
        result = check_french_readability(text)
        assert result.name == "french_readability_kandel_moles"
        assert result.value is not None
        # Don't pin an exact number; just sanity-check the bounds.
        assert FRENCH_GRADE_MIN - 2 <= result.value <= FRENCH_GRADE_MAX + 5

    def test_german_readability_runs_for_de_text(self):
        text = (
            "Steuerberater helfen kleinen Unternehmen bei der Buchhaltung und Steuererklaerung. "
            "Die Beratung erfolgt persoenlich oder digital. Ein guter Berater kennt die "
            "regionalen Besonderheiten in Hamburg und arbeitet eng mit den Mandanten zusammen."
        ) * 3
        result = check_german_readability(text)
        assert result.name == "german_readability_wiener"
        assert result.value is not None

    def test_evaluate_picks_french_readability_for_fr_contract(self):
        contract = _make_contract(language=ArticleLanguage.FR, target_keyword="expert-comptable paris")
        article_html = (
            "<h1>Expert-comptable paris</h1>"
            "<h2>Choisir</h2><p>" + ("Le cabinet local accompagne les entreprises locales. " * 30) + "</p>"
            "<h2>Comparer</h2><p>" + ("Les tarifs varient selon la taille de la societe. " * 30) + "</p>"
        )
        report = evaluate(
            article_html=article_html,
            contract=contract,
            host_domain="client.fr",
            meta_title=contract.meta_title,
            meta_description=contract.meta_description,
        )
        names = [r.name for r in report.deterministic]
        assert "french_readability_kandel_moles" in names
        assert "german_readability_wiener" not in names

    def test_evaluate_picks_german_readability_for_de_contract(self):
        contract = _make_contract(language=ArticleLanguage.DE, target_keyword="steuerberater hamburg")
        article_html = (
            "<h1>Steuerberater hamburg</h1>"
            "<h2>Erstes</h2><p>" + ("Der Steuerberater unterstuetzt Hamburger Unternehmen. " * 30) + "</p>"
            "<h2>Zweites</h2><p>" + ("Die Beratung umfasst Buchhaltung und Steuererklaerung. " * 30) + "</p>"
        )
        report = evaluate(
            article_html=article_html,
            contract=contract,
            host_domain="client.de",
            meta_title=contract.meta_title,
            meta_description=contract.meta_description,
        )
        names = [r.name for r in report.deterministic]
        assert "german_readability_wiener" in names
        assert "french_readability_kandel_moles" not in names


class TestLanguageConsistencyCheck:
    def test_clean_german_passes(self):
        text = (
            "Der Steuerberater hilft bei der Buchhaltung und der Steuererklaerung. "
            "Die Beratung ist persoenlich. Ein guter Berater kennt die regionalen Besonderheiten."
        )
        result = check_language_consistency(text, language="de")
        assert result.passed is True
        assert result.value == 0.0 or result.value <= LANGUAGE_CONSISTENCY_FOREIGN_TOKEN_MAX_RATIO

    def test_clean_french_passes(self):
        text = (
            "L'expert-comptable accompagne les entreprises. Il prepare la comptabilite. "
            "Les PME apprecient ce soutien."
        )
        result = check_language_consistency(text, language="fr")
        assert result.passed is True

    def test_french_text_in_german_contract_fails(self):
        # An article that's mostly French stop-words served as a German article
        # should fail the consistency check.
        text = (
            "Le cabinet est dans la ville et les entreprises sont satisfaites. "
            "Pour les pme qui veulent un expert avec qui travailler. "
            "Nous proposons des services pour les societes qui sont en France."
        ) * 5
        result = check_language_consistency(text, language="de")
        assert result.passed is False
        assert result.value is not None and result.value > LANGUAGE_CONSISTENCY_FOREIGN_TOKEN_MAX_RATIO

    def test_german_text_in_french_contract_fails(self):
        text = (
            "Der Steuerberater ist in der Stadt und die Unternehmen sind zufrieden. "
            "Fuer die kmu die einen experten mit dem sie arbeiten wollen. "
            "Wir bieten dienstleistungen fuer die unternehmen die in Deutschland sind."
        ) * 5
        result = check_language_consistency(text, language="fr")
        assert result.passed is False

    def test_empty_article_passes(self):
        result = check_language_consistency("", language="de")
        assert result.passed is True

    def test_unsupported_language_skipped(self):
        result = check_language_consistency("Hello world", language="en")
        assert result.passed is True
        assert "skipped" in result.detail


# ---- pipeline_runner locale plumbing --------------------------------------


class TestPipelineRunnerLocale:
    def test_unsupported_language_raises_pipeline_error(self):
        from creator.api.pipeline_runner import PipelineError, run_pipeline

        with pytest.raises(PipelineError) as exc:
            run_pipeline(
                target_keyword="kw",
                target_backlink_url="https://client.de/x",
                publishing_site_url="https://host.de",
                language="es",
            )
        assert exc.value.phase == "language"
        assert "es" in str(exc.value)

    def test_french_routes_to_paris_locale(self):
        # Patch run_research to capture the locale it was called with.
        captured: dict = {}

        def fake_research(**kwargs):
            captured.update(kwargs)
            raise RuntimeError("stop after capture")

        from creator.api.pipeline_runner import PipelineError, run_pipeline

        with patch("creator.api.pipeline_runner.run_research", side_effect=fake_research):
            with pytest.raises(PipelineError) as exc:
                run_pipeline(
                    target_keyword="expert-comptable paris",
                    target_backlink_url="https://client.fr/x",
                    publishing_site_url="https://host.fr",
                    language="fr",
                )
        assert exc.value.phase == "research"
        assert captured["location_code"] == 2250
        assert captured["language_code"] == "fr"

    def test_german_routes_to_germany_locale(self):
        captured: dict = {}

        def fake_research(**kwargs):
            captured.update(kwargs)
            raise RuntimeError("stop after capture")

        from creator.api.pipeline_runner import PipelineError, run_pipeline

        with patch("creator.api.pipeline_runner.run_research", side_effect=fake_research):
            with pytest.raises(PipelineError):
                run_pipeline(
                    target_keyword="steuerberater hamburg",
                    target_backlink_url="https://client.de/x",
                    publishing_site_url="https://host.de",
                    language="de",
                )
        assert captured["location_code"] == 2276
        assert captured["language_code"] == "de"


# ---- helpers --------------------------------------------------------------


def _make_contract(*, language: ArticleLanguage, target_keyword: str) -> ContentContract:
    return ContentContract(
        target_keyword=target_keyword,
        intent=SearchIntent.COMMERCIAL,
        language=language,
        target_audience="Public cible test okay",
        word_count_target=900,
        h1=f"{target_keyword.title()} Heading",
        meta_title=f"{target_keyword} - meta title de longueur suffisante",
        meta_description=f"Meta description de l'article {target_keyword} suffisamment longue pour passer la validation Pydantic.",
        slug=target_keyword.replace(" ", "-").replace("'", ""),
        sections=[
            SectionPlan(h2="Section un", mandate="Mandat un.", target_word_count=300),
            SectionPlan(h2="Section deux", mandate="Mandat deux.", target_word_count=300),
        ],
        ai_tell_blocklist=["Darüber hinaus", "De plus"],
    )
