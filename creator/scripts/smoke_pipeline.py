"""End-to-end smoke test: research -> contract live, on real APIs.

Spends ~$0.35-0.40 per run (DataForSEO ~$0.03, Haiku entity extraction ~$0.02,
Opus 4.7 contract generation with extended thinking ~$0.30, Haiku judge ~$0.005).

Asks for explicit confirmation before the Opus call to avoid accidental spend.

Usage:
    python -m creator.scripts.smoke_pipeline "steuerberater hamburg" https://client.de/leistungen
    python -m creator.scripts.smoke_pipeline "steuerberater hamburg" https://client.de/leistungen --skip-related-keywords
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List

from dotenv import load_dotenv

from creator.api.contract_generator import generate_contract
from creator.api.dataforseo import DataForSEOError
from creator.api.llm import LLMError
from creator.api.research import run_research


def _pretty_research_summary(payload) -> str:
    return (
        f"  organic results : {len(payload.organic)}\n"
        f"  PAA questions   : {len(payload.paa_questions)}\n"
        f"  related searches: {len(payload.related_searches)}\n"
        f"  competitors OK  : {payload.successful_competitor_count}/{len(payload.competitors)}\n"
        f"  word_count med  : {payload.competitor_word_count_median}\n"
        f"  common H2 themes: {len(payload.common_h2_themes)}\n"
        f"  entities found  : {len(payload.entities)}\n"
        f"  high-coverage   : {len(payload.high_coverage_entities)}\n"
        f"  research cost   : ${payload.total_cost_usd:.4f}"
    )


def _confirm(prompt: str) -> bool:
    try:
        answer = input(f"{prompt} [y/N] ").strip().lower()
    except EOFError:
        return False
    return answer in ("y", "yes")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(prog="smoke_pipeline")
    parser.add_argument("keyword", help="Target keyword (German)")
    parser.add_argument("target_url", help="Backlink target URL")
    parser.add_argument("--anchor-hint", default=None, help="Anchor strategy hint (optional)")
    parser.add_argument("--skip-related-keywords", action="store_true", help="Save the related-keywords API call")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompts")
    args = parser.parse_args(argv[1:])

    load_dotenv()

    print(f"Smoke pipeline: keyword={args.keyword!r}  target_url={args.target_url}")
    print("-" * 70)
    print("Step 1/2: running research (DataForSEO + scrape + Haiku entities)...")

    try:
        research = run_research(
            target_keyword=args.keyword,
            skip_related_keywords=args.skip_related_keywords,
        )
    except DataForSEOError as exc:
        print(f"FAILED at research: {exc}")
        return 1

    print(_pretty_research_summary(research))
    print()

    if not args.yes:
        if not _confirm("Proceed to Opus 4.7 contract generation (~$0.30)?"):
            print("Aborted before Opus call.")
            return 0

    print("Step 2/2: generating ContentContract with Opus 4.7 + extended thinking...")
    try:
        contract = generate_contract(
            research,
            target_backlink_url=args.target_url,
            anchor_hint=args.anchor_hint,
        )
    except LLMError as exc:
        print(f"FAILED at contract generation: {exc}")
        return 1

    print()
    print("=" * 70)
    print("CONTRACT GENERATED")
    print("=" * 70)
    print(f"  intent          : {contract.intent.value}")
    print(f"  word_count_target: {contract.word_count_target}")
    print(f"  H1              : {contract.h1}")
    print(f"  meta_title      : {contract.meta_title}  ({len(contract.meta_title)} chars)")
    print(f"  meta_description: {contract.meta_description}  ({len(contract.meta_description)} chars)")
    print(f"  slug            : {contract.slug}")
    print(f"  sections        : {len(contract.sections)}")
    for i, section in enumerate(contract.sections):
        print(f"    [{i}] {section.h2}  ({section.target_word_count}w)")
    print(f"  FAQ items       : {len(contract.faq_items)}")
    print(f"  required_entities: {len(contract.required_entities)}")
    print(f"  link_plan       : {len(contract.link_plan)}")
    for link in contract.link_plan:
        print(f"    -> {link.target_url}  [{link.anchor_strategy}, section {link.section_index}, {link.link_type}]")
    print(f"  ai_tell_blocklist: {len(contract.ai_tell_blocklist)} phrases")
    print()

    if not args.yes and _confirm("Print full contract JSON?"):
        print(json.dumps(contract.model_dump(mode="json"), ensure_ascii=False, indent=2))

    print(f"\nResearch spend: ${research.total_cost_usd:.4f} (Opus + Haiku spend tracked separately in API console)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
