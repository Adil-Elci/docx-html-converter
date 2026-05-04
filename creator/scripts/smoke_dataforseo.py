"""Smoke-test the DataForSEO client against the live API.

Spends ~$0.001 per run. Validates that DATAFORSEO_LOGIN / DATAFORSEO_PASSWORD
are set correctly and that response parsing matches the live response shape.

Usage:
    python -m creator.scripts.smoke_dataforseo "steuerberater hamburg"
    python -m creator.scripts.smoke_dataforseo  # defaults to "steuerberater hamburg"
"""

from __future__ import annotations

import sys

from dotenv import load_dotenv

from creator.api.dataforseo import DataForSEOClient, DataForSEOError


def main(argv: list[str]) -> int:
    load_dotenv()
    keyword = argv[1] if len(argv) > 1 else "steuerberater hamburg"
    print(f"DataForSEO smoke test — keyword: {keyword!r}")
    print("-" * 60)

    try:
        client = DataForSEOClient()
    except DataForSEOError as exc:
        print(f"FAILED to load config: {exc}")
        return 1

    total_cost = 0.0

    try:
        serp = client.serp_organic(keyword)
    except DataForSEOError as exc:
        print(f"SERP request failed: {exc}")
        return 1

    total_cost += serp.cost
    print(f"SERP organic results: {len(serp.organic)} (cost ${serp.cost:.6f})")
    for result in serp.organic[:3]:
        print(f"  #{result.rank} {result.domain}")
        print(f"    {result.title}")
        print(f"    {result.url}")
    print()
    print(f"People-Also-Ask ({len(serp.people_also_ask)}):")
    for q in serp.people_also_ask[:5]:
        print(f"  - {q}")
    print()
    print(f"Related searches ({len(serp.related_searches)}):")
    for q in serp.related_searches[:5]:
        print(f"  - {q}")
    print()

    print("-" * 60)
    print(f"TOTAL COST: ${total_cost:.6f}")
    print("(Run keyword_volume / related_keywords manually if you want to spend extra.)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
