"""Full end-to-end smoke test against live APIs.

Runs research -> contract -> sections -> assemble -> voice -> judge -> eval
for a single keyword + target URL. Saves every artifact to a timestamped
directory under ``creator/smoke_outputs/`` so you can inspect each phase.

Total spend per run, approximately:
    DataForSEO research:                 ~$0.05
    Haiku entity extraction:             ~$0.02
    Sonnet 4.6 contract w/ thinking:     ~$0.06
    4-6 Sonnet 4.6 section calls:        ~$0.10  (with prompt caching)
    Sonnet 4.6 voice pass:               ~$0.04
    Haiku 4.5 eval judge:                ~$0.005
    -------------------------------------------
    TOTAL:                               ~$0.30

Usage:
    python -m creator.scripts.smoke_full_pipeline "steuerberater hamburg" \\
        https://client.de/leistungen \\
        --publishing-site https://example.de
    python -m creator.scripts.smoke_full_pipeline "..." "..." --skip-judge
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List

from dotenv import load_dotenv

from creator.api.pipeline_runner import PipelineError, PipelineRun, run_pipeline


SMOKE_OUTPUT_ROOT = Path(__file__).resolve().parents[1] / "smoke_outputs"


def _confirm(prompt: str) -> bool:
    try:
        answer = input(f"{prompt} [y/N] ").strip().lower()
    except EOFError:
        return False
    return answer in ("y", "yes")


def _slugify(text: str) -> str:
    return "".join(c if c.isalnum() else "-" for c in text.lower()).strip("-")[:50]


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))


def _write_text(path: Path, payload: str) -> None:
    path.write_text(payload, encoding="utf-8")


def _print_quality_summary(run: PipelineRun) -> None:
    report = run.quality_report
    det_failed = [r for r in report.deterministic if not r.passed]
    judge_failed = [r for r in report.llm_judged if not r.passed]
    print(f"  passed             : {report.passed}")
    print(f"  deterministic ok   : {len(report.deterministic) - len(det_failed)}/{len(report.deterministic)}")
    print(f"  llm-judged ok      : {len(report.llm_judged) - len(judge_failed)}/{len(report.llm_judged)}")
    if det_failed:
        print("  deterministic FAIL :")
        for r in det_failed:
            print(f"    - {r.name}: {r.detail or r.value}")
    if judge_failed:
        print("  llm-judged FAIL    :")
        for r in judge_failed:
            print(f"    - {r.name}: {r.detail or r.value}")


def _save_run(run: PipelineRun, out_dir: Path) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []

    p = out_dir / "research.json"
    _write_json(p, run.research)
    paths.append(p)

    p = out_dir / "contract.json"
    _write_json(p, run.contract)
    paths.append(p)

    p = out_dir / "sections.json"
    _write_json(p, [s for s in run.sections])
    paths.append(p)

    p = out_dir / "article_assembled.html"
    _write_text(p, run.assembled.full_html)
    paths.append(p)

    p = out_dir / "article_refined.html"
    _write_text(p, run.refined_article_html)
    paths.append(p)

    p = out_dir / "article_final.html"
    _write_text(p, run.final_html)
    paths.append(p)

    if run.judge_scores is not None:
        p = out_dir / "judge_scores.json"
        _write_json(p, run.judge_scores)
        paths.append(p)

    p = out_dir / "quality_report.json"
    _write_json(p, run.quality_report.to_dict())
    paths.append(p)

    summary = {
        "target_keyword": run.target_keyword,
        "target_backlink_url": run.target_backlink_url,
        "publishing_site_host": run.publishing_site_host,
        "skipped_voice_pass": run.skipped_voice_pass,
        "skipped_judge": run.skipped_judge,
        "notes": run.notes,
        "research_cost_usd": run.research.total_cost_usd,
        "section_count": len(run.sections),
        "quality_passed": run.quality_report.passed,
    }
    p = out_dir / "summary.json"
    _write_json(p, summary)
    paths.append(p)

    return paths


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(prog="smoke_full_pipeline")
    parser.add_argument("keyword")
    parser.add_argument("target_backlink_url")
    parser.add_argument("--publishing-site", default="https://example.de", help="Host used for internal-link bookkeeping (URL or domain).")
    parser.add_argument("--anchor-hint", default=None)
    parser.add_argument("--canonical-url", default=None, help="Optional canonical URL for Article schema mainEntityOfPage.")
    parser.add_argument("--skip-voice-pass", action="store_true")
    parser.add_argument("--skip-judge", action="store_true")
    parser.add_argument("--skip-related-keywords", action="store_true")
    parser.add_argument("--skip-entity-extraction", action="store_true")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompts.")
    args = parser.parse_args(argv[1:])

    load_dotenv()

    print(f"Full pipeline smoke")
    print(f"  keyword           : {args.keyword!r}")
    print(f"  target_backlink   : {args.target_backlink_url}")
    print(f"  publishing_site   : {args.publishing_site}")
    if args.anchor_hint:
        print(f"  anchor_hint       : {args.anchor_hint}")
    print(f"  voice_pass        : {'skipped' if args.skip_voice_pass else 'on'}")
    print(f"  judge             : {'skipped' if args.skip_judge else 'on'}")
    print("-" * 70)

    if not args.yes:
        if not _confirm("Proceed with full pipeline (~$0.30 total spend)?"):
            print("Aborted before any spend.")
            return 0

    try:
        run = run_pipeline(
            target_keyword=args.keyword,
            target_backlink_url=args.target_backlink_url,
            publishing_site_url=args.publishing_site,
            anchor_hint=args.anchor_hint,
            canonical_url=args.canonical_url,
            skip_voice_pass=args.skip_voice_pass,
            skip_judge=args.skip_judge,
            skip_related_keywords=args.skip_related_keywords,
            skip_entity_extraction=args.skip_entity_extraction,
        )
    except PipelineError as exc:
        print(f"PIPELINE FAILED: {exc}")
        return 1

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = SMOKE_OUTPUT_ROOT / f"{timestamp}__{_slugify(args.keyword)}"
    paths = _save_run(run, out_dir)

    print()
    print("=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)
    print(f"Saved to: {out_dir}")
    for p in paths:
        print(f"  {p.name}")
    print()
    print("Quality report summary:")
    _print_quality_summary(run)
    print()
    print(f"Research spend (DataForSEO): ${run.research.total_cost_usd:.4f}")
    print("Anthropic spend tracked separately in console.anthropic.com.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
