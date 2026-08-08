#!/usr/bin/env python3
"""Score the TOC solver on whole issues against hand-read boundary gold.

The operational question is not "how often is a page classified correctly" --
pages are ~97% continuation, so that number is high and meaningless. It is "how
many emitted documents contain a bad cut". So everything here is measured per
boundary and per issue, over documents the solver chose to emit.

    python scripts/processing/evaluate_toc_solver.py \
        --gold offprint/pdf_footnotes/issue_boundary_gold.jsonl \
        --root corpus/scraped --hold-out www.example.org
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from offprint.pdf_footnotes import toc_solver as T  # noqa: E402

EMITTING = {"auto"}


def _metrics(records: list[dict], emitting: set[str], tolerance: int = 0) -> dict:
    """Boundary precision/recall over emitted issues, plus corrupt-child count."""
    emitted = [record for record in records if record["status"] in emitting]
    true_positive = false_positive = false_negative = 0
    perfect_issues = 0
    corrupt_children = 0
    total_children = 0

    for record in emitted:
        gold = set(record["true_starts"])
        predicted = list(record["predicted_starts"])
        matched_gold: set[int] = set()
        hits = 0
        for start in predicted:
            near = [
                value
                for value in gold - matched_gold
                if abs(value - start) <= tolerance
            ]
            if near:
                best = min(near, key=lambda value: abs(value - start))
                matched_gold.add(best)
                hits += 1
            else:
                corrupt_children += 1
        true_positive += hits
        false_positive += len(predicted) - hits
        false_negative += len(gold - matched_gold)
        # Every predicted start opens a child; the last child runs to the end.
        total_children += len(predicted)
        if hits == len(predicted) == len(gold):
            perfect_issues += 1

    precision = true_positive / max(true_positive + false_positive, 1)
    recall = true_positive / max(true_positive + false_negative, 1)
    # Files that are not compilations at all (a single article carrying its own
    # contents listing, an SEC filing, a bar-association transcript). Splitting
    # one of these is the worst outcome available: every child is corrupt.
    non_compilations = [record for record in records if not record["true_starts"]]
    wrongly_split = [record for record in non_compilations if record["status"] in emitting]
    return {
        "n_issues_gold": len(records),
        "n_issues_emitted": len(emitted),
        "coverage": round(len(emitted) / max(len(records), 1), 4),
        "boundary_precision": round(precision, 4),
        "boundary_recall": round(recall, 4),
        "tp": true_positive,
        "fp": false_positive,
        "fn": false_negative,
        "issues_all_boundaries_correct": perfect_issues,
        "issues_all_correct_share": round(perfect_issues / max(len(emitted), 1), 4),
        "corrupt_children": corrupt_children,
        "children_emitted": total_children,
        "corrupt_child_rate": round(corrupt_children / max(total_children, 1), 4),
        "n_non_compilations": len(non_compilations),
        "non_compilations_wrongly_split": len(wrongly_split),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", required=True)
    parser.add_argument("--root", default="corpus/scraped")
    parser.add_argument("--out", default="")
    parser.add_argument("--tolerance", type=int, default=0)
    parser.add_argument(
        "--include-review",
        action="store_true",
        help="score `review` rows as if they were emitted (upper bound on yield)",
    )
    options = parser.parse_args()

    emitting = set(EMITTING) | ({"review"} if options.include_review else set())

    records: list[dict] = []
    for line in Path(options.gold).read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        gold = json.loads(line)
        if gold.get("_meta"):
            continue
        pdf = Path(options.root) / gold["domain"] / gold["file"]
        result = T.solve_pdf(str(pdf))
        records.append(
            {
                "domain": gold["domain"],
                "file": gold["file"],
                "true_starts": gold["true_starts"],
                "predicted_starts": result.start_pages if result.status in emitting else [],
                "status": result.status,
                "reason": result.reason,
                "ledger": result.ledger(),
            }
        )

    overall = _metrics(records, emitting, options.tolerance)
    per_domain = {}
    grouped: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        grouped[record["domain"]].append(record)
    for domain, rows in sorted(grouped.items()):
        per_domain[domain] = _metrics(rows, emitting, options.tolerance)

    report = {
        "emitting_statuses": sorted(emitting),
        "tolerance_pages": options.tolerance,
        "overall": overall,
        "per_domain": per_domain,
        "per_issue": [
            {
                "domain": record["domain"],
                "file": record["file"],
                "status": record["status"],
                "reason": record["reason"],
                "true_starts": record["true_starts"],
                "predicted_starts": record["predicted_starts"],
            }
            for record in records
        ],
    }
    text = json.dumps(report, indent=2)
    if options.out:
        Path(options.out).write_text(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
