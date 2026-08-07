#!/usr/bin/env python3
"""Score `looks_like_article_opening` against the labelled gold set.

The detector gates every boundary the issue splitter emits, so its precision
sets a floor on how much the splitter can be trusted. Nothing measured it until
the gold set landed; the shipped version scores precision 0.508 at recall
1.000, meaning it accepts about half the continuation pages it is shown.

That matters beyond the number. A rule emitting entirely wrong boundaries still
scores about 0.5 against the splitter's 0.6 opening-share gate, so the gate is
barely better than chance -- which is why a `back_off` of 0 could score a
perfect 1.00 on domains where 0 is plainly wrong. The gate's score cannot be
used to tune offsets until this precision improves.

The gold stores only a domain, file and page number, so pages are re-read from
corpus/scraped rather than cached; labels are O (opening), C (continuation) and
F (front matter: cover, masthead, contents, blank). F counts with C, because
the question the detector answers is "does an article start here".

Read `_meta` in the gold file before trusting the output. The labels are
LLM-authored, so scoring an LLM-based detector against them measures
self-consistency as much as correctness; and recall is measured only over
boundaries the head rules proposed, so it says nothing about openings the rules
never surface.

Usage:
    python3 scripts/quality/score_opening_detector.py
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import warnings
from collections import Counter
from pathlib import Path

warnings.filterwarnings("ignore")
logging.getLogger("pypdf").setLevel(logging.CRITICAL)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from offprint.pdf_footnotes.issue_splitter import (  # noqa: E402
    looks_like_article_opening,
)

ROOT = Path("/mnt/shared_storage/law-review-corpus")
PDF_ROOT = ROOT / "corpus/scraped"
GOLD = (
    Path(__file__).resolve().parents[2]
    / "offprint/pdf_footnotes/issue_opening_gold.jsonl"
)


def load_gold(path: Path) -> list[dict]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if "label" in record:
            rows.append(record)
    return rows


def page_text(domain: str, filename: str, page: int) -> str | None:
    from pypdf import PdfReader

    path = PDF_ROOT / domain / filename
    if not path.exists():
        return None
    try:
        reader = PdfReader(str(path), strict=False)
        if page > len(reader.pages):
            return None
        return reader.pages[page - 1].extract_text() or ""
    except Exception:
        return None


def score(rows: list[dict]) -> dict:
    counts = Counter()
    missing = 0
    by_stratum: dict[str, Counter] = {}
    for row in rows:
        text = page_text(row["domain"], row["file"], int(row["page"]))
        if text is None:
            missing += 1
            continue
        predicted = looks_like_article_opening(text)
        actual = row["label"] == "O"
        cell = ("t" if predicted == actual else "f") + ("p" if predicted else "n")
        counts[cell] += 1
        by_stratum.setdefault(str(row.get("stratum") or "?"), Counter())[cell] += 1

    def metrics(c: Counter) -> dict:
        tp, fp, fn = c["tp"], c["fp"], c["fn"]
        return {
            "n": sum(c.values()),
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": c["tn"],
            "precision": round(tp / (tp + fp), 3) if tp + fp else None,
            "recall": round(tp / (tp + fn), 3) if tp + fn else None,
        }

    return {
        "overall": metrics(counts),
        "by_stratum": {k: metrics(v) for k, v in sorted(by_stratum.items())},
        "pages_unreadable": missing,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", type=Path, default=GOLD)
    args = parser.parse_args()

    rows = load_gold(args.gold)
    if not rows:
        raise SystemExit(f"no labelled rows in {args.gold}")
    print(f"scoring {len(rows)} labelled pages from {args.gold.name}\n")
    print(json.dumps(score(rows), indent=2))


if __name__ == "__main__":
    main()
