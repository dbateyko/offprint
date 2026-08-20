#!/usr/bin/env python3
"""Turn `review`-tier boundaries into a blind adjudication queue.

The solver's `review` tier holds boundaries that rest on a single strong signal
or a thin margin. Measured over the 2026-08-07 sweep, that is ~7.3 boundaries per
review document of which ~40% need a decision. This builds one queue item per
*weak* boundary.

**The queue is blind.** The item never says which page the solver chose. It
presents a window of candidate pages in document order, each with the top of its
text, and asks which one begins the named piece. Showing the model the solver's
pick would measure agreement-under-anchoring, not correctness -- the same
circularity that made the 2026-08-07 page-level gold set uninterpretable.

    python scripts/processing/build_adjudication_queue.py \
        --ledger ledger.jsonl --out queue.jsonl --window 3
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from offprint.pdf_footnotes import toc_solver as T  # noqa: E402

MIN_MARGIN = 2.0
LINES_PER_PAGE = 12


def is_weak(assignment: dict) -> bool:
    """A boundary the document does not by itself determine."""
    signals = assignment.get("signals", {})
    strong = set(signals.get("strong", []))
    two_ways = ("folio" in strong and (strong & {"title", "author"})) or (
        {"title", "author"} <= strong and signals.get("opening")
    )
    margin = assignment.get("margin")
    thin = margin is not None and margin < MIN_MARGIN
    return (not two_ways) or thin


def candidate_pages(assignment: dict, n_pages: int, window: int) -> list[int]:
    """Pages the adjudicator chooses between, in document order.

    The solver's choice and the runner-up from the margin DP are both included,
    surrounded by their neighbours so the answer is not simply the middle item.
    """
    anchors = [assignment["physical_page"]]
    if assignment.get("runner_up_page"):
        anchors.append(assignment["runner_up_page"])
    pages: set[int] = set()
    for anchor in anchors:
        for offset in range(-window, window + 1):
            page = anchor + offset
            if 1 <= page <= n_pages:
                pages.add(page)
    return sorted(pages)


def render_page(page: T.Page, lines: int = LINES_PER_PAGE) -> str:
    return "\n".join(line.text for line in page.lines[:lines])


def build_items(record: dict, window: int, only_weak: bool = True) -> list[dict]:
    assignments = record.get("assignments", [])
    weak = [a for a in assignments if is_weak(a)] if only_weak else list(assignments)
    if not weak:
        return []
    pages = T.extract_pages(record["pdf_path"])
    n_pages = len(pages)

    items: list[dict] = []
    for assignment in weak:
        options = candidate_pages(assignment, n_pages, window)
        if len(options) < 2:
            continue
        items.append(
            {
                "item_id": f"{record.get('pdf_relpath') or record['pdf_path']}#p{assignment['physical_page']}",
                "pdf_path": record["pdf_path"],
                "pdf_relpath": record.get("pdf_relpath", ""),
                "domain": record.get("domain", ""),
                "n_pages": n_pages,
                "entry": {
                    "title": assignment["title"],
                    "author": assignment["author"],
                    "section": assignment["section"],
                    "printed_page": assignment["printed_page"],
                },
                "candidates": [
                    {"physical_page": page, "text": render_page(pages[page - 1])} for page in options
                ],
                # Held back from the prompt; used only when scoring the answers.
                "_solver": {
                    "physical_page": assignment["physical_page"],
                    "runner_up_page": assignment.get("runner_up_page"),
                    "margin": assignment.get("margin"),
                    "signals": assignment.get("signals", {}),
                },
            }
        )
    return items


PROMPT = """You are reading one issue of a law journal that has been scanned into a single PDF.

The issue's table of contents lists this piece:

  Title:   {title}
  Author:  {author}
  Section: {section}
  Printed start page: {printed_page}

Below are candidate pages from the PDF, given by their physical page number in
the file, each showing the text at the top of the page.

{candidates}

Which physical page is the FIRST page of that piece?

An opening page shows the piece's display title, usually with the author's name
beneath it, and its body does not continue a sentence from the previous page. A
continuation page carries a running head (the journal name, or a shortened
title, with a page number) and then resumes prose already under way.

Answer with the physical page number, or 0 if none of these pages opens it."""


def render_prompt(item: dict) -> str:
    blocks = []
    for candidate in item["candidates"]:
        blocks.append(f"--- physical page {candidate['physical_page']} ---\n{candidate['text']}")
    entry = item["entry"]
    return PROMPT.format(
        title=entry["title"] or "(untitled)",
        author=entry["author"] or "(not given)",
        section=entry["section"] or "(not given)",
        printed_page=entry["printed_page"],
        candidates="\n\n".join(blocks),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--window", type=int, default=3)
    parser.add_argument("--status", default="review", help="comma-separated statuses to queue")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--print-one", action="store_true", help="show a rendered prompt and exit")
    parser.add_argument(
        "--all-boundaries",
        action="store_true",
        help="queue every boundary, not just weak ones. Used to build a CONTROL set from\n"
        "`auto` documents: those boundaries carry two independent strong signals and a\n"
        "fat margin, so they are near-certainly right, and the adjudicator's agreement\n"
        "rate on them measures the ADJUDICATOR rather than the solver.",
    )
    options = parser.parse_args()

    wanted = set(options.status.split(","))
    records = [json.loads(line) for line in Path(options.ledger).read_text().splitlines() if line.strip()]
    records = [r for r in records if r.get("status") in wanted]
    if options.limit:
        records = records[: options.limit]

    out_path = Path(options.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    n_items = 0
    with out_path.open("w", encoding="utf-8") as handle:
        for record in records:
            try:
                items = build_items(record, options.window, only_weak=not options.all_boundaries)
            except Exception as error:
                print(f"skip {record.get('pdf_relpath')}: {type(error).__name__}: {error}", file=sys.stderr)
                continue
            for item in items:
                if options.print_one:
                    print(render_prompt(item))
                    return 0
                handle.write(json.dumps(item, ensure_ascii=False) + "\n")
                n_items += 1

    print(json.dumps({"documents": len(records), "items": n_items}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
