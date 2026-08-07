#!/usr/bin/env python3
"""Split US law-review issue compilations into per-article child PDFs.

Uses `infer_law_review_boundaries`, which emits boundaries from one of two
grounded signals: a running head that names the article's own start page
(`Vol. 32:359`), or a per-domain head pattern from `issue_head_rules.json`.
Journals covered by neither are skipped with a reason rather than guessed at --
see the module docstring in issue_splitter.py for why generic change detection
is not safe to split on.

Children are staged OUTSIDE corpus/scraped/. They are derived artifacts, and
corpus/scraped/ is the record of what was actually downloaded; promoting them
is a separate, deliberate step.

Usage:
    python3 scripts/processing/split_law_review_issues.py \
        --candidates /path/to/relpaths.txt --workers 10
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import warnings
from multiprocessing import Pool
from pathlib import Path

warnings.filterwarnings("ignore")
logging.getLogger("pypdf").setLevel(logging.CRITICAL)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from offprint.pdf_footnotes.issue_splitter import (  # noqa: E402
    infer_law_review_boundaries,
    load_head_rules,
    sha256_file,
    utc_stamp,
)

HEAD_RULES = load_head_rules()

WORKSPACE = Path("/mnt/shared_storage/law-review-corpus")
PDF_ROOT = WORKSPACE / "corpus/scraped"
DEFAULT_OUT = WORKSPACE / "corpus/scraped_split"


def split_one(relpath: str, output_root: Path) -> dict:
    """Infer boundaries for one issue and write its child PDFs."""
    from pypdf import PdfReader, PdfWriter

    source = PDF_ROOT / relpath
    try:
        reader = PdfReader(str(source), strict=False)
        page_texts = [(page.extract_text() or "") for page in reader.pages]
    except Exception as exc:
        return {"pdf_relpath": relpath, "skip_reason": f"read_failed:{exc}"[:80]}

    domain = relpath.split("/", 1)[0]
    inference = infer_law_review_boundaries(page_texts, domain, HEAD_RULES)
    if not inference.ok:
        return {
            "pdf_relpath": relpath,
            "pages": len(page_texts),
            "skip_reason": inference.skip_reason,
            "method": inference.method,
        }

    stem = Path(relpath).stem
    parent_dir = output_root / domain / stem
    parent_dir.mkdir(parents=True, exist_ok=True)
    for stale in parent_dir.glob("*.pdf"):
        stale.unlink()

    parent_sha = sha256_file(source)
    children = []
    for index, boundary in enumerate(inference.boundaries, start=1):
        name = f"{stem}__a{index:02d}_p{boundary.start_page}-{boundary.end_page}.pdf"
        child = parent_dir / name
        writer = PdfWriter()
        for page_index in range(boundary.start_page - 1, boundary.end_page):
            writer.add_page(reader.pages[page_index])
        with child.open("wb") as handle:
            writer.write(handle)
        children.append(
            {
                "child_relpath": str(child.relative_to(output_root)),
                "article_index": index,
                "start_page": boundary.start_page,
                "end_page": boundary.end_page,
                "n_pages": boundary.end_page - boundary.start_page + 1,
                "title_guess": boundary.title_guess,
            }
        )

    return {
        "pdf_relpath": relpath,
        "pages": len(page_texts),
        "skip_reason": "",
        "method": inference.method,
        "confidence": inference.confidence,
        "parent_sha256": parent_sha,
        "domain": domain,
        "children": children,
    }


def _worker(args: tuple[str, str]) -> dict:
    relpath, output_root = args
    try:
        return split_one(relpath, Path(output_root))
    except Exception as exc:  # keep one bad PDF from killing the pool
        return {"pdf_relpath": relpath, "skip_reason": f"error:{exc}"[:80]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", required=True, type=Path)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    relpaths = [
        line.strip()
        for line in args.candidates.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if args.limit:
        relpaths = relpaths[: args.limit]
    args.out.mkdir(parents=True, exist_ok=True)

    manifest_path = args.out / f"split_manifest_{utc_stamp()}.jsonl"
    stats = {"processed": 0, "split": 0, "children": 0}
    skips: dict[str, int] = {}

    payload = [(relpath, str(args.out)) for relpath in relpaths]
    with manifest_path.open("w", encoding="utf-8") as manifest:
        with Pool(args.workers) as pool:
            for row in pool.imap_unordered(_worker, payload, chunksize=1):
                stats["processed"] += 1
                reason = row.get("skip_reason") or ""
                if reason:
                    skips[reason] = skips.get(reason, 0) + 1
                else:
                    stats["split"] += 1
                    stats["children"] += len(row.get("children") or [])
                manifest.write(json.dumps(row, ensure_ascii=False) + "\n")
                if stats["processed"] % 100 == 0:
                    print(
                        f"{stats['processed']}/{len(relpaths)} "
                        f"split={stats['split']} children={stats['children']}",
                        flush=True,
                    )

    summary = {**stats, "skip_reasons": skips, "manifest": str(manifest_path)}
    (args.out / "split_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
