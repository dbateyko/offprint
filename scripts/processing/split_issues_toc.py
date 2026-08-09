#!/usr/bin/env python3
"""Split issue-compilation PDFs using the TOC solver.

This is the production split runner. Boundaries come from
`offprint.pdf_footnotes.issue_split_plan`, which asks the TOC solver first and
can fall back to the per-domain running-head rules when the solver abstains.

Defaults are conservative on purpose:

* `--fallback none`. The running-head splitter is available but off: on the
  2026-08-09 trial it fired on two `review` documents and cut one 324-page BTLJ
  issue mid-article. It now runs only where the solver abstains outright, and
  only when asked for.

* `--tier auto` only. `--tier review` exists so the review tier can be enabled
  deliberately, per run, with the risk taken knowingly.
* `--dry-run` writes the manifest and no PDFs, so a candidate set can be
  inspected before anything is produced.
* Children are staged OUTSIDE `corpus/scraped/`. They are derived artifacts;
  `corpus/scraped/` is the record of what was actually downloaded. Promoting
  them into the corpus is a separate, deliberate step.

Every child gets a `<child>.split.json` sidecar carrying the parent path and
sha256, the page span, the tier, and the solver's per-boundary evidence, so any
split can be audited and reversed.

PyMuPDF is not thread-safe: this uses processes, never threads.

    python3 scripts/processing/split_issues_toc.py \
        --containers ../catalog/article_inventory/containers_to_split.parquet \
        --limit 20 --workers 6 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import warnings
from dataclasses import asdict
from multiprocessing import get_context
from pathlib import Path

warnings.filterwarnings("ignore")
logging.getLogger("pypdf").setLevel(logging.CRITICAL)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from offprint.pdf_footnotes import issue_split_plan as P  # noqa: E402
from offprint.pdf_footnotes.issue_splitter import sha256_file, utc_stamp  # noqa: E402

WORKSPACE = Path("/mnt/shared_storage/law-review-corpus")
PDF_ROOT = WORKSPACE / "corpus/scraped"
DEFAULT_OUT = WORKSPACE / "corpus/scraped_split_toc"


def _tool_stamp() -> dict:
    import subprocess

    try:
        sha = subprocess.run(
            ["git", "-c", "safe.directory=*", "rev-parse", "--short", "HEAD"],
            cwd=str(Path(__file__).resolve().parents[2]),
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout.strip()
    except Exception:
        sha = ""
    return {"script": "scripts/processing/split_issues_toc.py", "git_sha": sha}


def split_one(job: dict) -> dict:
    """Plan and (unless dry-run) write the children of one parent PDF."""
    from pypdf import PdfReader, PdfWriter

    relpath = job["relpath"]
    source = Path(job["pdf_root"]) / relpath
    domain = relpath.split("/", 1)[0]
    row: dict = {
        "pdf_relpath": relpath,
        "domain": domain,
        "container_id": job.get("container_id", ""),
    }

    if not source.exists():
        row["skip_reason"] = "missing_on_disk"
        return row

    try:
        plan = P.plan_pdf(
            str(source),
            domain,
            tier=job["tier"],
            fallback=job["fallback"],
        )
    except Exception as error:
        row["skip_reason"] = f"plan_failed:{type(error).__name__}:{error}"[:160]
        return row

    row.update(
        {
            "pages": plan.n_pages,
            "solver_status": plan.solver_status,
            "solver_reason": plan.solver_reason,
            "source": plan.source,
            "tier": plan.tier,
            "reason": plan.reason,
            "n_children": len(plan.spans),
            "ledger": plan.ledger if job["keep_ledger"] else None,
        }
    )
    if not plan.ok:
        row["skip_reason"] = plan.reason
        return row
    row["skip_reason"] = ""

    spans = [asdict(span) for span in plan.spans]
    row["spans"] = spans
    if job["dry_run"]:
        row["dry_run"] = True
        return row

    parent_sha = sha256_file(source)
    row["parent_sha256"] = parent_sha
    stem = Path(relpath).stem
    parent_dir = Path(job["output_root"]) / domain / stem
    parent_dir.mkdir(parents=True, exist_ok=True)
    for stale in list(parent_dir.glob("*.pdf")) + list(parent_dir.glob("*.split.json")):
        stale.unlink()

    reader = PdfReader(str(source), strict=False)
    parent_record = {
        "relpath": relpath,
        "path": str(source),
        "sha256": parent_sha,
        "domain": domain,
        "n_pages": plan.n_pages,
        "container_id": job.get("container_id", ""),
    }
    created = utc_stamp()
    tool = job["tool"]

    children = []
    for span in plan.spans:
        name = f"{stem}__a{span.index:02d}_p{span.start_page}-{span.end_page}.pdf"
        child_path = parent_dir / name
        writer = PdfWriter()
        for page_index in range(span.start_page - 1, span.end_page):
            writer.add_page(reader.pages[page_index])
        with child_path.open("wb") as handle:
            writer.write(handle)

        child_record = {
            "relpath": str(child_path.relative_to(job["output_root"])),
            "path": str(child_path),
            "sha256": sha256_file(child_path),
            "bytes": child_path.stat().st_size,
        }
        provenance = P.child_provenance(
            plan,
            span,
            parent=parent_record,
            child=child_record,
            created_utc=created,
            run_id=job["run_id"],
            tool=tool,
        )
        child_path.with_suffix(".split.json").write_text(
            json.dumps(provenance, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        children.append(
            {
                "child_relpath": child_record["relpath"],
                "article_index": span.index,
                "start_page": span.start_page,
                "end_page": span.end_page,
                "n_pages": span.n_pages,
                "title": span.title,
                "author": span.author,
            }
        )

    row["children"] = children
    return row


def _load_containers(path: Path, source_filter: str, needs_split_only: bool) -> list[dict]:
    import pandas as pd

    frame = pd.read_parquet(path)
    if source_filter:
        frame = frame[frame["source"] == source_filter]
    if needs_split_only and "split_status" in frame:
        frame = frame[frame["split_status"] == "needs_split"]
    if "format" in frame:
        frame = frame[frame["format"] == "pdf"]
    out: list[dict] = []
    for record in frame.to_dict("records"):
        locator = str(record.get("source_locator") or "")
        if not locator.startswith("corpus/scraped/"):
            continue
        out.append(
            {
                "relpath": locator[len("corpus/scraped/") :],
                "container_id": str(record.get("container_id") or ""),
                "page_count": record.get("page_count"),
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--containers", type=Path, help="containers_to_split.parquet")
    group.add_argument("--candidates", type=Path, help="text file of relpaths under --pdf-root")
    parser.add_argument("--pdf-root", type=Path, default=PDF_ROOT)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--tier", choices=sorted(P.TIER_RANK), default="auto")
    parser.add_argument(
        "--fallback",
        choices=["running_head", "none"],
        default="none",
        help="boundary source when the solver ABSTAINS (not merely when it is unsure)",
    )
    parser.add_argument("--source-filter", default="scraped")
    parser.add_argument("--all-split-status", action="store_true")
    parser.add_argument("--domain", default="", help="substring filter on the relpath")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--shuffle-seed", type=int, default=0, help="0 keeps file order")
    parser.add_argument("--dry-run", action="store_true", help="plan only, write no PDFs")
    parser.add_argument("--keep-ledger", action="store_true", help="full solver ledger in manifest")
    parser.add_argument("--dedupe", action="store_true", help="drop byte-identical parents")
    args = parser.parse_args()

    if args.containers:
        rows = _load_containers(
            args.containers, args.source_filter, not args.all_split_status
        )
    else:
        rows = [
            {"relpath": line.strip(), "container_id": ""}
            for line in args.candidates.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    if args.domain:
        rows = [row for row in rows if args.domain in row["relpath"]]
    if args.shuffle_seed:
        import random

        random.Random(args.shuffle_seed).shuffle(rows)

    if args.dedupe:
        seen: dict[str, str] = {}
        deduped = []
        for row in rows:
            path = args.pdf_root / row["relpath"]
            if not path.exists():
                continue
            digest = sha256_file(path)
            if digest in seen:
                continue
            seen[digest] = row["relpath"]
            deduped.append(row)
        rows = deduped

    if args.limit:
        rows = rows[: args.limit]

    run_id = f"toc_split_{utc_stamp()}"
    args.out.mkdir(parents=True, exist_ok=True)
    manifest_path = args.out / f"{run_id}.manifest.jsonl"
    tool = _tool_stamp()

    jobs = [
        {
            **row,
            "pdf_root": str(args.pdf_root),
            "output_root": str(args.out),
            "tier": args.tier,
            "fallback": args.fallback,
            "dry_run": bool(args.dry_run),
            "keep_ledger": bool(args.keep_ledger),
            "run_id": run_id,
            "tool": tool,
        }
        for row in rows
    ]

    stats = {
        "run_id": run_id,
        "tier": args.tier,
        "fallback": args.fallback,
        "dry_run": bool(args.dry_run),
        "candidates": len(jobs),
        "processed": 0,
        "split": 0,
        "children": 0,
        "by_source": {},
        "by_solver_status": {},
    }
    skips: dict[str, int] = {}

    # PyMuPDF is not thread-safe; "spawn" also keeps a crashed worker from
    # taking inherited state down with it.
    context = get_context("spawn")
    with manifest_path.open("w", encoding="utf-8") as manifest:
        with context.Pool(args.workers) as pool:
            for row in pool.imap_unordered(split_one, jobs, chunksize=1):
                stats["processed"] += 1
                status = row.get("solver_status") or "error"
                stats["by_solver_status"][status] = stats["by_solver_status"].get(status, 0) + 1
                reason = row.get("skip_reason") or ""
                if reason:
                    key = reason.split(":")[0]
                    skips[key] = skips.get(key, 0) + 1
                else:
                    stats["split"] += 1
                    stats["children"] += row.get("n_children", 0)
                    src = row.get("source") or "?"
                    stats["by_source"][src] = stats["by_source"].get(src, 0) + 1
                manifest.write(json.dumps(row, ensure_ascii=False) + "\n")
                if stats["processed"] % 25 == 0:
                    print(
                        f"{stats['processed']}/{len(jobs)} split={stats['split']} "
                        f"children={stats['children']}",
                        flush=True,
                    )

    summary = {**stats, "skip_reasons": skips, "manifest": str(manifest_path)}
    (args.out / f"{run_id}.summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    os.nice(10)
    main()
