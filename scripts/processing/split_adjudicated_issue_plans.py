#!/usr/bin/env python3
"""Split issue PDFs from a SHA-pinned, human-adjudicated boundary plan.

This is the precision fallback for compilations whose layouts defeat the
automatic TOC solver.  It never discovers boundaries: every parent hash, page
count, child start page, title, and author must be supplied in the plan.  The
source PDF is read-only; children and reversible provenance sidecars are
written beneath a separate output root.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pypdf import PdfReader, PdfWriter


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe(value: str) -> str:
    return "".join(char if char.isalnum() or char in "._-" else "-" for char in value)


def split_plan(plan_path: Path, output_root: Path, runs_dir: Path) -> dict[str, Any]:
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    if payload.get("schema") != "offprint.issue_split.adjudicated_plan.v1":
        raise ValueError("unsupported or missing plan schema")

    created = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = runs_dir / f"adjudicated_issue_split_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=False)
    output_root.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "manifest.jsonl"
    rows: list[dict[str, Any]] = []

    for parent in payload.get("parents", []):
        source = Path(parent["parent_pdf"])
        actual_sha = _sha256(source)
        if actual_sha != parent["parent_sha256"]:
            raise ValueError(f"SHA mismatch for {source}: {actual_sha}")
        reader = PdfReader(str(source), strict=False)
        n_pages = len(reader.pages)
        if n_pages != int(parent["parent_page_count"]):
            raise ValueError(f"page-count mismatch for {source}: {n_pages}")

        children = parent.get("children") or []
        starts = [int(child["start_page"]) for child in children]
        if len(starts) < 2 or starts != sorted(set(starts)):
            raise ValueError(f"invalid child starts for {source}: {starts}")
        if starts[0] < 1 or starts[-1] > n_pages:
            raise ValueError(f"out-of-range child starts for {source}: {starts}")

        parent_out = output_root / _safe(parent["domain"]) / _safe(source.stem)
        parent_out.mkdir(parents=True, exist_ok=False)
        for index, child in enumerate(children):
            start = starts[index]
            default_end = starts[index + 1] - 1 if index + 1 < len(starts) else n_pages
            end = int(child.get("end_page", default_end))
            if end < start or end > default_end:
                raise ValueError(
                    f"invalid end page for {source} child {index + 1}: {end}"
                )
            child_path = parent_out / f"article_{index + 1:03d}_p{start}-{end}.pdf"
            writer = PdfWriter()
            for page_index in range(start - 1, end):
                writer.add_page(reader.pages[page_index])
            with child_path.open("wb") as handle:
                writer.write(handle)
            child_sha = _sha256(child_path)
            provenance = {
                "schema": "offprint.issue_split.provenance.v1",
                "created_utc": created,
                "decision": "human_adjudicated",
                "parent": {
                    "path": str(source),
                    "sha256": actual_sha,
                    "domain": parent["domain"],
                    "n_pages": n_pages,
                },
                "child": {
                    "path": str(child_path),
                    "sha256": child_sha,
                    "index": index + 1,
                    "start_page": start,
                    "end_page": end,
                    "n_pages": end - start + 1,
                    "title": child["title"],
                    "author": child["author"],
                },
                "evidence": {
                    "source": child.get("evidence_source", "printed_toc_and_opening_page"),
                    "printed_page": child.get("printed_page"),
                    "review_notes": child.get("review_notes", ""),
                    "plan_path": str(plan_path),
                },
                "tool": {"script": "scripts/processing/split_adjudicated_issue_plans.py"},
            }
            sidecar = child_path.with_suffix(".split.json")
            sidecar.write_text(json.dumps(provenance, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            rows.append(provenance)

    with manifest_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    summary = {
        "schema": "offprint.issue_split.adjudicated_summary.v1",
        "created_utc": created,
        "plan_path": str(plan_path),
        "parents_split": len(payload.get("parents", [])),
        "children_written": len(rows),
        "failures": 0,
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--runs-dir", required=True, type=Path)
    args = parser.parse_args()
    split_plan(args.plan, args.output_root, args.runs_dir)


if __name__ == "__main__":
    main()
