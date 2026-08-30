#!/usr/bin/env python3
"""Apply the technology-journal promotion gate to a scrape run's records.

The gate that the 2026-08-24 technology wave promoted against, made reusable:
a staged PDF is eligible only when it is a real PDF on disk, carries a
substantive title (not front matter / masthead / contents / editorial board),
and resolves at least two of author, volume, year.

Reads ``offprint/artifacts/runs/<run_id>/records.jsonl`` and writes, per host,
``<out-dir>/<host>.qa.json`` and ``<out-dir>/<host>.allowlist.txt``. The
allowlist feeds ``promote_pdfs.py --host <host> --allowlist <file>``.

Usage:
    tech_promotion_gate.py --run-id depaul_jatip_full_20260824T133000Z_a1 \
        --out-dir offprint/artifacts/quality/tech_promotion_20260824
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path("/mnt/shared_storage/law-review-corpus")

# Journal furniture: a title matching any of these is not a substantive article.
FURNITURE = re.compile(
    r"^\s*(front|back)\s+matter\b"
    r"|^\s*masthead\b"
    r"|^\s*table\s+of\s+contents\b"
    r"|^\s*contents\b"
    r"|^\s*editorial\s+board\b"
    r"|^\s*editor'?s?\s+(note|page|foreword)\b"
    r"|^\s*board\s+of\s+editors\b"
    r"|^\s*index\b"
    r"|^\s*cover\b"
    r"|^\s*title\s+page\b"
    r"|^\s*acknowledg(e)?ments?\b"
    r"|^\s*subscription\s+information\b"
    r"|^\s*advertisement\b",
    re.IGNORECASE,
)

MIN_TITLE_CHARS = 12


def is_real_pdf(path: Path) -> bool:
    try:
        with path.open("rb") as fh:
            return fh.read(5) == b"%PDF-"
    except OSError:
        return False


def norm(value) -> str | None:
    """Collapse a metadata value to a non-empty string, else None."""
    if value is None:
        return None
    if isinstance(value, list):
        value = "; ".join(str(v) for v in value if v)
    text = str(value).strip()
    return text or None


def evaluate(record: dict) -> tuple[dict, list[str]]:
    meta = record.get("metadata") or {}
    local_path = record.get("local_path")
    title = norm(meta.get("title"))
    authors = norm(meta.get("authors")) or norm(meta.get("author"))
    volume = norm(meta.get("volume"))
    year = norm(meta.get("year"))

    reasons: list[str] = []

    if not local_path:
        reasons.append("no_local_path")
        path = None
    else:
        path = Path(local_path)
        if not path.exists():
            reasons.append("missing_file")
        elif not is_real_pdf(path):
            reasons.append("not_a_pdf")

    if not title:
        reasons.append("no_title")
    elif FURNITURE.search(title):
        reasons.append("journal_furniture")
    elif len(title) < MIN_TITLE_CHARS:
        reasons.append("title_too_short")

    if sum(1 for f in (authors, volume, year) if f) < 2:
        reasons.append("metadata_below_gate")

    entry = {
        "path": str(path) if path else None,
        "sha256": record.get("pdf_sha256"),
        "size_bytes": record.get("pdf_size_bytes"),
        "title": title,
        "authors": authors,
        "volume": volume,
        "year": year,
        "source_url": meta.get("source_url") or record.get("page_url"),
        "reasons": reasons,
    }
    return entry, reasons


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--runs-dir", type=Path, default=ROOT / "offprint/artifacts/runs")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--label", default="", help="Suffix for output filenames, e.g. 'postrepair'")
    args = ap.parse_args()

    records_path = args.runs_dir / args.run_id / "records.jsonl"
    if not records_path.exists():
        print(f"no records.jsonl at {records_path}", file=sys.stderr)
        return 1

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = f".{args.label}" if args.label else ""

    by_host: dict[str, list[dict]] = defaultdict(list)
    for line in records_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if not record.get("ok"):
            continue
        by_host[record.get("domain") or "unknown"].append(record)

    summary: dict[str, dict] = {}
    for host, records in sorted(by_host.items()):
        eligible: list[dict] = []
        rejected: list[dict] = []
        seen_sha: set[str] = set()
        reason_counts: dict[str, int] = defaultdict(int)

        for record in records:
            entry, reasons = evaluate(record)
            sha = entry["sha256"]
            if sha and sha in seen_sha:
                entry["reasons"] = reasons + ["duplicate_sha256"]
                reason_counts["duplicate_sha256"] += 1
                rejected.append(entry)
                continue
            if reasons:
                for reason in reasons:
                    reason_counts[reason] += 1
                rejected.append(entry)
                continue
            if sha:
                seen_sha.add(sha)
            eligible.append(entry)

        qa_path = out_dir / f"{host}{suffix}.qa.json"
        qa_path.write_text(
            json.dumps(
                {"host": host, "run_id": args.run_id, "eligible": eligible, "rejected": rejected},
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        allow_path = out_dir / f"{host}{suffix}.allowlist.txt"
        allow_path.write_text(
            "".join(f"{e['path']}\n" for e in eligible), encoding="utf-8"
        )

        summary[host] = {
            "run_id": args.run_id,
            "records": len(records),
            "unique_pdfs": len(seen_sha),
            "eligible": len(eligible),
            "rejected": len(rejected),
            "reasons": dict(sorted(reason_counts.items())),
        }
        print(
            f"{host}: {len(records)} records -> {len(eligible)} eligible, "
            f"{len(rejected)} rejected {dict(reason_counts) or ''}"
        )

    summary_path = out_dir / f"gate_summary{suffix or '.' + args.run_id}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\nwrote {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
