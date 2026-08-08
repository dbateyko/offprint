#!/usr/bin/env python3
"""Run the TOC-driven boundary solver over issue-compilation PDFs.

Writes one evidence-ledger record per document to JSONL. Nothing is split here:
this stage decides and explains, and the split is a separate consumer of the
`auto` rows.

    python scripts/processing/run_toc_solver.py \
        --pdf-list /path/to/pdfs.txt --out ledger.jsonl --workers 8
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from offprint.pdf_footnotes import toc_solver as T  # noqa: E402


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def process(args: tuple[str, str, bool]) -> dict:
    path_text, root_text, want_sha = args
    path = Path(path_text)
    record: dict = {"pdf_path": str(path)}
    if root_text:
        try:
            record["pdf_relpath"] = str(path.relative_to(root_text))
            record["domain"] = record["pdf_relpath"].split("/")[0]
        except ValueError:
            pass
    try:
        if want_sha:
            record["sha256"] = _sha256(path)
        result = T.solve_pdf(str(path))
        record.update(result.ledger())
    except Exception as error:  # a bad PDF is a data point, not a crash
        record["status"] = "error"
        record["reason"] = f"{type(error).__name__}: {error}"
        record["traceback"] = traceback.format_exc()[-600:]
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdf-list", required=True, help="file of PDF paths, one per line")
    parser.add_argument("--out", required=True)
    parser.add_argument("--root", default="", help="strip this prefix to derive domain")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) // 4))
    parser.add_argument("--sha256", action="store_true", help="hash each parent (slow)")
    options = parser.parse_args()

    paths = [line.strip() for line in Path(options.pdf_list).read_text().splitlines() if line.strip()]
    if options.limit:
        paths = paths[: options.limit]

    out_path = Path(options.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    jobs = [(path, options.root, options.sha256) for path in paths]

    with out_path.open("w", encoding="utf-8") as handle:
        if options.workers <= 1:
            results = (process(job) for job in jobs)
            for record in results:
                counts[record.get("status", "?")] = counts.get(record.get("status", "?"), 0) + 1
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        else:
            with ProcessPoolExecutor(max_workers=options.workers) as pool:
                futures = [pool.submit(process, job) for job in jobs]
                for done in as_completed(futures):
                    record = done.result()
                    status = record.get("status", "?")
                    counts[status] = counts.get(status, 0) + 1
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                    handle.flush()

    print(json.dumps({"n": len(paths), "status_counts": counts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
