#!/usr/bin/env python3
"""
Full-document OCR for all PDFs in the artifacts tree.

Writes a <pdf>.ocr.jsonl sidecar alongside each PDF:
  Line 1: {"type": "doc", "pdf_path": ..., "page_count": N, "ocr_backend": ..., "created_at": ...}
  Lines 2+: {"type": "page", "page": N, "text": ...}

Resumable: skips PDFs that already have a sidecar (use --overwrite to redo).

Usage:
    OLMOCR_SERVER_URL=http://localhost:8000 python scripts/processing/ocr_pdfs.py
    OLMOCR_SERVER_URL=http://localhost:8000 python scripts/processing/ocr_pdfs.py \\
        --pdf-workers 4 --page-workers 2 --limit 100
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pypdf import PdfReader
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.cli import DEFAULT_PDF_ROOT  # noqa: E402
from offprint.pdf_footnotes.ocr_worker import OCRWorkerPool  # noqa: E402

DEFAULT_OCR_ROOT = "artifacts/ocr"
_MAX_STEM = 200  # leave room for suffix + extension within 255-byte fs limit


def _sidecar_path(pdf_path: Path, pdf_root: Path, ocr_root: Path) -> Path:
    """Mirror artifacts/pdfs/<domain>/<stem>.pdf → artifacts/ocr/<domain>/<stem>.ocr.jsonl.
    If the stem is too long for the filesystem, truncate and append a short hash."""
    try:
        rel = pdf_path.relative_to(pdf_root)
    except ValueError:
        rel = Path(pdf_path.name)
    domain = rel.parts[0] if len(rel.parts) > 1 else ""
    stem = pdf_path.stem
    if len(stem.encode()) > _MAX_STEM:
        short_hash = hashlib.sha256(stem.encode()).hexdigest()[:12]
        stem = stem[:_MAX_STEM] + "_" + short_hash
    sidecar_name = stem + ".ocr.jsonl"
    return ocr_root / domain / sidecar_name


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_atomic(path: Path, lines: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for line in lines:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
    tmp.rename(path)


def _page_count(pdf_path: Path) -> int:
    try:
        return len(PdfReader(str(pdf_path)).pages)
    except Exception:
        return 0


def process_pdf(
    pdf_path: Path,
    pool: OCRWorkerPool,
    sidecar: Path,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    if sidecar.exists() and not overwrite:
        return {"status": "skipped", "pdf": str(pdf_path)}

    n_pages = _page_count(pdf_path)
    if n_pages == 0:
        return {"status": "error", "pdf": str(pdf_path), "reason": "unreadable"}

    page_numbers = list(range(1, n_pages + 1))
    doc, warnings = pool.extract_document(str(pdf_path), page_numbers=page_numbers)

    if doc is None:
        return {
            "status": "error",
            "pdf": str(pdf_path),
            "reason": "; ".join(warnings) or "ocr returned nothing",
        }

    pages_by_num = {p.page_number: p.raw_text or "" for p in doc.pages}
    lines: list[dict[str, Any]] = [
        {
            "type": "doc",
            "pdf_path": str(pdf_path),
            "page_count": n_pages,
            "pages_ocrd": len(doc.pages),
            "ocr_backend": pool.backend,
            "created_at": _utc_now(),
        }
    ]
    for pnum in sorted(pages_by_num):
        lines.append({"type": "page", "page": pnum, "text": pages_by_num[pnum]})

    _write_atomic(sidecar, lines)
    return {"status": "ok", "pdf": str(pdf_path), "pages": len(doc.pages)}


def main() -> None:
    parser = argparse.ArgumentParser(description="OCR all PDFs via GLM-OCR server")
    parser.add_argument(
        "--pdf-root",
        default=DEFAULT_PDF_ROOT,
        help="Root directory containing PDFs (default: %(default)s)",
    )
    parser.add_argument(
        "--ocr-root",
        default=DEFAULT_OCR_ROOT,
        help="Root directory for OCR sidecars (default: %(default)s)",
    )
    parser.add_argument(
        "--server",
        default=os.getenv("OLMOCR_SERVER_URL", "http://localhost:8000"),
        help="GLM-OCR / vLLM server URL (default: $OLMOCR_SERVER_URL or http://localhost:8000)",
    )
    parser.add_argument(
        "--pdf-workers",
        type=int,
        default=4,
        help="Concurrent PDFs to process (default: 4)",
    )
    parser.add_argument(
        "--page-workers",
        type=int,
        default=2,
        help="Concurrent page requests per OCR pool (default: 2)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Stop after N PDFs (0 = all)")
    parser.add_argument(
        "--overwrite", action="store_true", help="Re-OCR PDFs that already have sidecars"
    )
    parser.add_argument(
        "--backend",
        choices=["glmocr", "olmocr"],
        default="glmocr",
        help="OCR backend (default: glmocr)",
    )
    args = parser.parse_args()

    os.environ["OLMOCR_SERVER_URL"] = args.server

    pdf_root = Path(args.pdf_root)
    ocr_root = Path(args.ocr_root)

    all_pdfs = sorted(pdf_root.rglob("*.pdf"))
    pdf_sidecar_pairs = [
        (p, _sidecar_path(p, pdf_root, ocr_root)) for p in all_pdfs
    ]
    if not args.overwrite:
        pdf_sidecar_pairs = [(p, s) for p, s in pdf_sidecar_pairs if not s.exists()]

    if args.limit:
        pdf_sidecar_pairs = pdf_sidecar_pairs[: args.limit]

    total = len(pdf_sidecar_pairs)
    print(f"PDFs to OCR: {total:,}  |  server: {args.server}  |  backend: {args.backend}")
    print(f"pdf-workers: {args.pdf_workers}  |  page-workers: {args.page_workers}")
    print(f"OCR output:  {ocr_root}")

    if total == 0:
        print("Nothing to do.")
        return

    pool = OCRWorkerPool(workers=args.page_workers, backend=args.backend)
    if not pool.available():
        print(f"ERROR: {args.backend} server not reachable at {args.server}")
        sys.exit(1)

    ok = skipped = errors = 0

    with ThreadPoolExecutor(max_workers=args.pdf_workers) as executor:
        futures = {
            executor.submit(process_pdf, pdf, pool, sidecar, overwrite=args.overwrite): pdf
            for pdf, sidecar in pdf_sidecar_pairs
        }
        with tqdm(total=total, unit="pdf", dynamic_ncols=True) as bar:
            for future in as_completed(futures):
                result = future.result()
                status = result.get("status")
                if status == "ok":
                    ok += 1
                elif status == "skipped":
                    skipped += 1
                else:
                    errors += 1
                    tqdm.write(f"ERROR {result['pdf']}: {result.get('reason', '')}")
                bar.set_postfix(ok=ok, skip=skipped, err=errors)
                bar.update(1)

    print(f"\nDone — ok: {ok}  skipped: {skipped}  errors: {errors}")


if __name__ == "__main__":
    main()
