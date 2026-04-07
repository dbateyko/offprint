#!/usr/bin/env python3
"""
Extract footnotes from pre-computed GLM-OCR sidecars (.ocr.jsonl).

Reads artifacts/ocr/<domain>/<stem>.ocr.jsonl, builds an ExtractedDocument,
runs the standard footnote segmenter, and writes <stem>.footnotes.jsonl
alongside the OCR sidecar.

Skips files that already have a .footnotes.jsonl (use --overwrite to redo).

Usage:
    python scripts/processing/extract_footnotes_from_ocr.py
    python scripts/processing/extract_footnotes_from_ocr.py --workers 12 --limit 50
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.pdf_footnotes.citation_classify import enrich_note_features  # noqa: E402
from offprint.pdf_footnotes.context_link import attach_context_batch  # noqa: E402
from offprint.pdf_footnotes.note_segment import (  # noqa: E402
    NOTE_START_RE,
    segment_document_notes_extended,
)
from offprint.pdf_footnotes.pipeline import (  # noqa: E402
    EXTRACTOR_VERSION,
    _document_confidence,
    _note_confidence,
    _write_jsonl_sidecar_atomic,
)
from offprint.pdf_footnotes.schema import (  # noqa: E402
    OrdinalityReport,
    SidecarDocument,
    dependency_versions,
    utc_now_iso,
)
from offprint.pdf_footnotes.text_extract import (  # noqa: E402
    ExtractedDocument,
    ExtractedLine,
    ExtractedPage,
)

DEFAULT_OCR_ROOT = "artifacts/ocr"

# Footnote separator line (horizontal rule rendered as dashes/underscores by GLM-OCR)
_SEPARATOR_RE = re.compile(r"^[\-\u2014\u2015_]{4,}\s*$")


def _split_page_lines(
    text_lines: list[str], page_num: int
) -> tuple[list[ExtractedLine], list[ExtractedLine]]:
    """Split a page's text lines into (body_lines, note_lines).

    GLM-OCR places footnotes at the bottom of each page. We scan from the
    bottom upward to find where the note block starts (first NOTE_START_RE
    match or a separator line).

    Cross-page footnote continuations (a note started on page N continues at
    the top of page N+1's note section, before the next numbered marker) are
    handled by the caller: ALL lines are also placed in note_lines so the
    segmenter can see them. body_lines is kept clean (body text only) so the
    context linker can find the right sentence.
    """

    def _make(txts: list[str], pnum: int) -> list[ExtractedLine]:
        return [
            ExtractedLine(text=" ".join(t.split()), page_number=pnum, source="glmocr")
            for t in txts
            if t.strip()
        ]

    if not text_lines:
        return [], []

    # Scan upward from the bottom to find the start of the note block.
    # We accept a run of note-marker or continuation lines; stop when we reach
    # unambiguous body prose.
    note_start_idx = len(text_lines)
    for i in range(len(text_lines) - 1, -1, -1):
        line = text_lines[i].strip()
        if _SEPARATOR_RE.match(line):
            note_start_idx = i + 1  # separator discarded
            break
        if NOTE_START_RE.match(line):
            note_start_idx = i
        else:
            if note_start_idx < len(text_lines):
                # We already found at least one note marker below — stop here.
                # Lines between here and note_start_idx are cross-page continuations;
                # include them in the note block so the segmenter can concatenate them.
                note_start_idx = i + 1
                break

    body_lines = _make(text_lines[:note_start_idx], page_num)
    # note_lines: everything from note_start_idx onward (the note block, including
    # any continuation prefix before the first numbered marker on this page).
    note_lines = _make(text_lines[note_start_idx:], page_num)
    return body_lines, note_lines


def _merge_cross_page_continuations(pages: list[ExtractedPage]) -> None:
    """Move continuation lines from the top of page N+1's note block to page N's note_lines.

    A footnote that starts on page N and runs long may spill to page N+1. GLM-OCR
    places the overflow text at the top of the next page's note section, before
    the next numbered marker. We detect these lines (no NOTE_START_RE match) and
    move them to the previous page so the segmenter stitches the footnote together.
    """
    for i in range(1, len(pages)):
        prev_page = pages[i - 1]
        curr_page = pages[i]

        if not prev_page.note_lines or not curr_page.note_lines:
            continue

        # Find the index of the first numbered/symbol marker on the current page.
        first_marker_idx = next(
            (j for j, ln in enumerate(curr_page.note_lines) if NOTE_START_RE.match(ln.text)),
            None,
        )

        # If the page starts immediately with a marker, nothing to move.
        if first_marker_idx is None or first_marker_idx == 0:
            continue

        # Lines before the first marker are continuations of the previous page's last note.
        continuation = curr_page.note_lines[:first_marker_idx]
        prev_page.note_lines.extend(continuation)
        curr_page.note_lines = curr_page.note_lines[first_marker_idx:]


def _ocr_sidecar_to_document(sidecar_path: Path) -> tuple[ExtractedDocument | None, str]:
    """Parse a .ocr.jsonl sidecar into an ExtractedDocument ready for footnote extraction.

    body_lines: body prose only (for context linking).
    note_lines: the full page text (for cross-page footnote concatenation).
    """
    try:
        raw_lines = sidecar_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        return None, str(exc)

    records = []
    for raw in raw_lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            records.append(json.loads(raw))
        except json.JSONDecodeError:
            continue

    meta = next((r for r in records if r.get("type") == "metadata"), {})
    pdf_path = meta.get("pdf_path", str(sidecar_path))
    page_records = [r for r in records if r.get("type") == "page"]

    if not page_records:
        return None, "no page records in sidecar"

    pages: list[ExtractedPage] = []
    for rec in page_records:
        page_num = int(rec.get("page", 0))
        raw_text = rec.get("text", "") or ""
        text_lines = [ln for ln in raw_text.splitlines() if ln.strip()]

        body_lines, note_lines = _split_page_lines(text_lines, page_num)

        pages.append(
            ExtractedPage(
                page_number=page_num,
                body_lines=body_lines,
                note_lines=note_lines,
                raw_text=raw_text,
                source="glmocr",
            )
        )

    pages.sort(key=lambda p: p.page_number)
    _merge_cross_page_continuations(pages)
    return ExtractedDocument(pdf_path=pdf_path, pages=pages, parser="glmocr"), ""


def _process_sidecar(
    ocr_path: Path,
    *,
    features: str = "legal",
    overwrite: bool = False,
    emit_segments: bool = False,
) -> dict[str, Any]:
    out_path = ocr_path.with_suffix("").with_suffix(".footnotes.jsonl")
    if out_path.exists() and not overwrite:
        return {"status": "skipped", "path": str(ocr_path)}

    document, err = _ocr_sidecar_to_document(ocr_path)
    if document is None:
        return {"status": "error", "path": str(ocr_path), "reason": err}

    try:
        notes, author_notes, ordinality, seg_warnings = segment_document_notes_extended(document)
    except Exception as exc:
        return {"status": "error", "path": str(ocr_path), "reason": f"segmentation: {exc}"}

    attach_context_batch(notes, document)

    for note in notes:
        if features in {"legal", "all"}:
            enrich_note_features(note, preset=features)
        else:
            note.features = {}
        note.confidence = _note_confidence(note)

    warnings = sorted(set(seg_warnings))
    doc_conf = _document_confidence(notes, warnings)

    payload = SidecarDocument(
        source_pdf_path=document.pdf_path,
        pdf_sha256=None,
        extractor_version=EXTRACTOR_VERSION,
        created_at=utc_now_iso(),
        dependency_versions=dependency_versions(),
        document_confidence=doc_conf,
        warnings=warnings,
        features_preset=features,
        notes=notes,
        author_notes=author_notes,
        ordinality=ordinality,
    ).to_dict(emit_segments=emit_segments)

    _write_jsonl_sidecar_atomic(str(out_path), payload, emit_segments=emit_segments)
    return {"status": "ok", "path": str(ocr_path), "notes": len(notes)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract footnotes from GLM-OCR sidecars")
    parser.add_argument(
        "--ocr-root",
        default=DEFAULT_OCR_ROOT,
        help="Root directory containing .ocr.jsonl sidecars (default: %(default)s)",
    )
    parser.add_argument(
        "--workers", type=int, default=8, help="Parallel workers (default: 8)"
    )
    parser.add_argument(
        "--features",
        choices=["core", "legal", "all"],
        default="legal",
        help="Feature preset (default: legal)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Stop after N sidecars (0 = all)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing sidecars")
    parser.add_argument(
        "--emit-segments", action="store_true", help="Include per-segment lines in output"
    )
    args = parser.parse_args()

    ocr_root = Path(args.ocr_root)
    all_sidecars = sorted(ocr_root.rglob("*.ocr.jsonl"))
    if not args.overwrite:
        all_sidecars = [
            p for p in all_sidecars
            if not p.with_suffix("").with_suffix(".footnotes.jsonl").exists()
        ]
    if args.limit:
        all_sidecars = all_sidecars[: args.limit]

    total = len(all_sidecars)
    print(f"OCR sidecars to process: {total:,}  |  workers: {args.workers}  |  features: {args.features}")
    if total == 0:
        print("Nothing to do.")
        return

    ok = skipped = errors = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                _process_sidecar,
                p,
                features=args.features,
                overwrite=args.overwrite,
                emit_segments=args.emit_segments,
            ): p
            for p in all_sidecars
        }
        with tqdm(total=total, unit="doc", dynamic_ncols=True) as bar:
            for future in as_completed(futures):
                result = future.result()
                status = result.get("status")
                if status == "ok":
                    ok += 1
                    bar.set_postfix(ok=ok, skip=skipped, err=errors, notes=result.get("notes", 0))
                elif status == "skipped":
                    skipped += 1
                else:
                    errors += 1
                    tqdm.write(f"ERROR {result['path']}: {result.get('reason', '')}")
                bar.update(1)

    print(f"\nDone — ok: {ok}  skipped: {skipped}  errors: {errors}")


if __name__ == "__main__":
    main()
