#!/usr/bin/env python3
"""Aggregate per-PDF sidecars into the three corpus-audit manifests:

  - all_pdfs_manifest.jsonl  — every PDF with doc_policy classification
  - liteparse_results.jsonl  — included articles with extraction outcome
  - ocr_backlog.jsonl        — non-valid articles flagged for OCR review

Safe to re-run; reads sidecars, never mutates them. See
offprint/docs/full_corpus_audit_roadmap.md for the policy this implements.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Allow retroactive application of the current doc_policy filename rule to
# sidecars written by an earlier, looser version. Importing lazily so the
# aggregator still runs if the module layout shifts.
try:
    from offprint.pdf_footnotes.doc_policy import _NON_ARTICLE_FILENAME_RE
except Exception:
    _NON_ARTICLE_FILENAME_RE = None


def _iter_sidecars(pdf_root: Path):
    for dirpath, _dirnames, filenames in os.walk(pdf_root):
        for name in filenames:
            if name.endswith(".footnotes.json"):
                yield Path(dirpath) / name


def _load_sidecar(path: Path) -> dict | None:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        return {"_read_error": str(e)[:200], "_path": str(path)}


def _pdf_path_for_sidecar(sidecar_path: Path, sidecar: dict | None = None) -> str:
    # Preferred: trust pdf_path recorded inside the payload (handles the
    # hash-truncated sidecar names used for over-long filenames).
    if sidecar:
        recorded = sidecar.get("pdf_path")
        if isinstance(recorded, str) and recorded:
            return recorded
    # Fallback: sidecar is <pdf>.footnotes.json next to the PDF.
    return str(sidecar_path).removesuffix(".footnotes.json")


def _has_text_layer(sidecar: dict) -> bool:
    """Best-effort: if the solver selected any labels OR any notes were extracted
    AND no 'liteparse_no_pages' warning, there's a text layer."""
    meta = sidecar.get("document_metadata") or {}
    selected = meta.get("sequence_solver_selected_labels") or []
    if selected:
        return True
    if (sidecar.get("notes") or {}):
        return True
    warnings = sidecar.get("warnings") or []
    if "liteparse_no_pages" in warnings:
        return False
    # If neither present — ambiguous; default True (doc might be legit with zero notes)
    return True


def _title_guess(sidecar: dict) -> str | None:
    """Opportunistically extract a title-like string from first-page text.
    The sidecar doesn't carry first_page_text directly, so this returns None
    unless future extraction tagging populates it."""
    meta = sidecar.get("document_metadata") or {}
    return meta.get("title_guess") or None


def _failure_reasons(sidecar: dict, missing_labels: list[int]) -> list[str]:
    reasons: list[str] = []
    if missing_labels:
        reasons.append("missing_labels")
    warnings = set(sidecar.get("warnings") or [])
    if "liteparse_no_pages" in warnings:
        reasons.append("no_text_layer")
    if "low_font_variance_detected" in warnings:
        reasons.append("low_font_variance")
    if "reversed_word_order_suspected" in warnings:
        reasons.append("reversed_word_order")
    if "liteparse_body_marker_promotion_used" in warnings:
        reasons.append("body_marker_promotion_used")
    if not reasons:
        reasons.append("solver_gap")
    return reasons


def _ocr_recommendation(sidecar: dict, status: str, has_text_layer: bool, missing_labels: list[int]) -> str:
    if status == "valid":
        return "do_not_ocr"
    if not has_text_layer:
        return "full"
    if status == "empty":
        return "full"
    if len(missing_labels) >= 10:
        return "full"
    if missing_labels:
        # Target just the pages containing missing labels — determined post-hoc.
        return "pages:auto"
    return "do_not_ocr"


def aggregate(pdf_root: Path, out_dir: Path, extractor_version: str | None = None) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_all = out_dir / "all_pdfs_manifest.jsonl"
    manifest_lite = out_dir / "liteparse_results.jsonl"
    manifest_ocr = out_dir / "ocr_backlog.jsonl"

    counts = {
        "total_sidecars": 0,
        "article_included": 0,
        "excluded": 0,
        "excluded_no_footnote_sequence": 0,
        "valid": 0,
        "valid_with_gaps": 0,
        "invalid": 0,
        "empty": 0,
        "solver_clip_rescued": 0,
        "ocr_backlog": 0,
        "read_errors": 0,
        "version_mismatch": 0,
    }

    with manifest_all.open("w") as fa, manifest_lite.open("w") as fl, manifest_ocr.open("w") as fo:
        for sidecar_path in _iter_sidecars(pdf_root):
            counts["total_sidecars"] += 1
            sidecar = _load_sidecar(sidecar_path)
            if sidecar is None:
                continue
            if "_read_error" in sidecar:
                counts["read_errors"] += 1
                fa.write(json.dumps({
                    "pdf": _pdf_path_for_sidecar(sidecar_path),
                    "read_error": sidecar["_read_error"],
                }) + "\n")
                continue

            # Version filter (opt-in): skip sidecars from older extractor runs.
            if extractor_version and sidecar.get("extractor_version") != extractor_version:
                counts["version_mismatch"] += 1
                continue

            pdf = _pdf_path_for_sidecar(sidecar_path, sidecar)
            meta = sidecar.get("document_metadata") or {}
            doc_type = sidecar.get("doc_type") or "unknown"
            doc_policy_mode = sidecar.get("doc_policy") or ""

            # Retroactive reclassification: if the current filename blocklist
            # matches a sidecar that was written as an "article" under a
            # looser earlier version of the regex, demote it to "other" for
            # reporting. Recorded as reason_code for auditability.
            if (
                doc_type == "article"
                and _NON_ARTICLE_FILENAME_RE is not None
                and _NON_ARTICLE_FILENAME_RE.search(Path(pdf).name.lower())
            ):
                doc_type = "other"
                rcodes = list(meta.get("doc_policy_reason_codes") or [])
                rcodes.append("aggregator_filename_reclassified")
                meta["doc_policy_reason_codes"] = rcodes
            platform_family = sidecar.get("platform_family") or ""
            # Inclusion is driven by classification (doc_type=="article"), not the
            # runtime mode flag. Runs with --doc-policy=all still produce correct
            # doc_type labels; only the mode-controlled gating differs.
            include = doc_type == "article"
            ordinality = sidecar.get("ordinality")
            status = (ordinality or {}).get("status") if isinstance(ordinality, dict) else None
            has_layer = _has_text_layer(sidecar)

            fa.write(json.dumps({
                "pdf": pdf,
                "pdf_sha256": sidecar.get("pdf_sha256"),
                "page_count": meta.get("page_count"),
                "has_text_layer": has_layer,
                "doc_policy": {
                    "doc_type": doc_type,
                    "include": include,
                    "reason_codes": meta.get("doc_policy_reason_codes") or [],
                    "platform_family": platform_family,
                    "domain": meta.get("doc_policy_domain") or "",
                    "confidence": meta.get("doc_policy_confidence"),
                },
                "title_guess": _title_guess(sidecar),
                "extractor_version": sidecar.get("extractor_version"),
            }) + "\n")

            if not include:
                counts["excluded"] += 1
                continue

            # Honest-denominator filter: articles that classified as such but
            # produced zero footnote candidates AND have a usable text layer
            # AND aren't flagged for OCR are almost certainly non-footnoted
            # content (short essays, endnote-only, mis-classified reports).
            # Bucket them out of the article denominator instead of counting
            # them as extraction failures.
            selected_labels = meta.get("sequence_solver_selected_labels") or []
            notes_dict = sidecar.get("notes") or {}
            warnings_set = set(sidecar.get("warnings") or [])
            needs_ocr_flag = "needs_ocr_review" in warnings_set
            no_pages_flag = "liteparse_no_pages" in warnings_set
            is_empty_status = status in (None, "empty")
            zero_labels = len(selected_labels) == 0 and (
                len(notes_dict) == 0 if isinstance(notes_dict, dict) else True
            )
            if (
                is_empty_status
                and zero_labels
                and has_layer
                and not needs_ocr_flag
                and not no_pages_flag
            ):
                counts["excluded_no_footnote_sequence"] += 1
                continue

            notes = sidecar.get("notes") or {}
            note_count = len(notes) if isinstance(notes, dict) else 0
            gaps = (ordinality or {}).get("gaps") if isinstance(ordinality, dict) else []
            expected_range = (ordinality or {}).get("expected_range") if isinstance(ordinality, dict) else None
            expected_max = expected_range[1] if isinstance(expected_range, list) and len(expected_range) == 2 else None
            solver_clip_rescued = False

            # Solver-clip rescue: when solver produced a clean 1..N sequence
            # but the final ordinality was broken by stray notes whose labels
            # exceed max(solver_selected) — typically body_marker_promotion
            # promoting citation numerals ("280 F.3d …") — drop those notes
            # and recompute. Measured on 9.8k interim sidecars: rescues 490
            # docs with ~1% false-rescue rate (14/500 have in-range drops).
            if (
                status in ("invalid", "valid_with_gaps")
                and selected_labels
                and len(selected_labels) >= 3
                and min(selected_labels) == 1
            ):
                sel_max = max(selected_labels)
                note_labels = [int(k) for k in (notes_dict or {}).keys() if str(k).isdigit()]
                clipped = sorted(l for l in note_labels if l <= sel_max)
                if clipped:
                    lo, hi = min(clipped), max(clipped)
                    have = set(clipped)
                    missing = [x for x in range(lo, hi + 1) if x not in have]
                    total = hi - lo + 1
                    gap_frac = (len(missing) / total) if total else 1.0
                    new_status = None
                    if not missing:
                        new_status = "valid"
                    elif gap_frac <= 0.02:
                        new_status = "valid_with_gaps"
                    if new_status and new_status != status:
                        status = new_status
                        gaps = missing
                        expected_range = [lo, hi]
                        expected_max = hi
                        note_count = len(clipped)
                        solver_clip_rescued = True

            counts["article_included"] += 1

            if status == "valid":
                counts["valid"] += 1
            elif status == "valid_with_gaps":
                counts["valid_with_gaps"] += 1
            elif status == "invalid":
                counts["invalid"] += 1
            else:
                counts["empty"] += 1

            if solver_clip_rescued:
                counts["solver_clip_rescued"] += 1

            lite_row = {
                "pdf": pdf,
                "pdf_sha256": sidecar.get("pdf_sha256"),
                "status": status or "empty",
                "selected_candidate": meta.get("liteparse_selected_candidate") or "unknown",
                "notes_found": note_count,
                "expected_range": expected_range,
                "gap_count": len(gaps or []),
                "gaps": list(gaps or [])[:50],
                "solver_selected_labels_count": len(meta.get("sequence_solver_selected_labels") or []),
                "solver_clip_rescued": solver_clip_rescued,
                "warnings": sorted({w for w in (sidecar.get("warnings") or []) if w}),
                "has_text_layer": has_layer,
                "extractor_version": sidecar.get("extractor_version"),
            }
            fl.write(json.dumps(lite_row) + "\n")

            if status != "valid":
                counts["ocr_backlog"] += 1
                reasons = _failure_reasons(sidecar, gaps or [])
                rec = _ocr_recommendation(sidecar, status or "empty", has_layer, gaps or [])
                fo.write(json.dumps({
                    "pdf": pdf,
                    "pdf_sha256": sidecar.get("pdf_sha256"),
                    "status": status or "empty",
                    "notes_found": note_count,
                    "expected_max": expected_max,
                    "missing_labels": list(gaps or [])[:50],
                    "has_text_layer": has_layer,
                    "selected_candidate": meta.get("liteparse_selected_candidate") or "unknown",
                    "failure_reasons": reasons,
                    "ocr_recommendation": rec,
                    "extractor_version": sidecar.get("extractor_version"),
                }) + "\n")

    summary = out_dir / "summary.json"
    with summary.open("w") as f:
        json.dump(counts, f, indent=2)
    print(f"Wrote:\n  {manifest_all}\n  {manifest_lite}\n  {manifest_ocr}\n  {summary}")
    print(json.dumps(counts, indent=2))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--pdf-root", required=True, help="Root directory of PDFs (sidecars live next to PDFs)")
    p.add_argument("--out-dir", required=True, help="Output directory for the three manifests")
    p.add_argument("--extractor-version", default=None,
                   help="If set, only aggregate sidecars with this extractor_version")
    args = p.parse_args()

    pdf_root = Path(args.pdf_root)
    out_dir = Path(args.out_dir)
    if not pdf_root.exists():
        print(f"pdf_root not found: {pdf_root}", file=sys.stderr)
        sys.exit(2)

    aggregate(pdf_root, out_dir, args.extractor_version)


if __name__ == "__main__":
    main()
