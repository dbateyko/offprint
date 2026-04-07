#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.cli import DEFAULT_PDF_ROOT  # noqa: E402
from offprint.pdf_footnotes.citation_classify import enrich_note_features  # noqa: E402
from offprint.pdf_footnotes.context_link import attach_context_batch  # noqa: E402
from offprint.pdf_footnotes.note_segment import segment_document_notes_extended  # noqa: E402
from offprint.pdf_footnotes.schema import (  # noqa: E402
    SidecarDocument,
    dependency_versions,
    utc_now_iso,
)
from offprint.pdf_footnotes.text_extract import (  # noqa: E402
    ExtractedDocument,
    ExtractedLine,
    ExtractedPage,
    extract_document_text,
)

EXTRACTOR_VERSION = "docling-0.4.0"

# Patterns retained only for pdftotext fallback path
_NOTE_MARKER_RE = re.compile(
    r"^\s*(?P<label>(?:\d{1,4}|[*†‡§¶])[\]\)\.,:;-]?|[ivxlcdm]{1,7}[\]\)\.,:;-])\s+"
    r"(?P<text>\S.+)$",
    re.IGNORECASE,
)
_NOSPACE_MARKER_RE = re.compile(
    r"^\s*(?P<label>\d{1,4})(?P<text>[A-Z]\S.*)$",
)
_SPACE_DIGIT_MARKER_RE = re.compile(
    r"^\s*(?P<label>\d)\s+(?P<text>\S.+)$",
)
_QUOTE_MARKER_RE = re.compile(
    r'^["\u201c\u201d]\s+(?P<text>\S.+)$',
)
_QUOTEWRAP_MARKER_RE = re.compile(
    r"^'(?P<partial>\d+)'(?P<text>[A-Z]\S.*)$",
)
_AUTHOR_NOTE_RE = re.compile(
    r"^\?\s+(?P<text>(?:Visiting|Associate|Assistant|Adjunct|Professor|Dean|"
    r"Lecturer|Fellow|J\.D\.|LL\.M\.|S\.J\.D\.|Ph\.D\.)\S?.*)$",
    re.IGNORECASE,
)
_INLINE_LABEL_RE = re.compile(
    r"(?P<pre>.*?[.;:\")\]])\s*(?P<label>\d{1,4})(?P<text>[A-Z]\S.*)",
)
_DOT_LEADER_RE = re.compile(r"\.{3,}")
_TOC_TRAILING_PAGE_RE = re.compile(r"\b\d{1,3}\s*$")
_SHORT_ID_RE = re.compile(r"^\s*(?:id\.?|ibid\.?)(?:\s+at\s+\d+)?\.?\s*$", re.IGNORECASE)
_CITATION_CUE_RE = re.compile(
    r"(?:\b(?:See|Id\.|Ibid\.|Cf\.|But see|Compare|Accord|Supra|Infra)\b"
    r"|§|\b[A-Z][a-z]+\s+v\.\s+[A-Z][a-z]+"
    r"|\b\d{1,3}\s+(?:U\.?S\.?|F\.\d+d|S\.?\s*Ct\.?|C\.?F\.?R\.?|U\.?S\.?C\.?)\b)",
    re.IGNORECASE,
)
_PAGE_HEADER_RE = re.compile(
    r"^\s*\d{1,4}\s{4,}.*(?:Law\s|Journal\s|Review\s|Vol\.\s|University\s)",
    re.IGNORECASE,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract footnotes from PDFs using docling (with pdftotext fallback)."
    )
    parser.add_argument(
        "--pdf-root",
        default=DEFAULT_PDF_ROOT,
        help="Root directory containing PDFs.",
    )
    parser.add_argument(
        "--pdf",
        action="append",
        default=[],
        help="Specific PDF path(s) to process; can be repeated.",
    )
    parser.add_argument(
        "--layout",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use `pdftotext -layout` mode (only used in fallback path).",
    )
    parser.add_argument(
        "--features",
        choices=["core", "legal", "all"],
        default="legal",
        help="Feature enrichment preset.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of discovered PDFs from --pdf-root.",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Overwrite existing sidecars.",
    )
    parser.add_argument(
        "--out-suffix",
        default=".footnotes.pdftotext.json",
        help="Output sidecar suffix.",
    )
    parser.add_argument(
        "--report-out",
        default="",
        help="Optional path to write a JSON summary report.",
    )
    return parser.parse_args()


def _discover_pdfs(pdf_root: Path, limit: int = 0) -> list[Path]:
    found = sorted(path for path in pdf_root.rglob("*.pdf") if path.is_file())
    if limit and limit > 0:
        return found[:limit]
    return found


# ---------------------------------------------------------------------------
# pdftotext fallback helpers (used only when docling is unavailable)
# ---------------------------------------------------------------------------


def _run_pdftotext(pdf_path: Path, use_layout: bool) -> str:
    pdftotext_bin = shutil.which("pdftotext") or "/usr/bin/pdftotext"
    cmd = [pdftotext_bin]
    if use_layout:
        cmd.append("-layout")
    cmd.extend([str(pdf_path), "-"])
    result = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"pdftotext failed for {pdf_path}")
    return result.stdout


def _clean_marker_label(raw_label: str) -> str:
    return re.sub(r"[\]\)\.,:;-]+$", "", (raw_label or "").strip())


def _looks_toc_like(rest: str) -> bool:
    text = " ".join((rest or "").split())
    if not text:
        return True
    if _DOT_LEADER_RE.search(text):
        return True
    if _TOC_TRAILING_PAGE_RE.search(text) and not _CITATION_CUE_RE.search(text):
        token_count = len(text.split())
        if token_count <= 10:
            return True
    if text.isupper() and len(text.split()) <= 12:
        return True
    return False


def _has_strong_footnote_signal(rest: str) -> bool:
    text = " ".join((rest or "").split())
    if not text:
        return False
    if _SHORT_ID_RE.match(text):
        return True
    return bool(_CITATION_CUE_RE.search(text))


def _match_footnote_line(line: str, is_top_of_page: bool = False) -> tuple[str, str] | None:
    """Try all marker patterns. Returns (label, rest_text) or None."""
    if is_top_of_page and _PAGE_HEADER_RE.match(line):
        return None
    match = _NOTE_MARKER_RE.match(line)
    if match:
        return _clean_marker_label(match.group("label")), match.group("text")
    match = _NOSPACE_MARKER_RE.match(line)
    if match:
        label = match.group("label")
        rest = match.group("text")
        if label.isdigit():
            n = int(label)
            if 1900 <= n <= 2099:
                return None
        return label, rest
    match = _QUOTEWRAP_MARKER_RE.match(line)
    if match:
        return match.group("partial"), match.group("text")
    match = _QUOTE_MARKER_RE.match(line)
    if match:
        return "?quote", match.group("text")
    return None


def _extract_note_lines(page_lines: list[str]) -> list[str]:
    if not page_lines:
        return []

    marker_rows: list[tuple[int, str, str]] = []
    for idx, line in enumerate(page_lines):
        parsed = _match_footnote_line(line, is_top_of_page=(idx < 3))
        if not parsed:
            amatch = _AUTHOR_NOTE_RE.match(line)
            if amatch:
                marker_rows.append((idx, "†", amatch.group("text")))
            continue
        label, rest = parsed
        if label.isdigit():
            numeric_label = int(label)
            if 1900 <= numeric_label <= 2099:
                continue
            if numeric_label > 600:
                continue
        strong_signal = _has_strong_footnote_signal(rest)
        if len(label) == 1 and label.lower() in {"i", "v", "x"} and not strong_signal:
            continue
        if label.isdigit() and int(label) <= 5 and not strong_signal:
            if idx < len(page_lines) * 0.45:
                continue
        if _looks_toc_like(rest):
            continue
        marker_rows.append((idx, label, rest))

    if not marker_rows:
        return []

    threshold_idx = int(len(page_lines) * 0.45)
    bottom_markers = [
        (idx, label, rest) for idx, label, rest in marker_rows if idx >= threshold_idx
    ]
    if bottom_markers:
        start_idx = bottom_markers[0][0]
    else:
        signaled = [idx for idx, label, rest in marker_rows if _has_strong_footnote_signal(rest)]
        if not signaled:
            return []
        start_idx = min(signaled)
    return page_lines[start_idx:]


def _split_inline_labels(lines: list[str]) -> list[str]:
    """Split lines that contain merged footnotes like '...services.). 100See next...'"""
    result: list[str] = []
    for line in lines:
        m = _INLINE_LABEL_RE.match(line)
        if m:
            pre = m.group("pre").rstrip()
            label = m.group("label")
            rest = m.group("text")
            if pre:
                result.append(pre)
            result.append(f"{label} {rest}")
        else:
            result.append(line)
    return result


def _normalize_note_lines(lines: list[str]) -> list[str]:
    """Normalize note lines so the downstream segmenter's NOTE_START_RE can match them."""
    result: list[str] = []
    for line in lines:
        if _PAGE_HEADER_RE.match(line):
            result.append(line)
            continue
        amatch = _AUTHOR_NOTE_RE.match(line)
        if amatch:
            result.append(f"† {amatch.group('text')}")
            continue
        m = _NOSPACE_MARKER_RE.match(line)
        if m and not _NOTE_MARKER_RE.match(line):
            result.append(f"{m.group('label')} {m.group('text')}")
            continue
        m = _QUOTEWRAP_MARKER_RE.match(line)
        if m:
            result.append(f"{m.group('partial')} {m.group('text')}")
            continue
        m = _QUOTE_MARKER_RE.match(line)
        if m:
            result.append(f"0 {m.group('text')}")
            continue
        result.append(line)
    return result


def _repair_garbled_labels(note_lines: list[str], body_lines: list[str]) -> list[str]:
    """Repair pdftotext label garbling in extracted note lines."""
    repaired: list[str] = []
    known_labels: list[int] = []
    for line in note_lines:
        parsed = _match_footnote_line(line)
        if parsed:
            label, _ = parsed
            if label.isdigit():
                known_labels.append(int(label))

    for line in note_lines:
        m = _SPACE_DIGIT_MARKER_RE.match(line)
        if m and not _PAGE_HEADER_RE.match(line):
            single_digit = int(m.group("label"))
            rest = m.group("text")
            candidates = []
            for tens in range(1, 20):
                candidate = tens * 10 + single_digit
                candidates.append(candidate)
            best = None
            for c in candidates:
                if c not in known_labels:
                    if (
                        (c - 1) in known_labels
                        or (c + 1) in known_labels
                        or (c - 2) in known_labels
                    ):
                        best = c
                        break
            if best is not None:
                repaired.append(f"{best} {rest}")
                known_labels.append(best)
                known_labels.sort()
                continue
        repaired.append(line)
    return repaired


def _document_from_pdftotext(pdf_path: Path, text: str) -> ExtractedDocument:
    pages: list[ExtractedPage] = []
    for page_num, raw_page in enumerate(text.split("\f"), start=1):
        raw_lines = [line.rstrip() for line in raw_page.splitlines()]
        body_text_lines = [line for line in raw_lines if line.strip()]
        body_text_lines = [
            re.sub(r"\.{4,}", "\u2026.", line).replace("...", "\u2026") for line in body_text_lines
        ]
        body_text_lines = _split_inline_labels(body_text_lines)
        note_text_lines = _extract_note_lines(body_text_lines)
        note_text_lines = _repair_garbled_labels(note_text_lines, body_text_lines)
        note_text_lines = _normalize_note_lines(note_text_lines)

        body_lines = [
            ExtractedLine(text=line, page_number=page_num, source="pdftotext")
            for line in body_text_lines
        ]
        note_lines = [
            ExtractedLine(text=line, page_number=page_num, source="pdftotext")
            for line in note_text_lines
        ]
        pages.append(
            ExtractedPage(
                page_number=page_num,
                body_lines=body_lines,
                note_lines=note_lines,
                raw_text=raw_page,
                source="pdftotext",
            )
        )
    return ExtractedDocument(
        pdf_path=str(pdf_path),
        pages=pages,
        warnings=[],
        parser="pdftotext",
    )


def _repair_note_labels(notes: list[Any]) -> list[Any]:
    """Post-segmentation repair of garbled labels using global sequence analysis."""
    if not notes:
        return notes

    known: set[int] = set()
    for note in notes:
        if note.label.isdigit() and int(note.label) > 0:
            known.add(int(note.label))

    placeholders = []
    for i, note in enumerate(notes):
        if note.label == "?quote" or note.label == "0":
            placeholders.append(i)

    if placeholders:
        p_ptr = 0
        while p_ptr < len(placeholders):
            start_p_idx = p_ptr
            while (
                p_ptr + 1 < len(placeholders) and placeholders[p_ptr + 1] == placeholders[p_ptr] + 1
            ):
                p_ptr += 1
            cluster_indices = placeholders[start_p_idx : p_ptr + 1]

            prev_val = None
            for j in range(cluster_indices[0] - 1, -1, -1):
                if notes[j].label.isdigit() and int(notes[j].label) > 0:
                    prev_val = int(notes[j].label)
                    break
            next_val = None
            for j in range(cluster_indices[-1] + 1, len(notes)):
                if notes[j].label.isdigit() and int(notes[j].label) > 0:
                    next_val = int(notes[j].label)
                    break

            if prev_val is not None:
                curr = prev_val + 1
                for idx in cluster_indices:
                    notes[idx].label = str(curr)
                    curr += 1
            elif next_val is not None:
                curr = next_val - len(cluster_indices)
                for idx in cluster_indices:
                    if curr > 0:
                        notes[idx].label = str(curr)
                    curr += 1
            p_ptr += 1

    for i, note in enumerate(notes):
        if note.label.isdigit() and len(note.label) == 1:
            digit = int(note.label)
            if digit == 0:
                continue

            prev_val = None
            for j in range(i - 1, -1, -1):
                if notes[j].label.isdigit() and int(notes[j].label) > 9:
                    prev_val = int(notes[j].label)
                    break
            next_val = None
            for j in range(i + 1, len(notes)):
                if notes[j].label.isdigit() and int(notes[j].label) > 9:
                    next_val = int(notes[j].label)
                    break

            if prev_val:
                expected = prev_val + 1
                if expected % 10 == digit:
                    note.label = str(expected)
                    continue
                for tens in range(1, 40):
                    cand = tens * 10 + digit
                    if prev_val < cand and (next_val is None or cand < next_val):
                        if cand - prev_val <= 3:
                            note.label = str(cand)
                            break
            elif next_val:
                expected = next_val - 1
                if expected % 10 == digit and expected > 0:
                    note.label = str(expected)

    return notes


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _sidecar_path(pdf_path: Path, suffix: str) -> Path:
    return Path(f"{pdf_path}{suffix}")


def _note_confidence(note: Any) -> float:
    score = 0.55
    text_len = len((note.text or "").strip())
    if text_len >= 80:
        score += 0.2
    elif text_len >= 30:
        score += 0.1
    else:
        score -= 0.1
    if note.context_body:
        score += 0.1
    else:
        score -= 0.15
    if note.page_end > note.page_start:
        score += 0.05
    for flag in note.quality_flags:
        if flag in {"ambiguous_context", "missing_context"}:
            score -= 0.1
        if flag == "non_monotonic_labels":
            score -= 0.05
    return max(0.0, min(1.0, score))


def _document_confidence(notes: list[Any], warnings: list[str]) -> float:
    if not notes:
        return 0.0
    average = sum(note.confidence for note in notes) / max(len(notes), 1)
    penalty = min(0.25, 0.03 * len(warnings))
    return max(0.0, average - penalty)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------


def _process_pdf(
    pdf_path: Path,
    *,
    use_layout: bool,
    features: str,
    out_suffix: str,
    overwrite: bool,
    versions: dict[str, str],
) -> dict[str, Any]:
    sidecar = _sidecar_path(pdf_path, out_suffix)
    if sidecar.exists() and not overwrite:
        return {
            "pdf_path": str(pdf_path),
            "sidecar_path": str(sidecar),
            "status": "skipped_existing",
            "notes": 0,
            "author_notes": 0,
            "warnings": [],
        }

    # Primary path: docling extraction
    try:
        document = extract_document_text(str(pdf_path), parser_mode="docling_only")
    except Exception:
        document = None

    parser_used = "docling"
    warnings: list[str] = []

    if document is None or not document.pages:
        # Fallback: pdftotext
        try:
            text = _run_pdftotext(pdf_path, use_layout=use_layout)
            document = _document_from_pdftotext(pdf_path, text)
            parser_used = "pdftotext"
            warnings.append("docling_unavailable_used_pdftotext_fallback")
        except Exception as exc:
            return {
                "pdf_path": str(pdf_path),
                "sidecar_path": str(sidecar),
                "status": "failed",
                "error": f"Both docling and pdftotext failed: {exc}",
                "notes": 0,
                "author_notes": 0,
                "warnings": [],
            }

    notes, author_notes, ordinality, note_warnings = segment_document_notes_extended(
        document,
        gap_tolerance=6,
        strict_label_filter=True,
    )
    warnings.extend(note_warnings)

    # Post-segmentation label repair for pdftotext fallback
    if parser_used == "pdftotext":
        notes = _repair_note_labels(notes)

        # Remove duplicate labels
        seen_labels: dict[str, int] = {}
        to_remove: set[int] = set()
        for i, note in enumerate(notes):
            if not note.label.isdigit():
                continue
            if note.label in seen_labels:
                prev_idx = seen_labels[note.label]
                prev = notes[prev_idx]

                def _fn_score(n: Any) -> float:
                    s = len(n.text)
                    if _CITATION_CUE_RE.search(n.text):
                        s += 500
                    return s

                if _fn_score(note) > _fn_score(prev):
                    to_remove.add(prev_idx)
                    seen_labels[note.label] = i
                else:
                    to_remove.add(i)
            else:
                seen_labels[note.label] = i
        if to_remove:
            notes = [n for i, n in enumerate(notes) if i not in to_remove]

        # Re-validate ordinality after repairs
        from offprint.pdf_footnotes.note_segment import validate_ordinality

        numeric_labels = [int(n.label) for n in notes if n.label.isdigit()]
        if numeric_labels:
            ordinality = validate_ordinality(numeric_labels, gap_tolerance=6)

    attach_context_batch(notes, document)
    for note in notes:
        if features in {"legal", "all"}:
            enrich_note_features(note, preset=features)
        note.confidence = _note_confidence(note)

    payload = SidecarDocument(
        source_pdf_path=str(pdf_path),
        pdf_sha256=None,
        extractor_version=EXTRACTOR_VERSION,
        created_at=utc_now_iso(),
        dependency_versions=versions,
        document_confidence=_document_confidence(notes, warnings),
        warnings=sorted(set(warnings)),
        features_preset=features,
        notes=notes,
        author_notes=author_notes,
        ordinality=ordinality,
    ).to_dict()
    _write_json_atomic(sidecar, payload)
    return {
        "pdf_path": str(pdf_path),
        "sidecar_path": str(sidecar),
        "status": "ok",
        "notes": len(notes),
        "author_notes": len(author_notes),
        "warnings": payload["warnings"],
        "ordinality_status": ordinality.status if ordinality else None,
        "parser_used": parser_used,
    }


def main() -> None:
    args = _parse_args()
    pdf_root = (ROOT / args.pdf_root).resolve()
    specific = [
        Path(path).resolve() if Path(path).is_absolute() else (ROOT / path).resolve()
        for path in args.pdf
    ]

    if specific:
        targets = [path for path in specific if path.suffix.lower() == ".pdf"]
    else:
        targets = _discover_pdfs(pdf_root, limit=args.limit)

    versions = dependency_versions()
    results: list[dict[str, Any]] = []
    for pdf_path in targets:
        try:
            result = _process_pdf(
                pdf_path,
                use_layout=bool(args.layout),
                features=args.features,
                out_suffix=args.out_suffix,
                overwrite=bool(args.overwrite),
                versions=versions,
            )
        except Exception as exc:
            result = {
                "pdf_path": str(pdf_path),
                "status": "failed",
                "error": str(exc),
            }
        results.append(result)

    summary = {
        "run_at": utc_now_iso(),
        "pdf_root": str(pdf_root),
        "targets": len(targets),
        "layout": bool(args.layout),
        "features": args.features,
        "out_suffix": args.out_suffix,
        "ok": sum(1 for row in results if row.get("status") == "ok"),
        "failed": sum(1 for row in results if row.get("status") == "failed"),
        "skipped_existing": sum(1 for row in results if row.get("status") == "skipped_existing"),
        "notes_total": sum(
            int(row.get("notes") or 0) for row in results if row.get("status") == "ok"
        ),
        "results": results,
    }

    if args.report_out:
        report_path = Path(args.report_out).resolve()
        _write_json_atomic(report_path, summary)

    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
