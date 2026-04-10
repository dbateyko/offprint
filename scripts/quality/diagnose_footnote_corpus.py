#!/usr/bin/env python3
"""Corpus-wide footnote sidecar diagnostic report.

Reads all .footnotes.jsonl sidecars under a PDF root and produces aggregate
quality signals: ordinality health, retrieval completeness, cross-page stats,
junk-in-notes detection, and per-domain breakdowns.

Usage:
    python scripts/quality/diagnose_footnote_corpus.py --pdf-root artifacts/pdfs
    python scripts/quality/diagnose_footnote_corpus.py --pdf-root /path/to/pdfs --report-out report.json
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import re
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ── Junk detection patterns ─────────────────────────────────────────

# Running header: short ALL CAPS text (journal name, volume stamp)
RUNNING_HEADER_RE = re.compile(r"^[A-Z\s\.\,\-\&\:]{5,80}$")
# Page number artifact
PAGE_NUMBER_RE = re.compile(r"^\s*\d{1,4}\s*$")
# Volume/issue stamp
VOL_ISSUE_RE = re.compile(r"^(Vol\.|Volume|No\.|Issue|Iss\.)\s*\d", re.IGNORECASE)
# Very short note (not a valid legal cite)
SHORT_CITE_RE = re.compile(r"(Id\.|Ibid\.|See |Cf\.|Supra|Infra)", re.IGNORECASE)


@dataclass
class NoteStats:
    label: str = ""
    text: str = ""
    text_len: int = 0
    page_start: int = 0
    page_end: int = 0
    confidence: float = 0.0
    flags: list[str] = field(default_factory=list)
    is_cross_page: bool = False


@dataclass
class DocStats:
    pdf_path: str = ""
    domain: str = ""
    note_count: int = 0
    ordinality_status: str = ""
    gaps: list[int] = field(default_factory=list)
    gap_count: int = 0
    expected_range: tuple[int, int] = (0, 0)
    document_confidence: float = 0.0
    warnings: list[str] = field(default_factory=list)
    cross_page_notes: int = 0
    missing_context_count: int = 0
    junk_signals: list[dict[str, str]] = field(default_factory=list)
    note_lengths: list[int] = field(default_factory=list)
    page_count_approx: int = 0  # max page_end across notes


def _extract_domain(pdf_path: str) -> str:
    """Extract domain from PDF path like .../pdfs/domain.edu/file.pdf."""
    parts = Path(pdf_path).parts
    for i, part in enumerate(parts):
        if part == "pdfs" and i + 1 < len(parts):
            return parts[i + 1]
    # Fallback: parent directory name
    return Path(pdf_path).parent.name


def _detect_junk_notes(notes: list[NoteStats]) -> list[dict[str, str]]:
    """Detect junk signals in note text."""
    signals: list[dict[str, str]] = []

    # Collect text for duplicate detection
    text_counts: dict[str, int] = collections.Counter(n.text.strip() for n in notes if n.text.strip())

    for note in notes:
        text = note.text.strip()
        if not text:
            signals.append({"label": note.label, "type": "empty_note", "text": ""})
            continue

        if PAGE_NUMBER_RE.match(text):
            signals.append({"label": note.label, "type": "page_number_artifact", "text": text})
            continue

        if VOL_ISSUE_RE.match(text):
            signals.append({"label": note.label, "type": "vol_issue_stamp", "text": text})
            continue

        if RUNNING_HEADER_RE.match(text) and len(text) < 80:
            signals.append({"label": note.label, "type": "running_header_suspect", "text": text})

        if len(text) < 15 and not SHORT_CITE_RE.search(text):
            signals.append({"label": note.label, "type": "suspiciously_short", "text": text})

        if text_counts.get(text, 0) >= 3:
            signals.append({"label": note.label, "type": "duplicate_text", "text": text[:80]})

    return signals


def parse_sidecar(sidecar_path: str) -> DocStats | None:
    """Parse a .footnotes.jsonl sidecar into DocStats."""
    try:
        lines = Path(sidecar_path).read_text(encoding="utf-8").splitlines()
    except Exception:
        return None

    if not lines:
        return None

    doc = DocStats()
    notes: list[NoteStats] = []

    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue

        row_type = row.get("type", "")

        if row_type == "metadata":
            doc.pdf_path = row.get("source_pdf_path", "")
            doc.domain = _extract_domain(doc.pdf_path)
            doc.document_confidence = row.get("document_confidence", 0.0)
            doc.warnings = row.get("warnings", [])
            ord_data = row.get("ordinality")
            if ord_data:
                doc.ordinality_status = ord_data.get("status", "unknown")
                doc.gaps = ord_data.get("gaps", [])
                doc.gap_count = len(doc.gaps)
                er = ord_data.get("expected_range", [0, 0])
                doc.expected_range = (er[0], er[1]) if len(er) == 2 else (0, 0)

        elif row_type == "footnote":
            ns = NoteStats(
                label=str(row.get("label", "")),
                text=row.get("text", ""),
                text_len=len(row.get("text", "")),
                page_start=row.get("page_start", 0),
                page_end=row.get("page_end", 0),
                confidence=row.get("_qc", {}).get("confidence", 0.0),
                flags=row.get("_qc", {}).get("flags", []),
            )
            ns.is_cross_page = ns.page_start != ns.page_end
            notes.append(ns)

    doc.note_count = len(notes)
    doc.cross_page_notes = sum(1 for n in notes if n.is_cross_page)
    doc.missing_context_count = sum(1 for n in notes if "missing_context" in n.flags)
    doc.note_lengths = [n.text_len for n in notes]
    doc.page_count_approx = max((n.page_end for n in notes), default=0)
    doc.junk_signals = _detect_junk_notes(notes)

    return doc


def _find_sidecars(pdf_root: str) -> list[str]:
    """Find all .footnotes.jsonl files under pdf_root."""
    sidecars = []
    for root, _dirs, files in os.walk(pdf_root):
        for f in files:
            if f.endswith(".footnotes.jsonl"):
                sidecars.append(os.path.join(root, f))
    sidecars.sort()
    return sidecars


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_v = sorted(values)
    k = (len(sorted_v) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_v):
        return sorted_v[f]
    return sorted_v[f] + (k - f) * (sorted_v[c] - sorted_v[f])


def build_report(pdf_root: str) -> dict[str, Any]:
    """Build a corpus-wide diagnostic report from all sidecars."""
    sidecars = _find_sidecars(pdf_root)
    total_pdfs = sum(
        1
        for root, _dirs, files in os.walk(pdf_root)
        for f in files
        if f.lower().endswith(".pdf")
    )

    docs: list[DocStats] = []
    parse_errors = 0
    for sc in sidecars:
        ds = parse_sidecar(sc)
        if ds:
            docs.append(ds)
        else:
            parse_errors += 1

    # ── Ordinality health ────────────────────────────────────────
    ord_counts = collections.Counter(d.ordinality_status for d in docs)
    ord_by_domain: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for d in docs:
        ord_by_domain[d.domain][d.ordinality_status] += 1

    gap_counts = [d.gap_count for d in docs if d.gap_count > 0]
    gap_labels_flat = []
    for d in docs:
        gap_labels_flat.extend(d.gaps)

    # ── Retrieval completeness ───────────────────────────────────
    notes_per_doc = [d.note_count for d in docs]
    notes_per_page = []
    for d in docs:
        if d.page_count_approx > 0:
            notes_per_page.append(d.note_count / d.page_count_approx)

    zero_note_docs = [d.pdf_path for d in docs if d.note_count == 0]
    low_note_docs = [
        d.pdf_path
        for d in docs
        if d.note_count > 0 and d.page_count_approx > 10 and (d.note_count / d.page_count_approx) < 0.5
    ]

    # ── Cross-page notes ────────────────────────────────────────
    cross_page_total = sum(d.cross_page_notes for d in docs)
    docs_with_cross_page = sum(1 for d in docs if d.cross_page_notes > 0)

    # ── Context attachment ───────────────────────────────────────
    total_notes = sum(d.note_count for d in docs)
    total_missing_context = sum(d.missing_context_count for d in docs)

    # ── Confidence distribution ──────────────────────────────────
    confidences = [d.document_confidence for d in docs]

    # ── Junk signals ────────────────────────────────────────────
    junk_type_counts: collections.Counter = collections.Counter()
    docs_with_junk = 0
    junk_examples: list[dict[str, Any]] = []
    for d in docs:
        if d.junk_signals:
            docs_with_junk += 1
            for sig in d.junk_signals:
                junk_type_counts[sig["type"]] += 1
            if len(junk_examples) < 20:
                junk_examples.append({
                    "pdf_path": d.pdf_path,
                    "domain": d.domain,
                    "signals": d.junk_signals[:5],
                })

    # ── Warning frequency ────────────────────────────────────────
    warning_counts: collections.Counter = collections.Counter()
    for d in docs:
        for w in d.warnings:
            # Normalize parameterized warnings
            normalized = re.sub(r"=\d+[\d,]*", "=N", w)
            normalized = re.sub(r"pages=[\d,]+", "pages=...", normalized)
            warning_counts[normalized] += 1

    # ── Note length distribution ────────────────────────────────
    all_note_lengths = []
    for d in docs:
        all_note_lengths.extend(d.note_lengths)

    # ── Per-domain summary ──────────────────────────────────────
    domain_docs: dict[str, list[DocStats]] = collections.defaultdict(list)
    for d in docs:
        domain_docs[d.domain].append(d)

    domain_summaries = {}
    for domain, ddocs in sorted(domain_docs.items()):
        d_notes = [d.note_count for d in ddocs]
        d_ord = collections.Counter(d.ordinality_status for d in ddocs)
        d_junk = sum(1 for d in ddocs if d.junk_signals)
        d_conf = [d.document_confidence for d in ddocs]
        domain_summaries[domain] = {
            "sidecar_count": len(ddocs),
            "ordinality": dict(d_ord),
            "notes_median": round(statistics.median(d_notes), 1) if d_notes else 0,
            "notes_total": sum(d_notes),
            "confidence_median": round(statistics.median(d_conf), 3) if d_conf else 0,
            "docs_with_junk": d_junk,
            "invalid_rate": round(d_ord.get("invalid", 0) / len(ddocs), 3) if ddocs else 0,
        }

    # ── Assemble report ─────────────────────────────────────────
    report: dict[str, Any] = {
        "corpus_overview": {
            "total_pdfs": total_pdfs,
            "total_sidecars": len(docs),
            "coverage_pct": round(len(docs) / total_pdfs * 100, 1) if total_pdfs else 0,
            "parse_errors": parse_errors,
        },
        "ordinality_health": {
            "distribution": dict(ord_counts),
            "valid_pct": round(ord_counts.get("valid", 0) / len(docs) * 100, 1) if docs else 0,
            "valid_with_gaps_pct": round(
                ord_counts.get("valid_with_gaps", 0) / len(docs) * 100, 1
            )
            if docs
            else 0,
            "invalid_pct": round(ord_counts.get("invalid", 0) / len(docs) * 100, 1)
            if docs
            else 0,
            "gap_count_stats": {
                "docs_with_gaps": len(gap_counts),
                "median_gaps": round(statistics.median(gap_counts), 1) if gap_counts else 0,
                "p95_gaps": round(_percentile(gap_counts, 95), 1) if gap_counts else 0,
                "max_gaps": max(gap_counts) if gap_counts else 0,
            },
        },
        "retrieval_completeness": {
            "notes_per_doc": {
                "median": round(statistics.median(notes_per_doc), 1) if notes_per_doc else 0,
                "mean": round(statistics.mean(notes_per_doc), 1) if notes_per_doc else 0,
                "p5": round(_percentile(notes_per_doc, 5), 1) if notes_per_doc else 0,
                "p95": round(_percentile(notes_per_doc, 95), 1) if notes_per_doc else 0,
            },
            "notes_per_page": {
                "median": round(statistics.median(notes_per_page), 2) if notes_per_page else 0,
                "p5": round(_percentile(notes_per_page, 5), 2) if notes_per_page else 0,
                "p95": round(_percentile(notes_per_page, 95), 2) if notes_per_page else 0,
            },
            "zero_note_docs": len(zero_note_docs),
            "low_density_docs": len(low_note_docs),
            "total_notes": total_notes,
        },
        "cross_page_notes": {
            "total_cross_page": cross_page_total,
            "docs_with_cross_page": docs_with_cross_page,
            "cross_page_pct": round(cross_page_total / total_notes * 100, 1) if total_notes else 0,
        },
        "context_attachment": {
            "total_notes": total_notes,
            "missing_context": total_missing_context,
            "attachment_rate_pct": round(
                (total_notes - total_missing_context) / total_notes * 100, 1
            )
            if total_notes
            else 0,
        },
        "confidence_distribution": {
            "median": round(statistics.median(confidences), 3) if confidences else 0,
            "p10": round(_percentile(confidences, 10), 3) if confidences else 0,
            "p90": round(_percentile(confidences, 90), 3) if confidences else 0,
        },
        "junk_detection": {
            "docs_with_junk_signals": docs_with_junk,
            "junk_pct": round(docs_with_junk / len(docs) * 100, 1) if docs else 0,
            "by_type": dict(junk_type_counts.most_common()),
            "examples": junk_examples[:10],
        },
        "note_length_distribution": {
            "median": round(statistics.median(all_note_lengths), 0) if all_note_lengths else 0,
            "p5": round(_percentile(all_note_lengths, 5), 0) if all_note_lengths else 0,
            "p95": round(_percentile(all_note_lengths, 95), 0) if all_note_lengths else 0,
            "max": max(all_note_lengths) if all_note_lengths else 0,
        },
        "warning_frequency": dict(warning_counts.most_common(20)),
        "domains": domain_summaries,
    }

    # ── Worst domains by invalid rate ────────────────────────────
    worst_domains = sorted(
        [
            (domain, s["invalid_rate"], s["sidecar_count"])
            for domain, s in domain_summaries.items()
            if s["sidecar_count"] >= 3  # need enough data
        ],
        key=lambda x: (-x[1], -x[2]),
    )[:15]
    report["worst_domains_by_invalid_rate"] = [
        {"domain": d, "invalid_rate": r, "sidecars": c} for d, r, c in worst_domains
    ]

    return report


def _print_summary(report: dict[str, Any]) -> None:
    """Print human-readable summary to stdout."""
    ov = report["corpus_overview"]
    oh = report["ordinality_health"]
    rc = report["retrieval_completeness"]
    cp = report["cross_page_notes"]
    ca = report["context_attachment"]
    jd = report["junk_detection"]

    print(f"\n{'='*60}")
    print("FOOTNOTE CORPUS DIAGNOSTIC REPORT")
    print(f"{'='*60}\n")

    print(f"Coverage: {ov['total_sidecars']:,} sidecars / {ov['total_pdfs']:,} PDFs ({ov['coverage_pct']}%)")
    print(f"Total notes extracted: {rc['total_notes']:,}")
    print()

    print("ORDINALITY HEALTH")
    print(f"  Valid:          {oh['distribution'].get('valid', 0):>6,}  ({oh['valid_pct']}%)")
    print(f"  Valid w/ gaps:  {oh['distribution'].get('valid_with_gaps', 0):>6,}  ({oh['valid_with_gaps_pct']}%)")
    print(f"  Invalid:        {oh['distribution'].get('invalid', 0):>6,}  ({oh['invalid_pct']}%)")
    gs = oh["gap_count_stats"]
    if gs["docs_with_gaps"]:
        print(f"  Gap stats: median={gs['median_gaps']}, p95={gs['p95_gaps']}, max={gs['max_gaps']}")
    print()

    print("RETRIEVAL COMPLETENESS")
    npd = rc["notes_per_doc"]
    npp = rc["notes_per_page"]
    print(f"  Notes/doc: median={npd['median']}, p5={npd['p5']}, p95={npd['p95']}")
    print(f"  Notes/page: median={npp['median']}, p5={npp['p5']}, p95={npp['p95']}")
    print(f"  Zero-note docs: {rc['zero_note_docs']}")
    print(f"  Low-density docs (<0.5 notes/page, >10pp): {rc['low_density_docs']}")
    print()

    print("CROSS-PAGE NOTES")
    print(f"  {cp['total_cross_page']:,} cross-page notes ({cp['cross_page_pct']}%) across {cp['docs_with_cross_page']} docs")
    print()

    print("CONTEXT ATTACHMENT")
    print(f"  Attachment rate: {ca['attachment_rate_pct']}%  ({ca['missing_context']:,} missing)")
    print()

    print("JUNK DETECTION")
    print(f"  Docs with junk signals: {jd['docs_with_junk_signals']} ({jd['junk_pct']}%)")
    for jtype, count in sorted(jd["by_type"].items(), key=lambda x: -x[1]):
        print(f"    {jtype}: {count}")
    print()

    worst = report.get("worst_domains_by_invalid_rate", [])
    if worst:
        print("WORST DOMAINS (by invalid ordinality rate, min 3 sidecars)")
        for entry in worst[:10]:
            print(f"  {entry['domain']}: {entry['invalid_rate']*100:.0f}% invalid ({entry['sidecars']} sidecars)")
    print()

    wf = report.get("warning_frequency", {})
    if wf:
        print("TOP WARNINGS")
        for w, count in list(wf.items())[:10]:
            print(f"  {count:>5,}  {w}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Corpus-wide footnote sidecar diagnostic report")
    parser.add_argument(
        "--pdf-root",
        required=True,
        help="Root directory containing PDFs and .footnotes.jsonl sidecars",
    )
    parser.add_argument(
        "--report-out",
        default="",
        help="Optional JSON output path for full report",
    )
    parser.add_argument(
        "--json-only",
        action="store_true",
        help="Suppress terminal summary, emit only JSON",
    )
    args = parser.parse_args()

    report = build_report(args.pdf_root)

    if not args.json_only:
        _print_summary(report)

    if args.report_out:
        out_path = Path(args.report_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print(f"Full report written to: {out_path}")
    elif args.json_only:
        print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
