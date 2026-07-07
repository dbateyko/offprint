#!/usr/bin/env python3
"""Score predicted sidecars against evaluation.py-compatible HTML gold."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.pdf_footnotes.evaluation import _iter_numbered_notes, _load_gold


def normalize_note_text(text: str) -> str:
    """Normalize whitespace and line-break hyphenation before token scoring."""
    text = (text or "").replace("\u00ad", "")
    # HTML footnotes carry a backlink glyph absent from the PDF; without this
    # every gold note contributes one guaranteed false-negative token.
    text = text.replace("\u2191", "")
    text = re.sub(r"(?<=\w)-\s+(?=[a-z])", "", text)
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = text.replace("\u2018", "'").replace("\u2019", "'")
    return " ".join(text.lower().split())


def _tokens(text: str) -> list[str]:
    return re.findall(r"\w+(?:['.-]\w+)*|[^\w\s]", normalize_note_text(text))


def text_prf(gold_text: str, predicted_text: str) -> tuple[int, int, int]:
    gold = Counter(_tokens(gold_text))
    predicted = Counter(_tokens(predicted_text))
    return (
        sum((gold & predicted).values()),
        sum((predicted - gold).values()),
        sum((gold - predicted).values()),
    )


def _metrics(tp: int, fp: int, fn: int) -> dict[str, float | int]:
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"precision": precision, "recall": recall, "f1": f1, "tp": tp, "fp": fp, "fn": fn}


def _pair_qa_reasons(gold_n: int, pred_n: int, label_overlap: float) -> list[str]:
    """Structural checks that an HTML/PDF pair is the same article.

    Mismatched pairs (e.g. a one-pager PDF paired with a full article's HTML)
    score ~0 and would dominate the aggregate; these gates are structural
    (note counts and label-set overlap), NOT score-based, so excluding a pair
    cannot inflate the metric circularly. A same-length wrong-article pair can
    still slip through — triage per-document scores for that.
    """
    reasons = []
    if gold_n < 5:
        reasons.append(f"gold_notes<5 ({gold_n})")
    if pred_n < 1:
        reasons.append("no predicted notes")
    elif gold_n:
        overlap_min = 0.5
        if label_overlap < overlap_min:
            reasons.append(f"label_overlap {label_overlap:.2f}<{overlap_min}")
        ratio = pred_n / gold_n
        if not (0.5 <= ratio <= 2.0):
            reasons.append(f"note_count_ratio {ratio:.2f} outside [0.5,2.0]")
    return reasons


def score_gold(gold_path: Path, predictions_root: Path | None = None) -> dict:
    gold = _load_gold(str(gold_path))
    totals = [0, 0, 0]
    totals_unfiltered = [0, 0, 0]
    domains: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    documents = []
    excluded_pairs = []
    for pdf_path, gold_doc in gold.items():
        gold_meta = gold_doc.get("html_gold") or {}
        domain = str(
            gold_meta.get("domain")
            or urlparse(str(gold_meta.get("html_url") or "")).hostname
            or "unknown"
        )
        prediction_pdf = Path(pdf_path)
        if predictions_root is not None:
            prediction_pdf = predictions_root / domain / Path(pdf_path).name
            if not prediction_pdf.exists():
                prediction_pdf = predictions_root / Path(pdf_path).name
        sidecar = Path(f"{prediction_pdf}.footnotes.json")
        if not sidecar.exists():
            documents.append({"source_pdf_path": pdf_path, "domain": domain, "missing": True})
            continue
        predicted_doc = json.loads(sidecar.read_text(encoding="utf-8"))
        gold_notes = {note["label"]: note for note in _iter_numbered_notes(gold_doc)}
        pred_notes = {note["label"]: note for note in _iter_numbered_notes(predicted_doc)}
        gold_n, pred_n = len(gold_notes), len(pred_notes)
        label_overlap = (len(gold_notes.keys() & pred_notes.keys()) / gold_n) if gold_n else 0.0
        qa_reasons = _pair_qa_reasons(gold_n, pred_n, label_overlap)
        pair_ok = not qa_reasons
        doc_counts = [0, 0, 0]
        for label in gold_notes.keys() | pred_notes.keys():
            counts = text_prf(
                str(gold_notes.get(label, {}).get("text") or ""),
                str(pred_notes.get(label, {}).get("text") or ""),
            )
            for index, count in enumerate(counts):
                doc_counts[index] += count
                totals_unfiltered[index] += count
                if pair_ok:
                    totals[index] += count
                    domains[domain][index] += count
        entry = {
            "source_pdf_path": pdf_path,
            "domain": domain,
            **_metrics(*doc_counts),
            "gold_note_count": gold_n,
            "predicted_note_count": pred_n,
            "label_overlap": round(label_overlap, 3),
            "pair_qa": "pass" if pair_ok else "fail",
        }
        if not pair_ok:
            entry["pair_qa_reasons"] = qa_reasons
            excluded_pairs.append(
                {"source_pdf_path": pdf_path, "domain": domain, "reasons": qa_reasons}
            )
        documents.append(entry)
    n_scored = sum(1 for d in documents if not d.get("missing"))
    return {
        "normalization": "lowercase; collapse whitespace; remove soft hyphens; "
        "strip HTML backlink arrows; join word-hyphen line breaks; "
        "normalize dashes/quotes",
        "pair_qa": {
            "rule": "gold_notes>=5, predicted_notes>=1, label_overlap>=0.5, "
            "note_count_ratio in [0.5,2.0]",
            "passed": n_scored - len(excluded_pairs),
            "excluded": len(excluded_pairs),
        },
        "summary": _metrics(*totals),
        "summary_unfiltered": _metrics(*totals_unfiltered),
        "domains": {domain: _metrics(*counts) for domain, counts in sorted(domains.items())},
        "excluded_pairs": excluded_pairs,
        "documents": documents,
    }


def extract_gold_pdfs(gold_path: Path, predictions_root: Path, workers: int) -> dict:
    """Copy gold PDFs to an isolated tree and run the CPU-only pipeline there."""
    gold = _load_gold(str(gold_path))
    for pdf_path, gold_doc in gold.items():
        source = Path(pdf_path)
        if not source.exists():
            raise FileNotFoundError(source)
        metadata = gold_doc.get("html_gold") or {}
        domain = str(metadata.get("domain") or "unknown")
        destination = predictions_root / domain / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists() or destination.stat().st_size != source.stat().st_size:
            shutil.copy2(source, destination)

    from offprint.pdf_footnotes.pipeline import BatchConfig, run_batch

    return run_batch(
        BatchConfig(
            pdf_root=str(predictions_root),
            workers=max(1, workers),
            classifier_workers=max(1, workers),
            ocr_workers=1,
            ocr_mode="off",
            text_parser_mode="footnote_optimized",
            overwrite=True,
            respect_qc_exclusions=False,
            doc_policy="all",
            emit_doctype_manifest=False,
            emit_ocr_review_manifest=False,
            text_cache_enabled=False,
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", type=Path, required=True)
    parser.add_argument("--predictions-root", type=Path)
    parser.add_argument(
        "--extract",
        action="store_true",
        help="Run CPU-only extraction in predictions-root before scoring",
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    if args.extract:
        if args.predictions_root is None:
            parser.error("--extract requires --predictions-root")
        extract_gold_pdfs(args.gold, args.predictions_root, args.workers)
    report = score_gold(args.gold, args.predictions_root)
    print("domain\tprecision\trecall\tf1")
    for domain, metrics in report["domains"].items():
        print(f"{domain}\t{metrics['precision']:.4f}\t{metrics['recall']:.4f}\t{metrics['f1']:.4f}")
    summary = report["summary"]
    print(f"ALL\t{summary['precision']:.4f}\t{summary['recall']:.4f}\t{summary['f1']:.4f}")
    qa = report["pair_qa"]
    unfiltered = report["summary_unfiltered"]
    print(
        f"(pair QA: {qa['passed']} scored, {qa['excluded']} excluded; "
        f"unfiltered F1 {unfiltered['f1']:.4f})"
    )
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
