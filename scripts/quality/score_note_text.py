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


def score_gold(gold_path: Path, predictions_root: Path | None = None) -> dict:
    gold = _load_gold(str(gold_path))
    totals = [0, 0, 0]
    domains: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    documents = []
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
        doc_counts = [0, 0, 0]
        for label in gold_notes.keys() | pred_notes.keys():
            counts = text_prf(
                str(gold_notes.get(label, {}).get("text") or ""),
                str(pred_notes.get(label, {}).get("text") or ""),
            )
            for index, count in enumerate(counts):
                doc_counts[index] += count
                totals[index] += count
                domains[domain][index] += count
        documents.append({"source_pdf_path": pdf_path, "domain": domain, **_metrics(*doc_counts)})
    return {
        "normalization": "lowercase; collapse whitespace; remove soft hyphens; join word-hyphen line breaks; normalize dashes/quotes",
        "summary": _metrics(*totals),
        "domains": {domain: _metrics(*counts) for domain, counts in sorted(domains.items())},
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
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
