from __future__ import annotations

import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Iterable


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _normalize(text: str) -> str:
    s = (text or "").strip()
    # Normalize dashes: en-dash / em-dash → hyphen
    s = s.replace("\u2013", "-").replace("\u2014", "-")
    # Normalize smart quotes → straight quotes
    s = s.replace("\u2018", "'").replace("\u2019", "'")
    s = s.replace("\u201c", '"').replace("\u201d", '"')
    return re.sub(r"\s+", " ", s.lower())


def _safe_div(num: float, den: float) -> float:
    if den == 0:
        return 0.0
    return num / den


def _prf(tp: int, fp: int, fn: int) -> dict[str, float]:
    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f1 = _safe_div(2 * precision * recall, precision + recall) if (precision + recall) else 0.0
    return {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "tp": tp,
        "fp": fp,
        "fn": fn,
    }


def _rate(matches: int, total: int) -> dict[str, float | int]:
    return {
        "matches": matches,
        "total": total,
        "rate": round(_safe_div(matches, total), 4),
    }


def _load_gold(path: str) -> dict[str, dict[str, Any]]:
    payload = json.loads(open(path, encoding="utf-8").read())
    if isinstance(payload, dict) and "documents" in payload:
        rows = payload["documents"]
    elif isinstance(payload, list):
        rows = payload
    else:
        raise ValueError("Gold file must be a list or a dict with 'documents'")

    by_pdf: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        path_value = str(row.get("source_pdf_path") or "").strip()
        if not path_value:
            continue
        by_pdf[path_value] = row
    return by_pdf


def _load_prediction_for_pdf(pdf_path: str) -> dict[str, Any]:
    sidecar = f"{pdf_path}.footnotes.json"
    with open(sidecar, encoding="utf-8") as handle:
        return json.load(handle)


def _normalize_note_label(raw_label: str) -> str:
    label = str(raw_label or "").strip()
    if "__dup" in label:
        return label.split("__dup", 1)[0].strip()
    return label


def _iter_notes_payload(notes_payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(notes_payload, dict):
        for raw_label, note in notes_payload.items():
            if not isinstance(note, dict):
                continue
            row = dict(note)
            row.setdefault("label", _normalize_note_label(str(raw_label)))
            rows.append(row)
        return rows
    if isinstance(notes_payload, list):
        for note in notes_payload:
            if isinstance(note, dict):
                rows.append(dict(note))
    return rows


def _iter_ordered_notes(document: dict[str, Any]) -> list[dict[str, Any]]:
    notes = _iter_notes_payload(document.get("notes"))
    for index, note in enumerate(notes, start=1):
        note.setdefault("ordinal", index)
    return notes


def _iter_numbered_notes(document: dict[str, Any]) -> list[dict[str, Any]]:
    notes: list[dict[str, Any]] = []
    for note in _iter_ordered_notes(document):
        label = _normalize_note_label(str(note.get("label") or ""))
        if not label.isdigit():
            continue
        row = dict(note)
        row["label"] = label
        notes.append(row)
    return notes


def _note_key(note: dict[str, Any]) -> tuple[str, str, str]:
    return (
        _normalize_note_label(str(note.get("label") or "")),
        str(note.get("note_type") or ""),
        _normalize(str(note.get("text") or "")),
    )


def _label_key(note: dict[str, Any]) -> str:
    return _normalize_note_label(str(note.get("label") or ""))


def _citation_key(citation: dict[str, Any]) -> tuple[str, str]:
    return (
        _normalize(str(citation.get("text") or "")),
        str(citation.get("citation_type") or "other"),
    )


def _flatten_citations(notes: Iterable[dict[str, Any]]) -> set[tuple[str, str]]:
    flattened: set[tuple[str, str]] = set()
    for note in notes:
        for citation in note.get("citation_mentions", []) or []:
            if not isinstance(citation, dict):
                continue
            flattened.add(_citation_key(citation))
    return flattened


def _multiset_prf(gold_values: list[Any], pred_values: list[Any]) -> dict[str, float]:
    gold_counter = Counter(gold_values)
    pred_counter = Counter(pred_values)
    tp = sum((gold_counter & pred_counter).values())
    fp = sum((pred_counter - gold_counter).values())
    fn = sum((gold_counter - pred_counter).values())
    return _prf(tp, fp, fn)


def _failure_taxonomy_for_doc(
    gold_numbered: list[dict[str, Any]],
    pred_numbered: list[dict[str, Any]],
) -> list[str]:
    failures: list[str] = []

    gold_labels = [_label_key(note) for note in gold_numbered]
    pred_labels = [_label_key(note) for note in pred_numbered]

    gold_label_counter = Counter(gold_labels)
    pred_label_counter = Counter(pred_labels)
    if gold_label_counter - pred_label_counter:
        failures.append("missing_note")
    if pred_label_counter - gold_label_counter:
        failures.append("false_positive")
    if len(set(pred_labels)) < len(pred_labels):
        failures.append("duplicate_predicted_label")

    aligned = min(len(gold_numbered), len(pred_numbered))
    text_mismatch = False
    out_of_order = False
    for index in range(aligned):
        gold_note = gold_numbered[index]
        pred_note = pred_numbered[index]
        if _label_key(gold_note) != _label_key(pred_note):
            out_of_order = True
        elif _normalize(str(gold_note.get("text") or "")) != _normalize(
            str(pred_note.get("text") or "")
        ):
            text_mismatch = True

    if gold_labels != pred_labels:
        out_of_order = True
    if out_of_order:
        failures.append("out_of_order")
    if text_mismatch:
        failures.append("text_mismatch")

    return failures


def evaluate_predictions(gold_path: str) -> dict[str, Any]:
    gold = _load_gold(gold_path)

    boundary_tp = boundary_fp = boundary_fn = 0
    label_matches = label_total = 0
    note_type_matches = note_type_total = 0
    context_matches = context_total = 0
    citation_tp = citation_fp = citation_fn = 0
    citation_type_matches = citation_type_total = 0

    numbered_note_tp = numbered_note_fp = numbered_note_fn = 0
    numbered_label_tp = numbered_label_fp = numbered_label_fn = 0
    exact_ordered_doc_matches = 0
    label_sequence_doc_matches = 0

    missing_predictions: list[str] = []
    doc_reports: list[dict[str, Any]] = []
    failure_taxonomy: Counter[str] = Counter()

    for pdf_path, gold_doc in gold.items():
        try:
            pred_doc = _load_prediction_for_pdf(pdf_path)
        except FileNotFoundError:
            missing_predictions.append(pdf_path)
            continue

        gold_notes = _iter_ordered_notes(gold_doc)
        pred_notes = _iter_ordered_notes(pred_doc)

        gold_keys = Counter(_note_key(note) for note in gold_notes)
        pred_keys = Counter(_note_key(note) for note in pred_notes)

        boundary_tp += sum((gold_keys & pred_keys).values())
        boundary_fp += sum((pred_keys - gold_keys).values())
        boundary_fn += sum((gold_keys - pred_keys).values())

        max_len = max(len(gold_notes), len(pred_notes))
        for index in range(max_len):
            if index >= len(gold_notes) or index >= len(pred_notes):
                continue
            gold_note = gold_notes[index]
            pred_note = pred_notes[index]

            label_total += 1
            if _label_key(gold_note) == _label_key(pred_note):
                label_matches += 1

            note_type_total += 1
            if str(gold_note.get("note_type") or "") == str(pred_note.get("note_type") or ""):
                note_type_matches += 1

            context_total += 1
            if _normalize(str(gold_note.get("context_sentence") or "")) == _normalize(
                str(pred_note.get("context_sentence") or "")
            ):
                context_matches += 1

        gold_citations = _flatten_citations(gold_notes)
        pred_citations = _flatten_citations(pred_notes)

        citation_tp += len(gold_citations & pred_citations)
        citation_fp += len(pred_citations - gold_citations)
        citation_fn += len(gold_citations - pred_citations)

        for citation_text, citation_type in gold_citations & pred_citations:
            citation_type_total += 1
            if citation_type:
                citation_type_matches += 1

        gold_numbered = _iter_numbered_notes(gold_doc)
        pred_numbered = _iter_numbered_notes(pred_doc)

        gold_numbered_keys = Counter(_note_key(note) for note in gold_numbered)
        pred_numbered_keys = Counter(_note_key(note) for note in pred_numbered)
        numbered_note_tp += sum((gold_numbered_keys & pred_numbered_keys).values())
        numbered_note_fp += sum((pred_numbered_keys - gold_numbered_keys).values())
        numbered_note_fn += sum((gold_numbered_keys - pred_numbered_keys).values())

        gold_numbered_labels = [_label_key(note) for note in gold_numbered]
        pred_numbered_labels = [_label_key(note) for note in pred_numbered]
        gold_label_counter = Counter(gold_numbered_labels)
        pred_label_counter = Counter(pred_numbered_labels)
        numbered_label_tp += sum((gold_label_counter & pred_label_counter).values())
        numbered_label_fp += sum((pred_label_counter - gold_label_counter).values())
        numbered_label_fn += sum((gold_label_counter - pred_label_counter).values())

        exact_ordered_match = [
            (_label_key(note), _normalize(str(note.get("text") or ""))) for note in gold_numbered
        ] == [(_label_key(note), _normalize(str(note.get("text") or ""))) for note in pred_numbered]
        label_sequence_match = gold_numbered_labels == pred_numbered_labels
        if exact_ordered_match:
            exact_ordered_doc_matches += 1
        if label_sequence_match:
            label_sequence_doc_matches += 1

        doc_failures = _failure_taxonomy_for_doc(gold_numbered, pred_numbered)
        failure_taxonomy.update(doc_failures)
        doc_reports.append(
            {
                "source_pdf_path": pdf_path,
                "gold_numbered_notes": len(gold_numbered),
                "predicted_numbered_notes": len(pred_numbered),
                "exact_ordered_match": exact_ordered_match,
                "label_sequence_match": label_sequence_match,
                "note_exact": _multiset_prf(
                    [_note_key(note) for note in gold_numbered],
                    [_note_key(note) for note in pred_numbered],
                ),
                "label_match": _multiset_prf(gold_numbered_labels, pred_numbered_labels),
                "failure_taxonomy": doc_failures,
            }
        )

    boundary_metrics = _prf(boundary_tp, boundary_fp, boundary_fn)
    citation_metrics = _prf(citation_tp, citation_fp, citation_fn)

    evaluated_docs = len(doc_reports)
    return {
        "evaluated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "gold_documents": len(gold),
        "evaluated_documents": evaluated_docs,
        "missing_predictions": missing_predictions,
        "note_boundary": boundary_metrics,
        "label_accuracy": round(_safe_div(label_matches, label_total), 4),
        "note_type_accuracy": round(_safe_div(note_type_matches, note_type_total), 4),
        "context_sentence_accuracy": round(_safe_div(context_matches, context_total), 4),
        "citation_extraction": citation_metrics,
        "citation_type_accuracy": round(_safe_div(citation_type_matches, citation_type_total), 4),
        "numbered_footnotes": {
            "exact_note": _prf(numbered_note_tp, numbered_note_fp, numbered_note_fn),
            "label_match": _prf(numbered_label_tp, numbered_label_fp, numbered_label_fn),
            "exact_ordered_docs": _rate(exact_ordered_doc_matches, evaluated_docs),
            "label_sequence_docs": _rate(label_sequence_doc_matches, evaluated_docs),
            "failure_taxonomy": dict(sorted(failure_taxonomy.items())),
        },
        "documents": doc_reports,
    }


def write_evaluation_report(metrics: dict[str, Any], out_path: str = "") -> str:
    if not out_path:
        os.makedirs("artifacts/runs", exist_ok=True)
        out_path = os.path.join("artifacts/runs", f"footnote_eval_{_utc_stamp()}.json")
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2, sort_keys=True)
    return out_path
