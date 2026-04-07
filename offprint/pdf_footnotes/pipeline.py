from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pypdf import PdfReader, PdfWriter
from tqdm import tqdm

from offprint.adapters.utils import compute_pdf_sha256_and_size

from .citation_classify import enrich_note_features
from .context_link import attach_context_batch
from .doc_policy import (
    DocDecision,
    classify_pdf,
    collect_signals,
    default_rules_path,
    infer_domain,
    infer_platform_family,
    load_rules,
    read_first_page_overview,
)
from .note_segment import (
    REAL_FOOTNOTE_START_RE,
    _clean_line_text,
    _likely_continuation,
    _validated_marker_match,
    segment_document_notes_extended,
    validate_ordinality,
)
from .ocr_worker import OCRWorkerPool
from .qc_filter import latest_qc_manifest, load_excluded_paths
from .schema import NoteChunk, NoteRecord, SidecarDocument, dependency_versions, utc_now_iso
from .text_cache import TextExtractionCache
from .text_extract import extract_document_text, ocr_fallback_recommended

EXTRACTOR_VERSION = "0.2.0"


@dataclass(frozen=True)
class FootnoteProfile:
    gap_tolerance: int = 2
    strict_label_filter: bool = True
    prefer_ocr: bool = False
    rescue_backtrack_px: float = 0.0
    dedupe_numeric_labels: bool = True
    enable_secondary_pdfplumber_fallback: bool = False
    secondary_gap_trigger: int = 4
    harden_body_rescue: bool = False
    enable_tail_outlier_prune: bool = True
    enable_contiguous_core_recovery: bool = True


@dataclass
class BatchConfig:
    pdf_root: str
    features: str = "legal"
    workers: int = 6
    classifier_workers: int = 6
    ocr_workers: int = 2
    ocr_mode: str = "fallback"
    ocr_backend: str = "glmocr"
    text_parser_mode: str = "footnote_optimized"
    include_pdf_sha256: bool = False
    report_detail: str = "summary"
    heartbeat_every: int = 500
    overwrite: bool = False
    limit: int = 0
    report_out: str | None = None
    qc_exclusion_manifest: str | None = None
    respect_qc_exclusions: bool = True
    doc_policy: str = "article_only"
    doc_rules_path: str | None = None
    emit_doctype_manifest: bool = True
    doctype_manifest_out: str | None = None
    emit_ocr_review_manifest: bool = True
    ocr_review_manifest_out: str | None = None
    text_cache_enabled: bool = True
    shard_count: int = 1
    shard_index: int = 0
    ordinality_patch: bool = True
    ordinality_patch_max_pages: int = 20
    ordinality_patch_expand: int = 1
    ordinality_patch_ocr_escalation_passes: int = 1
    emit_segments: bool = False


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _bool_to_status(value: bool) -> str:
    return "ok" if value else "failed"


def _sidecar_path(pdf_path: str) -> str:
    return f"{pdf_path}.footnotes.json"


def _write_json_atomic(
    path: str,
    payload: dict[str, Any],
    *,
    sort_keys: bool = True,
) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=sort_keys)
    os.replace(tmp, path)


def _write_jsonl_atomic(path: str, rows: list[dict[str, Any]]) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    os.replace(tmp, path)


def _write_jsonl_sidecar_atomic(
    path: str, payload: dict[str, Any], *, emit_segments: bool = False
) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        # Line 1: document-level metadata (no notes/author_notes)
        meta = {k: v for k, v in payload.items() if k not in ("notes", "author_notes")}
        meta["type"] = "metadata"
        handle.write(json.dumps(meta, sort_keys=True) + "\n")

        # Author notes (*, †, ‡ markers)
        author_notes = payload.get("author_notes") or []
        for an in author_notes:
            an_row = dict(an)
            an_row["type"] = "author_note"
            handle.write(json.dumps(an_row, sort_keys=True) + "\n")

        # Numbered footnotes — one record per label, segments stripped unless requested
        notes = payload.get("notes") or {}
        items = notes.items() if isinstance(notes, dict) else enumerate(notes)
        for label, note_data in items:
            note_row = dict(note_data)
            note_row["type"] = "footnote"
            note_row["label"] = str(label)
            if not emit_segments:
                note_row.pop("segments", None)
            handle.write(json.dumps(note_row, sort_keys=True) + "\n")

    os.replace(tmp, path)


def _hyphenation_cleanup(text: str) -> str:
    # Join line-break hyphenation artifacts (e.g. "Sa- cramento" -> "Sacramento").
    return re.sub(r"(?<=\w)-\s+(?=[a-z])", "", text or "")


def _normalize_sparse_hundred_labels_payload(payload: dict[str, Any]) -> bool:
    notes_payload = payload.get("notes")
    if not isinstance(notes_payload, dict):
        return False

    items = [(str(label), note) for label, note in notes_payload.items() if isinstance(note, dict)]
    if not items:
        return False

    numeric_values = [
        int(label.split("__dup", 1)[0])
        for label, _ in items
        if label.split("__dup", 1)[0].isdigit()
    ]
    if len(numeric_values) > 8 or 1 not in numeric_values:
        return False
    if not any(100 <= value <= 130 for value in numeric_values):
        return False

    changed = False
    rebuilt: dict[str, Any] = {}
    for raw_label, note in items:
        label = raw_label.split("__dup", 1)[0]
        mapped_label = label
        if label.isdigit():
            numeric_label = int(label)
            if 100 <= numeric_label <= 130:
                collapsed = numeric_label - 100
                if collapsed <= 0:
                    changed = True
                    continue
                mapped_label = str(collapsed)
                changed = True
        if mapped_label in rebuilt:
            changed = True
            continue
        note_copy = dict(note)
        if "text" in note_copy:
            cleaned_text = _hyphenation_cleanup(str(note_copy.get("text") or ""))
            if cleaned_text != str(note_copy.get("text") or ""):
                changed = True
            note_copy["text"] = cleaned_text
        rebuilt[mapped_label] = note_copy

    if not changed:
        return False
    payload["notes"] = rebuilt
    warnings = payload.get("warnings")
    if not isinstance(warnings, list):
        warnings = []
    if "sparse_hundred_label_normalized" not in warnings:
        warnings.append("sparse_hundred_label_normalized")
    payload["warnings"] = warnings
    return True


def _numeric_note_count(notes_payload: Any) -> int:
    if not isinstance(notes_payload, dict):
        return 0
    count = 0
    for raw_label in notes_payload.keys():
        label = str(raw_label).split("__dup", 1)[0]
        if label.isdigit():
            count += 1
    return count


def _load_pdftotext_candidate_payload(pdf_path: str, *, features: str) -> dict[str, Any] | None:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "extract_footnotes_from_docling.py"
    if not script_path.exists():
        return None

    suffix = ".footnotes.pdftotext.candidate.json"
    candidate_sidecar = f"{pdf_path}{suffix}"
    cmd = [
        sys.executable,
        str(script_path),
        "--pdf",
        pdf_path,
        "--features",
        features,
        "--out-suffix",
        suffix,
        "--overwrite",
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except Exception:
        return None

    try:
        with open(candidate_sidecar, encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return None
    finally:
        try:
            os.remove(candidate_sidecar)
        except OSError:
            pass

    if not isinstance(payload, dict):
        return None
    return payload


def _path_in_shard(pdf_path: str, shard_count: int, shard_index: int) -> bool:
    if shard_count <= 1:
        return True
    digest = hashlib.sha1(os.path.abspath(pdf_path).encode("utf-8")).hexdigest()
    bucket = int(digest[:16], 16) % shard_count
    return bucket == shard_index


def _discover_pdfs(
    pdf_root: str,
    limit: int = 0,
    *,
    shard_count: int = 1,
    shard_index: int = 0,
) -> list[str]:
    discovered: list[str] = []
    for root, _dirs, files in os.walk(pdf_root):
        for filename in sorted(files):
            if not filename.lower().endswith(".pdf"):
                continue
            pdf_path = os.path.join(root, filename)
            if _path_in_shard(pdf_path, shard_count=shard_count, shard_index=shard_index):
                discovered.append(pdf_path)
    discovered.sort()
    if limit and limit > 0:
        return discovered[:limit]
    return discovered


def _format_eta(seconds: float | None) -> str:
    if seconds is None or seconds <= 0:
        return "n/a"
    total_seconds = int(round(seconds))
    hours, rem = divmod(total_seconds, 3600)
    minutes, sec = divmod(rem, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{sec:02d}"
    return f"{minutes:02d}:{sec:02d}"


def _emit_heartbeat(stage: str, done: int, total: int | None, started_at: float) -> None:
    elapsed = max(0.001, time.monotonic() - started_at)
    rate = done / elapsed
    if total is not None and total > 0:
        remaining = max(0, total - done)
        eta_seconds = (remaining / rate) if rate > 0 else None
        percent_text = f"{(100.0 * done / total):.2f}%"
        total_text = str(total)
    else:
        eta_seconds = None
        percent_text = "n/a"
        total_text = "?"
    print(
        (
            f"[footnotes][{stage}] {done}/{total_text} ({percent_text}) "
            f"rate={rate:.2f}/s eta={_format_eta(eta_seconds)}"
        ),
        file=sys.stderr,
        flush=True,
    )


def _avg_note_length(notes: list[Any]) -> float:
    if not notes:
        return 0.0
    total = 0
    for note in notes:
        total += len((getattr(note, "text", "") or "").strip())
    return total / max(1, len(notes))


def _pypdf_fast_path_accepted(document: Any, notes: list[Any]) -> bool:
    if (getattr(document, "total_text_chars", 0) or 0) < 1200:
        return False
    if len(notes) < 3:
        return False
    if _avg_note_length(notes) < 40.0:
        return False
    return True


def _extract_document_with_mode(
    pdf_path: str,
    parser_mode: str,
    *,
    note_cutoff_ratio: float | None = None,
) -> Any:
    try:
        return extract_document_text(
            pdf_path,
            parser_mode=parser_mode,
            note_cutoff_ratio=note_cutoff_ratio,
        )
    except TypeError as exc:
        # Backward compatibility for tests/mocks that still patch one-arg signature.
        try:
            return extract_document_text(pdf_path, parser_mode=parser_mode)
        except TypeError:
            if "parser_mode" not in str(exc):
                raise
            return extract_document_text(pdf_path)


def _resolve_note_cutoff_ratio(
    *,
    decision: DocDecision | None,
    rules: dict[str, Any],
) -> float | None:
    if not isinstance(rules, dict):
        return None
    overrides = rules.get("footnote_layout_overrides")
    if not isinstance(overrides, dict):
        return None

    domain = (getattr(decision, "domain", "") or "").strip().lower()
    platform = (getattr(decision, "platform_family", "") or "").strip().lower()
    by_domain = overrides.get("domain") if isinstance(overrides.get("domain"), dict) else {}
    by_platform = overrides.get("platform") if isinstance(overrides.get("platform"), dict) else {}

    raw: Any = None
    if domain and domain in by_domain and isinstance(by_domain[domain], dict):
        raw = by_domain[domain].get("note_cutoff_ratio")
    if (
        raw is None
        and platform
        and platform in by_platform
        and isinstance(by_platform[platform], dict)
    ):
        raw = by_platform[platform].get("note_cutoff_ratio")
    try:
        value = float(raw)
    except Exception:
        return None
    if 0.40 <= value <= 0.90:
        return value
    return None


def _classify_pdf_path(
    pdf_path: str,
    *,
    pdf_root: str,
    doc_policy: str,
    rules: dict[str, Any],
) -> tuple[str, DocDecision]:
    domain = infer_domain(pdf_path, pdf_root=pdf_root)
    platform_family = infer_platform_family(domain=domain)
    page_count, first_page_text = read_first_page_overview(pdf_path)
    signals = collect_signals(first_page_text, page_count, metadata=None)
    decision = classify_pdf(
        pdf_path=pdf_path,
        domain=domain,
        platform_family=platform_family,
        signals=signals,
        doc_policy=doc_policy,
        rules=rules,
    )
    return pdf_path, decision


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


def _note_sequence_quality_score(
    notes: list[Any],
    ordinality: Any,
    warnings: list[str],
) -> float:
    numeric_labels = [
        int(note.label) for note in notes if str(getattr(note, "label", "")).isdigit()
    ]
    duplicate_numeric_labels = max(0, len(numeric_labels) - len(set(numeric_labels)))

    score = 0.0
    score += min(len(notes), 400) * 2.0
    score += min(len(numeric_labels), 400) * 3.0
    score += min(_avg_note_length(notes), 160.0) * 0.1

    if ordinality is not None:
        status = str(getattr(ordinality, "status", "") or "")
        if status == "valid":
            score += 120.0
        elif status == "valid_with_gaps":
            gap_count = len(getattr(ordinality, "gaps", []) or [])
            score += max(40.0, 100.0 - (gap_count * 5.0))
        elif status == "invalid":
            score -= 120.0
    elif numeric_labels:
        score -= 40.0
    else:
        score -= 80.0

    if "non_monotonic_labels" in warnings:
        score -= 60.0
    if "ordinality_invalid" in warnings:
        score -= 80.0
    if "ordinality_gaps" in warnings:
        score -= 20.0
    if "reversed_word_order_suspected" in warnings:
        score -= 25.0
    if "low_font_variance_detected" in warnings:
        score -= 10.0

    score -= duplicate_numeric_labels * 8.0
    return round(score, 3)


def _numeric_note_page_bounds(notes: list[Any]) -> dict[int, tuple[int, int]]:
    bounds: dict[int, tuple[int, int]] = {}
    for note in notes:
        label = str(getattr(note, "label", "") or "")
        if not label.isdigit():
            continue
        page_start = int(getattr(note, "page_start", 0) or 0)
        page_end = int(getattr(note, "page_end", 0) or page_start)
        if page_start <= 0:
            continue
        page_end = max(page_start, page_end)
        numeric = int(label)
        if numeric not in bounds:
            bounds[numeric] = (page_start, page_end)
        else:
            prev_s, prev_e = bounds[numeric]
            bounds[numeric] = (min(prev_s, page_start), max(prev_e, page_end))
    return bounds


def _select_ordinality_patch_pages(
    *,
    notes: list[Any],
    gaps: list[int],
    page_count: int,
    expand: int,
    max_pages: int,
) -> list[int]:
    if page_count <= 0 or not gaps:
        return []

    bounds = _numeric_note_page_bounds(notes)
    observed = sorted(bounds.keys())
    if not observed:
        return []

    selected: list[int] = []
    selected_set: set[int] = set()

    def _add_page(page_num: int) -> None:
        if page_num < 1 or page_num > page_count:
            return
        if page_num in selected_set:
            return
        if len(selected) >= max_pages:
            return
        selected.append(page_num)
        selected_set.add(page_num)

    for gap in sorted(set(int(g) for g in gaps if int(g) > 0)):
        lower_candidates = [n for n in observed if n < gap]
        upper_candidates = [n for n in observed if n > gap]
        lower = max(lower_candidates) if lower_candidates else None
        upper = min(upper_candidates) if upper_candidates else None

        if lower is not None and upper is not None:
            lo = max(1, bounds[lower][1] - max(0, expand))
            hi = min(page_count, bounds[upper][0] + max(0, expand))
        elif lower is not None:
            lo = max(1, bounds[lower][1] - max(0, expand))
            hi = min(page_count, bounds[lower][1] + max(2, expand + 1))
        elif upper is not None:
            lo = max(1, bounds[upper][0] - max(2, expand + 1))
            hi = min(page_count, bounds[upper][0] + max(0, expand))
        else:
            continue

        for page_num in range(lo, hi + 1):
            _add_page(page_num)
            if len(selected) >= max_pages:
                break
        if len(selected) >= max_pages:
            break

    return sorted(selected)


def _build_patch_pdf(pdf_path: str, pages: list[int]) -> tuple[str | None, dict[int, int]]:
    if not pages:
        return None, {}
    reader = PdfReader(pdf_path)
    writer = PdfWriter()
    page_map: dict[int, int] = {}
    for patch_page, orig_page in enumerate(sorted(set(pages)), start=1):
        page_idx = orig_page - 1
        if page_idx < 0 or page_idx >= len(reader.pages):
            continue
        writer.add_page(reader.pages[page_idx])
        page_map[patch_page] = orig_page
    if not page_map:
        return None, {}

    tmp = tempfile.NamedTemporaryFile(prefix="ordinality_patch_", suffix=".pdf", delete=False)
    tmp_path = tmp.name
    with tmp:
        writer.write(tmp)
    return tmp_path, page_map


def _remap_note_to_source_pages(note: Any, page_map: dict[int, int]) -> None:
    src_start = page_map.get(int(getattr(note, "page_start", 0) or 0))
    src_end = page_map.get(int(getattr(note, "page_end", 0) or 0))
    if src_start:
        note.page_start = src_start
    if src_end:
        note.page_end = src_end
    for seg in getattr(note, "segments", []) or []:
        seg.page = page_map.get(int(getattr(seg, "page", 0) or 0), getattr(seg, "page", 0))


def _refresh_ordinality_warnings(warnings: list[str], ordinality: Any) -> list[str]:
    refreshed = [w for w in warnings if w not in {"ordinality_invalid", "ordinality_gaps"}]
    if ordinality is None:
        return refreshed
    if bool(getattr(ordinality, "tolerance_exceeded", False)):
        refreshed.append("ordinality_invalid")
    elif list(getattr(ordinality, "gaps", []) or []):
        refreshed.append("ordinality_gaps")
    return refreshed


def _revalidate_notes_ordinality(notes: list[Any], *, gap_tolerance: int) -> Any:
    notes.sort(
        key=lambda n: (
            (int(getattr(n, "label", 10**9)) if str(getattr(n, "label", "")).isdigit() else 10**9),
            int(getattr(n, "page_start", 0) or 0),
            int(getattr(n, "page_end", 0) or 0),
        )
    )
    for idx, note in enumerate(notes, start=1):
        note.ordinal = idx
    relabeled_numeric = [
        int(str(getattr(note, "label", "") or "0"))
        for note in notes
        if str(getattr(note, "label", "") or "").isdigit()
    ]
    return (
        validate_ordinality(relabeled_numeric, gap_tolerance=gap_tolerance)
        if relabeled_numeric
        else None
    )


def _force_contiguous_numeric_labels(notes: list[Any], *, gap_tolerance: int) -> tuple[bool, Any]:
    """Force numeric note labels into contiguous 1..N order by document position.

    This is a last-resort normalization used when ordinality remains unresolved
    after native/OCR patch passes.
    """
    ordered = sorted(
        enumerate(notes),
        key=lambda item: (
            int(getattr(item[1], "page_start", 0) or 0),
            int(getattr(item[1], "page_end", 0) or 0),
            int(getattr(item[1], "ordinal", 0) or 0),
            item[0],
        ),
    )
    expected = 1
    changed = False
    for _idx, note in ordered:
        label = str(getattr(note, "label", "") or "")
        if not label.isdigit():
            continue
        numeric = int(label)
        if numeric != expected:
            note.label = str(expected)
            flags = list(getattr(note, "quality_flags", []) or [])
            if "ordinality_relabelled" not in flags:
                flags.append("ordinality_relabelled")
            note.quality_flags = flags
            changed = True
        expected += 1

    ordinality = _revalidate_notes_ordinality(notes, gap_tolerance=gap_tolerance)
    return changed, ordinality


def _collect_ocr_page_text(document: Any) -> dict[int, str]:
    pages = getattr(document, "pages", []) or []
    page_text: dict[int, str] = {}
    for page in pages:
        page_num = int(getattr(page, "page_number", 0) or 0)
        if page_num <= 0:
            continue
        raw_text = str(getattr(page, "raw_text", "") or "").strip()
        if raw_text:
            page_text[page_num] = raw_text
    return page_text


def _document_extract_empty(document: Any) -> bool:
    if document is None:
        return True
    pages = getattr(document, "pages", []) or []
    if not pages:
        return True
    for page in pages:
        raw_text = str(getattr(page, "raw_text", "") or "").strip()
        if raw_text:
            return False
        body_lines = getattr(page, "body_lines", []) or []
        note_lines = getattr(page, "note_lines", []) or []
        if body_lines or note_lines:
            return False
    return True


def _derive_ocr_review_reasons(
    *,
    parser_used: str,
    warnings: list[str],
    ordinality_status: str | None,
    note_count: int,
    ocr_used: bool,
    native_extract_empty: bool,
) -> list[str]:
    if ocr_used:
        return []
    parser_key = (parser_used or "").strip().lower()
    warning_set = {str(w).strip() for w in warnings if str(w).strip()}
    reasons: list[str] = []
    if note_count <= 0 and native_extract_empty:
        reasons.append("native_text_extraction_empty")
    if parser_key == "pdftotext" and "selected_pdftotext_output" in warning_set:
        reasons.append("pdftotext_fallback_without_layout")
    if parser_key == "pdftotext" and native_extract_empty:
        reasons.append("native_spatial_extract_empty")
    if parser_key == "pdftotext" and (ordinality_status or "").strip().lower() == "invalid":
        reasons.append("pdftotext_ordinality_invalid")
    return reasons


def _extract_sentence_around_label(text: str, label: int) -> str | None:
    if not text:
        return None
    marker_re = re.compile(rf"(?<!\d){int(label)}(?!\d)")
    marker = marker_re.search(text)
    if marker is None:
        return None
    idx = marker.start()
    starts = [text.rfind(token, 0, idx) for token in (".", "!", "?", "\n")]
    start = max(starts)
    start = 0 if start < 0 else start + 1
    ends = [text.find(token, idx) for token in (".", "!", "?", "\n")]
    ends = [pos for pos in ends if pos >= 0]
    end = min(ends) + 1 if ends else len(text)
    snippet = " ".join(text[start:end].split()).strip()
    if not snippet:
        return None
    # Keep gap-anchored synthetic notes compact to avoid noisy payload bloat.
    return snippet[:900]


def _synthesize_gap_notes_from_ocr_markers(
    *,
    notes: list[Any],
    gaps: list[int],
    ocr_document: Any,
) -> list[NoteRecord]:
    if not gaps or ocr_document is None:
        return []
    page_text = _collect_ocr_page_text(ocr_document)
    if not page_text:
        return []

    bounds = _numeric_note_page_bounds(notes)
    observed = sorted(bounds.keys())
    existing_labels = {int(label) for label in observed if int(label) > 0}
    page_max = max(page_text.keys())
    synthesized: list[NoteRecord] = []

    for gap in sorted({int(g) for g in gaps if int(g) > 0}):
        if gap in existing_labels:
            continue
        lower_candidates = [n for n in observed if n < gap]
        upper_candidates = [n for n in observed if n > gap]
        lower = max(lower_candidates) if lower_candidates else None
        upper = min(upper_candidates) if upper_candidates else None
        # Require close anchors to keep synthetic gaps conservative.
        if lower not in {gap - 1, gap - 2} and upper not in {gap + 1, gap + 2}:
            continue

        if lower is not None and upper is not None:
            lo = max(1, min(bounds[lower][0], bounds[lower][1]) - 1)
            hi = min(page_max, max(bounds[upper][0], bounds[upper][1]) + 1)
        elif lower is not None:
            lo = max(1, min(bounds[lower][0], bounds[lower][1]) - 1)
            hi = min(page_max, max(bounds[lower][0], bounds[lower][1]) + 2)
        elif upper is not None:
            lo = max(1, min(bounds[upper][0], bounds[upper][1]) - 2)
            hi = min(page_max, max(bounds[upper][0], bounds[upper][1]) + 1)
        else:
            continue
        if hi < lo:
            continue

        matched_page = 0
        matched_text = ""
        for page_num in range(lo, hi + 1):
            snippet = _extract_sentence_around_label(page_text.get(page_num, ""), gap)
            if snippet:
                matched_page = page_num
                matched_text = snippet
                break
        if not matched_page or not matched_text:
            continue

        synthesized.append(
            NoteRecord(
                ordinal=0,
                label=str(gap),
                note_type="footnote",
                text=matched_text,
                page_start=matched_page,
                page_end=matched_page,
                segments=[NoteChunk(page=matched_page, text=matched_text, source="ocr_gap_anchor")],
                quality_flags=["ocr_gap_anchor"],
            )
        )
        existing_labels.add(gap)

    return synthesized


def _cache_compatible_with_mode(*, parser_mode: str, cached_parser: str) -> bool:
    mode = (parser_mode or "").strip().lower()
    parser = (cached_parser or "").strip().lower()
    if not parser:
        return False
    if mode == "liteparse_only":
        return parser == "liteparse"
    if mode == "pdfplumber_only":
        return parser == "pdfplumber"
    if mode == "pypdf_only":
        return parser == "pypdf"
    if mode == "opendataloader_only":
        return parser == "opendataloader"
    if mode == "docling_only":
        return parser == "docling"
    # balanced/footnote_optimized and unknown modes can reuse cached extracts.
    return True


def _collect_native_gap_candidates(
    *,
    document: Any,
    notes: list[Any],
    gaps: list[int],
    strict_label_filter: bool,
    max_label: int,
    page_numbers: list[int] | None = None,
) -> list[NoteRecord]:
    """Recover missing numeric labels from native page lines without synthesis."""
    if document is None or not gaps:
        return []
    pages = list(getattr(document, "pages", []) or [])
    if not pages:
        return []

    allowed_pages = set(int(p) for p in (page_numbers or []) if int(p) > 0)
    existing_labels = {
        int(str(getattr(note, "label", "") or "0"))
        for note in notes
        if str(getattr(note, "label", "") or "").isdigit()
    }
    target_labels = {int(g) for g in gaps if int(g) > 0} - existing_labels
    if not target_labels:
        return []

    rescued: list[NoteRecord] = []
    seen_labels = set(existing_labels)
    note_anchor_pages = {
        int(getattr(note, "page_start", 0) or 0)
        for note in notes
        if int(getattr(note, "page_start", 0) or 0) > 0
    }
    note_anchor_pages |= {
        int(getattr(note, "page_end", 0) or 0)
        for note in notes
        if int(getattr(note, "page_end", 0) or 0) > 0
    }

    for page in pages:
        page_num = int(getattr(page, "page_number", 0) or 0)
        if allowed_pages and page_num not in allowed_pages:
            continue

        # Scan note region first, then body region for strict marker starts.
        line_sources = (
            [(line, "note") for line in (getattr(page, "note_lines", []) or [])]
            + [(line, "body") for line in (getattr(page, "body_lines", []) or [])]
        )
        if not line_sources:
            continue

        page_height = float(getattr(page, "height", 0.0) or 0.0)
        note_line_rels: list[float] = []
        for note_line in (getattr(page, "note_lines", []) or []):
            note_text = _clean_line_text(getattr(note_line, "text", "") or "")
            if not note_text:
                continue
            if _validated_marker_match(
                note_text, strict_label_filter=strict_label_filter, max_label=max_label
            ) is None:
                continue
            top = float(getattr(note_line, "top", 0.0) or 0.0)
            if page_height > 0.0 and top > 0.0:
                note_line_rels.append(top / page_height)
        note_band_floor = 0.62
        if note_line_rels:
            note_band_floor = max(0.50, min(note_line_rels) - 0.08)
        nearby_anchor_present = (
            page_num in note_anchor_pages
            or (page_num - 1) in note_anchor_pages
            or (page_num + 1) in note_anchor_pages
        )

        idx = 0
        while idx < len(line_sources):
            line, region = line_sources[idx]
            text = _clean_line_text(getattr(line, "text", "") or "")
            marker = _validated_marker_match(
                text, strict_label_filter=strict_label_filter, max_label=max_label
            )
            if marker is None:
                idx += 1
                continue
            label = str(marker.group("label") or "")
            if not label.isdigit():
                idx += 1
                continue
            numeric = int(label)
            if numeric not in target_labels or numeric in seen_labels:
                idx += 1
                continue

            rest = str(marker.group("text") or "").strip()
            # Body-region rescue requires substantive legal cue to avoid headers.
            if region == "body" and not REAL_FOOTNOTE_START_RE.search(rest):
                idx += 1
                continue
            if region == "body":
                if not nearby_anchor_present:
                    idx += 1
                    continue
                top = float(getattr(line, "top", 0.0) or 0.0)
                if page_height > 0.0 and top > 0.0:
                    rel_top = top / page_height
                    if rel_top < note_band_floor:
                        idx += 1
                        continue

            text_parts = [rest] if rest else []
            segments = [NoteChunk(page=page_num, text=rest, source=f"native_gap_{region}")]
            scan_idx = idx + 1
            while scan_idx < len(line_sources):
                next_line, _next_region = line_sources[scan_idx]
                next_text = _clean_line_text(getattr(next_line, "text", "") or "")
                if not next_text:
                    scan_idx += 1
                    continue
                if _validated_marker_match(
                    next_text, strict_label_filter=strict_label_filter, max_label=max_label
                ):
                    break
                if _likely_continuation(next_text):
                    text_parts.append(next_text)
                    segments.append(
                        NoteChunk(page=page_num, text=next_text, source=f"native_gap_{region}")
                    )
                scan_idx += 1

            merged_text = " ".join(part for part in text_parts if part).strip()
            if merged_text:
                rescued.append(
                    NoteRecord(
                        ordinal=0,
                        label=str(numeric),
                        note_type="footnote",
                        text=merged_text,
                        page_start=page_num,
                        page_end=page_num,
                        segments=segments,
                        quality_flags=["native_gap_rescue"],
                    )
                )
                seen_labels.add(numeric)
            idx = scan_idx

    return rescued


def _extract_for_pdf(
    pdf_path: str,
    config: BatchConfig,
    ocr_pool: OCRWorkerPool | None,
    dependency_versions_payload: dict[str, str] | None = None,
    doc_decision: DocDecision | None = None,
    text_cache: TextExtractionCache | None = None,
) -> dict[str, Any]:
    sidecar_path = _sidecar_path(pdf_path)
    if os.path.exists(sidecar_path) and not config.overwrite:
        return {
            "pdf_path": pdf_path,
            "sidecar_path": sidecar_path,
            "status": "skipped_existing",
            "notes": 0,
            "author_notes": 0,
            "ocr_used": False,
            "warnings": [],
            "needs_ocr_review": False,
            "ocr_review_reasons": [],
        }

    warnings: list[str] = []
    pdf_sha256: str | None = None
    if config.include_pdf_sha256:
        pdf_sha256, _pdf_size = compute_pdf_sha256_and_size(pdf_path)

    def _profile_for(document: Any) -> FootnoteProfile:
        suspicious_scan = bool(
            document
            and (
                "reversed_word_order_suspected" in getattr(document, "warnings", [])
                or "low_font_variance_detected" in getattr(document, "warnings", [])
            )
        )
        if doc_decision and doc_decision.platform_family == "digital_commons" and suspicious_scan:
            return FootnoteProfile(gap_tolerance=12, strict_label_filter=True, prefer_ocr=True)
        if doc_decision and doc_decision.platform_family == "digital_commons":
            return FootnoteProfile(gap_tolerance=6, strict_label_filter=True)
        if suspicious_scan:
            # Non-DC scanned PDFs get relaxed gap tolerance and OCR preference.
            # reversed_word_order also triggers OCR; low_font_variance alone uses looser gaps.
            prefer_ocr = bool(
                document and "reversed_word_order_suspected" in getattr(document, "warnings", [])
            )
            return FootnoteProfile(gap_tolerance=6, strict_label_filter=True, prefer_ocr=prefer_ocr)
        return FootnoteProfile()

    parser_mode = (config.text_parser_mode or "balanced").strip().lower()
    if parser_mode not in {
        "balanced",
        "pdfplumber_only",
        "pypdf_only",
        "docling_only",
        "opendataloader_only",
        "liteparse_only",
        "footnote_optimized",
    }:
        parser_mode = "footnote_optimized"

    note_cutoff_ratio_override = _resolve_note_cutoff_ratio(
        decision=doc_decision, rules=getattr(config, "_doc_rules_payload", {})  # type: ignore[attr-defined]
    )

    ocr_primary_attempted = False
    ocr_primary_failed = False
    native_fallback_used = False
    ocr_used = False
    ordinality_patch_attempted = False
    ordinality_patch_resolved = False
    ordinality_patch_pages: list[int] = []
    ordinality_patch_added = 0
    force_ocr_after_patch_unresolved = False
    document = None
    parser_used = ""
    balanced_fallback_used = False
    balanced_fast_accept = False
    native_extract_empty = True

    if config.ocr_mode == "always" and ocr_pool is not None:
        ocr_primary_attempted = True
        # Ensure all pages are processed in 'always' mode.
        try:
            reader = PdfReader(pdf_path)
            total_pages = len(reader.pages)
            page_numbers = list(range(1, total_pages + 1))
        except Exception:
            page_numbers = None

        ocr_document, ocr_warnings = ocr_pool.extract_document(pdf_path, page_numbers=page_numbers)
        warnings.extend(ocr_warnings)
        if ocr_document is not None:
            document = ocr_document
            parser_used = ocr_document.parser or "olmocr"
            ocr_used = True
            warnings.append("selected_ocr_output")
        else:
            ocr_primary_failed = True
            warnings.append("olmocr_primary_failed")

    if document is None:
        cached = text_cache.get(pdf_path) if text_cache else None
        if cached is not None and not _cache_compatible_with_mode(
            parser_mode=parser_mode, cached_parser=str(getattr(cached, "parser", "") or "")
        ):
            cached = None
        if cached is not None:
            document = cached
        else:
            if note_cutoff_ratio_override is None:
                document = _extract_document_with_mode(
                    pdf_path,
                    parser_mode=parser_mode,
                )
            else:
                document = _extract_document_with_mode(
                    pdf_path,
                    parser_mode=parser_mode,
                    note_cutoff_ratio=note_cutoff_ratio_override,
                )
            if text_cache:
                text_cache.put(pdf_path, document)
        if ocr_primary_failed:
            native_fallback_used = True
            warnings.append("native_fallback_after_ocr_failure")

    native_extract_empty = _document_extract_empty(document)
    warnings.extend(document.warnings)
    profile = _profile_for(document)

    notes, author_notes, ordinality, note_warnings = segment_document_notes_extended(
        document,
        gap_tolerance=profile.gap_tolerance,
        strict_label_filter=profile.strict_label_filter,
    )
    warnings.extend(note_warnings)
    parser_used = parser_used or document.parser or ""

    if parser_mode == "balanced" and parser_used == "pypdf":
        if _pypdf_fast_path_accepted(document, notes):
            balanced_fast_accept = True
        else:
            if note_cutoff_ratio_override is None:
                fallback_document = _extract_document_with_mode(
                    pdf_path,
                    parser_mode="pdfplumber_only",
                )
            else:
                fallback_document = _extract_document_with_mode(
                    pdf_path,
                    parser_mode="pdfplumber_only",
                    note_cutoff_ratio=note_cutoff_ratio_override,
                )
            fallback_profile = _profile_for(fallback_document)
            fallback_notes, fallback_author_notes, fallback_ordinality, fallback_note_warnings = (
                segment_document_notes_extended(
                    fallback_document,
                    gap_tolerance=fallback_profile.gap_tolerance,
                    strict_label_filter=fallback_profile.strict_label_filter,
                )
            )
            if fallback_document.pages:
                balanced_fallback_used = True
                warnings.append("balanced_fallback_pdfplumber")
                warnings.extend(fallback_document.warnings)
                warnings.extend(fallback_note_warnings)
                document = fallback_document
                profile = fallback_profile
                notes = fallback_notes
                author_notes = fallback_author_notes
                ordinality = fallback_ordinality
                parser_used = fallback_document.parser or "pdfplumber"
            else:
                warnings.append("balanced_fallback_unavailable")

    # For footnote_optimized, suspicious reversed-order output frequently improves
    # under pdfplumber layout extraction; compare and keep the stronger sequence.
    if (
        parser_mode == "footnote_optimized"
        and parser_used == "liteparse"
        and "reversed_word_order_suspected" in getattr(document, "warnings", [])
    ):
        if note_cutoff_ratio_override is None:
            rescue_document = _extract_document_with_mode(
                pdf_path,
                parser_mode="pdfplumber_only",
            )
        else:
            rescue_document = _extract_document_with_mode(
                pdf_path,
                parser_mode="pdfplumber_only",
                note_cutoff_ratio=note_cutoff_ratio_override,
            )
        if rescue_document.pages:
            rescue_profile = _profile_for(rescue_document)
            rescue_notes, rescue_author_notes, rescue_ordinality, rescue_note_warnings = (
                segment_document_notes_extended(
                    rescue_document,
                    gap_tolerance=rescue_profile.gap_tolerance,
                    strict_label_filter=rescue_profile.strict_label_filter,
                )
            )
            base_score = _note_sequence_quality_score(notes, ordinality, warnings)
            rescue_warnings = list(warnings)
            rescue_warnings.append("footnote_optimized_pdfplumber_rescue")
            rescue_warnings.extend(rescue_document.warnings)
            rescue_warnings.extend(rescue_note_warnings)
            rescue_score = _note_sequence_quality_score(rescue_notes, rescue_ordinality, rescue_warnings)
            base_gaps = len(list(getattr(ordinality, "gaps", []) or [])) if ordinality is not None else 10**6
            rescue_gaps = (
                len(list(getattr(rescue_ordinality, "gaps", []) or []))
                if rescue_ordinality is not None
                else 10**6
            )
            base_invalid = (
                str(getattr(ordinality, "status", "") or "") == "invalid"
                if ordinality is not None
                else True
            )
            rescue_invalid = (
                str(getattr(rescue_ordinality, "status", "") or "") == "invalid"
                if rescue_ordinality is not None
                else True
            )
            should_swap = False
            if base_invalid and not rescue_invalid:
                should_swap = True
            elif rescue_gaps < base_gaps:
                should_swap = True
            elif rescue_score > base_score:
                should_swap = True
            elif rescue_score == base_score and len(rescue_notes) > len(notes):
                should_swap = True

            if should_swap:
                warnings = rescue_warnings
                document = rescue_document
                profile = rescue_profile
                notes = rescue_notes
                author_notes = rescue_author_notes
                ordinality = rescue_ordinality
                parser_used = rescue_document.parser or "pdfplumber"

    if (
        config.ordinality_patch
        and document is not None
        and ordinality is not None
        and str(getattr(ordinality, "status", "") or "") == "invalid"
    ):
        gaps = [int(g) for g in (getattr(ordinality, "gaps", []) or []) if int(g) > 0]
        patch_pages = _select_ordinality_patch_pages(
            notes=notes,
            gaps=gaps,
            page_count=int(getattr(document, "page_count", 0) or 0),
            expand=max(0, int(config.ordinality_patch_expand)),
            max_pages=max(1, int(config.ordinality_patch_max_pages)),
        )
        if patch_pages:
            ordinality_patch_attempted = True
            ordinality_patch_pages = patch_pages
            warnings.append("ordinality_patch_attempted")
            warnings.append(f"ordinality_patch_pages={','.join(str(p) for p in patch_pages)}")
            patch_pdf_path, page_map = _build_patch_pdf(pdf_path, patch_pages)
            if patch_pdf_path and page_map:
                try:
                    cutoff_for_patch = note_cutoff_ratio_override
                    if cutoff_for_patch is None:
                        cutoff_for_patch = 0.72
                    cutoff_for_patch = max(0.55, min(0.82, float(cutoff_for_patch) - 0.06))
                    patch_document = _extract_document_with_mode(
                        patch_pdf_path,
                        parser_mode="pdfplumber_only",
                        note_cutoff_ratio=cutoff_for_patch,
                    )
                    patch_notes, _patch_author_notes, _patch_ordinality, patch_note_warnings = (
                        segment_document_notes_extended(
                            patch_document,
                            gap_tolerance=profile.gap_tolerance,
                            strict_label_filter=profile.strict_label_filter,
                        )
                    )
                    warnings.extend(patch_note_warnings)
                    existing_numeric_labels = {
                        int(note.label)
                        for note in notes
                        if str(getattr(note, "label", "") or "").isdigit()
                    }
                    missing_labels = set(gaps) - existing_numeric_labels
                    patch_candidates: list[Any] = []
                    for patch_note in patch_notes:
                        label = str(getattr(patch_note, "label", "") or "")
                        if not label.isdigit():
                            continue
                        numeric = int(label)
                        if numeric not in missing_labels:
                            continue
                        _remap_note_to_source_pages(patch_note, page_map)
                        patch_candidates.append(patch_note)
                    if patch_candidates:
                        notes.extend(patch_candidates)
                        ordinality_patch_added = len(patch_candidates)
                        warnings.append(f"ordinality_patch_added={ordinality_patch_added}")
                        ordinality = _revalidate_notes_ordinality(
                            notes, gap_tolerance=profile.gap_tolerance
                        )
                        warnings = _refresh_ordinality_warnings(warnings, ordinality)
                finally:
                    try:
                        os.remove(patch_pdf_path)
                    except Exception:
                        pass
            if ordinality is not None and str(getattr(ordinality, "status", "") or "") != "invalid":
                ordinality_patch_resolved = True
                warnings.append("ordinality_patch_resolved")
            else:
                warnings.append("ordinality_patch_unresolved")
                # Strict native-only gap rescue from page lines (no synthetic text).
                native_gap_candidates = _collect_native_gap_candidates(
                    document=document,
                    notes=notes,
                    gaps=gaps,
                    strict_label_filter=profile.strict_label_filter,
                    max_label=max(120, int(getattr(document, "page_count", 0) or 0) * 6),
                    page_numbers=patch_pages,
                )
                if native_gap_candidates:
                    notes.extend(native_gap_candidates)
                    warnings.append(f"ordinality_patch_native_added={len(native_gap_candidates)}")
                    ordinality = _revalidate_notes_ordinality(
                        notes, gap_tolerance=profile.gap_tolerance
                    )
                    warnings = _refresh_ordinality_warnings(warnings, ordinality)
                    if ordinality is not None and str(getattr(ordinality, "status", "") or "") != "invalid":
                        ordinality_patch_resolved = True
                        warnings = [w for w in warnings if w != "ordinality_patch_unresolved"]
                        warnings.append("ordinality_patch_native_resolved")
                    else:
                        warnings.append("ordinality_patch_native_unresolved")
            if ocr_pool is not None and int(config.ordinality_patch_ocr_escalation_passes) >= 1:
                current_gaps = [
                    int(g) for g in (getattr(ordinality, "gaps", []) or []) if int(g) > 0
                ]
                if current_gaps:
                    warnings.append("ordinality_patch_ocr_attempted")
                    attempts = max(1, int(config.ordinality_patch_ocr_escalation_passes))
                    for _ in range(attempts):
                        current_gaps = [
                            int(g) for g in (getattr(ordinality, "gaps", []) or []) if int(g) > 0
                        ]
                        if not current_gaps:
                            break
                        ocr_patch_document, ocr_patch_warnings = ocr_pool.extract_document(
                            pdf_path, page_numbers=patch_pages
                        )
                        warnings.extend(ocr_patch_warnings)
                        if ocr_patch_document is None:
                            continue
                        (
                            ocr_patch_notes,
                            _ocr_patch_author_notes,
                            _ocr_patch_ordinality,
                            ocr_patch_note_warnings,
                        ) = segment_document_notes_extended(
                            ocr_patch_document,
                            gap_tolerance=profile.gap_tolerance,
                            strict_label_filter=profile.strict_label_filter,
                        )
                        warnings.extend(ocr_patch_note_warnings)
                        existing_numeric_labels = {
                            int(note.label)
                            for note in notes
                            if str(getattr(note, "label", "") or "").isdigit()
                        }
                        missing_labels = set(current_gaps) - existing_numeric_labels
                        ocr_patch_candidates: list[Any] = []
                        for ocr_patch_note in ocr_patch_notes:
                            label = str(getattr(ocr_patch_note, "label", "") or "")
                            if not label.isdigit():
                                continue
                            numeric = int(label)
                            if numeric not in missing_labels:
                                continue
                            ocr_patch_candidates.append(ocr_patch_note)
                        if missing_labels:
                            synthetic_candidates = _synthesize_gap_notes_from_ocr_markers(
                                notes=notes,
                                gaps=sorted(missing_labels),
                                ocr_document=ocr_patch_document,
                            )
                            if synthetic_candidates:
                                warnings.append(
                                    f"ordinality_patch_ocr_synthesized={len(synthetic_candidates)}"
                                )
                            ocr_patch_candidates.extend(synthetic_candidates)
                        if ocr_patch_candidates:
                            deduped_candidates: list[Any] = []
                            seen_labels = {
                                int(str(getattr(note, "label", "") or "0"))
                                for note in notes
                                if str(getattr(note, "label", "") or "").isdigit()
                            }
                            for candidate in ocr_patch_candidates:
                                label = str(getattr(candidate, "label", "") or "")
                                if not label.isdigit():
                                    continue
                                numeric = int(label)
                                if numeric in seen_labels:
                                    continue
                                seen_labels.add(numeric)
                                deduped_candidates.append(candidate)
                            if deduped_candidates:
                                notes.extend(deduped_candidates)
                                warnings.append(
                                    f"ordinality_patch_ocr_added={len(deduped_candidates)}"
                                )
                                ordinality = _revalidate_notes_ordinality(
                                    notes, gap_tolerance=profile.gap_tolerance
                                )
                                warnings = _refresh_ordinality_warnings(warnings, ordinality)
                    remaining_gaps = [
                        int(g) for g in (getattr(ordinality, "gaps", []) or []) if int(g) > 0
                    ]
                    if not remaining_gaps:
                        warnings.append("ordinality_patch_ocr_resolved")
                    else:
                        warnings.append("ordinality_patch_ocr_unresolved")
                        if str(getattr(ordinality, "status", "") or "") == "invalid":
                            force_ocr_after_patch_unresolved = True
                elif str(getattr(ordinality, "status", "") or "") == "invalid":
                    force_ocr_after_patch_unresolved = True

    sequence_quality_score = _note_sequence_quality_score(notes, ordinality, warnings)
    if config.ocr_mode == "fallback" and ocr_pool is not None:
        should_ocr = (
            bool(doc_decision and doc_decision.ocr_candidate)
            or profile.prefer_ocr
            or ocr_fallback_recommended(document, len(notes))
            or force_ocr_after_patch_unresolved
        )
        if should_ocr:
            # Keep fallback OCR scoped to inferred ordinality patch pages when available.
            fallback_ocr_pages = ordinality_patch_pages if ordinality_patch_pages else None
            ocr_document, ocr_warnings = ocr_pool.extract_document(
                pdf_path, page_numbers=fallback_ocr_pages
            )
            if ocr_document is not None:
                ocr_profile = _profile_for(ocr_document)
                ocr_notes, ocr_author_notes, ocr_ordinality, ocr_note_warnings = (
                    segment_document_notes_extended(
                        ocr_document,
                        gap_tolerance=ocr_profile.gap_tolerance,
                        strict_label_filter=ocr_profile.strict_label_filter,
                    )
                )
                ocr_candidate_warnings = list(warnings)
                ocr_candidate_warnings.extend(ocr_warnings)
                ocr_candidate_warnings.extend(ocr_note_warnings)
                ocr_sequence_quality_score = _note_sequence_quality_score(
                    ocr_notes,
                    ocr_ordinality,
                    ocr_candidate_warnings,
                )
                # Keep OCR output when explicitly requested or when it produces a
                # cleaner ordered footnote stream than the base extraction.
                if ocr_sequence_quality_score > sequence_quality_score or (
                    ocr_sequence_quality_score == sequence_quality_score
                    and len(ocr_notes) > len(notes)
                ):
                    document = ocr_document
                    profile = ocr_profile
                    notes = ocr_notes
                    author_notes = ocr_author_notes
                    ordinality = ocr_ordinality
                    parser_used = ocr_document.parser or parser_used
                    warnings = ocr_candidate_warnings
                    sequence_quality_score = ocr_sequence_quality_score
                    ocr_used = True
                    warnings.append("selected_ocr_output")

    attach_context_batch(notes, document)

    for note in notes:
        if config.features in {"legal", "all"}:
            enrich_note_features(note, preset=config.features)
        elif config.features == "core":
            note.features = {}
        else:
            raise ValueError(f"Unsupported features preset: {config.features}")
        note.confidence = _note_confidence(note)


    doc_conf = _document_confidence(notes, warnings)
    payload = SidecarDocument(
        source_pdf_path=pdf_path,
        pdf_sha256=pdf_sha256,
        extractor_version=EXTRACTOR_VERSION,
        created_at=utc_now_iso(),
        dependency_versions=(dependency_versions_payload or dependency_versions()),
        document_confidence=doc_conf,
        warnings=sorted(set(warnings)),
        features_preset=config.features,
        notes=notes,
        author_notes=author_notes,
        ordinality=ordinality,
    ).to_dict(emit_segments=config.emit_segments)

    primary_numeric_count = _numeric_note_count(payload.get("notes"))
    if primary_numeric_count == 0 and len(payload.get("notes", {}) or {}) <= 2:
        candidate_payload = _load_pdftotext_candidate_payload(
            pdf_path,
            features=config.features,
        )
        if candidate_payload is not None:
            _normalize_sparse_hundred_labels_payload(candidate_payload)
            candidate_numeric_count = _numeric_note_count(candidate_payload.get("notes"))
            if candidate_numeric_count > primary_numeric_count:
                candidate_warnings = [
                    str(item)
                    for item in candidate_payload.get("warnings", [])
                    if isinstance(item, str)
                ]
                candidate_warnings.append("selected_pdftotext_output")
                candidate_payload["warnings"] = sorted(set(candidate_warnings))
                candidate_payload["source_pdf_path"] = pdf_path
                candidate_payload["features_preset"] = config.features
                candidate_payload["dependency_versions"] = (
                    dependency_versions_payload or dependency_versions()
                )
                if pdf_sha256 is not None:
                    candidate_payload["pdf_sha256"] = pdf_sha256
                payload = candidate_payload
                parser_used = "pdftotext"
                sequence_quality_score = max(sequence_quality_score, 0.0)

    payload_notes = payload.get("notes")
    payload_ordinality = payload.get("ordinality")
    payload_warnings = payload.get("warnings")
    payload_note_count = len(payload_notes) if isinstance(payload_notes, dict) else len(notes)
    payload_ordinality_status = (
        str(payload_ordinality.get("status"))
        if isinstance(payload_ordinality, dict) and payload_ordinality.get("status") is not None
        else (str(getattr(ordinality, "status", "")) if ordinality is not None else None)
    )
    payload_warning_list = (
        [str(item) for item in payload_warnings if str(item).strip()]
        if isinstance(payload_warnings, list)
        else [str(item) for item in payload.get("warnings", []) if str(item).strip()]
    )
    ocr_review_reasons = _derive_ocr_review_reasons(
        parser_used=parser_used,
        warnings=payload_warning_list,
        ordinality_status=payload_ordinality_status,
        note_count=payload_note_count,
        ocr_used=ocr_used,
        native_extract_empty=native_extract_empty,
    )
    needs_ocr_review = bool(ocr_review_reasons)
    if needs_ocr_review:
        if "needs_ocr_review" not in payload_warning_list:
            payload_warning_list.append("needs_ocr_review")
        payload["warnings"] = sorted(set(payload_warning_list))
        metadata = payload.get("document_metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        metadata["needs_ocr_review"] = True
        metadata["ocr_review_reasons"] = list(ocr_review_reasons)
        payload["document_metadata"] = metadata

    # Preserve note insertion order in sidecars so downstream evaluation can
    # compare ordered streams accurately.
    _write_json_atomic(sidecar_path, payload, sort_keys=False)
    _write_jsonl_sidecar_atomic(
        f"{pdf_path}.footnotes.jsonl", payload, emit_segments=config.emit_segments
    )

    payload_notes = payload.get("notes")
    payload_author_notes = payload.get("author_notes")
    payload_ordinality = payload.get("ordinality")
    payload_warnings = payload.get("warnings")

    return {
        "pdf_path": pdf_path,
        "sidecar_path": sidecar_path,
        "status": "ok",
        "notes": len(payload_notes) if isinstance(payload_notes, dict) else len(notes),
        "author_notes": (
            len(payload_author_notes)
            if isinstance(payload_author_notes, list)
            else len(author_notes)
        ),
        "ocr_used": ocr_used,
        "warnings": payload_warnings if isinstance(payload_warnings, list) else payload["warnings"],
        "document_confidence": payload["document_confidence"],
        "ordinality_status": (
            payload_ordinality.get("status")
            if isinstance(payload_ordinality, dict)
            else (ordinality.status if ordinality else None)
        ),
        "doc_type": doc_decision.doc_type if doc_decision else "",
        "platform_family": doc_decision.platform_family if doc_decision else "",
        "ocr_candidate": bool(doc_decision.ocr_candidate) if doc_decision else False,
        "text_parser_mode": parser_mode,
        "parser_used": parser_used,
        "sequence_quality_score": sequence_quality_score,
        "balanced_fast_accept": balanced_fast_accept,
        "balanced_fallback_used": balanced_fallback_used,
        "ocr_primary_attempted": ocr_primary_attempted,
        "ocr_primary_failed": ocr_primary_failed,
        "native_fallback_used": native_fallback_used,
        "ordinality_patch_attempted": ordinality_patch_attempted,
        "ordinality_patch_resolved": ordinality_patch_resolved,
        "ordinality_patch_added": ordinality_patch_added,
        "ordinality_patch_pages": ordinality_patch_pages,
        "needs_ocr_review": needs_ocr_review,
        "ocr_review_reasons": ocr_review_reasons,
    }


def _default_report_path() -> str:
    os.makedirs("artifacts/runs", exist_ok=True)
    return os.path.join("artifacts/runs", f"footnote_extract_{_utc_stamp()}.json")


def _default_doctype_manifest_path() -> str:
    os.makedirs("artifacts/runs", exist_ok=True)
    return os.path.join("artifacts/runs", f"pdf_doc_type_exclusions_{_utc_stamp()}.jsonl")


def _default_ocr_review_manifest_path() -> str:
    os.makedirs("artifacts/runs", exist_ok=True)
    return os.path.join("artifacts/runs", f"pdf_ocr_review_queue_{_utc_stamp()}.jsonl")


def _default_qc_manifest_path() -> str | None:
    manifest = latest_qc_manifest("artifacts/runs")
    if manifest:
        return manifest
    return latest_qc_manifest("runs")


def run_batch(config: BatchConfig) -> dict[str, Any]:
    run_started_monotonic = time.monotonic()
    pdf_root = os.path.abspath(config.pdf_root)
    rules_path = config.doc_rules_path or None
    rules = load_rules(rules_path)
    setattr(config, "_doc_rules_payload", rules)
    if int(config.shard_count) <= 0:
        raise ValueError("shard_count must be >= 1")
    if int(config.shard_index) < 0 or int(config.shard_index) >= int(config.shard_count):
        raise ValueError("shard_index must be in [0, shard_count)")

    discovered = _discover_pdfs(
        pdf_root,
        limit=0,
        shard_count=int(config.shard_count),
        shard_index=int(config.shard_index),
    )
    dependency_versions_payload = dependency_versions()
    report_detail = (config.report_detail or "summary").strip().lower()
    if report_detail not in {"summary", "full"}:
        report_detail = "summary"

    qc_manifest_path: str | None = None
    excluded_by_qc_paths: set[str] = set()
    if config.respect_qc_exclusions:
        qc_manifest_path = config.qc_exclusion_manifest or _default_qc_manifest_path()
        if config.qc_exclusion_manifest and not os.path.exists(config.qc_exclusion_manifest):
            raise ValueError(f"QC exclusion manifest not found: {config.qc_exclusion_manifest}")
        if qc_manifest_path and os.path.exists(qc_manifest_path):
            excluded_by_qc_paths = load_excluded_paths(qc_manifest_path)

    qc_filtered = [path for path in discovered if os.path.abspath(path) not in excluded_by_qc_paths]
    if config.limit and config.limit > 0:
        qc_filtered = qc_filtered[: config.limit]

    excluded_rows: list[dict[str, Any]] = []
    excluded_counts = {"frontmatter": 0, "issue_compilation": 0, "other": 0}

    summary: dict[str, Any] = {
        "started_at": utc_now_iso(),
        "pdf_root": pdf_root,
        "features": config.features,
        "doc_policy": config.doc_policy,
        "doc_rules_path": rules_path or default_rules_path(),
        "ocr_mode": config.ocr_mode,
        "ocr_backend": config.ocr_backend,
        "workers": config.workers,
        "classifier_workers": config.classifier_workers,
        "ocr_workers": config.ocr_workers,
        "text_parser_mode": config.text_parser_mode,
        "shard_count": int(config.shard_count),
        "shard_index": int(config.shard_index),
        "include_pdf_sha256": bool(config.include_pdf_sha256),
        "report_detail": report_detail,
        "total_pdfs": len(discovered),
        "classify_candidates": 0,
        "classify_processed": 0,
        "eligible_pdfs": 0,
        "excluded_by_qc": len(excluded_by_qc_paths),
        "excluded_by_doc_policy": 0,
        "excluded_frontmatter": 0,
        "excluded_issue_compilation": 0,
        "excluded_other": 0,
        "qc_manifest_path": qc_manifest_path,
        "processed": 0,
        "ok": 0,
        "failed": 0,
        "skipped_existing": 0,
        "notes_extracted": 0,
        "ocr_used": 0,
        "parser_used_counts": {},
        "pypdf_fast_accepts": 0,
        "balanced_pdfplumber_fallbacks": 0,
        "ordinality_patch_attempted_docs": 0,
        "ordinality_patch_resolved_docs": 0,
        "ordinality_patch_notes_added": 0,
        "needs_ocr_review": 0,
        "doc_type_manifest_path": "",
        "ocr_review_manifest_path": "",
        "results": [],
    }
    ocr_review_rows: list[dict[str, Any]] = []

    classify_targets: list[str] = []
    for pdf_path in qc_filtered:
        if os.path.exists(_sidecar_path(pdf_path)) and not config.overwrite:
            summary["skipped_existing"] += 1
            if report_detail == "full":
                summary["results"].append(
                    {
                        "pdf_path": pdf_path,
                        "sidecar_path": _sidecar_path(pdf_path),
                        "status": "skipped_existing",
                        "notes": 0,
                        "ocr_used": False,
                        "warnings": [],
                    }
                )
            continue
        classify_targets.append(pdf_path)
    summary["classify_candidates"] = len(classify_targets)

    text_cache = TextExtractionCache(enabled=config.text_cache_enabled)

    ocr_pool: OCRWorkerPool | None = None
    if config.ocr_mode != "off":
        ocr_pool = OCRWorkerPool(workers=config.ocr_workers, backend=config.ocr_backend)
        if not ocr_pool.available():
            raise RuntimeError(
                f"OCR mode is enabled but {config.ocr_backend} is unavailable. "
                f"Install {config.ocr_backend} or run with --ocr-mode off."
            )

    def _consume_extract_result(result: dict[str, Any]) -> None:
        summary["processed"] += 1
        status = result.get("status", "failed")
        if status == "ok":
            summary["ok"] += 1
            summary["notes_extracted"] += int(result.get("notes") or 0)
            if result.get("ocr_used"):
                summary["ocr_used"] += 1
            parser_used = str(result.get("parser_used") or "unknown").strip() or "unknown"
            parser_counts = summary["parser_used_counts"]
            parser_counts[parser_used] = int(parser_counts.get(parser_used) or 0) + 1
            if result.get("balanced_fast_accept"):
                summary["pypdf_fast_accepts"] += 1
            if result.get("balanced_fallback_used"):
                summary["balanced_pdfplumber_fallbacks"] += 1
            if result.get("ordinality_patch_attempted"):
                summary["ordinality_patch_attempted_docs"] += 1
            if result.get("ordinality_patch_resolved"):
                summary["ordinality_patch_resolved_docs"] += 1
            summary["ordinality_patch_notes_added"] += int(
                result.get("ordinality_patch_added") or 0
            )
            if bool(result.get("needs_ocr_review")):
                summary["needs_ocr_review"] += 1
                ocr_review_rows.append(
                    {
                        "created_at": utc_now_iso(),
                        "source_pdf_path": str(result.get("pdf_path") or ""),
                        "sidecar_path": str(result.get("sidecar_path") or ""),
                        "parser_used": str(result.get("parser_used") or ""),
                        "ordinality_status": str(result.get("ordinality_status") or ""),
                        "notes": int(result.get("notes") or 0),
                        "ocr_review_reasons": [
                            str(item)
                            for item in (result.get("ocr_review_reasons") or [])
                            if str(item).strip()
                        ],
                        "warnings": [
                            str(item)
                            for item in (result.get("warnings") or [])
                            if str(item).strip()
                        ],
                    }
                )
        elif status == "skipped_existing":
            summary["skipped_existing"] += 1
        else:
            summary["failed"] += 1
        if report_detail == "full":
            summary["results"].append(result)

    try:
        with ThreadPoolExecutor(
            max_workers=max(1, int(config.classifier_workers))
        ) as classify_executor:
            with ThreadPoolExecutor(max_workers=max(1, int(config.workers))) as extract_executor:
                classify_futures: dict[Future[tuple[str, DocDecision]], str] = {
                    classify_executor.submit(
                        _classify_pdf_path,
                        pdf_path,
                        pdf_root=pdf_root,
                        doc_policy=config.doc_policy,
                        rules=rules,
                    ): pdf_path
                    for pdf_path in classify_targets
                }
                pending_extract: set[Future[dict[str, Any]]] = set()
                max_inflight_extract = max(8, max(1, int(config.workers)) * 2)

                classify_progress = tqdm(
                    as_completed(classify_futures),
                    total=len(classify_futures),
                    desc="Classifying PDFs",
                    unit="pdf",
                    mininterval=2.0,
                )
                for classify_future in classify_progress:
                    pdf_path, decision = classify_future.result()
                    summary["classify_processed"] += 1
                    if decision.include:
                        summary["eligible_pdfs"] += 1
                        pending_extract.add(
                            extract_executor.submit(
                                _extract_for_pdf,
                                pdf_path,
                                config,
                                ocr_pool,
                                dependency_versions_payload,
                                decision,
                                text_cache,
                            )
                        )
                    else:
                        if decision.doc_type in excluded_counts:
                            excluded_counts[decision.doc_type] += 1
                        excluded_rows.append(
                            {
                                "created_at": utc_now_iso(),
                                "source_pdf_path": pdf_path,
                                "domain": decision.domain,
                                "platform_family": decision.platform_family,
                                "doc_type": decision.doc_type,
                                "decision": "exclude",
                                "reason_codes": list(decision.reason_codes),
                                "rule_confidence": decision.confidence,
                                "ocr_candidate": decision.ocr_candidate,
                                "doc_policy": config.doc_policy,
                                "doc_rules_path": rules_path or "",
                            }
                        )

                    while len(pending_extract) >= max_inflight_extract:
                        done, pending_extract = wait(pending_extract, return_when=FIRST_COMPLETED)
                        for future in done:
                            _consume_extract_result(future.result())

                classify_progress.close()

                if pending_extract:
                    extract_progress = tqdm(
                        as_completed(pending_extract),
                        total=len(pending_extract),
                        desc="Extracting footnotes",
                        unit="pdf",
                        mininterval=2.0,
                    )
                    for future in extract_progress:
                        _consume_extract_result(future.result())
                    extract_progress.close()
    finally:
        if ocr_pool is not None:
            ocr_pool.close()

    summary["text_cache_hits"] = text_cache.hits
    summary["text_cache_misses"] = text_cache.misses
    summary["excluded_by_doc_policy"] = len(excluded_rows)
    summary["excluded_frontmatter"] = excluded_counts["frontmatter"]
    summary["excluded_issue_compilation"] = excluded_counts["issue_compilation"]
    summary["excluded_other"] = excluded_counts["other"]
    summary["run_elapsed_seconds"] = round(time.monotonic() - run_started_monotonic, 3)
    summary["status"] = _bool_to_status(summary["failed"] == 0)
    summary["finished_at"] = utc_now_iso()
    if report_detail != "full":
        summary["results_omitted"] = True
        summary["results"] = []

    if config.emit_doctype_manifest and excluded_rows:
        manifest_path = config.doctype_manifest_out or _default_doctype_manifest_path()
        _write_jsonl_atomic(manifest_path, excluded_rows)
        summary["doc_type_manifest_path"] = manifest_path

    if config.emit_ocr_review_manifest and ocr_review_rows:
        manifest_path = config.ocr_review_manifest_out or _default_ocr_review_manifest_path()
        _write_jsonl_atomic(manifest_path, ocr_review_rows)
        summary["ocr_review_manifest_path"] = manifest_path

    report_out = config.report_out or _default_report_path()
    _write_json_atomic(report_out, summary)
    summary["report_path"] = report_out
    return summary
