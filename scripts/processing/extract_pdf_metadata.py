#!/usr/bin/env python3
"""Extract title/authors/date/citation metadata from PDF front matter.

Modes:
- native_first_page: existing PyMuPDF heuristics on page 1
- olmocr_first2: OlmOCR over a temporary 2-page PDF (pages 1-2)

Updates records.jsonl metadata and mirrors extracted metadata into sidecars
(`document_metadata`) for traceability.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF
from pypdf import PdfReader, PdfWriter

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.cli import DEFAULT_PDF_ROOT, DEFAULT_RUNS_DIR  # noqa: E402
from offprint.pdf_footnotes.ocr_worker import OCRWorkerPool  # noqa: E402

# ---------------------------------------------------------------------------
# Shared parsing heuristics
# ---------------------------------------------------------------------------

_SECTION_STARTS = re.compile(
    r"^(TABLE OF CONTENTS|CONTENTS|INTRODUCTION|ABSTRACT|"
    r"I\.\s|PART [IV]+|BACKGROUND|OVERVIEW|PREFACE|FOREWORD)",
    re.I,
)
_FOOTNOTE_MARKERS = re.compile(r"[*†‡§¶∗✦]+$")
_BY_PREFIX = re.compile(r"^BY\s+", re.I)
_JUNK_LINE = re.compile(
    r"^(\d{1,4}\s*$"
    r"|Volume\s+\d|Vol\.\s*\d|No\.\s*\d"
    r"|Pages?\s+\d|Page\s+\d"
    r"|ISSN\s|DOI\s|http"
    r"|Permalink|Journal\b|Publication Date"
    r"|Copyright\s|©|All [Rr]ights"
    r"|File:\s|Created on:\s|Last Printed:\s"
    r"|Macro|\.docx?\s|\.doc\b"
    r")",
    re.I,
)
_JOURNAL_HEADER = re.compile(
    r"(LAW REVIEW|LAW JOURNAL|JOURNAL OF|QUARTERLY|LAW FORUM|" r"REVIEW OF|BULLETIN|LAW REPORTER)",
    re.I,
)
_MONTH_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},\s+(19|20)\d{2}\b",
    re.I,
)
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
_CITATION_CANDIDATE_RE = re.compile(
    r"\b\d{1,4}\s+[A-Z][A-Za-z.&'\-\s]{2,80}\sL\.\s*Rev\.\s+\d{1,5}\b",
    re.I,
)


FIELD_NAMES = ("title", "authors", "date", "citation")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _is_junk_line(line: str) -> bool:
    if not line or len(line.strip()) < 3:
        return True
    return bool(_JUNK_LINE.search(line))


def _looks_like_author_line(line: str) -> bool:
    stripped = line.rstrip("., ")
    if _FOOTNOTE_MARKERS.search(stripped):
        return True
    if _BY_PREFIX.match(line):
        return True
    return False


def _parse_authors(raw: str) -> list[str]:
    cleaned = _FOOTNOTE_MARKERS.sub("", raw).strip()
    cleaned = _BY_PREFIX.sub("", cleaned).strip()
    cleaned = cleaned.rstrip(".,;: ")

    if not cleaned or len(cleaned) < 4:
        return []

    parts = re.split(r"\s*(?:&|,?\s+and\s+)\s*", cleaned)
    expanded: list[str] = []
    for part in parts:
        expanded.extend(re.split(r",\s+(?=[A-Z])", part))

    authors: list[str] = []
    for name in expanded:
        name = name.strip().rstrip("*†‡§¶∗✦., ")
        if not name or len(name) < 4:
            continue
        words = name.split()
        if len(words) < 2 or len(words) > 7:
            continue
        if not words[0][0].isupper():
            continue
        low = name.lower()
        if any(w in low for w in ("table", "introduction", "abstract", "volume", "issue", "note")):
            continue
        authors.append(name)
    return authors


def _extract_date(text: str) -> str | None:
    month_match = _MONTH_RE.search(text)
    if month_match:
        return month_match.group(0)
    years = _YEAR_RE.findall(text)
    year_match = _YEAR_RE.search(text)
    if year_match:
        return year_match.group(0)
    if years:
        return years[0]
    return None


def _best_citation_line(lines: list[str]) -> str | None:
    best: tuple[int, str] | None = None
    for line in lines[:160]:
        txt = " ".join(line.split())
        if len(txt) < 20:
            continue
        score = 0
        if _CITATION_CANDIDATE_RE.search(txt):
            score += 4
        if "l. rev." in txt.lower() or "law review" in txt.lower() or "journal" in txt.lower():
            score += 2
        if _YEAR_RE.search(txt):
            score += 1
        if txt.lower().startswith("citation"):
            score += 1
        if score <= 0:
            continue
        if best is None or score > best[0] or (score == best[0] and len(txt) > len(best[1])):
            best = (score, txt)
    return best[1] if best else None


def _extract_title_and_authors(lines: list[str]) -> tuple[str | None, list[str]]:
    i = 0
    while i < len(lines):
        line = lines[i]
        if _is_junk_line(line):
            i += 1
            continue
        if _JOURNAL_HEADER.search(line) and len(line) < 90:
            i += 1
            continue
        break

    title_lines: list[str] = []
    while i < len(lines) and len(title_lines) < 6:
        line = lines[i]
        if _is_junk_line(line):
            i += 1
            continue
        if _SECTION_STARTS.match(line) or _looks_like_author_line(line):
            break
        if title_lines and line[:1].islower() and len(line) > 90:
            break
        title_lines.append(line)
        i += 1

    author_lines: list[str] = []
    while i < len(lines) and len(author_lines) < 4:
        line = lines[i]
        if _is_junk_line(line):
            i += 1
            continue
        if _SECTION_STARTS.match(line):
            break
        if _looks_like_author_line(line):
            author_lines.append(line)
            i += 1
            continue
        if author_lines and _parse_authors(line):
            author_lines.append(line)
            i += 1
            continue
        break

    title: str | None = None
    if title_lines:
        candidate = re.sub(r"\s+", " ", " ".join(title_lines)).strip()
        if len(candidate) > 10 and not candidate.startswith(("File:", "Microsoft Word")):
            title = candidate

    authors: list[str] = []
    for line in author_lines:
        authors.extend(_parse_authors(line))
    return title, authors


def _record_pdf_relative_path(record: dict[str, Any]) -> str:
    meta = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    rel = str(meta.get("pdf_relative_path") or "").strip()
    if rel:
        return rel
    domain = str(record.get("domain") or "").strip()
    filename = str(meta.get("pdf_filename") or "").strip()
    if domain and filename:
        return f"{domain}/{filename}"
    return ""


def _record_key(record: dict[str, Any]) -> str:
    rel = _record_pdf_relative_path(record)
    if rel:
        return rel
    return hashlib.sha1(json.dumps(record, sort_keys=True).encode("utf-8")).hexdigest()


def _path_in_shard(key: str, shard_count: int, shard_index: int) -> bool:
    if shard_count <= 1:
        return True
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return (int(digest[:16], 16) % shard_count) == shard_index


def extract_metadata_from_pdf(pdf_path: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    try:
        doc = fitz.open(pdf_path)
    except Exception:
        return result
    if doc.page_count == 0:
        doc.close()
        return result
    try:
        text = doc[0].get_text()
    except Exception:
        text = ""
    doc.close()
    if not text or len(text.strip()) < 20:
        return result

    lines = [line.strip() for line in text.split("\n") if line.strip()]
    title, authors = _extract_title_and_authors(lines)
    date = _extract_date(text)
    citation = _best_citation_line(lines)

    if title:
        result["title"] = title
    if authors:
        result["authors"] = authors
    if date:
        result["date"] = date
    if citation:
        result["citation"] = citation
    return result


def _build_two_page_pdf(pdf_path: str) -> tuple[str | None, list[int]]:
    reader = PdfReader(pdf_path)
    if len(reader.pages) <= 0:
        return None, []
    writer = PdfWriter()
    selected = [1, 2][: len(reader.pages)]
    for p in selected:
        writer.add_page(reader.pages[p - 1])

    tmp = tempfile.NamedTemporaryFile(prefix="meta_first2_", suffix=".pdf", delete=False)
    out_path = tmp.name
    with tmp:
        writer.write(tmp)
    return out_path, selected


def extract_metadata_from_olmocr_first2(pdf_path: str, ocr_pool: OCRWorkerPool) -> dict[str, Any]:
    out: dict[str, Any] = {}
    temp_pdf, selected_pages = _build_two_page_pdf(pdf_path)
    if not temp_pdf:
        return out
    try:
        ocr_doc, ocr_warnings = ocr_pool.extract_document(temp_pdf, page_numbers=[1, 2])
        if ocr_doc is None or not ocr_doc.pages:
            return out
        markdown = str(ocr_doc.pages[0].raw_text or "")
        lines = [line.strip() for line in markdown.splitlines() if line.strip()]
        title, authors = _extract_title_and_authors(lines)
        date = _extract_date(markdown)
        citation = _best_citation_line(lines)

        confidence: dict[str, float] = {
            "title": 0.85 if title else 0.0,
            "authors": 0.80 if authors else 0.0,
            "date": 0.72 if date else 0.0,
            "citation": 0.72 if citation else 0.0,
        }

        if title:
            out["title"] = title
        if authors:
            out["authors"] = authors
        if date:
            out["date"] = date
        if citation:
            out["citation"] = citation

        if any(confidence.values()):
            out["_field_confidence"] = confidence
            out["_source"] = "olmocr_first2"
            out["_source_pages"] = selected_pages
            if ocr_warnings:
                out["_warnings"] = [str(w) for w in ocr_warnings]
        return out
    finally:
        try:
            os.remove(temp_pdf)
        except OSError:
            pass


def _field_value(meta: dict[str, Any], field: str) -> Any:
    if field == "authors":
        value = meta.get("authors")
        return value if isinstance(value, list) and value else []
    return str(meta.get(field) or "").strip()


def _should_set_field(
    *,
    current_meta: dict[str, Any],
    field: str,
    new_value: Any,
    new_conf: float,
    overwrite_policy: str,
) -> bool:
    current_value = _field_value(current_meta, field)
    has_current = bool(current_value)
    if not has_current:
        return True
    if overwrite_policy == "always":
        return True
    if overwrite_policy == "higher_confidence":
        cur_conf = float(current_meta.get(f"{field}_confidence") or 0.0)
        return new_conf > cur_conf
    return False


def _build_metadata_patch(
    *,
    current_meta: dict[str, Any],
    extracted: dict[str, Any],
    overwrite_policy: str,
) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    field_conf = (
        extracted.get("_field_confidence")
        if isinstance(extracted.get("_field_confidence"), dict)
        else {}
    )
    source = str(extracted.get("_source") or "")

    for field in FIELD_NAMES:
        if field not in extracted:
            continue
        new_value = extracted[field]
        new_conf = float(field_conf.get(field) or 0.0)
        if not _should_set_field(
            current_meta=current_meta,
            field=field,
            new_value=new_value,
            new_conf=new_conf,
            overwrite_policy=overwrite_policy,
        ):
            continue
        patch[field] = new_value
        if source:
            patch[f"{field}_source"] = source
        if new_conf > 0:
            patch[f"{field}_confidence"] = round(new_conf, 3)

    if patch and source:
        patch["metadata_source"] = source
    return patch


def _merge_patch_into_metadata(meta: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    out = dict(meta)
    out.update(patch)
    return out


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _update_sidecar_metadata(
    sidecar_path: Path, patch: dict[str, Any], source_pages: list[int]
) -> bool:
    if not sidecar_path.exists():
        return False
    try:
        payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False

    doc_meta = (
        payload.get("document_metadata")
        if isinstance(payload.get("document_metadata"), dict)
        else {}
    )
    doc_meta = dict(doc_meta)

    changed = False
    for field in FIELD_NAMES:
        if field in patch:
            if doc_meta.get(field) != patch[field]:
                doc_meta[field] = patch[field]
                changed = True
            src_key = f"{field}_source"
            conf_key = f"{field}_confidence"
            if src_key in patch and doc_meta.get(src_key) != patch[src_key]:
                doc_meta[src_key] = patch[src_key]
                changed = True
            if conf_key in patch and doc_meta.get(conf_key) != patch[conf_key]:
                doc_meta[conf_key] = patch[conf_key]
                changed = True

    if not changed and payload.get("document_metadata"):
        return False

    doc_meta["source"] = "olmocr_first2"
    doc_meta["source_pages"] = list(source_pages or [1, 2])
    payload["document_metadata"] = doc_meta
    _write_json_atomic(sidecar_path, payload)
    return True


def _extract_for_record(
    *,
    record: dict[str, Any],
    pdfs_dir: str,
    engine: str,
    ocr_pool: OCRWorkerPool | None,
) -> tuple[str, dict[str, Any], str, list[int], str | None]:
    key = _record_key(record)
    rel = _record_pdf_relative_path(record)
    if not rel:
        return key, {}, "", [], "missing_relative_path"
    full_path = os.path.join(pdfs_dir, rel)
    if not os.path.exists(full_path):
        return key, {}, "", [], "missing_pdf"

    if engine == "native_first_page":
        extracted = extract_metadata_from_pdf(full_path)
    else:
        if ocr_pool is None:
            return key, {}, full_path, [], "ocr_pool_unavailable"
        extracted = extract_metadata_from_olmocr_first2(full_path, ocr_pool)

    source_pages = (
        extracted.get("_source_pages") if isinstance(extracted.get("_source_pages"), list) else []
    )
    extracted.pop("_source_pages", None)
    extracted.pop("_warnings", None)
    return key, extracted, full_path, source_pages, None


def _apply_patches_with_lock(
    *,
    records_path: Path,
    lock_path: Path,
    patches: dict[str, dict[str, Any]],
) -> tuple[int, dict[str, dict[str, Any]], list[dict[str, Any]]]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)

        all_records: list[dict[str, Any]] = []
        with records_path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    all_records.append(json.loads(line))

        enriched = 0
        applied_patches: dict[str, dict[str, Any]] = {}
        changed_rows: list[dict[str, Any]] = []
        for idx, record in enumerate(all_records):
            key = _record_key(record)
            patch = patches.get(key)
            if not patch:
                continue
            meta = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
            merged = _merge_patch_into_metadata(meta, patch)
            if merged == meta:
                continue
            record["metadata"] = merged
            all_records[idx] = record
            enriched += 1
            applied_patches[key] = patch
            changed_rows.append(record)

        if enriched > 0:
            backup_path = records_path.with_suffix(records_path.suffix + ".pre-olmocr-metadata.bak")
            if not backup_path.exists():
                records_path.replace(backup_path)
            tmp = records_path.with_suffix(records_path.suffix + ".tmp")
            with tmp.open("w", encoding="utf-8") as out:
                for record in all_records:
                    out.write(json.dumps(record, ensure_ascii=False) + "\n")
            tmp.replace(records_path)

        return enriched, applied_patches, changed_rows
    finally:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract/enrich PDF metadata from front matter.")
    parser.add_argument("--run-id", required=True, help="Run ID")
    parser.add_argument("--manifest-dir", default=DEFAULT_RUNS_DIR, help="Manifest directory")
    parser.add_argument("--pdfs-dir", default=DEFAULT_PDF_ROOT, help="PDFs directory")
    parser.add_argument(
        "--engine", choices=["native_first_page", "olmocr_first2"], default="native_first_page"
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--overwrite-policy",
        choices=["fill_gaps_only", "higher_confidence", "always"],
        default="fill_gaps_only",
    )
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument(
        "--lock-path", default="", help="Optional lock path for records.jsonl updates"
    )
    parser.add_argument("--report-out", default="", help="Optional report output path")
    parser.add_argument("--dry-run", action="store_true", help="Do not write records/sidecars")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.shard_count <= 0:
        raise SystemExit("--shard-count must be >= 1")
    if args.shard_index < 0 or args.shard_index >= args.shard_count:
        raise SystemExit("--shard-index must be in [0, shard-count)")

    run_dir = Path(args.manifest_dir) / args.run_id
    records_path = run_dir / "records.jsonl"
    if not records_path.exists():
        raise SystemExit(f"Records file not found: {records_path}")

    records: list[dict[str, Any]] = []
    with records_path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    candidates: list[dict[str, Any]] = []
    for record in records:
        key = _record_key(record)
        if _path_in_shard(key, args.shard_count, args.shard_index):
            candidates.append(record)

    ocr_pool: OCRWorkerPool | None = None
    if args.engine == "olmocr_first2":
        ocr_pool = OCRWorkerPool(workers=max(1, int(args.workers)), backend="olmocr")
        if not ocr_pool.available():
            raise SystemExit(
                "OlmOCR unavailable. Install/configure olmocr before running --engine olmocr_first2."
            )

    extracted_total = 0
    errors_total = 0
    extraction_errors: list[dict[str, Any]] = []
    patches: dict[str, dict[str, Any]] = {}
    path_by_key: dict[str, str] = {}
    source_pages_by_key: dict[str, list[int]] = {}

    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                _extract_for_record,
                record=record,
                pdfs_dir=args.pdfs_dir,
                engine=args.engine,
                ocr_pool=ocr_pool,
            ): record
            for record in candidates
        }
        for future in as_completed(futures):
            key, extracted, full_path, source_pages, err = future.result()
            if err:
                errors_total += 1
                extraction_errors.append({"key": key, "error": err})
                continue
            extracted_total += 1
            if not extracted:
                continue
            record = futures[future]
            current_meta = (
                record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
            )
            patch = _build_metadata_patch(
                current_meta=current_meta,
                extracted=extracted,
                overwrite_policy=args.overwrite_policy,
            )
            if patch:
                patches[key] = patch
                path_by_key[key] = full_path
                source_pages_by_key[key] = source_pages or [1, 2]

    if ocr_pool is not None:
        ocr_pool.close()

    enriched = 0
    sidecars_updated = 0
    if not args.dry_run and patches:
        lock_path = (
            Path(args.lock_path)
            if args.lock_path
            else records_path.with_suffix(records_path.suffix + ".lock")
        )
        enriched, applied_patches, _changed_rows = _apply_patches_with_lock(
            records_path=records_path,
            lock_path=lock_path,
            patches=patches,
        )
        for key, patch in applied_patches.items():
            full_path = path_by_key.get(key)
            if not full_path:
                continue
            sidecar_path = Path(f"{full_path}.footnotes.json")
            if _update_sidecar_metadata(sidecar_path, patch, source_pages_by_key.get(key, [1, 2])):
                sidecars_updated += 1

    report = {
        "run_id": args.run_id,
        "engine": args.engine,
        "overwrite_policy": args.overwrite_policy,
        "shard_count": int(args.shard_count),
        "shard_index": int(args.shard_index),
        "records_total": len(records),
        "records_in_shard": len(candidates),
        "records_extracted": extracted_total,
        "records_with_metadata_patch": len(patches),
        "records_enriched_written": enriched,
        "sidecars_updated": sidecars_updated,
        "errors_total": errors_total,
        "errors_sample": extraction_errors[:50],
        "dry_run": bool(args.dry_run),
        "finished_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    report_path = (
        Path(args.report_out)
        if args.report_out
        else Path(DEFAULT_RUNS_DIR) / f"pdf_metadata_enrich_{args.engine}_{_utc_stamp()}.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"status": "ok", "report": str(report_path), **report}, indent=2))


if __name__ == "__main__":
    main()
