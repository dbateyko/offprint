#!/usr/bin/env python3
"""Build Hugging Face-ready parquet configs for fulltext + footnotes.

This script materializes two dataset configs under `hf/`:
- `hf/text/fulltext/part-*.parquet`
- `hf/footnotes/footnotes/part-*.parquet`

OCR policy:
- `--ocr-mode off`: never run OCR.
- `--ocr-mode fallback|always`: requires `olmocr`; fail-fast if unavailable.

Screen/tmux usage:
    screen -S hf-build
    python scripts/build_hf_dataset.py --run-id <id> --ocr-mode off --workers 12 2>&1 | tee hf_build.log
    # Ctrl-A D to detach; screen -r hf-build to reattach
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tqdm import tqdm

log = logging.getLogger("build_hf_dataset")

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.cli import DEFAULT_PDF_ROOT, DEFAULT_RUNS_DIR
from offprint.pdf_footnotes.doc_policy import (
    DocDecision,
    classify_pdf,
    collect_signals,
    default_rules_path,
    infer_domain,
    infer_platform_family,
    load_rules,
    read_first_page_overview,
)
from offprint.pdf_footnotes.ocr_worker import OCRWorkerPool
from offprint.pdf_footnotes.citation_classify import enrich_note_features
from offprint.pdf_footnotes.context_link import attach_context_batch
from offprint.pdf_footnotes.note_segment import segment_document_notes_extended
from offprint.pdf_footnotes.text_cache import TextExtractionCache
from offprint.pdf_footnotes.text_extract import ExtractedDocument, extract_document_text


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _find_latest_run(runs_dir: Path) -> Path | None:
    candidates = [path for path in runs_dir.iterdir() if path.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.name)


def _iter_records(records_path: Path) -> Any:
    with records_path.open(encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except json.JSONDecodeError:
                continue


def _looks_downloaded(record: dict[str, Any]) -> bool:
    if str(record.get("download_state") or "").lower() == "downloaded":
        return True
    if bool(record.get("ok")) and record.get("local_path"):
        return True
    return False


def _normalize_year(meta: dict[str, Any]) -> str | None:
    raw = meta.get("year") or meta.get("publication_date") or meta.get("date")
    if raw is None:
        return None
    return str(raw)


def _resolve_pdf_relative(record: dict[str, Any], pdf_root: Path) -> str:
    meta = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    relative = (
        meta.get("pdf_relative_path") or meta.get("pdf_filename") or record.get("local_path") or ""
    )
    if not isinstance(relative, str):
        return ""

    text = relative.strip()
    if not text:
        return ""

    if text.startswith(str(pdf_root) + os.sep):
        return os.path.relpath(text, pdf_root)
    if text.startswith("artifacts/pdfs/"):
        return os.path.relpath(text, "artifacts/pdfs")
    return text


def _resolve_pdf_path(pdf_root: Path, pdf_relative_path: str) -> Path:
    candidate = Path(pdf_relative_path)
    if candidate.is_absolute():
        return candidate
    return pdf_root / pdf_relative_path


def _stable_doc_id(pdf_sha256: str | None, pdf_relative_path: str) -> str:
    key = pdf_sha256 or pdf_relative_path
    return hashlib.sha1(str(key).encode("utf-8")).hexdigest()[:24]


_CASE_CITE_RE = re.compile(
    r"\b\d{1,4}\s+(?:U\.S\.|S\.Ct\.|F\.?\s?Supp\.?\s?\d*|F\.?\s?(?:2d|3d)?)\s+\d{1,5}\b"
)
_USC_RE = re.compile(r"\b\d+\s+U\.S\.C\.\s*§+\s*[\w.\-()]+\b")
_CFR_RE = re.compile(r"\b\d+\s+C\.F\.R\.\s*§+\s*[\w.\-()]+\b")
_SECTION_RE = re.compile(
    r"^(?:INTRODUCTION|CONCLUSION|ABSTRACT|PART\s+[IVXLC]+|[IVXLC]+\.\s+|SECTION\s+\d+)\b",
    re.I,
)


def _extract_citations(text: str, max_items: int = 200) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for pattern in (_CASE_CITE_RE, _USC_RE, _CFR_RE):
        for match in pattern.finditer(text):
            value = match.group(0).strip()
            if value and value not in seen:
                seen.add(value)
                ordered.append(value)
                if len(ordered) >= max_items:
                    return ordered
    return ordered


def _extract_section_headers(text: str, max_items: int = 40) -> list[str]:
    seen: set[str] = set()
    headers: list[str] = []
    for raw in text.splitlines():
        line = " ".join(raw.split()).strip()
        if not line or len(line) > 120:
            continue
        word_count = len(line.split())
        if word_count < 1 or word_count > 16:
            continue
        looks_upper = line.isupper() and any(ch.isalpha() for ch in line)
        if not (looks_upper or _SECTION_RE.search(line)):
            continue
        if line not in seen:
            seen.add(line)
            headers.append(line)
        if len(headers) >= max_items:
            break
    return headers


def _document_text(document: ExtractedDocument) -> str:
    pages = [page.raw_text for page in document.pages if (page.raw_text or "").strip()]
    return "\n\n".join(pages).strip()


def _ocr_recommended_for_text(document: ExtractedDocument) -> bool:
    if not document.pages:
        return True
    return document.total_text_chars < 600


_DEFAULT_DONE_SHAS_PATH = Path("hf/metadata/done_shas.txt")


def _load_done_shas(path: Path) -> set[str]:
    """Load set of pdf_sha256 values already written to parquet."""
    if not path.exists():
        return set()
    shas = set()
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            sha = line.strip()
            if sha:
                shas.add(sha)
    log.info("Loaded %d already-processed SHAs from %s", len(shas), path)
    return shas


def _append_done_shas(path: Path, shas: list[str]) -> None:
    """Append newly processed SHA values to the sidecar file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for sha in shas:
            if sha:
                fh.write(sha + "\n")


@dataclass
class BuildConfig:
    records_path: Path
    pdf_root: Path
    hf_dir: Path
    run_id: str
    workers: int
    limit: int
    ocr_mode: str
    ocr_backend: str
    rows_per_shard: int
    max_text_chars: int
    doc_policy: str
    doc_rules_path: str | None
    include_issue_compilations_when_no_articles: bool
    emit_doctype_manifest: bool
    doctype_manifest_out: str | None
    inline_footnotes: bool = False
    incremental: bool = False
    done_shas_path: Path = _DEFAULT_DONE_SHAS_PATH
    text_cache_enabled: bool = True


@dataclass
class _PreparedRecord:
    record: dict[str, Any]
    doc_decision: DocDecision
    pdf_relative_path: str
    pdf_path: Path
    domain: str
    issue_key: str


class _ParquetShardWriter:
    def __init__(self, out_dir: Path, prefix: str, rows_per_shard: int) -> None:
        self.out_dir = out_dir
        self.prefix = prefix
        self.rows_per_shard = max(1, int(rows_per_shard))
        self.buffer: list[dict[str, Any]] = []
        self.shards = 0
        self.rows = 0
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def write_row(self, row: dict[str, Any]) -> None:
        self.buffer.append(row)
        if len(self.buffer) >= self.rows_per_shard:
            self._flush()

    def close(self) -> None:
        self._flush()

    def _flush(self) -> None:
        if not self.buffer:
            return
        try:
            import pyarrow as pa  # type: ignore
            import pyarrow.parquet as pq  # type: ignore
        except Exception as exc:
            raise RuntimeError("pyarrow is required to build HF parquet datasets") from exc

        self.shards += 1
        out_path = self.out_dir / f"{self.prefix}-{self.shards:05d}.parquet"
        table = pa.Table.from_pylist(self.buffer)
        pq.write_table(table, out_path, compression="zstd")
        self.rows += len(self.buffer)
        self.buffer = []


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _default_doctype_manifest_path(hf_dir: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return hf_dir / "metadata" / f"hf_doc_type_exclusions_{stamp}.jsonl"


def _issue_key(record: dict[str, Any], domain: str) -> str:
    meta = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    journal = str(meta.get("journal") or "").strip().lower()
    volume = str(meta.get("volume") or "").strip().lower()
    issue = str(meta.get("issue") or "").strip().lower()
    year = str(_normalize_year(meta) or "").strip().lower()
    key = "|".join([domain.lower(), journal, volume, issue, year])
    return key if any(part for part in [journal, volume, issue, year]) else ""


def _build_fulltext_row(
    prepared: _PreparedRecord,
    config: BuildConfig,
    ocr_pool: OCRWorkerPool | None,
    text_cache: TextExtractionCache | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    record = prepared.record
    meta = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    pdf_relative_path = prepared.pdf_relative_path
    pdf_path = prepared.pdf_path
    doc_decision = prepared.doc_decision

    cached = text_cache.get(str(pdf_path)) if text_cache else None
    if cached is not None:
        document = cached
    else:
        document = extract_document_text(str(pdf_path))
        if text_cache:
            text_cache.put(str(pdf_path), document)
    warnings = list(document.warnings)
    ocr_used = False
    extraction_method = "native"
    ocr_trigger_reason = ""

    if config.ocr_mode != "off" and ocr_pool is not None:
        should_ocr = (
            config.ocr_mode == "always"
            or _ocr_recommended_for_text(document)
            or bool(doc_decision.ocr_candidate)
        )
        if should_ocr:
            if config.ocr_mode == "always":
                ocr_trigger_reason = "mode_always"
            elif doc_decision.ocr_candidate:
                ocr_trigger_reason = "doc_policy_candidate"
            else:
                ocr_trigger_reason = "native_low_text"
            ocr_document, ocr_warnings = ocr_pool.extract_document(str(pdf_path))
            warnings.extend(ocr_warnings)
            if ocr_document and (
                config.ocr_mode == "always"
                or ocr_document.total_text_chars > document.total_text_chars
            ):
                document = ocr_document
                ocr_used = True
                extraction_method = "olmocr"

    text = _document_text(document)
    if config.max_text_chars > 0 and len(text) > config.max_text_chars:
        text = text[: config.max_text_chars]
        warnings.append("text_truncated")

    citations = _extract_citations(text)
    section_headers = _extract_section_headers(text)
    pdf_sha256 = record.get("pdf_sha256")
    doc_id = _stable_doc_id(
        str(pdf_sha256) if isinstance(pdf_sha256, str) and pdf_sha256 else None,
        pdf_relative_path,
    )

    row = {
        "doc_id": doc_id,
        "run_id": config.run_id,
        "pdf_sha256": pdf_sha256,
        "pdf_relative_path": pdf_relative_path,
        "domain": prepared.domain,
        "doc_type": doc_decision.doc_type,
        "doc_type_reason_codes": list(doc_decision.reason_codes),
        "doc_type_confidence": doc_decision.confidence,
        "platform_family": doc_decision.platform_family,
        "seed_url": record.get("seed_url"),
        "page_url": record.get("page_url"),
        "pdf_url": record.get("pdf_url"),
        "title": meta.get("title"),
        "authors": meta.get("authors"),
        "journal": meta.get("journal"),
        "volume": meta.get("volume"),
        "issue": meta.get("issue"),
        "year": _normalize_year(meta),
        "text": text,
        "char_count": len(text),
        "page_count": document.page_count,
        "ocr_used": ocr_used,
        "ocr_backend": "olmocr" if ocr_used else None,
        "ocr_trigger_reason": ocr_trigger_reason or None,
        "extraction_method": extraction_method,
        "warnings": sorted(set(str(w) for w in warnings if str(w).strip())),
        "citations": citations,
        "section_headers": section_headers,
        "built_at": _utc_now_iso(),
    }

    footnote_rows: list[dict[str, Any]] = []
    sidecar_path = Path(f"{pdf_path}.footnotes.json")
    use_sidecar = sidecar_path.exists()
    if not use_sidecar and config.inline_footnotes:
        # Extract footnotes inline from the already-parsed document
        try:
            notes_list, _author_notes, _ordinality, _note_warnings = (
                segment_document_notes_extended(document)
            )
            attach_context_batch(notes_list, document)
            for note in notes_list:
                enrich_note_features(note, preset="legal")
            for idx, note in enumerate(notes_list, start=1):
                citation_mentions = [cm.text for cm in note.citation_mentions]
                footnote_rows.append(
                    {
                        "doc_id": doc_id,
                        "run_id": config.run_id,
                        "note_id": f"{doc_id}:{idx}",
                        "ordinal": idx,
                        "label": note.label,
                        "note_type": note.note_type,
                        "text": note.text,
                        "context_sentence": note.context_sentence,
                        "context_page": note.context_page,
                        "page_start": note.page_start,
                        "page_end": note.page_end,
                        "confidence": note.confidence,
                        "quality_flags": note.quality_flags,
                        "citation_mentions": citation_mentions,
                        "pdf_relative_path": pdf_relative_path,
                        "domain": prepared.domain,
                        "doc_type": doc_decision.doc_type,
                        "platform_family": doc_decision.platform_family,
                        "journal": meta.get("journal"),
                        "year": _normalize_year(meta),
                        "source_sidecar": "inline",
                        "built_at": _utc_now_iso(),
                    }
                )
        except Exception:
            log.debug("Inline footnote extraction failed for %s", pdf_path, exc_info=True)
    if use_sidecar:
        try:
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        except Exception:
            sidecar = {}
        notes = sidecar.get("notes") if isinstance(sidecar, dict) else []
        if isinstance(notes, dict):
            # Sidecar writes notes as dict keyed by label; normalize to list
            notes = [{"label": k, **v} for k, v in notes.items()]
        if isinstance(notes, list):
            for idx, note in enumerate(notes, start=1):
                if not isinstance(note, dict):
                    continue
                ordinal = note.get("ordinal") or idx
                citations_payload = note.get("citation_mentions")
                citation_mentions: list[str] = []
                if isinstance(citations_payload, list):
                    for item in citations_payload:
                        if isinstance(item, dict) and item.get("text"):
                            citation_mentions.append(str(item.get("text")))
                footnote_rows.append(
                    {
                        "doc_id": doc_id,
                        "run_id": config.run_id,
                        "note_id": f"{doc_id}:{ordinal}",
                        "ordinal": ordinal,
                        "label": note.get("label"),
                        "note_type": note.get("note_type"),
                        "text": note.get("text"),
                        "context_sentence": note.get("context_sentence"),
                        "context_page": note.get("context_page"),
                        "page_start": note.get("page_start"),
                        "page_end": note.get("page_end"),
                        "confidence": note.get("confidence"),
                        "quality_flags": note.get("quality_flags"),
                        "citation_mentions": citation_mentions,
                        "pdf_relative_path": pdf_relative_path,
                        "domain": prepared.domain,
                        "doc_type": doc_decision.doc_type,
                        "platform_family": doc_decision.platform_family,
                        "journal": meta.get("journal"),
                        "year": _normalize_year(meta),
                        "source_sidecar": str(sidecar_path),
                        "built_at": _utc_now_iso(),
                    }
                )

    return row, footnote_rows


def build_hf_dataset(config: BuildConfig) -> dict[str, Any]:
    if not config.records_path.exists():
        raise RuntimeError(f"records.jsonl not found: {config.records_path}")
    if not config.pdf_root.exists():
        raise RuntimeError(f"pdf_root not found: {config.pdf_root}")

    rules = load_rules(config.doc_rules_path or default_rules_path())

    # Incremental mode: skip SHAs already written in a previous build
    done_shas: set[str] = set()
    if config.incremental:
        done_shas = _load_done_shas(config.done_shas_path)

    prepared_records: list[_PreparedRecord] = []
    excluded_rows: list[dict[str, Any]] = []
    article_issue_keys: set[str] = set()
    total_seen = 0
    skipped_incremental = 0

    for record in _iter_records(config.records_path):
        if config.limit > 0 and total_seen >= config.limit:
            break
        total_seen += 1
        if not _looks_downloaded(record):
            continue

        # Skip already-processed PDFs in incremental mode
        if config.incremental and record.get("pdf_sha256") in done_shas:
            skipped_incremental += 1
            continue
        pdf_relative_path = _resolve_pdf_relative(record, config.pdf_root)
        if not pdf_relative_path:
            continue
        pdf_path = _resolve_pdf_path(config.pdf_root, pdf_relative_path)
        if not pdf_path.exists():
            continue

        meta = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        domain = str(record.get("domain") or "").strip().lower() or infer_domain(
            str(pdf_path), pdf_root=str(config.pdf_root)
        )
        platform_family = infer_platform_family(
            domain=domain,
            source_adapter=str(record.get("source_adapter") or ""),
            platform_raw=str(meta.get("platform") or ""),
        )
        page_count, first_page_text = read_first_page_overview(str(pdf_path))
        signals = collect_signals(first_page_text, page_count, meta)
        decision = classify_pdf(
            pdf_path=str(pdf_path),
            domain=domain,
            platform_family=platform_family,
            signals=signals,
            doc_policy=config.doc_policy,
            rules=rules,
        )
        issue_key = _issue_key(record, domain)
        prepared = _PreparedRecord(
            record=record,
            doc_decision=decision,
            pdf_relative_path=pdf_relative_path,
            pdf_path=pdf_path,
            domain=domain,
            issue_key=issue_key,
        )
        prepared_records.append(prepared)
        if decision.doc_type == "article" and issue_key:
            article_issue_keys.add(issue_key)

    selected_records: list[_PreparedRecord] = []
    for prepared in prepared_records:
        decision = prepared.doc_decision
        include = decision.include
        reason_codes = list(decision.reason_codes)

        # Optional fallback lane: keep issue PDFs only when issue has no article-level PDF.
        if (
            not include
            and decision.doc_type == "issue_compilation"
            and config.include_issue_compilations_when_no_articles
            and prepared.issue_key
            and prepared.issue_key not in article_issue_keys
        ):
            include = True
            reason_codes.append("issue_fallback_no_articles")

        if include:
            selected_records.append(prepared)
            continue

        excluded_rows.append(
            {
                "created_at": _utc_now_iso(),
                "run_id": config.run_id,
                "source_pdf_path": str(prepared.pdf_path),
                "pdf_relative_path": prepared.pdf_relative_path,
                "domain": prepared.domain,
                "platform_family": decision.platform_family,
                "doc_type": decision.doc_type,
                "decision": "exclude",
                "reason_codes": sorted(set(reason_codes)),
                "rule_confidence": decision.confidence,
                "ocr_candidate": decision.ocr_candidate,
                "doc_policy": config.doc_policy,
                "doc_rules_path": config.doc_rules_path or default_rules_path(),
            }
        )

    text_cache = TextExtractionCache(enabled=config.text_cache_enabled)

    ocr_pool: OCRWorkerPool | None = None
    if config.ocr_mode != "off":
        ocr_pool = OCRWorkerPool(workers=max(1, config.workers), backend=config.ocr_backend)
        if not ocr_pool.available():
            raise RuntimeError(
                "OCR mode is enabled but olmocr is unavailable. "
                "Install olmocr or run with --ocr-mode off."
            )

    fulltext_writer = _ParquetShardWriter(
        out_dir=config.hf_dir / "text" / "fulltext",
        prefix="part",
        rows_per_shard=config.rows_per_shard,
    )
    footnotes_writer = _ParquetShardWriter(
        out_dir=config.hf_dir / "footnotes" / "footnotes",
        prefix="part",
        rows_per_shard=config.rows_per_shard,
    )

    total_errors = 0
    error_details: list[dict[str, str]] = []
    new_shas: list[str] = []

    if config.incremental:
        log.info("Incremental mode: skipped %d already-processed PDFs", skipped_incremental)
    log.info(
        "Processing %d selected records (%d excluded by doc policy) with %d workers",
        len(selected_records),
        len(excluded_rows),
        config.workers,
    )
    t0 = time.time()

    try:
        with ThreadPoolExecutor(max_workers=max(1, config.workers)) as executor:
            future_to_prepared: dict[Any, _PreparedRecord] = {}
            for prepared in selected_records:
                fut = executor.submit(_build_fulltext_row, prepared, config, ocr_pool, text_cache)
                future_to_prepared[fut] = prepared

            progress = tqdm(
                as_completed(future_to_prepared),
                total=len(future_to_prepared),
                desc="Extracting text",
                unit="pdf",
                mininterval=2.0,
            )
            for future in progress:
                prepared = future_to_prepared[future]
                try:
                    full_row, notes_rows = future.result()
                except Exception as exc:
                    total_errors += 1
                    detail = {
                        "pdf_path": str(prepared.pdf_path),
                        "domain": prepared.domain,
                        "error": str(exc)[:200],
                    }
                    error_details.append(detail)
                    log.debug("Error processing %s: %s", prepared.pdf_path, exc)
                    continue
                if full_row is not None:
                    fulltext_writer.write_row(full_row)
                    new_shas.append(str(full_row.get("pdf_sha256") or ""))
                for note_row in notes_rows:
                    footnotes_writer.write_row(note_row)
    finally:
        if ocr_pool is not None:
            ocr_pool.close()
        fulltext_writer.close()
        footnotes_writer.close()
        # Persist done SHAs for incremental re-runs
        if new_shas:
            _append_done_shas(config.done_shas_path, new_shas)
            log.info("Appended %d new SHAs to %s", len(new_shas), config.done_shas_path)

    doctype_manifest_path = ""
    if config.emit_doctype_manifest and excluded_rows:
        manifest_path = (
            Path(config.doctype_manifest_out)
            if config.doctype_manifest_out
            else _default_doctype_manifest_path(config.hf_dir)
        )
        _write_jsonl(manifest_path, excluded_rows)
        doctype_manifest_path = str(manifest_path)

    log.info("Text cache: %d hits, %d misses", text_cache.hits, text_cache.misses)
    elapsed = time.time() - t0
    log.info(
        "Extraction complete: %d fulltext rows, %d footnote rows, %d errors in %.1fs",
        fulltext_writer.rows,
        footnotes_writer.rows,
        total_errors,
        elapsed,
    )
    if error_details:
        log.warning(
            "Top errors by domain: %s",
            json.dumps(
                sorted(
                    {
                        d["domain"]: sum(1 for e in error_details if e["domain"] == d["domain"])
                        for d in error_details
                    }.items(),
                    key=lambda kv: kv[1],
                    reverse=True,
                )[:10],
            ),
        )

    # Write error log for debugging
    if error_details:
        error_log_path = config.hf_dir / "metadata" / "hf_extraction_errors.jsonl"
        error_log_path.parent.mkdir(parents=True, exist_ok=True)
        with error_log_path.open("w", encoding="utf-8") as fh:
            for detail in error_details:
                fh.write(json.dumps(detail, sort_keys=True) + "\n")
        log.info("Wrote %d error details to %s", len(error_details), error_log_path)

    summary = {
        "run_id": config.run_id,
        "records_seen": total_seen,
        "records_skipped_incremental": skipped_incremental,
        "processing_errors": total_errors,
        "records_selected": len(selected_records),
        "records_excluded_doc_policy": len(excluded_rows),
        "fulltext_rows": fulltext_writer.rows,
        "fulltext_shards": fulltext_writer.shards,
        "footnote_rows": footnotes_writer.rows,
        "footnote_shards": footnotes_writer.shards,
        "doc_policy": config.doc_policy,
        "doc_rules_path": config.doc_rules_path or default_rules_path(),
        "doc_type_manifest_path": doctype_manifest_path,
        "ocr_mode": config.ocr_mode,
        "ocr_backend": config.ocr_backend,
        "elapsed_seconds": round(elapsed, 1),
        "generated_at": _utc_now_iso(),
    }
    summary_path = config.hf_dir / "metadata" / "hf_dataset_build_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build HF parquet dataset configs.")
    parser.add_argument("--runs-dir", default=DEFAULT_RUNS_DIR)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--pdf-root", default=DEFAULT_PDF_ROOT)
    parser.add_argument("--hf-dir", default="hf")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--rows-per-shard", type=int, default=2000)
    parser.add_argument("--max-text-chars", type=int, default=300000)
    parser.add_argument("--ocr-mode", choices=["off", "fallback", "always"], default="fallback")
    parser.add_argument("--ocr-backend", choices=["olmocr"], default="olmocr")
    parser.add_argument(
        "--doc-policy",
        choices=["article_only", "include_issue_compilations", "all"],
        default="article_only",
    )
    parser.add_argument("--doc-rules-path", default="")
    parser.add_argument(
        "--include-issue-compilations-when-no-articles",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--emit-doctype-manifest",
        default=True,
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument("--doctype-manifest-out", default="")
    parser.add_argument(
        "--inline-footnotes",
        action="store_true",
        default=False,
        help="Extract footnotes inline when no .footnotes.json sidecar exists.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Skip PDFs already in done_shas.txt sidecar (for adding new PDFs without re-processing).",
    )
    parser.add_argument(
        "--done-shas-path",
        default=str(_DEFAULT_DONE_SHAS_PATH),
        help="Path to the done-SHAs sidecar file (default: hf/metadata/done_shas.txt).",
    )
    parser.add_argument(
        "--text-cache",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Enable/disable text extraction caching (default: on)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG-level) logging.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    runs_dir = Path(args.runs_dir)
    run_dir = (runs_dir / args.run_id) if args.run_id else _find_latest_run(runs_dir)
    if not run_dir or not run_dir.exists():
        raise SystemExit(f"Run directory not found at {run_dir}")

    log.info("Run directory: %s", run_dir)
    log.info("PDF root: %s", args.pdf_root)
    log.info("Output: %s", args.hf_dir)
    log.info("Workers: %d | OCR: %s | Doc policy: %s", args.workers, args.ocr_mode, args.doc_policy)
    if args.incremental:
        log.info("Incremental mode ON — done_shas: %s", args.done_shas_path)

    config = BuildConfig(
        records_path=run_dir / "records.jsonl",
        pdf_root=Path(args.pdf_root),
        hf_dir=Path(args.hf_dir),
        run_id=run_dir.name,
        workers=max(1, int(args.workers)),
        limit=max(0, int(args.limit)),
        ocr_mode=args.ocr_mode,
        ocr_backend=args.ocr_backend,
        rows_per_shard=max(1, int(args.rows_per_shard)),
        max_text_chars=max(0, int(args.max_text_chars)),
        doc_policy=args.doc_policy,
        doc_rules_path=(args.doc_rules_path or None),
        inline_footnotes=bool(args.inline_footnotes),
        include_issue_compilations_when_no_articles=bool(
            args.include_issue_compilations_when_no_articles
        ),
        emit_doctype_manifest=bool(args.emit_doctype_manifest),
        doctype_manifest_out=(args.doctype_manifest_out or None),
        incremental=bool(args.incremental),
        done_shas_path=Path(args.done_shas_path),
        text_cache_enabled=bool(args.text_cache),
    )
    summary = build_hf_dataset(config)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
