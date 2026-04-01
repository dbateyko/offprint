from __future__ import annotations

import csv
import json
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .ocr_worker import OCRWorkerPool
from .pipeline import BatchConfig, _extract_for_pdf
from .qc_filter import latest_qc_manifest, load_excluded_paths

CANONICAL_FAMILIES = (
    "digital_commons",
    "wordpress",
    "ojs",
    "scholastica",
    "custom_unknown",
    "other",
)

REVIEW_DECISIONS = {"", "pass", "minor_issue", "critical_failure"}


@dataclass
class SampleAuditConfig:
    pdf_roots: list[str]
    sitemaps_dir: str = "offprint/sitemaps"
    sample_size: int = 120
    floor_per_family: int = 10
    seed: int = 20260224
    features: str = "legal"
    workers: int = 6
    ocr_workers: int = 2
    ocr_backend: str = "olmocr"
    ocr_mode: str = "fallback"
    overwrite: bool = False
    run_extraction: bool = True
    respect_qc_exclusions: bool = True
    qc_exclusion_manifest: str | None = None
    manifest_out: str | None = None
    review_csv_out: str | None = None
    report_out: str | None = None
    critical_failure_threshold: float = 0.05
    stratum_failure_threshold: float = 0.10


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_dump_atomic(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _jsonl_dump_atomic(path: str, rows: list[dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    os.replace(tmp, path)


def _normalize_domain(value: str) -> str:
    text = (value or "").strip().lower()
    if not text:
        return "unknown"
    if text.startswith("www."):
        text = text[4:]
    return text


def _canonical_family(platform_raw: str) -> str:
    platform = " ".join((platform_raw or "").strip().lower().replace("_", " ").split())
    compact = platform.replace(" ", "")

    if "digitalcommons" in compact or "bepress" in compact:
        return "digital_commons"
    if "wordpress" in compact:
        return "wordpress"
    if compact == "ojs" or "openjournalsystems" in compact:
        return "ojs"
    if "scholastica" in compact:
        return "scholastica"
    if (
        not platform
        or platform in {"unknown", "custom", "custom/unknown", "law school custom"}
        or "unknown" in platform
        or "custom" in platform
    ):
        return "custom_unknown"
    return "other"


def _extract_seed_urls(payload: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for key in ("start_urls", "startUrl"):
        value = payload.get(key)
        if isinstance(value, list):
            urls.extend(str(item).strip() for item in value if str(item).strip())
        elif isinstance(value, str) and value.strip():
            urls.append(value.strip())

    url_value = payload.get("url")
    if isinstance(url_value, str) and url_value.strip():
        urls.append(url_value.strip())

    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    meta_url = metadata.get("url")
    if isinstance(meta_url, str) and meta_url.strip():
        urls.append(meta_url.strip())

    return urls


def _load_domain_metadata(sitemaps_dir: str) -> dict[str, dict[str, str]]:
    domain_votes: dict[str, dict[str, int]] = {}
    domain_seed: dict[str, str] = {}

    for path in sorted(Path(sitemaps_dir).glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue

        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        platform_raw = str(metadata.get("platform") or "").strip()
        family = _canonical_family(platform_raw)

        for seed_url in _extract_seed_urls(payload):
            domain = _normalize_domain(urlparse(seed_url).netloc)
            if not domain or domain == "unknown":
                continue
            if domain not in domain_votes:
                domain_votes[domain] = {}
            domain_votes[domain][family] = domain_votes[domain].get(family, 0) + 1
            domain_seed.setdefault(domain, seed_url)

    resolved: dict[str, dict[str, str]] = {}
    for domain, votes in domain_votes.items():
        ranked = sorted(votes.items(), key=lambda kv: (-kv[1], kv[0]))
        family = ranked[0][0] if ranked else "custom_unknown"
        resolved[domain] = {
            "platform_family": family,
            "seed_url": domain_seed.get(domain, ""),
        }

    return resolved


def _discover_pdfs(pdf_roots: list[str]) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    for root in pdf_roots:
        root_path = Path(root)
        if not root_path.exists():
            continue
        for path in sorted(root_path.rglob("*.pdf")):
            abs_path = str(path.resolve())
            if abs_path in seen:
                continue
            seen.add(abs_path)
            found.append(abs_path)
    return sorted(found)


def _infer_domain_from_path(pdf_path: str, pdf_roots: list[str]) -> str:
    path_obj = Path(pdf_path)
    for root in pdf_roots:
        root_path = Path(root)
        try:
            rel = path_obj.relative_to(root_path.resolve())
        except Exception:
            continue
        for part in rel.parts:
            part_norm = _normalize_domain(part)
            if "." in part_norm and not part_norm.endswith(".pdf"):
                return part_norm

    parts = [_normalize_domain(part) for part in path_obj.parts]
    for part in parts:
        if "." in part and not part.endswith(".pdf"):
            return part
    return "unknown"


def _allocate_counts(
    availability: dict[str, int], sample_size: int, floor_per_family: int
) -> dict[str, dict[str, int]]:
    non_empty = sorted([family for family, count in availability.items() if count > 0])
    allocation = {family: {"floor": 0, "proportional": 0, "spillover": 0, "total": 0} for family in non_empty}
    if not non_empty:
        return allocation

    target = min(sample_size, sum(availability.values()))
    if target <= 0:
        return allocation

    # Standard floor assignment when feasible.
    if floor_per_family * len(non_empty) <= target:
        for family in non_empty:
            floor_take = min(floor_per_family, availability[family])
            allocation[family]["floor"] = floor_take
            allocation[family]["total"] = floor_take
    else:
        # If target is smaller than strict floors, fall back to deterministic round-robin.
        remaining_capacity = {family: availability[family] for family in non_empty}
        assigned = 0
        while assigned < target:
            progressed = False
            for family in non_empty:
                if assigned >= target:
                    break
                if remaining_capacity[family] <= 0:
                    continue
                allocation[family]["floor"] += 1
                allocation[family]["total"] += 1
                remaining_capacity[family] -= 1
                assigned += 1
                progressed = True
            if not progressed:
                break

    remaining = target - sum(v["total"] for v in allocation.values())
    if remaining <= 0:
        return allocation

    capacity = {
        family: max(0, availability[family] - allocation[family]["total"]) for family in non_empty
    }
    total_capacity = sum(capacity.values())
    if total_capacity <= 0:
        return allocation

    raw_shares: dict[str, float] = {}
    for family in non_empty:
        raw = remaining * (capacity[family] / total_capacity)
        base = min(capacity[family], int(raw))
        allocation[family]["proportional"] += base
        allocation[family]["total"] += base
        capacity[family] -= base
        raw_shares[family] = raw - int(raw)

    remaining = target - sum(v["total"] for v in allocation.values())
    if remaining <= 0:
        return allocation

    remainder_order = sorted(non_empty, key=lambda family: (-raw_shares[family], family))
    for family in remainder_order:
        if remaining <= 0:
            break
        if capacity[family] <= 0:
            continue
        allocation[family]["proportional"] += 1
        allocation[family]["total"] += 1
        capacity[family] -= 1
        remaining -= 1

    # Spillover for any leftover slots after proportional rounding/cap effects.
    if remaining > 0:
        spill_order = sorted(non_empty, key=lambda family: (-capacity[family], family))
        for family in spill_order:
            if remaining <= 0:
                break
            if capacity[family] <= 0:
                continue
            take = min(capacity[family], remaining)
            allocation[family]["spillover"] += take
            allocation[family]["total"] += take
            capacity[family] -= take
            remaining -= take

    return allocation


def _build_manifest_rows(
    candidates: list[dict[str, Any]],
    allocation: dict[str, dict[str, int]],
    seed: int,
) -> list[dict[str, Any]]:
    by_family: dict[str, list[dict[str, Any]]] = {}
    for row in candidates:
        family = str(row.get("platform_family") or "custom_unknown")
        by_family.setdefault(family, []).append(row)

    selected: list[dict[str, Any]] = []
    for family in sorted(by_family):
        rows = sorted(
            by_family[family],
            key=lambda row: (
                str(row.get("platform_family") or ""),
                str(row.get("domain") or ""),
                os.path.basename(str(row.get("pdf_path") or "")),
                str(row.get("pdf_path") or ""),
            ),
        )

        family_seed = seed + sum(ord(ch) for ch in family)
        rng = random.Random(family_seed)
        rng.shuffle(rows)

        counts = allocation.get(family, {"floor": 0, "proportional": 0, "spillover": 0, "total": 0})
        take_total = min(int(counts.get("total", 0)), len(rows))
        chosen = rows[:take_total]

        floor_count = min(int(counts.get("floor", 0)), len(chosen))
        prop_count = min(int(counts.get("proportional", 0)), max(len(chosen) - floor_count, 0))

        for idx, row in enumerate(chosen):
            if idx < floor_count:
                reason = "floor_minimum"
            elif idx < floor_count + prop_count:
                reason = "proportional"
            else:
                reason = "spillover"
            selected.append(
                {
                    "pdf_path": row["pdf_path"],
                    "domain": row["domain"],
                    "platform_family": row["platform_family"],
                    "stratum": row["platform_family"],
                    "selection_reason": reason,
                    "seed_url": row.get("seed_url", ""),
                }
            )

    return sorted(
        selected,
        key=lambda row: (
            str(row.get("platform_family") or ""),
            str(row.get("domain") or ""),
            str(row.get("pdf_path") or ""),
        ),
    )


def _default_paths() -> tuple[str, str, str]:
    stamp = _utc_stamp()
    os.makedirs("artifacts/runs", exist_ok=True)
    return (
        os.path.join("artifacts/runs", f"footnote_sample_manifest_{stamp}.jsonl"),
        os.path.join("artifacts/runs", f"footnote_sample_review_{stamp}.csv"),
        os.path.join("artifacts/runs", f"footnote_sample_audit_{stamp}.json"),
    )


def _read_existing_review_rows(path: str) -> dict[str, dict[str, str]]:
    existing: dict[str, dict[str, str]] = {}
    if not path or not os.path.exists(path):
        return existing
    with open(path, encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            pdf_path = str(row.get("pdf_path") or "").strip()
            if not pdf_path:
                continue
            existing[pdf_path] = {key: str(value or "") for key, value in row.items()}
    return existing


def _sidecar_path(pdf_path: str) -> str:
    return f"{pdf_path}.footnotes.json"


def _load_sidecar_summary(pdf_path: str) -> dict[str, str]:
    path = _sidecar_path(pdf_path)
    if not os.path.exists(path):
        return {
            "sidecar_present": "false",
            "notes_extracted": "",
            "ocr_used": "",
            "document_confidence": "",
            "parser_warnings": "",
            "first5_labels": "",
            "first5_note_types": "",
        }

    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {
            "sidecar_present": "false",
            "notes_extracted": "",
            "ocr_used": "",
            "document_confidence": "",
            "parser_warnings": "",
            "first5_labels": "",
            "first5_note_types": "",
        }

    labels: list[str] = []
    note_types: list[str] = []
    note_count = 0

    notes_payload = payload.get("notes")
    if isinstance(notes_payload, dict):
        note_count = len(notes_payload)
        for key, note in list(notes_payload.items())[:5]:
            label = str(key or "").strip()
            if "__dup" in label:
                label = label.split("__dup", 1)[0].strip()
            labels.append(label)
            if isinstance(note, dict):
                note_types.append(str(note.get("note_type") or "").strip())
            else:
                note_types.append("")
    elif isinstance(notes_payload, list):
        note_count = len(notes_payload)
        for note in notes_payload[:5]:
            if not isinstance(note, dict):
                continue
            labels.append(str(note.get("label") or "").strip())
            note_types.append(str(note.get("note_type") or "").strip())

    warnings = payload.get("warnings") if isinstance(payload.get("warnings"), list) else []
    return {
        "sidecar_present": "true",
        "notes_extracted": str(note_count),
        "ocr_used": "",
        "document_confidence": str(payload.get("document_confidence", "")),
        "parser_warnings": "|".join(str(w) for w in warnings if str(w).strip()),
        "first5_labels": "|".join(labels),
        "first5_note_types": "|".join(note_types),
    }


def _write_review_csv(manifest_rows: list[dict[str, Any]], review_path: str) -> None:
    existing = _read_existing_review_rows(review_path)
    fieldnames = [
        "pdf_path",
        "domain",
        "platform_family",
        "stratum",
        "selection_reason",
        "seed_url",
        "review_decision",
        "failure_taxonomy",
        "review_notes",
        "inspected_note_count",
        "critical_rule_triggered",
        "edge_two_column_bottom_footnotes",
        "edge_endnotes_with_heading",
        "edge_headingless_endnotes",
        "edge_symbol_marked_notes",
        "edge_cross_page_continuation",
        "edge_ocr_heavy",
        "sidecar_present",
        "notes_extracted",
        "ocr_used",
        "document_confidence",
        "parser_warnings",
        "first5_labels",
        "first5_note_types",
    ]

    os.makedirs(os.path.dirname(review_path) or ".", exist_ok=True)
    tmp = f"{review_path}.tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()

        for row in manifest_rows:
            pdf_path = row["pdf_path"]
            base = {name: "" for name in fieldnames}
            base.update(
                {
                    "pdf_path": pdf_path,
                    "domain": row["domain"],
                    "platform_family": row["platform_family"],
                    "stratum": row["stratum"],
                    "selection_reason": row["selection_reason"],
                    "seed_url": row.get("seed_url", ""),
                }
            )

            previous = existing.get(pdf_path, {})
            for key in (
                "review_decision",
                "failure_taxonomy",
                "review_notes",
                "inspected_note_count",
                "critical_rule_triggered",
                "edge_two_column_bottom_footnotes",
                "edge_endnotes_with_heading",
                "edge_headingless_endnotes",
                "edge_symbol_marked_notes",
                "edge_cross_page_continuation",
                "edge_ocr_heavy",
            ):
                base[key] = previous.get(key, "")

            sidecar = _load_sidecar_summary(pdf_path)
            base.update(sidecar)
            writer.writerow(base)

    os.replace(tmp, review_path)


def _collect_review_rows(review_csv_path: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not os.path.exists(review_csv_path):
        return rows
    with open(review_csv_path, encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append({key: str(value or "") for key, value in row.items()})
    return rows


def _safe_float(text: str, default: float = 0.0) -> float:
    try:
        return float(text)
    except Exception:
        return default


def _build_report(
    manifest_rows: list[dict[str, Any]],
    review_rows: list[dict[str, str]],
    extraction_results: list[dict[str, Any]],
    config: SampleAuditConfig,
) -> dict[str, Any]:
    by_path = {str(row.get("pdf_path") or ""): row for row in review_rows}

    decision_counts = {"pass": 0, "minor_issue": 0, "critical_failure": 0, "unreviewed": 0}
    failure_taxonomy: dict[str, int] = {}
    by_stratum: dict[str, dict[str, Any]] = {}

    for manifest_row in manifest_rows:
        pdf_path = str(manifest_row.get("pdf_path") or "")
        stratum = str(manifest_row.get("stratum") or "custom_unknown")
        review = by_path.get(pdf_path, {})
        decision = str(review.get("review_decision") or "").strip().lower()
        if decision not in REVIEW_DECISIONS:
            decision = ""

        bucket = by_stratum.setdefault(
            stratum,
            {
                "sampled": 0,
                "reviewed": 0,
                "pass": 0,
                "minor_issue": 0,
                "critical_failure": 0,
                "critical_failure_rate": 0.0,
            },
        )
        bucket["sampled"] += 1

        if decision == "":
            decision_counts["unreviewed"] += 1
        else:
            decision_counts[decision] += 1
            bucket["reviewed"] += 1
            bucket[decision] += 1
            terms = [part.strip().lower() for part in str(review.get("failure_taxonomy") or "").split("|")]
            for term in terms:
                if not term:
                    continue
                failure_taxonomy[term] = int(failure_taxonomy.get(term, 0)) + 1

    for payload in by_stratum.values():
        reviewed = int(payload["reviewed"])
        critical = int(payload["critical_failure"])
        payload["critical_failure_rate"] = round((critical / reviewed) if reviewed else 0.0, 4)

    reviewed_total = decision_counts["pass"] + decision_counts["minor_issue"] + decision_counts["critical_failure"]
    overall_critical_rate = round(
        (decision_counts["critical_failure"] / reviewed_total) if reviewed_total else 0.0, 4
    )

    blocked = not manifest_rows
    major_stratum_failures = [
        name
        for name, payload in sorted(by_stratum.items())
        if payload["sampled"] >= 10 and payload["critical_failure_rate"] >= config.stratum_failure_threshold
    ]

    if blocked:
        go_no_go = "blocked"
    elif decision_counts["unreviewed"] > 0:
        go_no_go = "pending_manual_review"
    elif overall_critical_rate >= config.critical_failure_threshold or major_stratum_failures:
        go_no_go = "no_go"
    else:
        go_no_go = "go"

    examples: list[dict[str, str]] = []
    for row in review_rows:
        decision = str(row.get("review_decision") or "").strip().lower()
        if decision != "critical_failure":
            continue
        examples.append(
            {
                "pdf_path": str(row.get("pdf_path") or ""),
                "stratum": str(row.get("stratum") or ""),
                "failure_taxonomy": str(row.get("failure_taxonomy") or ""),
                "review_notes": str(row.get("review_notes") or ""),
            }
        )
        if len(examples) >= 10:
            break

    extraction_ok = len([r for r in extraction_results if str(r.get("status") or "") == "ok"])
    extraction_failed = len([r for r in extraction_results if str(r.get("status") or "") == "failed"])
    extraction_skipped = len(
        [r for r in extraction_results if str(r.get("status") or "") in {"skipped_existing", "skipped"}]
    )

    summary = {
        "created_at": _utc_now_iso(),
        "sample_size_target": config.sample_size,
        "sample_size_selected": len(manifest_rows),
        "reviewed_total": reviewed_total,
        "unreviewed_total": decision_counts["unreviewed"],
        "decision_counts": decision_counts,
        "overall_critical_failure_rate": overall_critical_rate,
        "critical_failure_threshold": config.critical_failure_threshold,
        "stratum_failure_threshold": config.stratum_failure_threshold,
        "major_stratum_failures": major_stratum_failures,
        "go_no_go": go_no_go,
        "blocked": blocked,
        "extraction": {
            "processed": len(extraction_results),
            "ok": extraction_ok,
            "failed": extraction_failed,
            "skipped": extraction_skipped,
        },
    }

    recommendations: list[str] = []
    if blocked:
        recommendations.append("Populate local PDFs under artifacts/pdfs or pdfs, then rerun the audit.")
    elif go_no_go == "pending_manual_review":
        recommendations.append("Complete review_decision for each sampled PDF in the review CSV and rerun.")
    elif go_no_go == "no_go":
        recommendations.append("Do not claim broad coverage; prioritize parser fixes for top failure taxonomy items.")
    else:
        recommendations.append("Coverage bar met for this sample; document residual risks and monitor on next batch.")

    return {
        "summary": summary,
        "by_stratum": by_stratum,
        "failure_taxonomy": dict(sorted(failure_taxonomy.items(), key=lambda kv: (-kv[1], kv[0]))),
        "examples": examples,
        "recommendations": recommendations,
    }


def _extract_sample(
    manifest_rows: list[dict[str, Any]],
    config: SampleAuditConfig,
) -> list[dict[str, Any]]:
    if not config.run_extraction:
        return []

    base_config = BatchConfig(
        pdf_root=".",
        features=config.features,
        workers=max(1, int(config.workers)),
        ocr_workers=max(1, int(config.ocr_workers)),
        ocr_backend=config.ocr_backend,
        ocr_mode=config.ocr_mode,
        overwrite=config.overwrite,
        respect_qc_exclusions=False,
    )

    results: list[dict[str, Any]] = []
    ocr_pool: OCRWorkerPool | None = None
    if config.ocr_mode != "off":
        ocr_pool = OCRWorkerPool(workers=max(1, int(config.ocr_workers)), backend=config.ocr_backend)
        if not ocr_pool.available():
            raise RuntimeError(
                "OCR mode is enabled but olmocr is unavailable. "
                "Install olmocr or run with --ocr-mode off."
            )

    try:
        with ThreadPoolExecutor(max_workers=max(1, int(config.workers))) as executor:
            futures = [
                executor.submit(_extract_for_pdf, str(row["pdf_path"]), base_config, ocr_pool)
                for row in manifest_rows
            ]
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append({"status": "failed", "error": str(exc)})
    finally:
        if ocr_pool is not None:
            ocr_pool.close()

    return sorted(results, key=lambda row: str(row.get("pdf_path") or ""))


def run_sample_audit(config: SampleAuditConfig) -> dict[str, Any]:
    manifest_default, review_default, report_default = _default_paths()
    manifest_out = config.manifest_out or manifest_default
    review_out = config.review_csv_out or review_default
    report_out = config.report_out or report_default

    normalized_roots = [str(Path(root).resolve()) for root in config.pdf_roots]
    domain_map = _load_domain_metadata(config.sitemaps_dir)

    qc_manifest_used = ""
    excluded_paths: set[str] = set()
    if config.respect_qc_exclusions:
        qc_manifest_path = config.qc_exclusion_manifest or latest_qc_manifest("artifacts/runs")
        if qc_manifest_path and os.path.exists(qc_manifest_path):
            qc_manifest_used = qc_manifest_path
            excluded_paths = load_excluded_paths(qc_manifest_path)

    discovered = _discover_pdfs(normalized_roots)

    candidates: list[dict[str, Any]] = []
    excluded_count = 0
    for pdf_path in discovered:
        abs_pdf_path = str(Path(pdf_path).resolve())
        if abs_pdf_path in excluded_paths:
            excluded_count += 1
            continue

        domain = _infer_domain_from_path(abs_pdf_path, normalized_roots)
        mapped = domain_map.get(domain, {})
        family = str(mapped.get("platform_family") or "custom_unknown")
        if family not in CANONICAL_FAMILIES:
            family = "other"

        candidates.append(
            {
                "pdf_path": abs_pdf_path,
                "domain": domain,
                "platform_family": family,
                "seed_url": str(mapped.get("seed_url") or ""),
            }
        )

    availability: dict[str, int] = {family: 0 for family in CANONICAL_FAMILIES}
    for row in candidates:
        availability[row["platform_family"]] = int(availability.get(row["platform_family"], 0)) + 1

    allocation = _allocate_counts(
        availability=availability,
        sample_size=max(0, int(config.sample_size)),
        floor_per_family=max(0, int(config.floor_per_family)),
    )
    manifest_rows = _build_manifest_rows(candidates, allocation=allocation, seed=int(config.seed))

    _jsonl_dump_atomic(manifest_out, manifest_rows)

    extraction_results = _extract_sample(manifest_rows, config=config)

    # Refresh review CSV after extraction so sidecar metadata is up to date.
    _write_review_csv(manifest_rows, review_out)
    review_rows = _collect_review_rows(review_out)

    report = _build_report(
        manifest_rows=manifest_rows,
        review_rows=review_rows,
        extraction_results=extraction_results,
        config=config,
    )

    report["summary"].update(
        {
            "pdf_roots": normalized_roots,
            "discovered_pdfs": len(discovered),
            "eligible_pdfs": len(candidates),
            "excluded_by_qc": excluded_count,
            "qc_manifest_used": qc_manifest_used,
            "manifest_out": manifest_out,
            "review_csv_out": review_out,
            "report_out": report_out,
            "seed": config.seed,
            "allocation": allocation,
            "availability": availability,
        }
    )

    _json_dump_atomic(report_out, report)

    return {
        "manifest_out": manifest_out,
        "review_csv_out": review_out,
        "report_out": report_out,
        "summary": report["summary"],
    }
