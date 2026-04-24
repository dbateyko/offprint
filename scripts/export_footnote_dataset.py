#!/usr/bin/env python3
"""Export extracted footnote sidecars into a gzip JSONL dataset."""

from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

DEFAULT_INCLUDE_STATUSES = ("valid", "valid_with_gaps")
SIDECAR_SUFFIXES = (".footnotes.json", ".footnotes.jsonl")


class SidecarError(ValueError):
    """Raised when a sidecar cannot be parsed into a dataset candidate."""


def default_sidecar_root() -> Path:
    artifacts_pdfs = Path("artifacts/pdfs")
    if artifacts_pdfs.exists():
        return artifacts_pdfs
    return Path("artifacts/samples/sample_1k_pdfs")


def parse_csv(value: Optional[str]) -> Optional[set[str]]:
    if value is None:
        return None
    values = {part.strip() for part in value.split(",") if part.strip()}
    return values


def sidecar_identity(path: Path) -> str:
    name = path.name
    for suffix in SIDECAR_SUFFIXES:
        if name.endswith(suffix):
            return str(path.with_name(name[: -len(suffix)]))
    return str(path)


def iter_sidecars(root: Path) -> Iterator[Path]:
    for path in sorted(root.rglob("*.footnotes.json")):
        yield path
    for path in sorted(root.rglob("*.footnotes.jsonl")):
        yield path


def load_json_sidecar(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - exact decoder errors vary by Python version
        raise SidecarError(f"failed to read JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise SidecarError("JSON sidecar root is not an object")
    return data


def load_jsonl_sidecar(path: Path) -> Dict[str, Any]:
    metadata: Optional[Dict[str, Any]] = None
    notes: Dict[str, Any] = {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise SidecarError(f"line {line_number} is not an object")
                row_type = row.get("type")
                if row_type == "metadata":
                    metadata = {k: v for k, v in row.items() if k != "type"}
                elif row_type == "footnote":
                    label = str(row.get("label", len(notes) + 1))
                    note = {k: v for k, v in row.items() if k != "type"}
                    notes[label] = note
    except SidecarError:
        raise
    except Exception as exc:  # pragma: no cover
        raise SidecarError(f"failed to read JSONL: {exc}") from exc
    if metadata is None:
        raise SidecarError("JSONL sidecar has no metadata row")
    metadata["notes"] = notes
    return metadata


def load_sidecar(path: Path) -> Dict[str, Any]:
    if path.name.endswith(".footnotes.jsonl"):
        return load_jsonl_sidecar(path)
    return load_json_sidecar(path)


def first_nonempty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def infer_domain(path: Path, root: Path, data: Dict[str, Any]) -> Optional[str]:
    explicit = first_nonempty(data.get("journal_domain"), data.get("domain"))
    if explicit:
        return str(explicit)

    source_pdf_path = data.get("source_pdf_path")
    if source_pdf_path:
        parent = Path(str(source_pdf_path)).parent.name
        if parent:
            return parent

    try:
        rel = path.relative_to(root)
    except ValueError:
        rel = path
    if len(rel.parts) > 1:
        return rel.parts[0]
    return None


def normalize_doc_policy(data: Dict[str, Any]) -> Dict[str, Any]:
    raw_policy = data.get("doc_policy")
    doc_type = data.get("doc_type")
    platform_family = data.get("platform_family")

    if isinstance(raw_policy, dict):
        normalized = dict(raw_policy)
        normalized.setdefault("doc_type", doc_type)
        normalized.setdefault("platform_family", platform_family)
        normalized.setdefault("include", bool(normalized.get("include", True)))
        return normalized

    include = True
    if doc_type and doc_type != "article":
        include = False
    return {
        "doc_type": doc_type,
        "platform_family": platform_family,
        "include": include,
        "raw_policy": raw_policy,
    }


def normalize_article(data: Dict[str, Any]) -> Dict[str, Any]:
    article = data.get("article")
    if isinstance(article, dict):
        return dict(article)

    metadata = data.get("document_metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    return {
        "title": first_nonempty(metadata.get("title"), data.get("title")),
        "authors": first_nonempty(metadata.get("authors"), data.get("authors"), []),
        "volume": first_nonempty(metadata.get("volume"), data.get("volume")),
        "issue": first_nonempty(metadata.get("issue"), data.get("issue")),
        "year": first_nonempty(metadata.get("year"), data.get("year")),
        "doi": first_nonempty(metadata.get("doi"), data.get("doi")),
    }


def int_or_none(value: Any) -> Optional[int]:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def note_sort_key(item: Tuple[str, Dict[str, Any]]) -> Tuple[int, str]:
    label, note = item
    ordinal = int_or_none(note.get("ordinal", label))
    if ordinal is None:
        return (10**9, str(label))
    return (ordinal, str(label))


def normalize_notes(raw_notes: Any) -> List[Dict[str, Any]]:
    if isinstance(raw_notes, dict):
        items = [(str(label), note) for label, note in raw_notes.items() if isinstance(note, dict)]
    elif isinstance(raw_notes, list):
        items = [(str(note.get("label", index + 1)), note) for index, note in enumerate(raw_notes) if isinstance(note, dict)]
    else:
        items = []

    normalized: List[Dict[str, Any]] = []
    for label, note in sorted(items, key=note_sort_key):
        qc = note.get("_qc") if isinstance(note.get("_qc"), dict) else {}
        ordinal = int_or_none(first_nonempty(note.get("ordinal"), label))
        normalized.append(
            {
                "ordinal": ordinal,
                "label": str(first_nonempty(note.get("label"), label)),
                "text": str(note.get("text", "")),
                "page_start": note.get("page_start"),
                "page_end": note.get("page_end"),
                "confidence": first_nonempty(note.get("confidence"), qc.get("confidence")),
                "features": note.get("features") if isinstance(note.get("features"), dict) else {},
            }
        )
    return normalized


def normalize_ordinality(data: Dict[str, Any]) -> Dict[str, Any]:
    ordinality = data.get("ordinality")
    if not isinstance(ordinality, dict):
        return {"status": "unknown", "expected_range": None, "gaps": [], "solver_selected_labels": []}
    solver_selected_labels = first_nonempty(
        ordinality.get("solver_selected_labels"),
        ordinality.get("selected_labels"),
        ordinality.get("actual_sequence"),
    )
    return {
        "status": str(ordinality.get("status", "unknown")),
        "expected_range": ordinality.get("expected_range"),
        "gaps": ordinality.get("gaps", []),
        "solver_selected_labels": solver_selected_labels or [],
    }


def build_record(path: Path, root: Path, data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "source_pdf_sha256": first_nonempty(data.get("source_pdf_sha256"), data.get("pdf_sha256")),
        "source_pdf_path": data.get("source_pdf_path"),
        "journal_domain": infer_domain(path, root, data),
        "doc_policy": normalize_doc_policy(data),
        "article": normalize_article(data),
        "ordinality": normalize_ordinality(data),
        "notes": normalize_notes(data.get("notes")),
    }


def export_dataset(
    sidecar_root: Path,
    out_path: Path,
    manifest_path: Path,
    include_statuses: Optional[set[str]],
    min_notes: int,
    journal_domain: Optional[set[str]],
) -> Dict[str, Any]:
    sidecar_root = sidecar_root.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    manifest: Dict[str, Any] = {
        "sidecar_root": str(sidecar_root),
        "out": str(out_path),
        "records_written": 0,
        "sidecars_seen": 0,
        "sidecars_loaded": 0,
        "notes_written": 0,
        "counts_by_status": {},
        "counts_by_domain": {},
        "included_reasons": {},
        "dropped_reasons": {},
    }
    statuses = Counter()
    domains = Counter()
    included = Counter()
    dropped = Counter()
    seen_identities: set[str] = set()

    with gzip.open(out_path, "wt", encoding="utf-8") as out_handle:
        for sidecar in iter_sidecars(sidecar_root):
            manifest["sidecars_seen"] += 1
            identity = sidecar_identity(sidecar.resolve())
            if identity in seen_identities:
                dropped["duplicate_sidecar"] += 1
                continue
            seen_identities.add(identity)

            try:
                data = load_sidecar(sidecar)
                record = build_record(sidecar, sidecar_root, data)
            except SidecarError:
                dropped["parse_error"] += 1
                continue

            manifest["sidecars_loaded"] += 1
            status = record["ordinality"].get("status", "unknown")
            domain = record.get("journal_domain") or "unknown"
            statuses[status] += 1
            domains[domain] += 1

            if include_statuses is not None and status not in include_statuses:
                dropped["status_not_included"] += 1
                continue
            if journal_domain is not None and domain not in journal_domain:
                dropped["domain_filter"] += 1
                continue
            if len(record["notes"]) < min_notes:
                dropped["below_min_notes"] += 1
                continue

            out_handle.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")
            manifest["records_written"] += 1
            manifest["notes_written"] += len(record["notes"])
            included["included"] += 1

    manifest["counts_by_status"] = dict(sorted(statuses.items()))
    manifest["counts_by_domain"] = dict(sorted(domains.items()))
    manifest["included_reasons"] = dict(sorted(included.items()))
    manifest["dropped_reasons"] = dict(sorted(dropped.items()))
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sidecar-root", type=Path, default=default_sidecar_root())
    parser.add_argument("--out", type=Path, default=Path("artifacts/datasets/footnotes_v1.jsonl.gz"))
    parser.add_argument(
        "--manifest-out",
        type=Path,
        default=Path("artifacts/datasets/footnotes_v1_manifest.json"),
    )
    parser.add_argument(
        "--include-statuses",
        default=",".join(DEFAULT_INCLUDE_STATUSES),
        help="Comma-separated ordinality statuses to include. Use an empty string to include all statuses.",
    )
    parser.add_argument("--min-notes", type=int, default=1)
    parser.add_argument("--journal-domain", help="Comma-separated journal domains to include.")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.min_notes < 0:
        parser.error("--min-notes must be non-negative")

    include_statuses = parse_csv(args.include_statuses)
    journal_domains = parse_csv(args.journal_domain)
    manifest = export_dataset(
        sidecar_root=args.sidecar_root,
        out_path=args.out,
        manifest_path=args.manifest_out,
        include_statuses=include_statuses,
        min_notes=args.min_notes,
        journal_domain=journal_domains,
    )
    print(
        "wrote {records} records ({notes} notes) to {out}; manifest {manifest}".format(
            records=manifest["records_written"],
            notes=manifest["notes_written"],
            out=args.out,
            manifest=args.manifest_out,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
