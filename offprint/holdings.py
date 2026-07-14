from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence
from urllib.parse import parse_qs, urlparse

from .gazetteer import load_registry


@dataclass(frozen=True)
class RegistryJournal:
    journal: str
    host: str
    slug: str


@dataclass(frozen=True)
class Holding:
    journal: str
    host: str
    context: str
    title: str
    authors: str
    year: str
    pdf_url: str
    local_path: str
    file_present: bool
    pdf_sha256: str
    retrieved_at: str


def _text(value: object) -> str:
    if isinstance(value, list):
        return "; ".join(_text(item) for item in value if _text(item))
    return str(value or "").strip()


def _host(value: object) -> str:
    raw = _text(value)
    parsed = urlparse(raw)
    host = (parsed.netloc or raw).lower().split(":", 1)[0]
    return host[4:] if host.startswith("www.") else host


def _slug(url: object) -> str:
    parts = [part for part in urlparse(_text(url)).path.split("/") if part]
    return parts[0].lower() if parts else ""


def build_registry_index(rows: Iterable[Mapping[str, str]]) -> dict[str, list[RegistryJournal]]:
    index: dict[str, list[RegistryJournal]] = defaultdict(list)
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        host = _host(row.get("host") or row.get("url"))
        journal = _text(row.get("journal_name"))
        if not host or not journal:
            continue
        candidate = RegistryJournal(journal=journal, host=host, slug=_slug(row.get("url")))
        identity = (candidate.journal, candidate.host, candidate.slug)
        if identity not in seen:
            index[host].append(candidate)
            seen.add(identity)
    return dict(index)


def _record_contexts(row: Mapping[str, object], metadata: Mapping[str, object]) -> list[str]:
    contexts: list[str] = []
    for value in (row.get("dc_set_spec"), metadata.get("dc_set_spec")):
        value_text = _text(value).lower()
        if value_text:
            contexts.append(value_text.rsplit(":", 1)[-1])

    for value in (
        row.get("pdf_url"),
        row.get("page_url"),
        metadata.get("url"),
        metadata.get("source_url"),
        metadata.get("page_url"),
    ):
        url = _text(value)
        if not url:
            continue
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        for key in ("context", "publication", "collection"):
            contexts.extend(_text(item).lower() for item in query.get(key, []) if _text(item))
        slug = _slug(url)
        if slug and not slug.endswith(".cgi"):
            contexts.append(slug)
    return list(dict.fromkeys(contexts))


def infer_journal(
    host: str,
    contexts: Sequence[str],
    registry_index: Mapping[str, Sequence[RegistryJournal]],
) -> str:
    candidates = list(registry_index.get(host, ()))
    matched_names = sorted(
        {candidate.journal for candidate in candidates if candidate.slug in contexts}
    )
    if len(matched_names) == 1:
        return matched_names[0]

    candidate_names = sorted({candidate.journal for candidate in candidates})
    if len(candidate_names) == 1:
        return candidate_names[0]
    if contexts:
        return f"{contexts[0]} ({host})"
    return host or "(unknown journal)"


def _year(metadata: Mapping[str, object]) -> str:
    for key in ("year", "date", "publication_date"):
        match = re.search(r"\b(?:18|19|20)\d{2}\b", _text(metadata.get(key)))
        if match:
            return match.group(0)
    return ""


def _holding_score(holding: Holding) -> tuple[int, int, int, int]:
    return (
        int(holding.file_present),
        int(bool(holding.title)),
        int(bool(holding.authors)),
        int(bool(holding.year)),
    )


def load_holdings(
    repo_root: Path, runs_dir: Path, pdf_root: Optional[Path] = None
) -> tuple[list[Holding], int]:
    registry_index = build_registry_index(
        load_registry(repo_root / "data/registry/lawjournals.csv")
    )
    holdings: dict[str, Holding] = {}
    invalid_lines = 0

    for records_path in sorted(runs_dir.glob("*/records.jsonl")):
        with records_path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    invalid_lines += 1
                    continue
                if not isinstance(row, dict):
                    invalid_lines += 1
                    continue
                if not row.get("ok") and row.get("download_state") != "downloaded":
                    continue

                metadata = row.get("metadata")
                metadata = metadata if isinstance(metadata, dict) else {}
                pdf_url = _text(row.get("pdf_url") or metadata.get("url"))
                local_path = _text(row.get("local_path"))
                sha = _text(row.get("pdf_sha256"))
                identity = sha or pdf_url or local_path
                if not identity:
                    continue

                host = _host(row.get("domain") or pdf_url)
                contexts = _record_contexts(row, metadata)
                path = Path(local_path) if local_path else None
                if path is not None and not path.is_absolute():
                    path = repo_root / path
                holding = Holding(
                    journal=infer_journal(host, contexts, registry_index),
                    host=host,
                    context=contexts[0] if contexts else "",
                    title=_text(metadata.get("title") or row.get("title")),
                    authors=_text(metadata.get("authors") or row.get("authors")),
                    year=_year(metadata),
                    pdf_url=pdf_url,
                    local_path=local_path,
                    file_present=bool(path and path.is_file()),
                    pdf_sha256=sha,
                    retrieved_at=_text(row.get("retrieved_at")),
                )
                existing = holdings.get(identity)
                if existing is None or _holding_score(holding) > _holding_score(existing):
                    holdings[identity] = holding

    # Historical corpus files can outlive their run manifests.  Supplement the
    # record-derived view from the host-partitioned PDF store so those files are
    # still represented (with filename-only metadata) rather than silently
    # disappearing from the holdings report.
    pdf_root = pdf_root or (repo_root / "artifacts/pdfs")
    if pdf_root.exists():
        recorded_paths = {
            str(Path(item.local_path).resolve())
            for item in holdings.values()
            if item.local_path
        }
        for host_dir in pdf_root.iterdir():
            if not host_dir.is_dir():
                continue
            host = _host(host_dir.name)
            if not host:
                continue
            for path in host_dir.glob("*.pdf"):
                resolved = str(path.resolve())
                if resolved in recorded_paths:
                    continue
                relative = path.relative_to(repo_root) if path.is_relative_to(repo_root) else path
                retrieved = datetime.fromtimestamp(
                    path.stat().st_mtime, tz=timezone.utc
                ).isoformat().replace("+00:00", "Z")
                holding = Holding(
                    journal=infer_journal(host, (), registry_index),
                    host=host,
                    context="filesystem",
                    title="",
                    authors="",
                    year="",
                    pdf_url="",
                    local_path=str(relative),
                    file_present=True,
                    pdf_sha256="",
                    retrieved_at=retrieved,
                )
                holdings[f"path:{resolved}"] = holding
                recorded_paths.add(resolved)

    return (
        sorted(holdings.values(), key=lambda item: (item.journal.casefold(), item.title)),
        invalid_lines,
    )


def write_csv(holdings: Sequence[Holding], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = (
        list(asdict(holdings[0]).keys())
        if holdings
        else [field.name for field in Holding.__dataclass_fields__.values()]
    )
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(asdict(holding) for holding in holdings)


def render_summary(holdings: Sequence[Holding], invalid_lines: int) -> str:
    groups: dict[str, list[Holding]] = defaultdict(list)
    for holding in holdings:
        groups[holding.journal].append(holding)

    latest = max((holding.retrieved_at for holding in holdings), default="")[:10] or "unknown"
    lines = [
        "# Downloaded Holdings by Journal",
        "",
        f"> Operational snapshot from local run records through **{latest}**. Unlike the tracked",
        "> journal catalog, these counts cannot be reproduced from a clean Git checkout.",
        "",
        f"**{len(holdings):,} unique successful PDF records** are grouped below. They are",
        "download candidates, not necessarily article-qualified documents; apply document QC",
        "before using the counts as a scholarly-article denominator.",
        "",
        "| Journal / inferred collection | Recorded PDFs | Present at recorded path | Titled | Years | Host |",
        "|---|---:|---:|---:|---|---|",
    ]
    for journal, items in sorted(groups.items(), key=lambda pair: pair[0].casefold()):
        years = sorted({item.year for item in items if item.year})
        year_range = "-"
        if years:
            year_range = years[0] if len(years) == 1 else f"{years[0]}-{years[-1]}"
        hosts = sorted({item.host for item in items if item.host})
        host_text = ", ".join(hosts[:2])
        if len(hosts) > 2:
            host_text += f" +{len(hosts) - 2}"
        label = journal.replace("|", "\\|")
        lines.append(
            f"| {label} | {len(items):,} | {sum(item.file_present for item in items):,} | "
            f"{sum(bool(item.title) for item in items):,} | {year_range} | {host_text} |"
        )

    lines.extend(
        (
            "",
            "## Reading This Table",
            "",
            "- Records are deduplicated by PDF SHA-256, then by URL or local path when no hash exists;",
            "  PDFs found in the host-partitioned corpus without a surviving run record are included",
            "  as filesystem-only entries with blank title/author/year metadata.",
            "- Journal names are matched from registry host/path metadata. Ambiguous collections are",
            "  labeled with their repository context and host rather than forced to the wrong journal.",
            "- `Present at recorded path` is a filesystem check at report time. A recorded download may",
            "  have been moved into another corpus location after acquisition.",
            f"- Invalid JSONL lines skipped: {invalid_lines:,}.",
            "- Run `make holdings` for an article-level CSV containing title, author, year, URL, path,",
            "  hash, and journal inference for the current machine.",
            "",
        )
    )
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Inventory successful Offprint PDF records")
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--runs-dir", type=Path, default=Path("artifacts/runs"))
    parser.add_argument(
        "--csv-out", type=Path, default=Path("artifacts/reports/article_holdings.csv")
    )
    parser.add_argument(
        "--summary-out", type=Path, default=Path("artifacts/reports/HOLDINGS_BY_JOURNAL.md")
    )
    parser.add_argument("--snapshot-out", type=Path)
    args = parser.parse_args(argv)

    root = args.repo_root.resolve()
    runs_dir = args.runs_dir if args.runs_dir.is_absolute() else root / args.runs_dir
    holdings, invalid_lines = load_holdings(root, runs_dir)

    csv_out = args.csv_out if args.csv_out.is_absolute() else root / args.csv_out
    summary_out = args.summary_out if args.summary_out.is_absolute() else root / args.summary_out
    write_csv(holdings, csv_out)
    summary = render_summary(holdings, invalid_lines)
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.write_text(summary, encoding="utf-8")
    print(f"Wrote {len(holdings):,} holdings to {csv_out}")
    print(f"Wrote {summary_out}")

    if args.snapshot_out:
        snapshot_out = (
            args.snapshot_out if args.snapshot_out.is_absolute() else root / args.snapshot_out
        )
        snapshot_out.parent.mkdir(parents=True, exist_ok=True)
        snapshot_out.write_text(summary, encoding="utf-8")
        print(f"Wrote {snapshot_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
