from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence
from urllib.parse import urlparse


@dataclass(frozen=True)
class GazetteerSnapshot:
    registry_rows: int
    registry_hosts: int
    registry_with_sitemap: int
    sitemap_files: int
    sitemap_start_urls: int
    sitemap_hosts: int
    invalid_sitemaps: tuple[str, ...]
    registry_statuses: Counter[str]
    registry_platforms: Counter[str]
    registry_sources: Counter[str]
    sitemap_statuses: Counter[str]
    registry_missing: Mapping[str, int]
    sitemap_missing: Mapping[str, int]


def _clean(value: object) -> str:
    return str(value or "").strip()


def normalized_platform(value: object) -> str:
    raw = _clean(value)
    if not raw:
        return "Unspecified"

    token = re.sub(r"[^a-z0-9]+", "", raw.lower())
    if "digitalcommons" in token or token == "bepress":
        return "Digital Commons"
    if "wordpress" in token:
        return "WordPress"
    if token in {"ojs", "openjournalsystems"}:
        return "OJS"
    if "drupal" in token:
        return "Drupal"
    if "squarespace" in token:
        return "Squarespace"
    if token.startswith("wix"):
        return "Wix"
    if token.startswith("dspace"):
        return "DSpace"
    if token in {"escholarship", "escholarshipplatform"}:
        return "eScholarship"
    if token in {"unknown", "customunknown", "generic", "genericweb"}:
        return "Unknown / generic"
    if token.startswith("custom") or token in {"lawschoolcustom", "publisher"}:
        return "Custom / publisher"
    return raw


def _host(url: object) -> str:
    host = urlparse(_clean(url)).netloc.lower()
    return host[4:] if host.startswith("www.") else host


def load_registry(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _start_urls(payload: Mapping[str, object]) -> list[str]:
    value = payload.get("start_urls") or payload.get("startUrl") or payload.get("url") or []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def build_snapshot(registry_path: Path, sitemaps_dir: Path) -> GazetteerSnapshot:
    rows = load_registry(registry_path)
    registry_hosts = {_clean(row.get("host")) for row in rows if _clean(row.get("host"))}
    registry_statuses = Counter(_clean(row.get("status")) or "(missing)" for row in rows)
    registry_platforms = Counter(normalized_platform(row.get("platform")) for row in rows)
    registry_sources = Counter(_clean(row.get("source")) or "(missing)" for row in rows)

    registry_missing = {
        field: sum(not _clean(row.get(field)) for row in rows)
        for field in ("journal_name", "host", "platform", "status", "sitemap_file")
    }

    sitemap_statuses: Counter[str] = Counter()
    sitemap_hosts: set[str] = set()
    sitemap_start_urls = 0
    invalid_sitemaps: list[str] = []
    sitemap_missing = {"explicit_status": 0, "journal_name": 0, "platform": 0, "start_url": 0}
    sitemap_files = sorted(sitemaps_dir.glob("*.json"))

    for path in sitemap_files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            invalid_sitemaps.append(path.name)
            continue
        if not isinstance(payload, dict):
            invalid_sitemaps.append(path.name)
            continue

        metadata = payload.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        explicit_status = _clean(metadata.get("status")).lower()
        sitemap_statuses[explicit_status or "active (inferred)"] += 1
        if not explicit_status:
            sitemap_missing["explicit_status"] += 1
        if not any(_clean(metadata.get(key)) for key in ("journal", "journal_name", "name")):
            sitemap_missing["journal_name"] += 1
        if not _clean(metadata.get("platform")):
            sitemap_missing["platform"] += 1

        urls = _start_urls(payload)
        if not urls:
            sitemap_missing["start_url"] += 1
        sitemap_start_urls += len(urls)
        sitemap_hosts.update(filter(None, (_host(url) for url in urls)))

    return GazetteerSnapshot(
        registry_rows=len(rows),
        registry_hosts=len(registry_hosts),
        registry_with_sitemap=sum(bool(_clean(row.get("sitemap_file"))) for row in rows),
        sitemap_files=len(sitemap_files),
        sitemap_start_urls=sitemap_start_urls,
        sitemap_hosts=len(sitemap_hosts),
        invalid_sitemaps=tuple(invalid_sitemaps),
        registry_statuses=registry_statuses,
        registry_platforms=registry_platforms,
        registry_sources=registry_sources,
        sitemap_statuses=sitemap_statuses,
        registry_missing=registry_missing,
        sitemap_missing=sitemap_missing,
    )


def _table(headers: Sequence[str], rows: Iterable[Sequence[object]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" if i == 0 else "---:" for i in range(len(headers))) + "|",
    ]
    lines.extend("| " + " | ".join(str(value) for value in row) + " |" for row in rows)
    return lines


def _count_rows(counter: Counter[str]) -> list[tuple[str, str]]:
    return [(label, f"{count:,}") for label, count in counter.most_common()]


def render_markdown(snapshot: GazetteerSnapshot) -> str:
    lines = [
        "# Gazetteer Snapshot",
        "",
        "> Generated from `data/registry/lawjournals.csv` and `offprint/sitemaps/*.json`.",
        "> Regenerate with `make gazetteer`; verify freshness with `make gazetteer-check`.",
        "",
        "## At a Glance",
        "",
    ]
    lines.extend(
        _table(
            ("Measure", "Count"),
            (
                ("Journal registry rows", f"{snapshot.registry_rows:,}"),
                ("Unique registry hosts", f"{snapshot.registry_hosts:,}"),
                ("Registry rows linked to a sitemap", f"{snapshot.registry_with_sitemap:,}"),
                ("Sitemap files", f"{snapshot.sitemap_files:,}"),
                ("Sitemap start URLs", f"{snapshot.sitemap_start_urls:,}"),
                ("Unique sitemap hosts", f"{snapshot.sitemap_hosts:,}"),
                ("Invalid sitemap files", f"{len(snapshot.invalid_sitemaps):,}"),
            ),
        )
    )

    sections = (
        ("Registry Status", snapshot.registry_statuses),
        ("Sitemap Lifecycle", snapshot.sitemap_statuses),
        ("Normalized Platform Family", snapshot.registry_platforms),
        ("Registry Row Provenance", snapshot.registry_sources),
    )
    for title, counter in sections:
        lines.extend(("", f"## {title}", ""))
        lines.extend(_table((title, "Rows"), _count_rows(counter)))

    lines.extend(("", "## Metadata Completeness", ""))
    quality_rows = [
        (f"Registry missing `{field}`", f"{count:,}")
        for field, count in snapshot.registry_missing.items()
    ]
    quality_rows.extend(
        (f"Sitemaps missing `{field}`", f"{count:,}")
        for field, count in snapshot.sitemap_missing.items()
    )
    lines.extend(_table(("Check", "Rows / files"), quality_rows))

    lines.extend(
        (
            "",
            "## Interpretation",
            "",
            "- A registry row means a journal is known; it does not mean PDFs have been downloaded.",
            "- A sitemap file means Offprint has a crawl seed. Its lifecycle status records readiness",
            "  or the reason it is deferred.",
            "- Missing sitemap status is interpreted by the current loader as `active`; the snapshot",
            "  labels this legacy behavior as `active (inferred)`.",
            "- Platform families above normalize spelling and case for readability. The CSV retains",
            "  the original source value.",
            "- Download and parse totals depend on local, gitignored artifacts and are intentionally",
            "  excluded from this reproducible snapshot.",
            "",
            "See [Gazetteer and Coverage](../GAZETTEER.md) for schema, provenance, and caveats.",
            "",
        )
    )
    return "\n".join(lines)


def _escape_cell(value: object) -> str:
    return _clean(value).replace("|", "\\|").replace("\n", " ")


def render_journal_catalog(rows: Sequence[Mapping[str, str]]) -> str:
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            _clean(row.get("journal_name")).casefold(),
            _clean(row.get("host")).casefold(),
        ),
    )
    lines = [
        "# Journal Catalog",
        "",
        "> Generated from `data/registry/lawjournals.csv`. Use your browser's find command",
        "> to search by journal, host, platform, or status.",
        "",
        f"**{len(sorted_rows):,} registry rows** are shown. A row means Offprint knows about",
        "the journal; it does not establish that an article has been downloaded. See",
        "[Gazetteer and Coverage](../GAZETTEER.md) for the stage definitions.",
        "",
        "| Journal | Host | Platform | Status | Crawl configuration |",
        "|---|---|---|---|---|",
    ]
    for row in sorted_rows:
        journal = _escape_cell(row.get("journal_name")) or "(unnamed)"
        host = _escape_cell(row.get("host")) or "(missing)"
        url = _clean(row.get("url"))
        host_cell = f"[{host}]({url})" if url.startswith(("http://", "https://")) else host
        platform = _escape_cell(normalized_platform(row.get("platform")))
        status = _escape_cell(row.get("status")) or "(missing)"
        sitemap = _clean(row.get("sitemap_file"))
        sitemap_cell = (
            f"[`{_escape_cell(sitemap)}`](../../offprint/sitemaps/{sitemap})" if sitemap else "-"
        )
        lines.append(f"| {journal} | {host_cell} | {platform} | {status} | {sitemap_cell} |")
    lines.append("")
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build the tracked Offprint gazetteer snapshot")
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--out", type=Path, default=Path("docs/generated/GAZETTEER_SNAPSHOT.md"))
    parser.add_argument(
        "--catalog-out", type=Path, default=Path("docs/generated/JOURNAL_CATALOG.md")
    )
    parser.add_argument(
        "--check", action="store_true", help="Fail if the output is missing or stale"
    )
    args = parser.parse_args(argv)

    root = args.repo_root.resolve()
    registry_path = root / "data/registry/lawjournals.csv"
    rendered = render_markdown(build_snapshot(registry_path, root / "offprint/sitemaps"))
    catalog_rendered = render_journal_catalog(load_registry(registry_path))
    output = args.out if args.out.is_absolute() else root / args.out
    catalog_output = args.catalog_out if args.catalog_out.is_absolute() else root / args.catalog_out

    if args.check:
        stale = [
            path
            for path, expected in ((output, rendered), (catalog_output, catalog_rendered))
            if not path.exists() or path.read_text(encoding="utf-8") != expected
        ]
        if stale:
            print("Gazetteer outputs are stale:")
            for path in stale:
                print(f"- {path}")
            print("Run: make gazetteer")
            return 1
        print(f"Gazetteer outputs are current: {output}, {catalog_output}")
        return 0

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    catalog_output.parent.mkdir(parents=True, exist_ok=True)
    catalog_output.write_text(catalog_rendered, encoding="utf-8")
    print(f"Wrote {output}")
    print(f"Wrote {catalog_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
