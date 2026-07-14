#!/usr/bin/env python3
"""Rank registered journals for reconnaissance using explainable coverage signals.

The registry is authoritative for journal identity.  Journal-view and worklist
rows may enrich a registered journal, but they never create one; this prevents
path fragments, host labels, and other scraper artifacts from becoming ranked
"journals". Scraped-only coverage on unrelated domains is rejected. Donation or
Anna's Archive coverage may be cross-domain, but is retained with an explicit flag.
"""

from __future__ import annotations

import argparse
import csv
import io
import math
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple
from urllib.parse import urlsplit

OUTPUT_FIELDS = [
    "rank",
    "journal",
    "wlu_rank",
    "registry_routes",
    "hosts",
    "platforms",
    "statuses",
    "combined_articles",
    "scraped_articles",
    "donation_articles",
    "aa_articles",
    "year_min",
    "year_max",
    "source_count",
    "coverage_rows_used",
    "coverage_rows_used_cross_domain",
    "coverage_rows_rejected_domain",
    "importance_points",
    "count_gap_points",
    "scrape_gap_points",
    "recency_gap_points",
    "temporal_gap_points",
    "source_gap_points",
    "worklist_points",
    "access_penalty",
    "priority_score",
    "rationale",
]


def normalize_journal_name(value: str) -> str:
    """Return a conservative comparison key for a journal title.

    Ampersand/"and" and the documented U.C./UC Irvine spelling are treated as
    equivalent.  No fuzzy edit-distance matching is used: journals with merely
    similar names remain separate.
    """

    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.casefold().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^the\s+", "", text)
    # The registry contains both "U.C. Irvine" and "UC Irvine".  Limit the
    # acronym collapse to this attested alias instead of joining arbitrary initials.
    text = re.sub(r"^u c irvine\b", "uc irvine", text)
    return text


def _int(value: object) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


def _optional_int(value: object) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _split_semicolon(value: str) -> Set[str]:
    return {item.strip() for item in (value or "").split(";") if item.strip()}


def _host_from_url(value: str) -> str:
    if not value:
        return ""
    try:
        return (urlsplit(value).hostname or "").lower()
    except ValueError:
        return ""


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


class _UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, item: int) -> int:
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: int, right: int) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def aggregate_registry(rows: Sequence[Mapping[str, str]]) -> List[Dict[str, object]]:
    """Aggregate duplicate routes using exact normalized aliases and W&L IDs."""

    usable = [row for row in rows if normalize_journal_name(row.get("journal_name", ""))]
    union = _UnionFind(len(usable))
    seen_names: Dict[str, int] = {}
    seen_wlu: Dict[str, int] = {}
    for index, row in enumerate(usable):
        name_key = normalize_journal_name(row.get("journal_name", ""))
        if name_key in seen_names:
            union.union(index, seen_names[name_key])
        else:
            seen_names[name_key] = index
        wlu_id = (row.get("wlu_mainid") or "").strip()
        if wlu_id:
            if wlu_id in seen_wlu:
                union.union(index, seen_wlu[wlu_id])
            else:
                seen_wlu[wlu_id] = index

    grouped: MutableMapping[int, List[Mapping[str, str]]] = defaultdict(list)
    for index, row in enumerate(usable):
        grouped[union.find(index)].append(row)

    journals: List[Dict[str, object]] = []
    for members in grouped.values():
        # Preserve the registry's first spelling, while retaining every spelling
        # as an exact normalized alias for enrichment joins.
        canonical = members[0].get("journal_name", "").strip()
        names = {row.get("journal_name", "").strip() for row in members if row.get("journal_name")}
        aliases = {normalize_journal_name(name) for name in names}
        hosts: Set[str] = set()
        platforms: Set[str] = set()
        statuses: Set[str] = set()
        wlu_ranks: List[int] = []
        in_cilp = False
        for row in members:
            host = (row.get("host") or "").strip().lower() or _host_from_url(row.get("url", ""))
            if host:
                hosts.add(host)
            if (row.get("platform") or "").strip():
                platforms.add(row["platform"].strip())
            if (row.get("status") or "").strip():
                statuses.add(row["status"].strip())
            rank = _optional_int(row.get("wlu_rank"))
            if rank is not None and rank > 0:
                wlu_ranks.append(rank)
            in_cilp = in_cilp or (row.get("in_cilp") or "").strip().casefold() == "yes"
        journals.append(
            {
                "journal": canonical,
                "aliases": aliases,
                "registry_routes": len(members),
                "hosts": hosts,
                "platforms": platforms,
                "statuses": statuses,
                "wlu_rank": min(wlu_ranks) if wlu_ranks else None,
                "in_cilp": in_cilp,
            }
        )
    return journals


def _domain_matches(coverage_domains: Set[str], registry_hosts: Set[str]) -> bool:
    if not coverage_domains or not registry_hosts:
        return True
    for coverage in coverage_domains:
        coverage = coverage.lower()
        for registered in registry_hosts:
            registered = registered.lower()
            if (
                coverage == registered
                or coverage.endswith("." + registered)
                or registered.endswith("." + coverage)
            ):
                return True
    return False


def attach_coverage(
    journals: Sequence[MutableMapping[str, object]],
    rows: Sequence[Mapping[str, str]],
) -> None:
    aliases: MutableMapping[str, List[MutableMapping[str, object]]] = defaultdict(list)
    for journal in journals:
        for alias in journal["aliases"]:  # type: ignore[index]
            aliases[str(alias)].append(journal)

    accepted: MutableMapping[int, List[Mapping[str, str]]] = defaultdict(list)
    accepted_cross_domain: MutableMapping[int, int] = defaultdict(int)
    rejected: MutableMapping[int, int] = defaultdict(int)
    seen_rows: Set[Tuple[object, ...]] = set()
    for row in rows:
        key = normalize_journal_name(row.get("journal_canonical", ""))
        matches = aliases.get(key, [])
        if len(matches) != 1:
            continue
        journal = matches[0]
        domains = {domain.lower() for domain in _split_semicolon(row.get("domains", ""))}
        registry_hosts = set(journal["hosts"])  # type: ignore[arg-type]
        domain_matches = _domain_matches(domains, registry_hosts)
        if not domain_matches:
            has_portable_source = _int(row.get("n_donation")) > 0 or _int(row.get("n_aa")) > 0
            if not has_portable_source:
                rejected[id(journal)] += 1
                continue
            accepted_cross_domain[id(journal)] += 1
        marker = (
            id(journal),
            tuple(sorted(domains)),
            row.get("n_articles", ""),
            row.get("n_scraped", ""),
            row.get("n_donation", ""),
            row.get("n_aa", ""),
            row.get("year_min", ""),
            row.get("year_max", ""),
        )
        if marker not in seen_rows:
            accepted[id(journal)].append(row)
            seen_rows.add(marker)

    for journal in journals:
        matched = accepted[id(journal)]
        journal["combined_articles"] = sum(_int(row.get("n_articles")) for row in matched)
        journal["scraped_articles"] = sum(_int(row.get("n_scraped")) for row in matched)
        journal["donation_articles"] = sum(_int(row.get("n_donation")) for row in matched)
        journal["aa_articles"] = sum(_int(row.get("n_aa")) for row in matched)
        years_min = [_optional_int(row.get("year_min")) for row in matched]
        years_max = [_optional_int(row.get("year_max")) for row in matched]
        journal["year_min"] = min((year for year in years_min if year is not None), default=None)
        journal["year_max"] = max((year for year in years_max if year is not None), default=None)
        journal["coverage_rows_used"] = len(matched)
        journal["coverage_rows_used_cross_domain"] = accepted_cross_domain[id(journal)]
        journal["coverage_rows_rejected_domain"] = rejected[id(journal)]
        journal["source_count"] = sum(
            int(journal[field] > 0)
            for field in ("scraped_articles", "donation_articles", "aa_articles")
        )


def attach_worklist(
    journals: Sequence[MutableMapping[str, object]], rows: Sequence[Mapping[str, str]]
) -> None:
    aliases: MutableMapping[str, List[MutableMapping[str, object]]] = defaultdict(list)
    for journal in journals:
        for alias in journal["aliases"]:  # type: ignore[index]
            aliases[str(alias)].append(journal)

    grouped: MutableMapping[int, List[Mapping[str, str]]] = defaultdict(list)
    for row in rows:
        matches = aliases.get(normalize_journal_name(row.get("journal_name", "")), [])
        if len(matches) == 1:
            grouped[id(matches[0])].append(row)
    for journal in journals:
        matched = grouped[id(journal)]
        journal["worklist_attempts"] = sum(_int(row.get("attempts_total")) for row in matched)
        journal["worklist_recommendations"] = {
            row.get("recommendation", "").strip()
            for row in matched
            if row.get("recommendation", "").strip()
        }
        journal["worklist_errors"] = {
            row.get("last_error_class", "").strip()
            for row in matched
            if row.get("last_error_class", "").strip()
        }
        journal["worklist_rows"] = len(matched)


def _round(value: float) -> float:
    return round(value + 1e-12, 2)


def score_journal(journal: MutableMapping[str, object], current_year: int) -> Dict[str, object]:
    wlu_rank = journal.get("wlu_rank")
    # Importance should break ties among real coverage gaps, not make a heavily
    # covered rank-one journal outrank every genuinely empty collection.
    importance = 40.0 / math.sqrt(int(wlu_rank)) if wlu_rank else 0.0
    if journal.get("in_cilp"):
        importance += 3.0

    articles = int(journal.get("combined_articles", 0))
    scraped = int(journal.get("scraped_articles", 0))
    count_gap = 40.0 / (1.0 + articles / 100.0)
    scrape_ratio = min(1.0, scraped / articles) if articles else 0.0
    scrape_gap = 20.0 * (1.0 - scrape_ratio)

    year_min = journal.get("year_min")
    year_max = journal.get("year_max")
    recency_gap = (
        10.0 if year_max is None else min(15.0, max(0, current_year - int(year_max)) * 1.5)
    )
    if year_min is None or year_max is None:
        temporal_gap = 10.0
    else:
        inclusive_span = max(1, int(year_max) - int(year_min) + 1)
        temporal_gap = max(0.0, 10.0 - min(10.0, float(inclusive_span)))

    source_count = int(journal.get("source_count", 0))
    source_gap = {0: 10.0, 1: 8.0, 2: 4.0}.get(source_count, 0.0)

    recommendations = set(journal.get("worklist_recommendations", set()))
    errors = set(journal.get("worklist_errors", set()))
    attempts = int(journal.get("worklist_attempts", 0))
    worklist_points = 0.0
    if "needs_investigation" in recommendations:
        worklist_points += 3.0
    if any(
        item.startswith("paused_") or item.startswith("todo_adapter") for item in recommendations
    ):
        worklist_points += 2.0

    statuses = {str(item).casefold() for item in journal.get("statuses", set())}
    blocked_statuses = {
        item
        for item in statuses
        if item.startswith(("paused_waf", "paused_login", "paused_paywall"))
    }
    access_penalty = -5.0 if blocked_statuses and "active" in statuses else 0.0
    if blocked_statuses and "active" not in statuses:
        access_penalty = -15.0
    elif any(item.startswith("paused_404") for item in statuses) and "active" not in statuses:
        access_penalty = -5.0
    if errors and attempts:
        access_penalty -= min(5.0, attempts / 50.0)

    components = {
        "importance_points": _round(importance),
        "count_gap_points": _round(count_gap),
        "scrape_gap_points": _round(scrape_gap),
        "recency_gap_points": _round(recency_gap),
        "temporal_gap_points": _round(temporal_gap),
        "source_gap_points": _round(source_gap),
        "worklist_points": _round(worklist_points),
        "access_penalty": _round(access_penalty),
    }
    score = _round(sum(float(value) for value in components.values()))

    rationale: List[str] = []
    rationale.append(f"W&L rank {wlu_rank}" if wlu_rank else "no W&L rank")
    if journal.get("coverage_rows_used"):
        rationale.append(f"{articles} combined articles")
        rationale.append(f"{scraped}/{articles} scraper-side")
    else:
        rationale.append("no matched journal-view coverage")
    if year_max is not None:
        rationale.append(f"coverage ends {year_max}")
    else:
        rationale.append("coverage years unknown")
    rationale.append(f"{source_count} populated coverage source(s)")
    if attempts:
        rationale.append(f"{attempts} worklist attempt(s)")
    display_statuses = sorted(str(item) for item in journal.get("statuses", set()))
    if display_statuses:
        rationale.append(f"registry status {','.join(display_statuses)}")
    rejected = int(journal.get("coverage_rows_rejected_domain", 0))
    if rejected:
        rationale.append(f"ignored {rejected} domain-mismatched coverage row(s)")
    cross_domain = int(journal.get("coverage_rows_used_cross_domain", 0))
    if cross_domain:
        rationale.append(f"accepted {cross_domain} cross-domain donation/AA row(s)")

    result = dict(journal)
    result.update(components)
    result["priority_score"] = score
    result["rationale"] = "; ".join(rationale)
    return result


def rank_journals(
    registry_rows: Sequence[Mapping[str, str]],
    journal_view_rows: Sequence[Mapping[str, str]],
    worklist_rows: Optional[Sequence[Mapping[str, str]]] = None,
    current_year: Optional[int] = None,
) -> List[Dict[str, object]]:
    journals = aggregate_registry(registry_rows)
    attach_coverage(journals, journal_view_rows)
    attach_worklist(journals, worklist_rows or [])
    year = current_year or datetime.now(timezone.utc).year
    ranked = [score_journal(journal, year) for journal in journals]
    ranked.sort(
        key=lambda row: (
            -float(row["priority_score"]),
            int(row["wlu_rank"]) if row.get("wlu_rank") else 10**9,
            normalize_journal_name(str(row["journal"])),
        )
    )
    for index, row in enumerate(ranked, 1):
        row["rank"] = index
    return ranked


def _display_row(row: Mapping[str, object]) -> Dict[str, object]:
    displayed = dict(row)
    for field in ("hosts", "platforms", "statuses"):
        displayed[field] = ";".join(sorted(str(item) for item in row.get(field, set())))
    for field in ("wlu_rank", "year_min", "year_max"):
        displayed[field] = "" if row.get(field) is None else row[field]
    return displayed


def render_csv(rows: Sequence[Mapping[str, object]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output,
        fieldnames=OUTPUT_FIELDS,
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(_display_row(row))
    return output.getvalue()


def _md(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def render_markdown(rows: Sequence[Mapping[str, object]]) -> str:
    lines = [
        "# Journal reconnaissance priorities",
        "",
        "Score = importance + count gap + scraper gap + recency gap + temporal gap + "
        "source gap + worklist points + access penalty. Registry titles define identity; cross-domain "
        "donation/AA rows are flagged, while scraped-only mismatches are excluded.",
        "",
        "| Rank | Journal | W&L | Articles | Scraped | Years | Sources | "
        "Components (I/C/S/R/T/F/W/A) | Score | Rationale |",
        "| ---: | --- | ---: | ---: | ---: | --- | ---: | --- | ---: | --- |",
    ]
    for raw in rows:
        row = _display_row(raw)
        years = (
            f"{row['year_min']}-{row['year_max']}"
            if row["year_min"] != "" and row["year_max"] != ""
            else "unknown"
        )
        components = "/".join(
            str(row[field])
            for field in (
                "importance_points",
                "count_gap_points",
                "scrape_gap_points",
                "recency_gap_points",
                "temporal_gap_points",
                "source_gap_points",
                "worklist_points",
                "access_penalty",
            )
        )
        lines.append(
            (
                "| {rank} | {journal} | {wlu} | {articles} | {scraped} | {years} | "
                "{sources} | {components} | {score} | {rationale} |"
            ).format(
                rank=_md(row["rank"]),
                journal=_md(row["journal"]),
                wlu=_md(row["wlu_rank"] or "—"),
                articles=_md(row["combined_articles"]),
                scraped=_md(row["scraped_articles"]),
                years=_md(years),
                sources=_md(row["source_count"]),
                components=_md(components),
                score=_md(row["priority_score"]),
                rationale=_md(row["rationale"]),
            )
        )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True, help="Registry lawjournals CSV")
    parser.add_argument(
        "--journal-view",
        type=Path,
        required=True,
        help="Normalized journal-view CSV",
    )
    parser.add_argument("--worklist", type=Path, help="Optional scrape-worklist CSV")
    parser.add_argument("--format", choices=("markdown", "csv"), default="markdown")
    parser.add_argument("--output", type=Path, help="Write output to this path instead of stdout")
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum ranked rows to emit; use 0 for all rows (default: 50)",
    )
    parser.add_argument(
        "--current-year",
        type=int,
        default=datetime.now(timezone.utc).year,
        help="Year used for the recency component (defaults to current UTC year)",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        registry_rows = _read_csv(args.registry)
        journal_view_rows = _read_csv(args.journal_view)
        worklist_rows = _read_csv(args.worklist) if args.worklist else []
    except OSError as exc:
        print(f"input error: {exc}", file=sys.stderr)
        return 2

    rows = rank_journals(
        registry_rows,
        journal_view_rows,
        worklist_rows=worklist_rows,
        current_year=args.current_year,
    )
    if args.limit > 0:
        rows = rows[: args.limit]
    rendered = render_csv(rows) if args.format == "csv" else render_markdown(rows)
    if args.output:
        try:
            args.output.write_text(rendered, encoding="utf-8")
        except OSError as exc:
            print(f"output error: {exc}", file=sys.stderr)
            return 2
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
