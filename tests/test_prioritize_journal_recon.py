from __future__ import annotations

import csv
import importlib.util
import io
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/research/prioritize_journal_recon.py"
SPEC = importlib.util.spec_from_file_location("prioritize_journal_recon", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def registry_row(name, host, rank="", wlu_id="", url=None):
    return {
        "journal_name": name,
        "url": url or f"https://{host}/journal/",
        "host": host,
        "platform": "digitalcommons",
        "status": "active",
        "sitemap_file": "journal.json",
        "wlu_mainid": wlu_id,
        "wlu_rank": rank,
        "in_cilp": "yes",
        "source": "sitemap",
        "fixed_domain_url": "",
    }


def view_row(name, domains, articles, scraped=0, donation=0, aa=0, year_min="", year_max=""):
    return {
        "journal_canonical": name,
        "domains": domains,
        "n_articles": str(articles),
        "n_scraped": str(scraped),
        "n_donation": str(donation),
        "n_aa": str(aa),
        "n_works": str(articles),
        "year_min": str(year_min),
        "year_max": str(year_max),
        "year_span": "",
        "source_mask": "",
    }


def by_name(rows, name):
    return next(row for row in rows if row["journal"] == name)


def write_rows(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_uc_irvine_aliases_and_duplicate_registry_routes_are_aggregated():
    registry = [
        registry_row("U.C. Irvine Law Review", "law.uci.edu", rank="40", wlu_id="1755"),
        registry_row("U.C. Irvine Law Review", "escholarship.org", rank="40", wlu_id="1755"),
        registry_row("UC Irvine Law Review", "escholarship.org"),
    ]
    coverage = [
        view_row(
            "UC Irvine Law Review",
            "escholarship.org",
            246,
            scraped=9,
            donation=237,
            year_min=2011,
            year_max=2026,
        )
    ]

    rows = MODULE.rank_journals(registry, coverage, current_year=2026)

    assert len(rows) == 1
    row = rows[0]
    assert row["journal"] == "U.C. Irvine Law Review"
    assert row["registry_routes"] == 3
    assert row["hosts"] == {"law.uci.edu", "escholarship.org"}
    assert row["combined_articles"] == 246
    assert row["coverage_rows_used"] == 1


def test_william_and_mary_domain_misattribution_is_excluded():
    registry = [
        registry_row(
            "William & Mary Law Review",
            "scholarship.law.wm.edu",
            rank="36",
            wlu_id="1844",
        ),
        registry_row(
            "William & Mary Law Review",
            "scholarship.law.wm.edu",
            rank="36",
            wlu_id="1844",
        ),
    ]
    coverage = [
        view_row(
            "William and Mary Law Review",
            "scholarship.law.wm.edu",
            419,
            scraped=1,
            donation=418,
            year_min=2008,
            year_max=2018,
        ),
        view_row(
            "William & Mary Law Review",
            "harvardlawreview.org",
            1,
            scraped=1,
            year_min=2015,
            year_max=2015,
        ),
    ]

    row = MODULE.rank_journals(registry, coverage, current_year=2026)[0]

    assert row["combined_articles"] == 419
    assert row["scraped_articles"] == 1
    assert row["coverage_rows_used"] == 1
    assert row["coverage_rows_rejected_domain"] == 1
    assert "ignored 1 domain-mismatched coverage row" in row["rationale"]


def test_unregistered_path_and_host_labels_never_become_journals():
    registry = [registry_row("Real Law Review", "law.example.edu", rank="10", wlu_id="10")]
    coverage = [
        view_row("Real Law Review", "law.example.edu", 2, scraped=2, year_min=2025, year_max=2026),
        view_row("wp-content", "law.example.edu", 200, scraped=200),
        view_row("uploads", "law.example.edu", 300, scraped=300),
        view_row("law.example.edu", "law.example.edu", 500, scraped=500),
    ]

    rows = MODULE.rank_journals(registry, coverage, current_year=2026)

    assert [row["journal"] for row in rows] == ["Real Law Review"]
    assert rows[0]["combined_articles"] == 2


def test_cross_domain_donation_is_retained_but_flagged():
    registry = [registry_row("Archive Law Review", "review.example.edu", rank="12")]
    coverage = [
        view_row(
            "Archive Law Review",
            "aggregator.example.org",
            200,
            donation=200,
            year_min=2008,
            year_max=2018,
        )
    ]

    row = MODULE.rank_journals(registry, coverage, current_year=2026)[0]

    assert row["combined_articles"] == 200
    assert row["coverage_rows_used_cross_domain"] == 1
    assert row["coverage_rows_rejected_domain"] == 0
    assert "accepted 1 cross-domain donation/AA row" in row["rationale"]


def test_absent_worklist_is_supported_and_scoring_components_sum():
    registry = [registry_row("Thin Law Review", "thin.example.edu", rank="25", wlu_id="25")]
    coverage = [view_row("Thin Law Review", "thin.example.edu", 0)]

    row = MODULE.rank_journals(registry, coverage, current_year=2026)[0]

    assert row["worklist_points"] == 0
    component_sum = sum(
        row[field]
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
    assert row["priority_score"] == round(component_sum, 2)


def test_worklist_failures_add_an_exposed_component():
    registry = [registry_row("Blocked Law Review", "blocked.example.edu", rank="50")]
    coverage = [view_row("Blocked Law Review", "blocked.example.edu", 10, scraped=0)]
    worklist = [
        {
            "journal_name": "Blocked Law Review",
            "recommendation": "needs_investigation",
            "attempts_total": "9",
            "last_error_class": "seed_failure",
        }
    ]

    row = MODULE.rank_journals(registry, coverage, worklist, current_year=2026)[0]

    assert row["worklist_points"] == 3
    assert row["access_penalty"] < 0
    assert "9 worklist attempt(s)" in row["rationale"]


def test_csv_and_markdown_rendering_are_deterministic_and_explain_components():
    registry = [
        registry_row("Beta Law Review", "beta.example.edu", rank="20"),
        registry_row("Alpha Law Review", "alpha.example.edu", rank="10"),
    ]
    coverage = [
        view_row(
            "Alpha Law Review",
            "alpha.example.edu",
            100,
            scraped=100,
            year_min=2000,
            year_max=2026,
        ),
        view_row("Beta Law Review", "beta.example.edu", 1, scraped=0, year_min=2020, year_max=2020),
    ]
    rows = MODULE.rank_journals(registry, coverage, current_year=2026)

    csv_text = MODULE.render_csv(rows)
    parsed = list(csv.DictReader(io.StringIO(csv_text)))
    markdown = MODULE.render_markdown(rows)

    assert parsed[0]["rank"] == "1"
    assert "importance_points" in parsed[0]
    assert "coverage_rows_rejected_domain" in parsed[0]
    assert "Components (I/C/S/R/T/F/W/A)" in markdown
    assert MODULE.render_csv(rows) == csv_text
    assert MODULE.render_markdown(rows) == markdown


def test_cli_accepts_required_inputs_without_optional_worklist(tmp_path):
    registry_path = tmp_path / "registry.csv"
    journal_view_path = tmp_path / "journal_view.csv"
    output_path = tmp_path / "priorities.csv"
    write_rows(
        registry_path,
        [registry_row("CLI Law Review", "cli.example.edu", rank="30", wlu_id="30")],
    )
    write_rows(
        journal_view_path,
        [
            view_row(
                "CLI Law Review",
                "cli.example.edu",
                12,
                scraped=2,
                year_min=2020,
                year_max=2024,
            )
        ],
    )

    result = MODULE.main(
        [
            "--registry",
            str(registry_path),
            "--journal-view",
            str(journal_view_path),
            "--format",
            "csv",
            "--current-year",
            "2026",
            "--limit",
            "1",
            "--output",
            str(output_path),
        ]
    )

    assert result == 0
    parsed = list(csv.DictReader(io.StringIO(output_path.read_text(encoding="utf-8"))))
    assert [row["journal"] for row in parsed] == ["CLI Law Review"]
    assert parsed[0]["worklist_points"] == "0.0"
