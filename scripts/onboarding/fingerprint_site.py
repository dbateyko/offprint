#!/usr/bin/env python3
"""CLI wrapper for the site fingerprinter module.

Single-site mode:
    python scripts/fingerprint_site.py https://example.edu/law-review/

Batch mode (fingerprint all sitemaps with a given metadata.status):
    python scripts/fingerprint_site.py --batch offprint/sitemaps/ --status todo_adapter

Outputs:
    artifacts/fingerprints/{domain}.json  — per-site fingerprint result
    artifacts/fingerprints/summary.csv    — batch summary (batch mode only)
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.site_fingerprinter import fingerprint_site, fingerprint_to_dict  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _domain_from_url(url: str) -> str:
    """Return the netloc component of *url*, lowercased."""
    parsed = urlparse(url)
    return (parsed.netloc or url).lower().strip("/")


def _slug_for_file(domain: str) -> str:
    """Return a filesystem-safe filename stem for *domain*."""
    return domain.replace(":", "_").replace("/", "_")


def _load_sitemaps(sitemaps_dir: Path, status_filter: str | None) -> list[dict[str, Any]]:
    """Yield seed entries from all JSON files in *sitemaps_dir*.

    Each returned dict is guaranteed to contain at least one of:
      - ``start_urls``: list[str]
      - ``url``: str  (legacy format)

    Args:
        sitemaps_dir: Directory containing ``*.json`` sitemap files.
        status_filter: If given, only include entries whose
            ``metadata.status`` matches this value (case-insensitive).

    Returns:
        A list of sitemap dicts passing the filter.
    """
    entries: list[dict[str, Any]] = []
    for path in sorted(sitemaps_dir.glob("*.json")):
        try:
            data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"[warn] could not parse {path.name}: {exc}", flush=True)
            continue

        if status_filter is not None:
            meta_status = str((data.get("metadata") or {}).get("status") or "").strip().lower()
            if meta_status != status_filter.lower():
                continue

        # Must have at least one seed URL
        has_start_urls = bool(data.get("start_urls"))
        has_url = bool(data.get("url"))
        if not has_start_urls and not has_url:
            continue

        entries.append(data)
    return entries


def _extract_urls(entry: dict[str, Any]) -> list[str]:
    """Return the list of seed URLs for a sitemap entry."""
    if entry.get("start_urls"):
        return list(entry["start_urls"])
    if entry.get("url"):
        return [str(entry["url"])]
    return []


def _run_single(url: str, out_dir: Path, max_pages: int, use_playwright: bool) -> dict[str, Any]:
    """Fingerprint *url* and write the result JSON to *out_dir*.

    Args:
        url: The seed URL to fingerprint.
        out_dir: Directory where ``{domain}.json`` will be written.
        max_pages: Maximum pages to fetch during fingerprinting.
        use_playwright: Whether to allow Playwright-based fetching.

    Returns:
        The fingerprint result dict (as returned by ``fingerprint_to_dict``).
    """
    result = fingerprint_to_dict(
        fingerprint_site(url, max_pages=max_pages, use_playwright_fallback=use_playwright)
    )
    domain = _domain_from_url(url)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{_slug_for_file(domain)}.json"
    out_path.write_text(json.dumps(result, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return result


def _result_to_csv_row(url: str, result: dict[str, Any]) -> dict[str, str]:
    """Flatten a fingerprint result dict to a CSV-friendly row."""
    return {
        "domain": _domain_from_url(url),
        "platform": str(result.get("platform") or "unknown"),
        "adapter_recommendation": str(result.get("adapter_recommendation") or ""),
        "pdf_count": str(result.get("pdf_count") or 0),
        "fetch_count": str(result.get("fetch_count") or 0),
        "errors": str(result.get("errors") or ""),
    }


def _write_summary_csv(out_dir: Path, rows: list[dict[str, str]]) -> Path:
    """Write *rows* to ``{out_dir}/summary.csv``.

    Args:
        out_dir: Output directory.
        rows: List of row dicts (must all share the same keys).

    Returns:
        Path to the written CSV file.
    """
    fieldnames = [
        "domain",
        "platform",
        "adapter_recommendation",
        "pdf_count",
        "fetch_count",
        "errors",
    ]
    csv_path = out_dir / "summary.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return csv_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Entry point for the fingerprint_site CLI."""
    parser = argparse.ArgumentParser(
        description="Fingerprint one or many law-review journal sites.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("url", nargs="?", help="Single site URL to fingerprint.")
    parser.add_argument(
        "--batch", metavar="DIR", help="Directory of sitemap JSON files for batch mode."
    )
    parser.add_argument(
        "--status",
        default=None,
        metavar="STATUS",
        help="Filter sitemaps by metadata.status (e.g. 'todo_adapter'). Batch mode only.",
    )
    parser.add_argument(
        "--out-dir", default="artifacts/fingerprints", metavar="DIR", help="Output directory."
    )
    parser.add_argument(
        "--max-pages", type=int, default=10, help="Maximum pages to fetch per site."
    )
    parser.add_argument(
        "--no-playwright", action="store_true", help="Disable Playwright-based fetching."
    )
    parser.add_argument("--max-workers", type=int, default=4, help="Thread count for batch mode.")
    args = parser.parse_args()

    use_playwright = not args.no_playwright
    out_dir = Path(args.out_dir)

    if args.batch and args.url:
        parser.error("Specify either a positional URL or --batch, not both.")
    if not args.batch and not args.url:
        parser.error("Provide a URL or --batch DIR.")

    # ------------------------------------------------------------------
    # Single-site mode
    # ------------------------------------------------------------------
    if args.url:
        print(f"[fingerprint] {args.url}", flush=True)
        result = _run_single(
            args.url, out_dir, max_pages=args.max_pages, use_playwright=use_playwright
        )
        domain = _domain_from_url(args.url)
        out_path = out_dir / f"{_slug_for_file(domain)}.json"
        print(f"  platform       : {result.get('platform', 'unknown')}", flush=True)
        print(f"  adapter        : {result.get('adapter_recommendation', '')}", flush=True)
        print(f"  pdf_count      : {result.get('pdf_count', 0)}", flush=True)
        print(f"  fetch_count    : {result.get('fetch_count', 0)}", flush=True)
        print(f"  result written : {out_path}", flush=True)
        return 0

    # ------------------------------------------------------------------
    # Batch mode
    # ------------------------------------------------------------------
    sitemaps_dir = Path(args.batch)
    if not sitemaps_dir.is_dir():
        print(f"[error] --batch path is not a directory: {sitemaps_dir}", flush=True)
        return 1

    entries = _load_sitemaps(sitemaps_dir, status_filter=args.status)
    if not entries:
        filter_note = f" with status={args.status!r}" if args.status else ""
        print(f"[warn] no sitemaps found in {sitemaps_dir}{filter_note}", flush=True)
        return 0

    print(f"[fingerprint] batch: {len(entries)} sitemaps (status={args.status!r})", flush=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Collect one representative URL per entry (first start_url)
    tasks: list[tuple[str, dict[str, Any]]] = []
    for entry in entries:
        urls = _extract_urls(entry)
        if urls:
            tasks.append((urls[0], entry))

    csv_rows: list[dict[str, str]] = []
    total = len(tasks)

    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
        future_to_url = {
            executor.submit(_run_single, url, out_dir, args.max_pages, use_playwright): url
            for url, _entry in tasks
        }
        for idx, future in enumerate(as_completed(future_to_url), start=1):
            url = future_to_url[future]
            try:
                result = future.result()
                csv_rows.append(_result_to_csv_row(url, result))
                print(
                    f"  [{idx}/{total}] OK  {_domain_from_url(url)} — "
                    f"{result.get('platform', 'unknown')} / {result.get('adapter_recommendation', '')}",
                    flush=True,
                )
            except Exception as exc:
                csv_rows.append(
                    {
                        "domain": _domain_from_url(url),
                        "platform": "error",
                        "adapter_recommendation": "",
                        "pdf_count": "0",
                        "fetch_count": "0",
                        "errors": str(exc),
                    }
                )
                print(f"  [{idx}/{total}] ERR {_domain_from_url(url)}: {exc}", flush=True)

    csv_path = _write_summary_csv(out_dir, csv_rows)
    print(f"\n[fingerprint] summary CSV written: {csv_path}", flush=True)
    print(f"[fingerprint] individual JSONs in : {out_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
