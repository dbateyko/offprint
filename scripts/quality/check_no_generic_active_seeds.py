#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    from offprint.cli import DEFAULT_SITEMAPS_DIR
    from offprint.adapters import UnmappedAdapterError, pick_adapter_for
    from offprint.seed_catalog import filter_active, load_seed_entries
except ModuleNotFoundError:
    ROOT = Path(__file__).resolve().parents[2]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from offprint.cli import DEFAULT_SITEMAPS_DIR
    from offprint.adapters import UnmappedAdapterError, pick_adapter_for
    from offprint.seed_catalog import filter_active, load_seed_entries


def find_policy_violations(sitemaps_dir: str) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    entries = filter_active(load_seed_entries(sitemaps_dir))
    for entry in entries:
        try:
            adapter = pick_adapter_for(entry.seed_url, allow_generic=False)
        except UnmappedAdapterError as exc:
            violations.append(
                {
                    "type": "unmapped_adapter",
                    "seed_url": entry.seed_url,
                    "host": exc.host,
                    "sitemap_file": entry.sitemap_file,
                    "sitemap_id": entry.sitemap_id,
                    "status": entry.status,
                    "error": str(exc),
                }
            )
            continue

        adapter_name = adapter.__class__.__name__
        if adapter_name == "GenericAdapter":
            violations.append(
                {
                    "type": "generic_adapter",
                    "seed_url": entry.seed_url,
                    "host": urlparse(entry.seed_url).netloc.lower(),
                    "sitemap_file": entry.sitemap_file,
                    "sitemap_id": entry.sitemap_id,
                    "status": entry.status,
                    "error": "GenericAdapter resolved for active seed",
                }
            )
    return violations


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fail when any active sitemap seed resolves to GenericAdapter or is unmapped."
        )
    )
    parser.add_argument("--sitemaps-dir", default=DEFAULT_SITEMAPS_DIR)
    args = parser.parse_args()

    violations = find_policy_violations(args.sitemaps_dir)
    if violations:
        print(
            f"[adapter-policy] FAIL: found {len(violations)} active seeds without explicit adapter mapping."
        )
        for row in violations:
            print(
                "[adapter-policy] "
                f"{row['type']} sitemap={row['sitemap_file']} "
                f"seed={row['seed_url']} error={row['error']}"
            )
        raise SystemExit(1)

    print("[adapter-policy] PASS: all active seeds resolve to non-generic adapters.")
    print(json.dumps({"violations": 0, "sitemaps_dir": args.sitemaps_dir}, sort_keys=True))


if __name__ == "__main__":
    main()
