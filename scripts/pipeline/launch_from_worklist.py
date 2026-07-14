#!/usr/bin/env python3
"""Launch scrape screens from the coverage-aware worklist.

Reads ``catalog/scrape_worklist.csv`` (built by
``offprint-data-ops/inventory/build_scrape_worklist.py``), filters to seeds
recommended for ``scrape``, splits them across N screens with a configurable
bepress concurrency cap, and writes ready-to-run launch scripts.

Bepress hosts share an IP-level WAF — keep concurrent bepress screens low
(default 4). Other platforms are unconstrained.

Usage:
    python3 scripts/pipeline/launch_from_worklist.py \
        --batch-id b1 --max-bepress-screens 4 --max-other-screens 12 \
        --seeds-per-screen 7

The script prints (and writes to ``/tmp/<batch_id>_launch.sh``) the screen
commands. Pass ``--launch`` to actually start them.
"""

from __future__ import annotations
import argparse
import datetime as dt
import os
import re
import shutil
import subprocess
from pathlib import Path

import pandas as pd

DEFAULT_ROOT = Path(os.environ.get("OFFPRINT_ROOT", "/mnt/shared_storage/law-review-corpus"))

BEPRESS_PATTERNS = re.compile(
    r"(digitalcommons\.|scholarship\.|scholarlycommons\.|scholarworks\.|"
    r"repository\.law\.|openscholarship\.|engagedscholarship\.|via\.library\.|"
    r"uknowledge\.|kentlaw\.iit\.edu|digitalrepository\.|ir\.lawnet\.|"
    r"lawdigitalcommons\.)"
)


def is_bepress(host: str, platform: str) -> bool:
    if BEPRESS_PATTERNS.search(host or ""):
        return True
    p = (platform or "").strip().lower()
    return p in {"digitalcommons", "digital_commons", "bepress_digital_commons"}


SCRIPT_TEMPLATE = """#!/usr/bin/env bash
set -u
cd {offprint_dir}
export LRS_VERBOSE_PDF_LOG=0
export LRS_MAX_BROWSERS=1
export LRS_WORDPRESS_FAST_MODE=1
export LRS_WORDPRESS_FAST_SEED_BUDGET_SECONDS=900
export LRS_WORDPRESS_FAST_TIMEOUT_SECONDS=15
export LRS_WORDPRESS_FAST_MAX_ATTEMPTS=3
export LRS_WORDPRESS_FAST_REST_MAX_PAGES=500
export LRS_WORDPRESS_FAST_SITEMAP_MAX_URLS=5000
export LRS_WORDPRESS_FAST_HTML_MAX_PAGES=500
exec timeout --kill-after=60s {wall_clock} python3 scripts/pipeline/run_pipeline.py \
  --mode full --sitemaps-dir {sitemaps_dir} \
  --out-dir {out_dir} --manifest-dir artifacts/runs \
  --run-id {run_id} \
  --max-workers 4 --max-seeds-per-domain 5 --max-consecutive-seed-failures-per-domain 5 \
  --min-delay 1.5 --max-delay 3.5 --use-playwright --playwright-headless \
  --skip-well-covered-seeds --well-covered-pdf-threshold {well_covered} \
  --no-skip-dc-sites --dc-enum-mode sitemap_only --no-dc-use-siteindex \
  --dc-min-domain-delay-ms 2500 --dc-max-domain-delay-ms 5000 \
  --dc-waf-fail-threshold 3 --dc-waf-cooldown-seconds 1800 \
  --dc-waf-browser-fallback --dc-browser-headless --dc-browser-backend playwright \
  --no-dc-round-robin-downloads --stalled-seed-timeout-seconds 600 \
  --no-pdf-progress-timeout-seconds 600 --skip-retry-pass
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    ap.add_argument("--batch-id", required=True, help="Short ID, e.g. 'b1' or '20260504'")
    ap.add_argument("--worklist", type=Path)
    ap.add_argument(
        "--max-bepress-screens",
        type=int,
        default=4,
        help="Cap on concurrent bepress-targeted screens (WAF safety)",
    )
    ap.add_argument("--max-other-screens", type=int, default=12)
    ap.add_argument("--seeds-per-screen", type=int, default=7)
    ap.add_argument(
        "--well-covered-threshold",
        type=int,
        default=200,
        help="Pipeline-side per-host skip threshold",
    )
    ap.add_argument("--wall-clock", default="4h")
    ap.add_argument(
        "--out-dir",
        default="artifacts/scraped_v2",
        help="Where the pipeline writes downloaded PDFs (host-level subdirs)",
    )
    ap.add_argument(
        "--launch",
        action="store_true",
        help="Actually start the screens (default: write scripts only)",
    )
    ap.add_argument(
        "--screen-prefix", default="sc", help="Screen name prefix (e.g. 'sc' -> sc_a, sc_b, ...)"
    )
    args = ap.parse_args()
    root = args.root.expanduser().resolve()
    worklist = args.worklist or root / "catalog" / "scrape_worklist.csv"
    sitemaps = root / "offprint" / "offprint" / "sitemaps"
    offprint_dir = root / "offprint"

    df = pd.read_csv(worklist)
    df = df[df.recommendation == "scrape"].copy()
    df["is_bepress"] = df.apply(lambda r: is_bepress(str(r.host), str(r.platform)), axis=1)

    bepress = df[df.is_bepress].sort_values(["host", "slug"])
    other = df[~df.is_bepress].sort_values(["host", "slug"])

    print(f"Worklist: {len(df)} eligible ({len(bepress)} bepress, {len(other)} other)")

    bepress_screens_n = min(
        args.max_bepress_screens,
        (len(bepress) + args.seeds_per_screen - 1) // args.seeds_per_screen,
    )
    other_screens_n = min(
        args.max_other_screens, (len(other) + args.seeds_per_screen - 1) // args.seeds_per_screen
    )
    bepress_take = bepress_screens_n * args.seeds_per_screen
    other_take = other_screens_n * args.seeds_per_screen
    print(
        f"  scheduling {bepress_screens_n} bepress screens "
        f"({min(bepress_take, len(bepress))} seeds)"
    )
    print(f"  scheduling {other_screens_n} other screens " f"({min(other_take, len(other))} seeds)")

    bepress_seeds = bepress.head(bepress_take).sitemap_file.tolist()
    other_seeds = other.head(other_take).sitemap_file.tolist()

    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    batch_root = Path(f"/tmp/scrape_{args.batch_id}")
    batch_root.mkdir(parents=True, exist_ok=True)

    plan_lines = []
    letter_iter = iter("abcdefghijklmnopqrstuvwxyz")

    def schedule(seeds: list[str], label: str) -> None:
        for chunk_start in range(0, len(seeds), args.seeds_per_screen):
            letter = next(letter_iter)
            screen_name = f"{args.screen_prefix}_{letter}"
            chunk = seeds[chunk_start : chunk_start + args.seeds_per_screen]
            screen_dir = batch_root / letter
            screen_dir.mkdir(exist_ok=True)
            for sf in chunk:
                src = sitemaps / sf
                if src.exists():
                    shutil.copy(src, screen_dir / sf)
            run_id = f"{ts}_{args.batch_id}_{letter}"
            script_path = batch_root / f"run_{letter}.sh"
            script_path.write_text(
                SCRIPT_TEMPLATE.format(
                    offprint_dir=offprint_dir,
                    wall_clock=args.wall_clock,
                    sitemaps_dir=str(screen_dir),
                    out_dir=args.out_dir,
                    run_id=run_id,
                    well_covered=args.well_covered_threshold,
                )
            )
            script_path.chmod(0o755)
            log_path = batch_root / f"{letter}.log"
            launch_cmd = (
                f"screen -dmS {screen_name} bash -lc " f'"{script_path} 2>&1 | tee {log_path}"'
            )
            plan_lines.append(f"# {label} chunk {letter}: {len(chunk)} seeds -> {screen_name}")
            plan_lines.append(launch_cmd)

    schedule(bepress_seeds, "BEPRESS")
    schedule(other_seeds, "OTHER")

    plan_path = batch_root / "launch.sh"
    plan_path.write_text("#!/usr/bin/env bash\nset -e\n" + "\n".join(plan_lines) + "\n")
    plan_path.chmod(0o755)
    print(f"\nPlan written to {plan_path}")
    print(f"Per-screen scripts in {batch_root}/run_*.sh")
    print(f"Per-screen sitemap dirs in {batch_root}/<letter>/")

    if args.launch:
        print("\nLaunching screens...")
        for line in plan_lines:
            if line.startswith("screen "):
                subprocess.run(line, shell=True, check=True)
        print("Verifying...")
        out = subprocess.check_output(["screen", "-ls"], text=True, stderr=subprocess.STDOUT)
        print("\n".join(line for line in out.splitlines() if args.screen_prefix in line))
    else:
        print("\nTo launch: bash " + str(plan_path))


if __name__ == "__main__":
    main()
