#!/usr/bin/env python3
"""Self-refilling scrape drainer.

Replaces the fixed-slice ``launch_from_worklist.py`` with a persistent loop
that keeps the screen budget full until the worklist (recommendation=scrape)
drains. Resumes cleanly across drainer restarts: it scans live screens whose
names start with ``--screen-prefix`` (default ``sd``), reads the sitemap
dirs they were launched with, and treats those sitemaps as in-flight.

Stop conditions (any one):
- worklist eligible count is 0
- ``--max-iterations`` reached (default unlimited)
- sentinel ``/tmp/scrape_drainer.stop`` exists

Bepress concurrency cap is enforced per-iteration.

Run via: ``python3 scripts/pipeline/scrape_drainer.py [--launch]``

Wrap in screen for unattended operation:
    screen -dmS sd_master python3 offprint/scripts/pipeline/scrape_drainer.py --launch
"""

from __future__ import annotations
import argparse
import datetime as dt
import os
import re
import shutil
import string
import subprocess
import time
from pathlib import Path

import pandas as pd

ROOT = Path(os.environ.get("OFFPRINT_ROOT", "/mnt/shared_storage/law-review-corpus"))
WORKLIST = ROOT / "catalog" / "scrape_worklist.csv"
SITEMAPS = ROOT / "offprint" / "offprint" / "sitemaps"
DRAINER_ROOT = Path("/tmp/scrape_drainer")
STOP_SENTINEL = Path("/tmp/scrape_drainer.stop")

BEPRESS_PATTERNS = re.compile(
    r"(digitalcommons\.|scholarship\.|scholarlycommons\.|scholarworks\.|"
    r"repository\.law\.|openscholarship\.|engagedscholarship\.|via\.library\.|"
    r"uknowledge\.|kentlaw\.iit\.edu|digitalrepository\.|ir\.lawnet\.|"
    r"lawdigitalcommons\.)"
)


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


def is_bepress(host: str, platform: str) -> bool:
    if BEPRESS_PATTERNS.search(host or ""):
        return True
    return (platform or "").strip().lower() in {
        "digitalcommons",
        "digital_commons",
        "bepress_digital_commons",
    }


def list_active_screens(prefix: str) -> list[str]:
    """Return socket names matching prefix_<letter> (excludes the sd_master loop)."""
    try:
        out = subprocess.check_output(["screen", "-ls"], text=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        # `screen -ls` returns 1 when no sockets exist
        out = e.output if e.output else ""
    names = []
    pat = re.compile(rf"\b\d+\.({re.escape(prefix)}_[a-z0-9]+)\b")
    for line in out.splitlines():
        m = pat.search(line)
        if m and not m.group(1).endswith("_master"):
            names.append(m.group(1))
    return names


def in_flight_sitemaps(active: list[str]) -> set[str]:
    """Return sitemap filenames assigned to any currently-alive screen."""
    out = set()
    for name in active:
        # Convention: per-screen sitemap dir is /tmp/scrape_drainer/<screen_name>/
        d = DRAINER_ROOT / name
        if d.exists():
            for sm in d.glob("*.json"):
                out.add(sm.name)
    return out


def refresh_worklist() -> None:
    print("[refresh] coverage + worklist + ledger ...")
    subprocess.run(
        ["python3", "offprint-data-ops/inventory/build_scrape_coverage.py"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    subprocess.run(
        ["python3", "offprint-data-ops/inventory/build_sitemap_ledger.py"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    subprocess.run(
        ["python3", "offprint-data-ops/inventory/build_scrape_worklist.py"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )


def screen_letter(used: set[str], prefix: str) -> str:
    """Pick a 2-char alphanumeric label not already in `used`."""
    chars = string.ascii_lowercase + string.digits
    for a in chars:
        for b in chars:
            cand = f"{prefix}_{a}{b}"
            if cand not in used:
                used.add(cand)
                return cand
    raise RuntimeError("ran out of screen labels (1296 in use)")


def schedule_chunk(
    seeds: list[str],
    used_screens: set[str],
    *,
    prefix: str,
    wall_clock: str,
    well_covered: int,
    out_dir: str,
    ts: str,
    dry_run: bool,
) -> str:
    name = screen_letter(used_screens, prefix)
    screen_dir = DRAINER_ROOT / name
    screen_dir.mkdir(parents=True, exist_ok=True)
    for sf in seeds:
        src = SITEMAPS / sf
        if src.exists():
            shutil.copy(src, screen_dir / sf)
    run_id = f"{ts}_{name}"
    script_path = DRAINER_ROOT / f"{name}.sh"
    script_path.write_text(
        SCRIPT_TEMPLATE.format(
            offprint_dir=ROOT / "offprint",
            wall_clock=wall_clock,
            sitemaps_dir=str(screen_dir),
            out_dir=out_dir,
            run_id=run_id,
            well_covered=well_covered,
        )
    )
    script_path.chmod(0o755)
    log_path = DRAINER_ROOT / f"{name}.log"
    cmd = f'screen -dmS {name} bash -lc "{script_path} 2>&1 | tee {log_path}"'
    if dry_run:
        print(f"  [dry] would launch {name} with {len(seeds)} seeds")
    else:
        subprocess.run(cmd, shell=True, check=True)
        print(f"  launched {name} ({len(seeds)} seeds)")
    return name


def iteration(args, used_screens: set[str]) -> dict:
    refresh_worklist()
    df = pd.read_csv(WORKLIST)
    eligible = df[df.recommendation == "scrape"].copy()
    if eligible.empty:
        return {"eligible": 0, "scheduled": 0, "active": 0, "done": True}

    eligible["is_bepress"] = eligible.apply(
        lambda r: is_bepress(str(r.host), str(r.platform)), axis=1
    )
    active = list_active_screens(args.screen_prefix)
    in_flight = in_flight_sitemaps(active)
    eligible = eligible[~eligible.sitemap_file.isin(in_flight)]

    bepress_active = sum(
        1
        for n in active
        if any(
            is_bepress(p.stem.split("_")[1] if "_" in p.stem else "", "")
            or BEPRESS_PATTERNS.search(p.read_text()) is not None
            for p in (DRAINER_ROOT / n).glob("*.json")
        )
        if (DRAINER_ROOT / n).exists()
    )
    # Simpler: count screens whose dir contains any bepress sitemap
    bepress_active = 0
    for n in active:
        d = DRAINER_ROOT / n
        if not d.exists():
            continue
        for sm in d.glob("*.json"):
            try:
                content = sm.read_text()
                if BEPRESS_PATTERNS.search(content):
                    bepress_active += 1
                    break
            except OSError:
                pass
    other_active = len(active) - bepress_active

    bepress_quota = max(0, args.max_bepress_screens - bepress_active)
    other_quota = max(0, args.max_other_screens - other_active)

    bepress = eligible[eligible.is_bepress].sort_values(["host", "slug"])
    other = eligible[~eligible.is_bepress].sort_values(["host", "slug"])

    bepress_take = bepress_quota * args.seeds_per_screen
    other_take = other_quota * args.seeds_per_screen

    bepress_seeds = bepress.head(bepress_take).sitemap_file.tolist()
    other_seeds = other.head(other_take).sitemap_file.tolist()

    print(
        f"[iter] eligible={len(eligible)} active={len(active)} "
        f"(bep={bepress_active}/{args.max_bepress_screens}, "
        f"oth={other_active}/{args.max_other_screens}) "
        f"-> scheduling bep_chunks={bepress_quota}, oth_chunks={other_quota}"
    )

    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    n_scheduled = 0
    for chunk_start in range(0, len(bepress_seeds), args.seeds_per_screen):
        chunk = bepress_seeds[chunk_start : chunk_start + args.seeds_per_screen]
        if not chunk:
            break
        schedule_chunk(
            chunk,
            used_screens,
            prefix=args.screen_prefix,
            wall_clock=args.wall_clock,
            well_covered=args.well_covered,
            out_dir=args.out_dir,
            ts=ts,
            dry_run=args.dry_run,
        )
        n_scheduled += 1
    for chunk_start in range(0, len(other_seeds), args.seeds_per_screen):
        chunk = other_seeds[chunk_start : chunk_start + args.seeds_per_screen]
        if not chunk:
            break
        schedule_chunk(
            chunk,
            used_screens,
            prefix=args.screen_prefix,
            wall_clock=args.wall_clock,
            well_covered=args.well_covered,
            out_dir=args.out_dir,
            ts=ts,
            dry_run=args.dry_run,
        )
        n_scheduled += 1

    return {
        "eligible": int(len(eligible)),
        "scheduled": n_scheduled,
        "active": len(active),
        "done": False,
    }


def main() -> None:
    global ROOT, WORKLIST, SITEMAPS
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=ROOT)
    ap.add_argument("--max-bepress-screens", type=int, default=4)
    ap.add_argument("--max-other-screens", type=int, default=12)
    ap.add_argument("--seeds-per-screen", type=int, default=7)
    ap.add_argument("--well-covered", type=int, default=200)
    ap.add_argument("--wall-clock", default="4h")
    ap.add_argument("--out-dir", default="artifacts/scraped_v2")
    ap.add_argument("--screen-prefix", default="sd")
    ap.add_argument(
        "--sleep-seconds",
        type=int,
        default=180,
        help="Sleep between iterations when nothing to schedule",
    )
    ap.add_argument(
        "--max-iterations", type=int, default=0, help="0 = run forever until worklist empty"
    )
    ap.add_argument(
        "--launch",
        action="store_true",
        help="Required to actually start screens (default: dry-run)",
    )
    ap.add_argument("--dry-run", action="store_true", help="Force dry-run regardless of --launch")
    args = ap.parse_args()
    ROOT = args.root.expanduser().resolve()
    WORKLIST = ROOT / "catalog" / "scrape_worklist.csv"
    SITEMAPS = ROOT / "offprint" / "offprint" / "sitemaps"

    # Default safety: if neither --launch nor --dry-run was passed, dry-run.
    if not args.launch:
        args.dry_run = True

    DRAINER_ROOT.mkdir(parents=True, exist_ok=True)
    used_screens: set[str] = set()

    print(f"drainer started prefix={args.screen_prefix} dry_run={args.dry_run}")
    i = 0
    while True:
        i += 1
        if STOP_SENTINEL.exists():
            print(f"[stop] sentinel {STOP_SENTINEL} present — exiting")
            break
        result = iteration(args, used_screens)
        if result["done"]:
            print(f"[done] worklist empty after {i} iterations")
            break
        if args.max_iterations and i >= args.max_iterations:
            print(f"[stop] reached --max-iterations {args.max_iterations}")
            break
        if args.dry_run:
            print("[dry] exiting after one iteration")
            break
        # Sleep until next refill check. Short if we just scheduled a lot,
        # longer if we're at capacity (no point checking immediately).
        sleep = args.sleep_seconds if result["scheduled"] == 0 else max(60, args.sleep_seconds // 3)
        print(f"[sleep] {sleep}s until next iteration")
        time.sleep(sleep)


if __name__ == "__main__":
    main()
