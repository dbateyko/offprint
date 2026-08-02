#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict, deque
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from offprint.digital_commons_gather import (
    AdaptivePacer,
    PersistentDigitalCommonsBrowser,
    append_attempt,
    fair_round_robin,
    load_items_jsonl,
    load_success_ids,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serial, resumable Digital Commons gatherer")
    parser.add_argument("--queue", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, default=Path("artifacts/dc_gather/pdfs"))
    parser.add_argument("--attempts", type=Path, default=Path("artifacts/dc_gather/attempts.jsonl"))
    parser.add_argument("--max-items", type=int, default=100)
    parser.add_argument("--allow-large-run", action="store_true")
    parser.add_argument("--start-delay-seconds", type=float, default=60.0)
    parser.add_argument("--min-delay-seconds", type=float, default=10.0)
    parser.add_argument("--successes-before-decrease", type=int, default=50)
    parser.add_argument("--timeout-seconds", type=int, default=75)
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--host-failure-threshold", type=int, default=3)
    parser.add_argument("--contact-email", default="")
    parser.add_argument("--project-url", default="")
    parser.add_argument("--max-download-mib-per-second", type=float, default=0.0)
    parser.add_argument("--pressure-host-cooldown-seconds", type=float, default=900.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_items < 1:
        raise SystemExit("--max-items must be positive")
    if args.max_items > 100 and not args.allow_large_run:
        raise SystemExit("Refusing >100 items without --allow-large-run")
    if args.allow_large_run and (not args.contact_email or not args.project_url):
        raise SystemExit(
            "Large runs require --contact-email and --project-url for transparent identity"
        )

    completed = load_success_ids(args.attempts)
    items = [item for item in load_items_jsonl(args.queue) if item.gather_id not in completed]
    ordered = fair_round_robin(items)
    pacer = AdaptivePacer(
        start_delay_seconds=args.start_delay_seconds,
        min_delay_seconds=args.min_delay_seconds,
        successes_before_decrease=args.successes_before_decrease,
    )
    host_failures = defaultdict(int)
    host_cooldown_until = defaultdict(float)
    recent_outcomes = deque(maxlen=100)
    recent_access_failures = deque()
    attempted = downloaded = deferred = 0
    stop_reason = ""

    print(
        json.dumps(
            {
                "event": "dc_gather_start",
                "queue_remaining": len(items),
                "max_items": args.max_items,
                "start_delay_seconds": pacer.delay_seconds,
                "headless": not args.headed,
            },
            sort_keys=True,
        ),
        flush=True,
    )

    with PersistentDigitalCommonsBrowser(
        headless=not args.headed,
        timeout_seconds=args.timeout_seconds,
        contact_email=args.contact_email,
        project_url=args.project_url,
        max_download_mib_per_second=args.max_download_mib_per_second,
    ) as browser:
        for item in ordered:
            if attempted >= args.max_items:
                break
            if host_failures[item.domain] >= args.host_failure_threshold:
                continue
            if time.monotonic() < host_cooldown_until[item.domain]:
                continue
            slept = pacer.wait()
            destination = args.out_dir / item.domain / f"{item.gather_id}.pdf"
            attempt = browser.download(item, destination)
            attempt.dispatch_delay_seconds = round(pacer.delay_seconds, 3)
            append_attempt(args.attempts, attempt)
            attempted += 1

            if attempt.status == "downloaded":
                downloaded += 1
                host_failures[item.domain] = 0
                pacer.record_success()
                recent_outcomes.append(True)
            else:
                deferred += 1
                host_failures[item.domain] += 1
                recent_outcomes.append(False)
                if attempt.http_status in {429, 503}:
                    if attempt.retry_after_seconds:
                        pacer.record_pressure(attempt.retry_after_seconds)
                        cooldown = attempt.retry_after_seconds
                    else:
                        pacer.record_pressure()
                        cooldown = args.pressure_host_cooldown_seconds
                    host_cooldown_until[item.domain] = time.monotonic() + max(cooldown, 0.0)
                else:
                    pacer.record_failure()
                if attempt.http_status == 403 or "waf_action=" in attempt.error:
                    now = time.monotonic()
                    recent_access_failures.append((now, item.domain))
                    while recent_access_failures and now - recent_access_failures[0][0] > 600:
                        recent_access_failures.popleft()
                    affected_hosts = {domain for _, domain in recent_access_failures}
                    if len(affected_hosts) >= 3:
                        stop_reason = "access_failures_on_three_hosts_within_ten_minutes"

            print(
                json.dumps(
                    {
                        "event": "dc_gather_attempt",
                        "gather_id": item.gather_id,
                        "domain": item.domain,
                        "status": attempt.status,
                        "http_status": attempt.http_status,
                        "error": attempt.error,
                        "bytes": attempt.pdf_size_bytes,
                        "elapsed_seconds": attempt.elapsed_seconds,
                        "slept_seconds": round(slept, 3),
                        "next_delay_seconds": round(pacer.delay_seconds, 3),
                        "retry_after_seconds": attempt.retry_after_seconds,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            if len(recent_outcomes) == 100 and sum(not ok for ok in recent_outcomes) > 1:
                stop_reason = stop_reason or (
                    "rolling_access_or_content_failure_rate_above_one_percent"
                )
            if stop_reason:
                break

    print(
        json.dumps(
            {
                "event": "dc_gather_complete",
                "attempted": attempted,
                "downloaded": downloaded,
                "deferred": deferred,
                "remaining_unattempted": max(len(items) - attempted, 0),
                "stop_reason": stop_reason,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    if stop_reason:
        return 3
    return 0 if downloaded else 2


if __name__ == "__main__":
    sys.exit(main())
