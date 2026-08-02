#!/usr/bin/env python3
"""Wait for a bounded DC gate, validate it, then launch the full resumable run."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable


def read_jsonl(path: Path) -> list[dict[str, object]]:
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def evaluate_gate(
    rows: Iterable[dict[str, object]],
    *,
    min_attempts: int,
    max_failure_rate: float,
) -> tuple[bool, str]:
    rows = list(rows)
    completions = [row for row in rows if row.get("event") == "dc_gather_complete"]
    if not completions:
        return False, "incomplete"
    complete = completions[-1]
    attempts = [row for row in rows if row.get("event") == "dc_gather_attempt"]
    attempted = int(complete.get("attempted") or len(attempts))
    deferred = int(complete.get("deferred") or 0)
    if attempted < min_attempts:
        return False, f"only_{attempted}_attempts"
    if str(complete.get("stop_reason") or ""):
        return False, f"stop_reason={complete['stop_reason']}"
    pressure = sum(int(row.get("http_status") or 0) in {403, 429, 503} for row in attempts)
    if pressure:
        return False, f"pressure_responses={pressure}"
    failure_rate = deferred / max(attempted, 1)
    if failure_rate > max_failure_rate:
        return False, f"failure_rate={failure_rate:.4f}"
    return True, (
        f"passed attempted={attempted} deferred={deferred} failure_rate={failure_rate:.4f}"
    )


def tmux_session_exists(name: str) -> bool:
    return (
        subprocess.run(
            ["tmux", "has-session", "-t", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
        == 0
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gate-log", type=Path, required=True)
    parser.add_argument("--gate-session", required=True)
    parser.add_argument("--gate-min-attempts", type=int, default=900)
    parser.add_argument("--max-failure-rate", type=float, default=0.01)
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    parser.add_argument("--queue", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--attempts", type=Path, required=True)
    parser.add_argument("--full-log", type=Path, required=True)
    parser.add_argument("--contact-email", required=True)
    parser.add_argument("--project-url", required=True)
    parser.add_argument("--delay-seconds", type=float, default=10.0)
    parser.add_argument("--max-download-mib-per-second", type=float, default=5.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    while True:
        rows = read_jsonl(args.gate_log)
        passed, reason = evaluate_gate(
            rows,
            min_attempts=args.gate_min_attempts,
            max_failure_rate=args.max_failure_rate,
        )
        if reason != "incomplete":
            if not passed:
                print(f"gate failed; full run not started: {reason}", flush=True)
                return 3
            print(f"gate passed: {reason}", flush=True)
            break
        if not tmux_session_exists(args.gate_session):
            print("gate session ended without a completion record", flush=True)
            return 4
        time.sleep(max(args.poll_seconds, 1.0))

    collector = Path(__file__).with_name("dc_gather.py")
    max_items = sum(1 for line in args.queue.open(encoding="utf-8") if line.strip())
    command = [
        sys.executable,
        str(collector),
        "--queue",
        str(args.queue),
        "--out-dir",
        str(args.out_dir),
        "--attempts",
        str(args.attempts),
        "--max-items",
        str(max_items),
        "--allow-large-run",
        "--start-delay-seconds",
        str(args.delay_seconds),
        "--min-delay-seconds",
        str(args.delay_seconds),
        "--successes-before-decrease",
        str(max_items + 1),
        "--contact-email",
        args.contact_email,
        "--project-url",
        args.project_url,
        "--max-download-mib-per-second",
        str(args.max_download_mib_per_second),
    ]
    args.full_log.parent.mkdir(parents=True, exist_ok=True)
    print("starting full run: " + " ".join(command), flush=True)
    with args.full_log.open("a", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            log.write(line)
            log.flush()
        return process.wait()


if __name__ == "__main__":
    sys.exit(main())
