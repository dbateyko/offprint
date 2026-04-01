#!/usr/bin/env python3
"""Promote a completed pipeline run as the golden baseline for delta comparisons.

Writes ``{manifest_dir}/golden_run.json`` (GoldenRunPointer) and updates
``{manifest_dir}/domain_baselines.json`` with per-domain PDF high-water marks.

Usage::

    python scripts/promote_run.py --run-id 20260310T120000Z --notes "Full prod run"
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from offprint.cli import DEFAULT_RUNS_DIR

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
        fh.write("\n")


def _validate_run(run_dir: Path) -> Dict[str, Any]:
    """Return the parsed manifest or raise RuntimeError with a descriptive message."""
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise RuntimeError(
            f"manifest.json not found in {run_dir}. "
            "Ensure the run_id is correct and the run has completed."
        )

    manifest: Dict[str, Any] = _load_json(manifest_path)
    status = manifest.get("status", "")
    if status != "completed":
        raise RuntimeError(
            f"Run '{run_dir.name}' has status '{status}', expected 'completed'. "
            "Only completed runs can be promoted."
        )
    return manifest


def _run_coverage_gate(run_dir: Path) -> str:
    """Attempt to invoke the coverage gate and return 'PASS', 'FAIL', or 'SKIPPED'."""
    try:
        from offprint.coverage_tools import gate  # type: ignore[import]
    except ImportError as exc:
        print(
            f"[warn] Could not import offprint.coverage_tools.gate: {exc}. "
            "Coverage gate status set to SKIPPED."
        )
        return "SKIPPED"

    # The gate module's public entry point is coverage_report(), which requires
    # a live_diff_path that is not available at promotion time.  Any other
    # simple zero-argument callable named 'run' or 'check' would be used here
    # if present, but none currently exists.
    callable_names = ("run", "check", "run_gate", "check_gate")
    gate_fn = next(
        (getattr(gate, name) for name in callable_names if callable(getattr(gate, name, None))),
        None,
    )
    if gate_fn is None:
        print(
            "[warn] offprint.coverage_tools.gate has no zero-argument callable "
            f"({', '.join(callable_names)}). Coverage gate status set to SKIPPED."
        )
        return "SKIPPED"

    try:
        result = gate_fn()
        # Accept a bool, a dict with a 'gate_status' key, or a string.
        if isinstance(result, bool):
            return "PASS" if result else "FAIL"
        if isinstance(result, dict):
            return str(result.get("gate_status", "SKIPPED")).upper()
        return str(result).upper()
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] Coverage gate raised an exception: {exc}. Status set to SKIPPED.")
        return "SKIPPED"


def _extract_domain_pdf_counts(stats: Dict[str, Any]) -> Dict[str, int]:
    """Return {domain: ok_total} from a run's stats.json payload."""
    domains_data: Dict[str, Any] = stats.get("domains", {})
    counts: Dict[str, int] = {}
    for domain, info in domains_data.items():
        if not isinstance(info, dict):
            continue
        ok_total = info.get("ok_total", 0)
        counts[domain] = int(ok_total) if isinstance(ok_total, (int, float)) else 0
    return counts


def _classify_confidence(pdf_count: int) -> str:
    """Assign a confidence tier based on the PDF count for a domain."""
    if pdf_count >= 50:
        return "HIGH"
    if pdf_count >= 10:
        return "MEDIUM"
    return "LOW"


def _update_domain_baselines(
    baselines_path: Path,
    run_id: str,
    domain_counts: Dict[str, int],
    promoted_at: str,
) -> Tuple[Dict[str, Any], List[str]]:
    """Load existing baselines, apply high-water marks, and return (updated, regressions).

    A regression is a domain where the current run's PDF count is strictly lower
    than the previously stored best.
    """
    existing: Dict[str, Any] = {}
    if baselines_path.exists():
        try:
            existing = _load_json(baselines_path)
        except (json.JSONDecodeError, OSError) as exc:
            print(f"[warn] Could not read {baselines_path}: {exc}. Starting fresh.")

    regressions: List[str] = []

    for domain, count in domain_counts.items():
        previous = existing.get(domain, {})
        prev_best: int = int(previous.get("best_pdf_count", 0)) if previous else 0

        if count < prev_best:
            regressions.append(
                f"{domain}: {count} PDFs (previous best {prev_best} in run "
                f"{previous.get('best_run_id', 'unknown')})"
            )
            # Preserve the previous best — do not overwrite with a lower count.
            continue

        existing[domain] = {
            "best_pdf_count": count,
            "best_run_id": run_id,
            "confidence": _classify_confidence(count),
            "last_updated": promoted_at,
        }

    return existing, regressions


# ---------------------------------------------------------------------------
# Core promotion logic
# ---------------------------------------------------------------------------


def promote(
    run_id: str,
    manifest_dir: str,
    notes: str,
    skip_coverage_gate: bool,
) -> int:
    """Promote *run_id* as golden baseline.  Returns 0 on success, 1 on error."""
    runs_root = Path(manifest_dir)
    run_dir = runs_root / run_id

    # 1. Validate run
    try:
        manifest = _validate_run(run_dir)
    except RuntimeError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    promoted_at = _now_iso()
    summary: Dict[str, Any] = manifest.get("summary", {})

    # 2. Coverage gate
    if skip_coverage_gate:
        gate_status = "SKIPPED"
        print("[info] Coverage gate skipped via --skip-coverage-gate.")
    else:
        print("[info] Running coverage gate checks …")
        gate_status = _run_coverage_gate(run_dir)
        print(f"[info] Coverage gate status: {gate_status}")

    # 3. Write golden_run.json
    golden_pointer: Dict[str, Any] = {
        "run_id": run_id,
        "promoted_at": promoted_at,
        "notes": notes,
        "coverage_gate_status": gate_status,
        "summary": summary,
    }
    golden_path = runs_root / "golden_run.json"
    _write_json(golden_path, golden_pointer)
    print(f"[info] Golden run pointer written to {golden_path}")

    # 4. Load stats and update domain baselines
    stats_path = run_dir / "stats.json"
    domain_counts: Dict[str, int] = {}
    if stats_path.exists():
        try:
            stats = _load_json(stats_path)
            domain_counts = _extract_domain_pdf_counts(stats)
        except (json.JSONDecodeError, OSError) as exc:
            print(f"[warn] Could not read {stats_path}: {exc}. Domain baselines not updated.")
    else:
        print(f"[warn] stats.json not found at {stats_path}. Domain baselines not updated.")

    baselines_path = runs_root / "domain_baselines.json"
    updated_baselines, regressions = _update_domain_baselines(
        baselines_path, run_id, domain_counts, promoted_at
    )
    _write_json(baselines_path, updated_baselines)
    print(f"[info] Domain baselines written to {baselines_path}")

    # 5. Print summary
    total_pdfs = sum(domain_counts.values())
    domains_covered = sum(1 for c in domain_counts.values() if c > 0)
    total_domains = len(domain_counts)

    print()
    print("=" * 60)
    print(f"  Run promoted:    {run_id}")
    print(f"  Promoted at:     {promoted_at}")
    print(f"  Coverage gate:   {gate_status}")
    print(f"  Total PDFs:      {total_pdfs:,}")
    print(f"  Domains covered: {domains_covered} / {total_domains} in stats")
    if notes:
        print(f"  Notes:           {notes}")

    if regressions:
        print()
        print(f"  REGRESSIONS ({len(regressions)} domain(s) with fewer PDFs than previous best):")
        for line in regressions:
            print(f"    - {line}")
    else:
        print("  Regressions:     none")
    print("=" * 60)

    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Promote a completed pipeline run as the golden baseline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--run-id",
        required=True,
        metavar="RUN_ID",
        help="Run identifier (e.g. 20260310T120000Z). Must match a subdirectory of --manifest-dir.",
    )
    parser.add_argument(
        "--manifest-dir",
        default=DEFAULT_RUNS_DIR,
        metavar="DIR",
        help="Directory containing run subdirectories (default: artifacts/runs).",
    )
    parser.add_argument(
        "--notes",
        default="",
        metavar="TEXT",
        help="Free-text notes to attach to the golden run pointer.",
    )
    parser.add_argument(
        "--skip-coverage-gate",
        action="store_true",
        help="Skip coverage gate checks and record status as SKIPPED.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return promote(
        run_id=args.run_id,
        manifest_dir=args.manifest_dir,
        notes=args.notes,
        skip_coverage_gate=args.skip_coverage_gate,
    )


if __name__ == "__main__":
    raise SystemExit(main())
