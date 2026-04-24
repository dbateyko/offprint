#!/usr/bin/env python3
"""Run the solver PoC across the full 1K sample and compare to the baseline audit."""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent))
from solver_poc import evaluate  # noqa

def run_one(pdf: str) -> dict:
    try:
        return evaluate(pdf)
    except Exception as e:
        return {"pdf": pdf, "error": str(e)[:300]}


def main():
    # Load baseline audit for per-doc comparison
    baseline = {r["pdf"]: r for r in json.load(open("artifacts/runs/liteparse_only_1k.json"))["rows"]}
    included_pdfs = [r["pdf"] for r in baseline.values() if r.get("doc_policy", {}).get("include")]
    print(f"running solver on {len(included_pdfs)} included PDFs")

    t0 = time.time()
    results = []
    with ProcessPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(run_one, pdf): pdf for pdf in included_pdfs}
        done = 0
        for f in as_completed(futures):
            r = f.result()
            results.append(r)
            done += 1
            if done % 50 == 0:
                print(f"  {done}/{len(included_pdfs)} ({time.time()-t0:.0f}s)")

    print(f"total elapsed: {time.time()-t0:.0f}s")

    # Per-doc join with baseline
    joined = []
    for r in results:
        b = baseline.get(r["pdf"])
        joined.append({
            **r,
            "baseline_status": b.get("selected_status") if b else None,
            "baseline_candidate": b.get("selected_candidate") if b else None,
        })

    # Summary
    status_ct = {}
    baseline_ct = {}
    transitions = {}
    for r in joined:
        s = r.get("status", "error")
        b = r.get("baseline_status", "unknown")
        status_ct[s] = status_ct.get(s, 0) + 1
        baseline_ct[b] = baseline_ct.get(b, 0) + 1
        transitions[(b, s)] = transitions.get((b, s), 0) + 1

    print("\n=== Solver vs Baseline (on 744 included docs) ===")
    print("Baseline status counts:", baseline_ct)
    print("Solver status counts:", status_ct)
    print("\nTransitions (baseline → solver):")
    for (b, s), ct in sorted(transitions.items()):
        print(f"  {b:>16} → {s:<16} : {ct}")

    total_included = sum(1 for r in joined if r.get("baseline_status") != "excluded")
    solver_valid = status_ct.get("valid", 0)
    solver_vog = status_ct.get("valid_with_gaps", 0)
    errors = status_ct.get("error", 0) + sum(1 for r in joined if "error" in r)
    print(f"\nSolver strict-valid rate: {solver_valid}/{total_included} = {100*solver_valid/total_included:.1f}%")
    print(f"Solver ≥valid_with_gaps rate: {(solver_valid+solver_vog)}/{total_included} = {100*(solver_valid+solver_vog)/total_included:.1f}%")
    print(f"Errors: {errors}")

    out = Path("artifacts/runs/solver_1k.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        "rows": joined,
        "summary": {
            "total_included": total_included,
            "solver_status_counts": status_ct,
            "baseline_status_counts": baseline_ct,
            "transitions": {f"{k[0]}->{k[1]}": v for k, v in transitions.items()},
            "solver_strict_valid": solver_valid,
            "solver_valid_with_gaps": solver_vog,
            "solver_strict_valid_rate": round(solver_valid/total_included, 4),
            "solver_any_valid_rate": round((solver_valid+solver_vog)/total_included, 4),
        },
    }, default=str, indent=2))
    print(f"\nResults written to {out}")


if __name__ == "__main__":
    main()
