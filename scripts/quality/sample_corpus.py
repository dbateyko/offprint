#!/usr/bin/env python3
"""Stratified PDF sampler for footnote extraction quality benchmarking.

Produces a deterministic manifest of ~N PDFs across journal directories:
- per-journal quota ~= sqrt(journal_pdf_count), scaled to hit the target total
- within a journal: oldest / middle / newest by mtime (cycled if quota > 3)
- skips sidecar/conflict directories

Emits one absolute path per line.
"""
from __future__ import annotations

import argparse
import math
import random
import sys
from pathlib import Path
from typing import List, Tuple

SKIP_DIRS = {"_sidecar_conflicts", "_smoke_conflicts"}


def enumerate_journals(root: Path) -> List[Tuple[Path, List[Path]]]:
    out: List[Tuple[Path, List[Path]]] = []
    for child in sorted(root.iterdir()):
        if not child.is_dir() or child.name in SKIP_DIRS or child.name.startswith("_"):
            continue
        pdfs = sorted(child.glob("*.pdf"))
        if pdfs:
            out.append((child, pdfs))
    return out


def pick_from_journal(pdfs: List[Path], quota: int) -> List[Path]:
    if quota <= 0 or not pdfs:
        return []
    if quota >= len(pdfs):
        return list(pdfs)
    # Sort by mtime ascending; slice oldest/middle/newest cyclically
    by_mtime = sorted(pdfs, key=lambda p: p.stat().st_mtime)
    n = len(by_mtime)
    idxs: List[int] = []
    # deterministic: oldest, newest, middle, then fill by evenly spaced
    anchors = [0, n - 1, n // 2]
    for a in anchors:
        if len(idxs) >= quota:
            break
        if a not in idxs:
            idxs.append(a)
    if len(idxs) < quota:
        # Fill by evenly spaced positions
        step = max(1, n // quota)
        for i in range(0, n, step):
            if i not in idxs:
                idxs.append(i)
            if len(idxs) >= quota:
                break
    idxs = sorted(set(idxs))[:quota]
    return [by_mtime[i] for i in idxs]


def build_sample(root: Path, target_total: int, max_per_journal: int, seed: int) -> List[Path]:
    journals = enumerate_journals(root)
    if not journals:
        return []
    # sqrt weighting
    raw_weights = [math.sqrt(len(pdfs)) for _, pdfs in journals]
    wsum = sum(raw_weights)
    # Initial quotas
    quotas = [
        max(1, min(max_per_journal, round(target_total * w / wsum))) for w in raw_weights
    ]
    # Clamp to available
    quotas = [min(q, len(pdfs)) for q, (_, pdfs) in zip(quotas, journals)]

    # Scale down if overshoot
    total = sum(quotas)
    if total > target_total:
        # Proportionally shrink but keep >=1 for journals with any PDFs
        factor = target_total / total
        new_q = []
        for q, (_, pdfs) in zip(quotas, journals):
            scaled = max(1, int(round(q * factor))) if pdfs else 0
            new_q.append(min(scaled, len(pdfs), max_per_journal))
        quotas = new_q
    # Scale up if undershoot (add 1 to largest journals by available headroom)
    rng = random.Random(seed)
    while sum(quotas) < target_total:
        # pick journal with largest headroom
        headrooms = [
            min(max_per_journal, len(pdfs)) - q for q, (_, pdfs) in zip(quotas, journals)
        ]
        if max(headrooms) <= 0:
            break
        max_hr = max(headrooms)
        candidates = [i for i, h in enumerate(headrooms) if h == max_hr]
        idx = rng.choice(candidates)
        quotas[idx] += 1

    out: List[Path] = []
    for (jdir, pdfs), quota in zip(journals, quotas):
        out.extend(pick_from_journal(pdfs, quota))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--root",
        default="/mnt/shared_storage/law-review-corpus/corpus/scraped",
        help="Corpus root (journal dirs as children)",
    )
    ap.add_argument("--target", type=int, default=1000, help="Target sample size")
    ap.add_argument("--max-per-journal", type=int, default=3)
    ap.add_argument("--seed", type=int, default=20260419)
    ap.add_argument(
        "--out",
        default="artifacts/samples/sample_1k.txt",
        help="Output manifest path (one abs path per line)",
    )
    args = ap.parse_args()

    root = Path(args.root).resolve()
    if not root.is_dir():
        print(f"root not found: {root}", file=sys.stderr)
        return 2

    paths = build_sample(root, args.target, args.max_per_journal, args.seed)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(str(p) for p in paths) + "\n")
    print(f"wrote {len(paths)} paths to {out}")
    # Summary: journals covered
    jset = {p.parent.name for p in paths}
    print(f"journals covered: {len(jset)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
