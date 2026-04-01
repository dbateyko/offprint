#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from offprint.pdf_footnotes.evaluation import (  # noqa: E402
    evaluate_predictions,
    write_evaluation_report,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate footnote extraction against a gold set")
    parser.add_argument("--gold", required=True, help="Path to gold annotations JSON")
    parser.add_argument("--out", default="", help="Optional output report path")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    metrics = evaluate_predictions(args.gold)
    report_path = write_evaluation_report(metrics, out_path=args.out)
    payload = {"report_path": report_path, "metrics": metrics}
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
