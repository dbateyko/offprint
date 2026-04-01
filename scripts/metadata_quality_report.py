#!/usr/bin/env python3
from __future__ import annotations

import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parent
runpy.run_path(str((ROOT / "reporting/metadata_quality_report.py").resolve()), run_name="__main__")
