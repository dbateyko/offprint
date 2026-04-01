#!/usr/bin/env python3
from __future__ import annotations

import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parent
runpy.run_path(str((ROOT / "processing/run_olmocr_dual_gpu.py").resolve()), run_name="__main__")
