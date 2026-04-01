#!/usr/bin/env python3
from __future__ import annotations

import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parent
runpy.run_path(str((ROOT / "processing/qc_quarantine_pdfs.py").resolve()), run_name="__main__")
