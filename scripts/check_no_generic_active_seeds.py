#!/usr/bin/env python3
from __future__ import annotations

import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parent
runpy.run_path(str((ROOT / "quality/check_no_generic_active_seeds.py").resolve()), run_name="__main__")
