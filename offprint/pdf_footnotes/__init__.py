"""Footnote extraction and audit pipeline for downloaded law review PDFs."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "BatchConfig",
    "run_batch",
    "QCConfig",
    "run_qc",
    "SampleAuditConfig",
    "run_sample_audit",
]


def __getattr__(name: str) -> Any:
    if name in {"BatchConfig", "run_batch"}:
        module = import_module(".pipeline", __name__)
        return getattr(module, name)
    if name in {"QCConfig", "run_qc"}:
        module = import_module(".qc_filter", __name__)
        return getattr(module, name)
    if name in {"SampleAuditConfig", "run_sample_audit"}:
        module = import_module(".sample_audit", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
