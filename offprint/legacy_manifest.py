from __future__ import annotations

import os


def _env_flag(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def should_write_legacy_manifests(explicit: bool | None = None) -> bool:
    """Return legacy-manifest policy.

    Priority:
    1) explicit CLI/runtime setting
    2) environment variable LRS_WRITE_LEGACY_MANIFESTS
    3) default False
    """
    if explicit is not None:
        return bool(explicit)
    return _env_flag("LRS_WRITE_LEGACY_MANIFESTS", default=False)
