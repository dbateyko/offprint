from __future__ import annotations

import sys
from typing import Mapping, Set


def warn_legacy_paths(
    *,
    tool_name: str,
    values_by_arg: Mapping[str, str],
    legacy_by_arg: Mapping[str, Set[str]],
) -> None:
    """Emit consistent warnings when callers pass legacy output paths."""
    for arg_name, legacy_values in legacy_by_arg.items():
        value = str(values_by_arg.get(arg_name, ""))
        if value in legacy_values:
            print(
                f"[{tool_name}] Warning: legacy path '{value}' detected for --{arg_name.replace('_', '-')}. "
                "Prefer artifacts/* paths.",
                file=sys.stderr,
            )
