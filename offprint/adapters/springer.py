from __future__ import annotations

from .generic import GenericAdapter


class SpringerAdapter(GenericAdapter):
    """Compatibility shim for Springer-hosted sources.

    Until a dedicated Springer adapter is implemented, use GenericAdapter
    behavior so registry imports and host routing remain functional.
    """

