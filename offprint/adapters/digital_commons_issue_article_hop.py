from __future__ import annotations

from .digital_commons_base import DigitalCommonsBaseAdapter


class DigitalCommonsIssueArticleHopAdapter(DigitalCommonsBaseAdapter):
    """Compatibility adapter for issue->article traversal on Digital Commons.

    The repository currently routes most Digital Commons domains to this class name.
    Keep it as a thin shim over `DigitalCommonsBaseAdapter` so existing registry
    mappings and imports remain valid.
    """

