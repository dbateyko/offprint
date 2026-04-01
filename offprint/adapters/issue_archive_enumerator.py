from __future__ import annotations

from typing import Any

from .generic import GenericAdapter


# Compatibility shim: legacy host configs dict (no longer used, kept for test imports)
_HOST_CONFIGS: dict[str, dict[str, Any]] = {}


class IssueArchiveEnumeratorAdapter(GenericAdapter):
    """Compatibility shim for legacy issue-archive enumerator routing.

    This preserves registry import targets while defaulting to generic discovery.
    """

    def _classify_link(self, url: str, config: dict[str, Any]) -> str:
        """Classify a link based on config path prefixes."""
        from urllib.parse import urlparse

        path = urlparse(url).path.lower()

        if path.endswith(".pdf"):
            return "pdf"

        archive_prefixes = config.get("archive_path_prefixes", [])
        issue_prefixes = config.get("issue_path_prefixes", [])
        article_prefixes = config.get("article_path_prefixes", [])

        # Check issue first (more specific) before archive
        for prefix in issue_prefixes:
            if path.startswith(prefix.lower()):
                return "issue"

        for prefix in article_prefixes:
            if path.startswith(prefix.lower()):
                return "article"

        for prefix in archive_prefixes:
            if path == prefix.lower() or path == prefix.lower().rstrip("/"):
                return "archive"

        return "other"


def register_enumerator_config(domain: str, config: dict[str, Any] | None = None) -> None:
    """Register a host config for the enumerator (compatibility shim)."""
    if config is not None:
        _HOST_CONFIGS[domain] = config


def get_enumerator_config(domain: str) -> dict[str, Any] | None:
    """Get config for a domain, with www fallback (compatibility shim)."""
    if domain in _HOST_CONFIGS:
        return _HOST_CONFIGS[domain]
    # Try without www prefix
    if domain.startswith("www."):
        return _HOST_CONFIGS.get(domain[4:])
    return None
