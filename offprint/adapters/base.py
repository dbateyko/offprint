from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional

import requests


@dataclass
class DiscoveryResult:
    page_url: str
    pdf_url: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    source_adapter: str = ""
    extraction_path: str = ""
    retrieved_at: str = ""
    http_status: int = 0
    content_type: str = ""
    pdf_sha256: Optional[str] = None
    pdf_size_bytes: Optional[int] = None

    def __post_init__(self):
        """Ensure metadata is always a dict."""
        if self.metadata is None:
            self.metadata = {}


# Type alias for session-like objects (requests.Session or PlaywrightSession)
SessionLike = Any  # Both have .get() method


class Adapter:
    """Adapter for a specific journal platform."""

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.last_download_meta: Dict[str, Any] = self._normalize_download_meta({})

    def _set_download_meta(self, **kwargs: Any) -> None:
        self.last_download_meta = self._normalize_download_meta(kwargs)

    def _normalize_download_meta(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        defaults: Dict[str, Any] = {
            "ok": False,
            "error_type": "unknown",
            "message": "",
            "status_code": 0,
            "content_type": "",
            "pdf_sha256": None,
            "pdf_size_bytes": None,
            "waf_action": "",
            "download_method": "requests",
            "fallback_used": False,
            "attempt": 1,
            "ua_profile_used": "",
            "robots_allowed": None,
            "download_status_class": "",
            "blocked_reason": "",
            "retry_after_hint": None,
        }
        normalized = dict(defaults)
        normalized.update(raw or {})
        return normalized

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        """Discover PDFs starting from a seed URL."""
        raise NotImplementedError

    def _download_with_generic(
        self, pdf_url: str, out_dir: str, **kwargs: Any
    ) -> Optional[str]:
        """Fallback download path shared by most adapters."""
        from .generic import GenericAdapter

        generic = GenericAdapter(session=self.session)
        local_path = generic.download_pdf(pdf_url, out_dir, **kwargs)
        self.last_download_meta = dict(generic.last_download_meta or {})
        return local_path

    def download_pdf(self, pdf_url: str, out_dir: str, **kwargs: Any) -> Optional[str]:
        """Download a PDF and return the local path."""
        return self._download_with_generic(pdf_url, out_dir, **kwargs)

    def get_stats(self) -> Dict[str, Any]:
        """Return discovery statistics and platform-specific metadata."""
        return {}
