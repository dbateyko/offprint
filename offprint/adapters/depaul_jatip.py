from __future__ import annotations

import threading
from typing import Iterable, Optional
from urllib.parse import parse_qs, urlparse

from .base import DiscoveryResult
from .digital_commons_origin_fetch import DigitalCommonsOriginFetchAdapter


class DePaulJATIPAdapter(DigitalCommonsOriginFetchAdapter):
    """Publication-scoped adapter for DePaul's ``jatip`` collection.

    ``via.library.depaul.edu`` hosts several journals, so routing the repository
    host as a unit can silently attribute thousands of unrelated PDFs to JATIP.
    This adapter accepts only the JATIP archive and its matching PDF context.
    """

    PUBLICATION_SLUG = "jatip"
    _delivery_halt_lock = threading.Lock()
    _delivery_halted = False

    @classmethod
    def _is_jatip_page(cls, url: str) -> bool:
        parsed = urlparse(url)
        return parsed.netloc.lower() == "via.library.depaul.edu" and (
            parsed.path.lower() == f"/{cls.PUBLICATION_SLUG}"
            or parsed.path.lower().startswith(f"/{cls.PUBLICATION_SLUG}/")
        )

    @classmethod
    def _is_jatip_pdf(cls, url: str) -> bool:
        parsed = urlparse(url)
        context = (parse_qs(parsed.query).get("context") or [""])[0].lower()
        return (
            parsed.netloc.lower() == "via.library.depaul.edu"
            and context == cls.PUBLICATION_SLUG
        )

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        if not self._is_jatip_page(seed_url):
            raise ValueError("DePaulJATIPAdapter requires a /jatip publication seed")

        for result in super().discover_pdfs(seed_url, max_depth=max_depth):
            if not self._is_article(result):
                continue
            if not self._is_jatip_page(result.page_url):
                continue
            if not self._is_jatip_pdf(result.pdf_url):
                continue
            yield result

    def download_pdf(self, pdf_url: str, out_dir: str, **kwargs) -> Optional[str]:
        if not self._is_jatip_pdf(pdf_url):
            raise ValueError("refusing a Digital Commons PDF outside context=jatip")

        with self._delivery_halt_lock:
            halted = self.__class__._delivery_halted
        if halted:
            self._set_download_meta(
                error_type="waf_challenge",
                message="DePaul JATIP delivery lane halted after an earlier failure",
                waf_action="halt_publication_lane",
                blocked_reason="prior_delivery_failure",
                final_url=pdf_url,
            )
            return None

        path = super().download_pdf(pdf_url, out_dir, **kwargs)
        if path is None:
            # This lane is deliberately fail-closed: one failed browser-origin
            # delivery may be the first sign of a 403, 429, or challenge. Leave
            # recovery to a later process instead of probing the edge again.
            with self._delivery_halt_lock:
                self.__class__._delivery_halted = True
            self._apply_waf_cooldown()
            self.last_download_meta["waf_action"] = "halt_publication_lane"
            self.last_download_meta["blocked_reason"] = "delivery_failure"
        return path
