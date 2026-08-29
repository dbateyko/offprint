from __future__ import annotations

from typing import Iterable, Optional
from urllib.parse import parse_qs, urlparse

from .base import DiscoveryResult
from .digital_commons_origin_fetch import DigitalCommonsOriginFetchAdapter


class ScopedDigitalCommonsTechAdapter(DigitalCommonsOriginFetchAdapter):
    """Exact-publication Digital Commons lane for technology journals."""

    HOST = ""
    PUBLICATION_SLUG = ""

    @classmethod
    def _is_publication_page(cls, url: str) -> bool:
        parsed = urlparse(url)
        path = parsed.path.lower()
        return parsed.netloc.lower() == cls.HOST and (
            path == f"/{cls.PUBLICATION_SLUG}"
            or path.startswith(f"/{cls.PUBLICATION_SLUG}/")
        )

    @classmethod
    def _is_publication_pdf(cls, url: str) -> bool:
        parsed = urlparse(url)
        context = (parse_qs(parsed.query).get("context") or [""])[0].lower()
        return parsed.netloc.lower() == cls.HOST and context == cls.PUBLICATION_SLUG

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        if not self._is_publication_page(seed_url):
            raise ValueError(
                f"{type(self).__name__} requires a /{self.PUBLICATION_SLUG} publication seed"
            )
        for result in super().discover_pdfs(seed_url, max_depth=max_depth):
            if not self._is_article(result):
                continue
            if not self._is_publication_page(result.page_url):
                continue
            if not self._is_publication_pdf(result.pdf_url):
                continue
            yield result

    def download_pdf(self, pdf_url: str, out_dir: str, **kwargs) -> Optional[str]:
        if not self._is_publication_pdf(pdf_url):
            raise ValueError(
                f"refusing a Digital Commons PDF outside context={self.PUBLICATION_SLUG}"
            )
        return super().download_pdf(pdf_url, out_dir, **kwargs)


class SeattleJTEILAdapter(ScopedDigitalCommonsTechAdapter):
    HOST = "digitalcommons.law.seattleu.edu"
    PUBLICATION_SLUG = "sjteil"


class CaseJOLTIAdapter(ScopedDigitalCommonsTechAdapter):
    HOST = "scholarlycommons.law.case.edu"
    PUBLICATION_SLUG = "jolti"
