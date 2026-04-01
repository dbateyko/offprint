from __future__ import annotations

from typing import Iterable, Set
from urllib.parse import urlparse

from .base import DiscoveryResult
from .drupal import DrupalAdapter
from .site_archive_base import SiteArchiveAdapterBase


class TLCPAdapter(SiteArchiveAdapterBase):
    """Adapter for Transnational Law and Contemporary Problems (Iowa).

    The `online-edition` page contains direct links to article PDFs under
    `/sites/tlcp.law.uiowa.edu/files/...`.
    """

    ONLINE_EDITION_PATH = "/online-edition"

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        parsed_seed = urlparse(seed_url)
        host = (parsed_seed.netloc or "").lower()
        scheme = parsed_seed.scheme or "https"
        online_url = f"{scheme}://{host}{self.ONLINE_EDITION_PATH}"

        candidate_pages = [seed_url]
        if seed_url.rstrip("/") != online_url.rstrip("/"):
            candidate_pages.append(online_url)

        seen_pdfs: Set[str] = set()
        yielded = False
        for page_url in candidate_pages:
            soup = self._get(page_url)
            for text, url in self._iter_links(soup, page_url):
                parsed = urlparse(url)
                if (parsed.netloc or "").lower() != host:
                    continue
                lowered = url.lower()
                if not self._is_pdf_candidate(url, text) and "/sites/tlcp.law.uiowa.edu/files/" not in lowered:
                    continue
                if ".pdf" not in lowered:
                    continue
                if url in seen_pdfs:
                    continue
                seen_pdfs.add(url)
                yielded = True
                yield DiscoveryResult(
                    page_url=page_url,
                    pdf_url=url,
                    metadata={
                        "title": text or url,
                        "source_url": page_url,
                        "url": page_url,
                        "platform": "Drupal",
                        "extraction_method": "tlcp_online_edition",
                    },
                    source_adapter="tlcp",
                    extraction_path="online_edition_pdf",
                )

        if yielded:
            return

        # Fallback for future site structure changes.
        yield from DrupalAdapter(session=self.session).discover_pdfs(seed_url, max_depth=max(1, max_depth))
