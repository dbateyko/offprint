from __future__ import annotations

from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import DiscoveryResult
from .generic import GenericAdapter


class ExampleSiteAdapter(GenericAdapter):
    """Specialized behavior for example.org-like sites.

    Heuristic: if on the seed page we see links that look like volumes/issues (e.g., '/vol', '/issue'),
    follow them even when max_depth == 0 so we can discover PDFs one level down.
    """

    def discover_pdfs(self, seed_url: str, max_depth: int = 0):  # type: ignore[override]
        # Keep smoke runs bounded and deterministic for this host family:
        # parse the seed page once, then perform one issue hop.
        if max_depth == 0:
            seen_pdf_urls: set[str] = set()
            resp = self._get(seed_url)
            if not resp or not resp.text:
                return
            soup = BeautifulSoup(resp.text, "lxml")
            issue_pages = []
            for a in soup.find_all("a", href=True):
                href = a["href"].lower()
                absolute = urljoin(seed_url, a["href"])
                if absolute.lower().endswith(".pdf") and absolute not in seen_pdf_urls:
                    seen_pdf_urls.add(absolute)
                    yield DiscoveryResult(page_url=seed_url, pdf_url=absolute)
                if any(token in href for token in ["/vol", "/issue", "volume", "journal"]):
                    issue_pages.append(absolute)

            for page in issue_pages:
                resp2 = self._get(page)
                if not resp2 or not resp2.text:
                    continue
                for a2 in BeautifulSoup(resp2.text, "lxml").find_all("a", href=True):
                    link = urljoin(page, a2["href"])
                    if link.lower().endswith(".pdf") and link not in seen_pdf_urls:
                        seen_pdf_urls.add(link)
                        yield DiscoveryResult(page_url=page, pdf_url=link)
            return

        # Non-smoke runs keep generic behavior unchanged.
        yield from super().discover_pdfs(seed_url, max_depth=max_depth)
