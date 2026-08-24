from __future__ import annotations

import re
from typing import Iterable

from .base import DiscoveryResult
from .digital_commons_issue_article_hop import DigitalCommonsIssueArticleHopAdapter


class SMUScienceTechnologyLawReviewAdapter(DigitalCommonsIssueArticleHopAdapter):
    """Publication-scoped Digital Commons adapter that excludes journal furniture."""

    # The SMU delivery edge returned 30 consecutive generic 403s after 236
    # successful downloads at the Digital Commons default 2--4 second cadence,
    # then resumed serving PDFs.  Keep this publication below that observed
    # rolling-request threshold.  Values supplied by an operator may make the
    # scraper slower, but cannot accidentally make this adapter less cautious.
    MIN_DOWNLOAD_DELAY_MS = 6_000
    MAX_DOWNLOAD_DELAY_MS = 8_000

    NON_ARTICLE_TITLE_RE = re.compile(
        r"^(?:front matter|back matter|masthead|table of contents|editorial board)$",
        re.IGNORECASE,
    )

    def __init__(self, session=None):
        super().__init__(session=session)
        # Manifest-only recovery selects the adapter and downloads immediately;
        # it does not call configure_dc first.  Make the safe cadence the
        # adapter default as well as enforcing it in configure_dc below.
        self.dc_min_domain_delay_ms = self.MIN_DOWNLOAD_DELAY_MS
        self.dc_max_domain_delay_ms = self.MAX_DOWNLOAD_DELAY_MS

    @classmethod
    def _is_article(cls, result: DiscoveryResult) -> bool:
        title = str((result.metadata or {}).get("title") or "").strip()
        return not cls.NON_ARTICLE_TITLE_RE.fullmatch(title)

    def discover_pdfs(self, seed_url: str, max_depth: int = 0) -> Iterable[DiscoveryResult]:
        for result in super().discover_pdfs(seed_url, max_depth=max_depth):
            if self._is_article(result):
                match = re.search(r"/vol(?P<volume>\d+)/iss(?P<issue>\d+)/", result.page_url)
                if match:
                    result.metadata = dict(result.metadata or {})
                    result.metadata.setdefault("volume", match.group("volume"))
                    result.metadata.setdefault("issue", match.group("issue"))
                yield result

    def configure_dc(self, **kwargs) -> None:
        """Use SMU's publication-scoped archive instead of its WAF-sensitive OAI route."""
        kwargs["enum_mode"] = "all_issues_only"
        kwargs["min_domain_delay_ms"] = max(
            int(kwargs.get("min_domain_delay_ms") or 0), self.MIN_DOWNLOAD_DELAY_MS
        )
        kwargs["max_domain_delay_ms"] = max(
            int(kwargs.get("max_domain_delay_ms") or 0),
            kwargs["min_domain_delay_ms"],
            self.MAX_DOWNLOAD_DELAY_MS,
        )
        super().configure_dc(**kwargs)
