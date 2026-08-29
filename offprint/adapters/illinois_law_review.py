"""Host-specific adapter for the University of Illinois Law Review.

`illinoislawreview.org` is a stock WordPress install, so almost all of the
`WordPressAcademicBaseAdapter` behaviour is correct.  The one site-specific
quirk is that the journal splits its PDF hosting across two origins:

* pre-2017 issues (``/print/volume-YYYY-issue-N/``) keep their PDFs on
  ``illinoislawreview.org/wp-content/uploads/...`` (same origin), and
* 2017-and-later issues (``/print/vol-YYYY-no-N/``) link out to the college's
  companion WordPress install at ``illinoislawrev.web.illinois.edu``, again
  under ``/wp-content/uploads/``.

The generic adapter's origin gate (``_is_preferred_pdf_url``) correctly drops
the second group because the host is neither same-origin nor one of the
"trusted repository" hosts.  Rather than widening that gate for every
WordPress journal, this adapter adds a single explicitly named companion host
and still requires the ``/wp-content/uploads/`` prefix, so a reference-list
link to some unrelated third-party PDF is still rejected.
"""

from __future__ import annotations

from urllib.parse import urlparse

from .wordpress_academic_base import WordPressAcademicBaseAdapter


class IllinoisLawReviewAdapter(WordPressAcademicBaseAdapter):
    """WordPress adapter that also accepts the journal's companion PDF host."""

    #: Hosts (besides the seed origin) that are allowed to serve article PDFs.
    #: Deliberately an exact-match allowlist, not a substring/suffix hint.
    COMPANION_PDF_HOSTS = frozenset(
        {
            "illinoislawrev.web.illinois.edu",
            "www.illinoislawrev.web.illinois.edu",
            "publish.illinois.edu",
        }
    )

    #: Path prefix required on the companion hosts.  ``publish.illinois.edu``
    #: is a multi-tenant campus WordPress, so PDFs there must also sit under
    #: the law review's own ``/lawreview/`` tenant path.
    COMPANION_PATH_PREFIXES = {
        "illinoislawrev.web.illinois.edu": ("/wp-content/uploads/",),
        "www.illinoislawrev.web.illinois.edu": ("/wp-content/uploads/",),
        "publish.illinois.edu": ("/lawreview/",),
    }

    def _is_companion_pdf_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.netloc or "").lower()
        if host not in self.COMPANION_PDF_HOSTS:
            return False
        path = (parsed.path or "").lower()
        if not path.endswith(".pdf"):
            return False
        prefixes = self.COMPANION_PATH_PREFIXES.get(host, ())
        return any(path.startswith(prefix) for prefix in prefixes)

    def _is_preferred_pdf_url(self, url: str) -> bool:
        if super()._is_preferred_pdf_url(url):
            return True
        return self._is_companion_pdf_url(url)

    def _is_valid_volume_issue_url(self, url: str) -> bool:
        # Navigation stays on the seed origin; the companion hosts only ever
        # serve the PDF binaries, never the issue/article index pages.
        return super()._is_valid_volume_issue_url(url)
