from __future__ import annotations

from typing import Generator, Optional
from urllib.parse import urlparse

from .base import DiscoveryResult
from .wordpress_academic_base import WordPressAcademicBaseAdapter


class GeorgetownLawWPAdapter(WordPressAcademicBaseAdapter):
    """Host adapter for ``www.law.georgetown.edu`` WordPress **multisite** journals.

    ``law.georgetown.edu`` is the whole law school. Each student journal lives in
    its own WordPress *subsite* mounted on the first path segment
    (``/georgetown-law-journal/``, ``/american-criminal-law-review/``, ...), and
    each subsite has its **own** REST API and sitemap:

        https://www.law.georgetown.edu/georgetown-law-journal/wp-json/wp/v2/pages
        https://www.law.georgetown.edu/georgetown-law-journal/wp-sitemap.xml

    ``WordPressAcademicBaseAdapter`` roots those endpoints at the *origin*, which
    on this host resolves to the law school's own site (252 pages of faculty
    bios, news, and program pages — zero journal articles). That is why the
    Georgetown Law Journal seed discovered ~7 PDFs (the newest issue, reachable
    by HTML crawl) out of 302 published article pages, while the corpus filled
    up with unrelated law-school PDFs (briefs, reports, motions).

    This adapter does two things and nothing else:

    1. Re-roots the REST and XML-sitemap discovery lanes at the journal subsite.
    2. Tightens the same-origin PDF gate. The base class exempts any
       ``/wp-content/uploads/`` path from seed-path scoping because on a
       single-tenant WordPress install that directory is global. On this
       multisite host it is *not* global — ``/american-criminal-law-review/
       wp-content/uploads/...`` is a different journal's PDF — so uploads must
       still sit under the seeded journal's subsite prefix.
    """

    JOURNAL_HOSTS = {"law.georgetown.edu", "www.law.georgetown.edu"}

    # Subsite path segments that are not journals (or are not article content).
    _NON_SUBSITE_SEGMENTS = {"wp-content", "wp-json", "wp-admin", "wp-includes"}

    # ------------------------------------------------------------------ helpers

    def _subsite_slug(self, url: str) -> Optional[str]:
        """Return the WordPress multisite slug (first path segment) for *url*."""
        try:
            parsed = urlparse(url)
        except Exception:
            return None
        if (parsed.netloc or "").lower() not in self.JOURNAL_HOSTS:
            return None
        parts = [p for p in (parsed.path or "").split("/") if p]
        if not parts:
            return None
        slug = parts[0]
        if slug.lower() in self._NON_SUBSITE_SEGMENTS:
            return None
        return slug

    def _subsite_base(self, url: str) -> Optional[str]:
        slug = self._subsite_slug(url)
        if not slug:
            return None
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/{slug}"

    def _pin_base_to_subsite(self, start_url: str) -> None:
        subsite_base = self._subsite_base(start_url)
        if subsite_base:
            self.base_url = subsite_base

    # ------------------------------------------------------- discovery lane fix

    def _discover_via_rest_api(self, start_url: str) -> Generator[DiscoveryResult, None, None]:
        self._pin_base_to_subsite(start_url)
        yield from super()._discover_via_rest_api(start_url)

    def _iter_sitemap_page_urls(self, start_url: str) -> Generator[str, None, None]:
        self._pin_base_to_subsite(start_url)
        yield from super()._iter_sitemap_page_urls(start_url)

    # ------------------------------------------------------------- origin gate

    def _is_preferred_pdf_url(self, url: str) -> bool:
        """Same-origin PDFs must live inside the seeded journal's subsite.

        Off-origin URLs fall through to the base implementation, which only
        admits trusted scholarly repository hosts — an arbitrary third-party
        host linked from a reference list is still rejected there.
        """
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.netloc or "").lower()
        if host in self.JOURNAL_HOSTS:
            slug = self._subsite_slug(url)
            seed_slug = None
            if self._seed_path_prefix:
                seed_parts = [p for p in self._seed_path_prefix.split("/") if p]
                seed_slug = seed_parts[0] if seed_parts else None
            if seed_slug and slug != seed_slug:
                # Another journal on the same multisite host.
                return False
            if not slug:
                return False
            return True
        return super()._is_preferred_pdf_url(url)

    # --------------------------------------------------------------- downloads

    # law.georgetown.edu sits behind Varnish, which 403s the
    # "Windows NT 10.0; Win64; x64 ... Chrome" UA family that
    # ``generic.DEFAULT_HEADERS`` sends on the download GET -- for HTML *and*
    # PDFs. Every other UA tested (Firefox, macOS Chrome, wget, python-requests,
    # a plain bot string) is served 200 by nginx. Discovery survives because the
    # WordPress base adapter rotates UAs; the download path does not, so a seed
    # that discovers cleanly still records 100% ``waf_challenge`` failures.
    DOWNLOAD_USER_AGENT = (
        "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0"
    )

    def download_pdf(self, pdf_url: str, out_dir: str, referer: str = "", **kwargs):
        from .generic import GenericAdapter

        generic = GenericAdapter(
            session=_UserAgentOverrideSession(self.session, self.DOWNLOAD_USER_AGENT)
        )
        local_path = generic.download_pdf(pdf_url, out_dir, referer=referer, **kwargs)
        self.last_download_meta = dict(generic.last_download_meta or {})
        return local_path


class _UserAgentOverrideSession:
    """Thin proxy that forces a User-Agent on outgoing requests.

    ``GenericAdapter.download_pdf`` builds its request headers from the module
    level ``DEFAULT_HEADERS`` and passes them per-request, so a session-level
    header cannot win. Rather than mutating that shared global (which would
    change every other site's downloads), wrap the session for this host only.
    """

    def __init__(self, session, user_agent: str):
        self._session = session
        self._user_agent = user_agent

    def __getattr__(self, name):
        return getattr(self._session, name)

    def _with_ua(self, kwargs: dict) -> dict:
        headers = dict(kwargs.get("headers") or {})
        headers["User-Agent"] = self._user_agent
        kwargs["headers"] = headers
        return kwargs

    def get(self, url, **kwargs):
        return self._session.get(url, **self._with_ua(kwargs))

    def head(self, url, **kwargs):
        return self._session.head(url, **self._with_ua(kwargs))
