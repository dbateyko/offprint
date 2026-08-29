from __future__ import annotations

"""Adapter for the Harvard Law School shared journals host.

``journals.law.harvard.edu`` is a **WordPress Multisite** install: every journal
is a separate WordPress site mounted at a subdirectory (``/ilj/``, ``/jol/``,
``/lpr/``, ``/crcl/``, ...).  Two consequences drive this adapter:

1. Each journal has its own REST API and its own Jetpack sitemap rooted at the
   *subdirectory*, e.g. ``/ilj/wp-json/wp/v2/posts`` (1,300+ posts) and
   ``/ilj/sitemap.xml``.  ``WordPressAcademicBaseAdapter`` roots those endpoints
   at the bare origin, and the network root site has **zero** posts — which is
   why these journals produced no PDFs at all.

2. ``host != journal``.  Every publication on this host shares one origin, so a
   same-origin check is *not* a publication-scope check.  The base adapter's
   ``_is_preferred_pdf_url`` accepts any same-origin URL containing
   ``/wp-content/uploads/`` regardless of seed scope, which on a multisite host
   would happily attribute ``/crcl/wp-content/uploads/...`` to the ILJ seed.
   ``_url_matches_seed_scope``'s ``allow_siblings`` escape hatch (triggered by a
   ``/category/`` seed prefix, as in ``/ilj/category/archives/``) opens the same
   hole for page discovery.

This adapter therefore pins every discovery lane to a single multisite slug and
overrides *both* scope gates with a strict ``/<slug>/`` path prefix, with no
sibling escape and no uploads bypass.
"""

from typing import Optional
from urllib.parse import urlparse

from .wordpress_academic_base import WordPressAcademicBaseAdapter

HOST = "journals.law.harvard.edu"

# Multisite slugs routed through this adapter.  Add a slug here (and a seed
# JSON) rather than widening the host mapping: other journals on this host are
# already routed to WordPressAcademicBaseAdapter and must not change behaviour
# silently.
SUPPORTED_SLUGS = frozenset({"ilj", "jol", "lpr"})

# Second-segment paths that belong to a *separately seeded* online companion
# (HILJ Online lives at /ilj/online/, HLPR Online at /lpr/online-articles/).
# Those seeds keep their previous routing: widening them to the whole
# ``/<slug>/`` subtree would make the online seed rediscover the print archive
# and attribute print articles to the online companion.
SUB_PUBLICATION_SEGMENTS = frozenset({"online", "online-articles", "markup"})


def slug_for_url(url: str) -> Optional[str]:
    """Return the multisite slug for *url* if it is a supported Harvard journal."""
    try:
        parsed = urlparse(url or "")
    except Exception:
        return None
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if host != HOST:
        return None
    parts = [p for p in (parsed.path or "").split("/") if p]
    if not parts:
        return None
    slug = parts[0].lower()
    if slug not in SUPPORTED_SLUGS:
        return None
    if len(parts) > 1 and parts[1].lower() in SUB_PUBLICATION_SEGMENTS:
        return None
    return slug


class HarvardJournalsMultisiteAdapter(WordPressAcademicBaseAdapter):
    """Publication-scoped adapter for one journal on journals.law.harvard.edu."""

    def __init__(self, slug: str, **kwargs):
        slug = (slug or "").strip("/").lower()
        if not slug:
            raise ValueError("HarvardJournalsMultisiteAdapter requires a multisite slug")
        # base_url carries the subdirectory so REST (/wp-json/...) and Jetpack
        # sitemap (/sitemap.xml) endpoints resolve to THIS journal's site.
        kwargs.setdefault("base_url", f"https://{HOST}/{slug}")
        super().__init__(**kwargs)
        self.slug = slug
        self.scope_prefix = f"/{slug}/"
        # base_url keeps the subpath; domain must stay the bare host so the
        # inherited same-origin comparisons still work.
        self.domain = HOST

    # -- scope gates ---------------------------------------------------------

    def _in_publication_scope(self, url: str) -> bool:
        """Strict same-origin + ``/<slug>/`` path check.

        Used for page discovery *and* PDF acceptance.  No sibling-path escape
        and no ``/wp-content/uploads/`` bypass: on a multisite host both would
        let another journal's content in.
        """
        if not url:
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if host and host != HOST:
            return False
        path = parsed.path or ""
        return path == self.scope_prefix.rstrip("/") or path.startswith(self.scope_prefix)

    def _url_matches_seed_scope(self, url: str) -> bool:  # noqa: D102 - see base
        return self._in_publication_scope(url)

    def _is_preferred_pdf_url(self, url: str) -> bool:
        """Accept only PDFs served from this journal's own multisite subtree.

        Deliberately narrower than the base implementation: it drops the
        trusted-external-repository allowance too, because these journals host
        every article PDF under ``/<slug>/wp-content/uploads/`` and the only
        off-subtree ``.pdf`` links observed on their pages were reference-list
        citations (supremecourt.gov, govinfo.gov, archive.org, third-party
        WordPress sites).
        """
        return self._in_publication_scope(url)

    def _is_valid_article_url(self, url: str) -> bool:
        if not self._in_publication_scope(url):
            return False
        return super()._is_valid_article_url(url)

    def _is_valid_volume_issue_url(self, url: str) -> bool:
        if not self._in_publication_scope(url):
            return False
        return super()._is_valid_volume_issue_url(url)

    # -- discovery shape -----------------------------------------------------

    def discover_pdfs(self, start_url: str, max_depth: int = 0):
        """Always traverse at least one hop from the seeded archive page.

        The archives here are hub pages: the issue/article pages that carry the
        PDFs are one link away.  ``max_depth=0`` (the orchestrator default for
        WordPress seeds) would stop on the hub itself.
        """
        return super().discover_pdfs(start_url, max_depth=max(int(max_depth or 0), 1))

    def _build_discovery_lanes(self, *, start_url: str, html_depth: int):
        """HTML-first, always.

        The seeds point at the *print* archive.  The Jetpack sitemap and the
        REST API enumerate the whole journal site, which on ILJ and LPR mixes in
        the separately-seeded online companions (HILJ Online, HLPR Online) and
        blog posts whose bodies link third-party citation PDFs.  Sitemap/REST
        stay as fallbacks for the case where HTML traversal yields nothing.
        """
        return [
            lambda: self._discover_via_html_parsing(start_url, max(html_depth, 1)),
            lambda: self._discover_via_xml_sitemaps(start_url),
            lambda: self._discover_via_rest_api(start_url),
        ]

    # Legacy ILJ permalinks name issues ``issue_50-2`` / ``issue_50-2_brewster``,
    # which none of the base volume/issue nav selectors match.
    EXTRA_VOLUME_ISSUE_SELECTORS = (
        'a[href*="issue_"]',
        'a[href*="issue-"]',
    )

    def _find_volume_issue_links(self, soup, base_url):
        links = list(super()._find_volume_issue_links(soup, base_url))
        from urllib.parse import urljoin

        for selector in self.EXTRA_VOLUME_ISSUE_SELECTORS:
            try:
                for el in soup.select(selector):
                    href = el.get("href")
                    if not href:
                        continue
                    full = urljoin(base_url, href)
                    if self._is_valid_volume_issue_url(full):
                        links.append(full)
            except Exception:
                continue
        return list(dict.fromkeys(links))
