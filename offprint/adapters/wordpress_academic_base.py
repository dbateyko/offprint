from __future__ import annotations

import base64
import json
import os
import random
import re
import time
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS, GenericAdapter


class WordPressAcademicBaseAdapter(Adapter):
    """
    Base adapter for WordPress-based academic law review sites.

    Handles common WordPress patterns found in legal journals:
    - Category-based volume/issue organization
    - Schema.org metadata markup
    - Common WordPress themes and plugins
    - Consistent article/PDF link patterns

    This adapter is shared by many journals. Any broad filter or selector change
    here should be treated as a cross-site behavior change, not a one-host fix.
    Prefer host-guarded logic or sitemap-level tuning when possible, and add
    regression coverage for both the triggering host and nearby WordPress lanes.

    Based on analysis of 18+ WordPress law review sites.
    """

    # Common selectors for WordPress academic sites
    WORDPRESS_SELECTORS = {
        "article_links": [
            "h3.pp-content-grid-post-title a",  # PowerPack Grid
            "h2.entry-title a",  # Standard WordPress
            "a.articletitle",  # Custom academic themes
            '.fwpl-item a[href*="202"]',  # FacetWP plugin
            ".open .open a",  # Virginia Law Review print feed/article cards
            "div.wp-block-latest-posts__post-excerpt a",  # WP latest posts blocks
            ".entry-content > ul a",  # Issue lists rendered as ul/li
            "#aab_accordion_31a5615f div[role='region'] a",  # Accordion issue archives
            "article h2 a",  # Semantic HTML5
            ".post-title a",  # Generic post titles
            "h1.entry-title a",  # Single post titles
            ".article-title a",  # Custom article titles
        ],
        "pdf_links": [
            'a[href$=".pdf"]',  # Direct PDF links
            "a.download",  # Download buttons
            'a[href*="/pdf/"]',  # PDF directory
            'a[href*="scholarship."]',  # External repositories
            'a:contains("PDF")',  # Text-based PDF links
            'a:contains("View PDF")',  # View PDF buttons
            'a:contains("Download")',  # Download links
            'a[href*="viewcontent.cgi"]',  # Digital Commons CGI
        ],
        "volume_issue_nav": [
            'a[href*="volume-"][href*="issue-"]',  # Category URLs
            'a[href*="volume-"]',  # Volume pages without explicit issue segment
            ".menu-item-type-taxonomy a",  # WordPress taxonomy
            "ul.sub-menu a",  # Dropdown submenus
            ".volume-issue-link",  # Custom volume links
            'a[href*="/volume/"]',  # Volume directory
            'a[href*="/issue/"]',  # Issue directory
            'a[href*="current-issue"]',  # Current issue landing pages
            'a[href*="print-edition"]',  # Print archive issue pages
            'a[href*="/archive"]',  # Archive pages
            'a[href*="/archive/"]',  # Archive pages (with trailing slash)
        ],
        "authors": [
            '[itemprop="author"]',  # Schema.org
            '.fwpl-item:contains("By ")',  # FacetWP pattern
            ".post-meta .author",  # Post metadata
            ".entry-meta .author",  # Entry metadata
            ".article-author",  # Custom author class
            ".byline",  # Traditional byline
        ],
        "publication_dates": [
            '[itemprop="datePublished"]',  # Schema.org
            "time.published",  # Semantic time
            "time.updated",  # Updated time
            "time.entry-date",  # WP entry date
            ".post-date",  # Post date
            ".entry-date",  # Entry date
            ".publication-date",  # Custom publication date
            'meta[property="article:published_time"]',  # OpenGraph article
            'meta[name="article:published_time"]',
            'meta[property="og:updated_time"]',
            "time",  # Any time tag as last resort
        ],
        "abstracts": [
            '[itemprop="description"]',  # Schema.org
            ".entry-content p:first-of-type",  # First paragraph
            ".post-content p:first-of-type",  # Post content
            ".article-abstract",  # Custom abstract
            ".excerpt",  # Post excerpt
        ],
    }

    @staticmethod
    def _env_flag(name: str, default: bool) -> bool:
        value = str(os.getenv(name, "")).strip().lower()
        if not value:
            return default
        if value in {"1", "true", "yes", "on"}:
            return True
        if value in {"0", "false", "no", "off"}:
            return False
        return default

    @staticmethod
    def _env_int(name: str, default: int, minimum: int = 0) -> int:
        value = str(os.getenv(name, "")).strip()
        if not value:
            return max(int(default), minimum)
        try:
            return max(int(value), minimum)
        except Exception:
            return max(int(default), minimum)

    def __init__(
        self,
        base_url: str = "",
        journal_name: str = "",
        enable_playwright_fallback: Optional[bool] = None,
        playwright_fallback_timeout: int = 12,
        **kwargs,
    ):
        super().__init__(**kwargs)
        # If base_url not provided, it will be set by the registry from the URL
        self.base_url = base_url.rstrip("/") if base_url else ""
        self.journal_name = journal_name
        self.domain = urlparse(base_url).netloc if base_url else ""
        self.enable_playwright_fallback = (
            self._env_flag("LRS_WORDPRESS_PLAYWRIGHT_FALLBACK", False)
            if enable_playwright_fallback is None
            else bool(enable_playwright_fallback)
        )
        self.playwright_fallback_timeout = max(3, int(playwright_fallback_timeout))
        # Path prefix from the seed URL, used to scope discovery on multi-tenant
        # WordPress installs (e.g. bu.edu/ilj/ should not crawl all of bu.edu).
        self._seed_path_prefix: str = ""
        self.fast_mode = self._env_flag("LRS_WORDPRESS_FAST_MODE", False)
        self.fast_timeout_seconds = self._env_int(
            "LRS_WORDPRESS_FAST_TIMEOUT_SECONDS",
            8 if self.fast_mode else 15,
            minimum=3,
        )
        self.fast_max_attempts = self._env_int(
            "LRS_WORDPRESS_FAST_MAX_ATTEMPTS",
            2 if self.fast_mode else 3,
            minimum=1,
        )
        self.fast_seed_budget_seconds = self._env_int(
            "LRS_WORDPRESS_FAST_SEED_BUDGET_SECONDS",
            150 if self.fast_mode else 0,
            minimum=0,
        )
        self.fast_rest_max_pages = self._env_int(
            "LRS_WORDPRESS_FAST_REST_MAX_PAGES",
            60 if self.fast_mode else 500,
            minimum=1,
        )
        self.fast_sitemap_max_urls = self._env_int(
            "LRS_WORDPRESS_FAST_SITEMAP_MAX_URLS",
            400 if self.fast_mode else 5000,
            minimum=1,
        )
        self.fast_html_max_pages = self._env_int(
            "LRS_WORDPRESS_FAST_HTML_MAX_PAGES",
            120 if self.fast_mode else 500,
            minimum=1,
        )
        self._discover_started_at = 0.0

    @classmethod
    def from_url(cls, url: str, **kwargs):
        """Factory method to create adapter from URL."""
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        return cls(base_url=base_url, **kwargs)

    def discover_pdfs(
        self, start_url: str, max_depth: int = 0
    ) -> Generator[DiscoveryResult, None, None]:
        """Discover PDFs from WordPress academic site."""
        print(f"🔍 Discovering PDFs from WordPress academic site: {start_url}")
        self._discover_started_at = time.time()

        # Set base_url and domain if not already set
        if not self.base_url:
            parsed = urlparse(start_url)
            # WordPress REST/sitemap endpoints are rooted at origin, even when seed is a subpath.
            self.base_url = f"{parsed.scheme}://{parsed.netloc}"
            self.domain = parsed.netloc

        # Capture the seed path prefix for scoping discovery on multi-tenant sites.
        # For multi-tenant origin hosts (like georgetown.edu or bu.edu), we take
        # the first path segment to avoid over-scoping to the specific seed subpath.
        # e.g. "https://www.law.georgetown.edu/american-criminal-law-review/in-print/"
        # should scope to "/american-criminal-law-review/"
        # UPDATE: For yaleconnect.yale.edu and others, we should take the full path prefix
        # if it's longer than one segment, but generally the first segment is the journal slug.
        parsed_start = urlparse(start_url)
        path_parts = [p for p in parsed_start.path.split("/") if p]
        if path_parts:
            # If the last part looks like an archive token (e.g. /archive), take everything before it.
            if path_parts[-1] in {"archive", "archives", "issues", "past-issues", "print-edition"}:
                prefix_parts = path_parts[:-1]
            else:
                prefix_parts = path_parts
            
            if prefix_parts:
                self._seed_path_prefix = "/" + "/".join(prefix_parts).strip("/") + "/"
            else:
                self._seed_path_prefix = ""
        else:
            self._seed_path_prefix = ""

        if self._seed_path_prefix == "/":
            self._seed_path_prefix = ""

        # Use _request_with_retry for the initial probe to handle potential 403s/WAF
        probe_response = self._request_with_retry(start_url, timeout=15)
        
        if probe_response is not None and probe_response.status_code < 400:
            # If we were redirected, update our base_url and prefix scoping
            final_url = probe_response.url
            if final_url != start_url:
                parsed_final = urlparse(final_url)
                self.base_url = f"{parsed_final.scheme}://{parsed_final.netloc}"
                
                # Re-calculate prefix from the final URL
                path_parts = [p for p in parsed_final.path.split("/") if p]
                if path_parts:
                    if path_parts[-1] in {"archive", "archives", "issues", "past-issues", "print-edition", "journal"}:
                        prefix_parts = path_parts[:-1]
                    else:
                        prefix_parts = path_parts
                    
                    if prefix_parts:
                        self._seed_path_prefix = "/" + "/".join(prefix_parts).strip("/") + "/"
                    else:
                        self._seed_path_prefix = ""
                else:
                    self._seed_path_prefix = ""
                
                if self._seed_path_prefix == "/":
                    self._seed_path_prefix = ""

        # For archive seeds, prefer HTML traversal first to stay on archive/article
        # paths and avoid irrelevant site-wide REST/API pages.
        html_depth = max_depth
        if max_depth <= 0 and self._seed_likely_archive(start_url):
            html_depth = 1

        lane_builders = self._build_discovery_lanes(start_url=start_url, html_depth=html_depth)
        yielded = False
        for result in self._run_discovery_lanes(lane_builders):
            yielded = True
            yield result

        if not yielded:
            # Some WordPress installs block API/sitemap fetches from default requests.
            # Fall back to the generic crawler for one more bounded pass.
            generic_depth = max(1, html_depth)
            generic = GenericAdapter(session=self.session)
            # Use same verify=False logic if needed
            for result in generic.discover_pdfs(start_url, max_depth=generic_depth):
                yield result

    def _build_discovery_lanes(
        self, *, start_url: str, html_depth: int
    ) -> list[Callable[[], Iterable[DiscoveryResult]]]:
        prefer_html_first = self._seed_likely_archive(start_url)
        if prefer_html_first:
            return [
                lambda: self._discover_via_html_parsing(start_url, html_depth),
                lambda: self._discover_via_xml_sitemaps(start_url),
                lambda: self._discover_via_rest_api(start_url),
            ]
        return [
            lambda: self._discover_via_xml_sitemaps(start_url),
            lambda: self._discover_via_rest_api(start_url),
            lambda: self._discover_via_html_parsing(start_url, html_depth),
        ]

    def _run_discovery_lanes(
        self, lane_builders: Iterable[Callable[[], Iterable[DiscoveryResult]]]
    ) -> Generator[DiscoveryResult, None, None]:
        seen_pdf_urls: set[str] = set()
        found_any = False
        for builder in lane_builders:
            if self._seed_budget_exhausted():
                print("⚠️  WordPress fast mode: seed discovery budget exhausted")
                break
            for result in builder():
                if self._seed_budget_exhausted():
                    print("⚠️  WordPress fast mode: seed discovery budget exhausted")
                    return
                if result.pdf_url in seen_pdf_urls:
                    continue
                seen_pdf_urls.add(result.pdf_url)
                found_any = True
                yield result
            if found_any:
                break

    def _seed_budget_exhausted(self) -> bool:
        if not self.fast_mode:
            return False
        if self.fast_seed_budget_seconds <= 0:
            return False
        if self._discover_started_at <= 0:
            return False
        return (time.time() - self._discover_started_at) >= float(self.fast_seed_budget_seconds)

    def _effective_request_timeout(self, timeout: int) -> int:
        base_timeout = max(int(timeout or 1), 1)
        if not self.fast_mode:
            return base_timeout
        return max(3, min(base_timeout, self.fast_timeout_seconds))

    def _url_matches_seed_scope(self, url: str) -> bool:
        """Return True if *url* falls under the seed path prefix.

        When the seed is the site root (no meaningful path prefix), every URL
        on the same domain is in scope.  When the seed has a subpath (e.g.
        ``/ilj/``), only URLs whose path starts with that prefix are accepted.
        This prevents multi-tenant WordPress installs from returning unrelated
        site-wide content.
        """
        if not self._seed_path_prefix:
            return True
        try:
            path = urlparse(url).path
            # Some journal archive hubs (e.g. /previous-issues/) link to sibling
            # issue pages (/vol-85-no-1/) outside the literal seed prefix.
            # Permit these known issue/archive siblings on same-origin seeds.
            allow_siblings = (
                self._seed_path_prefix in {
                    "/previous-issues/",
                    "/archive/",
                    "/archives/",
                    "/archives-2/",
                    "/issues/",
                    "/print/",
                    "/print-edition/",
                    "/past-issues/",
                    "/current-issue/",
                    "/by-volume/",
                    "/online/",
                    "/online-archive/",
                    "/previous-edition/",
                    "/issue-archive/",
                }
                or "/category/" in self._seed_path_prefix
            )
            
            if allow_siblings:
                archive_siblings = (
                    "/vol-",
                    "/volume-",
                    "/issue-",
                    "/issues/",
                    "/archive/",
                    "/archives/",
                    "/archives-2/",
                    "/print/",
                    "/print-edition/",
                    "/previous-issues/",
                    "/article/",
                    "/articles/",
                    "/category/",
                    "/publication/",
                    "/publications/",
                    "/commentaries/",
                )
                if any(token in path for token in archive_siblings):
                    return True
                # Some WordPress journal archives point to dated article permalinks
                # such as /2026/02/25/article-title/ or /2026/02/article-title/.
                if re.match(r"^/\d{4}/\d{2}/", path):
                    return True
            return path.startswith(self._seed_path_prefix)
        except Exception:
            return False

    def _discover_via_rest_api(self, start_url: str) -> Generator[DiscoveryResult, None, None]:
        """Discover content via WordPress REST API with deterministic pagination."""
        endpoints = [
            f"{self.base_url}/wp-json/wp/v2/posts",
            f"{self.base_url}/wp-json/wp/v2/pages",
            f"{self.base_url}/wp-json/wp/v2/articles",  # Some custom themes
            f"{self.base_url}/wp-json/wp/v2/publication",
        ]
        per_page = 100

        for base_endpoint in endpoints:
            page = 1
            total_pages = None
            safety_max_pages = self.fast_rest_max_pages if self.fast_mode else 500
            endpoint_name = base_endpoint.rsplit("/", 1)[-1]
            last_page_signature: tuple[str, ...] = tuple()
            repeated_signatures = 0
            consecutive_empty_pages = 0  # pages with zero in-scope posts

            while page <= safety_max_pages:
                if self._seed_budget_exhausted():
                    break
                api_url = f"{base_endpoint}?per_page={per_page}&page={page}"
                response = self._request_with_retry(
                    api_url,
                    timeout=self._effective_request_timeout(15),
                    headers={"Accept": "application/json"},
                    max_attempts=4,
                )
                if response is None:
                    print(f"⚠️  WordPress REST API not available ({endpoint_name})")
                    break
                if response.status_code != 200:
                    break

                try:
                    posts = response.json()
                except Exception:
                    break

                if not isinstance(posts, list) or not posts:
                    break

                page_signature = tuple(
                    str((post or {}).get("id") or (post or {}).get("link") or "") for post in posts
                )
                if page_signature and page_signature == last_page_signature:
                    repeated_signatures += 1
                else:
                    repeated_signatures = 0
                last_page_signature = page_signature
                if repeated_signatures >= 2:
                    print(
                        f"⚠️  Stopping REST pagination for {endpoint_name}: "
                        "repeated page signature detected"
                    )
                    break

                if page == 1:
                    print(
                        f"📡 Found {len(posts)} records via WordPress REST API "
                        f"{endpoint_name} (Page 1)"
                    )
                else:
                    print(
                        f"📄 Processing API {endpoint_name} page {page} "
                        f"with {len(posts)} records"
                    )

                page_in_scope = 0
                for post in posts:
                    post_url = post.get("link", "")
                    if not self._url_matches_seed_scope(post_url):
                        continue
                    page_in_scope += 1
                    yield from self._process_wp_api_post(post)

                # On multi-tenant sites most pages will have zero in-scope
                # posts.  Stop early to avoid crawling the entire WP install.
                if self._seed_path_prefix:
                    if page_in_scope == 0:
                        consecutive_empty_pages += 1
                    else:
                        consecutive_empty_pages = 0
                    if consecutive_empty_pages >= 3:
                        print(
                            f"⚠️  Stopping REST pagination for {endpoint_name}: "
                            f"3 consecutive pages with no in-scope posts "
                            f"(prefix {self._seed_path_prefix})"
                        )
                        break

                if total_pages is None:
                    try:
                        header = response.headers.get("X-WP-TotalPages")
                        if header:
                            total_pages = int(header)
                    except Exception:
                        total_pages = None

                if total_pages is not None and page >= total_pages:
                    break

                page += 1

    def _discover_via_xml_sitemaps(self, start_url: str) -> Generator[DiscoveryResult, None, None]:
        """Fallback: enumerate pages via wp-sitemap.xml / sitemap.xml and extract PDFs."""
        visited_pages = set()
        for page_url in self._iter_sitemap_page_urls(start_url):
            if self._seed_budget_exhausted():
                break
            if page_url in visited_pages:
                continue
            if not self._url_matches_seed_scope(page_url):
                continue
            visited_pages.add(page_url)

            try:
                response = self._request_with_retry(
                    page_url,
                    timeout=self._effective_request_timeout(20),
                )
                if response is None or response.status_code >= 400:
                    continue
            except Exception:
                continue

            soup = BeautifulSoup(response.content, "lxml")
            title = ""
            try:
                if soup.title:
                    title = soup.title.get_text(strip=True)
            except Exception:
                title = ""
            yield from self._extract_pdfs_from_article(soup, page_url, title)

    def _iter_sitemap_page_urls(self, start_url: str) -> Generator[str, None, None]:
        """Yield page URLs discovered via wp-sitemap.xml or sitemap.xml (supports sitemapindex)."""
        parsed = urlparse(start_url)
        base_url = self.base_url or f"{parsed.scheme}://{parsed.netloc}"
        candidates = [
            base_url.rstrip("/") + "/wp-sitemap.xml",
            base_url.rstrip("/") + "/sitemap.xml",
            base_url.rstrip("/") + "/sitemap_index.xml",
        ]

        seen_sitemaps = set()
        queue = list(candidates)
        yielded_urls = 0

        while queue:
            if self._seed_budget_exhausted():
                break
            sm_url = queue.pop(0)
            if sm_url in seen_sitemaps:
                continue
            seen_sitemaps.add(sm_url)

            try:
                xml_headers = dict(DEFAULT_HEADERS)
                xml_headers["Accept"] = "application/xml"
                resp = self._request_with_retry(
                    sm_url,
                    timeout=self._effective_request_timeout(20),
                    headers=xml_headers,
                )
                if resp is None or resp.status_code >= 400 or not resp.content:
                    continue
                content = resp.content
                if sm_url.lower().endswith(".gz"):
                    import gzip

                    content = gzip.decompress(content)
            except Exception:
                continue

            soup = BeautifulSoup(content, "xml")
            if soup.find("sitemapindex"):
                for loc in soup.find_all("loc"):
                    u = (loc.get_text() or "").strip()
                    if u:
                        queue.append(u)
                continue

            if soup.find("urlset"):
                for loc in soup.find_all("loc"):
                    u = (loc.get_text() or "").strip()
                    if u:
                        yielded_urls += 1
                        yield u
                        if self.fast_mode and yielded_urls >= self.fast_sitemap_max_urls:
                            return
                continue

    def _discover_via_html_parsing(
        self, start_url: str, max_depth: int
    ) -> Generator[DiscoveryResult, None, None]:
        """Discover content via HTML parsing when REST API unavailable."""
        visited = set()
        to_visit = [(start_url, 0)]
        processed_pages = 0
        max_pages = self.fast_html_max_pages if self.fast_mode else 500

        while to_visit:
            if processed_pages >= max_pages or self._seed_budget_exhausted():
                break
            url, depth = to_visit.pop(0)
            if url in visited or depth > max_depth:
                continue

            visited.add(url)
            processed_pages += 1

            try:
                response = self._request_with_retry(
                    url,
                    timeout=self._effective_request_timeout(15),
                )
                if response is None:
                    continue
                response.raise_for_status()
                soup = BeautifulSoup(response.content, "lxml")

                # Some issue/archive pages link directly to PDFs without separate article pages.
                page_title = soup.title.get_text(strip=True) if soup.title else url
                yield from self._extract_pdfs_from_article(soup, url, page_title)

                # Extract articles from current page
                yield from self._extract_articles_from_page(soup, url)

                # Find volume/issue navigation for deeper discovery
                if depth < max_depth:
                    for next_url in self._find_volume_issue_links(soup, url):
                        # Ensure next_url is in scope for multi-tenant sites
                        if self._url_matches_seed_scope(next_url):
                            to_visit.append((next_url, depth + 1))

            except Exception as e:
                print(f"⚠️  Error processing {url}: {e}")

    def _process_wp_api_post(self, post: Dict[str, Any]) -> Generator[DiscoveryResult, None, None]:
        """Process a WordPress API post object."""
        post_url = post.get("link", "")
        post_title = post.get("title", {}).get("rendered", "")

        if not post_url or not post_title:
            return

        # Extract authors from content.rendered before fetching the HTML page.
        # Many WP law journals embed the real author at the top of content.rendered
        # as <p><strong>Name</strong></p> (CUNY) or "By Name" text (JLSP/ASU).
        # The REST API user account (_embedded.author) is never the article author.
        api_authors = self._extract_authors_from_content_rendered(
            post.get("content", {}).get("rendered", "")
        )

        try:
            # Get full post content
            response = self._request_with_retry(post_url, timeout=10, max_attempts=3)
            if response is None:
                return
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "lxml")

            # Extract PDFs from post content, passing API-derived authors as hints
            results = list(self._extract_pdfs_from_article(
                soup, post_url, post_title, hint_authors=api_authors
            ))
            
            if not results:
                # Blog-style posts with /YYYY/MM/DD/ slugs usually don't have PDFs.
                # Skip the slow Playwright fallback if we've already done a static fetch.
                is_blog_slug = bool(re.search(r"/\d{4}/\d{2}/", post_url))
                if is_blog_slug:
                    # Just return if it looks like a blog post and static extraction failed.
                    return

                # Fallback to Playwright if no PDFs found in static HTML
                if self.enable_playwright_fallback:
                    try:
                        from ..playwright_session import PlaywrightSession

                        with PlaywrightSession(headless=True) as pw:
                            pw_resp = pw.get(post_url, timeout=self.playwright_fallback_timeout)
                            if pw_resp and pw_resp.status_code < 400:
                                pw_soup = BeautifulSoup(pw_resp.text, "lxml")
                                results = list(
                                    self._extract_pdfs_from_article(
                                        pw_soup,
                                        post_url,
                                        post_title,
                                        hint_authors=api_authors,
                                    )
                                )
                                if results:
                                    print(
                                        "  [wp] Playwright fallback SUCCESS for "
                                        f"{post_url} (Found {len(results)} PDFs)"
                                    )
                                else:
                                    print(f"  [wp] Playwright fallback found 0 PDFs for {post_url}")
                    except Exception as e:
                        print(f"  [wp] Playwright fallback FAILED for {post_url}: {e}")

            for result in results:
                yield result

        except Exception as e:
            print(f"⚠️  Error processing API post {post_url}: {e}")

    def _request_with_retry(
        self,
        url: str,
        timeout: int = 15,
        headers: Optional[Dict[str, str]] = None,
        max_attempts: int = 3,
    ):
        from .generic import DISCOVERY_UA_PROFILES
        last_error = None
        self._current_retry_session = self.session
        timeout = self._effective_request_timeout(timeout)
        effective_max_attempts = max(1, int(max_attempts or 1))
        if self.fast_mode:
            effective_max_attempts = min(effective_max_attempts, self.fast_max_attempts)

        for attempt in range(1, effective_max_attempts + 1):
            try:
                # Use a fresh header dict to avoid leaking session defaults that might be blocked
                current_headers = {}
                
                # Rotate UA
                ua_index = (attempt - 1) % len(DISCOVERY_UA_PROFILES)
                current_headers["User-Agent"] = DISCOVERY_UA_PROFILES[ua_index]
                
                if headers:
                    current_headers.update(headers)
                
                # Add Referer if we have a base_url
                if self.base_url and "Referer" not in current_headers:
                    current_headers["Referer"] = self.base_url + "/"

                response = self._current_retry_session.get(url, timeout=timeout, headers=current_headers)
                return response
            except Exception as exc:
                last_error = exc
                # Handle SSLError with verify=False fallback
                from requests.exceptions import SSLError
                if isinstance(exc, SSLError):
                    try:
                        print(f"⚠️  SSL verification failed for {url}, retrying with verify=False")
                        response = self.session.get(
                            url, timeout=timeout, headers=current_headers, verify=False
                        )
                        return response
                    except Exception as e2:
                        last_error = e2

                if attempt < effective_max_attempts:
                    if self.fast_mode:
                        backoff = min(1.2, 0.25 * attempt) + random.uniform(0.0, 0.1)
                    else:
                        backoff = min(8.0, 0.6 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.2)
                    time.sleep(backoff)
                    continue
                return None

            if (
                (response.status_code == 403 or response.status_code == 401)
                and attempt < effective_max_attempts
            ):
                # If blocked, try Playwright fallback
                if self.enable_playwright_fallback:
                    try:
                        from ..playwright_session import PlaywrightSession

                        with PlaywrightSession(headless=False) as pw:
                            pw_resp = pw.get(url, timeout=self.playwright_fallback_timeout)
                            if pw_resp and pw_resp.status_code < 400:
                                return pw_resp
                    except Exception:
                        pass
                
                # Even if Playwright not enabled or fails, we want to continue loop to retry with different UA
                # On 403, also try a fresh session to ensure no persistent cookies/headers are blocking
                retry_session = self.session
                if response.status_code == 403:
                    import requests
                    retry_session = requests.Session()
                    retry_session.cookies.clear()
                
                if self.fast_mode:
                    backoff = min(1.5, 0.3 * attempt) + random.uniform(0.0, 0.1)
                else:
                    backoff = min(12.0, 0.8 * (2 ** (attempt - 1))) + random.uniform(0.1, 0.5)
                time.sleep(backoff)
                
                # Update next attempt to use fresh session if created
                # Note: this is a local hack for this method
                self._current_retry_session = retry_session
                continue

            if response.status_code == 429 and attempt < effective_max_attempts:
                if self.fast_mode:
                    backoff = min(1.5, 0.3 * attempt) + random.uniform(0.0, 0.1)
                else:
                    backoff = min(12.0, 0.8 * (2 ** (attempt - 1))) + random.uniform(0.1, 0.5)
                time.sleep(backoff)
                continue
            return response

        if last_error is not None:
            print(f"⚠️  Request retry exhausted for {url}: {last_error}")
        return None

    def _extract_articles_from_page(
        self, soup: BeautifulSoup, page_url: str
    ) -> Generator[DiscoveryResult, None, None]:
        """Extract articles from a page using WordPress selectors."""
        article_links = []

        # Try each article link selector
        for selector in self.WORDPRESS_SELECTORS["article_links"]:
            try:
                links = soup.select(selector)
                if links:
                    article_links.extend(links)
                    print(f"✅ Found {len(links)} article links with selector: {selector}")
                    break
            except Exception as e:
                print(f"⚠️  Selector failed {selector}: {e}")

        # Process each article link
        for link in article_links:
            href = link.get("href")
            if not href:
                continue

            article_url = urljoin(page_url, href)
            article_title = link.get_text(strip=True)

            if self._is_valid_article_url(article_url):
                yield from self._extract_pdfs_from_article_url(article_url, article_title)

    def _extract_pdfs_from_article_url(
        self, article_url: str, article_title: str
    ) -> Generator[DiscoveryResult, None, None]:
        """Extract PDFs from an individual article page."""
        try:
            response = self._request_with_retry(article_url, timeout=15)
            if response is None or response.status_code >= 400:
                return
            soup = BeautifulSoup(response.content, "lxml")

            yield from self._extract_pdfs_from_article(soup, article_url, article_title)

        except Exception as e:
            print(f"⚠️  Error processing article {article_url}: {e}")

    def _extract_pdfs_from_article(
        self,
        soup: BeautifulSoup,
        article_url: str,
        article_title: str,
        hint_authors: Optional[List[str]] = None,
    ) -> Generator[DiscoveryResult, None, None]:
        """Extract PDFs and metadata from an article page."""
        pdf_links: List[str] = []

        # Try each PDF link selector
        for selector in self.WORDPRESS_SELECTORS["pdf_links"]:
            try:
                if ":contains(" in selector:
                    # Handle pseudo-selectors manually
                    text_search = selector.split(':contains("')[1].split('")')[0]
                    links = soup.find_all("a", string=re.compile(text_search, re.I))
                else:
                    links = soup.select(selector)

                if links:
                    pdf_links.extend(links)

            except Exception as e:
                print(f"⚠️  PDF selector failed {selector}: {e}", flush=True)

        # Embedded PDF support (EmbedPress, generic object/embed)
        # 1. EmbedPress uses <iframe data-emsrc="...">
        for iframe in soup.find_all("iframe", attrs={"data-emsrc": True}):
            pdf_links.append(iframe) # We'll handle data-emsrc in the loop below

        # 2. Generic <object data="...">
        for obj in soup.find_all("object", attrs={"data": True}):
            if ".pdf" in (obj["data"] or "").lower():
                pdf_links.append(obj)

        # 3. Generic <embed src="...">
        for emb in soup.find_all("embed", attrs={"src": True}):
            if ".pdf" in (emb["src"] or "").lower():
                pdf_links.append(emb)

        # Fallback: find any <a> tags containing "PDF", "Download", "View Content"
        if not pdf_links:
            fallback_count = 0
            for a in soup.find_all("a", href=True):
                text = a.get_text(" ", strip=True).lower()
                if any(x in text for x in ["pdf", "download", "view content", "full text"]):
                    pdf_links.append(a)
                    fallback_count += 1
            if fallback_count:
                pass

        # Deduplicate while preserving order
        seen_urls = set()
        pdf_anchor_signals: Dict[str, bool] = {}
        ordered_urls: List[str] = []
        for link in pdf_links:
            # Check multiple attributes for PDF URL
            href = None
            if hasattr(link, "get"):
                href = link.get("href") or link.get("data-emsrc") or link.get("data") or link.get("src")
            
            if not href:
                continue
            pdf_url = urljoin(article_url, href)
            anchor_text = ""
            if hasattr(link, "get_text"):
                try:
                    anchor_text = link.get_text(" ", strip=True)
                except Exception:
                    anchor_text = ""
            has_pdf_signal = self._anchor_text_has_pdf_signal(anchor_text=anchor_text)
            if pdf_url not in seen_urls:
                seen_urls.add(pdf_url)
                ordered_urls.append(pdf_url)
                pdf_anchor_signals[pdf_url] = has_pdf_signal
            else:
                pdf_anchor_signals[pdf_url] = pdf_anchor_signals.get(pdf_url, False) or has_pdf_signal

        # WordPress PDF Embedder plugin often stores the real PDF URL in a
        # base64 payload under iframe query param `pdfemb-data`.
        for embedded_url in self._extract_embedded_pdf_urls(soup, article_url):
            if embedded_url not in seen_urls:
                seen_urls.add(embedded_url)
                ordered_urls.append(embedded_url)
                # Embedded/PDFEmbed payloads are strong PDF intent signals.
                pdf_anchor_signals[embedded_url] = True

        inline_metadata_by_pdf = self._extract_inline_pdf_metadata(soup, article_url)

        if not ordered_urls:
            pass

        # Process each PDF link, preferring same-origin
        for pdf_url in ordered_urls:
            if not self._is_valid_pdf_url(pdf_url):
                continue
            if not self._is_likely_scholarly_pdf(
                pdf_url, article_url=article_url, article_title=article_title
            ):
                continue
            if not self._is_preferred_pdf_url(pdf_url):
                # Skip likely external citations or unrelated documents
                continue
            # For off-domain trusted repositories, require explicit PDF-like anchor
            # text/href intent. This blocks citation links to unrelated journals.
            if self._is_trusted_repository_external_url(pdf_url):
                if not pdf_anchor_signals.get(pdf_url, False):
                    continue

            # Refresh title from article page if available
            resolved_title = self._extract_title(soup, article_title)
            metadata = self._extract_metadata_from_article(
                soup, article_url, resolved_title, hint_authors=hint_authors
            )
            inline_meta = inline_metadata_by_pdf.get(pdf_url, {})
            if inline_meta:
                metadata.update(inline_meta)
            if not metadata.get("volume") or not metadata.get("date"):
                inferred_meta = self._infer_metadata_from_context(
                    pdf_url=pdf_url,
                    article_url=article_url,
                    article_title=resolved_title,
                )
                for key, value in inferred_meta.items():
                    if not metadata.get(key):
                        metadata[key] = value

            yield DiscoveryResult(page_url=article_url, pdf_url=pdf_url, metadata=metadata)

    @staticmethod
    def _anchor_text_has_pdf_signal(anchor_text: str) -> bool:
        text = (anchor_text or "").strip().lower()
        return any(token in text for token in ("pdf", "download", "full text", "view content"))

    def _is_trusted_repository_external_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            host = parsed.netloc.lower()
        except Exception:
            return False
        domain = (self.domain or "").lower()
        if not host or not domain:
            return False
        is_same_origin = host == domain or host.endswith("." + domain)
        return (not is_same_origin) and self._is_trusted_repository_host(host)

    def _extract_embedded_pdf_urls(self, soup: BeautifulSoup, article_url: str) -> List[str]:
        urls: List[str] = []

        # Common direct embed paths
        for selector, attr in (
            ('iframe[src]', "src"),
            ('iframe[data-src]', "data-src"),
            ('embed[src]', "src"),
            ('object[data]', "data"),
        ):
            try:
                for el in soup.select(selector):
                    raw = (el.get(attr) or "").strip()
                    if not raw:
                        continue
                    full = urljoin(article_url, raw)
                    parsed = urlparse(full)
                    path = (parsed.path or "").lower()
                    if path.endswith(".pdf"):
                        urls.append(full)
                        continue

                    if "pdfemb-data=" in full:
                        payload_values = parse_qs(parsed.query).get("pdfemb-data", [])
                        for payload in payload_values:
                            decoded_url = self._decode_pdfemb_payload_url(payload)
                            if decoded_url:
                                urls.append(decoded_url)
            except Exception:
                continue

        deduped: List[str] = []
        seen: set[str] = set()
        for u in urls:
            if not u or u in seen:
                continue
            seen.add(u)
            deduped.append(u)
        return deduped

    def _decode_pdfemb_payload_url(self, payload: str) -> Optional[str]:
        raw = unquote((payload or "").strip())
        if not raw:
            return None

        decoded_text = ""
        if raw.startswith("{"):
            decoded_text = raw
        else:
            padded = raw + "=" * (-len(raw) % 4)
            decoded_bytes: Optional[bytes] = None
            try:
                decoded_bytes = base64.urlsafe_b64decode(padded.encode("utf-8"))
            except Exception:
                try:
                    decoded_bytes = base64.b64decode(padded.encode("utf-8"))
                except Exception:
                    decoded_bytes = None
            if not decoded_bytes:
                return None
            decoded_text = decoded_bytes.decode("utf-8", "ignore")

        try:
            payload_obj = json.loads(decoded_text)
        except Exception:
            return None

        candidate = (
            payload_obj.get("pdfemb-serveurl")
            or payload_obj.get("pdfemb_serverurl")
            or payload_obj.get("serveurl")
            or payload_obj.get("pdf_url")
        )
        if not isinstance(candidate, str) or not candidate.strip():
            return None
        resolved = unquote(candidate.strip())
        return resolved if resolved.lower().endswith(".pdf") else None

    def _extract_inline_pdf_metadata(
        self, soup: BeautifulSoup, article_url: str
    ) -> Dict[str, Dict[str, Any]]:
        # GWLR issue pages expose article metadata inline in paragraph blocks:
        # <p><strong>Title</strong><br/>Author<br/><span>85 Geo. Wash. L. Rev. 1</span><br/>Abstract | PDF</p>
        if "gwlr.org" not in (self.domain or "").lower():
            return {}

        by_pdf: Dict[str, Dict[str, Any]] = {}
        for p in soup.find_all("p"):
            try:
                pdf_link = None
                for a in p.find_all("a", href=True):
                    href = (a.get("href") or "").strip()
                    text = a.get_text(" ", strip=True).lower()
                    if text == "pdf" or href.lower().endswith(".pdf") or "/wp-content/uploads/" in href.lower():
                        pdf_link = a
                        break
                if not pdf_link:
                    continue

                pdf_url = urljoin(article_url, (pdf_link.get("href") or "").strip())
                if not pdf_url:
                    continue

                strings = [s.strip() for s in p.stripped_strings if s and s.strip()]
                filtered = [s for s in strings if s not in {"Abstract", "PDF", "|"}]
                if not filtered:
                    continue

                title = filtered[0]
                author_line = filtered[1] if len(filtered) > 1 else ""
                citation_line = ""
                for token in filtered[2:]:
                    if "Geo. Wash. L. Rev." in token:
                        citation_line = token
                        break

                meta: Dict[str, Any] = {}
                if title:
                    meta["title"] = title
                authors = self._split_author_string(author_line) if author_line else []
                if authors:
                    meta["authors"] = authors

                if citation_line:
                    meta["citation"] = citation_line
                    vol_match = re.search(r"\b(\d{1,3})\s+Geo\.\s+Wash\.\s+L\.\s+Rev\.", citation_line)
                    page_match = re.search(r"Geo\.\s+Wash\.\s+L\.\s+Rev\.\s+(\d{1,4})\b", citation_line)
                    if vol_match:
                        meta["volume"] = vol_match.group(1)
                    if page_match:
                        meta["pages"] = page_match.group(1)

                if meta:
                    by_pdf[pdf_url] = meta
            except Exception:
                continue

        return by_pdf

    def _extract_metadata_from_article(
        self,
        soup: BeautifulSoup,
        article_url: str,
        article_title: str,
        hint_authors: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Extract comprehensive metadata from article page."""
        metadata = {
            "title": article_title,
            "page_url": article_url,
            "url": article_url,  # required field alias used by manifests
            "journal": self.journal_name,
            "domain": self.domain,
            "platform": "WordPress Academic",
            "extraction_method": "wordpress_academic_base",
        }

        # Extract authors with priority ordering:
        # 1. hint_authors from content.rendered (REST API path)
        # 2. Schema.org / semantic selectors on HTML page
        # 3. "By ..." or <strong> patterns in entry-content
        # 4. <em>/<i> after PDF links on volume/issue pages
        # 5. meta[name=author] as last resort (filtered for WP account slugs)
        authors: List[str] = list(hint_authors) if hint_authors else []

        if not authors:
            try:
                for a in soup.select(
                    '[itemprop="author"] [itemprop="name"], [itemprop="author"],'
                    " .entry-meta .author, .post-meta .author, .byline .author"
                ):
                    txt = a.get_text(strip=True)
                    if txt:
                        authors.append(txt)
            except Exception:
                pass

        if not authors:
            # Pattern: first <p><strong>Author Name</strong></p> in entry-content
            try:
                content_div = soup.select_one(
                    ".entry-content, .post-content, article .content,"
                    " .wp-block-post-content"
                )
                if content_div:
                    first_p = content_div.find("p")
                    if first_p:
                        strong = first_p.find("strong")
                        if (
                            strong
                            and strong.get_text(strip=True)
                            == first_p.get_text(strip=True)
                        ):
                            candidate = strong.get_text(strip=True)
                            names = self._split_author_string(candidate)
                            if names:
                                authors.extend(names)
            except Exception:
                pass

        if not authors:
            # Pattern: first paragraph starting with "By " in entry-content
            try:
                content_div = soup.select_one(
                    ".entry-content, .post-content, article .content,"
                    " .wp-block-post-content"
                )
                if content_div:
                    for p in content_div.find_all("p", limit=3):
                        text = p.get_text(strip=True)
                        if text.startswith(("By ", "by ")):
                            after_by = re.sub(r"^[Bb]y\s+", "", text)
                            segment = re.split(r"\.\s+[A-Z]", after_by)[0].rstrip(".")
                            if len(segment) < 150:
                                names = self._split_author_string(segment)
                                if names:
                                    authors.extend(names)
                                    break
            except Exception:
                pass

        if not authors:
            # Pattern: <em>/<i> tag immediately following a PDF link on volume/issue pages
            try:
                for pdf_link in soup.select('a[href$=".pdf"], a[href*="/pdf/"]'):
                    sibling = pdf_link.find_next_sibling()
                    if sibling and sibling.name in ("em", "i"):
                        candidate = sibling.get_text(strip=True)
                        names = self._split_author_string(candidate)
                        if names:
                            authors.extend(names)
                            break
                    parent = pdf_link.parent
                    if parent:
                        next_sib = parent.find_next_sibling()
                        if next_sib and next_sib.name in ("p", "em", "i"):
                            candidate = next_sib.get_text(strip=True)
                            names = self._split_author_string(candidate)
                            if names:
                                authors.extend(names)
                                break
            except Exception:
                pass

        if not authors:
            # Last resort: meta[name=author] (but filter WP account slugs)
            try:
                ma = soup.find("meta", attrs={"name": "author"})
                if ma and ma.get("content"):
                    candidate = ma["content"].strip()
                    names = self._split_author_string(candidate)
                    if names:
                        authors.extend(names)
            except Exception:
                pass

        # Cleanup: drop WP system account slugs and other non-names
        clean_authors: List[str] = []
        for a in authors:
            at = a.strip()
            if not at:
                continue
            if "http" in at.lower() or "@" in at:
                continue
            if len(at) > 120:
                continue
            low = at.lower()
            if "website designed" in low or "law review" in low:
                continue
            # Reject WP system account slugs: single-word, all lowercase/digits/hyphens
            if re.fullmatch(r"[a-z0-9_\-]+", at):
                continue
            at = re.sub(r"^(by\s+)", "", at, flags=re.I)
            at = at.rstrip("*").strip()
            clean_authors.append(at)
        if clean_authors:
            metadata["authors"] = sorted(set(clean_authors))

        # Extract publication date
        for selector in self.WORDPRESS_SELECTORS["publication_dates"]:
            try:
                elements = soup.select(selector)
                if elements:
                    el = elements[0]
                    # Support <meta> tags and <time> tags
                    date_text = el.get("datetime") or el.get("content") or el.get_text(strip=True)
                    if date_text:
                        metadata["publication_date"] = date_text
                        metadata["date"] = date_text  # align with required metadata field
                        break
            except Exception:
                pass

        # Extract abstract/description
        for selector in self.WORDPRESS_SELECTORS["abstracts"]:
            try:
                elements = soup.select(selector)
                if elements:
                    abstract = elements[0].get_text(strip=True)
                    if abstract and len(abstract) > 50:  # Ensure it's substantial
                        metadata["abstract"] = abstract[:500]  # Limit length
                        break
            except Exception:
                pass

        # Extract page range (best-effort)
        pages = self._extract_pages(soup)
        if pages:
            metadata["pages"] = pages

        # Site-specific enrichment used by several law-review themes where
        # byline/date/citation are rendered in feed-style blocks on article pages.
        try:
            if "virginialawreview.org" in (self.domain or "").lower():
                if not metadata.get("authors"):
                    for ael in soup.select(".article-feed-author"):
                        byline = ael.get_text(" ", strip=True)
                        if not byline:
                            continue
                        byline = re.sub(r"^[Bb]y\s+", "", byline).strip()
                        names = self._split_author_string(byline)
                        if names:
                            metadata["authors"] = names
                            break

                if not metadata.get("date"):
                    date_el = soup.select_one(".article-date")
                    if date_el:
                        dtxt = date_el.get_text(" ", strip=True)
                        if dtxt:
                            metadata["date"] = dtxt
                            metadata["publication_date"] = dtxt

                if not metadata.get("citation"):
                    for cel in soup.select(".article-feed-reference"):
                        ctxt = cel.get_text(" ", strip=True)
                        if not ctxt:
                            continue
                        if "Va. L. Rev." in ctxt or "Virginia Law Review" in ctxt:
                            metadata["citation"] = ctxt
                            m = re.search(
                                r"\b(\d{1,3})\s+Va\.\s+L\.\s+Rev\.\s+(\d{1,4}(?:-\d{1,4})?)\b",
                                ctxt,
                            )
                            if m:
                                metadata.setdefault("volume", m.group(1))
                                metadata.setdefault("pages", m.group(2))
                            break
        except Exception:
            pass

        # Extract volume/issue information from URL or content
        volume_issue = self._extract_volume_issue(article_url, soup)
        if volume_issue:
            metadata.update(volume_issue)

        # Construct a best-effort citation if possible
        if (
            not metadata.get("citation")
            and metadata.get("volume")
            and metadata.get("pages")
            and metadata.get("date")
        ):
            year_match = re.search(r"(19|20)\d{2}", metadata["date"])
            year = year_match.group(0) if year_match else ""
            journal = self.journal_name or self.domain or "Law Rev."
            metadata["citation"] = (
                f"{metadata['volume']} {journal} {metadata['pages']} ({year})".strip()
            )

        return metadata

    def _extract_title(self, soup: BeautifulSoup, fallback: str) -> str:
        try:
            t = soup.select_one("h1.entry-title")
            if t:
                return t.get_text(strip=True)
            t2 = soup.select_one("article h1")
            if t2:
                return t2.get_text(strip=True)
            og = soup.find("meta", attrs={"property": "og:title"})
            if og and og.get("content"):
                return og["content"].strip()
        except Exception:
            pass
        return fallback

    def _extract_pages(self, soup: BeautifulSoup) -> Optional[str]:
        # Common schema or textual hints for pagination
        try:
            # Try explicit start/end first
            start = soup.select_one('[itemprop="pageStart"],[itemprop="page-start"]')
            end = soup.select_one('[itemprop="pageEnd"],[itemprop="page-end"]')
            if start and end:
                s, e = start.get_text(strip=True), end.get_text(strip=True)
                if s and e:
                    return f"{s}-{e}"

            el = soup.select_one(
                '[itemprop="pagination"], .pagination, .article-pages, span.pagination'
            )
            if el:
                text = el.get_text(" ", strip=True)
                m = re.search(r"(?i)(pages?\s*)?(\d{1,4}\s*[–-]\s*\d{1,4})", text)
                if m:
                    return m.group(2).replace("–", "-").replace(" ", "")
        except Exception:
            pass
        # Fallback: scan paragraphs for a page range pattern
        try:
            p = soup.find(string=re.compile(r"(?i)pages?\s*\d{1,4}\s*[–-]\s*\d{1,4}"))
            if p:
                m = re.search(r"(\d{1,4}\s*[–-]\s*\d{1,4})", p)
                if m:
                    return m.group(1).replace("–", "-").replace(" ", "")
        except Exception:
            pass
        return None

    def _extract_volume_issue(self, url: str, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract volume and issue information."""
        info = {}

        # Try URL pattern matching
        volume_match = re.search(r"volume-?(\d+)", url, re.I)
        issue_match = re.search(r"issue-?(\d+)", url, re.I)

        if volume_match:
            info["volume"] = volume_match.group(1)
        if issue_match:
            info["issue"] = issue_match.group(1)

        # Try breadcrumbs or navigation
        try:
            breadcrumbs = soup.select(".breadcrumb a, .breadcrumbs a, nav a")
            for breadcrumb in breadcrumbs:
                text = breadcrumb.get_text().lower()
                if "volume" in text and "issue" in text:
                    # Extract volume and issue from breadcrumb text
                    vol_match = re.search(r"volume\s*(\d+)", text)
                    iss_match = re.search(r"issue\s*(\d+)", text)
                    if vol_match:
                        info["volume"] = vol_match.group(1)
                    if iss_match:
                        info["issue"] = iss_match.group(1)
        except Exception:
            pass

        return info

    @staticmethod
    def _infer_metadata_from_context(
        *, pdf_url: str, article_url: str, article_title: str
    ) -> Dict[str, str]:
        """Infer sparse volume/year metadata from the PDF URL, article URL, or title."""
        text = " ".join(part for part in (pdf_url or "", article_url or "", article_title or "") if part)
        inferred: Dict[str, str] = {}

        volume_match = re.search(
            r"\bvol(?:ume)?[.\s_-]*([0-9]{1,3})\b|[_/-]([0-9]{1,3})n[0-9]{1,2}[_/-]",
            text,
            re.I,
        )
        if volume_match:
            inferred["volume"] = volume_match.group(1) or volume_match.group(2)

        year_match = re.search(r"(19\d{2}|20\d{2})", text)
        if year_match:
            inferred["date"] = year_match.group(1)

        return inferred

    def _find_volume_issue_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Find volume/issue navigation links for deeper discovery."""
        links = []

        for selector in self.WORDPRESS_SELECTORS["volume_issue_nav"]:
            try:
                elements = soup.select(selector)
                for elem in elements:
                    href = elem.get("href")
                    if href:
                        full_url = urljoin(base_url, href)
                        if self._is_valid_volume_issue_url(full_url):
                            links.append(full_url)
            except Exception:
                pass

        return list(set(links))  # Remove duplicates

    def _is_valid_article_url(self, url: str) -> bool:
        """Check if URL is a valid article URL."""
        if not url:
            return False
        try:
            parsed = urlparse(url)
            host = parsed.netloc.lower()
        except Exception:
            return False
        same_domain = self.domain.lower() in host
        if not same_domain and not self._is_trusted_repository_host(host):
            return False
        # On multi-tenant sites, enforce seed path scope
        if same_domain and not self._url_matches_seed_scope(url):
            return False

        # Skip common non-article URLs
        skip_patterns = [
            "/wp-admin/",
            "/wp-content/",
            "/category/",
            "/tag/",
            "/author/",
            "/search/",
            "/feed/",
            ".css",
            ".js",
            "/comments/",
            "/contact/",
            "/about/",
        ]

        return not any(pattern in url.lower() for pattern in skip_patterns)

    def _is_valid_pdf_url(self, url: str) -> bool:
        """Check if URL is a valid PDF URL."""
        if not url:
            return False
        parsed = urlparse(url)
        path = parsed.path.lower()

        # Skip mastheads, editorial boards, etc.
        if any(x in path for x in ["masthead", "editorial-board", "board-of-editors"]):
            return False

        return (
            path.endswith(".pdf")
            or "viewcontent.cgi" in url
            or "/pdf/" in url
            or "download" in url.lower()
        )

    def _is_likely_scholarly_pdf(self, pdf_url: str, article_url: str, article_title: str) -> bool:
        """Reject obvious non-article assets that frequently appear on journal WordPress sites."""
        combined = " ".join(
            part
            for part in (
                urlparse(pdf_url).path.lower(),
                urlparse(article_url).path.lower(),
                (article_title or "").lower(),
            )
            if part
        )
        blocked_patterns = (
            r"(^|[/_-])cv([/_-]|\.pdf|$)",
            r"(^|[/_-])resume([/_-]|\.pdf|$)",
            r"(^|[/_-])board([/_-]|\.pdf|$)",
            r"(^|[/_-])staff([/_-]|\.pdf|$)",
            r"symposium[-_ ]program",
            r"conference[-_ ]program",
            r"(^|[/_-])program([/_-]|\.pdf|$)",
            r"(^|[/_-])blog([/_-]|\.pdf|$)",
            r"/blog/",
        )
        return not any(re.search(pattern, combined) for pattern in blocked_patterns)

    def _is_preferred_pdf_url(self, url: str) -> bool:
        """Favor same-origin or typical WordPress-hosted PDFs to avoid unrelated citations."""
        try:
            parsed = urlparse(url)
            host = parsed.netloc.lower()
            path = parsed.path.lower()
            # Same-origin only for generic WordPress adapter
            if host == self.domain.lower() or host.endswith("." + self.domain.lower()):
                # On multi-tenant sites, enforce seed path scope for same-origin content,
                # BUT exempt standard PDF asset directories which are often global.
                if "/wp-content/uploads/" in path:
                    return True
                if not self._url_matches_seed_scope(url):
                    return False
                return True
            if self._is_trusted_repository_host(host):
                # Allow trusted scholarly repository hosts for journals that externalize PDFs.
                # These should bypass seed path scoping as they are on a different domain.
                return "viewcontent.cgi" in path or path.endswith(".pdf") or "/files/" in path
            # Otherwise treat as external citation and skip
        except Exception:
            return False
        return False

    def _is_valid_volume_issue_url(self, url: str) -> bool:
        """Check if URL is a valid volume/issue navigation URL."""
        if not url:
            return False
        try:
            parsed = urlparse(url)
            host = parsed.netloc.lower()
        except Exception:
            return False
        same_domain = self.domain.lower() in host
        if not same_domain and not self._is_trusted_repository_host(host):
            return False
        # On multi-tenant sites, enforce seed path scope
        if same_domain and not self._url_matches_seed_scope(url):
            return False

        volume_issue_indicators = [
            "volume",
            "volume-",
            "issue",
            "archives",
            "all_issues",
            "current-issue",
            "print-edition",
            "back-issues",
            "previous",
        ]

        return any(indicator in url.lower() for indicator in volume_issue_indicators)

    def _is_trusted_repository_host(self, host: str) -> bool:
        host = (host or "").lower()
        hints = [
            "digitalcommons.",
            "scholarlycommons.",
            "scholarship.",
            "repository.",
            "commons.",
            "escholarship.org",
            "researchonline.",
            "ir.",
        ]
        return any(h in host for h in hints)

    def _seed_likely_archive(self, url: str) -> bool:
        lowered = (url or "").lower()
        return any(
            token in lowered
            for token in (
                "/archive",
                "/archives",
                "/issues",
                "/issue/",
                "/volume",
                "/volumes",
                "/publications",
                "all_issues.html",
                "print-edition",
            )
        )

    def _paginate_wp_api(self, api_url: str) -> Generator[DiscoveryResult, None, None]:
        """Handle WordPress REST API pagination."""
        page = 2
        max_pages = 100  # Safety limit, covers ~1000 posts (usually enough for full archives)

        while page <= max_pages:
            try:
                paginated_url = f"{api_url}?page={page}"
                response = self._request_with_retry(paginated_url, timeout=10)

                if response is None or response.status_code != 200:
                    break

                posts = response.json()
                if not posts:
                    break

                print(f"📄 Processing API page {page} with {len(posts)} posts")

                for post in posts:
                    yield from self._process_wp_api_post(post)

                page += 1

            except Exception as e:
                print(f"⚠️  Pagination error on page {page}: {e}")
                break

    def _extract_authors_from_content_rendered(self, rendered_html: str) -> List[str]:
        """Extract article author(s) from WordPress content.rendered HTML.

        Handles two dominant patterns:
        - Bold first paragraph (CUNY): <p><strong>Author Name</strong></p>
        - "By ..." opening text (JLSP, ASU): <p>By Author Name</p>
        """
        if not rendered_html:
            return []
        try:
            soup = BeautifulSoup(rendered_html, "lxml")
            paragraphs = soup.find_all("p")
            if not paragraphs:
                return []
            first_p = paragraphs[0]

            # Pattern A: first <p> is entirely a <strong> with the author name
            strong = first_p.find("strong")
            if strong and strong.get_text(strip=True) == first_p.get_text(strip=True):
                candidate = strong.get_text(strip=True)
                names = self._split_author_string(candidate)
                if names:
                    return names

            # Pattern B: first <p> starts with "By " followed by names
            first_text = first_p.get_text(strip=True)
            if first_text.startswith(("By ", "by ")):
                after_by = re.sub(r"^[Bb]y\s+", "", first_text)
                # Short paragraph: likely a dedicated byline
                if len(after_by) < 80:
                    names = self._split_author_string(after_by.rstrip(". "))
                    if names:
                        return names
                # Longer text: match structured name patterns at the start
                # "Firstname [M.] Lastname [& Firstname [M.] Lastname]"
                # Name parts: full words or single-letter initials (K.)
                name_match = re.match(
                    r"((?:(?:[A-Z][a-zA-Z''-]+|[A-Z]\.)\s+){1,3}[A-Z][a-zA-Z''-]+)"
                    r"(\s*(?:[&,]|,?\s+and)\s+"
                    r"(?:(?:[A-Z][a-zA-Z''-]+|[A-Z]\.)\s+){1,3}[A-Z][a-zA-Z''-]+)*",
                    after_by,
                )
                if name_match:
                    author_segment = name_match.group(0).strip()
                    if len(author_segment) < 100:
                        names = self._split_author_string(author_segment)
                        if names:
                            return names
        except Exception:
            pass
        return []

    def _split_author_string(self, author_string: str) -> List[str]:
        """Split 'Alice Jones & Bob Smith' or 'Alice Jones, Bob Smith, and Carol Lee'
        into individual author names.  Returns empty list if not real names."""
        if not author_string:
            return []
        # Normalise separators
        normalised = re.sub(r"\s*(?:&|;|\band\b)\s*", "|", author_string, flags=re.I)
        parts = [p.strip() for p in re.split(r"[|,]", normalised) if p.strip()]
        results = []
        for part in parts:
            part = part.strip().rstrip("*").strip()
            if not part:
                continue
            words = part.split()
            if len(words) < 2:
                continue
            if len(words) > 6:
                # Real author names rarely exceed 6 words
                continue
            if not words[0][0].isupper():
                continue
            if "@" in part or "http" in part.lower():
                continue
            if len(part) > 80:
                continue
            # Reject WP system account slugs
            if re.fullmatch(r"[a-z0-9_\-]+", part):
                continue
            results.append(part)
        return results

    # Download delegates to the generic adapter implementation
    def download_pdf(
        self,
        pdf_url: str,
        out_dir: str,
        referer: str = "",
        **kwargs: Any,
    ) -> Optional[str]:
        return self._download_with_generic(pdf_url, out_dir, referer=referer, **kwargs)
