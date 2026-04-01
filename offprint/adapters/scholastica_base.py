from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Generator, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult


class ScholasticaBaseAdapter(Adapter):
    """
    Base adapter for Scholastica-based law review sites.

    Scholastica is a commercial platform for academic journals with:
    - Angular SPA architecture with window.JOURNAL data object
    - Standardized article/issue organization
    - Consistent API patterns across all sites
    - S3-based asset storage for PDFs and images

    Sites analyzed: Albany Law Review, Boston College Law Review, etc.
    """

    def __init__(self, journal_slug: str = None, **kwargs):
        super().__init__(**kwargs)
        self.journal_slug = journal_slug
        self.journal_data = None
        self.base_scholastica_domain = "scholasticahq.com"
        self.site_url = None

    def discover_pdfs(
        self, start_url: str, max_depth: int = 0
    ) -> Generator[DiscoveryResult, None, None]:
        """Discover PDFs from Scholastica site using embedded JSON data."""
        print(f"🔍 Discovering PDFs from Scholastica site: {start_url}")

        # Store site URL for API calls
        parsed = urlparse(start_url)
        self.site_url = f"{parsed.scheme}://{parsed.netloc}"

        # Extract journal metadata from homepage
        self.journal_data = self._extract_journal_data(start_url)

        if not self.journal_data:
            print("⚠️  Could not extract Scholastica journal data")
            return

        print(f"📊 Found journal: {self.journal_data.get('name', 'Unknown')}")
        print(f"📄 Articles available: {len(self.journal_data.get('published_article_ids', []))}")
        print(f"📚 Issues available: {len(self.journal_data.get('published_issue_ids', []))}")

        yielded_any = False

        # Prefer issue-index API traversal first (especially for /issues seeds) because
        # it yields relevant article IDs quickly and keeps smoke runs responsive.
        for result in self._discover_articles_from_issue_api():
            yielded_any = True
            yield result

        # Process articles using embedded article IDs
        if not yielded_any:
            yield from self._discover_articles_from_ids()

        # If max_depth > 0, also try issue-based discovery
        if max_depth > 0:
            yield from self._discover_from_issues()

    def _discover_articles_from_issue_api(self) -> Generator[DiscoveryResult, None, None]:
        """Discover article PDFs by querying the public issue API and then article API.

        This path is significantly faster than brute-force article-id traversal on
        large Scholastica journals and preserves issue-level metadata context.
        """
        if not self.site_url or not self.journal_data:
            return

        journal_id = self.journal_data.get("id")
        if not journal_id:
            return

        endpoint = f"{self.site_url}/api/v1/journals/{journal_id}/issues"
        page = 1
        per_page = 100
        seen_article_ids = set()
        yielded = 0

        while True:
            params = {
                "page": page,
                "per_page": per_page,
                "sort_column": "published_at",
                "sort_order": "desc",
            }
            try:
                response = self.session.get(
                    endpoint,
                    params=params,
                    timeout=15,
                    headers={"Accept": "application/json"},
                )
                if response.status_code != 200:
                    break
                payload = response.json()
            except Exception:
                break

            issues = (payload or {}).get("issues") or []
            if not issues:
                break

            for issue in issues:
                issue_meta = {
                    "issue_id": issue.get("id"),
                    "issue_slug": issue.get("slug"),
                    "issue_year": issue.get("year"),
                    "issue_volume": issue.get("volume"),
                    "issue_number": issue.get("number"),
                    "issue_path": issue.get("path"),
                    "issue_url": issue.get("url"),
                    "issue_published_at": issue.get("published_at"),
                }
                for raw_id in issue.get("article_ids") or []:
                    try:
                        article_id = int(raw_id)
                    except Exception:
                        continue
                    if article_id in seen_article_ids:
                        continue
                    seen_article_ids.add(article_id)
                    result = self._fetch_article_api(article_id)
                    if result is None:
                        continue
                    result.metadata.update(
                        {
                            k: v
                            for k, v in issue_meta.items()
                            if v not in (None, "")
                        }
                    )
                    yielded += 1
                    yield result

            meta = (payload or {}).get("meta") or {}
            current_page = int(meta.get("current_page") or page)
            total_pages = int(meta.get("total_pages") or current_page)
            if current_page >= total_pages:
                break
            page += 1

    def _extract_journal_data(self, url: str) -> Optional[Dict[str, Any]]:
        """Extract window.JOURNAL data from Scholastica homepage."""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()

            # Verify this is a Scholastica site
            if not self._is_scholastica_site(response.text):
                print("⚠️  Not a Scholastica site")
                return None

            # Extract window.JOURNAL object
            journal_data = self._parse_window_journal(response.text)

            if journal_data:
                # Set journal slug if not provided
                if not self.journal_slug:
                    self.journal_slug = journal_data.get("slug", "")

                return journal_data

        except Exception as e:
            print(f"⚠️  Error extracting journal data: {e}")

        return None

    def _is_scholastica_site(self, html: str) -> bool:
        """Check if site is powered by Scholastica."""
        scholastica_indicators = [
            "assets.scholasticahq.com",
            "<app-root>",
            "window.JOURNAL",
            "/dist/journal-website/",
        ]

        return any(indicator in html for indicator in scholastica_indicators)

    def _parse_window_journal(self, html: str) -> Optional[Dict[str, Any]]:
        """Parse window.JOURNAL object from HTML."""
        try:
            # Find window.JOURNAL assignment with more flexible pattern
            patterns = [
                r"window\.JOURNAL\s*=\s*({.*?});",  # Original pattern
                r"window\.JOURNAL\s*=\s*({.*?})\s*[;\n]",  # Allow newline after
                r"window\.JOURNAL\s*=\s*({[^}]*}[^;]*);",  # Handle nested objects
            ]

            for pattern in patterns:
                journal_match = re.search(pattern, html, re.DOTALL)
                if journal_match:
                    journal_json = journal_match.group(1)
                    try:
                        return json.loads(journal_json)
                    except json.JSONDecodeError:
                        # Try fixing common JSON issues
                        fixed_json = journal_json.replace("\n", " ").replace("\r", "")
                        return json.loads(fixed_json)

        except Exception as e:
            print(f"⚠️  Error parsing window.JOURNAL: {e}")

            # Debug: Show part of the HTML around window.JOURNAL
            debug_match = re.search(r"window\.JOURNAL\s*=\s*[^;]{1,200}", html)
            if debug_match:
                print(f"🔍 Found window.JOURNAL snippet: {debug_match.group()[:200]}...")

        return None

    def _discover_articles_from_ids(self) -> Generator[DiscoveryResult, None, None]:
        """Discover articles using published_article_ids from journal data.

        Uses parallel API fetches (up to 12 concurrent) for speed, since
        Scholastica API calls return lightweight JSON.
        """
        article_ids = self.journal_data.get("published_article_ids", [])

        if not article_ids:
            print("⚠️  No published article IDs found")
            return

        print(f"🔍 Processing {len(article_ids)} article IDs (parallel)...")

        # Parallel API fetch using plain requests sessions (no rate limiter)
        BATCH_SIZE = 12

        def _fetch_one(article_id: int) -> Optional[DiscoveryResult]:
            """Fetch a single article via API using a lightweight session."""
            if not self.site_url:
                return None
            api_url = f"{self.site_url}/api/v1/articles/{article_id}"
            try:
                resp = requests.get(api_url, timeout=10, headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LawReviewBot/1.0)",
                    "Accept": "application/json",
                })
                if resp.status_code == 200:
                    return self._parse_api_response(resp.json(), article_id)
            except Exception:
                pass
            return None

        fetched = 0
        with ThreadPoolExecutor(max_workers=BATCH_SIZE) as pool:
            futures = {pool.submit(_fetch_one, aid): aid for aid in article_ids}
            for future in as_completed(futures):
                fetched += 1
                if fetched % 50 == 0:
                    print(f"[{fetched}/{len(article_ids)}] Processing articles...")
                result = future.result()
                if result:
                    yield result
                    continue

                # API failed — fallback to HTML scraping (serial, via rate-limited session)
                article_id = futures[future]
                article_urls = self._generate_article_urls(article_id)
                for article_url in article_urls:
                    try:
                        response = self.session.get(article_url, timeout=10)
                        if response.status_code == 200:
                            yield from self._process_article_page(
                                response.text, article_url, article_id
                            )
                            break
                    except Exception as e:
                        print(f"⚠️  Error accessing {article_url}: {e}")
                        continue

    def _fetch_article_api(self, article_id: int) -> Optional[DiscoveryResult]:
        """Fetch article metadata and PDF link via Scholastica API."""
        if not self.site_url:
            return None

        api_url = f"{self.site_url}/api/v1/articles/{article_id}"

        try:
            response = self.session.get(api_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return self._parse_api_response(data, article_id)
        except Exception:
            pass
        return None

    def _parse_api_response(
        self, data: Dict[str, Any], article_id: int
    ) -> Optional[DiscoveryResult]:
        """Parse JSON response from Scholastica API."""
        if not data:
            return None

        # Extract PDF URL
        pdf_path = data.get("pdf_download_path")
        pdf_url = None
        if pdf_path:
            pdf_url = urljoin(self.site_url, pdf_path)
        else:
            # Fallback: some records expose only attachment IDs.
            slug = data.get("slug")
            attachment_ids = data.get("attachment_ids") or []
            if slug and attachment_ids:
                try:
                    pdf_url = (
                        f"{self.site_url}/article/{slug}/attachment/{int(attachment_ids[0])}.pdf"
                    )
                except Exception:
                    pdf_url = None
        if not pdf_url:
            return None

        # Extract metadata
        metadata = {
            "title": data.get("title"),
            "article_id": article_id,
            "page_url": f"{self.site_url}/article/{article_id}",  # Best guess canonical
            "url": f"{self.site_url}/article/{article_id}",
            "pdf_url": pdf_url,
            "platform": "Scholastica",
            "extraction_method": "scholastica_api",
            "date": data.get("published_at"),
            "publication_date": data.get("published_at"),
            "abstract": data.get("abstract"),
            "slug": data.get("slug"),
            "attachment_ids": data.get("attachment_ids") or [],
        }

        # Authors
        # Sometimes 'author_info' is a string, sometimes list?
        # The sample showed "author_info": "Sarah C. C. Tishler"
        author_info = data.get("author_info")
        if author_info:
            metadata["authors"] = [author_info] if isinstance(author_info, str) else author_info

        # Check CSL data for structured authors
        csl = data.get("to_csl", {})
        if csl and "author" in csl:
            authors = []
            for a in csl["author"]:
                name = f"{a.get('given', '')} {a.get('family', '')}".strip()
                if name:
                    authors.append(name)
            if authors:
                metadata["authors"] = authors

        # Volume/Issue from CSL
        if csl:
            if "volume" in csl:
                metadata["volume"] = csl["volume"]
            if "issue" in csl:
                metadata["issue"] = csl["issue"]
            if "page" in csl:
                metadata["pages"] = csl["page"]
            if "container-title" in csl:
                metadata["journal"] = csl.get("container-title")
            if "url" in csl and csl.get("url"):
                metadata["page_url"] = csl.get("url")
                metadata["url"] = csl.get("url")
            issued = csl.get("issued", {})
            try:
                date_parts = (
                    issued.get("date-parts", [[None]])[0]
                    if isinstance(issued, dict)
                    else [None]
                )
                year = date_parts[0]
                if year:
                    metadata["year"] = str(year)
            except Exception:
                pass

            # Best-effort legal citation string
            if metadata.get("volume") and metadata.get("pages"):
                journal_short = csl.get("container-title-short") or metadata.get("journal") or ""
                year = metadata.get("year", "")
                citation = f"{metadata['volume']} {journal_short} {metadata['pages']}".strip()
                if year:
                    citation = f"{citation} ({year})"
                metadata["citation"] = citation.strip()

        # Journal metadata
        if self.journal_data:
            metadata["journal"] = self.journal_data.get("name")

        return DiscoveryResult(page_url=metadata["page_url"], pdf_url=pdf_url, metadata=metadata)

    def _generate_article_urls(self, article_id: int) -> List[str]:
        """Generate potential article URLs based on Scholastica patterns."""
        base_url = self._get_base_url()

        url_patterns = [
            f"{base_url}/articles/{article_id}",
            f"{base_url}/en/articles/{article_id}",
            f"https://{self.journal_slug}.scholasticahq.com/articles/{article_id}",
            f"https://{self.journal_slug}.scholasticahq.com/en/articles/{article_id}",
            f"https://app.scholasticahq.com/articles/{article_id}",
        ]

        return url_patterns

    def _get_base_url(self) -> str:
        """Get base URL for the journal."""
        if self.journal_slug:
            return f"https://{self.journal_slug}.scholasticahq.com"
        return "https://app.scholasticahq.com"

    def _process_article_page(
        self, html: str, article_url: str, article_id: int
    ) -> Generator[DiscoveryResult, None, None]:
        """Process individual article page to extract PDFs and metadata."""
        soup = BeautifulSoup(html, "lxml")

        # Extract article metadata
        metadata = self._extract_article_metadata(soup, article_url, article_id)

        # Find PDF links
        pdf_urls = self._find_pdf_urls(soup, article_url, article_id)

        for pdf_url in pdf_urls:
            yield DiscoveryResult(page_url=article_url, pdf_url=pdf_url, metadata=metadata)

    def _find_pdf_urls(self, soup: BeautifulSoup, article_url: str, article_id: int) -> List[str]:
        """Find PDF download URLs for article."""
        pdf_urls = []

        # Common Scholastica PDF selectors
        pdf_selectors = [
            'a[href$=".pdf"]',
            'a[href*="/pdf"]',
            "a.download-pdf",
            'a:contains("PDF")',
            'a:contains("Download")',
            '[data-download="pdf"]',
        ]

        for selector in pdf_selectors:
            try:
                if ":contains(" in selector:
                    # Handle pseudo-selectors
                    text_search = selector.split(':contains("')[1].split('")')[0]
                    elements = soup.find_all("a", string=re.compile(text_search, re.I))
                else:
                    elements = soup.select(selector)

                for elem in elements:
                    href = elem.get("href")
                    if href:
                        pdf_url = urljoin(article_url, href)
                        if self._is_valid_pdf_url(pdf_url):
                            pdf_urls.append(pdf_url)

            except Exception:
                pass

        # If no PDFs found, try constructing direct URL
        if not pdf_urls:
            direct_pdf_urls = [
                f"{article_url}.pdf",
                f"{article_url}/pdf",
                f"https://s3.amazonaws.com/production.scholastica/articles/{article_id}/pdf",
            ]

            for pdf_url in direct_pdf_urls:
                if self._test_pdf_url(pdf_url):
                    pdf_urls.append(pdf_url)

        return pdf_urls

    def _extract_article_metadata(
        self, soup: BeautifulSoup, article_url: str, article_id: int
    ) -> Dict[str, Any]:
        """Extract comprehensive metadata from article page."""
        metadata = {
            "article_id": article_id,
            "page_url": article_url,
            "platform": "Scholastica",
            "extraction_method": "scholastica_base",
        }

        # Add journal metadata
        if self.journal_data:
            metadata.update(
                {
                    "journal": self.journal_data.get("name", ""),
                    "journal_slug": self.journal_data.get("slug", ""),
                    "journal_id": self.journal_data.get("id", ""),
                    "institution": self.journal_data.get("institution", ""),
                    "e_issn": self.journal_data.get("e_issn", ""),
                    "p_issn": self.journal_data.get("p_issn", ""),
                }
            )

        # Extract title
        title_selectors = ["h1.article-title", 'h1[class*="title"]', ".article-header h1", "title"]

        for selector in title_selectors:
            elements = soup.select(selector)
            if elements:
                title = elements[0].get_text(strip=True)
                if title and len(title) > 5:  # Basic validation
                    metadata["title"] = title
                    break

        # Extract authors
        author_selectors = [".article-authors a", ".author-name", '[class*="author"]', ".byline a"]

        authors = []
        for selector in author_selectors:
            elements = soup.select(selector)
            for elem in elements:
                author = elem.get_text(strip=True)
                if author and author not in authors:
                    authors.append(author)

        if authors:
            metadata["authors"] = authors

        # Extract publication date
        date_selectors = ["[datetime]", ".publication-date", ".article-date", "time"]

        for selector in date_selectors:
            elements = soup.select(selector)
            if elements:
                date_text = elements[0].get("datetime") or elements[0].get_text(strip=True)
                if date_text:
                    metadata["publication_date"] = date_text
                    break

        # Extract DOI if present
        doi_selectors = ['a[href*="doi.org"]', "[data-doi]", ".doi"]

        for selector in doi_selectors:
            elements = soup.select(selector)
            if elements:
                doi = (
                    elements[0].get("href")
                    or elements[0].get("data-doi")
                    or elements[0].get_text(strip=True)
                )
                if doi and "doi" in doi.lower():
                    metadata["doi"] = doi
                    break

        # Extract abstract
        abstract_selectors = [".abstract", ".article-abstract", '[class*="abstract"]']

        for selector in abstract_selectors:
            elements = soup.select(selector)
            if elements:
                abstract = elements[0].get_text(strip=True)
                if abstract and len(abstract) > 50:
                    metadata["abstract"] = abstract[:1000]  # Limit length
                    break

        return metadata

    def _discover_from_issues(self) -> Generator[DiscoveryResult, None, None]:
        """Discover articles by browsing through issues."""
        issue_ids = self.journal_data.get("published_issue_ids", [])

        if not issue_ids:
            return

        print(f"🔍 Processing {len(issue_ids)} issue IDs...")

        for issue_id in issue_ids:
            issue_urls = self._generate_issue_urls(issue_id)

            for issue_url in issue_urls:
                try:
                    response = self.session.get(issue_url, timeout=10)
                    if response.status_code == 200:
                        yield from self._process_issue_page(response.text, issue_url)
                        break

                except Exception as e:
                    print(f"⚠️  Error accessing issue {issue_url}: {e}")
                    continue

    def _generate_issue_urls(self, issue_id: int) -> List[str]:
        """Generate potential issue URLs."""
        base_url = self._get_base_url()

        return [
            f"{base_url}/issues/{issue_id}",
            f"{base_url}/en/issues/{issue_id}",
        ]

    def _process_issue_page(
        self, html: str, issue_url: str
    ) -> Generator[DiscoveryResult, None, None]:
        """Process issue page to find articles."""
        soup = BeautifulSoup(html, "lxml")

        # Find article links in issue
        article_links = soup.select('a[href*="/articles/"]')

        for link in article_links:
            href = link.get("href")
            if href:
                article_url = urljoin(issue_url, href)

                # Extract article ID from URL
                article_id_match = re.search(r"/articles/(\d+)", article_url)
                if article_id_match:
                    article_id = int(article_id_match.group(1))

                    try:
                        response = self.session.get(article_url, timeout=10)
                        if response.status_code == 200:
                            yield from self._process_article_page(
                                response.text, article_url, article_id
                            )
                    except Exception as e:
                        print(f"⚠️  Error processing article {article_url}: {e}")

    def _is_valid_pdf_url(self, url: str) -> bool:
        """Check if URL appears to be a valid PDF."""
        if not url:
            return False

        return (
            url.endswith(".pdf")
            or "/pdf" in url
            or "download" in url.lower()
            or "s3.amazonaws.com" in url
        )

    def _test_pdf_url(self, url: str) -> bool:
        """Test if PDF URL is accessible."""
        try:
            response = self.session.head(url, timeout=5)
            return (
                response.status_code == 200
                and "pdf" in response.headers.get("content-type", "").lower()
            )
        except Exception:
            return False
