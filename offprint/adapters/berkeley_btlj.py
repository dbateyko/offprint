from __future__ import annotations

import re
from typing import Generator, List, Optional
from bs4 import BeautifulSoup
from .base import DiscoveryResult
from .wordpress_academic_base import WordPressAcademicBaseAdapter


class BerkeleyBTLJAdapter(WordPressAcademicBaseAdapter):
    """
    Berkeley Technology Law Journal adapter.

    Site: https://btlj.org/
    Platform: WordPress
    Issue: Base adapter picks up site-wide metadata ("BTLJ") because PDFs are listed directly
           on Volume/Issue pages, and the base adapter attributes page-level metadata to all links.
    Fix: Parse the specific <li> structure: "<li><a href='...pdf'>Title</a> by Author</li>"
    """

    def __init__(self, **kwargs):
        super().__init__(
            base_url="https://btlj.org", journal_name="Berkeley Technology Law Journal", **kwargs
        )

    def _extract_pdfs_from_article(
        self, soup: BeautifulSoup, article_url: str, article_title: str,
        hint_authors: Optional[List[str]] = None,
    ) -> Generator[DiscoveryResult, None, None]:
        """
        Override to parse the specific list structure on BTLJ volume pages.
        Structure: <li><a href="...">Title</a> by Author</li>
        """
        # First, try the specific list item parsing
        found_custom = False

        # Look for lists containing PDF links
        for li in soup.select("div.text ul li, section.cc-graf-block ul li, .entry-content ul li"):
            # Find PDF link
            pdf_link = li.find("a", href=re.compile(r"\.pdf$", re.I))
            if not pdf_link:
                continue

            pdf_url = pdf_link.get("href")
            if not pdf_url:
                continue

            # Extract Title
            title = pdf_link.get_text(strip=True)

            # Extract Author (text after "by")
            li_text = li.get_text(" ", strip=True)
            authors = []

            # Pattern: "Title by Author"
            # Note: The link text might not be the full title text in the li,
            # but usually "by" follows the link.
            if " by " in li_text:
                author_part = li_text.split(" by ", 1)[1]
                # Cleanup: remove any trailing punctuation or "and" logic if simple
                # BTLJ usually lists: "Name" or "Name & Name"
                # Remove common suffixes if present (unlikely in this context)
                authors = [a.strip() for a in re.split(r",|&|\sand\s", author_part) if a.strip()]

            # Construct Metadata
            metadata = {
                "title": title,
                "authors": authors,
                "page_url": article_url,
                "url": article_url,
                "journal": self.journal_name,
                "domain": self.domain,
                "platform": "WordPress Academic",
                "extraction_method": "berkeley_btlj_custom",
            }

            # Add volume/issue if available from the page URL/Title
            vol_issue = self._extract_volume_issue(article_url, soup)
            metadata.update(vol_issue)

            # Try to parse Year from volume/issue page context or date
            if "date" not in metadata:
                # Fallback to page publication date
                page_date = self._extract_metadata_from_article(
                    soup, article_url, article_title
                ).get("date")
                if page_date:
                    metadata["date"] = page_date
                    metadata["publication_date"] = page_date

            yield DiscoveryResult(page_url=article_url, pdf_url=pdf_url, metadata=metadata)
            found_custom = True

        # Fallback to base implementation only if we didn't find the specific structure
        # (This handles normal blog posts or other page types that might exist)
        if not found_custom:
            yield from super()._extract_pdfs_from_article(soup, article_url, article_title, hint_authors=hint_authors)
