from __future__ import annotations

import hashlib
import os
import re
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlsplit

import requests
from bs4 import BeautifulSoup

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

# Domain-guarded TLS override for hosts with persistent certificate mismatches.
INSECURE_TLS_HOSTS = {"tlp.law.pitt.edu"}


def request_verify_for_url(url: str) -> bool:
    host = (urlsplit((url or "").strip()).netloc or "").lower()
    return host not in INSECURE_TLS_HOSTS


def fetch_page(session: requests.Session, url: str, timeout: int = 20) -> Optional[BeautifulSoup]:
    """Fetch and parse a page with standard headers."""
    try:
        response = session.get(
            url,
            headers=DEFAULT_HEADERS,
            timeout=timeout,
            verify=request_verify_for_url(url),
        )
        if response.status_code >= 400:
            return None
        return BeautifulSoup(response.text, "lxml")
    except requests.RequestException:
        return None


def is_pdf_url(url: str) -> bool:
    """Check if URL is likely a PDF based on extension and patterns."""
    lowered = url.lower()
    if lowered.endswith(".pdf") or re.search(r"\.pdf(?:[?#]|$)", lowered):
        return True
    # Avoid false positives like ".../pdfarchive" listing pages.
    if "pdfarchive" in lowered:
        return False
    # Accept strong path/query PDF hints beyond plain extension.
    if re.search(r"/pdf(?:/|$|\?)", lowered):
        return True
    if any(token in lowered for token in ("format=pdf", "type=pdf", "download=pdf")):
        return True
    # Plone file download endpoint used by several journal archives.
    if "/@@display-file/file" in lowered:
        return True
    # Common Digital Commons/Bepress direct-download pattern.
    if "viewcontent.cgi" in lowered and "article=" in lowered:
        return True
    # Common OJS galley-style links: /article/view/<id>/<galley_id>
    if re.search(r"/article/view/\d+/\d+(?:$|[/?#])", lowered):
        return True
    return False


def check_pdf_content_type(session: requests.Session, url: str, timeout: int = 15) -> bool:
    """HEAD request to verify PDF content type."""
    try:
        resp = session.head(
            url,
            headers=DEFAULT_HEADERS,
            allow_redirects=True,
            timeout=timeout,
            verify=request_verify_for_url(url),
        )
        ctype = resp.headers.get("Content-Type", "").lower()
        return resp.status_code < 400 and "application/pdf" in ctype
    except requests.RequestException:
        return False


def pre_validate_pdf_url(session: requests.Session, url: str, timeout: int = 10) -> bool:
    """Pre-validate URL with HEAD before doing a potentially large GET."""
    if is_pdf_url(url):
        return True
    lowered = (url or "").lower()
    # Quartex full-document downloads are tokenized API URLs without .pdf suffix.
    # HEAD can return 404/405 on this endpoint while GET still redirects to a valid PDF.
    if (
        "frontend-api.quartexcollections.com" in lowered
        and "/download/" in lowered
        and "/original" in lowered
    ):
        return True
    # DSpace bitstream content endpoints often return JSON metadata on HEAD
    # but serve PDF bytes on GET.
    if "/server/api/core/bitstreams/" in lowered and lowered.endswith("/content"):
        return True
    try:
        resp = session.head(
            url,
            headers=DEFAULT_HEADERS,
            allow_redirects=True,
            timeout=timeout,
            verify=request_verify_for_url(url),
        )
    except requests.RequestException:
        # HEAD is often blocked/misconfigured; don't hard-fail before GET.
        return True
    content_type = resp.headers.get("Content-Type", "").lower()
    disposition = resp.headers.get("Content-Disposition", "").lower()

    if resp.status_code < 400 and "application/pdf" in content_type:
        return True
    if "filename=" in disposition and ".pdf" in disposition:
        return True
    # Some hosts block or alter HEAD responses while GET works.
    if resp.status_code in {401, 403, 405, 406, 429}:
        return True
    # Treat successful HTML HEAD responses as inconclusive (common with CDN/proxy setups).
    if resp.status_code < 400 and "text/html" in content_type:
        return True
    # Fail only for explicit terminal miss responses.
    if resp.status_code in {404, 410}:
        return False
    # Conservative default: allow GET and validate via PDF magic bytes afterwards.
    return True


def validate_pdf_magic_bytes(path: str) -> bool:
    """Confirm a downloaded file is actually a PDF."""
    try:
        with open(path, "rb") as f:
            header = f.read(8)
        return header.startswith(b"%PDF-")
    except OSError:
        return False


def compute_pdf_sha256_and_size(path: str) -> Tuple[Optional[str], Optional[int]]:
    """Return SHA-256 and size in bytes for a downloaded PDF."""
    h = hashlib.sha256()
    total = 0
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
                total += len(chunk)
    except OSError:
        return None, None
    # Use the on-disk size as the source of truth when available.
    try:
        total = os.path.getsize(path)
    except OSError:
        pass
    return h.hexdigest(), total


def parse_authors(text: str) -> List[str]:
    """Parse author string handling 'and', commas, and semicolons."""
    if not text:
        return []
    normalized = re.sub(r"\s+", " ", text)
    # Replace common separators with commas
    normalized = normalized.replace("; and", ",").replace(" and ", ",")
    parts = re.split(r",|;", normalized)
    return [p.strip() for p in parts if p.strip()]


def extract_year(text: str) -> Optional[str]:
    """Extract year from various date formats."""
    if not text:
        return None
    match = re.search(r"(20\d{2}|19\d{2})", text)
    if match:
        return match.group(1)
    return None


def absolutize(base_url: str, href: str) -> str:
    """Resolve href relative to base_url."""
    return urljoin(base_url, href)
