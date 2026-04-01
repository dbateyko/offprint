"""Shared URL classification patterns for law review sites.

Extracts URL-type heuristics (archive, issue, article, PDF, Digital Commons)
used by the probe scripts and the site fingerprinter.
"""

from __future__ import annotations

import re
from urllib.parse import urlsplit

ARCHIVE_URL_RE = re.compile(
    r"(?:issue/archive|all_issues\.html|issues?|archives?|past[-_ ]?issues?|volumes?)",
    re.I,
)
ISSUE_URL_RE = re.compile(
    r"(?:\bvol(?:ume)?[-_/ ]?\d+\b|\bissue[-_/ ]?\d+\b|\b(?:spring|summer|fall|winter)\b|\b(?:19|20)\d{2}\b)",
    re.I,
)
ARTICLE_URL_RE = re.compile(r"\b(?:article|articles|paper|papers|post|posts)\b", re.I)
PDF_URL_RE = re.compile(
    r"(?:\.pdf(?:$|\?)|viewcontent\.cgi|/download(?:/|$|\?)|/bitstream(?:/|$|\?))",
    re.I,
)

DIGITAL_COMMONS_HINTS = (
    "digitalcommons.",
    "scholarlycommons.",
    "scholarship.",
    "scholarworks.",
    "engagedscholarship.",
    "repository.",
    "uknowledge.",
    "via.library.",
    "ir.lawnet.",
    "academicworks.",
    "ecollections.",
    "researchonline.",
    "openscholarship.",
    "lawecommons.",
    "lawrepository.",
    "nsuworks.",
    "brooklynworks.",
)


def classify_url(url: str, anchor_text: str = "") -> str:
    """Classify a URL as ``archive``, ``issue``, ``article``, ``pdf``, or ``other``.

    Uses path and optional anchor text to determine the URL type via regex
    heuristics.  PDF detection tests the raw URL/path directly (not anchor
    text) so that end-of-string matches like ``.pdf$`` work correctly.
    """
    url_text = (url or "").strip().lower()
    anchor = (anchor_text or "").strip().lower()
    parsed = urlsplit(url_text)
    path = parsed.path or "/"
    path_and_anchor = f"{path} {anchor}".strip()

    # PDF detection must test the raw URL/path directly. Appending anchor text
    # can break end-of-string matches like ".pdf$".
    if PDF_URL_RE.search(url_text) or PDF_URL_RE.search(path) or "pdf" in anchor:
        return "pdf"
    if ARCHIVE_URL_RE.search(path_and_anchor):
        return "archive"
    # Restrict issue/article heuristics to path + anchor text so query ids like
    # "iid=1952" and upload paths like "/2025/12/foo.pdf" do not look like issues.
    if ISSUE_URL_RE.search(path_and_anchor):
        return "issue"
    if ARTICLE_URL_RE.search(path_and_anchor):
        return "article"
    return "other"


def is_digital_commons_like(url: str) -> bool:
    """Return *True* if *url* looks like a Digital Commons / BePress site."""
    from urllib.parse import urlsplit as _urlsplit

    u = (url or "").lower()
    if "all_issues.html" in u:
        return True
    host = (_urlsplit(u).netloc or "").lower()
    if host.startswith("ir.") or host.startswith("dc.law."):
        return True
    return any(token in host for token in DIGITAL_COMMONS_HINTS)
