from __future__ import annotations

from .drupal import DrupalAdapter


class NebraskaLawReviewAdapter(DrupalAdapter):
    """Host-specific adapter for Nebraska Law Review.

    This Drupal host exposes many article PDFs through Digital Commons
    `cgi/viewcontent.cgi` links that the shared Drupal PDF detector can miss.
    """

    @staticmethod
    def _looks_like_pdf_href(href: str, anchor_text: str = "") -> bool:
        if DrupalAdapter._looks_like_pdf_href(href, anchor_text):
            return True
        lowered_href = (href or "").lower()
        return "/cgi/viewcontent.cgi" in lowered_href
