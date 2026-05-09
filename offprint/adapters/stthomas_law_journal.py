from __future__ import annotations

import re
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import Adapter, DiscoveryResult
from .generic import DEFAULT_HEADERS
from .utils import extract_year, fetch_page


class StThomasLawJournalAdapter(Adapter):
    """Discover University of St. Thomas Law Journal PDFs via Esploro APIs."""

    INSTITUTION = "01CLIC_STTHOMAS"
    OUTPUT_RE = re.compile(r"/esploro/outputs/(\d+)")

    def discover_pdfs(
        self, seed_url: str, max_depth: int = 0
    ) -> Iterable[DiscoveryResult]:
        output_urls = list(self._iter_output_urls(seed_url))
        seen: set[str] = set()

        for output_url in output_urls:
            output_id = self._output_id(output_url)
            if not output_id or output_id in seen:
                continue
            seen.add(output_id)
            result = self._result_for_output(output_id, output_url)
            if result is not None:
                yield result

    def _iter_output_urls(self, seed_url: str) -> Iterable[str]:
        output_id = self._output_id(seed_url)
        if output_id:
            yield seed_url
            return

        soup = fetch_page(self.session, seed_url)
        if not soup:
            return

        for link in soup.select('a[href*="/esploro/outputs/"]'):
            href = link.get("href")
            if href:
                yield urljoin(seed_url, href)

    def _output_id(self, url: str) -> str:
        match = self.OUTPUT_RE.search(urlparse(url).path)
        return match.group(1) if match else ""

    def _result_for_output(
        self, output_id: str, output_url: str
    ) -> DiscoveryResult | None:
        api_url = (
            "https://researchonline.stthomas.edu/esplorows/rest/research/"
            f"userAssets/getAssetByMmsID/{output_id}"
        )
        payload = self._get_json(api_url)
        if not payload:
            return None

        files = payload.get("files") or []
        first_file = next(
            (
                item
                for item in files
                if self._is_pdf_file(item)
                and item.get("digitalFileCreationData", {}).get("mid")
            ),
            None,
        )
        if not first_file:
            return None

        file_data = first_file.get("digitalFileCreationData", {})
        file_pid = str(file_data.get("mid") or "")
        pdf_url = (
            "https://researchonline.stthomas.edu/view/pdfCoverPage"
            f"?instCode={self.INSTITUTION}&filePid={file_pid}&download=true"
        )

        metadata = self._metadata_from_payload(payload, output_url)
        return DiscoveryResult(
            page_url=output_url,
            pdf_url=pdf_url,
            metadata=metadata,
        )

    def _get_json(self, url: str) -> dict[str, Any] | None:
        try:
            response = self.session.get(
                url,
                params={"institution": self.INSTITUTION},
                headers={**DEFAULT_HEADERS, "Accept": "application/json"},
                timeout=20,
            )
            if response.status_code >= 400:
                return None
            data = response.json()
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def _is_pdf_file(self, item: dict[str, Any]) -> bool:
        file_data = item.get("digitalFileCreationData", {})
        values = [
            item.get("fullFileName"),
            item.get("fileDisplayNameEsploro"),
            file_data.get("fileName"),
            file_data.get("mimeType"),
            file_data.get("extension"),
        ]
        text = " ".join(str(value or "") for value in values).lower()
        return "application/pdf" in text or ".pdf" in text or " pdf" in text

    def _metadata_from_payload(
        self, payload: dict[str, Any], output_url: str
    ) -> dict[str, Any]:
        record = payload.get("esploroRecord") or {}
        relation = self._first_relation(record)
        date_published = str(record.get("datePublished") or "")
        local_fields = record.get("localFields") or {}
        citation_html = str(local_fields.get("localNote13") or "")
        citation = BeautifulSoup(citation_html, "lxml").get_text(" ", strip=True)

        return {
            "title": str(record.get("title") or "").strip(),
            "authors": self._authors(payload),
            "date": extract_year(date_published) or date_published,
            "year": extract_year(date_published) or "",
            "volume": str(
                relation.get("volume")
                or str(local_fields.get("localNote14") or "").replace("Volume", "").strip()
            ).strip(),
            "issue": str(
                relation.get("issue")
                or str(local_fields.get("localNote15") or "").replace("Issue", "").strip()
            ).strip(),
            "pages": str(relation.get("startPage") or "").strip(),
            "citation": citation,
            "url": output_url,
            "source_url": output_url,
        }

    def _first_relation(self, record: dict[str, Any]) -> dict[str, Any]:
        relations = record.get("relationship") or []
        for relation in relations:
            if isinstance(relation, dict):
                return relation
        return {}

    def _authors(self, payload: dict[str, Any]) -> list[str]:
        authors = []
        for creator in payload.get("creators") or []:
            if not isinstance(creator, dict):
                continue
            name_parts = [
                str(creator.get("firstName") or "").strip(),
                str(creator.get("middleName") or "").strip(),
                str(creator.get("lastName") or "").strip(),
                str(creator.get("nameSuffix") or "").strip(),
            ]
            name = " ".join(part for part in name_parts if part)
            if name:
                authors.append(name)
        if authors:
            return authors
        temporary = (payload.get("esploroRecord") or {}).get("temporary") or {}
        first_author = str(temporary.get("firstAuthorName") or "").strip()
        return [first_author] if first_author else []
