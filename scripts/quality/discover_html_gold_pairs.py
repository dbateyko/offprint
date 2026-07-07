#!/usr/bin/env python3
"""Discover and download complete HTML+PDF pairs for text-gold generation."""
from __future__ import annotations

import argparse
import csv
import importlib.util
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[2]
BUILDER_PATH = Path(__file__).with_name("build_html_gold.py")
SPEC = importlib.util.spec_from_file_location("build_html_gold", BUILDER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot import {BUILDER_PATH}")
BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILDER)

SOURCES = {
    "californialawreview.org": {
        "sitemaps": ("https://www.californialawreview.org/sitemap.xml",),
        "path_prefix": "/print/",
    },
    "columbialawreview.org": {
        "sitemaps": ("https://columbialawreview.org/wp-sitemap.xml",),
        "path_prefix": "/content/",
    },
    "houstonlawreview.org": {
        "sitemaps": ("https://houstonlawreview.org/sitemap.xml",),
        "path_prefix": "/article/",
    },
}


def _xml_locations(xml_text: str) -> tuple[str, list[str]]:
    root = ET.fromstring(xml_text)
    kind = root.tag.rsplit("}", 1)[-1]
    locations = [
        (element.text or "").strip()
        for element in root.iter()
        if element.tag.rsplit("}", 1)[-1] == "loc" and (element.text or "").strip()
    ]
    return kind, locations


def sitemap_urls(session: requests.Session, sitemap_url: str) -> list[str]:
    response = session.get(sitemap_url, timeout=60)
    response.raise_for_status()
    kind, locations = _xml_locations(response.text)
    if kind != "sitemapindex":
        return locations
    urls: list[str] = []
    for child_url in locations:
        child = session.get(child_url, timeout=60)
        child.raise_for_status()
        _child_kind, child_locations = _xml_locations(child.text)
        urls.extend(child_locations)
    return urls


def _pdf_url(soup: BeautifulSoup, html_url: str) -> str | None:
    candidates: list[tuple[int, str]] = []
    html_host = (urlparse(html_url).hostname or "").removeprefix("www.")
    for anchor in soup.select("a[href]"):
        href = urljoin(html_url, str(anchor.get("href") or ""))
        if ".pdf" not in urlparse(href).path.lower():
            continue
        text = " ".join(anchor.get_text(" ", strip=True).lower().split())
        host = (urlparse(href).hostname or "").removeprefix("www.")
        score = 0
        if "download" in text or text == "pdf":
            score += 4
        if host == html_host:
            score += 2
        if "/attachment/" in href or "/wp-content/uploads/" in href or "/s/" in href:
            score += 2
        candidates.append((score, href))
    score, best_url = max(candidates, default=(0, ""))
    domain = html_host
    path = urlparse(best_url).path.lower()
    if domain == "houstonlawreview.org" and "/attachment/" not in path:
        return None
    if domain == "columbialawreview.org" and "/wp-content/uploads/" not in path:
        return None
    if domain == "californialawreview.org" and score < 6:
        return None
    return best_url if score >= 6 else None


def discover_pairs(
    domains: list[str],
    *,
    output_dir: Path,
    per_domain: int,
    min_notes: int,
    max_notes: int,
) -> list[dict[str, str]]:
    session = requests.Session()
    session.headers["User-Agent"] = "Offprint HTML gold discovery/0.1"
    rows: list[dict[str, str]] = []
    for domain in domains:
        config = SOURCES[domain]
        candidates: list[str] = []
        for sitemap in config["sitemaps"]:
            candidates.extend(sitemap_urls(session, sitemap))
        prefix = str(config["path_prefix"])
        article_urls = sorted(
            {
                url
                for url in candidates
                if (urlparse(url).hostname or "").removeprefix("www.") == domain
                and urlparse(url).path.startswith(prefix)
                and urlparse(url).path.rstrip("/") != prefix.rstrip("/")
            },
            reverse=True,
        )
        accepted = 0
        for html_url in article_urls:
            if accepted >= per_domain:
                break
            try:
                response = session.get(html_url, timeout=60)
                response.raise_for_status()
                notes, selector = BUILDER.extract_html_footnotes(response.text, html_url)
                if not min_notes <= len(notes) <= max_notes:
                    continue
                soup = BeautifulSoup(response.text, "html.parser")
                pdf_url = _pdf_url(soup, html_url)
                if not pdf_url:
                    continue
                pdf_dir = output_dir / "pdfs" / domain
                pdf_dir.mkdir(parents=True, exist_ok=True)
                filename = Path(urlparse(pdf_url).path).name or f"document-{accepted + 1}.pdf"
                pdf_path = pdf_dir / filename
                if not pdf_path.exists():
                    pdf_response = session.get(pdf_url, timeout=120)
                    pdf_response.raise_for_status()
                    if not pdf_response.content.startswith(b"%PDF"):
                        continue
                    pdf_path.write_bytes(pdf_response.content)
                rows.append(
                    {
                        "html_url": html_url,
                        "pdf_path": str(pdf_path.resolve()),
                        "domain": domain,
                        "pdf_url": pdf_url,
                        "html_note_count": str(len(notes)),
                        "selector": selector,
                    }
                )
                accepted += 1
                print(f"{domain}: {accepted}/{per_domain} {html_url} ({len(notes)} notes)")
            except Exception as exc:
                print(f"skip {html_url}: {exc}")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="Output CSV pair manifest")
    parser.add_argument("--download-dir", type=Path, required=True)
    parser.add_argument("--domains", nargs="+", choices=sorted(SOURCES), default=sorted(SOURCES))
    parser.add_argument("--per-domain", type=int, default=17)
    parser.add_argument("--min-notes", type=int, default=5)
    parser.add_argument("--max-notes", type=int, default=180)
    args = parser.parse_args()
    rows = discover_pairs(
        args.domains,
        output_dir=args.download_dir,
        per_domain=args.per_domain,
        min_notes=args.min_notes,
        max_notes=args.max_notes,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    fields = ["html_url", "pdf_path", "domain", "pdf_url", "html_note_count", "selector"]
    with args.out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} pairs to {args.out}")
    return 0 if len(rows) >= args.per_domain * len(args.domains) else 1


if __name__ == "__main__":
    raise SystemExit(main())
