from __future__ import annotations

import json
import os
import re
from glob import glob
from typing import Dict, List, Optional
from urllib.parse import urlparse

RANGE_RE = re.compile(r"\[(\d+)-(\d+)\]")


def expand_ranges(url: str) -> List[str]:
    """Expand [start-end] patterns in URLs."""
    match = RANGE_RE.search(url)
    if not match:
        return [url]
    start, end = int(match.group(1)), int(match.group(2))
    expanded: List[str] = []
    for n in range(start, end + 1):
        expanded.extend(expand_ranges(RANGE_RE.sub(str(n), url, count=1)))
    return expanded


class SitemapStore:
    """Load and index sitemap JSON files for selector-driven scraping."""

    def __init__(self, directory: str) -> None:
        self.directory = directory
        self.sitemaps: List[Dict] = self._load()
        self._host_index: Dict[str, List[Dict]] = self._index_by_host()

    def _load(self) -> List[Dict]:
        configs: List[Dict] = []
        pattern = os.path.join(self.directory, "*.json")
        for path in sorted(glob(pattern)):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
            except Exception:
                continue

            start_urls = self._normalize_start_urls(raw)
            selectors = raw.get("selectors") or raw.get("raw_sitemap", {}).get("selectors", [])
            cfg = {
                "id": raw.get("id")
                or raw.get("_id")
                or raw.get("raw_sitemap", {}).get("_id")
                or os.path.splitext(os.path.basename(path))[0],
                "path": path,
                "start_urls": start_urls,
                "selectors": selectors,
                "raw": raw,
            }
            configs.append(cfg)
        return configs

    def _index_by_host(self) -> Dict[str, List[Dict]]:
        hosts: Dict[str, List[Dict]] = {}
        for cfg in self.sitemaps:
            for url in cfg.get("start_urls", []):
                host = urlparse(url).netloc.lower()
                if not host:
                    continue
                hosts.setdefault(host, []).append(cfg)
        return hosts

    def _normalize_start_urls(self, raw: Dict) -> List[str]:
        start_urls = raw.get("start_urls") or raw.get("startUrl") or []
        if isinstance(start_urls, str):
            start_urls = [start_urls]
        if not isinstance(start_urls, list):
            return []
        return [u for u in start_urls if isinstance(u, str) and u]

    def seeds(self) -> List[str]:
        seeds: List[str] = []
        for cfg in self.sitemaps:
            for url in cfg.get("start_urls", []):
                seeds.extend(expand_ranges(url))
        return seeds

    def find_for_host(self, host: str) -> Optional[Dict]:
        host = (host or "").lower()
        for domain, cfgs in self._host_index.items():
            if host == domain or host.endswith("." + domain):
                return cfgs[0]
        return None

    def find_for_url(self, url: str) -> Optional[Dict]:
        return self.find_for_host(urlparse(url).netloc)
