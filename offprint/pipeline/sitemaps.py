import csv
import hashlib
import json
import os
import re
from typing import List
from urllib.parse import urlparse

from ..seed_catalog import load_seed_entries as load_seed_catalog_entries

RANGE_RE = re.compile(r"\[(\d+)-(\d+)\]")

def _expand_ranges(url: str) -> List[str]:
    match = RANGE_RE.search(url)
    if not match:
        return [url]
    start, end = int(match.group(1)), int(match.group(2))
    expanded: List[str] = []
    for n in range(start, end + 1):
        expanded.extend(_expand_ranges(RANGE_RE.sub(str(n), url, count=1)))
    return expanded

def _is_http_seed(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)

def read_sitemaps_csv(path: str) -> List[str]:
    seeds: List[str] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        first = True
        for row in reader:
            if not row:
                continue
            if first and row[0].strip().lower() in {"law_review", "name"}:
                first = False
                continue
            first = False

            if len(row) > 1 and row[1].strip():
                raw = row[1].strip()
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    try:
                        data = json.loads(raw.strip().strip('"').replace('""', '"'))
                    except Exception:
                        data = None
                if isinstance(data, dict) and isinstance(data.get("startUrl"), list):
                    for u in data["startUrl"]:
                        if isinstance(u, str) and u:
                            for e in _expand_ranges(u):
                                if _is_http_seed(e):
                                    seeds.append(e)
                    continue

            url = row[0].strip()
            if not url or url.lower().startswith("#"):
                continue
            if _is_http_seed(url):
                seeds.append(url)
    return seeds

def read_sitemaps_dir(path: str) -> List[str]:
    return [entry.seed_url for entry in load_seed_catalog_entries(path)]

def _seed_checkpoint_path(checkpoint_dir: str, seed_url: str) -> str:
    digest = hashlib.sha1(seed_url.encode("utf-8")).hexdigest()[:16]
    return os.path.join(checkpoint_dir, f"{digest}.jsonl")

def _load_completed_issue_urls(path: str) -> set[str]:
    completed: set[str] = set()
    if not os.path.exists(path):
        return completed
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            issue_url = str(payload.get("issue_url") or "").strip()
            status = str(payload.get("status") or "").strip().lower()
            if issue_url and status == "completed":
                completed.add(issue_url)
    return completed
