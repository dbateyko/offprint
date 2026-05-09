import json, csv, glob, os, re
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).parent.parent.parent
REGISTRY = Path(__file__).parent
UPSTREAM = REGISTRY / "upstream"


def _host_from_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url.strip())
    host = (parsed.netloc or url.strip()).lower()
    host = re.sub(r"^https?://", "", host).rstrip("/").split("/")[0]
    return re.sub(r"^www\.", "", host)

# ── 1. Load sitemaps (canonical source) ──────────────────────────────────────
sitemaps_list = []
for f in sorted(glob.glob(str(ROOT / "offprint/sitemaps/*.json"))):
    try:
        d = json.load(open(f))
        meta = d.get("metadata", {})
        name = meta.get("journal_name") or meta.get("journal") or ""
        urls = d.get("start_urls") or ([d["url"]] if "url" in d else [])
        if not urls and "url" in meta:
            urls = [meta["url"]]
        if not urls or not name:
            continue
        url = meta.get("url") or urls[0]
        host = _host_from_url(url)
        rec = {
            "journal_name": name,
            "url": url,
            "host": host,
            "platform": meta.get("platform", ""),
            "status": meta.get("status", ""),
            "sitemap_file": os.path.basename(f),
            "source": "sitemap",
        }
        sitemaps_list.append(rec)
    except Exception as e:
        pass

print(f"Sitemaps: {len(sitemaps_list)} files")

# Index sitemaps for matching
sitemaps_by_host = {}
for s in sitemaps_list:
    sitemaps_by_host.setdefault(s["host"], []).append(s)

sitemaps_by_name = {}
for s in sitemaps_list:
    sitemaps_by_name.setdefault(s["journal_name"].lower().strip(), []).append(s)

# ── 2. Load W&L ───────────────────────────────────────────────────────────────
wlu_records = []  # list of {mainid, name, url, rank, host}
wlu_rank_map = {}  # name_lower -> rank

# Rankings
for row in csv.reader(open(UPSTREAM / "LawJournals.csv")):
    if len(row) >= 2 and row[0].strip().isdigit():
        wlu_rank_map[row[1].strip().lower()] = row[0].strip()

# Full W&L export
for row in csv.DictReader(open(UPSTREAM / "wlu_all_journals.csv")):
    url = (row.get("JournalURL") or row.get("FullTextURL") or "").strip()
    name = (row.get("Name") or "").strip()
    mainid = (row.get("MAINID") or "").strip()
    if not url or not name:
        continue
    host = _host_from_url(url)
    rank = wlu_rank_map.get(name.lower(), "")
    wlu_records.append({"mainid": mainid, "name": name, "url": url, "rank": rank, "host": host})

print(f"W&L: {len(wlu_records)} records")

# ── 3. CILP list ──────────────────────────────────────────────────────────────
cilp_names = set(open(UPSTREAM / "cilp_journals.csv").read().splitlines())
cilp_names.discard("Journal Name")  # strip header if present
cilp_lower = {n.lower(): n for n in cilp_names}
print(f"CILP: {len(cilp_names)} journals")

# ── 3b. Fixed-domain URL references (safe merge) ────────────────────────────
fixed_urls = [line.strip() for line in open(UPSTREAM / "fixed_domains.txt").read().splitlines() if line.strip()]
fixed_by_host = {}
for u in fixed_urls:
    h = _host_from_url(u)
    if not h:
        continue
    fixed_by_host.setdefault(h, []).append(u)
fixed_unique_by_host = {h: urls[0] for h, urls in fixed_by_host.items() if len(urls) == 1}
fixed_ambiguous_hosts = {h for h, urls in fixed_by_host.items() if len(urls) > 1}
print(
    f"Fixed-domain refs: {len(fixed_urls)} URLs "
    f"({len(fixed_unique_by_host)} unique-host mappable, {len(fixed_ambiguous_hosts)} ambiguous-host skipped)"
)

# ── 4. Merge: annotate sitemaps with W&L and CILP ────────────────────────────
rows = []
seen_names = set()

# Index W&L for faster lookup
wlu_by_host = {}
for w in wlu_records:
    wlu_by_host.setdefault(w["host"], []).append(w)
wlu_by_name = {w["name"].lower().strip(): w for w in wlu_records}

for rec in sitemaps_list:
    host = rec["host"]
    name_key = rec["journal_name"].lower().strip()
    
    # Try to find matching W&L info
    w = {}
    # Name match is strongest
    if name_key in wlu_by_name:
        w = wlu_by_name[name_key]
    # Then host match (if only one journal on that host in W&L)
    elif host in wlu_by_host and len(wlu_by_host[host]) == 1:
        w = wlu_by_host[host][0]
        
    rec["wlu_mainid"] = w.get("mainid", "")
    rec["wlu_rank"] = w.get("rank", "") or wlu_rank_map.get(name_key, "")
    rec["in_cilp"] = "yes" if name_key in cilp_lower else ""
    rec["fixed_domain_url"] = fixed_unique_by_host.get(host, "")
    
    rows.append(rec)
    seen_names.add(name_key)

# W&L journals not in sitemaps
wlu_only = 0
for w in wlu_records:
    name_key = w["name"].lower().strip()
    if name_key in seen_names:
        continue
    seen_names.add(name_key)
    rows.append({
        "journal_name": w["name"],
        "url": w["url"],
        "host": w["host"],
        "platform": "",
        "status": "no_sitemap",
        "sitemap_file": "",
        "wlu_mainid": w["mainid"],
        "wlu_rank": w["rank"],
        "in_cilp": "yes" if name_key in cilp_lower else "",
        "source": "wlu",
        "fixed_domain_url": fixed_unique_by_host.get(w["host"], ""),
    })
    wlu_only += 1

print(f"W&L-only additions: {wlu_only}")

# CILP names not yet in rows (no sitemap, no W&L match)
cilp_only = 0
for name_lower, name in cilp_lower.items():
    if name_lower not in seen_names:
        rows.append({
            "journal_name": name,
            "url": "",
            "host": "",
            "platform": "",
            "status": "no_sitemap",
            "sitemap_file": "",
            "wlu_mainid": "",
            "wlu_rank": wlu_rank_map.get(name_lower, ""),
            "in_cilp": "yes",
            "source": "cilp",
            "fixed_domain_url": "",
        })
        cilp_only += 1
        seen_names.add(name_lower)

print(f"CILP-only additions: {cilp_only}")
print(f"Total rows: {len(rows)}")

# ── 5. Sort: ranked first (by rank asc), then unranked alpha ────────────────
def sort_key(r):
    rk = r.get("wlu_rank")
    return (0 if rk else 1, int(rk) if rk else 0, r["journal_name"].lower())

rows.sort(key=sort_key)

# ── 6. Write ──────────────────────────────────────────────────────────────────
out = REGISTRY / "lawjournals.csv"
fields = [
    "journal_name",
    "url",
    "host",
    "platform",
    "status",
    "sitemap_file",
    "wlu_mainid",
    "wlu_rank",
    "in_cilp",
    "source",
    "fixed_domain_url",
]
with open(out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    w.writerows(rows)

print(f"\nWrote {out}")
