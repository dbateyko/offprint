import json, csv, glob, os, re
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path("/mnt/shared_storage/Data_Science/law-review-scrapers")
REGISTRY = ROOT / "data/registry"
UPSTREAM = REGISTRY / "upstream"


def _host_from_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url.strip())
    host = (parsed.netloc or url.strip()).lower()
    host = re.sub(r"^https?://", "", host).rstrip("/").split("/")[0]
    return re.sub(r"^www\.", "", host)

# ── 1. Load sitemaps (canonical source) ──────────────────────────────────────
sitemaps = {}  # domain -> record
for f in sorted(glob.glob(str(ROOT / "offprint/sitemaps/*.json"))):
    try:
        d = json.load(open(f))
        meta = d.get("metadata", {})
        name = meta.get("journal_name", "")
        urls = d.get("start_urls") or ([d["url"]] if "url" in d else [])
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
            "wlu_mainid": "",
            "wlu_rank": "",
            "in_cilp": "",
            "source": "sitemap",
        }
        # key by host; if host already seen keep active > paused > todo
        if host not in sitemaps:
            sitemaps[host] = rec
        else:
            existing = sitemaps[host]
            def rank_status(s):
                if s == "active": return 0
                if s and s.startswith("paused"): return 2
                return 1
            if rank_status(rec["status"]) < rank_status(existing["status"]):
                sitemaps[host] = rec
    except Exception as e:
        pass

print(f"Sitemaps: {len(sitemaps)} unique hosts")

# ── 2. Load W&L ───────────────────────────────────────────────────────────────
wlu_by_host = {}  # host -> {mainid, name, url, rank}
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
    wlu_by_host[host] = {"mainid": mainid, "name": name, "url": url, "rank": rank}

print(f"W&L: {len(wlu_by_host)} unique hosts")

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
seen_hosts = set()

for host, rec in sitemaps.items():
    seen_hosts.add(host)
    # Enrich with W&L
    if host in wlu_by_host:
        w = wlu_by_host[host]
        rec["wlu_mainid"] = w["mainid"]
        rec["wlu_rank"] = w["rank"] or wlu_rank_map.get(rec["journal_name"].lower(), "")
    else:
        rec["wlu_rank"] = wlu_rank_map.get(rec["journal_name"].lower(), "")
    # CILP match by name
    rec["in_cilp"] = "yes" if rec["journal_name"].lower() in cilp_lower else ""
    rec["fixed_domain_url"] = fixed_unique_by_host.get(host, "")
    rows.append(rec)

# W&L hosts not in sitemaps
wlu_only = 0
for host, w in wlu_by_host.items():
    if host in seen_hosts:
        continue
    seen_hosts.add(host)
    rows.append({
        "journal_name": w["name"],
        "url": w["url"],
        "host": host,
        "platform": "",
        "status": "no_sitemap",
        "sitemap_file": "",
        "wlu_mainid": w["mainid"],
        "wlu_rank": w["rank"],
        "in_cilp": "yes" if w["name"].lower() in cilp_lower else "",
        "source": "wlu",
        "fixed_domain_url": fixed_unique_by_host.get(host, ""),
    })
    wlu_only += 1

print(f"W&L-only additions: {wlu_only}")

# CILP names not yet in rows (no sitemap, no W&L match)
covered_names = {r["journal_name"].lower() for r in rows}
cilp_only = 0
for name_lower, name in cilp_lower.items():
    if name_lower not in covered_names:
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

print(f"CILP-only additions: {cilp_only}")
print(f"Total rows: {len(rows)}")

# ── 5. Sort: ranked first (by rank asc), then unranked alpha ────────────────
def sort_key(r):
    rk = r["wlu_rank"]
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
