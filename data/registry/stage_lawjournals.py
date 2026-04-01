import json, csv, glob, os, re
from collections import defaultdict
from pathlib import Path

ROOT = Path("/mnt/shared_storage/Data_Science/law-review-scrapers")
REGISTRY = ROOT / "data/registry"

# ── 1. Aggregate confirmed PDF counts per domain across all runs ──────────────
domain_ok = defaultdict(int)
for f in glob.glob(str(ROOT / "artifacts/runs/*/stats.json")):
    try:
        s = json.load(open(f))
        for domain, data in s.get("domains", {}).items():
            domain_ok[domain] += data.get("ok_total", 0)
    except:
        pass

confirmed = {d for d, n in domain_ok.items() if n >= 20}
print(f"Confirmed domains (20+ PDFs): {len(confirmed)}")

# ── 2. Read lawjournals.csv and add/update confirmed_working column ───────────
rows = list(csv.DictReader(open(REGISTRY / "lawjournals.csv")))

# Add new column if not present
if "confirmed_working" not in rows[0]:
    for r in rows:
        r["confirmed_working"] = ""

updated = 0
for r in rows:
    host = r.get("host", "")
    pdf_count = domain_ok.get(host, 0)
    was = r.get("confirmed_working", "")
    if pdf_count >= 20:
        r["confirmed_working"] = str(pdf_count)
        if was != str(pdf_count):
            updated += 1
    # don't clear existing confirmed if domain just isn't in recent runs

print(f"Rows updated with pdf count: {updated}")

# ── 3. Write back ──────────────────────────────────────────────────────────────
fields = list(rows[0].keys())
if "confirmed_working" not in fields:
    fields.append("confirmed_working")

with open(REGISTRY / "lawjournals.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    w.writerows(rows)

print(f"Wrote {REGISTRY / 'lawjournals.csv'}")

# Summary
no_sitemap = sum(1 for r in rows if r["status"] == "no_sitemap")
has_sitemap = sum(1 for r in rows if r["sitemap_file"])
confirmed_ct = sum(1 for r in rows if r.get("confirmed_working"))
print(f"\nSummary:")
print(f"  Total journals:        {len(rows)}")
print(f"  Have sitemap:          {has_sitemap}")
print(f"  Confirmed working:     {confirmed_ct}")
print(f"  No sitemap yet:        {no_sitemap}")
