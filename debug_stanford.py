import requests
from bs4 import BeautifulSoup
from offprint.adapters.selector_driven import SelectorDrivenAdapter
import json

# Stanford Law Review setup
sitemap_path = "offprint/sitemaps/stanford_law_review.json"
with open(sitemap_path) as f:
    sitemap = json.load(f)

adapter = SelectorDrivenAdapter(sitemap=sitemap)
seed_url = "https://www.stanfordlawreview.org/online-archive/"

# Run discovery
results = list(adapter.discover_pdfs(seed_url))
print(f"Results found: {len(results)}")
for r in results:
    print(f"PDF URL: {r.pdf_url}")
    print(f"Metadata title: {r.metadata.get('article_title')}")
