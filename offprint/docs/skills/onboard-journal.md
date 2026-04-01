# Onboard a New Law Journal

Guide for discovering, analyzing, and onboarding a new law review journal site into the scraper pipeline.

## Prerequisites

- Repository cloned and in working state
- Understanding of adapter pattern (see `offprint/adapters/README.md`)
- Python environment with `requests` and `beautifulsoup4` available

## Step 1: Identify the Journal URL

Start with the journal's canonical homepage. Common patterns:
- `https://law.university.edu/journal-name/`
- `https://journalname.university.edu/`
- `https://journalname.org/`

If the exact URL is unknown, search:
```bash
# DuckDuckGo search for journal site
curl "https://html.duckduckgo.com/html/?q=JOURNAL_NAME+site:university.edu"
```

## Step 2: Detect Platform

Fetch the homepage and identify the platform:

```python
import requests

url = "https://journal-url.com/"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
response = requests.get(url, headers=headers, timeout=10)
html = response.text

# Platform detection
if 'digitalcommons' in url.lower() or 'bepress' in html.lower():
    print("Platform: Digital Commons")
    # Use DigitalCommonsBaseAdapter - see separate DC onboarding guide
elif 'wordpress' in html.lower() or 'wp-content' in html.lower():
    print("Platform: WordPress")
    # Continue with WordPress onboarding below
elif 'ojs' in html.lower() or 'open journal' in html.lower():
    print("Platform: OJS")
    # Use OJSAdapter
else:
    print("Platform: Custom/Unknown")
    # May need custom adapter or GenericAdapter
```

## Step 3: Check for PDF Availability (CRITICAL)

**Web-only blogs are NOT suitable for our PDF corpus.** Check for actual downloadable PDFs:

```python
import re

# Check homepage for PDF links
pdf_links = re.findall(r'<a[^>]*href="([^"]*\.pdf[^"]*)"', html)
print(f"PDF links on homepage: {len(pdf_links)}")

# If 0 PDFs, check for archive/issues page
archive_links = re.findall(r'<a[^>]*href="([^"]*)"[^>]*>[^<]*(?:archive|issues|volumes)[^<]*</a>', html, re.IGNORECASE)
if archive_links:
    # Fetch archive page and check for PDFs there
    archive_url = archive_links[0]
    if not archive_url.startswith('http'):
        # Make absolute
        from urllib.parse import urljoin
        archive_url = urljoin(url, archive_url)
    
    archive_response = requests.get(archive_url, headers=headers, timeout=10)
    archive_pdfs = re.findall(r'<a[^>]*href="([^"]*\.pdf[^"]*)"', archive_response.text)
    print(f"PDF links on archive page: {len(archive_pdfs)}")
    
    # Verify PDFs are actual articles, not citations
    # Check context around PDF links
```

**Red flags - journal is NOT suitable:**
- Zero PDF links anywhere
- PDF links are only citations/references to external documents
- Articles are published as HTML blog posts only
- Sitemap shows 0 PDF URLs

**Green flags - journal IS suitable:**
- PDF links on archive/article listing pages
- PDF filenames match article titles or have clear article identifiers
- "Download PDF" or similar download buttons present

## Step 4: Identify Archive/Listing Page

Find where articles are listed (may be homepage, `/issues/`, `/archive/`, etc.):

```python
# Look for archive links
archive_patterns = [
    r'<a[^>]*href="([^"]*)"[^>]*>[^<]*(?:all issues|archive|issues|volumes)[^<]*</a>',
    r'<a[^>]*href="([^"]*/(?:issues|archive|volumes)/?)"',
]

for pattern in archive_patterns:
    matches = re.findall(pattern, html, re.IGNORECASE)
    if matches:
        print(f"Archive page: {matches[0]}")
        break
```

## Step 5: Extract CSS Selectors

Analyze the archive page structure to identify selectors:

```python
import re

# Fetch archive page
archive_url = "https://journal-url.com/issues/"  # or whatever you found
archive_response = requests.get(archive_url, headers=headers, timeout=10)
archive_html = archive_response.text

# Find article container pattern
# Common WordPress patterns:
# - <li class="kb-post-list-item"> (Kadence theme)
# - <article class="entry"> or <article class="post">
# - <div class="post"> or <div class="entry">
# - <li class="post">

# Look for the structure around article titles
title_pattern = r'<h2 class="entry-title"><a href="([^"]*)"[^>]*>(.*?)</a></h2>'
titles = list(re.finditer(title_pattern, archive_html, re.DOTALL))
print(f"Found {len(titles)} article titles")

# For first title, find parent container
if titles:
    match = titles[0]
    start = max(0, match.start() - 2000)
    context = archive_html[start:match.end() + 500]
    
    # Find the container
    container_patterns = [
        r'<li class="([^"]*post[^"]*)"[^>]*>.*?<h2 class="entry-title">',
        r'<article class="([^"]*entry[^"]*)"[^>]*>.*?<h2 class="entry-title">',
        r'<div class="([^"]*post[^"]*)"[^>]*>.*?<h2 class="entry-title">',
    ]
    
    for pattern in container_patterns:
        container_match = re.search(pattern, context, re.DOTALL)
        if container_match:
            print(f"Article container: <{container_match.group(0).split('>')[0]}>")
            break
```

### Selector Types Needed

For WordPress adapter, you need these selectors:

| ID | Type | Selector | Parent | Description |
|----|------|----------|--------|-------------|
| `article_container` | SelectorElement | e.g., `li.kb-post-list-item` | `_root` | Container for each article |
| `title` | SelectorText or SelectorLink | e.g., `h2.entry-title a` | `article_container` | Article title |
| `author` | SelectorText | e.g., `span.fn.n` | `article_container` | Author name (optional) |
| `date` | SelectorText | e.g., `time.entry-date.published` | `article_container` | Publication date (optional) |
| `pdf_link` | SelectorLink | e.g., `a[href$=".pdf"]` | `article_container` | Link to PDF |

## Step 6: Check Pagination

```python
# Look for pagination
pagination_patterns = [
    r'<a[^>]*rel="next"[^>]*href="([^"]*)"',
    r'<a[^>]*class="[^"]*next[^"]*"[^>]*href="([^"]*)"',
    r'<a[^>]*href="([^"]*/page/\d+)/"[^>]*>\d+</a>',
]

for pattern in pagination_patterns:
    matches = re.findall(pattern, archive_html)
    if matches:
        print(f"Pagination found: {matches[0]}")
        break
else:
    print("No pagination detected (single page archive)")
```

## Step 7: Create Seed JSON

Create a new file in `sitemaps/<domain-slug>.json`:

```json
{
  "id": "journal-domain-slug",
  "start_urls": [
    "https://journal-url.com/issues/"
  ],
  "source": "manual",
  "metadata": {
    "journal_name": "Journal Name",
    "platform": "wordpress",
    "url": "https://journal-url.com/",
    "created_date": "2026-03-31",
    "status": "active",
    "status_reason": "",
    "status_updated_at": "2026-03-31T00:00:00Z",
    "status_evidence_ref": ""
  },
  "selectors": [
    {
      "id": "article_container",
      "type": "SelectorElement",
      "selector": "li.kb-post-list-item",
      "parentSelectors": ["_root"]
    },
    {
      "id": "title",
      "type": "SelectorText",
      "selector": "h2.entry-title a",
      "parentSelectors": ["article_container"]
    },
    {
      "id": "author",
      "type": "SelectorText",
      "selector": "span.fn.n",
      "parentSelectors": ["article_container"]
    },
    {
      "id": "date",
      "type": "SelectorText",
      "selector": "time.entry-date.published",
      "parentSelectors": ["article_container"]
    },
    {
      "id": "pdf_link",
      "type": "SelectorLink",
      "selector": "a[href$='.pdf']",
      "parentSelectors": ["article_container"]
    }
  ]
}
```

## Step 8: Test with Smoke Run

```bash
# Run a smoke test on the new site
python scripts/smoke_one_pdf_per_site.py --sitemaps-dir sitemaps --max-workers 8

# Or run just your new site
python scripts/run_pipeline.py --mode full --seeds sitemaps/journal-domain-slug.json
```

Check `artifacts/runs/<run_id>/stats.json` for results.

## Step 9: Register Adapter (if needed)

For WordPress sites, the `WordPressAcademicBaseAdapter` is usually sufficient. It's already registered for all WordPress domains. If you need host-specific customization:

1. Create `offprint/adapters/journal_specific.py`
2. Subclass `WordPressAcademicBaseAdapter`
3. Override methods as needed
4. Register in `offprint/adapters/registry.py`

## Common Issues

### "No PDFs found"
- Journal may be web-only (not suitable)
- PDFs may be on article detail pages, not listing page
- May need to adjust PDF link selector

### "Wrong adapter selected"
- Check `offprint/adapters/registry.py` for domain mapping
- Platform detection may be wrong

### "Selectors not finding articles"
- Re-check the HTML structure
- WordPress themes vary widely; selectors are theme-specific
- Use browser dev tools to inspect actual structure

### Cloudflare/WAF blocking
- May need `--playwright-headless` or `--no-use-playwright`
- Some sites require JavaScript rendering

## When to Skip a Journal

Do NOT onboard if:
- Web-only blog format (no downloadable PDFs)
- PDF links are only citations to external documents
- Site requires login/authentication
- Heavily WAF-protected with no workaround
- Journal is defunct or no longer publishing
