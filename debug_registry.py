from offprint.adapters.registry import _find_sitemap_for_url

url = "https://www.stanfordlawreview.org/online-archive/"
sitemap = _find_sitemap_for_url(url)
if sitemap:
    print(f"Found sitemap for Stanford Law Review: {sitemap.get('id')}")
else:
    print(f"FAILED to find sitemap for Stanford Law Review: {url}")
