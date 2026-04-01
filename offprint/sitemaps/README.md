# sitemaps

One JSON file per journal seed.

- Each file defines start URL(s) and metadata used for routing and run-state management.
- Prefer updating sitemap metadata or adding a site-specific adapter before changing shared adapter bases.
- Validate changes with smoke runs (`scripts/smoke_one_pdf_per_site.py`) before production runs.
