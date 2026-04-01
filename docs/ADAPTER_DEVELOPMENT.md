# Adapter Development

## Goal
Adapters should maximize reliable PDF discovery and metadata completeness while preserving operational observability.

## When to Add a New Adapter
Add or customize an adapter when:
- platform-specific selectors differ from existing base adapters,
- discovery on the generic path is incomplete,
- repeated failures are isolated to one site family.

Prefer existing base adapters first:
- `DigitalCommonsBaseAdapter`
- `OJSAdapter`
- `WordPressAcademicBaseAdapter`
- `ScholasticaBaseAdapter`

`GenericAdapter` is reserved for diagnostics/triage and is not used in runtime scrape flows.

## Sitemap Status Lifecycle
For each sitemap entry, set `metadata.status` explicitly:
- `active`: adapter is mapped and verified for runtime.
- `todo_adapter`: host tracked but not scraped until adapter coverage is added.
- `paused_*`: host temporarily deregistered from runtime due to access or quality constraints.

Required metadata fields:
- `metadata.status_reason`
- `metadata.status_updated_at`
- `metadata.status_evidence_ref`

Lifecycle: `todo_adapter -> active` only after adapter mapping plus smoke validation.

## Implementation Checklist
1. Create or modify adapter under `offprint/adapters/`.
2. Implement discovery in `discover_pdfs(...)` returning `DiscoveryResult` items.
3. Reuse shared download logic unless a site requires custom handling.
4. Ensure provenance-friendly behavior:
   - set clear `metadata` fields,
   - keep page and PDF URLs canonical,
   - allow download meta to surface meaningful failures.
5. Register routing in `offprint/adapters/registry.py`.
6. Add tests in `tests/` for discovery and download semantics.

## Metadata Expectations
Populate as many of these as available:
- `title`
- `authors`
- `date` / `year`
- `volume`
- `issue`
- `citation`
- `pages`
- article landing `url`

## Testing Expectations
Run before PR:
```bash
ruff check .
black --check .
pytest -q
```

When touching discovery/download logic, also run a targeted smoke pass:
```bash
python scripts/smoke_one_pdf_per_site.py --target-file <targets.txt> --limit 10
```

## Operational Notes
- Keep selectors and parsing defensive against minor HTML drift.
- Respect polite crawling delays and avoid aggressive retries on hard blocks.
- Surface actionable failure metadata (`error_type`, status code, content type, WAF indicators).
