# Contributing

Thanks for contributing to Law Review Scrapers.

## Project Focus
This repository prioritizes production scraping reliability:
- accurate PDF discovery/download,
- metadata quality,
- completeness validation,
- resilient operations (retry/resume/auditability).

## Development Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .[dev] responses playwright
playwright install chromium
```

## Required Checks
```bash
ruff check .
black --check .
pytest -q
make adapter-policy-check
```

## Monitored Runtime Workflow
- Use `make production-monitored` for headed, operator-supervised runs (manual captcha/auth handoff).
- Pause with `Ctrl+C` and resume with `make production-monitored-resume RUN_ID=<RUN_ID>`.
- Prefer resume over fresh reruns to preserve incremental progress and avoid duplicate load.

## Code Guidelines
- Python 3.8+ with type hints.
- Prefer small composable functions and explicit failure handling.
- Keep adapter logic deterministic and traceable.
- Preserve compatibility contracts unless intentionally versioned.

## Adding a New Journal (Easiest Contribution)

The fastest way to contribute is to onboard a journal that isn't covered yet:

- **With Claude Code**: Run `/onboard-journal <url>` — it handles everything. See `docs/skills/README.md` for setup.
- **With Python scripts**: Run `python scripts/auto_onboard_site.py --url <url>`, then smoke test and submit a PR.

## Adapter Workflow
1. Add/modify adapter in `offprint/adapters/`.
2. Register routing in `offprint/adapters/registry.py` when needed.
3. Add/update sitemap seed in `sitemaps/` with required status metadata:
   - `metadata.status` (`active`, `todo_adapter`, `paused_404`, `paused_waf`, `paused_login`, `paused_paywall`, `paused_other`)
   - `metadata.status_reason`
   - `metadata.status_updated_at` (ISO UTC)
   - `metadata.status_evidence_ref`
4. For unresolved/deferred hosts, set `metadata.status=todo_adapter` instead of relying on runtime generic fallback.
5. Add tests in `tests/`.
6. Validate with targeted smoke run when touching discovery/download paths.

### Shared Base Adapter Policy (Required)
- Do **not** make direct changes to shared base adapters (for example `WordPressAcademicBaseAdapter`, `DigitalCommonsIssueArticleHopAdapter`, `OJSAdapter`, `DrupalAdapter`) to fix a single site.
- Preferred order:
  1. site-level tuning in sitemap metadata,
  2. host-specific adapter,
  3. shared-base change only if necessary.
- If a shared-base change is unavoidable, keep it domain-guarded where possible and include regression tests for both:
  - the target host behavior, and
  - nearby existing-family behavior.

## Pull Request Expectations
- Use focused, reviewable PRs.
- Describe what changed, why, and expected operational impact.
- Include relevant run/report evidence when behavior changes.
- Do not include generated runtime artifacts (`pdfs/`, `runs/`, `cache/http/`).

## Security and Compliance
- Never commit credentials or tokens.
- Respect publisher terms and `robots.txt`.
- Keep request rates polite and honor backoff signals.
