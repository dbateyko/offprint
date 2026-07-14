# Developer Workflow

## Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
make doctor
```

Install `.[pdf_footnotes]` only for parser work. Keep browser and GPU services outside the
base test loop.

## Daily Loop

1. Read the relevant architecture/runbook and nearby tests.
2. Reproduce the behavior with the smallest tracked fixture or bounded target.
3. Make a narrow change at the appropriate extension point.
4. Run the targeted test while iterating.
5. Run `make quality-check` and then `pytest -q` before review.
6. Inspect `git diff --check` and keep runtime artifacts out of the commit.

## Quality Commands

```bash
make adapter-policy-check
make repo-layout-check
make docs-check
make gazetteer-check
pytest -q
```

`make quality-check` applies Ruff and Black to the repository-facing reporting/check tooling
introduced under the current gate. The broader scraper/parser tree has legacy formatting and
lint debt; do not mass-format it inside a behavioral change. New or edited operational modules
should still be linted and formatted directly before review.

`make gazetteer` is a write operation; run it after changing registry or sitemap inputs and
commit the generated snapshot with the source change.

## Test Strategy

| Change | Minimum evidence |
|---|---|
| Gazetteer/reporting | Small CSV/JSON fixtures and deterministic output assertions |
| Sitemap only | Schema/policy checks plus dated bounded smoke evidence |
| Host adapter | Discovery fixture and routing assertion |
| Shared adapter | Target regression plus a neighboring-family regression |
| Orchestrator | Run-record/state tests without live network dependence |
| Parser | Real-layout or minimal reproducer fixture and denominator-aware evaluation |
| Documentation | `make docs-check` and verified executable command paths |

Live network checks supplement deterministic tests; they do not replace them.

## Paths

- Package behavior: `offprint/`
- Journal configurations: `offprint/sitemaps/`
- Public registry: `data/registry/`
- Workflow CLIs: `scripts/`
- Tests: `tests/`
- Local outputs: `artifacts/`

See [Repository layout](REPO_LAYOUT.md), [Contributor start](CONTRIBUTOR_START_HERE.md), and
[Contributing](../CONTRIBUTING.md).
