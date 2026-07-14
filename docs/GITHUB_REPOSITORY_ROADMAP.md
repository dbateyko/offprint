# GitHub Repository Roadmap

## Purpose

Make Offprint understandable and runnable by a new contributor without prior knowledge of
the local corpus workspace. The repository should explain, in order:

1. what Offprint collects and produces;
2. how a journal moves from the gazetteer to a parsed research artifact;
3. which command is appropriate for a small local trial;
4. where to inspect coverage and pipeline readiness; and
5. how to make and validate a contribution.

This roadmap treats the public `offprint` repository as the canonical home of scraper,
parser, journal-gazetteer, and public reporting code. Private corpus holdings, cross-source
inventories, and research-specific labels remain in the separate data-operations workspace.

## Design Principles

- **One public front door.** The root README provides orientation and a safe first run. It
  links to detailed runbooks instead of accumulating production history.
- **Task-oriented navigation.** Documentation is organized around finding journals,
  collecting PDFs, parsing documents, assessing quality, and contributing code.
- **Generated facts, authored explanations.** Counts and status tables are generated from
  versioned registry and sitemap inputs. Interpretation and policy remain authored prose.
- **Small-run defaults.** Onboarding commands use a temporary output directory and one
  journal. Corpus-scale and GPU workflows live in operations documentation.
- **Explicit data boundaries.** Versioned metadata is public; downloaded PDFs and derived
  corpus artifacts are local and subject to source rights.
- **Checkable documentation.** Local Markdown links, generated snapshot freshness, package
  imports, tests, and linting are automated quality gates.

## Information Architecture

### Root README

The README should answer the first questions in under two screens:

- What is Offprint?
- What stages are implemented?
- What can I run without private data or a GPU?
- What is in the journal gazetteer now?
- Where do I go for my task?

It should contain a compact pipeline diagram, a capabilities table, safe installation and
trial commands, a generated coverage excerpt, and a documentation map. Detailed benchmark
notes and production incidents should live in dedicated documents.

### Documentation Hub

The documentation index should expose five paths:

| Path | Reader goal | Canonical document |
|---|---|---|
| Understand | Learn boundaries, stages, and artifacts | `ARCHITECTURE.md` |
| Inspect | Read the gazetteer schema and coverage tables | `GAZETTEER.md` |
| Run | Execute and recover collection or parsing jobs | `OPERATIONS.md` |
| Extend | Add a journal, adapter, parser rule, or test | `CONTRIBUTOR_START_HERE.md` |
| Govern | Understand data, quality, and release policy | `DATA_AND_RELEASE_POLICY.md` |

### Generated Gazetteer Snapshot

A standard-library reporting command should read only tracked inputs and emit a deterministic
Markdown snapshot. At minimum it should report:

- journal rows and unique hosts;
- registry status counts;
- platform counts;
- provenance-source counts;
- sitemap lifecycle counts; and
- fields required to interpret whether a journal is known, runnable, or landed locally.

The generated file is committed so GitHub readers can inspect it without running code. CI
rebuilds it and fails if the committed snapshot is stale.

## Delivery Plan

### Phase 1: Repository Contract and Navigation

- Rewrite the root README around public package capabilities.
- Replace machine-specific and incident-oriented content with links to runbooks.
- Refresh the documentation index and architecture diagram.
- Document the public/private repository boundary and artifact policy.

**Exit criterion:** a new reader can identify the correct entry point for registry work,
scraping, parsing, reporting, and contribution from the README alone.

### Phase 2: Gazetteer and Inventory Visibility

- Document `data/registry/lawjournals.csv` as the public journal gazetteer.
- Define the difference between registry status, sitemap status, adapter support, successful
  downloads, and private corpus inclusion.
- Add the deterministic Markdown snapshot generator and generated tables.

**Exit criterion:** every displayed count has a tracked source and a regeneration command.

### Phase 3: Reproducible Onboarding

- Provide minimal and full development installation paths.
- Add a doctor command that checks Python, package imports, registry files, sitemap validity,
  writable artifact paths, and optional parser dependencies.
- Align script examples and contribution instructions with current paths.

**Exit criterion:** a clean checkout can run the doctor, inspect the gazetteer, and execute
  unit tests without access to the private corpus.

### Phase 4: Automated Guardrails

- Check local documentation links.
- Check generated gazetteer freshness.
- Correct CI commands to use canonical script locations.
- Keep expensive OCR, browser, and full-corpus jobs outside default CI.

**Exit criterion:** pull requests cannot silently break the documentation map or publish
stale coverage tables.

## Non-Goals

- Publishing or moving downloaded PDFs into Git.
- Combining the private data-operations repository with the public package.
- Redesigning adapter or footnote-extraction algorithms in this pass.
- Claiming that a known registry row has been successfully scraped.
- Presenting local corpus totals as reproducible public-package metrics.

## Success Measures

- README quickstart succeeds from a clean environment using tracked inputs.
- All README and documentation-index links resolve.
- Gazetteer snapshot regeneration produces no diff.
- The doctor distinguishes required failures from optional capability warnings.
- CI exercises documentation, registry, import, lint, and unit-test gates.
- Operations history no longer obscures first-time setup.

## Follow-On Work

After the repository surface is stable:

1. pay down the existing repository-wide Ruff and Black baseline, then expand the lint gate;
2. reconcile sitemap lifecycle semantics with the explicit-adapter policy and then restore the
   adapter audit as a blocking gate;
3. publish versioned gazetteer releases with machine-readable checksums;
4. add a lightweight static coverage site generated from the same reporting module;
5. expose normalized article records through a documented export schema;
6. add per-platform fixture-based adapter contract tests; and
7. report parser quality by document era, scan status, and journal platform rather than only
   as an aggregate.
