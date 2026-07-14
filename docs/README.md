# Documentation

Use this page as the task-oriented index for maintained Offprint documentation.

## Understand

| Document | What it answers |
|---|---|
| [Architecture](ARCHITECTURE.md) | How registry, seeds, adapters, run records, QC, and parsers fit together |
| [Inventory](INVENTORY.md) | Which journals are known and which PDF records are held locally |
| [Gazetteer and coverage](GAZETTEER.md) | What a journal row or status means and how coverage tables are generated |
| [Repository layout](REPO_LAYOUT.md) | Where code, tracked metadata, tests, and local artifacts belong |
| [Project overview](PROJECT_OVERVIEW.md) | Longer project history, goals, and operational context |

## Start and Contribute

| Document | What it answers |
|---|---|
| [Contributor start](CONTRIBUTOR_START_HERE.md) | The shortest path from clone to first validated change |
| [Developer workflow](DEVELOPER_WORKFLOW.md) | Environment setup and daily quality gates |
| [Contributing](../CONTRIBUTING.md) | Review expectations, adapter safety, and data rules |
| [Adapter development](ADAPTER_DEVELOPMENT.md) | How to add or change discovery logic safely |
| [Journal onboarding](skills/onboard-journal.md) | Evidence required to add one journal seed |

## Run and Evaluate

| Document | What it answers |
|---|---|
| [Operations](OPERATIONS.md) | Canonical collection, resume, retry, and promotion commands |
| [Operator playbook](OPERATOR_PLAYBOOK.md) | Supervised run lifecycle and recovery decisions |
| [Script catalog](../scripts/README.md) | Which maintained CLI handles each workflow |
| [Footnote corpus audit](FOOTNOTE_FULL_CORPUS_AUDIT.md) | Parser-quality denominators, audit outputs, and OCR routing |

## Govern and Plan

| Document | What it answers |
|---|---|
| [Data and release policy](DATA_AND_RELEASE_POLICY.md) | What is tracked, local-only, or suitable for release |
| [GitHub repository roadmap](GITHUB_REPOSITORY_ROADMAP.md) | Design rationale, phases, and success measures for the repository surface |
| [Journal reconnaissance roadmap](JOURNAL_RECONNAISSANCE_ROADMAP.md) | How priority coverage gaps become evidenced dossiers, scraper changes, and bounded collection runs |
| [Journal reconnaissance Wave 1](JOURNAL_RECONNAISSANCE_WAVE_1.md) | Dated first-wave targets, evidence denominators, lanes, and completion gates |
| [Digital Commons file-access diagnostics](DIGITAL_COMMONS_FILE_ACCESS.md) | Evidence, bounded diagnostic mode, and stop policy for shared Bepress PDF 403 responses |
| [Journal reconnaissance priorities](generated/JOURNAL_RECON_PRIORITIES.md) | Generated, explainable ranking of registry-defined journals using normalized coverage and worklist signals |
| [Gazetteer snapshot](generated/GAZETTEER_SNAPSHOT.md) | Current generated status, platform, source, and completeness tables |
| [Journal catalog](generated/JOURNAL_CATALOG.md) | Searchable list of all known journals and crawl configurations |
| [Holdings snapshot](generated/HOLDINGS_BY_JOURNAL.md) | Operational PDF-record counts grouped by journal or collection |

Long session logs and superseded instructions are historical evidence, not current runbooks.
When a historical document conflicts with this index or the Makefile, use the maintained
document and executable command as authoritative.
