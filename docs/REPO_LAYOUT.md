# Repository Layout

```text
offprint/
├── offprint/             Python package, adapters, parsers, and sitemap seeds
│   ├── adapters/         Platform and host-specific acquisition logic
│   ├── coverage_tools/   Completeness and sequence checks
│   ├── pdf_footnotes/    Document policy, text, citation, and note extraction
│   └── sitemaps/         One tracked JSON configuration per crawl target
├── data/registry/        Public journal gazetteer and versioned upstream snapshots
├── scripts/              Workflow-oriented command-line entry points
│   ├── onboarding/       Fingerprint and bootstrap one site
│   ├── pipeline/         Collect, smoke-test, resume, and promote runs
│   ├── processing/       QC, metadata, text, footnotes, OCR, and exports
│   ├── quality/          Evaluation, policy, doctor, and repository checks
│   ├── reporting/        Gazetteer and local corpus/run tables
│   └── research/         Reproducible analysis and benchmark drivers
├── tests/                Unit, contract, regression, and fixture tests
├── docs/                 Maintained architecture, contributor, and operations guides
├── config/               Versioned runtime configuration
├── references/           Small tracked research/reference inputs
├── artifacts/            Local runtime products; gitignored
├── pyproject.toml         Package metadata, dependencies, and tool configuration
└── Makefile               Canonical contributor and operator commands
```

## Placement Rules

| Item | Location | Tracked? |
|---|---|---:|
| Reusable Python behavior | `offprint/` | Yes |
| User/operator CLI | Appropriate `scripts/<workflow>/` directory | Yes |
| Journal crawl configuration | `offprint/sitemaps/` | Yes |
| Public reference/gazetteer input | `data/registry/` | Yes |
| Deterministic generated documentation | `docs/generated/` | Yes |
| Tests and small fixtures | `tests/` | Yes |
| Run records, PDFs, caches, parse outputs | `artifacts/` | No |
| Temporary investigation output | `tmp/` or outside the repository | No |

Historical scripts that no longer define supported entry points live in `scripts/archive/`.
They may explain past outputs but should not be linked as current onboarding commands.

The layout gate is `make repo-layout-check`. Data and artifact rules are detailed in
[Data and release policy](DATA_AND_RELEASE_POLICY.md).
