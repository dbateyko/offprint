# Project Overview

## Problem

Law-journal publishing is distributed across institutional repositories, journal-managed
sites, commercial platforms, and legacy archives. A research corpus built from those sources
needs more than a downloader: it needs a journal universe, reproducible target configuration,
site-aware discovery, provenance, document qualification, parsing, and honest failure
accounting.

Offprint packages those responsibilities into one public repository.

## Scope

Offprint supports:

- a versioned, provenance-aware journal gazetteer;
- one crawl configuration per journal target;
- platform and host-specific discovery/download adapters;
- polite, cached, resumable acquisition with structured run evidence;
- high-precision article/document policy;
- native and optional OCR text/footnote extraction;
- quality fixtures, gold scoring, and corpus audits; and
- machine-readable and GitHub-readable reporting.

It does not publish downloaded PDFs, guarantee exhaustive coverage of every known journal,
or treat technical access as permission to redistribute a work.

## Research Design

The data model keeps distinct units separate:

```text
known journal
  -> configured crawl target
  -> routed target
  -> discovered candidate URL
  -> validated local PDF
  -> article-qualified document
  -> parsed document
  -> quality-gated research record
```

This staging matters for descriptive scholarship. A high registry count can coexist with a
smaller configured universe; successful downloads can include frontmatter; parsed outputs can
have era- or scan-dependent missingness. Claims should state the stage and denominator they
use.

## Public and Private Work

The public package contains reusable acquisition/parsing code and public reference metadata.
A separate private data-operations workspace may join Offprint outputs with donated or
licensed corpora, research labels, and analysis-specific tables. That downstream workspace is
optional and must not become a hidden dependency of the public package.

## Where to Go Next

- [Architecture](ARCHITECTURE.md) for components and artifact contracts.
- [Gazetteer and coverage](GAZETTEER.md) for counts and status interpretation.
- [Contributor start](CONTRIBUTOR_START_HERE.md) for a clean-checkout workflow.
- [Operations](OPERATIONS.md) for collection and parsing commands.
- [Data and release policy](DATA_AND_RELEASE_POLICY.md) for repository boundaries.
