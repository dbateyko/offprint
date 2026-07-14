# Script Catalog

Maintained commands are grouped by workflow. Run scripts from the repository root so imports
and default paths resolve consistently. Commands that need only tracked metadata are marked
**tracked-only**; the others require local artifacts, network access, or optional dependencies.

## Repository and Gazetteer

| Command | Purpose | Inputs |
|---|---|---|
| `python scripts/quality/doctor.py` | Check checkout, registry, sitemap JSON, imports, and writable outputs | tracked-only |
| `python scripts/reporting/gazetteer_report.py` | Generate the committed registry/sitemap Markdown tables | tracked-only |
| `python scripts/reporting/holdings_report.py` | Generate local per-journal counts and article-level CSV from run records | local artifacts |
| `python scripts/quality/check_markdown_links.py` | Validate local links in maintained docs | tracked-only |
| `python scripts/quality/check_repo_layout.py --repo-root .` | Enforce repository placement policy | tracked-only |
| `python scripts/quality/check_no_generic_active_seeds.py --sitemaps-dir offprint/sitemaps` | Enforce adapter-routing policy | tracked-only |

## Journal Onboarding

| Command | Purpose |
|---|---|
| `python scripts/onboarding/fingerprint_site.py <url>` | Identify platform and structural signals for a site |
| `python scripts/onboarding/auto_onboard_site.py <url> --dry-run --smoke-test` | Draft a sitemap and registry route for one journal |

Use [the onboarding guide](../docs/skills/onboard-journal.md) for evidence and promotion
requirements.

## Acquisition Pipeline

| Command | Purpose |
|---|---|
| `python scripts/pipeline/run_pipeline.py --mode full ...` | Canonical full, delta, retry, and resume runner |
| `python scripts/pipeline/smoke_one_pdf_per_site.py ...` | Bounded validation across selected sitemap targets |
| `python scripts/pipeline/launch_from_worklist.py ...` | Launch targets from an explicit worklist |
| `python scripts/pipeline/scrape_drainer.py ...` | Drain a queued target set with progress accounting |
| `python scripts/pipeline/promote_run.py ...` | Promote a completed run to the golden baseline |
| `python scripts/pipeline/promote_pdfs.py ...` | Promote validated PDFs between artifact lanes |

The Makefile wraps common production and recovery forms. Start with `make help` and read
[Operations](../docs/OPERATIONS.md) before a corpus-scale run.

## Document Processing

| Command | Purpose |
|---|---|
| `python scripts/processing/qc_quarantine_pdfs.py ...` | Apply high-precision non-article quarantine rules |
| `python scripts/processing/extract_pdf_metadata.py ...` | Produce or enrich document metadata |
| `python scripts/processing/extract_text_jsonl.py ...` | Extract article text records |
| `python scripts/processing/extract_footnotes.py ...` | Extract ordered notes with native/OCR routing |
| `python scripts/processing/extract_footnotes_from_docling.py ...` | Run the Docling-specific extraction path |
| `python scripts/processing/extract_footnotes_from_ocr.py ...` | Parse footnotes from prior OCR output |
| `python scripts/processing/ocr_pdfs.py ...` | Generate OCR artifacts for selected PDFs |
| `python scripts/processing/run_olmocr_dual_gpu.py ...` | Shard OlmOCR extraction over two endpoints |
| `python scripts/processing/split_issue_compilation_pdfs.py ...` | Split issue-level compilations into article candidates |
| `python scripts/processing/build_hf_dataset.py ...` | Build a Hugging Face-oriented derived export |
| `python scripts/export_footnote_dataset.py ...` | Export footnote records with provenance |

Install `.[pdf_footnotes]` before using parsing commands. GPU/OCR commands require additional
runtime services described in [Operations](../docs/OPERATIONS.md).

## Quality and Evaluation

| Command | Purpose |
|---|---|
| `python scripts/quality/sample_corpus.py ...` | Draw a reproducible evaluation sample |
| `python scripts/quality/diagnose_footnote_corpus.py ...` | Profile extraction and document failure modes |
| `python scripts/quality/evaluate_footnotes.py ...` | Score extraction against a gold set |
| `python scripts/quality/build_html_gold.py ...` | Build HTML/PDF comparison gold data |
| `python scripts/quality/discover_html_gold_pairs.py ...` | Discover candidate HTML/PDF evaluation pairs |
| `python scripts/quality/score_note_text.py ...` | Score note-text fidelity for paired documents |
| `python scripts/quality/check_reextract_progress.py ...` | Check shard progress and completion accounting |

## Reporting and Research

| Command | Purpose |
|---|---|
| `python scripts/reporting/site_status_report.py --summary` | Join sitemaps, routes, local PDFs, and recent run state |
| `python scripts/reporting/holdings_report.py` | Inventory deduplicated downloaded records by journal and article metadata |
| `python scripts/reporting/metadata_quality_report.py ...` | Report title/author/volume/date coverage by domain |
| `python scripts/research/validate_journal_recon.py ...` | Validate tracked publication dossiers, evidence references, route graphs, and ready-state gates |
| `python scripts/research/prioritize_journal_recon.py ...` | Rank registry-defined journal identities from explainable importance, normalized coverage, provenance, and worklist signals |
| `python scripts/research/aggregate_corpus_audit.py ...` | Aggregate document-audit shards |
| `python scripts/research/bench_holdout_1k.py ...` | Evaluate parser behavior on a holdout sample |
| `python scripts/research/end_to_end_1k.py ...` | Run the end-to-end research benchmark |
| `python scripts/research/solver_*.py ...` | Develop and evaluate note-sequence solving |
| `python scripts/research/dedupe_articles.py ...` | Analyze duplicate article records |

`scripts/archive/` contains superseded entry points retained for provenance. Do not use them
in new automation or onboarding documentation.
