# Operator Playbook

Use this checklist for supervised collection and parser runs. Detailed commands live in
[Operations](OPERATIONS.md); this page focuses on decisions and evidence.

## Before a Run

- Confirm the exact seed directory or worklist. Do not assume all tracked sitemaps are an
  appropriate batch.
- Run `make doctor` and inspect local status with `make site-status`.
- Check disk, network egress, browser/OCR services, and artifact paths.
- Review lifecycle state, source terms, `robots.txt`, and recent WAF history.
- Choose a run ID and record the Offprint commit.
- Start with one target and low concurrency after any routing or adapter change.

## During a Run

- Watch successful PDF progress, per-domain failures, retry growth, and time since last
  success.
- Treat repeated WAF, authentication, or paywall signals as stop conditions.
- Keep run manifests and errors even when the run is unsuccessful.
- Use `Ctrl+C` for an orderly interruption and resume the same run ID.
- Do not launch overlapping bepress batches from one IP.

## Decision Table

| Observation | Decision |
|---|---|
| Healthy progress with bounded errors | Continue and recheck at a defined interval |
| One host stalls while others progress | Isolate the target; preserve the main run |
| WAF failures spread across hosts | Stop bepress traffic and begin cooldown |
| Browser requires manual authentication | Defer unless access and automation are authorized |
| Candidate URLs are non-articles | Stop downloads and correct discovery/QC with fixtures |
| Duplicate bytes or URLs rise | Pause promotion and inspect canonicalization/dedup records |
| OCR service is unstable | Stop OCR shards; retain the native outputs and review queue |
| Disk approaches reserve threshold | Interrupt cleanly and relocate/clear only known artifacts |

## Resume and Recovery

```bash
make production-resume RUN_ID=<run_id>
make production-retry RUN_ID=<run_id>
```

Resume preserves the run contract. Retry should target structured retryable errors, not
replay the full source universe. If configuration must materially change, document why the
new attempt is a separate run.

## After a Run

```bash
make site-status
python scripts/reporting/metadata_quality_report.py --warn-only
```

Inspect `manifest.json`, `records.jsonl`, `errors.jsonl`, and `stats.json`. Record:

- commit and exact command;
- run ID and target scope;
- successful, failed, skipped, and retry counts;
- affected hosts and lifecycle changes;
- output and log locations; and
- follow-up needed before promotion or parsing.

Promote only after accounting is coherent:

```bash
make promote-run RUN_ID=<run_id>
```

## Parser Operations

Separate document qualification, native extraction, OCR routing, and quality evaluation.
When comparing methods, use the same document set and report empty, excluded, invalid, and
successful outputs separately. See [Footnote corpus audit](FOOTNOTE_FULL_CORPUS_AUDIT.md).
