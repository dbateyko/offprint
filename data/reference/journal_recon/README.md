# Journal reconnaissance dossiers

This directory holds evidence-backed, publication-scoped descriptions of journal websites.
A dossier records how a human or future adapter can enumerate a journal and extract metadata;
it does not authorize or launch an acquisition run.

Use one `<journal-id>.json` file per publication. The ID should be stable, lowercase, and
hyphenated. Start from [example-law-review.json](example-law-review.json), which is deliberately
fictitious, and validate against [schema.json](schema.json). Do not copy the example URLs into a
real dossier.

## Reconnaissance contract

Reconnaissance is bounded and read-only with respect to the target site. Record observations
from the oldest, a middle, and the newest available issue, plus at least three representative
article pages when access permits. Check `robots.txt`, use publication-scoped entry points, and
do not place cookies, access tokens, credentials, or copied page bodies in a dossier.

Each dossier captures:

- canonical identity, aliases, registry references, platform, and publication path scope;
- durable evidence records with observation times and optional local artifact references;
- an archive/volume/issue/article/file route graph, URL patterns, pagination termination, and
  historical layout eras;
- provenance for title, authors, abstract, year, date, volume, issue, pages, DOI, and citation,
  including extraction method, locator, normalization, confidence, and missingness;
- direct, redirected, embedded, viewer, API, or tokenized file delivery and its session needs;
- per-host robots decisions and access barriers, scope exclusions, and an explicit implementation
  verdict with a recommended adapter.

Evidence IDs are local to a dossier. Every `evidence_ids` reference and route-graph endpoint must
resolve. A metadata field marked `always` or `sometimes` must name at least one observed source.
The validator enforces these relationships in addition to the JSON Schema structure.

File-delivery requirements are tri-state: use `required` or `not_required` only when the response
path proves the condition, and use `unknown` when a block or incomplete probe prevents a reliable
conclusion. Record WAF or challenge behavior separately under `access.barriers`; it is not itself
proof that JavaScript or cookies are an intended file-delivery requirement.

## Verdicts

Use `in_progress` while evidence is incomplete. Final statuses are:

- `ready`: routes and metadata are sufficiently evidenced for sitemap/adapter implementation;
- `metadata_only`: metadata can be enumerated, but files cannot currently be acquired;
- `blocked_waf`, `blocked_login`, or `blocked_paywall`: access is the limiting factor;
- `stale_route`: known routes have moved or no longer resolve;
- `needs_headless`: the bounded static probe is insufficient and browser reconnaissance is next.

For `ready`, the validator requires the full issue-era sampling policy, three article pages, no
declared blockers, no blocking access barrier, and completed PDF response checks. A ready dossier
is still research evidence: it must be translated into a sitemap or adapter and pass the normal
onboarding smoke gate before production use.

## Validation

From the repository root, validate every dossier in this directory:

```bash
python scripts/research/validate_journal_recon.py
```

Validate selected files or a work-in-progress file elsewhere:

```bash
python scripts/research/validate_journal_recon.py \
  data/reference/journal_recon/example-law-review.json \
  /tmp/draft-journal.json
```

The command uses only the Python standard library. It exits `0` when all dossiers pass, `1` for
document validation failures, and `2` when the schema or an input cannot be read.
