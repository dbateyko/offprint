# Contributor Start Here

Fast onboarding path for new contributors.

## 1) Understand Runtime Shape (10 min)
1. Read [`ARCHITECTURE.md`](ARCHITECTURE.md)
2. Read [`REPO_LAYOUT.md`](REPO_LAYOUT.md)
3. Skim adapter routing in [`../offprint/adapters/registry.py`](../offprint/adapters/registry.py)

## 2) Understand Operations (15 min)
1. Read [`OPERATIONS.md`](OPERATIONS.md)
2. Skim [`OPERATOR_PLAYBOOK.md`](OPERATOR_PLAYBOOK.md) for run lifecycle and resume conventions
3. Review canonical wrappers in [`../Makefile`](../Makefile)

## 3) Understand Adapter Rules (10 min)
1. Read [`ADAPTER_DEVELOPMENT.md`](ADAPTER_DEVELOPMENT.md)
2. Read shared-base safety policy in [`../CONTRIBUTING.md`](../CONTRIBUTING.md)

## 4) Use the Right Script for the Job
Use [`../scripts/README.md`](../scripts/README.md), especially the workflow index table.

Common first actions:
- Coverage/status view: `python scripts/site_status_report.py --summary`
- Preflight checks: `python scripts/run_preflight.py --sitemaps-dir sitemaps`
- Production: `make production`
- Resume: `make production-resume RUN_ID=<RUN_ID>`
- Smoke validation: `python scripts/smoke_one_pdf_per_site.py`

## 5) Contributor Baseline Checks
Run before opening a PR:
```bash
ruff check .
black --check .
make repo-layout-check
pytest -q
make adapter-policy-check
```

## 6) Quick Triage Flow
1. Confirm seed quality/status metadata in `sitemaps/*.json`
2. Fingerprint candidate hosts: `python scripts/fingerprint_site.py <url>`
3. For TODO backlog triage, use structure/evidence probes before broad smoke
4. Only promote `todo_adapter -> active` after adapter mapping + smoke evidence

## Single Source of Truth Notes
- Runtime policy, including non-DC-first posture: [`OPERATIONS.md`](OPERATIONS.md)
- Historical snapshots and long-form run notes: [`attic/README_legacy_20260316.md`](attic/README_legacy_20260316.md)
