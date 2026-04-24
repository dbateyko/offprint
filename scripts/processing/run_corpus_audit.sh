#!/usr/bin/env bash
# Full-corpus liteparse-only audit pass. See docs/full_corpus_audit_roadmap.md.
#
# Produces one sidecar per PDF next to each source file, then the post-run
# aggregator turns those into three manifests:
#   all_pdfs_manifest.jsonl, liteparse_results.jsonl, ocr_backlog.jsonl
#
# Run inside screen -S corpus_audit.
# This pass does NOT use OCR. vLLM should NOT be running.
set -euo pipefail

PROJECT_ROOT="/mnt/shared_storage/law-review-corpus/offprint"
PDF_ROOT="/mnt/shared_storage/law-review-corpus/corpus/scraped"
VERSION="0.3.0"                       # bump if code changes mid-effort
OUT_DIR="${PROJECT_ROOT}/artifacts/runs/corpus_audit_${VERSION}"
LOG_DIR="${PROJECT_ROOT}/artifacts/logs"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_FILE="${LOG_DIR}/corpus_audit_${VERSION}_${TIMESTAMP}.log"
REPORT_OUT="${OUT_DIR}/extraction_report.json"

# First-pass sets overwrite=true to replace stale sidecars from prior experiments.
# On resume after a crash, pass OVERWRITE=false so already-done PDFs are skipped.
OVERWRITE="${OVERWRITE:-true}"
WORKERS="${WORKERS:-16}"
# Classifier opens each PDF's first page (pypdf). A few malformed PDFs hang
# pypdf for tens of seconds; with too few workers, one bad PDF blocks overall
# progress. 24 workers keeps throughput above ~10 pdf/s even when 2-3 threads
# are stuck on slow PDFs.
CLASSIFIER_WORKERS="${CLASSIFIER_WORKERS:-24}"

mkdir -p "$OUT_DIR" "$LOG_DIR"
cd "$PROJECT_ROOT"

echo "[$(date -Iseconds)] Starting liteparse-only corpus audit"
echo "[$(date -Iseconds)] Version: $VERSION"
echo "[$(date -Iseconds)] PDF root: $PDF_ROOT"
echo "[$(date -Iseconds)] Output: $OUT_DIR"
echo "[$(date -Iseconds)] Overwrite: $OVERWRITE"
echo "[$(date -Iseconds)] Workers: $WORKERS"
echo "[$(date -Iseconds)] Log: $LOG_FILE"

# Pre-flight: confirm no vLLM server is hogging GPU (belt-and-suspenders —
# we're not using OCR, but if a vLLM leak is consuming RAM the workers suffer).
if pgrep -f "vllm serve" >/dev/null 2>&1; then
    echo "[$(date -Iseconds)] NOTE: vllm serve processes detected. OCR is not used in this run but GPU memory may be held."
fi

exec > >(tee -a "$LOG_FILE") 2>&1

.venv/bin/python scripts/processing/extract_footnotes.py \
    --pdf-root "$PDF_ROOT" \
    --workers "$WORKERS" \
    --classifier-workers "$CLASSIFIER_WORKERS" \
    --doc-policy all \
    --text-parser-mode footnote_optimized \
    --ocr-mode off \
    --overwrite "$OVERWRITE" \
    --skip-classification \
    --shuffle \
    --shuffle-seed 42 \
    --heartbeat-every 100 \
    --report-out "$REPORT_OUT"

echo "[$(date -Iseconds)] Extraction complete. Aggregating manifests..."

.venv/bin/python scripts/research/aggregate_corpus_audit.py \
    --pdf-root "$PDF_ROOT" \
    --out-dir "$OUT_DIR" \
    --extractor-version "$VERSION"

echo "[$(date -Iseconds)] Corpus audit complete."
echo "[$(date -Iseconds)] Manifests in: $OUT_DIR"
