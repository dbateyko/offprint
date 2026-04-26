#!/usr/bin/env bash
# Liteparse-only footnote extraction on the 1K sample. No OCR, no vLLM.
# Designed to run inside `screen -dmS footnotes_1k_liteparse`.
set -euo pipefail

cd /mnt/shared_storage/law-review-corpus/offprint
VENV=/mnt/shared_storage/law-review-corpus/offprint/.venv
PY=$VENV/bin/python
PDF_ROOT=artifacts/samples/sample_1k_pdfs
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EXTRACT_LOG="artifacts/runs/run_1k_liteparse_${TIMESTAMP}.log"
REPORT="artifacts/runs/run_1k_liteparse_${TIMESTAMP}_report.json"

run_extract() {
  echo "[$(date +%F_%T)] Starting liteparse-only extraction on 1K sample"
  "$PY" scripts/processing/extract_footnotes.py \
    --pdf-root "$PDF_ROOT" \
    --workers 12 \
    --classifier-workers 12 \
    --doc-policy all \
    --text-parser-mode footnote_optimized \
    --ocr-mode off \
    --no-text-cache \
    --overwrite true \
    --skip-classification \
    --shuffle \
    --shuffle-seed 42 \
    --heartbeat-every 100 \
    --report-out "$REPORT" 2>&1 | tee "$EXTRACT_LOG"
}

run_diagnostic() {
  echo "[$(date +%F_%T)] Running diagnostic"
  "$PY" scripts/quality/diagnose_footnote_corpus.py \
    --pdf-root "$PDF_ROOT" \
    --report-out "artifacts/runs/run_1k_liteparse_${TIMESTAMP}_diag.json" \
    --failures-out "artifacts/runs/run_1k_liteparse_${TIMESTAMP}_failures.csv"
}

main() {
  run_extract
  run_diagnostic
  echo "[$(date +%F_%T)] DONE; log: $EXTRACT_LOG"
}

main "$@"
