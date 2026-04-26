#!/usr/bin/env bash
# Run a persistent vLLM (OlmOCR) server + the 1K footnote extraction with OCR fallback.
# Designed to run inside `screen -dmS footnotes_1k`.
# Two panes: window 0 = vLLM, window 1 = extraction.
set -euo pipefail

cd /mnt/shared_storage/law-review-corpus/offprint
VENV=/mnt/shared_storage/law-review-corpus/offprint/.venv
PY=$VENV/bin/python
VLLM=/home/dbateyko/.local/bin/vllm
VLLM_LOG=/tmp/vllm_olmocr.log
EXTRACT_LOG=artifacts/runs/run_1k_ocr_fix_v2.log
REPORT=artifacts/runs/run_1k_ocr_fix_v2_report.json

start_vllm() {
  echo "[$(date +%F_%T)] Launching vLLM"
  CUDA_VISIBLE_DEVICES=0 "$VLLM" serve allenai/olmOCR-2-7B-1025-FP8 \
    --port 30024 --host 127.0.0.1 \
    --served-model-name olmocr \
    --tensor-parallel-size 1 \
    --gpu-memory-utilization 0.85 \
    --max-model-len 8192 > "$VLLM_LOG" 2>&1 &
  VLLM_PID=$!
  echo "vLLM pid=$VLLM_PID"
  # Wait for readiness
  for i in $(seq 1 120); do
    if curl -sfm 2 http://127.0.0.1:30024/health >/dev/null 2>&1; then
      echo "[$(date +%F_%T)] vLLM ready"
      return 0
    fi
    if ! kill -0 "$VLLM_PID" 2>/dev/null; then
      echo "[$(date +%F_%T)] vLLM died during startup; see $VLLM_LOG"
      tail -20 "$VLLM_LOG"
      return 1
    fi
    sleep 5
  done
  echo "[$(date +%F_%T)] vLLM did not become ready within 10min"
  return 1
}

run_extract() {
  echo "[$(date +%F_%T)] Starting extraction on 1K sample"
  OLMOCR_SERVER_URL=http://127.0.0.1:30024/v1 \
  OLMOCR_MODEL=olmocr \
  OLMOCR_TIMEOUT_SECONDS=1800 \
  "$PY" scripts/processing/extract_footnotes.py \
    --pdf-root artifacts/samples/sample_1k_pdfs \
    --ocr-mode fallback \
    --ocr-backend olmocr \
    --workers 12 \
    --classifier-workers 6 \
    --ocr-workers 4 \
    --respect-qc-exclusions false \
    --no-text-cache \
    --overwrite true \
    --heartbeat-every 100 \
    --report-out "$REPORT" 2>&1 | tee "$EXTRACT_LOG"
}

run_diagnostic() {
  echo "[$(date +%F_%T)] Running diagnostic"
  "$PY" scripts/quality/diagnose_footnote_corpus.py \
    --pdf-root artifacts/samples/sample_1k_pdfs \
    --report-out artifacts/runs/run_1k_ocr_fix_v2_diag.json \
    --failures-out artifacts/runs/run_1k_ocr_fix_v2_failures.csv
}

main() {
  if ! curl -sfm 2 http://127.0.0.1:30024/health >/dev/null 2>&1; then
    start_vllm || { echo "vLLM start failed; aborting"; exit 1; }
  else
    echo "[$(date +%F_%T)] vLLM already running"
  fi
  run_extract
  run_diagnostic
  echo "[$(date +%F_%T)] DONE; vLLM still running (kill manually if desired)"
}

main "$@"
