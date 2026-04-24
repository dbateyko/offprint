#!/usr/bin/env bash
# Overnight footnote extraction with dual-GPU vLLM OCR.
# Starts vLLM servers, runs extraction until 10am, then cleans up.
# Resumable: re-running skips PDFs that already have sidecars.
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────
PROJECT_ROOT="/mnt/shared_storage/Data_Science/offprint"
PDF_ROOT="/mnt/shared_storage/Data_Science/law-review-scrapers/artifacts/pdfs"
VENV_PYTHON="/usr/bin/python3"
VLLM_BIN="/home/dbateyko/.local/bin/vllm"
MODEL="allenai/olmOCR-2-7B-1025-FP8"
HOST="127.0.0.1"
PORT_A=8080
PORT_B=8081
GPU_MEM_UTIL=0.85
MAX_MODEL_LEN=16384
DEADLINE_HOUR=10              # stop at 10:00 local time
HEALTH_TIMEOUT=300            # max seconds to wait for vLLM startup
HEALTH_POLL_INTERVAL=5        # seconds between health checks
LOG_DIR="${PROJECT_ROOT}/artifacts/logs"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_FILE="${LOG_DIR}/overnight_footnotes_${TIMESTAMP}.log"
LOCK_FILE="${LOG_DIR}/.overnight_footnotes.lock"

# ── Lockfile ─────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "[$(date -Iseconds)] Another overnight footnotes run is active. Exiting."
    exit 0
fi

# ── Logging ──────────────────────────────────────────────────────────
exec > >(tee -a "$LOG_FILE") 2>&1
echo "[$(date -Iseconds)] Starting overnight footnote extraction"
echo "[$(date -Iseconds)] Log file: $LOG_FILE"

# ── Compute deadline ────────────────────────────────────────────────
now_epoch=$(date +%s)
deadline_today=$(date -d "today ${DEADLINE_HOUR}:00:00" +%s 2>/dev/null || date -d "${DEADLINE_HOUR}:00" +%s)
if [ "$now_epoch" -ge "$deadline_today" ]; then
    deadline_epoch=$(date -d "tomorrow ${DEADLINE_HOUR}:00:00" +%s 2>/dev/null || date -d "tomorrow ${DEADLINE_HOUR}:00" +%s)
else
    deadline_epoch="$deadline_today"
fi
echo "[$(date -Iseconds)] Deadline: $(date -d @${deadline_epoch} -Iseconds) ($(( deadline_epoch - now_epoch ))s from now)"

# ── PID tracking & cleanup ──────────────────────────────────────────
VLLM_PID_A=""
VLLM_PID_B=""
SERVERS_STARTED_BY_US=false

cleanup() {
    echo "[$(date -Iseconds)] Cleaning up..."
    if [ "$SERVERS_STARTED_BY_US" = true ]; then
        for pid in $VLLM_PID_A $VLLM_PID_B; do
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                echo "[$(date -Iseconds)] Stopping vLLM PID $pid"
                kill -TERM "$pid" 2>/dev/null || true
            fi
        done
        sleep 3
        for pid in $VLLM_PID_A $VLLM_PID_B; do
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        done
    fi
    echo "[$(date -Iseconds)] Cleanup complete."
}
trap cleanup EXIT INT TERM

# ── Health check helper ─────────────────────────────────────────────
check_health() {
    curl -sf "http://${HOST}:${1}/v1/models" >/dev/null 2>&1
}

# ── Start vLLM servers (if not already running) ─────────────────────
if check_health $PORT_A && check_health $PORT_B; then
    echo "[$(date -Iseconds)] Both vLLM endpoints already healthy -- reusing existing servers"
else
    # Kill stale vLLM processes that may be hogging GPU memory without
    # serving on the expected ports (e.g. leftover manual sessions).
    echo "[$(date -Iseconds)] Clearing stale vLLM processes to free GPU memory..."
    pkill -f "vllm serve" 2>/dev/null || true
    sleep 5
    # Force-kill EngineCore children that may linger after parent exits
    pkill -9 -f "VLLM::EngineCore" 2>/dev/null || true
    sleep 3
    echo "[$(date -Iseconds)] GPU memory after cleanup:"
    nvidia-smi --query-gpu=index,memory.used,memory.free --format=csv 2>/dev/null || true

    echo "[$(date -Iseconds)] Starting vLLM server on GPU 0, port $PORT_A"
    CUDA_VISIBLE_DEVICES=0 "$VLLM_BIN" serve "$MODEL" \
        --host "$HOST" --port "$PORT_A" \
        --tensor-parallel-size 1 \
        --gpu-memory-utilization "$GPU_MEM_UTIL" \
        --max-model-len "$MAX_MODEL_LEN" \
        >> "${LOG_DIR}/vllm_gpu0_${TIMESTAMP}.log" 2>&1 &
    VLLM_PID_A=$!

    echo "[$(date -Iseconds)] Starting vLLM server on GPU 1, port $PORT_B"
    CUDA_VISIBLE_DEVICES=1 "$VLLM_BIN" serve "$MODEL" \
        --host "$HOST" --port "$PORT_B" \
        --tensor-parallel-size 1 \
        --gpu-memory-utilization "$GPU_MEM_UTIL" \
        --max-model-len "$MAX_MODEL_LEN" \
        >> "${LOG_DIR}/vllm_gpu1_${TIMESTAMP}.log" 2>&1 &
    VLLM_PID_B=$!

    SERVERS_STARTED_BY_US=true
    echo "[$(date -Iseconds)] vLLM PIDs: GPU0=$VLLM_PID_A  GPU1=$VLLM_PID_B"

    # Wait for health
    echo "[$(date -Iseconds)] Waiting for vLLM servers to become healthy (timeout: ${HEALTH_TIMEOUT}s)"
    elapsed=0
    while [ $elapsed -lt $HEALTH_TIMEOUT ]; do
        a_ok=false; b_ok=false
        check_health $PORT_A && a_ok=true
        check_health $PORT_B && b_ok=true

        if $a_ok && $b_ok; then
            echo "[$(date -Iseconds)] Both vLLM servers healthy after ${elapsed}s"
            break
        fi

        # Fail fast if a server process died
        if ! kill -0 "$VLLM_PID_A" 2>/dev/null; then
            echo "[$(date -Iseconds)] ERROR: vLLM GPU0 process died. See ${LOG_DIR}/vllm_gpu0_${TIMESTAMP}.log" >&2
            exit 1
        fi
        if ! kill -0 "$VLLM_PID_B" 2>/dev/null; then
            echo "[$(date -Iseconds)] ERROR: vLLM GPU1 process died. See ${LOG_DIR}/vllm_gpu1_${TIMESTAMP}.log" >&2
            exit 1
        fi

        sleep $HEALTH_POLL_INTERVAL
        elapsed=$(( elapsed + HEALTH_POLL_INTERVAL ))
    done

    if [ $elapsed -ge $HEALTH_TIMEOUT ]; then
        echo "[$(date -Iseconds)] ERROR: vLLM servers did not become healthy within ${HEALTH_TIMEOUT}s" >&2
        exit 1
    fi
fi

# ── Run extraction under deadline ───────────────────────────────────
now_epoch=$(date +%s)
remaining_seconds=$(( deadline_epoch - now_epoch ))
if [ $remaining_seconds -le 60 ]; then
    echo "[$(date -Iseconds)] Less than 60s remaining after server startup -- skipping extraction"
    exit 0
fi

echo "[$(date -Iseconds)] Starting dual-GPU extraction (timeout: ${remaining_seconds}s / $(( remaining_seconds / 3600 ))h$(( (remaining_seconds % 3600) / 60 ))m)"

set +e
timeout "${remaining_seconds}s" "$VENV_PYTHON" \
    "${PROJECT_ROOT}/scripts/processing/run_olmocr_dual_gpu.py" \
    --pdf-root "$PDF_ROOT" \
    --features legal \
    --workers 8 \
    --classifier-workers 4 \
    --ocr-workers 2 \
    --text-parser-mode footnote_optimized \
    --doc-policy all \
    --host "$HOST" \
    --port-a "$PORT_A" \
    --port-b "$PORT_B" \
    --model "$MODEL" \
    --ocr-mode fallback \
    --olmocr-timeout-seconds 900 \
    --respect-qc-exclusions \
    --no-endpoint-check \
    --shuffle \
    --skip-classification \
    --python-bin "$VENV_PYTHON"
STATUS=$?
set -e

# ── Report outcome ──────────────────────────────────────────────────
if [ "$STATUS" -eq 124 ]; then
    echo "[$(date -Iseconds)] Deadline reached (${DEADLINE_HOUR}:00). Extraction will resume on next run."
    exit 0
elif [ "$STATUS" -eq 0 ]; then
    echo "[$(date -Iseconds)] Extraction completed successfully."
    exit 0
else
    echo "[$(date -Iseconds)] Extraction exited with status $STATUS"
    exit "$STATUS"
fi
