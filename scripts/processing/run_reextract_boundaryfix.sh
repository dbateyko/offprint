#!/usr/bin/env bash
# P0-1 boundary-fix corpus footnote re-extract (fan-out).
#
# Runs SHARDS single-worker extractor processes over the whole PDF corpus with
# --overwrite, re-clipping cross-page footnote text with the committed
# sequence_solver fix (offprint 6da4008). OCR is OFF (CPU-only): the live GPU
# labeling run owns ports 8000/8001 and must not be touched.
#
# WHY PROCESSES, NOT THREADS: the footnote_optimized liteparse path is
# thread-unsafe (--workers>1 silently writes empty sidecars, see the
# footnote_liteparse_thread_unsafe note). Parallelism is process-level via
# --shard-count/--shard-index, each shard --workers 1.
#
# OCR PROTECTION: PDFs listed in the QC exclusion manifest (olmOCR-rescued /
# OCR-content sidecars) are skipped so a CPU-only re-extract cannot regress
# them.
#
# SAFETY: sidecars are written atomically (tmp + os.replace), so an OOM-kill or
# interrupt cannot corrupt one. Re-running with the same flags resumes (it
# overwrites; already-fixed shards are simply redone). MUST be launched inside a
# capped `systemd --user --scope` and a screen (see the launch block in the
# runbook / handoff), with the mem watchdog running alongside on this 0-swap
# box.
set -uo pipefail

ROOT="${OFFPRINT_ROOT:-/mnt/shared_storage/law-review-corpus}"
PDF_ROOT="${PDF_ROOT:-$ROOT/corpus/scraped}"
PY="${PY:-$ROOT/offprint/.venv/bin/python}"
EXTRACT="$ROOT/offprint/scripts/processing/extract_footnotes.py"
MANIFEST="${QC_MANIFEST:-$ROOT/offprint/artifacts/runs/ocr_protection_exclusions_20260710.jsonl}"
SHARDS="${SHARDS:-6}"
RUNDIR="$ROOT/offprint/artifacts/runs"
TS="${TS:-$(date -u +%Y%m%dT%H%M%SZ)}"
STARTFILE="$RUNDIR/reextract_boundaryfix.start"

export OCR_MODE=off

mkdir -p "$RUNDIR"
date +%s >| "$STARTFILE"
echo "[reextract] start ts=$TS shards=$SHARDS"
echo "[reextract] pdf_root=$PDF_ROOT"
echo "[reextract] manifest=$MANIFEST"
echo "[reextract] run_start_epoch=$(cat "$STARTFILE")"

if [ ! -f "$MANIFEST" ]; then
  echo "[reextract] FATAL: OCR-protection manifest missing: $MANIFEST" >&2
  exit 2
fi

pids=()
for i in $(seq 0 $((SHARDS - 1))); do
  SLOG="$RUNDIR/reextract_boundaryfix_${TS}.shard${i}.log"
  "$PY" "$EXTRACT" \
    --pdf-root "$PDF_ROOT" \
    --workers 1 --classifier-workers 1 \
    --ocr-mode off \
    --overwrite true \
    --respect-qc-exclusions true \
    --qc-exclusion-manifest "$MANIFEST" \
    --doc-policy article_only \
    --text-parser-mode footnote_optimized \
    --shard-count "$SHARDS" --shard-index "$i" \
    --heartbeat-every 200 \
    --report-out "$RUNDIR/reextract_boundaryfix_${TS}.shard${i}.report.json" \
    >| "$SLOG" 2>&1 &
  pids+=($!)
  echo "[reextract] launched shard $i/$SHARDS pid=${pids[-1]} log=$SLOG"
done

# heartbeat loop until every shard exits
while :; do
  alive=0
  for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && alive=$((alive + 1)); done
  avail=$(awk '/MemAvailable/{print $2}' /proc/meminfo)
  echo "[reextract][hb $(date -u +%H:%M:%SZ)] shards_alive=$alive/$SHARDS MemAvailableKB=$avail"
  [ "$alive" -eq 0 ] && break
  sleep 60
done

wait
echo "[reextract] all shards finished at $(date -u +%H:%M:%SZ)"
for i in $(seq 0 $((SHARDS - 1))); do
  rep="$RUNDIR/reextract_boundaryfix_${TS}.shard${i}.report.json"
  [ -f "$rep" ] && echo "[reextract] shard$i: $("$PY" -c "import json,sys;d=json.load(open(sys.argv[1]));print('processed',d.get('processed'),'ok',d.get('ok'),'failed',d.get('failed'),'excluded_by_qc',d.get('excluded_by_qc'),'excluded_by_doc_policy',d.get('excluded_by_doc_policy'))" "$rep")"
done
