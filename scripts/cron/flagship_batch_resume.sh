#!/usr/bin/env bash
# One-shot 24h-after retry for flagship27 + batch2.
# Re-launches scraping but skips any seed whose host already has >=200 PDFs
# (so big journals aren't re-scraped, only WAFed/stalled ones get a fresh try).

set -u
LOG_DIR=/mnt/shared_storage/law-review-corpus/offprint/artifacts/logs
SENTINEL=/tmp/flagship_batch_resume.done
RUN_TS=$(date -u +%Y%m%dT%H%M%SZ)

mkdir -p "$LOG_DIR"
exec >> "$LOG_DIR/cron_resume_${RUN_TS}.log" 2>&1

if [ -f "$SENTINEL" ]; then
  echo "[$(date -u)] sentinel $SENTINEL exists — already ran, exiting"
  exit 0
fi

cd /mnt/shared_storage/law-review-corpus/offprint || exit 1

echo "[$(date -u)] starting resume run"

# Build combined dir of all 27 + 120 + 117 seeds
RESUME_DIR=/tmp/flagship_resume_sitemaps_${RUN_TS}
mkdir -p "$RESUME_DIR"
cp /tmp/flagship27_all_sitemaps/*.json "$RESUME_DIR/" 2>/dev/null
cp /tmp/batch2_sitemaps/*.json "$RESUME_DIR/" 2>/dev/null
cp /tmp/batch3_sitemaps/*.json "$RESUME_DIR/" 2>/dev/null
N=$(ls "$RESUME_DIR"/*.json 2>/dev/null | wc -l)
echo "[$(date -u)] staged $N seeds in $RESUME_DIR"

# Split into 4 dirs
SPLIT_BASE=/tmp/flagship_resume_split_${RUN_TS}
mkdir -p "$SPLIT_BASE"/{a,b,c,d}
i=0
for f in "$RESUME_DIR"/*.json; do
  case $((i % 4)) in
    0) cp "$f" "$SPLIT_BASE/a/" ;;
    1) cp "$f" "$SPLIT_BASE/b/" ;;
    2) cp "$f" "$SPLIT_BASE/c/" ;;
    3) cp "$f" "$SPLIT_BASE/d/" ;;
  esac
  i=$((i+1))
done

# Launch 4 screens
for letter in a b c d; do
  RUN_ID="${RUN_TS}_resume_${letter}"
  LOG="$LOG_DIR/${RUN_ID}.log"
  cat > "/tmp/run_resume_${letter}.sh" <<EOF
#!/usr/bin/env bash
set -u
cd /mnt/shared_storage/law-review-corpus/offprint
export LRS_VERBOSE_PDF_LOG=0
export LRS_MAX_BROWSERS=1
exec python3 scripts/pipeline/run_pipeline.py \\
  --mode full \\
  --sitemaps-dir $SPLIT_BASE/$letter \\
  --out-dir artifacts/flagship27_pdfs \\
  --manifest-dir artifacts/runs \\
  --run-id $RUN_ID \\
  --max-workers 4 \\
  --max-seeds-per-domain 5 \\
  --max-consecutive-seed-failures-per-domain 5 \\
  --min-delay 1.5 --max-delay 3.5 \\
  --use-playwright --playwright-headless \\
  --skip-well-covered-seeds \\
  --well-covered-pdf-threshold 200 \\
  --no-skip-dc-sites \\
  --dc-enum-mode sitemap_only \\
  --no-dc-use-siteindex \\
  --dc-min-domain-delay-ms 2000 --dc-max-domain-delay-ms 4500 \\
  --dc-waf-fail-threshold 3 --dc-waf-cooldown-seconds 1800 \\
  --dc-waf-browser-fallback \\
  --dc-browser-headless \\
  --no-dc-round-robin-downloads \\
  --skip-retry-pass
EOF
  chmod +x "/tmp/run_resume_${letter}.sh"
  screen -dmS "resume_${letter}" bash -c "/tmp/run_resume_${letter}.sh 2>&1 | tee $LOG"
  sleep 2
done

touch "$SENTINEL"
echo "[$(date -u)] resume launched in 4 screens (resume_a..d), sentinel $SENTINEL written"
screen -ls | grep resume_
