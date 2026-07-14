#!/usr/bin/env bash
# SSH-lockout guard for the P0-1 boundary-fix corpus re-extract on this
# 0-swap box. Polls /proc/meminfo every 10s; if MemAvailable drops below
# THRESHOLD_KB, kills the re-extract scope (SIGTERM then SIGKILL) so the kernel
# OOM killer can never reach sshd/the live labeling run. Exits 0 once the scope
# is gone (job finished), exits 1 if it had to kill.
#
# Matches the corpus-*.scope naming so the existing 6G belt watchdog
# (experiments/.../mem_watchdog.sh) is a second backstop.
set -uo pipefail
THRESHOLD_KB="${THRESHOLD_KB:-8000000}"   # 8 GB (task requirement)
SCOPE="${SCOPE:-corpus-reextract.scope}"
WLOG="${WLOG:-/tmp/reextract_watchdog_$(date -u +%Y%m%dT%H%M%SZ).log}"

echo "[watchdog] start scope=$SCOPE threshold=${THRESHOLD_KB}KB log=$WLOG" | tee "$WLOG"
# grace period so the scope has time to register
sleep 15
while true; do
  avail=$(awk '/MemAvailable/{print $2}' /proc/meminfo)
  active=$(systemctl --user list-units --type=scope --state=active "$SCOPE" --no-legend 2>/dev/null | wc -l)
  if [ "$avail" -lt "$THRESHOLD_KB" ]; then
    echo "[watchdog] $(date -u +%H:%M:%SZ) MemAvailable=${avail}KB < ${THRESHOLD_KB}KB -> KILLING $SCOPE" | tee -a "$WLOG"
    systemctl --user kill --signal=SIGTERM "$SCOPE" 2>/dev/null
    sleep 3
    systemctl --user kill --signal=SIGKILL "$SCOPE" 2>/dev/null
    echo "[watchdog] killed; exiting" | tee -a "$WLOG"
    exit 1
  fi
  if [ "$active" -eq 0 ]; then
    echo "[watchdog] $(date -u +%H:%M:%SZ) scope not active (avail=${avail}KB) -> job done, exiting" | tee -a "$WLOG"
    exit 0
  fi
  sleep 10
done
