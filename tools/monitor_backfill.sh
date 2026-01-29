#!/usr/bin/env bash
set -euo pipefail

LOG="/home/evan/Documents/takopi_adventures/projects/deepbookv3/backfill_progress.log"
STATE="/home/evan/Documents/takopi_adventures/projects/deepbookv3/backfill_progress.state"
START=237221502
END=239613186
FIRST_DELAY=300
INTERVAL=1800
METRICS_URL="http://localhost:9184/metrics"

# Wait 5 minutes before the first report to get an initial read.
sleep "$FIRST_DELAY"

first_run=1
while true; do
  TS=$(date -Is)
  PID=$(pgrep -f "deepbook-indexer --env mainnet --first-checkpoint ${START} --last-checkpoint ${END}" || true)
  if [ -z "$PID" ]; then
    PID=$(pgrep -f "cargo run -p deepbook-indexer -- --env mainnet --first-checkpoint ${START} --last-checkpoint ${END}" || true)
  fi

  if [ -z "$PID" ]; then
    STATUS="not running"
  else
    STATUS="running pid=${PID}"
  fi

  echo "[$TS] status: ${STATUS}" >> "$LOG"

  WATERMARKS=$(PGPASSWORD=postgrespw psql -h localhost -p 5433 -U postgres -d deepbook -Atc "SELECT MIN(checkpoint_hi_inclusive), MAX(checkpoint_hi_inclusive) FROM watermarks;" || true)
  INGEST_BYTES=$(curl -s "$METRICS_URL" | awk '/^deepbook_indexer_total_ingested_bytes /{print $2}' | tail -n 1)

  if [ -n "$WATERMARKS" ]; then
    MIN_CP=${WATERMARKS%%|*}
    MAX_CP=${WATERMARKS##*|}
    python - <<PY >> "$LOG"
import os
import time

start=${START}
end=${END}
state_path="${STATE}"
now_ts=int(time.time())
first_run=${first_run}
min_cp=int(${MIN_CP}) if str(${MIN_CP}).isdigit() else None
max_cp=int(${MAX_CP}) if str(${MAX_CP}).isdigit() else None
bytes_val=None
try:
    bytes_val=float("${INGEST_BYTES}") if "${INGEST_BYTES}" else None
except Exception:
    bytes_val=None

if min_cp is not None:
    pct = (min_cp - start) / (end - start) * 100
    if pct < 0:
        pct = 0.0
    if pct > 100:
        pct = 100.0

    cps_line = ""
    mbs_line = ""
    if not first_run:
        try:
            if os.path.exists(state_path):
                with open(state_path, "r") as f:
                    prev_ts, prev_cp, prev_bytes = f.read().strip().split(",")
                prev_ts = int(prev_ts)
                prev_cp = int(prev_cp)
                prev_bytes = float(prev_bytes)
                dt = max(1, now_ts - prev_ts)
                delta_cp = max(0, min_cp - prev_cp)
                cps = delta_cp / dt
                cps_line = f" cps~{cps:.2f}"
                if bytes_val is not None:
                    delta_bytes = max(0.0, bytes_val - prev_bytes)
                    mbs = delta_bytes / dt / (1024 * 1024)
                    mbs_line = f" mbps~{mbs:.2f}"
        except Exception:
            cps_line = ""
            mbs_line = ""

    print(f"min_checkpoint={min_cp} max_checkpoint={max_cp} progress={pct:.2f}%{cps_line}{mbs_line}")

    try:
        with open(state_path, "w") as f:
            f.write(f"{now_ts},{min_cp},{bytes_val if bytes_val is not None else 0.0}")
    except Exception:
        pass
else:
    print("no watermarks yet")
PY
  else
    echo "no watermarks yet" >> "$LOG"
  fi

  echo "" >> "$LOG"
  first_run=0
  sleep "$INTERVAL"
 done
