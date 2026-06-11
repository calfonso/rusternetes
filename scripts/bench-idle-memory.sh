#!/usr/bin/env bash
# Idle-RSS sampler for #1039 (in-process watch event bus).
#
# Reports VmRSS of the idle all-in-one process. The watch-cache ring buffers
# the event bus would shrink live in this process, so its idle RSS is the
# before/after number.
#
# Usage:
#   scripts/bench-idle-memory.sh                 # boot target/release/rusternetes, sample 30s
#   scripts/bench-idle-memory.sh --pid 12345     # sample an already-running process
#   scripts/bench-idle-memory.sh --seconds 60    # override sample window
#   scripts/bench-idle-memory.sh --settle 10     # boot settle time before sampling
set -euo pipefail

SECONDS_TO_SAMPLE=30
PID=""
SETTLE=5

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pid) PID="$2"; shift 2 ;;
    --seconds) SECONDS_TO_SAMPLE="$2"; shift 2 ;;
    --settle) SETTLE="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

cleanup() { :; }
BOOTED=""

if [[ -z "$PID" ]]; then
  BIN="target/release/rusternetes"
  if [[ ! -x "$BIN" ]]; then
    echo "error: $BIN not found. Build it: cargo build --release -p rusternetes" >&2
    echo "       or sample a running process with --pid <N>." >&2
    exit 1
  fi
  echo "booting $BIN ..." >&2
  "$BIN" >/tmp/bench-idle-memory.log 2>&1 &
  PID=$!
  BOOTED=1
  cleanup() { kill "$PID" 2>/dev/null || true; }
  trap cleanup EXIT
  echo "pid $PID; settling ${SETTLE}s ..." >&2
  sleep "$SETTLE"
fi

if [[ ! -r "/proc/$PID/status" ]]; then
  echo "error: /proc/$PID/status not readable (process gone?). See /tmp/bench-idle-memory.log" >&2
  exit 1
fi

echo "sampling VmRSS of pid $PID for ${SECONDS_TO_SAMPLE}s ..." >&2
min=""; max=""; sum=0; count=0
for _ in $(seq "$SECONDS_TO_SAMPLE"); do
  if [[ ! -r "/proc/$PID/status" ]]; then
    echo "error: process $PID exited mid-sample" >&2
    exit 1
  fi
  # VmRSS line is in kB.
  kb=$(awk '/^VmRSS:/ {print $2}' "/proc/$PID/status")
  [[ -z "$min" || "$kb" -lt "$min" ]] && min="$kb"
  [[ -z "$max" || "$kb" -gt "$max" ]] && max="$kb"
  sum=$((sum + kb))
  count=$((count + 1))
  sleep 1
done

if [[ "$count" -eq 0 ]]; then
  echo "error: no samples collected (--seconds must be >= 1)" >&2
  exit 1
fi
avg=$((sum / count))
printf 'idle VmRSS over %ss (pid %s)%s:\n' "$SECONDS_TO_SAMPLE" "$PID" "${BOOTED:+ [booted]}"
printf '  min %d MiB  avg %d MiB  max %d MiB\n' $((min / 1024)) $((avg / 1024)) $((max / 1024))
