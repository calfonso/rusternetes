#!/usr/bin/env bash
# Footprint benchmark harness (#1038).
#
# Measures the rusternetes all-in-one footprint so the "k3s without melting your
# laptop" claim rests on real numbers instead of vibes. Dimensions:
#
#   1. binary size      — release `rusternetes`, raw + stripped (musl variant noted)
#   2. time-to-cluster  — `compose up` → first node Ready
#   3. idle RSS         — all-in-one container RSS, sampled while idle
#   4. idle CPU %       — same window
#
# Run head-to-head against k3s on identical hardware and paste the numbers into
# docs/PERFORMANCE.md. The container dimensions (2-4) need Docker + the
# `compose.all-in-one.yml` stack; `--size-only` skips them.
#
# Usage:
#   scripts/footprint-benchmark.sh                 # full run (builds release, boots stack)
#   scripts/footprint-benchmark.sh --size-only     # binary size only (no Docker)
#   scripts/footprint-benchmark.sh --seconds 60    # idle sample window (default 30)
#   scripts/footprint-benchmark.sh --no-build      # use an existing target/release/rusternetes
set -euo pipefail

SECONDS_TO_SAMPLE=30
SIZE_ONLY=0
BUILD=1
# Honour CARGO_TARGET_DIR (dev boxes often share one target dir across worktrees).
BIN="${CARGO_TARGET_DIR:-target}/release/rusternetes"
COMPOSE_FILE="compose.all-in-one.yml"
CONTAINER="rusternetes"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --size-only) SIZE_ONLY=1; shift ;;
    --no-build) BUILD=0; shift ;;
    --seconds) SECONDS_TO_SAMPLE="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || { echo "error: '$1' not found in PATH" >&2; exit 1; }; }

human() { # bytes -> MiB with 1 decimal
  awk -v b="$1" 'BEGIN { printf "%.1f MiB", b / 1048576 }'
}

# ---------------------------------------------------------------------------
# 1. Binary size
# ---------------------------------------------------------------------------
need cargo
if [[ "$BUILD" == "1" ]]; then
  echo "==> building release all-in-one (cargo build --release -p rusternetes) ..." >&2
  cargo build --release -p rusternetes
fi
if [[ ! -x "$BIN" ]]; then
  echo "error: $BIN not found — build it (omit --no-build) or run from the repo root." >&2
  exit 1
fi

raw_bytes=$(stat -c %s "$BIN")
stripped="$(mktemp)"
cp "$BIN" "$stripped"
strip "$stripped" 2>/dev/null || true
stripped_bytes=$(stat -c %s "$stripped")
rm -f "$stripped"

echo
echo "## Binary size (release, host target)"
echo "  raw:      $(human "$raw_bytes")  ($raw_bytes bytes)"
echo "  stripped: $(human "$stripped_bytes")  ($stripped_bytes bytes)"
echo "  musl+strip: build with"
echo "    cargo build --release -p rusternetes --target x86_64-unknown-linux-musl && strip ..."

if [[ "$SIZE_ONLY" == "1" ]]; then
  exit 0
fi

# ---------------------------------------------------------------------------
# 2-4. Container footprint (time-to-cluster, idle RSS, idle CPU)
# ---------------------------------------------------------------------------
need docker
DC="docker compose -f $COMPOSE_FILE"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/rusternetes-config}"

cleanup() { $DC down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo
echo "==> bringing up the all-in-one stack ($COMPOSE_FILE) ..." >&2
start=$(date +%s)
$DC up -d >/dev/null

# time-to-cluster: poll for a Ready node (best-effort; needs kubectl + bootstrap)
echo "==> waiting for first Ready node (time-to-cluster) ..." >&2
ttc="(unmeasured — needs kubectl + a Ready node)"
if command -v kubectl >/dev/null 2>&1; then
  for _ in $(seq 1 120); do
    if kubectl get nodes 2>/dev/null | grep -qw Ready; then
      ttc="$(( $(date +%s) - start ))s"
      break
    fi
    sleep 1
  done
fi
echo "  time-to-cluster: $ttc"

# idle RSS + CPU via docker stats over the sample window
echo "==> sampling idle RSS + CPU of container '$CONTAINER' for ${SECONDS_TO_SAMPLE}s ..." >&2
rss_sum=0; cpu_sum=0; n=0; rss_max=0
for _ in $(seq "$SECONDS_TO_SAMPLE"); do
  line=$(docker stats --no-stream --format '{{.MemUsage}}|{{.CPUPerc}}' "$CONTAINER" 2>/dev/null || true)
  [[ -z "$line" ]] && { sleep 1; continue; }
  mem=$(echo "$line" | cut -d'|' -f1 | awk '{print $1}')   # e.g. 312.4MiB
  cpu=$(echo "$line" | cut -d'|' -f2 | tr -d '%')
  mem_mib=$(awk -v m="$mem" 'BEGIN { if (m ~ /GiB/) {gsub(/GiB/,"",m); print m*1024} else {gsub(/MiB/,"",m); print m+0} }')
  rss_sum=$(awk -v a="$rss_sum" -v b="$mem_mib" 'BEGIN{print a+b}')
  rss_max=$(awk -v a="$rss_max" -v b="$mem_mib" 'BEGIN{print (b>a)?b:a}')
  cpu_sum=$(awk -v a="$cpu_sum" -v b="$cpu" 'BEGIN{print a+(b+0)}')
  n=$((n+1))
  sleep 1
done

echo
echo "## Idle control-plane footprint (all-in-one container)"
if [[ "$n" -gt 0 ]]; then
  printf "  idle RSS (avg): %.1f MiB\n" "$(awk -v s="$rss_sum" -v n="$n" 'BEGIN{print s/n}')"
  printf "  idle RSS (max): %.1f MiB\n" "$rss_max"
  printf "  idle CPU (avg): %.2f %%\n" "$(awk -v s="$cpu_sum" -v n="$n" 'BEGIN{print s/n}')"
  echo "  samples: $n"
else
  echo "  (no samples — is the '$CONTAINER' container running? check '$DC ps')"
fi

echo
echo "Competitive bar (idle, from #1038): k3s ~535-750MB, k0s ~658MB, microk8s ~526MB; OS baseline ~167MB."
echo "Target to claim the niche: sub-400MB idle (stretch 250-300MB)."
