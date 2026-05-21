#!/usr/bin/env bash
# Run one upstream conformance test inside an existing agent dind.
# Idempotent cleanup of the `conformance` namespace + clusterrolebinding
# before the next call. Emits one CSV row per invocation:
#
#   <timestamp_iso>,<agent_id>,<status>,<duration_s>,<exit_code>,<slug>,"<upstream_name>"
#
# Status is one of: PASS | FAIL | TIMEOUT | ERROR.
#
# Required env:
#   AGENT_ID=1..9            picks the dind container + kubeconfig
#   AGENT_WORKDIR=<path>     per-agent worktree (mounted as /workspace in dind)
#
# Required positional:
#   $1                        upstream Ginkgo test description (exact string;
#                             will be regex-escaped for --focus)
#
# Optional env / flags:
#   PER_TEST_TIMEOUT=300      seconds before SIGTERM (default 5 min)
#   CONFORMANCE_IMAGE=…       defaults to v1.35.0
#   RESULTS_CSV=<path>        defaults to ${AGENT_WORKDIR}/.rusternetes/agents/${AGENT_ID}/results/results.csv

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=conformance-agent-common.sh
source "${SCRIPT_DIR}/conformance-agent-common.sh"

CONFORMANCE_IMAGE="${CONFORMANCE_IMAGE:-registry.k8s.io/conformance:v1.35.0}"
PER_TEST_TIMEOUT="${PER_TEST_TIMEOUT:-300}"

if [[ $# -lt 1 ]]; then
  echo "usage: $0 '<upstream conformance test name>'" >&2
  exit 64
fi

TEST_NAME="$1"

# Stable slug for per-test artifacts + CSV row.
slug() {
  printf '%s' "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | tr -c 'a-z0-9' '-' \
    | sed -e 's/-\{2,\}/-/g' -e 's/^-//' -e 's/-$//' \
    | cut -c1-80
}
SLUG="$(slug "$TEST_NAME")"

# Regex-escape brackets / metachars so hydrophone's --focus matches the
# literal description.
escape_regex() {
  printf '%s' "$1" | sed -e 's/[][\\.^$(){}*+?|]/\\&/g'
}
FOCUS="$(escape_regex "$TEST_NAME")"

PER_TEST_DIR="${AGENT_ARTIFACT_DIR}/per-test/${SLUG}"
mkdir -p "$PER_TEST_DIR"

RESULTS_CSV="${RESULTS_CSV:-${AGENT_ARTIFACT_DIR}/results/results.csv}"
mkdir -p "$(dirname "$RESULTS_CSV")"
if [[ ! -s "$RESULTS_CSV" ]]; then
  echo "timestamp,agent_id,status,duration_s,exit_code,slug,upstream_name" > "$RESULTS_CSV"
fi

if ! docker inspect "$DIND_NAME" >/dev/null 2>&1; then
  echo "[agent-${AGENT_ID}] dind ${DIND_NAME} not running — bring it up first" >&2
  exit 1
fi

# Make sure hydrophone is present inside the dind. agent-run.sh installs
# it on first call; replicate that here so this script can stand alone.
HYDROPHONE_VERSION="${HYDROPHONE_VERSION:-v0.7.0}"
docker exec "$DIND_NAME" sh -c '
  set -e
  if command -v hydrophone >/dev/null; then exit 0; fi
  apk add --no-cache curl tar >/dev/null
  curl -fsSL -o /tmp/hydrophone.tgz \
    "https://github.com/kubernetes-sigs/hydrophone/releases/download/'"$HYDROPHONE_VERSION"'/hydrophone_Linux_x86_64.tar.gz"
  tar -xzf /tmp/hydrophone.tgz -C /usr/local/bin hydrophone
  chmod +x /usr/local/bin/hydrophone
' >/dev/null 2>&1 || true

START=$(date +%s)
TS=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# Output dir for hydrophone inside the dind workspace.
INNER_OUT="/workspace/.rusternetes/agents/${AGENT_ID}/per-test/${SLUG}"

set +e
timeout --kill-after=15 "${PER_TEST_TIMEOUT}" docker exec \
  -w /workspace \
  -e KUBECONFIG="/workspace/.rusternetes/agents/${AGENT_ID}/kubeconfig-internal" \
  "$DIND_NAME" \
  hydrophone \
    --conformance-image "$CONFORMANCE_IMAGE" \
    --focus "$FOCUS" \
    --output-dir "$INNER_OUT" \
    --skip-preflight conformance \
  >"$PER_TEST_DIR/run.log" 2>&1
EXIT=$?
set -e

END=$(date +%s)
DURATION=$((END - START))

# Status decision.
STATUS="ERROR"
if [[ "$EXIT" -eq 124 || "$EXIT" -eq 137 ]]; then
  STATUS="TIMEOUT"
else
  JUNIT="$PER_TEST_DIR/junit_01.xml"
  if [[ -f "$JUNIT" ]]; then
    if command -v xmlstarlet >/dev/null 2>&1; then
      total=$(xmlstarlet sel -t -v 'count(//testcase)' "$JUNIT" 2>/dev/null || echo 0)
      failed=$(xmlstarlet sel -t -v 'count(//testcase[failure])' "$JUNIT" 2>/dev/null || echo 0)
    else
      total=$(grep -c '<testcase ' "$JUNIT" || echo 0)
      failed=$(grep -c '<failure' "$JUNIT" || echo 0)
    fi
    if [[ "$total" -gt 0 && "$failed" -eq 0 ]]; then
      STATUS="PASS"
    elif [[ "$total" -gt 0 ]]; then
      STATUS="FAIL"
    fi
  fi
fi

# Always cleanup so the next test starts fresh. Ignore errors — best effort.
docker exec "$DIND_NAME" \
  hydrophone --cleanup \
  --kubeconfig "/workspace/.rusternetes/agents/${AGENT_ID}/kubeconfig-internal" \
  >/dev/null 2>&1 || true

# CSV-quote the upstream name (contains commas, brackets).
quoted=$(printf '%s' "$TEST_NAME" | sed 's/"/""/g')
printf '%s,%s,%s,%s,%s,%s,"%s"\n' \
  "$TS" "$AGENT_ID" "$STATUS" "$DURATION" "$EXIT" "$SLUG" "$quoted" \
  >> "$RESULTS_CSV"

echo "[agent-${AGENT_ID}] ${STATUS} (${DURATION}s, exit=${EXIT}) ${TEST_NAME}"

# Always exit 0 so a per-test failure does not abort the worker loop.
exit 0
