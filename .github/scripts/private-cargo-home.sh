#!/usr/bin/env bash
# Gives the current job its own CARGO_HOME.
#
# The Leaseweb runners share one HOME. Swatinem/rust-cache deletes
# "$CARGO_HOME/registry/src" when it saves a cache, which breaks every other
# job compiling from that shared registry. A per-runner home keeps that
# cleanup inside the job that triggered it.
set -euo pipefail

if [ -n "${CARGO_HOME:-}" ]; then
  base="$(dirname "$CARGO_HOME")"
else
  base="$HOME"
fi
target="$base/.cargo-${RUNNER_NAME:-local}"
mkdir -p "$target/bin"

{
  echo "CARGO_HOME=$target"
  echo "PATH=$target/bin:$PATH"
} >> "${GITHUB_ENV:?GITHUB_ENV is required}"

echo "private CARGO_HOME: $target"
