#!/usr/bin/env bash
# Installs a pinned cargo-nextest binary. Leaseweb runners have no
# passwordless sudo, so this must not use apt or taiki-e/install-action.
set -euo pipefail

version="0.9.152"
asset="cargo-nextest-${version}-x86_64-unknown-linux-gnu.tar.gz"
expected="d039b43c3e0958441b6ad0a39c8884d018b3d2e8fb9928f209074b1100b5e17a"
dest="${CARGO_HOME:-$HOME/.cargo}/bin"
mkdir -p "$dest"

if ! command -v cargo-nextest >/dev/null 2>&1; then
  archive="$(mktemp)"
  trap 'rm -f "$archive"' EXIT
  curl --noproxy '*' -fsSL -o "$archive" \
    "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-${version}/${asset}"
  echo "${expected}  ${archive}" | sha256sum -c -
  tar -xzf "$archive" -C "$dest" cargo-nextest
  chmod 755 "$dest/cargo-nextest"
fi

echo "$dest" >> "${GITHUB_PATH:?GITHUB_PATH is required}"
"$dest/cargo-nextest" --version
