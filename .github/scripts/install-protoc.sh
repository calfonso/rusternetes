#!/usr/bin/env bash
# Installs a pinned protoc into the job home. Leaseweb runners have no
# passwordless sudo, so this must not use apt.
set -euo pipefail

if command -v protoc >/dev/null 2>&1; then
  protoc --version
  exit 0
fi

version="29.3"
expected="3e866620c5be27664f3d2fa2d656b5f3e09b5152b42f1bedbf427b333e90021a"
dest="${HOME}/.local"
archive="$(mktemp)"
trap 'rm -f "$archive"' EXIT

curl --noproxy '*' -fsSL -o "$archive" \
  "https://github.com/protocolbuffers/protobuf/releases/download/v${version}/protoc-${version}-linux-x86_64.zip"
echo "${expected}  ${archive}" | sha256sum -c -
mkdir -p "$dest"
python3 - "$archive" "$dest" <<'PY'
import sys, zipfile
zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])
PY
chmod 755 "$dest/bin/protoc"
echo "$dest/bin" >> "${GITHUB_PATH:?GITHUB_PATH is required}"
"$dest/bin/protoc" --version
