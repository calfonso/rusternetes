#!/bin/bash
# Generate a kubeconfig that embeds the rusternetes CA certificate.
#
# Why not just `--insecure-skip-tls-verify`? kubectl tolerates that
# kubeconfig shape, but other Kubernetes tooling does not — notably
# Lens, which always Buffer.from(certificate-authority-data, 'base64')
# regardless of insecure-skip-tls-verify and crashes with
#   "The \"data\" argument must be of type string or an instance of
#    Buffer, TypedArray, or DataView. Received undefined"
# when the field is missing. k9s, octant, kubeshark and other
# Node/JS-based tooling have similar parsers.
#
# Embedding the CA up-front keeps every tool happy and gives proper
# TLS verification as a bonus.
#
# Usage:
#   bash scripts/generate-kubeconfig.sh                    # default path
#   bash scripts/generate-kubeconfig.sh /path/to/config    # explicit path
#   KUBECONFIG_OUT=/path/to/config bash scripts/generate-kubeconfig.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CA_CERT="${PROJECT_ROOT}/.rusternetes/certs/ca.crt"
KUBECONFIG_OUT="${1:-${KUBECONFIG_OUT:-$HOME/.kube/rusternetes-config}}"
API_SERVER="${API_SERVER:-https://localhost:6443}"
CLUSTER_NAME="${CLUSTER_NAME:-rusternetes}"
USER_NAME="${USER_NAME:-admin}"
USER_TOKEN="${USER_TOKEN:-anonymous}"

if [ ! -f "$CA_CERT" ]; then
    echo "error: CA cert not found at $CA_CERT" >&2
    echo "       run \`bash scripts/generate-certs.sh\` first" >&2
    exit 1
fi

CA_DATA="$(base64 -w0 < "$CA_CERT")"
mkdir -p "$(dirname "$KUBECONFIG_OUT")"

# Back up an existing kubeconfig so a user can recover if they ran this
# against the wrong path.
if [ -f "$KUBECONFIG_OUT" ]; then
    cp "$KUBECONFIG_OUT" "${KUBECONFIG_OUT}.bak"
    echo "Backed up existing kubeconfig to ${KUBECONFIG_OUT}.bak"
fi

cat > "$KUBECONFIG_OUT" <<EOF
apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: ${CA_DATA}
    server: ${API_SERVER}
  name: ${CLUSTER_NAME}
contexts:
- context:
    cluster: ${CLUSTER_NAME}
    user: ${USER_NAME}
  name: ${CLUSTER_NAME}
current-context: ${CLUSTER_NAME}
users:
- name: ${USER_NAME}
  user:
    token: ${USER_TOKEN}
EOF

echo "Wrote kubeconfig to $KUBECONFIG_OUT"
echo "  Cluster:  ${CLUSTER_NAME} -> ${API_SERVER}"
echo "  CA:       embedded ($(wc -c < "$CA_CERT") bytes from $CA_CERT)"
echo "  User:     ${USER_NAME} (token=${USER_TOKEN})"
echo
echo "Usage:"
echo "  export KUBECONFIG=${KUBECONFIG_OUT}"
echo "  kubectl get ns"
