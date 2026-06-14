#!/bin/bash
# Script to generate TLS certificates for the API server
# These certificates are persisted and reused across restarts

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CERT_DIR="${PROJECT_ROOT}/.rusternetes/certs"
CERT_FILE="${CERT_DIR}/api-server.crt"
KEY_FILE="${CERT_DIR}/api-server.key"

# Generate SA signing key pair (always — even if TLS certs exist)
SA_KEY="${CERT_DIR}/sa.key"
SA_PUB="${CERT_DIR}/sa.pub"
if [ ! -f "$SA_KEY" ]; then
    mkdir -p "$CERT_DIR"
    echo "Generating ServiceAccount signing key pair..."
    openssl genrsa -out "$SA_KEY" 2048 2>/dev/null
    openssl rsa -in "$SA_KEY" -pubout -out "$SA_PUB" 2>/dev/null
    echo "  SA private key: $SA_KEY"
    echo "  SA public key:  $SA_PUB"
fi

# Generate the kube-scheduler client cert + kubeconfig for the in-cluster
# static pod (Task 3 of the scheduler-in-cluster plan). Runs whether or not the
# api-server TLS cert already exists, so it is idempotently produced on reruns.
#
# AUTHN INVESTIGATION (crates/api-server + crates/middleware): the api-server's
# auth middleware (crates/middleware/src/lib.rs `auth_middleware`) authenticates
# ONLY via the `Authorization: Bearer <token>` header (SA JWT or bootstrap
# token); there is NO client-certificate → user/group mapping. `--client-ca-file`
# wires an mTLS *verifier* at the rustls layer (crates/common/src/tls.rs
# `into_mtls_server_config`), but the request handler uses
# `app.into_make_service()` (not `_with_connect_info`), so the peer cert never
# reaches a handler and CN/O are never turned into a user. Separately, compose
# runs the api-server with `--skip-auth`, which injects an admin AuthContext
# (`system:masters`) for EVERY request and bypasses the authorizer entirely.
#
# Consequences for the scheduler static pod:
#   * It does NOT need client credentials — with --skip-auth every request is
#     already admin, so authz passes. The kubeconfig only needs the CA so the
#     scheduler can VALIDATE the api-server's self-signed serving cert.
#   * No RBAC objects (ClusterRole/ClusterRoleBinding for system:kube-scheduler)
#     are required, because the authorizer is not consulted under --skip-auth.
#     bootstrap-cluster.sh therefore creates none (revisit if --skip-auth is
#     ever dropped: then add a Bearer-token kubeconfig + RBAC, since cert authn
#     is unimplemented — tracked as a follow-up).
#
# We still emit a CN=system:kube-scheduler client cert (kubeadm parity) and
# embed it in scheduler.conf so the kubeconfig is forward-compatible the day
# client-cert authn lands; it is simply unused by today's authenticator.
generate_scheduler_kubeconfig() {
    local sched_key="${CERT_DIR}/kube-scheduler.key"
    local sched_crt="${CERT_DIR}/kube-scheduler.crt"
    local sched_conf="${CERT_DIR}/scheduler.conf"
    local ca_crt="${CERT_DIR}/ca.crt"

    # The CA is the api-server's self-signed serving cert (CA:TRUE in cert.conf).
    # If it does not exist yet (first run), this function is re-invoked after the
    # api-server cert is generated below.
    if [ ! -f "$ca_crt" ]; then
        return 0
    fi

    if [ ! -f "$sched_crt" ] || [ ! -f "$sched_conf" ]; then
        echo "Generating kube-scheduler client cert + kubeconfig..."
        openssl ecparam -name prime256v1 -genkey -noout -out "$sched_key"
        # CSR with CN=system:kube-scheduler, O=system:kube-scheduler (kubeadm
        # parity: CN → user, O → group).
        openssl req -new -key "$sched_key" -out "${CERT_DIR}/kube-scheduler.csr" \
            -subj "/CN=system:kube-scheduler/O=system:kube-scheduler"
        # Sign with the api-server CA (clientAuth EKU).
        openssl x509 -req -in "${CERT_DIR}/kube-scheduler.csr" \
            -CA "$CERT_FILE" -CAkey "$KEY_FILE" -CAcreateserial \
            -out "$sched_crt" -days 3650 \
            -extfile <(printf "extendedKeyUsage = clientAuth\n") 2>/dev/null
        rm -f "${CERT_DIR}/kube-scheduler.csr"

        local ca_b64 cert_b64 key_b64
        ca_b64=$(base64 -w0 < "$ca_crt")
        cert_b64=$(base64 -w0 < "$sched_crt")
        key_b64=$(base64 -w0 < "$sched_key")

        # kubeconfig pointing at the in-cluster api-server compose DNS name.
        cat > "$sched_conf" <<KUBECONFIG
apiVersion: v1
kind: Config
current-context: kube-scheduler@rusternetes
clusters:
- name: rusternetes
  cluster:
    server: https://api-server:6443
    certificate-authority-data: ${ca_b64}
users:
- name: system:kube-scheduler
  user:
    client-certificate-data: ${cert_b64}
    client-key-data: ${key_b64}
contexts:
- name: kube-scheduler@rusternetes
  context:
    cluster: rusternetes
    user: system:kube-scheduler
    namespace: kube-system
KUBECONFIG
        echo "  Scheduler cert:       $sched_crt (CN=system:kube-scheduler)"
        echo "  Scheduler kubeconfig: $sched_conf"
    fi
}

# Check if TLS certificates already exist
if [ -f "$CERT_FILE" ] && [ -f "$KEY_FILE" ]; then
    echo "Certificates already exist at:"
    echo "  Cert: $CERT_FILE"
    echo "  Key:  $KEY_FILE"
    # Still (re)generate the scheduler kubeconfig — it depends on the existing
    # CA, not on regenerating the api-server cert.
    generate_scheduler_kubeconfig
    echo ""
    echo "To regenerate certificates, delete them first:"
    echo "  rm $CERT_FILE $KEY_FILE"
    exit 0
fi

echo "Generating TLS certificates for API server..."

# Create certs directory if it doesn't exist
mkdir -p "$CERT_DIR"

# Use OpenSSL to generate a self-signed certificate
# This matches the behavior of the Rust TLS generation but persists it

# Generate private key
openssl ecparam -name prime256v1 -genkey -noout -out "$KEY_FILE"

# Detect container runtime and get network subnet
# Try podman first, then docker
CONTAINER_RT=""
NETWORK_SUBNET=""
NETWORK_GATEWAY=""

if command -v podman &>/dev/null && podman network inspect rusternetes-network &>/dev/null 2>&1; then
    CONTAINER_RT="podman"
    NETWORK_SUBNET=$(podman network inspect rusternetes-network 2>/dev/null | jq -r '.[0].subnets[0].subnet // empty')
    NETWORK_GATEWAY=$(podman network inspect rusternetes-network 2>/dev/null | jq -r '.[0].subnets[0].gateway // empty')
elif command -v docker &>/dev/null && docker network inspect rusternetes-network &>/dev/null 2>&1; then
    CONTAINER_RT="docker"
    NETWORK_SUBNET=$(docker network inspect rusternetes-network 2>/dev/null | jq -r '.[0].IPAM.Config[0].Subnet // empty')
    NETWORK_GATEWAY=$(docker network inspect rusternetes-network 2>/dev/null | jq -r '.[0].IPAM.Config[0].Gateway // empty')
fi

# Extract base IP from gateway (e.g., 10.89.0.1 -> 10.89.0)
BASE_IP=""
if [ -n "$NETWORK_GATEWAY" ]; then
    BASE_IP=$(echo "$NETWORK_GATEWAY" | sed 's/\.[0-9]*$//')
fi

# Create certificate configuration
cat > "${CERT_DIR}/cert.conf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = rusternetes-api
O = Rusternetes
C = US

[v3_req]
keyUsage = critical, digitalSignature, keyEncipherment, dataEncipherment, keyCertSign
extendedKeyUsage = serverAuth, clientAuth
basicConstraints = critical, CA:TRUE
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = api-server
DNS.3 = rusternetes-api-server
DNS.4 = kubernetes
DNS.5 = kubernetes.default
DNS.6 = kubernetes.default.svc
DNS.7 = kubernetes.default.svc.cluster.local
IP.1 = 127.0.0.1
IP.2 = 10.96.0.1
EOF

# Add container network IPs to certificate SANs
# Include gateway + first 10 IPs to cover typical pod IP assignments
if [ -n "$BASE_IP" ]; then
    echo "# Container network: $CONTAINER_RT ($NETWORK_SUBNET)" >> "${CERT_DIR}/cert.conf"
    IP_INDEX=3
    for i in {1..10}; do
        echo "IP.$IP_INDEX = ${BASE_IP}.${i}" >> "${CERT_DIR}/cert.conf"
        IP_INDEX=$((IP_INDEX + 1))
    done
fi

# Generate self-signed certificate (valid for 10 years, matching the Rust implementation)
openssl req -new -x509 \
    -key "$KEY_FILE" \
    -out "$CERT_FILE" \
    -days 3650 \
    -config "${CERT_DIR}/cert.conf" \
    -extensions v3_req \
    -set_serial 01

# Clean up config file
rm "${CERT_DIR}/cert.conf"

# Copy certificate to CoreDNS volume location for ca.crt
COREDNS_CA_DIR="${PROJECT_ROOT}/.rusternetes/volumes/coredns/kube-api-access"
mkdir -p "$COREDNS_CA_DIR"
cp "$CERT_FILE" "${COREDNS_CA_DIR}/ca.crt"
echo "Copied certificate to CoreDNS volume: ${COREDNS_CA_DIR}/ca.crt"

# Also create ca.crt in certs directory for consistency
cp "$CERT_FILE" "${CERT_DIR}/ca.crt"

# Now that the api-server cert + CA exist, emit the kube-scheduler client cert
# and kubeconfig for the in-cluster static pod.
generate_scheduler_kubeconfig

echo ""
echo "Certificates generated successfully:"
echo "  Cert: $CERT_FILE"
echo "  Key:  $KEY_FILE"
echo "  CA:   ${CERT_DIR}/ca.crt"
echo "  CoreDNS CA: ${COREDNS_CA_DIR}/ca.crt"
echo "  SA Key: ${CERT_DIR}/sa.key"
echo ""
echo "Certificate details:"
openssl x509 -in "$CERT_FILE" -text -noout | grep -E "(Subject:|Issuer:|Not Before|Not After|DNS:|IP:)"
