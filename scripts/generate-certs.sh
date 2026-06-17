#!/bin/bash
# Script to generate TLS certificates for the API server
# These certificates are persisted and reused across restarts

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CERT_DIR="${PROJECT_ROOT}/.rusternetes/certs"
CERT_FILE="${CERT_DIR}/api-server.crt"
KEY_FILE="${CERT_DIR}/api-server.key"
# Cluster CA — a real CA (CA:TRUE) that signs the api-server's leaf serving cert
# and every client cert, and is distributed as ca.crt (kubeconfigs + SA tokens).
# Kept distinct from the serving cert: a CA:TRUE leaf is rejected by strict TLS
# stacks (rustls/webpki: "CaUsedAsEndEntity"), which breaks Rust in-cluster
# clients such as kube-rs (flannel-rs). Go's crypto/tls tolerates it, hence the
# old single-cert shortcut went unnoticed until a CNI written in Rust appeared.
CA_CERT="${CERT_DIR}/ca.crt"
CA_KEY="${CERT_DIR}/ca.key"

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
# AUTHN (crates/api-server + crates/middleware): the api-server now supports BOTH
# `Authorization: Bearer <token>` (SA JWT / bootstrap token) AND x509 client-cert
# authentication (#1129). With `--client-ca-file` set, the api-server serves via
# crates/api-server/src/peer_cert_acceptor.rs, which surfaces the rustls-verified
# peer cert to the auth middleware; `auth_middleware` maps the cert's Subject
# CommonName → user and each Organization → group (CommonNameUserConversion
# parity). The client cert is OPTIONAL at the TLS layer (into_mtls_server_config
# uses allow_unauthenticated), so bearer-token clients still connect.
#
# Separately, compose still runs the api-server with `--skip-auth`, which injects
# an admin AuthContext (`system:masters`) for EVERY request and bypasses the
# authorizer entirely. So under the current compose stack the scheduler cert is
# still not strictly required — but it IS now honored the moment --skip-auth is
# dropped and --client-ca-file is set.
#
# Consequences for the scheduler static pod:
#   * Under --skip-auth: client credentials are not consulted (every request is
#     admin). The kubeconfig still only needs the CA to VALIDATE the api-server's
#     serving cert.
#   * With --skip-auth dropped + --client-ca-file set: the CN=system:kube-scheduler
#     cert below authenticates the scheduler as user `system:kube-scheduler`, and
#     the ClusterRole/ClusterRoleBinding of the same name (bootstrap-cluster.yaml)
#     grant it the verbs it issues.
#
# We emit a CN=system:kube-scheduler / O=system:kube-scheduler client cert
# (kubeadm parity: CN → user, O → group) and embed it in scheduler.conf.
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
            -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
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

# kube-controller-manager kubeconfig for the in-cluster static pod. Identical
# rationale to the scheduler one above (CA-only is sufficient under
# --skip-auth; a CN=system:kube-controller-manager client cert is emitted for
# kubeadm parity / forward-compat but unused by today's authenticator).
generate_controller_manager_kubeconfig() {
    local cm_key="${CERT_DIR}/kube-controller-manager.key"
    local cm_crt="${CERT_DIR}/kube-controller-manager.crt"
    local cm_conf="${CERT_DIR}/controller-manager.conf"
    local ca_crt="${CERT_DIR}/ca.crt"

    if [ ! -f "$ca_crt" ]; then
        return 0
    fi

    if [ ! -f "$cm_crt" ] || [ ! -f "$cm_conf" ]; then
        echo "Generating kube-controller-manager client cert + kubeconfig..."
        openssl ecparam -name prime256v1 -genkey -noout -out "$cm_key"
        openssl req -new -key "$cm_key" -out "${CERT_DIR}/kube-controller-manager.csr" \
            -subj "/CN=system:kube-controller-manager/O=system:kube-controller-manager"
        openssl x509 -req -in "${CERT_DIR}/kube-controller-manager.csr" \
            -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
            -out "$cm_crt" -days 3650 \
            -extfile <(printf "extendedKeyUsage = clientAuth\n") 2>/dev/null
        rm -f "${CERT_DIR}/kube-controller-manager.csr"

        local ca_b64 cert_b64 key_b64
        ca_b64=$(base64 -w0 < "$ca_crt")
        cert_b64=$(base64 -w0 < "$cm_crt")
        key_b64=$(base64 -w0 < "$cm_key")

        cat > "$cm_conf" <<KUBECONFIG
apiVersion: v1
kind: Config
current-context: kube-controller-manager@rusternetes
clusters:
- name: rusternetes
  cluster:
    server: https://api-server:6443
    certificate-authority-data: ${ca_b64}
users:
- name: system:kube-controller-manager
  user:
    client-certificate-data: ${cert_b64}
    client-key-data: ${key_b64}
contexts:
- name: kube-controller-manager@rusternetes
  context:
    cluster: rusternetes
    user: system:kube-controller-manager
    namespace: kube-system
KUBECONFIG
        echo "  Controller-manager cert:       $cm_crt (CN=system:kube-controller-manager)"
        echo "  Controller-manager kubeconfig: $cm_conf"
    fi
}

# kubelet kubeconfig for the in-cluster kubelets (node-1, node-2). On the
# compose network they reach the api-server by its DNS alias. One shared
# CA-only kubeconfig serves both nodes (sufficient under --skip-auth; a real
# per-node CN=system:node:<name> cert is needed only once authn is enabled,
# #1129).
generate_kubelet_kubeconfig() {
    local kl_key="${CERT_DIR}/kubelet.key"
    local kl_crt="${CERT_DIR}/kubelet.crt"
    local kl_conf="${CERT_DIR}/kubelet.conf"
    local ca_crt="${CERT_DIR}/ca.crt"

    if [ ! -f "$ca_crt" ]; then
        return 0
    fi

    if [ ! -f "$kl_crt" ] || [ ! -f "$kl_conf" ]; then
        echo "Generating kubelet client cert + kubeconfig..."
        openssl ecparam -name prime256v1 -genkey -noout -out "$kl_key"
        openssl req -new -key "$kl_key" -out "${CERT_DIR}/kubelet.csr" \
            -subj "/CN=system:node:rusternetes/O=system:nodes"
        openssl x509 -req -in "${CERT_DIR}/kubelet.csr" \
            -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
            -out "$kl_crt" -days 3650 \
            -extfile <(printf "extendedKeyUsage = clientAuth\n") 2>/dev/null
        rm -f "${CERT_DIR}/kubelet.csr"

        local ca_b64 cert_b64 key_b64
        ca_b64=$(base64 -w0 < "$ca_crt")
        cert_b64=$(base64 -w0 < "$kl_crt")
        key_b64=$(base64 -w0 < "$kl_key")

        cat > "$kl_conf" <<KUBECONFIG
apiVersion: v1
kind: Config
current-context: kubelet@rusternetes
clusters:
- name: rusternetes
  cluster:
    server: https://api-server:6443
    certificate-authority-data: ${ca_b64}
users:
- name: kubelet
  user:
    client-certificate-data: ${cert_b64}
    client-key-data: ${key_b64}
contexts:
- name: kubelet@rusternetes
  context:
    cluster: rusternetes
    user: kubelet
    namespace: default
KUBECONFIG
        echo "  Kubelet cert:       $kl_crt (CN=system:node:rusternetes)"
        echo "  Kubelet kubeconfig: $kl_conf"
    fi
}

# kube-proxy kubeconfig for the in-cluster compose service. kube-proxy runs
# host-network, so its kubeconfig points at the host-published api-server port
# (https://localhost:6443; cert SANs include localhost/127.0.0.1). CA-only
# suffices under --skip-auth; a CN=system:kube-proxy client cert is emitted for
# kubeadm parity (unused by today's authenticator).
generate_kube_proxy_kubeconfig() {
    local kp_key="${CERT_DIR}/kube-proxy.key"
    local kp_crt="${CERT_DIR}/kube-proxy.crt"
    local kp_conf="${CERT_DIR}/kube-proxy.conf"
    local ca_crt="${CERT_DIR}/ca.crt"

    if [ ! -f "$ca_crt" ]; then
        return 0
    fi

    if [ ! -f "$kp_crt" ] || [ ! -f "$kp_conf" ]; then
        echo "Generating kube-proxy client cert + kubeconfig..."
        openssl ecparam -name prime256v1 -genkey -noout -out "$kp_key"
        openssl req -new -key "$kp_key" -out "${CERT_DIR}/kube-proxy.csr" \
            -subj "/CN=system:kube-proxy/O=system:node-proxier"
        openssl x509 -req -in "${CERT_DIR}/kube-proxy.csr" \
            -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
            -out "$kp_crt" -days 3650 \
            -extfile <(printf "extendedKeyUsage = clientAuth\n") 2>/dev/null
        rm -f "${CERT_DIR}/kube-proxy.csr"

        local ca_b64 cert_b64 key_b64
        ca_b64=$(base64 -w0 < "$ca_crt")
        cert_b64=$(base64 -w0 < "$kp_crt")
        key_b64=$(base64 -w0 < "$kp_key")

        # Host-network kube-proxy reaches the api-server at the published port.
        cat > "$kp_conf" <<KUBECONFIG
apiVersion: v1
kind: Config
current-context: kube-proxy@rusternetes
clusters:
- name: rusternetes
  cluster:
    server: https://localhost:6443
    certificate-authority-data: ${ca_b64}
users:
- name: system:kube-proxy
  user:
    client-certificate-data: ${cert_b64}
    client-key-data: ${key_b64}
contexts:
- name: kube-proxy@rusternetes
  context:
    cluster: rusternetes
    user: system:kube-proxy
    namespace: kube-system
KUBECONFIG
        echo "  Kube-proxy cert:       $kp_crt (CN=system:kube-proxy)"
        echo "  Kube-proxy kubeconfig: $kp_conf"
    fi
}

# Check if TLS certificates already exist
if [ -f "$CERT_FILE" ] && [ -f "$KEY_FILE" ]; then
    echo "Certificates already exist at:"
    echo "  Cert: $CERT_FILE"
    echo "  Key:  $KEY_FILE"
    # Still (re)generate the scheduler + controller-manager kubeconfigs — they
    # depend on the existing CA, not on regenerating the api-server cert.
    generate_scheduler_kubeconfig
    generate_controller_manager_kubeconfig
    generate_kube_proxy_kubeconfig
    generate_kubelet_kubeconfig
    echo ""
    echo "To regenerate certificates, delete them first:"
    echo "  rm $CERT_FILE $KEY_FILE"
    exit 0
fi

echo "Generating TLS certificates for API server..."

# Create certs directory if it doesn't exist
mkdir -p "$CERT_DIR"

# Two-tier PKI: a self-signed CA (CA:TRUE) signs a separate api-server leaf
# serving cert (CA:FALSE, serverAuth). Distributing the CA as ca.crt — rather
# than the old shortcut of serving the CA cert itself — keeps strict TLS stacks
# (rustls/webpki) happy: a CA:TRUE cert presented as the leaf is rejected with
# "CaUsedAsEndEntity", which is exactly what blocked flannel-rs's kube-rs client.

# Generate the CA key + self-signed CA cert (only if absent, so reruns keep the
# CA stable and previously-signed client certs remain valid).
if [ ! -f "$CA_KEY" ] || [ ! -f "$CA_CERT" ]; then
    echo "Generating cluster CA..."
    openssl ecparam -name prime256v1 -genkey -noout -out "$CA_KEY"
    openssl req -new -x509 \
        -key "$CA_KEY" \
        -out "$CA_CERT" \
        -days 3650 \
        -subj "/CN=rusternetes-ca/O=Rusternetes/C=US" \
        -addext "basicConstraints = critical, CA:TRUE" \
        -addext "keyUsage = critical, digitalSignature, keyCertSign, cRLSign" \
        -set_serial 01
fi

# Generate the api-server leaf serving-cert private key
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
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
basicConstraints = critical, CA:FALSE
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

# Generate the api-server leaf serving cert: CSR signed by the CA (valid 10y).
# The leaf carries the SANs + serverAuth EKU; the CA stays in ca.crt.
openssl req -new \
    -key "$KEY_FILE" \
    -out "${CERT_DIR}/api-server.csr" \
    -config "${CERT_DIR}/cert.conf"
openssl x509 -req \
    -in "${CERT_DIR}/api-server.csr" \
    -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
    -out "$CERT_FILE" \
    -days 3650 \
    -extfile "${CERT_DIR}/cert.conf" \
    -extensions v3_req

# Clean up config + CSR
rm "${CERT_DIR}/cert.conf" "${CERT_DIR}/api-server.csr"

# ca.crt is already the cluster CA (written as $CA_CERT in the CA-generation
# block above) — do NOT copy the serving cert over it (that was the old
# single-cert model). CoreDNS SA-volume CA prestaging was dropped upstream as
# dead (f3bbb1fa), so there is nothing further to distribute here.

# Now that the api-server cert + CA exist, emit the kube-scheduler,
# kube-controller-manager (static pods) and kube-proxy (host-network compose
# service) client certs + kubeconfigs for the in-cluster components.
generate_scheduler_kubeconfig
generate_controller_manager_kubeconfig
generate_kube_proxy_kubeconfig
generate_kubelet_kubeconfig

echo ""
echo "Certificates generated successfully:"
echo "  Cert: $CERT_FILE"
echo "  Key:  $KEY_FILE"
echo "  CA:   ${CERT_DIR}/ca.crt"
echo "  SA Key: ${CERT_DIR}/sa.key"
echo ""
echo "Certificate details:"
openssl x509 -in "$CERT_FILE" -text -noout | grep -E "(Subject:|Issuer:|Not Before|Not After|DNS:|IP:)"
