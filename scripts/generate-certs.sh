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

# Check if TLS certificates already exist
if [ -f "$CERT_FILE" ] && [ -f "$KEY_FILE" ]; then
    echo "Certificates already exist at:"
    echo "  Cert: $CERT_FILE"
    echo "  Key:  $KEY_FILE"
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
