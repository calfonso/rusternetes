#!/bin/bash
# Start Rusternetes cluster using podman directly (bypasses podman-compose macOS issues)
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

# Ensure network exists
echo "Creating network..."
podman network create rusternetes-network 2>&1 || echo "Network already exists"

# Stop and remove any existing containers
echo "Cleaning up existing containers..."
for container in rusternetes-etcd rusternetes-api-server rusternetes-scheduler rusternetes-controller-manager rusternetes-kubelet rusternetes-kubelet2 rusternetes-kube-proxy; do
    podman stop "$container" 2>/dev/null || true
    podman rm "$container" 2>/dev/null || true
done

# Set volumes path
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
echo "Using volumes path: $KUBELET_VOLUMES_PATH"

# Start etcd
echo "Starting etcd..."
podman run -d \
  --name rusternetes-etcd \
  --network rusternetes-network \
  --network-alias etcd \
  -p 2379:2379 -p 2380:2380 \
  -v rusternetes-etcd-data:/etcd-data \
  -e ETCDCTL_API=3 \
  --health-cmd "/usr/local/bin/etcdctl --endpoints=http://localhost:2379 endpoint health" \
  --health-interval 10s \
  --health-timeout 5s \
  --health-retries 5 \
  quay.io/coreos/etcd:v3.5.17 \
  /usr/local/bin/etcd \
  --name=etcd \
  --data-dir=/etcd-data \
  --listen-client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://etcd:2379 \
  --listen-peer-urls=http://0.0.0.0:2380 \
  --auto-compaction-retention=10m \
  --auto-compaction-mode=periodic \
  --snapshot-count=5000 \
  --quota-backend-bytes=8589934592

# Wait for etcd to be healthy
echo "Waiting for etcd to be healthy..."
sleep 5
while ! podman healthcheck run rusternetes-etcd 2>/dev/null; do
  echo "Waiting for etcd..."
  sleep 2
done
echo "etcd is healthy"

# Start API server
echo "Starting api-server..."
podman run -d \
  --name rusternetes-api-server \
  --network rusternetes-network \
  --network-alias api-server \
  -p 6443:6443 \
  -v ./.rusternetes/certs:/etc/kubernetes/pki:ro \
  -v /var/run/docker.sock:/var/run/docker.sock:rw \
  -e RUST_LOG=info \
  -e DOCKER_HOST=unix:///var/run/docker.sock \
  localhost/rusternetes_api-server \
  --bind-address 0.0.0.0:6443 \
  --etcd-servers http://etcd:2379 \
  --tls \
  --tls-cert-file /etc/kubernetes/pki/api-server.crt \
  --tls-key-file /etc/kubernetes/pki/api-server.key \
  --skip-auth \
  --console-dir /app/console \
  --log-level info

# Wait for api-server to start
echo "Waiting for api-server..."
sleep 3

# Start scheduler
echo "Starting scheduler..."
podman run -d \
  --name rusternetes-scheduler \
  --network rusternetes-network \
  -e RUST_LOG=info \
  localhost/rusternetes_scheduler \
  --etcd-servers http://etcd:2379 \
  --interval 1

# Start controller-manager
echo "Starting controller-manager..."
podman run -d \
  --name rusternetes-controller-manager \
  --network rusternetes-network \
  -v ./.rusternetes/certs:/etc/kubernetes/pki:ro \
  -e RUST_LOG=info \
  localhost/rusternetes_controller-manager \
  --etcd-servers http://etcd:2379 \
  --sync-interval 1

# Start kubelets
echo "Starting kubelet node-1..."
podman run -d \
  --name rusternetes-kubelet \
  --network rusternetes-network \
  --privileged \
  -v /var/run/docker.sock:/var/run/docker.sock:rw \
  -v ${KUBELET_VOLUMES_PATH}:${KUBELET_VOLUMES_PATH}:rw \
  -v ./.rusternetes/certs:/root/.rusternetes/certs:ro \
  -e RUST_LOG=info \
  -e DOCKER_HOST=unix:///var/run/docker.sock \
  -e KUBERNETES_SERVICE_HOST_OVERRIDE=api-server \
  -e KUBELET_VOLUMES_PATH=${KUBELET_VOLUMES_PATH} \
  localhost/rusternetes_kubelet \
  --node-name node-1 \
  --etcd-servers http://etcd:2379 \
  --cluster-dns 10.96.0.10 \
  --metrics-port 10250 \
  --sync-interval 3

echo "Starting kubelet node-2..."
podman run -d \
  --name rusternetes-kubelet2 \
  --network rusternetes-network \
  --privileged \
  -v /var/run/docker.sock:/var/run/docker.sock:rw \
  -v ${KUBELET_VOLUMES_PATH}:${KUBELET_VOLUMES_PATH}:rw \
  -v ./.rusternetes/certs:/root/.rusternetes/certs:ro \
  -e RUST_LOG=info \
  -e DOCKER_HOST=unix:///var/run/docker.sock \
  -e KUBERNETES_SERVICE_HOST_OVERRIDE=api-server \
  -e KUBELET_VOLUMES_PATH=${KUBELET_VOLUMES_PATH} \
  localhost/rusternetes_kubelet \
  --node-name node-2 \
  --etcd-servers http://etcd:2379 \
  --cluster-dns 10.96.0.10 \
  --metrics-port 10250 \
  --sync-interval 3

# Start kube-proxy
echo "Starting kube-proxy..."
podman run -d \
  --name rusternetes-kube-proxy \
  --network host \
  --privileged \
  --user 0:0 \
  --cap-add NET_ADMIN \
  --cap-add NET_RAW \
  --cap-add SYS_ADMIN \
  -e RUST_LOG=info \
  localhost/rusternetes_kube-proxy \
  --node-name node-1 \
  --etcd-servers http://localhost:2379

echo ""
echo "Cluster started successfully!"
echo ""
echo "Container status:"
podman ps --format "{{.Names}}\t{{.Status}}" | grep rusternetes

echo ""
echo "To bootstrap the cluster (CoreDNS, services, etc):"
echo "  bash scripts/bootstrap-cluster.sh"
