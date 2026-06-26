# Rusternetes on Fedora Linux - Complete Setup Guide

This guide provides step-by-step instructions for installing and running Rusternetes on Fedora Linux using Podman in rootful mode.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Building Rusternetes](#building-rusternetes)
- [Starting the Cluster](#starting-the-cluster)
- [Bootstrapping](#bootstrapping)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)
- [Production Considerations](#production-considerations)

## Prerequisites

### System Requirements

- **Fedora Linux** 38+ (also works on RHEL/CentOS Stream 9+)
- **CPU**: 4+ cores recommended
- **RAM**: 8GB minimum, 16GB recommended
- **Disk**: 20GB free space minimum
- **Network**: Internet access for pulling container images

### User Requirements

- Root access (via `sudo`)
- User in `wheel` group (for sudo access)

## Installation

### 1. Install System Updates

```bash
# Update system
sudo dnf update -y

# Reboot if kernel was updated
sudo reboot
```

### 2. Install Development Tools

```bash
# Install essential build tools
sudo dnf groupinstall -y "Development Tools"

# Install additional required packages
sudo dnf install -y \
    git \
    curl \
    wget \
    gcc \
    gcc-c++ \
    make \
    openssl-devel \
    pkg-config \
    protobuf-compiler \
    protobuf-devel \
    lsof
```

### 3. Install Rust

```bash
# Install Rust using rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Follow prompts (default installation is fine)
# Source the environment
source $HOME/.cargo/env

# Verify installation
rustc --version
cargo --version
```

### 4. Install Container Runtime

You have two options: Podman (recommended for Fedora) or Docker.

#### Option A: Podman (Recommended)

```bash
# Install Podman and podman-compose
sudo dnf install -y podman podman-compose podman-plugins

# Verify installation
podman --version
podman-compose --version
```

**Important**: Rusternetes requires **rootful mode** for kube-proxy to access iptables.

#### Option B: Docker

```bash
# Install Docker
sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Start and enable Docker
sudo systemctl start docker
sudo systemctl enable docker

# Add your user to docker group (optional, for non-root access)
sudo usermod -aG docker $USER
newgrp docker

# Verify
docker --version
docker compose version
```

### 5. Configure Firewall (Optional)

If you plan to access the cluster from other machines:

```bash
# Allow API server port
sudo firewall-cmd --permanent --add-port=6443/tcp

# Allow etcd ports
sudo firewall-cmd --permanent --add-port=2379-2380/tcp

# Reload firewall
sudo firewall-cmd --reload
```

### 6. Disable SELinux (Optional, for Development)

**Note**: For production, keep SELinux enabled and configure proper contexts. For development, you may choose to set it to permissive:

```bash
# Set to permissive mode (survives reboot)
sudo sed -i 's/^SELINUX=enforcing/SELINUX=permissive/' /etc/selinux/config

# Set to permissive mode immediately
sudo setenforce 0

# Verify
getenforce
# Should show: Permissive
```

**Production Alternative**: Keep SELinux enforcing and use proper labels:
```bash
# When using volumes, add :Z flag for SELinux context
# This is already handled in the compose file
```

## Building Rusternetes

### 1. Clone the Repository

```bash
# Clone from GitHub
cd ~
git clone https://github.com/yourusername/rusternetes.git
cd rusternetes

# Or if you already have it
cd ~/rusternetes
git pull
```

### 2. Build Rust Binaries

```bash
# Build in release mode (optimized, slower compile)
cargo build --release

# Or debug mode (faster compile, for development)
cargo build

# Verify binaries
ls -lh target/release/
# Should see: api-server, scheduler, controller-manager, kubelet, kube-proxy, kubectl
```

This will take 5-15 minutes on first build as it downloads and compiles dependencies.

### 3. Build Container Images

Set the volumes path environment variable first:

```bash
# Set volumes path (REQUIRED)
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes

# Add to your shell profile for persistence
echo 'export KUBELET_VOLUMES_PATH=/home/'$(whoami)'/rusternetes/.rusternetes/volumes' >> ~/.bashrc
```

#### Using Podman (Rootful Mode)

```bash
# Build all images with Podman (rootful)
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml build

# This will build:
# - rusternetes-api-server
# - rusternetes-scheduler
# - rusternetes-controller-manager
# - rusternetes-kubelet
# - rusternetes-kube-proxy
# And pull: quay.io/coreos/etcd

# Verify images
sudo podman images | grep rusternetes
```

#### Using Docker

```bash
# Build all images with Docker
KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH docker-compose build

# Verify
docker images | grep rusternetes
```

Build time: 10-20 minutes depending on CPU and whether Rust cache exists.

## Starting the Cluster

### Using Podman (Rootful Mode)

```bash
# Ensure volumes directory exists
mkdir -p $KUBELET_VOLUMES_PATH

# Start the cluster in rootful mode
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml up -d

# Check status
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml ps

# Expected output: All containers in "Up" state
# - rusternetes-etcd
# - rusternetes-api-server
# - rusternetes-scheduler
# - rusternetes-controller-manager
# - rusternetes-kubelet
# - rusternetes-kube-proxy
```

### Using Docker

```bash
# Ensure volumes directory exists
mkdir -p $KUBELET_VOLUMES_PATH

# Start the cluster
KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH docker-compose up -d

# Check status
docker-compose ps
```

### Verify Services Started

```bash
# Check logs for any errors
sudo podman-compose -f compose.yml logs | grep -i error

# Or for Docker
docker-compose logs | grep -i error

# Check etcd health
sudo podman exec rusternetes-etcd etcdctl endpoint health
# Expected: 127.0.0.1:2379 is healthy

# Check API server is listening
curl -k https://localhost:6443/healthz
# Expected: (empty response or connection, not "connection refused")
```

## Bootstrapping

The cluster needs to be bootstrapped with core resources (namespaces, services, CoreDNS).

### 1. Apply Bootstrap Resources

```bash
# Expand environment variables in bootstrap config
cat bootstrap-cluster.yaml | envsubst > /tmp/bootstrap-expanded.yaml

# Apply using kubectl (skip TLS verification for self-signed certs)
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify apply -f /tmp/bootstrap-expanded.yaml

# Expected output:
# namespace/kube-system created
# namespace/default created
# service/kubernetes created
# endpoints/kubernetes created
# pod/coredns created
# service/kube-dns created
# configmap/coredns created
```

### 2. Wait for CoreDNS to Start

```bash
# Wait for CoreDNS pod to be running (may take 30-60 seconds)
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get pods -n kube-system -w

# Press Ctrl+C once STATUS shows "Running"
```

### 3. Verify Bootstrap

```bash
# Check all namespaces
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get namespaces

# Check services
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get svc -A

# Expected services:
# default       kubernetes    10.96.0.1     443
# kube-system   kube-dns      10.96.0.10    53,53,9153

# Check pods
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get pods -A

# Expected: CoreDNS pod in Running status
```

## Verification

### 1. Verify Kube-proxy Iptables Access

```bash
# Check kube-proxy logs (using Podman)
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml logs kube-proxy | grep -i iptables

# Expected to see:
# "Iptables chains initialized successfully"
# "Kube-proxy initialized successfully"

# Should NOT see:
# "Permission denied"

# Or with Docker
docker-compose logs kube-proxy | grep -i iptables
```

### 2. Verify Pod IPs

```bash
# Check that pods have IPs assigned
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get pods -n kube-system coredns -o json | grep podIp

# Expected: "podIp": "172.18.0.X"
```

### 3. Verify Service Endpoints

```bash
# Check that endpoints are populated
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get endpoints -n kube-system kube-dns

# Expected to see pod IP in ENDPOINTS column
```

### 4. Test DNS Resolution

```bash
# Create a test pod
cat <<'EOF' | KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: test-dns
  namespace: default
spec:
  containers:
  - name: test
    image: busybox:latest
    command: ["sleep", "3600"]
EOF

# Wait for pod to be ready
sleep 10

# Get the container ID
TEST_CONTAINER=$(sudo podman ps | grep test-dns_test | awk '{print $1}')

# Or for Docker
TEST_CONTAINER=$(docker ps | grep test-dns_test | awk '{print $1}')

# Test DNS resolution via service IP
sudo podman exec $TEST_CONTAINER nslookup kubernetes.default.svc.cluster.local 10.96.0.10

# Or with Docker
docker exec $TEST_CONTAINER nslookup kubernetes.default.svc.cluster.local 10.96.0.10

# Expected output:
# Server:    10.96.0.10
# Address:   10.96.0.10:53
# Name:      kubernetes.default.svc.cluster.local
# Address:   10.96.0.1
```

Success! DNS resolution is working through the service IP.

### 5. Test Creating a Deployment

```bash
# Create a simple deployment
cat <<'EOF' | KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-test
  namespace: default
spec:
  replicas: 2
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:latest
        ports:
        - containerPort: 80
EOF

# Wait for pods to be created
sleep 15

# Check deployment
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get deployments
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get pods -l app=nginx

# Expected: 2 pods in Running state
```

## Troubleshooting

### Podman Permission Errors

**Problem**: "Permission denied" errors from kube-proxy

**Solution**: Make sure you're running in rootful mode with `sudo`:
```bash
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml up -d
```

### Container Build Fails

**Problem**: Out of disk space or network issues

**Solution**:
```bash
# Check disk space
df -h

# Clean up old images and containers
sudo podman system prune -a -f

# Or with Docker
docker system prune -a -f

# Retry build
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml build
```

### etcd Won't Start

**Problem**: etcd container fails to start

**Solution**:
```bash
# Check logs
sudo podman logs rusternetes-etcd

# Remove and recreate volume
sudo podman volume rm rusternetes-etcd-data
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml up -d etcd
```

### API Server Not Responding

**Problem**: Cannot connect to API server

**Solution**:
```bash
# Check if container is running
sudo podman ps | grep api-server

# Check logs
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml logs api-server

# Verify etcd is healthy
sudo podman exec rusternetes-etcd etcdctl endpoint health

# Restart API server
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml restart api-server
```

### CoreDNS Not Starting

**Problem**: CoreDNS pod stuck in Pending

**Solution**:
```bash
# Check kubelet logs
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml logs kubelet

# Check if volumes directory exists and is writable
ls -la $KUBELET_VOLUMES_PATH

# Delete and recreate pod
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify delete pod coredns -n kube-system
cat bootstrap-cluster.yaml | envsubst > /tmp/bootstrap-expanded.yaml
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify apply -f /tmp/bootstrap-expanded.yaml
```

### Port Already in Use

**Problem**: "port is already allocated" error

**Solution**:
```bash
# Find what's using the ports
sudo lsof -i :6443
sudo lsof -i :2379

# Stop conflicting service or change ports in docker-compose.yml
```

### SELinux Denials

**Problem**: SELinux blocking container operations

**Solution** (temporary):
```bash
# Check for denials
sudo ausearch -m avc -ts recent

# Set to permissive (for development only)
sudo setenforce 0
```

**Solution** (production):
```bash
# Keep enforcing and use proper labels
# Volume mounts use :Z flag (already in docker-compose.yml)
# Check logs: sudo ausearch -m avc -ts recent
# Create policy if needed
```

## Production Considerations

### System Service Setup

Create a systemd service for automatic startup:

```bash
# Create service file
sudo tee /etc/systemd/system/rusternetes.service > /dev/null <<EOF
[Unit]
Description=Rusternetes Kubernetes Cluster
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/$(whoami)/rusternetes
Environment="KUBELET_VOLUMES_PATH=/home/$(whoami)/rusternetes/.rusternetes/volumes"
ExecStart=/usr/bin/podman-compose -f compose.yml up -d
ExecStop=/usr/bin/podman-compose -f compose.yml down
User=root

[Install]
WantedBy=multi-user.target
EOF

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable rusternetes
sudo systemctl start rusternetes

# Check status
sudo systemctl status rusternetes
```

### Resource Limits

Configure proper resource limits in `/etc/containers/containers.conf` or docker daemon:

```bash
# For Podman
sudo mkdir -p /etc/containers
sudo tee /etc/containers/containers.conf > /dev/null <<EOF
[containers]
default_ulimits = [
  "nofile=65536:65536",
]
EOF
```

### Monitoring

Set up basic monitoring:

```bash
# Install metrics tools
sudo dnf install -y sysstat htop iotop

# Check resource usage
htop
iotop
sudo podman stats
```

### Backups

Backup etcd data regularly:

```bash
# Create backup script
sudo tee /usr/local/bin/backup-rusternetes-etcd.sh > /dev/null <<'EOF'
#!/bin/bash
BACKUP_DIR="/var/backups/rusternetes"
mkdir -p $BACKUP_DIR
DATE=$(date +%Y%m%d-%H%M%S)
podman exec rusternetes-etcd etcdctl snapshot save /tmp/backup-$DATE.db
podman cp rusternetes-etcd:/tmp/backup-$DATE.db $BACKUP_DIR/
podman exec rusternetes-etcd rm /tmp/backup-$DATE.db
# Keep only last 7 backups
find $BACKUP_DIR -name "backup-*.db" -mtime +7 -delete
EOF

sudo chmod +x /usr/local/bin/backup-rusternetes-etcd.sh

# Add to crontab (daily at 2am)
(sudo crontab -l 2>/dev/null; echo "0 2 * * * /usr/local/bin/backup-rusternetes-etcd.sh") | sudo crontab -
```

### Security Hardening

1. **Keep SELinux enabled** in production
2. **Use TLS certificates** properly (not self-signed)
3. **Enable RBAC** authentication (remove `--skip-auth`)
4. **Configure firewall** to restrict access
5. **Regular updates**: `sudo dnf update -y`

### High Availability

For production HA setup:
- Use `docker-compose.ha.yml` for multi-master setup
- Set up external load balancer (HAProxy)
- Run etcd cluster (3-5 nodes)
- See [HIGH_AVAILABILITY.md](HIGH_AVAILABILITY.md) for details

## Quick Reference

### Common Commands

```bash
# Start cluster
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml up -d

# Stop cluster
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml down

# View logs (all services)
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml logs -f

# View logs (specific service)
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml logs -f api-server

# Restart service
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml restart api-server

# Check cluster status
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get nodes
KUBECONFIG=/dev/null ./target/release/kubectl --insecure-skip-tls-verify get pods -A

# Clean restart
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml down -v
sudo podman system prune -a -f
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml up -d
```

### Environment Variables

Always set before running compose:
```bash
export KUBELET_VOLUMES_PATH=/home/$(whoami)/rusternetes/.rusternetes/volumes
```

Or use inline:
```bash
sudo KUBELET_VOLUMES_PATH=$KUBELET_VOLUMES_PATH podman-compose -f compose.yml [command]
```

## Next Steps

- Explore example manifests in `examples/`
- Try creating Deployments, Services, StatefulSets
- Set up MetalLB for LoadBalancer support
- Run conformance tests
- See [DEVELOPMENT.md](DEVELOPMENT.md) for daily development workflows

## Support

- **Documentation**: See `docs/` directory
- **Troubleshooting**: [DEVELOPMENT.md](DEVELOPMENT.md#troubleshooting)
- **Issues**: Report on GitHub
- **Podman tips**: [PODMAN_TIPS.md](PODMAN_TIPS.md)

## Summary Checklist

✅ System updated
✅ Rust installed
✅ Podman/Docker installed
✅ Firewall configured (optional)
✅ SELinux configured
✅ Repository cloned
✅ Binaries built
✅ Container images built
✅ Cluster started
✅ Bootstrap applied
✅ DNS working
✅ Test pod created successfully

Congratulations! Your Rusternetes cluster is now running on Fedora Linux.
