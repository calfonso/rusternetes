# Container Runtime Support

Rusternetes supports both **Podman** and **Docker** as container runtimes on all platforms.

## Platform Support

### macOS

- **Podman Machine** — works on macOS with Apple Silicon and Intel. Requires rootful mode for kube-proxy iptables access. See [DEVELOPMENT.md](DEVELOPMENT.md) for setup.
- **Docker Desktop** — also supported. See [DEVELOPMENT.md](DEVELOPMENT.md) for setup.

### Linux

- **Podman** — rootful mode required for kube-proxy iptables access (`sudo` or rootful configuration)
- **Docker** — standard installation

### Windows

- **Docker Desktop** — recommended
- **WSL2** — with Docker or Podman

## Compose Files

Rusternetes provides separate compose files for each runtime:

| Runtime | etcd | SQLite | Redis | HA |
|---------|------|--------|-------|----|
| Podman | `compose.yml` | `compose.sqlite.yml` | `compose.redis.yml` | `compose.ha.yml` |
| Docker | `docker-compose.yml` | `docker-compose.sqlite.yml` | — | `docker-compose.ha.yml` |

All-in-one variants (Podman only):

| Storage | Compose file |
|---------|-------------|
| Embedded SQLite | `compose.all-in-one.yml` |
| Redis | `compose.all-in-one-redis.yml` |

The key difference is the container socket path:
- **Podman:** `/run/podman/podman.sock`
- **Docker:** `/var/run/docker.sock`

## Quick Start

### Podman (macOS/Linux)

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
podman compose build
podman compose up -d
bash scripts/bootstrap-cluster.sh
```

### Docker (macOS/Linux/Windows)

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
docker compose build
docker compose up -d
bash scripts/bootstrap-cluster.sh
```

## Rootful Mode (Podman)

Kube-proxy requires `CAP_NET_ADMIN` to configure iptables rules for service routing.

**macOS:**
```bash
podman machine init --memory 8192 --cpus 4
podman machine set --rootful
podman machine start
```

**Linux:**
```bash
# Prefix compose commands with sudo
sudo KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes podman compose up -d
```

## Code Architecture

The kubelet uses [bollard](https://github.com/fussybeaver/bollard) for container management, which speaks the Docker API. Both Podman and Docker expose this API, so the same code works with either runtime.

CNI networking automatically detects the container environment and falls back to bridge networking when CNI plugins are not available.

## Historical Note

An earlier version of this document (March 2026) recommended Docker Desktop on macOS due to a vfkit bug in macOS Sequoia 15.7+ that prevented Podman Machine VMs from starting. This issue has since been resolved and Podman Machine works correctly on current macOS versions.

## Related Documentation

- [DEVELOPMENT.md](DEVELOPMENT.md) — full development setup guide
- [PODMAN_TIPS.md](PODMAN_TIPS.md) — Podman tips and tricks
- [CNI Guide](CNI_GUIDE.md) — CNI networking details
