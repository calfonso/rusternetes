# Kubelet Configuration Guide


> **Tip:** Node status, CPU/memory utilization, and pod assignments are visible in the [web console](CONSOLE_USER_GUIDE.md) Nodes and Topology pages.
This guide explains how to configure the Rusternetes Kubelet using production-grade configuration methods.

## Table of Contents

- [Overview](#overview)
- [Configuration Methods](#configuration-methods)
- [Configuration Precedence](#configuration-precedence)
- [CLI Flags Reference](#cli-flags-reference)
- [Configuration File Reference](#configuration-file-reference)
- [Examples](#examples)
- [Migration from Environment Variables](#migration-from-environment-variables)

## Overview

The Rusternetes Kubelet supports three configuration methods, designed to work seamlessly in both development and production environments:

1. **CLI Flags** - Direct command-line arguments (highest precedence)
2. **Configuration File** - YAML file following Kubernetes conventions
3. **Environment Variables** - Legacy support for development (lowest precedence)

This design provides:
- **Discoverability**: CLI flags are visible in `--help` and process listings
- **Validation**: Early detection of configuration errors at startup
- **Flexibility**: Mix and match configuration methods as needed
- **Compatibility**: Backward compatible with existing environment variable setup

## Configuration Methods

### 1. CLI Flags (Recommended)

The most explicit and discoverable method. Perfect for:
- Container orchestration (Docker Compose, Kubernetes)
- Systemd units
- Quick testing and debugging

```bash
kubelet \
  --node-name=worker-1 \
  --etcd-servers=http://localhost:2379 \
  --volume-dir=/var/lib/kubelet/volumes \
  --root-dir=/var/lib/kubelet \
  --log-level=info \
  --sync-interval=10 \
  --metrics-port=8082
```

### 2. Configuration File (Production)

Structured YAML configuration following Kubernetes `KubeletConfiguration` API. Best for:
- Production deployments
- Complex configurations
- Version control and auditability

```bash
kubelet \
  --node-name=worker-1 \
  --etcd-servers=http://localhost:2379 \
  --config=/etc/kubernetes/kubelet-config.yaml
```

### 3. Environment Variables (Legacy)

Simple environment-based configuration. Maintained for:
- Backward compatibility
- Development convenience
- Docker Compose simplicity

```bash
export KUBELET_VOLUMES_PATH=/tmp/rusternetes-volumes
export KUBELET_ROOT_DIR=/var/lib/kubelet
export RUST_LOG=debug

kubelet --node-name=node-1 --etcd-servers=http://localhost:2379
```

## Configuration Precedence

When multiple configuration sources are used, they are merged in the following order (highest to lowest priority):

1. **CLI Flags** - Override everything
2. **Configuration File** - Overrides environment variables
3. **Environment Variables** - Overrides defaults
4. **Built-in Defaults** - Fallback values

### Example Precedence

```bash
# Configuration file sets volumeDir: /config/volumes
# Environment variable sets KUBELET_VOLUMES_PATH=/env/volumes
# CLI flag sets --volume-dir=/cli/volumes

kubelet --node-name=node-1 --config=config.yaml --volume-dir=/cli/volumes

# Result: Uses /cli/volumes (CLI flag wins)
```

## CLI Flags Reference

### Required Flags

| Flag | Type | Description |
|------|------|-------------|
| `--node-name` | string | Name of the node this kubelet is running on |
| `--etcd-servers` | string | Comma-separated list of etcd endpoints (default: `http://localhost:2379`) |

### Optional Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config` | path | - | Path to kubelet configuration file |
| `--root-dir` | path | `./` (dev)<br>`/var/lib/kubelet` (prod) | Root directory for kubelet files |
| `--volume-dir` | path | `<root-dir>/volumes` | Directory for volume data |
| `--volume-plugin-dir` | path | `/usr/libexec/kubernetes/kubelet-plugins/volume/exec` | Directory for volume plugins |
| `--log-level` | string | `info` | Log verbosity (trace, debug, info, warn, error) |
| `--sync-interval` | uint64 | `10` | Pod sync frequency in seconds |
| `--metrics-port` | uint16 | `8082` | Port for metrics server |

### Help and Version

```bash
kubelet --help     # Show all available flags
kubelet --version  # Show version information
```

## Configuration File Reference

The configuration file uses the Kubernetes `KubeletConfiguration` API format:

```yaml
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration

# Root directory for managing kubelet files
rootDir: /var/lib/kubelet

# Directory path for managing volume data
volumeDir: /var/lib/kubelet/volumes

# Directory where volume plugins are installed
volumePluginDir: /usr/libexec/kubernetes/kubelet-plugins/volume/exec

# How frequently to sync pod state (in seconds)
syncFrequency: 10

# Port for the metrics server
metricsBindPort: 8082

# Log verbosity level
logLevel: info
```

### Field Descriptions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `apiVersion` | string | Yes | Must be `kubelet.config.k8s.io/v1beta1` |
| `kind` | string | Yes | Must be `KubeletConfiguration` |
| `rootDir` | string | No | Root directory for kubelet state |
| `volumeDir` | string | No | Directory for volume data |
| `volumePluginDir` | string | No | Directory for volume plugins |
| `syncFrequency` | integer | No | Sync interval in seconds (must be > 0) |
| `metricsBindPort` | integer | No | Metrics server port |
| `logLevel` | string | No | One of: trace, debug, info, warn, error |

### Validation Rules

- `apiVersion` must be `kubelet.config.k8s.io/v1beta1`
- `kind` must be `KubeletConfiguration`
- `syncFrequency` must be greater than 0
- `logLevel` must be one of: trace, debug, info, warn, error
- Directories will be created if they don't exist

## Examples

### Development Configuration

For local development with verbose logging:

```yaml
# kubelet-config-dev.yaml
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration

rootDir: ./kubelet-data
volumeDir: ./volumes
logLevel: debug
syncFrequency: 5
metricsBindPort: 8082
```

```bash
kubelet --node-name=dev-node \
        --etcd-servers=http://localhost:2379 \
        --config=kubelet-config-dev.yaml
```

### Production Configuration

For production with standard paths:

```yaml
# /etc/kubernetes/kubelet-config.yaml
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration

rootDir: /var/lib/kubelet
volumeDir: /var/lib/kubelet/volumes
volumePluginDir: /usr/libexec/kubernetes/kubelet-plugins/volume/exec
logLevel: info
syncFrequency: 10
metricsBindPort: 10255
```

```bash
kubelet --node-name=worker-01 \
        --etcd-servers=https://etcd-1:2379,https://etcd-2:2379,https://etcd-3:2379 \
        --config=/etc/kubernetes/kubelet-config.yaml
```

### Systemd Unit

Production deployment using systemd:

```ini
# /etc/systemd/system/kubelet.service
[Unit]
Description=Rusternetes Kubelet
Documentation=https://github.com/yourusername/rusternetes
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/kubelet \
  --node-name=%H \
  --etcd-servers=http://localhost:2379 \
  --config=/etc/kubernetes/kubelet-config.yaml
Restart=always
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

### Docker Compose

#### Using CLI Flags (Recommended)

```yaml
services:
  kubelet:
    image: rusternetes/kubelet:latest
    command:
      - "--node-name=node-1"
      - "--etcd-servers=http://etcd:2379"
      - "--volume-dir=/volumes"
      - "--log-level=debug"
    volumes:
      - /run/containerd/containerd.sock:/run/containerd/containerd.sock
      - ./volumes:/volumes
```

#### Using Config File

```yaml
services:
  kubelet:
    image: rusternetes/kubelet:latest
    command:
      - "--node-name=node-1"
      - "--etcd-servers=http://etcd:2379"
      - "--config=/etc/kubernetes/kubelet-config.yaml"
    volumes:
      - /run/containerd/containerd.sock:/run/containerd/containerd.sock
      - ./volumes:/volumes
      - ./kubelet-config.yaml:/etc/kubernetes/kubelet-config.yaml:ro
```

### Override Specific Settings

Mix configuration file with CLI overrides:

```bash
# Use config file for most settings, override volume dir via CLI
kubelet --node-name=node-1 \
        --etcd-servers=http://localhost:2379 \
        --config=/etc/kubernetes/kubelet-config.yaml \
        --volume-dir=/mnt/fast-ssd/volumes
```

## Migration from Environment Variables

If you're currently using environment variables, here's how to migrate:

### Old Approach (Environment Variables)

```bash
export KUBELET_VOLUMES_PATH=/tmp/volumes
export RUST_LOG=debug

kubelet --node-name=node-1 --etcd-servers=http://localhost:2379
```

### New Approach (CLI Flags)

```bash
kubelet --node-name=node-1 \
        --etcd-servers=http://localhost:2379 \
        --volume-dir=/tmp/volumes \
        --log-level=debug
```

### New Approach (Config File)

```yaml
# kubelet-config.yaml
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
volumeDir: /tmp/volumes
logLevel: debug
```

```bash
kubelet --node-name=node-1 \
        --etcd-servers=http://localhost:2379 \
        --config=kubelet-config.yaml
```

### Backward Compatibility

Environment variables are still supported! Your existing setup will continue to work:

```bash
# This still works
export KUBELET_VOLUMES_PATH=/tmp/volumes
kubelet --node-name=node-1 --etcd-servers=http://localhost:2379
```

However, CLI flags and config files will override environment variables if specified.

## Troubleshooting

### View Effective Configuration

The kubelet logs its effective configuration at startup:

```
INFO Starting Rusternetes Kubelet
INFO Kubelet Runtime Configuration:
  Node Name: worker-1
  Root Directory: /var/lib/kubelet
  Volume Directory: /var/lib/kubelet/volumes
  Volume Plugin Directory: /usr/libexec/kubernetes/kubelet-plugins/volume/exec
  Sync Frequency: 10s
  Metrics Port: 8082
  Log Level: info
  Etcd Endpoints: http://localhost:2379
```

### Common Issues

#### Invalid Configuration File

```
Error: Failed to parse config file: /etc/kubernetes/kubelet-config.yaml
Caused by: Invalid kind: Pod. Expected: KubeletConfiguration
```

**Solution**: Ensure your config file has the correct `apiVersion` and `kind` fields.

#### Directory Creation Failed

```
Error: Failed to create volume directory: /var/lib/kubelet/volumes
Caused by: Permission denied (os error 13)
```

**Solution**: Ensure the kubelet has write permissions to the specified directories, or run with appropriate privileges.

#### Conflicting Configurations

If you see unexpected behavior, check the precedence order. CLI flags always win over config files and environment variables.

## Best Practices

1. **Production**: Use config files for maintainability and auditability
2. **Development**: Use CLI flags or environment variables for simplicity
3. **Testing**: Use CLI flags to override specific settings
4. **Documentation**: Always specify configuration explicitly rather than relying on defaults
5. **Validation**: Run `kubelet --help` to see all available options and their defaults

## Comparison with Kubernetes

This implementation follows Kubernetes conventions:

| Aspect | Rusternetes | Kubernetes |
|--------|-------------|------------|
| Config API | `kubelet.config.k8s.io/v1beta1` | `kubelet.config.k8s.io/v1beta1` |
| CLI Flags | `--root-dir`, `--volume-dir` | `--root-dir`, `--volume-plugin-dir` |
| Default Paths | `/var/lib/kubelet/volumes` | `/var/lib/kubelet/pods/{uid}/volumes` |
| Config File | YAML, `--config` flag | YAML, `--config` flag |
| Precedence | CLI > File > Env > Default | CLI > File > Default |

Key differences:
- Rusternetes adds environment variable support for development convenience
- Rusternetes simplifies volume path structure (no pod UID subdirectories)
- Rusternetes combines root-dir-based defaults for easier setup

## See Also

- [Storage Backends](storage/STORAGE_BACKENDS.md)
- [Development Guide](DEVELOPMENT.md)
- [Quick Start](QUICKSTART.md)
