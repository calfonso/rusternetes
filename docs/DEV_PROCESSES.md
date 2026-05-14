# Development Processes

## Building and Deploying

### Build the cluster

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
podman compose build                               # Full cluster with etcd
podman compose -f compose.sqlite.yml build         # SQLite cluster (no etcd)
podman compose -f compose.redis.yml build          # Redis cluster (no etcd)
podman compose -f compose.all-in-one.yml build     # All-in-one with SQLite
podman compose -f compose.all-in-one-redis.yml build # All-in-one with Redis
```

### Deploy the cluster

```bash
podman compose up -d
bash scripts/bootstrap-cluster.sh
export KUBECONFIG=~/.kube/rusternetes-config
kubectl get nodes
```

### Rebuild a single component

To rebuild and redeploy just the API server (e.g., after console changes):

```bash
podman compose -f compose.sqlite.yml build api-server
podman compose -f compose.sqlite.yml up -d api-server
```

### Clean redeploy

When you need a fresh start:

```bash
podman compose down
podman compose build
podman compose up -d
bash scripts/bootstrap-cluster.sh
```

**Important:** Do not redeploy while conformance tests are running — it destroys the test evidence.

## Pre-Commit Checks

Run before every commit:

```bash
make pre-commit   # Format + clippy + test
```

Or individually:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

## Conformance Testing

### Run conformance tests

```bash
bash scripts/run-conformance.sh
```

### Monitor progress

```bash
bash scripts/conformance-progress.sh
```

### View test results

The e2e output is inside the e2e container:

```bash
docker exec sonobuoy-e2e-job-*_e2e cat /tmp/sonobuoy/results/e2e.log | tail -50
```

### Fix conformance issues

1. Analyze the failure in the e2e log — identify the exact test and error
2. Research the expected behavior in the Kubernetes source code
3. Implement the fix with a test that verifies correctness
4. Commit the fix: `git commit -m "fix: description of what was fixed"`
5. Update `docs/CONFORMANCE_FAILURES.md` with the status
6. Do not redeploy until you've fixed multiple issues — batch them

## Console Development

### Hot reload development

```bash
# Terminal 1: cluster running
podman compose up -d

# Terminal 2: console dev server
cd console
npm run dev
# Open http://localhost:3000/console/
```

### Rebuild console in Docker

```bash
podman compose -f compose.sqlite.yml build api-server
podman compose -f compose.sqlite.yml up -d --force-recreate api-server
```

## Package Names

Cargo package names use hyphens: `cargo test -p rusternetes-kubelet` (not underscores).

The api-server crate takes 5-10 minutes to compile test binaries. Use `cargo check` for fast iteration.
