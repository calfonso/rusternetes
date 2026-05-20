# Kubelet Node Conformance

Rusternetes runs the official Kubernetes v1.35 `e2e.test` suite focused on the `[NodeConformance]` label (191 specs in v1.35) against a single kubelet via `scripts/run-node-conformance.sh`.

This is **complementary** to the full Sonobuoy run tracked in `docs/CONFORMANCE.md`. Node conformance is narrower (kubelet-scoped) and isolates kubelet bugs from scheduler/controller-manager/kube-proxy noise.

> **Note on binary choice:** upstream `e2e_node.test` chroots into `/rootfs` during BeforeSuite and is designed for the legacy `registry.k8s.io/node-test:<version>` privileged container (end-of-lifed). `e2e.test` has the same `[NodeConformance]`-labelled specs and runs them via the api-server without rootfs setup, which is what we use.

## How to run

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
bash scripts/run-node-conformance.sh
```

The script:

1. Tears down any previous stack
2. Brings up `compose.node-conformance.yml` (etcd + api-server + one kubelet)
3. Waits for kubelet readiness on `:10250/healthz` (PR2 onward) or `:10249/metrics` (PR1 fallback)
4. Runs `scripts/bootstrap-cluster.sh` so the `kubernetes` service and default ServiceAccounts exist
5. Fetches `kubernetes-test-linux-amd64.tar.gz` for v1.35 if not cached, extracts `e2e.test` + `ginkgo` to `.bin/`
6. Runs `ginkgo --focus='\[NodeConformance\]' --skip='\[Slow\]|\[Flaky\]|\[Serial\]'`
7. Writes the full log to `/tmp/node-conformance/ginkgo.log` and prints PASS / FAIL / Ran-of-Specs summary

A full run takes ~30-60 minutes (each spec creates a pod and waits for it to reach a terminal state). Subset runs via `FOCUS='\[NodeConformance\].*Pods'` etc. are useful for iterating on a single failure class.

## Results

| Round | Date | Pass | Fail | Skip | Pass Rate | Notes |
|-------|------|------|------|------|-----------|-------|
| 1 | 2026-05-17 | TBD | TBD | TBD | TBD | Scaffold lands; baseline pending first long-form run after merge |

## Kubelet endpoints expected by `[NodeConformance]` specs

| Route | Status |
|-------|--------|
| `GET /pods` — pods bound to this node | ✅ on main (`crates/kubelet/src/server.rs`) |
| `GET /runningpods/` — running subset | ⏳ pending |
| `GET /healthz` — sync-loop liveness probe | ⏳ pending |
| `GET /stats/summary` — minimal cAdvisor shape | ⏳ pending |
| `GET /logs/:pod/:ns/:container` — log proxy | ⏳ pending |
| `POST /run/:pod/:ns/:container` — exec alias | ⏳ pending |

## Related

- `docs/CONFORMANCE.md` — full Sonobuoy suite
- [Upstream node conformance docs](https://kubernetes.io/docs/setup/best-practices/node-conformance/)
