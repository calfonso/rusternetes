# Node Conformance — baseline & backlog

The `Node Conformance` workflow (`.github/workflows/node-conformance.yml`) runs
the upstream `e2e.test` with the `[NodeConformance]` focus against a single-node
compose stack (`compose.node-conformance.yml` + `scripts/run-node-conformance.sh`).

## What changed (why it had never passed)

Historically the job ran **8 of ~190 specs, 0 passed, then hit the suite
timeout**. Two infrastructure gaps caused it — both unrelated to actual kubelet
behaviour:

1. **No scheduler.** The stack was etcd + api-server + kubelet only. The
   upstream `e2e.test` creates pods with `schedulerName: default-scheduler` and
   no `spec.nodeName`, so with no scheduler every test pod sat `Pending`
   forever. Each spec's `CreateSync` then burned its full 5–7 min pod-start
   timeout, and the suite hit the default 1 h ginkgo cap after only ~8 specs.
2. **No parallelism / no suite-timeout override.** ~190 specs run serially at
   ~30–60 s each cannot finish inside ginkgo's default 1 h timeout even once
   pods schedule.

Fixes:

- Added a **scheduler** service to `compose.node-conformance.yml` (the
  per-namespace `kube-root-ca.crt` ConfigMap and default ServiceAccount the
  pods' projected `kube-api-access` volume needs are created by the api-server
  on namespace creation, so no controller-manager is required).
- Migrated the stack from **etcd to Rhino + SQLite** to match the conformance
  canary and CI (`compose.sqlite.yml`).
- `scripts/run-node-conformance.sh` now passes `--nodes` (default 4) and
  `--timeout` (default 85m); the workflow job timeout is 120m.

## Current baseline

A full local run (`--nodes=4`, real CI focus/skip) on the migrated stack:

```
Ran 191 of 7348 Specs in ~3161 seconds
139 Passed | 52 Failed | 0 Pending | 7157 Skipped
```

The suite now completes and reports a real signal. The remaining **52 failures
are genuine kubelet feature gaps** — the work to take the job fully green.

## Reproduce locally

```bash
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
docker compose -f compose.node-conformance.yml -f compose.dind.node-conformance.yml up -d --build
bash scripts/generate-certs.sh                  # if .rusternetes/certs is empty
bash scripts/bootstrap-cluster.sh
CONTAINER_RUNTIME=docker bash scripts/run-node-conformance.sh
# Narrow with e.g. FOCUS='Probing container' GINKGO_NODES=4
```

## Remaining failures (52), grouped

These are tracked in the task queue as a campaign. Categories:

### Probing — container + restartable init container (25)
Liveness/readiness/startup probe restart semantics (exec, http `/healthz`,
tcp, GRPC, redirect handling), `terminationGracePeriodSeconds` override,
readiness-during-termination, monotonic restart count. The restartable-init
variants are `[FeatureGate:SidecarContainers]`.

### Container lifecycle hooks (10)
`postStart`/`preStop` exec, http, and https hooks — regular containers and
restartable init containers.

### EmptyDir tmpfs volumes (7)
`(root|non-root, 0644|0666|0777, tmpfs)` mode/permission bits and
"volume on tmpfs should have the correct mode".

### Networking granular checks (4)
intra-pod and node-pod communication, http and udp (pod-to-pod networking on
the single node).

### Other (6)
- Sysctls (2): unsafe-sysctl rejection; slash-separator sysctl names.
- Projected configMap with mappings + Item mode (1).
- Pods readiness gates (1).
- PodOSRejection — reject pod when node OS ≠ pod OS (1).
- Kubelet hostAliases + hostNetwork: write entries to `/etc/hosts` (1).

The complete verbatim list lives in this run's ginkgo log
(uploaded as the `node-conformance-log-<run_id>` artifact).

## Path to green

Two options, to decide per project preference:

1. **Fix the 52** across the categories above (multi-PR campaign), keeping the
   job red until each category lands.
2. **Known-green baseline gate** — mirror the main conformance canary: skip the
   currently-failing specs so the job goes green on the 139 now, and shrink the
   skip list as fixes land. This makes "no regression in the passing set" the
   gate immediately.
