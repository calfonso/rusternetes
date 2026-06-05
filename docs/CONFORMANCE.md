# Kubernetes v1.35 Conformance

Rusternetes is a from-scratch Rust reimplementation of Kubernetes. This document tracks conformance testing progress against the official Kubernetes v1.35 e2e conformance suite.

> **Faster signal for kubelet-only regressions:** see [`docs/NODE_CONFORMANCE.md`](NODE_CONFORMANCE.md) — a single-kubelet harness that runs the `[NodeConformance]`-tagged subset in minutes, scoped to catch kubelet bugs without scheduler / controller-manager / kube-proxy noise.

## Current snapshot

**Every conformance figure in this repo is dated and tagged with the storage backend it was measured on.** etcd and the SQLite/rhino backend exercise different code paths and do not produce the same numbers — never treat an older etcd figure (e.g. the Round 160 / 94.1% below) as the current baseline.

| Date | OS | Backend | Image | Commit | Pass | Fail | Ran | Pass rate |
|------|----|---------|-------|--------|------|------|-----|-----------|
| 2026-06-02 | Zorin OS 18 | SQLite / rhino | `conformance:v1.35.0` | `e1758455` | 373 | 64 | 441 | 84.6% |
| 2026-05-31 | — | SQLite / rhino | `conformance:v1.35.0` | `e9c9f507` | 347 | 99 | 446 | 77.8% |

Hydrophone, full `[Conformance]` suite (441), multi-container SQLite stack (`compose.sqlite.yml` + `compose.dind.yml`), rhino submodule. Per-test PASS/FAIL per run (verbatim ginkgo keys, now date/OS/commit-tagged): [`conformance/PER_TEST_RESULTS.md`](conformance/PER_TEST_RESULTS.md).

**2026-06-02 (`e1758455`):** 373/441 (84.6%), up from 342 on 2026-05-31 — driven by PRs #886–#938 (RoleRef/CRD/decode Go-parity, CRD serving + OpenAPI publish, admission-webhook invocation, ReplicationController/GC lifecycle, RuntimeClass, InPlace resize, ServiceAccount token mount, kubectl client, Services endpoints). `known-green.txt` grew 323 → 385; this batch closes the backlog of conformance issues whose tests are now green. The remaining 64 failures are SchedulerPredicates/Preemption `[Serial]`, PreStop, InPlace-resize-via-replace, KubeletManagedEtcHosts, InitContainer, Aggregator sample API server, CSI PV/StorageClass/VolumeAttachment lifecycle, and Subpath — tracked in the live-cluster task set.

**2026-06-05:** `[sig-storage] PersistentVolumes CSI Conformance should run through the lifecycle of a PV and a PVC [Conformance]` (issue #591) verified **green** via focused Hydrophone on the SQLite/rhino stack (`SUCCESS! -- 1 Passed | 0 Failed`) — the PV/PVC API contract (create, list-by-labelSelector, patch, UID read, delete+confirm, PUT update, deleteCollection) was already complete; the test was simply never ratcheted. Added to `known-green.txt` and guarded by the router-level mirror `crates/api-server/tests/conformance_pv_csi_lifecycle_http.rs`.

This is the first full conformance run on the SQLite/rhino backend after migrating off etcd and landing a batch of breaking storage/watch changes. It is **not** comparable to the historical etcd peak below (Round 160, 94.1%): a different backend, and many fixes for SQLite-specific behaviour are still in flight. The 99 failures cluster tightly, and most are already being addressed:

- **~22 AdmissionWebhook** — all fail in setup creating a RoleBinding whose `roleRef` omits `apiGroup`; the decode layer rejected it. Fixed by the RoleRef Go-parity decode change (PR #890).
- **~20 CRD family** — CustomResourceDefinition, CustomResourcePublishOpenAPI, FieldValidation, AggregatedDiscovery, ConversionWebhook (CRD decode/handling).
- Remainder — garbage collector, FlowSchema, Aggregator, ResourceQuota, status subresources, API-chunking, Table 406.

## Historical rounds (etcd backend, pre-SQLite migration)

> These rounds ran on the **etcd** backend via Sonobuoy on Docker Desktop, March–April 2026. They predate the SQLite/rhino migration and the breaking storage/watch changes, so **Round 160's 94.1% is a historical etcd figure, not the current baseline** — see the dated snapshot above.

The official Kubernetes conformance suite is 441 `[Conformance]` tests.

| Round | Date       | Pass | Fail | Pass Rate | Notes |
|-------|------------|------|------|-----------|-------|
| 103   | 2026-03-10 | 245  | 196  | 56%       | Initial baseline |
| 104   | 2026-03-14 | 405  | 36   | 92%       | Major fix batch |
| 105   | 2026-03-17 | ~410 | ~31  | 93%       | |
| 106   | 2026-03-20 | ~416 | ~25  | 94%       | |
| 107   | 2026-03-23 | ~422 | ~19  | 96%       | Best deployed result |
| 108   | 2026-03-27 | 263  | 178  | 60%       | Regression (interaction bugs) |
| 110   | 2026-03-29 | 283  | 158  | 64%       | Fixes committed, not yet deployed |
| 116   | 2026-03-31 | 128  | 94   | 58%       | Pre-deploy, watch cancel loops |
| 117   | 2026-03-31 | 89   | 44   | 67%       | Partial run, first deploy of session fixes |
| 118   | 2026-04-01 | 299  | 142  | 68%       | Full run, all major fixes deployed |
| 119   | 2026-04-01 | —    | —    | —         | Pre-fix baseline, 16 fixes pending |
| 120   | 2026-04-01 | —    | —    | —         | Round with 16 new fixes deployed |
| 125   | 2026-04-04 | 329  | 112  | 74.6%     | New high score — 30 fixes deployed |
| 127   | 2026-04-07 | 397  | 44   | 90.0%     | Pre-regression baseline |
| 132   | 2026-04-09 | 363  | 78   | 82.3%     | First round with major fixes deployed |
| 133   | 2026-04-10 | 370  | 71   | 83.9%     | 47 fixes deployed, 18 staged |
| 135   | 2026-04-11 | 373  | 68   | 84.6%     | Previous high score |
| 146   | 2026-04-15 | 379  | 62   | 85.9%     | 16 fixes deployed |
| 147   | 2026-04-16 | 398  | 43   | 90.2%     | 31 fixes deployed |
| 155   | 2026-04-24 | 403  | 38   | 91.4%     | Previous high score |
| 159   | 2026-04-25 | 410  | 31   | 93.0%     | Previous high score |
| 160   | 2026-04-26 | 415  | 26   | 94.1%     | New high score |

**Historical etcd peak**: Round 160 at 94.1% (415/441) on 2026-04-26 — superseded as the reference point by the dated [Current snapshot](#current-snapshot) above. This number reflects the etcd backend before the SQLite migration and is no longer the baseline.

**Total commits**: 1,534+ across 30+ rounds of iterative testing and debugging.

## Failure Categories

Based on Round 147 analysis (43 failures):

- **CRD OpenAPI publishing (~9)**: CRD schema definitions in /openapi/v2 missing fields or x-kubernetes-group-version-kind after update/rename.
- **Service networking (~6)**: Session affinity (NodePort and ClusterIP), basic endpoint serving, endpoint latency, service status lifecycle.
- **Webhook admission (~5)**: Deny pod/configmap creation, deny attach, deny CR CRUD, mutate CR with pruning, webhook timeout.
- **EmptyDir volume perms (~4)**: macOS Docker bind mounts don't support 0666/0777 mode bits.
- **Apps controllers (~3)**: Deployment proportional scaling/rollover, ReplicaSet/RC basic image serving.
- **Proxy/Aggregator (~3)**: Proxy through service/pod, proxy valid responses, aggregator sample API server.
- **Node lifecycle (~3)**: Container runtime exit status, preStop hook, exec over websockets.
- **Init containers (~2)**: RestartNever invoke, RestartAlways failure handling.
- **Other (~8)**: GC orphan pods, ResourceQuota pod lifecycle, chunking compaction, DaemonSet rolling update, StatefulSet eviction, HostPort conflicts, preemption running path, service endpoints latency.

Detailed tracking in `.work/CONFORMANCE_TRACKER.md`.

## API Resources Implemented

Rusternetes implements 60+ resource types across 14 API groups. All resources support full CRUD operations, watch, list with field/label selectors, and status subresources where applicable.

### Core (api/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| Namespaces | Implemented | Isolation, finalizers, cascading delete |
| Pods | Implemented | Full lifecycle, init containers, probes, exec/attach/port-forward |
| Services | Implemented | ClusterIP, NodePort, LoadBalancer |
| Endpoints | Implemented | Auto-managed by endpoints controller |
| ConfigMaps | Implemented | |
| Secrets | Implemented | |
| Nodes | Implemented | Registration, status reporting, conditions |
| ServiceAccounts | Implemented | Token generation, automount |
| Events | Implemented | |
| PersistentVolumes | Implemented | Binding, reclaim policies |
| PersistentVolumeClaims | Implemented | Dynamic provisioning |
| ResourceQuotas | Implemented | |
| LimitRanges | Implemented | Default injection, constraint validation |
| ReplicationControllers | Implemented | |
| PodTemplates | Implemented | |
| ComponentStatus | Implemented | |
| Bindings | Implemented | Used by scheduler |

### Apps (apps/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| Deployments | Implemented | Rolling updates, rollbacks, scale subresource |
| ReplicaSets | Implemented | Scale subresource, owner references |
| StatefulSets | Implemented | Ordered pod management, scale subresource, rolling updates |
| DaemonSets | Implemented | Node-targeted scheduling |
| ControllerRevisions | Implemented | History tracking for StatefulSets and DaemonSets |

### Batch (batch/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| Jobs | Implemented | Completions, parallelism, backoff limits, indexed mode, FailIndex |
| CronJobs | Implemented | Schedule-based job creation |

### Networking (networking.k8s.io/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| Ingress | Implemented | HTTP/HTTPS routing |
| IngressClass | Implemented | |
| NetworkPolicies | Implemented | Ingress/egress rules, pod/namespace selectors |

### Networking (networking.k8s.io/v1alpha1)

| Resource | Status | Notes |
|----------|--------|-------|
| IPAddresses | Implemented | |
| ServiceCIDRs | Implemented | |

### RBAC (rbac.authorization.k8s.io/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| Roles | Implemented | |
| RoleBindings | Implemented | |
| ClusterRoles | Implemented | Aggregation rules |
| ClusterRoleBindings | Implemented | |

### Storage (storage.k8s.io/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| StorageClasses | Implemented | |
| CSIDrivers | Implemented | |
| CSINodes | Implemented | |
| CSIStorageCapacity | Implemented | |
| VolumeAttachments | Implemented | |

### Scheduling (scheduling.k8s.io/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| PriorityClasses | Implemented | Preemption support |

### Coordination (coordination.k8s.io/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| Leases | Implemented | Leader election, node heartbeats |

### API Extensions (apiextensions.k8s.io/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| CustomResourceDefinitions | Implemented | Validation, status/scale subresources, categories |
| Custom Resource Instances | Implemented | Full CRUD for any registered CRD |

### Admission Registration (admissionregistration.k8s.io/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| ValidatingWebhookConfigurations | Implemented | |
| MutatingWebhookConfigurations | Implemented | |
| ValidatingAdmissionPolicies | Implemented | CEL expression evaluation |

### Flow Control (flowcontrol.apiserver.k8s.io/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| PriorityLevelConfigurations | Implemented | |
| FlowSchemas | Implemented | |

### Certificates (certificates.k8s.io/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| CertificateSigningRequests | Implemented | Approval subresource |

### Policy (policy/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| PodDisruptionBudgets | Implemented | |

### Autoscaling (autoscaling/v2)

| Resource | Status | Notes |
|----------|--------|-------|
| HorizontalPodAutoscalers | Implemented | |

### Resource (resource.k8s.io/v1beta1)

| Resource | Status | Notes |
|----------|--------|-------|
| ResourceClaims | Implemented | |
| ResourceSlices | Implemented | |
| DeviceClasses | Implemented | |

### Snapshot Storage (snapshot.storage.k8s.io/v1)

| Resource | Status | Notes |
|----------|--------|-------|
| VolumeSnapshots | Implemented | |
| VolumeSnapshotClasses | Implemented | |
| VolumeSnapshotContents | Implemented | |

## Key Conformance Features

Beyond basic CRUD, these API-level features are implemented:

- **Server-Side Apply** — Field managers, conflict detection, field ownership tracking
- **Watch API** — Streaming watches with bookmarks, keep-alive, resource version semantics, WatchCache multiplexer
- **Patch formats** — Strategic Merge Patch, JSON Patch (RFC 6902), JSON Merge Patch (RFC 7386)
- **Subresources** — Status and scale subresources for all applicable resource types
- **API discovery** — Aggregated discovery, per-group resource listings, OpenAPI v2 and v3
- **Table format** — Responses formatted for kubectl's tabular output
- **Admission control** — Mutating and validating webhooks, ValidatingAdmissionPolicy with CEL
- **CRD features** — Schema validation, status subresource, scale subresource, categories
- **Pod operations** — Exec, attach, and port-forward via WebSocket
- **Dry-run** — Server-side dry-run for create, update, and patch
- **Selectors** — Field selectors and label selectors on list and watch
- **Pagination** — Limit/continue token support for large collections
- **Garbage collection** — Cascade delete via owner references, foreground and background modes
- **Finalizers** — Pre-deletion hooks with finalizer semantics
- **Authentication** — TLS/mTLS, token-based auth, service account tokens
- **Authorization** — Full RBAC evaluation
- **Pod security** — PodSecurity admission (enforce level from namespace labels)
- **In-place resize** — KEP-1287 pod resource resize without restart
- **RuntimeClass** — Overhead injection via podFixed

## Controllers

The controller manager runs 31 controllers:

- Deployment, ReplicaSet, StatefulSet, DaemonSet
- Job, CronJob
- Endpoints, EndpointSlice
- PV/PVC binding, dynamic volume provisioner
- Volume snapshot, volume expansion
- Garbage collector, TTL controller
- HPA, VPA, PDB
- Namespace lifecycle, taint eviction
- Service account token controller
- Service (ClusterIP/NodePort allocation), LoadBalancer
- Node lifecycle
- NetworkPolicy, Ingress
- CRD, CSR
- ResourceClaim

## Performance Optimizations

The following optimizations have been applied to improve throughput and reduce latency:

- **Lock-free etcd access** — etcd client uses gRPC/HTTP2 multiplexing (no mutex)
- **Watch-driven kubelet** — Reacts to pod changes via etcd watch instead of pure polling
- **Reduced etcd round-trips** — Create/update use transactions with inline GET for mod_revision
- **Single-pass selector filtering** — Field and label selectors applied in one JSON serialization pass
- **Bounded watch channels** — Prevents unbounded memory growth with slow clients
- **Release binary optimization** — LTO, single codegen unit, symbol stripping

See `docs/PERFORMANCE_PLAN.md` for the full optimization roadmap.

## Running Conformance Tests

```bash
# Build and start the cluster
export KUBELET_VOLUMES_PATH=$(pwd)/.rusternetes/volumes
podman compose build
podman compose up -d
bash scripts/bootstrap-cluster.sh

# Run the conformance suite
bash scripts/run-conformance.sh

# Monitor progress
bash scripts/conformance-progress.sh
```

E2e output is written to `/tmp/sonobuoy/results/e2e.log` inside the e2e container. To save logs:

```bash
E2E_CONTAINER=$(docker ps --filter name=e2e -q)
docker cp "$E2E_CONTAINER:/tmp/sonobuoy/results/e2e.log" /tmp/e2e-results.log
```

## Per-test failure files

After `bash scripts/run-conformance.sh` finishes, extract one text file per failing test:

```bash
bash scripts/conformance-split-junit.sh                      # failures only
bash scripts/conformance-split-junit.sh --all                # every testcase
bash scripts/conformance-split-junit.sh --input path/to/junit_01.xml
```

Output lands in `.rusternetes/volumes/conformance-per-test/` with an `INDEX.md` summary. Each file is self-contained: test name, status, failure message, system-out tail, and pointers to related `docs/conformance/*.md` matrices. Useful as the per-task input when dispatching a Claude Code worker to investigate a single failure.

## Coordinator (per-test work tracking)

`scripts/conformance-coordinator.sh` maintains a JSON state file over the per-test failures. It is the orchestration layer between the splitter (above) and the agent-driven fix loop.

```bash
# Ingest the per-test files into state. Re-runnable; preserves existing entries.
bash scripts/conformance-coordinator.sh init

# Claim the next unclaimed failure (prints "<safe_name>\n<per-test-file>").
bash scripts/conformance-coordinator.sh next

# Record a PR URL once a worker opens one (advances to status=pr_open).
bash scripts/conformance-coordinator.sh claim <safe_name> --pr-url <url>

# Poll GitHub for PR statuses (MERGED -> pr_merged; CLOSED -> fail + clear url).
bash scripts/conformance-coordinator.sh update

# Flip to verified after the shadow check (single-test hydrophone re-run) passes.
bash scripts/conformance-coordinator.sh mark-done <safe_name>

# Run the shadow check automatically and gate the verified flip on the
# runner's exit code. Wraps scripts/conformance-single-test.sh; passes the
# upstream test name from state. Exits non-zero if the test still fails,
# leaving the entry's status unchanged so the next `update`/`verify` cycle
# can retry.
bash scripts/conformance-coordinator.sh verify <safe_name>

# Unclaim if a worker abandons.
bash scripts/conformance-coordinator.sh release <safe_name>

# Summary counts.
bash scripts/conformance-coordinator.sh status
```

State file: `.rusternetes/volumes/conformance-coordinator-state.json` (gitignored alongside the per-test directory).

The coordinator does NOT call any agent itself — the driver session does that. Designed so the same state file can be read by manual operators and automated drivers interchangeably.

KUBECONFIG: `~/.kube/rusternetes-config`

## References

- [Kubernetes Conformance Requirements](https://github.com/cncf/k8s-conformance)
- [Kubernetes API Conventions](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md)
- [Kubernetes Conformance Testing](https://github.com/cncf/k8s-conformance/blob/master/instructions.md)
