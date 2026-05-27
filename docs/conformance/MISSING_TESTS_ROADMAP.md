# Missing Conformance Tests Roadmap

This document tracks the high-priority test cases from the official Kubernetes Go implementation that need to be mirrored in Rust for 100% conformance.

## Status (2026-05-27)

All 27 Phase 1–8 sections planned below have shipped via PRs **#792, #794–#820** (merged May 2026). The original bullet checklists are retained as the work-item record per section; each section header now carries its PR, the test file it landed in, and the test count.

Test types:
- **GREEN** — controller/handler is implemented and the suite passes today.
- **RED-state stub** — the test suite ships first and pins the expected upstream behaviour; the underlying controller is a stub. These suites are wired into CI so the next person to land the controller flips them green without re-writing the harness.

Roughly **270 tests** added across the eight phases (up from ~66 at the start of this roadmap). Per-phase totals in the progress table near the bottom.

## Priority 1: Critical Path Controllers (Phase 1)

### 1.1 Job Controller — Extended Coverage — ✅ PR #802 (19 tests, GREEN)
Source: `kubernetes/test/e2e/apps/job.go` (800+ lines), `test/e2e/framework/job/wait.go`
File: `crates/controller-manager/tests/job_extended_test.rs`

**Missing tests:**
- [x] `Job should adopt matching orphans` — orphan pod adoption when selector matches
- [x] `Job should release non-matching pods` — pods released when labels no longer match
- [x] `Indexed Job completion` — indexed completion mode with per-index tracking
- [x] `Job successPolicy` — success policy for distributed training workloads
- [x] `Job backoffLimitPerIndex` — per-index backoff for indexed jobs
- [x] `Job managedBy field` — external job management coordination
- [x] `Job pod failure policy` — pod-level failure handling strategies
- [x] `Job with nodeAffinity` — scheduling constraints on job pods
- [x] `Job TTL seconds after finished` — automatic cleanup after completion

### 1.2 StatefulSet Controller — Extended Coverage — ✅ PR #792 (17 tests, GREEN)
Source: `kubernetes/test/e2e/apps/statefulset.go` (1200+ lines)
File: `crates/controller-manager/tests/statefulset_extended_test.rs`

**Missing tests:**
- [x] `StatefulSet with persistent volumes` — PVC binding and mounting
- [x] `StatefulSet network identity` — stable network IDs across reschedules
- [x] `StatefulSet headless service DNS` — DNS records for stateful pods
- [x] `StatefulSet update strategies comparison` — RollingUpdate vs OnDelete
- [x] `StatefulSet force rollback` — forced rollback to previous revision
- [x] `StatefulSet with init containers` — init container ordering and execution
- [x] `StatefulSet volume claim template updates` — VCT modification handling
- [x] `StatefulSet status conditions` — comprehensive status field validation
- [x] `StatefulSet with topology spread constraints` — pod distribution
- [x] `StatefulSet deletion propagation` — cascading delete behavior

### 1.3 DaemonSet Controller — Extended Coverage — ✅ PR #794 (15 tests, GREEN)
Source: `kubernetes/test/e2e/apps/daemon_set.go` (900+ lines)
File: `crates/controller-manager/tests/daemonset_extended_test.rs`

**Missing tests:**
- [x] `DaemonSet with taints and tolerations` — scheduling on tainted nodes
- [x] `DaemonSet update strategy OnDelete` — manual pod deletion updates
- [x] `DaemonSet maxUnavailable scheduling` — respecting maxUnavailable during updates
- [x] `DaemonSet with priority class` — pod priority handling
- [x] `DaemonSet revision history` — ControllerRevision creation and retention
- [x] `DaemonSet status fields validation` — desired/ready/available counts
- [x] `DaemonSet with affinity rules` — node/pod affinity constraints
- [x] `DaemonSet burst updates` — rapid spec change handling

### 1.4 Deployment Controller — Extended Coverage — ✅ PR #819 (11 tests, GREEN)
Source: `kubernetes/test/e2e/apps/deployment.go` (1000+ lines)
File: `crates/controller-manager/tests/deployment_extended_test.rs`

**Missing tests:**
- [x] `Deployment progress deadline exceeded` — timeout on stalled rollout
- [x] `Deployment minimum replicas during update` — minReadySeconds enforcement
- [x] `Deployment with multiple container images` — multi-container pod updates
- [x] `Deployment environment variable updates` — env change rollouts
- [x] `Deployment resource limits updates` — CPU/memory change rollouts
- [x] `Deployment with pod disruption budget` — PDB interaction during updates
- [x] `Deployment observed generation` — status.observedGeneration tracking
- [x] `Deployment conditions lifecycle` — all deployment conditions

## Priority 2: Storage & Volume Controllers (Phase 2)

### 2.1 PersistentVolume Controller — ✅ PR #798 (11 tests, GREEN)
Source: `kubernetes/test/e2e/storage/persistent_volumes.go`
File: `crates/controller-manager/tests/pv_controller_extended_test.rs`

**Missing tests:**
- [x] `PV dynamic provisioning` — StorageClass-based provisioning
- [x] `PV reclaim policy Retain` — manual cleanup after PVC deletion
- [x] `PV reclaim policy Recycle` — deprecated but tested
- [x] `PV access modes validation` — RWO, ROX, RWX enforcement
- [x] `PV capacity enforcement` — storage size limits
- [x] `PV node affinity` — volume node constraints
- [x] `PV mount options` — custom mount flags
- [x] `PV fsGroup support` — filesystem group ownership

### 2.2 PVC Controller — ✅ PR #795 (6 tests, RED-state stub)
Source: `kubernetes/test/e2e/storage/persistent_volumes_claim.go`
File: `crates/controller-manager/tests/pvc_controller_test.rs`

**Missing tests:**
- [x] `PVC binding modes` — Immediate vs WaitForFirstConsumer
- [x] `PVC storage class selection` — default and explicit SC
- [x] `PVC resize operation` — online volume expansion
- [x] `PVC clone operation` — snapshot and PVC-to-PVC cloning
- [x] `PVC datasource population` — restoring from snapshots

### 2.3 StorageClass Controller — ✅ PR #807 (6 tests, RED-state stub)
Source: `kubernetes/test/e2e/storage/storage_class.go`
File: `crates/controller-manager/tests/storageclass_controller_test.rs`

**Missing tests:**
- [x] `StorageClass default designation` — single default per cluster
- [x] `StorageClass provisioner parameters` — custom provisioner config
- [x] `StorageClass mount options propagation` — to PV objects
- [x] `StorageClass reclaim policy default` — Delete vs Retain

### 2.4 Volume Attachment Controller — ✅ PR #805 (3 tests, RED-state stub)
Source: `kubernetes/test/e2e/storage/volume_attachment.go`
File: `crates/controller-manager/tests/volume_attachment_test.rs`

**Missing tests:**
- [x] `VolumeAttachment creation on attach` — CSI attach flow
- [x] `VolumeAttachment deletion on detach` — CSI detach flow
- [x] `VolumeAttachment error handling` — attach/detach failures

## Priority 3: Network Controllers (Phase 3)

### 3.1 Service Controller — LoadBalancer — ✅ PR #817 (10 tests, GREEN)
Source: `kubernetes/test/e2e/network/service.go`
File: `crates/controller-manager/tests/service_lb_extended_test.rs`

**Missing tests:**
- [x] `LoadBalancer external IP assignment` — cloud provider integration
- [x] `LoadBalancer health checks` — readiness probe integration
- [x] `Service external traffic policy Local` — source IP preservation
- [x] `Service internal traffic policy` — cluster-internal routing
- [x] `Service topology keys` — topology-aware routing
- [x] `Service publish not ready addresses` — serving before ready
- [x] `Service ipFamilyPolicy` — IPv4/IPv6 dual-stack

### 3.2 Ingress Controller — ✅ PR #796 (11 tests, GREEN)
Source: `kubernetes/test/e2e/network/ingress.go`
File: `crates/controller-manager/tests/ingress_controller_extended_test.rs`

**Missing tests:**
- [x] `Ingress path type matching` — Exact, Prefix, ImplementationSpecific
- [x] `Ingress TLS termination` — HTTPS routing
- [x] `Ingress default backend` — catch-all routing
- [x] `Ingress host-based routing` — virtual host support
- [x] `Ingress class selection` — ingressClassName field
- [x] `Ingress status load balancer` — status updates

### 3.3 NetworkPolicy Controller — ✅ PR #800 (7 tests, GREEN)
Source: `kubernetes/test/e2e/network/network_policy.go`
File: `crates/controller-manager/tests/networkpolicy_controller_test.rs`

**Missing tests:**
- [x] `NetworkPolicy ingress rules` — allow incoming traffic
- [x] `NetworkPolicy egress rules` — allow outgoing traffic
- [x] `NetworkPolicy pod selector` — target specific pods
- [x] `NetworkPolicy namespace selector` — cross-namespace rules
- [x] `NetworkPolicy port ranges` — port range specifications
- [x] `NetworkPolicy policy types` — Ingress, Egress, Both
- [x] `NetworkPolicy default deny` — zero-trust baseline

## Priority 4: Autoscaling Controllers (Phase 4)

### 4.1 HPA Extended Coverage — ✅ PR #804 (19 tests total in extended file, GREEN)
Source: `kubernetes/test/e2e/apps/hpa.go`
File: `crates/controller-manager/tests/hpa_controller_test.rs` (extended in place)

**Missing tests:**
- [x] `HPA scale down stabilization` — cooldown period
- [x] `HPA metrics server integration` — custom metrics API
- [x] `HPA external metrics` — metrics outside cluster
- [x] `HPA average utilization calculation` — per-pod vs total
- [x] `HPA initial readiness delay` — startup grace period
- [x] `HPA tolerance settings` — scale-up/down thresholds
- [x] `HPA behavior policies` — custom scale-up/down rates
- [x] `HPA with multiple metrics` — AND/OR logic

### 4.2 VPA Controller — ✅ PR #799 (11 tests, GREEN)
Source: `kubernetes/test/e2e/autoscaling/vpa.go`
File: `crates/controller-manager/tests/vpa_controller_test.rs`

**Missing tests:**
- [x] `VPA recommendation generation` — resource suggestions
- [x] `VPA update mode Auto` — automatic pod updates
- [x] `VPA update mode Initial` — one-time sizing
- [x] `VPA update mode Off` — recommendations only
- [x] `VPA history tracking` — historical usage data

## Priority 5: Batch & Scheduling (Phase 5)

### 5.1 CronJob Extended Coverage — ✅ PR #806 (15 tests total in extended file, GREEN)
Source: `kubernetes/test/e2e/apps/cronjob.go`
File: `crates/controller-manager/tests/cronjob_controller_test.rs` (extended in place)

**Missing tests:**
- [x] `CronJob timezone support` — timezone-aware scheduling
- [x] `CronJob successfulJobsHistoryLimit` — history retention
- [x] `CronJob failedJobsHistoryLimit` — failure history
- [x] `CronJob parallelism enforcement` — concurrent job limits
- [x] `CronJob time zone DST handling` — daylight saving transitions

### 5.2 PriorityClass Controller — ✅ PR #820 (6 tests, RED-state stub)
Source: `kubernetes/test/e2e/scheduling/priorityclass.go`
File: `crates/controller-manager/tests/priorityclass_controller_test.rs`

**Missing tests:**
- [x] `PriorityClass preemption` — lower priority pod eviction
- [x] `PriorityClass global default` — cluster-wide default
- [x] `PriorityClass namespace default` — per-namespace defaults
- [x] `PriorityClass value ordering` — numeric priority comparison

### 5.3 PriorityQueue & Preemption — ✅ PR #801 (5 tests, GREEN)
Source: `kubernetes/test/e2e/scheduling/preemption.go`
File: `crates/scheduler/tests/scheduler_preemption_test.rs`

**Missing tests:**
- [x] `Pod preemption victim selection` — choosing pods to evict
- [x] `Pod preemption with PDB` — respecting disruption budgets
- [x] `Pod preemption with priority` — priority-based eviction
- [x] `Scheduler queue sorting` — priority queue ordering

## Priority 6: Security & RBAC (Phase 6)

### 6.1 RBAC Authorization — ✅ PR #814 (9 tests, GREEN)
Source: `kubernetes/test/e2e/auth/rbac.go`
File: `crates/api-server/tests/rbac_authorization_test.rs`

**Missing tests:**
- [x] `ClusterRole aggregation` — aggregated role rules
- [x] `RoleBinding escalation prevention` — privilege escalation blocks
- [x] `RBAC wildcard permissions` — `*` verb/resource handling
- [x] `RBAC subject kinds` — User, Group, ServiceAccount
- [x] `RBAC namespace isolation` — Role vs ClusterRole

### 6.2 ServiceAccount Controller — ✅ PR #803 (11 tests total in extended file, GREEN)
Source: `kubernetes/test/e2e/auth/serviceaccount.go`
File: `crates/controller-manager/tests/serviceaccount_controller_test.rs` (extended in place)

**Missing tests:**
- [x] `ServiceAccount token projection` — bound service account tokens
- [x] `ServiceAccount automount disable` — disabling default mounts
- [x] `ServiceAccount image pull secrets` — registry authentication
- [x] `ServiceAccount secret synchronization` — token secret creation

### 6.3 PodSecurityPolicy/Admission — ✅ PR #808 (6 tests, RED-state stub)
Source: `kubernetes/test/e2e/auth/pod_security_policy.go`
File: `crates/api-server/tests/pod_security_admission_test.rs`

**Missing tests:**
- [x] `PSP privileged containers` — blocking privileged pods
- [x] `PSP host namespaces` — hostPID/hostNetwork restrictions
- [x] `PSP volume types` — allowed volume plugins
- [x] `PSP runAsUser` — user ID constraints

## Priority 7: Core Resources (Phase 7)

### 7.1 ConfigMap Controller — ✅ PR #818 (6 tests, GREEN)
Source: `kubernetes/test/e2e/common/configmap.go`
File: `crates/api-server/tests/configmap_consumption_test.rs`

**Missing tests:**
- [x] `ConfigMap volume projection` — as volume mounts
- [x] `ConfigMap environment variables` — as env vars
- [x] `ConfigMap command arguments` — in command arrays
- [x] `ConfigMap updates propagation` — live update behavior
- [x] `ConfigMap binary data` — binaryData field handling

### 7.2 Secret Controller — ✅ PR #812 (7 tests, GREEN)
Source: `kubernetes/test/e2e/common/secrets.go`
File: `crates/api-server/tests/secret_consumption_test.rs`

**Missing tests:**
- [x] `Secret volume projection` — as volume mounts
- [x] `Secret environment variables` — as env vars
- [x] `Secret types` — Opaque, docker-registry, tls, basic-auth
- [x] `Secret updates propagation` — live update behavior
- [x] `Secret immutable field` — immutable secrets

### 7.3 Namespace Controller — ✅ PR #813 (8 tests total in extended file, GREEN)
Source: `kubernetes/test/e2e/apimachinery/namespaces.go`
File: `crates/controller-manager/tests/namespace_controller_test.rs` (extended in place)

**Missing tests:**
- [x] `Namespace finalizers` — namespace deletion flow
- [x] `Namespace resource quota inheritance` — quota application
- [x] `Namespace network policy isolation` — network boundaries
- [x] `Namespace RBAC isolation` — role scoping

### 7.4 LimitRange Controller — ✅ PR #816 (5 tests, RED-state stub)
Source: `kubernetes/test/e2e/apimachinery/limit_range.go`
File: `crates/controller-manager/tests/limitrange_controller_test.rs`

**Missing tests:**
- [x] `LimitRange container defaults` — default CPU/memory
- [x] `LimitRange min/max enforcement` — constraint validation
- [x] `LimitRange ratio constraints` — CPU/memory ratios
- [x] `LimitRange PVC limits` — storage constraints

## Priority 8: Lifecycle & Maintenance (Phase 8)

### 8.1 Node Lifecycle Extended — ✅ PR #815 (11 tests total in extended file, GREEN)
Source: `kubernetes/test/e2e/node/lifecycle.go`
File: `crates/controller-manager/tests/node_controller_test.rs` (extended in place)

**Missing tests:**
- [x] `Node shutdown taint` — graceful node shutdown
- [x] `Node condition monitoring` — Ready, MemoryPressure, etc.
- [x] `Node lease renewal` — heartbeat mechanism
- [x] `Node resources capacity` — allocatable vs capacity

### 8.2 Pod Lifecycle Extended — ✅ PR #811 (10 tests, GREEN)
Source: `kubernetes/test/e2e/common/pod_lifecycle.go`
File: `crates/api-server/tests/pod_lifecycle_extended_test.rs`

**Missing tests:**
- [x] `Pod graceful termination` — terminationGracePeriodSeconds
- [x] `Pod preStop hooks` — lifecycle hook execution
- [x] `Pod postStart hooks` — startup hook execution
- [x] `Pod restart policies` — Always, OnFailure, Never
- [x] `Pod QoS classes` — Guaranteed, Burstable, BestEffort

### 8.3 TTL After Finished — ✅ PR #810 (14 tests total in extended file, GREEN)
Source: `kubernetes/test/e2e/framework/ttl.go`
File: `crates/controller-manager/tests/ttl_controller_test.rs` (extended in place)

**Missing tests:**
- [x] `TTL controller Job cleanup` — automatic Job deletion
- [x] `TTL controller Pod cleanup` — automatic Pod deletion
- [x] `TTL negative values` — immediate deletion

### 8.4 Garbage Collector Extended — ✅ PR #809 (10 tests, GREEN)
Source: `kubernetes/test/e2e/framework/gc.go`
File: `crates/controller-manager/tests/integration_gc_cascading.rs`

**Missing tests:**
- [x] `GC orphan dependency` — orphaned resource handling
- [x] `GC cross-namespace references` — namespace boundary GC
- [x] `GC finalizer blocking` — finalizer preventing deletion
- [x] `GC owner reference updates` — changing ownership

## Implementation Guidelines

### Test Structure Pattern
```rust
use rusternetes_common::resources::*;
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::{ControllerName};
use rusternetes_storage::{memory::MemoryStorage, Storage};
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

#[tokio::test]
async fn test_name_should_do_something() {
    let storage = setup_test().await;
    
    // Arrange: Create test fixtures
    let obj = create_test_object("name", "default");
    storage.create(&obj).await.unwrap();
    
    // Act: Run controller reconcile
    let controller = ControllerName::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    
    // Assert: Verify expected state
    let result = storage.get::<ObjectType>("name", "default").await.unwrap();
    assert_eq!(result.status.unwrap().phase, "Expected");
}
```

### Naming Convention
- Mirror upstream Ginkgo descriptor names (convert to snake_case)
- Format: `{resource}_should_{behavior}_when_{condition}`
- Example: `job_should_run_to_completion_when_tasks_succeed`

### Documentation Requirements
Each test file must include:
1. Upstream source reference (file path in k/kubernetes)
2. Sonobuoy round status (PASS/FAIL)
3. Cross-reference to conformance docs
4. Coverage matrix table

### Priority Scoring
Tests are prioritized by:
1. **Conformance impact** — Does this block 100% conformance?
2. **Usage frequency** — How commonly is this feature used?
3. **Complexity** — Start with simpler tests to build confidence
4. **Dependencies** — Some tests require other features first

## Progress Tracking

| Phase | Area | PR | Tests | State |
|-------|------|----|------:|------|
| 1.1 | Job Extended            | #802 | 19 | GREEN |
| 1.2 | StatefulSet Extended    | #792 | 17 | GREEN |
| 1.3 | DaemonSet Extended      | #794 | 15 | GREEN |
| 1.4 | Deployment Extended     | #819 | 11 | GREEN |
| 2.1 | PV Controller           | #798 | 11 | GREEN |
| 2.2 | PVC Controller          | #795 |  6 | RED-state stub |
| 2.3 | StorageClass            | #807 |  6 | RED-state stub |
| 2.4 | VolumeAttachment        | #805 |  3 | RED-state stub |
| 3.1 | Service LB              | #817 | 10 | GREEN |
| 3.2 | Ingress                 | #796 | 11 | GREEN |
| 3.3 | NetworkPolicy           | #800 |  7 | GREEN |
| 4.1 | HPA Extended            | #804 | 19 | GREEN |
| 4.2 | VPA                     | #799 | 11 | GREEN |
| 5.1 | CronJob Extended        | #806 | 15 | GREEN |
| 5.2 | PriorityClass           | #820 |  6 | RED-state stub |
| 5.3 | Scheduler Preemption    | #801 |  5 | GREEN |
| 6.1 | RBAC                    | #814 |  9 | GREEN |
| 6.2 | ServiceAccount          | #803 | 11 | GREEN |
| 6.3 | PodSecurity Admission   | #808 |  6 | RED-state stub |
| 7.1 | ConfigMap               | #818 |  6 | GREEN |
| 7.2 | Secret                  | #812 |  7 | GREEN |
| 7.3 | Namespace               | #813 |  8 | GREEN |
| 7.4 | LimitRange              | #816 |  5 | RED-state stub |
| 8.1 | Node Lifecycle          | #815 | 11 | GREEN |
| 8.2 | Pod Lifecycle           | #811 | 10 | GREEN |
| 8.3 | TTL                     | #810 | 14 | GREEN |
| 8.4 | GC Extended             | #809 | 10 | GREEN |
| **Total** | | | **270** | 21 GREEN sections, 6 RED-state stubs |

## Next Steps

The original Phase 1–8 backlog is closed. Remaining work falls in two buckets:

1. **Flip the 6 RED-state stubs to GREEN.** Each section above marked "RED-state stub" ships a passing harness against a stubbed controller. The next step per stub is to land the real controller implementation; the existing test file pins behavior and will fail at compile / reconcile time until the controller exists. The stubs are:
   - 2.2 PVC controller (`crates/controller-manager/tests/pvc_controller_test.rs`)
   - 2.3 StorageClass controller (`storageclass_controller_test.rs`)
   - 2.4 VolumeAttachment controller (`volume_attachment_test.rs`)
   - 5.2 PriorityClass controller (`priorityclass_controller_test.rs`)
   - 6.3 PodSecurity admission (`pod_security_admission_test.rs`)
   - 7.4 LimitRange controller (`limitrange_controller_test.rs`)

2. **Round-trip conformance gap.** With ~270 mirror tests landed, the next conformance-budget signal lives in upstream Sonobuoy / Hydrophone runs (see `docs/CONFORMANCE.md`). Drive net-new test files from what those runs surface as failing, not from this roadmap — this document is closed as a forward-looking backlog.

## References

- Kubernetes Go Tests: https://github.com/kubernetes/kubernetes/tree/master/test/e2e
- Sonobuoy Conformance: https://github.com/vmware-tanzu/sonobuoy
- Rusternetes Conformance Docs: `docs/conformance/`
- Existing Test Patterns: `crates/controller-manager/tests/conformance_*.rs`
