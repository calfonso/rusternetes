# Missing Conformance Tests Roadmap

This document tracks the high-priority test cases from the official Kubernetes Go implementation that need to be mirrored in Rust for 100% conformance.

## Priority 1: Critical Path Controllers (Phase 1)

### 1.1 Job Controller — Extended Coverage
Source: `kubernetes/test/e2e/apps/job.go` (800+ lines), `test/e2e/framework/job/wait.go`

**Missing tests:**
- [ ] `Job should adopt matching orphans` — orphan pod adoption when selector matches
- [ ] `Job should release non-matching pods` — pods released when labels no longer match
- [ ] `Indexed Job completion` — indexed completion mode with per-index tracking
- [ ] `Job successPolicy` — success policy for distributed training workloads
- [ ] `Job backoffLimitPerIndex` — per-index backoff for indexed jobs
- [ ] `Job managedBy field` — external job management coordination
- [ ] `Job pod failure policy` — pod-level failure handling strategies
- [ ] `Job with nodeAffinity` — scheduling constraints on job pods
- [ ] `Job TTL seconds after finished` — automatic cleanup after completion

File: `crates/controller-manager/tests/job_extended_test.rs`
Estimated: 15-20 tests

### 1.2 StatefulSet Controller — Extended Coverage
Source: `kubernetes/test/e2e/apps/statefulset.go` (1200+ lines)

**Missing tests:**
- [ ] `StatefulSet with persistent volumes` — PVC binding and mounting
- [ ] `StatefulSet network identity` — stable network IDs across reschedules
- [ ] `StatefulSet headless service DNS` — DNS records for stateful pods
- [ ] `StatefulSet update strategies comparison` — RollingUpdate vs OnDelete
- [ ] `StatefulSet force rollback` — forced rollback to previous revision
- [ ] `StatefulSet with init containers` — init container ordering and execution
- [ ] `StatefulSet volume claim template updates` — VCT modification handling
- [ ] `StatefulSet status conditions` — comprehensive status field validation
- [ ] `StatefulSet with topology spread constraints` — pod distribution
- [ ] `StatefulSet deletion propagation` — cascading delete behavior

File: Extend `conformance_apps_statefulset_daemonset.rs` or create `statefulset_extended_test.rs`
Estimated: 15-20 tests

### 1.3 DaemonSet Controller — Extended Coverage
Source: `kubernetes/test/e2e/apps/daemon_set.go` (900+ lines)

**Missing tests:**
- [ ] `DaemonSet with taints and tolerations` — scheduling on tainted nodes
- [ ] `DaemonSet update strategy OnDelete` — manual pod deletion updates
- [ ] `DaemonSet maxUnavailable scheduling` — respecting maxUnavailable during updates
- [ ] `DaemonSet with priority class` — pod priority handling
- [ ] `DaemonSet revision history` — ControllerRevision creation and retention
- [ ] `DaemonSet status fields validation` — desired/ready/available counts
- [ ] `DaemonSet with affinity rules` — node/pod affinity constraints
- [ ] `DaemonSet burst updates` — rapid spec change handling

File: Extend `conformance_apps_statefulset_daemonset.rs` or create `daemonset_extended_test.rs`
Estimated: 12-15 tests

### 1.4 Deployment Controller — Extended Coverage
Source: `kubernetes/test/e2e/apps/deployment.go` (1000+ lines)

**Missing tests:**
- [ ] `Deployment progress deadline exceeded` — timeout on stalled rollout
- [ ] `Deployment minimum replicas during update` — minReadySeconds enforcement
- [ ] `Deployment with multiple container images` — multi-container pod updates
- [ ] `Deployment environment variable updates` — env change rollouts
- [ ] `Deployment resource limits updates` — CPU/memory change rollouts
- [ ] `Deployment with pod disruption budget` — PDB interaction during updates
- [ ] `Deployment observed generation` — status.observedGeneration tracking
- [ ] `Deployment conditions lifecycle` — all deployment conditions

File: Extend `conformance_apps_deployment_replicaset.rs` or create `deployment_extended_test.rs`
Estimated: 12-15 tests

## Priority 2: Storage & Volume Controllers (Phase 2)

### 2.1 PersistentVolume Controller
Source: `kubernetes/test/e2e/storage/persistent_volumes.go`

**Missing tests:**
- [ ] `PV dynamic provisioning` — StorageClass-based provisioning
- [ ] `PV reclaim policy Retain` — manual cleanup after PVC deletion
- [ ] `PV reclaim policy Recycle` — deprecated but tested
- [ ] `PV access modes validation` — RWO, ROX, RWX enforcement
- [ ] `PV capacity enforcement` — storage size limits
- [ ] `PV node affinity` — volume node constraints
- [ ] `PV mount options` — custom mount flags
- [ ] `PV fsGroup support` — filesystem group ownership

File: Extend `pv_binder_test.rs` or create `pv_controller_extended_test.rs`
Estimated: 12-15 tests

### 2.2 PVC Controller
Source: `kubernetes/test/e2e/storage/persistent_volumes_claim.go`

**Missing tests:**
- [ ] `PVC binding modes` — Immediate vs WaitForFirstConsumer
- [ ] `PVC storage class selection` — default and explicit SC
- [ ] `PVC resize operation` — online volume expansion
- [ ] `PVC clone operation` — snapshot and PVC-to-PVC cloning
- [ ] `PVC datasource population` — restoring from snapshots

File: Create `pvc_controller_test.rs`
Estimated: 8-10 tests

### 2.3 StorageClass Controller
Source: `kubernetes/test/e2e/storage/storage_class.go`

**Missing tests:**
- [ ] `StorageClass default designation` — single default per cluster
- [ ] `StorageClass provisioner parameters` — custom provisioner config
- [ ] `StorageClass mount options propagation` — to PV objects
- [ ] `StorageClass reclaim policy default` — Delete vs Retain

File: Create `storageclass_controller_test.rs`
Estimated: 6-8 tests

### 2.4 Volume Attachment Controller
Source: `kubernetes/test/e2e/storage/volume_attachment.go`

**Missing tests:**
- [ ] `VolumeAttachment creation on attach` — CSI attach flow
- [ ] `VolumeAttachment deletion on detach` — CSI detach flow
- [ ] `VolumeAttachment error handling` — attach/detach failures

File: Create `volume_attachment_test.rs`
Estimated: 5-7 tests

## Priority 3: Network Controllers (Phase 3)

### 3.1 Service Controller — LoadBalancer
Source: `kubernetes/test/e2e/network/service.go`

**Missing tests:**
- [ ] `LoadBalancer external IP assignment` — cloud provider integration
- [ ] `LoadBalancer health checks` — readiness probe integration
- [ ] `Service external traffic policy Local` — source IP preservation
- [ ] `Service internal traffic policy` — cluster-internal routing
- [ ] `Service topology keys` — topology-aware routing
- [ ] `Service publish not ready addresses` — serving before ready
- [ ] `Service ipFamilyPolicy` — IPv4/IPv6 dual-stack

File: Extend `service_controller_test.rs` or `service_lb_endpoints_test.rs`
Estimated: 10-12 tests

### 3.2 Ingress Controller
Source: `kubernetes/test/e2e/network/ingress.go`

**Missing tests:**
- [ ] `Ingress path type matching` — Exact, Prefix, ImplementationSpecific
- [ ] `Ingress TLS termination` — HTTPS routing
- [ ] `Ingress default backend` — catch-all routing
- [ ] `Ingress host-based routing` — virtual host support
- [ ] `Ingress class selection` — ingressClassName field
- [ ] `Ingress status load balancer` — status updates

File: Create `ingress_controller_test.rs`
Estimated: 10-12 tests

### 3.3 NetworkPolicy Controller
Source: `kubernetes/test/e2e/network/network_policy.go`

**Missing tests:**
- [ ] `NetworkPolicy ingress rules` — allow incoming traffic
- [ ] `NetworkPolicy egress rules` — allow outgoing traffic
- [ ] `NetworkPolicy pod selector` — target specific pods
- [ ] `NetworkPolicy namespace selector` — cross-namespace rules
- [ ] `NetworkPolicy port ranges` — port range specifications
- [ ] `NetworkPolicy policy types` — Ingress, Egress, Both
- [ ] `NetworkPolicy default deny` — zero-trust baseline

File: Create `networkpolicy_controller_test.rs`
Estimated: 10-12 tests

## Priority 4: Autoscaling Controllers (Phase 4)

### 4.1 HPA Extended Coverage
Source: `kubernetes/test/e2e/apps/hpa.go`

**Missing tests:**
- [ ] `HPA scale down stabilization` — cooldown period
- [ ] `HPA metrics server integration` — custom metrics API
- [ ] `HPA external metrics` — metrics outside cluster
- [ ] `HPA average utilization calculation` — per-pod vs total
- [ ] `HPA initial readiness delay` — startup grace period
- [ ] `HPA tolerance settings` — scale-up/down thresholds
- [ ] `HPA behavior policies` — custom scale-up/down rates
- [ ] `HPA with multiple metrics` — AND/OR logic

File: Extend `hpa_controller_test.rs`
Estimated: 12-15 tests

### 4.2 VPA Controller (if implemented)
Source: `kubernetes/test/e2e/autoscaling/vpa.go`

**Missing tests:**
- [ ] `VPA recommendation generation` — resource suggestions
- [ ] `VPA update mode Auto` — automatic pod updates
- [ ] `VPA update mode Initial` — one-time sizing
- [ ] `VPA update mode Off` — recommendations only
- [ ] `VPA history tracking` — historical usage data

File: Extend `vpa_controller_test.rs`
Estimated: 8-10 tests

## Priority 5: Batch & Scheduling (Phase 5)

### 5.1 CronJob Extended Coverage
Source: `kubernetes/test/e2e/apps/cronjob.go`

**Missing tests:**
- [ ] `CronJob timezone support` — timezone-aware scheduling
- [ ] `CronJob successfulJobsHistoryLimit` — history retention
- [ ] `CronJob failedJobsHistoryLimit` — failure history
- [ ] `CronJob parallelism enforcement` — concurrent job limits
- [ ] `CronJob time zone DST handling` — daylight saving transitions

File: Extend `cronjob_controller_test.rs` or `conformance_apps_job_cronjob.rs`
Estimated: 8-10 tests

### 5.2 PriorityClass Controller
Source: `kubernetes/test/e2e/scheduling/priorityclass.go`

**Missing tests:**
- [ ] `PriorityClass preemption` — lower priority pod eviction
- [ ] `PriorityClass global default` — cluster-wide default
- [ ] `PriorityClass namespace default` — per-namespace defaults
- [ ] `PriorityClass value ordering` — numeric priority comparison

File: Create `priorityclass_controller_test.rs`
Estimated: 6-8 tests

### 5.3 PriorityQueue & Preemption
Source: `kubernetes/test/e2e/scheduling/preemption.go`

**Missing tests:**
- [ ] `Pod preemption victim selection` — choosing pods to evict
- [ ] `Pod preemption with PDB` — respecting disruption budgets
- [ ] `Pod preemption with priority` — priority-based eviction
- [ ] `Scheduler queue sorting` — priority queue ordering

File: Create `scheduler_preemption_test.rs`
Estimated: 8-10 tests

## Priority 6: Security & RBAC (Phase 6)

### 6.1 RBAC Authorization
Source: `kubernetes/test/e2e/auth/rbac.go`

**Missing tests:**
- [ ] `ClusterRole aggregation` — aggregated role rules
- [ ] `RoleBinding escalation prevention` — privilege escalation blocks
- [ ] `RBAC wildcard permissions` — `*` verb/resource handling
- [ ] `RBAC subject kinds` — User, Group, ServiceAccount
- [ ] `RBAC namespace isolation` — Role vs ClusterRole

File: Create `rbac_authorization_test.rs`
Estimated: 10-12 tests

### 6.2 ServiceAccount Controller
Source: `kubernetes/test/e2e/auth/serviceaccount.go`

**Missing tests:**
- [ ] `ServiceAccount token projection` — bound service account tokens
- [ ] `ServiceAccount automount disable` — disabling default mounts
- [ ] `ServiceAccount image pull secrets` — registry authentication
- [ ] `ServiceAccount secret synchronization` — token secret creation

File: Extend `serviceaccount_controller_test.rs`
Estimated: 6-8 tests

### 6.3 PodSecurityPolicy/Admission (if applicable)
Source: `kubernetes/test/e2e/auth/pod_security_policy.go`

**Missing tests:**
- [ ] `PSP privileged containers` — blocking privileged pods
- [ ] `PSP host namespaces` — hostPID/hostNetwork restrictions
- [ ] `PSP volume types` — allowed volume plugins
- [ ] `PSP runAsUser` — user ID constraints

File: Create `pod_security_admission_test.rs`
Estimated: 8-10 tests

## Priority 7: Core Resources (Phase 7)

### 7.1 ConfigMap Controller
Source: `kubernetes/test/e2e/common/configmap.go`

**Missing tests:**
- [ ] `ConfigMap volume projection` — as volume mounts
- [ ] `ConfigMap environment variables` — as env vars
- [ ] `ConfigMap command arguments` — in command arrays
- [ ] `ConfigMap updates propagation` — live update behavior
- [ ] `ConfigMap binary data` — binaryData field handling

File: Create `configmap_controller_test.rs`
Estimated: 8-10 tests

### 7.2 Secret Controller
Source: `kubernetes/test/e2e/common/secrets.go`

**Missing tests:**
- [ ] `Secret volume projection` — as volume mounts
- [ ] `Secret environment variables` — as env vars
- [ ] `Secret types` — Opaque, docker-registry, tls, basic-auth
- [ ] `Secret updates propagation` — live update behavior
- [ ] `Secret immutable field` — immutable secrets

File: Create `secret_controller_test.rs`
Estimated: 8-10 tests

### 7.3 Namespace Controller
Source: `kubernetes/test/e2e/apimachinery/namespaces.go`

**Missing tests:**
- [ ] `Namespace finalizers` — namespace deletion flow
- [ ] `Namespace resource quota inheritance` — quota application
- [ ] `Namespace network policy isolation` — network boundaries
- [ ] `Namespace RBAC isolation` — role scoping

File: Extend `namespace_controller_test.rs`
Estimated: 6-8 tests

### 7.4 LimitRange Controller
Source: `kubernetes/test/e2e/apimachinery/limit_range.go`

**Missing tests:**
- [ ] `LimitRange container defaults` — default CPU/memory
- [ ] `LimitRange min/max enforcement` — constraint validation
- [ ] `LimitRange ratio constraints` — CPU/memory ratios
- [ ] `LimitRange PVC limits` — storage constraints

File: Create `limitrange_controller_test.rs`
Estimated: 6-8 tests

## Priority 8: Lifecycle & Maintenance (Phase 8)

### 8.1 Node Lifecycle Extended
Source: `kubernetes/test/e2e/node/lifecycle.go`

**Missing tests:**
- [ ] `Node shutdown taint` — graceful node shutdown
- [ ] `Node condition monitoring` — Ready, MemoryPressure, etc.
- [ ] `Node lease renewal` — heartbeat mechanism
- [ ] `Node resources capacity` — allocatable vs capacity

File: Extend `node_controller_test.rs`
Estimated: 8-10 tests

### 8.2 Pod Lifecycle Extended
Source: `kubernetes/test/e2e/common/pod_lifecycle.go`

**Missing tests:**
- [ ] `Pod graceful termination` — terminationGracePeriodSeconds
- [ ] `Pod preStop hooks` — lifecycle hook execution
- [ ] `Pod postStart hooks` — startup hook execution
- [ ] `Pod restart policies` — Always, OnFailure, Never
- [ ] `Pod QoS classes` — Guaranteed, Burstable, BestEffort

File: Create `pod_lifecycle_extended_test.rs`
Estimated: 10-12 tests

### 8.3 TTL After Finished
Source: `kubernetes/test/e2e/framework/ttl.go`

**Missing tests:**
- [ ] `TTL controller Job cleanup` — automatic Job deletion
- [ ] `TTL controller Pod cleanup` — automatic Pod deletion
- [ ] `TTL negative values` — immediate deletion

File: Extend `ttl_controller_test.rs`
Estimated: 4-6 tests

### 8.4 Garbage Collector Extended
Source: `kubernetes/test/e2e/framework/gc.go`

**Missing tests:**
- [ ] `GC orphan dependency` — orphaned resource handling
- [ ] `GC cross-namespace references` — namespace boundary GC
- [ ] `GC finalizer blocking` — finalizer preventing deletion
- [ ] `GC owner reference updates` — changing ownership

File: Extend `garbage_collector_test.rs`
Estimated: 8-10 tests

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

| Phase | Area | Tests Planned | Tests Implemented | % Complete |
|-------|------|---------------|-------------------|------------|
| 1 | Job Extended | 15-20 | 0 | 0% |
| 1 | StatefulSet Extended | 15-20 | 0 | 0% |
| 1 | DaemonSet Extended | 12-15 | 0 | 0% |
| 1 | Deployment Extended | 12-15 | 0 | 0% |
| 2 | PV Controller | 12-15 | 0 | 0% |
| 2 | PVC Controller | 8-10 | 0 | 0% |
| 2 | StorageClass | 6-8 | 0 | 0% |
| 3 | Service LB | 10-12 | 0 | 0% |
| 3 | Ingress | 10-12 | 0 | 0% |
| 3 | NetworkPolicy | 10-12 | 0 | 0% |
| 4 | HPA Extended | 12-15 | 0 | 0% |
| 5 | CronJob Extended | 8-10 | 0 | 0% |
| 6 | RBAC | 10-12 | 0 | 0% |
| 7 | ConfigMap/Secret | 16-20 | 0 | 0% |
| 8 | Lifecycle | 18-22 | 0 | 0% |
| **Total** | | **~200** | **~66 existing** | **25%** |

## Next Steps

1. **Start with Phase 1** — Pick one controller (recommend Job or DaemonSet)
2. **Create test file** — Follow the pattern in existing conformance tests
3. **Implement 3-5 tests** — Start small, get the pattern right
4. **Run and verify** — Ensure tests pass locally
5. **Document** — Update coverage matrix in respective `.md` file
6. **Repeat** — Continue through the priority list

## References

- Kubernetes Go Tests: https://github.com/kubernetes/kubernetes/tree/master/test/e2e
- Sonobuoy Conformance: https://github.com/vmware-tanzu/sonobuoy
- Rusternetes Conformance Docs: `/workspace/docs/conformance/`
- Existing Test Patterns: `/workspace/crates/controller-manager/tests/conformance_*.rs`
