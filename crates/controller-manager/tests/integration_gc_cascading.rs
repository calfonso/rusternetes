//! RED-state TDD mirror of upstream Kubernetes garbage-collector integration
//! tests, ported to drive `GarbageCollector::scan_and_collect` directly against
//! `MemoryStorage`.
//!
//! Upstream source (permalink, release-1.35):
//!   https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/garbagecollector/garbage_collector_test.go
//!
//! Mirrored upstream tests (rust fn keeps the upstream Go fn name):
//!   * TestCascadingDeletion
//!   * TestCreateWithNonExistentOwner
//!   * TestStressingCascadingDeletion
//!   * TestCrossNamespaceReferencesWithWatchCache
//!
//! Deferred upstream tests (NOT mirrored here — out of scope for the
//! in-process MemoryStorage driver) and the reason each is skipped:
//!   * TestCrossNamespaceReferencesWithoutWatchCache — duplicate of the
//!     watch-cache variant; the watch-cache toggle has no analogue in our
//!     storage layer, so a second mirror would be tautological.
//!   * TestOrphaning, TestSolidOwnerDoesNotBlockWaitingOwner,
//!     TestNonBlockingOwnerRefDoesNotBlock, TestBlockingOwnerRefDoesBlock —
//!     foreground / blockOwnerDeletion semantics. Our GC currently honours
//!     `blockOwnerDeletion` only via the api-server delete pathway, not via
//!     `scan_and_collect`, so a unit-level test would assert behaviour the
//!     collector deliberately does not own. Tracked separately.
//!   * TestCustomResourceCascadingDeletion, TestMixedRelationships,
//!     TestCRDDeletionCascading, TestCascadingDeleteOnCRDConversionFailure —
//!     require live CRD registration through the api-server router. The
//!     in-process MemoryStorage harness has no CRD handler.
//!   * TestDoubleDeletionWithFinalizer — requires a real finalizer
//!     reconciler that re-issues DELETE; covered by
//!     `garbage_collector_idempotency_test`.
//!
//! Style note: these are RED-state pins. Tests that currently fail because
//! the matching collector behaviour is incomplete are marked `#[ignore]`
//! with an explanatory message, per the project's TDD convention. The
//! `#[ignore]` is the failing-spec marker — removing it is the unit of work
//! for whoever lands the corresponding behaviour.
//!
//! Part of the /batch landing upstream integration-test mirrors as
//! RED-state TDD pins.

use rusternetes_common::resources::pod::{Container, Pod, PodSpec, PodStatus};
use rusternetes_common::types::{ObjectMeta, OwnerReference, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::garbage_collector::GarbageCollector;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Fixture helpers — kept tiny and local so each test reads top-to-bottom.
// ---------------------------------------------------------------------------

fn fresh_storage() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

/// Minimal pod fixture. Upstream uses `newPod` from the integration helpers;
/// the surface we need is name + namespace + (optional) owner refs.
fn make_pod(name: &str, namespace: &str, owner_refs: Vec<OwnerReference>) -> Pod {
    let mut metadata = ObjectMeta::new(name);
    metadata.namespace = Some(namespace.to_string());
    metadata.uid = uuid::Uuid::new_v4().to_string();
    if !owner_refs.is_empty() {
        metadata.owner_references = Some(owner_refs);
    }

    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata,
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "main".to_string(),
                image: "nginx:1.25-alpine".to_string(),
                image_pull_policy: Some("IfNotPresent".to_string()),
                ports: Some(vec![]),
                env: None,
                volume_mounts: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
                resources: None,
                working_dir: None,
                command: None,
                args: None,
                restart_policy: None,
                resize_policy: None,
                security_context: None,
                lifecycle: None,
                termination_message_path: None,
                termination_message_policy: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                env_from: None,
                volume_devices: None,
            }],
            init_containers: None,
            restart_policy: Some("Always".to_string()),
            node_selector: None,
            node_name: None,
            volumes: None,
            affinity: None,
            tolerations: None,
            service_account_name: None,
            service_account: None,
            priority: None,
            priority_class_name: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            automount_service_account_token: None,
            ephemeral_containers: None,
            overhead: None,
            scheduler_name: None,
            topology_spread_constraints: None,
            resource_claims: None,
            active_deadline_seconds: None,
            dns_policy: None,
            dns_config: None,
            security_context: None,
            image_pull_secrets: None,
            share_process_namespace: None,
            readiness_gates: None,
            runtime_class_name: None,
            enable_service_links: None,
            preemption_policy: None,
            host_users: None,
            set_hostname_as_fqdn: None,
            termination_grace_period_seconds: None,
            host_aliases: None,
            os: None,
            scheduling_gates: None,
            resources: None,
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
        }),
    }
}

/// Upstream uses ReplicationController as the owner. Our GC follows refs by
/// `uid` only, so we can stand in any owner kind — a "controller pod" is
/// sufficient to drive the scan without pulling in the RC controller.
fn make_owner_pod(name: &str, namespace: &str) -> Pod {
    make_pod(name, namespace, vec![])
}

fn rc_ref(name: &str, uid: &str) -> OwnerReference {
    OwnerReference::new("v1", "ReplicationController", name, uid)
        .with_controller(true)
        .with_block_owner_deletion(true)
}

async fn save_pod(storage: &Arc<MemoryStorage>, pod: &Pod) {
    let key = build_key(
        "pods",
        pod.metadata.namespace.as_deref(),
        &pod.metadata.name,
    );
    storage.create(&key, pod).await.unwrap();
}

async fn pod_exists(storage: &Arc<MemoryStorage>, namespace: &str, name: &str) -> bool {
    let key = build_key("pods", Some(namespace), name);
    storage.get::<Pod>(&key).await.is_ok()
}

async fn list_pods(storage: &Arc<MemoryStorage>, namespace: &str) -> Vec<Pod> {
    let prefix = format!("/registry/pods/{}/", namespace);
    storage.list(&prefix).await.unwrap()
}

// ---------------------------------------------------------------------------
// Mirrored tests. Each fn name matches the upstream Go fn 1:1.
// ---------------------------------------------------------------------------

/// Mirror of `TestCascadingDeletion`
/// (test/integration/garbagecollector/garbage_collector_test.go).
///
/// Upstream: two RCs + three pods (one owned only by RC-A, one owned by both
/// RCs, one with no owner). Delete RC-A with non-orphan propagation; the
/// solo-owned pod is GC'd, the multi-owner pod and the unowned pod survive.
#[tokio::test]
async fn test_cascading_deletion() {
    let storage = fresh_storage();
    let gc = GarbageCollector::new(storage.clone());
    let ns = "ns-cascading-deletion";

    // Two owner stand-ins. Upstream uses RCs; we use owner-pods because the
    // GC keys off `uid` and does not look up `kind`.
    let rc_a = make_owner_pod("rc-a", ns);
    let rc_b = make_owner_pod("rc-b", ns);
    save_pod(&storage, &rc_a).await;
    save_pod(&storage, &rc_b).await;

    // pod-solo: owned only by RC-A. Must be GC'd after RC-A goes away.
    let pod_solo = make_pod("pod-solo", ns, vec![rc_ref("rc-a", &rc_a.metadata.uid)]);
    // pod-shared: owned by both. Must survive — RC-B is still valid.
    let pod_shared = make_pod(
        "pod-shared",
        ns,
        vec![
            rc_ref("rc-a", &rc_a.metadata.uid),
            rc_ref("rc-b", &rc_b.metadata.uid),
        ],
    );
    // pod-orphan: no owner. Must survive.
    let pod_no_owner = make_pod("pod-no-owner", ns, vec![]);
    save_pod(&storage, &pod_solo).await;
    save_pod(&storage, &pod_shared).await;
    save_pod(&storage, &pod_no_owner).await;

    // Delete RC-A. Upstream uses propagation=Background — for an in-process
    // driver, deleting the key from storage is the post-finalizer steady
    // state we observe after the api-server's background-delete path runs.
    let rc_a_key = build_key("pods", Some(ns), "rc-a");
    storage.delete(&rc_a_key).await.unwrap();

    gc.scan_and_collect().await.unwrap();

    assert!(
        !pod_exists(&storage, ns, "pod-solo").await,
        "pod-solo must be GC'd once its only owner (RC-A) is gone"
    );
    assert!(
        pod_exists(&storage, ns, "pod-shared").await,
        "pod-shared must survive because RC-B is still a valid owner"
    );
    assert!(
        pod_exists(&storage, ns, "pod-no-owner").await,
        "pod-no-owner must survive — it has no owner refs"
    );
}

/// Mirror of `TestCreateWithNonExistentOwner`.
///
/// Upstream creates a Pod whose owner ref points at an RC UID that was
/// never written to storage, then waits for the pod to disappear.
#[tokio::test]
async fn test_create_with_non_existent_owner() {
    let storage = fresh_storage();
    let gc = GarbageCollector::new(storage.clone());
    let ns = "ns-non-existent-owner";

    let phantom_uid = uuid::Uuid::new_v4().to_string();
    let pod = make_pod("orphan", ns, vec![rc_ref("ghost-rc", &phantom_uid)]);
    save_pod(&storage, &pod).await;

    gc.scan_and_collect().await.unwrap();

    assert!(
        !pod_exists(&storage, ns, "orphan").await,
        "pod with an owner ref to a non-existent RC must be GC'd"
    );
}

/// Mirror of `TestStressingCascadingDeletion`.
///
/// Upstream creates 10 collections of 3 RCs each (30 RCs total) with 4
/// pods per RC, exercising orphan / foreground / background propagation
/// across the collections and asserting 120 pods remain. Our pin is
/// scaled down (10 RCs × 3 pods each = 30 pods) but preserves the
/// concurrency shape: all RCs deleted in parallel, GC scan reaps every
/// dependent.
///
/// RED-state: ignored because our `scan_and_collect` is single-pass and
/// does not yet model the orphan / foreground propagation variants the
/// upstream test exercises. Re-enable when those code paths land.
#[tokio::test]
#[ignore = "GC does not yet differentiate orphan / foreground propagation under concurrent owner deletion"]
async fn test_stressing_cascading_deletion() {
    let storage = fresh_storage();
    let gc = GarbageCollector::new(storage.clone());
    let ns = "ns-stress";

    const N_RCS: usize = 10;
    const PODS_PER_RC: usize = 3;

    let mut rc_uids = Vec::with_capacity(N_RCS);
    for i in 0..N_RCS {
        let rc = make_owner_pod(&format!("rc-{}", i), ns);
        rc_uids.push(rc.metadata.uid.clone());
        save_pod(&storage, &rc).await;
        for j in 0..PODS_PER_RC {
            let pod = make_pod(
                &format!("pod-{}-{}", i, j),
                ns,
                vec![rc_ref(&format!("rc-{}", i), &rc.metadata.uid)],
            );
            save_pod(&storage, &pod).await;
        }
    }

    // Delete every RC concurrently — emulates the upstream "stress" load
    // where many owner deletions race against the GC graph.
    let mut handles = Vec::new();
    for i in 0..N_RCS {
        let storage = storage.clone();
        let key = build_key("pods", Some(ns), &format!("rc-{}", i));
        handles.push(tokio::spawn(async move {
            storage.delete(&key).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    gc.scan_and_collect().await.unwrap();

    // Every dependent must be gone: zero pods remain in the namespace.
    let remaining = list_pods(&storage, ns).await;
    assert_eq!(
        remaining.len(),
        0,
        "all dependent pods must be GC'd; {} survivors found",
        remaining.len()
    );
}

/// Mirror of `TestCrossNamespaceReferencesWithWatchCache`.
///
/// Upstream creates a valid parent/child pair in ns-A and 25+ invalid
/// children in ns-B that reference a UID that exists only in ns-A. Per
/// upstream contract, cross-namespace owner refs are unresolvable for
/// namespaced dependents — so the invalid ns-B children must be GC'd
/// while ns-A's parent/child pair persists.
#[tokio::test]
async fn test_cross_namespace_references_with_watch_cache() {
    let storage = fresh_storage();
    let gc = GarbageCollector::new(storage.clone());
    let ns_a = "ns-cross-a";
    let ns_b = "ns-cross-b";

    // Valid parent + child in ns-A.
    let parent_a = make_owner_pod("parent-a", ns_a);
    save_pod(&storage, &parent_a).await;
    let child_a = make_pod(
        "child-a",
        ns_a,
        vec![rc_ref("parent-a", &parent_a.metadata.uid)],
    );
    save_pod(&storage, &child_a).await;

    // 25 invalid cross-namespace children in ns-B referencing a UID that
    // exists only in ns-A (or, in the strict mirror, a phantom UID).
    // Upstream's invariant: cross-namespace owner refs are always invalid
    // for namespaced dependents.
    for i in 0..25 {
        let invalid = make_pod(
            &format!("invalid-{}", i),
            ns_b,
            // Points at parent-a (which lives in ns_a) — illegal cross-ns ref.
            vec![rc_ref("parent-a", &parent_a.metadata.uid)],
        );
        save_pod(&storage, &invalid).await;
    }

    gc.scan_and_collect().await.unwrap();

    // ns-A's pair survives.
    assert!(
        pod_exists(&storage, ns_a, "parent-a").await,
        "valid parent in ns-A must survive"
    );
    assert!(
        pod_exists(&storage, ns_a, "child-a").await,
        "valid child in ns-A must survive"
    );

    // Every invalid cross-namespace child in ns-B is GC'd.
    let surviving_b = list_pods(&storage, ns_b).await;
    assert_eq!(
        surviving_b.len(),
        0,
        "every cross-namespace invalid child in ns-B must be GC'd; {} survived",
        surviving_b.len()
    );
}
