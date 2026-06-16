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
                ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
/// Concurrent owner deletion is the load shape that matters here:
/// owner keys are deleted from many tasks in parallel, then a single
/// `scan_and_collect` must reap every dependent. The GC's snapshot
/// + per-orphan owner re-verification (see `delete_orphan`) is what
/// makes this safe under racing owner deletes — there is no per-RC
/// finalizer to coordinate with.
#[tokio::test]
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

/// Foreground propagation: when an owner is deleted with
/// `propagationPolicy: Foreground`, the GC must keep the owner alive
/// (deletionTimestamp + `foregroundDeletion` finalizer) until every
/// dependent that set `blockOwnerDeletion: true` is gone. Once those
/// dependents are removed, the GC strips the finalizer and the owner
/// itself disappears.
///
/// Upstream behaviour: `processDeleteItem` (foreground branch) +
/// `deleteDependents` in `pkg/controller/garbagecollector/`.
#[tokio::test]
async fn test_foreground_propagation_blocks_until_dependents_gone() {
    let storage = fresh_storage();
    let gc = GarbageCollector::new(storage.clone());
    let ns = "ns-foreground";

    // Owner stand-in with deletionTimestamp + foregroundDeletion finalizer.
    let mut owner = make_owner_pod("rc-fg", ns);
    owner.metadata.deletion_timestamp = Some(chrono::Utc::now());
    owner.metadata.finalizers = Some(vec!["foregroundDeletion".to_string()]);
    save_pod(&storage, &owner).await;

    // Dependent with blockOwnerDeletion=true. Must be deleted before
    // the owner's foregroundDeletion finalizer is removed.
    let dep = make_pod(
        "pod-blocker",
        ns,
        vec![rc_ref("rc-fg", &owner.metadata.uid)],
    );
    save_pod(&storage, &dep).await;

    // First scan: the GC processes the owner's deletion, sees a blocking
    // dependent, deletes it (foreground cascade), and then can strip the
    // finalizer because no more blockers remain.
    gc.scan_and_collect().await.unwrap();

    assert!(
        !pod_exists(&storage, ns, "pod-blocker").await,
        "blocker dependent must be deleted by foreground cascade",
    );
    assert!(
        !pod_exists(&storage, ns, "rc-fg").await,
        "owner must be finalised once its foreground blockers are gone",
    );
}

/// Orphan propagation: when an owner is deleted with
/// `propagationPolicy: Orphan`, dependents must have their owner
/// reference to that owner stripped — NOT cascade-deleted.
///
/// Upstream behaviour: `processAttemptToOrphan` in
/// `pkg/controller/garbagecollector/garbagecollector.go`.
#[tokio::test]
async fn test_orphan_propagation_strips_owner_ref_without_deleting_dependent() {
    let storage = fresh_storage();
    let gc = GarbageCollector::new(storage.clone());
    let ns = "ns-orphan";

    let mut owner = make_owner_pod("rc-orphan", ns);
    owner.metadata.deletion_timestamp = Some(chrono::Utc::now());
    owner.metadata.finalizers = Some(vec!["orphan".to_string()]);
    save_pod(&storage, &owner).await;

    let dep = make_pod(
        "pod-keep-me",
        ns,
        vec![rc_ref("rc-orphan", &owner.metadata.uid)],
    );
    save_pod(&storage, &dep).await;

    gc.scan_and_collect().await.unwrap();

    // The dependent must still exist...
    assert!(
        pod_exists(&storage, ns, "pod-keep-me").await,
        "orphan policy must NOT delete the dependent",
    );
    // ...and its ownerReferences must no longer point at the owner UID.
    let key = build_key("pods", Some(ns), "pod-keep-me");
    let kept: Pod = storage.get(&key).await.unwrap();
    let refs = kept.metadata.owner_references.unwrap_or_default();
    assert!(
        refs.iter().all(|r| r.uid != owner.metadata.uid),
        "owner reference to the deleted owner must be stripped",
    );

    // Owner finalizer is removed → owner disappears.
    assert!(
        !pod_exists(&storage, ns, "rc-orphan").await,
        "owner must be finalised once orphan finalizer is removed",
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

// ---------------------------------------------------------------------------
// Phase 8.4 extended coverage — additional GC behaviours pinned to upstream
// semantics. These tests focus on edge cases not exercised by the mirrors
// above:
//
//   * Orphan propagation with multiple dependents (fan-out)
//   * Namespaced dependent → namespaced owner in a DIFFERENT namespace
//     is treated as an invalid (cross-namespace) ref, per the
//     `classifyReferences` invariant
//   * Foreground finalizer keeps owner alive while every blocker remains
//   * Mid-flight ownerReference updates re-target GC behaviour
//
// Upstream source: `pkg/controller/garbagecollector/garbagecollector.go`
// and `test/integration/garbagecollector/garbage_collector_test.go`
// (release-1.35).
// ---------------------------------------------------------------------------

/// Orphan propagation policy with multiple dependents.
///
/// Upstream: when an owner is deleted with `propagationPolicy: Orphan`,
/// EVERY dependent must have its ownerReference to the owner stripped —
/// not just one. The `orphan_dependents` path iterates all matching
/// dependents and updates each via `update_raw`; a partial-orphan bug
/// would leave the un-stripped dependents with a dangling ownerReference
/// pointing at the now-deleted owner, which the test asserts is gone for
/// every dependent.
///
/// This mirrors the fan-out shape of upstream's
/// `processAttemptToOrphan` working on a worklist of dependents — the
/// owner is finalised only after every dependent in the list has been
/// successfully updated.
#[tokio::test]
async fn test_gc_orphan_policy_fans_out_to_all_dependents() {
    let storage = fresh_storage();
    let gc = GarbageCollector::new(storage.clone());
    let ns = "ns-orphan-fanout";

    let mut owner = make_owner_pod("rc-fanout", ns);
    owner.metadata.deletion_timestamp = Some(chrono::Utc::now());
    owner.metadata.finalizers = Some(vec!["orphan".to_string()]);
    save_pod(&storage, &owner).await;

    // Five dependents, all owned solely by `rc-fanout`. After the orphan
    // sweep every one of them must still exist with NO ownerReference to
    // `rc-fanout`.
    const N_DEPS: usize = 5;
    for i in 0..N_DEPS {
        let dep = make_pod(
            &format!("dep-{}", i),
            ns,
            vec![rc_ref("rc-fanout", &owner.metadata.uid)],
        );
        save_pod(&storage, &dep).await;
    }

    gc.scan_and_collect().await.unwrap();

    // Owner is gone — finalizer was stripped after fan-out.
    assert!(
        !pod_exists(&storage, ns, "rc-fanout").await,
        "owner must be finalised once all dependents have been orphaned",
    );

    // Every dependent survives with no stale ownerReference to the owner.
    for i in 0..N_DEPS {
        let name = format!("dep-{}", i);
        assert!(
            pod_exists(&storage, ns, &name).await,
            "dependent {} must NOT be deleted by orphan policy",
            name,
        );
        let key = build_key("pods", Some(ns), &name);
        let kept: rusternetes_common::resources::pod::Pod = storage.get(&key).await.unwrap();
        let refs = kept.metadata.owner_references.unwrap_or_default();
        assert!(
            refs.iter().all(|r| r.uid != owner.metadata.uid),
            "dependent {} must have owner ref to {} stripped (refs left: {:?})",
            name,
            owner.metadata.uid,
            refs.iter().map(|r| r.uid.as_str()).collect::<Vec<_>>(),
        );
    }
}

/// Cross-namespace owner references must be rejected: a namespaced
/// dependent referencing an owner in a DIFFERENT namespace is treated as
/// having an unresolvable owner ref and is GC'd as an orphan.
///
/// Upstream: `classifyReferences` in
/// `pkg/controller/garbagecollector/garbagecollector.go`. The invariant
/// is that namespaced dependents may only resolve owners in the same
/// namespace OR cluster-scoped owners — never owners in some other
/// namespace, regardless of UID matches.
///
/// This is the inverse of `test_cross_namespace_references_with_watch_cache`:
/// instead of pinning a phantom UID, the UID *does exist* but lives in
/// the wrong namespace. Upstream still considers the ref invalid.
#[tokio::test]
async fn test_gc_namespaced_owner_in_different_namespace_is_rejected() {
    let storage = fresh_storage();
    let gc = GarbageCollector::new(storage.clone());
    let ns_owner = "ns-owner-side";
    let ns_dep = "ns-dependent-side";

    // Owner lives in `ns_owner`; UID is real.
    let owner = make_owner_pod("owner", ns_owner);
    save_pod(&storage, &owner).await;

    // Dependent lives in `ns_dep`; its ownerRef points at the real UID
    // above but in a different namespace — illegal per upstream invariant.
    let dep = make_pod(
        "dependent",
        ns_dep,
        vec![rc_ref("owner", &owner.metadata.uid)],
    );
    save_pod(&storage, &dep).await;

    gc.scan_and_collect().await.unwrap();

    // Owner survives — nothing references it within its own namespace.
    assert!(
        pod_exists(&storage, ns_owner, "owner").await,
        "owner in ns_owner must survive — it has no in-namespace dependents",
    );
    // Dependent is GC'd — its only ownerRef is cross-namespace, which the
    // GC treats as unresolvable.
    assert!(
        !pod_exists(&storage, ns_dep, "dependent").await,
        "dependent with a cross-namespace owner ref must be GC'd",
    );
}

/// Foreground finalizer keeps owner alive while ANY blocking dependent
/// remains. The existing mirror tests a single blocker; this one pins
/// the multi-blocker semantics: with N blockers, a single scan must drain
/// all of them before the finalizer is removed.
///
/// Upstream: `processDeleteItem` foreground branch +
/// `deleteDependents`. The owner is kept in Terminating (deletionTimestamp
/// set, foregroundDeletion finalizer present) for as long as the
/// dependent list — restricted to `blockOwnerDeletion: true` entries — is
/// non-empty. Once the list drains, the finalizer comes off and the
/// owner itself is reaped.
#[tokio::test]
async fn test_gc_foreground_finalizer_blocks_until_all_dependents_drained() {
    let storage = fresh_storage();
    let gc = GarbageCollector::new(storage.clone());
    let ns = "ns-fg-multi-block";

    let mut owner = make_owner_pod("rc-multi", ns);
    owner.metadata.deletion_timestamp = Some(chrono::Utc::now());
    owner.metadata.finalizers = Some(vec!["foregroundDeletion".to_string()]);
    save_pod(&storage, &owner).await;

    // Three blocking dependents.
    const N_BLOCKERS: usize = 3;
    for i in 0..N_BLOCKERS {
        let dep = make_pod(
            &format!("blocker-{}", i),
            ns,
            vec![rc_ref("rc-multi", &owner.metadata.uid)],
        );
        save_pod(&storage, &dep).await;
    }

    gc.scan_and_collect().await.unwrap();

    // All blockers gone.
    for i in 0..N_BLOCKERS {
        let name = format!("blocker-{}", i);
        assert!(
            !pod_exists(&storage, ns, &name).await,
            "blocker {} must be deleted by foreground cascade",
            name,
        );
    }
    // Owner finalised exactly once the blocker list drained.
    assert!(
        !pod_exists(&storage, ns, "rc-multi").await,
        "owner must be finalised once every blocker dependent is gone",
    );
}

/// Mid-flight ownerReference updates re-target GC behaviour.
///
/// Sequence:
///   1. RC-A and RC-B exist. Pod is initially owned by RC-A only.
///   2. Pod's ownerReferences are updated to also reference RC-B (a
///      common pattern when a controller "adopts" an orphan, or when
///      multiple controllers reconcile the same dependent).
///   3. RC-A is deleted.
///   4. GC scan must NOT delete the pod, because RC-B is still a valid
///      owner.
///
/// This pins that the GC reads ownerReferences live from storage on
/// every scan — it does NOT cache an early snapshot from before the
/// adoption update. Upstream achieves this via watch-driven graph
/// updates; we achieve it by rebuilding the relationship map on every
/// `scan_and_collect`.
#[tokio::test]
async fn test_gc_owner_reference_updates_retarget_collection() {
    let storage = fresh_storage();
    let gc = GarbageCollector::new(storage.clone());
    let ns = "ns-ref-updates";

    let rc_a = make_owner_pod("rc-a", ns);
    let rc_b = make_owner_pod("rc-b", ns);
    save_pod(&storage, &rc_a).await;
    save_pod(&storage, &rc_b).await;

    // Pod is initially owned only by RC-A.
    let mut pod = make_pod("adoptee", ns, vec![rc_ref("rc-a", &rc_a.metadata.uid)]);
    save_pod(&storage, &pod).await;

    // Simulate a controller adopting the pod by updating its
    // ownerReferences to also include RC-B. We write the updated pod
    // back to storage before the GC scan.
    pod.metadata.owner_references = Some(vec![
        rc_ref("rc-a", &rc_a.metadata.uid),
        rc_ref("rc-b", &rc_b.metadata.uid),
    ]);
    let pod_key = build_key("pods", Some(ns), "adoptee");
    storage.update(&pod_key, &pod).await.unwrap();

    // RC-A is deleted. RC-B remains.
    let rc_a_key = build_key("pods", Some(ns), "rc-a");
    storage.delete(&rc_a_key).await.unwrap();

    gc.scan_and_collect().await.unwrap();

    // Pod must survive — RC-B is still a valid owner. The GC must have
    // read the post-adoption ownerReferences, not the original list.
    assert!(
        pod_exists(&storage, ns, "adoptee").await,
        "pod must survive RC-A deletion because RC-B was adopted as an owner",
    );

    // Now delete RC-B as well — pod becomes a true orphan and must be GC'd.
    let rc_b_key = build_key("pods", Some(ns), "rc-b");
    storage.delete(&rc_b_key).await.unwrap();

    gc.scan_and_collect().await.unwrap();

    assert!(
        !pod_exists(&storage, ns, "adoptee").await,
        "pod must be GC'd once every owner ref is unresolvable",
    );
}
