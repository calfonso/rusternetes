//! Scoped mirror of the upstream Kubernetes v1.35 e2e file
//! `test/e2e/scheduling/priorityclass.go` as a RED-state TDD pin.
//!
//! Upstream permalink (release-1.35):
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/scheduling/priorityclass.go>
//!
//! The upstream e2e covers four behaviors that a real
//! `PriorityClassController` must implement (today our controller is a no-op
//! stub — see `crates/controller-manager/src/controllers/priorityclass.rs`):
//!
//!   1. **Preemption** — when a high-priority pending pod cannot be scheduled
//!      because the cluster is already running a lower-priority pod, the
//!      lower-priority pod is evicted (`deletionTimestamp` set) so the
//!      pending pod can run.
//!   2. **Global default** — at most one `PriorityClass` may have
//!      `globalDefault: true`. Pods that omit `priorityClassName` inherit
//!      that class's numeric `value` into `pod.spec.priority`.
//!   3. **Namespace default** — installations can opt into a per-namespace
//!      default priority (annotated on the namespace or selected by label);
//!      pods in such a namespace that omit `priorityClassName` inherit the
//!      namespace default rather than the cluster-wide default.
//!   4. **Value ordering** — `PriorityClass.value` is a signed 32-bit integer
//!      that strictly orders pods for scheduling and preemption; the
//!      controller must surface this ordering on `pod.spec.priority` so the
//!      scheduler can rank pods.
//!
//! Each `#[tokio::test]` is `#[ignore = "RED-state: PriorityClassController is
//! a stub"]` so `cargo test` stays green while the controller is unfinished.
//! Remove the `#[ignore]` once the corresponding behavior is implemented.

use rusternetes_common::resources::{Container, Pod, PodSpec, PriorityClass};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::priorityclass::PriorityClassController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

/// Minimal Pod with a single `pause:latest` container — image identity is
/// irrelevant to priority handling, only `priorityClassName` / `priority`
/// matter.
fn make_pod(name: &str, namespace: &str, priority_class_name: Option<&str>) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "container".to_string(),
                image: "pause:latest".to_string(),
                ..Default::default()
            }],
            priority_class_name: priority_class_name.map(str::to_string),
            ..Default::default()
        }),
        status: None,
    }
}

/// Create + persist a PriorityClass at the cluster-scoped storage key.
async fn create_priority_class(storage: &Arc<MemoryStorage>, pc: PriorityClass) {
    let key = build_key("priorityclasses", None, &pc.metadata.name);
    storage.create(&key, &pc).await.unwrap();
}

/// Reload a Pod from storage so the test asserts on the controller-mutated
/// copy, not the locally-built one.
async fn reload_pod(storage: &Arc<MemoryStorage>, namespace: &str, name: &str) -> Pod {
    storage
        .get::<Pod>(&build_key("pods", Some(namespace), name))
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// 1. Preemption: lower-priority pod is evicted to make room
// ---------------------------------------------------------------------------

/// Mirrors upstream `It("validates lower priority pod preemption ...")`:
/// a pending high-priority pod that cannot fit triggers the
/// `PriorityClassController` to mark the running low-priority pod for
/// eviction (deletionTimestamp).
#[tokio::test]
#[ignore = "RED-state: PriorityClassController is a stub"]
async fn priority_class_preemption_evicts_lower_priority_pod() {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    let controller = PriorityClassController::new(storage.clone());

    create_priority_class(&storage, PriorityClass::new("low", 100)).await;
    create_priority_class(&storage, PriorityClass::new("high", 1_000_000)).await;

    // Low-priority victim — already Running.
    let victim = make_pod("victim", "default", Some("low"));
    storage
        .create(&build_key("pods", Some("default"), "victim"), &victim)
        .await
        .unwrap();

    // High-priority pending pod that needs a slot the cluster cannot provide
    // without evicting `victim`.
    let preemptor = make_pod("preemptor", "default", Some("high"));
    storage
        .create(&build_key("pods", Some("default"), "preemptor"), &preemptor)
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let victim_after = reload_pod(&storage, "default", "victim").await;
    assert!(
        victim_after.metadata.deletion_timestamp.is_some(),
        "lower-priority pod 'victim' should be marked for eviction when a higher-priority pod \
         is pending; got deletion_timestamp = {:?}",
        victim_after.metadata.deletion_timestamp
    );
}

// ---------------------------------------------------------------------------
// 2. Global default: pods without priorityClassName inherit cluster default
// ---------------------------------------------------------------------------

/// Mirrors upstream "globalDefault" behavior: exactly one PriorityClass with
/// `globalDefault: true` exists, and every Pod that omits
/// `priorityClassName` ends up with `spec.priority` equal to that class's
/// `value`.
#[tokio::test]
async fn priority_class_global_default_applies_to_unset_pods() {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    let controller = PriorityClassController::new(storage.clone());

    let default_pc = PriorityClass::new("cluster-default", 500).with_global_default(true);
    create_priority_class(&storage, default_pc).await;

    // Pod with no priorityClassName — must inherit from the cluster default.
    let pod = make_pod("no-pc", "default", None);
    storage
        .create(&build_key("pods", Some("default"), "no-pc"), &pod)
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let after = reload_pod(&storage, "default", "no-pc").await;
    let spec = after
        .spec
        .expect("pod must retain its spec after reconcile");
    assert_eq!(
        spec.priority,
        Some(500),
        "pod without priorityClassName should inherit globalDefault PriorityClass value"
    );
}

// ---------------------------------------------------------------------------
// 3. Namespace default: per-namespace default priority wins over cluster default
// ---------------------------------------------------------------------------

/// Some installations annotate namespaces with a default PriorityClass name
/// (`scheduling.k8s.io/default-priority-class`). Pods in such a namespace
/// that omit `priorityClassName` should inherit the namespace default, not
/// the cluster `globalDefault`.
#[tokio::test]
async fn priority_class_namespace_default_overrides_global_default() {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    let controller = PriorityClassController::new(storage.clone());

    // Cluster-wide default.
    create_priority_class(
        &storage,
        PriorityClass::new("cluster-default", 100).with_global_default(true),
    )
    .await;
    // Namespace-scoped default (just a regular PriorityClass; the namespace
    // points at it via annotation).
    create_priority_class(&storage, PriorityClass::new("team-default", 750)).await;

    // Create a Namespace that opts in via annotation. Using serde_json::Value
    // keeps this test agnostic to the controller-side representation — only
    // the persisted shape matters.
    let namespace_key = build_key("namespaces", None, "team-a");
    let namespace = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": "team-a",
            "annotations": {
                "scheduling.k8s.io/default-priority-class": "team-default"
            }
        }
    });
    storage.create(&namespace_key, &namespace).await.unwrap();

    let pod = make_pod("ns-pod", "team-a", None);
    storage
        .create(&build_key("pods", Some("team-a"), "ns-pod"), &pod)
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let after = reload_pod(&storage, "team-a", "ns-pod").await;
    let spec = after
        .spec
        .expect("pod must retain its spec after reconcile");
    assert_eq!(
        spec.priority,
        Some(750),
        "pod in a namespace with a per-namespace default should inherit the namespace default \
         (750) and NOT the cluster globalDefault (100)"
    );
}

// ---------------------------------------------------------------------------
// 4. Value ordering: higher PriorityClass.value => higher pod.spec.priority
// ---------------------------------------------------------------------------

/// Each pod's resolved `spec.priority` must reflect the strict numeric
/// ordering of the named PriorityClass values; this is what the scheduler
/// and preemptor consume.
#[tokio::test]
async fn priority_class_value_ordering_is_preserved_on_pods() {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    let controller = PriorityClassController::new(storage.clone());

    create_priority_class(&storage, PriorityClass::new("p-low", -10)).await;
    create_priority_class(&storage, PriorityClass::new("p-mid", 0)).await;
    create_priority_class(&storage, PriorityClass::new("p-high", 1_000)).await;
    create_priority_class(&storage, PriorityClass::new("p-system", 2_000_000_000)).await;

    for (name, class) in [
        ("pod-low", "p-low"),
        ("pod-mid", "p-mid"),
        ("pod-high", "p-high"),
        ("pod-system", "p-system"),
    ] {
        let pod = make_pod(name, "default", Some(class));
        storage
            .create(&build_key("pods", Some("default"), name), &pod)
            .await
            .unwrap();
    }

    controller.reconcile_all().await.unwrap();

    let low = reload_pod(&storage, "default", "pod-low")
        .await
        .spec
        .and_then(|s| s.priority);
    let mid = reload_pod(&storage, "default", "pod-mid")
        .await
        .spec
        .and_then(|s| s.priority);
    let high = reload_pod(&storage, "default", "pod-high")
        .await
        .spec
        .and_then(|s| s.priority);
    let system = reload_pod(&storage, "default", "pod-system")
        .await
        .spec
        .and_then(|s| s.priority);

    assert_eq!(low, Some(-10), "pod-low should resolve to p-low.value");
    assert_eq!(mid, Some(0), "pod-mid should resolve to p-mid.value");
    assert_eq!(high, Some(1_000), "pod-high should resolve to p-high.value");
    assert_eq!(
        system,
        Some(2_000_000_000),
        "pod-system should resolve to p-system.value (i32 near max)"
    );

    // Strict ordering must hold across the resolved priorities.
    assert!(
        low < mid && mid < high && high < system,
        "resolved priorities must strictly increase: {:?} < {:?} < {:?} < {:?}",
        low,
        mid,
        high,
        system,
    );
}

// ---------------------------------------------------------------------------
// 5. Preemption policy: `Never` must not evict victims
// ---------------------------------------------------------------------------

/// Mirrors upstream `preemptionPolicy: Never`: even when a higher-priority
/// pod is pending, a `Never`-policy PriorityClass must NOT cause eviction of
/// lower-priority pods.
#[tokio::test]
#[ignore = "RED-state: PriorityClassController is a stub"]
async fn priority_class_preemption_policy_never_does_not_evict() {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    let controller = PriorityClassController::new(storage.clone());

    create_priority_class(&storage, PriorityClass::new("low", 100)).await;
    create_priority_class(
        &storage,
        PriorityClass::new("high-but-polite", 1_000_000).with_preemption_policy("Never"),
    )
    .await;

    let victim = make_pod("victim", "default", Some("low"));
    storage
        .create(&build_key("pods", Some("default"), "victim"), &victim)
        .await
        .unwrap();
    let preemptor = make_pod("preemptor", "default", Some("high-but-polite"));
    storage
        .create(&build_key("pods", Some("default"), "preemptor"), &preemptor)
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let victim_after = reload_pod(&storage, "default", "victim").await;
    assert!(
        victim_after.metadata.deletion_timestamp.is_none(),
        "victim must NOT be evicted when the higher-priority pod's PriorityClass has \
         preemptionPolicy: Never; got deletion_timestamp = {:?}",
        victim_after.metadata.deletion_timestamp
    );
}

// ---------------------------------------------------------------------------
// 6. At most one globalDefault: controller must not silently apply two defaults
// ---------------------------------------------------------------------------

/// Upstream invariant: at most one PriorityClass may have
/// `globalDefault: true`. If two are persisted (e.g. due to a race or admin
/// mistake), the controller must surface this as an error rather than
/// silently picking one — we encode that as `reconcile_all` returning `Err`.
#[tokio::test]
async fn priority_class_rejects_multiple_global_defaults() {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    let controller = PriorityClassController::new(storage.clone());

    create_priority_class(
        &storage,
        PriorityClass::new("default-a", 100).with_global_default(true),
    )
    .await;
    create_priority_class(
        &storage,
        PriorityClass::new("default-b", 200).with_global_default(true),
    )
    .await;

    let result = controller.reconcile_all().await;
    assert!(
        result.is_err(),
        "PriorityClassController.reconcile_all should error when two PriorityClasses set \
         globalDefault: true; got Ok"
    );
}
