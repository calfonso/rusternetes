//! RED-state TDD pins mirroring upstream Kubernetes v1.35 e2e LimitRange
//! coverage in `test/e2e/apimachinery/limit_range.go`.
//!
//! Upstream permalink (release-1.35):
//!   <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/apimachinery/limit_range.go>
//!
//! In upstream Kubernetes, `LimitRange` is enforced by the `LimitRanger`
//! **admission plugin** (`plugin/pkg/admission/limitranger`), not by a
//! background controller. The controller-manager has no `limitrange`
//! reconcile loop — admission applies defaults and rejects pods/PVCs that
//! violate the configured min/max/ratio constraints at create time.
//!
//! Rusternetes currently has only an `AdmissionController` stub for
//! `LimitRanger` (see `crates/common/src/admission.rs`) that returns
//! `Allow` without consulting any `LimitRange` objects. The
//! `LimitRangeController` introduced alongside this test file is a no-op
//! stub: it watches the `LimitRange` prefix but does not enforce anything.
//!
//! These tests document the four behaviours upstream guarantees so that
//! once the admission path is wired up to fetch `LimitRange` from storage
//! and apply defaults / reject violations, each `#[ignore]` can be lifted
//! one by one and the suite will go green. Until then every test is
//! `#[ignore]`'d with a uniform RED-state reason.
//!
//! ## Pinned behaviours
//!
//! 1. **Container defaults** — Pods whose containers omit `resources.limits`
//!    inherit the `default` from a matching `LimitRangeItem` of `type:
//!    Container`. Same for `defaultRequest`. Upstream:
//!    `mergePodResourceRequirements` in
//!    `plugin/pkg/admission/limitranger/admission.go`.
//! 2. **Min/max enforcement** — A pod whose container resources fall
//!    outside `[min, max]` of a matching item is rejected with a
//!    `forbidden` error. Upstream: `PodValidateLimitFunc`.
//! 3. **Ratio constraints** — `maxLimitRequestRatio` is enforced: for any
//!    resource where the item sets a ratio, the container's
//!    `limits[r] / requests[r]` must be ≤ the configured ratio.
//!    Upstream: `maxRequestRatio` check in the same file.
//! 4. **PVC limits** — `type: PersistentVolumeClaim` items apply
//!    `min`/`max` on the PVC's `resources.requests.storage`. Upstream:
//!    `PersistentVolumeClaimValidateLimitFunc`.
//!
//! All four are RED today because:
//!   - The `LimitRangeController` stub does nothing.
//!   - The `LimitRangerController` admission stub does not load
//!     `LimitRange` objects from storage; it just returns `Allow`.
//!
//! Each test exercises the **controller-level** path
//! (`LimitRangeController::reconcile_all`) since this crate's tests cannot
//! reach into the api-server admission stack. The RED reason is identical
//! across the suite, which makes it obvious that the missing enforcement
//! is a single integration point rather than four independent gaps.

use rusternetes_common::resources::{
    Container, LimitRange, LimitRangeItem, LimitRangeSpec, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, Pod, PodSpec,
};
use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
use rusternetes_controller_manager::controllers::limitrange::LimitRangeController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

const NS: &str = "limitrange-e2e";

/// Build an empty MemoryStorage wrapped for shared use.
fn setup() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

/// Build a `LimitRangeItem` for `type: Container` with the supplied
/// constraints. Each input is a `(resource, value)` slice that gets
/// dropped into a `HashMap` if non-empty, otherwise the field stays
/// `None` (matches upstream YAML semantics — omitted maps are absent,
/// not empty).
fn container_item(
    default: &[(&str, &str)],
    default_request: &[(&str, &str)],
    min: &[(&str, &str)],
    max: &[(&str, &str)],
    ratio: &[(&str, &str)],
) -> LimitRangeItem {
    let to_map = |s: &[(&str, &str)]| -> Option<HashMap<String, String>> {
        if s.is_empty() {
            None
        } else {
            Some(
                s.iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            )
        }
    };
    LimitRangeItem {
        item_type: "Container".to_string(),
        default: to_map(default),
        default_request: to_map(default_request),
        min: to_map(min),
        max: to_map(max),
        max_limit_request_ratio: to_map(ratio),
    }
}

/// Build a `LimitRangeItem` for `type: PersistentVolumeClaim`.
fn pvc_item(min: &[(&str, &str)], max: &[(&str, &str)]) -> LimitRangeItem {
    let to_map = |s: &[(&str, &str)]| -> Option<HashMap<String, String>> {
        if s.is_empty() {
            None
        } else {
            Some(
                s.iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            )
        }
    };
    LimitRangeItem {
        item_type: "PersistentVolumeClaim".to_string(),
        default: None,
        default_request: None,
        min: to_map(min),
        max: to_map(max),
        max_limit_request_ratio: None,
    }
}

/// Persist a `LimitRange` named `lr` in `NS`.
async fn create_limit_range(storage: &Arc<MemoryStorage>, items: Vec<LimitRangeItem>) {
    let lr = LimitRange::new("lr", NS, LimitRangeSpec { limits: items });
    let key = build_key("limitranges", Some(NS), "lr");
    storage.create(&key, &lr).await.expect("create limitrange");
}

/// Build a pod with one container carrying the supplied
/// `requests`/`limits`. Either or both may be empty — empty maps become
/// `None` (so the container has no constraint declared, mirroring the
/// upstream YAML semantics that drive the defaulting path).
fn make_pod(name: &str, requests: &[(&str, &str)], limits: &[(&str, &str)]) -> Pod {
    let to_map = |s: &[(&str, &str)]| -> Option<HashMap<String, String>> {
        if s.is_empty() {
            None
        } else {
            Some(
                s.iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            )
        }
    };
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(NS),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause:latest".to_string(),
                resources: Some(ResourceRequirements {
                    requests: to_map(requests),
                    limits: to_map(limits),
                    claims: None,
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    }
}

/// Persist a pod.
async fn create_pod(storage: &Arc<MemoryStorage>, pod: &Pod) {
    let key = build_key("pods", Some(NS), &pod.metadata.name);
    storage.create(&key, pod).await.expect("create pod");
}

/// Persist a PVC requesting `storage` bytes.
async fn create_pvc(storage_arc: &Arc<MemoryStorage>, name: &str, storage_request: &str) {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), storage_request.to_string());
    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(NS),
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![],
            resources: rusternetes_common::resources::volume::ResourceRequirements {
                requests: Some(requests),
                limits: None,
            },
            volume_name: None,
            storage_class_name: None,
            volume_mode: None,
            selector: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    };
    let key = build_key("persistentvolumeclaims", Some(NS), name);
    storage_arc.create(&key, &pvc).await.expect("create pvc");
}

/// Pull the (possibly defaulted) container requests/limits for the first
/// container of a pod.
async fn pod_container_resources(
    storage: &Arc<MemoryStorage>,
    name: &str,
) -> (
    Option<HashMap<String, String>>,
    Option<HashMap<String, String>>,
) {
    let key = build_key("pods", Some(NS), name);
    let pod: Pod = storage.get(&key).await.expect("get pod");
    let spec = pod.spec.expect("pod spec");
    let c = spec.containers.into_iter().next().expect("container");
    match c.resources {
        Some(rr) => (rr.requests, rr.limits),
        None => (None, None),
    }
}

// ---------------------------------------------------------------------------
// 1. Container defaults
// ---------------------------------------------------------------------------

/// Upstream: a `LimitRange` with `type: Container, default: {cpu: 200m,
/// memory: 256Mi}` and `defaultRequest: {cpu: 100m, memory: 128Mi}` must
/// stamp those values onto a pod that declared no resources.
///
/// Rusternetes today: the `LimitRangeController` stub is a no-op, and the
/// admission plugin stub is `Allow` without mutation, so the pod stays
/// resource-less.
#[tokio::test]
#[ignore = "RED-state: LimitRangeController is a stub; enforcement happens at admission in real k8s — not yet wired"]
async fn limitrange_container_defaults_apply_to_unspecified_pods() {
    let storage = setup();
    create_limit_range(
        &storage,
        vec![container_item(
            &[("cpu", "200m"), ("memory", "256Mi")], // default (limits)
            &[("cpu", "100m"), ("memory", "128Mi")], // defaultRequest
            &[],
            &[],
            &[],
        )],
    )
    .await;

    let pod = make_pod("noresources", &[], &[]);
    create_pod(&storage, &pod).await;

    let controller = LimitRangeController::new(storage.clone());
    controller.reconcile_all().await.expect("reconcile");

    let (requests, limits) = pod_container_resources(&storage, "noresources").await;

    let requests = requests.expect("requests should be defaulted");
    let limits = limits.expect("limits should be defaulted");

    assert_eq!(requests.get("cpu"), Some(&"100m".to_string()));
    assert_eq!(requests.get("memory"), Some(&"128Mi".to_string()));
    assert_eq!(limits.get("cpu"), Some(&"200m".to_string()));
    assert_eq!(limits.get("memory"), Some(&"256Mi".to_string()));
}

// ---------------------------------------------------------------------------
// 2. Min/max enforcement
// ---------------------------------------------------------------------------

/// Upstream: a pod whose container declares `cpu: 4` (above the `max: 2`)
/// must be rejected with a forbidden error referencing the LimitRange.
/// Conversely, a pod declaring `cpu: 1` (within `[min=200m, max=2]`)
/// must be admitted unchanged.
///
/// We drive the controller-level path: after reconcile, the in-range pod
/// must still exist and the over-range pod must have been rejected (or
/// at minimum, an enforcement status surfaced in storage). Today the
/// stub does neither — both pods sit untouched, so the assertion that the
/// over-range pod is gone goes RED.
#[tokio::test]
#[ignore = "RED-state: LimitRangeController is a stub; enforcement happens at admission in real k8s — not yet wired"]
async fn limitrange_min_max_rejects_out_of_range_pods() {
    let storage = setup();
    create_limit_range(
        &storage,
        vec![container_item(
            &[],
            &[],
            &[("cpu", "200m")], // min
            &[("cpu", "2")],    // max
            &[],
        )],
    )
    .await;

    create_pod(
        &storage,
        &make_pod("in-range", &[("cpu", "500m")], &[("cpu", "1")]),
    )
    .await;
    create_pod(
        &storage,
        &make_pod("too-big", &[("cpu", "1")], &[("cpu", "4")]),
    )
    .await;

    let controller = LimitRangeController::new(storage.clone());
    controller.reconcile_all().await.expect("reconcile");

    // In-range pod survives.
    let in_range_key = build_key("pods", Some(NS), "in-range");
    storage
        .get::<Pod>(&in_range_key)
        .await
        .expect("in-range pod must still exist");

    // Out-of-range pod is rejected (deleted) by enforcement.
    let too_big_key = build_key("pods", Some(NS), "too-big");
    assert!(
        storage.get::<Pod>(&too_big_key).await.is_err(),
        "out-of-range pod should be rejected by LimitRange enforcement",
    );
}

// ---------------------------------------------------------------------------
// 3. Ratio constraints
// ---------------------------------------------------------------------------

/// Upstream: with `maxLimitRequestRatio: {cpu: 3}`, a container with
/// `requests.cpu = 100m` and `limits.cpu = 500m` (ratio = 5) must be
/// rejected, while one with `requests.cpu = 100m` and `limits.cpu = 200m`
/// (ratio = 2) must be admitted.
#[tokio::test]
#[ignore = "RED-state: LimitRangeController is a stub; enforcement happens at admission in real k8s — not yet wired"]
async fn limitrange_ratio_rejects_high_ratio_pods() {
    let storage = setup();
    create_limit_range(
        &storage,
        vec![container_item(
            &[],
            &[],
            &[],
            &[],
            &[("cpu", "3")], // maxLimitRequestRatio
        )],
    )
    .await;

    create_pod(
        &storage,
        &make_pod("low-ratio", &[("cpu", "100m")], &[("cpu", "200m")]),
    )
    .await;
    create_pod(
        &storage,
        &make_pod("high-ratio", &[("cpu", "100m")], &[("cpu", "500m")]),
    )
    .await;

    let controller = LimitRangeController::new(storage.clone());
    controller.reconcile_all().await.expect("reconcile");

    let low_key = build_key("pods", Some(NS), "low-ratio");
    storage
        .get::<Pod>(&low_key)
        .await
        .expect("ratio=2 pod must survive ratio=3 cap");

    let high_key = build_key("pods", Some(NS), "high-ratio");
    assert!(
        storage.get::<Pod>(&high_key).await.is_err(),
        "ratio=5 pod must be rejected when maxLimitRequestRatio=3",
    );
}

// ---------------------------------------------------------------------------
// 4. PVC storage min/max
// ---------------------------------------------------------------------------

/// Upstream: with `type: PersistentVolumeClaim, min.storage: 1Gi,
/// max.storage: 10Gi`, a PVC requesting `500Mi` (below min) must be
/// rejected, a PVC requesting `5Gi` (in range) must be admitted, and a
/// PVC requesting `100Gi` (above max) must be rejected.
#[tokio::test]
#[ignore = "RED-state: LimitRangeController is a stub; enforcement happens at admission in real k8s — not yet wired"]
async fn limitrange_pvc_storage_min_max_enforced() {
    let storage = setup();
    create_limit_range(
        &storage,
        vec![pvc_item(&[("storage", "1Gi")], &[("storage", "10Gi")])],
    )
    .await;

    create_pvc(&storage, "too-small", "500Mi").await;
    create_pvc(&storage, "ok", "5Gi").await;
    create_pvc(&storage, "too-big", "100Gi").await;

    let controller = LimitRangeController::new(storage.clone());
    controller.reconcile_all().await.expect("reconcile");

    let ok_key = build_key("persistentvolumeclaims", Some(NS), "ok");
    storage
        .get::<PersistentVolumeClaim>(&ok_key)
        .await
        .expect("in-range PVC must survive");

    let small_key = build_key("persistentvolumeclaims", Some(NS), "too-small");
    assert!(
        storage
            .get::<PersistentVolumeClaim>(&small_key)
            .await
            .is_err(),
        "PVC below LimitRange min.storage must be rejected",
    );

    let big_key = build_key("persistentvolumeclaims", Some(NS), "too-big");
    assert!(
        storage
            .get::<PersistentVolumeClaim>(&big_key)
            .await
            .is_err(),
        "PVC above LimitRange max.storage must be rejected",
    );
}

// ---------------------------------------------------------------------------
// 5. Pod-level limits (per-pod sum across containers)
// ---------------------------------------------------------------------------

/// Upstream: a `LimitRange` with `type: Pod, max.cpu: 2` aggregates
/// across all containers in the pod. A two-container pod whose containers
/// each request `1.5` exceeds the pod-level cap and must be rejected.
///
/// This pin captures the multi-container summing path, which is distinct
/// from the per-container `type: Container` check exercised above.
#[tokio::test]
#[ignore = "RED-state: LimitRangeController is a stub; enforcement happens at admission in real k8s — not yet wired"]
async fn limitrange_pod_level_aggregates_across_containers() {
    let storage = setup();
    // type: Pod item (not Container) — applies to the per-pod sum.
    let item = LimitRangeItem {
        item_type: "Pod".to_string(),
        default: None,
        default_request: None,
        min: None,
        max: Some({
            let mut m = HashMap::new();
            m.insert("cpu".to_string(), "2".to_string());
            m
        }),
        max_limit_request_ratio: None,
    };
    create_limit_range(&storage, vec![item]).await;

    // Two containers, each at 1.5 cpu — sum = 3, exceeds pod max=2.
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), "1500m".to_string());
    let container = |n: &str| Container {
        name: n.to_string(),
        image: "pause:latest".to_string(),
        resources: Some(ResourceRequirements {
            requests: Some(requests.clone()),
            limits: None,
            claims: None,
        }),
        ..Default::default()
    };
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("oversized-sum").with_namespace(NS),
        spec: Some(PodSpec {
            containers: vec![container("a"), container("b")],
            ..Default::default()
        }),
        status: None,
    };
    let key = build_key("pods", Some(NS), &pod.metadata.name);
    storage.create(&key, &pod).await.expect("create pod");

    let controller = LimitRangeController::new(storage.clone());
    controller.reconcile_all().await.expect("reconcile");

    assert!(
        storage.get::<Pod>(&key).await.is_err(),
        "two containers at 1500m cpu each (sum=3) must be rejected by type:Pod max.cpu=2",
    );
}
