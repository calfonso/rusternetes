// Integration tests for admission controllers (LimitRange and ResourceQuota)
//
// These tests use in-memory storage and don't require a running etcd instance.
// The admission controllers are tested through the functions which:
// 1. apply_limit_range() - applies defaults and validates constraints
// 2. check_resource_quota() - ensures pod doesn't exceed quota
//
// Full E2E testing happens through the workflow tests that test the pod creation handler.

use rusternetes_api_server::admission::{apply_limit_range, check_resource_quota};
use rusternetes_common::resources::{
    Container, LimitRange, LimitRangeItem, LimitRangeSpec, Pod, PodSpec, ResourceQuota,
    ResourceQuotaSpec,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

fn create_minimal_pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "test-container".to_string(),
                image: "nginx:latest".to_string(),
                command: None,
                args: None,
                working_dir: None,
                ports: None,
                env: None,
                resources: None,
                volume_mounts: None,
                image_pull_policy: None,
                liveness_probe: None,
                readiness_probe: None,
                startup_probe: None,
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
            volumes: None,
            restart_policy: None,
            node_name: None,
            node_selector: None,
            service_account_name: None,
            service_account: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: None,
            priority_class_name: None,
            priority: None,
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
        status: None,
    }
}

#[tokio::test]
async fn test_limit_range_allows_when_no_limit_exists() {
    let storage = Arc::new(MemoryStorage::new());
    let mut pod = create_minimal_pod("test-pod", "test-namespace");

    let result = apply_limit_range(&storage, "test-namespace", &mut pod).await;
    assert!(result.is_ok());
    assert!(
        result.unwrap(),
        "Pod should be allowed when no LimitRange exists"
    );
}

#[tokio::test]
async fn test_quota_allows_when_no_quota_exists() {
    let storage = Arc::new(MemoryStorage::new());
    let pod = create_minimal_pod("test-pod", "test-namespace");

    let result = check_resource_quota(&storage, "test-namespace", &pod).await;
    assert!(result.is_ok());
    assert!(
        result.unwrap(),
        "Pod should be allowed when no quota exists"
    );
}

#[tokio::test]
async fn test_limit_range_applies_defaults() {
    let storage = Arc::new(MemoryStorage::new());

    // Create a LimitRange with defaults
    let mut default_limits = HashMap::new();
    default_limits.insert("cpu".to_string(), "500m".to_string());
    default_limits.insert("memory".to_string(), "512Mi".to_string());

    let limit_range = LimitRange {
        type_meta: TypeMeta {
            kind: "LimitRange".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("test-limit-range").with_namespace("test-namespace"),
        spec: LimitRangeSpec {
            limits: vec![LimitRangeItem {
                item_type: "Container".to_string(),
                max: None,
                min: None,
                default: Some(default_limits.clone()),
                default_request: None,
                max_limit_request_ratio: None,
            }],
        },
    };

    let key = build_key("limitranges", Some("test-namespace"), "test-limit-range");
    storage.create(&key, &limit_range).await.unwrap();

    // Create a pod without resource limits
    let mut pod = create_minimal_pod("test-pod", "test-namespace");

    // Apply limit range
    let result = apply_limit_range(&storage, "test-namespace", &mut pod).await;
    assert!(result.is_ok());
    assert!(result.unwrap(), "LimitRange admission should pass");

    // Verify defaults were applied
    let container = &pod.spec.unwrap().containers[0];
    assert!(container.resources.is_some());
    let resources = container.resources.as_ref().unwrap();
    assert!(resources.limits.is_some());
    let limits = resources.limits.as_ref().unwrap();
    assert_eq!(limits.get("cpu").unwrap(), "500m");
    assert_eq!(limits.get("memory").unwrap(), "512Mi");
}

#[tokio::test]
async fn test_quota_allows_when_under_limit() {
    let storage = Arc::new(MemoryStorage::new());

    // Create a quota
    let mut hard = HashMap::new();
    hard.insert("pods".to_string(), "10".to_string());

    let quota = ResourceQuota::new(
        "test-quota",
        "test-namespace",
        ResourceQuotaSpec {
            hard: Some(hard),
            scopes: None,
            scope_selector: None,
        },
    );

    let key = build_key("resourcequotas", Some("test-namespace"), "test-quota");
    storage.create(&key, &quota).await.unwrap();

    // Try to create a pod
    let pod = create_minimal_pod("test-pod", "test-namespace");

    let result = check_resource_quota(&storage, "test-namespace", &pod).await;
    assert!(result.is_ok());
    assert!(result.unwrap(), "Pod should be allowed under quota");
}

#[tokio::test]
async fn test_quota_rejects_when_exceeding_pod_count() {
    let storage = Arc::new(MemoryStorage::new());

    // Create a quota with low pod limit
    let mut hard = HashMap::new();
    hard.insert("pods".to_string(), "1".to_string());

    let quota = ResourceQuota::new(
        "test-quota",
        "test-namespace",
        ResourceQuotaSpec {
            hard: Some(hard),
            scopes: None,
            scope_selector: None,
        },
    );

    let quota_key = build_key("resourcequotas", Some("test-namespace"), "test-quota");
    storage.create(&quota_key, &quota).await.unwrap();

    // Create an existing pod
    let existing_pod = create_minimal_pod("existing-pod", "test-namespace");
    let pod_key = build_key("pods", Some("test-namespace"), "existing-pod");
    storage.create(&pod_key, &existing_pod).await.unwrap();

    // Try to create a second pod (should exceed quota)
    let new_pod = create_minimal_pod("new-pod", "test-namespace");

    let result = check_resource_quota(&storage, "test-namespace", &new_pod).await;
    assert!(result.is_ok());
    assert!(
        !result.unwrap(),
        "Pod should be rejected for exceeding pod count quota"
    );
}

// ===== DefaultStorageClass admission tests =====

use rusternetes_api_server::admission::set_default_storage_class;
use rusternetes_common::resources::volume::ResourceRequirements;
use rusternetes_common::resources::{
    PersistentVolumeAccessMode, PersistentVolumeClaim, PersistentVolumeClaimSpec, StorageClass,
};

fn create_test_pvc(name: &str, namespace: &str) -> PersistentVolumeClaim {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "1Gi".to_string());

    PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                requests: Some(requests),
                limits: None,
            },
            storage_class_name: None,
            volume_name: None,
            selector: None,
            volume_mode: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    }
}

#[tokio::test]
async fn test_default_storage_class_no_default() {
    let storage = Arc::new(MemoryStorage::new());
    let mut pvc = create_test_pvc("test-pvc", "test-namespace");

    // No storage classes exist
    let result = set_default_storage_class(&storage, &mut pvc).await;
    assert!(result.is_ok());
    // storageClassName should remain None
    assert!(pvc.spec.storage_class_name.is_none());
}

#[tokio::test]
async fn test_default_storage_class_sets_default() {
    let storage = Arc::new(MemoryStorage::new());
    let mut pvc = create_test_pvc("test-pvc", "test-namespace");

    // Create a default storage class
    let mut annotations = HashMap::new();
    annotations.insert(
        "storageclass.kubernetes.io/is-default-class".to_string(),
        "true".to_string(),
    );

    let storage_class = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("default-sc").with_annotations(annotations),
        provisioner: "kubernetes.io/no-provisioner".to_string(),
        parameters: None,
        reclaim_policy: None,
        volume_binding_mode: None,
        allow_volume_expansion: None,
        mount_options: None,
        allowed_topologies: None,
    };

    let sc_key = build_key("storageclasses", None, "default-sc");
    storage.create(&sc_key, &storage_class).await.unwrap();

    // Apply default storage class
    let result = set_default_storage_class(&storage, &mut pvc).await;
    assert!(result.is_ok());

    // storageClassName should be set to "default-sc"
    assert_eq!(pvc.spec.storage_class_name, Some("default-sc".to_string()));
}

#[tokio::test]
async fn test_default_storage_class_already_set() {
    let storage = Arc::new(MemoryStorage::new());
    let mut pvc = create_test_pvc("test-pvc", "test-namespace");
    pvc.spec.storage_class_name = Some("my-custom-sc".to_string());

    // Create a default storage class
    let mut annotations = HashMap::new();
    annotations.insert(
        "storageclass.kubernetes.io/is-default-class".to_string(),
        "true".to_string(),
    );

    let storage_class = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("default-sc").with_annotations(annotations),
        provisioner: "kubernetes.io/no-provisioner".to_string(),
        parameters: None,
        reclaim_policy: None,
        volume_binding_mode: None,
        allow_volume_expansion: None,
        mount_options: None,
        allowed_topologies: None,
    };

    let sc_key = build_key("storageclasses", None, "default-sc");
    storage.create(&sc_key, &storage_class).await.unwrap();

    // Apply default storage class
    let result = set_default_storage_class(&storage, &mut pvc).await;
    assert!(result.is_ok());

    // storageClassName should remain "my-custom-sc"
    assert_eq!(
        pvc.spec.storage_class_name,
        Some("my-custom-sc".to_string())
    );
}

#[tokio::test]
async fn test_default_storage_class_beta_annotation() {
    let storage = Arc::new(MemoryStorage::new());
    let mut pvc = create_test_pvc("test-pvc", "test-namespace");

    // Create a default storage class with beta annotation
    let mut annotations = HashMap::new();
    annotations.insert(
        "storageclass.beta.kubernetes.io/is-default-class".to_string(),
        "true".to_string(),
    );

    let storage_class = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("default-sc").with_annotations(annotations),
        provisioner: "kubernetes.io/no-provisioner".to_string(),
        parameters: None,
        reclaim_policy: None,
        volume_binding_mode: None,
        allow_volume_expansion: None,
        mount_options: None,
        allowed_topologies: None,
    };

    let sc_key = build_key("storageclasses", None, "default-sc");
    storage.create(&sc_key, &storage_class).await.unwrap();

    // Apply default storage class
    let result = set_default_storage_class(&storage, &mut pvc).await;
    assert!(result.is_ok());

    // storageClassName should be set even with beta annotation
    assert_eq!(pvc.spec.storage_class_name, Some("default-sc".to_string()));
}

// ===========================================================================
// LimitRange admission — e2e parity pins (relocated from the controller-manager
// stub suite; issue #1031). Upstream enforces LimitRange in the LimitRanger
// ADMISSION plugin (plugin/pkg/admission/limitranger), NOT a background
// controller. These mirror test/e2e/apimachinery/limit_range.go (release-1.35).
// ===========================================================================

use rusternetes_api_server::admission::{apply_limit_range_to_pvc, apply_limit_range_with};
use rusternetes_common::resources::volume::ResourceRequirements as PvcResourceRequirements;
use rusternetes_common::types::ResourceRequirements as PodResourceRequirements;

const LR_NS: &str = "limitrange-e2e";

fn str_map(pairs: &[(&str, &str)]) -> Option<HashMap<String, String>> {
    if pairs.is_empty() {
        None
    } else {
        Some(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        )
    }
}

fn container_item(
    default: &[(&str, &str)],
    default_request: &[(&str, &str)],
    min: &[(&str, &str)],
    max: &[(&str, &str)],
    ratio: &[(&str, &str)],
) -> LimitRangeItem {
    LimitRangeItem {
        item_type: "Container".to_string(),
        default: str_map(default),
        default_request: str_map(default_request),
        min: str_map(min),
        max: str_map(max),
        max_limit_request_ratio: str_map(ratio),
    }
}

fn limit_range(items: Vec<LimitRangeItem>) -> LimitRange {
    LimitRange::new("lr", LR_NS, LimitRangeSpec { limits: items })
}

fn pod_one_container(name: &str, requests: &[(&str, &str)], limits: &[(&str, &str)]) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(LR_NS),
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "pause:latest".to_string(),
                resources: Some(PodResourceRequirements {
                    requests: str_map(requests),
                    limits: str_map(limits),
                    claims: None,
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: None,
    }
}

async fn make_pvc(storage: &Arc<MemoryStorage>, name: &str, storage_req: &str) {
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), storage_req.to_string());
    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(LR_NS),
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![],
            resources: PvcResourceRequirements {
                requests: Some(requests),
                limits: None,
            },
            storage_class_name: None,
            volume_name: None,
            selector: None,
            volume_mode: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    };
    let key = build_key("persistentvolumeclaims", Some(LR_NS), name);
    storage.create(&key, &pvc).await.expect("create pvc");
}

/// 1. Container defaults: a pod declaring no resources inherits `default`
/// (limits) and `defaultRequest` from a `type: Container` item.
#[tokio::test]
async fn limitrange_container_defaults_apply_to_unspecified_pods() {
    let lr = limit_range(vec![container_item(
        &[("cpu", "200m"), ("memory", "256Mi")],
        &[("cpu", "100m"), ("memory", "128Mi")],
        &[],
        &[],
        &[],
    )]);
    let mut pod = pod_one_container("noresources", &[], &[]);

    let allowed = apply_limit_range_with(&mut pod, &vec![lr]).expect("apply");
    assert!(allowed, "defaulting must not reject");

    let c = &pod.spec.unwrap().containers[0];
    let rr = c.resources.as_ref().unwrap();
    let requests = rr.requests.as_ref().unwrap();
    let limits = rr.limits.as_ref().unwrap();
    assert_eq!(requests.get("cpu"), Some(&"100m".to_string()));
    assert_eq!(requests.get("memory"), Some(&"128Mi".to_string()));
    assert_eq!(limits.get("cpu"), Some(&"200m".to_string()));
    assert_eq!(limits.get("memory"), Some(&"256Mi".to_string()));
}

/// 2. Min/max: a container outside `[min, max]` is rejected; in-range admitted.
#[tokio::test]
async fn limitrange_min_max_rejects_out_of_range_pods() {
    let lr = limit_range(vec![container_item(
        &[],
        &[],
        &[("cpu", "200m")],
        &[("cpu", "2")],
        &[],
    )]);

    let mut in_range = pod_one_container("in-range", &[("cpu", "500m")], &[("cpu", "1")]);
    assert!(
        apply_limit_range_with(&mut in_range, &vec![lr.clone()]).expect("apply"),
        "in-range pod must be admitted",
    );

    let mut too_big = pod_one_container("too-big", &[("cpu", "1")], &[("cpu", "4")]);
    assert!(
        !apply_limit_range_with(&mut too_big, &vec![lr]).expect("apply"),
        "cpu limit 4 above max 2 must be rejected",
    );
}

/// 3. Ratio: `maxLimitRequestRatio` caps `limits/requests` per resource.
#[tokio::test]
async fn limitrange_ratio_rejects_high_ratio_pods() {
    let lr = limit_range(vec![container_item(&[], &[], &[], &[], &[("cpu", "3")])]);

    let mut low = pod_one_container("low-ratio", &[("cpu", "100m")], &[("cpu", "200m")]);
    assert!(
        apply_limit_range_with(&mut low, &vec![lr.clone()]).expect("apply"),
        "ratio 2 must survive cap 3",
    );

    let mut high = pod_one_container("high-ratio", &[("cpu", "100m")], &[("cpu", "500m")]);
    assert!(
        !apply_limit_range_with(&mut high, &vec![lr]).expect("apply"),
        "ratio 5 must be rejected by cap 3",
    );
}

/// 4. PVC storage min/max: a `type: PersistentVolumeClaim` item bounds the
/// PVC's `resources.requests.storage`.
#[tokio::test]
async fn limitrange_pvc_storage_min_max_enforced() {
    let storage = Arc::new(MemoryStorage::new());
    let lr = LimitRange::new(
        "lr",
        LR_NS,
        LimitRangeSpec {
            limits: vec![LimitRangeItem {
                item_type: "PersistentVolumeClaim".to_string(),
                default: None,
                default_request: None,
                min: str_map(&[("storage", "1Gi")]),
                max: str_map(&[("storage", "10Gi")]),
                max_limit_request_ratio: None,
            }],
        },
    );
    let lr_key = build_key("limitranges", Some(LR_NS), "lr");
    storage.create(&lr_key, &lr).await.unwrap();

    make_pvc(&storage, "too-small", "500Mi").await;
    make_pvc(&storage, "ok", "5Gi").await;
    make_pvc(&storage, "too-big", "100Gi").await;

    let small_key = build_key("persistentvolumeclaims", Some(LR_NS), "too-small");
    let mut too_small: PersistentVolumeClaim = storage.get(&small_key).await.unwrap();
    assert!(
        !apply_limit_range_to_pvc(&storage, LR_NS, &mut too_small)
            .await
            .expect("apply"),
        "500Mi below min 1Gi must be rejected",
    );

    let ok_key = build_key("persistentvolumeclaims", Some(LR_NS), "ok");
    let mut ok: PersistentVolumeClaim = storage.get(&ok_key).await.unwrap();
    assert!(
        apply_limit_range_to_pvc(&storage, LR_NS, &mut ok)
            .await
            .expect("apply"),
        "5Gi within [1Gi,10Gi] must be admitted",
    );

    let big_key = build_key("persistentvolumeclaims", Some(LR_NS), "too-big");
    let mut too_big: PersistentVolumeClaim = storage.get(&big_key).await.unwrap();
    assert!(
        !apply_limit_range_to_pvc(&storage, LR_NS, &mut too_big)
            .await
            .expect("apply"),
        "100Gi above max 10Gi must be rejected",
    );
}

/// 5. Pod-level aggregation: a `type: Pod` item's max applies to the SUM
/// across all containers, not each container individually.
#[tokio::test]
async fn limitrange_pod_level_aggregates_across_containers() {
    let lr = LimitRange::new(
        "lr",
        LR_NS,
        LimitRangeSpec {
            limits: vec![LimitRangeItem {
                item_type: "Pod".to_string(),
                default: None,
                default_request: None,
                min: None,
                max: str_map(&[("cpu", "2")]),
                max_limit_request_ratio: None,
            }],
        },
    );

    let container = |n: &str| Container {
        name: n.to_string(),
        image: "pause:latest".to_string(),
        resources: Some(PodResourceRequirements {
            requests: str_map(&[("cpu", "1500m")]),
            limits: None,
            claims: None,
        }),
        ..Default::default()
    };
    let mut pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("oversized-sum").with_namespace(LR_NS),
        spec: Some(PodSpec {
            containers: vec![container("a"), container("b")],
            ..Default::default()
        }),
        status: None,
    };

    assert!(
        !apply_limit_range_with(&mut pod, &vec![lr]).expect("apply"),
        "two containers at 1500m (sum 3) must exceed type:Pod max.cpu=2",
    );
}
