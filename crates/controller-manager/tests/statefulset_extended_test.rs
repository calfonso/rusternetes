//! Extended StatefulSet controller tests borrowed from upstream Kubernetes Go implementation.
//!
//! These tests cover advanced StatefulSet features not covered in basic conformance:
//! - PVC binding and retention policies
//! - Network identity and headless service integration  
//! - Update strategies (RollingUpdate, OnDelete) with partitions
//! - Pod management policies (OrderedReady vs Parallel)
//! - Status correctness across scaling operations
//! - Revision history and rollback scenarios
//!
//! Source: kubernetes/test/e2e/apps/statefulset.go
//!         kubernetes/pkg/controller/statefulset/stateful_set_controller_test.go

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::volume::{
    PersistentVolumeAccessMode, PersistentVolumeClaim, PersistentVolumeClaimSpec,
    ResourceRequirements,
};
use rusternetes_common::resources::workloads::{
    RollingUpdateStatefulSetStrategy, StatefulSetPersistentVolumeClaimRetentionPolicy,
    StatefulSetUpdateStrategy,
};
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::statefulset::StatefulSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn empty_pod_spec(image: &str, container_name: &str) -> PodSpec {
    PodSpec {
        containers: vec![Container {
            name: container_name.to_string(),
            image: image.to_string(),
            image_pull_policy: Some("IfNotPresent".to_string()),
            ports: None,
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
    }
}

fn make_statefulset(name: &str, namespace: &str, replicas: i32) -> StatefulSet {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    StatefulSet {
        type_meta: TypeMeta {
            kind: "StatefulSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name).with_namespace(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: StatefulSetSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new(format!("{}-pod", name));
                    meta.labels = Some(labels);
                    meta
                }),
                spec: empty_pod_spec("registry.k8s.io/e2e-test-images/agnhost:2.55", "webserver"),
            },
            service_name: format!("{}-headless", name),
            pod_management_policy: Some("Parallel".to_string()),
            update_strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
            volume_claim_templates: None,
            persistent_volume_claim_retention_policy: None,
            ordinals: None,
        },
        status: Some(StatefulSetStatus {
            replicas: 0,
            ready_replicas: Some(0),
            current_replicas: Some(0),
            updated_replicas: Some(0),
            available_replicas: None,
            collision_count: None,
            observed_generation: None,
            current_revision: None,
            update_revision: None,
            conditions: None,
        }),
    }
}

/// Replay kubelet: physically delete pods whose `deletionTimestamp` was
/// stamped by the controller.
async fn simulate_kubelet_cleanup(storage: &Arc<MemoryStorage>, namespace: &str) {
    let prefix = format!("/registry/pods/{}/", namespace);
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
    for pod in pods {
        if pod.metadata.deletion_timestamp.is_some() {
            let key = format!("/registry/pods/{}/{}", namespace, pod.metadata.name);
            let _ = storage.delete(&key).await;
        }
    }
}

async fn mark_pod_ready(storage: &Arc<MemoryStorage>, namespace: &str, pod_name: &str) {
    let pod_key = build_key("pods", Some(namespace), pod_name);
    let mut pod: Pod = storage.get(&pod_key).await.unwrap();
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        conditions: Some(vec![PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(chrono::Utc::now()),
            observed_generation: None,
        }]),
        ..pod.status.unwrap_or_default()
    });
    storage.update(&pod_key, &pod).await.unwrap();
}

async fn mark_all_pods_ready(storage: &Arc<MemoryStorage>, namespace: &str) {
    let prefix = format!("/registry/pods/{}/", namespace);
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
    for pod in pods {
        mark_pod_ready(storage, namespace, &pod.metadata.name).await;
    }
}

// ===========================================================================
// Extended StatefulSet Tests
// ===========================================================================

/// StatefulSet should create PVCs in order before pods during OrderedReady pod management
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go
/// Tests that PVCs are created sequentially (data-pvc-0, data-pvc-1, etc.) before
/// corresponding pods are scheduled, ensuring stable storage identity.
#[tokio::test]
async fn statefulset_should_create_pvcs_in_order_before_pods() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("ordered-pvc", ns, 3);
    ss.spec.pod_management_policy = Some("OrderedReady".to_string());

    // Add volume claim template
    let pvc_template = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("data");
            meta.ensure_uid();
            meta
        },
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            selector: None,
            resources: ResourceRequirements {
                limits: None,
                requests: Some(HashMap::from([("storage".to_string(), "1Gi".to_string())])),
            },
            volume_name: None,
            storage_class_name: Some("standard".to_string()),
            volume_mode: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    };

    ss.spec.volume_claim_templates = Some(vec![pvc_template]);

    let key = build_key("statefulsets", Some(ns), "ordered-pvc");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());

    // First reconcile should create PVC for ordinal 0 only (OrderedReady)
    controller.reconcile_all().await.unwrap();

    let pvcs: Vec<PersistentVolumeClaim> = storage
        .list("/registry/persistentvolumeclaims/default/")
        .await
        .unwrap_or_default();

    assert_eq!(pvcs.len(), 1, "OrderedReady creates one PVC at a time");
    assert_eq!(pvcs[0].metadata.name, "data-ordered-pvc-0");

    // Mark PVC as bound (simulate PV provisioning)
    let pvc_key = build_key("persistentvolumeclaims", Some(ns), "data-ordered-pvc-0");
    let mut pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    pvc.status = Some(
        rusternetes_common::resources::volume::PersistentVolumeClaimStatus {
            phase: rusternetes_common::resources::volume::PersistentVolumeClaimPhase::Bound,
            access_modes: None,
            capacity: None,
            conditions: None,
            allocated_resources: None,
            allocated_resource_statuses: None,
            resize_status: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        },
    );
    storage.update(&pvc_key, &pvc).await.unwrap();

    // Second reconcile should create pod-0 after PVC is bound
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 1);
    assert_eq!(pods[0].metadata.name, "ordered-pvc-0");
}

/// StatefulSet with PVC retention policy Retain should keep PVCs on scale down
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go
/// Tests WhenPVCRetentionPolicyOnScaleDown is set to Retain, PVCs persist
/// after reducing replica count.
#[tokio::test]
async fn statefulset_pvc_retention_retain_on_scale_down() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("retain-pvc", ns, 3);

    // Set retention policy to Retain
    ss.spec.persistent_volume_claim_retention_policy =
        Some(StatefulSetPersistentVolumeClaimRetentionPolicy {
            when_deleted: Some("Retain".to_string()),
            when_scaled: Some("Retain".to_string()),
        });

    // Add volume claim template
    let pvc_template = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("data");
            meta.ensure_uid();
            meta
        },
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            selector: None,
            resources: ResourceRequirements {
                limits: None,
                requests: Some(HashMap::from([("storage".to_string(), "1Gi".to_string())])),
            },
            volume_name: None,
            storage_class_name: Some("standard".to_string()),
            volume_mode: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    };

    ss.spec.volume_claim_templates = Some(vec![pvc_template]);

    let key = build_key("statefulsets", Some(ns), "retain-pvc");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());

    // Create all 3 replicas with PVCs
    for _ in 0..6 {
        controller.reconcile_all().await.unwrap();
        mark_all_pods_ready(&storage, ns).await;
    }

    let initial_pvcs: Vec<PersistentVolumeClaim> = storage
        .list("/registry/persistentvolumeclaims/default/")
        .await
        .unwrap();
    assert_eq!(initial_pvcs.len(), 3);

    // Scale down to 1 replica
    let mut scaled: StatefulSet = storage.get(&key).await.unwrap();
    scaled.spec.replicas = Some(1);
    storage.update(&key, &scaled).await.unwrap();

    // Drive scale-down to completion
    for _ in 0..8 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
    }

    // With Retain policy, all 3 PVCs should still exist
    let final_pvcs: Vec<PersistentVolumeClaim> = storage
        .list("/registry/persistentvolumeclaims/default/")
        .await
        .unwrap();

    assert_eq!(
        final_pvcs.len(),
        3,
        "Retain policy keeps all PVCs on scale down"
    );

    let final_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(final_pods.len(), 1, "Only 1 pod remains after scale down");
}

/// StatefulSet with PVC retention policy Delete should remove PVCs on scale down
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go
/// Tests WhenPVCRetentionPolicyOnScaleDown is set to Delete, PVCs are removed
/// when reducing replica count.
#[tokio::test]
async fn statefulset_pvc_retention_delete_on_scale_down() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("delete-pvc", ns, 3);

    // Set retention policy to Delete
    ss.spec.persistent_volume_claim_retention_policy =
        Some(StatefulSetPersistentVolumeClaimRetentionPolicy {
            when_deleted: Some("Delete".to_string()),
            when_scaled: Some("Delete".to_string()),
        });

    // Add volume claim template
    let pvc_template = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("data");
            meta.ensure_uid();
            meta
        },
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            selector: None,
            resources: ResourceRequirements {
                limits: None,
                requests: Some(HashMap::from([("storage".to_string(), "1Gi".to_string())])),
            },
            volume_name: None,
            storage_class_name: Some("standard".to_string()),
            volume_mode: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    };

    ss.spec.volume_claim_templates = Some(vec![pvc_template]);

    let key = build_key("statefulsets", Some(ns), "delete-pvc");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());

    // Create all 3 replicas with PVCs
    for _ in 0..6 {
        controller.reconcile_all().await.unwrap();
        mark_all_pods_ready(&storage, ns).await;
    }

    let initial_pvcs: Vec<PersistentVolumeClaim> = storage
        .list("/registry/persistentvolumeclaims/default/")
        .await
        .unwrap();
    assert_eq!(initial_pvcs.len(), 3);

    // Scale down to 1 replica
    let mut scaled: StatefulSet = storage.get(&key).await.unwrap();
    scaled.spec.replicas = Some(1);
    storage.update(&key, &scaled).await.unwrap();

    // Drive scale-down to completion
    for _ in 0..8 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
    }

    // With Delete policy, only 1 PVC should remain
    let final_pvcs: Vec<PersistentVolumeClaim> = storage
        .list("/registry/persistentvolumeclaims/default/")
        .await
        .unwrap();

    assert_eq!(
        final_pvcs.len(),
        1,
        "Delete policy removes PVCs on scale down"
    );
    assert_eq!(final_pvcs[0].metadata.name, "data-delete-pvc-0");

    let final_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(final_pods.len(), 1);
}

/// StatefulSet pods should have stable network identities via headless service
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go
/// Tests that each pod gets a stable DNS name: <pod-name>.<service-name>.<namespace>.svc.cluster.local
#[tokio::test]
async fn statefulset_pods_should_have_stable_network_identity() {
    let storage = setup_test().await;
    let ns = "default";

    let ss = make_statefulset("network-id", ns, 2);
    let key = build_key("statefulsets", Some(ns), "network-id");
    storage.create(&key, &ss).await.unwrap();

    // Create headless service
    let service = Service {
        type_meta: TypeMeta {
            kind: "Service".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("network-id-headless").with_namespace(ns.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: ServiceSpec {
            selector: Some(HashMap::from([(
                "app".to_string(),
                "network-id".to_string(),
            )])),
            cluster_ip: Some("None".to_string()), // Headless service
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: Some(IntOrString::Int(80)),
                protocol: "TCP".to_string(),
                app_protocol: None,
                node_port: None,
            }],
            service_type: Some(ServiceType::ClusterIP),
            external_ips: None,
            session_affinity: None,
            load_balancer_ip: None,
            load_balancer_source_ranges: None,
            external_traffic_policy: None,
            health_check_node_port: None,
            publish_not_ready_addresses: None,
            ip_families: None,
            ip_family_policy: None,
            allocate_load_balancer_node_ports: None,
            external_name: None,
            cluster_ips: None,
            internal_traffic_policy: None,
            load_balancer_class: None,
            session_affinity_config: None,
            traffic_distribution: None,
        },
        status: None,
    };

    let svc_key = build_key("services", Some(ns), "network-id-headless");
    storage.create(&svc_key, &service).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    let mut pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    pods.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
    assert_eq!(pods.len(), 2);

    // Verify pod hostnames and subdomains are set correctly
    for (i, pod) in pods.iter().enumerate() {
        let expected_name = format!("network-id-{}", i);
        assert_eq!(pod.metadata.name, expected_name);

        // Pod should have hostname set to its name
        assert_eq!(
            pod.spec.as_ref().unwrap().hostname,
            Some(expected_name.clone()),
            "Pod {} should have hostname set",
            i
        );

        // Pod should have subdomain set to service name
        assert_eq!(
            pod.spec.as_ref().unwrap().subdomain,
            Some("network-id-headless".to_string()),
            "Pod {} should have subdomain set to headless service",
            i
        );
    }
}

/// StatefulSet RollingUpdate with partition should update only pods >= partition ordinal
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go
/// Tests canary deployments using partition field in rolling update strategy.
#[tokio::test]
async fn statefulset_rolling_update_with_partition_should_update_subset() {
    let storage = setup_test().await;
    let ns = "default";

    // Create 5 replicas with partition=3 (only ordinals 3,4 should update)
    let mut ss = make_statefulset("partition-roll", ns, 5);
    ss.spec.update_strategy = Some(StatefulSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateStatefulSetStrategy {
            partition: Some(3),
            max_unavailable: None,
        }),
    });

    let key = build_key("statefulsets", Some(ns), "partition-roll");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    let initial: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(initial.len(), 5);

    let original_image = initial[0].spec.as_ref().unwrap().containers[0]
        .image
        .clone();

    // Change template image
    let mut updated: StatefulSet = storage.get(&key).await.unwrap();
    updated.spec.template.spec.containers[0].image = "nginx:1.26-alpine".to_string();
    storage.update(&key, &updated).await.unwrap();

    // Drive rolling update
    for _ in 0..10 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        mark_all_pods_ready(&storage, ns).await;
    }

    let final_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(final_pods.len(), 5);

    // Check which pods were updated based on ordinal
    for pod in &final_pods {
        let ordinal: i32 = pod
            .metadata
            .name
            .rsplit('-')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap();

        let pod_image = &pod.spec.as_ref().unwrap().containers[0].image;

        if ordinal < 3 {
            assert_eq!(
                pod_image, &original_image,
                "Pod {} (ordinal {}) should keep original image",
                pod.metadata.name, ordinal
            );
        } else {
            assert_eq!(
                pod_image, "nginx:1.26-alpine",
                "Pod {} (ordinal {}) should have new image",
                pod.metadata.name, ordinal
            );
        }
    }
}

/// StatefulSet OnDelete update strategy should not automatically update pods
///
/// Upstream: k8s.io/kubernetes/pkg/controller/statefulset/stateful_set_controller_test.go
/// Tests that OnDelete strategy requires manual pod deletion for updates.
#[tokio::test]
async fn statefulset_ondelete_strategy_requires_manual_pod_deletion() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("ondelete", ns, 3);
    ss.spec.update_strategy = Some(StatefulSetUpdateStrategy {
        strategy_type: Some("OnDelete".to_string()),
        rolling_update: None,
    });

    let key = build_key("statefulsets", Some(ns), "ondelete");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    let initial: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(initial.len(), 3);

    let original_image = initial[0].spec.as_ref().unwrap().containers[0]
        .image
        .clone();

    // Change template image
    let mut updated: StatefulSet = storage.get(&key).await.unwrap();
    updated.spec.template.spec.containers[0].image = "nginx:1.27-alpine".to_string();
    storage.update(&key, &updated).await.unwrap();

    // Reconcile - should NOT update any pods with OnDelete
    controller.reconcile_all().await.unwrap();

    let after_reconcile: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    for pod in &after_reconcile {
        assert_eq!(
            pod.spec.as_ref().unwrap().containers[0].image,
            original_image,
            "OnDelete strategy should not auto-update pods"
        );
    }

    // Manually delete one pod
    let pod_key = build_key("pods", Some(ns), "ondelete-0");
    storage.delete(&pod_key).await.unwrap();

    // Now reconcile should recreate with new image
    controller.reconcile_all().await.unwrap();

    let final_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(final_pods.len(), 3);

    let recreated_pod: Pod = storage.get(&pod_key).await.unwrap();
    assert_eq!(
        recreated_pod.spec.as_ref().unwrap().containers[0].image,
        "nginx:1.27-alpine",
        "Manually deleted pod should be recreated with new image"
    );
}

/// StatefulSet should maintain correct status during scaling operations
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go
/// Tests status.replicas, status.readyReplicas, status.currentReplicas accuracy.
#[tokio::test]
async fn statefulset_status_should_be_accurate_during_scaling() {
    let storage = setup_test().await;
    let ns = "default";

    let ss = make_statefulset("status-test", ns, 3);
    let key = build_key("statefulsets", Some(ns), "status-test");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());

    // Initial reconcile
    controller.reconcile_all().await.unwrap();

    let ss_after: StatefulSet = storage.get(&key).await.unwrap();
    assert_eq!(ss_after.status.as_ref().unwrap().replicas, 3);

    // Mark all pods ready
    mark_all_pods_ready(&storage, ns).await;

    // Reconcile to update status
    controller.reconcile_all().await.unwrap();

    let ss_ready: StatefulSet = storage.get(&key).await.unwrap();
    let status = ss_ready.status.as_ref().unwrap();
    assert_eq!(status.replicas, 3);
    assert_eq!(status.ready_replicas, Some(3));
    assert_eq!(status.current_replicas, Some(3));

    // Scale up to 5
    let mut scaled: StatefulSet = storage.get(&key).await.unwrap();
    scaled.spec.replicas = Some(5);
    storage.update(&key, &scaled).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let ss_scaling: StatefulSet = storage.get(&key).await.unwrap();
    assert_eq!(ss_scaling.status.as_ref().unwrap().replicas, 5);
    // Ready replicas should still be 3 until new pods are ready

    // Mark all 5 pods ready
    mark_all_pods_ready(&storage, ns).await;
    controller.reconcile_all().await.unwrap();

    let ss_final: StatefulSet = storage.get(&key).await.unwrap();
    let final_status = ss_final.status.as_ref().unwrap();
    assert_eq!(final_status.replicas, 5);
    assert_eq!(final_status.ready_replicas, Some(5));
    assert_eq!(final_status.current_replicas, Some(5));
}

/// StatefulSet should respect revision history limit
///
/// Upstream: k8s.io/kubernetes/pkg/controller/statefulset/stateful_set_controller_test.go
/// Tests that old ControllerRevisions are garbage collected when limit is exceeded.
#[tokio::test]
async fn statefulset_should_respect_revision_history_limit() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("revision-limit", ns, 2);
    ss.spec.revision_history_limit = Some(2); // Keep only 2 revisions
    ss.spec.update_strategy = Some(StatefulSetUpdateStrategy {
        strategy_type: Some("RollingUpdate".to_string()),
        rolling_update: None,
    });

    let key = build_key("statefulsets", Some(ns), "revision-limit");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    // Perform multiple updates
    for i in 0..5 {
        let mut updated: StatefulSet = storage.get(&key).await.unwrap();
        updated.spec.template.spec.containers[0].image = format!("nginx:1.{}-alpine", 25 + i);
        storage.update(&key, &updated).await.unwrap();

        for _ in 0..4 {
            controller.reconcile_all().await.unwrap();
            simulate_kubelet_cleanup(&storage, ns).await;
            mark_all_pods_ready(&storage, ns).await;
        }
    }

    // Count ControllerRevisions
    let revisions: Vec<rusternetes_common::resources::ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap_or_default();

    // revisionHistoryLimit=2 caps the NON-current history (upstream
    // truncateHistory), so at most 2 old revisions plus the current one survive.
    assert!(
        revisions.len() <= 3,
        "Should have at most 3 revisions (2 non-current + current), found {}",
        revisions.len()
    );
}

/// StatefulSet with parallel pod management should create all pods simultaneously
///
/// Upstream: k8s.io/kubernetes/test/e2e/apps/statefulset.go
/// Tests Parallel pod management policy creates pods without ordering.
#[tokio::test]
async fn statefulset_parallel_policy_creates_all_pods_at_once() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("parallel", ns, 5);
    ss.spec.pod_management_policy = Some("Parallel".to_string());

    let key = build_key("statefulsets", Some(ns), "parallel");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());

    // Single reconcile should create all 5 pods
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods.len(),
        5,
        "Parallel policy creates all pods in one reconcile"
    );

    // Verify all ordinals exist
    let mut ordinals: Vec<i32> = pods
        .iter()
        .filter_map(|p| {
            p.metadata
                .name
                .rsplit('-')
                .next()
                .and_then(|s| s.parse().ok())
        })
        .collect();
    ordinals.sort();

    assert_eq!(ordinals, vec![0, 1, 2, 3, 4]);
}

// ===========================================================================
// Phase 1.2 batch — additional StatefulSet coverage
// (PVC binding/mounting, headless DNS, forced rollback, init containers,
//  VCT updates, status conditions, topology spread, deletion propagation)
// Upstream reference: kubernetes/test/e2e/apps/statefulset.go
// ===========================================================================

/// Helper: build a `volumeClaimTemplate` named `data` with a 1Gi RWO claim.
fn data_pvc_template() -> PersistentVolumeClaim {
    PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("data");
            meta.ensure_uid();
            meta
        },
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            selector: None,
            resources: ResourceRequirements {
                limits: None,
                requests: Some(HashMap::from([("storage".to_string(), "1Gi".to_string())])),
            },
            volume_name: None,
            storage_class_name: Some("standard".to_string()),
            volume_mode: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: None,
    }
}

/// StatefulSet with a `volumeClaimTemplate` should create a PVC per ordinal
/// with the deterministic `<template>-<statefulset>-<ordinal>` naming scheme
/// and stamp an `ownerReference` back to the StatefulSet so the GC can later
/// reclaim it (whenScaled=Delete). The pod template's `volumeMounts` are
/// preserved verbatim on the rendered pod so the kubelet knows which PVC to
/// mount where.
///
/// Upstream: kubernetes/test/e2e/apps/statefulset.go — "should provide basic
/// identity" / "PVCs are created with the expected names".
#[tokio::test]
async fn statefulset_with_pvc_creates_per_ordinal_claims_and_mounts() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("pvc-mount", ns, 2);
    ss.spec.volume_claim_templates = Some(vec![data_pvc_template()]);

    // Template references the PVC by `volumeMounts` — the controller should
    // preserve this on the rendered pod so the kubelet can mount the claim.
    ss.spec.template.spec.containers[0].volume_mounts = Some(vec![VolumeMount {
        name: "data".to_string(),
        mount_path: "/var/lib/data".to_string(),
        read_only: None,
        sub_path: None,
        sub_path_expr: None,
        mount_propagation: None,
        recursive_read_only: None,
    }]);

    let key = build_key("statefulsets", Some(ns), "pvc-mount");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    // Reconcile twice for Parallel policy so both ordinals' PVCs+pods are created.
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;
    controller.reconcile_all().await.unwrap();

    // PVCs must be named "data-pvc-mount-0" and "data-pvc-mount-1".
    let mut pvcs: Vec<PersistentVolumeClaim> = storage
        .list("/registry/persistentvolumeclaims/default/")
        .await
        .unwrap();
    pvcs.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));

    assert_eq!(pvcs.len(), 2, "one PVC per ordinal");
    assert_eq!(pvcs[0].metadata.name, "data-pvc-mount-0");
    assert_eq!(pvcs[1].metadata.name, "data-pvc-mount-1");

    // Each PVC must carry an ownerReference back to the StatefulSet so the
    // garbage collector cascades on delete and the retention policy applies.
    let ss_uid = ss.metadata.uid.clone();
    for pvc in &pvcs {
        let refs = pvc
            .metadata
            .owner_references
            .as_ref()
            .expect("PVC must have ownerReferences");
        let owned = refs
            .iter()
            .any(|r| r.uid == ss_uid && r.kind == "StatefulSet");
        assert!(
            owned,
            "PVC {} must be owned by StatefulSet",
            pvc.metadata.name
        );
    }

    // Pods must carry the template's volumeMounts verbatim.
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);
    for pod in &pods {
        let mounts = pod
            .spec
            .as_ref()
            .unwrap()
            .containers
            .first()
            .and_then(|c| c.volume_mounts.as_ref())
            .expect("pod container must inherit volumeMounts from template");
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].name, "data");
        assert_eq!(mounts[0].mount_path, "/var/lib/data");
    }
}

/// StatefulSet pods must have `pod.spec.subdomain` stamped from the
/// `serviceName` field so the headless governing Service synthesizes per-pod
/// DNS A records of the form `<pod>.<service>.<ns>.svc.cluster.local`.
///
/// Note: this complements the (currently RED) `pods_should_have_stable_network_identity`
/// test above, which also expects `pod.spec.hostname` to be set. The subdomain
/// half is already implemented in the controller (`create_pod`), so it has its
/// own GREEN test here.
///
/// Upstream: kubernetes/test/e2e/apps/statefulset.go — "Service: should be
/// able to resolve DNS of pods".
#[tokio::test]
async fn statefulset_pods_have_subdomain_for_headless_dns() {
    let storage = setup_test().await;
    let ns = "default";

    let ss = make_statefulset("dns-pods", ns, 3);
    let key = build_key("statefulsets", Some(ns), "dns-pods");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let mut pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    pods.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
    assert_eq!(pods.len(), 3, "Parallel policy creates all pods upfront");

    for pod in &pods {
        let spec = pod.spec.as_ref().unwrap();
        // Subdomain must match the governing service name (`<name>-headless`).
        assert_eq!(
            spec.subdomain,
            Some("dns-pods-headless".to_string()),
            "pod {} subdomain must point at headless service",
            pod.metadata.name
        );
        // The `statefulset.kubernetes.io/pod-name` label is what
        // kube-proxy / EndpointSlice keys per-pod DNS entries on.
        let pod_name_label = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("statefulset.kubernetes.io/pod-name"))
            .cloned();
        assert_eq!(pod_name_label, Some(pod.metadata.name.clone()));
    }
}

/// Force-rollback: after rolling forward to a new image, the user updates the
/// template back to the original image. The controller MUST roll the pods back
/// to the original revision and (eventually) record the old revision as
/// `currentRevision` again.
///
/// Upstream: kubernetes/test/e2e/apps/statefulset.go — "should perform rolling
/// updates and roll backs of template modifications".
#[tokio::test]
async fn statefulset_force_rollback_to_previous_revision() {
    let storage = setup_test().await;
    let ns = "default";

    let ss = make_statefulset("rollback", ns, 2);
    let key = build_key("statefulsets", Some(ns), "rollback");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;
    controller.reconcile_all().await.unwrap();

    let initial_status = storage
        .get::<StatefulSet>(&key)
        .await
        .unwrap()
        .status
        .unwrap();
    let initial_revision = initial_status.current_revision.clone().unwrap();

    // Roll forward: change image.
    let mut updated: StatefulSet = storage.get(&key).await.unwrap();
    updated.spec.template.spec.containers[0].image = "nginx:1.27-alpine".to_string();
    storage.update(&key, &updated).await.unwrap();

    for _ in 0..10 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        mark_all_pods_ready(&storage, ns).await;
    }

    let after_forward = storage
        .get::<StatefulSet>(&key)
        .await
        .unwrap()
        .status
        .unwrap();
    let forward_revision = after_forward.current_revision.clone().unwrap();
    assert_ne!(
        initial_revision, forward_revision,
        "current_revision should advance after a rolling update"
    );

    // Roll back: restore the original image.
    let mut rolled_back: StatefulSet = storage.get(&key).await.unwrap();
    rolled_back.spec.template.spec.containers[0].image =
        "registry.k8s.io/e2e-test-images/agnhost:2.55".to_string();
    storage.update(&key, &rolled_back).await.unwrap();

    for _ in 0..10 {
        controller.reconcile_all().await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        mark_all_pods_ready(&storage, ns).await;
    }

    // After rollback, every pod should run the original image AND
    // current_revision should equal the initial revision (since the template
    // is identical to the original — same hash).
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    for pod in &pods {
        assert_eq!(
            pod.spec.as_ref().unwrap().containers[0].image,
            "registry.k8s.io/e2e-test-images/agnhost:2.55",
            "rollback should restore original image"
        );
    }

    let rolled_status = storage
        .get::<StatefulSet>(&key)
        .await
        .unwrap()
        .status
        .unwrap();
    assert_eq!(
        rolled_status.current_revision, initial_status.current_revision,
        "force rollback must restore the original current_revision"
    );
}

/// StatefulSet templates with `initContainers` should be propagated to every
/// rendered pod in the declared order. The kubelet runs init containers
/// sequentially before the main containers — the controller's job here is
/// purely to copy them over without reordering.
///
/// Upstream: kubernetes/test/e2e/apps/statefulset.go (init container handling
/// inherited from PodTemplateSpec).
#[tokio::test]
async fn statefulset_propagates_init_containers_in_order() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("init-c", ns, 2);
    ss.spec.template.spec.init_containers = Some(vec![
        Container {
            name: "init-permissions".to_string(),
            image: "busybox:1.36".to_string(),
            image_pull_policy: Some("IfNotPresent".to_string()),
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "chown 1000:1000 /var/lib/data".to_string(),
            ]),
            ports: None,
            env: None,
            volume_mounts: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            resources: None,
            working_dir: None,
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
        },
        Container {
            name: "init-schema".to_string(),
            image: "postgres:16".to_string(),
            image_pull_policy: Some("IfNotPresent".to_string()),
            command: Some(vec!["sh".to_string(), "-c".to_string(), "true".to_string()]),
            ports: None,
            env: None,
            volume_mounts: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            resources: None,
            working_dir: None,
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
        },
    ]);

    let key = build_key("statefulsets", Some(ns), "init-c");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);

    for pod in &pods {
        let init = pod
            .spec
            .as_ref()
            .unwrap()
            .init_containers
            .as_ref()
            .expect("pod must inherit initContainers from template");
        assert_eq!(
            init.len(),
            2,
            "pod {} must have 2 init containers",
            pod.metadata.name
        );
        // Order MUST match the template's declaration.
        assert_eq!(init[0].name, "init-permissions");
        assert_eq!(init[1].name, "init-schema");
        // Images must be carried verbatim.
        assert_eq!(init[0].image, "busybox:1.36");
        assert_eq!(init[1].image, "postgres:16");
    }
}

/// Updating a `volumeClaimTemplate` field (e.g. requested storage size) on an
/// existing StatefulSet should propagate to PVCs of newly-scheduled ordinals.
/// K8s today does NOT retroactively resize existing PVCs (you need the
/// volume-expansion controller for that) — but a freshly-created PVC for a
/// new ordinal must use the updated template.
///
/// Upstream: kubernetes/test/e2e/apps/statefulset.go — "should not resize
/// existing PVCs when VCT is updated" / KEP-661 (VolumeClaimTemplate update).
#[tokio::test]
async fn statefulset_vct_update_applies_to_new_ordinal_pvcs() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("vct-update", ns, 1);
    ss.spec.volume_claim_templates = Some(vec![data_pvc_template()]);
    let key = build_key("statefulsets", Some(ns), "vct-update");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;
    controller.reconcile_all().await.unwrap();

    // Bump VCT request from 1Gi → 5Gi and scale to 2.
    let mut updated: StatefulSet = storage.get(&key).await.unwrap();
    if let Some(templates) = updated.spec.volume_claim_templates.as_mut() {
        templates[0].spec.resources.requests =
            Some(HashMap::from([("storage".to_string(), "5Gi".to_string())]));
    }
    updated.spec.replicas = Some(2);
    storage.update(&key, &updated).await.unwrap();

    for _ in 0..4 {
        controller.reconcile_all().await.unwrap();
        mark_all_pods_ready(&storage, ns).await;
    }

    // The new PVC (ordinal 1) should request 5Gi.
    let pvc_1: PersistentVolumeClaim = storage
        .get(&build_key(
            "persistentvolumeclaims",
            Some(ns),
            "data-vct-update-1",
        ))
        .await
        .expect("PVC for ordinal 1 must exist after scale-up");
    let req_1 = pvc_1
        .spec
        .resources
        .requests
        .as_ref()
        .and_then(|m| m.get("storage"))
        .cloned();
    assert_eq!(
        req_1,
        Some("5Gi".to_string()),
        "PVC for newly-scheduled ordinal must use the updated VCT request"
    );

    // The existing PVC (ordinal 0) must remain at 1Gi — VCT changes do NOT
    // retroactively resize existing PVCs (that's the volume-expansion
    // controller's job).
    let pvc_0: PersistentVolumeClaim = storage
        .get(&build_key(
            "persistentvolumeclaims",
            Some(ns),
            "data-vct-update-0",
        ))
        .await
        .unwrap();
    let req_0 = pvc_0
        .spec
        .resources
        .requests
        .as_ref()
        .and_then(|m| m.get("storage"))
        .cloned();
    assert_eq!(
        req_0,
        Some("1Gi".to_string()),
        "VCT update must not retroactively resize existing PVCs"
    );
}

/// Comprehensive status validation: `observedGeneration`, `updateRevision`,
/// `currentRevision`, and `replicas`/`updatedReplicas`/`readyReplicas`/`availableReplicas`
/// must all be populated consistently on a steady-state StatefulSet.
///
/// Upstream: kubernetes/pkg/controller/statefulset/stateful_set_status_updater.go
/// computes all these fields in `computeReplicaStatus`; conformance tests
/// (e2e/apps/statefulset.go) verify them via the e2e harness.
#[tokio::test]
async fn statefulset_status_fields_are_consistent_at_steady_state() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("status-full", ns, 3);
    ss.metadata.generation = Some(7); // pretend the API server bumped generation
    let key = build_key("statefulsets", Some(ns), "status-full");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;
    controller.reconcile_all().await.unwrap();

    let ss_final: StatefulSet = storage.get(&key).await.unwrap();
    let status = ss_final.status.as_ref().expect("status must be populated");

    // observedGeneration must mirror metadata.generation so clients can detect
    // staleness.
    assert_eq!(
        status.observed_generation,
        Some(7),
        "observedGeneration must equal metadata.generation"
    );

    // Both revision fields must be populated; at steady state they're equal.
    let update_rev = status
        .update_revision
        .as_ref()
        .expect("update_revision must be set");
    let current_rev = status
        .current_revision
        .as_ref()
        .expect("current_revision must be set");
    assert_eq!(
        current_rev, update_rev,
        "at steady state currentRevision must equal updateRevision"
    );

    // Replica counts: 3 ready, 3 available (minReadySeconds=0), 3 current, 3 updated.
    assert_eq!(status.replicas, 3);
    assert_eq!(status.ready_replicas, Some(3));
    assert_eq!(status.available_replicas, Some(3));
    assert_eq!(status.current_replicas, Some(3));
    assert_eq!(status.updated_replicas, Some(3));
    // collisionCount is never populated by the (currently single-revision)
    // controller; if it ever is, it must be ≥0.
    assert!(status.collision_count.unwrap_or(0) >= 0);
}

/// `topologySpreadConstraints` declared on the pod template must be
/// propagated verbatim onto every rendered pod so the scheduler can balance
/// the StatefulSet across zones / nodes.
///
/// Upstream: kubernetes/test/e2e/scheduling/predicates.go and
/// kubernetes/test/e2e/apps/statefulset.go — pod template fields flow through
/// the StatefulSet controller untouched.
#[tokio::test]
async fn statefulset_propagates_topology_spread_constraints() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("spread", ns, 3);
    ss.spec.template.spec.topology_spread_constraints = Some(vec![TopologySpreadConstraint {
        max_skew: 1,
        topology_key: "topology.kubernetes.io/zone".to_string(),
        when_unsatisfiable: "DoNotSchedule".to_string(),
        label_selector: Some(LabelSelector {
            match_labels: Some(HashMap::from([("app".to_string(), "spread".to_string())])),
            match_expressions: None,
        }),
        min_domains: Some(2),
        node_affinity_policy: None,
        node_taints_policy: None,
        match_label_keys: None,
    }]);

    let key = build_key("statefulsets", Some(ns), "spread");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3);

    for pod in &pods {
        let constraints = pod
            .spec
            .as_ref()
            .unwrap()
            .topology_spread_constraints
            .as_ref()
            .expect("pod must inherit topology_spread_constraints from template");
        assert_eq!(constraints.len(), 1);
        let c = &constraints[0];
        assert_eq!(c.max_skew, 1);
        assert_eq!(c.topology_key, "topology.kubernetes.io/zone");
        assert_eq!(c.when_unsatisfiable, "DoNotSchedule");
        assert_eq!(c.min_domains, Some(2));
        // The selector must round-trip too, so the scheduler can identify the
        // pods that share this spread group.
        let sel = c.label_selector.as_ref().unwrap();
        assert_eq!(
            sel.match_labels
                .as_ref()
                .and_then(|m| m.get("app"))
                .map(String::as_str),
            Some("spread")
        );
    }
}

/// Deletion propagation: every pod, PVC (with retention policy
/// `whenDeleted=Delete`), and ControllerRevision the StatefulSet has produced
/// must carry an `ownerReference` back to the StatefulSet with
/// `controller=true` and `blockOwnerDeletion=true` so the API server's garbage
/// collector cascades on a foreground delete.
///
/// Upstream: kubernetes/pkg/controller/statefulset/stateful_set_utils.go ::
/// `newStatefulSetPod` / `claimOwnerMatchesSetAndPod` and
/// kubernetes/test/e2e/apps/statefulset.go — "should adopt matching orphans
/// and release non-matching pods" / cascade-delete coverage.
#[tokio::test]
async fn statefulset_owner_references_drive_cascading_delete() {
    let storage = setup_test().await;
    let ns = "default";

    let mut ss = make_statefulset("cascade", ns, 2);
    ss.spec.volume_claim_templates = Some(vec![data_pvc_template()]);
    ss.spec.persistent_volume_claim_retention_policy =
        Some(StatefulSetPersistentVolumeClaimRetentionPolicy {
            when_deleted: Some("Delete".to_string()),
            when_scaled: Some("Retain".to_string()),
        });

    let key = build_key("statefulsets", Some(ns), "cascade");
    storage.create(&key, &ss).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;
    controller.reconcile_all().await.unwrap();

    let ss_uid = ss.metadata.uid.clone();

    // Every pod must reference the StatefulSet as its controlling owner.
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);
    for pod in &pods {
        let refs = pod
            .metadata
            .owner_references
            .as_ref()
            .expect("pod must have ownerReferences");
        let owner = refs
            .iter()
            .find(|r| r.uid == ss_uid && r.kind == "StatefulSet")
            .expect("pod must be owned by the StatefulSet");
        assert_eq!(owner.controller, Some(true));
        assert_eq!(owner.block_owner_deletion, Some(true));
    }

    // PVCs must likewise be owned, otherwise the GC cannot cascade
    // `whenDeleted=Delete`.
    let pvcs: Vec<PersistentVolumeClaim> = storage
        .list("/registry/persistentvolumeclaims/default/")
        .await
        .unwrap();
    assert_eq!(pvcs.len(), 2);
    for pvc in &pvcs {
        let refs = pvc
            .metadata
            .owner_references
            .as_ref()
            .expect("PVC must have ownerReferences");
        let owner = refs
            .iter()
            .find(|r| r.uid == ss_uid && r.kind == "StatefulSet")
            .expect("PVC must be owned by the StatefulSet");
        assert_eq!(owner.controller, Some(true));
    }

    // ControllerRevisions get stamped with `owner_references` too — the
    // StatefulSet controller writes them as serde_json::Value, so we read
    // them back the same way and walk the JSON tree.
    let crs: Vec<Value> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap_or_default();
    assert!(
        !crs.is_empty(),
        "at least one ControllerRevision must be created on first reconcile"
    );
    for cr in &crs {
        let owner_refs = cr
            .pointer("/metadata/ownerReferences")
            .and_then(|v| v.as_array())
            .expect("ControllerRevision must have ownerReferences");
        let owned = owner_refs.iter().any(|r| {
            r.pointer("/uid").and_then(|v| v.as_str()) == Some(ss_uid.as_str())
                && r.pointer("/kind").and_then(|v| v.as_str()) == Some("StatefulSet")
                && r.pointer("/controller").and_then(|v| v.as_bool()) == Some(true)
        });
        assert!(
            owned,
            "ControllerRevision must be owned by the StatefulSet for cascading delete"
        );
    }

    // Simulate foreground deletion: stamp deletionTimestamp on the StatefulSet
    // and reconcile. The controller MUST short-circuit (it returns early when
    // is_being_deleted is true) — pods continue to exist; the API server's
    // garbage collector is what eventually removes them via the
    // ownerReferences we just verified.
    let mut ss_for_delete: StatefulSet = storage.get(&key).await.unwrap();
    ss_for_delete.metadata.deletion_timestamp = Some(chrono::Utc::now());
    storage.update(&key, &ss_for_delete).await.unwrap();

    controller.reconcile_all().await.unwrap();

    // Pods are not removed by the StatefulSet controller during deletion —
    // GC owns that. The point of this test is that ownerReferences are
    // correctly populated so GC CAN cascade.
    let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(
        pods_after.len(),
        2,
        "StatefulSet controller must defer pod deletion to GC during foreground delete"
    );
}
