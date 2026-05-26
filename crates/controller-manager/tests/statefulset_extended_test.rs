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
    PersistentVolumeClaim, PersistentVolumeClaimSpec, ResourceRequirements, Volume,
    PersistentVolumeClaimVolumeSource,
};
use rusternetes_common::resources::workloads::{
    RollingUpdateStatefulSetStrategy, StatefulSetPersistentVolumeClaimRetentionPolicy,
    StatefulSetUpdateStrategy,
};
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::statefulset::StatefulSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
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
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            selector: None,
            resources: Some(ResourceRequirements {
                limits: None,
                requests: Some(HashMap::from([("storage".to_string(), "1Gi".to_string())])),
            }),
            volume_name: None,
            storage_class_name: Some("standard".to_string()),
            volume_mode: None,
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
    pvc.status = Some(rusternetes_common::resources::volume::PersistentVolumeClaimStatus {
        phase: Some("Bound".to_string()),
        access_modes: None,
        capacity: None,
        conditions: None,
        allocated_resources: None,
        allocated_resource_statuses: None,
    });
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
    ss.spec.persistent_volume_claim_retention_policy = Some(
        StatefulSetPersistentVolumeClaimRetentionPolicy {
            when_deleted: Some("Retain".to_string()),
            when_scaled_down: Some("Retain".to_string()),
        }
    );
    
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
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            selector: None,
            resources: Some(ResourceRequirements {
                limits: None,
                requests: Some(HashMap::from([("storage".to_string(), "1Gi".to_string())])),
            }),
            volume_name: None,
            storage_class_name: Some("standard".to_string()),
            volume_mode: None,
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
    
    assert_eq!(final_pvcs.len(), 3, "Retain policy keeps all PVCs on scale down");
    
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
    ss.spec.persistent_volume_claim_retention_policy = Some(
        StatefulSetPersistentVolumeClaimRetentionPolicy {
            when_deleted: Some("Delete".to_string()),
            when_scaled_down: Some("Delete".to_string()),
        }
    );
    
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
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            selector: None,
            resources: Some(ResourceRequirements {
                limits: None,
                requests: Some(HashMap::from([("storage".to_string(), "1Gi".to_string())])),
            }),
            volume_name: None,
            storage_class_name: Some("standard".to_string()),
            volume_mode: None,
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
    
    assert_eq!(final_pvcs.len(), 1, "Delete policy removes PVCs on scale down");
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
            let mut meta = ObjectMeta::new("network-id-headless")
                .with_namespace(ns.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: ServiceSpec {
            selector: Some(HashMap::from([("app".to_string(), "network-id".to_string())])),
            cluster_ip: Some("None".to_string()), // Headless service
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: Some("80".to_string()),
                protocol: Some("TCP".to_string()),
                app_protocol: None,
                node_port: None,
            }],
            type_: Some("ClusterIP".to_string()),
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
        },
        status: None,
    };
    
    let svc_key = build_key("services", Some(ns), "network-id-headless");
    storage.create(&svc_key, &service).await.unwrap();

    let controller = StatefulSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();
    mark_all_pods_ready(&storage, ns).await;

    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 2);
    
    // Verify pod hostnames and subdomains are set correctly
    for (i, pod) in pods.iter().enumerate() {
        let expected_name = format!("network-id-{}", i);
        assert_eq!(pod.metadata.name, expected_name);
        
        // Pod should have hostname set to its name
        assert_eq!(
            pod.spec.hostname, 
            Some(expected_name.clone()),
            "Pod {} should have hostname set", i
        );
        
        // Pod should have subdomain set to service name
        assert_eq!(
            pod.spec.subdomain,
            Some("network-id-headless".to_string()),
            "Pod {} should have subdomain set to headless service", i
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
    
    let original_image = initial[0].spec.containers[0].image.clone();
    
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
        
        let pod_image = &pod.spec.containers[0].image;
        
        if ordinal < 3 {
            assert_eq!(
                pod_image, &original_image,
                "Pod {} (ordinal {}) should keep original image", pod.metadata.name, ordinal
            );
        } else {
            assert_eq!(
                pod_image, "nginx:1.26-alpine",
                "Pod {} (ordinal {}) should have new image", pod.metadata.name, ordinal
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
    
    let original_image = initial[0].spec.containers[0].image.clone();
    
    // Change template image
    let mut updated: StatefulSet = storage.get(&key).await.unwrap();
    updated.spec.template.spec.containers[0].image = "nginx:1.27-alpine".to_string();
    storage.update(&key, &updated).await.unwrap();
    
    // Reconcile - should NOT update any pods with OnDelete
    controller.reconcile_all().await.unwrap();
    
    let after_reconcile: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    for pod in &after_reconcile {
        assert_eq!(
            pod.spec.containers[0].image, original_image,
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
        recreated_pod.spec.containers[0].image, "nginx:1.27-alpine",
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
    let revisions: Vec<rusternetes_common::resources::workloads::ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap_or_default();
    
    // Should have at most 2 revisions (the limit)
    assert!(
        revisions.len() <= 2,
        "Should have at most 2 revisions, found {}", revisions.len()
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
    assert_eq!(pods.len(), 5, "Parallel policy creates all pods in one reconcile");
    
    // Verify all ordinals exist
    let mut ordinals: Vec<i32> = pods
        .iter()
        .filter_map(|p| p.metadata.name.rsplit('-').next().and_then(|s| s.parse().ok()))
        .collect();
    ordinals.sort();
    
    assert_eq!(ordinals, vec![0, 1, 2, 3, 4]);
}
