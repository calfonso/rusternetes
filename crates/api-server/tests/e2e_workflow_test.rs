// End-to-End Workflow Tests
// Note: These tests use in-memory storage for fast, isolated testing

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::volume::*;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::deployment::DeploymentController;
use rusternetes_controller_manager::controllers::dynamic_provisioner::DynamicProvisionerController;
use rusternetes_controller_manager::controllers::pv_binder::PVBinderController;
use rusternetes_controller_manager::controllers::replicaset::ReplicaSetController;
use rusternetes_controller_manager::controllers::volume_snapshot::VolumeSnapshotController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());

    // Clean up all test data (memory storage starts fresh)
    storage.clear();

    storage
}

#[tokio::test]
async fn test_complete_pod_lifecycle() {
    let storage = setup_test().await;

    // 1. Create namespace
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("test-ns");
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: None,
        status: None,
    };

    let ns_key = build_key("namespaces", None, "test-ns");
    storage.create(&ns_key, &namespace).await.unwrap();

    // 2. Create pod via storage (simulating API)
    let pod = Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("test-pod");
            meta.namespace = Some("test-ns".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "nginx".to_string(),
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
            phase: Some(Phase::Pending),
            message: None,
            reason: None,
            host_ip: None,
            host_i_ps: None,
            pod_ip: None,
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            conditions: None,
            ..Default::default()
        }),
    };

    let pod_key = build_key("pods", Some("test-ns"), "test-pod");
    storage.create(&pod_key, &pod).await.unwrap();

    // 3. Verify pod stored in etcd
    let stored_pod: Pod = storage.get(&pod_key).await.unwrap();
    assert_eq!(stored_pod.metadata.name, "test-pod");
    assert_eq!(
        stored_pod.status.as_ref().unwrap().phase,
        Some(Phase::Pending)
    );

    // 4. Simulate scheduler assignment
    let mut updated_pod = stored_pod;
    updated_pod.spec.as_mut().unwrap().node_name = Some("worker-1".to_string());
    storage.update(&pod_key, &updated_pod).await.unwrap();

    // 5. Verify pod has nodeName
    let scheduled_pod: Pod = storage.get(&pod_key).await.unwrap();
    assert_eq!(
        scheduled_pod.spec.as_ref().unwrap().node_name,
        Some("worker-1".to_string())
    );

    // 6. Simulate kubelet updating status
    let mut running_pod = scheduled_pod;
    running_pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        message: None,
        reason: None,
        host_ip: Some("192.168.1.10".to_string()),
        host_i_ps: None,
        pod_ip: Some("10.244.1.5".to_string()),
        pod_i_ps: None,
        nominated_node_name: None,
        qos_class: None,
        start_time: None,
        container_statuses: None,
        init_container_statuses: None,
        ephemeral_container_statuses: None,
        resize: None,
        resource_claim_statuses: None,
        observed_generation: None,
        conditions: None,
        ..Default::default()
    });
    storage.update(&pod_key, &running_pod).await.unwrap();

    // 7. Verify pod phase is Running
    let final_pod: Pod = storage.get(&pod_key).await.unwrap();
    assert_eq!(
        final_pod.status.as_ref().unwrap().phase,
        Some(Phase::Running)
    );
    assert_eq!(
        final_pod.status.as_ref().unwrap().pod_ip,
        Some("10.244.1.5".to_string())
    );
}

#[tokio::test]
async fn test_deployment_workflow() {
    let storage = setup_test().await;

    // 1. Create deployment
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "web".to_string());

    let deployment = Deployment {
        type_meta: TypeMeta {
            kind: "Deployment".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("web-deployment");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: DeploymentSpec {
            replicas: Some(3),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                match_expressions: None,
            },
            min_ready_seconds: None,
            revision_history_limit: None,
            template: PodTemplateSpec {
                metadata: Some({
                    let mut meta = ObjectMeta::new("web-pod");
                    meta.labels = Some(labels);
                    meta
                }),
                spec: PodSpec {
                    containers: vec![Container {
                        name: "nginx".to_string(),
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
                },
            },
            strategy: None,
            paused: None,
            progress_deadline_seconds: None,
        },
        status: Some(DeploymentStatus {
            replicas: Some(0),
            ready_replicas: Some(0),
            available_replicas: Some(0),
            unavailable_replicas: Some(0),
            updated_replicas: Some(0),
            conditions: None,
            collision_count: None,
            observed_generation: None,
            terminating_replicas: None,
        }),
    };

    let deployment_key = build_key("deployments", Some("default"), "web-deployment");
    storage.create(&deployment_key, &deployment).await.unwrap();

    // 2. DeploymentController creates ReplicaSet
    let deployment_controller = DeploymentController::new(storage.clone(), 10);
    deployment_controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // 3. ReplicaSetController creates Pods
    let replicaset_controller = ReplicaSetController::new(storage.clone(), 10);
    replicaset_controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // 4. Verify all pods created
    let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 3, "Should create 3 pods");

    // 5. Simulate scheduler assigning pods
    for pod in &pods {
        let mut updated_pod = pod.clone();
        updated_pod.spec.as_mut().unwrap().node_name = Some("worker-1".to_string());
        let pod_key = build_key("pods", Some("default"), &pod.metadata.name);
        storage.update(&pod_key, &updated_pod).await.unwrap();
    }

    // 6. Simulate kubelet running containers
    for pod in &pods {
        let pod_key = build_key("pods", Some("default"), &pod.metadata.name);
        let mut updated_pod: Pod = storage.get(&pod_key).await.unwrap();
        updated_pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("192.168.1.10".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.1.5".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            conditions: None,
            ..Default::default()
        });
        storage.update(&pod_key, &updated_pod).await.unwrap();
    }

    // 7. Verify all pods running
    let final_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(final_pods.len(), 3);
    for pod in &final_pods {
        assert_eq!(pod.status.as_ref().unwrap().phase, Some(Phase::Running));
    }
}

#[tokio::test]
async fn test_dynamic_pvc_workflow() {
    let storage = setup_test().await;

    // 1. Create StorageClass
    let sc = StorageClass {
        type_meta: TypeMeta {
            kind: "StorageClass".to_string(),
            api_version: "storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("fast"),
        provisioner: "rusternetes.io/hostpath".to_string(),
        parameters: Some({
            let mut params = HashMap::new();
            params.insert(
                "path".to_string(),
                "/tmp/rusternetes/test-volumes".to_string(),
            );
            params
        }),
        reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
        volume_binding_mode: Some(VolumeBindingMode::Immediate),
        allowed_topologies: None,
        allow_volume_expansion: None,
        mount_options: None,
    };

    let sc_key = build_key("storageclasses", None, "fast");
    storage.create(&sc_key, &sc).await.unwrap();

    // 2. Create PVC
    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("my-pvc");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                limits: None,
                requests: Some(requests),
            },
            volume_name: None,
            storage_class_name: Some("fast".to_string()),
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            selector: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeClaimStatus {
            allocated_resources: None,
            allocated_resource_statuses: None,
            resize_status: None,
            phase: PersistentVolumeClaimPhase::Pending,
            access_modes: None,
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "my-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    // 3. Dynamic provisioner creates PV
    let provisioner = DynamicProvisionerController::new(storage.clone());
    provisioner.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify PV was created
    let pv_name = "pvc-default-my-pvc";
    let pv_key = build_key("persistentvolumes", None, pv_name);
    let pv: Result<PersistentVolume, _> = storage.get(&pv_key).await;
    assert!(pv.is_ok(), "PV should be created by provisioner");

    // 4. PV binder binds PVC to PV
    let binder = PVBinderController::new(storage.clone());
    binder.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // 5. Verify volume bound
    let bound_pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.unwrap();
    assert_eq!(bound_pvc.spec.volume_name, Some(pv_name.to_string()));
    assert_eq!(
        bound_pvc.status.as_ref().unwrap().phase,
        PersistentVolumeClaimPhase::Bound
    );

    let bound_pv: PersistentVolume = storage.get(&pv_key).await.unwrap();
    assert!(bound_pv.spec.claim_ref.is_some());
    assert_eq!(
        bound_pv.status.as_ref().unwrap().phase,
        PersistentVolumePhase::Bound
    );
}

#[tokio::test]
async fn test_snapshot_workflow() {
    let storage = setup_test().await;

    // 1. Create VolumeSnapshotClass
    let vsc = VolumeSnapshotClass {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotClass".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("snapshot-class"),
        driver: "rusternetes.io/hostpath-snapshotter".to_string(),
        parameters: None,
        deletion_policy: DeletionPolicy::Delete,
    };

    let vsc_key = build_key("volumesnapshotclasses", None, "snapshot-class");
    storage.create(&vsc_key, &vsc).await.unwrap();

    // 2. Create PVC with data (simulated as bound)
    let mut capacity = HashMap::new();
    capacity.insert("storage".to_string(), "10Gi".to_string());

    let pv = PersistentVolume {
        type_meta: TypeMeta {
            kind: "PersistentVolume".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("data-pv");
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: PersistentVolumeSpec {
            capacity: capacity.clone(),
            host_path: Some(volume::HostPathVolumeSource {
                path: "/tmp/test-pv/data-pv".to_string(),
                r#type: Some(volume::HostPathType::DirectoryOrCreate),
            }),
            nfs: None,
            iscsi: None,
            local: None,
            csi: None,
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            persistent_volume_reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
            storage_class_name: Some("fast".to_string()),
            mount_options: None,
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            node_affinity: None,
            claim_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeStatus {
            phase: PersistentVolumePhase::Available,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        }),
    };

    let pv_key = build_key("persistentvolumes", None, "data-pv");
    storage.create(&pv_key, &pv).await.unwrap();

    let mut requests = HashMap::new();
    requests.insert("storage".to_string(), "5Gi".to_string());

    let pvc = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("data-pvc");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                limits: None,
                requests: Some(requests),
            },
            volume_name: Some("data-pv".to_string()),
            storage_class_name: Some("fast".to_string()),
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            selector: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeClaimStatus {
            allocated_resources: None,
            allocated_resource_statuses: None,
            resize_status: None,
            phase: PersistentVolumeClaimPhase::Bound,
            access_modes: Some(vec![PersistentVolumeAccessMode::ReadWriteOnce]),
            capacity: Some(capacity),
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };

    let pvc_key = build_key("persistentvolumeclaims", Some("default"), "data-pvc");
    storage.create(&pvc_key, &pvc).await.unwrap();

    // 3. Create VolumeSnapshot
    let vs = VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new("my-snapshot");
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: VolumeSnapshotSpec {
            source: VolumeSnapshotSource {
                persistent_volume_claim_name: Some("data-pvc".to_string()),
                volume_snapshot_content_name: None,
            },
            volume_snapshot_class_name: "snapshot-class".to_string(),
        },
        status: None,
    };

    let vs_key = build_key("volumesnapshots", Some("default"), "my-snapshot");
    storage.create(&vs_key, &vs).await.unwrap();

    // 4. Controller creates VolumeSnapshotContent
    let snapshot_controller = VolumeSnapshotController::new(storage.clone());
    snapshot_controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // 5. Verify snapshot ready
    let updated_vs: VolumeSnapshot = storage.get(&vs_key).await.unwrap();
    assert!(updated_vs.status.is_some());
    assert_eq!(updated_vs.status.as_ref().unwrap().ready_to_use, Some(true));
    assert!(updated_vs
        .status
        .as_ref()
        .unwrap()
        .bound_volume_snapshot_content_name
        .is_some());

    let content_name = updated_vs
        .status
        .as_ref()
        .unwrap()
        .bound_volume_snapshot_content_name
        .as_ref()
        .unwrap();
    let content_key = build_key("volumesnapshotcontents", None, content_name);
    let content: VolumeSnapshotContent = storage.get(&content_key).await.unwrap();
    assert_eq!(content.spec.deletion_policy, DeletionPolicy::Delete);

    // 6. Delete snapshot with Delete policy
    storage.delete(&vs_key).await.unwrap();

    // 7. Run reconciliation to handle deletion
    snapshot_controller.reconcile_all().await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Verify content also deleted
    let content_after: Result<VolumeSnapshotContent, _> = storage.get(&content_key).await;
    assert!(
        content_after.is_err(),
        "VolumeSnapshotContent should be deleted"
    );
}
