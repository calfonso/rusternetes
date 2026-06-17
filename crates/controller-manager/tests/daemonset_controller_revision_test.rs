// DaemonSet ControllerRevision Integration Tests
// Tests the DaemonSet controller's creation and management of ControllerRevisions

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::daemonset::DaemonSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn create_test_node(name: &str) -> Node {
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new(name);
            m.ensure_uid();
            m
        },
        spec: Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        }),
        status: None,
    }
}

fn create_test_daemonset(name: &str, namespace: &str) -> DaemonSet {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), name.to_string());

    DaemonSet {
        type_meta: TypeMeta {
            kind: "DaemonSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some(namespace.to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: DaemonSetSpec {
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
                spec: PodSpec {
                    containers: vec![Container {
                        name: "logger".to_string(),
                        image: "busybox:latest".to_string(),
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
                },
            },
            update_strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
        },
        status: None,
    }
}

#[tokio::test]
async fn test_controller_revision_created_on_reconcile() {
    let storage = setup_test().await;

    // Create a node so the DaemonSet has somewhere to schedule
    let node = create_test_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    // Create a DaemonSet
    let ds = create_test_daemonset("my-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "my-ds"), &ds)
        .await
        .unwrap();

    // Run controller
    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // A ControllerRevision should have been created
    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    assert!(
        !revisions.is_empty(),
        "ControllerRevision should be created during reconciliation"
    );

    let cr = &revisions[0];
    assert_eq!(cr.type_meta.kind, "ControllerRevision");
    assert_eq!(cr.type_meta.api_version, "apps/v1");
    assert_eq!(cr.revision, 1);
}

#[tokio::test]
async fn test_controller_revision_has_correct_owner_references() {
    let storage = setup_test().await;

    let node = create_test_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let ds = create_test_daemonset("owner-ref-ds", "default");
    let ds_uid = ds.metadata.uid.clone();
    storage
        .create(
            &build_key("daemonsets", Some("default"), "owner-ref-ds"),
            &ds,
        )
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    assert!(!revisions.is_empty());

    let cr = &revisions[0];
    let owner_refs = cr
        .metadata
        .owner_references
        .as_ref()
        .expect("ControllerRevision should have ownerReferences");

    assert_eq!(owner_refs.len(), 1);
    assert_eq!(owner_refs[0].api_version, "apps/v1");
    assert_eq!(owner_refs[0].kind, "DaemonSet");
    assert_eq!(owner_refs[0].name, "owner-ref-ds");
    assert_eq!(owner_refs[0].uid, ds_uid);
    assert_eq!(
        owner_refs[0].controller,
        Some(true),
        "controller flag should be true"
    );
    assert_eq!(
        owner_refs[0].block_owner_deletion,
        Some(true),
        "blockOwnerDeletion should be true"
    );
}

#[tokio::test]
async fn test_controller_revision_has_expected_labels() {
    let storage = setup_test().await;

    let node = create_test_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let ds = create_test_daemonset("labeled-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "labeled-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    assert!(!revisions.is_empty());

    let cr = &revisions[0];
    let labels = cr
        .metadata
        .labels
        .as_ref()
        .expect("ControllerRevision should have labels");

    // Must have the controller-revision-hash label
    assert!(
        labels.contains_key("controller-revision-hash"),
        "Should have controller-revision-hash label"
    );

    // Must have the controller.kubernetes.io/hash label
    assert!(
        labels.contains_key("controller.kubernetes.io/hash"),
        "Should have controller.kubernetes.io/hash label"
    );

    // Both hash labels should have the same value
    assert_eq!(
        labels.get("controller-revision-hash"),
        labels.get("controller.kubernetes.io/hash"),
        "Both hash labels should match"
    );

    // DaemonSet's matchLabels should be copied to the ControllerRevision
    assert_eq!(
        labels.get("app"),
        Some(&"labeled-ds".to_string()),
        "DaemonSet matchLabels should be copied to ControllerRevision"
    );
}

#[tokio::test]
async fn test_controller_revision_has_uid_and_timestamp() {
    let storage = setup_test().await;

    let node = create_test_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let ds = create_test_daemonset("uid-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "uid-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    assert!(!revisions.is_empty());

    let cr = &revisions[0];
    assert!(
        !cr.metadata.uid.is_empty(),
        "ControllerRevision should have a UID"
    );
    assert!(
        cr.metadata.creation_timestamp.is_some(),
        "ControllerRevision should have a creation timestamp"
    );
}

#[tokio::test]
async fn test_controller_revision_contains_template_data() {
    let storage = setup_test().await;

    let node = create_test_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let ds = create_test_daemonset("data-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "data-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    assert!(!revisions.is_empty());

    let cr = &revisions[0];
    assert!(
        cr.data.is_some(),
        "ControllerRevision should contain serialized template data"
    );

    // The data should be a serialized PodTemplateSpec
    let data = cr.data.as_ref().unwrap();
    // It should contain the container image from the DaemonSet template
    let data_str = serde_json::to_string(data).unwrap();
    assert!(
        data_str.contains("busybox:latest"),
        "Revision data should contain the pod template's container image"
    );
}

#[tokio::test]
async fn test_controller_revision_not_duplicated_on_rereconcile() {
    let storage = setup_test().await;

    let node = create_test_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let ds = create_test_daemonset("idempotent-ds", "default");
    storage
        .create(
            &build_key("daemonsets", Some("default"), "idempotent-ds"),
            &ds,
        )
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());

    // Reconcile multiple times
    controller.reconcile_all().await.unwrap();
    controller.reconcile_all().await.unwrap();
    controller.reconcile_all().await.unwrap();

    // Should still only have one ControllerRevision (same template hash)
    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    assert_eq!(
        revisions.len(),
        1,
        "Multiple reconciliations with the same template should not create duplicate revisions"
    );
}

#[tokio::test]
async fn test_controller_revision_name_includes_hash() {
    let storage = setup_test().await;

    let node = create_test_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let ds = create_test_daemonset("hash-ds", "default");
    storage
        .create(&build_key("daemonsets", Some("default"), "hash-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    assert!(!revisions.is_empty());

    let cr = &revisions[0];
    // Name should be "{ds-name}-{hash}" format
    assert!(
        cr.metadata.name.starts_with("hash-ds-"),
        "ControllerRevision name should start with DaemonSet name followed by a dash"
    );
    // The hash suffix follows the K8s SafeEncodeString convention: each digit
    // of the decimal FNV hash is remapped through the
    // "bcdfghjklmnpqrstvwxz2456789" alphabet. Length varies (1–10 chars for a
    // u32) and characters are NOT hex.
    // K8s ref: staging/src/k8s.io/apimachinery/pkg/util/rand/rand.go
    const SAFE_ALPHABET: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
    let suffix = cr.metadata.name.strip_prefix("hash-ds-").unwrap();
    assert!(
        !suffix.is_empty() && suffix.len() <= 10,
        "Hash suffix should be 1-10 SafeEncodeString chars, got: {} ({} chars)",
        suffix,
        suffix.len()
    );
    assert!(
        suffix.bytes().all(|b| SAFE_ALPHABET.contains(&b)),
        "Hash suffix should only use the SafeEncodeString alphabet, got: {}",
        suffix
    );
}

#[tokio::test]
async fn test_controller_revision_namespace_matches_daemonset() {
    let storage = setup_test().await;

    let node = create_test_node("node-1");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    // Create DaemonSet in a non-default namespace
    let ds = create_test_daemonset("ns-ds", "kube-system");
    storage
        .create(&build_key("daemonsets", Some("kube-system"), "ns-ds"), &ds)
        .await
        .unwrap();

    let controller = DaemonSetController::new(storage.clone());
    controller.reconcile_all().await.unwrap();

    // ControllerRevision should be in the same namespace
    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/kube-system/")
        .await
        .unwrap();
    assert!(
        !revisions.is_empty(),
        "ControllerRevision should be in the same namespace as the DaemonSet"
    );

    let cr = &revisions[0];
    assert_eq!(
        cr.metadata.namespace,
        Some("kube-system".to_string()),
        "ControllerRevision namespace should match DaemonSet"
    );
}
