//! Integration tests mirroring upstream node lifecycle taint-eviction tests
//! as RED-state TDD pins.
//!
//! Upstream:
//!   - test/integration/node/lifecycle_test.go
//!     <https://github.com/kubernetes/kubernetes/blob/release-1.35/test/integration/node/lifecycle_test.go>
//!
//! Tests mirrored:
//!   1. `TestEvictionForNoExecuteTaintAddedByUser` — when a user manually applies
//!      a `NoExecute` taint to a node, the taint-eviction controller must evict
//!      pods that lack a matching toleration. A `DisruptionTarget` pod condition
//!      with `reason=DeletionByTaintManager` MUST be set before the
//!      `deletionTimestamp` is written.
//!   2. `TestTaintBasedEvictions` — when the node lifecycle controller marks a
//!      node `NotReady`, it must apply the `node.kubernetes.io/not-ready`
//!      taint with the `NoExecute` effect, and the taint-eviction controller
//!      must then honor `tolerationSeconds` (0, 200, 300, …) when deciding
//!      whether and when to evict pods.
//!
//! Both tests drive the in-tree controllers directly against an
//! `Arc<MemoryStorage>` — no API server, no informers — following the same
//! pattern used by `crates/controller-manager/tests/node_controller_test.rs`.

use chrono::{Duration, Utc};
use rusternetes_common::resources::pod::Toleration;
use rusternetes_common::resources::{
    Container, Node, NodeCondition, NodeSpec, NodeStatus, Pod, PodSpec, PodStatus, Taint,
};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::node::NodeController;
use rusternetes_controller_manager::controllers::taint_eviction::TaintEvictionController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

/// Build a minimal Ready node living in "zone1/region1" with no taints.
fn make_ready_node(name: &str) -> Node {
    let mut metadata = ObjectMeta::new(name);
    let mut labels = std::collections::HashMap::new();
    labels.insert(
        "topology.kubernetes.io/region".to_string(),
        "region1".to_string(),
    );
    labels.insert(
        "topology.kubernetes.io/zone".to_string(),
        "zone1".to_string(),
    );
    metadata.labels = Some(labels);

    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata,
        spec: Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        }),
        status: Some(NodeStatus {
            conditions: Some(vec![NodeCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_heartbeat_time: Some(Utc::now()),
                last_transition_time: Some(Utc::now()),
                reason: Some("KubeletReady".to_string()),
                message: Some("kubelet is ready".to_string()),
            }]),
            addresses: None,
            capacity: None,
            allocatable: None,
            node_info: None,
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            daemon_endpoints: None,
            config: None,
            features: None,
            runtime_handlers: None,
            declared_features: None,
        }),
    }
}

/// Build a minimal pod scheduled on `node_name`. `tolerations` may be empty.
fn make_running_pod(
    name: &str,
    namespace: &str,
    node_name: &str,
    tolerations: Vec<Toleration>,
) -> Pod {
    let mut metadata = ObjectMeta::new(name);
    metadata.namespace = Some(namespace.to_string());
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata,
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "main".to_string(),
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
            ephemeral_containers: None,
            volumes: None,
            restart_policy: Some("Always".to_string()),
            node_name: Some(node_name.to_string()),
            node_selector: None,
            service_account_name: None,
            service_account: None,
            automount_service_account_token: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: if tolerations.is_empty() {
                None
            } else {
                Some(tolerations)
            },
            priority: None,
            priority_class_name: None,
            scheduler_name: None,
            overhead: None,
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
            pod_ip: Some("10.244.0.1".to_string()),
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

/// Mirrors upstream `TestEvictionForNoExecuteTaintAddedByUser`.
///
/// When a user manually adds a `NoExecute` taint to a node, the taint-eviction
/// controller must:
///   (a) evict pods that lack a matching toleration (set `deletionTimestamp`),
///   (b) emit a `DisruptionTarget` pod condition with
///       `reason=DeletionByTaintManager` BEFORE deleting,
///   (c) leave pods that tolerate the taint untouched.
#[tokio::test]
async fn test_eviction_for_no_execute_taint_added_by_user() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = TaintEvictionController::new(storage.clone());

    // Three nodes — upstream uses 3.
    for i in 0..3 {
        let node = make_ready_node(&format!("node-{}", i));
        let key = build_key("nodes", None, &node.metadata.name);
        storage.create(&key, &node).await.unwrap();
    }

    // Victim pod on node-1 with no tolerations.
    let victim = make_running_pod("victim", "default", "node-1", vec![]);
    let victim_key = build_key("pods", Some("default"), "victim");
    storage.create(&victim_key, &victim).await.unwrap();

    // Tolerant pod also on node-1 with an Exists toleration for the user taint.
    let tolerator = make_running_pod(
        "tolerator",
        "default",
        "node-1",
        vec![Toleration {
            key: Some("CustomTaintByUser".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoExecute".to_string()),
            toleration_seconds: None,
        }],
    );
    let tolerator_key = build_key("pods", Some("default"), "tolerator");
    storage.create(&tolerator_key, &tolerator).await.unwrap();

    // User applies the NoExecute taint to node-1.
    let node_key = build_key("nodes", None, "node-1");
    let mut node: Node = storage.get(&node_key).await.unwrap();
    let spec = node.spec.get_or_insert(NodeSpec {
        pod_cidr: None,
        pod_cidrs: None,
        provider_id: None,
        unschedulable: None,
        taints: None,
    });
    spec.taints = Some(vec![Taint {
        key: "CustomTaintByUser".to_string(),
        value: None,
        effect: "NoExecute".to_string(),
        time_added: Some(Utc::now()),
    }]);
    storage.update(&node_key, &node).await.unwrap();

    // Drive the controller.
    controller.reconcile_all().await.unwrap();

    // (a) victim must be marked for deletion.
    let evicted: Pod = storage.get(&victim_key).await.unwrap();
    assert!(
        evicted.metadata.deletion_timestamp.is_some(),
        "victim pod must have deletionTimestamp set (upstream: \
         TestEvictionForNoExecuteTaintAddedByUser expects pod to reach \
         terminating state)"
    );

    // (b) DisruptionTarget condition with the expected reason.
    let dt = evicted
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.condition_type == "DisruptionTarget"))
        .expect("DisruptionTarget condition must be present on evicted pod");
    assert_eq!(dt.status, "True");
    assert_eq!(
        dt.reason.as_deref(),
        Some("DeletionByTaintManager"),
        "upstream sets DisruptionTarget reason to DeletionByTaintManager"
    );

    // (c) tolerant pod must remain untouched.
    let kept: Pod = storage.get(&tolerator_key).await.unwrap();
    assert!(
        kept.metadata.deletion_timestamp.is_none(),
        "pod tolerating the user-applied NoExecute taint must NOT be evicted"
    );
}

/// Mirrors upstream `TestTaintBasedEvictions`.
///
/// The node lifecycle controller, after grace, must apply the
/// `node.kubernetes.io/not-ready` taint with effect `NoExecute` to a node whose
/// Ready condition flipped to `False`. The taint-eviction controller must
/// then enforce `tolerationSeconds`:
///   * `tolerationSeconds=0` — evict immediately (`deletionTimestamp` set).
///   * `tolerationSeconds=200` — keep until 200s elapses since `timeAdded`.
///   * no toleration — evict immediately.
///
/// We exercise the 0-second and the 200-second branches plus the
/// no-toleration baseline.
#[tokio::test]
async fn test_taint_based_evictions() {
    let storage = Arc::new(MemoryStorage::new());
    let node_ctrl = NodeController::new(storage.clone());
    let evict_ctrl = TaintEvictionController::new(storage.clone());

    // Single NotReady node with a stale heartbeat (>40s) — node controller
    // should flip Ready=False and add the not-ready taint.
    let node_name = "node-not-ready";
    let stale = Utc::now() - Duration::seconds(120);
    let mut node = make_ready_node(node_name);
    {
        let status = node.status.as_mut().unwrap();
        status.conditions = Some(vec![NodeCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            last_heartbeat_time: Some(stale),
            last_transition_time: Some(stale),
            reason: Some("KubeletReady".to_string()),
            message: Some("kubelet is ready".to_string()),
        }]);
    }
    let node_key = build_key("nodes", None, node_name);
    storage.create(&node_key, &node).await.unwrap();
    node_ctrl.seed_first_seen_for_test(node_name);

    // Three pods on the NotReady node:
    //   pod-zero-toleration — tolerates with tolerationSeconds=0 (evict now).
    //   pod-200-toleration  — tolerates 200s (must remain on first tick).
    //   pod-no-toleration   — no toleration at all (evict now).
    let pod_zero = make_running_pod(
        "pod-zero-toleration",
        "default",
        node_name,
        vec![Toleration {
            key: Some("node.kubernetes.io/not-ready".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoExecute".to_string()),
            toleration_seconds: Some(0),
        }],
    );
    let pod_200 = make_running_pod(
        "pod-200-toleration",
        "default",
        node_name,
        vec![Toleration {
            key: Some("node.kubernetes.io/not-ready".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoExecute".to_string()),
            toleration_seconds: Some(200),
        }],
    );
    let pod_none = make_running_pod("pod-no-toleration", "default", node_name, vec![]);

    let key_zero = build_key("pods", Some("default"), "pod-zero-toleration");
    let key_200 = build_key("pods", Some("default"), "pod-200-toleration");
    let key_none = build_key("pods", Some("default"), "pod-no-toleration");
    storage.create(&key_zero, &pod_zero).await.unwrap();
    storage.create(&key_200, &pod_200).await.unwrap();
    storage.create(&key_none, &pod_none).await.unwrap();

    // 1. Node lifecycle controller observes the stale heartbeat and applies
    //    the not-ready taint with effect NoExecute.
    node_ctrl.reconcile_all().await.unwrap();

    let updated_node: Node = storage.get(&node_key).await.unwrap();
    let taints = updated_node
        .spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .expect("not-ready taint must be applied to NotReady node");
    let not_ready = taints
        .iter()
        .find(|t| t.key == "node.kubernetes.io/not-ready")
        .expect("node.kubernetes.io/not-ready taint must exist");
    assert_eq!(
        not_ready.effect, "NoExecute",
        "upstream TestTaintBasedEvictions requires the not-ready taint to be \
         NoExecute (current impl uses NoSchedule — RED pin)"
    );

    // 2. Taint-eviction controller reconciles given the NoExecute taint.
    evict_ctrl.reconcile_all().await.unwrap();

    // pod-zero-toleration: tolerationSeconds=0 — evict immediately.
    let evicted_zero: Pod = storage.get(&key_zero).await.unwrap();
    assert!(
        evicted_zero.metadata.deletion_timestamp.is_some(),
        "pod with tolerationSeconds=0 must be evicted on first reconcile"
    );

    // pod-no-toleration: no toleration — evict immediately.
    let evicted_none: Pod = storage.get(&key_none).await.unwrap();
    assert!(
        evicted_none.metadata.deletion_timestamp.is_some(),
        "pod with no toleration must be evicted on first reconcile"
    );

    // pod-200-toleration: tolerationSeconds=200 — must REMAIN this tick.
    let kept: Pod = storage.get(&key_200).await.unwrap();
    assert!(
        kept.metadata.deletion_timestamp.is_none(),
        "pod with tolerationSeconds=200 must NOT be evicted within 200s of \
         the taint being added (upstream TestTaintBasedEvictions)"
    );
}
