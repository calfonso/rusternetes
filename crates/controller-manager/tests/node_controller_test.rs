//! Integration tests for NodeController
//!
//! Extended coverage mirroring upstream `kubernetes/test/e2e/node/lifecycle.go`:
//!
//! - Shutdown taint (`node.kubernetes.io/shutdown`) applied on graceful shutdown
//! - Multi-condition monitoring (Ready / MemoryPressure / DiskPressure /
//!   PIDPressure / NetworkUnavailable) with `lastTransitionTime` handling
//! - `coordination.k8s.io/v1` Lease heartbeat semantics
//! - `status.allocatable = status.capacity - reserved`
//!
//! Tests that exercise behaviour not yet implemented in
//! `crates/controller-manager/src/controllers/node.rs` are pinned with
//! `#[ignore = "RED-state: ..."]`; lifting the ignore marker is the unit of
//! work to GREEN them.

use chrono::{Duration, Utc};
use rusternetes_common::resources::node::Taint;
use rusternetes_common::resources::{Lease, LeaseSpec, Node, NodeCondition, NodeSpec, NodeStatus};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::node::NodeController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_node_controller_creation() {
    let storage = Arc::new(MemoryStorage::new());
    let _controller = NodeController::new(storage);
}

#[tokio::test]
async fn test_node_ready_with_recent_heartbeat() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    // Create a node with recent heartbeat
    let node = Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-node-ready".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: None,
        status: Some(NodeStatus {
            conditions: Some(vec![NodeCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_heartbeat_time: Some(Utc::now()), // Recent heartbeat
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
    };

    let key = build_key("nodes", None, "test-node-ready");
    storage.create(&key, &node).await.unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Node should still be marked as ready
    let retrieved: Node = storage.get(&key).await.unwrap();
    let ready_condition = retrieved
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|conditions| conditions.iter().find(|c| c.condition_type == "Ready"));

    assert!(ready_condition.is_some());
    assert_eq!(ready_condition.unwrap().status, "True");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_not_ready_with_old_heartbeat() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    // Create a node with old heartbeat (60 seconds ago)
    let old_time = Utc::now() - Duration::seconds(60);

    let node = Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-node-not-ready".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: None,
        status: Some(NodeStatus {
            conditions: Some(vec![NodeCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_heartbeat_time: Some(old_time), // Old heartbeat
                last_transition_time: Some(old_time),
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
    };

    let key = build_key("nodes", None, "test-node-not-ready");
    storage.create(&key, &node).await.unwrap();

    // Skip the 60s startup grace so reconcile flips the condition this tick.
    controller.seed_first_seen_for_test("test-node-not-ready");

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Node should be marked as not ready
    let retrieved: Node = storage.get(&key).await.unwrap();
    let ready_condition = retrieved
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|conditions| conditions.iter().find(|c| c.condition_type == "Ready"));

    assert!(ready_condition.is_some());
    let condition = ready_condition.unwrap();

    // Status should be False due to old heartbeat
    assert_eq!(condition.status, "False");

    // Clean up
    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_without_ready_condition() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    // Create a node without Ready condition
    let node = Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-node-no-condition".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: None,
        status: Some(NodeStatus {
            conditions: None,
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
    };

    let key = build_key("nodes", None, "test-node-no-condition");
    storage.create(&key, &node).await.unwrap();

    // Skip the 60s startup grace so reconcile creates the condition this tick.
    controller.seed_first_seen_for_test("test-node-no-condition");

    // Reconcile should create a Ready condition
    controller.reconcile_all().await.unwrap();

    // Node should have a Ready condition now
    let retrieved: Node = storage.get(&key).await.unwrap();
    let ready_condition = retrieved
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|conditions| conditions.iter().find(|c| c.condition_type == "Ready"));

    assert!(ready_condition.is_some());

    // Clean up
    storage.delete(&key).await.unwrap();
}

// ----------------------------------------------------------------------------
// Shared helpers for the extended coverage below.
// ----------------------------------------------------------------------------

/// Build a bare Node with the given name and an optional list of conditions.
/// The status carries no capacity/allocatable so the helper composes with the
/// allocatable-vs-capacity tests further down.
fn make_node(name: &str, conditions: Option<Vec<NodeCondition>>) -> Node {
    Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: None,
        status: Some(NodeStatus {
            conditions,
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

// ----------------------------------------------------------------------------
// Shutdown taint
//
// Upstream behaviour (k8s.io/kubernetes/pkg/controller/nodelifecycle):
// when the kubelet enters graceful shutdown it sets a Ready=False condition
// with reason "NodeShutdown"; the node lifecycle controller then applies the
// taint `node.kubernetes.io/shutdown` (effect NoSchedule by default; some
// configs use NoExecute) so the scheduler stops admitting new pods to the
// shutting-down node.
//
// rusternetes' `NodeController` currently only knows about the
// `node.kubernetes.io/not-ready` taint (see `add_not_ready_taint`). It does
// not consult the Ready condition's `reason` to distinguish a shutdown from
// a generic NotReady, and so it never produces the shutdown taint.
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_node_shutdown_taint_applied_on_graceful_shutdown() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    let now = Utc::now();
    let stale = now - Duration::seconds(120);

    // Kubelet reports Ready=False with reason "NodeShutdown" as soon as it
    // starts the graceful-shutdown sequence (see
    // kubelet/nodeshutdown/nodeshutdown_manager_linux.go in upstream).
    let node = make_node(
        "test-node-shutdown",
        Some(vec![NodeCondition {
            condition_type: "Ready".to_string(),
            status: "False".to_string(),
            last_heartbeat_time: Some(stale),
            last_transition_time: Some(stale),
            reason: Some("NodeShutdown".to_string()),
            message: Some("node is shutting down".to_string()),
        }]),
    );

    let key = build_key("nodes", None, "test-node-shutdown");
    storage.create(&key, &node).await.unwrap();

    controller.seed_first_seen_for_test("test-node-shutdown");
    controller.reconcile_all().await.unwrap();

    let updated: Node = storage.get(&key).await.unwrap();
    let taints = updated
        .spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .cloned()
        .unwrap_or_default();

    assert!(
        taints
            .iter()
            .any(|t| t.key == "node.kubernetes.io/shutdown"),
        "shutdown taint not applied; taints present: {:?}",
        taints.iter().map(|t| &t.key).collect::<Vec<_>>()
    );

    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_shutdown_taint_removed_on_recovery() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    let now = Utc::now();

    // A recovered node: Ready=True with a fresh heartbeat and no NodeShutdown
    // reason, but still carrying a stale shutdown taint from a prior graceful
    // shutdown. The controller must clear it so scheduling resumes.
    let mut node = make_node(
        "test-node-recovered",
        Some(vec![NodeCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            last_heartbeat_time: Some(now),
            last_transition_time: Some(now),
            reason: Some("KubeletReady".to_string()),
            message: Some("kubelet is posting ready status".to_string()),
        }]),
    );
    node.spec
        .get_or_insert(rusternetes_common::resources::NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        })
        .taints = Some(vec![rusternetes_common::resources::node::Taint {
        key: "node.kubernetes.io/shutdown".to_string(),
        value: Some("".to_string()),
        effect: "NoSchedule".to_string(),
        time_added: None,
    }]);

    let key = build_key("nodes", None, "test-node-recovered");
    storage.create(&key, &node).await.unwrap();

    controller.seed_first_seen_for_test("test-node-recovered");
    controller.reconcile_all().await.unwrap();

    let updated: Node = storage.get(&key).await.unwrap();
    let taints = updated
        .spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .cloned()
        .unwrap_or_default();
    assert!(
        !taints
            .iter()
            .any(|t| t.key == "node.kubernetes.io/shutdown"),
        "shutdown taint should be removed once the node recovers; taints: {:?}",
        taints.iter().map(|t| &t.key).collect::<Vec<_>>()
    );

    storage.delete(&key).await.unwrap();
}

// ----------------------------------------------------------------------------
// Multi-condition monitoring
//
// `NodeCondition.lastTransitionTime` is the API contract: it only updates when
// `status` flips, not on every heartbeat. We exercise that for the Ready
// condition (which the controller manages) and leave the pressure conditions
// pinned `#[ignore]` because rusternetes doesn't yet evaluate MemoryPressure /
// DiskPressure / PIDPressure / NetworkUnavailable.
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_node_ready_condition_last_transition_time_preserved_on_no_flip() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    // Node has been Ready=True for an hour; last heartbeat is fresh.
    // Reconcile must NOT bump lastTransitionTime because the status doesn't
    // change. (Upstream contract from
    // staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go:
    // "lastTransitionTime ... is updated each time the condition transitions
    // from one status to another").
    let pinned = Utc::now() - Duration::seconds(3_600);
    let node = make_node(
        "test-node-ready-stable",
        Some(vec![NodeCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            last_heartbeat_time: Some(Utc::now()),
            last_transition_time: Some(pinned),
            reason: Some("KubeletReady".to_string()),
            message: Some("kubelet is posting ready status".to_string()),
        }]),
    );

    let key = build_key("nodes", None, "test-node-ready-stable");
    storage.create(&key, &node).await.unwrap();

    controller.seed_first_seen_for_test("test-node-ready-stable");
    controller.reconcile_all().await.unwrap();

    let updated: Node = storage.get(&key).await.unwrap();
    let ready = updated
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.condition_type == "Ready"))
        .expect("Ready condition must remain present");

    assert_eq!(ready.status, "True");
    assert_eq!(
        ready.last_transition_time,
        Some(pinned),
        "lastTransitionTime must not move when status doesn't flip"
    );

    storage.delete(&key).await.unwrap();
}

#[tokio::test]
async fn test_node_pressure_condition_transitions_observed() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    let now = Utc::now();
    // A node carrying every pressure condition the upstream API ships with,
    // each in its "good" state. A future controller surface must keep them
    // observable (i.e. preserve them across reconciles).
    let node = make_node(
        "test-node-pressure",
        Some(vec![
            NodeCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                last_heartbeat_time: Some(now),
                last_transition_time: Some(now),
                reason: Some("KubeletReady".to_string()),
                message: Some("kubelet is posting ready status".to_string()),
            },
            NodeCondition {
                condition_type: "MemoryPressure".to_string(),
                status: "False".to_string(),
                last_heartbeat_time: Some(now),
                last_transition_time: Some(now),
                reason: Some("KubeletHasSufficientMemory".to_string()),
                message: Some("kubelet has sufficient memory available".to_string()),
            },
            NodeCondition {
                condition_type: "DiskPressure".to_string(),
                status: "False".to_string(),
                last_heartbeat_time: Some(now),
                last_transition_time: Some(now),
                reason: Some("KubeletHasNoDiskPressure".to_string()),
                message: Some("kubelet has no disk pressure".to_string()),
            },
            NodeCondition {
                condition_type: "PIDPressure".to_string(),
                status: "False".to_string(),
                last_heartbeat_time: Some(now),
                last_transition_time: Some(now),
                reason: Some("KubeletHasSufficientPID".to_string()),
                message: Some("kubelet has sufficient PID available".to_string()),
            },
            NodeCondition {
                condition_type: "NetworkUnavailable".to_string(),
                status: "False".to_string(),
                last_heartbeat_time: Some(now),
                last_transition_time: Some(now),
                reason: Some("RouteCreated".to_string()),
                message: Some("RouteController created a route".to_string()),
            },
        ]),
    );

    let key = build_key("nodes", None, "test-node-pressure");
    storage.create(&key, &node).await.unwrap();

    controller.seed_first_seen_for_test("test-node-pressure");
    controller.reconcile_all().await.unwrap();

    let updated: Node = storage.get(&key).await.unwrap();
    let conditions = updated
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .cloned()
        .unwrap_or_default();

    // All five must survive the reconcile pass — upstream parity contract.
    for expected in [
        "Ready",
        "MemoryPressure",
        "DiskPressure",
        "PIDPressure",
        "NetworkUnavailable",
    ] {
        assert!(
            conditions.iter().any(|c| c.condition_type == expected),
            "condition {} missing after reconcile; got {:?}",
            expected,
            conditions
                .iter()
                .map(|c| c.condition_type.as_str())
                .collect::<Vec<_>>()
        );
    }

    // And a MemoryPressure flip should bump only its own lastTransitionTime,
    // not Ready's. Drop the controller back into the storage with a False→True
    // flip and reconcile.
    let mut node2: Node = storage.get(&key).await.unwrap();
    if let Some(status) = node2.status.as_mut() {
        if let Some(conditions) = status.conditions.as_mut() {
            if let Some(mp) = conditions
                .iter_mut()
                .find(|c| c.condition_type == "MemoryPressure")
            {
                mp.status = "True".to_string();
                mp.last_heartbeat_time = Some(Utc::now());
                // lastTransitionTime intentionally left stale: a real
                // controller must update it on flip.
            }
        }
    }
    storage.update(&key, &node2).await.unwrap();
    controller.reconcile_all().await.unwrap();

    let after: Node = storage.get(&key).await.unwrap();
    let mp_after = after
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.condition_type == "MemoryPressure"))
        .cloned()
        .expect("MemoryPressure condition expected");
    assert_eq!(mp_after.status, "True");
    // The pre-flip lastTransitionTime was `now` (set when the node was
    // created). A correct controller bumps it past `now` when it observes the
    // False -> True flip. Use a strict ordering rather than a fuzzy window
    // so the assertion is robust against clock jitter.
    let updated_ltt = mp_after
        .last_transition_time
        .expect("MemoryPressure lastTransitionTime must be set");
    assert!(
        updated_ltt > now,
        "lastTransitionTime must be refreshed on MemoryPressure flip: pre={:?}, post={:?}",
        now,
        updated_ltt
    );

    storage.delete(&key).await.unwrap();
}

// ----------------------------------------------------------------------------
// Node Lease heartbeats (coordination.k8s.io/v1)
//
// Upstream: kubelet writes a `Lease` named after the node in the
// `kube-node-lease` namespace every `nodeLeaseDurationSeconds / 4` seconds
// (default 10s). The node lifecycle controller treats a fresh Lease as
// equivalent to a fresh Ready-condition heartbeat (see
// `is_node_ready_async` in the rusternetes implementation, which mirrors
// `pkg/controller/nodelifecycle/node_lifecycle_controller.go`).
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_node_remains_ready_when_lease_is_fresh_despite_stale_heartbeat() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    let now = Utc::now();
    let stale = now - Duration::seconds(120);

    // Node condition's heartbeat is stale (>40s grace) — alone this would
    // flip the node to NotReady. The Lease is fresh, so the controller MUST
    // keep Ready=True.
    let node = make_node(
        "test-node-fresh-lease",
        Some(vec![NodeCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            last_heartbeat_time: Some(stale),
            last_transition_time: Some(stale),
            reason: Some("KubeletReady".to_string()),
            message: Some("kubelet is posting ready status".to_string()),
        }]),
    );

    let node_key = build_key("nodes", None, "test-node-fresh-lease");
    storage.create(&node_key, &node).await.unwrap();

    // Fresh Lease in kube-node-lease.
    let lease = Lease {
        type_meta: TypeMeta {
            kind: "Lease".to_string(),
            api_version: "coordination.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("test-node-fresh-lease").with_namespace("kube-node-lease"),
        spec: Some(LeaseSpec {
            holder_identity: Some("test-node-fresh-lease".to_string()),
            lease_duration_seconds: Some(40),
            acquire_time: Some(now),
            renew_time: Some(now),
            lease_transitions: Some(0),
            preferred_holder: None,
            strategy: None,
        }),
    };
    let lease_key = build_key("leases", Some("kube-node-lease"), "test-node-fresh-lease");
    storage.create(&lease_key, &lease).await.unwrap();

    controller.seed_first_seen_for_test("test-node-fresh-lease");
    controller.reconcile_all().await.unwrap();

    let updated: Node = storage.get(&node_key).await.unwrap();
    let ready = updated
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.condition_type == "Ready"))
        .expect("Ready condition expected");
    assert_eq!(
        ready.status, "True",
        "fresh Lease must keep the node Ready even with a stale condition heartbeat"
    );

    // And the not-ready taint must NOT have been applied.
    let has_not_ready_taint = updated
        .spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .map(|ts| ts.iter().any(|t| t.key == "node.kubernetes.io/not-ready"))
        .unwrap_or(false);
    assert!(
        !has_not_ready_taint,
        "not-ready taint must not be applied while Lease is fresh"
    );

    storage.delete(&node_key).await.unwrap();
    storage.delete(&lease_key).await.unwrap();
}

#[tokio::test]
async fn test_node_lease_renewal_bumps_renew_time() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    // Use a whole-second timestamp so the assertion is robust against the
    // microsecond truncation that MicroTime serialisation imposes on the
    // round trip (see `LeaseSpec`'s `%.6f` formatter).
    let initial = (Utc::now() - Duration::seconds(120))
        .timestamp_nanos_opt()
        .and_then(|ns| chrono::DateTime::<Utc>::from_timestamp(ns / 1_000_000_000, 0))
        .expect("constant timestamp must convert");
    let node = make_node(
        "test-node-lease-renew",
        Some(vec![NodeCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            last_heartbeat_time: Some(Utc::now()),
            last_transition_time: Some(Utc::now()),
            reason: Some("KubeletReady".to_string()),
            message: Some("kubelet is posting ready status".to_string()),
        }]),
    );
    let node_key = build_key("nodes", None, "test-node-lease-renew");
    storage.create(&node_key, &node).await.unwrap();

    let lease = Lease {
        type_meta: TypeMeta {
            kind: "Lease".to_string(),
            api_version: "coordination.k8s.io/v1".to_string(),
        },
        metadata: ObjectMeta::new("test-node-lease-renew").with_namespace("kube-node-lease"),
        spec: Some(LeaseSpec {
            holder_identity: Some("test-node-lease-renew".to_string()),
            lease_duration_seconds: Some(40),
            acquire_time: Some(initial),
            renew_time: Some(initial),
            lease_transitions: Some(0),
            preferred_holder: None,
            strategy: None,
        }),
    };
    let lease_key = build_key("leases", Some("kube-node-lease"), "test-node-lease-renew");
    storage.create(&lease_key, &lease).await.unwrap();

    controller.seed_first_seen_for_test("test-node-lease-renew");
    controller.reconcile_all().await.unwrap();

    let renewed: Lease = storage.get(&lease_key).await.unwrap();
    let renew_time = renewed
        .spec
        .as_ref()
        .and_then(|s| s.renew_time)
        .expect("renewTime must be present after reconcile");
    assert!(
        renew_time > initial,
        "controller must bump renewTime: initial={:?}, current={:?}",
        initial,
        renew_time
    );

    storage.delete(&node_key).await.unwrap();
    storage.delete(&lease_key).await.unwrap();
}

// ----------------------------------------------------------------------------
// Allocatable vs Capacity
//
// Upstream contract (`pkg/kubelet/cm/node_container_manager_linux.go` ->
// `setNodeStatusMachineInfo` & `getNodeAllocatableAbsolute`):
//   allocatable = capacity − kube-reserved − system-reserved − eviction-hard
// rusternetes' NodeController does not yet compute this; the kubelet stub
// also doesn't write Allocatable. Pin the invariant for the future.
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_node_allocatable_equals_capacity_minus_reserved() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = NodeController::new(storage.clone());

    // 4 CPUs, 8Gi memory; reserve 500m CPU + 1Gi via the upstream-style
    // annotations the kubelet would surface.
    let mut capacity = HashMap::new();
    capacity.insert("cpu".to_string(), "4".to_string());
    capacity.insert("memory".to_string(), "8Gi".to_string());
    capacity.insert("pods".to_string(), "110".to_string());

    let mut annotations = HashMap::new();
    // Upstream uses these CLI flags on the kubelet; we use annotations as a
    // proxy for the reservation inputs so the test doesn't depend on
    // kubelet-side flag plumbing.
    annotations.insert(
        "node.alpha.kubernetes.io/kube-reserved".to_string(),
        "cpu=500m,memory=1Gi".to_string(),
    );

    let now = Utc::now();
    let mut node = make_node(
        "test-node-allocatable",
        Some(vec![NodeCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            last_heartbeat_time: Some(now),
            last_transition_time: Some(now),
            reason: Some("KubeletReady".to_string()),
            message: Some("kubelet is posting ready status".to_string()),
        }]),
    );
    node.metadata.annotations = Some(annotations);
    if let Some(status) = node.status.as_mut() {
        status.capacity = Some(capacity.clone());
        // Allocatable intentionally left unset — the controller must derive it.
        status.allocatable = None;
    }

    let key = build_key("nodes", None, "test-node-allocatable");
    storage.create(&key, &node).await.unwrap();

    controller.seed_first_seen_for_test("test-node-allocatable");
    controller.reconcile_all().await.unwrap();

    let updated: Node = storage.get(&key).await.unwrap();
    let allocatable = updated
        .status
        .as_ref()
        .and_then(|s| s.allocatable.as_ref())
        .cloned()
        .expect("allocatable must be derived after reconcile");

    // 4 - 0.5 = 3500m CPU; 8Gi - 1Gi = 7Gi memory; pods unchanged.
    assert_eq!(
        allocatable.get("cpu").map(String::as_str),
        Some("3500m"),
        "cpu allocatable mismatch: {:?}",
        allocatable
    );
    assert_eq!(
        allocatable.get("memory").map(String::as_str),
        Some("7Gi"),
        "memory allocatable mismatch: {:?}",
        allocatable
    );
    assert_eq!(
        allocatable.get("pods").map(String::as_str),
        Some("110"),
        "pods allocatable should equal capacity"
    );

    storage.delete(&key).await.unwrap();
}

// ----------------------------------------------------------------------------
// Sanity: NodeSpec / Taint plumbing the new tests depend on.
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_node_spec_taint_round_trip_in_storage() {
    let storage = Arc::new(MemoryStorage::new());

    let now = Utc::now();
    let mut node = make_node(
        "test-node-taint-rt",
        Some(vec![NodeCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            last_heartbeat_time: Some(now),
            last_transition_time: Some(now),
            reason: Some("KubeletReady".to_string()),
            message: Some("kubelet is posting ready status".to_string()),
        }]),
    );
    node.spec = Some(NodeSpec {
        pod_cidr: None,
        pod_cidrs: None,
        provider_id: None,
        unschedulable: Some(false),
        taints: Some(vec![Taint {
            key: "node.kubernetes.io/shutdown".to_string(),
            value: Some("".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: Some(now),
        }]),
    });

    let key = build_key("nodes", None, "test-node-taint-rt");
    storage.create(&key, &node).await.unwrap();

    let fetched: Node = storage.get(&key).await.unwrap();
    let taints = fetched
        .spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .cloned()
        .expect("taints survive a round-trip");
    assert_eq!(taints.len(), 1);
    assert_eq!(taints[0].key, "node.kubernetes.io/shutdown");
    assert_eq!(taints[0].effect, "NoSchedule");

    storage.delete(&key).await.unwrap();
}
