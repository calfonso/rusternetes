//! Integration tests for preemption victim selection with PodDisruptionBudget.
//!
//! Mirrors the upstream e2e scenarios in `test/e2e/scheduling/preemption.go`
//! at sites :535 (PodDisruptionBudget protection during preemption) and :1025
//! (post-preemption availableReplicas recovery). Both tests fail upstream when
//! the scheduler picks too aggressive a victim set — every replica from the
//! PDB-protected ReplicaSet gets evicted, leaving `availableReplicas` at zero
//! and the test eventually times out with:
//!
//!     replicaset "rs-pod1" never had desired number of .status.availableReplicas
//!
//! Algorithm property we verify: given a node where preemption is required and
//! candidates include pods covered by a PDB, the scheduler MUST prefer evicting
//! pods that are *not* covered by the PDB when the math works out. PDB-covered
//! pods may only be selected as victims when no other choice would let the
//! incoming pod schedule.

use std::collections::HashMap;

use rusternetes_common::resources::{
    Container, IntOrString, Pod, PodDisruptionBudget, PodDisruptionBudgetSpec, PodSpec, PodStatus,
};
use rusternetes_common::types::{LabelSelector, Phase, ResourceRequirements};
use rusternetes_scheduler::advanced::check_preemption_with_pdbs;

fn make_container(cpu: &str, memory: &str) -> Container {
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), cpu.to_string());
    requests.insert("memory".to_string(), memory.to_string());
    Container {
        name: "main".to_string(),
        image: "busybox".to_string(),
        command: None,
        args: None,
        working_dir: None,
        ports: None,
        env: None,
        env_from: None,
        resources: Some(ResourceRequirements {
            requests: Some(requests),
            limits: None,
            claims: None,
        }),
        volume_mounts: None,
        volume_devices: None,
        image_pull_policy: None,
        liveness_probe: None,
        readiness_probe: None,
        startup_probe: None,
        security_context: None,
        restart_policy: None,
        resize_policy: None,
        lifecycle: None,
        termination_message_path: None,
        termination_message_policy: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        ..Default::default()
    }
}

use rusternetes_test_support::node_with_resources as make_node;

fn make_pod_with_labels(
    name: &str,
    priority: i32,
    cpu: &str,
    memory: &str,
    node_name: &str,
    labels: Option<HashMap<String, String>>,
) -> Pod {
    let spec = PodSpec {
        containers: vec![make_container(cpu, memory)],
        priority: Some(priority),
        node_name: Some(node_name.to_string()),
        ..Default::default()
    };
    let mut pod = Pod::new(name, spec);
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.labels = labels;
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        ..Default::default()
    });
    pod
}

fn make_incoming_pod(name: &str, priority: i32, cpu: &str, memory: &str) -> Pod {
    let spec = PodSpec {
        containers: vec![make_container(cpu, memory)],
        priority: Some(priority),
        ..Default::default()
    };
    let mut pod = Pod::new(name, spec);
    pod.metadata.namespace = Some("default".to_string());
    pod
}

fn make_pdb(name: &str, min_available: i32, app_label: &str) -> PodDisruptionBudget {
    let mut match_labels = HashMap::new();
    match_labels.insert("app".to_string(), app_label.to_string());
    PodDisruptionBudget::new(
        name,
        "default",
        PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(min_available)),
            max_unavailable: None,
            selector: LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            },
            unhealthy_pod_eviction_policy: None,
        },
    )
}

/// Mirrors preemption.go:535 — three PDB-protected replicas plus one
/// unprotected "filler" pod. Incoming high-priority pod needs the resources of
/// exactly one pod. The scheduler must pick the filler, not a PDB-protected
/// replica. Otherwise the ReplicaSet drops below the PDB minimum and
/// availableReplicas never recovers.
#[test]
fn preemption_prefers_non_pdb_victim_when_possible() {
    // Node has room for 4 unit pods.
    let node = make_node("node-1", "4", "4Gi");

    let mut rs_labels = HashMap::new();
    rs_labels.insert("app".to_string(), "rs-pod1".to_string());

    let rs1 = make_pod_with_labels(
        "rs-pod1-a",
        100,
        "1",
        "1Gi",
        "node-1",
        Some(rs_labels.clone()),
    );
    let rs2 = make_pod_with_labels(
        "rs-pod1-b",
        100,
        "1",
        "1Gi",
        "node-1",
        Some(rs_labels.clone()),
    );
    let rs3 = make_pod_with_labels(
        "rs-pod1-c",
        100,
        "1",
        "1Gi",
        "node-1",
        Some(rs_labels.clone()),
    );

    // Filler pod with same priority, but NOT covered by the PDB.
    let filler = make_pod_with_labels("filler", 100, "1", "1Gi", "node-1", None);

    // PDB requires 3 ReplicaSet replicas to remain available.
    let pdb = make_pdb("rs-pod1-pdb", 3, "rs-pod1");

    // Incoming high-priority pod needs 1 CPU (exactly one pod's worth).
    let incoming = make_incoming_pod("high-pri", 1000, "1", "1Gi");

    let all_pods = vec![rs1, rs2, rs3, filler];
    let (can_preempt, victims) = check_preemption_with_pdbs(&node, &incoming, &all_pods, &[pdb]);

    assert!(can_preempt, "Preemption should succeed");
    assert_eq!(
        victims.len(),
        1,
        "Exactly one victim should be selected, got {:?}",
        victims
    );
    assert_eq!(
        victims[0], "filler",
        "Filler (non-PDB-covered) pod must be the victim, not a PDB-protected replica. Got {}",
        victims[0]
    );
}

/// Mirrors preemption.go:1025 — when a PDB-protected pod *must* be evicted
/// (no other option), the scheduler MAY still preempt it, but only the minimum
/// number required. The remaining replicas should still satisfy the PDB.
#[test]
fn preemption_minimizes_pdb_victim_count() {
    // Node has room for 3 unit pods, currently full with PDB-protected pods.
    let node = make_node("node-1", "3", "3Gi");

    let mut rs_labels = HashMap::new();
    rs_labels.insert("app".to_string(), "rs-pod1".to_string());

    let rs1 = make_pod_with_labels(
        "rs-pod1-a",
        100,
        "1",
        "1Gi",
        "node-1",
        Some(rs_labels.clone()),
    );
    let rs2 = make_pod_with_labels(
        "rs-pod1-b",
        100,
        "1",
        "1Gi",
        "node-1",
        Some(rs_labels.clone()),
    );
    let rs3 = make_pod_with_labels(
        "rs-pod1-c",
        100,
        "1",
        "1Gi",
        "node-1",
        Some(rs_labels.clone()),
    );

    // PDB requires 2 replicas to remain — one eviction is allowed.
    let pdb = make_pdb("rs-pod1-pdb", 2, "rs-pod1");

    // Incoming pod needs 1 CPU — exactly one eviction needed.
    let incoming = make_incoming_pod("high-pri", 1000, "1", "1Gi");

    let all_pods = vec![rs1, rs2, rs3];
    let (can_preempt, victims) = check_preemption_with_pdbs(&node, &incoming, &all_pods, &[pdb]);

    assert!(
        can_preempt,
        "Preemption should still succeed with one victim"
    );
    assert_eq!(
        victims.len(),
        1,
        "Only one PDB-covered pod should be evicted (minAvailable=2 allows 1 disruption). Got {:?}",
        victims
    );
}

/// Sanity check — without a PDB the algorithm is unchanged.
#[test]
fn preemption_without_pdb_matches_legacy_behavior() {
    let node = make_node("node-1", "2", "2Gi");

    let mut rs_labels = HashMap::new();
    rs_labels.insert("app".to_string(), "rs-pod1".to_string());

    let p1 = make_pod_with_labels(
        "rs-pod1-a",
        100,
        "1",
        "1Gi",
        "node-1",
        Some(rs_labels.clone()),
    );
    let p2 = make_pod_with_labels("rs-pod1-b", 100, "1", "1Gi", "node-1", Some(rs_labels));

    let incoming = make_incoming_pod("high-pri", 1000, "1", "1Gi");

    let (can_preempt, victims) = check_preemption_with_pdbs(&node, &incoming, &[p1, p2], &[]);

    assert!(can_preempt);
    assert_eq!(
        victims.len(),
        1,
        "Without PDB, exactly one victim suffices; got {:?}",
        victims
    );
}
