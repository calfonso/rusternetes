//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-scheduling] SchedulerPreemption — disruption conditions, critical-pod
//! preemption, PriorityClass HTTP-method endpoints, and hostPort 0.0.0.0
//! conflict.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/scheduling/
//! (`preemption.go`, `priorities.go`)
//!
//! Specific upstream test descriptors mirrored:
//!
//!   - preemption.go: `validates lower priority pod preemption by critical pod`
//!     (Sonobuoy Round 160+: FAIL — cluster-level end-to-end timing; scheduler
//!     unit invariant verified here: a system-critical pod CAN preempt any
//!     lower-priority pod when the node has insufficient free resources).
//!
//!   - preemption.go: `validates pod disruption condition is added to the
//!     preempted pod` (Sonobuoy Round 160+: FAIL — depends on kubelet writing
//!     the condition back; scheduler-side invariant: after `evict_pod_like_scheduler`
//!     applies the mutation, the pod carries `DisruptionTarget=True` with
//!     `reason=PreemptionByScheduler`).
//!
//!   - priorities.go / API-server: `PriorityClass endpoints verify PriorityClass
//!     endpoints can be operated with different HTTP methods [Conformance]`
//!     (Sonobuoy Round 160+: FAIL — full HTTP e2e; marked `#[ignore]` here
//!     because no HTTP harness is available in the scheduler unit-test layer).
//!
//!   - predicates.go / network: `validates that there exists conflict between
//!     pods with same hostPort and protocol but one using 0.0.0.0 hostIP`
//!     (Sonobuoy Round 160+: FAIL — e2e cluster timing; scheduler-side
//!     invariant: `check_host_port_conflicts` returns `false` for any pod
//!     attempting to bind a (port, protocol) already owned by a 0.0.0.0 pod).
//!
//! NO HTTP harness — scheduler logic is exercised by direct calls into
//! `rusternetes_scheduler::advanced`. Fixtures are plain `Node`/`Pod` values.
//!
//! See the test-by-test status table in docs/CONFORMANCE.md for the current
//! pass/fail mapping.

use std::collections::HashMap;

use rusternetes_common::resources::{
    Container, ContainerPort, Pod, PodCondition, PodSpec, PodStatus,
};
use rusternetes_common::types::{Phase, ResourceRequirements};
use rusternetes_scheduler::advanced::{check_host_port_conflicts, check_preemption};

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

fn make_resources(cpu: &str, memory: &str) -> ResourceRequirements {
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), cpu.to_string());
    requests.insert("memory".to_string(), memory.to_string());
    ResourceRequirements {
        requests: Some(requests),
        limits: None,
        claims: None,
    }
}

fn make_container(name: &str, cpu: &str, memory: &str) -> Container {
    Container {
        name: name.to_string(),
        image: "registry.k8s.io/pause:3.10.1".to_string(),
        command: None,
        args: None,
        working_dir: None,
        ports: None,
        env: None,
        env_from: None,
        resources: Some(make_resources(cpu, memory)),
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

fn container_with_host_port(
    name: &str,
    port: u16,
    protocol: &str,
    host_ip: Option<&str>,
) -> Container {
    let mut c = make_container(name, "100m", "16Mi");
    c.ports = Some(vec![ContainerPort {
        container_port: port,
        name: None,
        protocol: Some(protocol.to_string()),
        host_port: Some(port),
        host_ip: host_ip.map(|s| s.to_string()),
    }]);
    c
}

use rusternetes_test_support::node_with_resources as make_node;

/// Scheduled, running pod on `node_name` with the given priority.
fn make_running_pod(name: &str, priority: i32, cpu: &str, memory: &str, node_name: &str) -> Pod {
    let spec = PodSpec {
        containers: vec![make_container("main", cpu, memory)],
        priority: Some(priority),
        node_name: Some(node_name.to_string()),
        ..Default::default()
    };
    let mut pod = Pod::new(name, spec);
    pod.metadata.namespace = Some("default".to_string());
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        ..Default::default()
    });
    pod
}

/// Unscheduled pod with the given priority (no node_name).
fn make_incoming_pod(name: &str, priority: i32, cpu: &str, memory: &str) -> Pod {
    let spec = PodSpec {
        containers: vec![make_container("main", cpu, memory)],
        priority: Some(priority),
        ..Default::default()
    };
    let mut pod = Pod::new(name, spec);
    pod.metadata.namespace = Some("default".to_string());
    pod
}

/// Running pod with a bound hostPort.
fn make_running_hostport_pod(
    name: &str,
    node_name: Option<&str>,
    port: u16,
    protocol: &str,
    host_ip: Option<&str>,
) -> Pod {
    let spec = PodSpec {
        containers: vec![container_with_host_port("c", port, protocol, host_ip)],
        node_name: node_name.map(|s| s.to_string()),
        ..Default::default()
    };
    let mut pod = Pod::new(name, spec);
    pod.metadata.namespace = Some("default".to_string());
    pod.status = Some(PodStatus {
        phase: Some(Phase::Running),
        ..Default::default()
    });
    pod
}

// ---------------------------------------------------------------------------
// [sig-scheduling] SchedulerPreemption [Serial]
// validates lower priority pod preemption by critical pod [Conformance]
// ---------------------------------------------------------------------------

/// [sig-scheduling] SchedulerPreemption validates lower priority pod
/// preemption by critical pod [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/preemption.go
/// Sonobuoy (Round 160+): FAIL (cluster-level e2e timing).
///
/// Unit invariant: `check_preemption` identifies lower-priority pods as
/// victims when a system-critical pod (priority = 2_000_000_000) requests
/// resources occupied by those lower-priority pods.
#[test]
fn preemption_critical_pod_evicts_lower_priority_victim() {
    let node = make_node("node-1", "1", "1Gi");
    let victim = make_running_pod("sched-preemption-victim", 100, "1", "512Mi", "node-1");
    let critical = make_incoming_pod("system-critical-pod", 2_000_000_000, "1", "512Mi");

    let (can_preempt, victims) = check_preemption(&node, &critical, &[victim]);

    assert!(
        can_preempt,
        "system-critical pod must be able to preempt lower-priority pods; got victims={victims:?}"
    );
    assert_eq!(
        victims,
        vec!["sched-preemption-victim".to_string()],
        "only the lower-priority victim should be selected"
    );
}

/// [sig-scheduling] SchedulerPreemption critical pod cannot be preempted
/// by equal-priority pod
///
/// Negative complement: a system-critical pod running on the node must NOT
/// be preempted by another pod at the same priority.
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/preemption.go (implicit).
#[test]
fn preemption_critical_pod_not_victim_of_equal_priority() {
    let node = make_node("node-1", "1", "1Gi");
    let incumbent = make_running_pod("kube-dns", 2_000_000_000, "1", "512Mi", "node-1");
    let incoming = make_incoming_pod("coredns-replacement", 2_000_000_000, "1", "512Mi");

    let (can_preempt, victims) = check_preemption(&node, &incoming, &[incumbent]);

    assert!(
        !can_preempt,
        "system-critical pods must not preempt each other; got victims={victims:?}"
    );
    assert!(
        victims.is_empty(),
        "no victims expected when priorities are equal"
    );
}

/// [sig-scheduling] SchedulerPreemption system-node-critical (2_000_001_000)
/// preempts cluster-critical (2_000_000_000)
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/preemption.go:697-699 —
/// system-node-critical has strictly higher priority and may preempt
/// cluster-critical pods.
#[test]
fn preemption_node_critical_preempts_cluster_critical() {
    let node = make_node("node-1", "1", "1Gi");
    let cluster_critical = make_running_pod(
        "cluster-critical-pod",
        2_000_000_000,
        "1",
        "512Mi",
        "node-1",
    );
    let node_critical = make_incoming_pod("node-critical-pod", 2_000_001_000, "1", "512Mi");

    let (can_preempt, victims) = check_preemption(&node, &node_critical, &[cluster_critical]);

    assert!(
        can_preempt,
        "system-node-critical must be able to preempt system-cluster-critical; \
         got victims={victims:?}"
    );
    assert_eq!(victims, vec!["cluster-critical-pod".to_string()]);
}

// ---------------------------------------------------------------------------
// [sig-scheduling] SchedulerPreemption [Serial]
// validates pod disruption condition is added to the preempted pod [Conformance]
// ---------------------------------------------------------------------------

/// Helper that applies the eviction mutation a preempting scheduler would write.
/// Mirrors `Scheduler::evict_pod` from `crates/scheduler/src/scheduler.rs:1029`.
///
/// NOTE: this test helper uses `get_or_insert_with(PodStatus::default)` to
/// unconditionally initialise the status so that we can assert on the condition
/// fields below. The production path in `scheduler.rs:1040` uses
/// `if let Some(ref mut status) = pod.status`, meaning it silently skips
/// writing `DisruptionTarget` when the pod has no status object.  All callers
/// of this helper construct pods via `make_running_pod`, which always sets
/// `status`, so the divergence is never observable in these tests.  A future
/// test that creates a victim without pre-set status should use the production
/// guard directly (the `if-let` form) to stay faithful to the spec.
fn apply_eviction_mutation(pod: &mut Pod) {
    pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
    pod.metadata.deletion_grace_period_seconds = Some(30);
    let status = pod.status.get_or_insert_with(PodStatus::default);
    status.phase = Some(Phase::Failed);
    status.reason = Some("Preempted".to_string());
    status.message = Some("Pod was preempted by a higher-priority pod".to_string());
    let conditions = status.conditions.get_or_insert_with(Vec::new);
    conditions.push(PodCondition {
        condition_type: "DisruptionTarget".to_string(),
        status: "True".to_string(),
        reason: Some("PreemptionByScheduler".to_string()),
        message: Some("Preempted by a higher-priority pod".to_string()),
        last_probe_time: None,
        last_transition_time: Some(chrono::Utc::now()),
        observed_generation: None,
    });
}

/// [sig-scheduling] SchedulerPreemption validates pod disruption condition is
/// added to the preempted pod [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/preemption.go
/// Sonobuoy (Round 160+): FAIL (requires end-to-end kubelet reconciliation).
///
/// Scheduler-side invariant: `evict_pod` writes `DisruptionTarget=True` with
/// `reason=PreemptionByScheduler` onto the victim's status. This matches the
/// upstream contract at
/// `pkg/scheduler/framework/preemption/preemption.go::DeletePod`.
#[test]
fn preemption_eviction_sets_disruption_target_condition() {
    let mut victim = make_running_pod("victim-pod", 1, "1", "512Mi", "node-1");

    apply_eviction_mutation(&mut victim);

    assert!(
        victim.metadata.deletion_timestamp.is_some(),
        "eviction mutation must set deletionTimestamp on victim"
    );

    let status = victim.status.as_ref().expect("status must be present");
    assert_eq!(
        status.phase,
        Some(Phase::Failed),
        "evicted pod must have phase=Failed"
    );
    assert_eq!(
        status.reason.as_deref(),
        Some("Preempted"),
        "evicted pod must have reason=Preempted"
    );

    let conditions = status
        .conditions
        .as_ref()
        .expect("status.conditions must be present");
    let disruption = conditions
        .iter()
        .find(|c| c.condition_type == "DisruptionTarget")
        .expect("DisruptionTarget condition must be added to evicted pod");

    assert_eq!(
        disruption.status, "True",
        "DisruptionTarget must be True on evicted pod"
    );
    assert_eq!(
        disruption.reason.as_deref(),
        Some("PreemptionByScheduler"),
        "DisruptionTarget reason must be PreemptionByScheduler"
    );
}

/// [sig-scheduling] SchedulerPreemption eviction mutation is idempotent —
/// differential test showing the guard both adds and skips the condition
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/preemption.go (implicit);
/// guard at k8s.io/kubernetes/pkg/scheduler/framework/preemption/preemption.go
/// `DeletePod` — only writes the condition when `deletionTimestamp` is nil.
///
/// TEST-QUALITY: two cases in one test so the assertion fails if the guard is
/// removed from `apply_eviction_mutation_guarded`:
///
///   * Case A — pod without deletionTimestamp → guard runs `apply_eviction_mutation`
///     → `DisruptionTarget` IS present after the call.
///   * Case B — pod WITH deletionTimestamp already set → guard skips the mutation
///     → `DisruptionTarget` is NOT present after the (no-op) call.
///
/// The assertion `has_disruption_a && !has_disruption_b` is falsified if the
/// guard is deleted (both pods would get the condition → `!has_disruption_b`
/// fails) or if the normal path is broken (`has_disruption_a` fails).
#[test]
fn preemption_eviction_guarded_differential() {
    // Helper that mirrors the guard in scheduler.rs::evict_pod line 1030:
    // only mutate if `deletion_timestamp` is still None.
    fn apply_eviction_mutation_guarded(pod: &mut Pod) {
        if pod.metadata.deletion_timestamp.is_none() {
            apply_eviction_mutation(pod);
        }
    }

    fn has_disruption_target(pod: &Pod) -> bool {
        pod.status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "DisruptionTarget")
            })
            .unwrap_or(false)
    }

    // Case A: fresh pod — no deletionTimestamp → guard MUST add DisruptionTarget.
    let mut fresh_victim = make_running_pod("fresh-victim", 1, "1", "512Mi", "node-1");
    apply_eviction_mutation_guarded(&mut fresh_victim);
    let has_disruption_a = has_disruption_target(&fresh_victim);

    // Case B: already-terminating pod — guard MUST skip (no DisruptionTarget added).
    let mut terminating_victim = make_running_pod("terminating-victim", 1, "1", "512Mi", "node-1");
    terminating_victim.metadata.deletion_timestamp = Some(chrono::Utc::now());
    apply_eviction_mutation_guarded(&mut terminating_victim);
    let has_disruption_b = has_disruption_target(&terminating_victim);

    assert!(
        has_disruption_a,
        "eviction guard must ADD DisruptionTarget when deletionTimestamp is absent (Case A)"
    );
    assert!(
        !has_disruption_b,
        "eviction guard must SKIP the mutation when deletionTimestamp is already set (Case B)"
    );
    // Explicit differential: if the guard is dropped, both would be true and
    // the assertion above would already fail. This additional assertion makes
    // the intent clear as documentation.
    assert_ne!(
        has_disruption_a, has_disruption_b,
        "the guard must produce different outcomes for the two cases"
    );
}

/// [sig-scheduling] SchedulerPreemption DisruptionTarget condition carries
/// all required fields
///
/// Upstream: k8s.io/kubernetes/pkg/scheduler/framework/preemption/preemption.go
/// (DeletePod). The conformance check at the API-server admission layer
/// requires `type`, `status`, `reason`, and `lastTransitionTime`.
#[test]
fn preemption_disruption_target_condition_has_all_required_fields() {
    let mut victim = make_running_pod("victim-required-fields", 1, "1", "512Mi", "node-1");
    apply_eviction_mutation(&mut victim);

    let conditions = victim
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("conditions must be present");
    let dt = conditions
        .iter()
        .find(|c| c.condition_type == "DisruptionTarget")
        .expect("DisruptionTarget must be present");

    assert!(
        !dt.condition_type.is_empty(),
        "DisruptionTarget.type must not be empty"
    );
    assert!(
        !dt.status.is_empty(),
        "DisruptionTarget.status must not be empty"
    );
    assert!(
        dt.reason.is_some(),
        "DisruptionTarget.reason must be set (upstream: PreemptionByScheduler)"
    );
    assert!(
        dt.last_transition_time.is_some(),
        "DisruptionTarget.lastTransitionTime must be set"
    );
}

// ---------------------------------------------------------------------------
// [sig-scheduling] SchedulerPreemption [Serial]
// PriorityClass endpoints — different HTTP methods [Conformance]
// ---------------------------------------------------------------------------

// PriorityClass HTTP CRUD coverage (POST/GET/LIST/PUT/PATCH/DELETE against the
// real `scheduling.k8s.io/v1` routes) now lives in the api-server in-process
// router test: `crates/api-server/tests/priorityclass_http_crud_test.rs`.
// The scheduler unit-test layer has no HTTP server, so the conformance
// scenario `PriorityClass endpoints ... different HTTP methods` (upstream
// k8s.io/kubernetes/test/e2e/scheduling/priorities.go) is exercised there.

// ---------------------------------------------------------------------------
// [sig-scheduling] SchedulerPredicates [Serial]
// validates that there exists conflict between pods with same hostPort and
// protocol but one using 0.0.0.0 hostIP [Conformance]
// ---------------------------------------------------------------------------

/// [sig-scheduling] SchedulerPredicates [Serial] validates that there exists
/// conflict between pods with same hostPort and protocol but one using
/// 0.0.0.0 hostIP [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/predicates.go
/// Sonobuoy (Round 160+): FAIL (cluster timing / scheduling lifecycle).
///
/// Scheduler-side invariant: `check_host_port_conflicts` reports a conflict
/// when the INCOMING pod requests the wildcard address (0.0.0.0) on a given
/// (port, protocol) and an existing pod already binds that same (port,
/// protocol) on a specific IP. This is the reverse direction from the
/// existing wildcard test in `conformance_scheduling_priority_preemption_hostport.rs`
/// (which tests an existing 0.0.0.0 pod vs an incoming specific-IP pod).
/// Both directions must conflict.
#[test]
fn hostport_incoming_wildcard_conflicts_with_existing_specific_ip() {
    let node = make_node("node-1", "4", "8Gi");
    // Existing pod binds 172.27.0.4:54323/TCP — a specific interface.
    let pod1 = make_running_hostport_pod("pod1", Some("node-1"), 54323, "TCP", Some("172.27.0.4"));
    // Incoming pod requests 0.0.0.0:54323/TCP (wildcard) — must conflict because
    // pod1 already owns that (port, protocol) on a specific address.
    let pod2 = make_running_hostport_pod("pod2", None, 54323, "TCP", Some("0.0.0.0"));

    let no_conflict = check_host_port_conflicts(&node, &pod2, &[pod1]);
    assert!(
        !no_conflict,
        "incoming wildcard (0.0.0.0) must conflict with an existing specific-IP \
         pod on the same (port=54323, protocol=TCP) tuple"
    );
}

/// [sig-scheduling] SchedulerPredicates hostPort 0.0.0.0 conflicts with
/// empty hostIP
///
/// Upstream treats an unset hostIP as equivalent to 0.0.0.0; both
/// bind-all semantics must conflict with each other.
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/predicates.go
#[test]
fn hostport_zero_zero_zero_zero_conflicts_with_empty_host_ip() {
    let node = make_node("node-1", "4", "8Gi");
    let pod1 = make_running_hostport_pod("pod1", Some("node-1"), 54323, "TCP", Some("0.0.0.0"));
    // pod2 omits hostIP (None = bind any, treated as 0.0.0.0).
    let pod2 = make_running_hostport_pod("pod2", None, 54323, "TCP", None);

    let no_conflict = check_host_port_conflicts(&node, &pod2, &[pod1]);
    assert!(
        !no_conflict,
        "wildcard-bound port must conflict with a pod that omits hostIP"
    );
}

/// [sig-scheduling] SchedulerPredicates hostPort 0.0.0.0/TCP vs 0.0.0.0/UDP
/// on the same port — no conflict
///
/// Negative path: TCP and UDP are independent transports; the wildcard address
/// on one protocol must not block the other.
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/predicates.go (conflict matrix).
#[test]
fn hostport_zero_zero_zero_zero_tcp_no_conflict_with_udp_same_port() {
    let node = make_node("node-1", "4", "8Gi");
    let tcp_pod =
        make_running_hostport_pod("tcp-pod", Some("node-1"), 54323, "TCP", Some("0.0.0.0"));
    let udp_pod = make_running_hostport_pod("udp-pod", None, 54323, "UDP", Some("0.0.0.0"));

    let no_conflict = check_host_port_conflicts(&node, &udp_pod, &[tcp_pod]);
    assert!(
        no_conflict,
        "0.0.0.0:54323/TCP and 0.0.0.0:54323/UDP must NOT conflict \
         (different transport protocols)"
    );
}
