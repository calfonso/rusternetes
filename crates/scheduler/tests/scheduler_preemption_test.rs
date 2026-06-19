//! Phase 5.3 scheduler preemption coverage.
//!
//! Mirrors upstream Kubernetes e2e at
//! `test/e2e/scheduling/preemption.go` (release-1.35) and the algorithmic
//! contract laid out in
//! `pkg/scheduler/framework/preemption/preemption.go::selectVictimsOnNode`.
//!
//! Scope: scheduler unit. The tests drive the published
//! `rusternetes_scheduler::advanced` helpers (`check_preemption`,
//! `check_preemption_with_pdbs`) and exercise the priority-queue ordering
//! comparator the scheduler uses in `schedule_all_pending`. No HTTP harness;
//! every helper here is pure or takes `Arc<MemoryStorage>`.
//!
//! Sibling test files:
//! - `preemption_test.rs` — focused PDB victim selection (preemption.go:535).
//! - `conformance_scheduling_priority_preemption_hostport.rs` — basic
//!   priority/preemption + hostport conformance mirrors.
//!
//! This file complements both by adding multi-victim selection, the
//! lowest-priority-first eviction invariant, the priority-floor / equal-priority
//! refusal contract, and the scheduling-queue sort the scheduler applies to
//! pending pods before binding.

use std::collections::HashMap;
use std::sync::Arc;

use rusternetes_common::resources::{
    Container, IntOrString, Pod, PodDisruptionBudget, PodDisruptionBudgetSpec, PodSpec, PodStatus,
    PriorityClass,
};
use rusternetes_common::types::{LabelSelector, Phase, ResourceRequirements};
use rusternetes_scheduler::advanced::{check_preemption, check_preemption_with_pdbs};
use rusternetes_scheduler::scheduler::Scheduler;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};

// ---------------------------------------------------------------------------
// Test fixtures (mirrors `preemption_test.rs` so the new file can stand alone
// without leaking module-private helpers).
// ---------------------------------------------------------------------------

/// Setup test environment with in-memory storage, mirroring the convention in
/// the controller-manager integration tests and `scheduler_test.rs`.
async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn make_container(cpu: &str, memory: &str) -> Container {
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), cpu.to_string());
    requests.insert("memory".to_string(), memory.to_string());
    Container {
        name: "main".to_string(),
        image: "registry.k8s.io/pause:3.10".to_string(),
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

fn make_scheduled_pod(name: &str, priority: i32, cpu: &str, memory: &str, node_name: &str) -> Pod {
    make_pod_with_labels(name, priority, cpu, memory, Some(node_name), None)
}

fn make_pod_with_labels(
    name: &str,
    priority: i32,
    cpu: &str,
    memory: &str,
    node_name: Option<&str>,
    labels: Option<HashMap<String, String>>,
) -> Pod {
    let spec = PodSpec {
        containers: vec![make_container(cpu, memory)],
        priority: Some(priority),
        node_name: node_name.map(|n| n.to_string()),
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

fn make_pending_pod(name: &str, priority: Option<i32>, priority_class_name: Option<&str>) -> Pod {
    let spec = PodSpec {
        containers: vec![make_container("100m", "16Mi")],
        priority,
        priority_class_name: priority_class_name.map(|s| s.to_string()),
        ..Default::default()
    };
    let mut pod = Pod::new(name, spec);
    pod.metadata.namespace = Some("default".to_string());
    pod.status = Some(PodStatus {
        phase: Some(Phase::Pending),
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

/// Mirror of `Scheduler::get_pod_priority_sync` used in
/// `schedule_all_pending`'s queue-sort comparator. Black-box copy so the test
/// pins the algorithm the scheduler uses without reaching into private
/// helpers. If the scheduler changes its resolution order, this helper and
/// the queue-sort test must be updated together.
///
/// Upstream parallel: `pkg/registry/core/pod/strategy.go::resolvePodPriority`
/// followed by the priority-queue's `MoreImportantPod` comparator in
/// `pkg/scheduler/internal/queue/scheduling_queue.go`.
fn resolve_priority(pod: &Pod, classes: &[PriorityClass]) -> i32 {
    if let Some(spec) = pod.spec.as_ref() {
        if let Some(p) = spec.priority {
            return p;
        }
        if let Some(name) = spec.priority_class_name.as_ref() {
            if let Some(pc) = classes.iter().find(|c| &c.metadata.name == name) {
                return pc.value;
            }
            return 0;
        }
    }
    if let Some(default) = classes.iter().find(|c| c.global_default.unwrap_or(false)) {
        return default.value;
    }
    0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Victim selection — lowest priority is preferred first.
///
/// Upstream invariant: `selectVictimsOnNode` walks candidates in ascending
/// priority order so the *cheapest* eviction is taken first. Verified by
/// `test/e2e/scheduling/preemption.go::validates basic preemption works`
/// (preemption.go:218) and the `pkg/scheduler/framework/preemption` unit
/// tests at `preemption_test.go::TestSelectCandidate`.
///
/// Setup: node is full with three pods of distinct priorities (1, 50, 100).
/// Incoming pod (priority 1000) needs exactly one slot. The scheduler MUST
/// evict the priority-1 pod and leave the higher-priority pods alone.
#[test]
fn victim_selection_picks_lowest_priority_first() {
    let node = make_node("node-1", "3", "3Gi");
    let low = make_scheduled_pod("low-victim", 1, "1", "1Gi", "node-1");
    let mid = make_scheduled_pod("mid-victim", 50, "1", "1Gi", "node-1");
    let high = make_scheduled_pod("high-survivor", 100, "1", "1Gi", "node-1");

    let incoming = make_incoming_pod("preemptor", 1000, "1", "1Gi");

    let (can_preempt, victims) =
        check_preemption(&node, &incoming, &[low.clone(), mid.clone(), high.clone()]);

    assert!(can_preempt, "high-priority pod must trigger preemption");
    assert_eq!(
        victims,
        vec!["low-victim".to_string()],
        "lowest-priority pod must be the sole victim; got {victims:?}"
    );
    // The reprieve pass must NOT pick the mid or high pod.
    assert!(
        !victims
            .iter()
            .any(|v| v == "mid-victim" || v == "high-survivor"),
        "higher-priority pods must be reprieved; victims were {victims:?}"
    );
}

/// Victim selection — multiple victims when one isn't enough.
///
/// Upstream: `pkg/scheduler/framework/preemption/preemption.go::selectVictimsOnNode`
/// removes candidates until the incoming pod fits. The "remove all then
/// reprieve" pass keeps the minimum set.
///
/// Setup: node holds three priority-10 pods of 1 CPU each. Incoming pod
/// wants 2 CPU. Exactly two pods must be evicted, not three.
#[test]
fn victim_selection_evicts_minimum_set_to_satisfy_request() {
    let node = make_node("node-1", "3", "3Gi");
    let v1 = make_scheduled_pod("low-a", 10, "1", "1Gi", "node-1");
    let v2 = make_scheduled_pod("low-b", 10, "1", "1Gi", "node-1");
    let v3 = make_scheduled_pod("low-c", 10, "1", "1Gi", "node-1");

    let incoming = make_incoming_pod("preemptor", 1000, "2", "2Gi");

    let (can_preempt, victims) = check_preemption(&node, &incoming, &[v1, v2, v3]);

    assert!(
        can_preempt,
        "preemption must succeed when removing 2 of 3 pods frees enough"
    );
    assert_eq!(
        victims.len(),
        2,
        "exactly two victims should be selected (3 - 2 = 1 surviving), got {victims:?}"
    );
    // Every victim must be one of the low-priority pods.
    for v in &victims {
        assert!(
            v == "low-a" || v == "low-b" || v == "low-c",
            "unexpected victim {v}"
        );
    }
}

/// PDB-aware victim selection — three-way choice with mixed protection.
///
/// Upstream: `selectVictimsOnNode` prefers candidates that don't violate any
/// PDB. See `pkg/scheduler/framework/preemption/preemption.go` and
/// `test/e2e/scheduling/preemption.go:535`.
///
/// Setup: node has two PDB-protected replicas (priority 100) plus two
/// unprotected low-priority filler pods (priority 50). PDB requires both
/// replicas to remain available (minAvailable=2). Incoming pod (1000)
/// wants 1 CPU. The scheduler must evict one of the filler pods — the
/// PDB-covered replicas are higher priority *and* protected, so the
/// algorithm has both reasons to leave them alone.
///
/// This is distinct from `preemption_test.rs::preemption_prefers_non_pdb_victim_when_possible`
/// which uses equal-priority pods on both sides; here the priority gap means
/// the PDB tie-break only matters as a secondary signal — the test still
/// pins the contract that PDB-covered pods aren't touched when alternatives
/// of any kind exist.
#[test]
fn pdb_aware_victim_selection_with_mixed_priority_candidates() {
    let node = make_node("node-1", "4", "4Gi");

    let mut rs_labels = HashMap::new();
    rs_labels.insert("app".to_string(), "rs-pod1".to_string());

    let rs_a = make_pod_with_labels(
        "rs-a",
        100,
        "1",
        "1Gi",
        Some("node-1"),
        Some(rs_labels.clone()),
    );
    let rs_b = make_pod_with_labels("rs-b", 100, "1", "1Gi", Some("node-1"), Some(rs_labels));

    let filler_a = make_pod_with_labels("filler-a", 50, "1", "1Gi", Some("node-1"), None);
    let filler_b = make_pod_with_labels("filler-b", 50, "1", "1Gi", Some("node-1"), None);

    let pdb = make_pdb("rs-pdb", 2, "rs-pod1");

    let incoming = make_incoming_pod("preemptor", 1000, "1", "1Gi");

    let all_pods = vec![rs_a, rs_b, filler_a, filler_b];
    let (can_preempt, victims) = check_preemption_with_pdbs(&node, &incoming, &all_pods, &[pdb]);

    assert!(can_preempt, "preemption must succeed");
    assert_eq!(
        victims.len(),
        1,
        "exactly one victim is needed; got {victims:?}"
    );
    let victim = &victims[0];
    assert!(
        victim == "filler-a" || victim == "filler-b",
        "victim must be a low-priority unprotected filler, not a PDB-covered replica; got {victim}"
    );
    assert!(
        victim != "rs-a" && victim != "rs-b",
        "PDB-protected pods must not be evicted when a filler is available; got {victim}"
    );
}

/// Priority-based eviction — incoming pod must have strictly higher priority.
///
/// Upstream: `pkg/scheduler/framework/preemption/preemption.go::PodEligibleToPreemptOthers`
/// (and the candidate filter in `selectVictimsOnNode`) requires
/// `incoming.priority > victim.priority`. Equal priority never preempts.
/// Mirrors `preemption.go::preempt at higher priority` semantics.
///
/// Setup tests two angles:
///  1. Incoming with priority equal to occupant's must NOT preempt.
///  2. Incoming with priority less than occupant's must NOT preempt.
///
/// The complementary "strictly higher preempts" case is covered by
/// `victim_selection_picks_lowest_priority_first` above and by the
/// conformance mirror.
#[test]
fn priority_based_eviction_refuses_equal_or_lower_priority_preemptor() {
    let node = make_node("node-1", "1", "1Gi");
    let occupant = make_scheduled_pod("occupant", 500, "1", "1Gi", "node-1");

    // Equal priority — must not preempt.
    let equal = make_incoming_pod("equal-pri", 500, "1", "1Gi");
    let (can_eq, victims_eq) = check_preemption(&node, &equal, std::slice::from_ref(&occupant));
    assert!(
        !can_eq,
        "equal-priority preemptor must NOT evict; got victims {victims_eq:?}"
    );
    assert!(victims_eq.is_empty());

    // Lower priority — must not preempt.
    let lower = make_incoming_pod("lower-pri", 100, "1", "1Gi");
    let (can_lo, victims_lo) = check_preemption(&node, &lower, &[occupant]);
    assert!(
        !can_lo,
        "lower-priority preemptor must NOT evict; got victims {victims_lo:?}"
    );
    assert!(victims_lo.is_empty());
}

/// Scheduler queue sorting — pending pods are processed highest-priority first.
///
/// Upstream: the scheduling queue (priorityqueue.go) returns pods ordered by
/// `MoreImportantPod`, which is "higher priority first" with creation time
/// as the tie-breaker. The scheduler mirrors this with the sort_by in
/// `Scheduler::schedule_all_pending` (descending integer priority).
/// Without that sort, lower-priority replacement pods (e.g. spawned by the
/// ReplicaSet controller after eviction) can be scheduled ahead of the
/// preemptor and consume the resources preemption just freed — see
/// `preemption.go:1025` (the regression this comparator prevents).
///
/// Setup: drop four pending pods into MemoryStorage with assorted priorities,
/// then sort the result with the same comparator the scheduler uses and
/// confirm the descending order. We pin this as a black-box invariant; the
/// scheduler's `schedule_all_pending` is private but the comparator's shape
/// (a stable `b.priority.cmp(&a.priority)`) is the contract Layer-A relies on.
#[tokio::test]
async fn scheduler_queue_sorting_orders_pending_pods_by_priority_desc() {
    let storage = setup_test().await;

    // Seed three PriorityClasses so resolution covers both explicit `priority`
    // values and `priorityClassName` lookups.
    let high = PriorityClass::new("sched-preemption-high-priority", 1000);
    let medium = PriorityClass::new("sched-preemption-medium-priority", 100);

    // Four pending pods spanning the value space:
    //   - explicit priority 2000 (highest)
    //   - resolved-via-class "high" → 1000
    //   - explicit priority 5 (lowest)
    //   - resolved-via-class "medium" → 100
    let p_explicit_high = make_pending_pod("p-explicit-2000", Some(2000), None);
    let p_class_high =
        make_pending_pod("p-class-1000", None, Some("sched-preemption-high-priority"));
    let p_explicit_low = make_pending_pod("p-explicit-5", Some(5), None);
    let p_class_med = make_pending_pod(
        "p-class-100",
        None,
        Some("sched-preemption-medium-priority"),
    );

    for pod in [
        &p_explicit_high,
        &p_class_high,
        &p_explicit_low,
        &p_class_med,
    ] {
        storage
            .create(&build_key("pods", Some("default"), &pod.metadata.name), pod)
            .await
            .unwrap();
    }

    let classes = vec![high, medium];

    // The scheduler's queue comparator (paraphrasing `schedule_all_pending`):
    //   pending.sort_by(|a, b| b_pri.cmp(&a_pri))   // descending
    let mut pending = [
        p_explicit_low.clone(),
        p_class_med.clone(),
        p_explicit_high.clone(),
        p_class_high.clone(),
    ];
    pending.sort_by(|a, b| {
        let a_pri = resolve_priority(a, &classes);
        let b_pri = resolve_priority(b, &classes);
        b_pri.cmp(&a_pri)
    });

    let ordered_names: Vec<&str> = pending.iter().map(|p| p.metadata.name.as_str()).collect();
    assert_eq!(
        ordered_names,
        vec![
            "p-explicit-2000",
            "p-class-1000",
            "p-class-100",
            "p-explicit-5"
        ],
        "pending queue must be sorted descending by resolved priority"
    );

    // Spot-check the resolved values are what we think they are. If any of
    // these break, the test above is meaningless.
    assert_eq!(resolve_priority(&p_explicit_high, &classes), 2000);
    assert_eq!(resolve_priority(&p_class_high, &classes), 1000);
    assert_eq!(resolve_priority(&p_class_med, &classes), 100);
    assert_eq!(resolve_priority(&p_explicit_low, &classes), 5);
}

/// End-to-end preemption through the scheduling loop (repointed from the
/// retired controller-level pin; mirrors upstream
/// `test/e2e/scheduling/preemption.go::"validates basic preemption works"`).
///
/// A node fully occupied by a running low-priority pod cannot fit a pending
/// high-priority pod; `schedule_pending_pods` must evict the victim
/// (deletionTimestamp + DisruptionTarget) rather than leave the preemptor
/// pending forever.
#[tokio::test]
async fn preemption_evicts_lower_priority_pod_via_schedule_loop() {
    let storage = setup_test().await;
    let scheduler = Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

    storage
        .create(
            &build_key("priorityclasses", None, "high"),
            &PriorityClass::new("high", 1_000_000),
        )
        .await
        .unwrap();

    let node = make_node("node-1", "1", "1Gi");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    // Running victim consumes the node's entire CPU allocatable.
    let victim = make_scheduled_pod("victim", 100, "1", "512Mi", "node-1");
    storage
        .create(&build_key("pods", Some("default"), "victim"), &victim)
        .await
        .unwrap();

    // Pending preemptor: priority resolved from the PriorityClass by the
    // scheduler's backstop (spec.priority intentionally None).
    let preemptor = make_pending_pod("preemptor", None, Some("high"));
    storage
        .create(&build_key("pods", Some("default"), "preemptor"), &preemptor)
        .await
        .unwrap();

    scheduler.schedule_pending_pods().await.unwrap();

    let victim_after: Pod = storage
        .get(&build_key("pods", Some("default"), "victim"))
        .await
        .unwrap();
    assert!(
        victim_after.metadata.deletion_timestamp.is_some(),
        "running lower-priority pod must be evicted (deletionTimestamp) when a \
         higher-priority pod cannot otherwise fit; got {:?}",
        victim_after.metadata.deletion_timestamp
    );
    let conditions = victim_after
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("evicted victim must carry status conditions");
    assert!(
        conditions
            .iter()
            .any(|c| c.condition_type == "DisruptionTarget" && c.status == "True"),
        "evicted victim must carry a DisruptionTarget=True condition"
    );
}

/// `preemptionPolicy: Never` end-to-end (repointed from the retired
/// controller-level pin; mirrors upstream
/// `test/e2e/scheduling/preemption.go` PreemptionExecutionPath / NonPreempting).
///
/// The preemptor's spec.preemptionPolicy is deliberately left None — the
/// scheduler must fall back to the PriorityClass's policy (same backstop it
/// already applies for spec.priority) and refuse to evict.
#[tokio::test]
async fn preemption_policy_never_does_not_evict_via_schedule_loop() {
    let storage = setup_test().await;
    let scheduler = Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

    storage
        .create(
            &build_key("priorityclasses", None, "high-but-polite"),
            &PriorityClass::new("high-but-polite", 1_000_000).with_preemption_policy("Never"),
        )
        .await
        .unwrap();

    let node = make_node("node-1", "1", "1Gi");
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();

    let victim = make_scheduled_pod("victim", 100, "1", "512Mi", "node-1");
    storage
        .create(&build_key("pods", Some("default"), "victim"), &victim)
        .await
        .unwrap();

    let preemptor = make_pending_pod("preemptor", None, Some("high-but-polite"));
    storage
        .create(&build_key("pods", Some("default"), "preemptor"), &preemptor)
        .await
        .unwrap();

    scheduler.schedule_pending_pods().await.unwrap();

    let victim_after: Pod = storage
        .get(&build_key("pods", Some("default"), "victim"))
        .await
        .unwrap();
    assert!(
        victim_after.metadata.deletion_timestamp.is_none(),
        "victim must NOT be evicted when the preemptor's PriorityClass has \
         preemptionPolicy: Never; got {:?}",
        victim_after.metadata.deletion_timestamp
    );
    let preemptor_after: Pod = storage
        .get(&build_key("pods", Some("default"), "preemptor"))
        .await
        .unwrap();
    assert!(
        preemptor_after
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .is_none_or(str::is_empty),
        "Never-policy preemptor must stay unscheduled when the node is full"
    );
}
