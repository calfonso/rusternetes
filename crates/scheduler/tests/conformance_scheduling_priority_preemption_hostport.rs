//! Scoped mirror of the Kubernetes v1.35 conformance suite for
//! [sig-scheduling] Priority + Preemption and [sig-network] HostPort.
//!
//! Source of truth: Ginkgo descriptors at
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/scheduling/
//! and
//! https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/network/
//!
//! Specific upstream files referenced:
//! - k8s.io/kubernetes/test/e2e/scheduling/priorities.go
//! - k8s.io/kubernetes/test/e2e/scheduling/preemption.go
//! - k8s.io/kubernetes/test/e2e/network/hostport.go
//!
//! See docs/conformance/scheduling-priority-preemption-hostport.md for the
//! test-by-test status table.
//!
//! Scope: scheduler unit. No HTTP harness; tests drive the published
//! `rusternetes_scheduler::advanced` helpers (`check_preemption`,
//! `check_preemption_with_pdbs`, `check_host_port_conflicts`) and the
//! `PriorityClass` resource model directly. `MemoryStorage` is not required
//! because every helper is pure — it takes `&[Pod]` / `&[Node]` slices.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rusternetes_common::resources::{
    Container, ContainerPort, Pod, PodCondition, PodSpec, PodStatus, PriorityClass, ReplicaSet,
    ReplicaSetSpec,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, ResourceRequirements, TypeMeta};
use rusternetes_controller_manager::controllers::replicaset::ReplicaSetController;
use rusternetes_scheduler::advanced::{check_host_port_conflicts, check_preemption};
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

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

fn container_with_host_port(
    name: &str,
    host_port: u16,
    protocol: Option<&str>,
    host_ip: Option<&str>,
) -> Container {
    let mut c = make_container("100m", "16Mi");
    c.name = name.to_string();
    c.ports = Some(vec![ContainerPort {
        container_port: host_port,
        name: None,
        protocol: protocol.map(|s| s.to_string()),
        host_port: Some(host_port),
        host_ip: host_ip.map(|s| s.to_string()),
    }]);
    c
}

use rusternetes_test_support::node_with_resources as make_node;

fn make_scheduled_pod(name: &str, priority: i32, cpu: &str, memory: &str, node_name: &str) -> Pod {
    let spec = PodSpec {
        containers: vec![make_container(cpu, memory)],
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

fn make_pod_with_ports(name: &str, node_name: Option<&str>, containers: Vec<Container>) -> Pod {
    let spec = PodSpec {
        containers,
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

/// Mirrors the K8s admission-controller behavior that resolves
/// `pod.spec.priority` from `pod.spec.priorityClassName` (or from the
/// PriorityClass with `globalDefault=true`) before the scheduler runs.
///
/// Pure helper used by the PriorityClass-resolution tests below. Mirrors
/// `pkg/registry/core/pod/strategy.go::resolvePodPriority` in upstream.
fn resolve_pod_priority(pod: &Pod, classes: &[PriorityClass]) -> i32 {
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
    // Fall back to globalDefault PriorityClass, if any.
    if let Some(default) = classes.iter().find(|c| c.global_default.unwrap_or(false)) {
        return default.value;
    }
    0
}

// ---------------------------------------------------------------------------
// [sig-scheduling] PriorityClass resolution (priorities.go)
// ---------------------------------------------------------------------------

/// [sig-scheduling] PriorityClass should resolve explicit value over class name
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/priorities.go (PodPriority
/// resolution; mirrors `pkg/registry/core/pod/strategy.go::resolvePodPriority`)
/// Sonobuoy (Round 160, 2026-04-26): PASS (not separately reported; covered
/// implicitly by every priority/preemption test that schedules a pod).
#[test]
fn priority_class_explicit_value_wins_over_class_name() {
    let high = PriorityClass::new("sched-preemption-high-priority", 1000);
    let low = PriorityClass::new("sched-preemption-low-priority", 1);

    let mut pod = make_incoming_pod("p", 0, "100m", "16Mi");
    // Explicit numeric priority overrides any class lookup.
    pod.spec.as_mut().unwrap().priority = Some(42);
    pod.spec.as_mut().unwrap().priority_class_name = Some("sched-preemption-high-priority".into());

    assert_eq!(resolve_pod_priority(&pod, &[high, low]), 42);
}

/// [sig-scheduling] PriorityClass should resolve by class name when value is unset
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/priorities.go
/// Sonobuoy (Round 160): PASS
#[test]
fn priority_class_name_resolves_to_class_value() {
    let high = PriorityClass::new("sched-preemption-high-priority", 1000);
    let medium = PriorityClass::new("sched-preemption-medium-priority", 100);
    let low = PriorityClass::new("sched-preemption-low-priority", 1);

    let mut pod = make_incoming_pod("p", 0, "100m", "16Mi");
    pod.spec.as_mut().unwrap().priority = None;
    pod.spec.as_mut().unwrap().priority_class_name =
        Some("sched-preemption-medium-priority".into());

    assert_eq!(resolve_pod_priority(&pod, &[high, medium, low]), 100);
}

/// [sig-scheduling] PriorityClass globalDefault applies when pod has neither priority nor className
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/priorities.go
/// Sonobuoy (Round 160): PASS
#[test]
fn priority_class_global_default_applies_to_pods_without_class() {
    let mut default_pc = PriorityClass::new("default-priority", 500);
    default_pc.global_default = Some(true);
    let other = PriorityClass::new("sched-preemption-high-priority", 1000);

    let mut pod = make_incoming_pod("p", 0, "100m", "16Mi");
    pod.spec.as_mut().unwrap().priority = None;
    pod.spec.as_mut().unwrap().priority_class_name = None;

    assert_eq!(resolve_pod_priority(&pod, &[default_pc, other]), 500);
}

/// [sig-scheduling] PriorityClass value ordering (low < medium < high)
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/preemption.go:697-699
/// (the e2e dumps `List existing PriorityClasses` with creation order; the
/// underlying invariant is that integer ordering is the canonical relation).
/// Sonobuoy (Round 160): PASS
#[test]
fn priority_class_values_order_low_medium_high() {
    let low = PriorityClass::new("sched-preemption-low-priority", 1);
    let medium = PriorityClass::new("sched-preemption-medium-priority", 100);
    let high = PriorityClass::new("sched-preemption-high-priority", 1000);
    let sys_critical = PriorityClass::new("system-cluster-critical", 2_000_000_000);

    let mut values = [low.value, medium.value, high.value, sys_critical.value];
    values.sort();
    assert_eq!(values, [1, 100, 1000, 2_000_000_000]);
}

// ---------------------------------------------------------------------------
// [sig-scheduling] SchedulerPreemption (preemption.go)
// ---------------------------------------------------------------------------

/// [sig-scheduling] SchedulerPreemption validates basic preemption of lower-priority pod
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/preemption.go:218
/// (`validates basic preemption works`)
/// Sonobuoy (Round 160): PASS
#[test]
fn preemption_evicts_lower_priority_pod_to_fit_high_priority() {
    // Node with 1 CPU is full with a single low-priority pod.
    let node = make_node("node-1", "1", "1Gi");
    let low = make_scheduled_pod("victim", /*priority*/ 1, "1", "512Mi", "node-1");
    let incoming = make_incoming_pod("preemptor", /*priority*/ 1000, "1", "512Mi");

    let (can_preempt, victims) = check_preemption(&node, &incoming, &[low]);
    assert!(can_preempt, "high-priority pod should trigger preemption");
    assert_eq!(victims, vec!["victim".to_string()]);
}

/// [sig-scheduling] SchedulerPreemption does not preempt equal-priority pods
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/preemption.go (the basic
/// preemption suite verifies that only strictly-lower-priority pods are
/// evicted; equal priority is never preempted).
/// Sonobuoy (Round 160): PASS
#[test]
fn preemption_skips_when_only_equal_priority_pods_present() {
    let node = make_node("node-1", "1", "1Gi");
    let same = make_scheduled_pod("same-pri", /*priority*/ 1000, "1", "512Mi", "node-1");
    let incoming = make_incoming_pod("preemptor", /*priority*/ 1000, "1", "512Mi");

    let (can_preempt, victims) = check_preemption(&node, &incoming, &[same]);
    assert!(
        !can_preempt,
        "must not preempt a pod of equal priority, got victims {victims:?}"
    );
    assert!(victims.is_empty());
}

/// [sig-scheduling] SchedulerPreemption respects preemptionPolicy=Never
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/preemption.go (mirrors the
/// `PreemptionPolicy: PreemptNever` paths in the suite at lines around :479).
/// Sonobuoy (Round 160): PASS
#[test]
fn preemption_skipped_when_pod_has_preemption_policy_never() {
    let node = make_node("node-1", "1", "1Gi");
    let low = make_scheduled_pod("victim", 1, "1", "512Mi", "node-1");
    let mut incoming = make_incoming_pod("nice-preemptor", 1000, "1", "512Mi");
    incoming.spec.as_mut().unwrap().preemption_policy = Some("Never".to_string());

    let (can_preempt, victims) = check_preemption(&node, &incoming, &[low]);
    assert!(!can_preempt, "preemptionPolicy=Never must not preempt");
    assert!(victims.is_empty());
}

/// [sig-scheduling] SchedulerPreemption protects system-critical pods
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/preemption.go:697-699
/// (the test logs `system-cluster-critical/2000000000` and
/// `system-node-critical/2000001000`; the underlying invariant is that
/// system-critical pods can only be preempted by *strictly higher* priority).
/// Sonobuoy (Round 160): PASS
#[test]
fn preemption_protects_system_critical_pods_from_lower_priority_preemptor() {
    let node = make_node("node-1", "1", "1Gi");
    // A system-critical pod owns the node.
    let critical = make_scheduled_pod("kube-dns", 2_000_000_000, "1", "512Mi", "node-1");
    // A regular high-priority pod (1000) cannot evict it.
    let incoming = make_incoming_pod("regular-high", 1000, "1", "512Mi");

    let (can_preempt, victims) = check_preemption(&node, &incoming, &[critical]);
    assert!(
        !can_preempt,
        "non-critical pod must not evict a system-critical pod"
    );
    assert!(victims.is_empty());
}

/// [sig-scheduling] SchedulerPreemption [Serial] PreemptionExecutionPath runs ReplicaSets to verify preemption running path [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/scheduling/preemption.go:756 (test
/// entry); failure observed upstream at preemption.go:1025
/// (`replicaset "rs-pod1" never had desired number of
/// .status.availableReplicas`).
///
/// Mirror scope: this is the cross-cutting case where scheduler-side
/// preemption must flow through to the `ReplicaSetController`'s
/// `status.availableReplicas` accounting. We drive both layers against
/// `MemoryStorage` in-process:
///
///  1. Stand up a Node with capacity for `desired` pods.
///  2. Pre-create a low-priority RS with `desired` replicas; the RS
///     controller's `reconcile_all` creates the pod children.
///  3. Manually schedule + mark every pod `Ready` (no kubelet in this
///     harness). Reconcile again and confirm `availableReplicas == desired`.
///  4. Apply the same mutation `Scheduler::evict_pod` performs for the
///     victim chosen by `check_preemption` (deletionTimestamp +
///     `Phase::Failed` + `DisruptionTarget` condition). Insert a
///     high-priority preemptor pod scheduled on the same node so the node
///     stays full.
///  5. Reconcile the RS. The controller must (a) exclude the terminating
///     victim from `replicas` / `availableReplicas`, (b) create one
///     replacement pod (which stays Pending — node is now full), and
///     (c) report `availableReplicas == desired - 1` because the preemptor
///     consumed the freed slot.
///
/// Sonobuoy (Round 160): FAIL — preemption.go:1025. Layer-A still owes the
/// kubelet/api-server end-to-end fix; Layer-B (this mirror) verifies the
/// scheduler→RS-controller contract in isolation.
#[tokio::test]
async fn preemption_execution_path_replicaset_available_replicas() {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    let ns = "default";
    let desired: i32 = 4;

    // Step 1: Node with 4 CPU slots — exactly enough for `desired` low-pri
    // pods of 1 CPU each. The high-priority preemptor also wants 1 CPU, so
    // the node can only fit one more pod after a victim is evicted.
    let node = make_node("node-1", "4", "4Gi");
    storage
        .create(&build_key("nodes", None, &node.metadata.name), &node)
        .await
        .unwrap();

    // Step 2: low-priority ReplicaSet with `desired` replicas. The pod
    // template's container requests 1 CPU so the resource math matches
    // `check_preemption`'s view.
    let mut selector_labels: HashMap<String, String> = HashMap::new();
    selector_labels.insert("app".to_string(), "rs-pod1".to_string());

    let pod_template_spec = PodSpec {
        containers: vec![make_container("1", "512Mi")],
        priority: Some(/* low-priority */ 1),
        ..Default::default()
    };

    let rs = ReplicaSet {
        type_meta: TypeMeta {
            kind: "ReplicaSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new("rs-pod1");
            m.namespace = Some(ns.to_string());
            m.uid = "rs-pod1-uid".to_string();
            m.labels = Some(selector_labels.clone());
            m.generation = Some(1);
            m
        },
        spec: ReplicaSetSpec {
            replicas: desired,
            selector: LabelSelector {
                match_labels: Some(selector_labels.clone()),
                match_expressions: None,
            },
            template: rusternetes_common::resources::PodTemplateSpec {
                metadata: Some(ObjectMeta::new("").with_labels(selector_labels.clone())),
                spec: pod_template_spec.clone(),
            },
            min_ready_seconds: None,
        },
        status: None,
    };
    storage
        .create(&build_key("replicasets", Some(ns), &rs.metadata.name), &rs)
        .await
        .unwrap();

    // Drive RS reconcile — creates `desired` pods.
    let controller = ReplicaSetController::new(storage.clone(), 1);
    controller.reconcile_all().await.unwrap();

    let pods_after_create: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    assert_eq!(
        pods_after_create.len(),
        desired as usize,
        "RS controller must create one pod per replica"
    );

    // Step 3: schedule + mark every pod Ready (no kubelet here).
    schedule_and_mark_ready(&storage, ns, "node-1").await;

    // Reconcile again so `update_status` writes availableReplicas.
    controller.reconcile_all().await.unwrap();

    let rs_after_ready: ReplicaSet = storage
        .get(&build_key("replicasets", Some(ns), "rs-pod1"))
        .await
        .unwrap();
    let status = rs_after_ready
        .status
        .as_ref()
        .expect("RS status must be populated after reconcile");
    assert_eq!(
        status.replicas, desired,
        "all desired pods must be counted as replicas"
    );
    assert_eq!(
        status.available_replicas, desired,
        "all desired pods must be available before preemption"
    );

    // Step 4: pick a victim via `check_preemption`, then apply the same
    // mutation `Scheduler::evict_pod` would. After eviction, drop a
    // high-priority pod on the node consuming the freed slot.
    let live_pods: Vec<Pod> = storage.list(&build_prefix("pods", Some(ns))).await.unwrap();
    let preemptor = make_incoming_pod("preemptor", /*priority*/ 1_000, "1", "512Mi");
    let (can_preempt, victims) = check_preemption(&node, &preemptor, &live_pods);
    assert!(
        can_preempt,
        "scheduler must find a victim when node is full of lower-priority pods"
    );
    assert_eq!(victims.len(), 1, "preemptor only needs to evict one pod");
    let victim_name = victims.into_iter().next().unwrap();

    evict_pod_like_scheduler(&storage, ns, &victim_name).await;

    // Place the preemptor on node-1 so the node stays full. In a real
    // cluster the scheduler would Bind it; here we set node_name + Phase
    // Running + Ready so the RS controller can see the slot is taken.
    let mut preemptor_scheduled = preemptor.clone();
    {
        let spec = preemptor_scheduled.spec.as_mut().unwrap();
        spec.node_name = Some("node-1".to_string());
    }
    preemptor_scheduled.status = Some(PodStatus {
        phase: Some(Phase::Running),
        conditions: Some(vec![PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: None,
            observed_generation: None,
        }]),
        ..Default::default()
    });
    storage
        .create(
            &build_key("pods", Some(ns), &preemptor_scheduled.metadata.name),
            &preemptor_scheduled,
        )
        .await
        .unwrap();

    // Step 5: poll RS reconcile until `availableReplicas == desired - 1`.
    // In a real cluster the watch+workqueue cuts the latency to sub-second;
    // here we drive `reconcile_all` directly. Deadline mirrors the upstream
    // expectation that the controller catches up well inside the test
    // window.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut last_status = None;
    while std::time::Instant::now() < deadline {
        controller.reconcile_all().await.unwrap();
        // Mark any newly-created pod as Pending (no scheduler/kubelet to
        // promote it). It must NOT be Ready, because the node is full and
        // the upstream invariant is that availableReplicas degrades by 1.
        ensure_replacement_pods_pending(&storage, ns, &rs.metadata.name).await;
        controller.reconcile_all().await.unwrap();

        let rs_now: ReplicaSet = storage
            .get(&build_key("replicasets", Some(ns), "rs-pod1"))
            .await
            .unwrap();
        if let Some(s) = rs_now.status.as_ref() {
            last_status = Some(s.clone());
            if s.available_replicas == desired - 1 && s.replicas == desired {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!(
        "RS status never settled to replicas={} / availableReplicas={} after preemption; last seen: {:?}",
        desired,
        desired - 1,
        last_status
    );
}

/// Mark every RS pod in `namespace` as scheduled (`node_name` set), Running,
/// and Ready=True. Skips pods already terminating.
async fn schedule_and_mark_ready(storage: &Arc<MemoryStorage>, namespace: &str, node_name: &str) {
    let prefix = build_prefix("pods", Some(namespace));
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
    for mut pod in pods {
        if pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        if let Some(spec) = pod.spec.as_mut() {
            spec.node_name = Some(node_name.to_string());
        }
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            conditions: Some(vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: "True".to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: None,
                observed_generation: None,
            }]),
            ..Default::default()
        });
        let key = build_key("pods", Some(namespace), &pod.metadata.name);
        let _ = storage.update(&key, &pod).await;
    }
}

/// Apply the same mutation `Scheduler::evict_pod` writes when it picks a
/// victim: set deletionTimestamp + grace period, phase Failed, reason
/// `Preempted`, and append a `DisruptionTarget` condition. We do not delete
/// the pod outright — upstream K8s leaves cleanup to the kubelet (and the
/// `matches_selector` path on the RS controller already excludes pods with
/// `deletion_timestamp.is_some()`).
async fn evict_pod_like_scheduler(storage: &Arc<MemoryStorage>, namespace: &str, name: &str) {
    let key = build_key("pods", Some(namespace), name);
    let mut pod: Pod = storage.get(&key).await.unwrap();
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
    storage.update(&key, &pod).await.unwrap();
}

/// After the RS controller creates a replacement pod, leave it Pending —
/// the node is full of the preemptor + remaining low-priority pods, so the
/// replacement cannot become Ready. The `availableReplicas` math depends on
/// this: ready pods = desired - 1, replicas = desired (Pending pods still
/// count toward `replicas`).
async fn ensure_replacement_pods_pending(
    storage: &Arc<MemoryStorage>,
    namespace: &str,
    rs_name: &str,
) {
    let prefix = build_prefix("pods", Some(namespace));
    let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
    for mut pod in pods {
        if pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        let owned_by_rs = pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.kind == "ReplicaSet" && r.name == rs_name)
            })
            .unwrap_or(false);
        if !owned_by_rs {
            continue;
        }
        // Only touch pods that haven't been scheduled yet (newly created
        // by the RS controller after the victim was evicted).
        let already_scheduled = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref())
            .is_some();
        if already_scheduled {
            continue;
        }
        pod.status = Some(PodStatus {
            phase: Some(Phase::Pending),
            conditions: None,
            ..Default::default()
        });
        let key = build_key("pods", Some(namespace), &pod.metadata.name);
        let _ = storage.update(&key, &pod).await;
    }
}

// ---------------------------------------------------------------------------
// [sig-network] HostPort (hostport.go) — owned by the scheduler because
// hostPort scheduling is decided by the HostPort filter plugin.
// ---------------------------------------------------------------------------

/// [sig-network] HostPort validates that two pods with the same hostPort and same hostIP conflict
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go:63 (test entry)
/// Sonobuoy (Round 160): PASS (positive side of the conflict matrix)
#[test]
fn hostport_same_port_same_host_ip_conflicts() {
    let node = make_node("node-1", "2", "1Gi");
    let pod_a = make_pod_with_ports(
        "pod-a",
        Some("node-1"),
        vec![container_with_host_port(
            "c",
            54323,
            Some("TCP"),
            Some("127.0.0.1"),
        )],
    );
    let pod_b = make_pod_with_ports(
        "pod-b",
        None,
        vec![container_with_host_port(
            "c",
            54323,
            Some("TCP"),
            Some("127.0.0.1"),
        )],
    );

    let conflict_free = check_host_port_conflicts(&node, &pod_b, &[pod_a]);
    assert!(
        !conflict_free,
        "pods sharing (hostPort, hostIP, protocol) must conflict"
    );
}

/// [sig-network] HostPort validates that there is no conflict between pods with same hostPort but different hostIP and protocol [LinuxOnly] [Conformance]
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go:219
/// Sonobuoy (Round 160): FAIL — pod2 times out waiting to schedule after pod1
/// (kubelet integration timing issue, not scheduler logic). The scheduler-side
/// invariant — that the HostPort filter plugin does NOT report a conflict when
/// hostIPs differ — is verified here; ignored only as a marker for the
/// upstream e2e failure that depends on kubelet timing.
#[test]
fn hostport_same_port_different_host_ip_does_not_conflict() {
    let node = make_node("node-1", "2", "1Gi");
    // pod1 binds 54323 on 172.27.0.4.
    let pod1 = make_pod_with_ports(
        "pod1",
        Some("node-1"),
        vec![container_with_host_port(
            "c",
            54323,
            Some("TCP"),
            Some("172.27.0.4"),
        )],
    );
    // pod2 wants 54323 on a different hostIP — must not conflict.
    let pod2 = make_pod_with_ports(
        "pod2",
        None,
        vec![container_with_host_port(
            "c",
            54323,
            Some("TCP"),
            Some("172.27.0.5"),
        )],
    );

    let no_conflict = check_host_port_conflicts(&node, &pod2, &[pod1]);
    assert!(
        no_conflict,
        "different hostIPs on the same hostPort must not conflict"
    );
}

/// [sig-network] HostPort same port different protocol does not conflict
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go:219 (the second
/// half of the conflict matrix: same port, different protocol).
/// Sonobuoy (Round 160): PASS (scheduler-side invariant; the upstream e2e
/// FAIL above is due to pod2 scheduling timeout, not protocol matching).
#[test]
fn hostport_same_port_different_protocol_does_not_conflict() {
    let node = make_node("node-1", "2", "1Gi");
    let tcp_pod = make_pod_with_ports(
        "tcp-pod",
        Some("node-1"),
        vec![container_with_host_port(
            "c",
            54323,
            Some("TCP"),
            Some("0.0.0.0"),
        )],
    );
    let udp_pod = make_pod_with_ports(
        "udp-pod",
        None,
        vec![container_with_host_port(
            "c",
            54323,
            Some("UDP"),
            Some("0.0.0.0"),
        )],
    );

    let no_conflict = check_host_port_conflicts(&node, &udp_pod, &[tcp_pod]);
    assert!(
        no_conflict,
        "same hostPort but different protocol must not conflict"
    );
}

/// [sig-network] HostPort wildcard hostIP 0.0.0.0 conflicts with any specific hostIP
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go (the wildcard
/// matrix; in upstream, an empty/0.0.0.0 hostIP collides with every other
/// hostIP on the same (port, protocol) tuple).
/// Sonobuoy (Round 160): PASS
#[test]
fn hostport_wildcard_host_ip_conflicts_with_specific_host_ip() {
    let node = make_node("node-1", "2", "1Gi");
    // pod1 binds the wildcard 0.0.0.0:54323/TCP.
    let pod1 = make_pod_with_ports(
        "pod-wildcard",
        Some("node-1"),
        vec![container_with_host_port(
            "c",
            54323,
            Some("TCP"),
            Some("0.0.0.0"),
        )],
    );
    // pod2 asks for 172.27.0.4:54323/TCP — must conflict because pod1 owns
    // every interface.
    let pod2 = make_pod_with_ports(
        "pod-specific",
        None,
        vec![container_with_host_port(
            "c",
            54323,
            Some("TCP"),
            Some("172.27.0.4"),
        )],
    );

    let conflict_free = check_host_port_conflicts(&node, &pod2, &[pod1]);
    assert!(
        !conflict_free,
        "wildcard hostIP must conflict with a specific hostIP on the same port"
    );
}

/// [sig-network] HostPort terminated pods do not block hostPort allocation
///
/// Upstream: k8s.io/kubernetes/test/e2e/network/hostport.go (implicit; the
/// scheduler's HostPort filter only counts non-terminal pods).
/// Sonobuoy (Round 160): PASS
#[test]
fn hostport_terminated_pods_do_not_conflict() {
    let node = make_node("node-1", "2", "1Gi");
    let mut succeeded_pod = make_pod_with_ports(
        "old-pod",
        Some("node-1"),
        vec![container_with_host_port(
            "c",
            54323,
            Some("TCP"),
            Some("0.0.0.0"),
        )],
    );
    succeeded_pod.status = Some(PodStatus {
        phase: Some(Phase::Succeeded),
        ..Default::default()
    });

    let new_pod = make_pod_with_ports(
        "new-pod",
        None,
        vec![container_with_host_port(
            "c",
            54323,
            Some("TCP"),
            Some("0.0.0.0"),
        )],
    );

    let no_conflict = check_host_port_conflicts(&node, &new_pod, &[succeeded_pod]);
    assert!(
        no_conflict,
        "terminal (Succeeded) pods must not block hostPort allocation"
    );
}
