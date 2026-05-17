# [sig-scheduling] Predicates + node/pod affinity + taints/tolerations — scoped conformance coverage

Crate: `crates/scheduler` · Test file: `crates/scheduler/tests/conformance_scheduling_predicates_affinity.rs`

Scoped mirror of the upstream Kubernetes v1.35 `[sig-scheduling]`
SchedulerPredicates Ginkgo suite (`test/e2e/scheduling/predicates.go`,
`taints.go`, `priorities.go`).

The scheduler unit exposes filter/score plugins directly, so this file
exercises them with direct calls into `rusternetes_scheduler::plugins` and
`rusternetes_scheduler::advanced`. `FrameworkHandle` accepts plain
`Vec<Pod>`/`Vec<Node>` slices, so no storage round-trip is needed — no HTTP
harness, no docker, no apiserver.

Sonobuoy R160 (2026-05-12) reports exactly two `[sig-scheduling]
SchedulerPredicates [Conformance]` descriptors; see
`.rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log:157`
and `:455`. Failure context for the `resource limits` case is the "Other"
bucket from `docs/CONFORMANCE.md:53` (context-deadline-exceeded at
`predicates.go:1102`). The remaining rows below mirror non-Conformance
predicate/affinity/taint scenarios from the same upstream files; they
exercise the same scheduler code paths and stay green locally because the
plugin logic is exercised in isolation.

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `SchedulerPredicates validates resource limits of pods that are allowed to run` | `predicates.go:333` | FAIL (Other bucket, `predicates.go:1102` deadline exceeded) | `predicates_validates_resource_limits_rejects_oversized_pod` | mirrored, passing (scheduler-level predicate verified directly) |
| `SchedulerPredicates validates that NodeSelector is respected if not matching` | `predicates.go:445` | PASS | `predicates_validates_nodeselector_rejects_unmatched_nodes` | mirrored, passing |
| `NodeAffinity required In operator accepts matching node` | `predicates.go` (NodeAffinity suite) | n/a (subsumed by SchedulerPredicates parent) | `node_affinity_required_in_operator_accepts_matching_node` | mirrored, passing |
| `NodeAffinity required In operator rejects non-matching node` | `predicates.go` (NodeAffinity suite, negative path) | n/a | `node_affinity_required_in_operator_rejects_nonmatching_node` | mirrored, passing |
| `NodeAffinity required Exists operator accepts labelled node` | `predicates.go` (NodeAffinity Exists/DoesNotExist) | n/a | `node_affinity_required_exists_accepts_labelled_node` | mirrored, passing |
| `NodeAffinity preferred raises score on matching node` | `predicates.go` (`preferredDuringSchedulingIgnoredDuringExecution`) | n/a | `node_affinity_preferred_adds_weight_to_score` | mirrored, passing |
| `PodAffinity required matches when target pod is present` | `predicates.go` (inter-pod affinity) | n/a | `pod_affinity_required_matches_when_target_pod_is_present` | mirrored, passing |
| `PodAffinity required rejects when no target pod` | `predicates.go` (inter-pod affinity, negative path) | n/a | `pod_affinity_required_rejects_when_no_target_pod` | mirrored, passing |
| `PodAntiAffinity required rejects conflicting node` | `predicates.go` (anti-affinity) | n/a | `pod_anti_affinity_required_rejects_conflicting_node` | mirrored, passing |
| `PodAffinity preferred contributes positive score` | `predicates.go` (preferred inter-pod affinity) | n/a | `pod_affinity_preferred_returns_positive_score` | mirrored, passing |
| `NoSchedule taint repels pod without matching toleration` | `taints.go` (NoSchedule effect) | n/a | `taint_noschedule_repels_pod_without_toleration` | mirrored, passing |
| `NoSchedule taint accepts pod with matching toleration` | `taints.go` (NoSchedule positive) | n/a | `taint_noschedule_accepts_pod_with_matching_toleration` | mirrored, passing |
| `PreferNoSchedule taint is treated as a soft constraint` | `taints.go` (PreferNoSchedule effect) | n/a | `taint_prefer_no_schedule_does_not_reject_pod` | mirrored, passing |
| `NoExecute taint without toleration filters pod` | `taints.go` (NoExecute admission) | n/a | `taint_noexecute_repels_pod_without_toleration` | mirrored, passing |
| `NoExecute taint accepts pod with Exists toleration` | `taints.go` (NoExecute + Exists toleration) | n/a | `taint_noexecute_accepts_pod_with_exists_toleration` | mirrored, passing |
| `NodeResourcesFit accounts for running pod requests` | `priorities.go` (LeastAllocated/MostAllocated) | n/a | `node_resources_fit_accounts_for_running_pod_requests` | mirrored, passing |

The `resource limits` row is **not** `#[ignore]`d: the scheduler-side
predicate (`NodeResourcesFit` rejects pods whose CPU request exceeds remaining
allocatable) works in isolation. The Sonobuoy failure is in the upstream
cluster-orchestration harness (filler-pod creation deadline) and is tracked
in `docs/CONFORMANCE.md` under the "Other" bucket; mirroring the upstream
context-deadline-exceeded path would require the full apiserver+kubelet
stack and is out of scope for this scheduler-unit mirror.

HostPort predicate coverage is owned by Unit 17
(`docs/conformance/scheduling-priority-preemption-hostport.md`); existing
HostPort cases live in `crates/scheduler/tests/predicates_test.rs`.
