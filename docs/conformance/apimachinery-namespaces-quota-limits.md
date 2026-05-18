# [sig-api-machinery] Namespaces + ResourceQuota + LimitRange — scoped conformance coverage

Crate: `crates/api-server` · Test file: `tests/conformance_apimachinery_namespaces_quota_limits.rs`

This unit mirrors the Kubernetes v1.35 conformance scenarios that exercise
namespace lifecycle (create / list / patch / delete / finalize / phase
transitions), ResourceQuota usage tracking (incl. usage recompute on
tracked-object delete), and LimitRange enforcement (defaults injection +
min/max constraint validation). The goal is a sub-second `cargo test`
signal that complements the hour-long Sonobuoy run captured in
`.rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log`.

Tests in this file drive the actual axum router (via `tower::ServiceExt::oneshot`)
backed by `MemoryStorage` + `AlwaysAllowAuthorizer`, so every assertion is
exercised through the same handler stack that production HTTPS traffic hits.
The ResourceQuota recompute tests additionally drive the
`ResourceQuotaController::reconcile_one()` entry point added by PR #45 so
the failure mode the upstream test catches is locked in regardless of the
admission path.

Cross-reference: `docs/CONFORMANCE.md` failure bucket
**"Other (~8)"** explicitly calls out *"ResourceQuota pod lifecycle"*
(Round 160). The single Sonobuoy descriptor that fails in this slice is

```
[sig-api-machinery] ResourceQuota
  should create a ResourceQuota and capture the life of a pod. [Conformance]
```

with the failure recorded at upstream `resource_quota.go:312`
(`Ensuring a pod cannot update its resource requirements` →
`Expected an error to have occurred. Got: nil`). The fix landed in two
stages: PR #45 (`748241cf`) wired the `reconcile_one()` entry point +
pod-watch fanout for the **recompute-on-delete** sub-symptom, and the
follow-up `fix(api-server): run ResourceQuota admission on pod
UPDATE/PATCH` plumbed the **request-immutability admission check** into
the Pod UPDATE and PATCH handlers with delta-usage semantics (new
request − old request, fail if cumulative exceeds `.spec.hard`). With
both pieces in place the upstream `:312` assertion is now satisfied
end-to-end and `resource_quota_captures_full_pod_lifecycle` no longer
needs an `#[ignore]`.

## Coverage matrix

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `Namespaces should ensure that all pods are removed when a namespace is deleted` | namespace.go:75 (family) | PASS | `namespace_delete_marks_terminating_and_keeps_finalizer` | mirrored, passing |
| `Namespaces should patch a Namespace` | namespace.go:262 | PASS | `namespace_patch_updates_labels` | mirrored, passing |
| `Namespaces should be created and listed` | namespace.go:188 | PASS | `namespace_create_then_list_contains_it` | mirrored, passing |
| `Namespaces should auto-provision the default ServiceAccount` | framework `WaitForServiceAccountInNamespace` | PASS | `namespace_create_auto_provisions_default_service_account` | mirrored, passing |
| `Namespaces /finalize subresource clears finalizers` | namespace.go (finalize family) | PASS | `namespace_finalize_subresource_removes_finalizer` | mirrored, passing |
| `Namespaces GET unknown returns 404 NotFound` | namespace.go (negative) | PASS | `namespace_get_unknown_returns_not_found` | mirrored, passing |
| `ResourceQuota should create a ResourceQuota and ensure its status is promptly calculated` | resource_quota.go:90 | PASS | `resource_quota_create_seeds_status_used_to_zero` | mirrored, passing |
| `ResourceQuota CRUD round-trip` | resource_quota.go:412 | PASS | `resource_quota_crud_round_trip_over_http` | mirrored, passing |
| `ResourceQuota list across namespaces` | resource_quota.go:412 (cross-ns) | PASS | `resource_quota_list_all_namespaces` | mirrored, passing |
| `ResourceQuota should create a ResourceQuota and capture the life of a pod` | resource_quota.go:243 (asserts at :312) | **FAIL** → fixed | `resource_quota_captures_full_pod_lifecycle` | mirrored, **passing** (Pod UPDATE/PATCH now run ResourceQuota admission with delta-usage semantics) |
| `ResourceQuota usage recompute on object delete` (PR #45 regression guard, HTTP surface) | resource_quota.go:243 (sub-assertion) | n/a (regression guard) | `resource_quota_usage_recomputes_on_pod_delete_via_http` | mirrored, passing |
| `ResourceQuota /status subresource returns reconciled used` | resource_quota.go (status family) | PASS | `resource_quota_status_subresource_returns_used` | mirrored, passing |
| `ResourceQuota PATCH spec.hard persists` | resource_quota.go:412 (update family) | PASS | `resource_quota_patch_spec_hard_persists` | mirrored, passing |
| `ResourceQuota BestEffort scope filter` | resource_quota.go:1063 | PASS | `resource_quota_scopes_best_effort_filter` | mirrored, passing |
| `ResourceQuota terminal pods not counted` | resource_quota.go (PodEvaluator) | PASS | `resource_quota_terminal_pods_not_counted` | mirrored, passing |
| `ResourceQuota DELETE then GET returns 404` | resource_quota.go (cleanup) | PASS | `resource_quota_delete_then_get_returns_not_found` | mirrored, passing |
| `LimitRange should create a LimitRange and ensure pod has correct defaults` | limit_range.go:60 (handler contract) | PASS | `limit_range_crud_round_trip_over_http` | mirrored, passing |
| `LimitRange list is namespace-scoped` | limit_range.go:60 (list family) | PASS | `limit_range_list_is_namespace_scoped` | mirrored, passing |
| `LimitRange admission injects defaults onto pods` | limit_range.go:60 (default-injection) | PASS | `pod_admission_applies_limitrange_defaults` | mirrored, passing |
| `LimitRange admission rejects pod over max` | limit_range.go:60 (max constraint) | PASS | `pod_admission_rejects_pod_violating_limit_range_max` | mirrored, passing |
| `LimitRange admission rejects pod below min` | limit_range.go:60 (min constraint) | PASS | `pod_admission_rejects_pod_below_limit_range_min` | mirrored, passing |
| `LimitRange admission is a no-op when no LR present` | limit_range.go (precondition) | PASS | `pod_admission_passes_when_no_limit_range_present` | mirrored, passing |

## Notes

- `resource_quota_captures_full_pod_lifecycle` now drives the full
  scenario end-to-end: quota seed → pod CREATE in-budget → controller
  reconcile → pod UPDATE that would exceed `requests.cpu` (rejected with
  403) → in-budget UPDATE (accepted, delta semantics) → PATCH that would
  exceed `requests.memory` (rejected with 403). The constituent
  sub-symptoms (usage initialization, usage recompute on pod create +
  delete, status subresource, scope filtering, terminal-pod exclusion)
  remain asserted by individual tests so a regression in any one of them
  surfaces immediately instead of being hidden inside this single
  end-to-end check.
- The recompute regression guard
  (`resource_quota_usage_recomputes_on_pod_delete_via_http`) drives the
  PR-#45 entry point `ResourceQuotaController::reconcile_one()` directly
  from the test, then reads the quota back through the REST surface — both
  sides of the regression (controller logic + REST exposure of
  `status.used`) are covered.
- The LimitRange admission tests call
  `rusternetes_api_server::admission::apply_limit_range_with` directly
  rather than going through pod-create HTTP. That helper is the same
  function the production pod-create handler invokes, so the test exercises
  the real admission code path while staying sub-millisecond. Going through
  pod-create HTTP would additionally exercise scheduling + IP allocation
  paths that are unrelated to LimitRange semantics and have their own
  scoped tests.
- Cross-reference: the namespace-cascade-delete scenario is owned by the
  namespace controller (in `controller-manager`) and is therefore covered
  by `crates/controller-manager` tests, not here. This unit owns only the
  **api-server** side of the contract (DELETE returns Terminating phase,
  finalizer present, /finalize PUT clears it).
