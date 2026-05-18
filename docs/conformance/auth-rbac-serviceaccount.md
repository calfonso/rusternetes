# [sig-auth] RBAC + ServiceAccount + TokenRequest — scoped conformance coverage

Crate: `crates/api-server` · Test file: `tests/conformance_auth_rbac_serviceaccount.rs`

Mirrors the Kubernetes v1.35 Ginkgo conformance descriptors in
[`test/e2e/auth/`](https://github.com/kubernetes/kubernetes/tree/release-1.35/test/e2e/auth)
that fall under the `[sig-auth]` group — specifically the ServiceAccount
lifecycle, TokenRequest / TokenReview interaction, the SelfSubjectReview
flow, the SubjectAccessReview / LocalSubjectAccessReview /
SelfSubjectAccessReview / SelfSubjectRulesReview APIs, and the RBAC
Role / RoleBinding / ClusterRole / ClusterRoleBinding REST surface that
every other `[sig-auth]` test depends on.

Per the conformance batch tracker (`docs/CONFORMANCE.md` Round 160,
2026-04-26: 415/441 PASS) the `[sig-auth]` slice was *stabilized early* —
none of the upstream failures in R160 fall in this slice, so every test in
this file mirrors a PASS and is expected to pass locally. There are
**no `#[ignore]`d entries** in this fragment.

All HTTP-shaped tests spin up an inline `spawn_state()` helper that wires a
fresh `MemoryStorage` into `StorageBackend::Memory`, builds the production
`ApiServerState` (`AlwaysAllowAuthorizer`, fresh `TokenManager`,
`skip_auth = true`), and drives the real `rusternetes_api_server::router::build_router`
output via `tower::ServiceExt::oneshot`. This is the same HTTP layer
Sonobuoy hits, just without a running server.

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `ServiceAccounts should run through the lifecycle of a ServiceAccount` | service_accounts.go:679 | PASS | `service_account_should_run_through_lifecycle` | mirrored, passing |
| `ServiceAccounts should update a ServiceAccount` | service_accounts.go:843 | PASS | `service_account_should_update` | mirrored, passing |
| `ServiceAccounts should create a serviceAccountToken and ensure a successful TokenReview` | service_accounts.go:882 | PASS | `service_account_token_request_then_token_review_authenticates` | mirrored, passing |
| `ServiceAccounts should mount an API token into pods` (TokenReview extras assertion only) | service_accounts.go:81 | PASS | `token_request_with_bound_pod_ref_includes_pod_extras` | mirrored, passing |
| `TokenReview negative path — invalid token MUST NOT authenticate` | service_accounts.go:882 | PASS | `token_review_rejects_invalid_token` | mirrored, passing |
| `SelfSubjectReview should support SelfSubjectReview API operations` | selfsubjectreviews.go:115 | PASS | `self_subject_review_returns_calling_user_info` | mirrored, passing |
| `SubjectReview should support SubjectReview API operations` (SAR half) | subjectreviews.go:50 | PASS | `subject_access_review_returns_allowed_status` | mirrored, passing |
| `SubjectReview should support SubjectReview API operations` (LSAR half) | subjectreviews.go:50 | PASS | `local_subject_access_review_returns_allowed_status` | mirrored, passing |
| `SelfSubjectAccessReview` (driven indirectly by per_node_update.go and `kubectl auth can-i`) | per_node_update.go | PASS | `self_subject_access_review_returns_decision` | mirrored, passing |
| `SelfSubjectRulesReview` (driven by `kubectl auth can-i --list`) | per_node_update.go | PASS | `self_subject_rules_review_returns_rule_arrays` | mirrored, passing |
| `RBAC Role REST round-trip` (used by every per_node_update + impersonation test) | per_node_update.go | PASS | `role_round_trip_create_get_delete` | mirrored, passing |
| `RBAC RoleBinding REST round-trip` | per_node_update.go | PASS | `rolebinding_round_trip_create_get_delete` | mirrored, passing |
| `RBAC ClusterRole REST round-trip` (nonResourceURLs rule shape) | per_node_update.go | PASS | `clusterrole_round_trip_with_nonresource_urls` | mirrored, passing |
| `RBAC ClusterRoleBinding REST round-trip` (multi-subject) | per_node_update.go | PASS | `clusterrolebinding_round_trip_with_multiple_subjects` | mirrored, passing |
| Compile-time RBAC struct shape guard | n/a | n/a | `rbac_typed_structs_round_trip_through_serde_json` | structural guard |

## Failure-category cross-reference

Per `docs/CONFORMANCE.md:40–53`, the nine R160 symptom buckets do not
include any `[sig-auth]` ServiceAccount / TokenRequest / RBAC failures —
authentication and authorization were among the first subsystems to land
fully and have not regressed in the rounds tracked there. If a regression
ever shows up, add the failing Ginkgo descriptor as a row above, add the
corresponding `#[tokio::test]` with `#[ignore = "Conformance failure tracker — see docs/conformance/auth-rbac-serviceaccount.md"]`,
and link the symptom bucket from the regression line.

## Running locally

```bash
cargo test -p rusternetes-api-server --test conformance_auth_rbac_serviceaccount
```

Expected runtime: well under one second on a warm build (no Docker, no
etcd, no cluster bootstrap). Compare with the ~1 hour Sonobuoy full
conformance run that exercises the same surface end-to-end.
