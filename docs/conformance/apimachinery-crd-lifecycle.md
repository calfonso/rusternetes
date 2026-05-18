# [sig-api-machinery] CRD lifecycle — scoped conformance coverage

Crate: `crates/api-server` · Test file: `tests/conformance_apimachinery_crd_lifecycle.rs`

This unit mirrors the Kubernetes v1.35 conformance scenarios that exercise the
CustomResourceDefinition (CRD) lifecycle — create, list, get, update, delete,
the `/status` and `/scale` subresources, defaulting, OpenAPI v3 structural
schema validation, and `x-kubernetes-validations` (CEL) rule enforcement. The
goal is a sub-second `cargo test` signal that complements the hour-long
Sonobuoy run captured in
`.rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log`.

Cross-reference: `docs/CONFORMANCE.md` failure bucket
**"CRD OpenAPI publishing"** (~9 failures in Round 160) is tracked separately
in Unit 2 (`apimachinery_crd_openapi`). The two failures in Round 160 that
showed up in the `crd_publish_openapi.go` file (`:101`, `:481`) are about
publishing the schema in the discovery / swagger endpoints — they do not
belong to CRD lifecycle proper, so they are out of scope for this fragment
and not duplicated here.

Tests in this file drive the actual axum router (via `tower::ServiceExt::oneshot`)
backed by `MemoryStorage` and `AlwaysAllowAuthorizer`, so every assertion is
exercised through the same handler stack that production HTTPS traffic hits.

## Coverage matrix

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `creating/deleting custom resource definition objects works` | custom_resource_definition.go:69 | PASS | `crd_create_and_delete_round_trip` | mirrored, passing |
| `listing custom resource definition objects works` | custom_resource_definition.go:89 | PASS | `crd_list_filters_by_label_selector_and_deletecollection` | mirrored, passing |
| `getting/updating/patching custom resource definition status sub-resource works` | custom_resource_definition.go:142 | PASS | `crd_status_subresource_get_update_patch` | mirrored, passing |
| `should include custom resource definition resources in discovery documents` | custom_resource_definition.go:188 | PASS | `crd_resources_in_discovery_documents` | mirrored, passing |
| `custom resource defaulting for requests and from storage works` | custom_resource_definition.go:238 | PASS | `crd_defaulting_for_requests_and_storage` | mirrored, passing |
| `watch on custom resource definition objects` | crd_watch.go:53 | PASS | `crd_watch_create_modify_delete` | mirrored, passing |
| `MUST list and watch custom resources matching the field selector` | crd_selectable_fields.go:174 | PASS | `crd_selectable_fields_list_watch_informer` | tracker only (selectable-fields not implemented) |
| `MUST NOT fail validation for create of a custom resource that satisfies the x-kubernetes-validations rules` | crd_validation_rules.go:97 | PASS | `cel_rule_satisfied_create_succeeds` | tracker only (CEL not enforced) |
| `MUST fail validation for create of a custom resource that does not satisfy the x-kubernetes-validations rules` | crd_validation_rules.go:124 | PASS | `cel_rule_violated_create_fails` | tracker only (CEL not enforced) |
| `MUST fail create of a CRD that contains a x-kubernetes-validations rule that refers to a property that do not exist` | crd_validation_rules.go:150 | PASS | `cel_rule_unknown_property_crd_rejected` | tracker only (CEL not enforced) |
| `MUST fail create of a CRD that contains an x-kubernetes-validations rule that contains a syntax error` | crd_validation_rules.go:177 | PASS | `cel_rule_syntax_error_crd_rejected` | tracker only (CEL not enforced) |
| `MUST fail create of a CRD that contains an x-kubernetes-validations rule that exceeds the estimated cost limit` | crd_validation_rules.go:203 | PASS | `cel_rule_cost_limit_exceeded_crd_rejected` | tracker only (CEL not enforced) |
| `MUST fail create of a CR that exceeds the runtime cost limit for x-kubernetes-validations rule execution` | crd_validation_rules.go:231 | PASS | `cel_rule_runtime_cost_limit_exceeded` | tracker only (CEL not enforced) |
| `MUST fail update of a CR that does not satisfy a x-kubernetes-validations transition rule` | crd_validation_rules.go:260 | PASS | `cel_transition_rule_violated_update_fails` | tracker only (CEL not enforced) |
| `MUST NOT fail to update a resource due to JSONSchema errors on unchanged correlatable fields` | crd_validation_ratcheting.go:201 | PASS | `ratcheting_unchanged_correlatable_jsonschema_errors_allowed` | tracker only (ratcheting not implemented) |
| `MUST fail to update a resource due to JSONSchema errors on unchanged uncorrelatable fields` | crd_validation_ratcheting.go:244 | PASS | `ratcheting_unchanged_uncorrelatable_jsonschema_errors_blocked` | tracker only (ratcheting not implemented) |
| `MUST fail to update a resource due to JSONSchema errors on changed fields` | crd_validation_ratcheting.go:280 | PASS | `ratcheting_changed_jsonschema_errors_blocked` | tracker only (ratcheting not implemented) |
| `MUST NOT fail to update a resource due to CRD Validation Rule errors on unchanged correlatable fields` | crd_validation_ratcheting.go:333 | PASS | `ratcheting_unchanged_correlatable_cel_errors_allowed` | tracker only (ratcheting not implemented) |
| `MUST fail to update a resource due to CRD Validation Rule errors on unchanged uncorrelatable fields` | crd_validation_ratcheting.go:412 | PASS | `ratcheting_unchanged_uncorrelatable_cel_errors_blocked` | tracker only (ratcheting not implemented) |
| `MUST fail to update a resource due to CRD Validation Rule errors on changed fields` | crd_validation_ratcheting.go:448 | PASS | `ratcheting_changed_cel_errors_blocked` | tracker only (ratcheting not implemented) |
| `MUST NOT ratchet errors raised by transition rules` | crd_validation_ratcheting.go:511 | PASS | `ratcheting_transition_rule_errors_never_ratcheted` | tracker only (ratcheting not implemented) |
| `MUST evaluate a CRD Validation Rule with oldSelf = nil for new values when optionalOldSelf is true` | crd_validation_ratcheting.go:569 | PASS | `ratcheting_optional_old_self_nil_for_new_values` | tracker only (ratcheting not implemented) |
| Lifecycle: scale subresource get + update (from `crd_publish_openapi.go` scale spec) | custom_resource_definition.go:142 (subresource family) | PASS | `crd_scale_subresource_get_and_update` | mirrored, passing (fix in PR #86) |
| Lifecycle: list CRDs across the group reflects newly created definitions | custom_resource_definition.go:188 | PASS | `crd_list_all_includes_newly_created` | mirrored, passing |
| Lifecycle: GET returns 404 with NotFound StatusReason for missing CRD | custom_resource_definition.go:69 (negative case) | PASS | `crd_get_unknown_name_returns_not_found` | mirrored, passing |

## Notes

- The scale subresource regression was fixed in PR #86 — `get_custom_resource_scale`
  and `update_custom_resource_scale` now strip the configured root prefix
  (`spec` / `status`) via `strip_root_prefix` before walking the already-narrowed
  `cr.spec` / `cr.status` value. The test was un-ignored once the helper landed
  on main.
- CEL (`x-kubernetes-validations`) is parsed and stored but never evaluated
  during CR write paths today. Once evaluation is wired in, drop `#[ignore]`
  from each `cel_*` test; their bodies are intentionally empty so adding
  enforcement logic is a clean per-test refactor.
- Ratcheting is unimplemented — the api-server validates every field on every
  update. Once we land ratcheting, drop `#[ignore]` from each `ratcheting_*`
  test.
- All other tests in this file mirror Sonobuoy-PASSING scenarios and pass
  locally. If a regression appears, follow the same pattern: flip the doc
  status to FAIL, add `#[ignore = "Conformance failure tracker — see
  docs/conformance/apimachinery-crd-lifecycle.md"]` and capture the failure
  mode in the notes above. Do NOT delete a regression-tracking test; the
  whole point of the doc fragment is that the failure stays visible.
