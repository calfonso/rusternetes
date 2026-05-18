# [sig-api-machinery] CRD OpenAPI publishing + conversion webhooks — scoped conformance coverage

Crate: `crates/api-server` · Test file: `tests/conformance_apimachinery_crd_openapi.rs`

This file is a scoped mirror of the Kubernetes v1.35 conformance tests in
`test/e2e/apimachinery/crd_publish_openapi.go` (10 cases) and
`test/e2e/apimachinery/crd_conversion_webhook.go` (2 cases), plus a handful
of supporting structural-schema checks that exercise the same publish
pipeline. It corresponds to the failure bucket "CRD OpenAPI publishing
(~9)" tracked at `docs/CONFORMANCE.md:44`.

Each test in the table below spawns the real Axum router on top of
`StorageBackend::Memory`, drives a `POST /apis/apiextensions.k8s.io/v1/customresourcedefinitions`
(or `PUT`/`DELETE`) through `tower::ServiceExt::oneshot`, then asserts the
published `/openapi/v2` (or `/openapi/v3/apis/<group>/<version>`) reflects
the expected schema. No Docker, no etcd, no kubelet — runs in <1s.

## Status table

| Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|
| `CustomResourcePublishOpenAPI works for CRD with validation schema` | crd_publish_openapi.go:68 | FAIL → PASS post fix | `crd_with_validation_schema_publishes_to_openapi_v2` | mirrored, passing |
| `CustomResourcePublishOpenAPI works for CRD without validation schema` | crd_publish_openapi.go:126 | PASS | `crd_without_validation_schema_publishes_to_openapi_v2` | mirrored, passing |
| `CustomResourcePublishOpenAPI preserving unknown fields at schema root` | crd_publish_openapi.go:157 | PASS | `crd_preserves_unknown_fields_at_root_in_openapi_v2` | mirrored, passing |
| `CustomResourcePublishOpenAPI preserving unknown fields in embedded object` | crd_publish_openapi.go:190 | PASS | `crd_preserves_unknown_fields_in_embedded_object_in_openapi_v2` | mirrored, passing |
| `CustomResourcePublishOpenAPI works for multiple CRDs of different groups` | crd_publish_openapi.go:224 | PASS | `multiple_crds_of_different_groups_publish_independently` | mirrored, passing |
| `CustomResourcePublishOpenAPI multiple CRDs of same group but different versions` | crd_publish_openapi.go:251 | PASS | `multiple_crds_same_group_different_versions_publish_separately` | mirrored, passing |
| `CustomResourcePublishOpenAPI multiple CRDs same group/version different kinds` | crd_publish_openapi.go:290 | PASS | `multiple_crds_same_group_version_different_kinds_publish_separately` | mirrored, passing |
| `CustomResourcePublishOpenAPI updates published spec when one version gets renamed` | crd_publish_openapi.go:318 | FAIL → PASS post fix | `crd_rename_version_updates_published_openapi_v2` | mirrored, passing |
| `CustomResourcePublishOpenAPI removes definition when version is unserved` | crd_publish_openapi.go:361 | FAIL → PASS post fix | `crd_unserved_version_is_removed_from_published_openapi_v2` | mirrored, passing |
| `CustomResourcePublishOpenAPI kubectl explain works for CR with same name as built-in` | crd_publish_openapi.go:406 | PASS | `crd_publish_does_not_collide_with_builtin_plural_name` | mirrored, passing |
| `CustomResourceConversionWebhook should convert from CR v1 to CR v2` | crd_conversion_webhook.go:142 | not exercised R160 | `crd_conversion_webhook_converts_v1_to_v2` | mirrored, passing (Webhook strategy implemented; ConversionReview POSTed on cross-version GET) |
| `CustomResourceConversionWebhook should convert non-homogeneous list of CRs` | crd_conversion_webhook.go:179 | not exercised R160 | `crd_conversion_webhook_converts_non_homogeneous_list` | mirrored, passing (batched ConversionReview on cross-version LIST) |
| _(supporting)_ CRD definition under `/openapi/v3/apis/<group>/<version>` | crd_publish_openapi.go:74 (root) | n/a | `crd_definition_appears_under_openapi_v3_group_version` | passing |
| _(supporting)_ `description` survives the publish round-trip | crd_publish_openapi.go:74 (root) | n/a | `crd_publish_preserves_description_metadata` | passing |
| _(supporting)_ `required` survives the publish round-trip | crd_publish_openapi.go:90 (step) | n/a | `crd_publish_preserves_required_fields` | passing |
| _(supporting)_ DELETE CRD drops definition from `/openapi/v2` | upstream cleanup (`defer cleanupCRD`) | n/a | `delete_crd_drops_definition_from_published_openapi_v2` | mirrored, passing |
| _(supporting)_ baseline `/openapi/v2` has no CRD definitions | n/a | n/a | `openapi_v2_baseline_has_no_crd_definitions` | passing |
| _(supporting)_ `/openapi/v2` is recomputed on every request | crd_publish_openapi.go (publish poll) | n/a | `openapi_v2_is_recomputed_after_crd_create` | passing |
| _(supporting)_ published path keys mirror `apis/<group>/<version>/<plural>` | crd_publish_openapi.go (kubectl resolve) | n/a | `crd_publish_includes_namespaced_get_path` | passing |
| _(supporting)_ schema PUT reflected in next `/openapi/v2` read | crd_publish_openapi.go:481 step | FAIL → PASS post fix | `crd_schema_update_reflected_in_published_openapi_v2` | mirrored, passing |
| _(supporting)_ unserved version absent from `/openapi/v3` per-GV | crd_publish_openapi.go:361 (v3 counterpart) | FAIL → PASS post fix | `crd_unserved_version_absent_from_openapi_v3_group_version` | mirrored, passing |

## Failure bucket cross-reference

Of the 26 Round 160 failures, the **CRD OpenAPI publishing (~9)** bucket
at `docs/CONFORMANCE.md:44` was the largest. The publish pipeline in
`crates/api-server/src/handlers/openapi.rs` rebuilds the spec from live
storage state on every request (`get_swagger_spec` for `/openapi/v2` and
`get_openapi_spec_path` for `/openapi/v3/apis/<group>/<version>`), so:

1. `/openapi/v2` is recomputed from the latest CRD list on every GET — no
   stale-cache leaks.
2. Definitions are keyed by reverse-domain `group.version.kind`, and old
   keys disappear on the next read after a CRD version is renamed.
3. Versions with `served=false` are filtered out of both `definitions`
   and `paths` (v2) and `components/schemas` (v3).
4. Deleting a CRD removes the corresponding definitions on the next read.
5. The structural schema (enum, required, description,
   x-kubernetes-preserve-unknown-fields) round-trips faithfully through
   `build_crd_schema_definition` + `strip_false_extensions`.

The previously-ignored tracker tests now run unconditionally; the two
remaining `#[ignore]`s on this file are the two webhook-conversion tests,
which need a conversion webhook implementation that is out of scope for
this bucket.
