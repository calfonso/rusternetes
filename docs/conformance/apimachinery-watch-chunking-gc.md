# [sig-api-machinery] Watch, chunking, GC, field selectors — scoped conformance coverage

Crate: `crates/api-server` · Test file: `tests/conformance_apimachinery_watch_chunking_gc.rs`

Cross-reference: `docs/CONFORMANCE.md` — failure bucket **"Other (GC orphan pods, chunking)"**.

Sonobuoy snapshot: Round 160 (2026-04-26), 415/441 PASS · log
`.rusternetes/volumes/sonobuoy-e2e-job-a61d864ba496412f/results/e2e.log`.

Upstream sources (Kubernetes v1.35 release branch):

- `test/e2e/apimachinery/watch.go`
- `test/e2e/apimachinery/chunking.go`
- `test/e2e/apimachinery/garbage_collector.go`
- `test/e2e/apimachinery/field_selector.go`
- `test/e2e/apimachinery/label_selector.go`

## Harness note

The canonical convention asks for axum router spawn via
`tower::ServiceExt::oneshot`. `ApiServerState::storage` is typed as
`Arc<StorageBackend>`, and `StorageBackend` has no `Memory` variant —
binding the router to in-memory storage requires a plumbing change that is
out of scope for this batch. We follow the prior-art pattern from
`crates/api-server/tests/watch_delete_test.rs`: drive the public surface of
`handlers::filtering`, `handlers::watch`, and the `MemoryStorage` watch
stream directly, validating the same wire-level invariants that the
Sonobuoy Ginkgo tests observe through the REST surface.

## Status table

| # | Upstream test (Ginkgo descriptor) | Upstream src | Sonobuoy R160 | Rust test fn | Status |
|---|---|---|---|---|---|
| 1 | `Watch should observe add, update, and delete on configmaps` | watch.go | PASS | `watch_should_observe_add_update_delete_on_configmaps` | mirrored, passing |
| 2 | `Watch should be able to start watching from a specific resource version` | watch.go | PASS | `watch_should_start_from_specific_resource_version` | mirrored, passing |
| 3 | `Watch should receive events for every added, modified, and deleted object` | watch.go | PASS | `watch_should_receive_event_per_object_lifecycle_op` | mirrored, passing |
| 4 | `Watch event types serialize as UPPERCASE` (precondition) | apimachinery/pkg/watch/watch.go | PASS | `watch_event_types_serialize_in_uppercase` | mirrored, passing |
| 5 | `Watch wraps each object in {type, object} envelope` | watch.go | PASS | `watch_envelope_includes_type_and_object` | mirrored, passing |
| 6 | `Watch DELETE includes body via key fallback` | watch.go + per-resource lifecycle | PASS | `watch_delete_event_includes_body_from_key_fallback` | mirrored, passing |
| 7 | `Watch DELETE preserves prev object body when valid` | watch.go | PASS | `watch_delete_event_preserves_prev_object_when_present` | mirrored, passing |
| 8 | `Watch resourceVersion extractable from raw JSON` (precondition) | apimachinery/pkg/watch | PASS | `watch_extract_resource_version_from_raw_json` | mirrored, passing |
| 9 | `Watch ?watch=true routes list endpoint to watch handler` | apimachinery runtime | PASS | `watch_query_param_recognised_for_list_endpoints` | mirrored, passing |
| 10 | `Watch filters by namespace prefix` | watch.go (scoped watch) | PASS | `watch_filters_events_outside_subscribed_namespace_prefix` | mirrored, passing |
| 11 | `Watch resourceVersion is monotonic across updates` | apimachinery/pkg/watch | PASS | `watch_resource_version_is_monotonic_across_updates` | mirrored, passing |
| 12 | `Watch ?allowWatchBookmarks=true delivers BOOKMARK events` | KEP-956 + watch.go | PASS | `watch_bookmark_optin_is_query_parameter` | mirrored, passing |
| 13 | `Servers should return chunks of results for list calls` | chunking.go | **FAIL** | `chunking_servers_should_return_chunks_of_results` | mirrored, **ignored** (tracks failure) |
| 14 | `Servers should support chunking with limit=1` | chunking.go | **FAIL** | `chunking_servers_should_support_limit_one` | mirrored, **ignored** (tracks failure) |
| 15 | `Continue token after compaction returns 410 Gone` | chunking.go | **FAIL** | `chunking_continue_after_compaction_returns_410_expired` | mirrored, **ignored** (tracks failure) |
| 16 | `ListMeta serializes continue field with lowercase key` (precondition) | apimachinery/pkg/apis/meta/v1/types.go | PASS | `chunking_listmeta_continue_field_serializes_as_continue_key` | mirrored, passing |
| 17 | `ListMeta defaults omit chunking fields` (precondition) | apimachinery/pkg/apis/meta/v1/types.go | PASS | `chunking_default_listmeta_omits_continue_and_remaining` | mirrored, passing |
| 18 | `FieldSelectors filter by metadata.name equality` | field_selector.go | PASS | `field_selector_filters_by_metadata_name_equality` | mirrored, passing |
| 19 | `FieldSelectors support inequality` | field_selector.go | PASS | `field_selector_filters_by_metadata_name_inequality` | mirrored, passing |
| 20 | `FieldSelectors AND with comma` | field_selector.go | PASS | `field_selector_supports_comma_and_of_predicates` | mirrored, passing |
| 21 | `LabelSelectors filter by equality` | label_selector.go | PASS | `label_selector_filters_by_equality` | mirrored, passing |
| 22 | `LabelSelectors support `in` set notation` | label_selector.go | PASS | `label_selector_supports_set_in_notation` | mirrored, passing |
| 23 | `Field+label selectors combine logically with AND` | field_selector.go + label_selector.go | PASS | `field_and_label_selectors_combine_with_logical_and` | mirrored, passing |
| 24 | `Empty fieldSelector is a no-op` | field_selector.go | PASS | `field_selector_empty_string_is_noop` | mirrored, passing |
| 25 | `Invalid field selectors return 400` | field_selector.go | PASS | `field_selector_invalid_returns_invalid_resource_error` | mirrored, passing |
| 26 | `OwnerReference required fields are present` (precondition) | apimachinery/pkg/apis/meta/v1/types.go | PASS | `gc_owner_reference_required_fields_present` | mirrored, passing |
| 27 | `OwnerReference controller flag serializes` | garbage_collector.go | PASS | `gc_owner_reference_controller_flag_serializes` | mirrored, passing |
| 28 | `OwnerReference blockOwnerDeletion serializes` | garbage_collector.go | PASS | `gc_owner_reference_block_owner_deletion_serializes` | mirrored, passing |
| 29 | `OwnerReference omits optional fields when unset` | apimachinery/pkg/apis/meta/v1/types.go | PASS | `gc_owner_reference_omits_optional_fields_when_unset` | mirrored, passing |
| 30 | `Object supports multiple owner references` | garbage_collector.go | PASS | `gc_object_supports_multiple_owner_references` | mirrored, passing |
| 31 | `DeletionPropagation policies match wire format` (precondition) | apimachinery/pkg/apis/meta/v1/types.go | PASS | `gc_deletion_propagation_policies_match_wire_format` | mirrored, passing |
| 32 | `GC deletes RC pods with Background propagation` | garbage_collector.go | PASS | `gc_background_deletion_leaves_no_orphan_dependents` | mirrored, passing |
| 33 | `GC orphans pods with Orphan propagation` | garbage_collector.go | **FAIL** | `gc_orphan_propagation_should_strip_owner_refs_not_delete` | mirrored, **ignored** (tracks failure) |
| 34 | `GC Foreground deletion via DeleteOptions body` | garbage_collector.go | PASS | `gc_foreground_deletion_propagation_serializes_in_delete_options` | mirrored, passing |
| 35 | `OwnerReferences round-trip through storage` | garbage_collector.go | PASS | `gc_owner_references_round_trip_through_storage` | mirrored, passing |
| 36 | `Object without owner refs omits field (omitempty)` | apimachinery/pkg/apis/meta/v1/types.go | PASS | `gc_object_without_owner_refs_omits_field` | mirrored, passing |
| 37 | `Namespace deletion emits DELETED watch event` | namespace.go (interlock) | PASS | `gc_namespace_deletion_emits_delete_watch_event` | mirrored, passing |
| 38 | `Delete then list reflects removal` | garbage_collector.go | PASS | `gc_delete_then_list_reflects_removal` | mirrored, passing |

## Known failures tracked here

- **Chunking (`?limit=` / `?continue=`)** — the API server does not implement
  pagination at the list endpoints. Three tests (#13, #14, #15) are
  `#[ignore]`d with the failure-bucket reason; they will compile and link
  but panic if ever un-ignored, which is the canary that the chunking
  handler still owes a real implementation.
- **GC orphan propagation** — Test #33 mirrors the upstream Orphan policy
  test which the controller does not honour server-side; the dependents
  are still deleted. `#[ignore]`d for the same reason. The companion crate
  `controller-manager` carries the implementation gap; this entry exists
  to surface the failure at the api-server layer where the wire contract
  (DeleteOptions.propagationPolicy) is parsed.

## How to run

```bash
cd /home/jones/PhpstormProjects/rusternetes
cargo fmt --all
cargo clippy -p rusternetes-api-server --tests -- -D warnings
cargo test -p rusternetes-api-server --test conformance_apimachinery_watch_chunking_gc
```

All non-`#[ignore]` tests must pass. The three `#[ignore]`d tests reproduce
the Sonobuoy-failing scenarios and are expected to remain ignored until the
underlying gaps are closed.
