# Conformance Test Coverage Summary

Tracks Rusternetes' Rust test coverage of upstream Kubernetes v1.35
conformance behaviours. Coverage is layered: each layer pins a
different slice of the upstream contract.

## Test layers

| Layer | Purpose | Where it lives | Runs in CI? |
|---|---|---|---|
| Resource-shape | camelCase wire format + serde round-trip pinned by upstream e2e tests | `crates/*/tests/conformance_*.rs` | yes |
| Component-unit | Pure decision functions (restart policy, image action, QoS, …) | `crates/*/src/**/*.rs` `#[cfg(test)]` modules | yes |
| Component-integration | Handler / plugin behaviour against `MemoryStorage` | `crates/api-server/tests/`, `crates/scheduler/tests/` | yes |
| Full conformance (Sonobuoy / Hydrophone) | Behaviour against a live cluster | `scripts/run-conformance.sh` | manual + nightly |

The Sonobuoy suite is the authoritative pass/fail metric. The Rust
tests pin the contract pieces that, if they drift, *guarantee* a
Sonobuoy regression — they catch the drift before Sonobuoy spends
~25 min telling us about it.

## Upstream → Rusternetes mapping (sig-node, partial)

| Upstream Go file (k8s v1.35) | Rusternetes test | Layer |
|---|---|---|
| `test/e2e/common/node/container_restart_policy.go` | `conformance_node_container_restart_policy.rs` | resource + decision-fn |
| `test/e2e/common/node/pods.go` | `conformance_node_pod_lifecycle.rs` | decision-fn |
| `test/e2e/common/node/pod_level_resources.go` | `conformance_node_pod_level_resources.rs` | decision-fn (QoS) |
| `test/e2e/common/node/secrets.go` | `conformance_node_secrets.rs` | resource + `MemoryStorage` |
| `test/e2e/common/node/ephemeral_containers.go` | `conformance_node_ephemeral_containers.rs` | resource |
| `test/e2e/common/node/image_volume.go` | `conformance_node_image_volume.rs` | resource |
| `test/e2e/common/node/privileged.go` | `conformance_node_privileged.rs` | resource |
| `test/e2e/common/node/security_context.go` | `conformance_node_security_context.rs` | resource |
| `test/e2e/common/node/runtimeclass.go` | `conformance_node_runtimeclass.rs` | resource |
| `test/e2e/common/node/pod_admission.go` | `conformance_node_pod_admission.rs` | resource |
| `test/e2e/common/node/pod_resize.go` | `conformance_node_pod_resize.rs` | resource |
| `test/e2e/scheduling/predicates.go` | `conformance_scheduling_predicates_affinity.rs`, `conformance_scheduling_node_affinity.rs` | plugin filter / score |
| `test/e2e/network/dns.go` | `conformance_network_dns.rs` (kube-proxy slice — Service shape) | resource |

See `docs/CONFORMANCE.md` for the live Sonobuoy pass/fail rollup, and
`docs/NODE_TEST_COVERAGE_SUMMARY.md` for the sig-node detail.

## What "resource-shape" tests do and don't catch

**Catch:**
- snake_case / camelCase drift on a field rename
- accidental loss of `#[serde(skip_serializing_if = "Option::is_none")]`
- `Option<bool>` → `bool` regressions that would serialize `false` as omitted
- wrong field-number ordering in protobuf encoders (covered by the
  separate `protobuf_schema_parity_upstream.rs` suite)
- legacy snake_case alias regressions

**Don't catch:**
- Kubelet actually mounting a Secret, restarting a container, applying
  privileged caps — those need a live runtime and live in the e2e suite.
- Admission controllers actually rejecting bad pods — those live in
  `crates/api-server/tests/pod_security_admission_test.rs`.
- Scheduler actually picking the right node — those live in
  `crates/scheduler/tests/`.

## Adding a new resource-shape test

1. Pick the upstream e2e file (or behaviour) you want to pin.
2. Identify which Rusternetes resource type carries the fields the
   upstream e2e sets.
3. In the appropriate crate's `tests/` dir, write a `conformance_*.rs`
   file. Construct the resource, serialize via `serde_json::to_value`,
   assert exact keys. Round-trip back, assert equality.
4. **Do not** import `kube` or `k8s_openapi` — those imply a live
   cluster. Use `rusternetes_common::resources::*` types only.
5. **Do not** mark tests `#[ignore]` unless they pin a known
   RED-state behaviour (use `#[ignore = "RED-state: <reason>"]`).
