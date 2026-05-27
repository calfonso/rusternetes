# sig-node Conformance Unit-Test Coverage

These tests pin the **resource-shape contract** that upstream Kubernetes
v1.35 sig-node conformance e2e tests exercise. They run as crate-scoped
Rust unit tests (no live cluster, no kube-rs client) and complement the
behavioural coverage in the e2e / Sonobuoy harness.

Each file maps one upstream Go conformance file to the Rusternetes
internals or resource types that the e2e behaviour ultimately depends
on.

## File map

| File (`crates/kubelet/tests/`) | Upstream (k8s v1.35) | What it pins |
|---|---|---|
| `conformance_node_container_restart_policy.rs` | `test/e2e/common/node/container_restart_policy.go` | `lifecycle::should_restart_container` decision table |
| `conformance_node_pod_lifecycle.rs` | `test/e2e/common/node/pods.go` | `lifecycle::{terminal_pod_phase, image_action, default_image_pull_policy}` |
| `conformance_node_pod_level_resources.rs` | `test/e2e/common/node/pod_level_resources.go` | Multi-container QoS edge cases + `spec.resources` serde |
| `conformance_node_secrets.rs` | `test/e2e/common/node/secrets.go` | Secret round-trip through `MemoryStorage` + the three consumption shapes (env, envFrom, volume) |
| `conformance_node_ephemeral_containers.rs` | `test/e2e/common/node/ephemeral_containers.go` | `EphemeralContainer` serde + `spec.ephemeralContainers` patch body shape |
| `conformance_node_image_volume.rs` | `test/e2e/common/node/image_volume.go` | `ImageVolumeSource` serde + Pod-manifest round-trip |
| `conformance_node_privileged.rs` | `test/e2e/common/node/privileged.go` | `securityContext.privileged` serde + omission |
| `conformance_node_security_context.rs` | `test/e2e/common/node/security_context.go` | `runAsUser/Group/NonRoot`, capabilities, seccomp profile wire shape |
| `conformance_node_runtimeclass.rs` | `test/e2e/common/node/runtimeclass.go` | `RuntimeClass` resource + `spec.runtimeClassName` field |
| `conformance_node_pod_admission.rs` | `test/e2e/common/node/pod_admission.go` | PSA-relevant fields (`hostNetwork/PID/IPC`, `runAsNonRoot`, `allowPrivilegeEscalation`), `spec.os`, tolerations |
| `conformance_node_pod_resize.rs` | `test/e2e/common/node/pod_resize.go` | `ContainerResizePolicy` serde + `status.resize` state enum |

## Scope

**In scope (these tests):**
- camelCase wire-format pinning
- serde round-tripping of every field the upstream e2e tests set or assert on
- Pure-function call-through to Rusternetes' decision helpers
  (`should_restart_container`, `get_qos_class`, `image_action`, …)
- `MemoryStorage` round-trips where the kubelet looks up Secrets / ConfigMaps

**Out of scope (covered elsewhere):**
- Container runtime behaviour (docker/CRI) — node-conformance e2e in
  `scripts/run-node-conformance.sh`
- Live API-server admission decisions — `crates/api-server/tests/`
- Live cluster end-to-end behaviour — Sonobuoy conformance

## Notes

- Where Rusternetes does not yet implement a feature (e.g. RuntimeClass
  selection, full Pod Security Admission), these tests pin only the
  resource shape so client tooling round-trips correctly. The RED-state
  behavioural pins live alongside the feature implementation.
- For the existing kubelet behavioural tests (PreStop hooks, status
  idempotency, eviction QoS), see the non-`conformance_node_*` files in
  the same `crates/kubelet/tests/` directory.
