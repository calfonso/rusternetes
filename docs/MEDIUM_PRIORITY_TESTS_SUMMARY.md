# Medium-Priority sig-node Test Additions

Resource-shape coverage for three sig-node feature areas surfaced as
gaps in the gap analysis against the upstream Go suite:

1. **Ephemeral Containers** —
   `crates/kubelet/tests/conformance_node_ephemeral_containers.rs`
2. **RuntimeClass** —
   `crates/kubelet/tests/conformance_node_runtimeclass.rs`
3. **Image Volumes** —
   `crates/kubelet/tests/conformance_node_image_volume.rs`

Each file pins the camelCase wire shape (serde round-trip) of the
Pod-side fields the upstream e2e tests set, plus any RuntimeClass /
ImageVolumeSource / EphemeralContainer resource fields client tooling
relies on.

## Scope

These are crate-scoped Rust unit tests. They do **not**:

- spin up a live cluster or use `kube-rs`,
- assert kubelet runtime behaviour (would-the-runtime-actually-do-it),
- imply implementation of the underlying feature.

What they pin:

- camelCase keys match upstream wire format
- field omission semantics (`#[serde(skip_serializing_if)]`)
- legacy snake_case alias acceptance where the resource type allows it
- serialize → deserialize → re-serialize idempotency

## Implementation status of the underlying features

| Feature | Resource type? | Behaviour implemented? |
|---|---|---|
| Ephemeral containers | yes | partial — kubelet handles `spec.ephemeralContainers` |
| RuntimeClass | yes | no — Rusternetes uses a single hardcoded Docker runtime |
| Image volumes | yes | no — pod manifest accepted, mount layer pending |

Behavioural coverage will land alongside the implementation work, gated
with `#[ignore = "RED-state: <reason>"]` in the appropriate behavioural
test file (not in these resource-shape tests).
