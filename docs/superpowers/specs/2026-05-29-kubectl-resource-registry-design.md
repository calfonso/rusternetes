# kubectl discovery-driven resource registry — design

**Date:** 2026-05-29
**Status:** approved (design); implementation pending
**Crate:** `crates/kubectl`

## Problem

The rusternetes `kubectl` resolves resource kinds through a separate hardcoded
`match kind { … }` table in **every** command (`apply`, `create`, `delete`,
`diff`, `get`, `set`). There is no shared registry, so the tables have drifted
out of sync with each other *and* with the api-server.

Measured against the live discovery API on 2026-05-29:

- The api-server serves **69 kinds** (58 writable + 11 virtual: reviews,
  metrics, `ComponentStatus`).
- `kubectl apply` handles **27**. **31 writable kinds are unsupported** by
  apply, including common ones: `ReplicaSet`, `ReplicationController`,
  `NetworkPolicy`, `IngressClass`, `EndpointSlice`, `PodDisruptionBudget`,
  `HorizontalPodAutoscaler`, `RuntimeClass`, the admission/webhook configs,
  the CSI storage kinds, and the DRA (`resource.k8s.io`) kinds.
- Per-command coverage is inconsistent: apply 27, diff 25, delete 21,
  create 13, get ~12. Concrete drift example: `ReplicaSet` is deletable and
  diffable but **not** appliable or creatable.

The trigger was `bootstrap-cluster.sh` failing with
`Error: Unsupported resource kind: EndpointSlice` when applying the kube-dns
EndpointSlice — the api-server supports the kind, only the client did not.

## Goal

Make client coverage track server capability **by construction**, so a kind
the api-server serves cannot silently be missing from the client. Eliminate
the per-command drift by routing all commands through a single resolver.

Non-goals (YAGNI):
- No disk caching of discovery (a single aggregated call is fast).
- No client-side field/struct validation (the api-server owns validation; this
  is the whole point of the conformance effort).
- No CRD-specific code path (custom resources fall out of discovery for free).

## Decisions

1. **Source of truth: discovery-driven.** kubectl builds its kind→resource
   mapping from the api-server's discovery endpoints at runtime, exactly as
   upstream `kubectl`/client-go do via a RESTMapper. This guarantees parity
   with the server, including CRDs and any future kind, with zero client
   changes.

2. **Untyped `serde_json::Value` operations.** Because a concrete Rust type
   cannot be selected per-kind at runtime, the verb implementations operate on
   `serde_json::Value` rather than generic-over-`T` (`apply_namespaced::<T>`).
   This mirrors upstream's `unstructured.Unstructured` model. All the work the
   typed path did is available on `Value`:
   - name/namespace: read `metadata.name` / `metadata.namespace`;
   - last-applied annotation: the existing `set_last_applied_annotation` helper
     already takes `&mut Value`;
   - `ensure_uid` / `ensure_creation_timestamp`: manipulate `metadata` on the
     `Value` directly.
   Client-side struct validation is dropped; the api-server validates.

3. **Aggregated discovery, one round-trip.** The api-server supports
   `APIGroupDiscoveryList` (apidiscovery.k8s.io v2) via content negotiation,
   verified 2026-05-29: a single `GET /apis` with
   `Accept: application/json;…as=APIGroupDiscoveryList` returns all 23 groups.
   Core (`/api`) is fetched the same way. So discovery is at most two HTTP
   calls per invocation — no caching needed.

## Architecture

New module `crates/kubectl/src/discovery.rs` exposing a `RestMapper`.

```
discovery.rs ── RestMapper ──> ResourceMapping {
                  ▲                group, version, plural, kind,
                  │                namespaced, verbs, short_names
   apply/create/delete/get/diff/set
        └─ resolve(name_or_kind) ──> ResourceMapping ──> generic Value op
```

### `ResourceMapping`

```text
group:       String   // "" for core
version:     String   // preferred version, e.g. "v1"
kind:        String   // "Deployment"
plural:      String   // "deployments"
namespaced:  bool
verbs:       Vec<String>
short_names: Vec<String>
```

Path building is uniform:
- `base = if group.is_empty() { "/api/v1" } else { "/apis/{group}/{version}" }`
- namespaced → `{base}/namespaces/{ns}/{plural}[/{name}]`
- cluster    → `{base}/{plural}[/{name}]`

### `RestMapper`

- Built lazily on first resolve from aggregated discovery (`/apis` + `/api`).
- Lookup keyed four ways, all lowercased where appropriate:
  - plural (`deployments`)
  - singular (`deployment`)
  - short name (`deploy`, `svc`, `ds`, …) — taken from discovery `shortNames`
  - kind (`Deployment`) — for YAML-driven commands that read `.kind`
- **Ambiguity rule:** when a kind/resource exists in more than one group
  (e.g. `Event` in core and `events.k8s.io`), prefer the core group, then the
  group's preferred version — matching upstream preferred-version resolution.
  Log at debug when an ambiguity is resolved.

## Command migration

Every command deletes its private `match kind { … }` table and replaces it with
`resolve → build path → generic op on Value`.

- **apply**: parse YAML → `Value`; read `.kind`; `resolve`; set last-applied
  annotation; GET item path to check existence; PUT if exists else POST to the
  collection (ensuring uid + creationTimestamp first). Collapses the ~27 arms
  and the namespaced/cluster split into one path.
- **create / get / delete / diff / set**: same resolve→path shape; each retains
  its verb-specific behaviour but loses its hardcoded kind table.

The concrete resource structs (`Pod`, `Deployment`, …) remain in `common`;
they are still used by the api-server and controllers. Only kubectl stops
depending on them for dispatch.

## Error handling

- Unknown kind/resource: `error: the server doesn't have a resource type "<x>"`
  (upstream wording), now sourced from discovery so it is accurate rather than
  a static "Unsupported resource kind".
- Discovery unreachable: `error: unable to fetch API discovery from <server>:
  <cause>` — fail fast, since every command depends on it.
- Namespace flag on a cluster-scoped kind: warn and ignore the namespace, as
  upstream does.

## Testing

- **Parity / anti-drift test** (directly answers the originating concern):
  fetch live discovery from a running api-server and assert the `RestMapper`
  resolves **every served writable kind** for the relevant verbs. This
  structurally catches "server gained a kind, client fell behind". Gated behind
  the cluster-dependent harness (as conformance tests are), or run against a
  recorded discovery fixture for speed.
- **Unit tests** against a checked-in aggregated-discovery JSON fixture:
  resolve by plural / singular / short-name / kind; path building for
  namespaced vs cluster; core-vs-grouped ambiguity resolution.
- **Regression**: replace the `EndpointSlice` curl workaround in
  `bootstrap-cluster.sh` with `kubectl apply` and assert it succeeds.

## Rollout / follow-ups

- After the registry lands, remove the curl/limitation note in
  `bootstrap-cluster.sh` (the CoreDNS EndpointSlice apply).
- Disk caching of discovery (with a TTL, like client-go's
  `~/.kube/cache/discovery`) is a possible later optimization if invocation
  latency ever matters; explicitly out of scope here.
