# Production readiness — beyond conformance

Kubernetes conformance (the Hydrophone/Sonobuoy `[Conformance]` suite and the
kubelet `[NodeConformance]` suite) proves the API *behaves like Kubernetes* for
the tagged cases. It is a necessary floor, not a sufficient proof of production
readiness: it says little about stability under load, failure recovery, security
posture, or whether real third-party software actually runs.

This page tracks the other axes of evidence, roughly by leverage.

## Real third-party software (highest signal)

Running a real operator exercises CRDs + admission/conversion webhooks + watches
+ RBAC + leader leases + controllers **together**, the way conformance never
does. A single healthy operator install is worth dozens of green conformance
specs.

| Target | What it exercises | Status |
|---|---|---|
| **cert-manager** | CRDs, validating/mutating webhook, cainjector caBundle patching, leader Leases, SelfSigned Issuer → Certificate → Secret reconcile | **Implemented** — `scripts/run-cert-manager-smoke.sh`, nightly `cert-manager Smoke` workflow |
| Prometheus + Prometheus Operator | CRDs, ServiceMonitor discovery, scrape of Rusternetes itself (dogfood) | Planned |
| ArgoCD / Flux | Heavy list/watch churn, Server-Side Apply, GitOps reconcile | Planned |
| StatefulSet database (Postgres/etcd Helm chart) | PVC binding, ordered pods, stable network IDs | Planned |
| metrics-server | Closes the HPA loop end-to-end | Planned |

## Wider correctness

- **Full upstream e2e suite**, not just `[Conformance]` — the per-`[sig-*]`
  tests are thousands more specs the same harness already runs (node
  conformance alone is 191 of 7348).
- **client-go / controller-runtime** compatibility (implied by the operators
  above).

## Stability & resilience

- **Soak**: run for days/weeks under steady churn; watch RSS/fd/tokio-task
  growth, SQLite/Rhino file growth, resourceVersion monotonicity, watch-cache
  drift. Ideally on the Raspberry-Pi target.
- **Failure injection / recovery**: restart each component mid-operation
  (api-server during a watch, kubelet, storage); HA leader failover; Chaos Mesh
  for network partition / disk pressure / clock skew.
- **Backup/restore** of the SQLite state file.
- **Upgrade & version skew**: control-plane upgrade/rollback; old kubelet ↔ new
  apiserver; storage migration round-trips (sqlite ↔ rhino ↔ etcd) with
  integrity checks.

## Performance & scale

- **clusterloader2** (the official scalability suite) emitting the upstream API
  SLOs: p99 mutating-call < 1s, read-only < 30s, watch propagation, pod-startup
  latency. Both proves performance and reveals the real ceiling.
- **Footprint benchmark** (idle/loaded RAM, binary size, time-to-cluster vs
  k3s) — the proof that matters most for the lightweight/edge positioning.

## Security

- **kube-bench** (CIS Kubernetes Benchmark).
- **Supply chain**: `cargo audit` / `cargo deny` in CI, an SBOM, `cargo-fuzz`
  on the decode/wire paths (protobuf, pagination continue-tokens).
- **Authz/authn hardening**: anonymous-access and privilege-escalation probes,
  audit-log completeness, cert rotation.

## Storage / networking ecosystem

- **csi-sanity** and the external-storage e2e testsuite for CSI paths.
- **Ingress / Gateway API conformance** suites (same pattern as core
  conformance).

---

The cert-manager smoke is the first concrete step on this list. The intent is to
grow the "real third-party software" table and wire each item into CI as a
non-gating nightly first, promoting to a gate once it is reliably green.
