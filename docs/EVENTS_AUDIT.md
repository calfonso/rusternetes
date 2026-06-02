# Events subsystem audit

Gap report for Kubernetes Events support in rusternetes, against upstream
(release-1.35). Scope: both `core/v1` Event and `events.k8s.io/v1` Event.

## Implemented

- **Resource structs** — `crates/common/src/resources/event.rs` models both API
  shapes: core/v1 (`involvedObject`, `message`, `firstTimestamp`,
  `lastTimestamp`, `count`, `source`) and events.k8s.io/v1 (`regarding`, `note`,
  `eventTime`, `series`, `reportingController`/`reportingInstance`, `action`).
  `EventSeries`, `EventSource`, `EventType` (Normal/Warning). MicroTime and Time
  serializers match the precision K8s clients expect.
- **Routes / handlers** — `crates/api-server/src/handlers/event.rs` provides CRUD
  + list/watch + patch + deletecollection for both `/api/v1/.../events` and
  `/apis/events.k8s.io/v1/.../events`. The v1 handlers map `regarding`→
  `involvedObject`, `note`→`message`, `reportingController`→`source.component`
  for storage/field-selector compatibility.
- **Field selectors** — `involvedObject.*`, `metadata.namespace`, `reason`,
  `type`, etc. filter correctly via the shared `filtering` helper.
- **Server-side validation** (this PR) — `crates/common/src/validation/events.rs`
  ports `pkg/apis/core/validation/events.go`: `ValidateEventCreate`,
  `ValidateEventUpdate`, `legacyValidateEvent`, `validateV1EventSeries`. Wired
  into all four create/update handlers. Strict rules (eventTime required, type
  Normal/Warning, reporting* + action + reason required/length-bounded, series
  count≥2 + lastObservedTime, new-style timestamp/count/source must be unset,
  field immutability with microsecond-precision eventTime tolerance) apply on
  the `events.k8s.io/v1` path; the documented legacy namespace/reporting rules
  apply on `core/v1`. Internally-generated events (scheduler/controllers, which
  write straight to storage) bypass the HTTP handlers and are not over-tightened.
- **Garbage collection** — `EventsController::cleanup_old_events`
  (`crates/controller-manager/src/controllers/events.rs`) deletes events older
  than 1h on its reconcile interval.
- **Component emission** — the events controller emits pod-lifecycle events
  (Started/Failed/restart) with a stable generated name + deduplication.

## Partial

- **Write-skip dedup** — `create_event_if_new` skips re-writing an event that
  already exists (by generated name) to avoid per-reconcile etcd churn. This is
  a coarse stand-in for upstream aggregation: it prevents spam but does **not**
  bump `count` or `series.count`, so a recurring event stays at `count: 1`.

## Missing

- **count / series aggregation** — no server- or recorder-side increment of
  `count` / `series.count` / `lastTimestamp` / `series.lastObservedTime` on
  repeat occurrences. Upstream's `EventAggregator` / `EventSeries` accumulation
  is absent.
- **EventCorrelator (spam + aggregation)** — no port of
  `client-go/tools/record`'s spam filter (per-source rate limiting / token
  bucket) or the aggregation key logic that collapses similar events.
- **Unified event recorder** — components construct `Event` structs ad hoc and
  write to storage directly. There is no shared `EventRecorder` /
  `EventBroadcaster` abstraction with the standard `Eventf(object, type, reason,
  messageFmt, ...)` API, so emission is inconsistent across components.
- **Kubelet events** — the kubelet does not emit node/pod events
  (Scheduled, Pulling/Pulled, Created, Started, BackOff, Unhealthy, etc.) that
  upstream relies on for `kubectl describe` and many conformance assertions.
- **`type` value coverage** — the modeled `EventType` enum only admits
  Normal/Warning, so the upstream "invalid type string" create case is rejected
  at the JSON-decode layer rather than by the validator; the validator's
  type-check is retained for parity but is currently unreachable.
