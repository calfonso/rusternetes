# Rūsternetes Architecture

A ground-up reimplementation of Kubernetes in Rust. Every component -- API
server, scheduler, controller manager, kubelet, kube-proxy -- is written from
scratch, implementing the real Kubernetes API surface, wire format, and
behavioral semantics.

**By the numbers:** 216,000+ lines of Rust across 10 crates. 31 controllers.
76 API handler files. 307+ registered routes. 3,100+ tests. 91% conformance
pass rate (415/441) across 160 rounds of testing against the official K8s e2e suite.
Built-in web console with real-time topology visualization and live metrics.

---

## System Overview

```
                              +-----------+
                              |  kubectl  |
                              +-----+-----+
                                    |
                                    v
+-------------------------------------------------------------------+
|                         Control Plane                              |
|                                                                    |
|  +---------------------------+     +----------------------------+  |
|  |    API Server (HTTPS)     |<----| Controller Manager         |  |
|  |                           |     |   31 reconciliation loops  |  |
|  |  Axum + rustls            |     +----------------------------+  |
|  |  75 handler files         |                                     |
|  |  307 routes               |     +----------------------------+  |
|  |  Watch API (SSE)          |<----| Scheduler                  |  |
|  |  RBAC + Admission         |     |   Filter/Score plugins     |  |
|  |  Server-Side Apply        |     |   Priority + Preemption    |  |
|  |  Web Console (React SPA)  |     +----------------------------+  |
|  |  OpenAPI v2/v3            |                                     |
|  +-------------+-------------+                                     |
|                |                                                   |
|  +-------------v-------------+                                     |
|  |  Storage (pluggable)      |                                     |
|  |  /registry/{type}/...     |                                     |
|  |  CAS via mod_revision     |                                     |
|  |  etcd | SQLite | Redis    |                                     |
|  +---------------------------+                                     |
+--------------------------------------------------------------------+
        |                              |
        | Watch assigned pods          | Watch services + endpoints
        v                              v
+-------------------+          +-------------------+
| Kubelet (node-1)  |          | Kube-Proxy        |
| Kubelet (node-2)  |          |   iptables NAT    |
|                   |          |   host network     |
|  Docker/bollard   |          +-------------------+
|  pause container  |
|  probes + volumes |          +-------------------+
|  CAS status sync  |          | CoreDNS           |
+-------------------+          |   10.96.0.10      |
                               +-------------------+
```

All components communicate exclusively through the storage backend. The
storage layer is pluggable: etcd (production), SQLite or Redis via rhino
gRPC (lighter alternatives), or embedded SQLite/Redis (all-in-one single
binary). Rhino is an etcd-compatible gRPC server that can be backed by
SQLite, Redis, PostgreSQL, or MySQL.
See [Storage Backends](storage/STORAGE_BACKENDS.md) for details.

---

## Project Layout

```
rusternetes/
  crates/
    common/                # Shared types, 36 resource files in src/resources/
    api-server/            # Axum HTTPS API, 75 handler files, router.rs (2,135 lines)
    storage/               # Storage trait, etcd/SQLite/Redis (rhino) + in-memory backends
    controller-manager/    # 31 controllers in src/controllers/
    scheduler/             # Filter/Score plugin architecture
    kubelet/               # Node agent, Docker runtime via bollard, CNI framework
    kube-proxy/            # iptables service routing, host network mode
    kubectl/               # CLI tool (get, create, apply, delete, logs, exec, ...)
    cloud-providers/       # AWS/GCP/Azure integrations
    rusternetes/           # All-in-one binary (all components as tokio tasks)
  scripts/                 # Cluster bootstrap, cert generation, conformance runner
  docs/                    # Architecture, conformance, development guides
  docker-compose.yml       # Full cluster with etcd
  docker-compose.sqlite.yml # Full cluster with rhino/SQLite (no etcd)
  compose.redis.yml        # Full cluster with rhino/Redis (no etcd)
  compose.all-in-one.yml   # Single container with embedded SQLite
  compose.all-in-one-redis.yml # Single container + Redis
  Dockerfile.rhino         # Builds rhino gRPC server
```

---

## Crates in Detail

### common

Shared types and resource definitions used by every other crate.

**36 resource type files** in `src/resources/` covering the full Kubernetes API
surface: Pod, Deployment, ReplicaSet, StatefulSet, DaemonSet, Job, CronJob,
Service, Endpoints, EndpointSlice, ConfigMap, Secret, ServiceAccount, Node,
Namespace, PersistentVolume, PersistentVolumeClaim, StorageClass, Ingress,
IngressClass, NetworkPolicy, Role, RoleBinding, ClusterRole, ClusterRoleBinding,
CustomResourceDefinition, HorizontalPodAutoscaler, PodDisruptionBudget,
LimitRange, ResourceQuota, RuntimeClass, PriorityClass, Lease,
ValidatingAdmissionPolicy, FlowControl, CertificateSigningRequest, CSI, DRA,
IPAddress, ServiceCIDR, ControllerRevision, VolumeAttachment, and more.

**Core types:** TypeMeta, ObjectMeta, LabelSelector, ResourceRequirements,
Affinity, Toleration, Taint, Condition.

**Error handling:** Unified `Error` enum mapping to Kubernetes StatusReason.
Each variant produces the correct HTTP status code (404 NotFound, 409 Conflict,
422 Unprocessable Entity, etc.) via Axum's `IntoResponse`.

**Serialization conventions** (critical for K8s API compatibility):
- `#[serde(rename_all = "camelCase")]` on all resource structs
- `#[serde(skip_serializing_if = "Option::is_none")]` on optional fields
- `#[serde(flatten)]` for TypeMeta embedding
- K8s-style camelCase abbreviations: `podIP`, `hostIP`, `containerID`

### api-server

Axum-based HTTPS REST API. The router (`src/router.rs`, ~2,135 lines) registers
307 routes across 75 handler files.

**API groups served:**

| Group | Resources |
|-------|-----------|
| core/v1 | pods, services, configmaps, secrets, serviceaccounts, nodes, namespaces, endpoints, events, persistentvolumes, persistentvolumeclaims, limitranges, resourcequotas, replicationcontrollers, podtemplates, componentstatuses, bindings |
| apps/v1 | deployments, replicasets, statefulsets, daemonsets, controllerrevisions |
| batch/v1 | jobs, cronjobs |
| networking.k8s.io/v1 | ingresses, ingressclasses, networkpolicies |
| rbac.authorization.k8s.io/v1 | roles, rolebindings, clusterroles, clusterrolebindings |
| storage.k8s.io/v1 | storageclasses, csidrivers, csinodes, csistoragecapacities, volumeattachments |
| scheduling.k8s.io/v1 | priorityclasses |
| coordination.k8s.io/v1 | leases |
| policy/v1 | poddisruptionbudgets, evictions |
| autoscaling/v1, v2 | horizontalpodautoscalers |
| apiextensions.k8s.io/v1 | customresourcedefinitions + dynamic custom resource instances |
| admissionregistration.k8s.io/v1 | mutatingwebhookconfigurations, validatingwebhookconfigurations, validatingadmissionpolicies, validatingadmissionpolicybindings |
| certificates.k8s.io/v1 | certificatesigningrequests |
| discovery.k8s.io/v1 | endpointslices |
| flowcontrol.apiserver.k8s.io/v1 | flowschemas, prioritylevelconfigurations |
| node.k8s.io/v1 | runtimeclasses |
| resource.k8s.io/v1alpha3 | resourceclaims, resourceslices, deviceclasses |
| networking.k8s.io/v1alpha1 | ipaddresses, servicecidrs |

**Subresource endpoints:** `/status`, `/scale`, `/log`, `/exec`, `/attach`,
`/portforward`, `/eviction`, `/token`, `/finalize`, `/binding`, `/approval`.

**Key capabilities:**
- Watch API via Server-Sent Events (SSE) with bookmark keep-alives
- RBAC authorization with JWT-based service account authentication
- Admission control: mutating/validating webhooks, ValidatingAdmissionPolicy (CEL), NamespaceLifecycle, LimitRanger, ResourceQuota, PodSecurityStandards
- Server-Side Apply with field manager tracking and conflict detection
- PATCH: JSON Patch (RFC 6902), JSON Merge Patch (RFC 7396), Strategic Merge Patch
- OpenAPI v2/v3 discovery endpoints with protobuf content negotiation
- Aggregated API discovery (`/apis`)
- Table format responses for kubectl compatibility
- Dry-run support
- Pagination (limit/continue)
- Field selectors and label selectors

**State** (`src/state.rs`): Holds the storage backend, auth configuration,
ClusterIP allocator, webhook manager, and watch event cache.

### storage

The `Storage` trait (`src/lib.rs`) with multiple backends:

- **EtcdStorage** (`src/etcd.rs`) -- Production backend. Resource versions map
  to etcd `mod_revision`. Watch support via etcd's native watch API.
- **RhinoStorage** (`src/rhino.rs`) -- SQLite or Redis via [Rhino](https://github.com/calfonso/rhino).
  Behind `sqlite` and `redis` feature flags. Rhino is an etcd-compatible gRPC
  server that can also be embedded in-process.
- **MemoryStorage** (`src/memory.rs`) -- In-memory backend for unit tests.

**Key pattern:** `/registry/{resource_type}/{namespace}/{name}` for namespaced
resources; `/registry/{resource_type}/{name}` for cluster-scoped resources.

**Optimistic concurrency:** All updates use compare-and-swap (CAS) via etcd
`mod_revision`. Clients must send the current `resourceVersion`; mismatches
return HTTP 409 Conflict.

### controller-manager

31 controllers, each running as a concurrent tokio task with its own
reconciliation loop.

```rust
pub struct FooController<S: Storage> {
    storage: Arc<S>,
    interval: Duration,
}
// run() loops: reconcile_all() then sleep(interval)
```

| # | Controller | What it does |
|---|------------|--------------|
| 1 | Deployment | Manages ReplicaSets for declarative rolling updates |
| 2 | ReplicaSet | Maintains desired pod replica count via label selectors |
| 3 | StatefulSet | Ordered pod deployment with stable identities and persistent storage |
| 4 | DaemonSet | Ensures exactly one pod per eligible node |
| 5 | Job | Batch execution with completions, parallelism, and backoff |
| 6 | CronJob | Time-based job scheduling with cron syntax |
| 7 | ReplicationController | Legacy replica management (pre-ReplicaSet) |
| 8 | Endpoints | Populates Endpoints from pod IPs matching Service selectors |
| 9 | EndpointSlice | Populates EndpointSlices (modern, scalable endpoint API) |
| 10 | Service | Manages Service lifecycle, ClusterIP allocation, defaults |
| 11 | ServiceAccount | Creates default ServiceAccount per namespace |
| 12 | Namespace | Handles namespace lifecycle, finalizers, cascading deletion |
| 13 | Node | Manages node heartbeats, status, and conditions |
| 14 | PV Binder | Binds PersistentVolumeClaims to matching PersistentVolumes |
| 15 | Dynamic Provisioner | Creates PVs from StorageClass for unbound PVCs |
| 16 | Volume Snapshot | Manages VolumeSnapshot creation and lifecycle |
| 17 | Volume Expansion | Handles PVC resize when storage request increases |
| 18 | Garbage Collector | Owner-reference-based cascade deletion (foreground and background) |
| 19 | TTL Controller | Cleans up finished Jobs after TTL expiry |
| 20 | Taint Eviction | Evicts pods from NoExecute-tainted nodes |
| 21 | HPA | Horizontal scaling based on CPU/memory/custom metrics |
| 22 | VPA | Vertical scaling with resource right-sizing recommendations |
| 23 | PDB | Enforces PodDisruptionBudget during voluntary disruptions |
| 24 | Events | Event lifecycle management and TTL-based cleanup |
| 25 | ResourceQuota | Tracks and enforces namespace-level resource quotas |
| 26 | ResourceClaim | Manages DRA (Dynamic Resource Allocation) claim lifecycle |
| 27 | LoadBalancer | Assigns external IPs to LoadBalancer-type Services |
| 28 | Ingress | Manages Ingress resource status |
| 29 | Network Policy | Manages NetworkPolicy status |
| 30 | CSR | Approves and signs CertificateSigningRequests |
| 31 | CRD | Manages CustomResourceDefinition status and Established condition |

### kubelet

Node agent managing the full pod lifecycle via Docker (bollard crate).

**Container model:** Each pod starts a pause container first. Application
containers join the pause container's network namespace via `container:pause`
mode, sharing a single IP address. This matches real Kubernetes pod networking.

**Probes:** Liveness, readiness, and startup probes with HTTP GET, TCP socket,
and exec checks. Readiness gates control endpoint inclusion.

**Volumes:** emptyDir (tmpfs when medium is Memory, bind mounts otherwise),
hostPath, projected volumes, configMap volumes, secret volumes. fsGroup
permissions applied via chmod.

**Status reporting:** Pod status updates use CAS (compare-and-swap) against the
API server. The kubelet re-reads the pod from storage before each write to get
a fresh `resourceVersion`, with retry logic for conflicts.

**CNI framework:** Full CNI v1.0.0+ specification support. Plugin discovery,
configuration loading, network attachment tracking.

**Node registration:** On startup, registers itself as a Node resource with
capacity, allocatable resources, and node conditions.

### kube-proxy

iptables-based service routing. Runs in host network mode with `CAP_NET_ADMIN`.

- **ClusterIP:** DNAT rules in the RUSTERNETES-SERVICES chain
- **NodePort:** Rules in RUSTERNETES-NODEPORTS chain (ports 30000-32767)
- **LoadBalancer:** External IP routing rules
- Probabilistic load balancing across endpoints
- Reads both Endpoints and EndpointSlices
- 30-second rule sync interval with cleanup on shutdown

### scheduler

Filter/Score plugin architecture for pod-to-node assignment.

**Filter phase** (eliminates ineligible nodes):
- Unschedulable node check
- Taint/toleration matching (NoSchedule, PreferNoSchedule, NoExecute)
- Node selector evaluation
- Node affinity hard requirements (requiredDuringSchedulingIgnoredDuringExecution)
- Resource capacity check (CPU, memory)

**Score phase** (ranks remaining candidates):
- Resource availability (balance CPU/memory utilization)
- Node affinity soft preferences
- Pod affinity/anti-affinity
- Priority class weight
- Topology spread constraints

**Preemption:** When no node passes filtering, the scheduler identifies
lower-priority pods that can be evicted to make room.

### kubectl

CLI tool: `get`, `create`, `apply`, `delete`, `describe`, `logs`, `exec`,
`port-forward`, `scale`, `edit`, `patch`, `cp`.

Supports YAML/JSON input, tabular/JSON/YAML output, `--namespace` and
`--all-namespaces`, label selectors (`-l`), field selectors
(`--field-selector`), watch mode (`-w`), multi-document YAML, and kubeconfig
for cluster connection.

### cloud-providers

AWS, GCP, and Azure integration modules for cloud-native load balancer
provisioning, storage backends, and node management.

---

## Key Architecture Patterns

### Optimistic Concurrency (CAS)

Every resource carries a `resourceVersion` derived from etcd's `mod_revision`.
Updates must include the current version. If the stored version changed since
the client last read, the API server returns 409 Conflict. This prevents lost
updates without pessimistic locking. The kubelet, controllers, and all clients
must re-read before retrying.

### Watch Semantics

The Watch API uses Server-Sent Events. Behavior depends on the
`resourceVersion` parameter:

- **rv=0, rv=1, or omitted:** Server sends an initial burst of ADDED events
  for all existing resources, then streams subsequent changes.
- **rv > 1:** Server starts an etcd watch from that revision, streaming only
  new changes.
- **Bookmarks:** Periodic BOOKMARK events with updated resourceVersions
  prevent client-side watch timeouts.

### Controller Reconciliation Loop

Every controller follows the same pattern:
1. List all resources of its type from storage.
2. For each resource, compare desired state (spec) vs. actual state (status).
3. Take corrective action (create/delete pods, update status, etc.).
4. Handle errors per-resource without stopping reconciliation of others.
5. Sleep for a configured interval (typically 5-10 seconds).
6. Repeat indefinitely.

### Pause Container Networking

The kubelet creates a pause container for each pod before starting application
containers. All app containers join the pause container's network namespace
(`container:pause` mode), sharing a single IP. This matches the real Kubernetes
networking model where all containers in a pod share localhost.

---

## Docker Compose Cluster

The development cluster runs the following services:

| Service | Network | Details |
|---------|---------|---------|
| etcd | bridge | Cluster state, port 2379 |
| api-server | bridge | HTTPS on port 6443, TLS certs in `.rusternetes/certs/` |
| scheduler | bridge | Watches for unscheduled pods |
| controller-manager | bridge | Runs all 31 controllers |
| node-1 (kubelet) | bridge | First worker node |
| node-2 (kubelet) | bridge | Second worker node |
| kube-proxy | **host** | Needs CAP_NET_ADMIN for iptables |
| CoreDNS | bridge | Cluster DNS, ClusterIP 10.96.0.10 |

TLS certs are generated by `scripts/generate-certs.sh`. SANs include Docker
bridge IPs (172.18.0.2-5). The cluster is bootstrapped by
`scripts/bootstrap-cluster.sh` (CoreDNS, default services, SA tokens).

**KUBECONFIG:** `~/.kube/rusternetes-config`

---

## Data Flow: Creating a Deployment

1. `kubectl apply -f deployment.yaml` sends a request to the API server.
2. API server authenticates the request (JWT/TLS), checks RBAC authorization,
   runs admission webhooks, validates the resource, and writes to etcd.
3. The **Deployment controller** detects the new Deployment via its
   reconciliation loop and creates a ReplicaSet.
4. The **ReplicaSet controller** detects the new ReplicaSet and creates Pod
   objects (without `spec.nodeName` set).
5. The **Scheduler** watches for unscheduled Pods, filters nodes, scores
   candidates, and binds each Pod to a node by setting `spec.nodeName`.
6. The **Kubelet** on the assigned node detects the binding, creates a pause
   container, then starts application containers via Docker.
7. The kubelet writes Pod status back to the API server (phase, conditions,
   container statuses, pod IP) using CAS updates.
8. The **Endpoints** and **EndpointSlice controllers** detect the ready Pod
   and add its IP to the relevant Service endpoints.
9. **kube-proxy** detects the endpoint change and programs iptables NAT rules
   so cluster traffic is routed to the new Pod.

---

## Storage Key Schema

```
/registry/pods/{namespace}/{name}
/registry/services/{namespace}/{name}
/registry/deployments/{namespace}/{name}
/registry/statefulsets/{namespace}/{name}
/registry/jobs/{namespace}/{name}
/registry/nodes/{name}
/registry/namespaces/{name}
/registry/persistentvolumes/{name}
/registry/clusterroles/{name}
/registry/customresourcedefinitions/{name}
/registry/{custom-resource-plural}/{namespace}/{name}
...
```

Namespaced resources use a three-part key; cluster-scoped resources use two.

---

## Concurrency Model

All components use Tokio for async I/O:

- **API server:** Each HTTPS request is a Tokio task. Concurrent request
  handling with shared state behind `Arc`.
- **Controller manager:** Each of the 31 controllers runs as an independent
  `tokio::spawn` task with its own reconciliation interval.
- **Scheduler:** Periodic async loop reading unscheduled pods and writing
  binding decisions.
- **Kubelet:** Async sync loop interleaving Docker API calls (via bollard) and
  status updates to the API server.
- **Storage:** All etcd operations are async. CAS prevents concurrent write
  conflicts without locks.

---

## Testing

3,100+ test functions across the workspace. All async tests use `#[tokio::test]`.
Unit tests use `MemoryStorage` instead of requiring a running etcd instance.
Tests that share mutable state use `#[serial_test::serial]`.

```bash
cargo test                                     # All tests
cargo test -p rusternetes_api_server           # Single crate
cargo test test_name -- --nocapture            # Single test with stdout
```

Conformance testing runs the official Kubernetes e2e suite via Sonobuoy against
the Docker Compose cluster. See [CONFORMANCE.md](CONFORMANCE.md) for results.

---

## Key Dependencies

| Crate | Purpose |
|-------|---------|
| tokio | Async runtime for all components |
| axum / axum-server | HTTP framework and TLS server |
| serde / serde_json | JSON serialization for API and storage |
| etcd-client | etcd v3 gRPC client |
| bollard | Docker Engine API client for the kubelet |
| rustls / tokio-rustls | TLS implementation (no OpenSSL dependency) |
| rcgen | Self-signed certificate generation |
| jsonwebtoken | JWT generation and validation for service account tokens |
| tracing / tracing-subscriber | Structured logging across all components |
| chrono | Date/time handling for CronJobs, Events, TTL |
| uuid | Resource UID generation |
