# Rūsternetes Console

The rusternetes console is a web-based cluster management dashboard served directly from the API server. It provides real-time topology visualization, live metrics, pod log streaming, and full resource management — with zero external dependencies.

## Accessing the Console

Open your browser to `https://localhost:6443/console/`. You'll need to accept the self-signed certificate warning on first visit.

The console auto-deploys with the cluster — no separate setup needed. It's baked into the API server Docker image and served at `/console/`.

---

## Overview Dashboard

[![Overview Dashboard](screenshots/thumbs/console-overview.png)](screenshots/console-overview.png)

The Overview is the landing page showing real-time cluster health at a glance.

**Health Rings** — Three animated circular gauges at the top:
- **Pods Running** (green) — running pods vs total pods
- **Nodes Ready** (blue) — ready nodes vs total nodes
- **Deploys Available** (orange) — available deployments vs total

**Metrics Cards** — Four cards with live sparkline charts that collect data every 30 seconds and show the last 30 minutes of history:
- **Total Pods** — count with trend. Click to browse all pods.
- **Nodes** — count with ready count. Click to go to Nodes view.
- **Restarts** — total container restart count. Color changes to red when high.
- **Resource Types** — how many API resource types were discovered. Click to open the Resource Explorer.

**Deployments** — Rollout progress bars color-coded by status:
- Green = fully available, Yellow = partially ready, Red = no ready replicas
- Click "View all" to see all deployments

**Recent Events** — Live event feed. Warning events highlighted in yellow. Click "View all" for the full Events view.

---

## Cluster Topology

[![Cluster Topology](screenshots/thumbs/console-topology.png)](screenshots/console-topology.png)

The Topology view is an animated visual map of your entire cluster showing nodes, pods, services, and the network connections between them.

**Nodes** — Shown as containers with:
- Node name and pod count
- CPU and Memory utilization bars with percentages (from real container stats via CRI, updated every 5 seconds)
- Green dot = Ready, pulsing red dot = NotReady

**Pods** — Colored squares inside their node:
- Green = Running, Yellow = Pending, Red = Failed
- Brightness indicates CPU usage — brighter pods are using more CPU
- Hover to see pod name, phase, CPU/memory usage, containers, IP, and ports

**Services** — Orange boxes across the top showing:
- Service name, ClusterIP, and type (ClusterIP, NodePort, LoadBalancer)
- Protocol/port badges (e.g., 80/TCP, 53/UDP)
- Endpoint count

**Network Connections** — Animated dashed lines connecting services to their target pods. Tiny particles flow along the lines showing traffic direction, colored by protocol (TCP=blue, UDP=green, HTTP=orange, gRPC=red). Lines fan out from the service bottom edge so individual connections are visible.

**Controls:**
- **Namespaces** toggle — shows/hides colored zones around pods grouped by namespace
- **Protocols** toggle — shows/hides port/protocol badges on services
- **Zoom** — in/out/reset controls

### Clicking a Pod

Click any pod square to:
- See its detail panel below the SVG (phase, CPU, memory, restarts, IP, ports)
- Automatically open live logs in a fixed overlay at the bottom of the screen
- Highlight its service connections

### Clicking a Service

Click a service to see its detail panel (type, ClusterIP, endpoints, port mappings with protocol colors).

### Live Pod Logs

[![Topology with Live Logs](screenshots/thumbs/console-topology-logs.png)](screenshots/console-topology-logs.png)

When you click a pod, a **Live Logs** panel slides up from the bottom of the screen:
- Timestamps dimmed, log content highlighted
- Log levels color-coded: ERROR (red), WARN (yellow), INFO (blue)
- Green pulsing dot indicates streaming (refreshes every 5 seconds)
- Click the pod again or "Close" to dismiss

### Activity Timeline

At the bottom of the topology, the **Cluster Activity** timeline records snapshots every 15 seconds:
- Bar chart of pod count history (green = pods added, red = pods removed)
- Live indicator (pulsing green dot) when viewing current state
- Click any bar to see pod/node/service count and deltas at that point
- Time range label shows how many minutes of history are recorded

---

## Workloads

[![Workloads](screenshots/thumbs/console-workloads.png)](screenshots/console-workloads.png)

The Workloads page gives you full visibility and control over your running applications.

**Pod Phase Chart** — Donut chart showing the breakdown of pod phases (Running, Pending, Failed, Succeeded) with counts.

**Deployment Cards** — Each deployment shows:
- Name and namespace
- Rollout progress bar (green = ready, yellow = progressing, red = failing)
- Ready/desired replica count and updated count
- **Scale controls** — click +/- to adjust replica count instantly
- **Restart** — rolling restart via annotation
- **View/Delete** actions

**Restart Heatmap** — Visualizes which pods have the most container restarts. Bar length = restart count relative to the worst pod. Red = high, yellow = moderate, teal = low.

**Pod Table** — Full table with:
- Name (clickable → detail view), namespace, status badge, ready containers, restart count (colored red when high), node, age
- View and delete action buttons per row
- Updates in real-time via K8s watch streams

**Zero State** — When no workloads exist, shows a "Quick Deploy" button linking to the Create page.

### How to Deploy an Application

1. Click **Create** in the sidebar (or **+ Deploy** button on the Workloads page)
2. Select **Quick Deploy**
3. Enter app name (e.g., `nginx`), container image (e.g., `nginx:latest`), replicas, port
4. Check **Create Service** to automatically expose it with a ClusterIP Service
5. Click **Deploy**

The Deployment and Service are created immediately. Switch to **Workloads** to see the rollout progress bar fill up as pods become ready.

### How to Scale

On any deployment card, click **+** or **-** to increase or decrease replicas. The change takes effect immediately — watch the progress bar and pod count update in real-time.

You can also scale from the **Resource Detail** view (click the deployment name → header has scale controls).

### How to Do a Rolling Restart

Click the **restart** icon (circular arrow) on a deployment card. This patches the pod template with a `restartedAt` annotation, triggering a zero-downtime rolling restart of all pods.

### How to Delete a Pod or Deployment

Click the **trash icon** on any row or card. Confirm the deletion in the dialog. The resource is deleted via the K8s API and disappears from the view in real-time.

Deleting a pod managed by a Deployment/ReplicaSet causes the controller to create a replacement automatically.

---

## Networking

[![Networking](screenshots/thumbs/console-networking.png)](screenshots/console-networking.png)

The Networking page shows your cluster's network configuration and all network resources.

### Cluster Network Configuration

Four cards always visible at the top, even with zero services:

- **Service CIDR** — the IP range for ClusterIP allocation (e.g., `10.96.0.0/12`). All ClusterIP addresses are allocated from this range.
- **Pod CIDRs** — per-node pod CIDR assignments. Shows `auto` when pods use the container runtime's default bridge networking.
- **Cluster DNS** — the kube-dns service at `10.96.0.10` with ports 53/UDP, 53/TCP (DNS), and 9153/TCP (metrics). All pods use this for service discovery.
- **Kube-Proxy** — mode (`iptables`), runs in host network mode, supports ClusterIP, NodePort, and LoadBalancer service types.

### Service Types

Services are color-coded:
- **Blue (ClusterIP)** — accessible only within the cluster. This is the default type. Every service gets a virtual IP from the Service CIDR.
- **Green (NodePort)** — accessible on port 30000-32767 on every node's IP. Useful for development and testing.
- **Orange (LoadBalancer)** — provisions an external load balancer. Requires a cloud provider integration or MetalLB for bare-metal clusters.

### Service Routing Visualization

The **Service Routing** section shows visual diagrams for each service with a selector:
- Service box (with ClusterIP) → arrow → matched pods (with status dot, name, and pod IP)
- This shows exactly which pods are receiving traffic for each service

### Service Cards

Each service gets a card showing:
- Name, namespace, type badge
- ClusterIP address
- Port mappings displayed as `port → targetPort / protocol` with arrows
- NodePort shown in yellow when applicable
- Target pod count

### Creating a Service

Use **Create > Service** to create a Service from a JSON template, or use **Quick Deploy** with the "Create Service" checkbox to create a Deployment + Service together.

### Network Policies

When NetworkPolicy resources exist, they appear as cards at the bottom of the Networking page. Click any to see its full spec in the detail view.

### Ingresses

Ingress resources appear in a table section when present, with name, namespace, and click-through to detail view.

### Load Balancers

For LoadBalancer services on bare-metal clusters (no cloud provider), configure MetalLB to allocate external IPs. Create a MetalLB IPAddressPool and L2Advertisement, then create a Service with `type: LoadBalancer`. The service will receive an external IP from the pool.

### CNI Plugins

Rusternetes implements the standard CNI specification. On Linux with network namespace support, you can use third-party CNI plugins:
- **Calico** — BGP-based networking with full NetworkPolicy support
- **Cilium** — eBPF-based high-performance networking
- **Flannel** — simple overlay networking

Drop the plugin binaries in `/opt/cni/bin/` and configuration in `/etc/cni/net.d/`. CNI requires Linux network namespaces, so it is only active on Linux nodes; on macOS the kubelet falls back to the container runtime's default bridge networking.

---

## Storage

[![Storage](screenshots/thumbs/console-storage.png)](screenshots/console-storage.png)

The Storage page manages persistent storage across the cluster.

### Overview Panel

Five stat cards:
- **Claims** — PVC count (with bound count)
- **Volumes** — PV count (with available count)
- **Classes** — StorageClass count
- **CSI Drivers** — Container Storage Interface drivers
- **Capacity** — total provisioned storage

### Storage Capabilities

Shows what the cluster supports:
- **Volume types**: emptyDir (ephemeral per-pod), hostPath (host directory), configMap, secret, projected, downwardAPI, persistentVolumeClaim
- **Access modes**: ReadWriteOnce (single node read-write), ReadOnlyMany (multi-node read-only), ReadWriteMany (multi-node read-write)
- **Reclaim policies**: Delete (PV deleted when PVC is deleted), Retain (PV preserved), Recycle (deprecated)
- **Dynamic provisioning**: enabled when a StorageClass with a provisioner exists

### Default StorageClass

Rusternetes bootstraps a `standard` StorageClass on startup with the `rusternetes.io/hostpath` provisioner. When you create a PVC referencing this class, the Dynamic Provisioner controller automatically creates a PV backed by a host directory.

### How to Create a StorageClass

1. Click **+ StorageClass** on the Storage page
2. **Name**: e.g., `fast-storage`
3. **Provisioner**: choose `rusternetes.io/hostpath` (auto-provisions directories) or `kubernetes.io/no-provisioner` (manual PV binding)
4. **Reclaim Policy**: `Delete` or `Retain`
5. **Volume Binding Mode**: `WaitForFirstConsumer` (recommended — binds when a pod actually uses it) or `Immediate`
6. Click **Create StorageClass**

Success message appears at the top of the page. The new class appears in the StorageClass section immediately.

### How to Create a PVC

1. Click **+ Create PVC** on the Storage page
2. **Name**: e.g., `my-data`
3. **Namespace**: defaults to `default`
4. **Storage Class**: dropdown populated from existing StorageClasses. If none exist, a hint says "Create a StorageClass first"
5. **Size**: 1Gi, 5Gi, 10Gi, 50Gi, or 100Gi
6. **Access Mode**: ReadWriteOnce (RWO), ReadOnlyMany (ROX), or ReadWriteMany (RWX)
7. Click **Create PVC**

If the selected StorageClass has a dynamic provisioner (like `rusternetes.io/hostpath`), a PV is automatically created and the PVC transitions from Pending → Bound. Without a provisioner, the PVC stays Pending until you manually create a matching PV.

### PVC Lifecycle

- **Pending** — waiting for a PV to be created or bound
- **Bound** — successfully bound to a PV, ready for use by pods
- **Released** — the PVC was deleted but the PV still exists (Retain policy)

### Using a PVC in a Pod

Reference the PVC name in your pod spec:

```json
{
  "spec": {
    "containers": [{
      "volumeMounts": [{ "name": "data", "mountPath": "/data" }]
    }],
    "volumes": [{
      "name": "data",
      "persistentVolumeClaim": { "claimName": "my-data" }
    }]
  }
}
```

### Volume Snapshots and Expansion

Rusternetes supports volume snapshots (point-in-time copies) and online volume expansion (resize PVCs without downtime). Create VolumeSnapshot resources via **Explore All > VolumeSnapshots** or the JSON editor.

---

## Nodes

[![Nodes](screenshots/thumbs/console-nodes.png)](screenshots/console-nodes.png)

The Nodes page shows every node in the cluster with real-time resource utilization.

**Node Cards** — Each node displays:
- **Name** (clickable → detail view) and **Ready/NotReady** status badge
- **Role badges** (e.g., "control-plane") from `node-role.kubernetes.io/` labels
- **"cordoned"** badge if the node is marked unschedulable
- **Version** — kubelet version (e.g., v1.35.0-rusternetes)
- **OS/Architecture** — e.g., linux/amd64
- **Pod count** — number of pods scheduled to this node
- **Age** — how long the node has been registered
- **CPU gauge** — real utilization from container stats via CRI (containerd), shown as millicores used / total cores with percentage (e.g., `2m / 4.0 cores (0.1%)`)
- **Memory gauge** — real utilization shown as used / total with percentage (e.g., `37Mi / 8.0Gi (0.5%)`)
- **Taint badges** — shows each taint as `key=value:effect` (e.g., `node.kubernetes.io/not-ready:NoSchedule`)

### How to Cordon/Uncordon a Node

Click the **ban icon** to cordon a node (marks it unschedulable — no new pods will be scheduled there, but existing pods continue running). Click the **check icon** to uncordon it (allows scheduling again).

### Node Metrics

CPU and memory values come from the metrics API (`/apis/metrics.k8s.io/v1beta1/nodes`), which queries real container stats via CRI (containerd, `ListContainerStats`). The metrics are:
- Per-node (each node shows only its own pods' usage)
- Updated every 5 seconds
- Derived from actual container CPU nanocores and memory working set bytes

---

## Configuration

[![Configuration](screenshots/thumbs/console-config.png)](screenshots/console-config.png)

The Config page manages cluster configuration resources.

**Summary Chips** — Quick counts: ConfigMaps, Secrets, Service Accounts

**ConfigMap Cards** — Each card shows:
- Name and namespace
- **Data key badges** — colored pills showing each key in the ConfigMap's data
- View (eye icon) and Delete (trash icon) actions

ConfigMaps are commonly used for application configuration, CoreDNS Corefile, and kube-proxy config.

**Secret Cards** — Each card shows:
- Name and namespace with eye-off icon (indicating sensitive content)
- **Type badge** — e.g., `kubernetes.io/service-account-token`, `Opaque`, `kubernetes.io/tls`
- **Key count** — number of data entries
- View and Delete actions

Secret values are base64-encoded in the API. The console shows the keys but not the decoded values.

**Service Account List** — Compact cards for each ServiceAccount. Click to view details including bound secrets and token information.

### How to Create a ConfigMap

Use **Create > ConfigMap** to open the JSON editor pre-populated with a ConfigMap template. Edit the `data` section with your key-value pairs and click Create.

### How to Create a Secret

Use **Create > From JSON** and paste a Secret definition. Use `stringData` for plain-text values (the API server base64-encodes them automatically) or `data` for pre-encoded values.

---

## RBAC (Access Control)

[![RBAC](screenshots/thumbs/console-rbac.png)](screenshots/console-rbac.png)

The RBAC page visualizes who has access to what in the cluster.

**Subjects** — Shows every identity that has RBAC permissions:
- **ServiceAccount** (teal icon) — automated identities for pods and controllers
- **Group** (yellow icon) — groups like `system:masters`, `system:authenticated`
- **User** (blue icon) — individual user identities
- Each card shows the roles bound to that subject

**ClusterRoleBindings** — Visualizes the connection between subjects and roles:
- Subject badge → arrow → role reference
- Shows which specific role each subject is bound to

**ClusterRoles** — Cards showing role permissions:
- Role name and rule count
- **Rule badges** showing verbs (get, list, create, delete, etc.) on resources
- **"full access (*)"** in red for roles with wildcard access to everything

Click "View all" to see the complete list of ClusterRoles or ClusterRoleBindings via the Resource Explorer.

### Understanding RBAC

- **Role** — defines permissions within a namespace (verbs × resources)
- **ClusterRole** — defines permissions cluster-wide
- **RoleBinding** — grants a Role to a subject in a specific namespace
- **ClusterRoleBinding** — grants a ClusterRole to a subject across all namespaces

### Default ClusterRoles

- `cluster-admin` — full access to everything
- `admin` — full access within a namespace (no RBAC modification)
- `edit` — read/write to most resources
- `view` — read-only access

### How to Secure the Cluster

By default, `--skip-auth` is enabled — all requests are treated as admin with no token required. To enable real authentication:

1. **Generate RSA signing keys** for JWT token signing:
   ```bash
   mkdir -p .rusternetes/certs
   openssl genrsa -out .rusternetes/certs/sa.key 2048
   openssl rsa -in .rusternetes/certs/sa.key -pubout -out .rusternetes/certs/sa.pub
   ```

2. **Create an admin ServiceAccount** (while still in skip-auth mode):
   ```bash
   kubectl create serviceaccount cluster-admin -n kube-system
   kubectl create clusterrolebinding cluster-admin-binding \
     --clusterrole=cluster-admin \
     --serviceaccount=kube-system:cluster-admin
   ```

3. **Create a token Secret** and retrieve it:
   ```bash
   cat <<EOF | kubectl apply -f -
   apiVersion: v1
   kind: Secret
   metadata:
     name: cluster-admin-token
     namespace: kube-system
     annotations:
       kubernetes.io/service-account.name: cluster-admin
   type: kubernetes.io/service-account-token
   EOF

   TOKEN=$(kubectl get secret cluster-admin-token -n kube-system \
     -o jsonpath='{.data.token}' | base64 -d)
   kubectl config set-credentials rusternetes-admin --token="$TOKEN"
   ```

4. **Restart without `--skip-auth`** — edit `docker-compose.yml` to remove the `--skip-auth` line, rebuild, and redeploy the API server.

5. **Verify**: `curl -k https://localhost:6443/api/v1/pods` should return 401 Unauthorized.

For the console in auth mode, set the token in the browser: `sessionStorage.setItem('rusternetes-token', '<TOKEN>')`.

### TLS and mTLS

The API server supports:
- **TLS** with self-signed or custom certificates (`--tls`, `--tls-cert-file`, `--tls-key-file`)
- **mTLS** with client certificate authentication (`--client-ca-file`) — clients must present a certificate signed by the specified CA

---

## Events

[![Events](screenshots/thumbs/console-events.png)](screenshots/console-events.png)

The Events page shows everything happening in the cluster with rich filtering.

**Event Frequency Histogram** — Stacked bar chart showing event count over the last hour in 5-minute buckets:
- Blue bars = Normal events
- Yellow bars = Warning events

**Filter Controls:**
- **Type buttons** — All, Warning (with count badge), Normal
- **Text search** — filter by reason, message, object name, or kind
- **Quick reason filters** — one-click buttons for common reasons (Created, Pulled, Scheduled, Started)

**Event List** — Each event shows:
- Severity icon (blue=Normal, yellow=Warning, red=Error)
- **Reason** (e.g., Created, Pulled, Started, FailedScheduling) — bold and prominent
- **Involved object** (clickable — navigates to that resource's detail view)
- Namespace in parentheses
- Message text
- Time ago and occurrence count (e.g., "x3" if the event fired multiple times)
- Warning events have amber background

Auto-refreshes every 10 seconds. Shows up to 100 most recent events.

---

## Create Resource

[![Create Resource](screenshots/thumbs/console-create.png)](screenshots/console-create.png)

The Create page offers four ways to create resources:

**Quick Deploy** — Form-based application deployment:
1. Enter app name, container image, replicas, port, namespace
2. Check **Create Service** to expose it with a ClusterIP Service
3. Click **Deploy** — creates the Deployment (and Service) immediately

**From JSON** — Create any Kubernetes resource from a JSON definition:
- Pre-populated with a Deployment template by default
- When opened via a "Create" button on a resource list, the template matches that resource type (e.g., clicking Create on the Ingress list gives you an Ingress template)
- Supports all 100+ resource types including CRDs

**Service** — JSON editor pre-populated with a Service template (ClusterIP, selector, port mapping)

**ConfigMap** — JSON editor pre-populated with a ConfigMap template (key-value data)

---

## Resource Explorer

Navigate to **Explore All** in the sidebar. This is the universal entry point for browsing any resource type in the cluster.

The explorer auto-discovers all resource types from the API server (100+), including any Custom Resource Definitions. Resources are grouped into categories:
- Workloads, Networking, Storage, Access Control, Configuration, Cluster, Extensions, Coordination, Certificates, Scheduling, Autoscaling, and custom API groups

**Search** — Type to filter by kind name, plural name, or short name (e.g., "po" for pods, "svc" for services, "deploy" for deployments).

**Resource Cards** — Each type shows:
- Kind name and API group/version (e.g., `apps/v1`)
- Short names in orange (e.g., `po`, `svc`, `deploy`)
- "ns" badge if the resource is namespaced
- Live resource count (yellow number)

Click any card to open the **Resource List View** for that type.

### Resource List View

A table of all instances of the selected resource type:
- Name (clickable → detail view), namespace, status, age
- Search bar to filter by name or namespace
- Create button (opens JSON editor with correct template)
- Refresh button to re-fetch
- Delete button (trash icon) with confirmation dialog per row
- View button (eye icon) to open detail view
- Real-time updates via K8s watch stream

### Resource Detail View

Click any resource name to see:

**Overview Tab** — Metadata (UID, generation, finalizers), labels as colored chips, conditions table with status badges, owner references, annotations

**YAML/JSON Tab** — Full resource JSON in an editor. Edit and click Save to apply changes via PUT. Reset to revert.

**Events Tab** — Events filtered to this specific resource, auto-refreshing every 15 seconds

**Header Actions:**
- **Scale** (Deployments/StatefulSets/ReplicaSets) — +/- replica controls
- **Restart** (Deployments/StatefulSets/DaemonSets) — rolling restart via annotation
- **Copy** — copies full JSON to clipboard
- **Delete** — deletes with confirmation dialog

---

## Custom Resource Definitions (CRDs)

CRDs let you extend the Kubernetes API with your own resource types. Rusternetes supports the full CRD lifecycle:

- **Create a CRD** via kubectl or the JSON editor — defines a new resource type with its own API group, version, kind, schema, and subresources
- **Auto-discovery** — the Resource Explorer discovers new CRDs within 5 minutes (discovery cache TTL)
- **CRUD operations** — create, read, update, delete custom resource instances through the console just like built-in resources
- **Watch** — custom resources support real-time watch streams
- **Status/Scale subresources** — if defined in the CRD, the status and scale endpoints work
- **Schema validation** — the API server validates custom resources against the CRD's OpenAPI v3 schema

To create a CRD, use **Create > From JSON** and define a CustomResourceDefinition. After creating it, navigate to **Explore All** — your new resource type appears under its API group.

---

## Monitoring & Metrics

### Where Metrics Come From

The console collects metrics from two sources:

1. **K8s Metrics API** — `/apis/metrics.k8s.io/v1beta1/nodes` and `/pods`. The kubelet queries container stats via CRI (containerd, `ListContainerStats`), aggregates CPU nanocores and memory working set bytes per node based on pod-to-node assignments. Updated every 5 seconds on both Nodes and Topology pages.

2. **In-browser collection** — the Overview page polls `/api/v1/pods`, `/api/v1/nodes`, `/apis/apps/v1/deployments`, and `/api/v1/events` every 30 seconds, storing time-series data points for sparkline charts. Up to 30 minutes of history (60 data points).

### What You Can Monitor

| Where | What | Update Frequency |
|---|---|---|
| Overview health rings | Pod/node/deployment readiness ratios | 30 seconds |
| Overview sparklines | Pod count, node count, restart rate, event rate trends | 30 seconds |
| Topology node bars | Per-node CPU and memory utilization | 5 seconds |
| Topology pod brightness | Per-pod CPU usage intensity | 5 seconds |
| Topology particles | Service-to-pod traffic flow visualization | Animated |
| Topology timeline | Pod/node/service count history | 15-second snapshots |
| Nodes page gauges | Per-node CPU (millicores) and memory (Mi) with percentage | 5 seconds |
| Events histogram | Event frequency over last hour | 10 seconds |
| Workloads restart heatmap | Container restart counts per pod | 30 seconds |

### Node Metrics Detail

CPU is shown as millicores used vs total cores. For example, `2m / 4.0 cores (0.1%)` means 2 millicores (0.002 cores) out of 4 available cores. These are real values from CRI container stats, not estimates.

Memory is shown as megabytes or gigabytes used vs total allocatable. For example, `37Mi / 8.0Gi (0.5%)`.

When utilization is very low (< 1%), the gauge shows one decimal place (e.g., `0.1%`) with a minimum visible bar width so you can always see that something is running.

---

## Multi-Cluster (Fleet Mode)

The console supports managing multiple rusternetes clusters from a single browser tab.

### Enable Fleet Mode

Click the **Fleet** button in the header bar. A cluster switcher appears showing "Local" (the current cluster).

### Register a Remote Cluster

1. Click the **+** button in the cluster switcher
2. Enter a **Name** (e.g., "production") and the cluster's **API server URL** (e.g., `https://10.0.1.5:6443`)
3. Click **Add**

### Switch Clusters

Click any cluster name in the switcher. All console views immediately route their API calls to the selected cluster. The header shows which cluster is active.

### How It Works

- **Local cluster**: API calls go same-origin (no prefix)
- **Remote clusters**: API calls are prefixed with `/clusters/{cluster-id}` and proxied by the hub API server to the remote cluster
- **Persistence**: cluster registrations are stored in the browser's `localStorage` and survive page reloads
- **Remove**: click the X next to any remote cluster name to unregister it

---

## Namespace Filtering

The **namespace dropdown** in the header bar filters all views to a specific namespace:
- **"All namespaces"** — shows resources from every namespace (default)
- **Select a namespace** — only shows resources in that namespace

The dropdown is populated live from the cluster's namespace list. Namespace filtering applies to all tabbed views (Workloads, Networking, Storage, Config, Events) and the Resource Explorer/List views for namespaced resource types. Cluster-scoped resources (Nodes, ClusterRoles, PVs, StorageClasses) are always shown regardless of the namespace filter.

---

## Troubleshooting

### Console shows "Loading..." forever
- Check that the API server is running: `curl -k https://localhost:6443/healthz`
- Check browser developer console for errors (F12 > Console tab)
- If using auth, ensure a valid token is in sessionStorage: `sessionStorage.getItem('rusternetes-token')`

### Console shows stale data
- The namespace filter may be set to a specific namespace — check the dropdown in the header
- Click the refresh button on any resource list to force a re-fetch
- Hard refresh the page (Cmd+Shift+R / Ctrl+Shift+R)

### Resources created but don't appear
- The resource may be in a different namespace — set namespace filter to "All namespaces"
- Watch updates may have disconnected — check the "Connected" indicator in the header bar
- The resource may have been created successfully but is in a non-Running state — check the Events page

### Metrics show 0% on everything
- CPU/memory metrics come from container stats via CRI (containerd, `ListContainerStats`)
- Very low utilization (< 1%) shows as "0.1%" with a tiny visible bar
- If metrics are stuck at exactly 0, check that the kubelet can reach the CRI endpoint (`CONTAINER_RUNTIME_ENDPOINT`, default `unix:///run/containerd/containerd.sock`)
- Run `curl -sk https://localhost:6443/apis/metrics.k8s.io/v1beta1/nodes` to verify the metrics API returns data

### Console accessible but API calls fail
- The console is served at `/console/` and the API at `/api/` — both from the same server. If the console loads but shows errors, the issue is with the API endpoint, not the console itself.
- Check API server logs: `docker compose logs api-server`

### "e2e-fake-node" or stale nodes appearing
- Conformance tests create temporary fake nodes that may not get cleaned up
- Delete them: `kubectl delete node <fake-node-name>`

---

## Architecture

The console is a React single-page application served by the Axum API server at `/console/`. Because the SPA and API share the same origin, there is no CORS, no proxy, and no separate deployment.

```
Browser ─── https://localhost:6443/console/
                │
                ├── /console/              Static SPA (Axum ServeDir)
                ├── /api/v1/pods           K8s REST API (same server)
                ├── /api/v1/pods?watch=1   Watch stream (chunked HTTP)
                └── /apis/apps/v1/...      API group resources
```

### Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 19, TypeScript 5.9 |
| Bundler | Vite |
| State | Zustand (UI state) + TanStack Query (server state) |
| Styling | Tailwind CSS + Radix UI |
| Charts | Recharts (pie, area, bar) |
| Theme | WALL-E earth tones matching the docs site |
| Serving | Axum `tower-http::ServeDir` with SPA fallback |

### K8s API Client

The console uses the standard Kubernetes REST protocol — no custom API endpoints:
- **Resource Discovery** — fetches `/api/v1` and `/apis` to discover all resource types (including CRDs), cached 5 minutes
- **Watch Streams** — chunked HTTP with newline-delimited JSON, resourceVersion tracking, bookmark support, exponential backoff reconnection (1s → 30s max), 410 Gone recovery
- **Authentication** — reads JWT from `sessionStorage`, passes as `Authorization: Bearer`. No token needed in `--skip-auth` mode

### Console Deployment

| Method | How |
|---|---|
| Docker Compose | Console is built into the API server image (multi-stage Dockerfile). `--console-dir /app/console` is passed automatically. |
| All-in-one binary | Pass `--console-dir ./console/dist` to serve the SPA. |
| Development | Run `npm run dev` in `console/` for hot-reload on port 3000 with API proxy to localhost:6443. |

### Building from Source

```bash
cd console
npm install
npm run build
# Output: console/dist/ (~800KB JS + ~18KB CSS, gzipped ~230KB)
```
