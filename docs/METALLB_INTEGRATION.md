# MetalLB Integration with Rusternetes


> **Tip:** You can manage related resources through the [web console](CONSOLE_USER_GUIDE.md).
This guide explains how to use MetalLB with Rusternetes to provide LoadBalancer services without requiring a cloud provider. MetalLB is a production-ready, battle-tested load balancer implementation for bare-metal and on-premises Kubernetes clusters.

## Overview

**MetalLB** is a network load balancer implementation for Kubernetes that:
- Provides LoadBalancer service type support for non-cloud environments
- Supports Layer 2 (ARP/NDP) and BGP modes
- Automatically assigns external IPs to LoadBalancer services
- Works with standard Kubernetes APIs (no custom changes needed)

**Why use MetalLB with Rusternetes?**
- Development and testing without cloud provider costs
- On-premises deployments
- Edge computing scenarios
- Local Kubernetes learning environments

## Architecture

MetalLB consists of two components:

1. **Controller**: Watches for LoadBalancer services and assigns IP addresses from configured pools
2. **Speaker**: Announces assigned IPs to the network using Layer 2 (ARP) or BGP protocols

```
┌─────────────────────────────────────────────────────────┐
│                    Rusternetes Cluster                   │
│                                                           │
│  ┌──────────────┐      ┌─────────────────────────────┐  │
│  │   MetalLB    │      │    LoadBalancer Service     │  │
│  │  Controller  │─────▶│   (type: LoadBalancer)      │  │
│  └──────────────┘      │   External IP: 192.168.1.240│  │
│         │              └─────────────────────────────┘  │
│         │ Assigns IP                    │                │
│         │                               │                │
│         ▼                               ▼                │
│  ┌──────────────┐              ┌──────────────┐         │
│  │IPAddressPool │              │  kube-proxy  │         │
│  │192.168.1.240 │              │   iptables   │         │
│  │     to       │              └──────────────┘         │
│  │192.168.1.250 │                      │                │
│  └──────────────┘                      │                │
│         │                               │                │
│         │                               ▼                │
│         │                      ┌──────────────┐         │
│         └────────────────────▶ │   Speaker    │         │
│                                │  (DaemonSet) │         │
│                                │  Announces   │         │
│                                │  via ARP/BGP │         │
│                                └──────────────┘         │
└─────────────────────────────────────────────────────────┘
                                  │
                                  │ ARP/BGP
                                  ▼
                          External Network
```

## Prerequisites

1. **Rusternetes cluster running** with:
   - API server accessible
   - kube-proxy running and configured
   - Nodes with network connectivity

2. **kubectl configured** to access your Rusternetes cluster

3. **IP address range** available on your network for LoadBalancer services

## Installation

### Step 1: Install MetalLB

Install MetalLB using kubectl:

```bash
kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.14.3/config/manifests/metallb-native.yaml
```

This creates:
- `metallb-system` namespace
- MetalLB controller deployment
- MetalLB speaker daemonset
- Required RBAC resources and CRDs

Wait for MetalLB pods to be ready:

```bash
kubectl wait --namespace metallb-system \
  --for=condition=ready pod \
  --selector=app=metallb \
  --timeout=90s
```

### Step 2: Configure IP Address Pool

Choose an IP range that:
- Is on the same network as your Rusternetes nodes
- Does NOT overlap with DHCP ranges
- Is routable from clients that need to access services

Create an IPAddressPool configuration:

```bash
cat <<EOF | kubectl apply -f -
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: default-pool
  namespace: metallb-system
spec:
  addresses:
  # For local development (adjust to your network):
  - 192.168.1.240-192.168.1.250
  # Or use CIDR notation:
  # - 192.168.1.240/28
EOF
```

For **Docker/Podman environments**, use IPs from your container network:

```bash
# Find your container network range
podman network inspect podman | grep -A 2 subnet

# Example for typical 10.88.0.0/16 network:
cat <<EOF | kubectl apply -f -
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: podman-pool
  namespace: metallb-system
spec:
  addresses:
  - 10.88.100.1-10.88.100.50
EOF
```

### Step 3: Configure Layer 2 Advertisement

Enable Layer 2 mode (recommended for simple setups):

```bash
cat <<EOF | kubectl apply -f -
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: default-l2
  namespace: metallb-system
spec:
  ipAddressPools:
  - default-pool
EOF
```

## Verification

Check MetalLB is running:

```bash
kubectl get pods -n metallb-system
```

Expected output:
```
NAME                          READY   STATUS    RESTARTS   AGE
controller-5f7bb57799-xxxxx   1/1     Running   0          2m
speaker-xxxxx                 1/1     Running   0          2m
```

Check the configuration:

```bash
kubectl get ipaddresspools -n metallb-system
kubectl get l2advertisements -n metallb-system
```

## Usage Examples

### Example 1: Simple HTTP Service

Create a LoadBalancer service:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: web-service
  namespace: default
spec:
  type: LoadBalancer
  selector:
    app: web
  ports:
    - name: http
      protocol: TCP
      port: 80
      targetPort: 8080
```

Apply and check the external IP:

```bash
kubectl apply -f web-service.yaml
kubectl get service web-service
```

Output:
```
NAME          TYPE           CLUSTER-IP      EXTERNAL-IP      PORT(S)        AGE
web-service   LoadBalancer   10.96.100.123   192.168.1.240    80:30080/TCP   10s
```

MetalLB automatically assigned an IP from the pool!

### Example 2: Request Specific IP

You can request a specific IP from the pool:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: database-service
  namespace: default
spec:
  type: LoadBalancer
  loadBalancerIP: 192.168.1.245  # Must be in configured pool
  selector:
    app: database
  ports:
    - name: postgres
      protocol: TCP
      port: 5432
      targetPort: 5432
```

### Example 3: Shared IP (Multiple Services)

Share a single IP across multiple services on different ports:

```yaml
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: shared-pool
  namespace: metallb-system
spec:
  addresses:
  - 192.168.1.250/32
  serviceAllocation:
    priority: 100
    namespaces:
      - default
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: shared-l2
  namespace: metallb-system
spec:
  ipAddressPools:
  - shared-pool
```

Then use the annotation `metallb.universe.tf/allow-shared-ip: "key"`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: http-service
  namespace: default
  annotations:
    metallb.universe.tf/allow-shared-ip: "shared-services"
spec:
  type: LoadBalancer
  selector:
    app: web
  ports:
    - name: http
      port: 80
---
apiVersion: v1
kind: Service
metadata:
  name: https-service
  namespace: default
  annotations:
    metallb.universe.tf/allow-shared-ip: "shared-services"
spec:
  type: LoadBalancer
  selector:
    app: web
  ports:
    - name: https
      port: 443
```

Both services will share the same external IP on different ports.

## Rusternetes-Specific Considerations

### kube-proxy Compatibility

Rusternetes uses iptables-based kube-proxy, which works seamlessly with MetalLB. The flow is:

1. Client sends traffic to LoadBalancer external IP
2. MetalLB speaker responds to ARP requests for that IP
3. Traffic reaches the node
4. kube-proxy iptables rules forward to NodePort
5. Traffic reaches service endpoints

### Service NodePort Allocation

Rusternetes automatically allocates NodePorts for LoadBalancer services. MetalLB relies on these NodePorts:

```
External IP:80 → Node:30080 → Pod:8080
     ↑              ↑            ↑
  MetalLB      kube-proxy    Actual pod
```

### Testing Locally

For local development with Podman:

```bash
# Start Rusternetes cluster
podman-compose -f compose.yml up -d

# Install MetalLB
kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.14.3/config/manifests/metallb-native.yaml

# Configure with Podman network IPs
kubectl apply -f examples/metallb-config-podman.yaml

# Test with example service
kubectl apply -f examples/networking/test-loadbalancer-service.yaml

# Get the external IP
EXTERNAL_IP=$(kubectl get svc test-loadbalancer-service -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Test access (from host or another container)
curl http://$EXTERNAL_IP
```

## Troubleshooting

### Service Shows "Pending" External IP

```bash
kubectl get service my-service
NAME         TYPE           CLUSTER-IP      EXTERNAL-IP   PORT(S)
my-service   LoadBalancer   10.96.100.123   <pending>     80:30080/TCP
```

**Possible causes:**

1. **MetalLB not installed or not running:**
   ```bash
   kubectl get pods -n metallb-system
   ```

2. **No IP pools configured:**
   ```bash
   kubectl get ipaddresspools -n metallb-system
   ```

3. **IP pool exhausted:**
   Check MetalLB controller logs:
   ```bash
   kubectl logs -n metallb-system -l app=metallb,component=controller
   ```

4. **Invalid IP range:**
   Ensure your IP range is valid and on your network

### Cannot Reach External IP

1. **Check MetalLB speaker logs:**
   ```bash
   kubectl logs -n metallb-system -l app=metallb,component=speaker
   ```

2. **Verify ARP announcement:**
   ```bash
   # From a machine on the same network
   arping <EXTERNAL_IP>
   ```

3. **Check kube-proxy rules:**
   ```bash
   # On a Rusternetes node
   iptables-save | grep <NODEPORT>
   ```

4. **Verify service endpoints:**
   ```bash
   kubectl get endpoints my-service
   ```

### MetalLB Controller Not Starting

Check for RBAC or CRD issues:

```bash
kubectl describe pod -n metallb-system -l component=controller
kubectl get crds | grep metallb
```

## Configuration Examples

Example configurations are provided in `examples/metallb/`:

- `metallb-config-local.yaml` - For local bare-metal deployments
- `metallb-config-podman.yaml` - For Podman/Docker environments
- `metallb-config-bgp.yaml` - BGP mode configuration (advanced)

## Advanced Configuration

### BGP Mode

For production environments with BGP routing:

```yaml
apiVersion: metallb.io/v1beta2
kind: BGPPeer
metadata:
  name: router
  namespace: metallb-system
spec:
  myASN: 64500
  peerASN: 64501
  peerAddress: 192.168.1.1
---
apiVersion: metallb.io/v1beta1
kind: BGPAdvertisement
metadata:
  name: bgp-advert
  namespace: metallb-system
spec:
  ipAddressPools:
  - default-pool
```

### Multiple IP Pools

Separate pools for different environments:

```yaml
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: production-pool
  namespace: metallb-system
spec:
  addresses:
  - 192.168.1.100-192.168.1.150
  serviceAllocation:
    priority: 100
    namespaces:
      - production
---
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: development-pool
  namespace: metallb-system
spec:
  addresses:
  - 192.168.1.200-192.168.1.250
  serviceAllocation:
    priority: 50
    namespaces:
      - development
```

## Comparison: MetalLB vs Cloud Provider

| Feature | MetalLB | Cloud Provider (AWS/GCP/Azure) |
|---------|---------|-------------------------------|
| Cost | Free | Pay per load balancer |
| Setup | Install once | Automatic with credentials |
| IP Management | Manual pool configuration | Automatic from cloud |
| Health Checks | Basic (kube-proxy) | Advanced cloud health checks |
| SSL/TLS Termination | Requires Ingress | Native support |
| Geographic Distribution | Single location | Multi-region |
| Use Case | Local, edge, development | Production cloud deployments |

## Resources

- [MetalLB Official Documentation](https://metallb.universe.tf/)
- [MetalLB GitHub](https://github.com/metallb/metallb)
- [Kubernetes Service Types](https://kubernetes.io/docs/concepts/services-networking/service/#loadbalancer)

## Next Steps

1. Install MetalLB in your Rusternetes cluster
2. Configure IP address pool for your network
3. Test with the example LoadBalancer service
4. Explore advanced features like IP sharing and BGP mode
