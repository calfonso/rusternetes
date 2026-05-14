# Chuck Norris Joke App

A web application that serves random Chuck Norris jokes, deployed on Rūsternetes. Demonstrates:

- **Deployment** with multiple replicas
- **Service** with ClusterIP load balancing
- **ConfigMap** for serving HTML content
- **Readiness and liveness probes**

## Deploy

```bash
export KUBECONFIG=~/.kube/rusternetes-config

kubectl apply -f examples/chuck-norris-app/namespace.yaml
kubectl apply -f examples/chuck-norris-app/configmap.yaml
kubectl apply -f examples/chuck-norris-app/deployment.yaml
kubectl apply -f examples/chuck-norris-app/service.yaml
```

## Access

Open in your browser (accept the self-signed certificate):

```
https://localhost:6443/api/v1/namespaces/chuck-norris/services/chuck-norris-svc/proxy/
```

## Check Status

```bash
kubectl get pods -n chuck-norris
kubectl get svc -n chuck-norris
```

## Architecture

```
┌──────────────────────────────────────────┐
│  Namespace: chuck-norris                 │
│                                          │
│  ConfigMap ──► nginx pods serve HTML     │
│                                          │
│  Service (ClusterIP :80)                 │
│     │                                    │
│     ├──► Pod 1 (nginx:1.25-alpine :80)   │
│     └──► Pod 2 (nginx:1.25-alpine :80)   │
└──────────────────────────────────────────┘
```

## Clean Up

```bash
kubectl delete namespace chuck-norris
```
