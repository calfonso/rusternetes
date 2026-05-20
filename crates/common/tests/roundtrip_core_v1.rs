//! JSON roundtrip tests for core/v1 resources.
//!
//! Mirrors the test layer at upstream
//! `staging/src/k8s.io/apimachinery/pkg/api/apitesting/roundtrip/` which
//! verifies that every typed resource can survive a JSON encode -> decode ->
//! encode -> decode cycle without mutation.
//!
//! For each resource we exercise representative payloads (minimal, full-
//! featured, list wrapper, nested edge cases) and assert four things:
//!
//!   1. `serde_json::from_str::<T>(&fixture)` succeeds (initial decode)
//!   2. `serde_json::to_string(&decoded)` succeeds (re-encode)
//!   3. `serde_json::from_str::<T>(&re_encoded)` succeeds (second decode)
//!   4. The two decoded values are equal -- comparing via `serde_json::Value`
//!      because not every core/v1 struct derives `PartialEq` yet.
//!
//! The fixtures use the same camelCase wire format the api-server emits
//! (`podIP`, `containerID`, `apiVersion`, etc.). This layer catches regressions
//! at the serde boundary before any router/storage code is involved.

use rusternetes_common::resources::{
    ConfigMap, Endpoints, Event, LimitRange, Namespace, Node, PersistentVolume,
    PersistentVolumeClaim, Pod, ResourceQuota, Secret, Service, ServiceAccount,
};
use serde::{de::DeserializeOwned, Serialize};

/// Run the four-step roundtrip assertion for a typed payload.
///
/// 1. decode fixture -> `T`
/// 2. encode `T` -> JSON
/// 3. decode JSON -> `T`
/// 4. compare the two decoded `T`s via their `serde_json::Value` projection
///
/// We compare as `Value` rather than via `PartialEq` because many core/v1
/// structs (Pod, Service, Node, ...) don't currently derive `PartialEq` and
/// the goal of the layer is to verify the *wire* shape survives -- that's
/// exactly what Value-equality measures.
fn assert_roundtrip<T>(fixture: &str)
where
    T: Serialize + DeserializeOwned,
{
    let decoded: T = serde_json::from_str(fixture)
        .unwrap_or_else(|e| panic!("initial decode failed: {e}\nfixture: {fixture}"));
    let re_encoded = serde_json::to_string(&decoded).expect("re-encode failed");
    let re_decoded: T = serde_json::from_str(&re_encoded)
        .unwrap_or_else(|e| panic!("second decode failed: {e}\nre_encoded: {re_encoded}"));

    let decoded_value = serde_json::to_value(&decoded).expect("decoded -> Value");
    let re_decoded_value = serde_json::to_value(&re_decoded).expect("re_decoded -> Value");
    assert_eq!(
        decoded_value, re_decoded_value,
        "roundtrip not stable\nfirst:  {decoded_value}\nsecond: {re_decoded_value}",
    );
}

// =============================================================================
// Pod
// =============================================================================

#[test]
fn roundtrip_pod_minimal() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "minimal", "namespace": "default"},
        "spec": {
            "containers": [{"name": "c", "image": "nginx"}]
        }
    }"#;
    assert_roundtrip::<Pod>(fixture);
}

#[test]
fn roundtrip_pod_with_node_name_and_status() {
    // Status uses camelCase IP abbreviations: podIP / hostIP / containerID.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "scheduled", "namespace": "default", "uid": "abc-123"},
        "spec": {
            "nodeName": "node-1",
            "containers": [{"name": "c", "image": "nginx:1.25"}]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.244.0.5",
            "podIPs": [{"ip": "10.244.0.5"}],
            "hostIP": "192.168.1.10",
            "hostIPs": [{"ip": "192.168.1.10"}]
        }
    }"#;
    assert_roundtrip::<Pod>(fixture);
}

#[test]
fn roundtrip_pod_full_spec() {
    // Exercises probes, env, volumeMounts, resources, restart policy,
    // tolerations, affinity, securityContext.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "full",
            "namespace": "default",
            "labels": {"app": "web", "tier": "frontend"},
            "annotations": {"note": "primary"}
        },
        "spec": {
            "restartPolicy": "Always",
            "terminationGracePeriodSeconds": 30,
            "dnsPolicy": "ClusterFirst",
            "serviceAccountName": "default",
            "automountServiceAccountToken": true,
            "nodeSelector": {"disktype": "ssd"},
            "tolerations": [
                {"key": "node-role", "operator": "Exists", "effect": "NoSchedule"}
            ],
            "containers": [{
                "name": "app",
                "image": "myapp:v1",
                "command": ["/usr/bin/app"],
                "args": ["--port", "8080"],
                "ports": [{"containerPort": 8080, "protocol": "TCP", "name": "http"}],
                "env": [
                    {"name": "LOG_LEVEL", "value": "info"},
                    {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}
                ],
                "resources": {
                    "requests": {"cpu": "100m", "memory": "128Mi"},
                    "limits":   {"cpu": "500m", "memory": "512Mi"}
                },
                "livenessProbe": {
                    "httpGet": {"path": "/healthz", "port": 8080},
                    "initialDelaySeconds": 10,
                    "periodSeconds": 5
                },
                "readinessProbe": {
                    "tcpSocket": {"port": 8080},
                    "initialDelaySeconds": 5
                },
                "volumeMounts": [
                    {"name": "data", "mountPath": "/var/data", "readOnly": false}
                ],
                "securityContext": {
                    "runAsUser": 1000,
                    "runAsNonRoot": true,
                    "allowPrivilegeEscalation": false
                }
            }],
            "volumes": [{
                "name": "data",
                "emptyDir": {}
            }]
        }
    }"#;
    assert_roundtrip::<Pod>(fixture);
}

#[test]
fn roundtrip_pod_init_and_ephemeral_containers() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "init", "namespace": "default"},
        "spec": {
            "initContainers": [
                {"name": "setup", "image": "busybox", "command": ["sh", "-c", "echo init"]}
            ],
            "containers": [{"name": "main", "image": "nginx"}],
            "ephemeralContainers": [
                {"name": "debugger", "image": "busybox", "command": ["sh"]}
            ]
        }
    }"#;
    assert_roundtrip::<Pod>(fixture);
}

#[test]
fn roundtrip_pod_with_affinity_and_topology_spread() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "affine", "namespace": "default"},
        "spec": {
            "containers": [{"name": "c", "image": "nginx"}],
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [{
                            "matchExpressions": [
                                {"key": "kubernetes.io/os", "operator": "In", "values": ["linux"]}
                            ]
                        }]
                    }
                }
            },
            "topologySpreadConstraints": [{
                "maxSkew": 1,
                "topologyKey": "topology.kubernetes.io/zone",
                "whenUnsatisfiable": "DoNotSchedule",
                "labelSelector": {"matchLabels": {"app": "web"}}
            }]
        }
    }"#;
    assert_roundtrip::<Pod>(fixture);
}

#[test]
fn roundtrip_pod_with_container_status() {
    // Conformance tests round-trip ContainerStatus with containerID + image IDs.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "with-status", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ],
            "containerStatuses": [{
                "name": "c",
                "image": "nginx",
                "imageID": "docker-pullable://nginx@sha256:abc",
                "containerID": "containerd://1234",
                "ready": true,
                "restartCount": 0,
                "started": true
            }]
        }
    }"#;
    assert_roundtrip::<Pod>(fixture);
}

// =============================================================================
// ConfigMap
// =============================================================================

#[test]
fn roundtrip_configmap_minimal() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "minimal", "namespace": "default"}
    }"#;
    assert_roundtrip::<ConfigMap>(fixture);
}

#[test]
fn roundtrip_configmap_with_data() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "app-config", "namespace": "default"},
        "data": {
            "log.level": "info",
            "feature.beta": "enabled"
        }
    }"#;
    assert_roundtrip::<ConfigMap>(fixture);
}

#[test]
fn roundtrip_configmap_with_binary_data() {
    // binaryData values are base64-encoded on the wire.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "bin", "namespace": "default"},
        "data": {"text": "hello"},
        "binaryData": {"blob": "aGVsbG8="}
    }"#;
    assert_roundtrip::<ConfigMap>(fixture);
}

#[test]
fn roundtrip_configmap_immutable() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "frozen", "namespace": "default"},
        "data": {"k": "v"},
        "immutable": true
    }"#;
    assert_roundtrip::<ConfigMap>(fixture);
}

#[test]
fn roundtrip_configmap_with_owner_reference() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "owned",
            "namespace": "default",
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "web",
                "uid": "11111111-2222-3333-4444-555555555555",
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "data": {"k": "v"}
    }"#;
    assert_roundtrip::<ConfigMap>(fixture);
}

// =============================================================================
// Secret
// =============================================================================

#[test]
fn roundtrip_secret_minimal() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "minimal", "namespace": "default"},
        "type": "Opaque"
    }"#;
    assert_roundtrip::<Secret>(fixture);
}

#[test]
fn roundtrip_secret_opaque_with_data() {
    // Secret.data values are base64-encoded on the wire.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "creds", "namespace": "default"},
        "type": "Opaque",
        "data": {
            "username": "YWRtaW4=",
            "password": "cGFzc3dvcmQ="
        }
    }"#;
    assert_roundtrip::<Secret>(fixture);
}

#[test]
fn roundtrip_secret_with_string_data() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "tls", "namespace": "default"},
        "type": "kubernetes.io/tls",
        "stringData": {
            "tls.crt": "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----",
            "tls.key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----"
        }
    }"#;
    assert_roundtrip::<Secret>(fixture);
}

#[test]
fn roundtrip_secret_service_account_token() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "default-token-abcde",
            "namespace": "default",
            "annotations": {
                "kubernetes.io/service-account.name": "default",
                "kubernetes.io/service-account.uid": "uuid-12345"
            }
        },
        "type": "kubernetes.io/service-account-token",
        "data": {
            "ca.crt": "Y2EuY3J0",
            "namespace": "ZGVmYXVsdA==",
            "token": "dG9rZW4="
        }
    }"#;
    assert_roundtrip::<Secret>(fixture);
}

#[test]
fn roundtrip_secret_immutable() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "frozen", "namespace": "default"},
        "type": "Opaque",
        "immutable": true,
        "data": {"k": "dg=="}
    }"#;
    assert_roundtrip::<Secret>(fixture);
}

// =============================================================================
// Service
// =============================================================================

#[test]
fn roundtrip_service_clusterip_minimal() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "minimal", "namespace": "default"},
        "spec": {
            "selector": {"app": "web"},
            "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP"}]
        }
    }"#;
    assert_roundtrip::<Service>(fixture);
}

#[test]
fn roundtrip_service_clusterip_assigned() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "assigned", "namespace": "default"},
        "spec": {
            "type": "ClusterIP",
            "selector": {"app": "web"},
            "clusterIP": "10.96.0.10",
            "clusterIPs": ["10.96.0.10"],
            "ipFamilies": ["IPv4"],
            "ipFamilyPolicy": "SingleStack",
            "ports": [
                {"name": "http", "port": 80, "targetPort": 8080, "protocol": "TCP"},
                {"name": "https", "port": 443, "targetPort": "https", "protocol": "TCP"}
            ]
        }
    }"#;
    assert_roundtrip::<Service>(fixture);
}

#[test]
fn roundtrip_service_nodeport() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "np", "namespace": "default"},
        "spec": {
            "type": "NodePort",
            "selector": {"app": "web"},
            "externalTrafficPolicy": "Local",
            "ports": [{
                "name": "http",
                "port": 80,
                "targetPort": 8080,
                "nodePort": 31000,
                "protocol": "TCP",
                "appProtocol": "http"
            }]
        }
    }"#;
    assert_roundtrip::<Service>(fixture);
}

#[test]
fn roundtrip_service_loadbalancer_with_status() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "lb", "namespace": "default"},
        "spec": {
            "type": "LoadBalancer",
            "selector": {"app": "web"},
            "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP"}],
            "externalTrafficPolicy": "Cluster",
            "allocateLoadBalancerNodePorts": true,
            "loadBalancerSourceRanges": ["10.0.0.0/8", "192.168.0.0/16"]
        },
        "status": {
            "loadBalancer": {
                "ingress": [
                    {"ip": "203.0.113.10", "ports": [{"port": 80, "protocol": "TCP"}]},
                    {"hostname": "lb.example.com"}
                ]
            }
        }
    }"#;
    assert_roundtrip::<Service>(fixture);
}

#[test]
fn roundtrip_service_externalname() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "extn", "namespace": "default"},
        "spec": {
            "type": "ExternalName",
            "externalName": "db.prod.svc.example.com"
        }
    }"#;
    assert_roundtrip::<Service>(fixture);
}

#[test]
fn roundtrip_service_headless_with_session_affinity() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "headless", "namespace": "default"},
        "spec": {
            "type": "ClusterIP",
            "clusterIP": "None",
            "selector": {"app": "db"},
            "ports": [{"port": 5432, "targetPort": 5432, "protocol": "TCP"}],
            "sessionAffinity": "ClientIP",
            "sessionAffinityConfig": {
                "clientIP": {"timeoutSeconds": 10800}
            },
            "publishNotReadyAddresses": true
        }
    }"#;
    assert_roundtrip::<Service>(fixture);
}

// =============================================================================
// Namespace
// =============================================================================

#[test]
fn roundtrip_namespace_minimal() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "default"}
    }"#;
    assert_roundtrip::<Namespace>(fixture);
}

#[test]
fn roundtrip_namespace_active() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": "kube-system",
            "labels": {"kubernetes.io/metadata.name": "kube-system"}
        },
        "spec": {"finalizers": ["kubernetes"]},
        "status": {"phase": "Active"}
    }"#;
    assert_roundtrip::<Namespace>(fixture);
}

#[test]
fn roundtrip_namespace_terminating() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": "stale",
            "deletionTimestamp": "2026-01-01T00:00:00Z"
        },
        "spec": {"finalizers": ["kubernetes"]},
        "status": {
            "phase": "Terminating",
            "conditions": [{
                "type": "NamespaceDeletionDiscoveryFailure",
                "status": "True",
                "reason": "DiscoveryFailed",
                "message": "Discovery failed for some groups"
            }]
        }
    }"#;
    assert_roundtrip::<Namespace>(fixture);
}

#[test]
fn roundtrip_namespace_with_annotations() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": "tenant-a",
            "labels": {"team": "platform"},
            "annotations": {"cost-center": "1234"}
        },
        "status": {"phase": "Active"}
    }"#;
    assert_roundtrip::<Namespace>(fixture);
}

// =============================================================================
// Node
// =============================================================================

#[test]
fn roundtrip_node_minimal() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-1"},
        "spec": null
    }"#;
    assert_roundtrip::<Node>(fixture);
}

#[test]
fn roundtrip_node_with_taints_and_cidr() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-1", "labels": {"kubernetes.io/hostname": "node-1"}},
        "spec": {
            "podCIDR": "10.244.0.0/24",
            "podCIDRs": ["10.244.0.0/24"],
            "providerID": "rusternetes://node-1",
            "unschedulable": false,
            "taints": [
                {"key": "node-role.kubernetes.io/control-plane", "effect": "NoSchedule", "value": null}
            ]
        }
    }"#;
    assert_roundtrip::<Node>(fixture);
}

#[test]
fn roundtrip_node_full_status() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-1"},
        "spec": {"podCIDR": "10.244.0.0/24"},
        "status": {
            "capacity": {"cpu": "4", "memory": "8Gi", "pods": "110"},
            "allocatable": {"cpu": "3800m", "memory": "7Gi", "pods": "110"},
            "addresses": [
                {"type": "InternalIP", "address": "192.168.1.10"},
                {"type": "Hostname", "address": "node-1"}
            ],
            "conditions": [
                {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "kubelet is healthy"},
                {"type": "MemoryPressure", "status": "False"},
                {"type": "DiskPressure", "status": "False"}
            ],
            "nodeInfo": {
                "machineID": "abc",
                "systemUUID": "def",
                "bootID": "ghi",
                "kernelVersion": "6.1.0",
                "osImage": "Ubuntu 22.04",
                "containerRuntimeVersion": "containerd://1.7",
                "kubeletVersion": "v1.35.0",
                "kubeProxyVersion": "v1.35.0",
                "operatingSystem": "linux",
                "architecture": "amd64"
            },
            "daemonEndpoints": {"kubeletEndpoint": {"Port": 10250}}
        }
    }"#;
    assert_roundtrip::<Node>(fixture);
}

#[test]
fn roundtrip_node_with_images() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-img"},
        "spec": {},
        "status": {
            "images": [
                {"names": ["nginx:latest", "nginx@sha256:abc"], "sizeBytes": 12345678},
                {"names": ["busybox:1.36"], "sizeBytes": 1024000}
            ]
        }
    }"#;
    assert_roundtrip::<Node>(fixture);
}

#[test]
fn roundtrip_node_with_runtime_handlers() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-rh"},
        "spec": {},
        "status": {
            "runtimeHandlers": [
                {"name": "runc", "features": {"recursiveReadOnlyMounts": true, "userNamespaces": false}},
                {"name": "kata"}
            ],
            "features": {"supplementalGroupsPolicy": true}
        }
    }"#;
    assert_roundtrip::<Node>(fixture);
}

// =============================================================================
// Event
// =============================================================================

#[test]
fn roundtrip_event_core_v1_minimal() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": "evt.0001", "namespace": "default"},
        "involvedObject": {
            "kind": "Pod",
            "namespace": "default",
            "name": "web-0",
            "uid": "11111111-2222-3333-4444-555555555555"
        },
        "reason": "Scheduled",
        "message": "Successfully assigned default/web-0 to node-1",
        "type": "Normal",
        "source": {"component": "default-scheduler"},
        "count": 1,
        "firstTimestamp": "2026-01-01T00:00:00Z",
        "lastTimestamp": "2026-01-01T00:00:00Z"
    }"#;
    assert_roundtrip::<Event>(fixture);
}

#[test]
fn roundtrip_event_warning_with_series() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": "evt.warn", "namespace": "default"},
        "involvedObject": {"kind": "Pod", "namespace": "default", "name": "broken", "uid": "u"},
        "reason": "BackOff",
        "message": "Back-off restarting failed container",
        "type": "Warning",
        "source": {"component": "kubelet", "host": "node-1"},
        "count": 5,
        "firstTimestamp": "2026-01-01T00:00:00Z",
        "lastTimestamp": "2026-01-01T00:05:00Z",
        "series": {"count": 5, "lastObservedTime": "2026-01-01T00:05:00.000000Z"}
    }"#;
    assert_roundtrip::<Event>(fixture);
}

#[test]
fn roundtrip_event_events_k8s_io_v1_format() {
    // events.k8s.io/v1 introduced reportingController/reportingInstance/regarding/note/eventTime.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": "evt.new", "namespace": "default"},
        "involvedObject": {"kind": "Pod", "namespace": "default", "name": "web-0", "uid": "u"},
        "regarding": {"kind": "Pod", "namespace": "default", "name": "web-0", "uid": "u"},
        "related": {"kind": "Node", "name": "node-1", "uid": "n"},
        "reason": "Started",
        "note": "Started container nginx",
        "message": "Started container nginx",
        "type": "Normal",
        "action": "Binding",
        "eventTime": "2026-01-01T00:00:00.123456Z",
        "reportingComponent": "kubelet",
        "reportingInstance": "kubelet-node-1",
        "source": {"component": "kubelet", "host": "node-1"},
        "count": 1
    }"#;
    assert_roundtrip::<Event>(fixture);
}

#[test]
fn roundtrip_event_zero_count() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": "evt.zero", "namespace": "default"},
        "involvedObject": {"kind": "Pod", "name": "p", "uid": "u"},
        "reason": "",
        "message": "",
        "type": "Normal",
        "source": {"component": ""},
        "count": 0
    }"#;
    assert_roundtrip::<Event>(fixture);
}

// =============================================================================
// ServiceAccount
// =============================================================================

#[test]
fn roundtrip_service_account_minimal() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "default", "namespace": "default"}
    }"#;
    assert_roundtrip::<ServiceAccount>(fixture);
}

#[test]
fn roundtrip_service_account_with_secrets() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "builder", "namespace": "ci"},
        "secrets": [
            {"kind": "Secret", "namespace": "ci", "name": "builder-token-abc"}
        ],
        "imagePullSecrets": [{"name": "regcred"}],
        "automountServiceAccountToken": false
    }"#;
    assert_roundtrip::<ServiceAccount>(fixture);
}

#[test]
fn roundtrip_service_account_with_owner_refs() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": "owned-sa",
            "namespace": "default",
            "uid": "abc",
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "Pod",
                "name": "p",
                "uid": "p-uid",
                "controller": false,
                "blockOwnerDeletion": false
            }]
        }
    }"#;
    assert_roundtrip::<ServiceAccount>(fixture);
}

// =============================================================================
// PersistentVolume
// =============================================================================

#[test]
fn roundtrip_persistent_volume_hostpath() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "pv-host"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": "manual",
            "hostPath": {"path": "/mnt/data", "type": "DirectoryOrCreate"}
        }
    }"#;
    assert_roundtrip::<PersistentVolume>(fixture);
}

#[test]
fn roundtrip_persistent_volume_nfs() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "pv-nfs"},
        "spec": {
            "capacity": {"storage": "100Gi"},
            "accessModes": ["ReadWriteMany"],
            "persistentVolumeReclaimPolicy": "Recycle",
            "nfs": {"server": "nfs.example.com", "path": "/exports/data", "readOnly": false},
            "mountOptions": ["nfsvers=4.1"],
            "volumeMode": "Filesystem"
        }
    }"#;
    assert_roundtrip::<PersistentVolume>(fixture);
}

#[test]
fn roundtrip_persistent_volume_csi() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "pv-csi"},
        "spec": {
            "capacity": {"storage": "10Gi"},
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Delete",
            "storageClassName": "fast",
            "csi": {
                "driver": "ebs.csi.aws.com",
                "volumeHandle": "vol-abc123",
                "fsType": "ext4",
                "readOnly": false,
                "volumeAttributes": {"type": "gp3"}
            },
            "volumeMode": "Filesystem"
        },
        "status": {"phase": "Available"}
    }"#;
    assert_roundtrip::<PersistentVolume>(fixture);
}

#[test]
fn roundtrip_persistent_volume_bound_with_claim_ref() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "pv-bound"},
        "spec": {
            "capacity": {"storage": "5Gi"},
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": "standard",
            "hostPath": {"path": "/mnt/pv-bound"},
            "claimRef": {
                "kind": "PersistentVolumeClaim",
                "namespace": "default",
                "name": "my-pvc",
                "uid": "abc",
                "apiVersion": "v1",
                "resourceVersion": "42"
            }
        },
        "status": {"phase": "Bound"}
    }"#;
    assert_roundtrip::<PersistentVolume>(fixture);
}

#[test]
fn roundtrip_persistent_volume_with_node_affinity() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "pv-local"},
        "spec": {
            "capacity": {"storage": "1Gi"},
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Delete",
            "storageClassName": "local-storage",
            "local": {"path": "/mnt/disks/ssd1", "fsType": "ext4"},
            "nodeAffinity": {
                "required": {
                    "nodeSelectorTerms": [{
                        "matchExpressions": [
                            {"key": "kubernetes.io/hostname", "operator": "In", "values": ["node-1"]}
                        ]
                    }]
                }
            }
        }
    }"#;
    assert_roundtrip::<PersistentVolume>(fixture);
}

// =============================================================================
// PersistentVolumeClaim
// =============================================================================

#[test]
fn roundtrip_persistent_volume_claim_minimal() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "pvc-min", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}}
        }
    }"#;
    assert_roundtrip::<PersistentVolumeClaim>(fixture);
}

#[test]
fn roundtrip_persistent_volume_claim_with_storage_class() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "pvc-sc", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "10Gi"}, "limits": {"storage": "10Gi"}},
            "storageClassName": "fast",
            "volumeMode": "Filesystem"
        }
    }"#;
    assert_roundtrip::<PersistentVolumeClaim>(fixture);
}

#[test]
fn roundtrip_persistent_volume_claim_with_selector() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "pvc-sel", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}},
            "selector": {
                "matchLabels": {"tier": "gold"},
                "matchExpressions": [
                    {"key": "env", "operator": "In", "values": ["prod", "staging"]}
                ]
            }
        }
    }"#;
    assert_roundtrip::<PersistentVolumeClaim>(fixture);
}

#[test]
fn roundtrip_persistent_volume_claim_bound_status() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "pvc-bound", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "1Gi"}},
            "volumeName": "pv-bound",
            "storageClassName": "standard"
        },
        "status": {
            "phase": "Bound",
            "accessModes": ["ReadWriteOnce"],
            "capacity": {"storage": "1Gi"}
        }
    }"#;
    assert_roundtrip::<PersistentVolumeClaim>(fixture);
}

#[test]
fn roundtrip_persistent_volume_claim_with_data_source() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": "pvc-clone", "namespace": "default"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "5Gi"}},
            "storageClassName": "fast",
            "dataSource": {
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": "snap-1"
            },
            "dataSourceRef": {
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": "snap-1"
            }
        }
    }"#;
    assert_roundtrip::<PersistentVolumeClaim>(fixture);
}

// =============================================================================
// Endpoints
// =============================================================================

#[test]
fn roundtrip_endpoints_empty() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "empty", "namespace": "default"},
        "subsets": []
    }"#;
    assert_roundtrip::<Endpoints>(fixture);
}

#[test]
fn roundtrip_endpoints_single_subset() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "web", "namespace": "default"},
        "subsets": [{
            "addresses": [
                {"ip": "10.244.0.5", "nodeName": "node-1"},
                {"ip": "10.244.0.6", "nodeName": "node-2"}
            ],
            "ports": [
                {"name": "http", "port": 8080, "protocol": "TCP"}
            ]
        }]
    }"#;
    assert_roundtrip::<Endpoints>(fixture);
}

#[test]
fn roundtrip_endpoints_with_not_ready_addresses() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "mixed", "namespace": "default"},
        "subsets": [{
            "addresses": [{"ip": "10.244.0.5", "nodeName": "node-1"}],
            "notReadyAddresses": [{"ip": "10.244.0.6", "nodeName": "node-2", "hostname": "pod-1"}],
            "ports": [{"name": "http", "port": 80, "protocol": "TCP", "appProtocol": "http"}]
        }]
    }"#;
    assert_roundtrip::<Endpoints>(fixture);
}

#[test]
fn roundtrip_endpoints_with_target_ref() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "ref", "namespace": "default"},
        "subsets": [{
            "addresses": [{
                "ip": "10.244.0.5",
                "nodeName": "node-1",
                "targetRef": {"kind": "Pod", "namespace": "default", "name": "web-0", "uid": "p-uid"}
            }],
            "ports": [{"port": 8080, "protocol": "TCP"}]
        }]
    }"#;
    assert_roundtrip::<Endpoints>(fixture);
}

#[test]
fn roundtrip_endpoints_multiple_subsets() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "multi", "namespace": "default"},
        "subsets": [
            {
                "addresses": [{"ip": "10.0.0.1"}, {"ip": "10.0.0.2"}],
                "ports": [{"name": "http", "port": 80, "protocol": "TCP"}]
            },
            {
                "addresses": [{"ip": "10.0.0.3"}],
                "ports": [{"name": "metrics", "port": 9090, "protocol": "TCP"}]
            }
        ]
    }"#;
    assert_roundtrip::<Endpoints>(fixture);
}

// =============================================================================
// LimitRange
// =============================================================================

#[test]
fn roundtrip_limit_range_container() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "container-defaults", "namespace": "default"},
        "spec": {
            "limits": [{
                "type": "Container",
                "max": {"cpu": "2", "memory": "1Gi"},
                "min": {"cpu": "100m", "memory": "64Mi"},
                "default": {"cpu": "500m", "memory": "256Mi"},
                "defaultRequest": {"cpu": "200m", "memory": "128Mi"},
                "maxLimitRequestRatio": {"cpu": "10"}
            }]
        }
    }"#;
    assert_roundtrip::<LimitRange>(fixture);
}

#[test]
fn roundtrip_limit_range_pod() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "pod-limits", "namespace": "default"},
        "spec": {
            "limits": [{
                "type": "Pod",
                "max": {"cpu": "4", "memory": "8Gi"}
            }]
        }
    }"#;
    assert_roundtrip::<LimitRange>(fixture);
}

#[test]
fn roundtrip_limit_range_pvc() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "pvc-limits", "namespace": "default"},
        "spec": {
            "limits": [{
                "type": "PersistentVolumeClaim",
                "max": {"storage": "100Gi"},
                "min": {"storage": "1Gi"}
            }]
        }
    }"#;
    assert_roundtrip::<LimitRange>(fixture);
}

#[test]
fn roundtrip_limit_range_multiple_items() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "all", "namespace": "default"},
        "spec": {
            "limits": [
                {"type": "Pod", "max": {"cpu": "4"}},
                {"type": "Container", "default": {"memory": "256Mi"}, "defaultRequest": {"memory": "128Mi"}},
                {"type": "PersistentVolumeClaim", "min": {"storage": "1Gi"}, "max": {"storage": "10Gi"}}
            ]
        }
    }"#;
    assert_roundtrip::<LimitRange>(fixture);
}

// =============================================================================
// ResourceQuota
// =============================================================================

#[test]
fn roundtrip_resource_quota_compute() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": "compute-quota", "namespace": "default"},
        "spec": {
            "hard": {
                "pods": "10",
                "requests.cpu": "4",
                "requests.memory": "8Gi",
                "limits.cpu": "10",
                "limits.memory": "16Gi"
            }
        }
    }"#;
    assert_roundtrip::<ResourceQuota>(fixture);
}

#[test]
fn roundtrip_resource_quota_object_counts() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": "obj-quota", "namespace": "default"},
        "spec": {
            "hard": {
                "configmaps": "10",
                "persistentvolumeclaims": "4",
                "secrets": "10",
                "services": "5",
                "services.loadbalancers": "0"
            }
        }
    }"#;
    assert_roundtrip::<ResourceQuota>(fixture);
}

#[test]
fn roundtrip_resource_quota_with_scopes() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": "best-effort", "namespace": "default"},
        "spec": {
            "hard": {"pods": "1000"},
            "scopes": ["BestEffort"]
        }
    }"#;
    assert_roundtrip::<ResourceQuota>(fixture);
}

#[test]
fn roundtrip_resource_quota_with_scope_selector() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": "pc-quota", "namespace": "default"},
        "spec": {
            "hard": {"pods": "10"},
            "scopeSelector": {
                "matchExpressions": [{
                    "scopeName": "PriorityClass",
                    "operator": "In",
                    "values": ["high", "critical"]
                }]
            }
        }
    }"#;
    assert_roundtrip::<ResourceQuota>(fixture);
}

#[test]
fn roundtrip_resource_quota_with_status() {
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": "used", "namespace": "default"},
        "spec": {"hard": {"pods": "10", "requests.cpu": "4"}},
        "status": {
            "hard": {"pods": "10", "requests.cpu": "4"},
            "used": {"pods": "3", "requests.cpu": "1500m"}
        }
    }"#;
    assert_roundtrip::<ResourceQuota>(fixture);
}
