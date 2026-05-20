// JSON roundtrip tests for the long tail of group/version resources.
//
// These tests mirror upstream Kubernetes' "roundtrip" test layer: each
// hand-written JSON fixture is deserialized into the strongly-typed Rust
// representation, then re-serialized and deserialized a second time, with the
// final value compared structurally to the first decode. Equality is checked
// via `serde_json::Value` so that types that intentionally do not implement
// `PartialEq` (e.g. `StorageClass`, `CSIDriver`, `PriorityClass`) are still
// exercised. Any drift in `#[serde(rename_all = "camelCase")]`, alias handling,
// enum variant casing, or `skip_serializing_if` logic surfaces here as a
// failing assertion.
//
// Resources covered:
//   - Role, RoleBinding, ClusterRole, ClusterRoleBinding (rbac.authorization.k8s.io/v1)
//   - PriorityClass (scheduling.k8s.io/v1)
//   - StorageClass, CSIDriver, VolumeAttachment (storage.k8s.io/v1)
//   - RuntimeClass (node.k8s.io/v1)
//   - FlowSchema, PriorityLevelConfiguration (flowcontrol.apiserver.k8s.io/v1)
//   - ValidatingWebhookConfiguration, MutatingWebhookConfiguration (admissionregistration.k8s.io/v1)
//   - CustomResourceDefinition (apiextensions.k8s.io/v1)

use rusternetes_common::resources::{
    CSIDriver, ClusterRole, ClusterRoleBinding, CustomResourceDefinition, FlowSchema,
    MutatingWebhookConfiguration, PriorityClass, PriorityLevelConfiguration, Role, RoleBinding,
    RuntimeClass, StorageClass, ValidatingWebhookConfiguration, VolumeAttachment,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Roundtrip a hand-written JSON fixture through `T`.
///
/// Asserts (per the upstream contract):
///   1. `serde_json::from_str::<T>(fixture)` succeeds (`decoded`),
///   2. `serde_json::to_string(&decoded)` succeeds (`re_encoded`),
///   3. `serde_json::from_str::<T>(&re_encoded)` succeeds (`re_decoded`),
///   4. `decoded == re_decoded` — compared as `serde_json::Value` so the check
///      works uniformly for types whether or not they implement `PartialEq`.
fn assert_roundtrip<T: Serialize + DeserializeOwned>(fixture: &str) {
    let decoded: T = serde_json::from_str(fixture)
        .unwrap_or_else(|e| panic!("step 1 decode failed: {e}\nfixture: {fixture}"));

    let re_encoded =
        serde_json::to_string(&decoded).unwrap_or_else(|e| panic!("step 2 encode failed: {e}"));

    let re_decoded: T = serde_json::from_str(&re_encoded)
        .unwrap_or_else(|e| panic!("step 3 re-decode failed: {e}\nre_encoded: {re_encoded}"));

    let lhs = serde_json::to_value(&decoded).expect("decoded -> Value");
    let rhs = serde_json::to_value(&re_decoded).expect("re_decoded -> Value");
    assert_eq!(
        lhs, rhs,
        "step 4: decoded != re_decoded after JSON roundtrip"
    );
}

// =============================================================================
// rbac.authorization.k8s.io/v1
// =============================================================================

#[test]
fn roundtrip_role_minimal() {
    let fixture = r#"{
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": "pod-reader", "namespace": "default"},
        "rules": []
    }"#;
    assert_roundtrip::<Role>(fixture);
}

#[test]
fn roundtrip_role_with_rules() {
    let fixture = r#"{
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": "pod-reader", "namespace": "kube-system"},
        "rules": [
            {
                "verbs": ["get", "list", "watch"],
                "apiGroups": [""],
                "resources": ["pods", "pods/log"]
            },
            {
                "verbs": ["get"],
                "apiGroups": [""],
                "resources": ["pods"],
                "resourceNames": ["my-pod"]
            }
        ]
    }"#;
    assert_roundtrip::<Role>(fixture);
}

#[test]
fn roundtrip_rolebinding_user_subject() {
    let fixture = r#"{
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "read-pods", "namespace": "default"},
        "subjects": [
            {
                "kind": "User",
                "name": "jane",
                "apiGroup": "rbac.authorization.k8s.io"
            }
        ],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "pod-reader"
        }
    }"#;
    assert_roundtrip::<RoleBinding>(fixture);
}

#[test]
fn roundtrip_rolebinding_service_account_subject() {
    let fixture = r#"{
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "sa-binding", "namespace": "kube-system"},
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "default",
                "namespace": "kube-system"
            }
        ],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "pod-reader"
        }
    }"#;
    assert_roundtrip::<RoleBinding>(fixture);
}

#[test]
fn roundtrip_clusterrole_with_non_resource_urls() {
    let fixture = r#"{
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": "metrics-reader"},
        "rules": [
            {
                "verbs": ["get"],
                "nonResourceURLs": ["/metrics", "/healthz", "/livez"]
            }
        ]
    }"#;
    assert_roundtrip::<ClusterRole>(fixture);
}

#[test]
fn roundtrip_clusterrole_with_aggregation_rule() {
    let fixture = r#"{
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": "monitoring"},
        "rules": [],
        "aggregationRule": {
            "clusterRoleSelectors": [
                {
                    "matchLabels": {"rbac.example.com/aggregate-to-monitoring": "true"}
                }
            ]
        }
    }"#;
    assert_roundtrip::<ClusterRole>(fixture);
}

#[test]
fn roundtrip_clusterrolebinding_group_subject() {
    let fixture = r#"{
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": "system-monitoring"},
        "subjects": [
            {
                "kind": "Group",
                "name": "system:monitoring",
                "apiGroup": "rbac.authorization.k8s.io"
            }
        ],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "view"
        }
    }"#;
    assert_roundtrip::<ClusterRoleBinding>(fixture);
}

// =============================================================================
// scheduling.k8s.io/v1
// =============================================================================

#[test]
fn roundtrip_priority_class_minimal() {
    let fixture = r#"{
        "apiVersion": "scheduling.k8s.io/v1",
        "kind": "PriorityClass",
        "metadata": {"name": "high-priority"},
        "value": 1000000
    }"#;
    assert_roundtrip::<PriorityClass>(fixture);
}

#[test]
fn roundtrip_priority_class_global_default() {
    let fixture = r#"{
        "apiVersion": "scheduling.k8s.io/v1",
        "kind": "PriorityClass",
        "metadata": {"name": "default-priority"},
        "value": 0,
        "globalDefault": true,
        "description": "default priority class for all pods"
    }"#;
    assert_roundtrip::<PriorityClass>(fixture);
}

#[test]
fn roundtrip_priority_class_preempt_never() {
    let fixture = r#"{
        "apiVersion": "scheduling.k8s.io/v1",
        "kind": "PriorityClass",
        "metadata": {"name": "system-cluster-critical"},
        "value": 2000000000,
        "globalDefault": false,
        "preemptionPolicy": "Never"
    }"#;
    assert_roundtrip::<PriorityClass>(fixture);
}

// =============================================================================
// storage.k8s.io/v1
// =============================================================================

#[test]
fn roundtrip_storage_class_minimal() {
    let fixture = r#"{
        "apiVersion": "storage.k8s.io/v1",
        "kind": "StorageClass",
        "metadata": {"name": "standard"},
        "provisioner": "kubernetes.io/no-provisioner"
    }"#;
    assert_roundtrip::<StorageClass>(fixture);
}

#[test]
fn roundtrip_storage_class_full() {
    let fixture = r#"{
        "apiVersion": "storage.k8s.io/v1",
        "kind": "StorageClass",
        "metadata": {"name": "fast-ssd"},
        "provisioner": "kubernetes.io/aws-ebs",
        "parameters": {"type": "gp3", "iopsPerGB": "10"},
        "reclaimPolicy": "Delete",
        "volumeBindingMode": "WaitForFirstConsumer",
        "allowVolumeExpansion": true,
        "mountOptions": ["debug", "ro"]
    }"#;
    assert_roundtrip::<StorageClass>(fixture);
}

#[test]
fn roundtrip_storage_class_with_topology() {
    let fixture = r#"{
        "apiVersion": "storage.k8s.io/v1",
        "kind": "StorageClass",
        "metadata": {"name": "zonal"},
        "provisioner": "kubernetes.io/gce-pd",
        "volumeBindingMode": "Immediate",
        "allowedTopologies": [
            {
                "matchLabelExpressions": [
                    {
                        "key": "topology.kubernetes.io/zone",
                        "values": ["us-central1-a", "us-central1-b"]
                    }
                ]
            }
        ]
    }"#;
    assert_roundtrip::<StorageClass>(fixture);
}

#[test]
fn roundtrip_csi_driver_minimal() {
    let fixture = r#"{
        "apiVersion": "storage.k8s.io/v1",
        "kind": "CSIDriver",
        "metadata": {"name": "csi.example.com"},
        "spec": {}
    }"#;
    assert_roundtrip::<CSIDriver>(fixture);
}

#[test]
fn roundtrip_csi_driver_full() {
    let fixture = r#"{
        "apiVersion": "storage.k8s.io/v1",
        "kind": "CSIDriver",
        "metadata": {"name": "ebs.csi.aws.com"},
        "spec": {
            "attachRequired": true,
            "podInfoOnMount": true,
            "fsGroupPolicy": "ReadWriteOnceWithFSType",
            "storageCapacity": true,
            "volumeLifecycleModes": ["Persistent", "Ephemeral"],
            "tokenRequests": [
                {"audience": "ebs.csi.aws.com", "expirationSeconds": 3600}
            ],
            "requiresRepublish": false,
            "seLinuxMount": true
        }
    }"#;
    assert_roundtrip::<CSIDriver>(fixture);
}

#[test]
fn roundtrip_volume_attachment_minimal() {
    let fixture = r#"{
        "apiVersion": "storage.k8s.io/v1",
        "kind": "VolumeAttachment",
        "metadata": {"name": "csi-attachment-1"},
        "spec": {
            "attacher": "csi.example.com",
            "nodeName": "worker-1",
            "source": {
                "persistentVolumeName": "pv-001"
            }
        }
    }"#;
    assert_roundtrip::<VolumeAttachment>(fixture);
}

#[test]
fn roundtrip_volume_attachment_with_status() {
    let fixture = r#"{
        "apiVersion": "storage.k8s.io/v1",
        "kind": "VolumeAttachment",
        "metadata": {"name": "csi-attachment-2"},
        "spec": {
            "attacher": "csi.example.com",
            "nodeName": "worker-2",
            "source": {
                "persistentVolumeName": "pv-002"
            }
        },
        "status": {
            "attached": true,
            "attachmentMetadata": {"device": "/dev/sdb"}
        }
    }"#;
    assert_roundtrip::<VolumeAttachment>(fixture);
}

// =============================================================================
// node.k8s.io/v1
// =============================================================================

#[test]
fn roundtrip_runtime_class_minimal() {
    let fixture = r#"{
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": {"name": "runc"},
        "handler": "runc"
    }"#;
    assert_roundtrip::<RuntimeClass>(fixture);
}

#[test]
fn roundtrip_runtime_class_with_overhead() {
    let fixture = r#"{
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": {"name": "kata"},
        "handler": "kata-runtime",
        "overhead": {
            "podFixed": {"cpu": "250m", "memory": "128Mi"}
        }
    }"#;
    assert_roundtrip::<RuntimeClass>(fixture);
}

#[test]
fn roundtrip_runtime_class_with_scheduling() {
    let fixture = r#"{
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": {"name": "gvisor"},
        "handler": "runsc",
        "scheduling": {
            "nodeSelector": {"runtime": "gvisor"},
            "tolerations": [
                {
                    "key": "runtime",
                    "operator": "Equal",
                    "value": "gvisor",
                    "effect": "NoSchedule"
                }
            ]
        }
    }"#;
    assert_roundtrip::<RuntimeClass>(fixture);
}

// =============================================================================
// flowcontrol.apiserver.k8s.io/v1
// =============================================================================

#[test]
fn roundtrip_flow_schema_minimal() {
    let fixture = r#"{
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "FlowSchema",
        "metadata": {"name": "exempt"},
        "spec": {
            "priorityLevelConfiguration": {"name": "exempt"},
            "matchingPrecedence": 1
        }
    }"#;
    assert_roundtrip::<FlowSchema>(fixture);
}

#[test]
fn roundtrip_flow_schema_with_rules() {
    let fixture = r#"{
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "FlowSchema",
        "metadata": {"name": "system-leader-election"},
        "spec": {
            "priorityLevelConfiguration": {"name": "leader-election"},
            "matchingPrecedence": 200,
            "distinguisherMethod": {"type": "ByUser"},
            "rules": [
                {
                    "subjects": [
                        {"kind": "User", "user": {"name": "system:kube-controller-manager"}}
                    ],
                    "resourceRules": [
                        {
                            "verbs": ["get", "create", "update"],
                            "apiGroups": ["coordination.k8s.io"],
                            "resources": ["leases"],
                            "namespaces": ["kube-system"]
                        }
                    ]
                }
            ]
        }
    }"#;
    assert_roundtrip::<FlowSchema>(fixture);
}

#[test]
fn roundtrip_priority_level_configuration_exempt() {
    let fixture = r#"{
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "PriorityLevelConfiguration",
        "metadata": {"name": "exempt"},
        "spec": {
            "type": "Exempt",
            "exempt": {
                "nominalConcurrencyShares": 0,
                "lendingConcurrencyLimit": 0
            }
        }
    }"#;
    assert_roundtrip::<PriorityLevelConfiguration>(fixture);
}

#[test]
fn roundtrip_priority_level_configuration_limited_queue() {
    let fixture = r#"{
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "PriorityLevelConfiguration",
        "metadata": {"name": "workload-low"},
        "spec": {
            "type": "Limited",
            "limited": {
                "nominalConcurrencyShares": 100,
                "borrowingLimitPercent": 50,
                "limitResponse": {
                    "type": "Queue",
                    "queuing": {
                        "queues": 128,
                        "handSize": 6,
                        "queueLengthLimit": 50
                    }
                }
            }
        }
    }"#;
    assert_roundtrip::<PriorityLevelConfiguration>(fixture);
}

#[test]
fn roundtrip_priority_level_configuration_limited_reject() {
    let fixture = r#"{
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "PriorityLevelConfiguration",
        "metadata": {"name": "catch-all"},
        "spec": {
            "type": "Limited",
            "limited": {
                "nominalConcurrencyShares": 5,
                "limitResponse": {
                    "type": "Reject"
                }
            }
        }
    }"#;
    assert_roundtrip::<PriorityLevelConfiguration>(fixture);
}

// =============================================================================
// admissionregistration.k8s.io/v1
// =============================================================================

#[test]
fn roundtrip_validating_webhook_configuration_minimal() {
    let fixture = r#"{
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "validate-pods"}
    }"#;
    assert_roundtrip::<ValidatingWebhookConfiguration>(fixture);
}

#[test]
fn roundtrip_validating_webhook_configuration_with_service() {
    let fixture = r#"{
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "validate-pods"},
        "webhooks": [
            {
                "name": "pod-validator.example.com",
                "clientConfig": {
                    "service": {
                        "namespace": "example",
                        "name": "pod-validator",
                        "path": "/validate",
                        "port": 443
                    },
                    "caBundle": "Zm9vYmFy"
                },
                "rules": [
                    {
                        "operations": ["CREATE", "UPDATE"],
                        "apiGroups": [""],
                        "apiVersions": ["v1"],
                        "resources": ["pods"],
                        "scope": "Namespaced"
                    }
                ],
                "failurePolicy": "Fail",
                "matchPolicy": "Equivalent",
                "sideEffects": "None",
                "timeoutSeconds": 10,
                "admissionReviewVersions": ["v1"]
            }
        ]
    }"#;
    assert_roundtrip::<ValidatingWebhookConfiguration>(fixture);
}

#[test]
fn roundtrip_validating_webhook_configuration_with_url_and_selector() {
    let fixture = r#"{
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "validate-external"},
        "webhooks": [
            {
                "name": "external.example.com",
                "clientConfig": {
                    "url": "https://example.com/validate"
                },
                "rules": [
                    {
                        "operations": ["*"],
                        "apiGroups": ["apps"],
                        "apiVersions": ["v1"],
                        "resources": ["deployments"]
                    }
                ],
                "namespaceSelector": {
                    "matchLabels": {"env": "prod"}
                },
                "objectSelector": {
                    "matchExpressions": [
                        {"key": "skip-validation", "operator": "DoesNotExist"}
                    ]
                },
                "sideEffects": "NoneOnDryRun",
                "admissionReviewVersions": ["v1"],
                "matchConditions": [
                    {"name": "exclude-system", "expression": "request.userInfo.username != 'system:serviceaccount:kube-system:default'"}
                ]
            }
        ]
    }"#;
    assert_roundtrip::<ValidatingWebhookConfiguration>(fixture);
}

#[test]
fn roundtrip_mutating_webhook_configuration_minimal() {
    let fixture = r#"{
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mutate-pods"}
    }"#;
    assert_roundtrip::<MutatingWebhookConfiguration>(fixture);
}

#[test]
fn roundtrip_mutating_webhook_configuration_reinvocation_if_needed() {
    let fixture = r#"{
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "sidecar-injector"},
        "webhooks": [
            {
                "name": "sidecar-injector.example.com",
                "clientConfig": {
                    "service": {
                        "namespace": "istio-system",
                        "name": "sidecar-injector",
                        "path": "/inject"
                    }
                },
                "rules": [
                    {
                        "operations": ["CREATE"],
                        "apiGroups": [""],
                        "apiVersions": ["v1"],
                        "resources": ["pods"]
                    }
                ],
                "failurePolicy": "Ignore",
                "sideEffects": "None",
                "admissionReviewVersions": ["v1"],
                "reinvocationPolicy": "IfNeeded"
            }
        ]
    }"#;
    assert_roundtrip::<MutatingWebhookConfiguration>(fixture);
}

// =============================================================================
// apiextensions.k8s.io/v1
// =============================================================================

#[test]
fn roundtrip_crd_minimal_namespaced() {
    let fixture = r#"{
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "crontabs.stable.example.com"},
        "spec": {
            "group": "stable.example.com",
            "names": {
                "plural": "crontabs",
                "singular": "crontab",
                "kind": "CronTab",
                "listKind": "CronTabList"
            },
            "scope": "Namespaced",
            "versions": [
                {"name": "v1", "served": true, "storage": true}
            ]
        }
    }"#;
    assert_roundtrip::<CustomResourceDefinition>(fixture);
}

#[test]
fn roundtrip_crd_cluster_scoped_with_short_names() {
    let fixture = r#"{
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "clusterthings.stable.example.com"},
        "spec": {
            "group": "stable.example.com",
            "names": {
                "plural": "clusterthings",
                "kind": "ClusterThing",
                "shortNames": ["ct", "cthing"],
                "categories": ["all", "cluster"]
            },
            "scope": "Cluster",
            "versions": [
                {"name": "v1", "served": true, "storage": true}
            ]
        }
    }"#;
    assert_roundtrip::<CustomResourceDefinition>(fixture);
}

#[test]
fn roundtrip_crd_with_schema_and_subresources() {
    let fixture = r#"{
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "names": {
                "plural": "widgets",
                "kind": "Widget"
            },
            "scope": "Namespaced",
            "versions": [
                {
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "spec": {
                                    "type": "object",
                                    "required": ["replicas"],
                                    "properties": {
                                        "replicas": {
                                            "type": "integer",
                                            "minimum": 0
                                        },
                                        "color": {
                                            "type": "string",
                                            "enum": ["red", "green", "blue"]
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "subresources": {
                        "status": {},
                        "scale": {
                            "specReplicasPath": ".spec.replicas",
                            "statusReplicasPath": ".status.replicas",
                            "labelSelectorPath": ".status.labelSelector"
                        }
                    },
                    "additionalPrinterColumns": [
                        {"name": "Replicas", "type": "integer", "jsonPath": ".spec.replicas"}
                    ]
                }
            ]
        }
    }"#;
    assert_roundtrip::<CustomResourceDefinition>(fixture);
}

#[test]
fn roundtrip_crd_with_webhook_conversion() {
    let fixture = r#"{
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "things.example.com"},
        "spec": {
            "group": "example.com",
            "names": {
                "plural": "things",
                "kind": "Thing"
            },
            "scope": "Namespaced",
            "versions": [
                {"name": "v1", "served": true, "storage": false},
                {"name": "v2", "served": true, "storage": true}
            ],
            "conversion": {
                "strategy": "Webhook",
                "webhook": {
                    "clientConfig": {
                        "service": {
                            "namespace": "example",
                            "name": "conversion-webhook",
                            "path": "/convert"
                        }
                    },
                    "conversionReviewVersions": ["v1"]
                }
            }
        }
    }"#;
    assert_roundtrip::<CustomResourceDefinition>(fixture);
}
