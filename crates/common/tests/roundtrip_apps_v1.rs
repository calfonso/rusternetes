//! Hand-written JSON roundtrip tests for apps/v1 resources.
//!
//! Mirrors the upstream Kubernetes test layer that exercises
//! `k8s.io/api/apps/v1` types through JSON (de)serialization. Each fixture
//! verifies the four-step roundtrip contract:
//!
//!   1. `serde_json::from_str::<T>(&fixture)` succeeds.
//!   2. `serde_json::to_string(&decoded)` succeeds.
//!   3. `serde_json::from_str::<T>(&re_encoded)` succeeds.
//!   4. The JSON value produced by step (2) re-encodes byte-identically
//!      after a second decode pass — i.e. the canonical JSON shape is a
//!      fixed point of the encode/decode round.
//!
//! Because most apps/v1 top-level structs in this crate do not derive
//! `PartialEq` (they hold deeply-nested types like `PodSpec` that include
//! `serde_json::Value` for typed-but-opaque fields), we assert stability
//! via `serde_json::Value` equality of two successive encodings. This is
//! the same property upstream's `RoundTrip` helpers establish.

use rusternetes_common::resources::controllerrevision::ControllerRevision;
use rusternetes_common::resources::deployment::Deployment;
use rusternetes_common::resources::workloads::{DaemonSet, ReplicaSet, StatefulSet};
use rusternetes_common::types::List;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Runs the four-step JSON roundtrip contract against a hand-written
/// fixture. Asserts that the second decode + re-encode produces a value
/// identical (as a `serde_json::Value`) to the first re-encode.
fn assert_roundtrip<T>(fixture: &str)
where
    T: DeserializeOwned + Serialize,
{
    // 1. Decode the fixture.
    let decoded: T =
        serde_json::from_str(fixture).expect("step 1: fixture must deserialize into T");

    // 2. Re-encode.
    let re_encoded = serde_json::to_string(&decoded).expect("step 2: decoded value must serialize");

    // 3. Decode the re-encoded form.
    let re_decoded: T =
        serde_json::from_str(&re_encoded).expect("step 3: re-encoded form must deserialize");

    // 4. Stability check: encode the second decode and compare JSON values.
    let re_re_encoded =
        serde_json::to_string(&re_decoded).expect("step 4a: re-decoded value must serialize");

    let a: serde_json::Value = serde_json::from_str(&re_encoded)
        .expect("step 4b: first re-encoding must parse as JSON Value");
    let b: serde_json::Value = serde_json::from_str(&re_re_encoded)
        .expect("step 4c: second re-encoding must parse as JSON Value");
    assert_eq!(
        a, b,
        "roundtrip unstable: re-encoded JSON drifted on second pass.\n\
         first:  {re_encoded}\n\
         second: {re_re_encoded}",
    );
}

// ---------------------------------------------------------------------------
// Deployment fixtures
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_deployment_minimal() {
    let fixture = r#"{
        "kind": "Deployment",
        "apiVersion": "apps/v1",
        "metadata": {"name": "nginx", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "nginx"}},
            "template": {
                "metadata": {"labels": {"app": "nginx"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx:1.21"}]}
            }
        }
    }"#;
    assert_roundtrip::<Deployment>(fixture);
}

#[test]
fn roundtrip_deployment_full_featured() {
    let fixture = r#"{
        "kind": "Deployment",
        "apiVersion": "apps/v1",
        "metadata": {
            "name": "web",
            "namespace": "production",
            "labels": {"app": "web", "tier": "frontend"},
            "annotations": {"deployment.kubernetes.io/revision": "3"},
            "generation": 4
        },
        "spec": {
            "replicas": 5,
            "selector": {"matchLabels": {"app": "web"}},
            "template": {
                "metadata": {"labels": {"app": "web"}},
                "spec": {
                    "containers": [{
                        "name": "web",
                        "image": "nginx:1.25",
                        "ports": [{"containerPort": 80, "protocol": "TCP"}]
                    }],
                    "restartPolicy": "Always"
                }
            },
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxSurge": "25%", "maxUnavailable": "25%"}
            },
            "minReadySeconds": 10,
            "revisionHistoryLimit": 10,
            "paused": false,
            "progressDeadlineSeconds": 600
        },
        "status": {
            "replicas": 5,
            "updatedReplicas": 5,
            "readyReplicas": 5,
            "availableReplicas": 5,
            "observedGeneration": 4,
            "conditions": [
                {
                    "type": "Available",
                    "status": "True",
                    "reason": "MinimumReplicasAvailable",
                    "message": "Deployment has minimum availability."
                }
            ]
        }
    }"#;
    assert_roundtrip::<Deployment>(fixture);
}

#[test]
fn roundtrip_deployment_recreate_strategy() {
    // Recreate strategy has no rollingUpdate sub-field.
    let fixture = r#"{
        "kind": "Deployment",
        "apiVersion": "apps/v1",
        "metadata": {"name": "db", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "db"}},
            "template": {
                "metadata": {"labels": {"app": "db"}},
                "spec": {"containers": [{"name": "db", "image": "postgres:16"}]}
            },
            "strategy": {"type": "Recreate"}
        }
    }"#;
    assert_roundtrip::<Deployment>(fixture);
}

#[test]
fn roundtrip_deployment_status_subresource() {
    // Status-only payload as returned by the /status subresource. Some
    // controllers omit `spec` entirely when reading via /status; the type
    // requires spec, so we include a minimal one matching upstream behaviour.
    let fixture = r#"{
        "kind": "Deployment",
        "apiVersion": "apps/v1",
        "metadata": {"name": "status-only", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "status-only"}},
            "template": {
                "metadata": {"labels": {"app": "status-only"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        },
        "status": {
            "replicas": 3,
            "updatedReplicas": 2,
            "readyReplicas": 2,
            "availableReplicas": 2,
            "unavailableReplicas": 1,
            "collisionCount": 0,
            "observedGeneration": 7,
            "terminatingReplicas": 0
        }
    }"#;
    assert_roundtrip::<Deployment>(fixture);
}

#[test]
fn roundtrip_deployment_list_wrapper() {
    let fixture = r#"{
        "kind": "DeploymentList",
        "apiVersion": "apps/v1",
        "metadata": {"resourceVersion": "12345"},
        "items": [
            {
                "kind": "Deployment",
                "apiVersion": "apps/v1",
                "metadata": {"name": "a", "namespace": "default"},
                "spec": {
                    "selector": {"matchLabels": {"app": "a"}},
                    "template": {
                        "metadata": {"labels": {"app": "a"}},
                        "spec": {"containers": [{"name": "a", "image": "busybox"}]}
                    }
                }
            },
            {
                "kind": "Deployment",
                "apiVersion": "apps/v1",
                "metadata": {"name": "b", "namespace": "default"},
                "spec": {
                    "replicas": 2,
                    "selector": {"matchLabels": {"app": "b"}},
                    "template": {
                        "metadata": {"labels": {"app": "b"}},
                        "spec": {"containers": [{"name": "b", "image": "busybox"}]}
                    }
                }
            }
        ]
    }"#;
    assert_roundtrip::<List<Deployment>>(fixture);
}

#[test]
fn roundtrip_deployment_max_surge_integer() {
    // IntOrString edge case: integer maxSurge/maxUnavailable, not percent.
    let fixture = r#"{
        "kind": "Deployment",
        "apiVersion": "apps/v1",
        "metadata": {"name": "int-surge", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "int-surge"}},
            "template": {
                "metadata": {"labels": {"app": "int-surge"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            },
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0}
            }
        }
    }"#;
    assert_roundtrip::<Deployment>(fixture);
}

// ---------------------------------------------------------------------------
// ReplicaSet fixtures
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_replicaset_minimal() {
    let fixture = r#"{
        "kind": "ReplicaSet",
        "apiVersion": "apps/v1",
        "metadata": {"name": "frontend", "namespace": "default"},
        "spec": {
            "replicas": 3,
            "selector": {"matchLabels": {"tier": "frontend"}},
            "template": {
                "metadata": {"labels": {"tier": "frontend"}},
                "spec": {"containers": [{"name": "php-redis", "image": "gcr.io/google_samples/gb-frontend:v3"}]}
            }
        }
    }"#;
    assert_roundtrip::<ReplicaSet>(fixture);
}

#[test]
fn roundtrip_replicaset_full_featured() {
    let fixture = r#"{
        "kind": "ReplicaSet",
        "apiVersion": "apps/v1",
        "metadata": {
            "name": "backend",
            "namespace": "production",
            "labels": {"app": "backend"},
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "backend",
                "uid": "abc-123-def",
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "spec": {
            "replicas": 4,
            "selector": {
                "matchLabels": {"app": "backend"},
                "matchExpressions": [
                    {"key": "tier", "operator": "In", "values": ["backend", "api"]}
                ]
            },
            "template": {
                "metadata": {"labels": {"app": "backend", "tier": "backend"}},
                "spec": {
                    "containers": [{"name": "backend", "image": "myapp:v2"}]
                }
            },
            "minReadySeconds": 5
        },
        "status": {
            "replicas": 4,
            "fullyLabeledReplicas": 4,
            "readyReplicas": 4,
            "availableReplicas": 4,
            "observedGeneration": 1
        }
    }"#;
    assert_roundtrip::<ReplicaSet>(fixture);
}

#[test]
fn roundtrip_replicaset_status_subresource() {
    let fixture = r#"{
        "kind": "ReplicaSet",
        "apiVersion": "apps/v1",
        "metadata": {"name": "rs-status", "namespace": "default"},
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app": "rs-status"}},
            "template": {
                "metadata": {"labels": {"app": "rs-status"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        },
        "status": {
            "replicas": 2,
            "readyReplicas": 1,
            "availableReplicas": 1,
            "observedGeneration": 3,
            "conditions": [
                {"type": "ReplicaFailure", "status": "False"}
            ],
            "terminatingReplicas": 0
        }
    }"#;
    assert_roundtrip::<ReplicaSet>(fixture);
}

#[test]
fn roundtrip_replicaset_list_wrapper() {
    let fixture = r#"{
        "kind": "ReplicaSetList",
        "apiVersion": "apps/v1",
        "metadata": {"resourceVersion": "9000"},
        "items": [
            {
                "kind": "ReplicaSet",
                "apiVersion": "apps/v1",
                "metadata": {"name": "one", "namespace": "default"},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"n": "one"}},
                    "template": {
                        "metadata": {"labels": {"n": "one"}},
                        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                    }
                }
            }
        ]
    }"#;
    assert_roundtrip::<List<ReplicaSet>>(fixture);
}

#[test]
fn roundtrip_replicaset_zero_replicas() {
    // Edge case: explicit zero replicas (paused/scaled-down RS).
    let fixture = r#"{
        "kind": "ReplicaSet",
        "apiVersion": "apps/v1",
        "metadata": {"name": "scaled-to-zero", "namespace": "default"},
        "spec": {
            "replicas": 0,
            "selector": {"matchLabels": {"app": "scaled-to-zero"}},
            "template": {
                "metadata": {"labels": {"app": "scaled-to-zero"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        }
    }"#;
    assert_roundtrip::<ReplicaSet>(fixture);
}

// ---------------------------------------------------------------------------
// StatefulSet fixtures
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_statefulset_minimal() {
    let fixture = r#"{
        "kind": "StatefulSet",
        "apiVersion": "apps/v1",
        "metadata": {"name": "web", "namespace": "default"},
        "spec": {
            "serviceName": "web",
            "selector": {"matchLabels": {"app": "web"}},
            "template": {
                "metadata": {"labels": {"app": "web"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx:1.25"}]}
            }
        }
    }"#;
    assert_roundtrip::<StatefulSet>(fixture);
}

#[test]
fn roundtrip_statefulset_full_featured() {
    let fixture = r#"{
        "kind": "StatefulSet",
        "apiVersion": "apps/v1",
        "metadata": {
            "name": "etcd",
            "namespace": "kube-system",
            "labels": {"app": "etcd"},
            "generation": 2
        },
        "spec": {
            "replicas": 3,
            "serviceName": "etcd",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "etcd"}},
            "template": {
                "metadata": {"labels": {"app": "etcd"}},
                "spec": {
                    "containers": [{
                        "name": "etcd",
                        "image": "quay.io/coreos/etcd:v3.5.0",
                        "volumeMounts": [{"name": "data", "mountPath": "/var/lib/etcd"}]
                    }]
                }
            },
            "updateStrategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"partition": 0, "maxUnavailable": "1"}
            },
            "minReadySeconds": 10,
            "revisionHistoryLimit": 10,
            "persistentVolumeClaimRetentionPolicy": {
                "whenDeleted": "Retain",
                "whenScaled": "Delete"
            },
            "ordinals": {"start": 0}
        },
        "status": {
            "replicas": 3,
            "readyReplicas": 3,
            "currentReplicas": 3,
            "updatedReplicas": 3,
            "availableReplicas": 3,
            "collisionCount": 0,
            "observedGeneration": 2,
            "currentRevision": "etcd-7f9c4b5d8c",
            "updateRevision": "etcd-7f9c4b5d8c"
        }
    }"#;
    assert_roundtrip::<StatefulSet>(fixture);
}

#[test]
fn roundtrip_statefulset_on_delete_strategy() {
    let fixture = r#"{
        "kind": "StatefulSet",
        "apiVersion": "apps/v1",
        "metadata": {"name": "manual", "namespace": "default"},
        "spec": {
            "serviceName": "manual",
            "selector": {"matchLabels": {"app": "manual"}},
            "template": {
                "metadata": {"labels": {"app": "manual"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            },
            "updateStrategy": {"type": "OnDelete"}
        }
    }"#;
    assert_roundtrip::<StatefulSet>(fixture);
}

#[test]
fn roundtrip_statefulset_status_subresource() {
    let fixture = r#"{
        "kind": "StatefulSet",
        "apiVersion": "apps/v1",
        "metadata": {"name": "sts-status", "namespace": "default"},
        "spec": {
            "serviceName": "sts-status",
            "selector": {"matchLabels": {"app": "sts-status"}},
            "template": {
                "metadata": {"labels": {"app": "sts-status"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        },
        "status": {
            "replicas": 3,
            "conditions": [
                {
                    "type": "Available",
                    "status": "True",
                    "reason": "MinimumReplicasAvailable",
                    "message": "StatefulSet has minimum availability"
                }
            ]
        }
    }"#;
    assert_roundtrip::<StatefulSet>(fixture);
}

#[test]
fn roundtrip_statefulset_list_wrapper() {
    let fixture = r#"{
        "kind": "StatefulSetList",
        "apiVersion": "apps/v1",
        "metadata": {"resourceVersion": "777"},
        "items": [
            {
                "kind": "StatefulSet",
                "apiVersion": "apps/v1",
                "metadata": {"name": "sts1", "namespace": "default"},
                "spec": {
                    "serviceName": "sts1",
                    "selector": {"matchLabels": {"app": "sts1"}},
                    "template": {
                        "metadata": {"labels": {"app": "sts1"}},
                        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                    }
                }
            }
        ]
    }"#;
    assert_roundtrip::<List<StatefulSet>>(fixture);
}

// ---------------------------------------------------------------------------
// DaemonSet fixtures
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_daemonset_minimal() {
    let fixture = r#"{
        "kind": "DaemonSet",
        "apiVersion": "apps/v1",
        "metadata": {"name": "fluentd", "namespace": "kube-system"},
        "spec": {
            "selector": {"matchLabels": {"app": "fluentd"}},
            "template": {
                "metadata": {"labels": {"app": "fluentd"}},
                "spec": {"containers": [{"name": "fluentd", "image": "fluent/fluentd:v1.16"}]}
            }
        }
    }"#;
    assert_roundtrip::<DaemonSet>(fixture);
}

#[test]
fn roundtrip_daemonset_full_featured() {
    let fixture = r#"{
        "kind": "DaemonSet",
        "apiVersion": "apps/v1",
        "metadata": {
            "name": "node-exporter",
            "namespace": "monitoring",
            "labels": {"app": "node-exporter"}
        },
        "spec": {
            "selector": {"matchLabels": {"app": "node-exporter"}},
            "template": {
                "metadata": {"labels": {"app": "node-exporter"}},
                "spec": {
                    "hostNetwork": true,
                    "hostPID": true,
                    "containers": [{
                        "name": "node-exporter",
                        "image": "prom/node-exporter:v1.7.0",
                        "ports": [{"containerPort": 9100, "name": "metrics", "protocol": "TCP"}]
                    }],
                    "tolerations": [
                        {"operator": "Exists"}
                    ]
                }
            },
            "updateStrategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": "10%", "maxSurge": "0"}
            },
            "minReadySeconds": 5,
            "revisionHistoryLimit": 10
        },
        "status": {
            "desiredNumberScheduled": 3,
            "currentNumberScheduled": 3,
            "numberReady": 3,
            "numberMisscheduled": 0,
            "numberAvailable": 3,
            "numberUnavailable": 0,
            "updatedNumberScheduled": 3,
            "observedGeneration": 1,
            "collisionCount": 0
        }
    }"#;
    assert_roundtrip::<DaemonSet>(fixture);
}

#[test]
fn roundtrip_daemonset_status_subresource() {
    let fixture = r#"{
        "kind": "DaemonSet",
        "apiVersion": "apps/v1",
        "metadata": {"name": "ds-status", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "ds-status"}},
            "template": {
                "metadata": {"labels": {"app": "ds-status"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        },
        "status": {
            "desiredNumberScheduled": 5,
            "currentNumberScheduled": 4,
            "numberReady": 3,
            "numberMisscheduled": 1,
            "conditions": [
                {"type": "Progressing", "status": "True"}
            ]
        }
    }"#;
    assert_roundtrip::<DaemonSet>(fixture);
}

#[test]
fn roundtrip_daemonset_on_delete_strategy() {
    let fixture = r#"{
        "kind": "DaemonSet",
        "apiVersion": "apps/v1",
        "metadata": {"name": "ondelete-ds", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "ondelete-ds"}},
            "template": {
                "metadata": {"labels": {"app": "ondelete-ds"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            },
            "updateStrategy": {"type": "OnDelete"}
        }
    }"#;
    assert_roundtrip::<DaemonSet>(fixture);
}

#[test]
fn roundtrip_daemonset_list_wrapper() {
    let fixture = r#"{
        "kind": "DaemonSetList",
        "apiVersion": "apps/v1",
        "metadata": {"resourceVersion": "42"},
        "items": [
            {
                "kind": "DaemonSet",
                "apiVersion": "apps/v1",
                "metadata": {"name": "ds-a", "namespace": "default"},
                "spec": {
                    "selector": {"matchLabels": {"app": "ds-a"}},
                    "template": {
                        "metadata": {"labels": {"app": "ds-a"}},
                        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
                    }
                }
            }
        ]
    }"#;
    assert_roundtrip::<List<DaemonSet>>(fixture);
}

// ---------------------------------------------------------------------------
// ControllerRevision fixtures
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_controller_revision_minimal() {
    let fixture = r#"{
        "kind": "ControllerRevision",
        "apiVersion": "apps/v1",
        "metadata": {"name": "rev-1", "namespace": "default"},
        "revision": 1
    }"#;
    assert_roundtrip::<ControllerRevision>(fixture);
}

#[test]
fn roundtrip_controller_revision_with_data() {
    // Mirrors what StatefulSet/DaemonSet controllers persist: an opaque
    // serialized snapshot under `data`.
    let fixture = r#"{
        "kind": "ControllerRevision",
        "apiVersion": "apps/v1",
        "metadata": {
            "name": "web-7c5b8f9d6",
            "namespace": "default",
            "labels": {
                "app": "web",
                "controller-revision-hash": "7c5b8f9d6"
            }
        },
        "data": {
            "spec": {
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "web", "image": "nginx:1.25"}]}
                }
            }
        },
        "revision": 3
    }"#;
    assert_roundtrip::<ControllerRevision>(fixture);
}

#[test]
fn roundtrip_controller_revision_owned_by_statefulset() {
    let fixture = r#"{
        "kind": "ControllerRevision",
        "apiVersion": "apps/v1",
        "metadata": {
            "name": "etcd-abc123",
            "namespace": "kube-system",
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "name": "etcd",
                "uid": "deadbeef-1234",
                "controller": true,
                "blockOwnerDeletion": true
            }]
        },
        "revision": 7,
        "data": {"key": "value"}
    }"#;
    assert_roundtrip::<ControllerRevision>(fixture);
}

#[test]
fn roundtrip_controller_revision_large_revision_number() {
    // Edge case: revision is i64; verify large numbers roundtrip without
    // narrowing.
    let fixture = r#"{
        "kind": "ControllerRevision",
        "apiVersion": "apps/v1",
        "metadata": {"name": "big-rev", "namespace": "default"},
        "revision": 9223372036854775807
    }"#;
    assert_roundtrip::<ControllerRevision>(fixture);
}

#[test]
fn roundtrip_controller_revision_list_wrapper() {
    let fixture = r#"{
        "kind": "ControllerRevisionList",
        "apiVersion": "apps/v1",
        "metadata": {"resourceVersion": "100"},
        "items": [
            {
                "kind": "ControllerRevision",
                "apiVersion": "apps/v1",
                "metadata": {"name": "rev-1", "namespace": "default"},
                "revision": 1
            },
            {
                "kind": "ControllerRevision",
                "apiVersion": "apps/v1",
                "metadata": {"name": "rev-2", "namespace": "default"},
                "revision": 2,
                "data": {"foo": "bar"}
            }
        ]
    }"#;
    assert_roundtrip::<List<ControllerRevision>>(fixture);
}
