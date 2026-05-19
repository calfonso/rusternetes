//! Generic Kubernetes protobuf-to-JSON decoder.
//!
//! Kubernetes wraps all protobuf-encoded resources in an `Unknown` envelope:
//!   k8s\0 + proto(Unknown { typeMeta, raw, contentEncoding, contentType })
//!
//! The `raw` field contains the native protobuf encoding of the resource
//! (e.g., apps/v1.Deployment). This module decodes native protobuf into
//! JSON using field number → name mappings extracted from the K8s .proto
//! schema files.
//!
//! The Go API server uses generated .pb.go Unmarshal methods. We achieve
//! the same result by maintaining a registry of proto schemas and using
//! a generic recursive decoder.

use serde_json::{json, Map, Value};
use std::collections::HashMap;
use tracing::{debug, warn};

/// Wire types in protobuf encoding
const WIRE_VARINT: u8 = 0;
const WIRE_64BIT: u8 = 1;
const WIRE_LENGTH_DELIMITED: u8 = 2;
const WIRE_32BIT: u8 = 5;

/// Describes how a protobuf field should be decoded to JSON
#[derive(Debug, Clone)]
pub enum FieldType {
    /// Scalar string field
    String,
    /// Scalar integer field (int32, int64, uint32, uint64)
    Int,
    /// Scalar boolean field
    Bool,
    /// Nested message — value is the message type name for schema lookup
    Message(String),
    /// Inline-embedded message — Go's JSON tags flatten the inner fields
    /// into the parent object. The proto wire format still nests the
    /// message at this field number, but the decoded JSON merges the
    /// inner fields one level up. Used for `Volume.volumeSource` and
    /// every `LocalObjectReference` embedding (ConfigMapVolumeSource,
    /// SecretProjection, ConfigMapProjection, ...).
    InlineMessage(String),
    /// map<string, string> — encoded as repeated MapEntry messages
    StringMap,
    /// Repeated field — value is the element type
    Repeated(Box<FieldType>),
    /// Bytes field — base64 encode
    Bytes,
    /// IntOrString — K8s special type, try string first then int
    IntOrString,
    /// map<string, Message> — encoded as repeated MapEntry with key=string, value=message
    MessageMap(String),
    /// K8s JSON type — a message with a single `raw` bytes field containing JSON
    JsonRaw,
}

/// Schema for a single protobuf message type
#[derive(Debug, Clone)]
pub struct MessageSchema {
    /// Map of field number → (json_field_name, field_type)
    pub fields: HashMap<u32, (String, FieldType)>,
}

/// Registry of all known K8s protobuf message schemas
pub struct ProtoRegistry {
    /// Map of message type name → schema
    schemas: HashMap<String, MessageSchema>,
}

impl Default for ProtoRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtoRegistry {
    /// Build the registry with all known K8s proto schemas.
    /// Field numbers are from the generated.proto files in k8s.io/api.
    pub fn new() -> Self {
        let mut schemas = HashMap::new();

        // ========== apimachinery types ==========

        schemas.insert("ObjectMeta".into(), Self::object_meta_schema());
        schemas.insert("LabelSelector".into(), Self::label_selector_schema());
        schemas.insert(
            "LabelSelectorRequirement".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("key".into(), FieldType::String)),
                    (2, ("operator".into(), FieldType::String)),
                    (
                        3,
                        (
                            "values".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert("OwnerReference".into(), Self::owner_reference_schema());
        schemas.insert(
            "Time".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("seconds".into(), FieldType::Int)),
                    (2, ("nanos".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "ManagedFieldsEntry".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("manager".into(), FieldType::String)),
                    (2, ("operation".into(), FieldType::String)),
                    (3, ("apiVersion".into(), FieldType::String)),
                    (4, ("time".into(), FieldType::Message("Time".into()))),
                    (6, ("fieldsType".into(), FieldType::String)),
                    (7, ("fieldsV1".into(), FieldType::Bytes)),
                    (8, ("subresource".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "DeleteOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("gracePeriodSeconds".into(), FieldType::Int)),
                    (
                        2,
                        (
                            "preconditions".into(),
                            FieldType::Message("Preconditions".into()),
                        ),
                    ),
                    (3, ("orphanDependents".into(), FieldType::Bool)),
                    (4, ("propagationPolicy".into(), FieldType::String)),
                    (
                        5,
                        (
                            "dryRun".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        6,
                        (
                            "ignoreStoreReadErrorWithClusterBreakingPotential".into(),
                            FieldType::Bool,
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "Preconditions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("uid".into(), FieldType::String)),
                    (2, ("resourceVersion".into(), FieldType::String)),
                ]),
            },
        );

        // ========== apps/v1 types ==========

        schemas.insert("Deployment".into(), Self::deployment_schema());
        schemas.insert("DeploymentSpec".into(), Self::deployment_spec_schema());
        schemas.insert("DeploymentStatus".into(), Self::deployment_status_schema());
        schemas.insert(
            "DeploymentCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                    (
                        6,
                        ("lastUpdateTime".into(), FieldType::Message("Time".into())),
                    ),
                    (
                        7,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "DeploymentStrategy".into(),
            Self::deployment_strategy_schema(),
        );
        schemas.insert(
            "RollingUpdateDeployment".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("maxUnavailable".into(), FieldType::IntOrString)),
                    (2, ("maxSurge".into(), FieldType::IntOrString)),
                ]),
            },
        );
        schemas.insert(
            "ReplicaSet".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("ReplicaSetSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("ReplicaSetStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ReplicaSetSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("replicas".into(), FieldType::Int)),
                    (
                        2,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                    (4, ("minReadySeconds".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "ReplicaSetStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("replicas".into(), FieldType::Int)),
                    (2, ("fullyLabeledReplicas".into(), FieldType::Int)),
                    (3, ("observedGeneration".into(), FieldType::Int)),
                    (4, ("readyReplicas".into(), FieldType::Int)),
                    (5, ("availableReplicas".into(), FieldType::Int)),
                    (
                        6,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ReplicaSetCondition".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ReplicaSetCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "StatefulSet".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("StatefulSetSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("StatefulSetStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "StatefulSetSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("replicas".into(), FieldType::Int)),
                    (
                        2,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                    (
                        4,
                        (
                            "volumeClaimTemplates".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PersistentVolumeClaim".into(),
                            ))),
                        ),
                    ),
                    (5, ("serviceName".into(), FieldType::String)),
                    (6, ("podManagementPolicy".into(), FieldType::String)),
                    (
                        7,
                        (
                            "updateStrategy".into(),
                            FieldType::Message("StatefulSetUpdateStrategy".into()),
                        ),
                    ),
                    (8, ("revisionHistoryLimit".into(), FieldType::Int)),
                    (9, ("minReadySeconds".into(), FieldType::Int)),
                    (
                        10,
                        (
                            "persistentVolumeClaimRetentionPolicy".into(),
                            FieldType::Message(
                                "StatefulSetPersistentVolumeClaimRetentionPolicy".into(),
                            ),
                        ),
                    ),
                    (
                        11,
                        (
                            "ordinals".into(),
                            FieldType::Message("StatefulSetOrdinals".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "StatefulSetUpdateStrategy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (
                        2,
                        (
                            "rollingUpdate".into(),
                            FieldType::Message("RollingUpdateStatefulSetStrategy".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "RollingUpdateStatefulSetStrategy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("partition".into(), FieldType::Int)),
                    (2, ("maxUnavailable".into(), FieldType::IntOrString)),
                ]),
            },
        );
        schemas.insert(
            "StatefulSetPersistentVolumeClaimRetentionPolicy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("whenDeleted".into(), FieldType::String)),
                    (2, ("whenScaled".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "StatefulSetOrdinals".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("start".into(), FieldType::Int))]),
            },
        );
        schemas.insert(
            "StatefulSetStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("observedGeneration".into(), FieldType::Int)),
                    (2, ("replicas".into(), FieldType::Int)),
                    (3, ("readyReplicas".into(), FieldType::Int)),
                    (4, ("currentReplicas".into(), FieldType::Int)),
                    (5, ("updatedReplicas".into(), FieldType::Int)),
                    (6, ("currentRevision".into(), FieldType::String)),
                    (7, ("updateRevision".into(), FieldType::String)),
                    (8, ("collisionCount".into(), FieldType::Int)),
                    (
                        9,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "StatefulSetCondition".into(),
                            ))),
                        ),
                    ),
                    (10, ("availableReplicas".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "StatefulSetCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "DaemonSet".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("DaemonSetSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("DaemonSetStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "DaemonSetSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "updateStrategy".into(),
                            FieldType::Message("DaemonSetUpdateStrategy".into()),
                        ),
                    ),
                    (4, ("minReadySeconds".into(), FieldType::Int)),
                    (5, ("revisionHistoryLimit".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "DaemonSetUpdateStrategy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (
                        2,
                        (
                            "rollingUpdate".into(),
                            FieldType::Message("RollingUpdateDaemonSet".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "RollingUpdateDaemonSet".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("maxUnavailable".into(), FieldType::IntOrString)),
                    (2, ("maxSurge".into(), FieldType::IntOrString)),
                ]),
            },
        );
        schemas.insert(
            "DaemonSetStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("currentNumberScheduled".into(), FieldType::Int)),
                    (2, ("numberMisscheduled".into(), FieldType::Int)),
                    (3, ("desiredNumberScheduled".into(), FieldType::Int)),
                    (4, ("numberReady".into(), FieldType::Int)),
                    (5, ("observedGeneration".into(), FieldType::Int)),
                    (6, ("updatedNumberScheduled".into(), FieldType::Int)),
                    (7, ("numberAvailable".into(), FieldType::Int)),
                    (8, ("numberUnavailable".into(), FieldType::Int)),
                    (9, ("collisionCount".into(), FieldType::Int)),
                    (
                        10,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DaemonSetCondition".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "DaemonSetCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                ]),
            },
        );

        // ========== core/v1 types ==========

        schemas.insert("PodTemplateSpec".into(), Self::pod_template_spec_schema());
        schemas.insert("PodSpec".into(), Self::pod_spec_schema());
        schemas.insert("Container".into(), Self::container_schema());
        schemas.insert("ContainerPort".into(), Self::container_port_schema());
        schemas.insert("SecurityContext".into(), Self::security_context_schema());
        schemas.insert(
            "ResourceRequirements".into(),
            Self::resource_requirements_schema(),
        );
        schemas.insert("Volume".into(), Self::volume_schema());
        schemas.insert("VolumeSource".into(), Self::volume_source_schema());
        schemas.insert("VolumeMount".into(), Self::volume_mount_schema());
        schemas.insert("EnvVar".into(), Self::env_var_schema());
        schemas.insert("EnvVarSource".into(), Self::env_var_source_schema());
        schemas.insert(
            "ObjectFieldSelector".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("apiVersion".into(), FieldType::String)),
                    (2, ("fieldPath".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ResourceFieldSelector".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("containerName".into(), FieldType::String)),
                    (2, ("resource".into(), FieldType::String)),
                    (3, ("divisor".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ConfigMapKeySelector".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("key".into(), FieldType::String)),
                    (3, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "SecretKeySelector".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("key".into(), FieldType::String)),
                    (3, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert("Probe".into(), Self::probe_schema());
        schemas.insert("ProbeHandler".into(), Self::probe_handler_schema());
        schemas.insert(
            "ExecAction".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "command".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                )]),
            },
        );
        schemas.insert(
            "HTTPGetAction".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("path".into(), FieldType::String)),
                    (2, ("port".into(), FieldType::IntOrString)),
                    (3, ("host".into(), FieldType::String)),
                    (4, ("scheme".into(), FieldType::String)),
                    (
                        5,
                        (
                            "httpHeaders".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("HTTPHeader".into()))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "HTTPHeader".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("value".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "TCPSocketAction".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("port".into(), FieldType::IntOrString)),
                    (2, ("host".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "GRPCAction".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("port".into(), FieldType::Int)),
                    (2, ("service".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "Lifecycle".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "postStart".into(),
                            FieldType::Message("LifecycleHandler".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "preStop".into(),
                            FieldType::Message("LifecycleHandler".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "LifecycleHandler".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("exec".into(), FieldType::Message("ExecAction".into()))),
                    (
                        2,
                        ("httpGet".into(), FieldType::Message("HTTPGetAction".into())),
                    ),
                    (
                        3,
                        (
                            "tcpSocket".into(),
                            FieldType::Message("TCPSocketAction".into()),
                        ),
                    ),
                    (
                        4,
                        ("sleep".into(), FieldType::Message("SleepAction".into())),
                    ),
                ]),
            },
        );
        schemas.insert(
            "SleepAction".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("seconds".into(), FieldType::Int))]),
            },
        );
        schemas.insert(
            "Capabilities".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "add".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "drop".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "SELinuxOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("user".into(), FieldType::String)),
                    (2, ("role".into(), FieldType::String)),
                    (3, ("type".into(), FieldType::String)),
                    (4, ("level".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "SeccompProfile".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("localhostProfile".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "AppArmorProfile".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("localhostProfile".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "PodSecurityContext".into(),
            Self::pod_security_context_schema(),
        );
        schemas.insert(
            "Toleration".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("key".into(), FieldType::String)),
                    (2, ("operator".into(), FieldType::String)),
                    (3, ("value".into(), FieldType::String)),
                    (4, ("effect".into(), FieldType::String)),
                    (5, ("tolerationSeconds".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "PodDNSConfig".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "nameservers".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "searches".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        3,
                        (
                            "options".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PodDNSConfigOption".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PodDNSConfigOption".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("value".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "LocalObjectReference".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("name".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "Affinity".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "nodeAffinity".into(),
                            FieldType::Message("NodeAffinity".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "podAffinity".into(),
                            FieldType::Message("PodAffinity".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "podAntiAffinity".into(),
                            FieldType::Message("PodAntiAffinity".into()),
                        ),
                    ),
                ]),
            },
        );
        // Affinity sub-types are complex — decode as opaque messages
        schemas.insert(
            "NodeAffinity".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "PodAffinity".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "PodAntiAffinity".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "TopologySpreadConstraint".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("maxSkew".into(), FieldType::Int)),
                    (2, ("topologyKey".into(), FieldType::String)),
                    (3, ("whenUnsatisfiable".into(), FieldType::String)),
                    (
                        4,
                        (
                            "labelSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (5, ("minDomains".into(), FieldType::Int)),
                    (6, ("nodeAffinityPolicy".into(), FieldType::String)),
                    (7, ("nodeTaintsPolicy".into(), FieldType::String)),
                    (
                        8,
                        (
                            "matchLabelKeys".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        // Service, ConfigMap, Secret, etc. — common pattern
        schemas.insert(
            "Service".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("ServiceSpec".into()))),
                    (
                        3,
                        ("status".into(), FieldType::Message("ServiceStatus".into())),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ServiceSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "ports".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("ServicePort".into()))),
                        ),
                    ),
                    (2, ("selector".into(), FieldType::StringMap)),
                    (3, ("clusterIP".into(), FieldType::String)),
                    (4, ("type".into(), FieldType::String)),
                    (
                        5,
                        (
                            "externalIPs".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (7, ("sessionAffinity".into(), FieldType::String)),
                    (8, ("loadBalancerIP".into(), FieldType::String)),
                    (
                        9,
                        (
                            "loadBalancerSourceRanges".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (10, ("externalName".into(), FieldType::String)),
                    (11, ("externalTrafficPolicy".into(), FieldType::String)),
                    (12, ("healthCheckNodePort".into(), FieldType::Int)),
                    (13, ("publishNotReadyAddresses".into(), FieldType::Bool)),
                    (
                        14,
                        (
                            "sessionAffinityConfig".into(),
                            FieldType::Message("SessionAffinityConfig".into()),
                        ),
                    ),
                    (
                        17,
                        (
                            "ipFamilies".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (18, ("ipFamilyPolicy".into(), FieldType::String)),
                    (
                        19,
                        (
                            "clusterIPs".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (20, ("internalTrafficPolicy".into(), FieldType::String)),
                    (
                        21,
                        ("allocateLoadBalancerNodePorts".into(), FieldType::Bool),
                    ),
                    (22, ("loadBalancerClass".into(), FieldType::String)),
                    (23, ("trafficDistribution".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ServicePort".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("protocol".into(), FieldType::String)),
                    (3, ("port".into(), FieldType::Int)),
                    (4, ("targetPort".into(), FieldType::IntOrString)),
                    (5, ("nodePort".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "ServiceStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "SessionAffinityConfig".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // Batch types
        schemas.insert(
            "Job".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("JobSpec".into()))),
                    (3, ("status".into(), FieldType::Message("JobStatus".into()))),
                ]),
            },
        );
        schemas.insert(
            "JobSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("parallelism".into(), FieldType::Int)),
                    (2, ("completions".into(), FieldType::Int)),
                    (3, ("activeDeadlineSeconds".into(), FieldType::Int)),
                    (
                        4,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (5, ("manualSelector".into(), FieldType::Bool)),
                    (
                        6,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                    (7, ("backoffLimit".into(), FieldType::Int)),
                    (8, ("ttlSecondsAfterFinished".into(), FieldType::Int)),
                    (9, ("completionMode".into(), FieldType::String)),
                    (10, ("suspend".into(), FieldType::Bool)),
                    (11, ("podReplacementPolicy".into(), FieldType::String)),
                    (12, ("managedBy".into(), FieldType::String)),
                    (13, ("backoffLimitPerIndex".into(), FieldType::Int)),
                    (14, ("maxFailedIndexes".into(), FieldType::Int)),
                    (
                        15,
                        (
                            "podFailurePolicy".into(),
                            FieldType::Message("PodFailurePolicy".into()),
                        ),
                    ),
                    (
                        16,
                        (
                            "successPolicy".into(),
                            FieldType::Message("SuccessPolicy".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "JobStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "PodFailurePolicy".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "SuccessPolicy".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // Pod (standalone)
        schemas.insert(
            "Pod".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("PodSpec".into()))),
                    (3, ("status".into(), FieldType::Message("PodStatus".into()))),
                ]),
            },
        );
        schemas.insert(
            "PodStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // ConfigMap & Secret
        schemas.insert(
            "ConfigMap".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("data".into(), FieldType::StringMap)),
                    (3, ("binaryData".into(), FieldType::StringMap)),
                    (4, ("immutable".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "Secret".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("data".into(), FieldType::StringMap)),
                    (3, ("type".into(), FieldType::String)),
                    (4, ("stringData".into(), FieldType::StringMap)),
                    (5, ("immutable".into(), FieldType::Bool)),
                ]),
            },
        );

        // Namespace
        schemas.insert(
            "Namespace".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("NamespaceSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("NamespaceStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "NamespaceSpec".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "finalizers".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                )]),
            },
        );
        schemas.insert(
            "NamespaceStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("phase".into(), FieldType::String)),
                    (
                        2,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NamespaceCondition".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "NamespaceCondition".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // ServiceAccount
        schemas.insert(
            "ServiceAccount".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "secrets".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ObjectReference".into(),
                            ))),
                        ),
                    ),
                    (
                        3,
                        (
                            "imagePullSecrets".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "LocalObjectReference".into(),
                            ))),
                        ),
                    ),
                    (4, ("automountServiceAccountToken".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "ObjectReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("kind".into(), FieldType::String)),
                    (2, ("namespace".into(), FieldType::String)),
                    (3, ("name".into(), FieldType::String)),
                    (4, ("uid".into(), FieldType::String)),
                    (5, ("apiVersion".into(), FieldType::String)),
                    (6, ("resourceVersion".into(), FieldType::String)),
                    (7, ("fieldPath".into(), FieldType::String)),
                ]),
            },
        );

        // PersistentVolumeClaim (used by StatefulSet volumeClaimTemplates)
        schemas.insert(
            "PersistentVolumeClaim".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("PersistentVolumeClaimSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("PersistentVolumeClaimStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PersistentVolumeClaimSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "accessModes".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "resources".into(),
                            FieldType::Message("VolumeResourceRequirements".into()),
                        ),
                    ),
                    (3, ("volumeName".into(), FieldType::String)),
                    (
                        4,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (5, ("storageClassName".into(), FieldType::String)),
                    (6, ("volumeMode".into(), FieldType::String)),
                    (
                        7,
                        (
                            "dataSource".into(),
                            FieldType::Message("TypedLocalObjectReference".into()),
                        ),
                    ),
                    (
                        8,
                        (
                            "dataSourceRef".into(),
                            FieldType::Message("TypedObjectReference".into()),
                        ),
                    ),
                    (9, ("volumeAttributesClassName".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "PersistentVolumeClaimStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "VolumeResourceRequirements".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("limits".into(), FieldType::StringMap)),
                    (2, ("requests".into(), FieldType::StringMap)),
                ]),
            },
        );
        schemas.insert(
            "TypedLocalObjectReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("apiGroup".into(), FieldType::String)),
                    (2, ("kind".into(), FieldType::String)),
                    (3, ("name".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "TypedObjectReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("apiGroup".into(), FieldType::String)),
                    (2, ("kind".into(), FieldType::String)),
                    (3, ("name".into(), FieldType::String)),
                    (4, ("namespace".into(), FieldType::String)),
                ]),
            },
        );

        // ReplicationController (core/v1)
        schemas.insert(
            "ReplicationController".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("ReplicationControllerSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("ReplicationControllerStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ReplicationControllerSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("replicas".into(), FieldType::Int)),
                    (2, ("selector".into(), FieldType::StringMap)),
                    (
                        3,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                    (4, ("minReadySeconds".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "ReplicationControllerStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // Endpoints
        schemas.insert(
            "Endpoints".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "subsets".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "EndpointSubset".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "EndpointSubset".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // Node
        schemas.insert(
            "Node".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("NodeSpec".into()))),
                    (
                        3,
                        ("status".into(), FieldType::Message("NodeStatus".into())),
                    ),
                ]),
            },
        );
        schemas.insert(
            "NodeSpec".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "NodeStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // ========== apiextensions types (CRDs) ==========

        schemas.insert(
            "CustomResourceDefinition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("CustomResourceDefinitionSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("CustomResourceDefinitionStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceDefinitionSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("group".into(), FieldType::String)),
                    (
                        3,
                        (
                            "names".into(),
                            FieldType::Message("CustomResourceDefinitionNames".into()),
                        ),
                    ),
                    (4, ("scope".into(), FieldType::String)),
                    (
                        7,
                        (
                            "versions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "CustomResourceDefinitionVersion".into(),
                            ))),
                        ),
                    ),
                    (
                        9,
                        (
                            "conversion".into(),
                            FieldType::Message("CustomResourceConversion".into()),
                        ),
                    ),
                    (10, ("preserveUnknownFields".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceDefinitionNames".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("plural".into(), FieldType::String)),
                    (2, ("singular".into(), FieldType::String)),
                    (
                        3,
                        (
                            "shortNames".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (4, ("kind".into(), FieldType::String)),
                    (5, ("listKind".into(), FieldType::String)),
                    (
                        6,
                        (
                            "categories".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceDefinitionVersion".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("served".into(), FieldType::Bool)),
                    (3, ("storage".into(), FieldType::Bool)),
                    (
                        4,
                        (
                            "schema".into(),
                            FieldType::Message("CustomResourceValidation".into()),
                        ),
                    ),
                    (
                        5,
                        (
                            "subresources".into(),
                            FieldType::Message("CustomResourceSubresources".into()),
                        ),
                    ),
                    (
                        6,
                        (
                            "additionalPrinterColumns".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "CustomResourceColumnDefinition".into(),
                            ))),
                        ),
                    ),
                    (7, ("deprecated".into(), FieldType::Bool)),
                    (8, ("deprecationWarning".into(), FieldType::String)),
                    (
                        9,
                        (
                            "selectableFields".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "SelectableField".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceValidation".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "openAPIV3Schema".into(),
                        FieldType::Message("JSONSchemaProps".into()),
                    ),
                )]),
            },
        );
        schemas.insert(
            "JSONSchemaProps".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("id".into(), FieldType::String)),
                    (2, ("$schema".into(), FieldType::String)),
                    (3, ("$ref".into(), FieldType::String)),
                    (4, ("description".into(), FieldType::String)),
                    (5, ("type".into(), FieldType::String)),
                    (6, ("format".into(), FieldType::String)),
                    (7, ("title".into(), FieldType::String)),
                    (8, ("default".into(), FieldType::JsonRaw)),
                    (9, ("maximum".into(), FieldType::Int)),
                    (10, ("exclusiveMaximum".into(), FieldType::Bool)),
                    (11, ("minimum".into(), FieldType::Int)),
                    (12, ("exclusiveMinimum".into(), FieldType::Bool)),
                    (13, ("maxLength".into(), FieldType::Int)),
                    (14, ("minLength".into(), FieldType::Int)),
                    (15, ("pattern".into(), FieldType::String)),
                    (16, ("maxItems".into(), FieldType::Int)),
                    (17, ("minItems".into(), FieldType::Int)),
                    (18, ("uniqueItems".into(), FieldType::Bool)),
                    (19, ("multipleOf".into(), FieldType::Int)), // double, but Int works for decode
                    (
                        20,
                        (
                            "enum".into(),
                            FieldType::Repeated(Box::new(FieldType::JsonRaw)),
                        ),
                    ),
                    (21, ("maxProperties".into(), FieldType::Int)),
                    (22, ("minProperties".into(), FieldType::Int)),
                    (
                        23,
                        (
                            "required".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        24,
                        (
                            "items".into(),
                            FieldType::Message("JSONSchemaPropsOrArray".into()),
                        ),
                    ),
                    (
                        25,
                        (
                            "allOf".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "JSONSchemaProps".into(),
                            ))),
                        ),
                    ),
                    (
                        26,
                        (
                            "oneOf".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "JSONSchemaProps".into(),
                            ))),
                        ),
                    ),
                    (
                        27,
                        (
                            "anyOf".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "JSONSchemaProps".into(),
                            ))),
                        ),
                    ),
                    (
                        28,
                        ("not".into(), FieldType::Message("JSONSchemaProps".into())),
                    ),
                    // field 29: properties — map<string, JSONSchemaProps>
                    // Protobuf maps are encoded as repeated MapEntry messages.
                    // We handle this as a special StringMap-like type but with Message values.
                    // For now, decode properties entries manually.
                    (
                        29,
                        (
                            "properties".into(),
                            FieldType::MessageMap("JSONSchemaProps".into()),
                        ),
                    ),
                    (
                        30,
                        (
                            "additionalProperties".into(),
                            FieldType::Message("JSONSchemaPropsOrBool".into()),
                        ),
                    ),
                    (37, ("nullable".into(), FieldType::Bool)),
                    (
                        38,
                        (
                            "x-kubernetes-preserve-unknown-fields".into(),
                            FieldType::Bool,
                        ),
                    ),
                    (
                        39,
                        ("x-kubernetes-embedded-resource".into(), FieldType::Bool),
                    ),
                    (40, ("x-kubernetes-int-or-string".into(), FieldType::Bool)),
                    (
                        41,
                        (
                            "x-kubernetes-list-map-keys".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (42, ("x-kubernetes-list-type".into(), FieldType::String)),
                    (43, ("x-kubernetes-map-type".into(), FieldType::String)),
                    (
                        44,
                        (
                            "x-kubernetes-validations".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ValidationRule".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        // JSONSchemaPropsOrArray: field 1 = schema (JSONSchemaProps), field 2 = jsonSchemas (repeated JSONSchemaProps)
        schemas.insert(
            "JSONSchemaPropsOrArray".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "schema".into(),
                            FieldType::Message("JSONSchemaProps".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "jsonSchemas".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "JSONSchemaProps".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "JSONSchemaPropsOrBool".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("allows".into(), FieldType::Bool)),
                    (
                        2,
                        (
                            "schema".into(),
                            FieldType::Message("JSONSchemaProps".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceSubresources".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "status".into(),
                            FieldType::Message("CustomResourceSubresourceStatus".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "scale".into(),
                            FieldType::Message("CustomResourceSubresourceScale".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceSubresourceStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "CustomResourceSubresourceScale".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("specReplicasPath".into(), FieldType::String)),
                    (2, ("statusReplicasPath".into(), FieldType::String)),
                    (3, ("labelSelectorPath".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceConversion".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("strategy".into(), FieldType::String)),
                    (
                        2,
                        (
                            "webhook".into(),
                            FieldType::Message("WebhookConversion".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "WebhookConversion".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );
        schemas.insert(
            "CustomResourceDefinitionStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "CustomResourceDefinitionCondition".into(),
                            ))),
                        ),
                    ),
                    (
                        2,
                        (
                            "acceptedNames".into(),
                            FieldType::Message("CustomResourceDefinitionNames".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "storedVersions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceDefinitionCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "CustomResourceColumnDefinition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("type".into(), FieldType::String)),
                    (3, ("format".into(), FieldType::String)),
                    (4, ("description".into(), FieldType::String)),
                    (5, ("priority".into(), FieldType::Int)),
                    (6, ("jsonPath".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "SelectableField".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("jsonPath".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "ValidationRule".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("rule".into(), FieldType::String)),
                    (2, ("message".into(), FieldType::String)),
                    (4, ("messageExpression".into(), FieldType::String)),
                    (5, ("reason".into(), FieldType::String)),
                    (6, ("fieldPath".into(), FieldType::String)),
                    (7, ("optionalOldSelf".into(), FieldType::Bool)),
                ]),
            },
        );

        // ========== rbac.authorization.k8s.io/v1 types ==========
        //
        // Field numbers from
        // https://github.com/kubernetes/kubernetes/blob/release-1.35/staging/src/k8s.io/api/rbac/v1/generated.proto
        // Without these, client-go (hydrophone, controller-runtime) sends
        // `Content-Type: application/vnd.kubernetes.protobuf` for RBAC
        // CREATE/UPDATE and the api-server rejects the body with
        // "No schema found for kind 'ClusterRole'" before any handler runs.

        schemas.insert(
            "PolicyRule".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "verbs".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "apiGroups".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        3,
                        (
                            "resources".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        4,
                        (
                            "resourceNames".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        5,
                        (
                            "nonResourceURLs".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "Subject".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("kind".into(), FieldType::String)),
                    (2, ("apiGroup".into(), FieldType::String)),
                    (3, ("name".into(), FieldType::String)),
                    (4, ("namespace".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "RoleRef".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("apiGroup".into(), FieldType::String)),
                    (2, ("kind".into(), FieldType::String)),
                    (3, ("name".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "AggregationRule".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "clusterRoleSelectors".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("LabelSelector".into()))),
                    ),
                )]),
            },
        );

        schemas.insert(
            "Role".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "rules".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("PolicyRule".into()))),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "ClusterRole".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "rules".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("PolicyRule".into()))),
                        ),
                    ),
                    (
                        3,
                        (
                            "aggregationRule".into(),
                            FieldType::Message("AggregationRule".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "RoleBinding".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "subjects".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("Subject".into()))),
                        ),
                    ),
                    (3, ("roleRef".into(), FieldType::Message("RoleRef".into()))),
                ]),
            },
        );

        schemas.insert(
            "ClusterRoleBinding".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "subjects".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("Subject".into()))),
                        ),
                    ),
                    (3, ("roleRef".into(), FieldType::Message("RoleRef".into()))),
                ]),
            },
        );

        // ========== core/v1 Pod volume + projection types ==========
        //
        // Field numbers from
        // https://github.com/kubernetes/kubernetes/blob/release-1.35/staging/src/k8s.io/api/core/v1/generated.proto
        //
        // Without these the Volume schema references projection/source
        // submessages that have no registered decoder, so client-go pod
        // CREATE/UPDATE bodies decode KeyToPath / ServiceAccountTokenProjection
        // entries as `{}` and the JSON-conversion step rejects them with
        // "missing field `path`" (KeyToPath.path and
        // ServiceAccountTokenProjection.path are required).
        //
        // ConfigMapVolumeSource, SecretProjection, and ConfigMapProjection
        // define field 1 as an embedded LocalObjectReference message that
        // Go's JSON tag flattens to a top-level `name`. They use
        // `FieldType::InlineMessage` to merge the inner `name` into the
        // parent's JSON output (same mechanism Volume uses for
        // VolumeSource).
        schemas.insert(
            "ProjectedVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "sources".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "VolumeProjection".into(),
                            ))),
                        ),
                    ),
                    (2, ("defaultMode".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "VolumeProjection".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "secret".into(),
                            FieldType::Message("SecretProjection".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "downwardAPI".into(),
                            FieldType::Message("DownwardAPIProjection".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "configMap".into(),
                            FieldType::Message("ConfigMapProjection".into()),
                        ),
                    ),
                    (
                        4,
                        (
                            "serviceAccountToken".into(),
                            FieldType::Message("ServiceAccountTokenProjection".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ServiceAccountTokenProjection".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("audience".into(), FieldType::String)),
                    (2, ("expirationSeconds".into(), FieldType::Int)),
                    (3, ("path".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "KeyToPath".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("key".into(), FieldType::String)),
                    (2, ("path".into(), FieldType::String)),
                    (3, ("mode".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "DownwardAPIVolumeFile".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("path".into(), FieldType::String)),
                    (
                        2,
                        (
                            "fieldRef".into(),
                            FieldType::Message("ObjectFieldSelector".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "resourceFieldRef".into(),
                            FieldType::Message("ResourceFieldSelector".into()),
                        ),
                    ),
                    (4, ("mode".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "DownwardAPIVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DownwardAPIVolumeFile".into(),
                            ))),
                        ),
                    ),
                    (2, ("defaultMode".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "DownwardAPIProjection".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "items".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "DownwardAPIVolumeFile".into(),
                        ))),
                    ),
                )]),
            },
        );
        schemas.insert(
            "SecretProjection".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "localObjectReference".into(),
                            FieldType::InlineMessage("LocalObjectReference".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("KeyToPath".into()))),
                        ),
                    ),
                    (4, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "ConfigMapProjection".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "localObjectReference".into(),
                            FieldType::InlineMessage("LocalObjectReference".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("KeyToPath".into()))),
                        ),
                    ),
                    (4, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "ConfigMapVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "localObjectReference".into(),
                            FieldType::InlineMessage("LocalObjectReference".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("KeyToPath".into()))),
                        ),
                    ),
                    (3, ("defaultMode".into(), FieldType::Int)),
                    (4, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "SecretVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("secretName".into(), FieldType::String)),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("KeyToPath".into()))),
                        ),
                    ),
                    (3, ("defaultMode".into(), FieldType::Int)),
                    (4, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "EmptyDirVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("medium".into(), FieldType::String)),
                    // field 2 = sizeLimit (Quantity) — no FieldType variant
                    // for Quantity strings; skip until callers need it.
                ]),
            },
        );
        schemas.insert(
            "HostPathVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("path".into(), FieldType::String)),
                    (2, ("type".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "PersistentVolumeClaimVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("claimName".into(), FieldType::String)),
                    (2, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        Self::register_scheduling_v1(&mut schemas);
        Self::register_apiextensions_v1(&mut schemas);
        Self::register_admissionregistration_v1(&mut schemas);
        Self::register_core_v1_status_networking(&mut schemas);
        Self::register_apimachinery_meta_v1(&mut schemas);
        Self::register_networking_v1(&mut schemas);
        Self::register_autoscaling_v2(&mut schemas);
        Self::register_batch_v1(&mut schemas);
        Self::register_core_v1_container_runtime(&mut schemas);
        Self::register_core_v1_kinds(&mut schemas);
        Self::register_apps_v1(&mut schemas);
        Self::register_discovery_v1(&mut schemas);
        Self::register_core_v1_cloud_volume_sources(&mut schemas);
        Self::register_apiregistration_v1(&mut schemas);
        Self::register_storage_v1(&mut schemas);
        Self::register_coordination_v1(&mut schemas);
        Self::register_policy_v1(&mut schemas);

        ProtoRegistry { schemas }
    }

    /// Register scheduling/v1 message schemas.
    ///
    /// Field numbers from
    /// k8s.io/api/scheduling/v1/generated.proto (release-1.35).
    fn register_scheduling_v1(schemas: &mut HashMap<String, MessageSchema>) {
        // PriorityClass — maps a priority class name to an integer priority.
        schemas.insert(
            "PriorityClass".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("value".into(), FieldType::Int)),
                    (3, ("globalDefault".into(), FieldType::Bool)),
                    (4, ("description".into(), FieldType::String)),
                    (5, ("preemptionPolicy".into(), FieldType::String)),
                ]),
            },
        );
    }

    fn register_apiextensions_v1(schemas: &mut HashMap<String, MessageSchema>) {
        // ExternalDocumentation — referenced from JSONSchemaProps.externalDocs (field 35).
        schemas.insert(
            "ExternalDocumentation".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("description".into(), FieldType::String)),
                    (2, ("url".into(), FieldType::String)),
                ]),
            },
        );

        // JSON — the K8s "raw JSON" wrapper. A single `raw` bytes field
        // containing the JSON payload. Used in JSONSchemaProps for `default`,
        // `enum`, and `example`. `FieldType::JsonRaw` already handles decoding
        // at the field level; this schema entry exists so callers can look the
        // type up by name and so the decoder's recursive walk has a defined
        // shape if it ever lands here directly.
        schemas.insert(
            "JSON".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("raw".into(), FieldType::Bytes))]),
            },
        );

        // JSONSchemaPropsOrStringArray — K8s oneof helper:
        //   field 1: schema    (JSONSchemaProps, optional)
        //   field 2: property  (repeated string)
        // Encoded as a regular message with both fields optional; at most one
        // is set in practice. Referenced from JSONSchemaProps.dependencies
        // (field 32, map<string, JSONSchemaPropsOrStringArray>).
        schemas.insert(
            "JSONSchemaPropsOrStringArray".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "schema".into(),
                            FieldType::Message("JSONSchemaProps".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "property".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );

        // ServiceReference — webhook service coordinates.
        schemas.insert(
            "ServiceReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("namespace".into(), FieldType::String)),
                    (2, ("name".into(), FieldType::String)),
                    (3, ("path".into(), FieldType::String)),
                    (4, ("port".into(), FieldType::Int)),
                ]),
            },
        );

        // WebhookClientConfig — how the api-server reaches a conversion
        // webhook. Either `service` (in-cluster Service reference) or `url`
        // (direct URL) is set, plus an optional `caBundle` for TLS.
        schemas.insert(
            "WebhookClientConfig".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "service".into(),
                            FieldType::Message("ServiceReference".into()),
                        ),
                    ),
                    (2, ("caBundle".into(), FieldType::Bytes)),
                    (3, ("url".into(), FieldType::String)),
                ]),
            },
        );
    }

    fn register_admissionregistration_v1(schemas: &mut HashMap<String, MessageSchema>) {
        // ----- Kinds -----

        schemas.insert(
            "MutatingWebhookConfiguration".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "webhooks".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "MutatingWebhook".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ValidatingWebhookConfiguration".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "webhooks".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ValidatingWebhook".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ValidatingAdmissionPolicy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("ValidatingAdmissionPolicySpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("ValidatingAdmissionPolicyStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ValidatingAdmissionPolicyBinding".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("ValidatingAdmissionPolicyBindingSpec".into()),
                        ),
                    ),
                ]),
            },
        );

        // ----- Webhook descriptions -----

        schemas.insert(
            "MutatingWebhook".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        (
                            "clientConfig".into(),
                            FieldType::Message("WebhookClientConfig".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "rules".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "RuleWithOperations".into(),
                            ))),
                        ),
                    ),
                    (4, ("failurePolicy".into(), FieldType::String)),
                    (
                        5,
                        (
                            "namespaceSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (6, ("sideEffects".into(), FieldType::String)),
                    (7, ("timeoutSeconds".into(), FieldType::Int)),
                    (
                        8,
                        (
                            "admissionReviewVersions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (9, ("matchPolicy".into(), FieldType::String)),
                    (10, ("reinvocationPolicy".into(), FieldType::String)),
                    (
                        11,
                        (
                            "objectSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        12,
                        (
                            "matchConditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "MatchCondition".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ValidatingWebhook".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        (
                            "clientConfig".into(),
                            FieldType::Message("WebhookClientConfig".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "rules".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "RuleWithOperations".into(),
                            ))),
                        ),
                    ),
                    (4, ("failurePolicy".into(), FieldType::String)),
                    (
                        5,
                        (
                            "namespaceSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (6, ("sideEffects".into(), FieldType::String)),
                    (7, ("timeoutSeconds".into(), FieldType::Int)),
                    (
                        8,
                        (
                            "admissionReviewVersions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (9, ("matchPolicy".into(), FieldType::String)),
                    (
                        10,
                        (
                            "objectSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        11,
                        (
                            "matchConditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "MatchCondition".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // ----- Client configuration / service reference -----

        schemas.insert(
            "WebhookClientConfig".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "service".into(),
                            FieldType::Message("ServiceReference".into()),
                        ),
                    ),
                    (2, ("caBundle".into(), FieldType::Bytes)),
                    (3, ("url".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ServiceReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("namespace".into(), FieldType::String)),
                    (2, ("name".into(), FieldType::String)),
                    (3, ("path".into(), FieldType::String)),
                    (4, ("port".into(), FieldType::Int)),
                ]),
            },
        );

        // ----- Rules -----

        schemas.insert(
            "Rule".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "apiGroups".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "apiVersions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        3,
                        (
                            "resources".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (4, ("scope".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "RuleWithOperations".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "operations".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("rule".into(), FieldType::Message("Rule".into()))),
                ]),
            },
        );
        schemas.insert(
            "NamedRuleWithOperations".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "resourceNames".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "ruleWithOperations".into(),
                            FieldType::Message("RuleWithOperations".into()),
                        ),
                    ),
                ]),
            },
        );

        // ----- Match criteria -----

        schemas.insert(
            "MatchCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("expression".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "MatchResources".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "namespaceSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "objectSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "resourceRules".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NamedRuleWithOperations".into(),
                            ))),
                        ),
                    ),
                    (
                        4,
                        (
                            "excludeResourceRules".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NamedRuleWithOperations".into(),
                            ))),
                        ),
                    ),
                    (7, ("matchPolicy".into(), FieldType::String)),
                ]),
            },
        );

        // ----- Policy parameters -----

        schemas.insert(
            "ParamKind".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("apiVersion".into(), FieldType::String)),
                    (2, ("kind".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ParamRef".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("namespace".into(), FieldType::String)),
                    (
                        3,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (4, ("parameterNotFoundAction".into(), FieldType::String)),
                ]),
            },
        );

        // ----- Validation primitives -----

        schemas.insert(
            "Validation".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("expression".into(), FieldType::String)),
                    (2, ("message".into(), FieldType::String)),
                    (3, ("reason".into(), FieldType::String)),
                    (4, ("messageExpression".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "Variable".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("expression".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "AuditAnnotation".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("key".into(), FieldType::String)),
                    (2, ("valueExpression".into(), FieldType::String)),
                ]),
            },
        );

        // ----- Status / type checking -----

        schemas.insert(
            "TypeChecking".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "expressionWarnings".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "ExpressionWarning".into(),
                        ))),
                    ),
                )]),
            },
        );
        schemas.insert(
            "ExpressionWarning".into(),
            MessageSchema {
                fields: HashMap::from([
                    (2, ("fieldRef".into(), FieldType::String)),
                    (3, ("warning".into(), FieldType::String)),
                ]),
            },
        );

        // ----- Spec / status / binding-spec messages -----

        schemas.insert(
            "ValidatingAdmissionPolicySpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("paramKind".into(), FieldType::Message("ParamKind".into())),
                    ),
                    (
                        2,
                        (
                            "matchConstraints".into(),
                            FieldType::Message("MatchResources".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "validations".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("Validation".into()))),
                        ),
                    ),
                    (4, ("failurePolicy".into(), FieldType::String)),
                    (
                        5,
                        (
                            "auditAnnotations".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "AuditAnnotation".into(),
                            ))),
                        ),
                    ),
                    (
                        6,
                        (
                            "matchConditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "MatchCondition".into(),
                            ))),
                        ),
                    ),
                    (
                        7,
                        (
                            "variables".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("Variable".into()))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ValidatingAdmissionPolicyBindingSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("policyName".into(), FieldType::String)),
                    (
                        2,
                        ("paramRef".into(), FieldType::Message("ParamRef".into())),
                    ),
                    (
                        3,
                        (
                            "matchResources".into(),
                            FieldType::Message("MatchResources".into()),
                        ),
                    ),
                    (
                        4,
                        (
                            "validationActions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ValidatingAdmissionPolicyStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("observedGeneration".into(), FieldType::Int)),
                    (
                        2,
                        (
                            "typeChecking".into(),
                            FieldType::Message("TypeChecking".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("Condition".into()))),
                        ),
                    ),
                ]),
            },
        );
    }

    fn register_core_v1_status_networking(schemas: &mut HashMap<String, MessageSchema>) {
        // ---------- node-level status sub-messages ----------

        schemas.insert(
            "AttachedVolume".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("devicePath".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "NodeAddress".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("address".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "NodeCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastHeartbeatTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (
                        4,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (5, ("reason".into(), FieldType::String)),
                    (6, ("message".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "NodeConfigSource".into(),
            MessageSchema {
                fields: HashMap::from([(
                    2,
                    (
                        "configMap".into(),
                        FieldType::Message("ConfigMapNodeConfigSource".into()),
                    ),
                )]),
            },
        );
        schemas.insert(
            "NodeConfigStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "assigned".into(),
                            FieldType::Message("NodeConfigSource".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "active".into(),
                            FieldType::Message("NodeConfigSource".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "lastKnownGood".into(),
                            FieldType::Message("NodeConfigSource".into()),
                        ),
                    ),
                    (4, ("error".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "NodeDaemonEndpoints".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "kubeletEndpoint".into(),
                        FieldType::Message("DaemonEndpoint".into()),
                    ),
                )]),
            },
        );
        schemas.insert(
            "NodeFeatures".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("supplementalGroupsPolicy".into(), FieldType::Bool))]),
            },
        );
        schemas.insert(
            "NodeRuntimeHandler".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        (
                            "features".into(),
                            FieldType::Message("NodeRuntimeHandlerFeatures".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "NodeRuntimeHandlerFeatures".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("recursiveReadOnlyMounts".into(), FieldType::Bool)),
                    (2, ("userNamespaces".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "NodeSwapStatus".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("capacity".into(), FieldType::Int))]),
            },
        );
        schemas.insert(
            "NodeSystemInfo".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("machineID".into(), FieldType::String)),
                    (2, ("systemUUID".into(), FieldType::String)),
                    (3, ("bootID".into(), FieldType::String)),
                    (4, ("kernelVersion".into(), FieldType::String)),
                    (5, ("osImage".into(), FieldType::String)),
                    (6, ("containerRuntimeVersion".into(), FieldType::String)),
                    (7, ("kubeletVersion".into(), FieldType::String)),
                    (8, ("kubeProxyVersion".into(), FieldType::String)),
                    (9, ("operatingSystem".into(), FieldType::String)),
                    (10, ("architecture".into(), FieldType::String)),
                    (
                        11,
                        ("swap".into(), FieldType::Message("NodeSwapStatus".into())),
                    ),
                ]),
            },
        );

        // ---------- scheduling sub-messages ----------

        schemas.insert(
            "NodeSelector".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "nodeSelectorTerms".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "NodeSelectorTerm".into(),
                        ))),
                    ),
                )]),
            },
        );
        schemas.insert(
            "NodeSelectorRequirement".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("key".into(), FieldType::String)),
                    (2, ("operator".into(), FieldType::String)),
                    (
                        3,
                        (
                            "values".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "NodeSelectorTerm".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "matchExpressions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NodeSelectorRequirement".into(),
                            ))),
                        ),
                    ),
                    (
                        2,
                        (
                            "matchFields".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NodeSelectorRequirement".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PodAffinityTerm".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "labelSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "namespaces".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (3, ("topologyKey".into(), FieldType::String)),
                    (
                        4,
                        (
                            "namespaceSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        5,
                        (
                            "matchLabelKeys".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        6,
                        (
                            "mismatchLabelKeys".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "Taint".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("key".into(), FieldType::String)),
                    (2, ("value".into(), FieldType::String)),
                    (3, ("effect".into(), FieldType::String)),
                    (4, ("timeAdded".into(), FieldType::Message("Time".into()))),
                ]),
            },
        );
        schemas.insert(
            "TopologySelectorLabelRequirement".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("key".into(), FieldType::String)),
                    (
                        2,
                        (
                            "values".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "TopologySelectorTerm".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "matchLabelExpressions".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "TopologySelectorLabelRequirement".into(),
                        ))),
                    ),
                )]),
            },
        );

        // ---------- pod / replication-controller condition + identity ----------

        schemas.insert(
            "PodCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        ("lastProbeTime".into(), FieldType::Message("Time".into())),
                    ),
                    (
                        4,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (5, ("reason".into(), FieldType::String)),
                    (6, ("message".into(), FieldType::String)),
                    (7, ("observedGeneration".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "PodIP".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("ip".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "PodOS".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("name".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "PodReadinessGate".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("conditionType".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "ReplicationControllerCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                ]),
            },
        );

        // ---------- service / endpoint / load-balancer networking ----------

        schemas.insert(
            "ClientIPConfig".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("timeoutSeconds".into(), FieldType::Int))]),
            },
        );
        schemas.insert(
            "EndpointAddress".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("ip".into(), FieldType::String)),
                    (
                        2,
                        (
                            "targetRef".into(),
                            FieldType::Message("ObjectReference".into()),
                        ),
                    ),
                    (3, ("hostname".into(), FieldType::String)),
                    (4, ("nodeName".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "EndpointPort".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("port".into(), FieldType::Int)),
                    (3, ("protocol".into(), FieldType::String)),
                    (4, ("appProtocol".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "HostAlias".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("ip".into(), FieldType::String)),
                    (
                        2,
                        (
                            "hostnames".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "IPBlock".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("cidr".into(), FieldType::String)),
                    (
                        2,
                        (
                            "except".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "LoadBalancerIngress".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("ip".into(), FieldType::String)),
                    (2, ("hostname".into(), FieldType::String)),
                    (3, ("ipMode".into(), FieldType::String)),
                    (
                        4,
                        (
                            "ports".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("PortStatus".into()))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "LoadBalancerStatus".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "ingress".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "LoadBalancerIngress".into(),
                        ))),
                    ),
                )]),
            },
        );
        schemas.insert(
            "PortStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("port".into(), FieldType::Int)),
                    (2, ("protocol".into(), FieldType::String)),
                    (3, ("error".into(), FieldType::String)),
                ]),
            },
        );

        // ---------- volume projection ----------

        schemas.insert(
            "ClusterTrustBundleProjection".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("signerName".into(), FieldType::String)),
                    (
                        3,
                        (
                            "labelSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (4, ("path".into(), FieldType::String)),
                    (5, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
    }

    fn register_apimachinery_meta_v1(schemas: &mut HashMap<String, MessageSchema>) {
        // Condition — generic status condition shared by most resources.
        // `lastTransitionTime` is a `Time` message (already registered).
        schemas.insert(
            "Condition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (3, ("observedGeneration".into(), FieldType::Int)),
                    (
                        4,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (5, ("reason".into(), FieldType::String)),
                    (6, ("message".into(), FieldType::String)),
                ]),
            },
        );

        // FieldsV1 — opaque JSON blob carried as bytes. The Go side decodes
        // the inner `Raw` field as a raw JSON document; treating it as
        // `Bytes` mirrors the on-wire encoding (consumers re-parse the JSON
        // separately, same as `ManagedFieldsEntry.fieldsV1`).
        schemas.insert(
            "FieldsV1".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("Raw".into(), FieldType::Bytes))]),
            },
        );

        // ListMeta — pagination/continue metadata returned on every list.
        schemas.insert(
            "ListMeta".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("selfLink".into(), FieldType::String)),
                    (2, ("resourceVersion".into(), FieldType::String)),
                    (3, ("continue".into(), FieldType::String)),
                    (4, ("remainingItemCount".into(), FieldType::Int)),
                ]),
            },
        );

        // MicroTime — microsecond-precision sibling of `Time`. Same wire
        // layout (seconds + nanos), distinct message type.
        schemas.insert(
            "MicroTime".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("seconds".into(), FieldType::Int)),
                    (2, ("nanos".into(), FieldType::Int)),
                ]),
            },
        );

        // Patch — empty message; PATCH request bodies are decoded by the
        // patch handler, not via the proto registry. Registered for
        // completeness so the decoder never reports "No schema found for
        // kind 'Patch'".
        schemas.insert(
            "Patch".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // Status — the error/result envelope returned by failing requests
        // and by DELETE on collections.
        schemas.insert(
            "Status".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ListMeta".into())),
                    ),
                    (2, ("status".into(), FieldType::String)),
                    (3, ("message".into(), FieldType::String)),
                    (4, ("reason".into(), FieldType::String)),
                    (
                        5,
                        ("details".into(), FieldType::Message("StatusDetails".into())),
                    ),
                    (6, ("code".into(), FieldType::Int)),
                ]),
            },
        );

        // StatusCause — leaf type for `StatusDetails.causes`. Not separately
        // listed in the coverage doc, but required for `Status` to decode
        // its nested `details.causes` array.
        schemas.insert(
            "StatusCause".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("reason".into(), FieldType::String)),
                    (2, ("message".into(), FieldType::String)),
                    (3, ("field".into(), FieldType::String)),
                ]),
            },
        );

        // StatusDetails — populated alongside `Status` to give clients a
        // structured handle on what failed. `uid` is field 6, not 4.
        schemas.insert(
            "StatusDetails".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("group".into(), FieldType::String)),
                    (3, ("kind".into(), FieldType::String)),
                    (
                        4,
                        (
                            "causes".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("StatusCause".into()))),
                        ),
                    ),
                    (5, ("retryAfterSeconds".into(), FieldType::Int)),
                    (6, ("uid".into(), FieldType::String)),
                ]),
            },
        );

        // TypeMeta — embedded inline in the protobuf `Unknown` envelope
        // around every kind. Registered here so a bare `TypeMeta` body
        // can also be decoded.
        schemas.insert(
            "TypeMeta".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("kind".into(), FieldType::String)),
                    (2, ("apiVersion".into(), FieldType::String)),
                ]),
            },
        );
    }

    fn register_networking_v1(schemas: &mut HashMap<String, MessageSchema>) {
        // ----- Kinds -----

        // IPAddress
        schemas.insert(
            "IPAddress".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("IPAddressSpec".into())),
                    ),
                ]),
            },
        );

        // Ingress
        schemas.insert(
            "Ingress".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("IngressSpec".into()))),
                    (
                        3,
                        ("status".into(), FieldType::Message("IngressStatus".into())),
                    ),
                ]),
            },
        );

        // IngressClass
        schemas.insert(
            "IngressClass".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("IngressClassSpec".into())),
                    ),
                ]),
            },
        );

        // NetworkPolicy
        schemas.insert(
            "NetworkPolicy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("NetworkPolicySpec".into()),
                        ),
                    ),
                ]),
            },
        );

        // ServiceCIDR
        schemas.insert(
            "ServiceCIDR".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("ServiceCIDRSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("ServiceCIDRStatus".into()),
                        ),
                    ),
                ]),
            },
        );

        // ----- Nested messages -----

        // HTTPIngressPath
        schemas.insert(
            "HTTPIngressPath".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("path".into(), FieldType::String)),
                    (3, ("pathType".into(), FieldType::String)),
                    (
                        2,
                        (
                            "backend".into(),
                            FieldType::Message("IngressBackend".into()),
                        ),
                    ),
                ]),
            },
        );

        // HTTPIngressRuleValue
        schemas.insert(
            "HTTPIngressRuleValue".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "paths".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("HTTPIngressPath".into()))),
                    ),
                )]),
            },
        );

        // IPAddressSpec
        schemas.insert(
            "IPAddressSpec".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "parentRef".into(),
                        FieldType::Message("ParentReference".into()),
                    ),
                )]),
            },
        );

        // IPBlock
        schemas.insert(
            "IPBlock".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("cidr".into(), FieldType::String)),
                    (
                        2,
                        (
                            "except".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );

        // IngressBackend
        // `resource` references core/v1.TypedLocalObjectReference, which is
        // already registered earlier in `new()`.
        schemas.insert(
            "IngressBackend".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        4,
                        (
                            "service".into(),
                            FieldType::Message("IngressServiceBackend".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "resource".into(),
                            FieldType::Message("TypedLocalObjectReference".into()),
                        ),
                    ),
                ]),
            },
        );

        // IngressClassParametersReference
        schemas.insert(
            "IngressClassParametersReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("apiGroup".into(), FieldType::String)),
                    (2, ("kind".into(), FieldType::String)),
                    (3, ("name".into(), FieldType::String)),
                    (4, ("scope".into(), FieldType::String)),
                    (5, ("namespace".into(), FieldType::String)),
                ]),
            },
        );

        // IngressClassSpec
        schemas.insert(
            "IngressClassSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("controller".into(), FieldType::String)),
                    (
                        2,
                        (
                            "parameters".into(),
                            FieldType::Message("IngressClassParametersReference".into()),
                        ),
                    ),
                ]),
            },
        );

        // IngressLoadBalancerIngress
        schemas.insert(
            "IngressLoadBalancerIngress".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("ip".into(), FieldType::String)),
                    (2, ("hostname".into(), FieldType::String)),
                    (
                        4,
                        (
                            "ports".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "IngressPortStatus".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // IngressLoadBalancerStatus
        schemas.insert(
            "IngressLoadBalancerStatus".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "ingress".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "IngressLoadBalancerIngress".into(),
                        ))),
                    ),
                )]),
            },
        );

        // IngressPortStatus
        schemas.insert(
            "IngressPortStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("port".into(), FieldType::Int)),
                    (2, ("protocol".into(), FieldType::String)),
                    (3, ("error".into(), FieldType::String)),
                ]),
            },
        );

        // IngressRule
        schemas.insert(
            "IngressRule".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("host".into(), FieldType::String)),
                    (
                        2,
                        (
                            "ingressRuleValue".into(),
                            FieldType::Message("IngressRuleValue".into()),
                        ),
                    ),
                ]),
            },
        );

        // IngressRuleValue
        schemas.insert(
            "IngressRuleValue".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "http".into(),
                        FieldType::Message("HTTPIngressRuleValue".into()),
                    ),
                )]),
            },
        );

        // IngressServiceBackend
        schemas.insert(
            "IngressServiceBackend".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        (
                            "port".into(),
                            FieldType::Message("ServiceBackendPort".into()),
                        ),
                    ),
                ]),
            },
        );

        // IngressSpec
        schemas.insert(
            "IngressSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (4, ("ingressClassName".into(), FieldType::String)),
                    (
                        1,
                        (
                            "defaultBackend".into(),
                            FieldType::Message("IngressBackend".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "tls".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("IngressTLS".into()))),
                        ),
                    ),
                    (
                        3,
                        (
                            "rules".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("IngressRule".into()))),
                        ),
                    ),
                ]),
            },
        );

        // IngressStatus
        schemas.insert(
            "IngressStatus".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "loadBalancer".into(),
                        FieldType::Message("IngressLoadBalancerStatus".into()),
                    ),
                )]),
            },
        );

        // IngressTLS
        schemas.insert(
            "IngressTLS".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "hosts".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("secretName".into(), FieldType::String)),
                ]),
            },
        );

        // NetworkPolicyEgressRule
        schemas.insert(
            "NetworkPolicyEgressRule".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "ports".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NetworkPolicyPort".into(),
                            ))),
                        ),
                    ),
                    (
                        2,
                        (
                            "to".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NetworkPolicyPeer".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // NetworkPolicyIngressRule
        schemas.insert(
            "NetworkPolicyIngressRule".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "ports".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NetworkPolicyPort".into(),
                            ))),
                        ),
                    ),
                    (
                        2,
                        (
                            "from".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NetworkPolicyPeer".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // NetworkPolicyPeer
        // `podSelector` and `namespaceSelector` reference apimachinery
        // LabelSelector, which is already registered in `new()`.
        schemas.insert(
            "NetworkPolicyPeer".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "podSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "namespaceSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (3, ("ipBlock".into(), FieldType::Message("IPBlock".into()))),
                ]),
            },
        );

        // NetworkPolicyPort
        schemas.insert(
            "NetworkPolicyPort".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("protocol".into(), FieldType::String)),
                    (2, ("port".into(), FieldType::IntOrString)),
                    (3, ("endPort".into(), FieldType::Int)),
                ]),
            },
        );

        // NetworkPolicySpec
        schemas.insert(
            "NetworkPolicySpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "podSelector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "ingress".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NetworkPolicyIngressRule".into(),
                            ))),
                        ),
                    ),
                    (
                        3,
                        (
                            "egress".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NetworkPolicyEgressRule".into(),
                            ))),
                        ),
                    ),
                    (
                        4,
                        (
                            "policyTypes".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );

        // ParentReference
        schemas.insert(
            "ParentReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("group".into(), FieldType::String)),
                    (2, ("resource".into(), FieldType::String)),
                    (3, ("namespace".into(), FieldType::String)),
                    (4, ("name".into(), FieldType::String)),
                ]),
            },
        );

        // ServiceBackendPort
        // The proto defines `number: int32` and `name: string` as two
        // separate (mutually-exclusive) fields — not a oneof / IntOrString.
        schemas.insert(
            "ServiceBackendPort".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("number".into(), FieldType::Int)),
                ]),
            },
        );

        // ServiceCIDRSpec
        schemas.insert(
            "ServiceCIDRSpec".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "cidrs".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                )]),
            },
        );

        // ServiceCIDRStatus
        // `conditions` references apimachinery `Condition`. That type is
        // not yet registered in the registry; it will decode to `{}` until
        // a future apimachinery/meta/v1 pass registers it.
        schemas.insert(
            "ServiceCIDRStatus".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "conditions".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Condition".into()))),
                    ),
                )]),
            },
        );
    }

    fn register_autoscaling_v2(schemas: &mut HashMap<String, MessageSchema>) {
        // CrossVersionObjectReference
        schemas.insert(
            "CrossVersionObjectReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("kind".into(), FieldType::String)),
                    (2, ("name".into(), FieldType::String)),
                    (3, ("apiVersion".into(), FieldType::String)),
                ]),
            },
        );

        // MetricIdentifier
        schemas.insert(
            "MetricIdentifier".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                ]),
            },
        );

        // MetricTarget — value/averageValue are Quantity (skipped)
        schemas.insert(
            "MetricTarget".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (4, ("averageUtilization".into(), FieldType::Int)),
                ]),
            },
        );

        // MetricValueStatus — value/averageValue are Quantity (skipped)
        schemas.insert(
            "MetricValueStatus".into(),
            MessageSchema {
                fields: HashMap::from([(3, ("averageUtilization".into(), FieldType::Int))]),
            },
        );

        // ContainerResourceMetricSource
        schemas.insert(
            "ContainerResourceMetricSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        ("target".into(), FieldType::Message("MetricTarget".into())),
                    ),
                    (3, ("container".into(), FieldType::String)),
                ]),
            },
        );

        // ContainerResourceMetricStatus
        schemas.insert(
            "ContainerResourceMetricStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        (
                            "current".into(),
                            FieldType::Message("MetricValueStatus".into()),
                        ),
                    ),
                    (3, ("container".into(), FieldType::String)),
                ]),
            },
        );

        // ExternalMetricSource
        schemas.insert(
            "ExternalMetricSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "metric".into(),
                            FieldType::Message("MetricIdentifier".into()),
                        ),
                    ),
                    (
                        2,
                        ("target".into(), FieldType::Message("MetricTarget".into())),
                    ),
                ]),
            },
        );

        // ExternalMetricStatus
        schemas.insert(
            "ExternalMetricStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "metric".into(),
                            FieldType::Message("MetricIdentifier".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "current".into(),
                            FieldType::Message("MetricValueStatus".into()),
                        ),
                    ),
                ]),
            },
        );

        // ObjectMetricSource
        schemas.insert(
            "ObjectMetricSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "describedObject".into(),
                            FieldType::Message("CrossVersionObjectReference".into()),
                        ),
                    ),
                    (
                        2,
                        ("target".into(), FieldType::Message("MetricTarget".into())),
                    ),
                    (
                        3,
                        (
                            "metric".into(),
                            FieldType::Message("MetricIdentifier".into()),
                        ),
                    ),
                ]),
            },
        );

        // ObjectMetricStatus
        schemas.insert(
            "ObjectMetricStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "metric".into(),
                            FieldType::Message("MetricIdentifier".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "current".into(),
                            FieldType::Message("MetricValueStatus".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "describedObject".into(),
                            FieldType::Message("CrossVersionObjectReference".into()),
                        ),
                    ),
                ]),
            },
        );

        // PodsMetricSource
        schemas.insert(
            "PodsMetricSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "metric".into(),
                            FieldType::Message("MetricIdentifier".into()),
                        ),
                    ),
                    (
                        2,
                        ("target".into(), FieldType::Message("MetricTarget".into())),
                    ),
                ]),
            },
        );

        // PodsMetricStatus
        schemas.insert(
            "PodsMetricStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "metric".into(),
                            FieldType::Message("MetricIdentifier".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "current".into(),
                            FieldType::Message("MetricValueStatus".into()),
                        ),
                    ),
                ]),
            },
        );

        // ResourceMetricSource
        schemas.insert(
            "ResourceMetricSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        ("target".into(), FieldType::Message("MetricTarget".into())),
                    ),
                ]),
            },
        );

        // ResourceMetricStatus
        schemas.insert(
            "ResourceMetricStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        (
                            "current".into(),
                            FieldType::Message("MetricValueStatus".into()),
                        ),
                    ),
                ]),
            },
        );

        // MetricSpec
        schemas.insert(
            "MetricSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (
                        2,
                        (
                            "object".into(),
                            FieldType::Message("ObjectMetricSource".into()),
                        ),
                    ),
                    (
                        3,
                        ("pods".into(), FieldType::Message("PodsMetricSource".into())),
                    ),
                    (
                        4,
                        (
                            "resource".into(),
                            FieldType::Message("ResourceMetricSource".into()),
                        ),
                    ),
                    (
                        5,
                        (
                            "external".into(),
                            FieldType::Message("ExternalMetricSource".into()),
                        ),
                    ),
                    (
                        7,
                        (
                            "containerResource".into(),
                            FieldType::Message("ContainerResourceMetricSource".into()),
                        ),
                    ),
                ]),
            },
        );

        // MetricStatus
        schemas.insert(
            "MetricStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (
                        2,
                        (
                            "object".into(),
                            FieldType::Message("ObjectMetricStatus".into()),
                        ),
                    ),
                    (
                        3,
                        ("pods".into(), FieldType::Message("PodsMetricStatus".into())),
                    ),
                    (
                        4,
                        (
                            "resource".into(),
                            FieldType::Message("ResourceMetricStatus".into()),
                        ),
                    ),
                    (
                        5,
                        (
                            "external".into(),
                            FieldType::Message("ExternalMetricStatus".into()),
                        ),
                    ),
                    (
                        7,
                        (
                            "containerResource".into(),
                            FieldType::Message("ContainerResourceMetricStatus".into()),
                        ),
                    ),
                ]),
            },
        );

        // HPAScalingPolicy
        schemas.insert(
            "HPAScalingPolicy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("value".into(), FieldType::Int)),
                    (3, ("periodSeconds".into(), FieldType::Int)),
                ]),
            },
        );

        // HPAScalingRules — tolerance is Quantity (skipped)
        schemas.insert(
            "HPAScalingRules".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("selectPolicy".into(), FieldType::String)),
                    (
                        2,
                        (
                            "policies".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "HPAScalingPolicy".into(),
                            ))),
                        ),
                    ),
                    (3, ("stabilizationWindowSeconds".into(), FieldType::Int)),
                ]),
            },
        );

        // HorizontalPodAutoscalerBehavior
        schemas.insert(
            "HorizontalPodAutoscalerBehavior".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "scaleUp".into(),
                            FieldType::Message("HPAScalingRules".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "scaleDown".into(),
                            FieldType::Message("HPAScalingRules".into()),
                        ),
                    ),
                ]),
            },
        );

        // HorizontalPodAutoscalerCondition
        schemas.insert(
            "HorizontalPodAutoscalerCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                ]),
            },
        );

        // HorizontalPodAutoscalerSpec
        schemas.insert(
            "HorizontalPodAutoscalerSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "scaleTargetRef".into(),
                            FieldType::Message("CrossVersionObjectReference".into()),
                        ),
                    ),
                    (2, ("minReplicas".into(), FieldType::Int)),
                    (3, ("maxReplicas".into(), FieldType::Int)),
                    (
                        4,
                        (
                            "metrics".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("MetricSpec".into()))),
                        ),
                    ),
                    (
                        5,
                        (
                            "behavior".into(),
                            FieldType::Message("HorizontalPodAutoscalerBehavior".into()),
                        ),
                    ),
                ]),
            },
        );

        // HorizontalPodAutoscalerStatus
        schemas.insert(
            "HorizontalPodAutoscalerStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("observedGeneration".into(), FieldType::Int)),
                    (
                        2,
                        ("lastScaleTime".into(), FieldType::Message("Time".into())),
                    ),
                    (3, ("currentReplicas".into(), FieldType::Int)),
                    (4, ("desiredReplicas".into(), FieldType::Int)),
                    (
                        5,
                        (
                            "currentMetrics".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "MetricStatus".into(),
                            ))),
                        ),
                    ),
                    (
                        6,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "HorizontalPodAutoscalerCondition".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // HorizontalPodAutoscaler (top-level kind)
        schemas.insert(
            "HorizontalPodAutoscaler".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("HorizontalPodAutoscalerSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("HorizontalPodAutoscalerStatus".into()),
                        ),
                    ),
                ]),
            },
        );
    }

    fn register_batch_v1(schemas: &mut HashMap<String, MessageSchema>) {
        schemas.insert(
            "CronJob".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("CronJobSpec".into()))),
                    (
                        3,
                        ("status".into(), FieldType::Message("CronJobStatus".into())),
                    ),
                ]),
            },
        );
        schemas.insert(
            "CronJobSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("schedule".into(), FieldType::String)),
                    (2, ("startingDeadlineSeconds".into(), FieldType::Int)),
                    (3, ("concurrencyPolicy".into(), FieldType::String)),
                    (4, ("suspend".into(), FieldType::Bool)),
                    (
                        5,
                        (
                            "jobTemplate".into(),
                            FieldType::Message("JobTemplateSpec".into()),
                        ),
                    ),
                    (6, ("successfulJobsHistoryLimit".into(), FieldType::Int)),
                    (7, ("failedJobsHistoryLimit".into(), FieldType::Int)),
                    (8, ("timeZone".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "CronJobStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "active".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ObjectReference".into(),
                            ))),
                        ),
                    ),
                    (
                        4,
                        ("lastScheduleTime".into(), FieldType::Message("Time".into())),
                    ),
                    (
                        5,
                        (
                            "lastSuccessfulTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "JobCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        ("lastProbeTime".into(), FieldType::Message("Time".into())),
                    ),
                    (
                        4,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (5, ("reason".into(), FieldType::String)),
                    (6, ("message".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "JobTemplateSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("JobSpec".into()))),
                ]),
            },
        );
        schemas.insert(
            "PodFailurePolicyOnExitCodesRequirement".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("containerName".into(), FieldType::String)),
                    (2, ("operator".into(), FieldType::String)),
                    (
                        3,
                        (
                            "values".into(),
                            FieldType::Repeated(Box::new(FieldType::Int)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PodFailurePolicyOnPodConditionsPattern".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "PodFailurePolicyRule".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("action".into(), FieldType::String)),
                    (
                        2,
                        (
                            "onExitCodes".into(),
                            FieldType::Message("PodFailurePolicyOnExitCodesRequirement".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "onPodConditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PodFailurePolicyOnPodConditionsPattern".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "SuccessPolicyRule".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("succeededIndexes".into(), FieldType::String)),
                    (2, ("succeededCount".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "UncountedTerminatedPods".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "succeeded".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "failed".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
    }

    fn register_core_v1_container_runtime(schemas: &mut HashMap<String, MessageSchema>) {
        schemas.insert(
            "ContainerImage".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "names".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("sizeBytes".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "ContainerResizePolicy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("resourceName".into(), FieldType::String)),
                    (2, ("restartPolicy".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ContainerRestartRule".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("action".into(), FieldType::String)),
                    (
                        2,
                        (
                            "exitCodes".into(),
                            FieldType::Message("ContainerRestartRuleOnExitCodes".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ContainerRestartRuleOnExitCodes".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("operator".into(), FieldType::String)),
                    (
                        2,
                        (
                            "values".into(),
                            FieldType::Repeated(Box::new(FieldType::Int)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ContainerStateRunning".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    ("startedAt".into(), FieldType::Message("Time".into())),
                )]),
            },
        );
        schemas.insert(
            "ContainerStateTerminated".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("exitCode".into(), FieldType::Int)),
                    (2, ("signal".into(), FieldType::Int)),
                    (3, ("reason".into(), FieldType::String)),
                    (4, ("message".into(), FieldType::String)),
                    (5, ("startedAt".into(), FieldType::Message("Time".into()))),
                    (6, ("finishedAt".into(), FieldType::Message("Time".into()))),
                    (7, ("containerID".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ContainerStateWaiting".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("reason".into(), FieldType::String)),
                    (2, ("message".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ContainerUser".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "linux".into(),
                        FieldType::Message("LinuxContainerUser".into()),
                    ),
                )]),
            },
        );
        schemas.insert(
            "EnvFromSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("prefix".into(), FieldType::String)),
                    (
                        2,
                        (
                            "configMapRef".into(),
                            FieldType::Message("ConfigMapEnvSource".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "secretRef".into(),
                            FieldType::Message("SecretEnvSource".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "EphemeralContainer".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "ephemeralContainerCommon".into(),
                            FieldType::Message("EphemeralContainerCommon".into()),
                        ),
                    ),
                    (2, ("targetContainerName".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "EphemeralContainerCommon".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("image".into(), FieldType::String)),
                    (
                        3,
                        (
                            "command".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        4,
                        (
                            "args".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (5, ("workingDir".into(), FieldType::String)),
                    (
                        6,
                        (
                            "ports".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ContainerPort".into(),
                            ))),
                        ),
                    ),
                    (
                        7,
                        (
                            "env".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("EnvVar".into()))),
                        ),
                    ),
                    (
                        8,
                        (
                            "resources".into(),
                            FieldType::Message("ResourceRequirements".into()),
                        ),
                    ),
                    (
                        9,
                        (
                            "volumeMounts".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("VolumeMount".into()))),
                        ),
                    ),
                    (
                        10,
                        ("livenessProbe".into(), FieldType::Message("Probe".into())),
                    ),
                    (
                        11,
                        ("readinessProbe".into(), FieldType::Message("Probe".into())),
                    ),
                    (
                        12,
                        ("lifecycle".into(), FieldType::Message("Lifecycle".into())),
                    ),
                    (13, ("terminationMessagePath".into(), FieldType::String)),
                    (14, ("imagePullPolicy".into(), FieldType::String)),
                    (
                        15,
                        (
                            "securityContext".into(),
                            FieldType::Message("SecurityContext".into()),
                        ),
                    ),
                    (16, ("stdin".into(), FieldType::Bool)),
                    (17, ("stdinOnce".into(), FieldType::Bool)),
                    (18, ("tty".into(), FieldType::Bool)),
                    (
                        19,
                        (
                            "envFrom".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "EnvFromSource".into(),
                            ))),
                        ),
                    ),
                    (20, ("terminationMessagePolicy".into(), FieldType::String)),
                    (
                        21,
                        (
                            "volumeDevices".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "VolumeDevice".into(),
                            ))),
                        ),
                    ),
                    (
                        22,
                        ("startupProbe".into(), FieldType::Message("Probe".into())),
                    ),
                    (
                        23,
                        (
                            "resizePolicy".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ContainerResizePolicy".into(),
                            ))),
                        ),
                    ),
                    (24, ("restartPolicy".into(), FieldType::String)),
                    (
                        25,
                        (
                            "restartPolicyRules".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ContainerRestartRule".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "LinuxContainerUser".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("uid".into(), FieldType::Int)),
                    (2, ("gid".into(), FieldType::Int)),
                    (
                        3,
                        (
                            "supplementalGroups".into(),
                            FieldType::Repeated(Box::new(FieldType::Int)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PodResourceClaim".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (3, ("resourceClaimName".into(), FieldType::String)),
                    (4, ("resourceClaimTemplateName".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "PodResourceClaimStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("resourceClaimName".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "PodSchedulingGate".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("name".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "ResourceClaim".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("request".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ResourceHealth".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("resourceID".into(), FieldType::String)),
                    (2, ("health".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ResourceStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        (
                            "resources".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ResourceHealth".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "Sysctl".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("value".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "WindowsSecurityContextOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("gmsaCredentialSpecName".into(), FieldType::String)),
                    (2, ("gmsaCredentialSpec".into(), FieldType::String)),
                    (3, ("runAsUserName".into(), FieldType::String)),
                    (4, ("hostProcess".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "LocalVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("path".into(), FieldType::String)),
                    (2, ("fsType".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "PreferredSchedulingTerm".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("weight".into(), FieldType::Int)),
                    (
                        2,
                        (
                            "preference".into(),
                            FieldType::Message("NodeSelectorTerm".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "WeightedPodAffinityTerm".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("weight".into(), FieldType::Int)),
                    (
                        2,
                        (
                            "podAffinityTerm".into(),
                            FieldType::Message("PodAffinityTerm".into()),
                        ),
                    ),
                ]),
            },
        );
    }

    fn register_core_v1_kinds(schemas: &mut HashMap<String, MessageSchema>) {
        // Binding — core/v1
        schemas.insert(
            "Binding".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "target".into(),
                            FieldType::Message("ObjectReference".into()),
                        ),
                    ),
                ]),
            },
        );

        // ComponentStatus / ComponentCondition
        schemas.insert(
            "ComponentStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ComponentCondition".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ComponentCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (3, ("message".into(), FieldType::String)),
                    (4, ("error".into(), FieldType::String)),
                ]),
            },
        );

        // Event / EventSeries / EventSource
        schemas.insert(
            "Event".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "involvedObject".into(),
                            FieldType::Message("ObjectReference".into()),
                        ),
                    ),
                    (3, ("reason".into(), FieldType::String)),
                    (4, ("message".into(), FieldType::String)),
                    (
                        5,
                        ("source".into(), FieldType::Message("EventSource".into())),
                    ),
                    (
                        6,
                        ("firstTimestamp".into(), FieldType::Message("Time".into())),
                    ),
                    (
                        7,
                        ("lastTimestamp".into(), FieldType::Message("Time".into())),
                    ),
                    (8, ("count".into(), FieldType::Int)),
                    (9, ("type".into(), FieldType::String)),
                    (
                        10,
                        ("eventTime".into(), FieldType::Message("MicroTime".into())),
                    ),
                    (
                        11,
                        ("series".into(), FieldType::Message("EventSeries".into())),
                    ),
                    (12, ("action".into(), FieldType::String)),
                    (
                        13,
                        (
                            "related".into(),
                            FieldType::Message("ObjectReference".into()),
                        ),
                    ),
                    (14, ("reportingComponent".into(), FieldType::String)),
                    (15, ("reportingInstance".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "EventSeries".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("count".into(), FieldType::Int)),
                    (
                        2,
                        (
                            "lastObservedTime".into(),
                            FieldType::Message("MicroTime".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "EventSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("component".into(), FieldType::String)),
                    (2, ("host".into(), FieldType::String)),
                ]),
            },
        );

        // LimitRange / LimitRangeSpec / LimitRangeItem
        schemas.insert(
            "LimitRange".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("LimitRangeSpec".into())),
                    ),
                ]),
            },
        );
        schemas.insert(
            "LimitRangeSpec".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "limits".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("LimitRangeItem".into()))),
                    ),
                )]),
            },
        );
        // LimitRangeItem: max/min/default/defaultRequest/maxLimitRequestRatio are
        // map<string, Quantity> — Quantity is a leaf scalar with no schema entry,
        // so we skip those fields silently. Only `type` is registered.
        schemas.insert(
            "LimitRangeItem".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("type".into(), FieldType::String))]),
            },
        );

        // PersistentVolume / PersistentVolumeSpec / PersistentVolumeStatus / VolumeNodeAffinity
        schemas.insert(
            "PersistentVolume".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("PersistentVolumeSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("PersistentVolumeStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        // PersistentVolumeSpec.capacity is map<string, Quantity> — skipped.
        // persistentVolumeSource (field 2) references PersistentVolumeSource, which
        // is not registered (one of many vendor volume sources excluded from this
        // worker's scope) — it will decode to `{}` per the registry's fallback.
        schemas.insert(
            "PersistentVolumeSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        2,
                        (
                            "persistentVolumeSource".into(),
                            FieldType::Message("PersistentVolumeSource".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "accessModes".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        4,
                        (
                            "claimRef".into(),
                            FieldType::Message("ObjectReference".into()),
                        ),
                    ),
                    (
                        5,
                        ("persistentVolumeReclaimPolicy".into(), FieldType::String),
                    ),
                    (6, ("storageClassName".into(), FieldType::String)),
                    (
                        7,
                        (
                            "mountOptions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (8, ("volumeMode".into(), FieldType::String)),
                    (
                        9,
                        (
                            "nodeAffinity".into(),
                            FieldType::Message("VolumeNodeAffinity".into()),
                        ),
                    ),
                    (10, ("volumeAttributesClassName".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "PersistentVolumeStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("phase".into(), FieldType::String)),
                    (2, ("message".into(), FieldType::String)),
                    (3, ("reason".into(), FieldType::String)),
                    (
                        4,
                        (
                            "lastPhaseTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                ]),
            },
        );
        // VolumeNodeAffinity.required references NodeSelector which is not yet
        // registered — decodes as `{}`. Field number per generated.proto.
        schemas.insert(
            "VolumeNodeAffinity".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    ("required".into(), FieldType::Message("NodeSelector".into())),
                )]),
            },
        );

        // PersistentVolumeClaimTemplate
        schemas.insert(
            "PersistentVolumeClaimTemplate".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("PersistentVolumeClaimSpec".into()),
                        ),
                    ),
                ]),
            },
        );

        // PodStatusResult
        schemas.insert(
            "PodStatusResult".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("status".into(), FieldType::Message("PodStatus".into()))),
                ]),
            },
        );

        // PodTemplate
        schemas.insert(
            "PodTemplate".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "template".into(),
                            FieldType::Message("PodTemplateSpec".into()),
                        ),
                    ),
                ]),
            },
        );

        // RangeAllocation
        schemas.insert(
            "RangeAllocation".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("range".into(), FieldType::String)),
                    (3, ("data".into(), FieldType::Bytes)),
                ]),
            },
        );

        // ResourceQuota / ResourceQuotaSpec / ResourceQuotaStatus
        schemas.insert(
            "ResourceQuota".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("ResourceQuotaSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("ResourceQuotaStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        // ResourceQuotaSpec.hard is map<string, Quantity> — skipped.
        schemas.insert(
            "ResourceQuotaSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        2,
                        (
                            "scopes".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        3,
                        (
                            "scopeSelector".into(),
                            FieldType::Message("ScopeSelector".into()),
                        ),
                    ),
                ]),
            },
        );
        // ResourceQuotaStatus.hard and .used are map<string, Quantity> — skipped.
        // No non-Quantity fields remain, but register an empty schema so the type
        // is known to the decoder (otherwise nested decode produces a missing-schema
        // warning instead of `{}`).
        schemas.insert(
            "ResourceQuotaStatus".into(),
            MessageSchema {
                fields: HashMap::new(),
            },
        );

        // ScopeSelector / ScopedResourceSelectorRequirement
        schemas.insert(
            "ScopeSelector".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "matchExpressions".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "ScopedResourceSelectorRequirement".into(),
                        ))),
                    ),
                )]),
            },
        );
        schemas.insert(
            "ScopedResourceSelectorRequirement".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("scopeName".into(), FieldType::String)),
                    (2, ("operator".into(), FieldType::String)),
                    (
                        3,
                        (
                            "values".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
    }

    fn object_meta_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (2, ("generateName".into(), FieldType::String)),
                (3, ("namespace".into(), FieldType::String)),
                (5, ("uid".into(), FieldType::String)),
                (6, ("resourceVersion".into(), FieldType::String)),
                (7, ("generation".into(), FieldType::Int)),
                (
                    8,
                    (
                        "creationTimestamp".into(),
                        FieldType::Message("Time".into()),
                    ),
                ),
                (
                    9,
                    (
                        "deletionTimestamp".into(),
                        FieldType::Message("Time".into()),
                    ),
                ),
                (10, ("deletionGracePeriodSeconds".into(), FieldType::Int)),
                (11, ("labels".into(), FieldType::StringMap)),
                (12, ("annotations".into(), FieldType::StringMap)),
                (
                    13,
                    (
                        "ownerReferences".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("OwnerReference".into()))),
                    ),
                ),
                (
                    14,
                    (
                        "finalizers".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                ),
                (
                    17,
                    (
                        "managedFields".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "ManagedFieldsEntry".into(),
                        ))),
                    ),
                ),
            ]),
        }
    }

    fn owner_reference_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("apiVersion".into(), FieldType::String)),
                (2, ("kind".into(), FieldType::String)),
                (3, ("name".into(), FieldType::String)),
                (4, ("uid".into(), FieldType::String)),
                (6, ("controller".into(), FieldType::Bool)),
                (7, ("blockOwnerDeletion".into(), FieldType::Bool)),
            ]),
        }
    }

    fn label_selector_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("matchLabels".into(), FieldType::StringMap)),
                (
                    2,
                    (
                        "matchExpressions".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "LabelSelectorRequirement".into(),
                        ))),
                    ),
                ),
            ]),
        }
    }

    fn deployment_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                ),
                (
                    2,
                    ("spec".into(), FieldType::Message("DeploymentSpec".into())),
                ),
                (
                    3,
                    (
                        "status".into(),
                        FieldType::Message("DeploymentStatus".into()),
                    ),
                ),
            ]),
        }
    }

    fn deployment_spec_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("replicas".into(), FieldType::Int)),
                (
                    2,
                    (
                        "selector".into(),
                        FieldType::Message("LabelSelector".into()),
                    ),
                ),
                (
                    3,
                    (
                        "template".into(),
                        FieldType::Message("PodTemplateSpec".into()),
                    ),
                ),
                (
                    4,
                    (
                        "strategy".into(),
                        FieldType::Message("DeploymentStrategy".into()),
                    ),
                ),
                (5, ("minReadySeconds".into(), FieldType::Int)),
                (6, ("revisionHistoryLimit".into(), FieldType::Int)),
                (7, ("paused".into(), FieldType::Bool)),
                (9, ("progressDeadlineSeconds".into(), FieldType::Int)),
            ]),
        }
    }

    fn deployment_status_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("observedGeneration".into(), FieldType::Int)),
                (2, ("replicas".into(), FieldType::Int)),
                (3, ("updatedReplicas".into(), FieldType::Int)),
                (4, ("unavailableReplicas".into(), FieldType::Int)),
                (5, ("availableReplicas".into(), FieldType::Int)),
                (
                    6,
                    (
                        "conditions".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "DeploymentCondition".into(),
                        ))),
                    ),
                ),
                (7, ("readyReplicas".into(), FieldType::Int)),
                (8, ("collisionCount".into(), FieldType::Int)),
            ]),
        }
    }

    fn deployment_strategy_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("type".into(), FieldType::String)),
                (
                    2,
                    (
                        "rollingUpdate".into(),
                        FieldType::Message("RollingUpdateDeployment".into()),
                    ),
                ),
            ]),
        }
    }

    fn pod_template_spec_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                ),
                (2, ("spec".into(), FieldType::Message("PodSpec".into()))),
            ]),
        }
    }

    fn pod_spec_schema() -> MessageSchema {
        // From core/v1/generated.proto — PodSpec has MANY fields
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    (
                        "volumes".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Volume".into()))),
                    ),
                ),
                (
                    2,
                    (
                        "containers".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Container".into()))),
                    ),
                ),
                (3, ("restartPolicy".into(), FieldType::String)),
                (4, ("terminationGracePeriodSeconds".into(), FieldType::Int)),
                (5, ("activeDeadlineSeconds".into(), FieldType::Int)),
                (6, ("dnsPolicy".into(), FieldType::String)),
                (7, ("nodeSelector".into(), FieldType::StringMap)),
                (8, ("serviceAccountName".into(), FieldType::String)),
                (9, ("serviceAccount".into(), FieldType::String)),
                (10, ("nodeName".into(), FieldType::String)),
                (11, ("hostNetwork".into(), FieldType::Bool)),
                (12, ("hostPID".into(), FieldType::Bool)),
                (13, ("hostIPC".into(), FieldType::Bool)),
                (
                    14,
                    (
                        "securityContext".into(),
                        FieldType::Message("PodSecurityContext".into()),
                    ),
                ),
                (
                    15,
                    (
                        "imagePullSecrets".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "LocalObjectReference".into(),
                        ))),
                    ),
                ),
                (16, ("hostname".into(), FieldType::String)),
                (17, ("subdomain".into(), FieldType::String)),
                (
                    18,
                    ("affinity".into(), FieldType::Message("Affinity".into())),
                ),
                (19, ("schedulerName".into(), FieldType::String)),
                (
                    20,
                    (
                        "initContainers".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Container".into()))),
                    ),
                ),
                (21, ("automountServiceAccountToken".into(), FieldType::Bool)),
                (
                    22,
                    (
                        "tolerations".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Toleration".into()))),
                    ),
                ),
                (
                    24,
                    (
                        "hostAliases".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("HostAlias".into()))),
                    ),
                ),
                (25, ("priorityClassName".into(), FieldType::String)),
                (26, ("priority".into(), FieldType::Int)),
                (
                    27,
                    (
                        "dnsConfig".into(),
                        FieldType::Message("PodDNSConfig".into()),
                    ),
                ),
                (28, ("shareProcessNamespace".into(), FieldType::Bool)),
                (
                    29,
                    (
                        "readinessGates".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "PodReadinessGate".into(),
                        ))),
                    ),
                ),
                (30, ("runtimeClassName".into(), FieldType::String)),
                (32, ("overhead".into(), FieldType::StringMap)),
                (33, ("enableServiceLinks".into(), FieldType::Bool)),
                (
                    34,
                    (
                        "ephemeralContainers".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Container".into()))),
                    ),
                ),
                (
                    35,
                    (
                        "topologySpreadConstraints".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "TopologySpreadConstraint".into(),
                        ))),
                    ),
                ),
                (36, ("setHostnameAsFQDN".into(), FieldType::Bool)),
                (37, ("os".into(), FieldType::Message("PodOS".into()))),
                (
                    39,
                    (
                        "resourceClaims".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "PodResourceClaim".into(),
                        ))),
                    ),
                ),
                (
                    40,
                    (
                        "schedulingGates".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "PodSchedulingGate".into(),
                        ))),
                    ),
                ),
            ]),
        }
    }

    fn container_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (2, ("image".into(), FieldType::String)),
                (
                    3,
                    (
                        "command".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                ),
                (
                    4,
                    (
                        "args".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                ),
                (5, ("workingDir".into(), FieldType::String)),
                (
                    6,
                    (
                        "ports".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("ContainerPort".into()))),
                    ),
                ),
                (
                    7,
                    (
                        "env".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("EnvVar".into()))),
                    ),
                ),
                (
                    8,
                    (
                        "resources".into(),
                        FieldType::Message("ResourceRequirements".into()),
                    ),
                ),
                (
                    9,
                    (
                        "volumeMounts".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("VolumeMount".into()))),
                    ),
                ),
                (
                    10,
                    ("livenessProbe".into(), FieldType::Message("Probe".into())),
                ),
                (
                    11,
                    ("readinessProbe".into(), FieldType::Message("Probe".into())),
                ),
                (
                    12,
                    ("lifecycle".into(), FieldType::Message("Lifecycle".into())),
                ),
                (13, ("terminationMessagePath".into(), FieldType::String)),
                (14, ("imagePullPolicy".into(), FieldType::String)),
                (
                    15,
                    (
                        "securityContext".into(),
                        FieldType::Message("SecurityContext".into()),
                    ),
                ),
                (16, ("stdin".into(), FieldType::Bool)),
                (17, ("stdinOnce".into(), FieldType::Bool)),
                (18, ("tty".into(), FieldType::Bool)),
                (
                    19,
                    (
                        "envFrom".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("EnvFromSource".into()))),
                    ),
                ),
                (20, ("terminationMessagePolicy".into(), FieldType::String)),
                (
                    22,
                    ("startupProbe".into(), FieldType::Message("Probe".into())),
                ),
                (
                    23,
                    (
                        "volumeDevices".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("VolumeDevice".into()))),
                    ),
                ),
                (
                    24,
                    (
                        "resizePolicy".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "ContainerResizePolicy".into(),
                        ))),
                    ),
                ),
                (25, ("restartPolicy".into(), FieldType::String)),
            ]),
        }
    }

    fn container_port_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (2, ("hostPort".into(), FieldType::Int)),
                (3, ("containerPort".into(), FieldType::Int)),
                (4, ("protocol".into(), FieldType::String)),
                (5, ("hostIP".into(), FieldType::String)),
            ]),
        }
    }

    fn security_context_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    (
                        "capabilities".into(),
                        FieldType::Message("Capabilities".into()),
                    ),
                ),
                (2, ("privileged".into(), FieldType::Bool)),
                (
                    3,
                    (
                        "seLinuxOptions".into(),
                        FieldType::Message("SELinuxOptions".into()),
                    ),
                ),
                (4, ("runAsUser".into(), FieldType::Int)),
                (5, ("runAsNonRoot".into(), FieldType::Bool)),
                (6, ("readOnlyRootFilesystem".into(), FieldType::Bool)),
                (7, ("allowPrivilegeEscalation".into(), FieldType::Bool)),
                (8, ("runAsGroup".into(), FieldType::Int)),
                (9, ("procMount".into(), FieldType::String)),
                (
                    11,
                    (
                        "seccompProfile".into(),
                        FieldType::Message("SeccompProfile".into()),
                    ),
                ),
                (
                    12,
                    (
                        "appArmorProfile".into(),
                        FieldType::Message("AppArmorProfile".into()),
                    ),
                ),
            ]),
        }
    }

    fn resource_requirements_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("limits".into(), FieldType::StringMap)),
                (2, ("requests".into(), FieldType::StringMap)),
                (
                    3,
                    (
                        "claims".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("ResourceClaim".into()))),
                    ),
                ),
            ]),
        }
    }

    fn volume_schema() -> MessageSchema {
        // The proto wire format wraps every Volume source type in an
        // embedded `VolumeSource` message at field 2. Go's JSON tag
        // flattens VolumeSource into Volume, so decoded JSON keys
        // (`hostPath`, `emptyDir`, ...) appear at the Volume level.
        // The inline-message variant performs that merge — fields live in
        // `volume_source_schema()` below.
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (
                    2,
                    (
                        "volumeSource".into(),
                        FieldType::InlineMessage("VolumeSource".into()),
                    ),
                ),
            ]),
        }
    }

    fn volume_source_schema() -> MessageSchema {
        // Field numbers from
        // https://github.com/kubernetes/kubernetes/blob/release-1.35/staging/src/k8s.io/api/core/v1/generated.proto
        // (message VolumeSource). Source kinds we don't yet decode
        // (gitRepo, nfs, iscsi, glusterfs, rbd, flex, cinder, cephfs,
        // flocker, azure*, vsphere, photon, portworx, scaleIO,
        // storageOS, csi, ephemeral) are intentionally omitted — the
        // decoder ignores unknown field numbers so requests using them
        // still round-trip with the supported subset.
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    (
                        "hostPath".into(),
                        FieldType::Message("HostPathVolumeSource".into()),
                    ),
                ),
                (
                    2,
                    (
                        "emptyDir".into(),
                        FieldType::Message("EmptyDirVolumeSource".into()),
                    ),
                ),
                (
                    6,
                    (
                        "secret".into(),
                        FieldType::Message("SecretVolumeSource".into()),
                    ),
                ),
                (
                    10,
                    (
                        "persistentVolumeClaim".into(),
                        FieldType::Message("PersistentVolumeClaimVolumeSource".into()),
                    ),
                ),
                (
                    19,
                    (
                        "configMap".into(),
                        FieldType::Message("ConfigMapVolumeSource".into()),
                    ),
                ),
                (
                    16,
                    (
                        "downwardAPI".into(),
                        FieldType::Message("DownwardAPIVolumeSource".into()),
                    ),
                ),
                (
                    26,
                    (
                        "projected".into(),
                        FieldType::Message("ProjectedVolumeSource".into()),
                    ),
                ),
            ]),
        }
    }

    fn volume_mount_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (2, ("readOnly".into(), FieldType::Bool)),
                (3, ("mountPath".into(), FieldType::String)),
                (4, ("subPath".into(), FieldType::String)),
                (5, ("mountPropagation".into(), FieldType::String)),
                (6, ("subPathExpr".into(), FieldType::String)),
                (7, ("recursiveReadOnly".into(), FieldType::String)),
            ]),
        }
    }

    fn env_var_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("name".into(), FieldType::String)),
                (2, ("value".into(), FieldType::String)),
                (
                    3,
                    (
                        "valueFrom".into(),
                        FieldType::Message("EnvVarSource".into()),
                    ),
                ),
            ]),
        }
    }

    fn env_var_source_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    (
                        "fieldRef".into(),
                        FieldType::Message("ObjectFieldSelector".into()),
                    ),
                ),
                (
                    2,
                    (
                        "resourceFieldRef".into(),
                        FieldType::Message("ResourceFieldSelector".into()),
                    ),
                ),
                (
                    3,
                    (
                        "configMapKeyRef".into(),
                        FieldType::Message("ConfigMapKeySelector".into()),
                    ),
                ),
                (
                    4,
                    (
                        "secretKeyRef".into(),
                        FieldType::Message("SecretKeySelector".into()),
                    ),
                ),
            ]),
        }
    }

    fn probe_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    ("handler".into(), FieldType::Message("ProbeHandler".into())),
                ),
                (2, ("initialDelaySeconds".into(), FieldType::Int)),
                (3, ("timeoutSeconds".into(), FieldType::Int)),
                (4, ("periodSeconds".into(), FieldType::Int)),
                (5, ("successThreshold".into(), FieldType::Int)),
                (6, ("failureThreshold".into(), FieldType::Int)),
                (7, ("terminationGracePeriodSeconds".into(), FieldType::Int)),
            ]),
        }
    }

    fn probe_handler_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (1, ("exec".into(), FieldType::Message("ExecAction".into()))),
                (
                    2,
                    ("httpGet".into(), FieldType::Message("HTTPGetAction".into())),
                ),
                (
                    3,
                    (
                        "tcpSocket".into(),
                        FieldType::Message("TCPSocketAction".into()),
                    ),
                ),
                (4, ("grpc".into(), FieldType::Message("GRPCAction".into()))),
            ]),
        }
    }

    fn pod_security_context_schema() -> MessageSchema {
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    (
                        "seLinuxOptions".into(),
                        FieldType::Message("SELinuxOptions".into()),
                    ),
                ),
                (2, ("runAsUser".into(), FieldType::Int)),
                (3, ("runAsNonRoot".into(), FieldType::Bool)),
                (
                    4,
                    (
                        "supplementalGroups".into(),
                        FieldType::Repeated(Box::new(FieldType::Int)),
                    ),
                ),
                (5, ("fsGroup".into(), FieldType::Int)),
                (6, ("runAsGroup".into(), FieldType::Int)),
                (
                    7,
                    (
                        "sysctls".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("Sysctl".into()))),
                    ),
                ),
                (9, ("fsGroupChangePolicy".into(), FieldType::String)),
                (
                    10,
                    (
                        "seccompProfile".into(),
                        FieldType::Message("SeccompProfile".into()),
                    ),
                ),
                (
                    12,
                    (
                        "appArmorProfile".into(),
                        FieldType::Message("AppArmorProfile".into()),
                    ),
                ),
                (13, ("supplementalGroupsPolicy".into(), FieldType::String)),
            ]),
        }
    }

    /// Decode a protobuf message to JSON using the schema for the given message type.
    /// Returns None if the message type is not in the registry.
    pub fn decode_message(&self, msg_type: &str, data: &[u8]) -> Option<Value> {
        let schema = self.schemas.get(msg_type)?;
        Some(self.decode_with_schema(schema, data))
    }

    /// Decode protobuf bytes using a specific schema
    fn decode_with_schema(&self, schema: &MessageSchema, data: &[u8]) -> Value {
        let mut obj = Map::new();
        let mut repeated_fields: HashMap<String, Vec<Value>> = HashMap::new();
        let mut pos = 0;

        while pos < data.len() {
            // Read tag as varint
            let (tag, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            let field_num = (tag >> 3) as u32;
            let wire_type = (tag & 0x07) as u8;

            match wire_type {
                WIRE_VARINT => {
                    let (value, new_pos) = match read_varint(data, pos) {
                        Some(v) => v,
                        None => break,
                    };
                    pos = new_pos;

                    if let Some((name, field_type)) = schema.fields.get(&field_num) {
                        let json_val = match field_type {
                            FieldType::Bool => Value::Bool(value != 0),
                            FieldType::Int => json!(value as i64),
                            _ => json!(value as i64),
                        };
                        match field_type {
                            FieldType::Repeated(_) => {
                                repeated_fields
                                    .entry(name.clone())
                                    .or_default()
                                    .push(json_val);
                            }
                            _ => {
                                obj.insert(name.clone(), json_val);
                            }
                        }
                    }
                }
                WIRE_64BIT => {
                    if pos + 8 > data.len() {
                        break;
                    }
                    let value = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    if let Some((name, _)) = schema.fields.get(&field_num) {
                        obj.insert(name.clone(), json!(value));
                    }
                }
                WIRE_LENGTH_DELIMITED => {
                    let (len, new_pos) = match read_varint(data, pos) {
                        Some(v) => v,
                        None => break,
                    };
                    pos = new_pos;
                    let len = len as usize;
                    if pos + len > data.len() {
                        break;
                    }
                    let field_data = &data[pos..pos + len];
                    pos += len;

                    if let Some((name, field_type)) = schema.fields.get(&field_num) {
                        match field_type {
                            FieldType::InlineMessage(msg_type) => {
                                // Go's JSON tag flattens this nested message
                                // into the parent. Decode the embedded message,
                                // then merge its fields into `obj` directly so
                                // the surrounding JSON struct sees them at the
                                // top level (e.g. `Volume.volumeSource → emptyDir`).
                                if let Some(Value::Object(inner)) =
                                    self.decode_message(msg_type, field_data)
                                {
                                    for (k, v) in inner {
                                        obj.insert(k, v);
                                    }
                                }
                            }
                            FieldType::Repeated(_) => {
                                let json_val = self.decode_field_value(field_type, field_data);
                                repeated_fields
                                    .entry(name.clone())
                                    .or_default()
                                    .push(json_val);
                            }
                            FieldType::StringMap => {
                                // Maps are encoded as repeated MapEntry messages.
                                // Each MapEntry has field 1 (key) and field 2 (value).
                                let (key, val) = decode_map_entry(field_data);
                                let map = obj
                                    .entry(name.clone())
                                    .or_insert_with(|| Value::Object(Map::new()));
                                if let Value::Object(ref mut m) = map {
                                    m.insert(key, Value::String(val));
                                }
                            }
                            FieldType::MessageMap(ref msg_type) => {
                                // map<string, Message> — decode MapEntry with message value
                                let (key, val) =
                                    self.decode_message_map_entry(msg_type, field_data);
                                let map = obj
                                    .entry(name.clone())
                                    .or_insert_with(|| Value::Object(Map::new()));
                                if let Value::Object(ref mut m) = map {
                                    m.insert(key, val);
                                }
                            }
                            _ => {
                                let json_val = self.decode_field_value(field_type, field_data);
                                obj.insert(name.clone(), json_val);
                            }
                        }
                    }
                }
                WIRE_32BIT => {
                    if pos + 4 > data.len() {
                        break;
                    }
                    let value = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    if let Some((name, _)) = schema.fields.get(&field_num) {
                        obj.insert(name.clone(), json!(value));
                    }
                }
                _ => break,
            }
        }

        // Insert accumulated repeated fields
        for (name, values) in repeated_fields {
            obj.insert(name, Value::Array(values));
        }

        Value::Object(obj)
    }

    /// Decode a single field value based on its type
    fn decode_field_value(&self, field_type: &FieldType, data: &[u8]) -> Value {
        match field_type {
            FieldType::String => Value::String(String::from_utf8_lossy(data).to_string()),
            FieldType::Bytes => {
                use base64::Engine;
                Value::String(base64::engine::general_purpose::STANDARD.encode(data))
            }
            FieldType::Message(msg_type) | FieldType::InlineMessage(msg_type) => {
                // InlineMessage merging is handled at the caller (decode_with_schema).
                // Reaching here means it's nested under a Repeated wrapper, which is
                // not a documented K8s pattern — decode as a normal message instead.
                if msg_type == "Time" {
                    // K8s Time is a Timestamp proto — decode to RFC3339 string
                    return decode_timestamp(data);
                }
                match self.decode_message(msg_type, data) {
                    Some(v) => v,
                    None => {
                        // Unknown message type — try to decode generically
                        debug!("Unknown proto message type: {}", msg_type);
                        Value::Object(Map::new())
                    }
                }
            }
            FieldType::Int => {
                // Length-delimited int is unusual — treat as a submessage or packed repeated
                if let Some((val, _)) = read_varint(data, 0) {
                    json!(val as i64)
                } else {
                    Value::Null
                }
            }
            FieldType::Bool => {
                if data.first() == Some(&1) {
                    Value::Bool(true)
                } else {
                    Value::Bool(false)
                }
            }
            FieldType::Repeated(inner) => {
                // Single element of a repeated field (not packed)
                self.decode_field_value(inner, data)
            }
            FieldType::StringMap => {
                // Should be handled at the caller level as MapEntry
                Value::Object(Map::new())
            }
            FieldType::MessageMap(_) => {
                // Should be handled at the caller level as MessageMapEntry
                Value::Object(Map::new())
            }
            FieldType::IntOrString => {
                // K8s IntOrString: in protobuf, encoded as a message with
                // field 1 (type: int32), field 2 (intVal: int32), field 3 (strVal: string)
                decode_int_or_string(data)
            }
            FieldType::JsonRaw => {
                // K8s JSON type: a message with field 1 = bytes containing raw JSON.
                // Decode the message to extract the raw bytes, then parse as JSON.
                let mut pos = 0;
                while pos < data.len() {
                    let (tag, new_pos) = match read_varint(data, pos) {
                        Some(v) => v,
                        None => break,
                    };
                    pos = new_pos;
                    let field_num = (tag >> 3) as u32;
                    let wire_type = (tag & 0x07) as u8;
                    if wire_type == WIRE_LENGTH_DELIMITED && field_num == 1 {
                        // field 1: raw bytes containing JSON
                        let (len, new_pos) = match read_varint(data, pos) {
                            Some(v) => v,
                            None => break,
                        };
                        pos = new_pos;
                        let len = len as usize;
                        if pos + len <= data.len() {
                            let raw = &data[pos..pos + len];
                            if let Ok(v) = serde_json::from_slice(raw) {
                                return v;
                            }
                            // If not valid JSON, return as string
                            return Value::String(String::from_utf8_lossy(raw).to_string());
                        }
                    } else {
                        // Skip unknown fields
                        match wire_type {
                            WIRE_VARINT => {
                                let _ = read_varint(data, pos).map(|(_, p)| pos = p);
                            }
                            WIRE_64BIT => {
                                pos += 8;
                            }
                            WIRE_LENGTH_DELIMITED => {
                                if let Some((len, new_pos)) = read_varint(data, pos) {
                                    pos = new_pos + len as usize;
                                } else {
                                    break;
                                }
                            }
                            WIRE_32BIT => {
                                pos += 4;
                            }
                            _ => break,
                        }
                    }
                }
                Value::Null
            }
        }
    }

    /// Decode a protobuf map entry where value is a message type
    fn decode_message_map_entry(&self, msg_type: &str, data: &[u8]) -> (String, Value) {
        let mut key = String::new();
        let mut val = Value::Null;
        let mut pos = 0;
        while pos < data.len() {
            let (tag, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            let field_num = (tag >> 3) as u32;
            let wire_type = (tag & 0x07) as u8;
            if wire_type == WIRE_LENGTH_DELIMITED {
                let (len, new_pos) = match read_varint(data, pos) {
                    Some(v) => v,
                    None => break,
                };
                pos = new_pos;
                let len = len as usize;
                if pos + len > data.len() {
                    break;
                }
                match field_num {
                    1 => {
                        key = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                    }
                    2 => {
                        val = self
                            .decode_message(msg_type, &data[pos..pos + len])
                            .unwrap_or(Value::Null);
                    }
                    _ => {}
                }
                pos += len;
            } else if wire_type == WIRE_VARINT {
                let (_, new_pos) = match read_varint(data, pos) {
                    Some(v) => v,
                    None => break,
                };
                pos = new_pos;
            } else {
                break;
            }
        }
        (key, val)
    }

    /// Decode a full K8s protobuf-encoded resource (with k8s\0 prefix) to JSON.
    /// Returns (apiVersion, kind, json_bytes) on success.
    pub fn decode_k8s_resource(&self, data: &[u8]) -> Option<Vec<u8>> {
        if data.len() < 5 || &data[0..4] != b"k8s\0" {
            return None;
        }
        let envelope = &data[4..];

        // Parse the Unknown envelope to get TypeMeta and raw bytes
        let mut api_version = String::new();
        let mut kind = String::new();
        let mut raw_bytes: Option<&[u8]> = None;

        let mut pos = 0;
        while pos < envelope.len() {
            let (tag, new_pos) = read_varint(envelope, pos)?;
            pos = new_pos;
            let field_num = (tag >> 3) as u32;
            let wire_type = (tag & 0x07) as u8;

            if wire_type == WIRE_LENGTH_DELIMITED {
                let (len, new_pos) = read_varint(envelope, pos)?;
                pos = new_pos;
                let len = len as usize;
                if pos + len > envelope.len() {
                    break;
                }
                let field_data = &envelope[pos..pos + len];
                pos += len;

                match field_num {
                    1 => {
                        // TypeMeta
                        let mut tp = 0;
                        while tp < field_data.len() {
                            let (t, ntp) = read_varint(field_data, tp)?;
                            tp = ntp;
                            let fnum = (t >> 3) as u32;
                            let wt = (t & 0x07) as u8;
                            if wt == WIRE_LENGTH_DELIMITED {
                                let (slen, ntp) = read_varint(field_data, tp)?;
                                tp = ntp;
                                let slen = slen as usize;
                                if tp + slen <= field_data.len() {
                                    if let Ok(s) = std::str::from_utf8(&field_data[tp..tp + slen]) {
                                        match fnum {
                                            1 => api_version = s.to_string(),
                                            2 => kind = s.to_string(),
                                            _ => {}
                                        }
                                    }
                                }
                                tp += slen;
                            } else if wt == WIRE_VARINT {
                                let (_, ntp) = read_varint(field_data, tp)?;
                                tp = ntp;
                            } else {
                                break;
                            }
                        }
                    }
                    2 => {
                        // raw bytes — the serialized resource
                        raw_bytes = Some(field_data);
                    }
                    // field 3 = contentEncoding (string, skip)
                    // field 4 = contentType (string, skip)
                    _ => {}
                }
            } else if wire_type == WIRE_VARINT {
                let (_, new_pos) = read_varint(envelope, pos)?;
                pos = new_pos;
            } else if wire_type == WIRE_64BIT {
                pos += 8;
            } else if wire_type == WIRE_32BIT {
                pos += 4;
            } else {
                break;
            }
        }

        if api_version.is_empty() || kind.is_empty() {
            return None;
        }

        let raw = raw_bytes?;

        // Check if raw is already JSON
        if !raw.is_empty() && (raw[0] == b'{' || raw[0] == b'[') {
            return Some(raw.to_vec());
        }

        // Look up the schema for this kind
        if let Some(json_obj) = self.decode_message(&kind, raw) {
            // Add apiVersion and kind to the JSON
            let result = match json_obj {
                Value::Object(m) => {
                    // Insert apiVersion/kind at the top (they're part of TypeMeta, not the raw message)
                    let mut ordered = Map::new();
                    ordered.insert("apiVersion".into(), Value::String(api_version));
                    ordered.insert("kind".into(), Value::String(kind));
                    // Merge the decoded fields
                    for (k, v) in m {
                        ordered.insert(k, v);
                    }
                    Value::Object(ordered)
                }
                other => other,
            };

            serde_json::to_vec(&result).ok()
        } else {
            warn!(
                "No schema found for kind '{}', cannot decode protobuf",
                kind
            );
            None
        }
    }
}

// ========== Helper functions ==========

/// Read a varint from data starting at pos. Returns (value, new_pos).
fn read_varint(data: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0;
    loop {
        if pos >= data.len() {
            return None;
        }
        let b = data[pos] as u64;
        pos += 1;
        value |= (b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some((value, pos));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

/// Decode a protobuf map entry (field 1 = key, field 2 = value, both strings)
fn decode_map_entry(data: &[u8]) -> (String, String) {
    let mut key = String::new();
    let mut val = String::new();
    let mut pos = 0;
    while pos < data.len() {
        let (tag, new_pos) = match read_varint(data, pos) {
            Some(v) => v,
            None => break,
        };
        pos = new_pos;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;
        if wire_type == WIRE_LENGTH_DELIMITED {
            let (len, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            let len = len as usize;
            if pos + len > data.len() {
                break;
            }
            if let Ok(s) = std::str::from_utf8(&data[pos..pos + len]) {
                match field_num {
                    1 => key = s.to_string(),
                    2 => val = s.to_string(),
                    _ => {}
                }
            }
            pos += len;
        } else if wire_type == WIRE_VARINT {
            let (_, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
        } else {
            break;
        }
    }
    (key, val)
}

/// Decode a K8s Timestamp protobuf to RFC3339 string
fn decode_timestamp(data: &[u8]) -> Value {
    let mut seconds: i64 = 0;
    let mut nanos: i32 = 0;
    let mut pos = 0;
    while pos < data.len() {
        let (tag, new_pos) = match read_varint(data, pos) {
            Some(v) => v,
            None => break,
        };
        pos = new_pos;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;
        if wire_type == WIRE_VARINT {
            let (val, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            match field_num {
                1 => seconds = val as i64,
                2 => nanos = val as i32,
                _ => {}
            }
        } else {
            break;
        }
    }
    if seconds == 0 && nanos == 0 {
        return Value::Null;
    }
    // Convert to RFC3339
    let dt = chrono::DateTime::from_timestamp(seconds, nanos as u32);
    match dt {
        Some(dt) => Value::String(dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        None => Value::String(format!("{}s", seconds)),
    }
}

/// Decode K8s IntOrString protobuf message
/// Proto: message IntOrString { int64 type = 1; int32 intVal = 2; string strVal = 3; }
fn decode_int_or_string(data: &[u8]) -> Value {
    let mut kind: i64 = 0; // 0 = int, 1 = string
    let mut int_val: i64 = 0;
    let mut str_val = String::new();
    let mut pos = 0;
    while pos < data.len() {
        let (tag, new_pos) = match read_varint(data, pos) {
            Some(v) => v,
            None => break,
        };
        pos = new_pos;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;
        if wire_type == WIRE_VARINT {
            let (val, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            match field_num {
                1 => kind = val as i64,
                2 => int_val = val as i64,
                _ => {}
            }
        } else if wire_type == WIRE_LENGTH_DELIMITED {
            let (len, new_pos) = match read_varint(data, pos) {
                Some(v) => v,
                None => break,
            };
            pos = new_pos;
            let len = len as usize;
            if pos + len > data.len() {
                break;
            }
            if field_num == 3 {
                str_val = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
            }
            pos += len;
        } else {
            break;
        }
    }
    if kind == 1 {
        Value::String(str_val)
    } else {
        json!(int_val)
    }
}

// Placeholder schemas for types we handle but don't need full detail
// These are empty — the decoder treats unknown fields as ignored
impl ProtoRegistry {
    // Additional placeholder types that we reference but don't need full schemas for

    /// Register apps/v1 schemas not covered by the dedicated kind helpers above.
    ///
    /// Existing apps/v1 kinds (`Deployment`, `ReplicaSet`, `DaemonSet`,
    /// `StatefulSet`) and their nested messages are registered inline in
    /// [`ProtoRegistry::new`]. This helper rounds out the group with the
    /// remaining kind, `ControllerRevision`, so that conformant clients
    /// (kubectl / controllers) can decode it from native protobuf.
    ///
    /// Upstream proto: k8s.io/api/apps/v1/generated.proto (release-1.35).
    fn register_apps_v1(schemas: &mut HashMap<String, MessageSchema>) {
        // ControllerRevision: an immutable snapshot used by DaemonSet and
        // StatefulSet for rollouts. The `data` field is a RawExtension, which
        // is encoded as a message with a single `raw` bytes field carrying
        // the serialized payload — modelled by `FieldType::JsonRaw`.
        schemas.insert(
            "ControllerRevision".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("data".into(), FieldType::JsonRaw)),
                    (3, ("revision".into(), FieldType::Int)),
                ]),
            },
        );
    }

    /// Register all discovery/v1 protobuf schemas.
    ///
    /// Field numbers come from
    /// k8s.io/api/discovery/v1/generated.proto (release-1.35). Covers the
    /// `EndpointSlice` top-level kind and its nested messages so that
    /// kube-proxy and every Service-aware conformance test that writes
    /// EndpointSlices over protobuf decodes correctly.
    fn register_discovery_v1(schemas: &mut HashMap<String, MessageSchema>) {
        // EndpointSlice — top-level kind. `addressType` is a string enum
        // ("IPv4" | "IPv6" | "FQDN") on the wire.
        schemas.insert(
            "EndpointSlice".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "endpoints".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("Endpoint".into()))),
                        ),
                    ),
                    (
                        3,
                        (
                            "ports".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "EndpointPort".into(),
                            ))),
                        ),
                    ),
                    (4, ("addressType".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "Endpoint".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "addresses".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "conditions".into(),
                            FieldType::Message("EndpointConditions".into()),
                        ),
                    ),
                    (3, ("hostname".into(), FieldType::String)),
                    (
                        4,
                        (
                            "targetRef".into(),
                            FieldType::Message("ObjectReference".into()),
                        ),
                    ),
                    (5, ("deprecatedTopology".into(), FieldType::StringMap)),
                    (6, ("nodeName".into(), FieldType::String)),
                    (7, ("zone".into(), FieldType::String)),
                    (
                        8,
                        ("hints".into(), FieldType::Message("EndpointHints".into())),
                    ),
                ]),
            },
        );

        schemas.insert(
            "EndpointConditions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("ready".into(), FieldType::Bool)),
                    (2, ("serving".into(), FieldType::Bool)),
                    (3, ("terminating".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "EndpointHints".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "forZones".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("ForZone".into()))),
                        ),
                    ),
                    (
                        2,
                        (
                            "forNodes".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("ForNode".into()))),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "EndpointPort".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("protocol".into(), FieldType::String)),
                    (3, ("port".into(), FieldType::Int)),
                    (4, ("appProtocol".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "ForNode".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("name".into(), FieldType::String))]),
            },
        );

        schemas.insert(
            "ForZone".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("name".into(), FieldType::String))]),
            },
        );
    }

    fn register_core_v1_cloud_volume_sources(schemas: &mut HashMap<String, MessageSchema>) {
        // SecretReference — namespaced secret pointer referenced by several
        // PersistentVolumeSource flavors (CSI, CephFS, Cinder, Flex, iSCSI,
        // RBD, ScaleIO). Not yet registered elsewhere.
        schemas.insert(
            "SecretReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("namespace".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "AWSElasticBlockStoreVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("volumeID".into(), FieldType::String)),
                    (2, ("fsType".into(), FieldType::String)),
                    (3, ("partition".into(), FieldType::Int)),
                    (4, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "AzureDiskVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("diskName".into(), FieldType::String)),
                    (2, ("diskURI".into(), FieldType::String)),
                    (3, ("cachingMode".into(), FieldType::String)),
                    (4, ("fsType".into(), FieldType::String)),
                    (5, ("readOnly".into(), FieldType::Bool)),
                    (6, ("kind".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "AzureFilePersistentVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("secretName".into(), FieldType::String)),
                    (2, ("shareName".into(), FieldType::String)),
                    (3, ("readOnly".into(), FieldType::Bool)),
                    (4, ("secretNamespace".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "AzureFileVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("secretName".into(), FieldType::String)),
                    (2, ("shareName".into(), FieldType::String)),
                    (3, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        // CSIPersistentVolumeSource — note: `capacity` Quantity field is not
        // expressed in this proto (it lives on PersistentVolumeSpec, not the
        // source), so no Quantity skip is required here.
        schemas.insert(
            "CSIPersistentVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("driver".into(), FieldType::String)),
                    (2, ("volumeHandle".into(), FieldType::String)),
                    (3, ("readOnly".into(), FieldType::Bool)),
                    (4, ("fsType".into(), FieldType::String)),
                    (5, ("volumeAttributes".into(), FieldType::StringMap)),
                    (
                        6,
                        (
                            "controllerPublishSecretRef".into(),
                            FieldType::Message("SecretReference".into()),
                        ),
                    ),
                    (
                        7,
                        (
                            "nodeStageSecretRef".into(),
                            FieldType::Message("SecretReference".into()),
                        ),
                    ),
                    (
                        8,
                        (
                            "nodePublishSecretRef".into(),
                            FieldType::Message("SecretReference".into()),
                        ),
                    ),
                    (
                        9,
                        (
                            "controllerExpandSecretRef".into(),
                            FieldType::Message("SecretReference".into()),
                        ),
                    ),
                    (
                        10,
                        (
                            "nodeExpandSecretRef".into(),
                            FieldType::Message("SecretReference".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "CSIVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("driver".into(), FieldType::String)),
                    (2, ("readOnly".into(), FieldType::Bool)),
                    (3, ("fsType".into(), FieldType::String)),
                    (4, ("volumeAttributes".into(), FieldType::StringMap)),
                    (
                        5,
                        (
                            "nodePublishSecretRef".into(),
                            FieldType::Message("LocalObjectReference".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "CephFSPersistentVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "monitors".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("path".into(), FieldType::String)),
                    (3, ("user".into(), FieldType::String)),
                    (4, ("secretFile".into(), FieldType::String)),
                    (
                        5,
                        (
                            "secretRef".into(),
                            FieldType::Message("SecretReference".into()),
                        ),
                    ),
                    (6, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "CephFSVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "monitors".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("path".into(), FieldType::String)),
                    (3, ("user".into(), FieldType::String)),
                    (4, ("secretFile".into(), FieldType::String)),
                    (
                        5,
                        (
                            "secretRef".into(),
                            FieldType::Message("LocalObjectReference".into()),
                        ),
                    ),
                    (6, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "CinderPersistentVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("volumeID".into(), FieldType::String)),
                    (2, ("fsType".into(), FieldType::String)),
                    (3, ("readOnly".into(), FieldType::Bool)),
                    (
                        4,
                        (
                            "secretRef".into(),
                            FieldType::Message("SecretReference".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "CinderVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("volumeID".into(), FieldType::String)),
                    (2, ("fsType".into(), FieldType::String)),
                    (3, ("readOnly".into(), FieldType::Bool)),
                    (
                        4,
                        (
                            "secretRef".into(),
                            FieldType::Message("LocalObjectReference".into()),
                        ),
                    ),
                ]),
            },
        );

        // EphemeralVolumeSource — wraps a PersistentVolumeClaimTemplate.
        // The PVC template schema is owned by other PRs; reference by name so
        // the field is preserved as an opaque message if the template isn't
        // registered yet, and decoded fully once it is.
        schemas.insert(
            "EphemeralVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "volumeClaimTemplate".into(),
                        FieldType::Message("PersistentVolumeClaimTemplate".into()),
                    ),
                )]),
            },
        );

        schemas.insert(
            "FCVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "targetWWNs".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("lun".into(), FieldType::Int)),
                    (3, ("fsType".into(), FieldType::String)),
                    (4, ("readOnly".into(), FieldType::Bool)),
                    (
                        5,
                        (
                            "wwids".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "FlexPersistentVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("driver".into(), FieldType::String)),
                    (2, ("fsType".into(), FieldType::String)),
                    (
                        3,
                        (
                            "secretRef".into(),
                            FieldType::Message("SecretReference".into()),
                        ),
                    ),
                    (4, ("readOnly".into(), FieldType::Bool)),
                    (5, ("options".into(), FieldType::StringMap)),
                ]),
            },
        );

        schemas.insert(
            "FlexVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("driver".into(), FieldType::String)),
                    (2, ("fsType".into(), FieldType::String)),
                    (
                        3,
                        (
                            "secretRef".into(),
                            FieldType::Message("LocalObjectReference".into()),
                        ),
                    ),
                    (4, ("readOnly".into(), FieldType::Bool)),
                    (5, ("options".into(), FieldType::StringMap)),
                ]),
            },
        );

        schemas.insert(
            "FlockerVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("datasetName".into(), FieldType::String)),
                    (2, ("datasetUUID".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "GCEPersistentDiskVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("pdName".into(), FieldType::String)),
                    (2, ("fsType".into(), FieldType::String)),
                    (3, ("partition".into(), FieldType::Int)),
                    (4, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "GitRepoVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("repository".into(), FieldType::String)),
                    (2, ("revision".into(), FieldType::String)),
                    (3, ("directory".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "GlusterfsPersistentVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("endpoints".into(), FieldType::String)),
                    (2, ("path".into(), FieldType::String)),
                    (3, ("readOnly".into(), FieldType::Bool)),
                    (4, ("endpointsNamespace".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "GlusterfsVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("endpoints".into(), FieldType::String)),
                    (2, ("path".into(), FieldType::String)),
                    (3, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "ISCSIPersistentVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("targetPortal".into(), FieldType::String)),
                    (2, ("iqn".into(), FieldType::String)),
                    (3, ("lun".into(), FieldType::Int)),
                    (4, ("iscsiInterface".into(), FieldType::String)),
                    (5, ("fsType".into(), FieldType::String)),
                    (6, ("readOnly".into(), FieldType::Bool)),
                    (
                        7,
                        (
                            "portals".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (8, ("chapAuthDiscovery".into(), FieldType::Bool)),
                    (
                        10,
                        (
                            "secretRef".into(),
                            FieldType::Message("SecretReference".into()),
                        ),
                    ),
                    (11, ("chapAuthSession".into(), FieldType::Bool)),
                    (12, ("initiatorName".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "ISCSIVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("targetPortal".into(), FieldType::String)),
                    (2, ("iqn".into(), FieldType::String)),
                    (3, ("lun".into(), FieldType::Int)),
                    (4, ("iscsiInterface".into(), FieldType::String)),
                    (5, ("fsType".into(), FieldType::String)),
                    (6, ("readOnly".into(), FieldType::Bool)),
                    (
                        7,
                        (
                            "portals".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (8, ("chapAuthDiscovery".into(), FieldType::Bool)),
                    (
                        10,
                        (
                            "secretRef".into(),
                            FieldType::Message("LocalObjectReference".into()),
                        ),
                    ),
                    (11, ("chapAuthSession".into(), FieldType::Bool)),
                    (12, ("initiatorName".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "ImageVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("reference".into(), FieldType::String)),
                    (2, ("pullPolicy".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "NFSVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("server".into(), FieldType::String)),
                    (2, ("path".into(), FieldType::String)),
                    (3, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "PhotonPersistentDiskVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("pdID".into(), FieldType::String)),
                    (2, ("fsType".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "PortworxVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("volumeID".into(), FieldType::String)),
                    (2, ("fsType".into(), FieldType::String)),
                    (3, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "QuobyteVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("registry".into(), FieldType::String)),
                    (2, ("volume".into(), FieldType::String)),
                    (3, ("readOnly".into(), FieldType::Bool)),
                    (4, ("user".into(), FieldType::String)),
                    (5, ("group".into(), FieldType::String)),
                    (6, ("tenant".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "RBDPersistentVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "monitors".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("image".into(), FieldType::String)),
                    (3, ("fsType".into(), FieldType::String)),
                    (4, ("pool".into(), FieldType::String)),
                    (5, ("user".into(), FieldType::String)),
                    (6, ("keyring".into(), FieldType::String)),
                    (
                        7,
                        (
                            "secretRef".into(),
                            FieldType::Message("SecretReference".into()),
                        ),
                    ),
                    (8, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "RBDVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "monitors".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("image".into(), FieldType::String)),
                    (3, ("fsType".into(), FieldType::String)),
                    (4, ("pool".into(), FieldType::String)),
                    (5, ("user".into(), FieldType::String)),
                    (6, ("keyring".into(), FieldType::String)),
                    (
                        7,
                        (
                            "secretRef".into(),
                            FieldType::Message("LocalObjectReference".into()),
                        ),
                    ),
                    (8, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "ScaleIOPersistentVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("gateway".into(), FieldType::String)),
                    (2, ("system".into(), FieldType::String)),
                    (
                        3,
                        (
                            "secretRef".into(),
                            FieldType::Message("SecretReference".into()),
                        ),
                    ),
                    (4, ("sslEnabled".into(), FieldType::Bool)),
                    (5, ("protectionDomain".into(), FieldType::String)),
                    (6, ("storagePool".into(), FieldType::String)),
                    (7, ("storageMode".into(), FieldType::String)),
                    (8, ("volumeName".into(), FieldType::String)),
                    (9, ("fsType".into(), FieldType::String)),
                    (10, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "ScaleIOVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("gateway".into(), FieldType::String)),
                    (2, ("system".into(), FieldType::String)),
                    (
                        3,
                        (
                            "secretRef".into(),
                            FieldType::Message("LocalObjectReference".into()),
                        ),
                    ),
                    (4, ("sslEnabled".into(), FieldType::Bool)),
                    (5, ("protectionDomain".into(), FieldType::String)),
                    (6, ("storagePool".into(), FieldType::String)),
                    (7, ("storageMode".into(), FieldType::String)),
                    (8, ("volumeName".into(), FieldType::String)),
                    (9, ("fsType".into(), FieldType::String)),
                    (10, ("readOnly".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "StorageOSPersistentVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("volumeName".into(), FieldType::String)),
                    (2, ("volumeNamespace".into(), FieldType::String)),
                    (3, ("fsType".into(), FieldType::String)),
                    (4, ("readOnly".into(), FieldType::Bool)),
                    (
                        5,
                        (
                            "secretRef".into(),
                            FieldType::Message("ObjectReference".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "StorageOSVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("volumeName".into(), FieldType::String)),
                    (2, ("volumeNamespace".into(), FieldType::String)),
                    (3, ("fsType".into(), FieldType::String)),
                    (4, ("readOnly".into(), FieldType::Bool)),
                    (
                        5,
                        (
                            "secretRef".into(),
                            FieldType::Message("LocalObjectReference".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "VsphereVirtualDiskVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("volumePath".into(), FieldType::String)),
                    (2, ("fsType".into(), FieldType::String)),
                    (3, ("storagePolicyName".into(), FieldType::String)),
                    (4, ("storagePolicyID".into(), FieldType::String)),
                ]),
            },
        );
    }

    fn register_apiregistration_v1(schemas: &mut HashMap<String, MessageSchema>) {
        schemas.insert(
            "APIService".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("APIServiceSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("APIServiceStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "APIServiceSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "service".into(),
                            FieldType::Message("ServiceReference".into()),
                        ),
                    ),
                    (2, ("group".into(), FieldType::String)),
                    (3, ("version".into(), FieldType::String)),
                    (4, ("insecureSkipTLSVerify".into(), FieldType::Bool)),
                    (5, ("caBundle".into(), FieldType::Bytes)),
                    (7, ("groupPriorityMinimum".into(), FieldType::Int)),
                    (8, ("versionPriority".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "APIServiceStatus".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "conditions".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "APIServiceCondition".into(),
                        ))),
                    ),
                )]),
            },
        );
        schemas.insert(
            "APIServiceCondition".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
                    (
                        3,
                        (
                            "lastTransitionTime".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("message".into(), FieldType::String)),
                ]),
            },
        );
        // ServiceReference (apiregistration/v1) — separate proto from
        // admissionregistration/v1.ServiceReference (which has `path` at
        // field 3 instead of `port`). No conflict today; if admissionregistration
        // registers its own ServiceReference, prefix one with the group.
        schemas.insert(
            "ServiceReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("namespace".into(), FieldType::String)),
                    (2, ("name".into(), FieldType::String)),
                    (3, ("port".into(), FieldType::Int)),
                ]),
            },
        );
    }

    fn register_storage_v1(schemas: &mut HashMap<String, MessageSchema>) {
        // ----- Kinds -----

        schemas.insert(
            "StorageClass".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("provisioner".into(), FieldType::String)),
                    (3, ("parameters".into(), FieldType::StringMap)),
                    (4, ("reclaimPolicy".into(), FieldType::String)),
                    (
                        5,
                        (
                            "mountOptions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (6, ("allowVolumeExpansion".into(), FieldType::Bool)),
                    (7, ("volumeBindingMode".into(), FieldType::String)),
                    (
                        8,
                        (
                            "allowedTopologies".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "TopologySelectorTerm".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "VolumeAttachment".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("VolumeAttachmentSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("VolumeAttachmentStatus".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "VolumeAttributesClass".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("driverName".into(), FieldType::String)),
                    (3, ("parameters".into(), FieldType::StringMap)),
                ]),
            },
        );

        schemas.insert(
            "CSIDriver".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("CSIDriverSpec".into())),
                    ),
                ]),
            },
        );

        schemas.insert(
            "CSINode".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("CSINodeSpec".into()))),
                ]),
            },
        );

        schemas.insert(
            "CSIStorageCapacity".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "nodeTopology".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (3, ("storageClassName".into(), FieldType::String)),
                    // field 4 = capacity (Quantity) — skipped; see fn doc
                    // field 5 = maximumVolumeSize (Quantity) — skipped; see fn doc
                ]),
            },
        );

        // ----- Nested messages -----

        schemas.insert(
            "CSIDriverSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("attachRequired".into(), FieldType::Bool)),
                    (2, ("podInfoOnMount".into(), FieldType::Bool)),
                    (
                        3,
                        (
                            "volumeLifecycleModes".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (4, ("storageCapacity".into(), FieldType::Bool)),
                    (5, ("fsGroupPolicy".into(), FieldType::String)),
                    (
                        6,
                        (
                            "tokenRequests".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "TokenRequest".into(),
                            ))),
                        ),
                    ),
                    (7, ("requiresRepublish".into(), FieldType::Bool)),
                    (8, ("seLinuxMount".into(), FieldType::Bool)),
                    (
                        9,
                        ("nodeAllocatableUpdatePeriodSeconds".into(), FieldType::Int),
                    ),
                    (10, ("serviceAccountTokenInSecrets".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "CSINodeSpec".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "drivers".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("CSINodeDriver".into()))),
                    ),
                )]),
            },
        );

        schemas.insert(
            "CSINodeDriver".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("nodeID".into(), FieldType::String)),
                    (
                        3,
                        (
                            "topologyKeys".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        4,
                        (
                            "allocatable".into(),
                            FieldType::Message("VolumeNodeResources".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "VolumeNodeResources".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("count".into(), FieldType::Int))]),
            },
        );

        schemas.insert(
            "TokenRequest".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("audience".into(), FieldType::String)),
                    (2, ("expirationSeconds".into(), FieldType::Int)),
                ]),
            },
        );

        schemas.insert(
            "VolumeAttachmentSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("attacher".into(), FieldType::String)),
                    (
                        2,
                        (
                            "source".into(),
                            FieldType::Message("VolumeAttachmentSource".into()),
                        ),
                    ),
                    (3, ("nodeName".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "VolumeAttachmentSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("persistentVolumeName".into(), FieldType::String)),
                    (
                        2,
                        (
                            "inlineVolumeSpec".into(),
                            FieldType::Message("PersistentVolumeSpec".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "VolumeAttachmentStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("attached".into(), FieldType::Bool)),
                    (2, ("attachmentMetadata".into(), FieldType::StringMap)),
                    (
                        3,
                        (
                            "attachError".into(),
                            FieldType::Message("VolumeError".into()),
                        ),
                    ),
                    (
                        4,
                        (
                            "detachError".into(),
                            FieldType::Message("VolumeError".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "VolumeError".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("time".into(), FieldType::Message("Time".into()))),
                    (2, ("message".into(), FieldType::String)),
                    (3, ("errorCode".into(), FieldType::Int)),
                ]),
            },
        );
    }

    /// Register coordination/v1 message schemas.
    ///
    /// Field numbers from
    /// k8s.io/api/coordination/v1/generated.proto (release-1.35).
    /// Covers the `Lease` top-level kind and its nested `LeaseSpec` —
    /// every controller-manager + scheduler election cycle posts Lease
    /// objects over protobuf, so without these schemas the api-server
    /// rejects the write with `No schema found for kind 'Lease'`.
    fn register_coordination_v1(schemas: &mut HashMap<String, MessageSchema>) {
        schemas.insert(
            "Lease".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("LeaseSpec".into()))),
                ]),
            },
        );
        schemas.insert(
            "LeaseSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("holderIdentity".into(), FieldType::String)),
                    (2, ("leaseDurationSeconds".into(), FieldType::Int)),
                    (3, ("acquireTime".into(), FieldType::Message("Time".into()))),
                    (4, ("renewTime".into(), FieldType::Message("Time".into()))),
                    (5, ("leaseTransitions".into(), FieldType::Int)),
                    (6, ("strategy".into(), FieldType::String)),
                    (7, ("preferredHolder".into(), FieldType::String)),
                ]),
            },
        );
    }

    /// Register protobuf schemas for the `policy/v1` API group.
    ///
    /// Covers `Eviction`, `PodDisruptionBudget`, `PodDisruptionBudgetSpec`, and
    /// `PodDisruptionBudgetStatus`. Field numbers are taken from
    /// `k8s.io/api/policy/v1/generated.proto` (Kubernetes release-1.35).
    ///
    /// `PodDisruptionBudgetSpec.minAvailable` and `maxUnavailable` are
    /// `IntOrString`. `PodDisruptionBudgetStatus.disruptedPods` is a
    /// `map<string, Time>` (decoded via `MessageMap`).
    fn register_policy_v1(schemas: &mut HashMap<String, MessageSchema>) {
        schemas.insert(
            "Eviction".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "deleteOptions".into(),
                            FieldType::Message("DeleteOptions".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PodDisruptionBudget".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "spec".into(),
                            FieldType::Message("PodDisruptionBudgetSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("PodDisruptionBudgetStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PodDisruptionBudgetSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("minAvailable".into(), FieldType::IntOrString)),
                    (
                        2,
                        (
                            "selector".into(),
                            FieldType::Message("LabelSelector".into()),
                        ),
                    ),
                    (3, ("maxUnavailable".into(), FieldType::IntOrString)),
                    (4, ("unhealthyPodEvictionPolicy".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "PodDisruptionBudgetStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("observedGeneration".into(), FieldType::Int)),
                    (
                        2,
                        ("disruptedPods".into(), FieldType::MessageMap("Time".into())),
                    ),
                    (3, ("disruptionsAllowed".into(), FieldType::Int)),
                    (4, ("currentHealthy".into(), FieldType::Int)),
                    (5, ("desiredHealthy".into(), FieldType::Int)),
                    (6, ("expectedPods".into(), FieldType::Int)),
                    (
                        7,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("Condition".into()))),
                        ),
                    ),
                ]),
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint() {
        assert_eq!(read_varint(&[0x08], 0), Some((8, 1)));
        assert_eq!(read_varint(&[0x96, 0x01], 0), Some((150, 2)));
        assert_eq!(read_varint(&[0xac, 0x02], 0), Some((300, 2)));
    }

    #[test]
    fn test_decode_simple_message() {
        let registry = ProtoRegistry::new();
        // A simple LabelSelector with matchLabels = {"app": "nginx"}
        // Encoded as: field 1 (matchLabels) = MapEntry { key="app", value="nginx" }
        // MapEntry: field 1 (key) = "app", field 2 (value) = "nginx"
        // field 1 tag = 0x0a (field 1, wire type 2)
        let map_entry = {
            let mut buf = Vec::new();
            // key field: tag=0x0a, len=3, "app"
            buf.extend_from_slice(&[0x0a, 0x03]);
            buf.extend_from_slice(b"app");
            // value field: tag=0x12, len=5, "nginx"
            buf.extend_from_slice(&[0x12, 0x05]);
            buf.extend_from_slice(b"nginx");
            buf
        };

        let mut label_selector = Vec::new();
        // matchLabels field: tag=0x0a (field 1, wire 2), length, then map_entry
        label_selector.push(0x0a);
        label_selector.push(map_entry.len() as u8);
        label_selector.extend_from_slice(&map_entry);

        let result = registry.decode_message("LabelSelector", &label_selector);
        assert!(result.is_some());
        let val = result.unwrap();
        assert_eq!(
            val.pointer("/matchLabels/app"),
            Some(&Value::String("nginx".into()))
        );
    }

    #[test]
    fn test_decode_deployment_spec_with_template() {
        let registry = ProtoRegistry::new();

        // Build a minimal DeploymentSpec protobuf:
        // field 1 (replicas): varint 1
        // field 3 (template): PodTemplateSpec with a container
        let mut spec = Vec::new();

        // replicas = 1 (field 1, wire type 0 = varint)
        spec.push(0x08); // field 1, varint
        spec.push(0x01); // value = 1

        // Build a minimal PodTemplateSpec
        let mut template = Vec::new();
        // PodTemplateSpec.spec (field 2) = PodSpec
        let mut pod_spec = Vec::new();
        // PodSpec.containers (field 2) = repeated Container
        let mut container = Vec::new();
        // Container.name (field 1) = "test"
        container.push(0x0a); // field 1, length-delimited
        container.push(0x04); // length = 4
        container.extend_from_slice(b"test");
        // Container.image (field 2) = "nginx"
        container.push(0x12); // field 2, length-delimited
        container.push(0x05); // length = 5
        container.extend_from_slice(b"nginx");

        // PodSpec field 2 (containers)
        pod_spec.push(0x12); // field 2, length-delimited
        pod_spec.push(container.len() as u8);
        pod_spec.extend_from_slice(&container);

        // PodTemplateSpec field 2 (spec)
        template.push(0x12); // field 2, length-delimited
        template.push(pod_spec.len() as u8);
        template.extend_from_slice(&pod_spec);

        // DeploymentSpec field 3 (template)
        spec.push(0x1a); // field 3, length-delimited
        spec.push(template.len() as u8);
        spec.extend_from_slice(&template);

        let result = registry.decode_message("DeploymentSpec", &spec);
        assert!(result.is_some());
        let val = result.unwrap();

        // Verify replicas
        assert_eq!(val.get("replicas"), Some(&json!(1)));

        // Verify template exists and has containers
        let tmpl = val.get("template").expect("template should exist");
        let spec_inner = tmpl.get("spec").expect("template.spec should exist");
        let containers = spec_inner
            .get("containers")
            .expect("containers should exist");
        assert!(containers.is_array());
        let first = &containers.as_array().unwrap()[0];
        assert_eq!(first.get("name"), Some(&Value::String("test".into())));
        assert_eq!(first.get("image"), Some(&Value::String("nginx".into())));
    }

    #[test]
    fn test_apimachinery_meta_v1_schemas_registered() {
        // Every shared `apimachinery/pkg/apis/meta/v1` type listed in
        // docs/conformance/protobuf-schema-coverage.md must be in the
        // registry, plus the StatusCause / StatusDetails leaves that
        // Status transitively requires.
        let registry = ProtoRegistry::new();
        for kind in [
            "Condition",
            "FieldsV1",
            "ListMeta",
            "MicroTime",
            "Patch",
            "Status",
            "StatusCause",
            "StatusDetails",
            "TypeMeta",
        ] {
            assert!(
                registry.schemas.contains_key(kind),
                "missing apimachinery/meta/v1 schema: {kind}",
            );
        }
    }

    #[test]
    fn test_decode_status_with_details_and_causes() {
        let registry = ProtoRegistry::new();

        // Build StatusCause { reason = "BadValue", field = "spec.replicas" }
        let mut cause = Vec::new();
        // field 1 (reason) length-delimited
        cause.push(0x0a);
        cause.push(8);
        cause.extend_from_slice(b"BadValue");
        // field 3 (field) length-delimited
        cause.push(0x1a);
        cause.push(13);
        cause.extend_from_slice(b"spec.replicas");

        // Build StatusDetails { name="x", causes=[cause] }
        // field 1 (name) length-delimited: tag 0x0a, len 1, 'x'
        // field 4 (causes) length-delimited: tag 0x22, len, cause bytes
        let mut details = vec![0x0a, 1, b'x', 0x22, cause.len() as u8];
        details.extend_from_slice(&cause);

        // Build Status { status="Failure", code=422, details=details }
        let mut status = Vec::new();
        // field 2 (status) length-delimited
        status.push(0x12);
        status.push(7);
        status.extend_from_slice(b"Failure");
        // field 5 (details) length-delimited
        status.push(0x2a);
        status.push(details.len() as u8);
        status.extend_from_slice(&details);
        // field 6 (code) varint = 422 -> two bytes: 0xa6 0x03
        status.push(0x30);
        status.push(0xa6);
        status.push(0x03);

        let val = registry
            .decode_message("Status", &status)
            .expect("Status should decode");
        assert_eq!(val.get("status"), Some(&Value::String("Failure".into())));
        assert_eq!(val.get("code"), Some(&json!(422)));
        let d = val.get("details").expect("details should decode");
        assert_eq!(d.get("name"), Some(&Value::String("x".into())));
        let causes = d.get("causes").expect("causes should be present");
        assert!(causes.is_array());
        let first = &causes.as_array().unwrap()[0];
        assert_eq!(first.get("reason"), Some(&Value::String("BadValue".into())));
        assert_eq!(
            first.get("field"),
            Some(&Value::String("spec.replicas".into())),
        );
    }
}
