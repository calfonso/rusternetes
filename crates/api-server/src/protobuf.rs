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
    /// Scalar 64-bit floating-point field (proto `double`). Wire type 1
    /// (fixed64). Used by `JSONSchemaProps.maximum`/`minimum`/
    /// `multipleOf` and similar numeric-tolerance fields.
    Double,
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
    /// map<string, bytes> — encoded as repeated MapEntry messages where
    /// the value side is a `bytes` field. Decoded to a JSON object whose
    /// values are base64-encoded strings, matching what typed K8s clients
    /// expect for `Secret.data` and `ConfigMap.binaryData`.
    BytesMap,
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
    /// K8s Quantity — protobuf message with field 1 (string) = canonical string form.
    /// Decodes to the string value directly (e.g. "100m", "32M", "1").
    /// Used for `resourceFieldRef.divisor` and any standalone Quantity field.
    Quantity,
    /// map<string, Quantity> — repeated MapEntry where value is a Quantity message.
    /// Used for ResourceRequirements.limits and ResourceRequirements.requests.
    QuantityMap,
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
                    // Upstream wraps the raw JSON in a `FieldsV1` message that
                    // carries a single `Raw` bytes field — registering this as
                    // bare `Bytes` mismatches the wire format and causes
                    // typed-client SSA writes to silently drop managed-field
                    // metadata.
                    (
                        7,
                        ("fieldsV1".into(), FieldType::Message("FieldsV1".into())),
                    ),
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
            // Upstream apps/v1.StatefulSetStatus per generated.proto:
            //   collisionCount     = 9    (was registered at 8)
            //   conditions         = 10   (was registered at 9)
            //   availableReplicas  = 11   (was registered at 10)
            // Field 8 is unallocated upstream (gap in the schema).
            MessageSchema {
                fields: HashMap::from([
                    (1, ("observedGeneration".into(), FieldType::Int)),
                    (2, ("replicas".into(), FieldType::Int)),
                    (3, ("readyReplicas".into(), FieldType::Int)),
                    (4, ("currentReplicas".into(), FieldType::Int)),
                    (5, ("updatedReplicas".into(), FieldType::Int)),
                    (6, ("currentRevision".into(), FieldType::String)),
                    (7, ("updateRevision".into(), FieldType::String)),
                    (9, ("collisionCount".into(), FieldType::Int)),
                    (
                        10,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "StatefulSetCondition".into(),
                            ))),
                        ),
                    ),
                    (11, ("availableReplicas".into(), FieldType::Int)),
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
                    // Upstream skips field #5 in DaemonSetSpec —
                    // revisionHistoryLimit is wire-tag #6, not #5.
                    (6, ("revisionHistoryLimit".into(), FieldType::Int)),
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
                    // divisor is resource.Quantity in proto — decode via Quantity message parser
                    (3, ("divisor".into(), FieldType::Quantity)),
                ]),
            },
        );
        // Upstream core/v1.ConfigMapKeySelector + SecretKeySelector both
        // embed `LocalObjectReference` at field 1, with `name` living inside
        // it. Go's protobuf tags flatten the inner field into the parent
        // JSON, which is what `InlineMessage` models — so the decoded JSON
        // still ends up as `{name, key, optional}`, but the wire format now
        // matches what real clients send (a length-delimited
        // LocalObjectReference message at #1, not a top-level string).
        schemas.insert(
            "ConfigMapKeySelector".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "localObjectReference".into(),
                            FieldType::InlineMessage("LocalObjectReference".into()),
                        ),
                    ),
                    (2, ("key".into(), FieldType::String)),
                    (3, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );
        schemas.insert(
            "SecretKeySelector".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "localObjectReference".into(),
                            FieldType::InlineMessage("LocalObjectReference".into()),
                        ),
                    ),
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
        // Affinity sub-types — wire layout per k8s.io/api/core/v1/generated.proto
        // (release-1.35). NodeAffinity field numbers:
        //   1 = requiredDuringSchedulingIgnoredDuringExecution  (NodeSelector)
        //   2 = preferredDuringSchedulingIgnoredDuringExecution (repeated PreferredSchedulingTerm)
        // PodAffinity / PodAntiAffinity field numbers (same shape):
        //   1 = requiredDuringSchedulingIgnoredDuringExecution  (repeated PodAffinityTerm)
        //   2 = preferredDuringSchedulingIgnoredDuringExecution (repeated WeightedPodAffinityTerm)
        schemas.insert(
            "NodeAffinity".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "requiredDuringSchedulingIgnoredDuringExecution".into(),
                            FieldType::Message("NodeSelector".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "preferredDuringSchedulingIgnoredDuringExecution".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PreferredSchedulingTerm".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PodAffinity".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "requiredDuringSchedulingIgnoredDuringExecution".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PodAffinityTerm".into(),
                            ))),
                        ),
                    ),
                    (
                        2,
                        (
                            "preferredDuringSchedulingIgnoredDuringExecution".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "WeightedPodAffinityTerm".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PodAntiAffinity".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "requiredDuringSchedulingIgnoredDuringExecution".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PodAffinityTerm".into(),
                            ))),
                        ),
                    ),
                    (
                        2,
                        (
                            "preferredDuringSchedulingIgnoredDuringExecution".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "WeightedPodAffinityTerm".into(),
                            ))),
                        ),
                    ),
                ]),
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
                    (17, ("ipFamilyPolicy".into(), FieldType::String)),
                    (
                        18,
                        (
                            "clusterIPs".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        19,
                        (
                            "ipFamilies".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        20,
                        ("allocateLoadBalancerNodePorts".into(), FieldType::Bool),
                    ),
                    (21, ("loadBalancerClass".into(), FieldType::String)),
                    (22, ("internalTrafficPolicy".into(), FieldType::String)),
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
                    (
                        11,
                        (
                            "podFailurePolicy".into(),
                            FieldType::Message("PodFailurePolicy".into()),
                        ),
                    ),
                    (12, ("backoffLimitPerIndex".into(), FieldType::Int)),
                    (13, ("maxFailedIndexes".into(), FieldType::Int)),
                    (14, ("podReplacementPolicy".into(), FieldType::String)),
                    (15, ("managedBy".into(), FieldType::String)),
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
                fields: HashMap::from([(
                    1,
                    (
                        "rules".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "PodFailurePolicyRule".into(),
                        ))),
                    ),
                )]),
            },
        );
        schemas.insert(
            "SuccessPolicy".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "rules".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "SuccessPolicyRule".into(),
                        ))),
                    ),
                )]),
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
        // PodStatus — field numbers from k8s.io/api/core/v1/generated.proto
        // (release-1.35). Critically, *every* field the typed kubernetes client
        // round-trips through Pods().UpdateStatus(...) must be enumerated here:
        // any unknown field is silently dropped by the protobuf decoder, which
        // wipes the status (including .conditions) — breaking the
        // `[sig-node] Pods should run through the lifecycle of Pods and
        // PodStatus` conformance test (and any other PodStatus PATCH/PUT).
        schemas.insert(
            "PodStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("phase".into(), FieldType::String)),
                    (
                        2,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PodCondition".into(),
                            ))),
                        ),
                    ),
                    (3, ("message".into(), FieldType::String)),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("hostIP".into(), FieldType::String)),
                    (6, ("podIP".into(), FieldType::String)),
                    (7, ("startTime".into(), FieldType::Message("Time".into()))),
                    (
                        8,
                        (
                            "containerStatuses".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ContainerStatus".into(),
                            ))),
                        ),
                    ),
                    (9, ("qosClass".into(), FieldType::String)),
                    (
                        10,
                        (
                            "initContainerStatuses".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ContainerStatus".into(),
                            ))),
                        ),
                    ),
                    (11, ("nominatedNodeName".into(), FieldType::String)),
                    (
                        12,
                        (
                            "podIPs".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("PodIP".into()))),
                        ),
                    ),
                    (
                        13,
                        (
                            "ephemeralContainerStatuses".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ContainerStatus".into(),
                            ))),
                        ),
                    ),
                    (14, ("resize".into(), FieldType::String)),
                    (
                        15,
                        (
                            "resourceClaimStatuses".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PodResourceClaimStatus".into(),
                            ))),
                        ),
                    ),
                    (
                        16,
                        (
                            "hostIPs".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("HostIP".into()))),
                        ),
                    ),
                    (17, ("observedGeneration".into(), FieldType::Int)),
                ]),
            },
        );
        // PodCondition / HostIP / PodIP / ContainerStatus / PodResourceClaimStatus
        // are defined further down in this same function; the PodStatus
        // schema above references them by name, so they only need to exist
        // somewhere in this registry (HashMap insertion order doesn't matter
        // for lookup).

        // ConfigMap & Secret
        // Upstream proto (k8s.io/api/core/v1/generated.proto):
        //   ConfigMap.data         (#2) = map<string, string>
        //   ConfigMap.binaryData   (#3) = map<string, bytes>
        //   Secret.data            (#2) = map<string, bytes>
        //   Secret.stringData      (#4) = map<string, string>
        // The bytes-valued maps are decoded to JSON objects with
        // base64-encoded string values, matching the typed-client wire
        // shape.
        schemas.insert(
            "ConfigMap".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("data".into(), FieldType::StringMap)),
                    (3, ("binaryData".into(), FieldType::BytesMap)),
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
                    (2, ("data".into(), FieldType::BytesMap)),
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
                // Field tags mirror upstream k8s.io/api/core/v1 NamespaceCondition.
                // Tag 3 is unused for namespaces (no lastProbeTime); the typed Go
                // client decodes these back into v1.NamespaceCondition, so an empty
                // schema would drop type/status/reason/message and break the
                // `should apply changes to a namespace status` conformance test.
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (2, ("status".into(), FieldType::String)),
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
                    // limits/requests are map<string, Quantity> — use QuantityMap decoder
                    (1, ("limits".into(), FieldType::QuantityMap)),
                    (2, ("requests".into(), FieldType::QuantityMap)),
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
                // NOTE: this schema is otherwise empty — the full NodeStatus
                // field set (capacity/allocatable maps, conditions, addresses,
                // nodeInfo, images, ...) is still unmapped, so those are dropped
                // for protobuf clients (tracked separately). daemonEndpoints
                // (field 6, matching upstream core/v1 NodeStatus) is mapped here
                // because the e2e metrics grabber reads
                // kubeletEndpoint.Port over protobuf and otherwise sees 0.
                fields: HashMap::from([(
                    6,
                    (
                        "daemonEndpoints".into(),
                        FieldType::Message("NodeDaemonEndpoints".into()),
                    ),
                )]),
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
                    (9, ("maximum".into(), FieldType::Double)),
                    (10, ("exclusiveMaximum".into(), FieldType::Bool)),
                    (11, ("minimum".into(), FieldType::Double)),
                    (12, ("exclusiveMinimum".into(), FieldType::Bool)),
                    (13, ("maxLength".into(), FieldType::Int)),
                    (14, ("minLength".into(), FieldType::Int)),
                    (15, ("pattern".into(), FieldType::String)),
                    (16, ("maxItems".into(), FieldType::Int)),
                    (17, ("minItems".into(), FieldType::Int)),
                    (18, ("uniqueItems".into(), FieldType::Bool)),
                    (19, ("multipleOf".into(), FieldType::Double)),
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
            // Upstream apiextensions-apiserver/pkg/apis/apiextensions/v1
            // generated.proto: rule=1, message=2, messageExpression=3,
            // reason=4, fieldPath=5, optionalOldSelf=6. The historical
            // registry numbered messageExpression onwards starting at 4 —
            // wire-incompatible with every CRD that ships
            // x-kubernetes-validations.
            MessageSchema {
                fields: HashMap::from([
                    (1, ("rule".into(), FieldType::String)),
                    (2, ("message".into(), FieldType::String)),
                    (3, ("messageExpression".into(), FieldType::String)),
                    (4, ("reason".into(), FieldType::String)),
                    (5, ("fieldPath".into(), FieldType::String)),
                    (6, ("optionalOldSelf".into(), FieldType::Bool)),
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
        Self::register_autoscaling_v1(&mut schemas);
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
        Self::register_core_v1_remaining_nested(&mut schemas);
        Self::register_list_kinds(&mut schemas);
        Self::register_apimachinery_extras(&mut schemas);
        Self::register_core_v1_subresource_options(&mut schemas);
        Self::register_autoscaling_v1_scale(&mut schemas);
        Self::register_apiextensions_v1_conversion(&mut schemas);
        Self::register_resource_v1(&mut schemas);
        Self::register_flowcontrol_v1(&mut schemas);
        Self::register_node_v1(&mut schemas);
        Self::register_authentication_v1(&mut schemas);
        Self::register_authorization_v1(&mut schemas);

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
                    // Go embeds `Rule` as `json:",inline"`, so the decoded JSON
                    // must merge apiGroups/apiVersions/resources/scope up into
                    // RuleWithOperations — not nest them under "rule". The Rust
                    // type uses `#[serde(flatten)]`, so a nested "rule" object is
                    // silently dropped, leaving every webhook rule empty (matching
                    // nothing → the webhook is never invoked). Use InlineMessage.
                    (2, ("rule".into(), FieldType::InlineMessage("Rule".into()))),
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
                            // Go embeds RuleWithOperations as `json:",inline"`; the
                            // Rust type flattens it too, so merge its fields up
                            // rather than nesting under "ruleWithOperations".
                            "ruleWithOperations".into(),
                            FieldType::InlineMessage("RuleWithOperations".into()),
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

        // FieldsV1 — wrapper message for the opaque JSON managed-fields
        // payload. Upstream defines it with a single `Raw` bytes field at #1
        // (capital R — proto field name follows Go style); the JSON document
        // is re-parsed by consumers. `ManagedFieldsEntry.fieldsV1` references
        // this as a Message, not bare bytes.
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

        // PartialObjectMetadata — the metadata-only projection requested via
        // `Accept: application/vnd.kubernetes.protobuf;as=PartialObjectMetadata`
        // (and the JSON sibling). Upstream:
        // `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto`
        // message #PartialObjectMetadata with a single `metadata` field at #1
        // pointing at `ObjectMeta`. Used by informer-cache and metadata-only
        // clients to keep watch traffic light.
        schemas.insert(
            "PartialObjectMetadata".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                )]),
            },
        );

        // PartialObjectMetadataList — list shape paired with
        // `PartialObjectMetadata`. Field #1 is `metadata` (ListMeta), field
        // #2 is `items` (repeated PartialObjectMetadata).
        schemas.insert(
            "PartialObjectMetadataList".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ListMeta".into())),
                    ),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PartialObjectMetadata".into(),
                            ))),
                        ),
                    ),
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
        // Field #1 is `aPIGroup` (not `apiGroup`) per upstream
        // k8s.io/api/networking/v1/generated.proto. K8s's Go-to-proto
        // generator lower-cases only the first character of a trailing
        // acronym in the field name, producing the unusual capitalisation.
        // Match it verbatim — the wire shape must equal the upstream
        // schema for typed clients to deserialize correctly.
        schemas.insert(
            "IngressClassParametersReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("aPIGroup".into(), FieldType::String)),
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

    /// Register autoscaling/v1 message schemas.
    ///
    /// Field numbers come from
    /// k8s.io/api/autoscaling/v1/generated.proto (release-1.35). Covers the
    /// `Scale` subresource (`Scale` + `ScaleSpec` + `ScaleStatus`), which is
    /// the request/response payload for `/scale` subresource endpoints on
    /// scalable kinds (`Deployment`, `StatefulSet`, `ReplicaSet`,
    /// `ReplicationController`). Other autoscaling/v1 messages (legacy v1
    /// HPA) are intentionally omitted; conformant clients negotiate
    /// autoscaling/v2 for HPA.
    fn register_autoscaling_v1(schemas: &mut HashMap<String, MessageSchema>) {
        // Scale: top-level subresource wrapper.
        //   field 1 = metadata (ObjectMeta)
        //   field 2 = spec (ScaleSpec)
        //   field 3 = status (ScaleStatus)
        schemas.insert(
            "Scale".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("ScaleSpec".into()))),
                    (
                        3,
                        ("status".into(), FieldType::Message("ScaleStatus".into())),
                    ),
                ]),
            },
        );

        // ScaleSpec: desired replicas (single int32 field).
        schemas.insert(
            "ScaleSpec".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("replicas".into(), FieldType::Int))]),
            },
        );

        // ScaleStatus: observed replicas plus selector string.
        //   field 1 = replicas (int32)
        //   field 2 = selector (string — already serialized to label-query form)
        schemas.insert(
            "ScaleStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("replicas".into(), FieldType::Int)),
                    (2, ("selector".into(), FieldType::String)),
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
        // EphemeralContainer wraps EphemeralContainerCommon at proto field 1.
        // Upstream Go declares the embed as `json:",inline"`, so its keys
        // (name, image, command, ...) appear at the EphemeralContainer JSON
        // level — never under an `ephemeralContainerCommon` wrapper. Our
        // Rust `EphemeralContainer` struct mirrors that flat shape. Use
        // `InlineMessage` so the proto→JSON decoder merges the inner
        // fields into the parent object.
        schemas.insert(
            "EphemeralContainer".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "ephemeralContainerCommon".into(),
                            FieldType::InlineMessage("EphemeralContainerCommon".into()),
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

        // ContainerState — the per-container state holder. Wire-format is
        // a normal protobuf message with three optional sub-state fields;
        // upstream's "+optional" semantics mean at most one is populated
        // at any time, but the registry only cares about the field
        // numbers + types. PR #141 added the three sub-state messages
        // (ContainerStateRunning/Waiting/Terminated) but explicitly
        // deferred this holder; without it, anything decoding a parent
        // that embeds `state` (most notably `ContainerStatus.state` at
        // field 2 below) gets `{}` for that field.
        //
        // K8s ref: k8s.io/api/core/v1/generated.proto:1083 (release-1.35).
        schemas.insert(
            "ContainerState".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "waiting".into(),
                            FieldType::Message("ContainerStateWaiting".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "running".into(),
                            FieldType::Message("ContainerStateRunning".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "terminated".into(),
                            FieldType::Message("ContainerStateTerminated".into()),
                        ),
                    ),
                ]),
            },
        );

        // ContainerStatus — the per-container status surface that the
        // kubelet writes on every Pod sync. Without this schema,
        // pod.status.containerStatuses entries decode as `{}` over
        // protobuf; today this is masked by the kubelet running with
        // `--kube-api-content-type=application/json` (see
        // scripts/run-conformance.sh) but any client that ignores the
        // override (or any future flip to protobuf-by-default) would
        // lose every container status field.
        //
        // Field 10 (allocatedResources) is `map<string, Quantity>`,
        // encoded with the QuantityMap codec (the same one
        // ResourceRequirements.requests/limits uses). Without it the
        // KEP-1287 in-place-resize conformance tests, which fetch the pod
        // over protobuf, see `allocatedResources: nil` and fail with
        // "status allocatedResources mismatch".
        //
        // K8s ref: k8s.io/api/core/v1/generated.proto:1137 (release-1.35).
        schemas.insert(
            "ContainerStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        ("state".into(), FieldType::Message("ContainerState".into())),
                    ),
                    (
                        3,
                        (
                            "lastState".into(),
                            FieldType::Message("ContainerState".into()),
                        ),
                    ),
                    (4, ("ready".into(), FieldType::Bool)),
                    (5, ("restartCount".into(), FieldType::Int)),
                    (6, ("image".into(), FieldType::String)),
                    (7, ("imageID".into(), FieldType::String)),
                    (8, ("containerID".into(), FieldType::String)),
                    (9, ("started".into(), FieldType::Bool)),
                    // 10: allocatedResources — map<string, Quantity>. The
                    //     kubelet sets this to the accepted resource requests
                    //     for KEP-1287 in-place pod resize; the conformance
                    //     `[sig-node] Pod InPlace Resize Container` tests read
                    //     it back over protobuf (client-go default) and assert
                    //     it equals `spec.resources.requests`. Encoding it via
                    //     the QuantityMap codec (same as ResourceRequirements)
                    //     keeps it from decoding as `nil`.
                    (10, ("allocatedResources".into(), FieldType::QuantityMap)),
                    (
                        11,
                        (
                            "resources".into(),
                            FieldType::Message("ResourceRequirements".into()),
                        ),
                    ),
                    (
                        12,
                        (
                            "volumeMounts".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "VolumeMountStatus".into(),
                            ))),
                        ),
                    ),
                    (
                        13,
                        ("user".into(), FieldType::Message("ContainerUser".into())),
                    ),
                    (
                        14,
                        (
                            "allocatedResourcesStatus".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ResourceStatus".into(),
                            ))),
                        ),
                    ),
                    (15, ("stopSignal".into(), FieldType::String)),
                ]),
            },
        );

        // VolumeMountStatus — referenced by ContainerStatus.volumeMounts
        // at field 12 above. Four scalar fields; trivial to register and
        // closes the only nested-message gap that ContainerStatus would
        // otherwise leave decoded as `{}`.
        //
        // K8s ref: k8s.io/api/core/v1/generated.proto (release-1.35).
        schemas.insert(
            "VolumeMountStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("mountPath".into(), FieldType::String)),
                    (3, ("readOnly".into(), FieldType::Bool)),
                    (4, ("recursiveReadOnly".into(), FieldType::String)),
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
        // events.k8s.io/v1.Event — distinct wire layout from core/v1.Event.
        // Registered under a group-qualified key so `decode_k8s_resource`'s
        // apiVersion-aware lookup picks the right schema; the unqualified
        // `Event` schema above stays as the core/v1 default.
        // Field numbers from k8s.io/api/events/v1/generated.proto (release-1.35).
        schemas.insert(
            "events.k8s.io/v1.Event".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("eventTime".into(), FieldType::Message("MicroTime".into())),
                    ),
                    (
                        3,
                        ("series".into(), FieldType::Message("EventSeries".into())),
                    ),
                    (4, ("reportingController".into(), FieldType::String)),
                    (5, ("reportingInstance".into(), FieldType::String)),
                    (6, ("action".into(), FieldType::String)),
                    (7, ("reason".into(), FieldType::String)),
                    (
                        8,
                        (
                            "regarding".into(),
                            FieldType::Message("ObjectReference".into()),
                        ),
                    ),
                    (
                        9,
                        (
                            "related".into(),
                            FieldType::Message("ObjectReference".into()),
                        ),
                    ),
                    (10, ("note".into(), FieldType::String)),
                    (11, ("type".into(), FieldType::String)),
                    // Fields 12-15 must mirror upstream
                    // `k8s.io/api/events/v1/generated.proto` exactly. Any
                    // drift here is silent: the WIRE_VARINT branch of
                    // `decode_with_schema` writes the raw varint into the
                    // JSON object without checking the declared FieldType,
                    // so a Message-typed slot fed a varint becomes
                    // `"<fieldName>": <number>` — which then round-trips
                    // through the `Event.extra` catch-all and is rejected
                    // by client-go's typed Event decoder (regression seen
                    // in [sig-instrumentation] Events API conformance,
                    // canary run 2026-05-27).
                    (
                        12,
                        (
                            "deprecatedSource".into(),
                            FieldType::Message("EventSource".into()),
                        ),
                    ),
                    (
                        13,
                        (
                            "deprecatedFirstTimestamp".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (
                        14,
                        (
                            "deprecatedLastTimestamp".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                    (15, ("deprecatedCount".into(), FieldType::Int)),
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
        schemas.insert(
            "LimitRangeItem".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    // max/min/default/defaultRequest/maxLimitRequestRatio are map<string, Quantity>
                    (2, ("max".into(), FieldType::QuantityMap)),
                    (3, ("min".into(), FieldType::QuantityMap)),
                    (4, ("default".into(), FieldType::QuantityMap)),
                    (5, ("defaultRequest".into(), FieldType::QuantityMap)),
                    (6, ("maxLimitRequestRatio".into(), FieldType::QuantityMap)),
                ]),
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
        //
        // persistentVolumeSource (field 2) references PersistentVolumeSource,
        // which is registered separately (see `register_core_v1_remaining_nested`).
        // Upstream Go marks the embed `json:",inline"`, so individual
        // volume-source keys (hostPath, nfs, csi, ...) live at the
        // PersistentVolumeSpec level on the JSON wire — never wrapped in
        // a `persistentVolumeSource` object. Our Rust struct mirrors that
        // flat shape. Use `InlineMessage` so proto→JSON merges correctly.
        schemas.insert(
            "PersistentVolumeSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        2,
                        (
                            "persistentVolumeSource".into(),
                            FieldType::InlineMessage("PersistentVolumeSource".into()),
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
        // ResourceQuotaSpec.hard is map<string, Quantity> per
        // k8s.io/api/core/v1/generated.proto (field 1).
        schemas.insert(
            "ResourceQuotaSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("hard".into(), FieldType::QuantityMap)),
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
        // ResourceQuotaStatus.hard and .used are map<string, Quantity> per
        // k8s.io/api/core/v1/generated.proto (fields 1 and 2).
        schemas.insert(
            "ResourceQuotaStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("hard".into(), FieldType::QuantityMap)),
                    (2, ("used".into(), FieldType::QuantityMap)),
                ]),
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
        // Field numbers match upstream
        // k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto exactly:
        // kind=1, name=3, uid=4, apiVersion=5, controller=6,
        // blockOwnerDeletion=7. The ordering is historical, not
        // monotonic — `kind` was added before `apiVersion`. Prior to
        // this fix the registry had apiVersion=1/kind=2 — wire-
        // incompatible with every real client and silently mis-decoded
        // ownerReferences on the way in.
        MessageSchema {
            fields: HashMap::from([
                (1, ("kind".into(), FieldType::String)),
                (3, ("name".into(), FieldType::String)),
                (4, ("uid".into(), FieldType::String)),
                (5, ("apiVersion".into(), FieldType::String)),
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
                (4, ("availableReplicas".into(), FieldType::Int)),
                (5, ("unavailableReplicas".into(), FieldType::Int)),
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
        // From core/v1/generated.proto — PodSpec has MANY fields.
        // Field numbers must match upstream k8s.io/api/core/v1 v1.35 verbatim;
        // protobuf dispatch is number-based, so any drift silently mis-decodes
        // typed-client traffic into adjacent fields.
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
                    23,
                    (
                        "hostAliases".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("HostAlias".into()))),
                    ),
                ),
                (24, ("priorityClassName".into(), FieldType::String)),
                (25, ("priority".into(), FieldType::Int)),
                (
                    26,
                    (
                        "dnsConfig".into(),
                        FieldType::Message("PodDNSConfig".into()),
                    ),
                ),
                (27, ("shareProcessNamespace".into(), FieldType::Bool)),
                (
                    28,
                    (
                        "readinessGates".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "PodReadinessGate".into(),
                        ))),
                    ),
                ),
                (29, ("runtimeClassName".into(), FieldType::String)),
                (30, ("enableServiceLinks".into(), FieldType::Bool)),
                (31, ("preemptionPolicy".into(), FieldType::String)),
                (32, ("overhead".into(), FieldType::QuantityMap)),
                (
                    33,
                    (
                        "topologySpreadConstraints".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "TopologySpreadConstraint".into(),
                        ))),
                    ),
                ),
                (
                    34,
                    (
                        "ephemeralContainers".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "EphemeralContainer".into(),
                        ))),
                    ),
                ),
                (35, ("setHostnameAsFQDN".into(), FieldType::Bool)),
                (36, ("os".into(), FieldType::Message("PodOS".into()))),
                (37, ("hostUsers".into(), FieldType::Bool)),
                (
                    38,
                    (
                        "schedulingGates".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "PodSchedulingGate".into(),
                        ))),
                    ),
                ),
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
                        "resources".into(),
                        FieldType::Message("ResourceRequirements".into()),
                    ),
                ),
                (41, ("hostnameOverride".into(), FieldType::String)),
                (
                    42,
                    (
                        "workloadRef".into(),
                        FieldType::Message("WorkloadReference".into()),
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
                    21,
                    (
                        "volumeDevices".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("VolumeDevice".into()))),
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
                // limits/requests are map<string, Quantity> — use QuantityMap decoder
                (1, ("limits".into(), FieldType::QuantityMap)),
                (2, ("requests".into(), FieldType::QuantityMap)),
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
        // Probe embeds ProbeHandler at proto field 1. Upstream Go marks the
        // embed `json:",inline"`, so the action keys (httpGet, tcpSocket,
        // exec, grpc) appear at the Probe JSON level — never under a
        // `handler` wrapper. Our Rust `Probe` struct exposes those fields
        // directly; use `InlineMessage` so the proto→JSON decoder lifts
        // them out of the inner message into the Probe object.
        MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    (
                        "handler".into(),
                        FieldType::InlineMessage("ProbeHandler".into()),
                    ),
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
                // Upstream core/v1.PodSecurityContext numbers per generated.proto:
                //   appArmorProfile          = 11   (was registered at 12)
                //   supplementalGroupsPolicy = 12   (was registered at 13)
                //   seLinuxChangePolicy      = 13   (was missing entirely)
                (
                    11,
                    (
                        "appArmorProfile".into(),
                        FieldType::Message("AppArmorProfile".into()),
                    ),
                ),
                (12, ("supplementalGroupsPolicy".into(), FieldType::String)),
                (13, ("seLinuxChangePolicy".into(), FieldType::String)),
            ]),
        }
    }

    /// Iterate over all registered `(message_name, schema)` pairs.
    /// Intended for tests that compare the registry against upstream `.proto`
    /// definitions. Not part of any runtime path.
    pub fn iter_schemas(&self) -> impl Iterator<Item = (&str, &MessageSchema)> {
        self.schemas.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Number of schemas registered.
    pub fn schema_count(&self) -> usize {
        self.schemas.len()
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
                        // For a repeated scalar the element type — not the
                        // `Repeated` wrapper — decides the JSON form. Decoding a
                        // repeated bool through the wildcard arm yielded integer
                        // `1`, which then failed to re-encode (`1.as_bool()` is
                        // None) and silently dropped the element (e.g.
                        // DeviceAttribute.bools).
                        let scalar_type = match field_type {
                            FieldType::Repeated(inner) => inner.as_ref(),
                            other => other,
                        };
                        let json_val = match scalar_type {
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
                    let bits = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    if let Some((name, field_type)) = schema.fields.get(&field_num) {
                        let json_val = match field_type {
                            FieldType::Double => {
                                match serde_json::Number::from_f64(f64::from_bits(bits)) {
                                    Some(n) => Value::Number(n),
                                    None => Value::Null,
                                }
                            }
                            _ => json!(bits),
                        };
                        obj.insert(name.clone(), json_val);
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
                            FieldType::BytesMap => {
                                // map<string, bytes> — each MapEntry has field 1
                                // (key string) and field 2 (bytes value). Decode
                                // the value as raw bytes and base64-encode for
                                // the JSON representation, matching what typed
                                // K8s clients expect for Secret.data and
                                // ConfigMap.binaryData.
                                let (key, val) = decode_bytes_map_entry(field_data);
                                let map = obj
                                    .entry(name.clone())
                                    .or_insert_with(|| Value::Object(Map::new()));
                                if let Value::Object(ref mut m) = map {
                                    m.insert(key, Value::String(val));
                                }
                            }
                            FieldType::QuantityMap => {
                                // map<string, Quantity> — each MapEntry has field 1 (key string)
                                // and field 2 (Quantity message). Decode the Quantity message to
                                // extract its string representation (field 1 of Quantity).
                                let (key, val) = decode_quantity_map_entry(field_data);
                                if !key.is_empty() {
                                    let map = obj
                                        .entry(name.clone())
                                        .or_insert_with(|| Value::Object(Map::new()));
                                    if let Value::Object(ref mut m) = map {
                                        m.insert(key, Value::String(val));
                                    }
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
                            FieldType::String => {
                                // proto2 optional string: skip when empty so that an
                                // absent field (zero-length wire encoding) is not
                                // confused with a present-but-empty value.  This
                                // prevents `"value": ""` from appearing in an EnvVar
                                // that only has valueFrom set, which would otherwise
                                // shadow the valueFrom path in the kubelet.
                                if !field_data.is_empty() {
                                    let s = String::from_utf8_lossy(field_data).to_string();
                                    obj.insert(name.clone(), Value::String(s));
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
                if msg_type == "MicroTime" {
                    // K8s MicroTime is wire-identical to Time but its JSON form
                    // keeps microsecond precision (see metav1.MicroTime.MarshalJSON).
                    // Falling back to the generic message decoder here would emit
                    // `{seconds, nanos}` and break every consumer that types
                    // these fields as RFC3339 strings — most visibly
                    // `events.k8s.io/v1.Event.eventTime` (see canary run
                    // #26315095760).
                    return decode_micro_timestamp(data);
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
            FieldType::Double => {
                // Length-delimited 8-byte double is unusual outside of
                // packed-repeated. Decode the first 8 bytes if present.
                if data.len() >= 8 {
                    let bits = u64::from_le_bytes(data[..8].try_into().unwrap());
                    match serde_json::Number::from_f64(f64::from_bits(bits)) {
                        Some(n) => Value::Number(n),
                        None => Value::Null,
                    }
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
            FieldType::BytesMap => {
                // Should be handled at the caller level as BytesMapEntry
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
            FieldType::Quantity => {
                // K8s Quantity: protobuf message with field 1 = canonical string.
                let s = decode_quantity(data);
                Value::String(s)
            }
            FieldType::QuantityMap => {
                // Should be handled at the caller level as QuantityMapEntry
                Value::Object(Map::new())
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

    // =====================================================================
    // Schema-driven JSON → native protobuf encoder
    // =====================================================================
    //
    // Symmetric mirror of `decode_message`: walks the registered schema in
    // lockstep with a `serde_json::Value`, producing wire-format bytes for
    // every `FieldType` variant the decoder consumes. The result is exactly
    // what upstream `pb.go` `Marshal` methods would produce — the same bytes
    // that, when piped back through `decode_message`, reconstruct the source
    // JSON.
    //
    // Note on field ordering: we walk the schema's field table by ascending
    // tag number so the wire output is stable across hash-map iteration
    // order. Upstream `pb.go` codegen also emits in tag order.
    //
    // Note on missing schemas: unknown message types return `None`, the same
    // signal the decoder uses. Callers in `response.rs` fall back to the
    // JSON-wrapping encoder in that case so a missing schema cannot break
    // an endpoint that already worked under the JSON path.

    /// Encode a JSON value as the named message type. Returns `None` if the
    /// type is not registered.
    pub fn encode_message(&self, msg_type: &str, value: &Value) -> Option<Vec<u8>> {
        let schema = self.schemas.get(msg_type)?;
        Some(self.encode_with_schema(schema, value))
    }

    /// Encode a JSON value (expected `Value::Object`) using the supplied
    /// schema. Non-object values produce an empty payload (a missing message
    /// on the wire), matching what an absent JSON field would emit at the
    /// caller.
    fn encode_with_schema(&self, schema: &MessageSchema, value: &Value) -> Vec<u8> {
        let mut buf = Vec::new();
        let obj = match value.as_object() {
            Some(o) => o,
            None => return buf,
        };

        // Sort by tag for stable output.
        let mut fields: Vec<(&u32, &(String, FieldType))> = schema.fields.iter().collect();
        fields.sort_by_key(|(tag, _)| **tag);

        for (tag, (name, field_type)) in fields {
            // Inline messages collect their fields from the parent object —
            // there is no JSON key for the embedded message itself.
            if let FieldType::InlineMessage(inner_type) = field_type {
                self.encode_inline_message(&mut buf, *tag, inner_type, obj);
                continue;
            }

            let Some(val) = obj.get(name) else { continue };
            if val.is_null() {
                continue;
            }
            self.encode_field(&mut buf, *tag, field_type, val);
        }

        buf
    }

    /// Encode a single (tag, FieldType, Value) tuple to the buffer.
    fn encode_field(&self, buf: &mut Vec<u8>, tag: u32, field_type: &FieldType, val: &Value) {
        match field_type {
            FieldType::String => {
                if let Some(s) = val.as_str() {
                    push_string_field(buf, tag, s.as_bytes());
                }
            }
            FieldType::Int => {
                if let Some(n) = json_to_i64(val) {
                    // proto2/3 int{32,64} both fit a varint of the unsigned
                    // representation — match upstream by zero-extending.
                    push_varint_field(buf, tag, n as u64);
                }
            }
            FieldType::Bool => {
                let b = val.as_bool().unwrap_or(false);
                if b {
                    push_varint_field(buf, tag, 1);
                } else {
                    // proto2 omits false; emit only when present-and-true to
                    // match the decoder which treats missing as default.
                }
            }
            FieldType::Double => {
                if let Some(f) = val.as_f64() {
                    encode_varint(buf, ((tag as u64) << 3) | 1); // wire type 1 (fixed64)
                    buf.extend_from_slice(&f.to_bits().to_le_bytes());
                }
            }
            FieldType::Bytes => {
                // K8s wire format for `bytes` is base64 in JSON. Decode the
                // string then write raw bytes on the wire.
                let raw = json_bytes_value(val);
                push_length_delimited_field(buf, tag, &raw);
            }
            FieldType::Message(inner_type) => {
                // `Time` / `MicroTime` are registered as `{seconds, nanos}`
                // schemas, but K8s JSON serialises them as RFC3339 strings —
                // mirroring `decode_timestamp` which produces a string on
                // the decode side. When the JSON value is a string we must
                // round-trip via `encode_timestamp` rather than the
                // generic `encode_with_schema` (which would see a non-Object
                // value and emit a zero-length submessage).
                if (inner_type == "Time" || inner_type == "MicroTime") && val.is_string() {
                    let bytes = encode_timestamp(val);
                    push_length_delimited_field(buf, tag, &bytes);
                } else if let Some(inner) = self.encode_message(inner_type, val) {
                    push_length_delimited_field(buf, tag, &inner);
                }
            }
            FieldType::InlineMessage(_) => {
                // Handled at the caller in `encode_with_schema` because the
                // inline message's fields live in the surrounding object.
            }
            FieldType::Repeated(inner) => {
                if let Some(arr) = val.as_array() {
                    for elem in arr {
                        if elem.is_null() {
                            continue;
                        }
                        self.encode_field(buf, tag, inner, elem);
                    }
                }
            }
            FieldType::StringMap => {
                if let Some(map) = val.as_object() {
                    for (k, v) in map {
                        let mut entry = Vec::new();
                        push_string_field(&mut entry, 1, k.as_bytes());
                        if let Some(s) = v.as_str() {
                            push_string_field(&mut entry, 2, s.as_bytes());
                        }
                        push_length_delimited_field(buf, tag, &entry);
                    }
                }
            }
            FieldType::BytesMap => {
                if let Some(map) = val.as_object() {
                    for (k, v) in map {
                        let mut entry = Vec::new();
                        push_string_field(&mut entry, 1, k.as_bytes());
                        let raw = json_bytes_value(v);
                        push_length_delimited_field(&mut entry, 2, &raw);
                        push_length_delimited_field(buf, tag, &entry);
                    }
                }
            }
            FieldType::QuantityMap => {
                if let Some(map) = val.as_object() {
                    for (k, v) in map {
                        let mut entry = Vec::new();
                        push_string_field(&mut entry, 1, k.as_bytes());
                        // Quantity message: field 1 = canonical string.
                        let s = v.as_str().unwrap_or("");
                        let mut qbuf = Vec::new();
                        push_string_field(&mut qbuf, 1, s.as_bytes());
                        push_length_delimited_field(&mut entry, 2, &qbuf);
                        push_length_delimited_field(buf, tag, &entry);
                    }
                }
            }
            FieldType::MessageMap(inner_type) => {
                if let Some(map) = val.as_object() {
                    for (k, v) in map {
                        let mut entry = Vec::new();
                        push_string_field(&mut entry, 1, k.as_bytes());
                        if let Some(inner) = self.encode_message(inner_type, v) {
                            push_length_delimited_field(&mut entry, 2, &inner);
                        }
                        push_length_delimited_field(buf, tag, &entry);
                    }
                }
            }
            FieldType::IntOrString => {
                // K8s IntOrString proto: field 1 (type, int32), field 2
                // (intVal, int32), field 3 (strVal, string). type=0 means
                // intVal is set; type=1 means strVal is set.
                let mut inner = Vec::new();
                if let Some(s) = val.as_str() {
                    push_varint_field(&mut inner, 1, 1);
                    push_string_field(&mut inner, 3, s.as_bytes());
                } else if let Some(n) = json_to_i64(val) {
                    push_varint_field(&mut inner, 1, 0);
                    push_varint_field(&mut inner, 2, n as u64);
                }
                push_length_delimited_field(buf, tag, &inner);
            }
            FieldType::Quantity => {
                // Quantity message with field 1 = canonical string.
                let s = val.as_str().unwrap_or("");
                let mut inner = Vec::new();
                push_string_field(&mut inner, 1, s.as_bytes());
                push_length_delimited_field(buf, tag, &inner);
            }
            FieldType::JsonRaw => {
                // K8s JSON / RawExtension: a message with field 1 = raw
                // bytes containing JSON. Re-serialise the Value into bytes.
                let raw = serde_json::to_vec(val).unwrap_or_default();
                let mut inner = Vec::new();
                push_length_delimited_field(&mut inner, 1, &raw);
                push_length_delimited_field(buf, tag, &inner);
            }
        }
    }

    /// Encode an inline message: gather every JSON field defined by the
    /// inner schema out of the parent object, build a sub-`Value::Object`,
    /// and emit it as a nested message at `tag`. Skip emission entirely if
    /// none of the inner fields are present so an absent inline message
    /// does not produce a zero-length submessage on the wire.
    fn encode_inline_message(
        &self,
        buf: &mut Vec<u8>,
        tag: u32,
        inner_type: &str,
        parent: &Map<String, Value>,
    ) {
        let Some(inner_schema) = self.schemas.get(inner_type) else {
            return;
        };
        let mut sub = Map::new();
        for (name, inner_ft) in inner_schema.fields.values() {
            // Recurse: an inline message can itself contain inline messages.
            if let FieldType::InlineMessage(deeper) = inner_ft {
                if let Some(deep_schema) = self.schemas.get(deeper) {
                    for (deep_name, _) in deep_schema.fields.values() {
                        if let Some(v) = parent.get(deep_name) {
                            sub.insert(deep_name.clone(), v.clone());
                        }
                    }
                }
                continue;
            }
            if let Some(v) = parent.get(name) {
                sub.insert(name.clone(), v.clone());
            }
        }
        if sub.is_empty() {
            return;
        }
        let inner = self.encode_with_schema(inner_schema, &Value::Object(sub));
        push_length_delimited_field(buf, tag, &inner);
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

        // Look up the schema. Prefer a group-qualified key
        // (`<apiVersion>.<kind>`, e.g. `events.k8s.io/v1.Event`) so kinds that
        // collide on bare name but differ across groups — most notably
        // `Event` between `core/v1` and `events.k8s.io/v1`, which have
        // entirely distinct proto field numberings — pick the right schema.
        // Fall back to the bare kind for the (vast majority) of types where
        // only one definition exists.
        let qualified = format!("{}.{}", api_version, kind);
        let decoded = self
            .decode_message(&qualified, raw)
            .or_else(|| self.decode_message(&kind, raw));
        if let Some(json_obj) = decoded {
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
/// Decode a K8s `resource.Quantity` protobuf message to its canonical string form.
///
/// The Quantity message (from `k8s.io/apimachinery/pkg/api/resource/generated.proto`):
///   field 1 (string): the canonical string representation, e.g. "100m", "32M", "1"
///   field 2 (SuffixedValue): internal binary representation (we ignore this)
///
/// We extract field 1. If not found, return an empty string (caller's responsibility
/// to handle the fallback case).
fn decode_quantity(data: &[u8]) -> String {
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
            if field_num == 1 {
                // Field 1 is the canonical string form of the quantity
                return String::from_utf8_lossy(&data[pos..pos + len]).to_string();
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
    String::new()
}

/// Decode a protobuf map entry where the value is a Quantity message.
/// Returns (key_string, quantity_string).
fn decode_quantity_map_entry(data: &[u8]) -> (String, String) {
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
            match field_num {
                1 => {
                    key = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                }
                2 => {
                    // Value is a Quantity protobuf message — decode field 1 of that message
                    val = decode_quantity(&data[pos..pos + len]);
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

/// Decode a `map<string, bytes>` entry. MapEntry messages have field 1
/// (key string) and field 2 (value bytes). The value is base64-encoded
/// for the JSON representation, matching what typed K8s clients expect
/// for `Secret.data` and `ConfigMap.binaryData`.
fn decode_bytes_map_entry(data: &[u8]) -> (String, String) {
    use base64::Engine;
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
            match field_num {
                1 => {
                    key = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                }
                2 => {
                    val = base64::engine::general_purpose::STANDARD.encode(&data[pos..pos + len]);
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

/// Decode a K8s Timestamp protobuf to RFC3339 string (second precision).
fn decode_timestamp(data: &[u8]) -> Value {
    decode_timestamp_inner(data, "%Y-%m-%dT%H:%M:%SZ")
}

/// Decode a K8s MicroTime protobuf to RFC3339 string with microsecond
/// precision. Same wire layout as `Time`; differs only in JSON output
/// granularity (`metav1.MicroTime.MarshalJSON` keeps fractional seconds).
fn decode_micro_timestamp(data: &[u8]) -> Value {
    decode_timestamp_inner(data, "%Y-%m-%dT%H:%M:%S%.6fZ")
}

fn decode_timestamp_inner(data: &[u8], fmt: &str) -> Value {
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
        Some(dt) => Value::String(dt.format(fmt).to_string()),
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
                                // Group-qualified so the bare `EndpointPort`
                                // key stays bound to core/v1 (whose field
                                // 2/3 order is the OPPOSITE of discovery's
                                // — see the schema registration below).
                                "discovery.k8s.io/v1.EndpointPort".into(),
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

        // discovery.k8s.io/v1.EndpointPort — field 2/3 order is swapped
        // vs core/v1.EndpointPort. Registered under a group-qualified key
        // so the bare `EndpointPort` slot (registered earlier with the
        // core/v1 layout) stays bound to core/v1 — last-writer-wins on a
        // shared bare key would silently flip the wire layout under
        // core/v1.Endpoints decode.
        schemas.insert(
            "discovery.k8s.io/v1.EndpointPort".into(),
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

    /// Register the last 14 core/v1 nested messages that previous PRs
    /// left behind. None of these are top-level kinds; they're all leaf
    /// messages reached only via specific feature paths (envFrom, DRA
    /// alpha, node-config alpha, certificate projection, etc.). Closes
    /// the entire core/v1 gap recorded in
    /// docs/conformance/protobuf-schema-coverage.md after PR #184/#185.
    ///
    /// Field numbers verified against
    /// k8s.io/api/core/v1/generated.proto (release-1.35).
    fn register_core_v1_remaining_nested(schemas: &mut HashMap<String, MessageSchema>) {
        // -- envFrom sources (same shape: LocalObjectReference + optional bool) ----

        // Field 1 is an embedded LocalObjectReference whose Go JSON tag is
        // `json:",inline"` — the `name` key surfaces directly on the parent
        // object on the wire. Use `InlineMessage` so the proto→JSON decoder
        // flattens its fields, matching every other LocalObjectReference
        // embedding (ConfigMapVolumeSource, SecretProjection, etc.). The
        // wrapped `Message` variant produces `{"localObjectReference":{...}}`,
        // which then fails the typed `ConfigMapEnvSource`/`SecretEnvSource`
        // decode with `missing field 'name'`.
        schemas.insert(
            "ConfigMapEnvSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "localObjectReference".into(),
                            FieldType::InlineMessage("LocalObjectReference".into()),
                        ),
                    ),
                    (2, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );

        schemas.insert(
            "SecretEnvSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "localObjectReference".into(),
                            FieldType::InlineMessage("LocalObjectReference".into()),
                        ),
                    ),
                    (2, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );

        // -- ConfigMapNodeConfigSource: alpha NodeConfig (deprecated DynamicKubeletConfig)
        schemas.insert(
            "ConfigMapNodeConfigSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("namespace".into(), FieldType::String)),
                    (2, ("name".into(), FieldType::String)),
                    (3, ("uid".into(), FieldType::String)),
                    (4, ("resourceVersion".into(), FieldType::String)),
                    (5, ("kubeletConfigKey".into(), FieldType::String)),
                ]),
            },
        );

        // -- DRA (Dynamic Resource Allocation) --

        schemas.insert(
            "ContainerExtendedResourceRequest".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("containerName".into(), FieldType::String)),
                    (2, ("resourceName".into(), FieldType::String)),
                    (3, ("requestName".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "PodExtendedResourceClaimStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "requestMappings".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ContainerExtendedResourceRequest".into(),
                            ))),
                        ),
                    ),
                    (2, ("resourceClaimName".into(), FieldType::String)),
                ]),
            },
        );

        // -- Node + Pod IP / endpoint leaf messages --

        schemas.insert(
            "DaemonEndpoint".into(),
            // Upstream field name is the unusual capitalised `Port` (not
            // `port`); preserve the JSON key.
            MessageSchema {
                fields: HashMap::from([(1, ("Port".into(), FieldType::Int))]),
            },
        );

        schemas.insert(
            "HostIP".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("ip".into(), FieldType::String))]),
            },
        );

        // -- file-key selector (used by env from file) --
        schemas.insert(
            "FileKeySelector".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("volumeName".into(), FieldType::String)),
                    (2, ("path".into(), FieldType::String)),
                    (3, ("key".into(), FieldType::String)),
                    (4, ("optional".into(), FieldType::Bool)),
                ]),
            },
        );

        // -- volume status leaf messages --
        schemas.insert(
            "ModifyVolumeStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("targetVolumeAttributesClassName".into(), FieldType::String),
                    ),
                    (2, ("status".into(), FieldType::String)),
                ]),
            },
        );

        schemas.insert(
            "PersistentVolumeClaimCondition".into(),
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

        // -- PersistentVolumeSource: the oneof-style holder over every
        //    PV backend. Each sub-message is already registered (most by
        //    register_core_v1_cloud_volume_sources). Field numbers below
        //    are upstream-stable.
        schemas.insert(
            "PersistentVolumeSource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "gcePersistentDisk".into(),
                            FieldType::Message("GCEPersistentDiskVolumeSource".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "awsElasticBlockStore".into(),
                            FieldType::Message("AWSElasticBlockStoreVolumeSource".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "hostPath".into(),
                            FieldType::Message("HostPathVolumeSource".into()),
                        ),
                    ),
                    (
                        4,
                        (
                            "glusterfs".into(),
                            FieldType::Message("GlusterfsPersistentVolumeSource".into()),
                        ),
                    ),
                    (
                        5,
                        ("nfs".into(), FieldType::Message("NFSVolumeSource".into())),
                    ),
                    (
                        6,
                        (
                            "rbd".into(),
                            FieldType::Message("RBDPersistentVolumeSource".into()),
                        ),
                    ),
                    (
                        7,
                        (
                            "iscsi".into(),
                            FieldType::Message("ISCSIPersistentVolumeSource".into()),
                        ),
                    ),
                    (
                        8,
                        (
                            "cinder".into(),
                            FieldType::Message("CinderPersistentVolumeSource".into()),
                        ),
                    ),
                    (
                        9,
                        (
                            "cephfs".into(),
                            FieldType::Message("CephFSPersistentVolumeSource".into()),
                        ),
                    ),
                    (
                        10,
                        ("fc".into(), FieldType::Message("FCVolumeSource".into())),
                    ),
                    (
                        11,
                        (
                            "flocker".into(),
                            FieldType::Message("FlockerVolumeSource".into()),
                        ),
                    ),
                    (
                        12,
                        (
                            "flexVolume".into(),
                            FieldType::Message("FlexPersistentVolumeSource".into()),
                        ),
                    ),
                    (
                        13,
                        (
                            "azureFile".into(),
                            FieldType::Message("AzureFilePersistentVolumeSource".into()),
                        ),
                    ),
                    (
                        14,
                        (
                            "vsphereVolume".into(),
                            FieldType::Message("VsphereVirtualDiskVolumeSource".into()),
                        ),
                    ),
                    (
                        15,
                        (
                            "quobyte".into(),
                            FieldType::Message("QuobyteVolumeSource".into()),
                        ),
                    ),
                    (
                        16,
                        (
                            "azureDisk".into(),
                            FieldType::Message("AzureDiskVolumeSource".into()),
                        ),
                    ),
                    (
                        17,
                        (
                            "photonPersistentDisk".into(),
                            FieldType::Message("PhotonPersistentDiskVolumeSource".into()),
                        ),
                    ),
                    (
                        18,
                        (
                            "portworxVolume".into(),
                            FieldType::Message("PortworxVolumeSource".into()),
                        ),
                    ),
                    (
                        19,
                        (
                            "scaleIO".into(),
                            FieldType::Message("ScaleIOPersistentVolumeSource".into()),
                        ),
                    ),
                    (
                        20,
                        (
                            "local".into(),
                            FieldType::Message("LocalVolumeSource".into()),
                        ),
                    ),
                    (
                        21,
                        (
                            "storageos".into(),
                            FieldType::Message("StorageOSPersistentVolumeSource".into()),
                        ),
                    ),
                    (
                        22,
                        (
                            "csi".into(),
                            FieldType::Message("CSIPersistentVolumeSource".into()),
                        ),
                    ),
                ]),
            },
        );

        // -- PodCertificateProjection (alpha PodCertificate feature) --
        schemas.insert(
            "PodCertificateProjection".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("signerName".into(), FieldType::String)),
                    (2, ("keyType".into(), FieldType::String)),
                    (3, ("maxExpirationSeconds".into(), FieldType::Int)),
                    (4, ("credentialBundlePath".into(), FieldType::String)),
                    (5, ("keyPath".into(), FieldType::String)),
                    (6, ("certificateChainPath".into(), FieldType::String)),
                    (7, ("userAnnotations".into(), FieldType::StringMap)),
                ]),
            },
        );

        // -- VolumeDevice: raw block volume mount --
        schemas.insert(
            "VolumeDevice".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("devicePath".into(), FieldType::String)),
                ]),
            },
        );

        // -- WorkloadReference: PodGroup / Coscheduling --
        schemas.insert(
            "WorkloadReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("podGroup".into(), FieldType::String)),
                    (3, ("podGroupReplicaKey".into(), FieldType::String)),
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
                            // apiregistration's ServiceReference has a different
                            // proto layout (port at field 3, no path) than the
                            // admissionregistration/CRD one — use a distinct schema
                            // key so they don't clobber each other in the registry.
                            "service".into(),
                            FieldType::Message("APIServiceReference".into()),
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
        // ServiceReference (apiregistration/v1) — DISTINCT proto from
        // admissionregistration/v1 + apiextensions/v1 `ServiceReference`, which
        // have `path` at field 3 and `port` at field 4. apiregistration's has
        // `port` at field 3 and no `path`. These previously shared the
        // "ServiceReference" registry key, so this one clobbered the webhook/CRD
        // schema — breaking webhook clientConfig.service decode (path dropped,
        // port read from the path's first byte). Keyed separately as
        // "APIServiceReference" and referenced only by APIServiceSpec.service.
        schemas.insert(
            "APIServiceReference".into(),
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
                    (
                        3,
                        ("acquireTime".into(), FieldType::Message("MicroTime".into())),
                    ),
                    (
                        4,
                        ("renewTime".into(), FieldType::Message("MicroTime".into())),
                    ),
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

    /// Register every K8s `*List` collection wrapper. Each is the canonical
    /// `{ metadata: ListMeta = 1, items: repeated T = 2 }` envelope upstream
    /// `protoc` emits — only the element type differs. Registering them is
    /// not strictly required for the proto-decode path (the watch envelope
    /// uses `Unknown`+`raw`, not a registered shape for the list message
    /// itself), but the upstream schema-parity dashboard expects parity, and
    /// future direct-decode paths get a real schema to look up.
    ///
    /// Field numbers from the respective `generated.proto` files
    /// (release-1.35). Both fields use the JSON name (`metadata`, `items`)
    /// as the registry key; the upstream parser emits the same lowercase
    /// names.
    fn register_list_kinds(schemas: &mut HashMap<String, MessageSchema>) {
        fn list_schema(item: &str) -> MessageSchema {
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ListMeta".into())),
                    ),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(item.into()))),
                        ),
                    ),
                ]),
            }
        }

        // core/v1 Lists
        schemas.insert("ComponentStatusList".into(), list_schema("ComponentStatus"));
        schemas.insert("ConfigMapList".into(), list_schema("ConfigMap"));
        schemas.insert("EndpointsList".into(), list_schema("Endpoints"));
        schemas.insert("EventList".into(), list_schema("Event"));
        schemas.insert("LimitRangeList".into(), list_schema("LimitRange"));
        schemas.insert("NamespaceList".into(), list_schema("Namespace"));
        schemas.insert("NodeList".into(), list_schema("Node"));
        schemas.insert(
            "PersistentVolumeClaimList".into(),
            list_schema("PersistentVolumeClaim"),
        );
        schemas.insert(
            "PersistentVolumeList".into(),
            list_schema("PersistentVolume"),
        );
        schemas.insert("PodList".into(), list_schema("Pod"));
        schemas.insert("PodTemplateList".into(), list_schema("PodTemplate"));
        schemas.insert(
            "ReplicationControllerList".into(),
            list_schema("ReplicationController"),
        );
        schemas.insert("ResourceQuotaList".into(), list_schema("ResourceQuota"));
        schemas.insert("SecretList".into(), list_schema("Secret"));
        schemas.insert("ServiceAccountList".into(), list_schema("ServiceAccount"));
        schemas.insert("ServiceList".into(), list_schema("Service"));

        // core/v1.List and meta/v1.List both have the same simple name and
        // the same shape: items is `repeated runtime.RawExtension`. The
        // upstream by-simple-name index keeps one of them (insertion order
        // dependent). Either way the registered shape matches both.
        schemas.insert(
            "List".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ListMeta".into())),
                    ),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::JsonRaw)),
                        ),
                    ),
                ]),
            },
        );

        // apps/v1 Lists
        schemas.insert(
            "ControllerRevisionList".into(),
            list_schema("ControllerRevision"),
        );
        schemas.insert("DaemonSetList".into(), list_schema("DaemonSet"));
        schemas.insert("DeploymentList".into(), list_schema("Deployment"));
        schemas.insert("ReplicaSetList".into(), list_schema("ReplicaSet"));
        schemas.insert("StatefulSetList".into(), list_schema("StatefulSet"));

        // batch/v1
        schemas.insert("CronJobList".into(), list_schema("CronJob"));
        schemas.insert("JobList".into(), list_schema("Job"));

        // networking/v1
        schemas.insert("IPAddressList".into(), list_schema("IPAddress"));
        schemas.insert("IngressClassList".into(), list_schema("IngressClass"));
        schemas.insert("IngressList".into(), list_schema("Ingress"));
        schemas.insert("NetworkPolicyList".into(), list_schema("NetworkPolicy"));
        schemas.insert("ServiceCIDRList".into(), list_schema("ServiceCIDR"));

        // policy/v1
        schemas.insert(
            "PodDisruptionBudgetList".into(),
            list_schema("PodDisruptionBudget"),
        );

        // rbac/v1
        schemas.insert(
            "ClusterRoleBindingList".into(),
            list_schema("ClusterRoleBinding"),
        );
        schemas.insert("ClusterRoleList".into(), list_schema("ClusterRole"));
        schemas.insert("RoleBindingList".into(), list_schema("RoleBinding"));
        schemas.insert("RoleList".into(), list_schema("Role"));

        // scheduling/v1
        schemas.insert("PriorityClassList".into(), list_schema("PriorityClass"));

        // storage/v1
        schemas.insert("CSIDriverList".into(), list_schema("CSIDriver"));
        schemas.insert("CSINodeList".into(), list_schema("CSINode"));
        schemas.insert(
            "CSIStorageCapacityList".into(),
            list_schema("CSIStorageCapacity"),
        );
        schemas.insert("StorageClassList".into(), list_schema("StorageClass"));
        schemas.insert(
            "VolumeAttachmentList".into(),
            list_schema("VolumeAttachment"),
        );
        schemas.insert(
            "VolumeAttributesClassList".into(),
            list_schema("VolumeAttributesClass"),
        );

        // autoscaling/v1 and v2 — both files declare HorizontalPodAutoscalerList
        // with the same shape (metadata, repeated HorizontalPodAutoscaler items).
        schemas.insert(
            "HorizontalPodAutoscalerList".into(),
            list_schema("HorizontalPodAutoscaler"),
        );

        // discovery/v1
        schemas.insert("EndpointSliceList".into(), list_schema("EndpointSlice"));

        // admissionregistration/v1
        schemas.insert(
            "MutatingWebhookConfigurationList".into(),
            list_schema("MutatingWebhookConfiguration"),
        );
        schemas.insert(
            "ValidatingAdmissionPolicyBindingList".into(),
            list_schema("ValidatingAdmissionPolicyBinding"),
        );
        schemas.insert(
            "ValidatingAdmissionPolicyList".into(),
            list_schema("ValidatingAdmissionPolicy"),
        );
        schemas.insert(
            "ValidatingWebhookConfigurationList".into(),
            list_schema("ValidatingWebhookConfiguration"),
        );

        // coordination/v1
        schemas.insert("LeaseList".into(), list_schema("Lease"));

        // apiextensions/v1
        schemas.insert(
            "CustomResourceDefinitionList".into(),
            list_schema("CustomResourceDefinition"),
        );

        // kube-aggregator/v1
        schemas.insert("APIServiceList".into(), list_schema("APIService"));
    }

    /// Register apimachinery types that ride alongside the resource schemas:
    /// runtime envelopes (`RawExtension`, `Unknown`), the quantity/intstr
    /// scalar wrappers, the discovery API (`APIGroup`/`APIResource`/...),
    /// the per-verb request-side `*Options`, and the small group/version
    /// identity messages used in error replies.
    ///
    /// Field numbers from `k8s.io/apimachinery/pkg/...` (release-1.35).
    fn register_apimachinery_extras(schemas: &mut HashMap<String, MessageSchema>) {
        // runtime/RawExtension — wire shape used by `WatchEvent.object`,
        // CRD conversion review bodies, and the meta/v1.List items array.
        // Single `raw` bytes field carries the nested serialized object.
        schemas.insert(
            "RawExtension".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("raw".into(), FieldType::Bytes))]),
            },
        );

        // runtime/Unknown — the envelope every K8s protobuf payload is wrapped
        // in (`k8s\0` + proto-encoded Unknown). Field order matches the proto
        // header: typeMeta (1), raw (2), contentEncoding (3), contentType (4).
        schemas.insert(
            "Unknown".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("typeMeta".into(), FieldType::Message("TypeMeta".into())),
                    ),
                    (2, ("raw".into(), FieldType::Bytes)),
                    (3, ("contentEncoding".into(), FieldType::String)),
                    (4, ("contentType".into(), FieldType::String)),
                ]),
            },
        );

        // api/resource/Quantity — single string field that holds the canonical
        // form (e.g. "100m", "32Mi"). `QuantityValue` is the by-value sibling
        // with the same wire layout. The registry's `FieldType::Quantity`
        // short-circuits decoding for nested usages; the schema entry exists
        // so the registry/upstream-parity dashboard accounts for them.
        schemas.insert(
            "Quantity".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("string".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "QuantityValue".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("string".into(), FieldType::String))]),
            },
        );

        // util/intstr/IntOrString — packs an int32 or string into one field.
        // `type` is the discriminator (0=int, 1=string). The proto wire layout
        // is what the `FieldType::IntOrString` decoder consumes inline.
        schemas.insert(
            "IntOrString".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::Int)),
                    (2, ("intVal".into(), FieldType::Int)),
                    (3, ("strVal".into(), FieldType::String)),
                ]),
            },
        );

        // ---------- discovery API ----------

        // APIGroup — entry in `/apis` showing a single group's versions.
        schemas.insert(
            "APIGroup".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        (
                            "versions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "GroupVersionForDiscovery".into(),
                            ))),
                        ),
                    ),
                    (
                        3,
                        (
                            "preferredVersion".into(),
                            FieldType::Message("GroupVersionForDiscovery".into()),
                        ),
                    ),
                    (
                        4,
                        (
                            "serverAddressByClientCIDRs".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ServerAddressByClientCIDR".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // APIGroupList — top-level discovery body at `/apis`.
        schemas.insert(
            "APIGroupList".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "groups".into(),
                        FieldType::Repeated(Box::new(FieldType::Message("APIGroup".into()))),
                    ),
                )]),
            },
        );

        // APIResource — entry in an APIResourceList. Field numbers are *not*
        // contiguous upstream (added incrementally over many releases): 1,6,2,
        // 8,9,3,4,5,7,10.
        schemas.insert(
            "APIResource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (6, ("singularName".into(), FieldType::String)),
                    (2, ("namespaced".into(), FieldType::Bool)),
                    (8, ("group".into(), FieldType::String)),
                    (9, ("version".into(), FieldType::String)),
                    (3, ("kind".into(), FieldType::String)),
                    (4, ("verbs".into(), FieldType::Message("Verbs".into()))),
                    (
                        5,
                        (
                            "shortNames".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        7,
                        (
                            "categories".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (10, ("storageVersionHash".into(), FieldType::String)),
                ]),
            },
        );

        // APIResourceList — body returned by `/apis/<group>/<version>` and
        // the legacy `/api/v1`.
        schemas.insert(
            "APIResourceList".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("groupVersion".into(), FieldType::String)),
                    (
                        2,
                        (
                            "resources".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("APIResource".into()))),
                        ),
                    ),
                ]),
            },
        );

        // APIVersions — body returned by `/api` (legacy core-group discovery).
        schemas.insert(
            "APIVersions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "versions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "serverAddressByClientCIDRs".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ServerAddressByClientCIDR".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // ServerAddressByClientCIDR — value type in APIGroup/APIVersions.
        schemas.insert(
            "ServerAddressByClientCIDR".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("clientCIDR".into(), FieldType::String)),
                    (2, ("serverAddress".into(), FieldType::String)),
                ]),
            },
        );

        // GroupVersionForDiscovery — `{groupVersion, version}` pair embedded
        // in APIGroup.
        schemas.insert(
            "GroupVersionForDiscovery".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("groupVersion".into(), FieldType::String)),
                    (2, ("version".into(), FieldType::String)),
                ]),
            },
        );

        // RootPaths — body returned by `/` (top-level path discovery).
        schemas.insert(
            "RootPaths".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "paths".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                )]),
            },
        );

        // Verbs — opaque newtype around `repeated string`. Used in
        // APIResource.verbs and elsewhere.
        schemas.insert(
            "Verbs".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "items".into(),
                        FieldType::Repeated(Box::new(FieldType::String)),
                    ),
                )]),
            },
        );

        // ---------- request-side *Options ----------

        // ApplyOptions — server-side apply request options.
        schemas.insert(
            "ApplyOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "dryRun".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("force".into(), FieldType::Bool)),
                    (3, ("fieldManager".into(), FieldType::String)),
                ]),
            },
        );

        // CreateOptions — query-side options for POST requests.
        schemas.insert(
            "CreateOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "dryRun".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (3, ("fieldManager".into(), FieldType::String)),
                    (4, ("fieldValidation".into(), FieldType::String)),
                ]),
            },
        );

        // GetOptions — query-side options for GET requests.
        schemas.insert(
            "GetOptions".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("resourceVersion".into(), FieldType::String))]),
            },
        );

        // ListOptions — every list/watch query carries this on the wire.
        schemas.insert(
            "ListOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("labelSelector".into(), FieldType::String)),
                    (2, ("fieldSelector".into(), FieldType::String)),
                    (3, ("watch".into(), FieldType::Bool)),
                    (9, ("allowWatchBookmarks".into(), FieldType::Bool)),
                    (4, ("resourceVersion".into(), FieldType::String)),
                    (10, ("resourceVersionMatch".into(), FieldType::String)),
                    (5, ("timeoutSeconds".into(), FieldType::Int)),
                    (7, ("limit".into(), FieldType::Int)),
                    (8, ("continue".into(), FieldType::String)),
                    (11, ("sendInitialEvents".into(), FieldType::Bool)),
                ]),
            },
        );

        // PatchOptions — query-side options for PATCH requests.
        schemas.insert(
            "PatchOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "dryRun".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("force".into(), FieldType::Bool)),
                    (3, ("fieldManager".into(), FieldType::String)),
                    (4, ("fieldValidation".into(), FieldType::String)),
                ]),
            },
        );

        // UpdateOptions — query-side options for PUT requests.
        schemas.insert(
            "UpdateOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "dryRun".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("fieldManager".into(), FieldType::String)),
                    (3, ("fieldValidation".into(), FieldType::String)),
                ]),
            },
        );

        // TableOptions — used by clients requesting `Table` responses.
        schemas.insert(
            "TableOptions".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("includeObject".into(), FieldType::String))]),
            },
        );

        // ---------- identity / lookup types ----------

        // FieldSelectorRequirement — typed field-selector clause used by
        // ResourceRule and webhook selectors.
        schemas.insert(
            "FieldSelectorRequirement".into(),
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

        // Duration — nanosecond-resolution duration sibling of Time.
        schemas.insert(
            "Duration".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("duration".into(), FieldType::Int))]),
            },
        );

        // GroupKind — `{group, kind}` identifier used in status errors and
        // OwnerReferences.
        schemas.insert(
            "GroupKind".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("group".into(), FieldType::String)),
                    (2, ("kind".into(), FieldType::String)),
                ]),
            },
        );

        // GroupResource — `{group, resource}` identifier; used in StatusDetails
        // and admission requests.
        schemas.insert(
            "GroupResource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("group".into(), FieldType::String)),
                    (2, ("resource".into(), FieldType::String)),
                ]),
            },
        );

        // GroupVersion — `{group, version}` discovery primitive.
        schemas.insert(
            "GroupVersion".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("group".into(), FieldType::String)),
                    (2, ("version".into(), FieldType::String)),
                ]),
            },
        );

        // GroupVersionKind — `{group, version, kind}` identifier.
        schemas.insert(
            "GroupVersionKind".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("group".into(), FieldType::String)),
                    (2, ("version".into(), FieldType::String)),
                    (3, ("kind".into(), FieldType::String)),
                ]),
            },
        );

        // GroupVersionResource — `{group, version, resource}` identifier.
        schemas.insert(
            "GroupVersionResource".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("group".into(), FieldType::String)),
                    (2, ("version".into(), FieldType::String)),
                    (3, ("resource".into(), FieldType::String)),
                ]),
            },
        );

        // PartialObjectMetadata — metadata-only client view used by `--watch`
        // with `as=PartialObjectMetadata` and by garbage-collector lookups.
        schemas.insert(
            "PartialObjectMetadata".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                )]),
            },
        );

        // PartialObjectMetadataList — paginated wrapper around
        // PartialObjectMetadata items.
        schemas.insert(
            "PartialObjectMetadataList".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ListMeta".into())),
                    ),
                    (
                        2,
                        (
                            "items".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PartialObjectMetadata".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // Timestamp — google.protobuf.Timestamp-shaped (seconds+nanos), but
        // K8s carries its own copy. Distinct from `Time`/`MicroTime` only in
        // that its nanos slot is int32.
        schemas.insert(
            "Timestamp".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("seconds".into(), FieldType::Int)),
                    (2, ("nanos".into(), FieldType::Int)),
                ]),
            },
        );

        // WatchEvent — single envelope every watch frame is wrapped in.
        // `object` is a RawExtension whose `raw` bytes carry the per-event
        // resource payload.
        schemas.insert(
            "WatchEvent".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (
                        2,
                        ("object".into(), FieldType::Message("RawExtension".into())),
                    ),
                ]),
            },
        );
    }

    /// Register core/v1 subresource request shapes (exec/attach/log/portforward/
    /// proxy) and the rarely-used scheduling helpers `AvoidPods`, `PodSignature`,
    /// `PreferAvoidPodsEntry`, `SerializedReference`. These are not stored as
    /// resources but appear on the wire as request bodies / `Node.annotations`
    /// entries.
    ///
    /// Field numbers from `k8s.io/api/core/v1/generated.proto` (release-1.35).
    fn register_core_v1_subresource_options(schemas: &mut HashMap<String, MessageSchema>) {
        // AvoidPods — historical Node annotation payload listing pods that
        // should avoid the node. Carried inside `scheduler.alpha.kubernetes.io/
        // preferAvoidPods` as JSON. Still defined in the proto schema.
        schemas.insert(
            "AvoidPods".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "preferAvoidPods".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "PreferAvoidPodsEntry".into(),
                        ))),
                    ),
                )]),
            },
        );

        // PreferAvoidPodsEntry — one item in `AvoidPods.preferAvoidPods`.
        schemas.insert(
            "PreferAvoidPodsEntry".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "podSignature".into(),
                            FieldType::Message("PodSignature".into()),
                        ),
                    ),
                    (
                        2,
                        ("evictionTime".into(), FieldType::Message("Time".into())),
                    ),
                    (3, ("reason".into(), FieldType::String)),
                    (4, ("message".into(), FieldType::String)),
                ]),
            },
        );

        // PodSignature — owner reference of the pod class to avoid.
        schemas.insert(
            "PodSignature".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "podController".into(),
                        FieldType::Message("OwnerReference".into()),
                    ),
                )]),
            },
        );

        // SerializedReference — payload of legacy event `involvedObject`
        // serialization. Still part of the upstream proto.
        schemas.insert(
            "SerializedReference".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "reference".into(),
                        FieldType::Message("ObjectReference".into()),
                    ),
                )]),
            },
        );

        // NodeProxyOptions / PodProxyOptions / ServiceProxyOptions — each is
        // a request body for the proxy subresource that carries just the
        // forwarded URL path.
        schemas.insert(
            "NodeProxyOptions".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("path".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "PodProxyOptions".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("path".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "ServiceProxyOptions".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("path".into(), FieldType::String))]),
            },
        );

        // PodAttachOptions — POST body for `/pods/{name}/attach`.
        schemas.insert(
            "PodAttachOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("stdin".into(), FieldType::Bool)),
                    (2, ("stdout".into(), FieldType::Bool)),
                    (3, ("stderr".into(), FieldType::Bool)),
                    (4, ("tty".into(), FieldType::Bool)),
                    (5, ("container".into(), FieldType::String)),
                ]),
            },
        );

        // PodExecOptions — POST body for `/pods/{name}/exec`.
        schemas.insert(
            "PodExecOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("stdin".into(), FieldType::Bool)),
                    (2, ("stdout".into(), FieldType::Bool)),
                    (3, ("stderr".into(), FieldType::Bool)),
                    (4, ("tty".into(), FieldType::Bool)),
                    (5, ("container".into(), FieldType::String)),
                    (
                        6,
                        (
                            "command".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );

        // PodLogOptions — GET query params for `/pods/{name}/log` rendered as
        // a proto body when clients use a wrapped client.
        schemas.insert(
            "PodLogOptions".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("container".into(), FieldType::String)),
                    (2, ("follow".into(), FieldType::Bool)),
                    (3, ("previous".into(), FieldType::Bool)),
                    (4, ("sinceSeconds".into(), FieldType::Int)),
                    (5, ("sinceTime".into(), FieldType::Message("Time".into()))),
                    (6, ("timestamps".into(), FieldType::Bool)),
                    (7, ("tailLines".into(), FieldType::Int)),
                    (8, ("limitBytes".into(), FieldType::Int)),
                    (9, ("insecureSkipTLSVerifyBackend".into(), FieldType::Bool)),
                    (10, ("stream".into(), FieldType::String)),
                ]),
            },
        );

        // PodPortForwardOptions — POST body for `/pods/{name}/portforward`.
        schemas.insert(
            "PodPortForwardOptions".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "ports".into(),
                        FieldType::Repeated(Box::new(FieldType::Int)),
                    ),
                )]),
            },
        );
    }

    /// Register autoscaling/v1 `Scale`, `ScaleSpec`, and `ScaleStatus`. These
    /// are not autoscaler resources themselves but the body of every
    /// `/scale` subresource — clients GET/PUT/PATCH them against Deployments,
    /// ReplicaSets, StatefulSets, and ReplicationControllers.
    ///
    /// Field numbers from `k8s.io/api/autoscaling/v1/generated.proto`
    /// (release-1.35).
    fn register_autoscaling_v1_scale(schemas: &mut HashMap<String, MessageSchema>) {
        schemas.insert(
            "Scale".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("spec".into(), FieldType::Message("ScaleSpec".into()))),
                    (
                        3,
                        ("status".into(), FieldType::Message("ScaleStatus".into())),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ScaleSpec".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("replicas".into(), FieldType::Int))]),
            },
        );
        schemas.insert(
            "ScaleStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("replicas".into(), FieldType::Int)),
                    (2, ("selector".into(), FieldType::String)),
                ]),
            },
        );
    }

    /// Register apiextensions/v1 CRD conversion-webhook envelope messages.
    /// `ConversionReview` is the body POSTed to a CRD's conversion webhook
    /// and the response payload it returns; it wraps a `ConversionRequest`
    /// (objects to convert) and `ConversionResponse` (converted objects +
    /// status).
    ///
    /// Field numbers from
    /// `k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/generated.proto`
    /// (release-1.35).
    fn register_apiextensions_v1_conversion(schemas: &mut HashMap<String, MessageSchema>) {
        schemas.insert(
            "ConversionRequest".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("uid".into(), FieldType::String)),
                    (2, ("desiredAPIVersion".into(), FieldType::String)),
                    (
                        3,
                        (
                            "objects".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "RawExtension".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ConversionResponse".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("uid".into(), FieldType::String)),
                    (
                        2,
                        (
                            "convertedObjects".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "RawExtension".into(),
                            ))),
                        ),
                    ),
                    (3, ("result".into(), FieldType::Message("Status".into()))),
                ]),
            },
        );
        schemas.insert(
            "ConversionReview".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "request".into(),
                            FieldType::Message("ConversionRequest".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "response".into(),
                            FieldType::Message("ConversionResponse".into()),
                        ),
                    ),
                ]),
            },
        );
    }

    /// Register `resource.k8s.io/v1` (Dynamic Resource Allocation) message
    /// schemas.
    ///
    /// Field numbers from `k8s.io/api/resource/v1/generated.proto`
    /// (upstream Kubernetes release-1.36; the v1 GA group was introduced
    /// after v1alpha3 / v1beta1 / v1beta2 in earlier releases). The bundled
    /// 1.35 proto snapshot under `crates/api-server/proto/upstream/v1.35/`
    /// predates the v1 GA promotion, so the upstream module path
    /// `k8s.io/api/resource/v1` is the source of truth for these field
    /// numbers (cross-checked against upstream HEAD).
    ///
    /// The four top-level kinds collide on bare name with unrelated types:
    ///
    /// - `ResourceClaim` already exists as the `core/v1.ResourceClaim`
    ///   PodSpec sub-message (`{ name, request }`).
    /// - The other three top-level kinds happen not to collide today, but
    ///   are nevertheless registered under group-qualified keys to keep the
    ///   pattern uniform and to prevent silent shadowing if future
    ///   refactors introduce same-name types in another group.
    ///
    /// Nested message types (DeviceRequest, DeviceClaim, ExactDeviceRequest,
    /// AllocationResult, DeviceAllocationResult, DeviceRequestAllocationResult,
    /// ResourcePool, DeviceClassSpec, DeviceClassConfiguration,
    /// DeviceConfiguration, OpaqueDeviceConfiguration, CELDeviceSelector,
    /// DeviceSelector, Device, ResourceClaimSpec, ResourceClaimStatus,
    /// ResourceClaimTemplateSpec, ResourceSliceSpec) are registered under
    /// their bare names because they are uniquely defined within DRA — the
    /// recursive `decode_message` path uses the bare lookup for nested
    /// fields.
    ///
    /// `DeviceClassConfiguration` inlines `DeviceConfiguration` per the
    /// upstream JSON tag (`json:",inline"`), so its only field is modelled
    /// with `FieldType::InlineMessage` and the decoder merges
    /// `DeviceConfiguration`'s fields (currently just `opaque`) into the
    /// parent JSON object.
    ///
    /// `OpaqueDeviceConfiguration.parameters` is a `runtime.RawExtension`,
    /// modelled with `FieldType::JsonRaw` so the inner JSON body parses
    /// rather than surfacing as base64-encoded bytes.
    fn register_resource_v1(schemas: &mut HashMap<String, MessageSchema>) {
        // ========== Top-level kinds (group-qualified keys) ==========

        // ResourceClaim — the bare name is taken by core/v1.ResourceClaim
        // (PodSpec sub-message). DRA's top-level kind must live behind the
        // group-qualified key so `decode_k8s_resource`'s apiVersion-aware
        // lookup picks the right schema.
        schemas.insert(
            "resource.k8s.io/v1.ResourceClaim".into(),
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
                            FieldType::Message("ResourceClaimSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("ResourceClaimStatus".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "resource.k8s.io/v1.ResourceClaimTemplate".into(),
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
                            FieldType::Message("ResourceClaimTemplateSpec".into()),
                        ),
                    ),
                ]),
            },
        );

        schemas.insert(
            "resource.k8s.io/v1.DeviceClass".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("DeviceClassSpec".into())),
                    ),
                ]),
            },
        );

        schemas.insert(
            "resource.k8s.io/v1.ResourceSlice".into(),
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
                            FieldType::Message("ResourceSliceSpec".into()),
                        ),
                    ),
                ]),
            },
        );

        // ========== Nested message types (bare names) ==========

        // ResourceClaimSpec { devices=1 (DeviceClaim) }
        schemas.insert(
            "ResourceClaimSpec".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    ("devices".into(), FieldType::Message("DeviceClaim".into())),
                )]),
            },
        );

        // ResourceClaimStatus { allocation=1, reservedFor=2 (repeated),
        //                       devices=4 (repeated AllocatedDeviceStatus) }
        schemas.insert(
            "ResourceClaimStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "allocation".into(),
                            FieldType::Message("AllocationResult".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "reservedFor".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ResourceClaimConsumerReference".into(),
                            ))),
                        ),
                    ),
                    (
                        4,
                        (
                            "devices".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "AllocatedDeviceStatus".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // ResourceClaimConsumerReference { apiGroup=1, resource=3,
        //                                  name=4, uid=5 }
        schemas.insert(
            "ResourceClaimConsumerReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("apiGroup".into(), FieldType::String)),
                    (3, ("resource".into(), FieldType::String)),
                    (4, ("name".into(), FieldType::String)),
                    (5, ("uid".into(), FieldType::String)),
                ]),
            },
        );

        // ResourceClaimTemplateSpec { metadata=1 (ObjectMeta),
        //                             spec=2 (ResourceClaimSpec) }
        schemas.insert(
            "ResourceClaimTemplateSpec".into(),
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
                            FieldType::Message("ResourceClaimSpec".into()),
                        ),
                    ),
                ]),
            },
        );

        // DeviceClaim { requests=1, constraints=2, config=3 }
        schemas.insert(
            "DeviceClaim".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "requests".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceRequest".into(),
                            ))),
                        ),
                    ),
                    (
                        2,
                        (
                            "constraints".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceConstraint".into(),
                            ))),
                        ),
                    ),
                    (
                        3,
                        (
                            "config".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceClaimConfiguration".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // DeviceRequest { name=1, exactly=2, firstAvailable=3 }
        schemas.insert(
            "DeviceRequest".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        (
                            "exactly".into(),
                            FieldType::Message("ExactDeviceRequest".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "firstAvailable".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceSubRequest".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // ExactDeviceRequest { deviceClassName=1, selectors=2 (repeated),
        //                      allocationMode=3, count=4, adminAccess=5,
        //                      tolerations=6, capacity=7 }
        schemas.insert(
            "ExactDeviceRequest".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("deviceClassName".into(), FieldType::String)),
                    (
                        2,
                        (
                            "selectors".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceSelector".into(),
                            ))),
                        ),
                    ),
                    (3, ("allocationMode".into(), FieldType::String)),
                    (4, ("count".into(), FieldType::Int)),
                    (5, ("adminAccess".into(), FieldType::Bool)),
                    (
                        6,
                        (
                            "tolerations".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceToleration".into(),
                            ))),
                        ),
                    ),
                    (
                        7,
                        (
                            "capacity".into(),
                            FieldType::Message("CapacityRequirements".into()),
                        ),
                    ),
                ]),
            },
        );

        // DeviceSubRequest { name=1, deviceClassName=2, selectors=3,
        //                    allocationMode=4, count=5, tolerations=6,
        //                    capacity=7 }
        schemas.insert(
            "DeviceSubRequest".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("deviceClassName".into(), FieldType::String)),
                    (
                        3,
                        (
                            "selectors".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceSelector".into(),
                            ))),
                        ),
                    ),
                    (4, ("allocationMode".into(), FieldType::String)),
                    (5, ("count".into(), FieldType::Int)),
                    (
                        6,
                        (
                            "tolerations".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceToleration".into(),
                            ))),
                        ),
                    ),
                    (
                        7,
                        (
                            "capacity".into(),
                            FieldType::Message("CapacityRequirements".into()),
                        ),
                    ),
                ]),
            },
        );

        // DeviceSelector { cel=1 }
        schemas.insert(
            "DeviceSelector".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    ("cel".into(), FieldType::Message("CELDeviceSelector".into())),
                )]),
            },
        );

        // CELDeviceSelector { expression=1 }
        schemas.insert(
            "CELDeviceSelector".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("expression".into(), FieldType::String))]),
            },
        );

        // DeviceConstraint { requests=1 (repeated), matchAttribute=2,
        //                    distinctAttribute=3 }
        schemas.insert(
            "DeviceConstraint".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "requests".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("matchAttribute".into(), FieldType::String)),
                    (3, ("distinctAttribute".into(), FieldType::String)),
                ]),
            },
        );

        // DeviceClaimConfiguration { requests=1 (repeated string),
        //                            deviceConfiguration=2 (inline) }
        schemas.insert(
            "DeviceClaimConfiguration".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "requests".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        2,
                        (
                            "deviceConfiguration".into(),
                            FieldType::InlineMessage("DeviceConfiguration".into()),
                        ),
                    ),
                ]),
            },
        );

        // DeviceToleration { key=1, operator=2, value=3, effect=4,
        //                    tolerationSeconds=5 }
        schemas.insert(
            "DeviceToleration".into(),
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

        // CapacityRequirements { requests=1 (map<string, Quantity>) }
        schemas.insert(
            "CapacityRequirements".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("requests".into(), FieldType::QuantityMap))]),
            },
        );

        // ========== AllocationResult chain ==========

        // AllocationResult { devices=1, nodeSelector=3 (NodeSelector),
        //                    allocationTimestamp=5 (Time) }
        schemas.insert(
            "AllocationResult".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "devices".into(),
                            FieldType::Message("DeviceAllocationResult".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "nodeSelector".into(),
                            FieldType::Message("NodeSelector".into()),
                        ),
                    ),
                    (
                        5,
                        (
                            "allocationTimestamp".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                ]),
            },
        );

        // DeviceAllocationResult { results=1 (repeated), config=2 (repeated) }
        schemas.insert(
            "DeviceAllocationResult".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "results".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceRequestAllocationResult".into(),
                            ))),
                        ),
                    ),
                    (
                        2,
                        (
                            "config".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceAllocationConfiguration".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );

        // DeviceRequestAllocationResult { request=1, driver=2, pool=3,
        //                                 device=4, adminAccess=5,
        //                                 tolerations=6, bindingConditions=7,
        //                                 bindingFailureConditions=8,
        //                                 shareID=9, consumedCapacity=10 }
        schemas.insert(
            "DeviceRequestAllocationResult".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("request".into(), FieldType::String)),
                    (2, ("driver".into(), FieldType::String)),
                    (3, ("pool".into(), FieldType::String)),
                    (4, ("device".into(), FieldType::String)),
                    (5, ("adminAccess".into(), FieldType::Bool)),
                    (
                        6,
                        (
                            "tolerations".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceToleration".into(),
                            ))),
                        ),
                    ),
                    (
                        7,
                        (
                            "bindingConditions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        8,
                        (
                            "bindingFailureConditions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (9, ("shareID".into(), FieldType::String)),
                    (10, ("consumedCapacity".into(), FieldType::QuantityMap)),
                ]),
            },
        );

        // DeviceAllocationConfiguration { source=1, requests=2 (repeated),
        //                                 deviceConfiguration=3 (inline) }
        schemas.insert(
            "DeviceAllocationConfiguration".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("source".into(), FieldType::String)),
                    (
                        2,
                        (
                            "requests".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        3,
                        (
                            "deviceConfiguration".into(),
                            FieldType::InlineMessage("DeviceConfiguration".into()),
                        ),
                    ),
                ]),
            },
        );

        // AllocatedDeviceStatus { driver=1, pool=2, device=3,
        //                         conditions=4 (repeated Condition),
        //                         data=5 (RawExtension/JsonRaw),
        //                         networkData=6 (NetworkDeviceData),
        //                         shareID=7 }
        schemas.insert(
            "AllocatedDeviceStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("driver".into(), FieldType::String)),
                    (2, ("pool".into(), FieldType::String)),
                    (3, ("device".into(), FieldType::String)),
                    (
                        4,
                        (
                            "conditions".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("Condition".into()))),
                        ),
                    ),
                    (5, ("data".into(), FieldType::JsonRaw)),
                    (
                        6,
                        (
                            "networkData".into(),
                            FieldType::Message("NetworkDeviceData".into()),
                        ),
                    ),
                    (7, ("shareID".into(), FieldType::String)),
                ]),
            },
        );

        // NetworkDeviceData { interfaceName=1, ips=2 (repeated),
        //                     hardwareAddress=3 }
        schemas.insert(
            "NetworkDeviceData".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("interfaceName".into(), FieldType::String)),
                    (
                        2,
                        (
                            "ips".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (3, ("hardwareAddress".into(), FieldType::String)),
                ]),
            },
        );

        // ========== DeviceClass chain ==========

        // DeviceClassSpec { selectors=1 (repeated DeviceSelector),
        //                   config=2 (repeated DeviceClassConfiguration),
        //                   extendedResourceName=4 }
        schemas.insert(
            "DeviceClassSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "selectors".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceSelector".into(),
                            ))),
                        ),
                    ),
                    (
                        2,
                        (
                            "config".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceClassConfiguration".into(),
                            ))),
                        ),
                    ),
                    (4, ("extendedResourceName".into(), FieldType::String)),
                ]),
            },
        );

        // DeviceClassConfiguration { deviceConfiguration=1 (inline) }
        // Per upstream `json:",inline"`, DeviceConfiguration's fields are
        // hoisted into the DeviceClassConfiguration JSON object.
        schemas.insert(
            "DeviceClassConfiguration".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "deviceConfiguration".into(),
                        FieldType::InlineMessage("DeviceConfiguration".into()),
                    ),
                )]),
            },
        );

        // DeviceConfiguration { opaque=1 }
        schemas.insert(
            "DeviceConfiguration".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "opaque".into(),
                        FieldType::Message("OpaqueDeviceConfiguration".into()),
                    ),
                )]),
            },
        );

        // OpaqueDeviceConfiguration { driver=1, parameters=2 (RawExtension)
        // Parameters is a runtime.RawExtension carrying arbitrary JSON; use
        // FieldType::JsonRaw so it surfaces as a parsed JSON value rather
        // than base64-encoded bytes.
        schemas.insert(
            "OpaqueDeviceConfiguration".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("driver".into(), FieldType::String)),
                    (2, ("parameters".into(), FieldType::JsonRaw)),
                ]),
            },
        );

        // ========== ResourceSlice chain ==========

        // ResourceSliceSpec { driver=1, pool=2, nodeName=3, nodeSelector=4,
        //                     allNodes=5, devices=6 (repeated Device),
        //                     perDeviceNodeSelection=7,
        //                     sharedCounters=8 (repeated CounterSet) }
        schemas.insert(
            "ResourceSliceSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("driver".into(), FieldType::String)),
                    (
                        2,
                        ("pool".into(), FieldType::Message("ResourcePool".into())),
                    ),
                    (3, ("nodeName".into(), FieldType::String)),
                    (
                        4,
                        (
                            "nodeSelector".into(),
                            FieldType::Message("NodeSelector".into()),
                        ),
                    ),
                    (5, ("allNodes".into(), FieldType::Bool)),
                    (
                        6,
                        (
                            "devices".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("Device".into()))),
                        ),
                    ),
                    (7, ("perDeviceNodeSelection".into(), FieldType::Bool)),
                    (
                        8,
                        (
                            "sharedCounters".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("CounterSet".into()))),
                        ),
                    ),
                ]),
            },
        );

        // ResourcePool { name=1, generation=2, resourceSliceCount=3 }
        schemas.insert(
            "ResourcePool".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (2, ("generation".into(), FieldType::Int)),
                    (3, ("resourceSliceCount".into(), FieldType::Int)),
                ]),
            },
        );

        // Device { name=1, attributes=2 (map<string, DeviceAttribute>),
        //          capacity=3 (map<string, DeviceCapacity>),
        //          consumesCounters=4 (repeated DeviceCounterConsumption),
        //          nodeName=5, nodeSelector=6, allNodes=7,
        //          taints=8 (repeated DeviceTaint), bindsToNode=9,
        //          bindingConditions=10 (repeated string),
        //          bindingFailureConditions=11 (repeated string),
        //          allowMultipleAllocations=12,
        //          nodeAllocatableResourceMappings=13 (map<string, ...>) }
        schemas.insert(
            "Device".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        (
                            "attributes".into(),
                            FieldType::MessageMap("DeviceAttribute".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "capacity".into(),
                            FieldType::MessageMap("DeviceCapacity".into()),
                        ),
                    ),
                    (
                        4,
                        (
                            "consumesCounters".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "DeviceCounterConsumption".into(),
                            ))),
                        ),
                    ),
                    (5, ("nodeName".into(), FieldType::String)),
                    (
                        6,
                        (
                            "nodeSelector".into(),
                            FieldType::Message("NodeSelector".into()),
                        ),
                    ),
                    (7, ("allNodes".into(), FieldType::Bool)),
                    (
                        8,
                        (
                            "taints".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("DeviceTaint".into()))),
                        ),
                    ),
                    (9, ("bindsToNode".into(), FieldType::Bool)),
                    (
                        10,
                        (
                            "bindingConditions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        11,
                        (
                            "bindingFailureConditions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (12, ("allowMultipleAllocations".into(), FieldType::Bool)),
                    (
                        13,
                        (
                            "nodeAllocatableResourceMappings".into(),
                            FieldType::MessageMap("NodeAllocatableResourceMapping".into()),
                        ),
                    ),
                ]),
            },
        );

        // DeviceAttribute (one-of) { int=2, bool=3, string=4, version=5,
        //                            ints=6, bools=7, strings=8, versions=9 }
        schemas.insert(
            "DeviceAttribute".into(),
            MessageSchema {
                fields: HashMap::from([
                    (2, ("int".into(), FieldType::Int)),
                    (3, ("bool".into(), FieldType::Bool)),
                    (4, ("string".into(), FieldType::String)),
                    (5, ("version".into(), FieldType::String)),
                    (
                        6,
                        ("ints".into(), FieldType::Repeated(Box::new(FieldType::Int))),
                    ),
                    (
                        7,
                        (
                            "bools".into(),
                            FieldType::Repeated(Box::new(FieldType::Bool)),
                        ),
                    ),
                    (
                        8,
                        (
                            "strings".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (
                        9,
                        (
                            "versions".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );

        // DeviceCapacity { value=1 (Quantity), requestPolicy=2 }
        schemas.insert(
            "DeviceCapacity".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("value".into(), FieldType::Quantity)),
                    (
                        2,
                        (
                            "requestPolicy".into(),
                            FieldType::Message("CapacityRequestPolicy".into()),
                        ),
                    ),
                ]),
            },
        );

        // CapacityRequestPolicy { default=1 (Quantity),
        //                         validValues=3 (repeated Quantity),
        //                         validRange=4 }
        schemas.insert(
            "CapacityRequestPolicy".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("default".into(), FieldType::Quantity)),
                    (
                        3,
                        (
                            "validValues".into(),
                            FieldType::Repeated(Box::new(FieldType::Quantity)),
                        ),
                    ),
                    (
                        4,
                        (
                            "validRange".into(),
                            FieldType::Message("CapacityRequestPolicyRange".into()),
                        ),
                    ),
                ]),
            },
        );

        // CapacityRequestPolicyRange { min=1, max=2, step=3 } (all Quantity)
        schemas.insert(
            "CapacityRequestPolicyRange".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("min".into(), FieldType::Quantity)),
                    (2, ("max".into(), FieldType::Quantity)),
                    (3, ("step".into(), FieldType::Quantity)),
                ]),
            },
        );

        // CounterSet { name=1, counters=2 (map<string, Counter>) }
        schemas.insert(
            "CounterSet".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("name".into(), FieldType::String)),
                    (
                        2,
                        ("counters".into(), FieldType::MessageMap("Counter".into())),
                    ),
                ]),
            },
        );

        // Counter { value=1 (Quantity) }
        schemas.insert(
            "Counter".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("value".into(), FieldType::Quantity))]),
            },
        );

        // DeviceCounterConsumption { counterSet=1, counters=2 (map) }
        schemas.insert(
            "DeviceCounterConsumption".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("counterSet".into(), FieldType::String)),
                    (
                        2,
                        ("counters".into(), FieldType::MessageMap("Counter".into())),
                    ),
                ]),
            },
        );

        // DeviceTaint { key=1, value=2, effect=3, timeAdded=4 (Time) }
        schemas.insert(
            "DeviceTaint".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("key".into(), FieldType::String)),
                    (2, ("value".into(), FieldType::String)),
                    (3, ("effect".into(), FieldType::String)),
                    (4, ("timeAdded".into(), FieldType::Message("Time".into()))),
                ]),
            },
        );

        // NodeAllocatableResourceMapping { capacityKey=1,
        //                                  allocationMultiplier=2 (Quantity) }
        schemas.insert(
            "NodeAllocatableResourceMapping".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("capacityKey".into(), FieldType::String)),
                    (2, ("allocationMultiplier".into(), FieldType::Quantity)),
                ]),
            },
        );
    }

    /// `flowcontrol.apiserver.k8s.io/v1` — FlowSchema and
    /// PriorityLevelConfiguration plus their nested shapes. Field numbers come
    /// from upstream `k8s.io/api/flowcontrol/v1/generated.proto`. These are the
    /// API-priority-and-fairness kinds the conformance suite POSTs as native
    /// protobuf; without these schemas the request decoder dropped `spec` and
    /// the create failed with a 422.
    fn register_flowcontrol_v1(schemas: &mut HashMap<String, MessageSchema>) {
        // ----- FlowSchema -----
        schemas.insert(
            "FlowSchema".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("FlowSchemaSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("FlowSchemaStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "FlowSchemaSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "priorityLevelConfiguration".into(),
                            FieldType::Message("PriorityLevelConfigurationReference".into()),
                        ),
                    ),
                    (2, ("matchingPrecedence".into(), FieldType::Int)),
                    (
                        3,
                        (
                            "distinguisherMethod".into(),
                            FieldType::Message("FlowDistinguisherMethod".into()),
                        ),
                    ),
                    (
                        4,
                        (
                            "rules".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "PolicyRulesWithSubjects".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PriorityLevelConfigurationReference".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("name".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "FlowDistinguisherMethod".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("type".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "PolicyRulesWithSubjects".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "subjects".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "FlowSchemaSubject".into(),
                            ))),
                        ),
                    ),
                    (
                        2,
                        (
                            "resourceRules".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "ResourcePolicyRule".into(),
                            ))),
                        ),
                    ),
                    (
                        3,
                        (
                            "nonResourceRules".into(),
                            FieldType::Repeated(Box::new(FieldType::Message(
                                "NonResourcePolicyRule".into(),
                            ))),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "FlowSchemaSubject".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("kind".into(), FieldType::String)),
                    (2, ("user".into(), FieldType::Message("UserSubject".into()))),
                    (
                        3,
                        ("group".into(), FieldType::Message("GroupSubject".into())),
                    ),
                    (
                        4,
                        (
                            "serviceAccount".into(),
                            FieldType::Message("ServiceAccountSubject".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "UserSubject".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("name".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "GroupSubject".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("name".into(), FieldType::String))]),
            },
        );
        schemas.insert(
            "ServiceAccountSubject".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("namespace".into(), FieldType::String)),
                    (2, ("name".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "ResourcePolicyRule".into(),
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
                    (4, ("clusterScope".into(), FieldType::Bool)),
                    (
                        5,
                        (
                            "namespaces".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "NonResourcePolicyRule".into(),
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
                            "nonResourceURLs".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "FlowSchemaStatus".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "conditions".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "FlowSchemaCondition".into(),
                        ))),
                    ),
                )]),
            },
        );
        schemas.insert(
            "FlowSchemaCondition".into(),
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

        // ----- PriorityLevelConfiguration -----
        schemas.insert(
            "PriorityLevelConfiguration".into(),
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
                            FieldType::Message("PriorityLevelConfigurationSpec".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("PriorityLevelConfigurationStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "PriorityLevelConfigurationSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (
                        2,
                        (
                            "limited".into(),
                            FieldType::Message("LimitedPriorityLevelConfiguration".into()),
                        ),
                    ),
                    (
                        3,
                        (
                            "exempt".into(),
                            FieldType::Message("ExemptPriorityLevelConfiguration".into()),
                        ),
                    ),
                ]),
            },
        );
        // Upstream LimitedPriorityLevelConfiguration proto:
        //   1=nominalConcurrencyShares, 2=limitResponse, 3=lendingLimit,
        //   4=borrowingLimitPercent. Our resource struct names the lending
        //   field `lendingConcurrencyLimit`, so field 3 maps to that JSON key.
        schemas.insert(
            "LimitedPriorityLevelConfiguration".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("nominalConcurrencyShares".into(), FieldType::Int)),
                    (
                        2,
                        (
                            "limitResponse".into(),
                            FieldType::Message("LimitResponse".into()),
                        ),
                    ),
                    (3, ("lendingConcurrencyLimit".into(), FieldType::Int)),
                    (4, ("borrowingLimitPercent".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "ExemptPriorityLevelConfiguration".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("nominalConcurrencyShares".into(), FieldType::Int)),
                    (2, ("lendingConcurrencyLimit".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "LimitResponse".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("type".into(), FieldType::String)),
                    (
                        2,
                        (
                            "queuing".into(),
                            FieldType::Message("QueuingConfiguration".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "QueuingConfiguration".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("queues".into(), FieldType::Int)),
                    (2, ("handSize".into(), FieldType::Int)),
                    (3, ("queueLengthLimit".into(), FieldType::Int)),
                ]),
            },
        );
        schemas.insert(
            "PriorityLevelConfigurationStatus".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    (
                        "conditions".into(),
                        FieldType::Repeated(Box::new(FieldType::Message(
                            "PriorityLevelConfigurationCondition".into(),
                        ))),
                    ),
                )]),
            },
        );
        schemas.insert(
            "PriorityLevelConfigurationCondition".into(),
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
    }

    /// `node.k8s.io/v1` — RuntimeClass and its nested Overhead / Scheduling.
    /// Field numbers from `k8s.io/api/node/v1/generated.proto`.
    fn register_node_v1(schemas: &mut HashMap<String, MessageSchema>) {
        schemas.insert(
            "RuntimeClass".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (2, ("handler".into(), FieldType::String)),
                    (
                        3,
                        ("overhead".into(), FieldType::Message("Overhead".into())),
                    ),
                    (
                        4,
                        ("scheduling".into(), FieldType::Message("Scheduling".into())),
                    ),
                ]),
            },
        );
        schemas.insert(
            "Overhead".into(),
            MessageSchema {
                fields: HashMap::from([(1, ("podFixed".into(), FieldType::QuantityMap))]),
            },
        );
        schemas.insert(
            "Scheduling".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("nodeSelector".into(), FieldType::StringMap)),
                    (
                        2,
                        (
                            "tolerations".into(),
                            FieldType::Repeated(Box::new(FieldType::Message("Toleration".into()))),
                        ),
                    ),
                ]),
            },
        );
    }

    /// `authentication.k8s.io/v1` — TokenReview, SelfSubjectReview and shared
    /// UserInfo. Field numbers from `k8s.io/api/authentication/v1/generated.proto`.
    /// TokenRequest is namespaced under the ServiceAccount `token` subresource;
    /// its request body is decoded the same way so it is registered here too.
    /// authorization.k8s.io/v1 review kinds. Field numbers from
    /// staging/src/k8s.io/api/authorization/v1/generated.proto.
    ///
    /// The typed Go client POSTs these over vnd.kubernetes.protobuf; without a
    /// schema the nested `resourceAttributes` message was dropped on decode and
    /// the handler returned 500 "Either resourceAttributes or
    /// nonResourceAttributes must be specified" (the [sig-auth] SubjectReview
    /// conformance test). `spec.extra` (map<string, ExtraValue=repeated string>)
    /// has no matching FieldType variant and is rarely set, so it is omitted —
    /// the decoder simply skips field 5.
    fn register_authorization_v1(schemas: &mut HashMap<String, MessageSchema>) {
        let review_kind = |spec_type: &str| MessageSchema {
            fields: HashMap::from([
                (
                    1,
                    ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                ),
                (2, ("spec".into(), FieldType::Message(spec_type.into()))),
                (
                    3,
                    (
                        "status".into(),
                        FieldType::Message("SubjectAccessReviewStatus".into()),
                    ),
                ),
            ]),
        };
        schemas.insert(
            "SubjectAccessReview".into(),
            review_kind("SubjectAccessReviewSpec"),
        );
        schemas.insert(
            "LocalSubjectAccessReview".into(),
            review_kind("SubjectAccessReviewSpec"),
        );
        schemas.insert(
            "SelfSubjectAccessReview".into(),
            review_kind("SelfSubjectAccessReviewSpec"),
        );

        schemas.insert(
            "SubjectAccessReviewSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "resourceAttributes".into(),
                            FieldType::Message("ResourceAttributes".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "nonResourceAttributes".into(),
                            FieldType::Message("NonResourceAttributes".into()),
                        ),
                    ),
                    (3, ("user".into(), FieldType::String)),
                    (
                        4,
                        (
                            "groups".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (6, ("uid".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "SelfSubjectAccessReviewSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "resourceAttributes".into(),
                            FieldType::Message("ResourceAttributes".into()),
                        ),
                    ),
                    (
                        2,
                        (
                            "nonResourceAttributes".into(),
                            FieldType::Message("NonResourceAttributes".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "ResourceAttributes".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("namespace".into(), FieldType::String)),
                    (2, ("verb".into(), FieldType::String)),
                    (3, ("group".into(), FieldType::String)),
                    (4, ("version".into(), FieldType::String)),
                    (5, ("resource".into(), FieldType::String)),
                    (6, ("subresource".into(), FieldType::String)),
                    (7, ("name".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "NonResourceAttributes".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("path".into(), FieldType::String)),
                    (2, ("verb".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "SubjectAccessReviewStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("allowed".into(), FieldType::Bool)),
                    (2, ("reason".into(), FieldType::String)),
                    (3, ("evaluationError".into(), FieldType::String)),
                    (4, ("denied".into(), FieldType::Bool)),
                ]),
            },
        );
    }

    fn register_authentication_v1(schemas: &mut HashMap<String, MessageSchema>) {
        schemas.insert(
            "TokenReview".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("TokenReviewSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("TokenReviewStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "TokenReviewSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("token".into(), FieldType::String)),
                    (
                        2,
                        (
                            "audiences".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "TokenReviewStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("authenticated".into(), FieldType::Bool)),
                    (2, ("user".into(), FieldType::Message("UserInfo".into()))),
                    (3, ("error".into(), FieldType::String)),
                    (
                        4,
                        (
                            "audiences".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "UserInfo".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("username".into(), FieldType::String)),
                    (2, ("uid".into(), FieldType::String)),
                    (
                        3,
                        (
                            "groups".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    // extra is map<string, ExtraValue> where ExtraValue is a
                    // repeated-string wrapper; omitted from the decode schema
                    // because no conformance write populates it.
                ]),
            },
        );
        schemas.insert(
            "SelfSubjectReview".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        (
                            "status".into(),
                            FieldType::Message("SelfSubjectReviewStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "SelfSubjectReviewStatus".into(),
            MessageSchema {
                fields: HashMap::from([(
                    1,
                    ("userInfo".into(), FieldType::Message("UserInfo".into())),
                )]),
            },
        );
        // TokenRequest (POSTed to the ServiceAccount `token` subresource).
        //   1=metadata, 2=spec, 3=status
        //
        // The bare `TokenRequest` key is already taken by the storage/v1 CSI
        // `TokenRequest` (a {audience, expirationSeconds} pair embedded in
        // CSIDriver.tokenRequests), which is a DIFFERENT type that shares the
        // name. `decode_k8s_resource` prefers a group-qualified
        // `<apiVersion>.<kind>` key, so register this authentication variant
        // under that qualified name to avoid clobbering the CSI schema.
        schemas.insert(
            "authentication.k8s.io/v1.TokenRequest".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        ("metadata".into(), FieldType::Message("ObjectMeta".into())),
                    ),
                    (
                        2,
                        ("spec".into(), FieldType::Message("TokenRequestSpec".into())),
                    ),
                    (
                        3,
                        (
                            "status".into(),
                            FieldType::Message("TokenRequestStatus".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "TokenRequestSpec".into(),
            MessageSchema {
                fields: HashMap::from([
                    (
                        1,
                        (
                            "audiences".into(),
                            FieldType::Repeated(Box::new(FieldType::String)),
                        ),
                    ),
                    (2, ("expirationSeconds".into(), FieldType::Int)),
                    (
                        3,
                        (
                            "boundObjectRef".into(),
                            FieldType::Message("BoundObjectReference".into()),
                        ),
                    ),
                ]),
            },
        );
        schemas.insert(
            "BoundObjectReference".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("kind".into(), FieldType::String)),
                    (2, ("apiVersion".into(), FieldType::String)),
                    (3, ("name".into(), FieldType::String)),
                    (4, ("uid".into(), FieldType::String)),
                ]),
            },
        );
        schemas.insert(
            "TokenRequestStatus".into(),
            MessageSchema {
                fields: HashMap::from([
                    (1, ("token".into(), FieldType::String)),
                    (
                        2,
                        (
                            "expirationTimestamp".into(),
                            FieldType::Message("Time".into()),
                        ),
                    ),
                ]),
            },
        );
    }
}

// ============================================================================
// Native protobuf encoding for `metav1.Status`
// ============================================================================
//
// `IntoResponse for Error` in `crates/common/src/error.rs` builds a typed
// `Status` and serializes it as JSON via `axum::Json<Status>`. When the client
// negotiates `application/vnd.kubernetes.protobuf`, upstream Kubernetes
// returns the same `Status` wrapped in the `k8s\0` + `Unknown` envelope with
// `raw` carrying the native protobuf encoding (NOT JSON-in-protobuf, which is
// what the rest of our middleware does for resource bodies).
//
// The encoder here mirrors the upstream wire layout from
// `staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto`:
//
// ```text
// message Status {
//     optional ListMeta      metadata = 1;
//     optional string        status   = 2;
//     optional string        message  = 3;
//     optional string        reason   = 4;
//     optional StatusDetails details  = 5;
//     optional int32         code     = 6;
// }
// message StatusDetails {
//     optional string name              = 1;
//     optional string group             = 2;
//     optional string kind              = 3;
//     repeated StatusCause causes       = 4;
//     optional int32 retryAfterSeconds  = 5;
//     optional string uid               = 6;
// }
// message StatusCause {
//     optional string reason  = 1;
//     optional string message = 2;
//     optional string field   = 3;
// }
// ```
//
// `Status.metadata` (ListMeta) is intentionally omitted: the upstream Go
// `metav1.Status` value built by `apierrors.New*` helpers carries a
// zero-valued ListMeta, and proto3 message-typed optional fields encode as
// nothing on the wire when the value is the zero value.

/// Encode a [`Status`] to the native K8s protobuf envelope:
/// `k8s\0` magic + `Unknown { typeMeta, raw, contentType="application/vnd.kubernetes.protobuf" }`
/// where `raw` carries the native protobuf encoding of `Status` itself.
///
/// The returned bytes are decodable via `ProtoRegistry::decode_message("Status", &bytes[k8s\0+Unknown.raw offset..])`.
pub fn encode_status_protobuf(status: &rusternetes_common::types::Status) -> Vec<u8> {
    let raw = encode_status_native(status);

    // Wrap in the K8s runtime.Unknown envelope.
    //   field 1 (typeMeta, TypeMeta): nested message { apiVersion, kind }
    //   field 2 (raw, bytes)
    //   field 4 (contentType, string)
    let api_version = status.api_version.as_str();
    let kind = status.kind.as_str();
    let content_type = b"application/vnd.kubernetes.protobuf";

    let mut type_meta = Vec::with_capacity(api_version.len() + kind.len() + 8);
    if !api_version.is_empty() {
        push_string_field(&mut type_meta, 1, api_version.as_bytes());
    }
    if !kind.is_empty() {
        push_string_field(&mut type_meta, 2, kind.as_bytes());
    }

    let mut unknown = Vec::with_capacity(raw.len() + type_meta.len() + content_type.len() + 16);
    if !type_meta.is_empty() {
        push_length_delimited_field(&mut unknown, 1, &type_meta);
    }
    push_length_delimited_field(&mut unknown, 2, &raw);
    push_string_field(&mut unknown, 4, content_type);

    let mut out = Vec::with_capacity(unknown.len() + 4);
    out.extend_from_slice(b"k8s\0");
    out.extend_from_slice(&unknown);
    out
}

/// Encode a `Status` to native protobuf bytes (without the `k8s\0` + Unknown
/// envelope). Public for tests that want to round-trip via
/// `ProtoRegistry::decode_message("Status", …)`.
pub fn encode_status_native(status: &rusternetes_common::types::Status) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);

    if let Some(ref s) = status.status {
        push_string_field(&mut buf, 2, s.as_bytes());
    }
    if let Some(ref m) = status.message {
        push_string_field(&mut buf, 3, m.as_bytes());
    }
    if let Some(ref r) = status.reason {
        push_string_field(&mut buf, 4, r.as_bytes());
    }
    if let Some(ref d) = status.details {
        let inner = encode_status_details_native(d);
        push_length_delimited_field(&mut buf, 5, &inner);
    }
    if let Some(code) = status.code {
        push_varint_field(&mut buf, 6, code as u64);
    }
    buf
}

fn encode_status_details_native(d: &rusternetes_common::types::StatusDetails) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32);
    if let Some(ref name) = d.name {
        push_string_field(&mut buf, 1, name.as_bytes());
    }
    if let Some(ref group) = d.group {
        push_string_field(&mut buf, 2, group.as_bytes());
    }
    if let Some(ref kind) = d.kind {
        push_string_field(&mut buf, 3, kind.as_bytes());
    }
    if let Some(ref causes) = d.causes {
        for cause in causes {
            let inner = encode_status_cause_native(cause);
            push_length_delimited_field(&mut buf, 4, &inner);
        }
    }
    if let Some(retry) = d.retry_after_seconds {
        push_varint_field(&mut buf, 5, retry as u64);
    }
    if let Some(ref uid) = d.uid {
        push_string_field(&mut buf, 6, uid.as_bytes());
    }
    buf
}

fn encode_status_cause_native(c: &rusternetes_common::types::StatusCause) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    if let Some(ref r) = c.reason {
        push_string_field(&mut buf, 1, r.as_bytes());
    }
    if let Some(ref m) = c.message {
        push_string_field(&mut buf, 2, m.as_bytes());
    }
    if let Some(ref f) = c.field {
        push_string_field(&mut buf, 3, f.as_bytes());
    }
    buf
}

// --- low-level wire helpers (encode side) -----------------------------------

fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

fn push_string_field(buf: &mut Vec<u8>, field_number: u32, payload: &[u8]) {
    push_length_delimited_field(buf, field_number, payload);
}

fn push_length_delimited_field(buf: &mut Vec<u8>, field_number: u32, payload: &[u8]) {
    encode_varint(buf, ((field_number as u64) << 3) | 2);
    encode_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

fn push_varint_field(buf: &mut Vec<u8>, field_number: u32, value: u64) {
    encode_varint(buf, (field_number as u64) << 3);
    encode_varint(buf, value);
}

// --- JSON → proto value helpers (encode side) ------------------------------

/// Parse a JSON value into an i64, accepting both numeric and string-encoded
/// integers (resourceVersion arrives from K8s as a string in JSON but is
/// emitted as a varint on the wire — see `ObjectMeta.resourceVersion`).
fn json_to_i64(val: &Value) -> Option<i64> {
    if let Some(i) = val.as_i64() {
        return Some(i);
    }
    if let Some(u) = val.as_u64() {
        return Some(u as i64);
    }
    if let Some(f) = val.as_f64() {
        return Some(f as i64);
    }
    if let Some(s) = val.as_str() {
        return s.parse::<i64>().ok();
    }
    None
}

/// Decode a JSON value that represents proto `bytes` into the raw byte
/// vector. K8s wire format expects base64 in JSON; this helper undoes the
/// base64. Non-string values fall back to UTF-8 bytes.
fn json_bytes_value(val: &Value) -> Vec<u8> {
    use base64::Engine;
    if let Some(s) = val.as_str() {
        // Try standard base64 first (matches the decoder's STANDARD engine).
        if let Ok(b) = base64::engine::general_purpose::STANDARD.decode(s) {
            return b;
        }
        // Fall back to URL-safe (some K8s tooling emits this for `bytes`).
        if let Ok(b) = base64::engine::general_purpose::URL_SAFE.decode(s) {
            return b;
        }
        return s.as_bytes().to_vec();
    }
    Vec::new()
}

/// Encode a JSON timestamp string (RFC3339 — possibly with fractional
/// seconds for MicroTime) into the K8s `Time` / `MicroTime` proto:
///     message Time { int64 seconds = 1; int32 nanos = 2; }
/// Falls back to a zero-length message on parse failure, matching the
/// decoder which emits `Value::Null` for `seconds == 0 && nanos == 0`.
fn encode_timestamp(val: &Value) -> Vec<u8> {
    let Some(s) = val.as_str() else {
        return Vec::new();
    };
    let parsed = chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc));
    let Some(dt) = parsed else {
        return Vec::new();
    };
    let seconds = dt.timestamp();
    let nanos = dt.timestamp_subsec_nanos() as i32;
    let mut buf = Vec::new();
    if seconds != 0 {
        push_varint_field(&mut buf, 1, seconds as u64);
    }
    if nanos != 0 {
        push_varint_field(&mut buf, 2, nanos as u64);
    }
    buf
}

// --- Shared registry singleton --------------------------------------------

/// Process-wide `ProtoRegistry`. Built once on first access. Used by both
/// the response-wrapping middleware (to decode incoming proto requests) and
/// the response encoder (to emit native proto bytes for opt-in kinds).
pub static PROTO_REGISTRY: std::sync::LazyLock<ProtoRegistry> =
    std::sync::LazyLock::new(ProtoRegistry::new);

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
    fn test_decode_rule_with_operations_inlines_rule() {
        // Reproduces the conformance AdmissionWebhook failure: client-go creates
        // ValidatingWebhookConfiguration via protobuf. RuleWithOperations embeds
        // `Rule` as Go `json:",inline"` (proto field 2). The decoded JSON must
        // MERGE apiGroups/apiVersions/resources up into RuleWithOperations, because
        // the Rust type uses #[serde(flatten)]. A nested "rule" object is dropped,
        // leaving every webhook rule empty → the webhook never matches/fires.
        let registry = ProtoRegistry::new();

        // Inner Rule: apiVersions=["v1"], resources=["configmaps"]
        let rule = {
            let mut b = Vec::new();
            b.extend_from_slice(&[0x12, 0x02]); // field 2 (apiVersions), len 2
            b.extend_from_slice(b"v1");
            b.extend_from_slice(&[0x1A, 0x0A]); // field 3 (resources), len 10
            b.extend_from_slice(b"configmaps");
            b
        };
        let mut rwo = Vec::new();
        rwo.extend_from_slice(&[0x0A, 0x06]); // field 1 (operations), len 6
        rwo.extend_from_slice(b"CREATE");
        rwo.push(0x12); // field 2 (rule), wire type 2
        rwo.push(rule.len() as u8);
        rwo.extend_from_slice(&rule);

        let val = registry
            .decode_message("RuleWithOperations", &rwo)
            .expect("RuleWithOperations decodes");

        assert_eq!(
            val.pointer("/operations/0"),
            Some(&Value::String("CREATE".into()))
        );
        // The whole point: rule fields are flattened, NOT nested under "rule".
        assert_eq!(
            val.pointer("/resources/0"),
            Some(&Value::String("configmaps".into())),
            "rule.resources must be inlined to top level; got: {val}"
        );
        assert_eq!(
            val.pointer("/apiVersions/0"),
            Some(&Value::String("v1".into()))
        );
        assert!(
            val.pointer("/rule").is_none(),
            "rule must be inlined, not nested: {val}"
        );
    }

    #[test]
    fn test_decode_service_reference_path_and_port() {
        // Webhook clientConfig.service must keep its path and port: dropping the
        // path makes the api-server POST to "/" (404 from the webhook server);
        // the e2e readiness marker then never gets denied.
        let registry = ProtoRegistry::new();
        let mut b = Vec::new();
        b.extend_from_slice(&[0x0A, 0x0C]); // f1 namespace, len 12
        b.extend_from_slice(b"webhook-2926");
        b.extend_from_slice(&[0x12, 0x10]); // f2 name, len 16
        b.extend_from_slice(b"e2e-test-webhook");
        b.extend_from_slice(&[0x1A, 0x0B]); // f3 path, len 11
        b.extend_from_slice(b"/configmaps");
        b.extend_from_slice(&[0x20, 0xFB, 0x41]); // f4 port varint = 8443

        let val = registry
            .decode_message("ServiceReference", &b)
            .expect("ServiceReference decodes");
        assert_eq!(
            val.pointer("/namespace"),
            Some(&Value::String("webhook-2926".into()))
        );
        assert_eq!(
            val.pointer("/path"),
            Some(&Value::String("/configmaps".into())),
            "service.path dropped; got: {val}"
        );
        assert_eq!(
            val.pointer("/port").and_then(|v| v.as_i64()),
            Some(8443),
            "service.port garbled; got: {val}"
        );
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

    /// `Secret.data` is `map<string, bytes>` upstream; values must arrive
    /// at the handler as base64-encoded JSON strings (matching what
    /// `kubectl get secret -o json` shows). Before adding `FieldType::BytesMap`
    /// the registry mapped this field to `StringMap`, which lost UTF-8
    /// invalid bytes (silently corrupting any non-text Secret payload).
    #[test]
    fn test_decode_secret_data_is_base64_bytes_map() {
        use base64::Engine;
        let registry = ProtoRegistry::new();

        // MapEntry { key="binary", value=[0xff, 0x00, 0x42] }
        let bytes_val: [u8; 3] = [0xff, 0x00, 0x42];
        let mut entry = Vec::new();
        // field 1 (key) length-delimited: tag 0x0a
        entry.push(0x0a);
        entry.push(b"binary".len() as u8);
        entry.extend_from_slice(b"binary");
        // field 2 (value) length-delimited: tag 0x12
        entry.push(0x12);
        entry.push(bytes_val.len() as u8);
        entry.extend_from_slice(&bytes_val);

        // Secret { data: [entry] } — field 2 (data), length-delimited
        let mut secret = Vec::new();
        secret.push(0x12); // field 2
        secret.push(entry.len() as u8);
        secret.extend_from_slice(&entry);

        let val = registry
            .decode_message("Secret", &secret)
            .expect("Secret must decode");

        let expected = base64::engine::general_purpose::STANDARD.encode(bytes_val);
        assert_eq!(
            val.pointer("/data/binary"),
            Some(&Value::String(expected)),
            "Secret.data values must be base64-encoded — registered as BytesMap"
        );
    }

    /// `ConfigMap.binaryData` is `map<string, bytes>` upstream. Same fix as
    /// Secret.data — must base64-encode the value. The sibling `data` field
    /// stays `map<string, string>` (no base64) so this test also verifies
    /// the two map types coexist on one message.
    #[test]
    fn test_decode_configmap_binary_data_is_base64() {
        use base64::Engine;
        let registry = ProtoRegistry::new();

        // binaryData entry: key="raw", value=[0x01, 0x02, 0x03]
        let bytes_val: [u8; 3] = [0x01, 0x02, 0x03];
        let mut bin_entry = Vec::new();
        bin_entry.push(0x0a);
        bin_entry.push(b"raw".len() as u8);
        bin_entry.extend_from_slice(b"raw");
        bin_entry.push(0x12);
        bin_entry.push(bytes_val.len() as u8);
        bin_entry.extend_from_slice(&bytes_val);

        // data entry: key="text", value="hello"
        let mut data_entry = Vec::new();
        data_entry.push(0x0a);
        data_entry.push(b"text".len() as u8);
        data_entry.extend_from_slice(b"text");
        data_entry.push(0x12);
        data_entry.push(b"hello".len() as u8);
        data_entry.extend_from_slice(b"hello");

        // ConfigMap { data: [...], binaryData: [...] }
        // field 2 (data) tag = 0x12
        // field 3 (binaryData) tag = 0x1a
        let mut cm = Vec::new();
        cm.push(0x12);
        cm.push(data_entry.len() as u8);
        cm.extend_from_slice(&data_entry);
        cm.push(0x1a);
        cm.push(bin_entry.len() as u8);
        cm.extend_from_slice(&bin_entry);

        let val = registry
            .decode_message("ConfigMap", &cm)
            .expect("ConfigMap must decode");

        // data is map<string,string> — stays UTF-8.
        assert_eq!(
            val.pointer("/data/text"),
            Some(&Value::String("hello".into()))
        );
        // binaryData is map<string,bytes> — base64-encoded.
        let expected = base64::engine::general_purpose::STANDARD.encode(bytes_val);
        assert_eq!(
            val.pointer("/binaryData/raw"),
            Some(&Value::String(expected)),
            "ConfigMap.binaryData values must be base64-encoded"
        );
    }

    /// Regression test for the upstream conformance test
    /// `[sig-node] Pods should run through the lifecycle of Pods and PodStatus`
    /// (k8s.io/kubernetes/test/e2e/common/node/pods.go:1044).
    ///
    /// The typed Kubernetes client (`clientset.CoreV1().Pods(ns).UpdateStatus`)
    /// transmits Pod bodies as `application/vnd.kubernetes.protobuf`. Our
    /// `normalize_content_type_middleware` decodes those via
    /// `ProtoRegistry::decode_message`. If the `PodStatus` (or `PodCondition`)
    /// schema is missing fields, the decoder silently drops them — so
    /// `status.conditions` arrives at the handler as an empty array (or status
    /// becomes `{}` entirely) and the conformance test's
    /// "failed to update PodStatus - field patch count doesn't match the total"
    /// fires.
    ///
    /// This test builds a minimal PodStatus on the wire (phase=Running, two
    /// conditions with status=False) and asserts the registry decodes them
    /// faithfully to JSON.
    #[test]
    fn test_decode_pod_status_preserves_conditions() {
        let registry = ProtoRegistry::new();

        // PodCondition #1 — type=Ready, status=False
        let mut cond1 = Vec::new();
        cond1.push(0x0a); // field 1 (type), length-delimited
        cond1.push(b"Ready".len() as u8);
        cond1.extend_from_slice(b"Ready");
        cond1.push(0x12); // field 2 (status), length-delimited
        cond1.push(b"False".len() as u8);
        cond1.extend_from_slice(b"False");

        // PodCondition #2 — type=ContainersReady, status=False
        let mut cond2 = Vec::new();
        cond2.push(0x0a);
        cond2.push(b"ContainersReady".len() as u8);
        cond2.extend_from_slice(b"ContainersReady");
        cond2.push(0x12);
        cond2.push(b"False".len() as u8);
        cond2.extend_from_slice(b"False");

        let mut status = Vec::new();
        // phase = "Running" (field 1, length-delimited)
        status.push(0x0a);
        status.push(b"Running".len() as u8);
        status.extend_from_slice(b"Running");
        // conditions[0] (field 2, length-delimited, repeated)
        status.push(0x12);
        status.push(cond1.len() as u8);
        status.extend_from_slice(&cond1);
        // conditions[1]
        status.push(0x12);
        status.push(cond2.len() as u8);
        status.extend_from_slice(&cond2);
        // podIP = "10.1.2.3" (field 6 → tag 0x32)
        status.push(0x32);
        status.push(b"10.1.2.3".len() as u8);
        status.extend_from_slice(b"10.1.2.3");
        // qosClass = "BestEffort" (field 9 → tag 0x4a)
        status.push(0x4a);
        status.push(b"BestEffort".len() as u8);
        status.extend_from_slice(b"BestEffort");

        let val = registry
            .decode_message("PodStatus", &status)
            .expect("PodStatus must decode");

        assert_eq!(val.get("phase"), Some(&Value::String("Running".into())));
        assert_eq!(val.get("podIP"), Some(&Value::String("10.1.2.3".into())));
        assert_eq!(
            val.get("qosClass"),
            Some(&Value::String("BestEffort".into()))
        );

        let conds = val
            .get("conditions")
            .and_then(|c| c.as_array())
            .expect("conditions must be a JSON array");
        assert_eq!(conds.len(), 2, "both conditions must round-trip");
        assert_eq!(conds[0].get("type"), Some(&Value::String("Ready".into())));
        assert_eq!(conds[0].get("status"), Some(&Value::String("False".into())));
        assert_eq!(
            conds[1].get("type"),
            Some(&Value::String("ContainersReady".into()))
        );
        assert_eq!(conds[1].get("status"), Some(&Value::String("False".into())));
    }

    /// End-to-end: a full Pod wire body with metadata + spec + status with
    /// flipped Ready/ContainersReady conditions. Mirrors the wire body that
    /// `clientset.CoreV1().Pods(ns).UpdateStatus(...)` produces from a typed
    /// `v1.Pod`. Verifies the protobuf middleware → JSON conversion preserves
    /// `status.conditions` end to end.
    #[test]
    fn test_decode_pod_with_status_conditions_round_trips() {
        let registry = ProtoRegistry::new();

        // Build a minimal ObjectMeta { name = "pod-test" }
        let mut meta = Vec::new();
        meta.push(0x0a); // field 1 (name)
        meta.push(b"pod-test".len() as u8);
        meta.extend_from_slice(b"pod-test");

        // Build a minimal PodSpec { containers: [{name=pod-test, image=agnhost}] }
        let mut container = Vec::new();
        container.push(0x0a);
        container.push(b"pod-test".len() as u8);
        container.extend_from_slice(b"pod-test");
        container.push(0x12);
        container.push(b"agnhost".len() as u8);
        container.extend_from_slice(b"agnhost");
        let mut pod_spec = Vec::new();
        pod_spec.push(0x12); // field 2 (containers)
        pod_spec.push(container.len() as u8);
        pod_spec.extend_from_slice(&container);

        // Build PodStatus { phase="Running", conditions=[Ready/False, ContainersReady/False] }
        let mut cond1 = Vec::new();
        cond1.extend_from_slice(&[0x0a, b"Ready".len() as u8]);
        cond1.extend_from_slice(b"Ready");
        cond1.extend_from_slice(&[0x12, b"False".len() as u8]);
        cond1.extend_from_slice(b"False");
        let mut cond2 = Vec::new();
        cond2.extend_from_slice(&[0x0a, b"ContainersReady".len() as u8]);
        cond2.extend_from_slice(b"ContainersReady");
        cond2.extend_from_slice(&[0x12, b"False".len() as u8]);
        cond2.extend_from_slice(b"False");
        let mut pod_status = Vec::new();
        pod_status.extend_from_slice(&[0x0a, b"Running".len() as u8]);
        pod_status.extend_from_slice(b"Running");
        pod_status.push(0x12);
        pod_status.push(cond1.len() as u8);
        pod_status.extend_from_slice(&cond1);
        pod_status.push(0x12);
        pod_status.push(cond2.len() as u8);
        pod_status.extend_from_slice(&cond2);

        // Build Pod { metadata, spec, status }
        let mut pod = Vec::new();
        pod.push(0x0a); // field 1 (metadata)
        pod.push(meta.len() as u8);
        pod.extend_from_slice(&meta);
        pod.push(0x12); // field 2 (spec)
        pod.push(pod_spec.len() as u8);
        pod.extend_from_slice(&pod_spec);
        pod.push(0x1a); // field 3 (status)
        pod.push(pod_status.len() as u8);
        pod.extend_from_slice(&pod_status);

        let val = registry.decode_message("Pod", &pod).expect("Pod decodes");

        // Spec and metadata still arrive.
        assert_eq!(
            val.pointer("/metadata/name"),
            Some(&Value::String("pod-test".into()))
        );
        assert_eq!(
            val.pointer("/spec/containers/0/name"),
            Some(&Value::String("pod-test".into()))
        );

        // The bug we are fixing: status.conditions must be present with both
        // Ready/False and ContainersReady/False entries, and status.phase must
        // be "Running". Before this fix, `status` was always `{}`.
        let status = val.get("status").expect("status must round-trip");
        assert_eq!(status.get("phase"), Some(&Value::String("Running".into())));
        let conds = status
            .get("conditions")
            .and_then(|c| c.as_array())
            .expect("status.conditions must be decoded as an array");
        let mut false_ready = 0;
        for c in conds {
            let ty = c.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let st = c.get("status").and_then(|v| v.as_str()).unwrap_or("");
            if (ty == "Ready" || ty == "ContainersReady") && st == "False" {
                false_ready += 1;
            }
        }
        assert_eq!(
            false_ready, 2,
            "PodStatus protobuf decode must preserve both Ready=False and \
             ContainersReady=False conditions (regression test for the \
             '[sig-node] Pods should run through the lifecycle of Pods and \
             PodStatus' conformance test)"
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

    /// Regression for `[sig-api-machinery] Namespaces should apply changes to a
    /// namespace status [Conformance]`. The typed Go client negotiates
    /// `application/vnd.kubernetes.protobuf` for core/v1, so a Namespace /status
    /// response is encoded to protobuf and decoded back into a typed
    /// `v1.NamespaceCondition`. With an empty `NamespaceCondition` schema every
    /// condition field was silently dropped. Encode a Namespace with conditions
    /// and confirm type/status/reason/message survive a proto round-trip.
    #[test]
    fn test_namespace_condition_protobuf_roundtrip() {
        let registry = ProtoRegistry::new();
        let ns = json!({
            "metadata": { "name": "nstest" },
            "status": {
                "phase": "Active",
                "conditions": [{
                    "type": "StatusUpdate",
                    "status": "True",
                    "reason": "E2E",
                    "message": "Updated by an e2e test"
                }]
            }
        });

        let bytes = registry
            .encode_message("Namespace", &ns)
            .expect("Namespace must encode to protobuf");
        let decoded = registry
            .decode_message("Namespace", &bytes)
            .expect("Namespace must decode from protobuf");

        let cond = decoded
            .pointer("/status/conditions/0")
            .expect("condition must round-trip");
        assert_eq!(
            cond.get("type"),
            Some(&Value::String("StatusUpdate".into()))
        );
        assert_eq!(cond.get("status"), Some(&Value::String("True".into())));
        assert_eq!(cond.get("reason"), Some(&Value::String("E2E".into())));
        assert_eq!(
            cond.get("message"),
            Some(&Value::String("Updated by an e2e test".into())),
            "NamespaceCondition.message must survive protobuf encode/decode"
        );
    }

    // ------------------------------------------------------------------
    // Wide protobuf coverage: exercise EVERY registered message schema.
    //
    // We keep hitting per-type protobuf regressions where a schema is empty,
    // missing, or references a sub-message that was never registered (e.g.
    // NodeStatus shipped empty -> daemonEndpoints dropped; SubjectAccessReview
    // had no schema -> resourceAttributes dropped -> 500). These two tests
    // guard the whole registry at once instead of one ad-hoc test per type.
    // ------------------------------------------------------------------

    /// Collect every message-type name a FieldType references (recursing
    /// through Repeated wrappers).
    fn proto_referenced_types(ft: &FieldType, out: &mut Vec<String>) {
        match ft {
            FieldType::Message(t) | FieldType::InlineMessage(t) | FieldType::MessageMap(t) => {
                out.push(t.clone())
            }
            FieldType::Repeated(inner) => proto_referenced_types(inner, out),
            _ => {}
        }
    }

    /// Message types resolved by bespoke decode/encode paths rather than a
    /// registered MessageSchema — referencing them without a schema entry is
    /// legitimate.
    const PROTO_SPECIAL_LEAF_TYPES: &[&str] = &["Time", "MicroTime"];

    #[test]
    fn test_all_protobuf_schemas_reference_registered_types() {
        let registry = ProtoRegistry::new();
        let mut missing: Vec<String> = Vec::new();
        for (owner, schema) in &registry.schemas {
            for (name, ft) in schema.fields.values() {
                let mut refs = Vec::new();
                proto_referenced_types(ft, &mut refs);
                for r in refs {
                    if PROTO_SPECIAL_LEAF_TYPES.contains(&r.as_str()) {
                        continue;
                    }
                    if !registry.schemas.contains_key(&r) {
                        missing.push(format!("{owner}.{name} -> unregistered message type `{r}`"));
                    }
                }
            }
        }
        missing.sort();
        assert!(
            missing.is_empty(),
            "protobuf schemas reference {} unregistered message type(s) — \
             nested fields of these will silently drop on the wire:\n{}",
            missing.len(),
            missing.join("\n")
        );
    }

    /// Build a sample JSON value for a FieldType whose every leaf survives the
    /// encode/decode round-trip. `path` carries the chain of message types
    /// currently being expanded for cycle + depth guarding.
    fn proto_synth_value(
        reg: &ProtoRegistry,
        ft: &FieldType,
        path: &mut Vec<String>,
    ) -> serde_json::Value {
        use serde_json::json;
        match ft {
            FieldType::String => json!("x"),
            FieldType::Int => json!(7),
            FieldType::Double => json!(1.5),
            FieldType::Bool => json!(true),
            FieldType::Bytes => json!("YWJj"), // base64("abc")
            FieldType::IntOrString => json!("x"),
            FieldType::Quantity => json!("1"),
            FieldType::JsonRaw => json!({ "x": 1 }),
            FieldType::StringMap => json!({ "k": "v" }),
            FieldType::BytesMap => json!({ "k": "YWJj" }),
            FieldType::QuantityMap => json!({ "k": "1" }),
            FieldType::Repeated(inner) => {
                let v = proto_synth_value(reg, inner, path);
                if v.is_null() {
                    json!([])
                } else {
                    json!([v])
                }
            }
            FieldType::Message(t) | FieldType::InlineMessage(t) => {
                proto_synth_message(reg, t, path)
            }
            FieldType::MessageMap(t) => {
                let v = proto_synth_message(reg, t, path);
                if v.is_null() {
                    json!({})
                } else {
                    json!({ "k": v })
                }
            }
        }
    }

    /// Build a sample object for a message type. Returns Null when the type is
    /// a fragile timestamp leaf, unregistered, or already on the expansion
    /// path (cycle) / past the depth cap — the caller then omits the field.
    fn proto_synth_message(
        reg: &ProtoRegistry,
        t: &str,
        path: &mut Vec<String>,
    ) -> serde_json::Value {
        use serde_json::{Map, Value};
        // Time/MicroTime JSON forms are RFC3339 strings whose re-encode is not
        // byte-identical to an arbitrary synthesized instant; skip them.
        if PROTO_SPECIAL_LEAF_TYPES.contains(&t) {
            return Value::Null;
        }
        if path.iter().any(|p| p == t) || path.len() > 5 {
            return Value::Null;
        }
        let Some(schema) = reg.schemas.get(t) else {
            return Value::Null;
        };
        path.push(t.to_string());
        let mut obj = Map::new();
        for (name, ft) in schema.fields.values() {
            match ft {
                // Inline messages live flattened in the parent object.
                FieldType::InlineMessage(inner) => {
                    if let Value::Object(m) = proto_synth_message(reg, inner, path) {
                        for (k, v) in m {
                            obj.insert(k, v);
                        }
                    }
                }
                _ => {
                    let v = proto_synth_value(reg, ft, path);
                    if !v.is_null() {
                        obj.insert(name.clone(), v);
                    }
                }
            }
        }
        path.pop();
        Value::Object(obj)
    }

    #[test]
    fn test_all_protobuf_schemas_roundtrip_consistently() {
        let reg = ProtoRegistry::new();
        let mut names: Vec<String> = reg.schemas.keys().cloned().collect();
        names.sort();
        let mut failures: Vec<String> = Vec::new();
        for name in &names {
            let mut path = Vec::new();
            let synth = proto_synth_message(&reg, name, &mut path);
            let Some(bytes1) = reg.encode_message(name, &synth) else {
                continue;
            };
            let Some(decoded) = reg.decode_message(name, &bytes1) else {
                failures.push(format!("{name}: encodes but fails to decode"));
                continue;
            };
            let Some(bytes2) = reg.encode_message(name, &decoded) else {
                failures.push(format!("{name}: decoded value fails to re-encode"));
                continue;
            };
            if bytes1 != bytes2 {
                failures.push(format!(
                    "{name}: wire bytes differ after decode->re-encode ({} -> {} bytes) — \
                     a field is dropped or mis-typed in the schema",
                    bytes1.len(),
                    bytes2.len()
                ));
            }
        }
        assert!(
            failures.is_empty(),
            "{} protobuf schema(s) do not round-trip consistently:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    #[test]
    fn test_subject_access_review_protobuf_roundtrip() {
        // The [sig-auth] SubjectReview conformance test POSTs a SubjectAccessReview
        // over vnd.kubernetes.protobuf with spec.resourceAttributes set. An absent
        // schema dropped resourceAttributes -> the handler saw None and returned
        // 500 "Either resourceAttributes or nonResourceAttributes must be specified".
        let registry = ProtoRegistry::new();
        let sar = json!({
            "metadata": {},
            "spec": {
                "user": "system:serviceaccount:subjectreview-1416:e2e",
                "groups": ["system:authenticated", "system:serviceaccounts"],
                "resourceAttributes": {
                    "namespace": "subjectreview-1416",
                    "verb": "create",
                    "group": "",
                    "resource": "pods"
                }
            }
        });
        let bytes = registry
            .encode_message("SubjectAccessReview", &sar)
            .expect("SubjectAccessReview must encode to protobuf");
        let decoded = registry
            .decode_message("SubjectAccessReview", &bytes)
            .expect("SubjectAccessReview must decode from protobuf");

        assert_eq!(
            decoded.pointer("/spec/resourceAttributes/resource"),
            Some(&json!("pods")),
            "resourceAttributes.resource must survive protobuf encode/decode"
        );
        assert_eq!(
            decoded.pointer("/spec/resourceAttributes/verb"),
            Some(&json!("create"))
        );
        assert_eq!(
            decoded.pointer("/spec/resourceAttributes/namespace"),
            Some(&json!("subjectreview-1416"))
        );
        assert_eq!(
            decoded.pointer("/spec/user"),
            Some(&json!("system:serviceaccount:subjectreview-1416:e2e"))
        );
    }

    #[test]
    fn test_node_daemon_endpoints_protobuf_roundtrip() {
        // The e2e metrics grabber reads status.daemonEndpoints.kubeletEndpoint.Port
        // over protobuf; an empty NodeStatus schema dropped it -> "Invalid Kubelet
        // port 0". Field 6 (daemonEndpoints) must now survive the round-trip.
        let registry = ProtoRegistry::new();
        let node = json!({
            "metadata": { "name": "node-test" },
            "status": {
                "daemonEndpoints": { "kubeletEndpoint": { "Port": 10250 } }
            }
        });
        let bytes = registry
            .encode_message("Node", &node)
            .expect("Node must encode to protobuf");
        let decoded = registry
            .decode_message("Node", &bytes)
            .expect("Node must decode from protobuf");
        assert_eq!(
            decoded.pointer("/status/daemonEndpoints/kubeletEndpoint/Port"),
            Some(&json!(10250)),
            "kubeletEndpoint.Port must survive protobuf encode/decode"
        );
    }
}
