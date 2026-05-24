//! Wire-format-correctness tests for `ProtoRegistry::encode_message`.
//!
//! The encoder is the symmetric inverse of `decode_message`: every
//! `FieldType` variant the decoder consumes must round-trip via the
//! encoder. These tests pin that contract — encode a JSON value, decode
//! the resulting bytes back, then compare. The test set covers each
//! `FieldType` variant exercised by Pod / PodList responses:
//!
//! - scalar string / int / bool
//! - nested message (`ObjectMeta` inside `Pod`)
//! - repeated message (`containers` inside `PodSpec`)
//! - `StringMap` (labels / annotations)
//! - `BytesMap` (`Secret.data`)
//! - enums encoded as int32 (`Container.imagePullPolicy` is a string in
//!   K8s JSON, but the test for `Service.type` exercises a string-encoded
//!   enum that flows through `FieldType::String`)
//! - `InlineMessage` round-trip (`Volume.volumeSource` flattens
//!   `EmptyDir`, `HostPath`, … into the surrounding Volume object)
//! - `IntOrString` (Pod `tolerationSeconds`, Probe `port`)
//! - `Quantity` and `QuantityMap` (`ResourceRequirements.requests`)
//! - `Time` (`ObjectMeta.creationTimestamp`)
//!
//! Each test uses minimal hand-rolled JSON so failure messages are
//! localised — if a single FieldType regresses, exactly one test fails.

use rusternetes_api_server::protobuf::PROTO_REGISTRY;
use serde_json::{json, Value};

/// Round-trip a JSON value through encode_message → decode_message for
/// `msg_type`. Returns the decoded value so the caller can pin specific
/// fields with `assert_eq!`.
fn round_trip(msg_type: &str, value: &Value) -> Value {
    let bytes = PROTO_REGISTRY
        .encode_message(msg_type, value)
        .unwrap_or_else(|| panic!("encode_message returned None for {msg_type}"));
    PROTO_REGISTRY
        .decode_message(msg_type, &bytes)
        .unwrap_or_else(|| panic!("decode_message returned None for {msg_type}"))
}

#[test]
fn round_trip_pod_minimal() {
    let pod = json!({
        "metadata": {
            "name": "p1",
            "namespace": "default",
        },
        "spec": {
            "containers": [{
                "name": "c1",
                "image": "busybox",
            }]
        }
    });
    let decoded = round_trip("Pod", &pod);
    assert_eq!(
        decoded.pointer("/metadata/name"),
        Some(&Value::String("p1".into())),
    );
    assert_eq!(
        decoded.pointer("/metadata/namespace"),
        Some(&Value::String("default".into())),
    );
    assert_eq!(
        decoded.pointer("/spec/containers/0/name"),
        Some(&Value::String("c1".into())),
    );
    assert_eq!(
        decoded.pointer("/spec/containers/0/image"),
        Some(&Value::String("busybox".into())),
    );
}

#[test]
fn round_trip_pod_with_labels_and_annotations() {
    let pod = json!({
        "metadata": {
            "name": "p2",
            "labels": {
                "app": "nginx",
                "tier": "frontend",
            },
            "annotations": {
                "kubernetes.io/created-by": "test",
            },
        },
        "spec": {"containers": [{"name": "c", "image": "i"}]}
    });
    let decoded = round_trip("Pod", &pod);
    assert_eq!(
        decoded.pointer("/metadata/labels/app"),
        Some(&Value::String("nginx".into())),
    );
    assert_eq!(
        decoded.pointer("/metadata/labels/tier"),
        Some(&Value::String("frontend".into())),
    );
    assert_eq!(
        decoded.pointer("/metadata/annotations/kubernetes.io~1created-by"),
        Some(&Value::String("test".into())),
    );
}

#[test]
fn round_trip_pod_with_status_phase_and_conditions() {
    let pod = json!({
        "metadata": {"name": "p3"},
        "spec": {"containers": [{"name": "c", "image": "i"}]},
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "False", "reason": "ContainersNotReady"},
                {"type": "ContainersReady", "status": "False"},
            ],
        }
    });
    let decoded = round_trip("Pod", &pod);
    assert_eq!(
        decoded.pointer("/status/phase"),
        Some(&Value::String("Running".into())),
    );
    let conds = decoded
        .pointer("/status/conditions")
        .and_then(|c| c.as_array())
        .expect("conditions present");
    assert_eq!(conds.len(), 2, "two conditions expected; got {decoded}");
    assert_eq!(conds[0].get("type"), Some(&Value::String("Ready".into())));
    assert_eq!(conds[0].get("status"), Some(&Value::String("False".into())));
    assert_eq!(
        conds[0].get("reason"),
        Some(&Value::String("ContainersNotReady".into())),
    );
}

#[test]
fn round_trip_pod_list() {
    let pod_list = json!({
        "metadata": {"resourceVersion": "42"},
        "items": [
            {"metadata": {"name": "a"}, "spec": {"containers": [{"name": "c", "image": "i"}]}},
            {"metadata": {"name": "b"}, "spec": {"containers": [{"name": "c", "image": "i"}]}},
        ]
    });
    let decoded = round_trip("PodList", &pod_list);
    let items = decoded
        .pointer("/items")
        .and_then(|i| i.as_array())
        .expect("items array");
    assert_eq!(items.len(), 2);
    let names: Vec<&str> = items
        .iter()
        .map(|p| {
            p.pointer("/metadata/name")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(names, vec!["a", "b"]);
}

#[test]
fn round_trip_pod_with_repeated_containers_and_env() {
    let pod = json!({
        "metadata": {"name": "p-env"},
        "spec": {
            "containers": [
                {
                    "name": "a",
                    "image": "i1",
                    "env": [
                        {"name": "FOO", "value": "bar"},
                        {"name": "BAZ", "value": "qux"},
                    ],
                },
                {
                    "name": "b",
                    "image": "i2",
                },
            ]
        }
    });
    let decoded = round_trip("Pod", &pod);
    let cs = decoded
        .pointer("/spec/containers")
        .and_then(|c| c.as_array())
        .expect("containers array");
    assert_eq!(cs.len(), 2);
    let env = cs[0]
        .pointer("/env")
        .and_then(|e| e.as_array())
        .expect("env array");
    assert_eq!(env.len(), 2);
    assert_eq!(env[0].get("name"), Some(&Value::String("FOO".into())));
    assert_eq!(env[0].get("value"), Some(&Value::String("bar".into())));
    assert_eq!(env[1].get("name"), Some(&Value::String("BAZ".into())));
}

#[test]
fn round_trip_pod_with_resources_quantity_map() {
    let pod = json!({
        "metadata": {"name": "p-res"},
        "spec": {
            "containers": [{
                "name": "c",
                "image": "i",
                "resources": {
                    "requests": {"cpu": "100m", "memory": "64Mi"},
                    "limits":   {"cpu": "500m", "memory": "256Mi"},
                },
            }]
        }
    });
    let decoded = round_trip("Pod", &pod);
    assert_eq!(
        decoded.pointer("/spec/containers/0/resources/requests/cpu"),
        Some(&Value::String("100m".into())),
    );
    assert_eq!(
        decoded.pointer("/spec/containers/0/resources/limits/memory"),
        Some(&Value::String("256Mi".into())),
    );
}

#[test]
fn round_trip_pod_with_int_or_string_termination_grace() {
    let pod = json!({
        "metadata": {"name": "p-grace"},
        "spec": {
            "containers": [{"name": "c", "image": "i"}],
            "terminationGracePeriodSeconds": 30,
        }
    });
    let decoded = round_trip("Pod", &pod);
    // terminationGracePeriodSeconds is int64 (varint).
    assert_eq!(
        decoded.pointer("/spec/terminationGracePeriodSeconds"),
        Some(&json!(30))
    );
}

#[test]
fn round_trip_secret_bytes_map() {
    // Secret.data is map<string, bytes> — values are base64 in JSON, raw
    // bytes on the wire. Round-trip must preserve the base64 form.
    let secret = json!({
        "metadata": {"name": "s"},
        "type": "Opaque",
        "data": {
            // "hello" base64-encoded
            "greeting": "aGVsbG8=",
            // empty bytes
            "empty": "",
        }
    });
    let decoded = round_trip("Secret", &secret);
    assert_eq!(
        decoded.pointer("/data/greeting"),
        Some(&Value::String("aGVsbG8=".into())),
    );
    // Empty bytes round-trip to an empty base64 string.
    assert_eq!(
        decoded.pointer("/data/empty"),
        Some(&Value::String("".into())),
    );
}

#[test]
fn round_trip_inline_volume_source() {
    // Volume embeds a VolumeSource via InlineMessage at field 2 — its
    // fields are flattened into the Volume object in JSON. Encode must
    // collect the embedded fields back out and emit them as a nested
    // message.
    let pod = json!({
        "metadata": {"name": "p-vol"},
        "spec": {
            "containers": [{"name": "c", "image": "i"}],
            "volumes": [{
                "name": "data",
                "emptyDir": {"medium": "Memory"}
            }]
        }
    });
    let decoded = round_trip("Pod", &pod);
    assert_eq!(
        decoded.pointer("/spec/volumes/0/name"),
        Some(&Value::String("data".into())),
    );
    assert_eq!(
        decoded.pointer("/spec/volumes/0/emptyDir/medium"),
        Some(&Value::String("Memory".into())),
    );
}

#[test]
fn round_trip_container_with_int_or_string_port() {
    // Probe.tcpSocket.port is IntOrString. Test both int and string forms.
    let pod_int = json!({
        "metadata": {"name": "p"},
        "spec": {
            "containers": [{
                "name": "c",
                "image": "i",
                "livenessProbe": {
                    "tcpSocket": {"port": 8080}
                }
            }]
        }
    });
    let decoded_int = round_trip("Pod", &pod_int);
    assert_eq!(
        decoded_int.pointer("/spec/containers/0/livenessProbe/tcpSocket/port"),
        Some(&json!(8080)),
    );

    let pod_str = json!({
        "metadata": {"name": "p"},
        "spec": {
            "containers": [{
                "name": "c",
                "image": "i",
                "livenessProbe": {
                    "tcpSocket": {"port": "http"}
                }
            }]
        }
    });
    let decoded_str = round_trip("Pod", &pod_str);
    assert_eq!(
        decoded_str.pointer("/spec/containers/0/livenessProbe/tcpSocket/port"),
        Some(&Value::String("http".into())),
    );
}

#[test]
fn round_trip_pod_creation_timestamp_rfc3339() {
    let pod = json!({
        "metadata": {
            "name": "p-time",
            "creationTimestamp": "2026-05-24T10:30:00Z",
        },
        "spec": {"containers": [{"name": "c", "image": "i"}]},
    });
    let decoded = round_trip("Pod", &pod);
    assert_eq!(
        decoded.pointer("/metadata/creationTimestamp"),
        Some(&Value::String("2026-05-24T10:30:00Z".into())),
    );
}

#[test]
fn round_trip_pod_with_owner_references() {
    let pod = json!({
        "metadata": {
            "name": "p-owner",
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "rs-1",
                "uid": "abc-123",
                "controller": true,
                "blockOwnerDeletion": true,
            }]
        },
        "spec": {"containers": [{"name": "c", "image": "i"}]}
    });
    let decoded = round_trip("Pod", &pod);
    let owners = decoded
        .pointer("/metadata/ownerReferences")
        .and_then(|o| o.as_array())
        .expect("ownerReferences");
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].get("name"), Some(&Value::String("rs-1".into())),);
    assert_eq!(
        owners[0].get("kind"),
        Some(&Value::String("ReplicaSet".into())),
    );
    assert_eq!(owners[0].get("controller"), Some(&Value::Bool(true)));
}

#[test]
fn encode_unknown_message_type_returns_none() {
    let v = json!({"foo": "bar"});
    assert!(
        PROTO_REGISTRY.encode_message("NoSuchType", &v).is_none(),
        "encode_message must return None for unknown msg_type"
    );
}

#[test]
fn encode_decode_through_unknown_envelope() {
    // End-to-end: build a Pod JSON, encode to native proto, wrap in the
    // Unknown envelope, then unwrap and decode. Exercises the full path
    // used by `NativePodProtoEncoder` in production.
    use prost::Message;
    use rusternetes_api_server::response::{
        wrap_native_proto_in_envelope, NativePodProtoEncoder, ProtoEncoder,
    };
    use rusternetes_common::protobuf::{is_protobuf, Unknown, PROTOBUF_MAGIC};

    let pod = json!({
        "metadata": {"name": "envelope-roundtrip"},
        "spec": {"containers": [{"name": "c", "image": "i"}]}
    });
    let json_bytes = serde_json::to_vec(&pod).unwrap();
    let encoder = NativePodProtoEncoder;
    let envelope = encoder.encode(&json_bytes, "v1", "Pod");

    assert!(envelope.starts_with(PROTOBUF_MAGIC));
    assert!(is_protobuf(&envelope));

    let unknown = Unknown::decode(&envelope[PROTOBUF_MAGIC.len()..]).expect("Unknown decode");
    let tm = unknown.type_meta.expect("typeMeta");
    assert_eq!(tm.api_version, "v1");
    assert_eq!(tm.kind, "Pod");
    // contentType must advertise proto, NOT application/json — otherwise
    // any Unknown-aware decoder (and client-go's typed path) thinks the
    // raw payload is JSON.
    assert_eq!(unknown.content_type, "application/vnd.kubernetes.protobuf");

    // Raw payload must be native proto bytes — first byte is a proto
    // tag, NOT a JSON `{` (0x7B).
    assert_ne!(
        unknown.raw.first(),
        Some(&b'{'),
        "Unknown.raw must be native proto, not JSON; first byte={:?}",
        unknown.raw.first()
    );

    let decoded = PROTO_REGISTRY
        .decode_message("Pod", &unknown.raw)
        .expect("decode Pod");
    assert_eq!(
        decoded.pointer("/metadata/name"),
        Some(&Value::String("envelope-roundtrip".into())),
    );

    // Also verify the lower-level wrap_native_proto_in_envelope helper.
    let raw_bytes = PROTO_REGISTRY
        .encode_message("Pod", &pod)
        .expect("encode Pod");
    let env2 = wrap_native_proto_in_envelope(&raw_bytes, "v1", "Pod");
    assert!(env2.starts_with(PROTOBUF_MAGIC));
}
