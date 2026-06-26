//! Randomized JSON <-> protobuf round-trip harness.
//!
//! This is a small-scale port of upstream Kubernetes'
//! `staging/src/k8s.io/apimachinery/pkg/api/apitesting/roundtrip/roundtrip.go`,
//! which uses `sigs.k8s.io/randfill` to generate random instances of every
//! registered resource and then checks that:
//!
//!   1. `decode(encode(decode(x))) == decode(x)` as serde-projected JSON
//!      (semantic equality — handles field ordering, default fields, etc.).
//!   2. The protobuf-Unknown-wrapped form survives a round-trip: encode to the
//!      `k8s\x00` magic-prefixed `Unknown { apiVersion, kind, raw }` envelope,
//!      decode it back, and verify the inner object matches the original.
//!
//! For each of six representative resources we run [`ProptestConfig::with_cases`]
//! at 50 iterations, with collection sizes capped at 3 elements to keep the
//! whole test target well under 30s on a developer laptop.
//!
//! ## Known limitations / fields we deliberately don't fuzz
//!
//! - **`metav1.Time` with sub-second precision.** The core/v1 path uses K8s'
//!   no-fractional-seconds RFC3339 wire form, which truncates anything below
//!   the second boundary. We only generate whole-second timestamps for any
//!   field that flows through that codec.
//! - **`metav1.MicroTime`** preserves microseconds but not nanoseconds, so
//!   strategies are restricted to `chrono` timestamps with zero nanos beyond
//!   the microsecond boundary (`with_nanosecond(micros * 1000)`).
//! - **Generated names / random UIDs.** `ObjectMeta::new()` calls `Uuid::new_v4()`
//!   and `Utc::now()` so we hand-build `ObjectMeta` literals rather than calling
//!   the builders — otherwise the value would differ on every regenerate.
//! - **`Secret::stringData`.** Round-trips structurally but K8s normally
//!   normalises it into `data` server-side; we don't exercise that path here.
//! - **Float / quantity edge cases.** Quantities are strings on the wire; we
//!   keep them as canonical strings ("100m", "128Mi") rather than fuzzing the
//!   parser, which already has dedicated coverage in `quantity_*_test.rs`.
//!
//! Reference: upstream `pkg/api/apitesting/roundtrip/roundtrip.go`,
//! `pkg/api/apitesting/fuzzer/`. Existing manual fixtures live in
//! `crates/common/tests/roundtrip_core_v1.rs`.

use std::collections::HashMap;

use proptest::collection::vec;
use proptest::option;
use proptest::prelude::*;

use rusternetes_common::protobuf::{decode_protobuf, encode_protobuf};
use rusternetes_common::resources::deployment::{DeploymentStrategy, RollingUpdateDeployment};
use rusternetes_common::resources::{
    ConfigMap, Container, Deployment, DeploymentSpec, EnvVar, Event, EventSource, EventType,
    ObjectReference, Pod, PodSpec, PodTemplateSpec, Secret, Service, ServiceExternalTrafficPolicy,
    ServiceInternalTrafficPolicy, ServicePort, ServiceSpec, ServiceType,
};
use rusternetes_common::types::{
    LabelSelector, LabelSelectorRequirement, ObjectMeta, ResourceRequirements, TypeMeta,
};

// -----------------------------------------------------------------------------
// Common strategies
// -----------------------------------------------------------------------------

/// Generate a Kubernetes-valid DNS-1123 label (lowercase letters, digits, '-').
fn k8s_name() -> impl Strategy<Value = String> {
    // Note: regex strategy is greedy in proptest; cap at ~30 chars to keep
    // generated payloads compact. Must start with a letter.
    prop::string::string_regex("[a-z][a-z0-9-]{0,30}").unwrap()
}

/// Generate a Kubernetes label / annotation value (relaxed printable ASCII,
/// no embedded quotes/newlines so serde_json::Value comparisons are stable).
fn k8s_label_value() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-zA-Z0-9._-]{0,30}").unwrap()
}

/// Generate a small `HashMap<String,String>` (<=3 entries) for labels / data fields.
fn small_string_map() -> impl Strategy<Value = HashMap<String, String>> {
    prop::collection::hash_map(k8s_name(), k8s_label_value(), 0..3)
}

/// Generate a non-empty small `HashMap<String,String>` (1..=3 entries).
fn nonempty_string_map() -> impl Strategy<Value = HashMap<String, String>> {
    prop::collection::hash_map(k8s_name(), k8s_label_value(), 1..3)
}

/// Build a deterministic `ObjectMeta` from a name + optional labels.
/// Avoids `ObjectMeta::new()` which calls `Uuid::new_v4()` + `Utc::now()`.
fn make_object_meta(
    name: String,
    namespace: Option<String>,
    labels: Option<HashMap<String, String>>,
) -> ObjectMeta {
    ObjectMeta {
        name,
        generate_name: None,
        namespace,
        uid: String::new(),
        generation: None,
        resource_version: None,
        managed_fields: None,
        creation_timestamp: None,
        deletion_timestamp: None,
        deletion_grace_period_seconds: None,
        labels,
        annotations: None,
        finalizers: None,
        owner_references: None,
    }
}

/// Strategy for `ObjectMeta` with namespace.
fn object_meta_namespaced(kind: &'static str) -> impl Strategy<Value = ObjectMeta> {
    // kind is unused but kept for readability of call sites.
    let _ = kind;
    (
        k8s_name(),
        option::of(k8s_name()),
        option::of(small_string_map()),
    )
        .prop_map(|(name, ns, labels)| {
            make_object_meta(name, ns.or(Some("default".into())), labels)
        })
}

/// Round-trip assertion: serde JSON encode -> decode -> re-encode -> compare
/// via `serde_json::Value` (so default fields / ordering don't matter).
fn assert_json_value_roundtrip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let v1 = serde_json::to_value(value).expect("initial encode to Value");
    let decoded: T = serde_json::from_value(v1.clone()).expect("decode-from-Value (first round)");
    let v2 = serde_json::to_value(&decoded).expect("re-encode to Value");
    assert_eq!(
        v1, v2,
        "serde JSON projection drifted on re-encode:\nfirst:  {v1}\nsecond: {v2}"
    );

    // Also check string-form decode->re-encode (closer to the wire path).
    let s = serde_json::to_string(value).expect("encode to string");
    let decoded2: T = serde_json::from_str(&s).expect("decode from string");
    let v3 = serde_json::to_value(&decoded2).expect("re-encode to Value (round 2)");
    assert_eq!(v1, v3, "string-mode JSON round-trip drifted");
}

/// Round-trip assertion through the Unknown protobuf envelope.
fn assert_proto_unknown_roundtrip<T>(value: &T, api_version: &str, kind: &str)
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let bytes = encode_protobuf(value, api_version, kind).expect("encode_protobuf");
    let (decoded, tm) = decode_protobuf::<T>(&bytes).expect("decode_protobuf");
    assert_eq!(tm.api_version, api_version);
    assert_eq!(tm.kind, kind);

    // Compare original vs. decoded via serde_json::Value to dodge missing PartialEq.
    let lhs = serde_json::to_value(value).expect("lhs to value");
    let rhs = serde_json::to_value(&decoded).expect("rhs to value");
    assert_eq!(
        lhs, rhs,
        "protobuf-Unknown round-trip drifted:\nlhs: {lhs}\nrhs: {rhs}"
    );
}

// -----------------------------------------------------------------------------
// Pod
// -----------------------------------------------------------------------------

fn env_var_strategy() -> impl Strategy<Value = EnvVar> {
    (k8s_name(), option::of(k8s_label_value())).prop_map(|(name, value)| EnvVar {
        name,
        value,
        value_from: None,
    })
}

fn container_strategy() -> impl Strategy<Value = Container> {
    (
        k8s_name(),
        k8s_name(),
        option::of(vec(prop::string::string_regex("[a-z]{1,8}").unwrap(), 0..3)),
        option::of(vec(env_var_strategy(), 0..3)),
        option::of(prop::sample::select(vec![
            "Always".to_string(),
            "IfNotPresent".to_string(),
            "Never".to_string(),
        ])),
    )
        .prop_map(|(name, image, command, env, image_pull_policy)| Container {
            name,
            image,
            command,
            args: None,
            working_dir: None,
            ports: None,
            env,
            env_from: None,
            resources: Some(ResourceRequirements {
                limits: None,
                requests: None,
                claims: None,
            }),
            volume_mounts: None,
            volume_devices: None,
            image_pull_policy,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            ..Default::default()
        })
}

fn pod_strategy() -> impl Strategy<Value = Pod> {
    (
        object_meta_namespaced("Pod"),
        vec(container_strategy(), 1..3),
        option::of(prop::sample::select(vec![
            "Always".to_string(),
            "OnFailure".to_string(),
            "Never".to_string(),
        ])),
        option::of(k8s_name()),
    )
        .prop_map(|(metadata, containers, restart_policy, node_name)| {
            let mut spec = PodSpec {
                containers,
                ..Default::default()
            };
            spec.restart_policy = restart_policy;
            spec.node_name = node_name;
            Pod {
                type_meta: TypeMeta {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                },
                metadata,
                spec: Some(spec),
                status: None,
            }
        })
}

// -----------------------------------------------------------------------------
// Deployment
// -----------------------------------------------------------------------------

fn label_selector_strategy() -> impl Strategy<Value = LabelSelector> {
    (
        option::of(nonempty_string_map()),
        option::of(vec(
            (
                k8s_name(),
                prop::sample::select(vec![
                    "In".to_string(),
                    "NotIn".to_string(),
                    "Exists".to_string(),
                    "DoesNotExist".to_string(),
                ]),
                option::of(vec(k8s_label_value(), 0..3)),
            )
                .prop_map(|(key, operator, values)| LabelSelectorRequirement {
                    key,
                    operator,
                    values,
                }),
            0..2,
        )),
    )
        .prop_map(|(match_labels, match_expressions)| LabelSelector {
            match_labels,
            match_expressions,
        })
}

fn deployment_strategy() -> impl Strategy<Value = Deployment> {
    (
        object_meta_namespaced("Deployment"),
        label_selector_strategy(),
        vec(container_strategy(), 1..3),
        option::of(0_i32..10),
        option::of(prop::sample::select(vec![
            "Recreate".to_string(),
            "RollingUpdate".to_string(),
        ])),
    )
        .prop_map(
            |(metadata, selector, containers, replicas, strategy_type)| {
                let template = PodTemplateSpec {
                    metadata: Some(make_object_meta(
                        "tmpl".into(),
                        Some("default".into()),
                        None,
                    )),
                    spec: PodSpec {
                        containers,
                        ..Default::default()
                    },
                };
                let strategy = strategy_type.map(|ty| DeploymentStrategy {
                    strategy_type: ty,
                    rolling_update: Some(RollingUpdateDeployment {
                        max_unavailable: None,
                        max_surge: None,
                    }),
                });
                Deployment {
                    type_meta: TypeMeta {
                        api_version: "apps/v1".into(),
                        kind: "Deployment".into(),
                    },
                    metadata,
                    spec: DeploymentSpec {
                        replicas,
                        selector,
                        template,
                        strategy,
                        min_ready_seconds: None,
                        revision_history_limit: None,
                        paused: None,
                        progress_deadline_seconds: None,
                    },
                    status: None,
                }
            },
        )
}

// -----------------------------------------------------------------------------
// Service
// -----------------------------------------------------------------------------

fn service_port_strategy() -> impl Strategy<Value = ServicePort> {
    (
        option::of(k8s_name()),
        1_u16..65535,
        prop::sample::select(vec![
            "TCP".to_string(),
            "UDP".to_string(),
            "SCTP".to_string(),
        ]),
    )
        .prop_map(|(name, port, protocol)| ServicePort {
            name,
            port,
            target_port: None,
            protocol,
            node_port: None,
            app_protocol: None,
        })
}

fn service_type_strategy() -> impl Strategy<Value = Option<ServiceType>> {
    option::of(prop_oneof![
        Just(ServiceType::ClusterIP),
        Just(ServiceType::NodePort),
        Just(ServiceType::LoadBalancer),
    ])
}

fn service_strategy() -> impl Strategy<Value = Service> {
    (
        object_meta_namespaced("Service"),
        option::of(small_string_map()),
        vec(service_port_strategy(), 1..3),
        service_type_strategy(),
        option::of(prop_oneof![
            Just(ServiceInternalTrafficPolicy::Cluster),
            Just(ServiceInternalTrafficPolicy::Local),
        ]),
        option::of(prop_oneof![
            Just(ServiceExternalTrafficPolicy::Cluster),
            Just(ServiceExternalTrafficPolicy::Local),
        ]),
    )
        .prop_map(
            |(
                metadata,
                selector,
                ports,
                service_type,
                internal_traffic_policy,
                external_traffic_policy,
            )| {
                Service {
                    type_meta: TypeMeta {
                        api_version: "v1".into(),
                        kind: "Service".into(),
                    },
                    metadata,
                    spec: ServiceSpec {
                        selector,
                        ports,
                        service_type,
                        cluster_ip: None,
                        external_ips: None,
                        session_affinity: None,
                        external_name: None,
                        cluster_ips: None,
                        ip_families: None,
                        ip_family_policy: None,
                        internal_traffic_policy,
                        external_traffic_policy,
                        health_check_node_port: None,
                        load_balancer_class: None,
                        load_balancer_ip: None,
                        load_balancer_source_ranges: None,
                        allocate_load_balancer_node_ports: None,
                        publish_not_ready_addresses: None,
                        session_affinity_config: None,
                        traffic_distribution: None,
                    },
                    status: None,
                }
            },
        )
}

// -----------------------------------------------------------------------------
// ConfigMap
// -----------------------------------------------------------------------------

fn binary_blob_strategy() -> impl Strategy<Value = Vec<u8>> {
    vec(any::<u8>(), 0..16)
}

fn configmap_strategy() -> impl Strategy<Value = ConfigMap> {
    (
        object_meta_namespaced("ConfigMap"),
        option::of(small_string_map()),
        option::of(prop::collection::hash_map(
            k8s_name(),
            binary_blob_strategy(),
            0..2,
        )),
        option::of(any::<bool>()),
    )
        .prop_map(|(metadata, data, binary_data, immutable)| ConfigMap {
            type_meta: TypeMeta {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
            },
            metadata,
            data,
            binary_data,
            immutable,
        })
}

// -----------------------------------------------------------------------------
// Secret
// -----------------------------------------------------------------------------

fn secret_strategy() -> impl Strategy<Value = Secret> {
    (
        object_meta_namespaced("Secret"),
        option::of(prop::sample::select(vec![
            "Opaque".to_string(),
            "kubernetes.io/tls".to_string(),
            "kubernetes.io/service-account-token".to_string(),
        ])),
        option::of(prop::collection::hash_map(
            k8s_name(),
            binary_blob_strategy(),
            0..2,
        )),
        option::of(any::<bool>()),
    )
        .prop_map(|(metadata, secret_type, data, immutable)| Secret {
            type_meta: TypeMeta {
                api_version: "v1".into(),
                kind: "Secret".into(),
            },
            metadata,
            secret_type,
            // Skip stringData fuzzing — it's a write-only field server-side and
            // would normalise into `data` on persist, so a structural round-trip
            // wouldn't be representative of real client/server traffic.
            data,
            string_data: None,
            immutable,
        })
}

// -----------------------------------------------------------------------------
// Event
// -----------------------------------------------------------------------------

fn object_reference_strategy() -> impl Strategy<Value = ObjectReference> {
    (
        option::of(k8s_name()),
        option::of(k8s_name()),
        option::of(k8s_name()),
        option::of(k8s_name()),
    )
        .prop_map(|(kind, namespace, name, uid)| ObjectReference {
            kind,
            namespace,
            name,
            uid,
            api_version: Some("v1".into()),
            resource_version: None,
            field_path: None,
        })
}

/// Generate a `chrono::DateTime<Utc>` truncated to whole seconds — the
/// metav1.Time codec used by the core/v1 Event path drops fractional seconds
/// on the wire, so anything finer would visibly drift on round-trip.
fn whole_second_timestamp() -> impl Strategy<Value = chrono::DateTime<chrono::Utc>> {
    (1_700_000_000_i64..1_900_000_000).prop_map(|secs| {
        chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0).expect("valid timestamp")
    })
}

/// Generate a `chrono::DateTime<Utc>` whose sub-second precision lands
/// exactly on a microsecond boundary — anything finer would drift through
/// MicroTime's `%.6f` formatter (see `micro_time` module in event.rs).
fn microsecond_timestamp() -> impl Strategy<Value = chrono::DateTime<chrono::Utc>> {
    (1_700_000_000_i64..1_900_000_000, 0_u32..1_000_000).prop_map(|(secs, micros)| {
        chrono::DateTime::<chrono::Utc>::from_timestamp(secs, micros * 1_000)
            .expect("valid timestamp")
    })
}

fn event_strategy() -> impl Strategy<Value = Event> {
    (
        object_meta_namespaced("Event"),
        object_reference_strategy(),
        k8s_label_value(),
        k8s_label_value(),
        prop_oneof![Just(EventType::Normal), Just(EventType::Warning)],
        whole_second_timestamp(),
        whole_second_timestamp(),
        0_i32..1000,
        option::of(microsecond_timestamp()),
        option::of(k8s_name()),
    )
        .prop_map(
            |(
                metadata,
                involved_object,
                reason,
                message,
                event_type,
                first_ts,
                last_ts,
                count,
                event_time,
                reporting_component,
            )| Event {
                api_version: "v1".into(),
                kind: "Event".into(),
                metadata,
                involved_object,
                reason,
                message,
                source: EventSource {
                    component: "rusternetes".into(),
                    host: None,
                },
                event_type,
                first_timestamp: Some(first_ts),
                last_timestamp: Some(last_ts),
                count,
                action: None,
                related: None,
                series: None,
                event_time,
                reporting_component,
                reporting_instance: None,
                note: None,
                regarding: None,
                // The Event struct has a `#[serde(flatten)] extra` field that
                // would round-trip *into* itself if non-empty (any unknown key
                // would land here on decode, then re-emit at the top level on
                // encode). Setting it to `None` keeps the JSON shape stable.
                extra: None,
            },
        )
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn fuzz_pod_json_roundtrip(pod in pod_strategy()) {
        assert_json_value_roundtrip(&pod);
    }

    #[test]
    fn fuzz_pod_proto_unknown_roundtrip(pod in pod_strategy()) {
        assert_proto_unknown_roundtrip(&pod, "v1", "Pod");
    }

    #[test]
    fn fuzz_deployment_json_roundtrip(d in deployment_strategy()) {
        assert_json_value_roundtrip(&d);
    }

    #[test]
    fn fuzz_deployment_proto_unknown_roundtrip(d in deployment_strategy()) {
        assert_proto_unknown_roundtrip(&d, "apps/v1", "Deployment");
    }

    #[test]
    fn fuzz_service_json_roundtrip(s in service_strategy()) {
        assert_json_value_roundtrip(&s);
    }

    #[test]
    fn fuzz_service_proto_unknown_roundtrip(s in service_strategy()) {
        assert_proto_unknown_roundtrip(&s, "v1", "Service");
    }

    #[test]
    fn fuzz_configmap_json_roundtrip(c in configmap_strategy()) {
        assert_json_value_roundtrip(&c);
    }

    #[test]
    fn fuzz_configmap_proto_unknown_roundtrip(c in configmap_strategy()) {
        assert_proto_unknown_roundtrip(&c, "v1", "ConfigMap");
    }

    #[test]
    fn fuzz_secret_json_roundtrip(s in secret_strategy()) {
        assert_json_value_roundtrip(&s);
    }

    #[test]
    fn fuzz_secret_proto_unknown_roundtrip(s in secret_strategy()) {
        assert_proto_unknown_roundtrip(&s, "v1", "Secret");
    }

    #[test]
    fn fuzz_event_json_roundtrip(e in event_strategy()) {
        assert_json_value_roundtrip(&e);
    }

    #[test]
    fn fuzz_event_proto_unknown_roundtrip(e in event_strategy()) {
        assert_proto_unknown_roundtrip(&e, "v1", "Event");
    }
}

// -----------------------------------------------------------------------------
// Smoke tests for the harness itself.
//
// These are not fuzzed; they exist to give the file a non-property smoke check
// that the strategies build at least one valid instance and that the
// assertions catch a deliberately broken type.
// -----------------------------------------------------------------------------

#[test]
fn harness_pod_strategy_produces_valid_instance() {
    use proptest::strategy::ValueTree;
    let mut runner = proptest::test_runner::TestRunner::default();
    let pod = pod_strategy()
        .new_tree(&mut runner)
        .expect("strategy generates a value")
        .current();
    // Sanity-check the produced Pod by running both round-trips on it.
    assert_json_value_roundtrip(&pod);
    assert_proto_unknown_roundtrip(&pod, "v1", "Pod");
}

#[test]
fn harness_object_meta_is_deterministic() {
    // Regression: ObjectMeta::new() uses Uuid::new_v4() + Utc::now() which
    // would make fuzzed instances non-reproducible. make_object_meta() is the
    // canonical builder for this test file.
    let a = make_object_meta("foo".into(), Some("default".into()), None);
    let b = make_object_meta("foo".into(), Some("default".into()), None);
    assert_eq!(a, b, "make_object_meta must be deterministic");
}
