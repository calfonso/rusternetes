//! Wire-format parity tests for Kubernetes `IntOrString` decoding.
//!
//! Upstream Kubernetes serializes the `IntOrString` type (see
//! `staging/src/k8s.io/apimachinery/pkg/util/intstr/intstr.go`) on the wire
//! as a protobuf message
//!
//! ```text
//! message IntOrString {
//!   int64  type   = 1;  // 0 = Int, 1 = String
//!   int32  intVal = 2;
//!   string strVal = 3;
//! }
//! ```
//!
//! In JSON, the same value is emitted as a bare integer (e.g. `8080`) for
//! the int branch or a bare string (e.g. `"http"`) for the string branch —
//! never a `{type, intVal, strVal}` object. The proto→JSON middleware in
//! `rusternetes_api_server::protobuf::ProtoRegistry` must therefore collapse
//! the wire shape into the correct JSON primitive depending on the `type`
//! discriminator. Otherwise the typed `serde` decoders downstream
//! (`Service`, `Ingress`, `Pod` probes, `Deployment.rollingUpdate`, …)
//! reject the body with `invalid type: map, expected …`.
//!
//! These tests pin both branches of `IntOrString` on every host field that
//! references it in the registry: `ServicePort.targetPort`,
//! `IngressBackend.service.port.{name,number}` (the non-IntOrString sibling,
//! pinned for contrast), `HTTPGetAction.port`, `TCPSocketAction.port`, and
//! `RollingUpdateDeployment.{maxSurge,maxUnavailable}`. Any future schema
//! shuffle that drops `FieldType::IntOrString` from one of these fields will
//! flip the corresponding test red.

use rusternetes_api_server::protobuf::ProtoRegistry;
use serde_json::Value;

/// Encode a varint into `out`.
fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(((v & 0x7f) as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Protobuf wire types used by these tests. Wire type 0 = varint, 2 =
/// length-delimited (strings / embedded messages). See
/// https://protobuf.dev/programming-guides/encoding/#structure.
const WIRE_VARINT: u8 = 0;
const WIRE_LEN: u8 = 2;

/// Encode a protobuf field tag: `(field_number << 3) | wire_type`. Wrapped
/// in a helper so the bit-arithmetic stays explicit at every call site
/// without tripping clippy's `identity_op` lint on `| WIRE_VARINT`.
const fn tag(field_num: u8, wire_type: u8) -> u8 {
    (field_num << 3) | wire_type
}

/// Hand-crafted protobuf bytes for the `IntOrString` int branch
/// (`{ type: 0, intVal: <n> }`).
fn int_or_string_int_bytes(n: i32) -> Vec<u8> {
    let mut buf = Vec::new();
    // field 1 (type), varint = 0 — emit explicitly so we exercise the
    // discriminator parser path, not just rely on the default.
    buf.push(tag(1, WIRE_VARINT)); // tag byte = 0x08
    write_varint(&mut buf, 0);
    // field 2 (intVal), varint = n
    buf.push(tag(2, WIRE_VARINT)); // tag byte = 0x10
    write_varint(&mut buf, n as u64);
    buf
}

/// Hand-crafted protobuf bytes for the `IntOrString` string branch
/// (`{ type: 1, strVal: "<s>" }`).
fn int_or_string_str_bytes(s: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    // field 1 (type), varint = 1
    buf.push(tag(1, WIRE_VARINT));
    write_varint(&mut buf, 1);
    // field 3 (strVal), length-delimited string
    buf.push(tag(3, WIRE_LEN)); // tag byte = 0x1a
    write_varint(&mut buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
    buf
}

/// Wrap a message payload as a length-delimited field inside a parent.
fn embed_message(field_num: u32, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    let tag_value = (field_num << 3) | u32::from(WIRE_LEN); // length-delimited
    write_varint(&mut buf, u64::from(tag_value));
    write_varint(&mut buf, payload.len() as u64);
    buf.extend_from_slice(payload);
    buf
}

// -- ServicePort.targetPort ---------------------------------------------------

/// `core/v1.ServicePort.targetPort` is field 4, `IntOrString`. The int
/// branch must come out as a bare JSON integer so typed
/// `IntOrString::Int(8080)` deserializes cleanly.
#[test]
fn test_service_port_target_port_int_branch_decodes_as_bare_int() {
    let registry = ProtoRegistry::new();
    let ios = int_or_string_int_bytes(8080);
    let bytes = embed_message(4, &ios);

    let decoded = registry
        .decode_message("ServicePort", &bytes)
        .expect("ServicePort schema must be registered");

    let tp = decoded
        .get("targetPort")
        .unwrap_or_else(|| panic!("targetPort missing in {decoded}"));
    assert_eq!(
        tp,
        &Value::from(8080),
        "targetPort int branch must decode to a bare JSON int; got {tp}",
    );
}

/// `ServicePort.targetPort` string branch — named ports like `"http"` /
/// `"https"` must surface as bare JSON strings.
#[test]
fn test_service_port_target_port_string_branch_decodes_as_bare_string() {
    let registry = ProtoRegistry::new();
    let ios = int_or_string_str_bytes("http");
    let bytes = embed_message(4, &ios);

    let decoded = registry
        .decode_message("ServicePort", &bytes)
        .expect("ServicePort schema must be registered");

    let tp = decoded
        .get("targetPort")
        .unwrap_or_else(|| panic!("targetPort missing in {decoded}"));
    assert_eq!(
        tp.as_str(),
        Some("http"),
        "targetPort string branch must decode to a bare JSON string; got {tp}",
    );
}

// -- HTTPGetAction.port (probe) ----------------------------------------------

/// `core/v1.HTTPGetAction.port` is field 2, `IntOrString`. Liveness/readiness
/// probes commonly use the int branch (`port: 8080`).
#[test]
fn test_http_get_action_port_int_branch_decodes_as_bare_int() {
    let registry = ProtoRegistry::new();
    let ios = int_or_string_int_bytes(8080);
    let bytes = embed_message(2, &ios);

    let decoded = registry
        .decode_message("HTTPGetAction", &bytes)
        .expect("HTTPGetAction schema must be registered");

    let p = decoded
        .get("port")
        .unwrap_or_else(|| panic!("port missing in {decoded}"));
    assert_eq!(
        p,
        &Value::from(8080),
        "HTTPGetAction.port int branch; got {p}"
    );
}

/// `HTTPGetAction.port` string branch — named ports (`"http"`) referenced
/// from a probe must round-trip as bare JSON string.
#[test]
fn test_http_get_action_port_string_branch_decodes_as_bare_string() {
    let registry = ProtoRegistry::new();
    let ios = int_or_string_str_bytes("http");
    let bytes = embed_message(2, &ios);

    let decoded = registry
        .decode_message("HTTPGetAction", &bytes)
        .expect("HTTPGetAction schema must be registered");

    let p = decoded
        .get("port")
        .unwrap_or_else(|| panic!("port missing in {decoded}"));
    assert_eq!(
        p.as_str(),
        Some("http"),
        "HTTPGetAction.port string branch; got {p}",
    );
}

// -- TCPSocketAction.port (probe) --------------------------------------------

/// `core/v1.TCPSocketAction.port` is field 1, `IntOrString`. TCP probes use
/// the same wire shape as HTTP probes — pinned separately so a schema
/// re-shuffle doesn't fix one and regress the other.
#[test]
fn test_tcp_socket_action_port_int_branch_decodes_as_bare_int() {
    let registry = ProtoRegistry::new();
    let ios = int_or_string_int_bytes(5432);
    let bytes = embed_message(1, &ios);

    let decoded = registry
        .decode_message("TCPSocketAction", &bytes)
        .expect("TCPSocketAction schema must be registered");

    let p = decoded
        .get("port")
        .unwrap_or_else(|| panic!("port missing in {decoded}"));
    assert_eq!(
        p,
        &Value::from(5432),
        "TCPSocketAction.port int branch; got {p}"
    );
}

#[test]
fn test_tcp_socket_action_port_string_branch_decodes_as_bare_string() {
    let registry = ProtoRegistry::new();
    let ios = int_or_string_str_bytes("postgres");
    let bytes = embed_message(1, &ios);

    let decoded = registry
        .decode_message("TCPSocketAction", &bytes)
        .expect("TCPSocketAction schema must be registered");

    let p = decoded
        .get("port")
        .unwrap_or_else(|| panic!("port missing in {decoded}"));
    assert_eq!(
        p.as_str(),
        Some("postgres"),
        "TCPSocketAction.port string branch; got {p}",
    );
}

// -- RollingUpdateDeployment.maxSurge / maxUnavailable -----------------------

/// `apps/v1.RollingUpdateDeployment.maxUnavailable` is field 1, `IntOrString`.
/// Default for that field upstream is `"25%"` (string branch) — pin it.
#[test]
fn test_rolling_update_deployment_max_unavailable_string_branch() {
    let registry = ProtoRegistry::new();
    let ios = int_or_string_str_bytes("25%");
    let bytes = embed_message(1, &ios);

    let decoded = registry
        .decode_message("RollingUpdateDeployment", &bytes)
        .expect("RollingUpdateDeployment schema must be registered");

    let mu = decoded
        .get("maxUnavailable")
        .unwrap_or_else(|| panic!("maxUnavailable missing in {decoded}"));
    assert_eq!(
        mu.as_str(),
        Some("25%"),
        "maxUnavailable string branch must decode to a bare JSON string; got {mu}",
    );
}

/// `RollingUpdateDeployment.maxUnavailable` — int branch (e.g. `maxUnavailable: 2`).
#[test]
fn test_rolling_update_deployment_max_unavailable_int_branch() {
    let registry = ProtoRegistry::new();
    let ios = int_or_string_int_bytes(2);
    let bytes = embed_message(1, &ios);

    let decoded = registry
        .decode_message("RollingUpdateDeployment", &bytes)
        .expect("RollingUpdateDeployment schema must be registered");

    let mu = decoded
        .get("maxUnavailable")
        .unwrap_or_else(|| panic!("maxUnavailable missing in {decoded}"));
    assert_eq!(
        mu,
        &Value::from(2),
        "maxUnavailable int branch must decode to a bare JSON int; got {mu}",
    );
}

/// `RollingUpdateDeployment.maxSurge` is field 2, `IntOrString`. Default
/// upstream is `"25%"` (string).
#[test]
fn test_rolling_update_deployment_max_surge_string_branch() {
    let registry = ProtoRegistry::new();
    let ios = int_or_string_str_bytes("25%");
    let bytes = embed_message(2, &ios);

    let decoded = registry
        .decode_message("RollingUpdateDeployment", &bytes)
        .expect("RollingUpdateDeployment schema must be registered");

    let ms = decoded
        .get("maxSurge")
        .unwrap_or_else(|| panic!("maxSurge missing in {decoded}"));
    assert_eq!(
        ms.as_str(),
        Some("25%"),
        "maxSurge string branch must decode to a bare JSON string; got {ms}",
    );
}

#[test]
fn test_rolling_update_deployment_max_surge_int_branch() {
    let registry = ProtoRegistry::new();
    let ios = int_or_string_int_bytes(3);
    let bytes = embed_message(2, &ios);

    let decoded = registry
        .decode_message("RollingUpdateDeployment", &bytes)
        .expect("RollingUpdateDeployment schema must be registered");

    let ms = decoded
        .get("maxSurge")
        .unwrap_or_else(|| panic!("maxSurge missing in {decoded}"));
    assert_eq!(
        ms,
        &Value::from(3),
        "maxSurge int branch must decode to a bare JSON int; got {ms}",
    );
}

/// Both fields populated together — the typical Deployment rollout config
/// shape (`maxSurge: 1`, `maxUnavailable: "25%"`). Verifies that mixed
/// branches in sibling fields don't cross-contaminate.
#[test]
fn test_rolling_update_deployment_mixed_int_and_string_branches() {
    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&embed_message(1, &int_or_string_str_bytes("25%")));
    bytes.extend_from_slice(&embed_message(2, &int_or_string_int_bytes(1)));

    let decoded = registry
        .decode_message("RollingUpdateDeployment", &bytes)
        .expect("RollingUpdateDeployment schema must be registered");

    let mu = decoded
        .get("maxUnavailable")
        .unwrap_or_else(|| panic!("maxUnavailable missing in {decoded}"));
    assert_eq!(
        mu.as_str(),
        Some("25%"),
        "maxUnavailable string branch; got {mu}"
    );

    let ms = decoded
        .get("maxSurge")
        .unwrap_or_else(|| panic!("maxSurge missing in {decoded}"));
    assert_eq!(ms, &Value::from(1), "maxSurge int branch; got {ms}");
}

// -- IngressBackend.service.port (ServiceBackendPort) ------------------------

/// `networking/v1.IngressBackend.service` is an `IngressServiceBackend`
/// (field 4 of `IngressBackend`, field 2 of `IngressServiceBackend` is
/// `port: ServiceBackendPort`). Unlike the IntOrString cases above,
/// upstream split this into two mutually-exclusive scalar fields:
/// `name: string` (field 1) and `number: int32` (field 2). Pin both
/// branches so a future "let's just use IntOrString here too" refactor
/// gets caught — typed clients distinguish by which field is present, not
/// by a discriminator. See `k8s.io/api/networking/v1/generated.proto`.
#[test]
fn test_ingress_backend_service_port_number_branch_decodes_as_int_field() {
    let registry = ProtoRegistry::new();

    // ServiceBackendPort { number: 80 }
    let mut sbp = Vec::new();
    sbp.push(tag(2, WIRE_VARINT)); // field 2 (number), varint
    write_varint(&mut sbp, 80);

    // IngressServiceBackend { name: "svc", port: ServiceBackendPort{...} }
    let mut isb = Vec::new();
    isb.push(tag(1, WIRE_LEN)); // field 1 (name), length-delimited
    write_varint(&mut isb, 3);
    isb.extend_from_slice(b"svc");
    isb.extend_from_slice(&embed_message(2, &sbp)); // field 2 (port)

    // IngressBackend { service: IngressServiceBackend{...} }
    let bytes = embed_message(4, &isb);

    let decoded = registry
        .decode_message("IngressBackend", &bytes)
        .expect("IngressBackend schema must be registered");

    let service = decoded
        .get("service")
        .unwrap_or_else(|| panic!("service missing in {decoded}"));
    assert_eq!(
        service.get("name").and_then(|v| v.as_str()),
        Some("svc"),
        "IngressBackend.service.name must round-trip; got {service}",
    );
    let port = service
        .get("port")
        .unwrap_or_else(|| panic!("service.port missing in {service}"));
    assert_eq!(
        port.get("number"),
        Some(&Value::from(80)),
        "ServiceBackendPort.number must surface as a JSON int field on the port \
         object — not a bare scalar, since networking/v1 split the IntOrString \
         into name/number; got {port}",
    );
    assert!(
        port.get("name").is_none() || port.get("name").and_then(|v| v.as_str()) == Some(""),
        "name branch must be absent when number is populated; got {port}",
    );
}

#[test]
fn test_ingress_backend_service_port_name_branch_decodes_as_string_field() {
    let registry = ProtoRegistry::new();

    // ServiceBackendPort { name: "http" }
    let mut sbp = Vec::new();
    sbp.push(tag(1, WIRE_LEN)); // field 1 (name), length-delimited
    write_varint(&mut sbp, 4);
    sbp.extend_from_slice(b"http");

    // IngressServiceBackend { name: "svc", port: ServiceBackendPort{...} }
    let mut isb = Vec::new();
    isb.push(tag(1, WIRE_LEN)); // field 1 (name), length-delimited
    write_varint(&mut isb, 3);
    isb.extend_from_slice(b"svc");
    isb.extend_from_slice(&embed_message(2, &sbp)); // field 2 (port)

    let bytes = embed_message(4, &isb);

    let decoded = registry
        .decode_message("IngressBackend", &bytes)
        .expect("IngressBackend schema must be registered");

    let port = decoded
        .get("service")
        .and_then(|s| s.get("port"))
        .unwrap_or_else(|| panic!("service.port missing in {decoded}"));
    assert_eq!(
        port.get("name").and_then(|v| v.as_str()),
        Some("http"),
        "ServiceBackendPort.name must surface as a JSON string field on the port \
         object; got {port}",
    );
}

// -- Round-trip through the typed deserializer -------------------------------

/// End-to-end: decode a `ServicePort` whose `targetPort` is the string
/// branch and round-trip the resulting JSON through the typed
/// `rusternetes_common::resources::ServicePort` deserializer. The
/// `IntOrString` enum's `serde` derive expects a bare primitive — if the
/// proto decoder ever wraps it in `{type, intVal, strVal}` again, this
/// test fails with the same `invalid type: map` error seen on the
/// conformance canary.
#[test]
fn test_service_port_target_port_string_round_trips_through_typed_decoder() {
    use rusternetes_common::resources::{IntOrString, ServicePort};

    let registry = ProtoRegistry::new();
    // ServicePort { name: "web", protocol: "TCP", port: 80, targetPort: "http" }
    let mut bytes = Vec::new();
    // field 1 (name)
    bytes.push(tag(1, WIRE_LEN));
    write_varint(&mut bytes, 3);
    bytes.extend_from_slice(b"web");
    // field 2 (protocol)
    bytes.push(tag(2, WIRE_LEN));
    write_varint(&mut bytes, 3);
    bytes.extend_from_slice(b"TCP");
    // field 3 (port)
    bytes.push(tag(3, WIRE_VARINT));
    write_varint(&mut bytes, 80);
    // field 4 (targetPort) = string branch
    bytes.extend_from_slice(&embed_message(4, &int_or_string_str_bytes("http")));

    let decoded = registry
        .decode_message("ServicePort", &bytes)
        .expect("ServicePort schema must be registered");

    let json_bytes = serde_json::to_vec(&decoded).unwrap();
    let sp: ServicePort = serde_json::from_slice(&json_bytes).unwrap_or_else(|e| {
        panic!(
            "ServicePort must round-trip through typed decoder; decoder produced \
             {decoded}; serde error: {e}",
        )
    });

    match sp.target_port {
        Some(IntOrString::String(s)) => assert_eq!(s, "http"),
        other => panic!("expected IntOrString::String(\"http\"), got {other:?}"),
    }
}

/// Same round-trip but for the int branch — pins the inverse failure
/// mode (proto decoder emits a string when wire says int).
#[test]
fn test_service_port_target_port_int_round_trips_through_typed_decoder() {
    use rusternetes_common::resources::{IntOrString, ServicePort};

    let registry = ProtoRegistry::new();
    let mut bytes = Vec::new();
    // field 1 (name)
    bytes.push(tag(1, WIRE_LEN));
    write_varint(&mut bytes, 3);
    bytes.extend_from_slice(b"web");
    // field 3 (port) — required by typed `ServicePort` for round-trip
    bytes.push(tag(3, WIRE_VARINT));
    write_varint(&mut bytes, 80);
    // field 4 (targetPort) = int branch
    bytes.extend_from_slice(&embed_message(4, &int_or_string_int_bytes(8080)));

    let decoded = registry
        .decode_message("ServicePort", &bytes)
        .expect("ServicePort schema must be registered");

    let json_bytes = serde_json::to_vec(&decoded).unwrap();
    let sp: ServicePort = serde_json::from_slice(&json_bytes).unwrap_or_else(|e| {
        panic!(
            "ServicePort must round-trip through typed decoder; decoder produced \
             {decoded}; serde error: {e}",
        )
    });

    match sp.target_port {
        Some(IntOrString::Int(n)) => assert_eq!(n, 8080),
        other => panic!("expected IntOrString::Int(8080), got {other:?}"),
    }
}
