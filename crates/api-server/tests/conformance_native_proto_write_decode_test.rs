//! Black-box reproduction of the native-protobuf WRITE decode gap that the
//! full conformance run surfaced.
//!
//! client-go POSTs writes as native Kubernetes protobuf: a `k8s\0`-framed
//! `runtime.Unknown` envelope whose `raw` field carries the generated
//! `pb.go`-marshalled resource bytes (NOT JSON), with
//! `contentType: application/vnd.kubernetes.protobuf`. When the resource's
//! proto schema is missing from [`ProtoRegistry`], the api-server's
//! request-decode middleware fell through to a brace-scan / TypeMeta fallback
//! that SILENTLY produced `{"apiVersion":..,"kind":..,"metadata":{}}` —
//! dropping `spec` entirely. The handler then either rejected the body
//! (required field missing) or persisted an empty object. client-go reports
//! this opaquely as "the server rejected our request due to an error in our
//! request (post X)".
//!
//! The conformance run failed these native-proto POSTs:
//!   * flowschemas.flowcontrol.apiserver.k8s.io
//!   * prioritylevelconfigurations.flowcontrol.apiserver.k8s.io
//!   * runtimeclasses.node.k8s.io
//!   * tokenreviews.authentication.k8s.io
//!
//! Each test hand-encodes the exact native-proto bytes client-go sends,
//! frames them in the Unknown envelope, POSTs with
//! `Content-Type: application/vnd.kubernetes.protobuf`, and asserts the
//! `spec` survives the decode (i.e. the create succeeds and the persisted /
//! echoed object carries the spec fields, not an empty stub).

use axum::http::StatusCode;
use rusternetes_api_server::protobuf::ProtoRegistry;
use rusternetes_test_support::harness::TestApiServer;

const PROTO_CT: &str = "application/vnd.kubernetes.protobuf";

fn spawn_router() -> TestApiServer {
    TestApiServer::new()
}

// ----- proto wire helpers ---------------------------------------------------

fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        buf.push(b);
        if v == 0 {
            break;
        }
    }
}

/// length-delimited field (wire type 2)
fn ld_field(buf: &mut Vec<u8>, field: u32, payload: &[u8]) {
    put_varint(buf, ((field as u64) << 3) | 2);
    put_varint(buf, payload.len() as u64);
    buf.extend_from_slice(payload);
}

fn string_field(buf: &mut Vec<u8>, field: u32, s: &str) {
    ld_field(buf, field, s.as_bytes());
}

fn varint_field(buf: &mut Vec<u8>, field: u32, v: u64) {
    // wire type 0 (varint): tag = field << 3
    put_varint(buf, (field as u64) << 3);
    put_varint(buf, v);
}

/// ObjectMeta with just a name (field 1).
fn object_meta(name: &str) -> Vec<u8> {
    let mut m = Vec::new();
    string_field(&mut m, 1, name);
    m
}

/// Wrap native-proto `raw` bytes in a `k8s\0` Unknown envelope whose
/// contentType is the protobuf type (so the middleware does NOT short-circuit
/// to the JSON-extract path).
fn k8s_envelope(api_version: &str, kind: &str, raw: &[u8]) -> Vec<u8> {
    let mut type_meta = Vec::new();
    string_field(&mut type_meta, 1, api_version);
    string_field(&mut type_meta, 2, kind);

    let mut unknown = Vec::new();
    ld_field(&mut unknown, 1, &type_meta); // typeMeta
    ld_field(&mut unknown, 2, raw); // raw
    string_field(&mut unknown, 4, PROTO_CT); // contentType

    let mut out = Vec::with_capacity(4 + unknown.len());
    out.extend_from_slice(b"k8s\0");
    out.extend_from_slice(&unknown);
    out
}

async fn post_proto(router: TestApiServer, uri: &str, body: Vec<u8>) -> (StatusCode, String) {
    let (status, _headers, bytes, _) = router
        .send_with_headers(
            "POST",
            uri,
            &[("content-type", PROTO_CT), ("accept", "application/json")],
            Some(body),
        )
        .await;
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

// ----- Registry parity ------------------------------------------------------
//
// Guard against a future edit that forgets one of the newly-registered
// schemas and silently reintroduces the spec-dropping fallback.

#[test]
fn newly_registered_write_schemas_are_present() {
    let registry = ProtoRegistry::new();
    for name in [
        // flowcontrol.apiserver.k8s.io/v1
        "FlowSchema",
        "FlowSchemaSpec",
        "PriorityLevelConfigurationReference",
        "PolicyRulesWithSubjects",
        "ResourcePolicyRule",
        "NonResourcePolicyRule",
        "PriorityLevelConfiguration",
        "PriorityLevelConfigurationSpec",
        "LimitedPriorityLevelConfiguration",
        "LimitResponse",
        "QueuingConfiguration",
        // node.k8s.io/v1
        "RuntimeClass",
        "Overhead",
        "Scheduling",
        // authentication.k8s.io/v1
        "TokenReview",
        "TokenReviewSpec",
        "TokenReviewStatus",
        "UserInfo",
        "SelfSubjectReview",
        // authentication TokenRequest is registered group-qualified to avoid
        // colliding with the storage/v1 CSI TokenRequest of the same bare name.
        "authentication.k8s.io/v1.TokenRequest",
        "TokenRequestSpec",
    ] {
        assert!(
            registry.decode_message(name, &[]).is_some(),
            "{name} schema must be registered in ProtoRegistry::new (decoder returned None)",
        );
    }
}

// ----- TokenReview ----------------------------------------------------------

#[tokio::test]
async fn tokenreview_native_proto_create_preserves_spec_token() {
    let router = spawn_router();

    // authentication.k8s.io/v1 TokenReview:
    //   1=metadata(ObjectMeta), 2=spec(TokenReviewSpec), 3=status
    // TokenReviewSpec: 1=token(string), 2=audiences(repeated string)
    let mut spec = Vec::new();
    string_field(&mut spec, 1, "deadbeef-token");
    string_field(&mut spec, 2, "https://kubernetes.default.svc");

    let mut tr = Vec::new();
    ld_field(&mut tr, 1, &object_meta(""));
    ld_field(&mut tr, 2, &spec);

    let env = k8s_envelope("authentication.k8s.io/v1", "TokenReview", &tr);
    let (status, body) =
        post_proto(router, "/apis/authentication.k8s.io/v1/tokenreviews", env).await;

    assert!(
        status.is_success(),
        "TokenReview native-proto POST must succeed; got {status}, body={body}",
    );
    // The echoed object must carry a status computed from the supplied token,
    // proving spec.token survived the decode. An empty/dropped token would
    // make the server authenticate the empty string.
    let v: serde_json::Value = serde_json::from_str(&body).expect("response is JSON");
    let echoed_token = v
        .get("spec")
        .and_then(|s| s.get("token"))
        .and_then(|t| t.as_str())
        .unwrap_or("");
    assert_eq!(
        echoed_token, "deadbeef-token",
        "spec.token must survive native-proto decode; body={body}",
    );
}

// ----- RuntimeClass ---------------------------------------------------------

#[tokio::test]
async fn runtimeclass_native_proto_create_preserves_handler() {
    let router = spawn_router();

    // node.k8s.io/v1 RuntimeClass:
    //   1=metadata, 2=handler(string), 3=overhead, 4=scheduling
    let mut rc = Vec::new();
    ld_field(&mut rc, 1, &object_meta("rc-proto"));
    string_field(&mut rc, 2, "runc");

    let env = k8s_envelope("node.k8s.io/v1", "RuntimeClass", &rc);
    let (status, body) = post_proto(router, "/apis/node.k8s.io/v1/runtimeclasses", env).await;

    assert!(
        status.is_success(),
        "RuntimeClass native-proto POST must succeed; got {status}, body={body}",
    );
    let v: serde_json::Value = serde_json::from_str(&body).expect("response is JSON");
    assert_eq!(
        v.get("handler").and_then(|h| h.as_str()),
        Some("runc"),
        "RuntimeClass.handler must survive native-proto decode; body={body}",
    );
}

// ----- FlowSchema -----------------------------------------------------------

#[tokio::test]
async fn flowschema_native_proto_create_preserves_spec() {
    let router = spawn_router();

    // flowcontrol.apiserver.k8s.io/v1 FlowSchema:
    //   1=metadata, 2=spec(FlowSchemaSpec), 3=status
    // FlowSchemaSpec: 1=priorityLevelConfiguration(ref), 2=matchingPrecedence(int32),
    //                 3=distinguisherMethod, 4=rules
    // PriorityLevelConfigurationReference: 1=name(string)
    let mut plc_ref = Vec::new();
    string_field(&mut plc_ref, 1, "exempt");

    let mut spec = Vec::new();
    ld_field(&mut spec, 1, &plc_ref);
    varint_field(&mut spec, 2, 1000); // matchingPrecedence

    let mut fs = Vec::new();
    ld_field(&mut fs, 1, &object_meta("fs-proto"));
    ld_field(&mut fs, 2, &spec);

    let env = k8s_envelope("flowcontrol.apiserver.k8s.io/v1", "FlowSchema", &fs);
    let (status, body) = post_proto(
        router,
        "/apis/flowcontrol.apiserver.k8s.io/v1/flowschemas",
        env,
    )
    .await;

    assert!(
        status.is_success(),
        "FlowSchema native-proto POST must succeed; got {status}, body={body}",
    );
    let v: serde_json::Value = serde_json::from_str(&body).expect("response is JSON");
    assert_eq!(
        v.get("spec")
            .and_then(|s| s.get("matchingPrecedence"))
            .and_then(|m| m.as_i64()),
        Some(1000),
        "FlowSchema.spec.matchingPrecedence must survive decode; body={body}",
    );
    assert_eq!(
        v.get("spec")
            .and_then(|s| s.get("priorityLevelConfiguration"))
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str()),
        Some("exempt"),
        "FlowSchema.spec.priorityLevelConfiguration.name must survive decode; body={body}",
    );
}

// ----- PriorityLevelConfiguration ------------------------------------------

#[tokio::test]
async fn prioritylevelconfiguration_native_proto_create_preserves_spec() {
    let router = spawn_router();

    // flowcontrol.apiserver.k8s.io/v1 PriorityLevelConfiguration:
    //   1=metadata, 2=spec(PriorityLevelConfigurationSpec), 3=status
    // PriorityLevelConfigurationSpec: 1=type(string), 2=limited, 3=exempt
    // LimitedPriorityLevelConfiguration: 1=nominalConcurrencyShares(int32),
    //   2=limitResponse, 3=lendingLimit, 4=borrowingLimit
    let mut limited = Vec::new();
    varint_field(&mut limited, 1, 30); // nominalConcurrencyShares

    let mut spec = Vec::new();
    string_field(&mut spec, 1, "Limited"); // type
    ld_field(&mut spec, 2, &limited);

    let mut plc = Vec::new();
    ld_field(&mut plc, 1, &object_meta("plc-proto"));
    ld_field(&mut plc, 2, &spec);

    let env = k8s_envelope(
        "flowcontrol.apiserver.k8s.io/v1",
        "PriorityLevelConfiguration",
        &plc,
    );
    let (status, body) = post_proto(
        router,
        "/apis/flowcontrol.apiserver.k8s.io/v1/prioritylevelconfigurations",
        env,
    )
    .await;

    assert!(
        status.is_success(),
        "PriorityLevelConfiguration native-proto POST must succeed; got {status}, body={body}",
    );
    let v: serde_json::Value = serde_json::from_str(&body).expect("response is JSON");
    assert_eq!(
        v.get("spec")
            .and_then(|s| s.get("type"))
            .and_then(|t| t.as_str()),
        Some("Limited"),
        "PLC.spec.type must survive decode; body={body}",
    );
    assert_eq!(
        v.get("spec")
            .and_then(|s| s.get("limited"))
            .and_then(|l| l.get("nominalConcurrencyShares"))
            .and_then(|n| n.as_i64()),
        Some(30),
        "PLC.spec.limited.nominalConcurrencyShares must survive decode; body={body}",
    );
}
