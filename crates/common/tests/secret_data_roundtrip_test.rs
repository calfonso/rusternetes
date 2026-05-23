//! JSON roundtrip tests for `Secret.data` and `Secret.stringData`.
//!
//! Mirrors the layer at upstream
//! `staging/src/k8s.io/apimachinery/pkg/api/apitesting/roundtrip/`.
//!
//! Upstream `Secret` (k8s.io/api/core/v1/types.go):
//!
//! ```text
//! Data       map[string][]byte  // proto field 2, base64 on the wire
//! StringData map[string]string  // proto field 4, write-only convenience
//!                               // (`json:"stringData,omitempty"`, not
//!                               // round-tripped back from typed clients)
//! ```
//!
//! These tests pin the JSON wire fidelity of both fields against the typed
//! `rusternetes_common::resources::Secret` decoder + encoder. For each
//! fixture we assert that:
//!
//! 1. `serde_json::from_str::<Secret>` succeeds.
//! 2. `serde_json::to_string(&decoded)` succeeds.
//! 3. The re-encoded JSON deserializes again.
//! 4. The two decoded values compare equal as `serde_json::Value`.
//!
//! We compare as `Value` (not by `PartialEq`) so the test cares about the
//! *wire shape*, not field ordering or struct identity. This is the same
//! invariant `crates/common/tests/roundtrip_core_v1.rs` enforces for the
//! rest of core/v1.
//!
//! Edge cases covered (per the worker spec):
//!   - empty `data` value (`""` → `[]` bytes)
//!   - binary content with embedded NUL and high bytes (`[0x00, 0xFF, 0x42]`)
//!   - unicode in `stringData` (write-only field stays as string)
//!   - a large (~10 KiB) value to make sure the base64-roundtrip path
//!     scales beyond the small-fixture happy case
//!   - the `kubernetes.io/dockerconfigjson` type (data key is `.dockerconfigjson`)
//!   - `data` and `stringData` populated on the same secret (the upstream
//!     "tls" pattern) — both must survive the wire trip independently.

use base64::Engine;
use rusternetes_common::resources::Secret;
use serde::{de::DeserializeOwned, Serialize};

/// Same shape as `assert_roundtrip` in `roundtrip_core_v1.rs`. Re-implemented
/// locally because the helper there is `fn`-private to that module.
fn assert_roundtrip<T>(fixture: &str)
where
    T: Serialize + DeserializeOwned,
{
    let decoded: T = serde_json::from_str(fixture)
        .unwrap_or_else(|e| panic!("initial decode failed: {e}\nfixture: {fixture}"));
    let re_encoded = serde_json::to_string(&decoded).expect("re-encode failed");
    let re_decoded: T = serde_json::from_str(&re_encoded)
        .unwrap_or_else(|e| panic!("second decode failed: {e}\nre-encoded: {re_encoded}"));

    let decoded_value = serde_json::to_value(&decoded).expect("decoded -> Value");
    let re_decoded_value = serde_json::to_value(&re_decoded).expect("re_decoded -> Value");
    assert_eq!(
        decoded_value, re_decoded_value,
        "roundtrip not stable\nfirst:  {decoded_value}\nsecond: {re_decoded_value}",
    );
}

/// Standard-padded base64 of `bytes`, matching what client-go writes for
/// `Secret.data` values on the JSON wire.
fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

// =============================================================================
// Secret.data
// =============================================================================

#[test]
fn roundtrip_secret_data_empty_value() {
    // base64 of [] is "" — the empty byte string. Upstream accepts this and
    // emits it back as `""` on the wire.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "empty-val", "namespace": "default"},
        "type": "Opaque",
        "data": {
            "empty": ""
        }
    }"#;
    assert_roundtrip::<Secret>(fixture);
}

#[test]
fn roundtrip_secret_data_binary_with_nul_and_high_bytes() {
    // Worker spec: `value=[0x00, 0xFF, 0x42]`. base64("AP9C") under standard
    // padded encoding. This is the canonical "binary secret payload" case —
    // raw bytes that are not valid UTF-8 must survive the base64 wire trip.
    let payload = [0x00u8, 0xFF, 0x42];
    let encoded = b64(&payload);
    assert_eq!(encoded, "AP9C", "sanity-check base64 encoding");

    let fixture = format!(
        r#"{{
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {{"name": "binary", "namespace": "default"}},
            "type": "Opaque",
            "data": {{
                "api-key": "{encoded}"
            }}
        }}"#
    );
    assert_roundtrip::<Secret>(&fixture);

    // And confirm the decoded `Vec<u8>` actually matches the raw bytes — not
    // just round-trips as a value. This guards against a future change that
    // silently re-encodes the data via `String` and loses non-UTF-8 bytes.
    let secret: Secret = serde_json::from_str(&fixture).unwrap();
    let data = secret.data.as_ref().expect("data must be set");
    assert_eq!(
        data.get("api-key").map(Vec::as_slice),
        Some(payload.as_slice()),
        "non-UTF-8 secret bytes must decode losslessly",
    );
}

#[test]
fn roundtrip_secret_data_all_byte_values() {
    // Sweep 0..=255 in a single value — any byte that gets mangled by an
    // accidental UTF-8 conversion shows up here. base64 of this sequence is
    // a deterministic ~344-char string; we don't hard-code it, just trust
    // the encoder/decoder symmetry.
    let payload: Vec<u8> = (0u8..=255).collect();
    let encoded = b64(&payload);

    let fixture = format!(
        r#"{{
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {{"name": "all-bytes", "namespace": "default"}},
            "type": "Opaque",
            "data": {{
                "sweep": "{encoded}"
            }}
        }}"#
    );
    assert_roundtrip::<Secret>(&fixture);

    let secret: Secret = serde_json::from_str(&fixture).unwrap();
    let data = secret.data.as_ref().expect("data must be set");
    assert_eq!(
        data.get("sweep").map(Vec::as_slice),
        Some(payload.as_slice()),
        "0..=255 byte sweep must decode losslessly",
    );
}

#[test]
fn roundtrip_secret_data_large_value() {
    // ~10 KiB — large enough to land outside any small-string optimization
    // path the json/base64 stack might take. Filled with a repeating
    // pattern so we can verify the decoded length without holding the
    // raw bytes in the assertion.
    let payload: Vec<u8> = (0..10_240).map(|i| (i % 251) as u8).collect();
    assert_eq!(payload.len(), 10_240);
    let encoded = b64(&payload);

    let fixture = format!(
        r#"{{
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {{"name": "big", "namespace": "default"}},
            "type": "Opaque",
            "data": {{
                "blob": "{encoded}"
            }}
        }}"#
    );
    assert_roundtrip::<Secret>(&fixture);

    let secret: Secret = serde_json::from_str(&fixture).unwrap();
    let data = secret.data.as_ref().expect("data must be set");
    let decoded = data.get("blob").expect("blob must be set");
    assert_eq!(
        decoded.len(),
        10_240,
        "large secret value must not be truncated"
    );
    assert_eq!(
        decoded, &payload,
        "large secret payload must roundtrip exactly"
    );
}

#[test]
fn roundtrip_secret_dockerconfigjson() {
    // kubernetes.io/dockerconfigjson uses a fixed key `.dockerconfigjson`
    // whose value is base64-encoded JSON. This pins both the type tag and
    // the dotted-key fidelity, which is the exact shape pull-secret
    // controllers emit.
    let dockercfg = br#"{"auths":{"reg.example.com":{"auth":"dXNlcjpwYXNz"}}}"#;
    let encoded = b64(dockercfg);

    let fixture = format!(
        r#"{{
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {{"name": "regcred", "namespace": "default"}},
            "type": "kubernetes.io/dockerconfigjson",
            "data": {{
                ".dockerconfigjson": "{encoded}"
            }}
        }}"#
    );
    assert_roundtrip::<Secret>(&fixture);
}

// =============================================================================
// Secret.stringData
// =============================================================================

#[test]
fn roundtrip_secret_string_data_unicode() {
    // stringData carries plain UTF-8 strings, *not* base64. The wire form
    // is `"stringData": {"key": "value"}` with the value left verbatim.
    // We use a mix of ASCII, Latin-1, CJK and an emoji to make sure the
    // serde path stays UTF-8-clean.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "i18n", "namespace": "default"},
        "type": "Opaque",
        "stringData": {
            "greeting.en": "hello",
            "greeting.de": "grüße",
            "greeting.jp": "こんにちは",
            "greeting.emoji": "👋🌍"
        }
    }"#;
    assert_roundtrip::<Secret>(fixture);

    // Cross-check the typed value still contains the raw unicode strings.
    let secret: Secret = serde_json::from_str(fixture).unwrap();
    let string_data = secret.string_data.as_ref().expect("stringData must be set");
    assert_eq!(
        string_data.get("greeting.jp").map(String::as_str),
        Some("こんにちは")
    );
    assert_eq!(
        string_data.get("greeting.emoji").map(String::as_str),
        Some("👋🌍")
    );
}

#[test]
fn roundtrip_secret_string_data_only_no_data_field() {
    // stringData on a write — `data` is absent. Round-trip must keep
    // `stringData` and must not synthesise a phantom `data` key.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "write-only", "namespace": "default"},
        "type": "Opaque",
        "stringData": {
            "config.yaml": "log_level: info\nport: 8080\n"
        }
    }"#;
    assert_roundtrip::<Secret>(fixture);

    let secret: Secret = serde_json::from_str(fixture).unwrap();
    assert!(
        secret.data.is_none(),
        "data must stay None when only stringData is set"
    );
    assert!(
        secret.string_data.is_some(),
        "stringData must survive the decode"
    );

    // Re-encode and check the wire shape directly: stringData must round
    // trip with the multi-line value intact; data must not appear.
    let value: serde_json::Value = serde_json::to_value(&secret).unwrap();
    let string_data = value
        .get("stringData")
        .and_then(|v| v.as_object())
        .expect("stringData must serialise as an object");
    assert_eq!(
        string_data.get("config.yaml").and_then(|v| v.as_str()),
        Some("log_level: info\nport: 8080\n"),
    );
    assert!(
        value.get("data").is_none(),
        "data must not appear on the wire when unset"
    );
}

#[test]
fn roundtrip_secret_data_and_string_data_coexist() {
    // The "tls" pattern: a typed Secret with both `data` (binary cert
    // payload, base64) AND `stringData` (convenience field, e.g. inline
    // tls.key for kubectl apply). Both must survive the wire trip.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "mixed", "namespace": "default"},
        "type": "kubernetes.io/tls",
        "data": {
            "tls.crt": "Y2VydA=="
        },
        "stringData": {
            "tls.key": "-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----"
        }
    }"#;
    assert_roundtrip::<Secret>(fixture);

    let secret: Secret = serde_json::from_str(fixture).unwrap();
    assert_eq!(
        secret
            .data
            .as_ref()
            .and_then(|d| d.get("tls.crt"))
            .map(Vec::as_slice),
        Some(b"cert".as_slice()),
        "data.tls.crt must base64-decode to bytes",
    );
    assert!(
        secret
            .string_data
            .as_ref()
            .and_then(|s| s.get("tls.key"))
            .is_some(),
        "stringData.tls.key must be preserved",
    );
}

#[test]
fn roundtrip_secret_string_data_empty_value() {
    // Empty string value in stringData — must survive as `""`, not vanish.
    let fixture = r#"{
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "empty-sd", "namespace": "default"},
        "type": "Opaque",
        "stringData": {
            "blank": ""
        }
    }"#;
    assert_roundtrip::<Secret>(fixture);

    let secret: Secret = serde_json::from_str(fixture).unwrap();
    assert_eq!(
        secret
            .string_data
            .as_ref()
            .and_then(|s| s.get("blank"))
            .map(String::as_str),
        Some(""),
    );
}
