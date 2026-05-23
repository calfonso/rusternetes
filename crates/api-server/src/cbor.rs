//! CBOR codec for the Kubernetes `application/cbor` content type.
//!
//! Mirrors upstream Kubernetes' CBOR serializer in
//! `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/cbor/`. The wire
//! format is plain CBOR (RFC 8949) — there is no envelope analogous to the
//! `k8s\0` prefix used by `application/vnd.kubernetes.protobuf`.
//!
//! Upstream supports two CBOR media types:
//!
//! - `application/cbor` — full object encoding for create/update/get/list.
//! - `application/apply-patch+cbor` — CBOR-encoded server-side apply patch;
//!   the constant is exported as
//!   [`rusternetes_common::validation::metav1::APPLY_CBOR_PATCH_TYPE`].
//!
//! Our strategy is "decode CBOR → JSON value, re-encode as JSON" on the
//! request side, and "encode JSON value → CBOR" on the response side. This
//! keeps the handlers JSON-only (Axum's `Json` extractor and `axum::Json`
//! responder remain the canonical interface) while still honoring the wire
//! contract a CBOR-speaking client expects. The round-trip is lossless for
//! every Kubernetes resource because Kubernetes models its types as JSON
//! values to begin with — there is no CBOR-only datum (e.g. raw bytes
//! outside of base64 strings) that loses fidelity here.

use ciborium::value::Value as CborValue;
use serde_json::Value as JsonValue;

/// MIME type for the standard CBOR codec.
///
/// Upstream constant: `runtime.ContentTypeCBOR` in
/// `staging/src/k8s.io/apimachinery/pkg/runtime/types.go`.
pub const CBOR_CONTENT_TYPE: &str = "application/cbor";

/// MIME type for CBOR-encoded server-side apply patches.
///
/// Mirrors `APPLY_CBOR_PATCH_TYPE` exported from
/// `rusternetes_common::validation::metav1`. Kept here as a sibling
/// constant so the codec module can be used standalone in tests.
pub const APPLY_PATCH_CBOR_CONTENT_TYPE: &str = "application/apply-patch+cbor";

/// Errors produced by the CBOR codec helpers.
///
/// Variants are kept narrow on purpose: the middleware classifies any error
/// from this module as `HTTP 400 Bad Request` for request bodies and as
/// `HTTP 500 Internal Server Error` for response encoding (the JSON value
/// came from a handler, so a serialization failure is a server bug).
#[derive(Debug, thiserror::Error)]
pub enum CborError {
    /// The byte slice was not a syntactically valid CBOR item.
    #[error("malformed CBOR: {0}")]
    Decode(String),
    /// The decoded CBOR value cannot be expressed as a JSON value
    /// (e.g. it contains a non-string map key, a CBOR tag, or a value
    /// that is structurally unrepresentable). Upstream's CBOR serializer
    /// reports the same class of failure as a "transcode" error.
    #[error("cannot transcode CBOR to JSON: {0}")]
    Transcode(String),
    /// Encoding a JSON value into CBOR failed. In practice this only
    /// happens if the JSON value contains a non-finite float, which JSON
    /// itself disallows — the variant exists so callers can distinguish
    /// "encoder bug" from "decoder bug" in error logs.
    #[error("failed to encode CBOR: {0}")]
    Encode(String),
}

/// Decode a CBOR-encoded request body into a `serde_json::Value`.
///
/// This is the inverse of [`encode_json_to_cbor`]. It is used by the
/// content-type normalization middleware to translate
/// `application/cbor` requests into the JSON shape that every resource
/// handler already accepts.
pub fn decode_cbor_to_json(bytes: &[u8]) -> Result<JsonValue, CborError> {
    let value: CborValue =
        ciborium::de::from_reader(bytes).map_err(|e| CborError::Decode(format!("{}", e)))?;
    cbor_to_json(value)
}

/// Encode a `serde_json::Value` as CBOR bytes.
///
/// Used by the response wrapping middleware when the client negotiated
/// `application/cbor` via the `Accept` header. Returns canonical CBOR
/// bytes that any RFC 8949 decoder can read.
pub fn encode_json_to_cbor(value: &JsonValue) -> Result<Vec<u8>, CborError> {
    let cbor = json_to_cbor(value);
    let mut buf = Vec::with_capacity(64);
    ciborium::ser::into_writer(&cbor, &mut buf).map_err(|e| CborError::Encode(format!("{}", e)))?;
    Ok(buf)
}

/// Convenience wrapper: decode CBOR and immediately re-emit as canonical
/// JSON bytes. The middleware hands the resulting bytes to the same
/// handler pipeline used for `application/json` requests.
pub fn decode_cbor_to_json_bytes(bytes: &[u8]) -> Result<Vec<u8>, CborError> {
    let value = decode_cbor_to_json(bytes)?;
    serde_json::to_vec(&value).map_err(|e| CborError::Transcode(format!("{}", e)))
}

/// Convert a CBOR value into a JSON value. CBOR has a few constructs that
/// JSON cannot represent — most notably maps with non-string keys, tagged
/// values, and byte strings. We follow upstream's strategy:
///
/// - Byte strings are base64 (RFC 4648 §4) encoded into JSON strings, the
///   same convention `apiserver` uses for `[]byte` proto fields.
/// - Map keys that are not strings are rejected with [`CborError::Transcode`].
/// - Tagged values are unwrapped (the tag is discarded) — upstream behaves
///   the same way because Kubernetes does not assign semantic tags.
fn cbor_to_json(value: CborValue) -> Result<JsonValue, CborError> {
    use base64::Engine;
    match value {
        CborValue::Null => Ok(JsonValue::Null),
        CborValue::Bool(b) => Ok(JsonValue::Bool(b)),
        CborValue::Integer(i) => {
            // ciborium's Integer wraps an i128. Try i64 first (most K8s
            // integers fit), then u64, otherwise fail loudly.
            let as_i128: i128 = i.into();
            if let Ok(v) = i64::try_from(as_i128) {
                Ok(JsonValue::Number(v.into()))
            } else if let Ok(v) = u64::try_from(as_i128) {
                Ok(JsonValue::Number(v.into()))
            } else {
                Err(CborError::Transcode(format!(
                    "integer {} out of JSON number range",
                    as_i128
                )))
            }
        }
        CborValue::Float(f) => serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .ok_or_else(|| {
                CborError::Transcode(format!("non-finite float {} is not JSON-representable", f))
            }),
        CborValue::Text(s) => Ok(JsonValue::String(s)),
        CborValue::Bytes(b) => {
            // Mirrors upstream `kjson.Marshal` behaviour for byte slices:
            // they round-trip as standard base64 strings.
            let encoded = base64::engine::general_purpose::STANDARD.encode(&b);
            Ok(JsonValue::String(encoded))
        }
        CborValue::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(cbor_to_json(item)?);
            }
            Ok(JsonValue::Array(out))
        }
        CborValue::Map(pairs) => {
            let mut out = serde_json::Map::with_capacity(pairs.len());
            for (k, v) in pairs {
                let key = match k {
                    CborValue::Text(s) => s,
                    other => {
                        return Err(CborError::Transcode(format!(
                            "JSON object keys must be strings, got CBOR {:?}",
                            other
                        )));
                    }
                };
                out.insert(key, cbor_to_json(v)?);
            }
            Ok(JsonValue::Object(out))
        }
        CborValue::Tag(_tag, inner) => cbor_to_json(*inner),
        other => Err(CborError::Transcode(format!(
            "unsupported CBOR value: {:?}",
            other
        ))),
    }
}

/// Convert a JSON value into a CBOR value. The reverse of [`cbor_to_json`].
/// JSON numbers are emitted as the smallest CBOR numeric form that can
/// hold them, matching upstream's canonical encoding.
fn json_to_cbor(value: &JsonValue) -> CborValue {
    match value {
        JsonValue::Null => CborValue::Null,
        JsonValue::Bool(b) => CborValue::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                CborValue::Integer(i.into())
            } else if let Some(u) = n.as_u64() {
                CborValue::Integer(u.into())
            } else if let Some(f) = n.as_f64() {
                CborValue::Float(f)
            } else {
                // Unreachable in practice — every JSON Number falls into
                // exactly one of the three branches above.
                CborValue::Null
            }
        }
        JsonValue::String(s) => CborValue::Text(s.clone()),
        JsonValue::Array(items) => CborValue::Array(items.iter().map(json_to_cbor).collect()),
        JsonValue::Object(map) => CborValue::Map(
            map.iter()
                .map(|(k, v)| (CborValue::Text(k.clone()), json_to_cbor(v)))
                .collect(),
        ),
    }
}

/// Return `true` if `content_type` denotes any CBOR media type the
/// apiserver understands. Used by middleware to gate the CBOR codec.
pub fn is_cbor_content_type(content_type: &str) -> bool {
    let base = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    base == CBOR_CONTENT_TYPE || base == APPLY_PATCH_CBOR_CONTENT_TYPE
}

/// Return `true` if the request's `Accept` header asks for CBOR. Matches
/// the same media-range walk upstream does in
/// `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/negotiated_codec_factory.go`:
/// the first range whose base type equals one of the CBOR MIME types wins.
/// Quality factors are not parsed — upstream's apiserver tolerates `q=`
/// but does not actually rank by it for content-type selection.
pub fn accept_wants_cbor(accept: &str) -> bool {
    for range in accept.split(',') {
        let base = range
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        if base == CBOR_CONTENT_TYPE || base == APPLY_PATCH_CBOR_CONTENT_TYPE {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Round-trip a representative ConfigMap shape through CBOR and JSON
    /// to prove the codec preserves the wire-visible structure. Mirrors
    /// the upstream cbor serializer's `TestRoundtrip` in
    /// `staging/src/k8s.io/apimachinery/pkg/runtime/serializer/cbor/cbor_test.go`.
    #[test]
    fn test_roundtrip_configmap() {
        let original = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "cbor-cm",
                "namespace": "default",
            },
            "data": {
                "key": "value",
                "another": "entry",
            }
        });

        let cbor = encode_json_to_cbor(&original).expect("encode");
        let back = decode_cbor_to_json(&cbor).expect("decode");
        assert_eq!(back, original);
    }

    /// `null` is a valid CBOR major-type 7 value. The codec must
    /// preserve it during a pure round-trip so the JSON `null` literal
    /// is not destroyed when a CRD chooses to use it for "field cleared"
    /// semantics.
    #[test]
    fn test_roundtrip_null() {
        let value = JsonValue::Null;
        let cbor = encode_json_to_cbor(&value).expect("encode");
        let back = decode_cbor_to_json(&cbor).expect("decode");
        assert_eq!(back, value);
    }

    /// Integer ranges that fit in i64 must round-trip without going
    /// through floats. K8s uses int64 for several spec fields
    /// (e.g. `metadata.generation`, `spec.replicas`).
    #[test]
    fn test_roundtrip_large_integer() {
        let value = json!({"big": i64::MAX, "small": i64::MIN});
        let cbor = encode_json_to_cbor(&value).expect("encode");
        let back = decode_cbor_to_json(&cbor).expect("decode");
        assert_eq!(back, value);
    }

    /// Arrays of objects (Container[], Toleration[]) are a hot path for
    /// K8s resources — make sure they round-trip element-by-element.
    #[test]
    fn test_roundtrip_array_of_objects() {
        let value = json!({
            "containers": [
                {"name": "c1", "image": "busybox"},
                {"name": "c2", "image": "nginx", "ports": [{"containerPort": 80}]},
            ]
        });
        let cbor = encode_json_to_cbor(&value).expect("encode");
        let back = decode_cbor_to_json(&cbor).expect("decode");
        assert_eq!(back, value);
    }

    /// Malformed bytes produce a `Decode` error — not a panic, not a
    /// silent empty object.
    #[test]
    fn test_decode_malformed_errors() {
        // 0xff is a "break" stop code outside any indefinite-length item.
        let err = decode_cbor_to_json(&[0xff]).expect_err("must reject malformed CBOR");
        match err {
            CborError::Decode(_) => {}
            other => panic!("expected Decode error, got {:?}", other),
        }
    }

    /// Maps with non-string keys cannot survive a JSON round-trip and
    /// must be reported as `Transcode` errors so the caller can return
    /// 400 with a useful message.
    #[test]
    fn test_decode_non_string_key_errors() {
        // Map with one entry: key = unsigned 1, value = unsigned 2.
        // CBOR: 0xa1 (map of 1) 0x01 (uint 1) 0x02 (uint 2).
        let bytes = [0xa1, 0x01, 0x02];
        let err =
            decode_cbor_to_json(&bytes).expect_err("must reject maps with non-string keys");
        match err {
            CborError::Transcode(_) => {}
            other => panic!("expected Transcode error, got {:?}", other),
        }
    }

    #[test]
    fn test_is_cbor_content_type() {
        assert!(is_cbor_content_type("application/cbor"));
        assert!(is_cbor_content_type("application/CBOR"));
        assert!(is_cbor_content_type("application/cbor; charset=utf-8"));
        assert!(is_cbor_content_type("application/apply-patch+cbor"));
        assert!(!is_cbor_content_type("application/json"));
        assert!(!is_cbor_content_type("application/vnd.kubernetes.protobuf"));
        assert!(!is_cbor_content_type(""));
    }

    #[test]
    fn test_accept_wants_cbor() {
        assert!(accept_wants_cbor("application/cbor"));
        assert!(accept_wants_cbor("application/json, application/cbor"));
        assert!(accept_wants_cbor(
            "application/json;q=0.9, application/cbor;q=1.0"
        ));
        assert!(accept_wants_cbor("application/apply-patch+cbor"));
        assert!(!accept_wants_cbor("application/json"));
        assert!(!accept_wants_cbor("*/*"));
        assert!(!accept_wants_cbor(""));
    }

    /// CBOR byte strings come back as base64-encoded JSON strings. This
    /// mirrors `[]byte` round-trips through Go's `encoding/json`.
    #[test]
    fn test_byte_string_becomes_base64() {
        // CBOR: 0x44 (byte string length 4) 0x01 0x02 0x03 0x04
        let bytes = [0x44, 0x01, 0x02, 0x03, 0x04];
        let value = decode_cbor_to_json(&bytes).expect("decode");
        assert_eq!(value, JsonValue::String("AQIDBA==".to_string()));
    }
}
