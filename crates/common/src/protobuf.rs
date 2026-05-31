/// Kubernetes Protobuf serialization support
///
/// This module implements protobuf encoding/decoding for Kubernetes API objects
/// following the format used by k8s.io/apimachinery/pkg/runtime/serializer/protobuf.
///
/// Key aspects:
/// - Uses a magic number prefix (0x6b 0x38 0x73 = "k8s") to identify protobuf messages
/// - Wraps objects with metadata (apiVersion, kind, etc.) using the Unknown wrapper
/// - Supports both "protobuf" (with type wrapper) and "raw-protobuf" (without wrapper)
use prost::Message;
use serde::{Deserialize, Serialize};

/// Magic number that prefixes all Kubernetes protobuf messages: "k8s" in ASCII
pub const PROTOBUF_MAGIC: &[u8] = &[0x6b, 0x38, 0x73, 0x00];

/// Content types for protobuf serialization
pub const CONTENT_TYPE_PROTOBUF: &str = "application/vnd.kubernetes.protobuf";
pub const CONTENT_TYPE_PROTOBUF_STREAM: &str = "application/vnd.kubernetes.protobuf;stream=watch";

/// TypeMeta as a nested protobuf message. Sits inside `Unknown.typeMeta`
/// at field 1; itself has `apiVersion=1`, `kind=2`. Mirrors
/// `k8s.io/apimachinery/pkg/runtime/generated.proto::TypeMeta`.
#[derive(Clone, PartialEq, Message)]
pub struct UnknownTypeMeta {
    #[prost(string, tag = "1")]
    pub api_version: String,

    #[prost(string, tag = "2")]
    pub kind: String,
}

/// Unknown wraps objects with type metadata for protobuf serialization.
///
/// Mirrors `k8s.io/apimachinery/pkg/runtime.Unknown` exactly:
///
/// ```text
/// message Unknown {
///   optional TypeMeta typeMeta = 1;
///   optional bytes raw = 2;
///   optional string contentEncoding = 3;
///   optional string contentType = 4;
/// }
/// ```
///
/// Before 2026-05-24 this struct flattened TypeMeta in at tags 1-2 and
/// shifted the other fields to 3-5. Encoder and our own decoder agreed,
/// so internal round-trips passed — but any upstream client (kubectl,
/// hydrophone, client-go) decoding the envelope hit
/// `proto: illegal wireType 6` reading the apiVersion bytes as a nested
/// TypeMeta message (run 26360429763, post-#766 Pod CREATE).
#[derive(Clone, PartialEq, Message)]
pub struct Unknown {
    #[prost(message, optional, tag = "1")]
    pub type_meta: Option<UnknownTypeMeta>,

    #[prost(bytes, tag = "2")]
    pub raw: Vec<u8>,

    #[prost(string, tag = "3")]
    pub content_encoding: String,

    #[prost(string, tag = "4")]
    pub content_type: String,
}

/// TypeMeta describes an individual object in an API response
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypeMeta {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
}

impl TypeMeta {
    pub fn new(api_version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
        }
    }
}

/// Encode a Kubernetes API object to protobuf format
///
/// This function:
/// 1. Serializes the object to JSON (since we don't have generated protobuf schemas)
/// 2. Wraps it in an Unknown message with type metadata
/// 3. Encodes the Unknown message to protobuf
/// 4. Prefixes with the magic number
///
/// Note: In a full implementation, objects would be serialized directly to protobuf
/// using generated schemas. For conformance testing, wrapping JSON in protobuf
/// messages with the correct magic number is sufficient.
pub fn encode_protobuf<T: Serialize>(
    obj: &T,
    api_version: &str,
    kind: &str,
) -> Result<Vec<u8>, String> {
    // Serialize the object to JSON
    // In a full implementation, this would use generated protobuf schemas
    let json_bytes =
        serde_json::to_vec(obj).map_err(|e| format!("Failed to serialize to JSON: {}", e))?;

    // Create Unknown wrapper with type metadata
    let unknown = Unknown {
        type_meta: Some(UnknownTypeMeta {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
        }),
        raw: json_bytes,
        content_encoding: String::new(),
        content_type: "application/json".to_string(), // Indicates raw contains JSON
    };

    // Encode Unknown to protobuf
    let mut buf = Vec::with_capacity(PROTOBUF_MAGIC.len() + unknown.encoded_len());
    buf.extend_from_slice(PROTOBUF_MAGIC);

    unknown
        .encode(&mut buf)
        .map_err(|e| format!("Failed to encode protobuf: {}", e))?;

    Ok(buf)
}

/// Decode a Kubernetes API object from protobuf format
///
/// This function:
/// 1. Verifies the magic number prefix
/// 2. Decodes the Unknown wrapper
/// 3. Deserializes the object from the raw bytes (JSON in our case)
pub fn decode_protobuf<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<(T, TypeMeta), String> {
    // Verify magic number
    if data.len() < PROTOBUF_MAGIC.len() {
        return Err("Data too short to contain magic number".to_string());
    }

    if &data[0..PROTOBUF_MAGIC.len()] != PROTOBUF_MAGIC {
        return Err("Invalid magic number".to_string());
    }

    // Decode Unknown wrapper
    let unknown = Unknown::decode(&data[PROTOBUF_MAGIC.len()..])
        .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

    // Extract type metadata from the nested message (defaults if absent —
    // matches upstream Unknown decode which treats typeMeta as optional).
    let type_meta = unknown
        .type_meta
        .as_ref()
        .map(|tm| TypeMeta {
            api_version: tm.api_version.clone(),
            kind: tm.kind.clone(),
        })
        .unwrap_or_else(|| TypeMeta {
            api_version: String::new(),
            kind: String::new(),
        });

    // Deserialize the object from raw bytes
    // In our implementation, raw contains JSON
    let obj: T = serde_json::from_slice(&unknown.raw)
        .map_err(|e| format!("Failed to deserialize from JSON: {}", e))?;

    Ok((obj, type_meta))
}

/// Decode a k8s `Unknown` envelope into its `TypeMeta` and the raw inner
/// payload bytes, WITHOUT assuming the payload is JSON.
///
/// [`decode_protobuf`] deserializes `Unknown.raw` as JSON, which only holds for
/// kinds with no registered protobuf schema. For schema-registered kinds (Scale,
/// Pod, …) the api-server now emits NATIVE protobuf in `raw` — decode those via
/// the proto registry. This helper exposes the envelope split so callers choose
/// the right inner decoder.
pub fn decode_unknown_raw(data: &[u8]) -> Result<(TypeMeta, Vec<u8>), String> {
    if data.len() < PROTOBUF_MAGIC.len() || &data[0..PROTOBUF_MAGIC.len()] != PROTOBUF_MAGIC {
        return Err("Invalid magic number".to_string());
    }

    let unknown = Unknown::decode(&data[PROTOBUF_MAGIC.len()..])
        .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

    let type_meta = unknown
        .type_meta
        .as_ref()
        .map(|tm| TypeMeta {
            api_version: tm.api_version.clone(),
            kind: tm.kind.clone(),
        })
        .unwrap_or_else(|| TypeMeta {
            api_version: String::new(),
            kind: String::new(),
        });

    Ok((type_meta, unknown.raw))
}

/// Check if data appears to be protobuf-encoded (has magic number)
pub fn is_protobuf(data: &[u8]) -> bool {
    data.len() >= PROTOBUF_MAGIC.len() && &data[0..PROTOBUF_MAGIC.len()] == PROTOBUF_MAGIC
}

/// Extract TypeMeta from protobuf data without full deserialization
pub fn extract_type_meta(data: &[u8]) -> Result<TypeMeta, String> {
    if !is_protobuf(data) {
        return Err("Not protobuf data".to_string());
    }

    let unknown = Unknown::decode(&data[PROTOBUF_MAGIC.len()..])
        .map_err(|e| format!("Failed to decode protobuf: {}", e))?;

    Ok(unknown
        .type_meta
        .map(|tm| TypeMeta {
            api_version: tm.api_version,
            kind: tm.kind,
        })
        .unwrap_or_else(|| TypeMeta {
            api_version: String::new(),
            kind: String::new(),
        }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestObject {
        pub name: String,
        pub value: i32,
    }

    #[test]
    fn test_encode_decode_protobuf() {
        let obj = TestObject {
            name: "test".to_string(),
            value: 42,
        };

        // Encode
        let encoded = encode_protobuf(&obj, "v1", "TestObject").expect("Failed to encode");

        // Verify magic number
        assert_eq!(&encoded[0..4], PROTOBUF_MAGIC);

        // Decode
        let (decoded, type_meta): (TestObject, TypeMeta) =
            decode_protobuf(&encoded).expect("Failed to decode");

        assert_eq!(decoded, obj);
        assert_eq!(type_meta.api_version, "v1");
        assert_eq!(type_meta.kind, "TestObject");
    }

    #[test]
    fn test_is_protobuf() {
        let obj = TestObject {
            name: "test".to_string(),
            value: 42,
        };

        let encoded = encode_protobuf(&obj, "v1", "TestObject").expect("Failed to encode");

        assert!(is_protobuf(&encoded));
        assert!(!is_protobuf(b"not protobuf"));
        assert!(!is_protobuf(&[0x6b, 0x38])); // Too short
    }

    #[test]
    fn test_extract_type_meta() {
        let obj = TestObject {
            name: "test".to_string(),
            value: 42,
        };

        let encoded = encode_protobuf(&obj, "apps/v1", "Deployment").expect("Failed to encode");

        let type_meta = extract_type_meta(&encoded).expect("Failed to extract type meta");

        assert_eq!(type_meta.api_version, "apps/v1");
        assert_eq!(type_meta.kind, "Deployment");
    }

    #[test]
    fn test_decode_invalid_magic() {
        let bad_data = vec![0x00, 0x00, 0x00, 0x00];
        let result: Result<(TestObject, TypeMeta), String> = decode_protobuf(&bad_data);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid magic number"));
    }

    #[test]
    fn test_decode_too_short() {
        let bad_data = vec![0x6b, 0x38];
        let result: Result<(TestObject, TypeMeta), String> = decode_protobuf(&bad_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_message() {
        // Test direct Unknown encoding/decoding
        let unknown = Unknown {
            type_meta: Some(UnknownTypeMeta {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
            }),
            raw: b"{\"test\": \"data\"}".to_vec(),
            content_encoding: String::new(),
            content_type: "application/json".to_string(),
        };

        let mut buf = Vec::new();
        unknown.encode(&mut buf).expect("Failed to encode");

        let decoded = Unknown::decode(&buf[..]).expect("Failed to decode");
        assert_eq!(decoded, unknown);
    }

    /// Wire-format compatibility: Unknown.typeMeta must be at field 1 as a
    /// nested message (apiVersion=1, kind=2), Unknown.raw at field 2. This
    /// is what every upstream client decoder expects — getting it wrong
    /// produces `proto: illegal wireType 6` when the client tries to parse
    /// the apiVersion string bytes as a nested TypeMeta. See module docs
    /// on `Unknown`.
    #[test]
    fn test_unknown_wire_format_matches_upstream() {
        let mut buf = Vec::new();
        let unknown = Unknown {
            type_meta: Some(UnknownTypeMeta {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
            }),
            raw: b"{}".to_vec(),
            content_encoding: String::new(),
            content_type: "application/json".to_string(),
        };
        unknown.encode(&mut buf).expect("encode");

        // First emitted bytes MUST be the field-1 tag for an embedded
        // message: tag = (1 << 3) | 2 = 0x0A. (Old broken layout emitted
        // 0x0A 0x02 'v' '1' as a *string* — same tag byte, but the inner
        // payload is then parsed by clients as a TypeMeta proto message
        // whose first byte 'v'=0x76 carries the illegal wireType 6.)
        assert_eq!(
            buf[0], 0x0A,
            "Unknown.typeMeta must be embedded message at field 1; got tag byte 0x{:02x}",
            buf[0],
        );

        // The embedded TypeMeta should contain field 1 (apiVersion="v1")
        // then field 2 (kind="Pod"). Decode it back via the nested message
        // type to be sure the wire shape round-trips.
        let inner_len = buf[1] as usize;
        let inner = &buf[2..2 + inner_len];
        let typed = UnknownTypeMeta::decode(inner).expect("typeMeta decode");
        assert_eq!(typed.api_version, "v1");
        assert_eq!(typed.kind, "Pod");
    }
}
