/// Integration tests for protobuf support in the API server
///
/// These tests verify that the API server can encode responses in protobuf format
/// when requested via the Accept header.
use rusternetes_common::protobuf::{decode_protobuf, encode_protobuf, is_protobuf, TypeMeta};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Pod {
    #[serde(rename = "apiVersion")]
    api_version: String,
    kind: String,
    metadata: Metadata,
    spec: PodSpec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Metadata {
    name: String,
    namespace: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PodSpec {
    containers: Vec<Container>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Container {
    name: String,
    image: String,
}

#[test]
fn test_protobuf_encode_decode_pod() {
    let pod = Pod {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        metadata: Metadata {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
        },
        spec: PodSpec {
            containers: vec![Container {
                name: "nginx".to_string(),
                image: "nginx:latest".to_string(),
            }],
        },
    };

    // Encode to protobuf
    let encoded = encode_protobuf(&pod, "v1", "Pod").expect("Failed to encode pod to protobuf");

    // Verify it's protobuf format
    assert!(is_protobuf(&encoded), "Encoded data should be protobuf");

    // Decode from protobuf
    let (decoded_pod, type_meta): (Pod, TypeMeta) =
        decode_protobuf(&encoded).expect("Failed to decode pod from protobuf");

    // Verify the data matches
    assert_eq!(decoded_pod, pod);
    assert_eq!(type_meta.api_version, "v1");
    assert_eq!(type_meta.kind, "Pod");
}

#[test]
fn test_protobuf_with_list() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct PodList {
        #[serde(rename = "apiVersion")]
        api_version: String,
        kind: String,
        items: Vec<Pod>,
    }

    let pod_list = PodList {
        api_version: "v1".to_string(),
        kind: "PodList".to_string(),
        items: vec![
            Pod {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                metadata: Metadata {
                    name: "pod1".to_string(),
                    namespace: "default".to_string(),
                },
                spec: PodSpec {
                    containers: vec![Container {
                        name: "nginx".to_string(),
                        image: "nginx:1.14".to_string(),
                    }],
                },
            },
            Pod {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                metadata: Metadata {
                    name: "pod2".to_string(),
                    namespace: "default".to_string(),
                },
                spec: PodSpec {
                    containers: vec![Container {
                        name: "redis".to_string(),
                        image: "redis:6".to_string(),
                    }],
                },
            },
        ],
    };

    // Encode to protobuf
    let encoded =
        encode_protobuf(&pod_list, "v1", "PodList").expect("Failed to encode pod list to protobuf");

    // Verify it's protobuf format
    assert!(is_protobuf(&encoded), "Encoded data should be protobuf");

    // Decode from protobuf
    let (decoded_list, type_meta): (PodList, TypeMeta) =
        decode_protobuf(&encoded).expect("Failed to decode pod list from protobuf");

    // Verify the data matches
    assert_eq!(decoded_list, pod_list);
    assert_eq!(type_meta.api_version, "v1");
    assert_eq!(type_meta.kind, "PodList");
    assert_eq!(decoded_list.items.len(), 2);
}

// TODO: Re-enable when encode_response is implemented
#[test]
#[ignore]
fn test_content_negotiation() {
    use axum::http::{header, HeaderMap};
    use rusternetes_api_server::response::{negotiate_content_type, ContentType};

    let _pod = Pod {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        metadata: Metadata {
            name: "test-pod".to_string(),
            namespace: "default".to_string(),
        },
        spec: PodSpec {
            containers: vec![Container {
                name: "nginx".to_string(),
                image: "nginx:latest".to_string(),
            }],
        },
    };

    // Test JSON negotiation
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT, "application/json".parse().unwrap());
    let content_type = negotiate_content_type(&headers);
    assert_eq!(content_type, ContentType::Json);

    // TODO: Re-enable when encode_response is implemented
    // let (bytes, mime) = encode_response(&pod, content_type, "v1", "Pod")
    //     .expect("Failed to encode to JSON");
    // assert_eq!(mime, "application/json");
    // let decoded: Pod = serde_json::from_slice(&bytes).expect("Failed to decode JSON");
    // assert_eq!(decoded, pod);

    // Test Protobuf negotiation
    let mut headers = HeaderMap::new();
    headers.insert(
        header::ACCEPT,
        "application/vnd.kubernetes.protobuf".parse().unwrap(),
    );
    let content_type = negotiate_content_type(&headers);
    assert_eq!(content_type, ContentType::Protobuf);

    // TODO: Re-enable when encode_response is implemented
    // let (bytes, mime) = encode_response(&pod, content_type, "v1", "Pod")
    //     .expect("Failed to encode to protobuf");
    // assert_eq!(mime, "application/vnd.kubernetes.protobuf");
    // assert!(is_protobuf(&bytes));
    //
    // let (decoded, type_meta): (Pod, TypeMeta) =
    //     decode_protobuf(&bytes).expect("Failed to decode protobuf");
    // assert_eq!(decoded, pod);
    // assert_eq!(type_meta.api_version, "v1");
    // assert_eq!(type_meta.kind, "Pod");
}
