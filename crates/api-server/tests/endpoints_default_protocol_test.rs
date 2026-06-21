//! SetDefaults_Endpoints / SetDefaults_EndpointSlice: port protocol defaults to
//! TCP when omitted.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::json;

const NS: &str = "default";

#[tokio::test]
async fn endpoints_port_protocol_defaults_tcp() {
    let state = TestApiServer::new();
    let uri = format!("/api/v1/namespaces/{NS}/endpoints");
    let ep = json!({
        "apiVersion": "v1", "kind": "Endpoints",
        "metadata": {"name": "ep"},
        "subsets": [{
            "addresses": [{"ip": "10.0.0.1"}],
            "ports": [{"port": 80}]
        }]
    });
    let (code, body) = state.post(&uri, &ep).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["subsets"][0]["ports"][0]["protocol"],
        json!("TCP"),
        "{body}"
    );
}

#[tokio::test]
async fn endpointslice_port_protocol_defaults_tcp() {
    let state = TestApiServer::new();
    let uri = format!("/apis/discovery.k8s.io/v1/namespaces/{NS}/endpointslices");
    let slice = json!({
        "apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
        "metadata": {"name": "es"},
        "addressType": "IPv4",
        "endpoints": [{"addresses": ["10.0.0.1"]}],
        "ports": [{"port": 80}]
    });
    let (code, body) = state.post(&uri, &slice).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(body["ports"][0]["protocol"], json!("TCP"), "{body}");
}
