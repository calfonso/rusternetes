use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};

/// Endpoints is a collection of endpoints that implement the actual service.
/// It tracks the IP addresses and ports of Pods that match a Service's selector.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Endpoints {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    pub subsets: Vec<EndpointSubset>,
}

impl Endpoints {
    pub fn new(name: impl Into<String>, subsets: Vec<EndpointSubset>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Endpoints".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            subsets,
        }
    }

    /// Create empty endpoints (no ready pods yet)
    pub fn new_empty(name: impl Into<String>) -> Self {
        Self::new(name, vec![])
    }
}

/// EndpointSubset is a group of addresses with a common set of ports.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointSubset {
    /// IP addresses which offer the related ports that are marked as ready.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addresses: Option<Vec<EndpointAddress>>,

    /// IP addresses which offer the related ports but are not currently marked as ready.
    #[serde(
        rename = "notReadyAddresses",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub not_ready_addresses: Option<Vec<EndpointAddress>>,

    /// Port numbers available on the related IP addresses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<EndpointPort>>,
}

/// EndpointAddress is a tuple that describes single IP address.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointAddress {
    /// The IP of this endpoint.
    #[serde(default, deserialize_with = "crate::types::deserialize_null_string")]
    pub ip: String,

    /// The Hostname of this endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,

    /// Optional: Node hosting this endpoint. This can be used to determine endpoints local to a node.
    #[serde(skip_serializing_if = "Option::is_none", rename = "nodeName")]
    pub node_name: Option<String>,

    /// Reference to object providing the endpoint.
    #[serde(skip_serializing_if = "Option::is_none", rename = "targetRef")]
    pub target_ref: Option<EndpointReference>,
}

/// EndpointPort is a tuple that describes a single port.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointPort {
    /// The name of this port. Must match a port name in the Service.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// The port number of the endpoint.
    pub port: u16,

    /// The IP protocol for this port. Must be UDP, TCP, or SCTP.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,

    /// The application protocol for this port.
    #[serde(skip_serializing_if = "Option::is_none", rename = "appProtocol")]
    pub app_protocol: Option<String>,
}

/// EndpointReference contains enough information to let you identify the referenced object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointReference {
    /// Kind of the referent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,

    /// Namespace of the referent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Name of the referent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// UID of the referent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}
