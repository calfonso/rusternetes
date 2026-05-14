use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};

/// EndpointSlice represents a subset of the endpoints that implement a service.
/// It is part of the discovery.k8s.io/v1 API group.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointSlice {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,

    /// addressType specifies the type of address carried by this EndpointSlice.
    /// All addresses in this slice must be the same type. This field is immutable after creation.
    /// The following address types are currently supported:
    /// * IPv4: Represents an IPv4 Address.
    /// * IPv6: Represents an IPv6 Address.
    /// * FQDN: Represents a Fully Qualified Domain Name.
    #[serde(rename = "addressType")]
    pub address_type: String,

    /// endpoints is a list of unique endpoints in this slice.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<Endpoint>,

    /// ports specifies the list of network ports exposed by each endpoint in this slice.
    #[serde(
        default,
        deserialize_with = "crate::deserialize_null_default",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub ports: Vec<EndpointPort>,
}

impl EndpointSlice {
    pub fn new(name: impl Into<String>, address_type: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "EndpointSlice".to_string(),
                api_version: "discovery.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            address_type: address_type.into(),
            endpoints: vec![],
            ports: vec![],
        }
    }

    /// Create an EndpointSlice from Endpoints resource
    pub fn from_endpoints(endpoints: &crate::resources::Endpoints) -> Vec<Self> {
        let mut slices = Vec::new();

        // Create one EndpointSlice per subset. Each subset has its own port list
        // and endpoint list. K8s keeps all ports together in one slice per subset.
        for subset in &endpoints.subsets {
            let port_group: Vec<&crate::resources::EndpointPort> = subset
                .ports
                .as_ref()
                .map(|p| p.iter().collect())
                .unwrap_or_default();
            let mut slice = Self::new(
                &endpoints.metadata.name,
                "IPv4", // Default to IPv4
            );

            // Set namespace if present
            if let Some(ns) = &endpoints.metadata.namespace {
                slice.metadata.namespace = Some(ns.clone());
            }

            // Add label to link back to the service
            let labels = slice.metadata.labels.get_or_insert_with(Default::default);
            labels.insert(
                "kubernetes.io/service-name".to_string(),
                endpoints.metadata.name.clone(),
            );
            labels.insert(
                "endpointslice.kubernetes.io/managed-by".to_string(),
                "endpointslice-controller.k8s.io".to_string(),
            );

            // Convert ports — use the port group for this slice
            slice.ports = port_group
                .iter()
                .map(|p| EndpointPort {
                    name: p.name.clone(),
                    port: Some(p.port as i32),
                    protocol: p.protocol.clone(),
                    app_protocol: p.app_protocol.clone(),
                })
                .collect();

            // Convert ready addresses
            if let Some(addresses) = &subset.addresses {
                for addr in addresses {
                    slice.endpoints.push(Endpoint {
                        addresses: vec![addr.ip.clone()],
                        conditions: Some(EndpointConditions {
                            ready: Some(true),
                            serving: Some(true),
                            terminating: Some(false),
                        }),
                        hostname: addr.hostname.clone(),
                        target_ref: addr.target_ref.as_ref().map(|tr| EndpointReference {
                            kind: tr.kind.clone(),
                            namespace: tr.namespace.clone(),
                            name: tr.name.clone(),
                            uid: tr.uid.clone(),
                            resource_version: None,
                            field_path: None,
                        }),
                        node_name: addr.node_name.clone(),
                        zone: None,
                        hints: None,
                        deprecated_topology: None,
                    });
                }
            }

            // Convert not ready addresses
            if let Some(not_ready_addresses) = &subset.not_ready_addresses {
                for addr in not_ready_addresses {
                    slice.endpoints.push(Endpoint {
                        addresses: vec![addr.ip.clone()],
                        conditions: Some(EndpointConditions {
                            ready: Some(false),
                            serving: Some(false),
                            terminating: Some(false),
                        }),
                        hostname: addr.hostname.clone(),
                        target_ref: addr.target_ref.as_ref().map(|tr| EndpointReference {
                            kind: tr.kind.clone(),
                            namespace: tr.namespace.clone(),
                            name: tr.name.clone(),
                            uid: tr.uid.clone(),
                            resource_version: None,
                            field_path: None,
                        }),
                        node_name: addr.node_name.clone(),
                        zone: None,
                        hints: None,
                        deprecated_topology: None,
                    });
                }
            }

            if !slice.endpoints.is_empty() {
                slices.push(slice);
            }
        }

        // If no subsets, create an empty slice
        if slices.is_empty() {
            let mut slice = Self::new(&endpoints.metadata.name, "IPv4");
            if let Some(ns) = &endpoints.metadata.namespace {
                slice.metadata.namespace = Some(ns.clone());
            }
            let labels = slice.metadata.labels.get_or_insert_with(Default::default);
            labels.insert(
                "kubernetes.io/service-name".to_string(),
                endpoints.metadata.name.clone(),
            );
            labels.insert(
                "endpointslice.kubernetes.io/managed-by".to_string(),
                "endpointslice-controller.k8s.io".to_string(),
            );
            slices.push(slice);
        }

        slices
    }
}

/// Endpoint represents a single logical "backend" implementing a service.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Endpoint {
    /// addresses of this endpoint. The contents of this field are interpreted according to
    /// the corresponding EndpointSlice addressType field.
    pub addresses: Vec<String>,

    /// conditions contains information about the current status of the endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<EndpointConditions>,

    /// hostname of this endpoint. This field may be used by consumers of endpoints to
    /// distinguish endpoints from each other.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,

    /// targetRef is a reference to a Kubernetes object that represents this endpoint.
    #[serde(skip_serializing_if = "Option::is_none", rename = "targetRef")]
    pub target_ref: Option<EndpointReference>,

    /// nodeName represents the name of the Node hosting this endpoint.
    #[serde(skip_serializing_if = "Option::is_none", rename = "nodeName")]
    pub node_name: Option<String>,

    /// zone is the name of the Zone this endpoint exists in.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,

    /// hints contains information associated with how an endpoint should be consumed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hints: Option<EndpointHints>,

    /// deprecatedTopology is deprecated and has been replaced by the zone and hints fields.
    #[serde(
        rename = "deprecatedTopology",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub deprecated_topology: Option<std::collections::HashMap<String, String>>,
}

/// EndpointConditions represents the current condition of an endpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointConditions {
    /// ready indicates that this endpoint is prepared to receive traffic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<bool>,

    /// serving is identical to ready except that it is set regardless of the terminating state of endpoints.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serving: Option<bool>,

    /// terminating indicates that this endpoint is terminating.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminating: Option<bool>,
}

/// EndpointPort represents a Port used by an EndpointSlice.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointPort {
    /// name represents the name of this port.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// port represents the port number of the endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,

    /// protocol represents the IP protocol for this port. Must be UDP, TCP, or SCTP.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,

    /// appProtocol is the application protocol for this port.
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

    /// Specific resourceVersion to which this reference is made, if any.
    #[serde(skip_serializing_if = "Option::is_none", rename = "resourceVersion")]
    pub resource_version: Option<String>,

    /// If referring to a piece of an object instead of an entire object, this string should contain a valid JSON/Go field access statement.
    #[serde(skip_serializing_if = "Option::is_none", rename = "fieldPath")]
    pub field_path: Option<String>,
}

/// EndpointHints provides hints describing how an endpoint should be consumed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointHints {
    /// forZones indicates the zone(s) this endpoint should be consumed by to enable topology aware routing.
    #[serde(rename = "forZones", default, skip_serializing_if = "Option::is_none")]
    pub for_zones: Option<Vec<ForZone>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub for_nodes: Option<Vec<ForNode>>,
}

/// ForZone provides information about which zones should consume this endpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForZone {
    /// name represents the name of the zone.
    pub name: String,
}

/// ForNode represents a node for topology-aware endpoint routing
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ForNode {
    pub name: String,
}
