use crate::types::{LabelSelector, ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};

/// NetworkPolicy describes what network traffic is allowed for a set of Pods
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicy {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    pub spec: NetworkPolicySpec,
}

impl NetworkPolicy {
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        spec: NetworkPolicySpec,
    ) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "NetworkPolicy".to_string(),
                api_version: "networking.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec,
        }
    }
}

/// NetworkPolicySpec provides the specification of a NetworkPolicy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicySpec {
    /// Selects the pods to which this NetworkPolicy applies
    pub pod_selector: LabelSelector,

    /// List of ingress rules to be applied
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<Vec<NetworkPolicyIngressRule>>,

    /// List of egress rules to be applied
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<Vec<NetworkPolicyEgressRule>>,

    /// List of policy types that the NetworkPolicy relates to
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_types: Option<Vec<String>>, // "Ingress", "Egress"
}

/// NetworkPolicyIngressRule describes a particular set of traffic that is allowed to the pods
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicyIngressRule {
    /// List of ports which should be made accessible
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<NetworkPolicyPort>>,

    /// List of sources which should be able to access the pods
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<Vec<NetworkPolicyPeer>>,
}

/// NetworkPolicyEgressRule describes a particular set of traffic that is allowed out of pods
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicyEgressRule {
    /// List of destination ports for outgoing traffic
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<NetworkPolicyPort>>,

    /// List of destinations for outgoing traffic of pods selected for this rule
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<Vec<NetworkPolicyPeer>>,
}

fn default_protocol() -> String {
    "TCP".to_string()
}

/// NetworkPolicyPort describes a port to allow traffic on
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicyPort {
    /// The protocol (TCP, UDP, SCTP)
    #[serde(default = "default_protocol")]
    pub protocol: String,

    /// The port on the given protocol
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<serde_json::Value>, // IntOrString

    /// The end port range
    #[serde(skip_serializing_if = "Option::is_none", rename = "endPort")]
    pub end_port: Option<i32>,
}

/// NetworkPolicyPeer describes a peer to allow traffic from/to
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicyPeer {
    /// Selects pods
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_selector: Option<LabelSelector>,

    /// Selects namespaces
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<LabelSelector>,

    /// IPBlock defines policy on a particular IPBlock
    #[serde(skip_serializing_if = "Option::is_none", rename = "ipBlock")]
    pub ip_block: Option<IPBlock>,
}

/// IPBlock describes a particular CIDR range
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IPBlock {
    /// CIDR is a string representing the IP Block
    pub cidr: String,

    /// Except is a slice of CIDRs that should not be included within an IP Block
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub except: Option<Vec<String>>,
}
