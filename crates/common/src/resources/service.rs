use crate::resources::policy::IntOrString;
use crate::resources::serde_helpers::empty_string_as_none;
use crate::types::{Condition, ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Service is an abstraction for exposing applications running on a set of Pods
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Service {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: ServiceSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ServiceStatus>,
}

impl Service {
    pub fn new(name: impl Into<String>, spec: ServiceSpec) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name),
            spec,
            status: None,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSpec {
    #[serde(default)]
    pub selector: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub ports: Vec<ServicePort>,

    #[serde(
        default,
        rename = "type",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub service_type: Option<ServiceType>,

    #[serde(skip_serializing_if = "Option::is_none", rename = "clusterIP")]
    pub cluster_ip: Option<String>,

    #[serde(rename = "externalIPs", skip_serializing_if = "Option::is_none")]
    pub external_ips: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_affinity: Option<String>, // ClientIP or None

    /// ExternalName is the external reference that kubedns or equivalent will return as a CNAME record for this service.
    /// Required for ExternalName type services. No proxying will be involved.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_name: Option<String>,

    /// ClusterIPs is a list of IP addresses assigned to this service, and is usually assigned randomly.
    /// If specified, must be valid IPs. Used for dual-stack support.
    #[serde(rename = "clusterIPs", skip_serializing_if = "Option::is_none")]
    pub cluster_ips: Option<Vec<String>>,

    /// IPFamilies is a list of IP families (IPv4, IPv6) assigned to this service.
    /// Dual-stack services use both IPv4 and IPv6.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_families: Option<Vec<IPFamily>>,

    /// IPFamilyPolicy represents the dual-stack-ness requested or required by this Service.
    /// Can be SingleStack, PreferDualStack, or RequireDualStack.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub ip_family_policy: Option<IPFamilyPolicy>,

    /// InternalTrafficPolicy specifies if the cluster internal traffic should be routed to all endpoints
    /// or node-local endpoints only. "Cluster" routes to all endpoints. "Local" routes to node-local endpoints.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub internal_traffic_policy: Option<ServiceInternalTrafficPolicy>,

    /// ExternalTrafficPolicy denotes if this Service desires to route external traffic to node-local or
    /// cluster-wide endpoints. "Local" preserves the client source IP and avoids a second hop for LoadBalancer
    /// and Nodeport type services, but risks potentially imbalanced traffic spreading.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "empty_string_as_none"
    )]
    pub external_traffic_policy: Option<ServiceExternalTrafficPolicy>,

    /// HealthCheckNodePort specifies the healthcheck nodePort for the service (when type=LoadBalancer and externalTrafficPolicy=Local)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_check_node_port: Option<i32>,

    /// LoadBalancerClass is the class of the load balancer implementation this Service belongs to
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balancer_class: Option<String>,

    /// LoadBalancerIP is the IP to use when creating a load balancer (deprecated, use loadBalancerClass instead)
    #[serde(rename = "loadBalancerIP", skip_serializing_if = "Option::is_none")]
    pub load_balancer_ip: Option<String>,

    /// LoadBalancerSourceRanges restricts traffic through the cloud-provider load-balancer to these CIDRs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balancer_source_ranges: Option<Vec<String>>,

    /// AllocateLoadBalancerNodePorts defines if NodePorts will be allocated for Services with type LoadBalancer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocate_load_balancer_node_ports: Option<bool>,

    /// PublishNotReadyAddresses indicates that endpoints for this Service should be published even when not ready
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_not_ready_addresses: Option<bool>,

    /// SessionAffinityConfig contains the configurations of session affinity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_affinity_config: Option<SessionAffinityConfig>,

    /// TrafficDistribution offers a way to express preferences for how traffic is distributed to Service endpoints
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traffic_distribution: Option<String>,
}

fn default_protocol() -> String {
    "TCP".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServicePort {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    pub port: u16,

    /// Number or name of the port to access on the pods targeted by the service.
    /// Can be a port number (e.g., 8080) or a named port (e.g., "http").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_port: Option<IntOrString>,

    #[serde(default = "default_protocol")]
    pub protocol: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_port: Option<u16>,

    /// Application protocol for the port (e.g., "http", "https")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_protocol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ServiceType {
    ClusterIP,
    NodePort,
    LoadBalancer,
    ExternalName,
}

/// ServiceStatus represents the current status of a service
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceStatus {
    #[serde(skip_serializing_if = "Option::is_none", rename = "loadBalancer")]
    pub load_balancer: Option<LoadBalancerStatus>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}

/// LoadBalancerStatus represents the status of a load balancer
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadBalancerStatus {
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub ingress: Vec<LoadBalancerIngress>,
}

/// LoadBalancerIngress represents the status of a load balancer ingress point
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadBalancerIngress {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    /// IPMode specifies how the load-balancer IP behaves (VIP or Proxy)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_mode: Option<String>,
    /// Ports is a list of records of service ports
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<PortStatus>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortStatus {
    pub port: i32,
    #[serde(default = "default_protocol")]
    pub protocol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// SessionAffinityConfig contains the configurations of session affinity
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionAffinityConfig {
    #[serde(rename = "clientIP", skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<ClientIPConfig>,
}

/// ClientIPConfig represents the configurations of Client IP based session affinity
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientIPConfig {
    /// TimeoutSeconds specifies the seconds of ClientIP type session sticky time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i32>,
}

/// IPFamily represents the IP family (IPv4 or IPv6)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IPFamily {
    IPv4,
    IPv6,
}

/// IPFamilyPolicy represents the dual-stack-ness requested or required by a Service
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IPFamilyPolicy {
    /// SingleStack indicates that this service is required to have a single IPFamily.
    /// The IPFamily assigned is based on the default IPFamily used by the cluster
    /// or as identified by service.spec.ipFamilies field.
    SingleStack,
    /// PreferDualStack indicates that this service prefers dual-stack when the cluster is configured for dual-stack.
    /// If the cluster is not configured for dual-stack the service will be assigned a single IPFamily.
    PreferDualStack,
    /// RequireDualStack indicates that this service requires dual-stack.
    /// The service will fail if the cluster is not configured for dual-stack.
    RequireDualStack,
}

/// ServiceInternalTrafficPolicy describes how nodes distribute service traffic they
/// receive on the ClusterIP. If set to "Local", the proxy will assume that pods only
/// want to talk to endpoints of the service on the same node as the pod, dropping the
/// traffic if there are no local endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ServiceInternalTrafficPolicy {
    /// Cluster routes traffic to all endpoints
    Cluster,
    /// Local routes traffic only to node-local endpoints, dropping the traffic if no endpoints exist on the node
    Local,
}

/// ServiceExternalTrafficPolicy describes how nodes distribute service traffic they
/// receive on one of the Service's "externally-facing" addresses (NodePorts, ExternalIPs,
/// and LoadBalancer IPs). If set to "Local", the proxy will assume that pods only want
/// to talk to endpoints of the service on the same node as the pod, dropping the traffic
/// if there are no local endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ServiceExternalTrafficPolicy {
    /// Cluster routes traffic to all endpoints
    Cluster,
    /// Local routes traffic only to node-local endpoints, preserving client source IP and avoiding second hop
    Local,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_port_with_int_target_port() {
        let port = ServicePort {
            name: Some("http".to_string()),
            port: 80,
            target_port: Some(IntOrString::Int(8080)),
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        };

        let json = serde_json::to_string(&port).unwrap();
        assert!(json.contains("\"targetPort\":8080"));

        let deserialized: ServicePort = serde_json::from_str(&json).unwrap();
        match deserialized.target_port {
            Some(IntOrString::Int(p)) => assert_eq!(p, 8080),
            _ => panic!("Expected IntOrString::Int"),
        }
    }

    #[test]
    fn test_service_port_with_string_target_port() {
        let port = ServicePort {
            name: Some("http".to_string()),
            port: 80,
            target_port: Some(IntOrString::String("http-server".to_string())),
            protocol: "TCP".to_string(),
            node_port: None,
            app_protocol: None,
        };

        let json = serde_json::to_string(&port).unwrap();
        assert!(json.contains("\"targetPort\":\"http-server\""));

        let deserialized: ServicePort = serde_json::from_str(&json).unwrap();
        match deserialized.target_port {
            Some(IntOrString::String(ref s)) => assert_eq!(s, "http-server"),
            _ => panic!("Expected IntOrString::String"),
        }
    }

    #[test]
    fn test_service_port_deserialization_from_kubernetes() {
        // Kubernetes sends targetPort as either number or string
        let json_int = r#"{"port": 80, "targetPort": 8080, "protocol": "TCP"}"#;
        let port: ServicePort = serde_json::from_str(json_int).unwrap();
        assert!(matches!(port.target_port, Some(IntOrString::Int(8080))));

        let json_str = r#"{"port": 80, "targetPort": "http", "protocol": "TCP"}"#;
        let port: ServicePort = serde_json::from_str(json_str).unwrap();
        match port.target_port {
            Some(IntOrString::String(ref s)) => assert_eq!(s, "http"),
            _ => panic!("Expected IntOrString::String for named port"),
        }
    }

    #[test]
    fn test_service_with_session_affinity_config() {
        let svc = Service::new(
            "test-svc",
            ServiceSpec {
                selector: Some(HashMap::new()),
                ports: vec![],
                service_type: None,
                cluster_ip: None,
                external_ips: None,
                session_affinity: Some("ClientIP".to_string()),
                external_name: None,
                cluster_ips: None,
                ip_families: None,
                ip_family_policy: None,
                internal_traffic_policy: None,
                external_traffic_policy: None,
                health_check_node_port: None,
                load_balancer_class: None,
                load_balancer_ip: None,
                load_balancer_source_ranges: None,
                allocate_load_balancer_node_ports: None,
                publish_not_ready_addresses: None,
                session_affinity_config: Some(SessionAffinityConfig {
                    client_ip: Some(ClientIPConfig {
                        timeout_seconds: Some(10800),
                    }),
                }),
                traffic_distribution: None,
            },
        );

        let json = serde_json::to_string(&svc).unwrap();
        assert!(json.contains("\"timeoutSeconds\":10800"));
    }

    #[test]
    fn test_service_spec_default() {
        // ServiceSpec should have a Default impl
        let spec = ServiceSpec::default();
        assert!(spec.selector.is_none());
        assert!(spec.ports.is_empty());
        assert!(spec.service_type.is_none());
        assert!(spec.cluster_ip.is_none());
    }

    #[test]
    fn test_service_deserialize_without_selector() {
        // A Service JSON without selector should deserialize correctly
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "test"},
            "spec": {"ports": []}
        }"#;
        let svc: Service = serde_json::from_str(json).unwrap();
        assert!(svc.spec.selector.is_none());
    }

    #[test]
    fn test_service_deserialize_without_spec() {
        // A Service JSON without spec should deserialize (uses Default)
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "test"}
        }"#;
        let svc: Service = serde_json::from_str(json).unwrap();
        assert!(svc.spec.selector.is_none());
        assert!(svc.spec.ports.is_empty());
    }

    #[test]
    fn test_service_selector_empty_serialization() {
        // Empty selector should be serialized as {} (K8s clients require the field present)
        let spec = ServiceSpec {
            selector: Some(HashMap::new()),
            ..Default::default()
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"selector\":{}"));
    }

    #[test]
    fn test_service_selector_some_serialization() {
        // When selector has values, it should be included
        let mut sel = HashMap::new();
        sel.insert("app".to_string(), "web".to_string());
        let spec = ServiceSpec {
            selector: Some(sel),
            ..Default::default()
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"selector\""));
        assert!(json.contains("\"app\":\"web\""));
    }

    #[test]
    fn test_service_selector_null_deserialization() {
        // A Service with "selector": null should deserialize correctly
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "test"},
            "spec": {"selector": null, "ports": []}
        }"#;
        let svc: Service = serde_json::from_str(json).unwrap();
        assert!(svc.spec.selector.is_none());
    }

    /// Regression: upstream e2e (and client-go in general) serialize unset
    /// enum-typed Option<_> fields as the empty string instead of omitting
    /// them. Without empty_string_as_none, strict serde rejects "" with
    /// `unknown variant ''` and the api-server returns 422 before any
    /// handler runs, breaking `[sig-node] Pods should contain environment
    /// variables for services` and every other test that builds a Service
    /// via framework helpers.
    #[test]
    fn service_spec_accepts_empty_enum_strings() {
        let json = r#"{
            "type": "",
            "ipFamilyPolicy": "",
            "internalTrafficPolicy": "",
            "externalTrafficPolicy": ""
        }"#;
        let spec: ServiceSpec = serde_json::from_str(json).unwrap();
        assert!(spec.service_type.is_none());
        assert!(spec.ip_family_policy.is_none());
        assert!(spec.internal_traffic_policy.is_none());
        assert!(spec.external_traffic_policy.is_none());
    }

    #[test]
    fn service_spec_still_accepts_named_enum_variants() {
        let json = r#"{
            "type": "ClusterIP",
            "ipFamilyPolicy": "SingleStack",
            "internalTrafficPolicy": "Cluster",
            "externalTrafficPolicy": "Local"
        }"#;
        let spec: ServiceSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.service_type, Some(ServiceType::ClusterIP));
        assert_eq!(spec.ip_family_policy, Some(IPFamilyPolicy::SingleStack));
        assert_eq!(
            spec.internal_traffic_policy,
            Some(ServiceInternalTrafficPolicy::Cluster)
        );
        assert_eq!(
            spec.external_traffic_policy,
            Some(ServiceExternalTrafficPolicy::Local)
        );
    }

    #[test]
    fn service_spec_rejects_genuinely_bogus_enum_strings() {
        let json = r#"{ "externalTrafficPolicy": "NotARealPolicy" }"#;
        let r: Result<ServiceSpec, _> = serde_json::from_str(json);
        assert!(r.is_err(), "unknown non-empty variant must still error");
    }

    /// The e2e payload that triggered the bug, captured verbatim from the
    /// `[sig-node] Pods should contain environment variables for services`
    /// failure on node-1.
    #[test]
    fn service_e2e_envvars_payload_deserializes() {
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "fooservice"},
            "spec": {
                "selector": {"name": "server-envvars"},
                "ports": [{"port": 8765, "targetPort": 8080}],
                "type": "",
                "internalTrafficPolicy": "",
                "externalTrafficPolicy": "",
                "ipFamilyPolicy": ""
            }
        }"#;
        let svc: Service = serde_json::from_str(json).unwrap();
        assert!(svc.spec.service_type.is_none());
        assert!(svc.spec.internal_traffic_policy.is_none());
        assert!(svc.spec.external_traffic_policy.is_none());
        assert!(svc.spec.ip_family_policy.is_none());
    }
}
