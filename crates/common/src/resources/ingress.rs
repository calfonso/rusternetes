use crate::types::{ObjectMeta, TypeMeta};
use serde::{Deserialize, Serialize};

/// Ingress is a collection of rules that allow inbound connections to reach the cluster services
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Ingress {
    #[serde(flatten)]
    pub type_meta: TypeMeta,

    pub metadata: ObjectMeta,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<IngressSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<IngressStatus>,
}

impl Ingress {
    pub fn new(name: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta {
                kind: "Ingress".to_string(),
                api_version: "networking.k8s.io/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec: None,
            status: None,
        }
    }

    pub fn with_spec(mut self, spec: IngressSpec) -> Self {
        self.spec = Some(spec);
        self
    }
}

/// IngressSpec describes the Ingress the user wishes to exist
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressSpec {
    /// IngressClassName is the name of the IngressClass cluster resource
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingress_class_name: Option<String>,

    /// Default backend for the ingress
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_backend: Option<IngressBackend>,

    /// List of TLS configurations
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<Vec<IngressTLS>>,

    /// List of host rules
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules: Option<Vec<IngressRule>>,
}

/// IngressTLS describes the transport layer security associated with an Ingress
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressTLS {
    /// Hosts are a list of hosts included in the TLS certificate
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hosts: Option<Vec<String>>,

    /// SecretName is the name of the secret used to terminate TLS traffic
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
}

/// IngressRule represents a rule to route traffic based on host and path
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressRule {
    /// Host is the fully qualified domain name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,

    /// HTTP rule configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http: Option<HTTPIngressRuleValue>,
}

/// HTTPIngressRuleValue is a list of HTTP paths
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HTTPIngressRuleValue {
    /// Paths is a collection of paths that map requests to backends
    pub paths: Vec<HTTPIngressPath>,
}

/// HTTPIngressPath associates a path with a backend
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HTTPIngressPath {
    /// Path is matched against the path of an incoming request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// PathType determines the interpretation of the Path matching
    /// Exact, Prefix, or ImplementationSpecific
    #[serde(alias = "pathType")]
    pub path_type: String,

    /// Backend defines the referenced service endpoint
    pub backend: IngressBackend,
}

/// IngressBackend describes all endpoints for a given service and port
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressBackend {
    /// Service references a Service as a backend
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<IngressServiceBackend>,

    /// Resource is a reference to another Kubernetes resource backend
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<TypedLocalObjectReference>,
}

/// TypedLocalObjectReference references a typed object in the same namespace.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TypedLocalObjectReference {
    /// APIGroup of the referent. Empty/None for the core API group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_group: Option<String>,
    /// Kind of the referent.
    pub kind: String,
    /// Name of the referent.
    pub name: String,
}

/// IngressServiceBackend references a Kubernetes Service as a backend
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressServiceBackend {
    /// Name is the referenced service
    pub name: String,

    /// Port of the referenced service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<ServiceBackendPort>,
}

/// ServiceBackendPort is the service port being referenced
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceBackendPort {
    /// Name is the name of the port on the Service
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Number is the numerical port number
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number: Option<i32>,
}

/// IngressStatus describes the current state of the Ingress
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressStatus {
    /// LoadBalancer contains the current status of the load-balancer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balancer: Option<IngressLoadBalancerStatus>,
}

/// IngressLoadBalancerStatus represents the status of a load-balancer
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressLoadBalancerStatus {
    /// Ingress is a list containing ingress points for the load-balancer
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<Vec<IngressLoadBalancerIngress>>,
}

/// IngressLoadBalancerIngress represents the status of a load-balancer ingress point
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressLoadBalancerIngress {
    /// IP is set for load-balancer ingress points that are IP based
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,

    /// Hostname is set for load-balancer ingress points that are DNS based
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,

    /// Ports provide information about the ports exposed by the load-balancer
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<IngressPortStatus>>,
}

fn default_protocol() -> String {
    "TCP".to_string()
}

/// IngressPortStatus represents the status of a port exposed by the load-balancer
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IngressPortStatus {
    /// Port is the port number of the ingress port
    pub port: i32,

    /// Protocol is the protocol of the ingress port (TCP, UDP, SCTP)
    #[serde(default = "default_protocol")]
    pub protocol: String,

    /// Error is to record the problem with the service port
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingress_creation() {
        let ingress = Ingress::new("test-ingress", "default");

        assert_eq!(ingress.metadata.name, "test-ingress");
        assert_eq!(ingress.type_meta.kind, "Ingress");
        assert_eq!(ingress.type_meta.api_version, "networking.k8s.io/v1");
    }

    #[test]
    fn test_ingress_with_spec() {
        let backend = IngressBackend {
            service: Some(IngressServiceBackend {
                name: "test-service".to_string(),
                port: Some(ServiceBackendPort {
                    name: None,
                    number: Some(80),
                }),
            }),
            resource: None,
        };

        let path = HTTPIngressPath {
            path: Some("/".to_string()),
            path_type: "Prefix".to_string(),
            backend,
        };

        let http = HTTPIngressRuleValue { paths: vec![path] };

        let rule = IngressRule {
            host: Some("example.com".to_string()),
            http: Some(http),
        };

        let spec = IngressSpec {
            ingress_class_name: Some("nginx".to_string()),
            default_backend: None,
            tls: None,
            rules: Some(vec![rule]),
        };

        let ingress = Ingress::new("test-ingress", "default").with_spec(spec);

        assert!(ingress.spec.is_some());
        let spec = ingress.spec.unwrap();
        assert_eq!(spec.rules.as_ref().unwrap().len(), 1);
    }
}
