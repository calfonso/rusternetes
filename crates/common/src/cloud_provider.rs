use crate::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// CloudProvider defines the interface for cloud provider integrations
#[async_trait]
pub trait CloudProvider: Send + Sync {
    /// Create a load balancer for the given service
    async fn ensure_load_balancer(
        &self,
        service: &LoadBalancerService,
    ) -> Result<LoadBalancerStatus>;

    /// Delete a load balancer for the given service
    async fn delete_load_balancer(&self, service_namespace: &str, service_name: &str)
        -> Result<()>;

    /// Get the status of a load balancer
    async fn get_load_balancer_status(
        &self,
        service_namespace: &str,
        service_name: &str,
    ) -> Result<Option<LoadBalancerStatus>>;

    /// Get the cloud provider name
    fn name(&self) -> &str;
}

/// LoadBalancerService contains the information needed to create a load balancer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerService {
    pub namespace: String,
    pub name: String,
    pub cluster_name: String,
    pub ports: Vec<LoadBalancerPort>,
    pub node_addresses: Vec<String>,
    pub session_affinity: Option<String>,
    pub annotations: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerPort {
    pub name: Option<String>,
    pub protocol: String,
    pub port: u16,
    pub node_port: u16,
}

/// LoadBalancerStatus represents the status of a load balancer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerStatus {
    /// External IP addresses or DNS names assigned to the load balancer
    pub ingress: Vec<LoadBalancerIngress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerIngress {
    pub ip: Option<String>,
    pub hostname: Option<String>,
}

/// LoadBalancerConfig contains configuration for load balancer provisioning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerConfig {
    pub provider: String,
    pub cluster_name: String,
    pub region: Option<String>,
    pub zone: Option<String>,
    pub tags: std::collections::HashMap<String, String>,
}

/// CloudProviderType represents the supported cloud provider types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloudProviderType {
    AWS,
    GCP,
    Azure,
    None,
}

impl CloudProviderType {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "aws" => Some(CloudProviderType::AWS),
            "gcp" | "google" => Some(CloudProviderType::GCP),
            "azure" => Some(CloudProviderType::Azure),
            "none" | "" => Some(CloudProviderType::None),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            CloudProviderType::AWS => "aws",
            CloudProviderType::GCP => "gcp",
            CloudProviderType::Azure => "azure",
            CloudProviderType::None => "none",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloud_provider_type_from_str() {
        assert_eq!(
            CloudProviderType::from_str("aws"),
            Some(CloudProviderType::AWS)
        );
        assert_eq!(
            CloudProviderType::from_str("AWS"),
            Some(CloudProviderType::AWS)
        );
        assert_eq!(
            CloudProviderType::from_str("gcp"),
            Some(CloudProviderType::GCP)
        );
        assert_eq!(
            CloudProviderType::from_str("google"),
            Some(CloudProviderType::GCP)
        );
        assert_eq!(
            CloudProviderType::from_str("azure"),
            Some(CloudProviderType::Azure)
        );
        assert_eq!(
            CloudProviderType::from_str("AZURE"),
            Some(CloudProviderType::Azure)
        );
        assert_eq!(
            CloudProviderType::from_str("none"),
            Some(CloudProviderType::None)
        );
        assert_eq!(
            CloudProviderType::from_str(""),
            Some(CloudProviderType::None)
        );
        assert_eq!(CloudProviderType::from_str("invalid"), None);
    }

    #[test]
    fn test_cloud_provider_type_as_str() {
        assert_eq!(CloudProviderType::AWS.as_str(), "aws");
        assert_eq!(CloudProviderType::GCP.as_str(), "gcp");
        assert_eq!(CloudProviderType::Azure.as_str(), "azure");
        assert_eq!(CloudProviderType::None.as_str(), "none");
    }

    #[test]
    fn test_cloud_provider_type_roundtrip() {
        let types = vec![
            CloudProviderType::AWS,
            CloudProviderType::GCP,
            CloudProviderType::Azure,
            CloudProviderType::None,
        ];

        for provider_type in types {
            let str_repr = provider_type.as_str();
            let parsed = CloudProviderType::from_str(str_repr);
            assert_eq!(parsed, Some(provider_type));
        }
    }

    #[test]
    fn test_loadbalancer_service_creation() {
        let mut annotations = std::collections::HashMap::new();
        annotations.insert("test-key".to_string(), "test-value".to_string());

        let service = LoadBalancerService {
            namespace: "default".to_string(),
            name: "test-service".to_string(),
            cluster_name: "test-cluster".to_string(),
            ports: vec![LoadBalancerPort {
                name: Some("http".to_string()),
                protocol: "TCP".to_string(),
                port: 80,
                node_port: 30080,
            }],
            node_addresses: vec!["192.168.1.10".to_string(), "192.168.1.11".to_string()],
            session_affinity: Some("ClientIP".to_string()),
            annotations,
        };

        assert_eq!(service.namespace, "default");
        assert_eq!(service.name, "test-service");
        assert_eq!(service.ports.len(), 1);
        assert_eq!(service.node_addresses.len(), 2);
        assert_eq!(
            service.annotations.get("test-key"),
            Some(&"test-value".to_string())
        );
    }

    #[test]
    fn test_loadbalancer_status_with_ip() {
        let status = LoadBalancerStatus {
            ingress: vec![LoadBalancerIngress {
                ip: Some("203.0.113.1".to_string()),
                hostname: None,
            }],
        };

        assert_eq!(status.ingress.len(), 1);
        assert_eq!(status.ingress[0].ip, Some("203.0.113.1".to_string()));
        assert_eq!(status.ingress[0].hostname, None);
    }

    #[test]
    fn test_loadbalancer_status_with_hostname() {
        let status = LoadBalancerStatus {
            ingress: vec![LoadBalancerIngress {
                ip: None,
                hostname: Some("lb-abc123.us-west-2.elb.amazonaws.com".to_string()),
            }],
        };

        assert_eq!(status.ingress.len(), 1);
        assert_eq!(status.ingress[0].ip, None);
        assert!(status.ingress[0]
            .hostname
            .as_ref()
            .unwrap()
            .contains("elb.amazonaws.com"));
    }

    #[test]
    fn test_loadbalancer_status_multiple_ingress() {
        let status = LoadBalancerStatus {
            ingress: vec![
                LoadBalancerIngress {
                    ip: Some("203.0.113.1".to_string()),
                    hostname: None,
                },
                LoadBalancerIngress {
                    ip: Some("203.0.113.2".to_string()),
                    hostname: None,
                },
            ],
        };

        assert_eq!(status.ingress.len(), 2);
    }
}
