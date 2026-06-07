#[cfg(feature = "aws")]
use async_trait::async_trait;
#[cfg(feature = "aws")]
use rusternetes_common::{
    cloud_provider::{CloudProvider, LoadBalancerIngress, LoadBalancerService, LoadBalancerStatus},
    Result,
};
#[cfg(feature = "aws")]
use std::collections::HashMap;
#[cfg(feature = "aws")]
use tracing::{debug, info, warn};

#[cfg(feature = "aws")]
use aws_sdk_elasticloadbalancingv2::{
    types::{
        Action, ActionTypeEnum, ForwardActionConfig, IpAddressType, LoadBalancer,
        LoadBalancerSchemeEnum, LoadBalancerTypeEnum, TargetGroup, TargetGroupIpAddressTypeEnum,
        TargetTypeEnum,
    },
    Client as ElbClient,
};

/// AWS LoadBalancer provider using Network Load Balancer (NLB) by default
#[cfg(feature = "aws")]
pub struct AwsProvider {
    elb_client: ElbClient,
    vpc_id: Option<String>,
    subnet_ids: Vec<String>,
    cluster_name: String,
    tags: HashMap<String, String>,
}

#[cfg(feature = "aws")]
impl AwsProvider {
    /// Create a new AWS provider
    pub async fn new(cluster_name: String, region: Option<String>) -> Result<Self> {
        let config = if let Some(r) = region {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_config::Region::new(r))
                .load()
                .await
        } else {
            aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await
        };

        let elb_client = ElbClient::new(&config);

        // Auto-detect VPC and subnets from EC2 metadata
        let (vpc_id, subnet_ids) = Self::detect_vpc_and_subnets().await?;

        let mut tags = HashMap::new();
        tags.insert("kubernetes.io/cluster".to_string(), cluster_name.clone());
        tags.insert("managed-by".to_string(), "rusternetes".to_string());

        Ok(Self {
            elb_client,
            vpc_id: Some(vpc_id),
            subnet_ids,
            cluster_name,
            tags,
        })
    }

    /// Detect VPC and subnets from EC2 instance metadata
    async fn detect_vpc_and_subnets() -> Result<(String, Vec<String>)> {
        // For now, return placeholder values
        // In production, this should query EC2 instance metadata service
        // or use environment variables
        let vpc_id = std::env::var("AWS_VPC_ID").unwrap_or_else(|_| "vpc-placeholder".to_string());

        let subnet_ids_str = std::env::var("AWS_SUBNET_IDS")
            .unwrap_or_else(|_| "subnet-placeholder-1,subnet-placeholder-2".to_string());

        let subnet_ids: Vec<String> = subnet_ids_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        Ok((vpc_id, subnet_ids))
    }

    /// Generate load balancer name from service
    fn lb_name(&self, service: &LoadBalancerService) -> String {
        Self::lb_name_for(&self.cluster_name, service)
    }

    /// Generate target group name
    fn tg_name(&self, service: &LoadBalancerService, port: u16) -> String {
        Self::tg_name_for(&self.cluster_name, service, port)
    }

    /// Generate load balancer name from cluster + service (pure helper)
    fn lb_name_for(cluster_name: &str, service: &LoadBalancerService) -> String {
        // AWS LB names max 32 chars, must be alphanumeric and hyphens
        let name = format!("{}-{}-{}", cluster_name, service.namespace, service.name);
        Self::sanitize_name(&name)
    }

    /// Generate target group name from cluster + service (pure helper)
    fn tg_name_for(cluster_name: &str, service: &LoadBalancerService, port: u16) -> String {
        let name = format!(
            "{}-{}-{}-{}",
            cluster_name, service.namespace, service.name, port
        );
        Self::sanitize_name(&name)
    }

    /// Truncate to 32 chars and replace disallowed characters with hyphens
    fn sanitize_name(name: &str) -> String {
        name.chars()
            .take(32)
            .map(|c| {
                if c.is_alphanumeric() || c == '-' {
                    c
                } else {
                    '-'
                }
            })
            .collect()
    }

    /// Find existing load balancer by tags
    async fn find_load_balancer(
        &self,
        service: &LoadBalancerService,
    ) -> Result<Option<LoadBalancer>> {
        let lb_name = self.lb_name(service);

        match self
            .elb_client
            .describe_load_balancers()
            .names(&lb_name)
            .send()
            .await
        {
            Ok(output) => Ok(output.load_balancers().first().cloned()),
            Err(e) => {
                debug!("Load balancer {} not found: {}", lb_name, e);
                Ok(None)
            }
        }
    }

    /// Create or update target group
    async fn ensure_target_group(
        &self,
        service: &LoadBalancerService,
        port: u16,
    ) -> Result<TargetGroup> {
        let tg_name = self.tg_name(service, port);

        // Try to find existing target group
        match self
            .elb_client
            .describe_target_groups()
            .names(&tg_name)
            .send()
            .await
        {
            Ok(output) => {
                if let Some(tg) = output.target_groups().first() {
                    info!("Found existing target group: {}", tg_name);
                    return Ok(tg.clone());
                }
            }
            Err(_) => {
                debug!("Target group {} not found, creating new", tg_name);
            }
        }

        // Create new target group
        info!("Creating target group: {}", tg_name);
        let create_output = self
            .elb_client
            .create_target_group()
            .name(&tg_name)
            .protocol(aws_sdk_elasticloadbalancingv2::types::ProtocolEnum::Tcp)
            .port(port as i32)
            .vpc_id(self.vpc_id.as_ref().unwrap())
            .target_type(TargetTypeEnum::Ip)
            .ip_address_type(TargetGroupIpAddressTypeEnum::Ipv4)
            .send()
            .await
            .map_err(|e| {
                rusternetes_common::Error::Internal(format!("Failed to create target group: {}", e))
            })?;

        let tg = create_output
            .target_groups()
            .first()
            .ok_or_else(|| {
                rusternetes_common::Error::Internal("No target group returned".to_string())
            })?
            .clone();

        // Register targets (node IPs)
        if !service.node_addresses.is_empty() {
            info!(
                "Registering {} targets to target group",
                service.node_addresses.len()
            );

            let targets: Vec<_> = service
                .node_addresses
                .iter()
                .map(|ip| {
                    aws_sdk_elasticloadbalancingv2::types::TargetDescription::builder()
                        .id(ip)
                        .port(port as i32)
                        .build()
                })
                .collect();

            self.elb_client
                .register_targets()
                .target_group_arn(tg.target_group_arn().unwrap())
                .set_targets(Some(targets))
                .send()
                .await
                .map_err(|e| {
                    rusternetes_common::Error::Internal(format!(
                        "Failed to register targets: {}",
                        e
                    ))
                })?;
        }

        Ok(tg)
    }

    /// Create Network Load Balancer
    async fn create_load_balancer(&self, service: &LoadBalancerService) -> Result<LoadBalancer> {
        let lb_name = self.lb_name(service);

        info!("Creating NLB: {}", lb_name);

        // Determine if internal or internet-facing
        let scheme = if service
            .annotations
            .get("service.beta.kubernetes.io/aws-load-balancer-internal")
            == Some(&"true".to_string())
        {
            LoadBalancerSchemeEnum::Internal
        } else {
            LoadBalancerSchemeEnum::InternetFacing
        };

        // Create load balancer
        let create_output = self
            .elb_client
            .create_load_balancer()
            .name(&lb_name)
            .r#type(LoadBalancerTypeEnum::Network)
            .scheme(scheme)
            .ip_address_type(IpAddressType::Ipv4)
            .set_subnets(Some(self.subnet_ids.clone()))
            .send()
            .await
            .map_err(|e| {
                rusternetes_common::Error::Internal(format!(
                    "Failed to create load balancer: {}",
                    e
                ))
            })?;

        let lb = create_output
            .load_balancers()
            .first()
            .ok_or_else(|| {
                rusternetes_common::Error::Internal("No load balancer returned".to_string())
            })?
            .clone();

        // Tag the load balancer
        if let Some(lb_arn) = lb.load_balancer_arn() {
            let tags: Vec<_> = self
                .tags
                .iter()
                .map(|(k, v)| {
                    aws_sdk_elasticloadbalancingv2::types::Tag::builder()
                        .key(k)
                        .value(v)
                        .build()
                })
                .collect();

            let _ = self
                .elb_client
                .add_tags()
                .resource_arns(lb_arn)
                .set_tags(Some(tags))
                .send()
                .await;
        }

        Ok(lb)
    }

    /// Create listeners for the load balancer
    async fn ensure_listeners(&self, lb_arn: &str, service: &LoadBalancerService) -> Result<()> {
        for lb_port in &service.ports {
            // Create target group
            let tg = self.ensure_target_group(service, lb_port.node_port).await?;
            let tg_arn = tg.target_group_arn().ok_or_else(|| {
                rusternetes_common::Error::Internal("Target group has no ARN".to_string())
            })?;

            // Check if listener already exists
            let existing_listeners = self
                .elb_client
                .describe_listeners()
                .load_balancer_arn(lb_arn)
                .send()
                .await
                .map_err(|e| {
                    rusternetes_common::Error::Internal(format!(
                        "Failed to describe listeners: {}",
                        e
                    ))
                })?;

            let listener_exists = existing_listeners
                .listeners()
                .iter()
                .any(|l| l.port() == Some(lb_port.port as i32));

            if !listener_exists {
                info!("Creating listener for port {}", lb_port.port);

                // Create forward action
                let forward_config = ForwardActionConfig::builder()
                    .target_groups(
                        aws_sdk_elasticloadbalancingv2::types::TargetGroupTuple::builder()
                            .target_group_arn(tg_arn)
                            .build(),
                    )
                    .build();

                let action = Action::builder()
                    .r#type(ActionTypeEnum::Forward)
                    .forward_config(forward_config)
                    .build();

                self.elb_client
                    .create_listener()
                    .load_balancer_arn(lb_arn)
                    .protocol(aws_sdk_elasticloadbalancingv2::types::ProtocolEnum::Tcp)
                    .port(lb_port.port as i32)
                    .default_actions(action)
                    .send()
                    .await
                    .map_err(|e| {
                        rusternetes_common::Error::Internal(format!(
                            "Failed to create listener: {}",
                            e
                        ))
                    })?;
            }
        }

        Ok(())
    }
}

#[cfg(feature = "aws")]
#[async_trait]
impl CloudProvider for AwsProvider {
    async fn ensure_load_balancer(
        &self,
        service: &LoadBalancerService,
    ) -> Result<LoadBalancerStatus> {
        info!(
            "Ensuring AWS NLB for service {}/{}",
            service.namespace, service.name
        );

        // Find or create load balancer
        let lb = match self.find_load_balancer(service).await? {
            Some(existing) => {
                info!("Found existing load balancer");
                existing
            }
            None => {
                info!("Creating new load balancer");
                self.create_load_balancer(service).await?
            }
        };

        // Ensure listeners and target groups
        if let Some(lb_arn) = lb.load_balancer_arn() {
            self.ensure_listeners(lb_arn, service).await?;
        }

        // Build status with DNS name and/or IP
        let mut ingress = Vec::new();

        if let Some(dns_name) = lb.dns_name() {
            ingress.push(LoadBalancerIngress {
                ip: None,
                hostname: Some(dns_name.to_string()),
            });
        }

        Ok(LoadBalancerStatus { ingress })
    }

    async fn delete_load_balancer(
        &self,
        service_namespace: &str,
        service_name: &str,
    ) -> Result<()> {
        let service = LoadBalancerService {
            namespace: service_namespace.to_string(),
            name: service_name.to_string(),
            cluster_name: self.cluster_name.clone(),
            ports: vec![],
            node_addresses: vec![],
            session_affinity: None,
            annotations: HashMap::new(),
        };

        let lb_name = self.lb_name(&service);

        info!("Deleting AWS NLB: {}", lb_name);

        // Find load balancer
        let lb = match self.find_load_balancer(&service).await? {
            Some(lb) => lb,
            None => {
                warn!("Load balancer {} not found, already deleted", lb_name);
                return Ok(());
            }
        };

        let lb_arn = lb.load_balancer_arn().ok_or_else(|| {
            rusternetes_common::Error::Internal("Load balancer has no ARN".to_string())
        })?;

        // Delete load balancer
        self.elb_client
            .delete_load_balancer()
            .load_balancer_arn(lb_arn)
            .send()
            .await
            .map_err(|e| {
                rusternetes_common::Error::Internal(format!(
                    "Failed to delete load balancer: {}",
                    e
                ))
            })?;

        info!("Successfully deleted load balancer {}", lb_name);

        Ok(())
    }

    async fn get_load_balancer_status(
        &self,
        service_namespace: &str,
        service_name: &str,
    ) -> Result<Option<LoadBalancerStatus>> {
        let service = LoadBalancerService {
            namespace: service_namespace.to_string(),
            name: service_name.to_string(),
            cluster_name: self.cluster_name.clone(),
            ports: vec![],
            node_addresses: vec![],
            session_affinity: None,
            annotations: HashMap::new(),
        };

        match self.find_load_balancer(&service).await? {
            Some(lb) => {
                let mut ingress = Vec::new();
                if let Some(dns_name) = lb.dns_name() {
                    ingress.push(LoadBalancerIngress {
                        ip: None,
                        hostname: Some(dns_name.to_string()),
                    });
                }
                Ok(Some(LoadBalancerStatus { ingress }))
            }
            None => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "aws"
    }
}

// Stub implementation when AWS feature is not enabled
#[cfg(not(feature = "aws"))]
pub struct AwsProvider;

#[cfg(not(feature = "aws"))]
impl AwsProvider {
    pub async fn new(
        _cluster_name: String,
        _region: Option<String>,
    ) -> rusternetes_common::Result<Self> {
        Err(rusternetes_common::Error::Internal(
            "AWS provider not compiled. Enable 'aws' feature".to_string(),
        ))
    }
}

#[cfg(all(test, feature = "aws"))]
mod tests {
    use super::*;

    #[test]
    fn test_lb_name_generation() {
        let service = LoadBalancerService {
            namespace: "default".to_string(),
            name: "my-service".to_string(),
            cluster_name: "my-cluster".to_string(),
            ports: vec![],
            node_addresses: vec![],
            session_affinity: None,
            annotations: HashMap::new(),
        };

        let lb_name = AwsProvider::lb_name_for("my-cluster", &service);

        // Should be truncated to 32 chars and alphanumeric + hyphens only
        assert!(lb_name.len() <= 32);
        assert!(lb_name.chars().all(|c| c.is_alphanumeric() || c == '-'));
        assert!(lb_name.contains("my-cluster"));
        assert!(lb_name.contains("default"));
        assert!(lb_name.contains("my-service"));
    }

    #[test]
    fn test_lb_name_sanitization() {
        let service = LoadBalancerService {
            namespace: "my@namespace".to_string(),
            name: "service#123".to_string(),
            cluster_name: "test_cluster".to_string(),
            ports: vec![],
            node_addresses: vec![],
            session_affinity: None,
            annotations: HashMap::new(),
        };

        let lb_name = AwsProvider::lb_name_for("test_cluster", &service);

        // All special characters should be converted to hyphens
        assert!(!lb_name.contains('@'));
        assert!(!lb_name.contains('#'));
        assert!(!lb_name.contains('_'));
    }

    #[test]
    fn test_tg_name_generation() {
        let service = LoadBalancerService {
            namespace: "ns".to_string(),
            name: "svc".to_string(),
            cluster_name: "cluster".to_string(),
            ports: vec![],
            node_addresses: vec![],
            session_affinity: None,
            annotations: HashMap::new(),
        };

        let tg_name = AwsProvider::tg_name_for("cluster", &service, 80);

        assert!(tg_name.len() <= 32);
        assert!(tg_name.contains("80"));
    }

    #[test]
    fn test_is_driver_supported() {
        assert!(AwsProvider::is_driver_supported(
            "rusternetes.io/hostpath-snapshotter"
        ));
        assert!(AwsProvider::is_driver_supported("hostpath-snapshotter"));
        assert!(!AwsProvider::is_driver_supported("kubernetes.io/aws-ebs"));
        assert!(!AwsProvider::is_driver_supported("unknown-driver"));
    }
}

// Helper trait implementation for testing
#[cfg(all(test, feature = "aws"))]
impl AwsProvider {
    fn is_driver_supported(driver: &str) -> bool {
        // Strip an optional `rusternetes.io/` vendor prefix, then accept the
        // in-tree hostpath snapshotter driver. Anything else (e.g. the upstream
        // `kubernetes.io/*` drivers) is not supported by this provider.
        let bare = driver.strip_prefix("rusternetes.io/").unwrap_or(driver);
        bare == "hostpath-snapshotter"
    }
}
