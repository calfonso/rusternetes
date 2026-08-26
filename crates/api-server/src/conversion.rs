//! Conversion webhook support for Custom Resource Definitions
//!
//! This module implements Kubernetes-compatible conversion webhooks that allow
//! automatic conversion between different versions of custom resources.

#![allow(dead_code)]

use rusternetes_common::resources::{
    CustomResource, CustomResourceDefinition, WebhookClientConfig,
};
use rusternetes_common::Result;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, info, warn};

/// ConversionReview is the request/response object for conversion webhooks
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversionReview {
    pub api_version: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<ConversionRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<ConversionResponse>,
}

/// ConversionRequest describes the conversion request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversionRequest {
    /// UID is an identifier for the conversion request
    pub uid: String,
    /// DesiredAPIVersion is the version to convert to
    pub desired_api_version: String,
    /// Objects is the list of custom resources to convert
    pub objects: Vec<serde_json::Value>,
}

/// ConversionResponse describes the conversion response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversionResponse {
    /// UID echoes the request UID
    pub uid: String,
    /// ConvertedObjects is the list of converted custom resources
    pub converted_objects: Vec<serde_json::Value>,
    /// Result indicates whether the conversion succeeded
    pub result: ConversionResult,
}

/// ConversionResult indicates the success or failure of a conversion
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversionResult {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
}

impl ConversionResult {
    pub fn success() -> Self {
        Self {
            status: "Success".to_string(),
            message: None,
            reason: None,
            code: None,
        }
    }

    pub fn failure(message: String) -> Self {
        Self {
            status: "Failure".to_string(),
            message: Some(message),
            reason: Some("ConversionError".to_string()),
            code: Some(500),
        }
    }
}

/// Conversion webhook client
pub struct ConversionWebhookClient {
    client: reqwest::Client,
}

impl ConversionWebhookClient {
    /// Create a new conversion webhook client
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    /// Convert custom resources using a webhook
    pub async fn convert(
        &self,
        crd: &CustomResourceDefinition,
        objects: Vec<CustomResource>,
        desired_version: &str,
    ) -> Result<Vec<CustomResource>> {
        // Check if conversion is enabled
        let conversion = crd.spec.conversion.as_ref().ok_or_else(|| {
            rusternetes_common::Error::InvalidResource(
                "Conversion not configured for CRD".to_string(),
            )
        })?;

        // Get webhook configuration
        let webhook = conversion.webhook.as_ref().ok_or_else(|| {
            rusternetes_common::Error::InvalidResource(
                "Webhook conversion strategy requires webhook configuration".to_string(),
            )
        })?;

        // Build webhook URL
        let url = self.build_webhook_url(&webhook.client_config)?;

        info!(
            "Calling conversion webhook at {} for CRD {} to version {}",
            url, crd.metadata.name, desired_version
        );

        // Prepare conversion request
        let request = ConversionRequest {
            uid: uuid::Uuid::new_v4().to_string(),
            desired_api_version: format!("{}/{}", crd.spec.group, desired_version),
            objects: objects
                .iter()
                .map(|obj| serde_json::to_value(obj).unwrap())
                .collect(),
        };

        let review = ConversionReview {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "ConversionReview".to_string(),
            request: Some(request.clone()),
            response: None,
        };

        // Call webhook
        debug!("Sending conversion request: {:?}", review);

        let response = match self.client.post(&url).json(&review).send().await {
            Ok(r) => r,
            Err(e) => {
                // Webhook unreachable — return objects unconverted
                warn!(
                    "Conversion webhook unreachable, returning unconverted: {}",
                    e
                );
                return Ok(objects.to_vec());
            }
        };

        if !response.status().is_success() {
            // Webhook returned error — return objects unconverted
            warn!(
                "Conversion webhook returned {}, returning unconverted",
                response.status()
            );
            return Ok(objects.to_vec());
        }

        let review_response: ConversionReview =
            response.json::<ConversionReview>().await.map_err(|e| {
                rusternetes_common::Error::Network(format!(
                    "Failed to parse webhook response: {}",
                    e
                ))
            })?;

        debug!("Received conversion response: {:?}", review_response);

        // Extract response
        let conv_response = review_response.response.ok_or_else(|| {
            rusternetes_common::Error::Network(
                "Webhook response missing response field".to_string(),
            )
        })?;

        // Check result
        if conv_response.result.status != "Success" {
            return Err(rusternetes_common::Error::Network(format!(
                "Conversion failed: {}",
                conv_response
                    .result
                    .message
                    .unwrap_or_else(|| "Unknown error".to_string())
            )));
        }

        // Deserialize converted objects
        let converted_objects: Vec<CustomResource> = conv_response
            .converted_objects
            .into_iter()
            .map(serde_json::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                rusternetes_common::Error::Network(format!(
                    "Failed to deserialize converted objects: {}",
                    e
                ))
            })?;

        info!(
            "Successfully converted {} objects to version {}",
            converted_objects.len(),
            desired_version
        );

        Ok(converted_objects)
    }

    /// Build webhook URL from client config
    fn build_webhook_url(&self, config: &WebhookClientConfig) -> Result<String> {
        if let Some(ref url) = config.url {
            return Ok(url.clone());
        }

        if let Some(ref service) = config.service {
            // Build service URL
            let namespace = &service.namespace;
            let name = &service.name;
            let path = service.path.as_deref().unwrap_or("/convert");
            let port = service.port.unwrap_or(443);

            // In-cluster service URL
            let url = format!("https://{}.{}.svc:{}{}", name, namespace, port, path);

            return Ok(url);
        }

        Err(rusternetes_common::Error::InvalidResource(
            "Webhook client config must specify either url or service".to_string(),
        ))
    }
}

impl Default for ConversionWebhookClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a custom resource to a different version
pub async fn convert_custom_resource(
    crd: &CustomResourceDefinition,
    resource: CustomResource,
    target_version: &str,
) -> Result<CustomResource> {
    // Check if conversion is needed
    let current_version = extract_version(&resource.api_version);
    if current_version == target_version {
        debug!("Resource already at target version {}", target_version);
        return Ok(resource);
    }

    // Check conversion strategy
    let conversion = crd.spec.conversion.as_ref().ok_or_else(|| {
        rusternetes_common::Error::InvalidResource(
            "Conversion not configured for this CRD".to_string(),
        )
    })?;

    match conversion.strategy {
        rusternetes_common::resources::ConversionStrategyType::None => {
            // No conversion - just update the API version
            warn!(
                "Conversion strategy is None, simply updating API version from {} to {}",
                current_version, target_version
            );
            let mut converted = resource;
            converted.api_version = format!("{}/{}", crd.spec.group, target_version);
            Ok(converted)
        }
        rusternetes_common::resources::ConversionStrategyType::Webhook => {
            // Use webhook for conversion
            let client = ConversionWebhookClient::new();
            let mut converted = client.convert(crd, vec![resource], target_version).await?;
            Ok(converted.remove(0))
        }
    }
}

/// Extract version from API version string (e.g., "stable.example.com/v1" -> "v1")
fn extract_version(api_version: &str) -> &str {
    api_version.split('/').next_back().unwrap_or(api_version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{
        ConversionStrategyType, CustomResourceConversion, CustomResourceDefinitionNames,
        CustomResourceDefinitionSpec, CustomResourceDefinitionVersion, ResourceScope,
    };
    use rusternetes_common::types::ObjectMeta;

    fn create_test_crd() -> CustomResourceDefinition {
        CustomResourceDefinition {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
            metadata: ObjectMeta::new("crontabs.stable.example.com"),
            spec: CustomResourceDefinitionSpec {
                group: "stable.example.com".to_string(),
                names: CustomResourceDefinitionNames {
                    plural: "crontabs".to_string(),
                    singular: Some("crontab".to_string()),
                    kind: "CronTab".to_string(),
                    short_names: Some(vec!["ct".to_string()]),
                    categories: None,
                    list_kind: Some("CronTabList".to_string()),
                },
                scope: ResourceScope::Namespaced,
                versions: vec![
                    CustomResourceDefinitionVersion {
                        name: "v1".to_string(),
                        served: true,
                        storage: true,
                        deprecated: None,
                        deprecation_warning: None,
                        schema: None,
                        subresources: None,
                        additional_printer_columns: None,
                    },
                    CustomResourceDefinitionVersion {
                        name: "v2".to_string(),
                        served: true,
                        storage: false,
                        deprecated: None,
                        deprecation_warning: None,
                        schema: None,
                        subresources: None,
                        additional_printer_columns: None,
                    },
                ],
                conversion: Some(CustomResourceConversion {
                    strategy: ConversionStrategyType::None,
                    webhook: None,
                }),
                preserve_unknown_fields: None,
            },
            status: None,
        }
    }

    #[test]
    fn test_extract_version() {
        assert_eq!(extract_version("stable.example.com/v1"), "v1");
        assert_eq!(extract_version("v1"), "v1");
        assert_eq!(extract_version("apps/v1"), "v1");
    }

    #[test]
    fn test_conversion_result_success() {
        let result = ConversionResult::success();
        assert_eq!(result.status, "Success");
        assert!(result.message.is_none());
    }

    #[test]
    fn test_conversion_result_failure() {
        let result = ConversionResult::failure("Test error".to_string());
        assert_eq!(result.status, "Failure");
        assert_eq!(result.message, Some("Test error".to_string()));
        assert_eq!(result.code, Some(500));
    }

    #[tokio::test]
    async fn test_convert_same_version() {
        let crd = create_test_crd();
        let resource = CustomResource {
            api_version: "stable.example.com/v1".to_string(),
            kind: "CronTab".to_string(),
            metadata: ObjectMeta::new("my-crontab"),
            spec: Some(serde_json::json!({
                "cronSpec": "* * * * */5",
                "image": "my-cron-image"
            })),
            status: None,
            extra: std::collections::HashMap::new(),
        };

        let result = convert_custom_resource(&crd, resource.clone(), "v1").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().api_version, resource.api_version);
    }

    #[test]
    fn test_webhook_url_from_service() {
        let client = ConversionWebhookClient::new();
        let config = WebhookClientConfig {
            url: None,
            service: Some(rusternetes_common::resources::ServiceReference {
                namespace: "default".to_string(),
                name: "converter".to_string(),
                path: Some("/convert".to_string()),
                port: Some(443),
            }),
            ca_bundle: None,
        };

        let url = client.build_webhook_url(&config);
        assert!(url.is_ok());
        assert_eq!(url.unwrap(), "https://converter.default.svc:443/convert");
    }

    #[test]
    fn test_webhook_url_from_url() {
        let client = ConversionWebhookClient::new();
        let config = WebhookClientConfig {
            url: Some("https://example.com/convert".to_string()),
            service: None,
            ca_bundle: None,
        };

        let url = client.build_webhook_url(&config);
        assert!(url.is_ok());
        assert_eq!(url.unwrap(), "https://example.com/convert");
    }
}
