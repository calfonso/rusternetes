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
    /// DesiredAPIVersion is the version to convert to.
    ///
    /// Wire field is `desiredAPIVersion` (uppercase `API`), matching the
    /// upstream apiextensions.k8s.io/v1 `ConversionRequest` schema —
    /// `staging/src/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1/types.go`.
    #[serde(rename = "desiredAPIVersion")]
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

/// Convert a custom resource to a different version.
///
/// Mirrors the upstream apiextensions-apiserver behavior:
/// - If the stored object is already at `target_version`, return it as-is.
/// - If `spec.conversion` is unset, default to `None` strategy (no schema change,
///   just rewrite `apiVersion`).
/// - For `None` strategy, rewrite the `apiVersion` field on the wire object.
/// - For `Webhook` strategy, POST a `ConversionReview` to the configured
///   webhook URL and return the converted object from the response.
pub async fn convert_custom_resource(
    crd: &CustomResourceDefinition,
    resource: CustomResource,
    target_version: &str,
) -> Result<CustomResource> {
    convert_custom_resources(crd, vec![resource], target_version)
        .await
        .map(|mut v| v.remove(0))
}

/// Batch version of [`convert_custom_resource`] — converts a list of CRs in a
/// single ConversionReview round-trip when the strategy is Webhook.
pub async fn convert_custom_resources(
    crd: &CustomResourceDefinition,
    resources: Vec<CustomResource>,
    target_version: &str,
) -> Result<Vec<CustomResource>> {
    if resources.is_empty() {
        return Ok(resources);
    }

    // Default strategy is None when spec.conversion is unset.
    let strategy = crd
        .spec
        .conversion
        .as_ref()
        .map(|c| c.strategy.clone())
        .unwrap_or(rusternetes_common::resources::ConversionStrategyType::None);

    match strategy {
        rusternetes_common::resources::ConversionStrategyType::None => {
            // No conversion - just update the API version on objects that need it.
            let target_api_version = format!("{}/{}", crd.spec.group, target_version);
            Ok(resources
                .into_iter()
                .map(|mut r| {
                    if extract_version(&r.api_version) != target_version {
                        r.api_version = target_api_version.clone();
                    }
                    r
                })
                .collect())
        }
        rusternetes_common::resources::ConversionStrategyType::Webhook => {
            // Webhook strategy: only objects whose stored version differs from
            // the target need to round-trip through the webhook. Preserve
            // input order when stitching converted objects back into the result.
            let mut needs_idx: Vec<usize> = Vec::new();
            let mut needs_obj: Vec<CustomResource> = Vec::new();
            let mut passthrough: Vec<(usize, CustomResource)> = Vec::new();
            for (i, r) in resources.into_iter().enumerate() {
                if extract_version(&r.api_version) == target_version {
                    passthrough.push((i, r));
                } else {
                    needs_idx.push(i);
                    needs_obj.push(r);
                }
            }

            let converted = if needs_obj.is_empty() {
                Vec::new()
            } else {
                ConversionWebhookClient::new()
                    .convert(crd, needs_obj, target_version)
                    .await?
            };

            let mut indexed: Vec<(usize, CustomResource)> = passthrough;
            indexed.extend(needs_idx.into_iter().zip(converted));
            indexed.sort_by_key(|(i, _)| *i);
            Ok(indexed.into_iter().map(|(_, r)| r).collect())
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
