use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    extract::{Path, Query, State},
    Extension, Json,
};
use chrono::Utc;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    resources::{custom_metrics::ListMetadata, ExternalMetricValue, ExternalMetricValueList},
    Result,
};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Debug, Deserialize)]
pub struct ExternalMetricQuery {
    #[serde(rename = "labelSelector")]
    label_selector: Option<String>,
}

/// GET /apis/external.metrics.k8s.io/v1beta1/namespaces/{namespace}/{metric}
///
/// Lists values for a global (external) metric. The HPA controller's `External`
/// metric path queries this. Values come from Prometheus when configured,
/// otherwise a fallback mock value keeps the wiring exercised in dev clusters.
pub async fn list_external_metrics(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path((namespace, metric_name)): Path<(String, String)>,
    Query(query): Query<ExternalMetricQuery>,
) -> Result<Json<ExternalMetricValueList>> {
    info!(
        "Listing external metric {} in namespace {}",
        metric_name, namespace
    );

    // External metrics are authorized as a resource named by the metric, in the
    // external.metrics.k8s.io group, scoped to the requesting namespace.
    let attrs = RequestAttributes::new(auth_ctx.user, "list", &metric_name)
        .with_api_group("external.metrics.k8s.io")
        .with_namespace(&namespace);

    if let Decision::Deny(reason) = state.authorizer.authorize(&attrs).await? {
        return Err(rusternetes_common::Error::Forbidden(reason));
    }

    // Parse the label selector into both a map (for Prometheus) and the labels
    // echoed back on each metric value.
    let label_map: Option<HashMap<String, String>> = query.label_selector.as_ref().map(|sel| {
        sel.split(',')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                match (parts.next(), parts.next()) {
                    (Some(k), Some(v)) => Some((k.trim().to_string(), v.trim().to_string())),
                    _ => None,
                }
            })
            .collect()
    });
    let metric_labels: Option<BTreeMap<String, String>> = label_map
        .as_ref()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect());

    let value = if let Some(ref prometheus_client) = state.prometheus_client {
        match prometheus_client
            .query_external_metric(&metric_name, &namespace, label_map.as_ref())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "Failed to query Prometheus for external metric {}: {}. Using fallback value.",
                    metric_name, e
                );
                "0".to_string()
            }
        }
    } else {
        // Fallback mock value when Prometheus is not configured.
        "100".to_string()
    };

    let items = vec![ExternalMetricValue {
        api_version: "external.metrics.k8s.io/v1beta1".to_string(),
        kind: "ExternalMetricValue".to_string(),
        metric_name: metric_name.clone(),
        metric_labels,
        timestamp: Utc::now(),
        window: Some("60s".to_string()),
        value,
    }];

    let list = ExternalMetricValueList {
        api_version: "external.metrics.k8s.io/v1beta1".to_string(),
        kind: "ExternalMetricValueList".to_string(),
        metadata: ListMetadata {
            self_link: Some(format!(
                "/apis/external.metrics.k8s.io/v1beta1/namespaces/{}/{}",
                namespace, metric_name
            )),
        },
        items,
    };

    Ok(Json(list))
}
