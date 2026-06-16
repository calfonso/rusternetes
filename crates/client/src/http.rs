use anyhow::{Context, Result};
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// List-level metadata (`metadata` on a `*List` envelope).
///
/// `resourceVersion` is what a reflector resumes its watch from after the
/// initial list.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListMeta {
    pub resource_version: Option<String>,
    #[serde(rename = "continue")]
    pub continue_token: Option<String>,
}

/// Generic Kubernetes List wrapper
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubernetesList<T> {
    #[allow(dead_code)]
    pub api_version: String,
    #[allow(dead_code)]
    pub kind: String,
    pub metadata: Option<ListMeta>,
    pub items: Vec<T>,
}

pub struct ApiClient {
    base_url: String,
    /// Client for normal CRUD requests. Has a total-request timeout so a
    /// stalled connection can't hang a caller forever (#1165: a controller's
    /// single worker would block on a hung GET/LIST/UPDATE and stop reconciling
    /// until process restart).
    client: Client,
    /// Client for long-lived watch streams (`get_stream`). No total timeout —
    /// a watch is meant to stay open indefinitely — but still bounded by
    /// connect-timeout + TCP keepalive so a dead connection is detected.
    stream_client: Client,
    token: Option<String>,
}

#[derive(Debug)]
pub enum GetError {
    NotFound,
    Other(anyhow::Error),
}

impl std::fmt::Display for GetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetError::NotFound => write!(f, "Resource not found"),
            GetError::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for GetError {}

/// Format a non-2xx HTTP response into a readable error.
///
/// The Kubernetes API server returns a `Status` object on failures, e.g.
/// `{"kind":"Status","status":"Failure","reason":"AlreadyExists","message":"...","code":409}`.
/// When the body parses as such, surface a clean upstream-`kubectl`-style
/// message: `Error from server (AlreadyExists): <message>`. Otherwise fall
/// back to the raw status + body text.
fn format_status_error(status: reqwest::StatusCode, body: &str) -> anyhow::Error {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(body) {
        if v.get("kind").and_then(|k| k.as_str()) == Some("Status") {
            let reason = v.get("reason").and_then(|r| r.as_str()).unwrap_or("");
            let message = v.get("message").and_then(|m| m.as_str()).unwrap_or(body);
            if !reason.is_empty() {
                return anyhow::anyhow!("Error from server ({reason}): {message}");
            }
            return anyhow::anyhow!("Error from server: {message}");
        }
    }
    anyhow::anyhow!("request failed with status {status}: {body}")
}

impl ApiClient {
    /// Ergonomic constructor without a CA (system trust store / insecure).
    /// Used widely by tests; production wiring uses [`Self::with_tls`].
    #[allow(dead_code)]
    pub fn new(
        base_url: &str,
        insecure_skip_tls_verify: bool,
        token: Option<String>,
    ) -> Result<Self> {
        Self::with_tls(base_url, insecure_skip_tls_verify, None, token)
    }

    /// Build a client from a resolved [`crate::config::ClientConfig`]
    /// (kubeconfig- or in-cluster-sourced), reusing the [`Self::with_tls`]
    /// CA/token path.
    pub fn from_config(config: &crate::config::ClientConfig) -> Result<Self> {
        Self::with_tls(
            &config.base_url,
            false,
            config.ca_pem.as_ref().map(|pem| pem.as_bytes().to_vec()),
            config.token.clone(),
        )
    }

    /// Build a client, optionally trusting a kubeconfig-supplied CA certificate.
    ///
    /// TLS precedence mirrors upstream kubectl:
    ///   1. `insecure_skip_tls_verify` — accept any cert (skips CA entirely).
    ///   2. `ca_pem` present — verify against that CA (added to the default
    ///      roots), which is how we trust the api-server's self-signed cert.
    ///   3. neither — verify against the system roots only.
    pub fn with_tls(
        base_url: &str,
        insecure_skip_tls_verify: bool,
        ca_pem: Option<Vec<u8>>,
        token: Option<String>,
    ) -> Result<Self> {
        // Build a reqwest client with the shared TLS config. `total_timeout`
        // is applied only to the CRUD client — the stream client must stay open
        // for long-lived watches. Both get a connect-timeout and TCP keepalive
        // so a dead connection surfaces as an error instead of hanging forever
        // (#1165).
        let build_client = |total_timeout: Option<std::time::Duration>| -> Result<Client> {
            let mut builder = Client::builder()
                .connect_timeout(std::time::Duration::from_secs(15))
                .tcp_keepalive(std::time::Duration::from_secs(60));
            if insecure_skip_tls_verify {
                builder = builder.danger_accept_invalid_certs(true);
            } else if let Some(ref ca) = ca_pem {
                let cert = reqwest::Certificate::from_pem(ca)
                    .context("parsing kubeconfig CA certificate (certificate-authority-data)")?;
                builder = builder.add_root_certificate(cert);
            }
            if let Some(t) = total_timeout {
                builder = builder.timeout(t);
            }
            builder.build().context("Failed to build HTTP client")
        };

        let client = build_client(Some(std::time::Duration::from_secs(60)))?;
        let stream_client = build_client(None)?;

        Ok(Self {
            base_url: base_url.to_string(),
            client,
            stream_client,
            token,
        })
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, GetError> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.get(&url);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .map_err(|e| GetError::Other(anyhow::anyhow!("Failed to send GET request: {}", e)))?;

        let status = response.status();

        if status == StatusCode::NOT_FOUND {
            return Err(GetError::NotFound);
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GetError::Other(format_status_error(status, &body)));
        }

        response
            .json()
            .await
            .map_err(|e| GetError::Other(anyhow::anyhow!("Failed to parse response: {}", e)))
    }

    /// Get a list of resources, automatically unwrapping the Kubernetes List wrapper
    #[allow(dead_code)]
    pub async fn get_list<T: DeserializeOwned>(&self, path: &str) -> Result<Vec<T>, GetError> {
        let list: KubernetesList<T> = self.get(path).await?;
        Ok(list.items)
    }

    /// Get a streaming response for watch mode
    pub async fn get_stream(&self, path: &str) -> Result<reqwest::Response, GetError> {
        let url = format!("{}{}", self.base_url, path);
        // Use the stream client (no total-request timeout) — a watch is a
        // long-lived stream and must not be killed by the CRUD timeout (#1165).
        let mut request = self.stream_client.get(&url);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .map_err(|e| GetError::Other(anyhow::anyhow!("Failed to send GET request: {}", e)))?;

        let status = response.status();

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(GetError::Other(anyhow::anyhow!(
                "Watch request failed with status {}: {}",
                status,
                body
            )));
        }

        Ok(response)
    }

    pub async fn post<T: Serialize, R: DeserializeOwned>(&self, path: &str, body: &T) -> Result<R> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.post(&url).json(body);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .context("Failed to send POST request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format_status_error(status, &body));
        }

        response.json().await.context("Failed to parse response")
    }

    pub async fn put<T: Serialize, R: DeserializeOwned>(&self, path: &str, body: &T) -> Result<R> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.put(&url).json(body);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await.context("Failed to send PUT request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format_status_error(status, &body));
        }

        response.json().await.context("Failed to parse response")
    }

    /// DELETE with query parameters and optional JSON body (for DeleteOptions).
    /// Returns the response status code (useful for checking 404 on wait-polling).
    pub async fn delete_with_options(
        &self,
        path: &str,
        query_params: &[(String, String)],
        body: Option<&serde_json::Value>,
    ) -> Result<StatusCode> {
        let mut url = format!("{}{}", self.base_url, path);

        if !query_params.is_empty() {
            let qs: Vec<String> = query_params
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            let separator = if url.contains('?') { "&" } else { "?" };
            url.push_str(&format!("{}{}", separator, qs.join("&")));
        }

        let mut request = self.client.delete(&url);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        if let Some(b) = body {
            request = request.header("Content-Type", "application/json").json(b);
        }

        let response = request
            .send()
            .await
            .context("Failed to send DELETE request")?;

        let status = response.status();

        if !status.is_success() && status != StatusCode::NOT_FOUND {
            let body = response.text().await.unwrap_or_default();
            return Err(format_status_error(status, &body));
        }

        Ok(status)
    }

    /// Check if a resource exists (GET returns 200). Returns false on 404.
    pub async fn resource_exists(&self, path: &str) -> Result<bool> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.get(&url);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await.context("Failed to send GET request")?;

        Ok(response.status().is_success())
    }

    pub async fn patch<T: Serialize, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &T,
        content_type: &str,
    ) -> Result<R> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self
            .client
            .patch(&url)
            .header("Content-Type", content_type)
            .json(body);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .context("Failed to send PATCH request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format_status_error(status, &body));
        }

        response.json().await.context("Failed to parse response")
    }

    /// Get a resource as plain text (for logs, etc.)
    pub async fn get_text(&self, path: &str) -> Result<String> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.get(&url);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await.context("Failed to send GET request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Request failed with status {}: {}", status, body);
        }

        response
            .text()
            .await
            .context("Failed to read response text")
    }

    /// Convert HTTP(S) base URL to WebSocket URL for streaming endpoints
    pub fn get_ws_url(&self, path: &str) -> Result<String> {
        let ws_base = if self.base_url.starts_with("https://") {
            self.base_url.replace("https://", "wss://")
        } else if self.base_url.starts_with("http://") {
            self.base_url.replace("http://", "ws://")
        } else {
            anyhow::bail!("Invalid base URL: {}", self.base_url);
        };

        let mut url = format!("{}{}", ws_base, path);

        // Add token as query parameter if present (WebSocket doesn't support headers)
        if let Some(ref token) = self.token {
            let separator = if url.contains('?') { "&" } else { "?" };
            url.push_str(&format!("{}token={}", separator, token));
        }

        Ok(url)
    }

    pub fn get_base_url(&self) -> &str {
        &self.base_url
    }

    pub fn get_token(&self) -> Option<&String> {
        self.token.as_ref()
    }

    /// GET a path with a custom `Accept` header, returning the parsed JSON.
    /// Used for aggregated API discovery (`APIGroupDiscoveryList`).
    pub async fn get_raw_with_accept(
        &self,
        path: &str,
        accept: &str,
    ) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self.client.get(&url).header("Accept", accept);

        if let Some(ref token) = self.token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await.context("Failed to send GET request")?;

        let status = response.status();
        let text = response
            .text()
            .await
            .context("Failed to read response body")?;

        if !status.is_success() {
            anyhow::bail!("discovery request to {url} failed: {status}: {text}");
        }

        serde_json::from_str(&text)
            .with_context(|| format!("failed to parse discovery response from {url} as JSON"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_status_error_already_exists() {
        let body = r#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"AlreadyExists","message":"configmaps \"f5-cm\" already exists","code":409}"#;
        let err = format_status_error(StatusCode::CONFLICT, body);
        assert_eq!(
            err.to_string(),
            "Error from server (AlreadyExists): configmaps \"f5-cm\" already exists"
        );
    }

    #[test]
    fn test_format_status_error_not_found() {
        let body = r#"{"kind":"Status","status":"Failure","reason":"NotFound","message":"pods \"x\" not found","code":404}"#;
        let err = format_status_error(StatusCode::NOT_FOUND, body);
        assert_eq!(
            err.to_string(),
            "Error from server (NotFound): pods \"x\" not found"
        );
    }

    #[test]
    fn test_format_status_error_no_reason() {
        // A Status object without a reason field still surfaces the message.
        let body = r#"{"kind":"Status","status":"Failure","message":"something bad","code":500}"#;
        let err = format_status_error(StatusCode::INTERNAL_SERVER_ERROR, body);
        assert_eq!(err.to_string(), "Error from server: something bad");
    }

    #[test]
    fn test_format_status_error_non_status_body_falls_back() {
        let body = "plain text gateway error";
        let err = format_status_error(StatusCode::BAD_GATEWAY, body);
        assert_eq!(
            err.to_string(),
            "request failed with status 502 Bad Gateway: plain text gateway error"
        );
    }

    #[test]
    fn test_format_status_error_non_json_body_falls_back() {
        let body = "<html>503</html>";
        let err = format_status_error(StatusCode::SERVICE_UNAVAILABLE, body);
        assert_eq!(
            err.to_string(),
            "request failed with status 503 Service Unavailable: <html>503</html>"
        );
    }
}
