// Audit logging for Kubernetes API requests
//
// This module provides audit logging functionality that tracks all API requests
// for security, compliance, and debugging purposes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{error, info};

/// Audit event representing an API request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEvent {
    /// API version of the audit event
    pub api_version: String,
    /// Kind is always "Event"
    pub kind: String,
    /// Level of audit detail
    pub level: AuditLevel,
    /// Unique ID for this audit event
    pub audit_id: String,
    /// Stage of the request (RequestReceived, ResponseStarted, ResponseComplete, Panic)
    pub stage: AuditStage,
    /// Request URI
    pub request_uri: String,
    /// HTTP verb (GET, POST, PUT, DELETE, etc.)
    pub verb: String,
    /// Authenticated user information
    pub user: UserInfo,
    /// Resource being accessed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_ref: Option<ObjectReference>,
    /// HTTP response code
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_status: Option<ResponseStatus>,
    /// Request received timestamp
    #[serde(
        serialize_with = "crate::time::micro_time::serialize_required",
        deserialize_with = "crate::time::micro_time::deserialize_required"
    )]
    pub request_received_timestamp: DateTime<Utc>,
    /// Stage timestamp
    #[serde(
        serialize_with = "crate::time::micro_time::serialize_required",
        deserialize_with = "crate::time::micro_time::deserialize_required"
    )]
    pub stage_timestamp: DateTime<Utc>,
    /// Annotations (optional metadata)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AuditLevel {
    /// No logging
    None,
    /// Metadata only (no request/response bodies)
    Metadata,
    /// Metadata + request body (no response body)
    Request,
    /// Metadata + request body + response body
    RequestResponse,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AuditStage {
    /// Request received
    RequestReceived,
    /// Response headers sent
    ResponseStarted,
    /// Response complete
    ResponseComplete,
    /// Panic during request processing
    Panic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    pub username: String,
    pub uid: String,
    pub groups: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<std::collections::HashMap<String, Vec<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectReference {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseStatus {
    pub code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Audit policy defining what to log
#[derive(Debug, Clone)]
pub struct AuditPolicy {
    /// Minimum level to log
    pub level: AuditLevel,
    /// Whether to log requests
    pub log_requests: bool,
    /// Whether to log responses
    pub log_responses: bool,
    /// Whether to log metadata changes
    pub log_metadata: bool,
}

impl Default for AuditPolicy {
    fn default() -> Self {
        Self {
            level: AuditLevel::Metadata,
            log_requests: true,
            log_responses: false,
            log_metadata: true,
        }
    }
}

/// Audit backend for writing audit events
#[async_trait::async_trait]
pub trait AuditBackend: Send + Sync {
    /// Write an audit event
    async fn log(&self, event: AuditEvent) -> Result<(), String>;

    /// Flush any buffered events
    async fn flush(&self) -> Result<(), String>;
}

/// File-based audit backend
pub struct FileAuditBackend {
    file: Arc<Mutex<tokio::fs::File>>,
    path: String,
}

/// Webhook-based audit backend
pub struct WebhookAuditBackend {
    /// URL of the webhook endpoint
    url: String,
    /// HTTP client for making requests
    client: reqwest::Client,
}

impl WebhookAuditBackend {
    pub fn new(url: String) -> Self {
        info!("Audit webhook enabled: sending to {}", url);
        Self {
            url,
            client: reqwest::Client::new(),
        }
    }
}

impl FileAuditBackend {
    pub async fn new(path: String) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        info!("Audit logging enabled: writing to {}", path);

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            path,
        })
    }
}

#[async_trait::async_trait]
impl AuditBackend for FileAuditBackend {
    async fn log(&self, event: AuditEvent) -> Result<(), String> {
        // Serialize event to JSON
        let json = match serde_json::to_string(&event) {
            Ok(j) => j,
            Err(e) => {
                error!("Failed to serialize audit event: {}", e);
                return Err(format!("Serialization error: {}", e));
            }
        };

        // Write to file
        let mut file = self.file.lock().await;
        if let Err(e) = file.write_all(json.as_bytes()).await {
            error!("Failed to write audit event to {}: {}", self.path, e);
            return Err(format!("Write error: {}", e));
        }
        if let Err(e) = file.write_all(b"\n").await {
            error!("Failed to write newline to audit log: {}", e);
            return Err(format!("Write error: {}", e));
        }

        Ok(())
    }

    async fn flush(&self) -> Result<(), String> {
        let mut file = self.file.lock().await;
        if let Err(e) = file.flush().await {
            error!("Failed to flush audit log: {}", e);
            return Err(format!("Flush error: {}", e));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AuditBackend for WebhookAuditBackend {
    async fn log(&self, event: AuditEvent) -> Result<(), String> {
        // Serialize event to JSON
        let json = match serde_json::to_value(&event) {
            Ok(j) => j,
            Err(e) => {
                error!("Failed to serialize audit event: {}", e);
                return Err(format!("Serialization error: {}", e));
            }
        };

        // Send to webhook endpoint
        match self
            .client
            .post(&self.url)
            .json(&json)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) => {
                if !response.status().is_success() {
                    error!("Webhook returned error status: {}", response.status());
                    return Err(format!("HTTP error: {}", response.status()));
                }
                Ok(())
            }
            Err(e) => {
                error!("Failed to send audit event to webhook {}: {}", self.url, e);
                Err(format!("Network error: {}", e))
            }
        }
    }

    async fn flush(&self) -> Result<(), String> {
        // Webhooks don't need flushing
        Ok(())
    }
}

/// Multi-backend audit logger that can send events to multiple backends
pub struct MultiAuditBackend {
    backends: Vec<Arc<dyn AuditBackend>>,
}

impl MultiAuditBackend {
    pub fn new(backends: Vec<Arc<dyn AuditBackend>>) -> Self {
        Self { backends }
    }
}

#[async_trait::async_trait]
impl AuditBackend for MultiAuditBackend {
    async fn log(&self, event: AuditEvent) -> Result<(), String> {
        let mut errors = Vec::new();
        for backend in &self.backends {
            if let Err(e) = backend.log(event.clone()).await {
                errors.push(e);
            }
        }
        if !errors.is_empty() {
            return Err(format!("Some backends failed: {}", errors.join("; ")));
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), String> {
        let mut errors = Vec::new();
        for backend in &self.backends {
            if let Err(e) = backend.flush().await {
                errors.push(e);
            }
        }
        if !errors.is_empty() {
            return Err(format!(
                "Some backends failed to flush: {}",
                errors.join("; ")
            ));
        }
        Ok(())
    }
}

/// Audit logger
pub struct AuditLogger {
    backend: Arc<dyn AuditBackend>,
    policy: AuditPolicy,
}

impl AuditLogger {
    pub fn new(backend: Arc<dyn AuditBackend>, policy: AuditPolicy) -> Self {
        Self { backend, policy }
    }

    /// Log an API request
    pub async fn log_request(
        &self,
        request_uri: String,
        verb: String,
        user: UserInfo,
        object_ref: Option<ObjectReference>,
    ) -> String {
        if self.policy.level == AuditLevel::None {
            return String::new();
        }

        let audit_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        let event = AuditEvent {
            api_version: "audit.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            level: self.policy.level.clone(),
            audit_id: audit_id.clone(),
            stage: AuditStage::RequestReceived,
            request_uri,
            verb,
            user,
            object_ref,
            response_status: None,
            request_received_timestamp: now,
            stage_timestamp: now,
            annotations: None,
        };

        if let Err(e) = self.backend.log(event).await {
            error!("Failed to log audit event: {}", e);
        }

        audit_id
    }

    /// Log an API response
    #[allow(clippy::too_many_arguments)]
    pub async fn log_response(
        &self,
        audit_id: String,
        request_uri: String,
        verb: String,
        user: UserInfo,
        object_ref: Option<ObjectReference>,
        status_code: u16,
        message: Option<String>,
    ) {
        if self.policy.level == AuditLevel::None {
            return;
        }

        let now = Utc::now();

        let event = AuditEvent {
            api_version: "audit.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            level: self.policy.level.clone(),
            audit_id,
            stage: AuditStage::ResponseComplete,
            request_uri,
            verb,
            user,
            object_ref,
            response_status: Some(ResponseStatus {
                code: status_code,
                message,
            }),
            request_received_timestamp: now, // Should be the original timestamp
            stage_timestamp: now,
            annotations: None,
        };

        if let Err(e) = self.backend.log(event).await {
            error!("Failed to log audit event: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_level() {
        let level = AuditLevel::Metadata;
        assert_eq!(level, AuditLevel::Metadata);
    }

    #[test]
    fn test_audit_stage() {
        let stage = AuditStage::RequestReceived;
        assert_eq!(stage, AuditStage::RequestReceived);
    }

    #[test]
    fn test_audit_policy_default() {
        let policy = AuditPolicy::default();
        assert_eq!(policy.level, AuditLevel::Metadata);
        assert!(policy.log_requests);
        assert!(!policy.log_responses);
        assert!(policy.log_metadata);
    }

    #[tokio::test]
    async fn test_audit_event_serialization() {
        let event = AuditEvent {
            api_version: "audit.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            level: AuditLevel::Metadata,
            audit_id: "test-id".to_string(),
            stage: AuditStage::RequestReceived,
            request_uri: "/api/v1/pods".to_string(),
            verb: "GET".to_string(),
            user: UserInfo {
                username: "test-user".to_string(),
                uid: "123".to_string(),
                groups: vec!["system:authenticated".to_string()],
                extra: None,
            },
            object_ref: None,
            response_status: None,
            request_received_timestamp: Utc::now(),
            stage_timestamp: Utc::now(),
            annotations: None,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("test-user"));
        assert!(json.contains("GET"));
    }

    #[tokio::test]
    async fn test_file_audit_backend() {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let backend = FileAuditBackend::new(path.clone()).await.unwrap();

        let event = AuditEvent {
            api_version: "audit.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            level: AuditLevel::Metadata,
            audit_id: "test-123".to_string(),
            stage: AuditStage::RequestReceived,
            request_uri: "/api/v1/namespaces".to_string(),
            verb: "list".to_string(),
            user: UserInfo {
                username: "admin".to_string(),
                uid: "admin-uid".to_string(),
                groups: vec!["system:masters".to_string()],
                extra: None,
            },
            object_ref: None,
            response_status: None,
            request_received_timestamp: Utc::now(),
            stage_timestamp: Utc::now(),
            annotations: None,
        };

        backend.log(event).await.unwrap();
        backend.flush().await.unwrap();

        // Read the file and verify content
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("admin"));
        assert!(content.contains("test-123"));
    }

    #[test]
    fn test_webhook_audit_backend_creation() {
        let backend = WebhookAuditBackend::new("http://localhost:8080/audit".to_string());
        assert_eq!(backend.url, "http://localhost:8080/audit");
    }

    #[tokio::test]
    async fn test_multi_backend() {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let file_backend = Arc::new(FileAuditBackend::new(path.clone()).await.unwrap());
        let multi_backend = MultiAuditBackend::new(vec![file_backend]);

        let event = AuditEvent {
            api_version: "audit.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            level: AuditLevel::Metadata,
            audit_id: "multi-123".to_string(),
            stage: AuditStage::ResponseComplete,
            request_uri: "/api/v1/pods".to_string(),
            verb: "create".to_string(),
            user: UserInfo {
                username: "developer".to_string(),
                uid: "dev-uid".to_string(),
                groups: vec!["developers".to_string()],
                extra: None,
            },
            object_ref: Some(ObjectReference {
                resource: Some("pods".to_string()),
                namespace: Some("default".to_string()),
                name: Some("test-pod".to_string()),
                uid: Some("pod-123".to_string()),
                api_version: Some("v1".to_string()),
                resource_version: Some("1".to_string()),
            }),
            response_status: Some(ResponseStatus {
                code: 201,
                message: Some("Created".to_string()),
            }),
            request_received_timestamp: Utc::now(),
            stage_timestamp: Utc::now(),
            annotations: None,
        };

        multi_backend.log(event).await.unwrap();
        multi_backend.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("developer"));
        assert!(content.contains("multi-123"));
    }

    #[tokio::test]
    async fn test_audit_logger() {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let backend = Arc::new(FileAuditBackend::new(path.clone()).await.unwrap());
        let policy = AuditPolicy::default();
        let logger = AuditLogger::new(backend, policy);

        let user = UserInfo {
            username: "test-user".to_string(),
            uid: "test-uid".to_string(),
            groups: vec!["users".to_string()],
            extra: None,
        };

        let audit_id = logger
            .log_request(
                "/api/v1/pods".to_string(),
                "list".to_string(),
                user.clone(),
                None,
            )
            .await;

        assert!(!audit_id.is_empty());

        logger
            .log_response(
                audit_id,
                "/api/v1/pods".to_string(),
                "list".to_string(),
                user,
                None,
                200,
                Some("OK".to_string()),
            )
            .await;

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("test-user"));
        assert!(content.contains("list"));
    }
}
