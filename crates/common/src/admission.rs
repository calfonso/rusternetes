// Admission control for Kubernetes API requests
//
// This module provides admission controllers that intercept API requests
// before they are persisted to etcd. Admission controllers can:
// - Validate requests (ValidatingAdmissionController)
// - Mutate requests (MutatingAdmissionController)
// - Both validate and mutate (MutatingAdmissionWebhook, ValidatingAdmissionWebhook)

use crate::resources::pod::Pod;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Result of an admission decision
#[derive(Debug, Clone, PartialEq)]
pub enum AdmissionResponse {
    /// Request is allowed
    Allow,
    /// Request is denied with a reason
    Deny(String),
    /// Request is allowed with mutations (JSON Patch format)
    AllowWithPatch(Vec<PatchOperation>),
}

impl AdmissionResponse {
    pub fn is_allowed(&self) -> bool {
        matches!(
            self,
            AdmissionResponse::Allow | AdmissionResponse::AllowWithPatch(_)
        )
    }

    pub fn deny_reason(&self) -> Option<&str> {
        match self {
            AdmissionResponse::Deny(reason) => Some(reason),
            _ => None,
        }
    }

    pub fn patches(&self) -> Option<&[PatchOperation]> {
        match self {
            AdmissionResponse::AllowWithPatch(patches) => Some(patches),
            _ => None,
        }
    }
}

/// JSON Patch operation (RFC 6902)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PatchOperation {
    pub op: PatchOp,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PatchOp {
    Add,
    Remove,
    Replace,
    Move,
    Copy,
    Test,
}

/// Admission request containing the resource to be validated/mutated
#[derive(Debug, Clone)]
pub struct AdmissionRequest {
    /// The operation being performed (CREATE, UPDATE, DELETE)
    pub operation: Operation,
    /// The resource kind (Pod, Service, etc.)
    pub kind: String,
    /// The resource namespace (if namespaced)
    pub namespace: Option<String>,
    /// The resource name
    pub name: String,
    /// The resource object (JSON)
    pub object: serde_json::Value,
    /// The old resource object for UPDATE operations
    pub old_object: Option<serde_json::Value>,
    /// The user making the request
    pub user_info: UserInfo,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Operation {
    Create,
    Update,
    Delete,
    Connect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub username: String,
    pub uid: String,
    pub groups: Vec<String>,
}

/// Trait for all admission controllers
#[async_trait]
pub trait AdmissionController: Send + Sync {
    /// Name of the admission controller
    fn name(&self) -> &str;

    /// Admit a request, returning whether to allow it and any patches
    async fn admit(&self, request: &AdmissionRequest) -> AdmissionResponse;

    /// Whether this controller supports the given operation
    fn supports_operation(&self, operation: &Operation) -> bool {
        matches!(operation, Operation::Create | Operation::Update)
    }
}

/// Chain of admission controllers
pub struct AdmissionChain {
    controllers: Vec<Arc<dyn AdmissionController>>,
}

impl AdmissionChain {
    pub fn new() -> Self {
        Self {
            controllers: Vec::new(),
        }
    }

    pub fn with_controller(mut self, controller: Arc<dyn AdmissionController>) -> Self {
        self.controllers.push(controller);
        self
    }

    pub fn with_built_in_controllers(self) -> Self {
        self.with_controller(Arc::new(NamespaceLifecycleController))
            .with_controller(Arc::new(ResourceQuotaController))
            .with_controller(Arc::new(LimitRangerController))
            .with_controller(Arc::new(PodSecurityStandardsController))
    }

    /// Run all admission controllers in the chain
    pub async fn admit(&self, request: &AdmissionRequest) -> AdmissionResponse {
        let mut all_patches = Vec::new();

        for controller in &self.controllers {
            // Skip controllers that don't support this operation
            if !controller.supports_operation(&request.operation) {
                continue;
            }

            let response = controller.admit(request).await;

            match response {
                AdmissionResponse::Deny(reason) => {
                    return AdmissionResponse::Deny(format!("{}: {}", controller.name(), reason));
                }
                AdmissionResponse::AllowWithPatch(mut patches) => {
                    all_patches.append(&mut patches);
                }
                AdmissionResponse::Allow => {}
            }
        }

        if all_patches.is_empty() {
            AdmissionResponse::Allow
        } else {
            AdmissionResponse::AllowWithPatch(all_patches)
        }
    }
}

impl Default for AdmissionChain {
    fn default() -> Self {
        Self::new()
    }
}

// ===== Built-in Admission Controllers =====

/// NamespaceLifecycle prevents creating resources in non-existent or terminating namespaces
pub struct NamespaceLifecycleController;

#[async_trait]
impl AdmissionController for NamespaceLifecycleController {
    fn name(&self) -> &str {
        "NamespaceLifecycle"
    }

    async fn admit(&self, request: &AdmissionRequest) -> AdmissionResponse {
        // Only check namespaced resources
        if let Some(namespace) = &request.namespace {
            // Special namespaces are always allowed
            if namespace == "kube-system" || namespace == "kube-public" || namespace == "default" {
                return AdmissionResponse::Allow;
            }

            // For CREATE operations, check if namespace exists
            // In real implementation, this would query etcd
            // For now, we'll allow it
        }

        AdmissionResponse::Allow
    }
}

/// ResourceQuota enforces resource consumption limits per namespace
pub struct ResourceQuotaController;

#[async_trait]
impl AdmissionController for ResourceQuotaController {
    fn name(&self) -> &str {
        "ResourceQuota"
    }

    async fn admit(&self, _request: &AdmissionRequest) -> AdmissionResponse {
        // TODO: Implement resource quota checking
        // For now, always allow
        AdmissionResponse::Allow
    }
}

/// LimitRanger enforces min/max resource limits and provides defaults
pub struct LimitRangerController;

#[async_trait]
impl AdmissionController for LimitRangerController {
    fn name(&self) -> &str {
        "LimitRanger"
    }

    async fn admit(&self, request: &AdmissionRequest) -> AdmissionResponse {
        // Only process Pod and PersistentVolumeClaim resources
        if request.kind != "Pod" && request.kind != "PersistentVolumeClaim" {
            return AdmissionResponse::Allow;
        }

        // Skip if this is a DELETE operation
        if request.operation == Operation::Delete {
            return AdmissionResponse::Allow;
        }

        // For now, return Allow since we need access to etcd to fetch LimitRange objects
        // This will be implemented when admission controllers have access to the API server state
        //
        // Full implementation would:
        // 1. Fetch all LimitRange objects from the namespace
        // 2. For Pods: apply default limits/requests to containers, validate min/max, check ratios
        // 3. For PVCs: validate storage requests against limits
        // 4. Return patches for default values or deny if validation fails
        AdmissionResponse::Allow
    }
}

/// PodSecurityStandards enforces Pod Security Standards (restricted, baseline, privileged)
pub struct PodSecurityStandardsController;

#[async_trait]
impl AdmissionController for PodSecurityStandardsController {
    fn name(&self) -> &str {
        "PodSecurityStandards"
    }

    async fn admit(&self, request: &AdmissionRequest) -> AdmissionResponse {
        // Only check Pod resources
        if request.kind != "Pod" {
            return AdmissionResponse::Allow;
        }

        // Parse the pod from the request object
        let pod: Pod = match serde_json::from_value(request.object.clone()) {
            Ok(p) => p,
            Err(e) => return AdmissionResponse::Deny(format!("Failed to parse Pod: {}", e)),
        };

        // Get namespace security level (default to baseline)
        // In real implementation, this would read from namespace labels
        let security_level = PodSecurityLevel::Baseline;

        // Check pod against security level
        if let Err(violations) = check_pod_security(&pod, &security_level) {
            return AdmissionResponse::Deny(format!(
                "Pod violates {} security standard: {}",
                security_level.as_str(),
                violations.join(", ")
            ));
        }

        AdmissionResponse::Allow
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PodSecurityLevel {
    Privileged, // Unrestricted
    Baseline,   // Minimally restrictive
    Restricted, // Heavily restricted
}

impl PodSecurityLevel {
    pub fn as_str(&self) -> &str {
        match self {
            PodSecurityLevel::Privileged => "privileged",
            PodSecurityLevel::Baseline => "baseline",
            PodSecurityLevel::Restricted => "restricted",
        }
    }
}

/// Check a pod against a security level
fn check_pod_security(pod: &Pod, level: &PodSecurityLevel) -> Result<(), Vec<String>> {
    let mut violations = Vec::new();

    match level {
        PodSecurityLevel::Privileged => {
            // Allow everything
            return Ok(());
        }
        PodSecurityLevel::Baseline => {
            // Baseline: disallow known privilege escalations
            if let Some(spec) = &pod.spec {
                // Check hostNetwork, hostPID, hostIPC
                if spec.host_network.unwrap_or(false) {
                    violations.push("hostNetwork=true not allowed in baseline mode".to_string());
                }
                if spec.host_pid.unwrap_or(false) {
                    violations.push("hostPID=true not allowed in baseline mode".to_string());
                }
                if spec.host_ipc.unwrap_or(false) {
                    violations.push("hostIPC=true not allowed in baseline mode".to_string());
                }

                // Check privileged containers
                for container in &spec.containers {
                    if let Some(security_context) = &container.security_context {
                        if security_context.privileged.unwrap_or(false) {
                            violations.push(format!(
                                "Container '{}' has privileged=true not allowed in baseline mode",
                                container.name
                            ));
                        }

                        // Check capabilities
                        if let Some(caps) = &security_context.capabilities {
                            if let Some(add) = &caps.add {
                                let disallowed: Vec<_> = add
                                    .iter()
                                    .filter(|c| !is_allowed_capability_baseline(c))
                                    .collect();
                                if !disallowed.is_empty() {
                                    violations.push(format!(
                                        "Container '{}' adds disallowed capabilities: {:?}",
                                        container.name, disallowed
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
        PodSecurityLevel::Restricted => {
            // Restricted: most restrictive policy
            if let Some(spec) = &pod.spec {
                // Disallow host namespaces
                if spec.host_network.unwrap_or(false) {
                    violations.push("hostNetwork=true not allowed in restricted mode".to_string());
                }
                if spec.host_pid.unwrap_or(false) {
                    violations.push("hostPID=true not allowed in restricted mode".to_string());
                }
                if spec.host_ipc.unwrap_or(false) {
                    violations.push("hostIPC=true not allowed in restricted mode".to_string());
                }

                // All containers must run as non-root
                for container in &spec.containers {
                    if let Some(security_context) = &container.security_context {
                        if security_context.privileged.unwrap_or(false) {
                            violations.push(format!(
                                "Container '{}' must not be privileged",
                                container.name
                            ));
                        }

                        if security_context.run_as_non_root != Some(true) {
                            violations.push(format!(
                                "Container '{}' must set runAsNonRoot=true",
                                container.name
                            ));
                        }

                        // No privilege escalation
                        if security_context.allow_privilege_escalation.unwrap_or(true) {
                            violations.push(format!(
                                "Container '{}' must set allowPrivilegeEscalation=false",
                                container.name
                            ));
                        }

                        // Must drop ALL capabilities
                        if let Some(caps) = &security_context.capabilities {
                            if caps
                                .drop
                                .as_ref()
                                .is_none_or(|d| !d.contains(&"ALL".to_string()))
                            {
                                violations.push(format!(
                                    "Container '{}' must drop ALL capabilities",
                                    container.name
                                ));
                            }
                        } else {
                            violations.push(format!(
                                "Container '{}' must drop ALL capabilities",
                                container.name
                            ));
                        }

                        // Seccomp profile required
                        if security_context.seccomp_profile.is_none() {
                            violations.push(format!(
                                "Container '{}' must define seccomp profile",
                                container.name
                            ));
                        }
                    } else {
                        violations.push(format!(
                            "Container '{}' must define securityContext",
                            container.name
                        ));
                    }
                }

                // Volume types restrictions
                if let Some(volumes) = &spec.volumes {
                    for volume in volumes {
                        // Check for hostPath volumes
                        if volume.host_path.is_some() {
                            violations.push(format!(
                                "Volume '{}' uses hostPath which is not allowed in restricted mode",
                                volume.name
                            ));
                        }
                    }
                }
            }
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

/// Check if a capability is allowed in baseline mode
fn is_allowed_capability_baseline(cap: &str) -> bool {
    matches!(
        cap.to_uppercase().as_str(),
        "AUDIT_WRITE"
            | "CHOWN"
            | "DAC_OVERRIDE"
            | "FOWNER"
            | "FSETID"
            | "KILL"
            | "MKNOD"
            | "NET_BIND_SERVICE"
            | "SETFCAP"
            | "SETGID"
            | "SETPCAP"
            | "SETUID"
            | "SYS_CHROOT"
    )
}

// ===== Admission Webhook Types (for external webhooks) =====

/// AdmissionReview is the top-level request/response object for admission webhooks
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionReview {
    pub api_version: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<AdmissionReviewRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<AdmissionReviewResponse>,
}

impl AdmissionReview {
    /// Create a new AdmissionReview request
    pub fn new_request(request: AdmissionReviewRequest) -> Self {
        Self {
            api_version: "admission.k8s.io/v1".to_string(),
            kind: "AdmissionReview".to_string(),
            request: Some(request),
            response: None,
        }
    }

    /// Create a new AdmissionReview response
    pub fn new_response(response: AdmissionReviewResponse) -> Self {
        Self {
            api_version: "admission.k8s.io/v1".to_string(),
            kind: "AdmissionReview".to_string(),
            request: None,
            response: Some(response),
        }
    }
}

/// AdmissionReviewRequest describes an admission request sent to a webhook
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionReviewRequest {
    /// UID is a unique identifier for this admission request
    pub uid: String,

    /// Kind is the fully-qualified type of the object being submitted
    pub kind: GroupVersionKind,

    /// Resource is the fully-qualified resource being requested
    pub resource: GroupVersionResource,

    /// SubResource is the sub-resource being requested, if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_resource: Option<String>,

    /// RequestKind is the type of the object in the original request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_kind: Option<GroupVersionKind>,

    /// RequestResource is the resource in the original request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_resource: Option<GroupVersionResource>,

    /// RequestSubResource is the sub-resource in the original request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_sub_resource: Option<String>,

    /// Name is the name of the object being modified
    pub name: String,

    /// Namespace is the namespace of the object being modified (if namespaced)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Operation is the operation being performed
    pub operation: Operation,

    /// UserInfo contains information about the user making the request
    pub user_info: UserInfo,

    /// Object is the object being admitted (for CREATE and UPDATE operations)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<serde_json::Value>,

    /// OldObject is the existing object (for UPDATE and DELETE operations)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_object: Option<serde_json::Value>,

    /// DryRun indicates the request is for a dry-run
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,

    /// Options contains the operation options (e.g., CreateOptions, UpdateOptions)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<serde_json::Value>,
}

/// GroupVersionKind identifies a type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GroupVersionKind {
    pub group: String,
    pub version: String,
    pub kind: String,
}

/// GroupVersionResource identifies a resource
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GroupVersionResource {
    pub group: String,
    pub version: String,
    pub resource: String,
}

/// AdmissionReviewResponse describes the admission response from a webhook
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionReviewResponse {
    /// UID echoes the UID from the request
    pub uid: String,

    /// Allowed indicates whether the request is allowed
    pub allowed: bool,

    /// Status contains extra details about the result (for denials)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<AdmissionStatus>,

    /// Patch is a JSONPatch to apply to the object (for mutating webhooks)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch: Option<String>, // Base64-encoded JSON patch

    /// PatchType is the type of patch (currently only "JSONPatch" is supported)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_type: Option<String>,

    /// AuditAnnotations are key-value pairs to add to the audit event
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audit_annotations: Option<HashMap<String, String>>,

    /// Warnings are messages to return to the user
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
}

impl AdmissionReviewResponse {
    /// Create an allowed response
    pub fn allow(uid: String) -> Self {
        Self {
            uid,
            allowed: true,
            status: None,
            patch: None,
            patch_type: None,
            audit_annotations: None,
            warnings: None,
        }
    }

    /// Create a denied response
    pub fn deny(uid: String, message: String) -> Self {
        Self {
            uid,
            allowed: false,
            status: Some(AdmissionStatus {
                status: "Failure".to_string(),
                message: Some(message),
                reason: Some("Denied by webhook".to_string()),
                code: Some(403),
                metadata: None,
            }),
            patch: None,
            patch_type: None,
            audit_annotations: None,
            warnings: None,
        }
    }

    /// Create an allowed response with a JSON patch
    pub fn allow_with_patch(uid: String, patch: Vec<PatchOperation>) -> Self {
        let patch_json = serde_json::to_string(&patch).unwrap_or_default();
        let patch_base64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            patch_json.as_bytes(),
        );

        Self {
            uid,
            allowed: true,
            status: None,
            patch: Some(patch_base64),
            patch_type: Some("JSONPatch".to_string()),
            audit_annotations: None,
            warnings: None,
        }
    }
}

/// AdmissionStatus contains extra details about the admission response.
/// Maps to metav1.Status in K8s which has metadata, status, message, reason, code.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionStatus {
    #[serde(default)]
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    /// K8s metav1.Status includes metadata — we ignore it but accept it
    #[serde(default, skip_serializing)]
    pub metadata: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admission_response() {
        let allow = AdmissionResponse::Allow;
        assert!(allow.is_allowed());
        assert!(allow.deny_reason().is_none());
        assert!(allow.patches().is_none());

        let deny = AdmissionResponse::Deny("test reason".to_string());
        assert!(!deny.is_allowed());
        assert_eq!(deny.deny_reason(), Some("test reason"));

        let patch = AdmissionResponse::AllowWithPatch(vec![]);
        assert!(patch.is_allowed());
        assert_eq!(patch.patches(), Some(&[][..]));
    }

    #[test]
    fn test_pod_security_levels() {
        assert_eq!(PodSecurityLevel::Privileged.as_str(), "privileged");
        assert_eq!(PodSecurityLevel::Baseline.as_str(), "baseline");
        assert_eq!(PodSecurityLevel::Restricted.as_str(), "restricted");
    }

    #[test]
    fn test_allowed_capabilities() {
        assert!(is_allowed_capability_baseline("NET_BIND_SERVICE"));
        assert!(is_allowed_capability_baseline("CHOWN"));
        assert!(!is_allowed_capability_baseline("SYS_ADMIN"));
        assert!(!is_allowed_capability_baseline("NET_ADMIN"));
    }

    #[test]
    fn test_admission_review_response() {
        let allow = AdmissionReviewResponse::allow("test-uid".to_string());
        assert!(allow.allowed);
        assert!(allow.status.is_none());

        let deny = AdmissionReviewResponse::deny("test-uid".to_string(), "denied".to_string());
        assert!(!deny.allowed);
        assert!(deny.status.is_some());
    }

    #[test]
    fn test_group_version_kind() {
        let gvk = GroupVersionKind {
            group: "apps".to_string(),
            version: "v1".to_string(),
            kind: "Deployment".to_string(),
        };
        assert_eq!(gvk.group, "apps");
        assert_eq!(gvk.version, "v1");
        assert_eq!(gvk.kind, "Deployment");
    }

    #[test]
    fn test_parse_real_webhook_response() {
        // Exact response from K8s conformance test webhook server
        let response_json = r#"{
            "kind": "AdmissionReview",
            "apiVersion": "admission.k8s.io/v1",
            "response": {
                "uid": "ab818250-607b-4060-812a-973a0c206d26",
                "allowed": false,
                "status": {
                    "metadata": {},
                    "message": "this webhook denies all requests"
                }
            }
        }"#;

        let review: AdmissionReview =
            serde_json::from_str(response_json).expect("Should parse real webhook response");
        let response = review.response.expect("Should have response");
        assert_eq!(response.uid, "ab818250-607b-4060-812a-973a0c206d26");
        assert!(!response.allowed);
        let status = response.status.expect("Should have status");
        assert_eq!(
            status.message.as_deref(),
            Some("this webhook denies all requests")
        );
    }

    #[test]
    fn test_parse_webhook_allow_response() {
        let response_json = r#"{
            "kind": "AdmissionReview",
            "apiVersion": "admission.k8s.io/v1",
            "response": {
                "uid": "test-uid",
                "allowed": true
            }
        }"#;

        let review: AdmissionReview =
            serde_json::from_str(response_json).expect("Should parse allow response");
        let response = review.response.expect("Should have response");
        assert!(response.allowed);
        assert!(response.status.is_none());
    }

    #[test]
    fn test_parse_webhook_mutating_response() {
        let response_json = r#"{
            "kind": "AdmissionReview",
            "apiVersion": "admission.k8s.io/v1",
            "response": {
                "uid": "test-uid",
                "allowed": true,
                "patchType": "JSONPatch",
                "patch": "W3sib3AiOiAiYWRkIiwgInBhdGgiOiAiL21ldGFkYXRhL2xhYmVscy9tdXRhdGVkIiwgInZhbHVlIjogInRydWUifV0="
            }
        }"#;

        let review: AdmissionReview =
            serde_json::from_str(response_json).expect("Should parse mutating response");
        let response = review.response.expect("Should have response");
        assert!(response.allowed);
        assert!(response.patch.is_some());
        assert_eq!(response.patch_type.as_deref(), Some("JSONPatch"));
    }
}
