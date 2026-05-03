use anyhow::{Context, Result};
use chrono::Utc;
use rusternetes_common::resources::{
    CertificateSigningRequest, CertificateSigningRequestCondition, CertificateSigningRequestStatus,
    KeyUsage,
};
use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// CertificateSigningRequestController manages certificate signing requests.
///
/// This controller:
/// 1. Watches CertificateSigningRequest resources
/// 2. Validates certificate requests (PEM format)
/// 3. Auto-approves requests based on policy (e.g., kubelet certificates)
/// 4. Updates CSR status with approval/denial
///
/// Note: Actual certificate signing is typically handled by external signers
/// like cert-manager or cloud provider certificate managers in production.
/// This controller focuses on request validation and auto-approval.
pub struct CertificateSigningRequestController<S: Storage> {
    storage: Arc<S>,
    auto_approve_kubelet_certs: bool,
}

impl<S: Storage + 'static> CertificateSigningRequestController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            auto_approve_kubelet_certs: true,
        }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        use futures::StreamExt;

        info!("Starting CertificateSigningRequest controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = rusternetes_storage::build_prefix("certificatesigningrequests", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let name = key
                .strip_prefix("certificatesigningrequests/")
                .unwrap_or(&key);
            let storage_key = build_key("certificatesigningrequests", None, name);
            match self
                .storage
                .get::<CertificateSigningRequest>(&storage_key)
                .await
            {
                Ok(resource) => match self.reconcile_csr(&resource).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("Failed to reconcile {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    // Resource was deleted — nothing to reconcile
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<CertificateSigningRequest>("/registry/certificatesigningrequests/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let key = format!("certificatesigningrequests/{}", item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!(
                    "Failed to list certificatesigningrequests for enqueue: {}",
                    e
                );
            }
        }
    }

    /// Main reconciliation loop - processes all CSR resources
    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting CertificateSigningRequest reconciliation");

        // List all CSRs (CSRs are cluster-scoped, not namespaced)
        let csrs: Vec<CertificateSigningRequest> = self
            .storage
            .list("/registry/certificatesigningrequests/")
            .await?;

        debug!(
            "Found {} certificate signing requests to reconcile",
            csrs.len()
        );

        for csr in csrs {
            if let Err(e) = self.reconcile_csr(&csr).await {
                error!("Failed to reconcile CSR {}: {}", &csr.metadata.name, e);
            }
        }

        Ok(())
    }

    /// Reconcile a single CertificateSigningRequest
    async fn reconcile_csr(&self, csr: &CertificateSigningRequest) -> Result<()> {
        let csr_name = &csr.metadata.name;

        debug!("Reconciling CSR {}", csr_name);

        // Validate the CSR spec
        if let Err(e) = self.validate_csr_spec(&csr.spec) {
            warn!("CSR {} validation failed: {}", csr_name, e);
            return self
                .deny_csr(csr, &format!("Validation failed: {}", e))
                .await;
        }

        // Check if CSR is already approved/denied
        if let Some(status) = &csr.status {
            if let Some(conditions) = &status.conditions {
                for condition in conditions {
                    match condition.type_.as_str() {
                        "Approved" => {
                            debug!("CSR {} is already approved", csr_name);
                            return Ok(());
                        }
                        "Denied" => {
                            debug!("CSR {} is already denied", csr_name);
                            return Ok(());
                        }
                        "Failed" => {
                            debug!("CSR {} has failed", csr_name);
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
        }

        // Auto-approve if policy allows
        if self.should_auto_approve(csr)? {
            info!("Auto-approving CSR {}", csr_name);
            return self.approve_csr(csr).await;
        }

        debug!(
            "CSR {} awaiting manual approval (signer: {})",
            csr_name, csr.spec.signer_name
        );

        Ok(())
    }

    /// Check if CSR should be auto-approved
    fn should_auto_approve(&self, csr: &CertificateSigningRequest) -> Result<bool> {
        if !self.auto_approve_kubelet_certs {
            return Ok(false);
        }

        // Auto-approve kubelet client certificates
        if csr.spec.signer_name == "kubernetes.io/kube-apiserver-client-kubelet" {
            // Validate this is a kubelet certificate request
            if csr.spec.usages.contains(&KeyUsage::ClientAuth)
                && csr.spec.usages.contains(&KeyUsage::DigitalSignature)
            {
                return Ok(true);
            }
        }

        // Auto-approve kubelet serving certificates
        if csr.spec.signer_name == "kubernetes.io/kubelet-serving"
            && csr.spec.usages.contains(&KeyUsage::ServerAuth)
            && csr.spec.usages.contains(&KeyUsage::DigitalSignature)
            && csr.spec.usages.contains(&KeyUsage::KeyEncipherment)
        {
            return Ok(true);
        }

        Ok(false)
    }

    /// Approve a CSR
    async fn approve_csr(&self, csr: &CertificateSigningRequest) -> Result<()> {
        let mut updated_csr = csr.clone();

        // Add Approved condition
        let now = Utc::now().to_rfc3339();
        let condition = CertificateSigningRequestCondition {
            type_: "Approved".to_string(),
            status: "True".to_string(),
            reason: Some("AutoApproved".to_string()),
            message: Some(format!(
                "Auto-approved by CSR controller (signer: {})",
                csr.spec.signer_name
            )),
            last_update_time: Some(now.clone()),
            last_transition_time: Some(now),
        };

        let mut conditions = csr
            .status
            .as_ref()
            .and_then(|s| s.conditions.clone())
            .unwrap_or_default();
        conditions.push(condition);

        updated_csr.status = Some(CertificateSigningRequestStatus {
            conditions: Some(conditions),
            certificate: None, // External signer will add the certificate
        });

        // Save approval
        self.storage
            .update(
                &format!("/registry/certificatesigningrequests/{}", csr.metadata.name),
                &updated_csr,
            )
            .await
            .context("Failed to save CSR approval")?;

        info!(
            "Approved CSR {} - awaiting external signer",
            csr.metadata.name
        );
        Ok(())
    }

    /// Deny a CSR
    async fn deny_csr(&self, csr: &CertificateSigningRequest, reason: &str) -> Result<()> {
        let mut updated_csr = csr.clone();

        let now = Utc::now().to_rfc3339();
        let condition = CertificateSigningRequestCondition {
            type_: "Denied".to_string(),
            status: "True".to_string(),
            reason: Some("Denied".to_string()),
            message: Some(reason.to_string()),
            last_update_time: Some(now.clone()),
            last_transition_time: Some(now),
        };

        let mut conditions = csr
            .status
            .as_ref()
            .and_then(|s| s.conditions.clone())
            .unwrap_or_default();
        conditions.push(condition);

        updated_csr.status = Some(CertificateSigningRequestStatus {
            conditions: Some(conditions),
            certificate: None,
        });

        self.storage
            .update(
                &format!("/registry/certificatesigningrequests/{}", csr.metadata.name),
                &updated_csr,
            )
            .await
            .context("Failed to save CSR denial")?;

        Ok(())
    }

    /// Validate CSR spec
    fn validate_csr_spec(
        &self,
        spec: &rusternetes_common::resources::CertificateSigningRequestSpec,
    ) -> Result<()> {
        // Validate request is present
        if spec.request.is_empty() {
            return Err(anyhow::anyhow!("CSR request cannot be empty"));
        }

        // Validate signerName is present
        if spec.signer_name.is_empty() {
            return Err(anyhow::anyhow!("CSR signerName cannot be empty"));
        }

        // Validate usages
        if spec.usages.is_empty() {
            return Err(anyhow::anyhow!("CSR must specify at least one usage"));
        }

        // Validate known usages
        for usage in &spec.usages {
            self.validate_key_usage(usage)?;
        }

        // Validate the request format (PEM)
        self.validate_pem_format(&spec.request)
            .context("Invalid CSR request format")?;

        Ok(())
    }

    /// Validate PEM format
    fn validate_pem_format(&self, request: &str) -> Result<()> {
        use base64::{engine::general_purpose, Engine as _};

        // Decode base64
        let decoded = general_purpose::STANDARD.decode(request.trim()).or_else(
            |_| -> Result<Vec<u8>, base64::DecodeError> {
                // If not base64, try treating as raw PEM
                Ok(request.as_bytes().to_vec())
            },
        )?;

        // Parse PEM
        let pem_items = pem::parse_many(&decoded)?;
        let _ = pem_items
            .into_iter()
            .find(|p| p.tag() == "CERTIFICATE REQUEST" || p.tag() == "NEW CERTIFICATE REQUEST")
            .ok_or_else(|| anyhow::anyhow!("No certificate request found in PEM"))?;

        Ok(())
    }

    /// Validate key usage value
    fn validate_key_usage(&self, usage: &KeyUsage) -> Result<()> {
        // All defined KeyUsage variants are valid
        match usage {
            KeyUsage::Signing
            | KeyUsage::DigitalSignature
            | KeyUsage::ContentCommitment
            | KeyUsage::KeyEncipherment
            | KeyUsage::KeyAgreement
            | KeyUsage::DataEncipherment
            | KeyUsage::CertSign
            | KeyUsage::CRLSign
            | KeyUsage::EncipherOnly
            | KeyUsage::DecipherOnly
            | KeyUsage::Any
            | KeyUsage::ServerAuth
            | KeyUsage::ClientAuth
            | KeyUsage::CodeSigning
            | KeyUsage::EmailProtection
            | KeyUsage::SMIME
            | KeyUsage::IPSECEndSystem
            | KeyUsage::IPSECTunnel
            | KeyUsage::IPSECUser
            | KeyUsage::Timestamping
            | KeyUsage::OCSPSigning
            | KeyUsage::MicrosoftSGC
            | KeyUsage::NetscapeSGC => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{CertificateSigningRequest, CertificateSigningRequestSpec};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_validate_csr_spec_valid() {
        use base64::{engine::general_purpose, Engine as _};

        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        // Create a CSR for testing (using rcgen for test data generation)
        let params = rcgen::CertificateParams::new(vec!["test.example.com".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        // Generate CSR, not a certificate
        let csr_der = params.serialize_request(&key_pair).unwrap();
        let csr_pem = pem::encode(&pem::Pem::new(
            "CERTIFICATE REQUEST",
            csr_der.der().to_vec(),
        ));
        let csr_b64 = general_purpose::STANDARD.encode(csr_pem);

        let spec = CertificateSigningRequestSpec {
            request: csr_b64,
            signer_name: "kubernetes.io/kube-apiserver-client".to_string(),
            usages: vec![KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
            expiration_seconds: Some(3600),
            uid: None,
            groups: None,
            username: None,
            extra: None,
        };

        assert!(controller.validate_csr_spec(&spec).is_ok());
    }

    #[tokio::test]
    async fn test_validate_csr_spec_empty_request() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        let spec = CertificateSigningRequestSpec {
            request: "".to_string(),
            signer_name: "kubernetes.io/kube-apiserver-client".to_string(),
            usages: vec![KeyUsage::ClientAuth],
            expiration_seconds: Some(3600),
            uid: None,
            groups: None,
            username: None,
            extra: None,
        };

        assert!(controller.validate_csr_spec(&spec).is_err());
    }

    #[tokio::test]
    async fn test_validate_csr_spec_empty_signer() {
        use base64::{engine::general_purpose, Engine as _};

        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        let params = rcgen::CertificateParams::new(vec!["test.example.com".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let csr_der = params.serialize_request(&key_pair).unwrap();
        let csr_pem = pem::encode(&pem::Pem::new(
            "CERTIFICATE REQUEST",
            csr_der.der().to_vec(),
        ));
        let csr_b64 = general_purpose::STANDARD.encode(csr_pem);

        let spec = CertificateSigningRequestSpec {
            request: csr_b64,
            signer_name: "".to_string(),
            usages: vec![KeyUsage::ClientAuth],
            expiration_seconds: Some(3600),
            uid: None,
            groups: None,
            username: None,
            extra: None,
        };

        assert!(controller.validate_csr_spec(&spec).is_err());
    }

    #[tokio::test]
    async fn test_validate_csr_spec_no_usages() {
        use base64::{engine::general_purpose, Engine as _};

        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        let params = rcgen::CertificateParams::new(vec!["test.example.com".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let csr_der = params.serialize_request(&key_pair).unwrap();
        let csr_pem = pem::encode(&pem::Pem::new(
            "CERTIFICATE REQUEST",
            csr_der.der().to_vec(),
        ));
        let csr_b64 = general_purpose::STANDARD.encode(csr_pem);

        let spec = CertificateSigningRequestSpec {
            request: csr_b64,
            signer_name: "kubernetes.io/kube-apiserver-client".to_string(),
            usages: vec![],
            expiration_seconds: Some(3600),
            uid: None,
            groups: None,
            username: None,
            extra: None,
        };

        assert!(controller.validate_csr_spec(&spec).is_err());
    }

    #[tokio::test]
    async fn test_validate_key_usage() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        // Test common usages
        assert!(controller
            .validate_key_usage(&KeyUsage::DigitalSignature)
            .is_ok());
        assert!(controller.validate_key_usage(&KeyUsage::ClientAuth).is_ok());
        assert!(controller.validate_key_usage(&KeyUsage::ServerAuth).is_ok());
        assert!(controller
            .validate_key_usage(&KeyUsage::KeyEncipherment)
            .is_ok());
    }

    #[tokio::test]
    async fn test_should_auto_approve_kubelet_client() {
        use base64::{engine::general_purpose, Engine as _};

        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        let params = rcgen::CertificateParams::new(vec!["system:node:test".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let csr_der = params.serialize_request(&key_pair).unwrap();
        let csr_pem = pem::encode(&pem::Pem::new(
            "CERTIFICATE REQUEST",
            csr_der.der().to_vec(),
        ));
        let csr_b64 = general_purpose::STANDARD.encode(csr_pem);

        let csr = CertificateSigningRequest {
            api_version: "certificates.k8s.io/v1".to_string(),
            kind: "CertificateSigningRequest".to_string(),
            metadata: ObjectMeta::new("kubelet-client-test"),
            spec: CertificateSigningRequestSpec {
                request: csr_b64,
                signer_name: "kubernetes.io/kube-apiserver-client-kubelet".to_string(),
                usages: vec![KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
                expiration_seconds: Some(3600),
                uid: None,
                groups: None,
                username: Some("system:node:test".to_string()),
                extra: None,
            },
            status: None,
        };

        assert!(controller.should_auto_approve(&csr).unwrap());
    }

    #[tokio::test]
    async fn test_should_auto_approve_kubelet_serving() {
        use base64::{engine::general_purpose, Engine as _};

        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage);

        let params = rcgen::CertificateParams::new(vec!["node1.example.com".to_string()]).unwrap();
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let csr_der = params.serialize_request(&key_pair).unwrap();
        let csr_pem = pem::encode(&pem::Pem::new(
            "CERTIFICATE REQUEST",
            csr_der.der().to_vec(),
        ));
        let csr_b64 = general_purpose::STANDARD.encode(csr_pem);

        let csr = CertificateSigningRequest {
            api_version: "certificates.k8s.io/v1".to_string(),
            kind: "CertificateSigningRequest".to_string(),
            metadata: ObjectMeta::new("kubelet-serving-test"),
            spec: CertificateSigningRequestSpec {
                request: csr_b64,
                signer_name: "kubernetes.io/kubelet-serving".to_string(),
                usages: vec![
                    KeyUsage::DigitalSignature,
                    KeyUsage::KeyEncipherment,
                    KeyUsage::ServerAuth,
                ],
                expiration_seconds: Some(3600),
                uid: None,
                groups: None,
                username: Some("system:node:test".to_string()),
                extra: None,
            },
            status: None,
        };

        assert!(controller.should_auto_approve(&csr).unwrap());
    }
}
