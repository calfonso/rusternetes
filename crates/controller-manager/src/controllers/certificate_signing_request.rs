use anyhow::{Context, Result};
use chrono::Utc;
use rusternetes_common::resources::{
    CertificateSigningRequest, CertificateSigningRequestCondition, CertificateSigningRequestStatus,
    KeyUsage,
};
use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::cert_authority::CertificateAuthority;

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
    /// Cluster signer. When present, the controller issues a certificate into
    /// `status.certificate` for approved CSRs (mirroring the upstream
    /// kube-controller-manager signer). When `None`, issuance is left to an
    /// external signer — the controller only validates + auto-approves.
    ca: Option<Arc<CertificateAuthority>>,
}

impl<S: Storage + 'static> CertificateSigningRequestController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            auto_approve_kubelet_certs: true,
            ca: None,
        }
    }

    /// Attach a cluster certificate authority so approved CSRs are signed
    /// in-process. Without it the controller behaves as before (approve only).
    pub fn with_certificate_authority(mut self, ca: Arc<CertificateAuthority>) -> Self {
        self.ca = Some(ca);
        self
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

        // Inspect existing conditions + whether a certificate was already issued.
        let mut approved = false;
        if let Some(status) = &csr.status {
            if let Some(conditions) = &status.conditions {
                for condition in conditions {
                    match condition.type_.as_str() {
                        "Approved" => approved = true,
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
        let already_issued = csr
            .status
            .as_ref()
            .and_then(|s| s.certificate.as_ref())
            .is_some_and(|c| !c.is_empty());

        if approved {
            if already_issued {
                debug!("CSR {} already has an issued certificate", csr_name);
                return Ok(());
            }
            // Sign it ourselves if a cluster CA is configured; otherwise leave
            // issuance to an external signer (unchanged behaviour).
            if self.ca.is_some() {
                return self.issue_certificate(csr).await;
            }
            debug!("CSR {} approved, awaiting external signer", csr_name);
            return Ok(());
        }

        // Auto-approve if policy allows, then issue immediately when we are the
        // signer so a freshly auto-approved kubelet CSR gets its cert in one pass.
        if self.should_auto_approve(csr)? {
            info!("Auto-approving CSR {}", csr_name);
            self.approve_csr(csr).await?;
            if self.ca.is_some() {
                let key =
                    format!("/registry/certificatesigningrequests/{}", csr.metadata.name);
                if let Ok(approved_csr) =
                    self.storage.get::<CertificateSigningRequest>(&key).await
                {
                    return self.issue_certificate(&approved_csr).await;
                }
            }
            return Ok(());
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

    /// Sign an approved CSR with the configured cluster CA and write the issued
    /// certificate into `status.certificate`. On a signing/parse error the CSR
    /// is marked `Failed` (terminal) rather than retried forever.
    async fn issue_certificate(&self, csr: &CertificateSigningRequest) -> Result<()> {
        let Some(ca) = &self.ca else {
            return Ok(());
        };

        let request_pem = match decode_request_pem(&csr.spec.request) {
            Ok(pem) => pem,
            Err(e) => return self.fail_csr(csr, &format!("invalid request: {e}")).await,
        };

        let cert_pem = match ca.sign(&request_pem, &csr.spec.usages, csr.spec.expiration_seconds) {
            Ok(pem) => pem,
            Err(e) => {
                warn!("Failed to sign CSR {}: {}", csr.metadata.name, e);
                return self.fail_csr(csr, &format!("signing failed: {e}")).await;
            }
        };

        let mut updated = csr.clone();
        let status = updated
            .status
            .get_or_insert_with(|| CertificateSigningRequestStatus {
                conditions: None,
                certificate: None,
            });
        status.certificate = Some(cert_pem);

        self.storage
            .update(
                &format!("/registry/certificatesigningrequests/{}", csr.metadata.name),
                &updated,
            )
            .await
            .context("Failed to persist issued certificate")?;

        info!("Issued certificate for CSR {}", csr.metadata.name);
        Ok(())
    }

    /// Mark a CSR `Failed` (terminal) — used when signing cannot proceed.
    async fn fail_csr(&self, csr: &CertificateSigningRequest, reason: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let condition = CertificateSigningRequestCondition {
            type_: "Failed".to_string(),
            status: "True".to_string(),
            reason: Some("SignerFailure".to_string()),
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
        let certificate = csr.status.as_ref().and_then(|s| s.certificate.clone());

        let mut updated = csr.clone();
        updated.status = Some(CertificateSigningRequestStatus {
            conditions: Some(conditions),
            certificate,
        });
        self.storage
            .update(
                &format!("/registry/certificatesigningrequests/{}", csr.metadata.name),
                &updated,
            )
            .await
            .context("Failed to persist CSR failure")?;
        warn!("CSR {} failed: {}", csr.metadata.name, reason);
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

/// Decode `spec.request` into PEM text. The field is `[]byte` on the wire
/// (base64-encoded PEM); fall back to treating it as raw PEM, matching
/// `validate_pem_format`.
fn decode_request_pem(request: &str) -> Result<String> {
    use base64::{engine::general_purpose, Engine as _};
    let bytes = general_purpose::STANDARD
        .decode(request.trim())
        .unwrap_or_else(|_| request.as_bytes().to_vec());
    String::from_utf8(bytes).context("CSR request is not valid UTF-8 PEM")
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

    /// Build a self-signed CA and return `(CertificateAuthority, base64-PEM CSR
    /// for `cn`)` ready to drop into a CSR `spec.request`.
    fn ca_and_request(cn: &str) -> (Arc<crate::controllers::cert_authority::CertificateAuthority>, String) {
        use base64::{engine::general_purpose, Engine as _};

        let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "rusternetes-ca");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca = Arc::new(
            crate::controllers::cert_authority::CertificateAuthority::from_pem(
                &ca_cert.pem(),
                &ca_key.serialize_pem(),
            )
            .unwrap(),
        );

        let leaf_params = rcgen::CertificateParams::new(vec![cn.to_string()]).unwrap();
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let req = leaf_params.serialize_request(&leaf_key).unwrap();
        let req_pem = pem::encode(&pem::Pem::new("CERTIFICATE REQUEST", req.der().to_vec()));
        let request_b64 = general_purpose::STANDARD.encode(req_pem);
        (ca, request_b64)
    }

    fn approved_csr(name: &str, request_b64: String) -> CertificateSigningRequest {
        CertificateSigningRequest {
            api_version: "certificates.k8s.io/v1".to_string(),
            kind: "CertificateSigningRequest".to_string(),
            metadata: ObjectMeta::new(name),
            spec: CertificateSigningRequestSpec {
                request: request_b64,
                signer_name: "kubernetes.io/kube-apiserver-client".to_string(),
                usages: vec![KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
                expiration_seconds: Some(86400),
                uid: None,
                groups: None,
                username: None,
                extra: None,
            },
            status: Some(CertificateSigningRequestStatus {
                conditions: Some(vec![CertificateSigningRequestCondition {
                    type_: "Approved".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_update_time: None,
                    last_transition_time: None,
                }]),
                certificate: None,
            }),
        }
    }

    #[tokio::test]
    async fn reconcile_issues_certificate_for_approved_csr_when_ca_present() {
        let (ca, request_b64) = ca_and_request("system:node:n1");
        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage.clone())
            .with_certificate_authority(ca);

        let csr = approved_csr("issue-me", request_b64);
        let key = "/registry/certificatesigningrequests/issue-me";
        storage.create(key, &csr).await.unwrap();

        controller.reconcile_csr(&csr).await.unwrap();

        let updated: CertificateSigningRequest = storage.get(key).await.unwrap();
        let cert = updated
            .status
            .and_then(|s| s.certificate)
            .expect("status.certificate must be populated by the signer");
        assert!(
            cert.contains("BEGIN CERTIFICATE"),
            "issued certificate must be PEM; got {cert}"
        );
    }

    #[tokio::test]
    async fn reconcile_leaves_certificate_empty_when_no_ca_configured() {
        // Without a configured signer the controller must not invent a cert —
        // it leaves issuance to an external signer (pre-existing behaviour).
        let (_ca, request_b64) = ca_and_request("system:node:n2");
        let storage = Arc::new(MemoryStorage::new());
        let controller = CertificateSigningRequestController::new(storage.clone());

        let csr = approved_csr("no-signer", request_b64);
        let key = "/registry/certificatesigningrequests/no-signer";
        storage.create(key, &csr).await.unwrap();

        controller.reconcile_csr(&csr).await.unwrap();

        let updated: CertificateSigningRequest = storage.get(key).await.unwrap();
        assert!(
            updated.status.and_then(|s| s.certificate).is_none(),
            "no CA configured → status.certificate must stay empty"
        );
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
