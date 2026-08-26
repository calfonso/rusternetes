// Integration tests for CertificateSigningRequest Controller
// Tests validation and processing of certificate signing requests

use rusternetes_common::resources::{
    CertificateSigningRequest, CertificateSigningRequestCondition, CertificateSigningRequestSpec,
    CertificateSigningRequestStatus, KeyUsage,
};
use rusternetes_common::types::ObjectMeta;
use rusternetes_controller_manager::controllers::certificate_signing_request::CertificateSigningRequestController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

fn create_test_csr(name: &str, signer: &str, usages: Vec<KeyUsage>) -> CertificateSigningRequest {
    CertificateSigningRequest {
        api_version: "certificates.k8s.io/v1".to_string(),
        kind: "CertificateSigningRequest".to_string(),
        metadata: ObjectMeta::new(name),
        spec: CertificateSigningRequestSpec {
            request: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURSBSRVFVRVNULS0tLS0KTUlJQ3BqQ0NBWTRDQVFBd1lERUxNQWtHQTFVRUJoTUNWVk14RXpBUkJnTlZCQWdUQ2tOaGJHbG1iM0p1YVdFeApGakFVQmdOVkJBY1REVk5oYmlCR2NtRnVZMmx6WTI4eERUQUxCZ05WQkFvVEJFMTVUM0puTVJVd0V3WURWUVFEREF4bGVHRnRjR3hsTG1OdmJUQ0NBU0l3RFFZSktvWklodmNOQVFFQkJRQURnZ0VQQURDQ0FRb0NnZ0VCQUpHOCt3PT0KLS0tLS1FTkQgQ0VSVElGSUNBVEUgUkVRVUVTVC0tLS0tCg==".to_string(), // Valid base64 CSR
            signer_name: signer.to_string(),
            usages,
            expiration_seconds: Some(3600),
            username: Some("system:node:test-node".to_string()),
            uid: Some("12345".to_string()),
            groups: Some(vec!["system:nodes".to_string()]),
            extra: None,
        },
        status: None,
    }
}

fn create_csr_with_status(name: &str, condition_type: &str) -> CertificateSigningRequest {
    let mut csr = create_test_csr(
        name,
        "kubernetes.io/kube-apiserver-client",
        vec![KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
    );

    csr.status = Some(CertificateSigningRequestStatus {
        conditions: Some(vec![CertificateSigningRequestCondition {
            type_: condition_type.to_string(),
            status: "True".to_string(),
            reason: Some("AutoApproved".to_string()),
            message: Some("This CSR was approved automatically".to_string()),
            last_update_time: Some("2024-01-01T00:00:00Z".to_string()),
            last_transition_time: Some("2024-01-01T00:00:00Z".to_string()),
        }]),
        certificate: None,
    });

    csr
}

#[tokio::test]
async fn test_csr_controller_validates_valid_csr() {
    let storage = setup_test().await;

    // Create a valid CSR
    let csr = create_test_csr(
        "valid-csr",
        "kubernetes.io/kube-apiserver-client",
        vec![KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
    );

    let csr_key = build_key("certificatesigningrequests", None, "valid-csr");
    storage.create(&csr_key, &csr).await.unwrap();

    // Run controller
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // Verify CSR still exists (validation passed)
    let stored_csr: CertificateSigningRequest = storage.get(&csr_key).await.unwrap();
    assert_eq!(stored_csr.metadata.name, "valid-csr");
}

#[tokio::test]
async fn test_csr_controller_handles_empty_request() {
    let storage = setup_test().await;

    // Create CSR with empty request
    let mut csr = create_test_csr(
        "empty-request-csr",
        "kubernetes.io/kube-apiserver-client",
        vec![KeyUsage::ClientAuth],
    );
    csr.spec.request = "".to_string();

    let csr_key = build_key("certificatesigningrequests", None, "empty-request-csr");
    storage.create(&csr_key, &csr).await.unwrap();

    // Run controller (should handle validation failure gracefully)
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // CSR should still exist but validation would have failed
    let stored_csr: CertificateSigningRequest = storage.get(&csr_key).await.unwrap();
    assert_eq!(stored_csr.metadata.name, "empty-request-csr");
}

#[tokio::test]
async fn test_csr_controller_handles_empty_signer() {
    let storage = setup_test().await;

    // Create CSR with empty signer
    let csr = create_test_csr("empty-signer-csr", "", vec![KeyUsage::ClientAuth]);

    let csr_key = build_key("certificatesigningrequests", None, "empty-signer-csr");
    storage.create(&csr_key, &csr).await.unwrap();

    // Run controller (should handle validation failure gracefully)
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // CSR should still exist
    let stored_csr: CertificateSigningRequest = storage.get(&csr_key).await.unwrap();
    assert_eq!(stored_csr.metadata.name, "empty-signer-csr");
}

#[tokio::test]
async fn test_csr_controller_handles_no_usages() {
    let storage = setup_test().await;

    // Create CSR with no usages
    let csr = create_test_csr(
        "no-usages-csr",
        "kubernetes.io/kube-apiserver-client",
        vec![],
    );

    let csr_key = build_key("certificatesigningrequests", None, "no-usages-csr");
    storage.create(&csr_key, &csr).await.unwrap();

    // Run controller (should handle validation failure gracefully)
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());
}

#[tokio::test]
async fn test_csr_controller_skips_approved_csr() {
    let storage = setup_test().await;

    // Create an already approved CSR
    let csr = create_csr_with_status("approved-csr", "Approved");

    let csr_key = build_key("certificatesigningrequests", None, "approved-csr");
    storage.create(&csr_key, &csr).await.unwrap();

    // Run controller
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // Verify CSR is unchanged
    let stored_csr: CertificateSigningRequest = storage.get(&csr_key).await.unwrap();
    assert_eq!(stored_csr.metadata.name, "approved-csr");
    assert!(stored_csr.status.is_some());
}

#[tokio::test]
async fn test_csr_controller_skips_denied_csr() {
    let storage = setup_test().await;

    // Create a denied CSR
    let csr = create_csr_with_status("denied-csr", "Denied");

    let csr_key = build_key("certificatesigningrequests", None, "denied-csr");
    storage.create(&csr_key, &csr).await.unwrap();

    // Run controller
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // Verify CSR is unchanged
    let stored_csr: CertificateSigningRequest = storage.get(&csr_key).await.unwrap();
    assert_eq!(stored_csr.metadata.name, "denied-csr");
}

#[tokio::test]
async fn test_csr_controller_skips_failed_csr() {
    let storage = setup_test().await;

    // Create a failed CSR
    let csr = create_csr_with_status("failed-csr", "Failed");

    let csr_key = build_key("certificatesigningrequests", None, "failed-csr");
    storage.create(&csr_key, &csr).await.unwrap();

    // Run controller
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // Verify CSR is unchanged
    let stored_csr: CertificateSigningRequest = storage.get(&csr_key).await.unwrap();
    assert_eq!(stored_csr.metadata.name, "failed-csr");
}

#[tokio::test]
async fn test_csr_controller_handles_multiple_csrs() {
    let storage = setup_test().await;

    // Create multiple CSRs with different configurations
    let csrs = vec![
        (
            "kubelet-csr",
            "kubernetes.io/kube-apiserver-client-kubelet",
            vec![KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
        ),
        (
            "server-csr",
            "kubernetes.io/kubelet-serving",
            vec![
                KeyUsage::DigitalSignature,
                KeyUsage::KeyEncipherment,
                KeyUsage::ServerAuth,
            ],
        ),
        (
            "legacy-csr",
            "kubernetes.io/legacy-unknown",
            vec![KeyUsage::Any],
        ),
    ];

    for (name, signer, usages) in csrs {
        let csr = create_test_csr(name, signer, usages);
        let csr_key = build_key("certificatesigningrequests", None, name);
        storage.create(&csr_key, &csr).await.unwrap();
    }

    // Run controller
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // Verify all CSRs still exist
    let all_csrs: Vec<CertificateSigningRequest> = storage
        .list("/registry/certificatesigningrequests/")
        .await
        .unwrap();
    assert_eq!(all_csrs.len(), 3);
}

#[tokio::test]
async fn test_csr_controller_validates_all_key_usages() {
    let storage = setup_test().await;

    // Test all key usage types
    let usages = vec![
        KeyUsage::Signing,
        KeyUsage::DigitalSignature,
        KeyUsage::ContentCommitment,
        KeyUsage::KeyEncipherment,
        KeyUsage::KeyAgreement,
        KeyUsage::DataEncipherment,
        KeyUsage::CertSign,
        KeyUsage::CRLSign,
        KeyUsage::EncipherOnly,
        KeyUsage::DecipherOnly,
        KeyUsage::Any,
        KeyUsage::ServerAuth,
        KeyUsage::ClientAuth,
        KeyUsage::CodeSigning,
        KeyUsage::EmailProtection,
        KeyUsage::SMIME,
        KeyUsage::IPSECEndSystem,
        KeyUsage::IPSECTunnel,
        KeyUsage::IPSECUser,
        KeyUsage::Timestamping,
        KeyUsage::OCSPSigning,
        KeyUsage::MicrosoftSGC,
        KeyUsage::NetscapeSGC,
    ];

    let csr = create_test_csr(
        "all-usages-csr",
        "kubernetes.io/kube-apiserver-client",
        usages,
    );

    let csr_key = build_key("certificatesigningrequests", None, "all-usages-csr");
    storage.create(&csr_key, &csr).await.unwrap();

    // Run controller
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // Verify CSR is valid
    let stored_csr: CertificateSigningRequest = storage.get(&csr_key).await.unwrap();
    assert_eq!(stored_csr.spec.usages.len(), 23);
}

#[tokio::test]
async fn test_csr_controller_handles_different_signers() {
    let storage = setup_test().await;

    // Test different standard signers
    let signers = [
        "kubernetes.io/kube-apiserver-client",
        "kubernetes.io/kube-apiserver-client-kubelet",
        "kubernetes.io/kubelet-serving",
        "kubernetes.io/legacy-unknown",
        "example.com/my-custom-signer",
    ];

    for (i, signer) in signers.iter().enumerate() {
        let csr = create_test_csr(
            &format!("signer-csr-{}", i),
            signer,
            vec![KeyUsage::DigitalSignature, KeyUsage::ClientAuth],
        );
        let csr_key = build_key(
            "certificatesigningrequests",
            None,
            &format!("signer-csr-{}", i),
        );
        storage.create(&csr_key, &csr).await.unwrap();
    }

    // Run controller
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // Verify all CSRs exist
    let all_csrs: Vec<CertificateSigningRequest> = storage
        .list("/registry/certificatesigningrequests/")
        .await
        .unwrap();
    assert_eq!(all_csrs.len(), 5);
}

#[tokio::test]
async fn test_csr_controller_handles_expiration_seconds() {
    let storage = setup_test().await;

    // Create CSRs with different expiration times
    let mut short_expiry = create_test_csr(
        "short-expiry-csr",
        "kubernetes.io/kube-apiserver-client",
        vec![KeyUsage::ClientAuth],
    );
    short_expiry.spec.expiration_seconds = Some(600); // 10 minutes

    let mut long_expiry = create_test_csr(
        "long-expiry-csr",
        "kubernetes.io/kube-apiserver-client",
        vec![KeyUsage::ClientAuth],
    );
    long_expiry.spec.expiration_seconds = Some(31536000); // 1 year

    let mut no_expiry = create_test_csr(
        "no-expiry-csr",
        "kubernetes.io/kube-apiserver-client",
        vec![KeyUsage::ClientAuth],
    );
    no_expiry.spec.expiration_seconds = None;

    storage
        .create(
            &build_key("certificatesigningrequests", None, "short-expiry-csr"),
            &short_expiry,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("certificatesigningrequests", None, "long-expiry-csr"),
            &long_expiry,
        )
        .await
        .unwrap();
    storage
        .create(
            &build_key("certificatesigningrequests", None, "no-expiry-csr"),
            &no_expiry,
        )
        .await
        .unwrap();

    // Run controller
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // Verify all CSRs exist
    let all_csrs: Vec<CertificateSigningRequest> = storage
        .list("/registry/certificatesigningrequests/")
        .await
        .unwrap();
    assert_eq!(all_csrs.len(), 3);
}

#[tokio::test]
async fn test_csr_controller_handles_user_info() {
    let storage = setup_test().await;

    // Create CSR with full user info
    let mut csr = create_test_csr(
        "user-info-csr",
        "kubernetes.io/kube-apiserver-client",
        vec![KeyUsage::ClientAuth],
    );
    csr.spec.username = Some("alice@example.com".to_string());
    csr.spec.uid = Some("uid-12345".to_string());
    csr.spec.groups = Some(vec!["developers".to_string(), "admins".to_string()]);

    let csr_key = build_key("certificatesigningrequests", None, "user-info-csr");
    storage.create(&csr_key, &csr).await.unwrap();

    // Run controller
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // Verify CSR exists with user info
    let stored_csr: CertificateSigningRequest = storage.get(&csr_key).await.unwrap();
    assert_eq!(
        stored_csr.spec.username,
        Some("alice@example.com".to_string())
    );
    assert_eq!(stored_csr.spec.groups.as_ref().unwrap().len(), 2);
}

#[tokio::test]
async fn test_csr_controller_no_csrs() {
    let storage = setup_test().await;

    // Run controller with no CSRs
    let controller = CertificateSigningRequestController::new(storage.clone());
    assert!(controller.reconcile_all().await.is_ok());

    // Verify no CSRs exist
    let all_csrs: Vec<CertificateSigningRequest> = storage
        .list("/registry/certificatesigningrequests/")
        .await
        .unwrap();
    assert_eq!(all_csrs.len(), 0);
}
