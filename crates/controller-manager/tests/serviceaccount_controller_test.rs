//! Integration tests for ServiceAccountController
//!
//! Mirrors behaviour exercised by upstream `kubernetes/test/e2e/auth/serviceaccount.go`
//! against rusternetes' in-process controller. Extended coverage includes
//! automount preservation, imagePullSecrets retention, multi-namespace fan-out,
//! token-secret annotations, and RED-state gaps for bound-token projection and
//! pod-spec propagation of SA-level imagePullSecrets.

use rusternetes_common::resources::{
    namespace::{NamespaceSpec, NamespaceStatus},
    LocalObjectReference, Namespace, Secret, ServiceAccount,
};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::serviceaccount::ServiceAccountController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

/// Build an Active Namespace ready for use in tests.
fn active_namespace(name: &str) -> Namespace {
    Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Active),
            conditions: None,
        }),
    }
}

/// Build a ServiceAccount in `namespace` named `name`.
fn service_account(namespace: &str, name: &str) -> ServiceAccount {
    ServiceAccount {
        type_meta: TypeMeta {
            kind: "ServiceAccount".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some(namespace.to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        secrets: None,
        image_pull_secrets: None,
        automount_service_account_token: Some(true),
    }
}

#[tokio::test]
async fn test_serviceaccount_controller_creation() {
    let storage = Arc::new(MemoryStorage::new());
    let _controller = ServiceAccountController::new(storage);
}

#[tokio::test]
async fn test_serviceaccount_creates_default_in_namespace() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    // Create a test namespace
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-sa-namespace".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Active),
            conditions: None,
        }),
    };

    let ns_key = build_key("namespaces", None, "test-sa-namespace");
    storage.create(&ns_key, &namespace).await.unwrap();

    // Reconcile should create default ServiceAccount
    controller.reconcile_all().await.unwrap();

    // Verify default ServiceAccount was created
    let sa_key = build_key("serviceaccounts", Some("test-sa-namespace"), "default");
    let sa: ServiceAccount = storage.get(&sa_key).await.unwrap();
    assert_eq!(sa.metadata.name, "default");
    assert_eq!(sa.metadata.namespace.as_ref().unwrap(), "test-sa-namespace");

    // Verify token secret was created
    let secret_key = build_key("secrets", Some("test-sa-namespace"), "default-token");
    let secret: Secret = storage.get(&secret_key).await.unwrap();
    assert_eq!(secret.metadata.name, "default-token");
    assert_eq!(
        secret.secret_type.as_ref().unwrap(),
        "kubernetes.io/service-account-token"
    );

    // Clean up
    storage.delete(&sa_key).await.unwrap();
    storage.delete(&secret_key).await.unwrap();
    storage.delete(&ns_key).await.unwrap();
}

#[tokio::test]
async fn test_serviceaccount_does_not_recreate_existing() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    // Create a namespace
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-existing-sa".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Active),
            conditions: None,
        }),
    };

    let ns_key = build_key("namespaces", None, "test-existing-sa");
    storage.create(&ns_key, &namespace).await.unwrap();

    // Create default ServiceAccount manually
    let service_account = ServiceAccount {
        type_meta: TypeMeta {
            kind: "ServiceAccount".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "default".to_string(),
            namespace: Some("test-existing-sa".to_string()),
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        secrets: None,
        image_pull_secrets: None,
        automount_service_account_token: Some(true),
    };

    let sa_key = build_key("serviceaccounts", Some("test-existing-sa"), "default");
    storage.create(&sa_key, &service_account).await.unwrap();

    // Reconcile should not recreate
    controller.reconcile_all().await.unwrap();

    // ServiceAccount should still exist with same UID
    let retrieved: ServiceAccount = storage.get(&sa_key).await.unwrap();
    assert_eq!(retrieved.metadata.uid, service_account.metadata.uid);

    // Clean up
    storage.delete(&sa_key).await.unwrap();
    storage.delete(&ns_key).await.unwrap();
}

#[tokio::test]
async fn test_serviceaccount_token_contains_required_fields() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    // Create a namespace
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-token-fields".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: None,
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Active),
            conditions: None,
        }),
    };

    let ns_key = build_key("namespaces", None, "test-token-fields");
    storage.create(&ns_key, &namespace).await.unwrap();

    // Reconcile
    controller.reconcile_all().await.unwrap();

    // Get the token secret
    let secret_key = build_key("secrets", Some("test-token-fields"), "default-token");
    let secret: Secret = storage.get(&secret_key).await.unwrap();

    // Verify secret has required fields
    assert!(secret.data.is_some());
    let data = secret.data.as_ref().unwrap();

    // Should have token, namespace, and ca.crt
    assert!(data.contains_key("token"));
    assert!(data.contains_key("namespace"));
    assert!(data.contains_key("ca.crt"));

    // Token should not be empty
    let token = data.get("token").unwrap();
    assert!(!token.is_empty());

    // Namespace should match
    let namespace_bytes = data.get("namespace").unwrap();
    let namespace_str = String::from_utf8(namespace_bytes.clone()).unwrap();
    assert_eq!(namespace_str, "test-token-fields");

    // Clean up
    let sa_key = build_key("serviceaccounts", Some("test-token-fields"), "default");
    storage.delete(&sa_key).await.unwrap();
    storage.delete(&secret_key).await.unwrap();
    storage.delete(&ns_key).await.unwrap();
}

#[tokio::test]
async fn test_serviceaccount_skips_terminating_namespaces() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    // Create a namespace that's being deleted
    let namespace = Namespace {
        type_meta: TypeMeta {
            kind: "Namespace".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: "test-terminating".to_string(),
            namespace: None,
            uid: uuid::Uuid::new_v4().to_string(),
            resource_version: None,
            deletion_grace_period_seconds: None,
            finalizers: None,
            owner_references: None,
            creation_timestamp: Some(chrono::Utc::now()),
            deletion_timestamp: Some(chrono::Utc::now()), // Being deleted
            labels: None,
            annotations: None,
            generate_name: None,
            generation: None,
            managed_fields: None,
        },
        spec: Some(NamespaceSpec { finalizers: None }),
        status: Some(NamespaceStatus {
            phase: Some(rusternetes_common::types::Phase::Terminating),
            conditions: None,
        }),
    };

    let ns_key = build_key("namespaces", None, "test-terminating");
    storage.create(&ns_key, &namespace).await.unwrap();

    // Reconcile should skip terminating namespace
    controller.reconcile_all().await.unwrap();

    // ServiceAccount should NOT be created
    let sa_key = build_key("serviceaccounts", Some("test-terminating"), "default");
    let result = storage.get::<ServiceAccount>(&sa_key).await;
    assert!(result.is_err()); // Should not exist

    // Clean up
    storage.delete(&ns_key).await.unwrap();
}

// ---------------------------------------------------------------------------
// Phase 6.2 extended coverage
// ---------------------------------------------------------------------------

/// Reconciling a ServiceAccount whose owner has disabled automount must not
/// silently flip the flag back to true. Mirrors the upstream e2e expectation
/// that `automountServiceAccountToken: false` is honoured for the lifetime of
/// the SA (see `kubernetes/test/e2e/auth/serviceaccount.go` "should mount an
/// API token into pods").
#[tokio::test]
async fn test_serviceaccount_automount_disable_is_preserved() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    let ns_name = "test-sa-automount-disable";
    let namespace = active_namespace(ns_name);
    let ns_key = build_key("namespaces", None, ns_name);
    storage.create(&ns_key, &namespace).await.unwrap();

    // User-created SA explicitly opts out of token automounting.
    let mut sa = service_account(ns_name, "no-automount");
    sa.automount_service_account_token = Some(false);
    let sa_key = build_key("serviceaccounts", Some(ns_name), "no-automount");
    storage.create(&sa_key, &sa).await.unwrap();

    // `reconcile_all` only ensures the *default* SA per namespace, so to
    // exercise the preservation contract we must drive the per-SA reconcile
    // entry point directly.
    controller
        .reconcile_serviceaccount(ns_name, "no-automount")
        .await
        .unwrap();
    controller.reconcile_all().await.unwrap();

    let after: ServiceAccount = storage.get(&sa_key).await.unwrap();
    assert_eq!(
        after.automount_service_account_token,
        Some(false),
        "controller must not overwrite an explicit automount=false on existing SA"
    );

    storage.delete(&sa_key).await.unwrap();
    storage.delete(&ns_key).await.unwrap();
}

/// SA-level `imagePullSecrets` set by the user must round-trip through reconcile
/// unchanged. Upstream Kubernetes additionally propagates these into pods via
/// the ServiceAccount admission plugin (`plugin/pkg/admission/serviceaccount`),
/// which rusternetes does not implement in the controller-manager. The
/// propagation half is marked `#[ignore]` as a RED-state spec.
#[tokio::test]
async fn test_serviceaccount_image_pull_secrets_persist_through_reconcile() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    let ns_name = "test-sa-pullsecrets";
    storage
        .create(
            &build_key("namespaces", None, ns_name),
            &active_namespace(ns_name),
        )
        .await
        .unwrap();

    let mut sa = service_account(ns_name, "with-pull-secrets");
    sa.image_pull_secrets = Some(vec![
        LocalObjectReference {
            name: "registry-creds".to_string(),
        },
        LocalObjectReference {
            name: "backup-registry-creds".to_string(),
        },
    ]);
    let sa_key = build_key("serviceaccounts", Some(ns_name), "with-pull-secrets");
    storage.create(&sa_key, &sa).await.unwrap();

    // Drive the per-SA reconcile path explicitly — `reconcile_all` only walks
    // namespaces to seed default SAs and never visits user-created SAs.
    controller
        .reconcile_serviceaccount(ns_name, "with-pull-secrets")
        .await
        .unwrap();

    let after: ServiceAccount = storage.get(&sa_key).await.unwrap();
    let pull = after
        .image_pull_secrets
        .as_ref()
        .expect("imagePullSecrets must survive reconcile");
    assert_eq!(pull.len(), 2);
    assert_eq!(pull[0].name, "registry-creds");
    assert_eq!(pull[1].name, "backup-registry-creds");

    storage
        .delete(&build_key("namespaces", None, ns_name))
        .await
        .unwrap();
    storage.delete(&sa_key).await.unwrap();
}

// NOTE: SA `imagePullSecrets` propagation onto pods is an **admission** concern,
// not a controller one — upstream does it in the ServiceAccount admission plugin
// at pod CREATE (plugin/pkg/admission/serviceaccount/admission.go:167), not in a
// reconcile loop. In rusternetes it runs in
// `admission::inject_service_account_token` via
// `serviceaccount::propagate_image_pull_secrets`, and is covered end-to-end by
// `api-server/tests/integration_serviceaccount_token.rs::
// test_service_account_image_pull_secrets_propagate_on_pod_create`. The previous
// `#[ignore]`d controller test here asserted the wrong layer (controller doing
// the propagation) against a stale "absent in admission.rs" claim, so it has
// been removed.

/// Every default SA created by the controller must have a companion legacy
/// token Secret of type `kubernetes.io/service-account-token`, and that Secret
/// must carry the upstream annotations (`kubernetes.io/service-account.name`
/// and `kubernetes.io/service-account.uid`) that legacy auth flows rely on.
#[tokio::test]
async fn test_serviceaccount_token_secret_sync_annotations() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    let ns_name = "test-sa-token-sync";
    storage
        .create(
            &build_key("namespaces", None, ns_name),
            &active_namespace(ns_name),
        )
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let sa_key = build_key("serviceaccounts", Some(ns_name), "default");
    let sa: ServiceAccount = storage.get(&sa_key).await.unwrap();

    let secret_key = build_key("secrets", Some(ns_name), "default-token");
    let secret: Secret = storage.get(&secret_key).await.unwrap();

    assert_eq!(
        secret.secret_type.as_deref(),
        Some("kubernetes.io/service-account-token"),
        "token Secret must be typed kubernetes.io/service-account-token"
    );

    let annotations = secret
        .metadata
        .annotations
        .as_ref()
        .expect("token Secret must carry SA annotations");
    assert_eq!(
        annotations
            .get("kubernetes.io/service-account.name")
            .map(String::as_str),
        Some("default"),
        "missing service-account.name annotation"
    );
    assert_eq!(
        annotations
            .get("kubernetes.io/service-account.uid")
            .cloned(),
        Some(sa.metadata.uid.clone()),
        "service-account.uid annotation must match owning SA UID"
    );

    let data = secret.data.as_ref().expect("token Secret must have data");
    assert!(!data.get("token").map(|v| v.is_empty()).unwrap_or(true));
    assert_eq!(
        data.get("namespace").map(|v| v.as_slice()),
        Some(ns_name.as_bytes())
    );

    storage.delete(&secret_key).await.unwrap();
    storage.delete(&sa_key).await.unwrap();
    storage
        .delete(&build_key("namespaces", None, ns_name))
        .await
        .unwrap();
}

/// `reconcile_all` must fan out default SA + token creation across every
/// active namespace it sees, and must do so idempotently. Mirrors the upstream
/// invariant exercised by the e2e suite when a fresh cluster spins up several
/// namespaces back-to-back.
#[tokio::test]
async fn test_serviceaccount_reconcile_fans_out_to_all_namespaces() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    let names = ["fanout-alpha", "fanout-beta", "fanout-gamma"];
    for name in &names {
        storage
            .create(
                &build_key("namespaces", None, name),
                &active_namespace(name),
            )
            .await
            .unwrap();
    }

    // Two passes — second pass must be a no-op (idempotency).
    controller.reconcile_all().await.unwrap();
    controller.reconcile_all().await.unwrap();

    for name in &names {
        let sa: ServiceAccount = storage
            .get(&build_key("serviceaccounts", Some(name), "default"))
            .await
            .unwrap_or_else(|e| panic!("default SA missing in {name}: {e}"));
        assert_eq!(sa.metadata.name, "default");
        assert_eq!(sa.metadata.namespace.as_deref(), Some(*name));

        let secret: Secret = storage
            .get(&build_key("secrets", Some(name), "default-token"))
            .await
            .unwrap_or_else(|e| panic!("default-token missing in {name}: {e}"));
        assert_eq!(
            secret.secret_type.as_deref(),
            Some("kubernetes.io/service-account-token")
        );
    }

    for name in &names {
        let _ = storage
            .delete(&build_key("secrets", Some(name), "default-token"))
            .await;
        let _ = storage
            .delete(&build_key("serviceaccounts", Some(name), "default"))
            .await;
        let _ = storage.delete(&build_key("namespaces", None, name)).await;
    }
}

/// RED-state: the ServiceAccount controller does not issue bound, audience-
/// scoped tokens with caller-supplied `expirationSeconds`. Upstream this is
/// served by the `TokenRequest` subresource on the api-server
/// (`crates/api-server/src/handlers/service_account.rs::create_token`), not by
/// the controller. The controller still owns the legacy long-lived token in
/// the Secret. This test pins the eventual contract: a per-namespace SA should
/// be able to request a projected token with a specific audience that ends up
/// in a Secret reachable by the SA controller (or a successor projection path).
#[tokio::test]
#[ignore = "RED-state: bound SA token projection (audience + expirationSeconds) lives in api-server TokenRequest, not yet wired to the controller"]
async fn test_serviceaccount_bound_token_projection_audience_and_expiry() {
    let storage = Arc::new(MemoryStorage::new());
    let controller = ServiceAccountController::new(storage.clone());

    let ns_name = "test-sa-bound-token";
    storage
        .create(
            &build_key("namespaces", None, ns_name),
            &active_namespace(ns_name),
        )
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    // The contract we want once bound-token projection is wired through the
    // controller: a projected token Secret carrying the requested audience and
    // a non-default expirationSeconds annotation. The current implementation
    // only writes the legacy `default-token` Secret with no audience metadata,
    // so this test deliberately fails until the feature lands.
    let secret_key = build_key("secrets", Some(ns_name), "default-token-bound");
    let secret: Secret = storage
        .get(&secret_key)
        .await
        .expect("bound token Secret should be projected by controller");

    let annotations = secret
        .metadata
        .annotations
        .as_ref()
        .expect("bound token Secret must carry audience/expiration annotations");
    assert_eq!(
        annotations
            .get("kubernetes.io/service-account.token-audience")
            .map(String::as_str),
        Some("https://kubernetes.default.svc")
    );
    assert_eq!(
        annotations
            .get("kubernetes.io/service-account.token-expiration-seconds")
            .map(String::as_str),
        Some("3600")
    );
}
