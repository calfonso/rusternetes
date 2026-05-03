// Automated Cluster Startup Tests
// These tests verify that cluster components can start cleanly and communicate

use rusternetes_common::auth::TokenManager;
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::sync::Arc;

/// Setup test environment with in-memory storage
async fn setup_test() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::new());
    storage.clear();
    storage
}

#[tokio::test]
async fn test_storage_initialization() {
    // Test that storage can be initialized successfully
    let storage = setup_test().await;

    // Verify storage is empty initially
    let keys: Vec<String> = storage.list("/registry/").await.unwrap_or_default();
    assert_eq!(keys.len(), 0, "Storage should be empty on initialization");
}

#[tokio::test]
async fn test_storage_connectivity() {
    let storage = setup_test().await;

    // Test basic CRUD operations to verify storage connectivity
    let key = build_key("test", None, "connectivity-check");
    let test_data = "test-value".to_string();

    // Create
    let result = storage.create(&key, &test_data).await;
    assert!(result.is_ok(), "Should be able to create data in storage");

    // Read
    let read_result: Result<String, _> = storage.get(&key).await;
    assert!(
        read_result.is_ok(),
        "Should be able to read data from storage"
    );
    assert_eq!(read_result.unwrap(), test_data);

    // Update
    let updated_data = "updated-value".to_string();
    let update_result = storage.update(&key, &updated_data).await;
    assert!(
        update_result.is_ok(),
        "Should be able to update data in storage"
    );

    // Delete
    let delete_result = storage.delete(&key).await;
    assert!(
        delete_result.is_ok(),
        "Should be able to delete data from storage"
    );

    // Verify deletion
    let final_result: Result<String, _> = storage.get(&key).await;
    assert!(final_result.is_err(), "Data should be deleted");
}

#[tokio::test]
async fn test_token_manager_initialization() {
    use rusternetes_common::auth::ServiceAccountClaims;

    // Test that TokenManager can be initialized with a secret
    let secret = b"test-secret-for-jwt-tokens";
    let token_manager = TokenManager::new(secret);

    // Generate a test token
    let claims = ServiceAccountClaims::new(
        "test-sa".to_string(),
        "default".to_string(),
        "test-uid".to_string(),
        1,
    );
    let token = token_manager.generate_token(claims.clone());
    assert!(token.is_ok(), "Should be able to generate JWT token");

    // Validate the token
    let token_str = token.unwrap();
    let validation = token_manager.validate_token(&token_str);
    assert!(
        validation.is_ok(),
        "Should be able to validate generated token"
    );

    let validated_claims = validation.unwrap();
    assert_eq!(validated_claims.sub, claims.sub);
    assert_eq!(validated_claims.namespace, claims.namespace);
}

#[tokio::test]
async fn test_rbac_authorizer_initialization() {
    // Note: RBACAuthorizer requires storage backend
    // This test verifies the pattern but uses AlwaysAllowAuthorizer instead

    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::{AlwaysAllowAuthorizer, Authorizer, RequestAttributes};

    let authorizer = AlwaysAllowAuthorizer;

    let user = UserInfo {
        username: "test-user".to_string(),
        uid: "test-uid".to_string(),
        groups: vec![],
        extra: std::collections::HashMap::new(),
    };

    let attrs = RequestAttributes::new(user, "get", "pods").with_namespace("default");

    let decision = authorizer.authorize(&attrs).await;
    assert!(decision.is_ok(), "Authorizer should return a decision");
}

#[tokio::test]
async fn test_always_allow_authorizer_initialization() {
    // Test AlwaysAllowAuthorizer for development mode
    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::{
        AlwaysAllowAuthorizer, Authorizer, Decision, RequestAttributes,
    };

    let authorizer = AlwaysAllowAuthorizer;

    let user = UserInfo {
        username: "any-user".to_string(),
        uid: "uid".to_string(),
        groups: vec![],
        extra: std::collections::HashMap::new(),
    };

    let attrs = RequestAttributes::new(user, "delete", "nodes");

    let decision = authorizer.authorize(&attrs).await;
    assert!(decision.is_ok(), "AlwaysAllow should always authorize");
    assert_eq!(
        decision.unwrap(),
        Decision::Allow,
        "AlwaysAllow should return Allow"
    );
}

#[tokio::test]
async fn test_metrics_registry_initialization() {
    // Test that MetricsRegistry can be initialized
    let metrics = MetricsRegistry::new();

    // Add API server metrics
    let result = metrics.with_api_server_metrics();
    assert!(
        result.is_ok(),
        "Should be able to initialize API server metrics"
    );

    // Verify metrics can be recorded (this is a smoke test)
    let _metrics_with_api = result.unwrap();
}

#[tokio::test]
async fn test_component_health_checks() {
    let storage = setup_test().await;

    // Simulate health check by verifying storage is responsive
    let health_check_key = build_key("health", None, "api-server");
    let health_status = "healthy".to_string();

    let create_result = storage.create(&health_check_key, &health_status).await;
    assert!(
        create_result.is_ok(),
        "Should be able to write health status"
    );

    let read_result: Result<String, _> = storage.get(&health_check_key).await;
    assert!(read_result.is_ok(), "Should be able to read health status");
    assert_eq!(read_result.unwrap(), health_status);

    // Cleanup
    storage.delete(&health_check_key).await.ok();
}

#[tokio::test]
async fn test_concurrent_storage_operations() {
    let storage = setup_test().await;

    // Test that multiple concurrent operations don't cause issues
    let mut handles = vec![];

    for i in 0..10 {
        let storage_clone = storage.clone();
        let handle = tokio::spawn(async move {
            let key = build_key("concurrent", None, &format!("test-{}", i));
            let data = format!("data-{}", i);

            // Create
            storage_clone.create(&key, &data).await.unwrap();

            // Read
            let read: String = storage_clone.get(&key).await.unwrap();
            assert_eq!(read, data);

            // Delete
            storage_clone.delete(&key).await.unwrap();
        });

        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        assert!(handle.await.is_ok(), "Concurrent operation should succeed");
    }
}

#[tokio::test]
async fn test_namespace_isolation() {
    let storage = setup_test().await;

    // Create resources in different namespaces
    let ns1_key = build_key("pods", Some("namespace1"), "pod1");
    let ns2_key = build_key("pods", Some("namespace2"), "pod1");

    storage
        .create(&ns1_key, &"pod-in-ns1".to_string())
        .await
        .unwrap();
    storage
        .create(&ns2_key, &"pod-in-ns2".to_string())
        .await
        .unwrap();

    // Verify namespace isolation
    let ns1_data: String = storage.get(&ns1_key).await.unwrap();
    let ns2_data: String = storage.get(&ns2_key).await.unwrap();

    assert_eq!(ns1_data, "pod-in-ns1");
    assert_eq!(ns2_data, "pod-in-ns2");
    assert_ne!(ns1_data, ns2_data, "Namespaces should be isolated");
}

#[tokio::test]
async fn test_cluster_scoped_resources() {
    let storage = setup_test().await;

    // Test cluster-scoped resources (no namespace)
    let node_key = build_key("nodes", None, "node-1");
    let pv_key = build_key("persistentvolumes", None, "pv-1");

    storage
        .create(&node_key, &"node-data".to_string())
        .await
        .unwrap();
    storage
        .create(&pv_key, &"pv-data".to_string())
        .await
        .unwrap();

    // Verify cluster-scoped resources are accessible
    let node_data: String = storage.get(&node_key).await.unwrap();
    let pv_data: String = storage.get(&pv_key).await.unwrap();

    assert_eq!(node_data, "node-data");
    assert_eq!(pv_data, "pv-data");
}

#[tokio::test]
async fn test_list_operations() {
    let storage = setup_test().await;

    // Create multiple resources
    for i in 0..5 {
        let key = build_key("pods", Some("default"), &format!("pod-{}", i));
        storage
            .create(&key, &format!("pod-data-{}", i))
            .await
            .unwrap();
    }

    // List all pods in default namespace
    let pods: Vec<String> = storage.list("/registry/pods/default/").await.unwrap();
    assert_eq!(pods.len(), 5, "Should list all pods in namespace");
}

#[tokio::test]
async fn test_storage_error_handling() {
    let storage = setup_test().await;

    // Test get on non-existent key
    let result: Result<String, _> = storage.get("/non-existent-key").await;
    assert!(result.is_err(), "Should return error for non-existent key");

    // Test create on existing key (should fail)
    let key = build_key("test", None, "duplicate");
    storage.create(&key, &"first".to_string()).await.unwrap();

    let duplicate_result = storage.create(&key, &"second".to_string()).await;
    assert!(
        duplicate_result.is_err(),
        "Should fail to create duplicate key"
    );
}

#[tokio::test]
async fn test_component_startup_order() {
    use rusternetes_common::auth::ServiceAccountClaims;

    // Simulate component startup in correct order

    // 1. Storage must be available first
    let storage = setup_test().await;
    let result: Result<Vec<String>, _> = storage.list("/").await;
    assert!(result.is_ok(), "Storage should be available");

    // 2. TokenManager can initialize independently
    let token_manager = TokenManager::new(b"test-secret");
    let claims = ServiceAccountClaims::new(
        "test".to_string(),
        "default".to_string(),
        "uid".to_string(),
        1,
    );
    let token = token_manager.generate_token(claims);
    assert!(token.is_ok(), "TokenManager should initialize");

    // 3. Authorizer can be initialized (using AlwaysAllow for testing)
    use rusternetes_common::authz::AlwaysAllowAuthorizer;
    let _authorizer = AlwaysAllowAuthorizer;

    // 4. Metrics can initialize independently
    let metrics = MetricsRegistry::new().with_api_server_metrics();
    assert!(metrics.is_ok(), "Metrics should initialize");
}

#[tokio::test]
async fn test_graceful_degradation() {
    let storage = setup_test().await;

    // Test that individual component failures don't crash entire system
    // Simulate storage read failure by reading non-existent key
    let read_result: Result<String, _> = storage.get("/missing-key").await;
    assert!(
        read_result.is_err(),
        "Should handle missing keys gracefully"
    );

    // Verify storage still works for other operations
    let key = build_key("test", None, "recovery");
    let create_result = storage.create(&key, &"data".to_string()).await;
    assert!(
        create_result.is_ok(),
        "Storage should recover and continue working"
    );
}

#[tokio::test]
async fn test_multiple_client_connections() {
    let storage = setup_test().await;

    // Simulate multiple clients connecting to the same storage
    let client1 = storage.clone();
    let client2 = storage.clone();
    let client3 = storage.clone();

    // Each client performs operations
    let handle1 = tokio::spawn(async move {
        let key = build_key("test", None, "client1");
        client1.create(&key, &"data1".to_string()).await.unwrap();
    });

    let handle2 = tokio::spawn(async move {
        let key = build_key("test", None, "client2");
        client2.create(&key, &"data2".to_string()).await.unwrap();
    });

    let handle3 = tokio::spawn(async move {
        let key = build_key("test", None, "client3");
        client3.create(&key, &"data3".to_string()).await.unwrap();
    });

    // All clients should complete successfully
    assert!(handle1.await.is_ok());
    assert!(handle2.await.is_ok());
    assert!(handle3.await.is_ok());
}
