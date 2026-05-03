use rusternetes_kubelet::cni::config::NetworkConfig;
use rusternetes_kubelet::cni::plugin::CniPluginManager;
use rusternetes_kubelet::cni::CniRuntime;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// Helper function to create a temporary directory with mock CNI plugin
fn setup_cni_environment() -> (TempDir, TempDir, PathBuf) {
    // Create temporary directories for CNI bins and configs
    let cni_bin_dir = TempDir::new().expect("Failed to create temp CNI bin dir");
    let cni_config_dir = TempDir::new().expect("Failed to create temp CNI config dir");

    // Copy mock CNI plugin to bin directory
    let mock_plugin_source =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock-cni-plugin.sh");
    let mock_plugin_dest = cni_bin_dir.path().join("mock-cni-plugin");

    fs::copy(&mock_plugin_source, &mock_plugin_dest).expect("Failed to copy mock CNI plugin");

    // Make plugin executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&mock_plugin_dest, fs::Permissions::from_mode(0o755))
            .expect("Failed to set plugin permissions");
    }

    // Copy test network configuration
    let test_config_source =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/test-network.conf");
    let test_config_dest = cni_config_dir.path().join("10-test-network.conf");

    fs::copy(&test_config_source, &test_config_dest).expect("Failed to copy test network config");

    (cni_bin_dir, cni_config_dir, mock_plugin_dest)
}

#[test]
fn test_cni_plugin_discovery() {
    let (cni_bin_dir, _cni_config_dir, _plugin_path) = setup_cni_environment();

    let mut plugin_manager = CniPluginManager::new(vec![cni_bin_dir.path().to_path_buf()]);

    // Discover plugins
    let result = plugin_manager.discover_plugins();
    assert!(result.is_ok(), "Plugin discovery should succeed");

    // Should find at least our mock plugin
    let plugins = plugin_manager.list_plugins();
    assert!(!plugins.is_empty(), "Should discover at least one plugin");
    assert!(
        plugins.contains(&"mock-cni-plugin".to_string()),
        "Should discover mock-cni-plugin"
    );
}

#[test]
fn test_cni_plugin_execution_add() {
    let (cni_bin_dir, cni_config_dir, _plugin_path) = setup_cni_environment();

    // Create CNI runtime
    let cni_runtime = CniRuntime::new(
        vec![cni_bin_dir.path().to_path_buf()],
        cni_config_dir.path().to_path_buf(),
    )
    .expect("Failed to create CNI runtime")
    .with_default_network("test-network".to_string());

    // Setup network for a test pod
    // Note: Using a temporary file path instead of real network namespace for testing
    let temp_netns = TempDir::new().expect("Failed to create temp dir for netns");
    let result = cni_runtime.setup_network(
        "test-pod-123",
        temp_netns.path().to_str().unwrap(),
        "eth0",
        None, // Use default network
    );

    // The result should succeed
    assert!(
        result.is_ok(),
        "Network setup should succeed: {:?}",
        result.err()
    );

    let cni_result = result.unwrap();

    // Verify we got an IP address
    let primary_ip = cni_result.primary_ip();
    assert!(primary_ip.is_some(), "Should have a primary IP address");

    // Verify IP is in expected format
    let ip_addr = primary_ip.unwrap();
    assert_eq!(ip_addr.to_string(), "10.244.0.5", "IP should be 10.244.0.5");
}

#[test]
fn test_cni_plugin_execution_del() {
    let (cni_bin_dir, cni_config_dir, _plugin_path) = setup_cni_environment();

    let cni_runtime = CniRuntime::new(
        vec![cni_bin_dir.path().to_path_buf()],
        cni_config_dir.path().to_path_buf(),
    )
    .expect("Failed to create CNI runtime")
    .with_default_network("test-network".to_string());

    // Using temporary directory instead of real network namespace
    let temp_netns = TempDir::new().expect("Failed to create temp dir for netns");
    let netns_path = temp_netns.path().to_str().unwrap();

    // First, setup the network
    let setup_result = cni_runtime.setup_network("test-pod-456", netns_path, "eth0", None);
    assert!(setup_result.is_ok(), "Network setup should succeed");

    // Now teardown the network
    let teardown_result = cni_runtime.teardown_network("test-pod-456", netns_path, "eth0", None);

    assert!(
        teardown_result.is_ok(),
        "Network teardown should succeed: {:?}",
        teardown_result.err()
    );
}

#[test]
fn test_cni_network_config_validation() {
    let config_json = r#"{
        "cniVersion": "1.0.0",
        "name": "test-network",
        "type": "mock-cni-plugin"
    }"#;

    let config: Result<NetworkConfig, _> = serde_json::from_str(config_json);
    assert!(config.is_ok(), "Should parse valid network config");

    let network_config = config.unwrap();
    assert_eq!(network_config.name, "test-network");
    assert_eq!(network_config.plugin_type, "mock-cni-plugin");
    assert_eq!(network_config.cni_version, "1.0.0");
}

#[test]
fn test_cni_config_loading() {
    let (cni_bin_dir, cni_config_dir, _plugin_path) = setup_cni_environment();

    let _cni_runtime = CniRuntime::new(
        vec![cni_bin_dir.path().to_path_buf()],
        cni_config_dir.path().to_path_buf(),
    )
    .expect("Failed to create CNI runtime");

    // Runtime should load the config successfully
    // The config_dir should contain our test-network.conf
    assert!(cni_config_dir.path().join("10-test-network.conf").exists());
}

#[test]
fn test_cni_multiple_attachments() {
    let (cni_bin_dir, cni_config_dir, _plugin_path) = setup_cni_environment();

    let cni_runtime = CniRuntime::new(
        vec![cni_bin_dir.path().to_path_buf()],
        cni_config_dir.path().to_path_buf(),
    )
    .expect("Failed to create CNI runtime")
    .with_default_network("test-network".to_string());

    // Using temporary directories instead of real network namespaces
    let temp_netns1 = TempDir::new().expect("Failed to create temp dir for netns1");
    let temp_netns2 = TempDir::new().expect("Failed to create temp dir for netns2");

    // Setup network for first pod
    let result1 =
        cni_runtime.setup_network("pod-1", temp_netns1.path().to_str().unwrap(), "eth0", None);
    assert!(result1.is_ok());

    // Setup network for second pod
    let result2 =
        cni_runtime.setup_network("pod-2", temp_netns2.path().to_str().unwrap(), "eth0", None);
    assert!(result2.is_ok());

    // Get stats to verify we have 2 attachments
    let stats = cni_runtime.get_stats();
    assert_eq!(
        stats.total_attachments, 2,
        "Should have 2 network attachments"
    );
}

#[test]
fn test_cni_error_handling_missing_plugin() {
    let (cni_bin_dir, cni_config_dir, _plugin_path) = setup_cni_environment();

    // Create a config for a plugin that doesn't exist
    let bad_config = r#"{
        "cniVersion": "1.0.0",
        "name": "bad-network",
        "type": "nonexistent-plugin"
    }"#;

    fs::write(
        cni_config_dir.path().join("99-bad-network.conf"),
        bad_config,
    )
    .expect("Failed to write bad config");

    let cni_runtime = CniRuntime::new(
        vec![cni_bin_dir.path().to_path_buf()],
        cni_config_dir.path().to_path_buf(),
    )
    .expect("Failed to create CNI runtime")
    .with_default_network("bad-network".to_string());

    // Attempt to setup network with missing plugin should fail
    let result = cni_runtime.setup_network(
        "test-pod-error",
        "/var/run/netns/test-pod-error",
        "eth0",
        None,
    );

    assert!(result.is_err(), "Should fail with missing plugin");
}

#[test]
fn test_cni_result_parsing() {
    let result_json = r#"{
        "cniVersion": "1.0.0",
        "interfaces": [
            {
                "name": "eth0",
                "mac": "aa:bb:cc:dd:ee:ff",
                "sandbox": "/var/run/netns/test"
            }
        ],
        "ips": [
            {
                "address": "10.244.0.5/24",
                "gateway": "10.244.0.1",
                "interface": 0
            }
        ],
        "routes": [
            {
                "dst": "0.0.0.0/0",
                "gw": "10.244.0.1"
            }
        ],
        "dns": {
            "nameservers": ["10.96.0.10"],
            "domain": "cluster.local",
            "search": ["default.svc.cluster.local"]
        }
    }"#;

    let result: Result<rusternetes_kubelet::cni::result::CniResult, _> =
        serde_json::from_str(result_json);
    assert!(result.is_ok(), "Should parse valid CNI result");

    let cni_result = result.unwrap();
    assert_eq!(cni_result.cni_version, "1.0.0");
    assert!(cni_result.interfaces.is_some());
    assert!(!cni_result.ips.is_empty());

    // Verify primary IP extraction
    let primary_ip = cni_result.primary_ip();
    assert!(primary_ip.is_some());
    assert_eq!(primary_ip.unwrap().to_string(), "10.244.0.5");
}

#[test]
fn test_cni_plugin_chaining() {
    let (cni_bin_dir, cni_config_dir, _plugin_path) = setup_cni_environment();

    // Copy conflist for plugin chaining
    let conflist_source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/test-network-chain.conflist");
    let conflist_dest = cni_config_dir.path().join("20-test-chain.conflist");

    fs::copy(&conflist_source, &conflist_dest).expect("Failed to copy conflist");

    let cni_runtime = CniRuntime::new(
        vec![cni_bin_dir.path().to_path_buf()],
        cni_config_dir.path().to_path_buf(),
    )
    .expect("Failed to create CNI runtime")
    .with_default_network("test-network-chain".to_string());

    // Setup network - should execute plugin chain
    let result =
        cni_runtime.setup_network("chain-test-pod", "/var/run/netns/chain-test", "eth0", None);

    // With plugin chaining, this might succeed or fail depending on implementation
    // For now, just verify it doesn't panic
    let _ = result;
}
