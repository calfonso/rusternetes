use rusternetes_client::config::ClientConfig;

#[test]
fn in_cluster_reads_token_and_host_from_paths_and_env() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("token"), "sekret").unwrap();
    std::fs::write(dir.path().join("ca.crt"), "PEM").unwrap();
    let cfg =
        ClientConfig::in_cluster_from(dir.path(), Some("10.0.0.1".into()), Some("6443".into()))
            .unwrap();
    assert_eq!(cfg.base_url, "https://10.0.0.1:6443");
    assert_eq!(cfg.token.as_deref(), Some("sekret"));
    assert_eq!(cfg.ca_pem.as_deref(), Some("PEM"));
}

#[test]
fn in_cluster_fails_without_host() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("token"), "t").unwrap();
    assert!(ClientConfig::in_cluster_from(dir.path(), None, None).is_err());
}
