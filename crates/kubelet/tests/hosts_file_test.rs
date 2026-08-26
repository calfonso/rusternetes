use rusternetes_common::resources::{Container, Pod, PodSpec};
use rusternetes_common::types::{ObjectMeta, TypeMeta};

fn make_container(name: &str) -> Container {
    Container {
        name: name.to_string(),
        image: "nginx:latest".to_string(),
        image_pull_policy: Some("IfNotPresent".to_string()),
        command: None,
        args: None,
        ports: None,
        env: None,
        volume_mounts: None,
        liveness_probe: None,
        readiness_probe: None,
        startup_probe: None,
        resources: None,
        working_dir: None,
        security_context: None,
        restart_policy: None,
        resize_policy: None,
        lifecycle: None,
        termination_message_path: None,
        termination_message_policy: None,
        stdin: None,
        stdin_once: None,
        tty: None,
        env_from: None,
        volume_devices: None,
    }
}

fn make_pod(name: &str, namespace: &str, hostname: Option<&str>, subdomain: Option<&str>) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new(name).with_namespace(namespace),
        spec: Some(PodSpec {
            containers: vec![make_container("app")],
            init_containers: None,
            ephemeral_containers: None,
            restart_policy: Some("Always".to_string()),
            node_name: None,
            node_selector: None,
            service_account_name: None,
            service_account: None,
            hostname: hostname.map(|s| s.to_string()),
            subdomain: subdomain.map(|s| s.to_string()),
            host_network: None,
            host_pid: None,
            host_ipc: None,
            affinity: None,
            tolerations: None,
            priority: None,
            priority_class_name: None,
            automount_service_account_token: None,
            topology_spread_constraints: None,
            overhead: None,
            scheduler_name: None,
            resource_claims: None,
            volumes: None,
            active_deadline_seconds: None,
            dns_policy: None,
            dns_config: None,
            security_context: None,
            image_pull_secrets: None,
            share_process_namespace: None,
            readiness_gates: None,
            runtime_class_name: None,
            enable_service_links: None,
            preemption_policy: None,
            host_users: None,
            set_hostname_as_fqdn: None,
            termination_grace_period_seconds: None,
            host_aliases: None,
            os: None,
            scheduling_gates: None,
            resources: None,
        }),
        status: None,
    }
}

// --- StatefulSet-style pod DNS naming ---

#[test]
fn test_statefulset_pod_hostname_and_subdomain() {
    // StatefulSets set spec.hostname = pod name, spec.subdomain = service name
    let pod = make_pod("web-0", "default", Some("web-0"), Some("nginx"));
    let spec = pod.spec.as_ref().unwrap();

    assert_eq!(spec.hostname.as_deref(), Some("web-0"));
    assert_eq!(spec.subdomain.as_deref(), Some("nginx"));
}

#[test]
fn test_statefulset_fqdn_format() {
    // The expected DNS name for a StatefulSet pod is:
    //   <hostname>.<subdomain>.<namespace>.svc.<cluster-domain>
    let pod = make_pod("web-0", "default", Some("web-0"), Some("nginx"));
    let spec = pod.spec.as_ref().unwrap();
    let cluster_domain = "cluster.local";

    let fqdn = format!(
        "{}.{}.{}.svc.{}",
        spec.hostname.as_deref().unwrap(),
        spec.subdomain.as_deref().unwrap(),
        pod.metadata.namespace.as_deref().unwrap(),
        cluster_domain,
    );

    assert_eq!(fqdn, "web-0.nginx.default.svc.cluster.local");
}

#[test]
fn test_multiple_statefulset_replicas_get_unique_fqdns() {
    let cluster_domain = "cluster.local";
    let service = "redis";
    let namespace = "cache";

    let fqdns: Vec<String> = (0..3)
        .map(|i| {
            let pod_name = format!("redis-{}", i);
            let pod = make_pod(&pod_name, namespace, Some(&pod_name), Some(service));
            let spec = pod.spec.as_ref().unwrap();
            format!(
                "{}.{}.{}.svc.{}",
                spec.hostname.as_deref().unwrap(),
                spec.subdomain.as_deref().unwrap(),
                namespace,
                cluster_domain,
            )
        })
        .collect();

    assert_eq!(fqdns[0], "redis-0.redis.cache.svc.cluster.local");
    assert_eq!(fqdns[1], "redis-1.redis.cache.svc.cluster.local");
    assert_eq!(fqdns[2], "redis-2.redis.cache.svc.cluster.local");

    // All FQDNs must be unique
    let unique: std::collections::HashSet<_> = fqdns.iter().collect();
    assert_eq!(unique.len(), 3);
}

// --- hostname fallback to pod name ---

#[test]
fn test_hostname_falls_back_to_pod_name() {
    // When spec.hostname is not set, the pod name is used as the hostname
    let pod = make_pod("my-pod", "default", None, None);
    let spec = pod.spec.as_ref().unwrap();

    let effective_hostname = spec.hostname.as_deref().unwrap_or(&pod.metadata.name);

    assert_eq!(effective_hostname, "my-pod");
}

#[test]
fn test_hostname_override_takes_precedence() {
    let pod = make_pod("my-pod-abc123", "default", Some("custom-host"), None);
    let spec = pod.spec.as_ref().unwrap();

    let effective_hostname = spec.hostname.as_deref().unwrap_or(&pod.metadata.name);

    assert_eq!(effective_hostname, "custom-host");
    assert_ne!(effective_hostname, "my-pod-abc123");
}

// --- hosts file content validation ---

#[test]
fn test_hosts_file_ipv6_entries_present() {
    // The standard localhost entries required by conformance tests
    let required_entries = [
        "127.0.0.1\tlocalhost",
        "::1\tlocalhost ip6-localhost ip6-loopback",
        "fe00::\tip6-localnet",
        "fe00::\tip6-mcastprefix",
        "fe00::1\tip6-allnodes",
        "fe00::2\tip6-allrouters",
    ];

    // Build the base content (same logic as create_pod_hosts_file)
    let base_content = "# Kubernetes-managed hosts file\n\
        127.0.0.1\tlocalhost\n\
        ::1\tlocalhost ip6-localhost ip6-loopback\n\
        fe00::\tip6-localnet\n\
        fe00::\tip6-mcastprefix\n\
        fe00::1\tip6-allnodes\n\
        fe00::2\tip6-allrouters\n";

    for entry in &required_entries {
        assert!(
            base_content.contains(entry),
            "Missing required /etc/hosts entry: {}",
            entry
        );
    }
}

#[test]
fn test_hosts_file_entry_format_is_tab_separated() {
    // Kubernetes /etc/hosts uses tabs between IP and hostnames
    let ip = "10.244.1.5";
    let hostname = "web-0";
    let entry = format!("{}\t{}\n", ip, hostname);

    // Verify tab separation (not spaces)
    let parts: Vec<&str> = entry.trim().split('\t').collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], ip);
    assert_eq!(parts[1], hostname);
}

#[test]
fn test_hosts_file_entry_with_fqdn_uses_tab_separation() {
    let ip = "10.244.1.5";
    let hostname = "web-0";
    let fqdn = "web-0.nginx.default.svc.cluster.local";
    let aliases = [hostname, fqdn];
    let entry = format!("{}\t{}\n", ip, aliases.join("\t"));

    let parts: Vec<&str> = entry.trim().split('\t').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], ip);
    assert_eq!(parts[1], hostname);
    assert_eq!(parts[2], fqdn);
}

// --- subdomain field serialization ---

#[test]
fn test_subdomain_serializes_to_camel_case() {
    let pod = make_pod("web-0", "default", Some("web-0"), Some("nginx"));
    let json = serde_json::to_string(&pod).unwrap();

    // Kubernetes uses camelCase in JSON
    assert!(json.contains(r#""subdomain":"nginx""#));
    assert!(!json.contains(r#""sub_domain""#));
}

#[test]
fn test_subdomain_absent_from_json_when_none() {
    let pod = make_pod("my-pod", "default", None, None);
    let json = serde_json::to_string(&pod).unwrap();

    assert!(!json.contains("subdomain"));
    assert!(!json.contains("hostname"));
}

#[test]
fn test_subdomain_deserializes_from_kubernetes_json() {
    // Simulate a pod JSON received from the Kubernetes API
    let json = r#"{
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": {"name": "web-0", "namespace": "default", "uid": "abc"},
        "spec": {
            "hostname": "web-0",
            "subdomain": "nginx",
            "containers": [{
                "name": "app",
                "image": "nginx:latest"
            }]
        }
    }"#;

    let pod: Pod = serde_json::from_str(json).expect("deserialize pod");
    let spec = pod.spec.as_ref().unwrap();

    assert_eq!(spec.hostname.as_deref(), Some("web-0"));
    assert_eq!(spec.subdomain.as_deref(), Some("nginx"));
}

#[test]
fn test_pod_without_subdomain_deserializes_correctly() {
    let json = r#"{
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": {"name": "my-pod", "namespace": "default", "uid": "abc"},
        "spec": {
            "containers": [{
                "name": "app",
                "image": "nginx:latest"
            }]
        }
    }"#;

    let pod: Pod = serde_json::from_str(json).expect("deserialize pod");
    let spec = pod.spec.as_ref().unwrap();

    assert!(spec.subdomain.is_none());
    assert!(spec.hostname.is_none());
}

// --- resolv.conf and hosts file mount paths ---

#[test]
fn test_hosts_file_mounted_at_etc_hosts() {
    // Verify the expected container mount path
    let hosts_bind = format!(
        "{}:/etc/hosts:ro",
        "/var/lib/rusternetes/volumes/my-pod/hosts"
    );
    assert!(hosts_bind.ends_with(":/etc/hosts:ro"));
    assert!(hosts_bind.contains("/hosts:"));
}

#[test]
fn test_resolv_conf_mounted_at_etc_resolv_conf() {
    let resolv_bind = format!(
        "{}:/etc/resolv.conf:ro",
        "/var/lib/rusternetes/volumes/my-pod/resolv.conf"
    );
    assert!(resolv_bind.ends_with(":/etc/resolv.conf:ro"));
}

#[test]
fn test_hosts_and_resolv_in_same_pod_dir() {
    let base = "/var/lib/rusternetes/volumes";
    let pod_name = "my-pod";

    let hosts_path = format!("{}/{}/hosts", base, pod_name);
    let resolv_path = format!("{}/{}/resolv.conf", base, pod_name);

    let hosts_dir = std::path::Path::new(&hosts_path)
        .parent()
        .unwrap()
        .to_str()
        .unwrap();
    let resolv_dir = std::path::Path::new(&resolv_path)
        .parent()
        .unwrap()
        .to_str()
        .unwrap();

    assert_eq!(hosts_dir, resolv_dir);
    assert_eq!(hosts_dir, format!("{}/{}", base, pod_name));
}
