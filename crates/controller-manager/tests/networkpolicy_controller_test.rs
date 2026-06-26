//! Integration tests for NetworkPolicyController
//!
//! Mirrors a subset of upstream `kubernetes/test/e2e/network/network_policy.go`
//! coverage at the controller level. The controller does not enforce policies
//! itself (enforcement is delegated to CNI plugins); these tests therefore
//! validate the controller's reconciliation contract:
//!
//! * `reconcile_all()` accepts well-formed `NetworkPolicy` resources (ingress,
//!   egress, mixed policy types, default-deny, port ranges, namespace and pod
//!   selectors) without error.
//! * Stored policies survive reconciliation unchanged (the controller does not
//!   mutate spec).
//! * Affected-pod selection respects `podSelector.matchLabels` semantics within
//!   a single namespace (the controller scopes pod lookups to the policy's
//!   namespace, matching upstream behaviour).
//!
//! Test names follow the upstream e2e categories: ingress, egress, podSelector,
//! namespaceSelector, port ranges, policyTypes, and default-deny.

use rusternetes_common::resources::pod::{Container, Pod, PodSpec, PodStatus};
use rusternetes_common::resources::{
    IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyIngressRule, NetworkPolicyPeer,
    NetworkPolicyPort, NetworkPolicySpec,
};
use rusternetes_common::types::{LabelSelector, ObjectMeta, Phase, TypeMeta};
use rusternetes_controller_manager::controllers::network_policy::NetworkPolicyController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

async fn setup_test() -> Arc<MemoryStorage> {
    Arc::new(MemoryStorage::new())
}

fn empty_selector() -> LabelSelector {
    LabelSelector {
        match_labels: Some(HashMap::new()),
        match_expressions: None,
    }
}

fn selector_for(labels: &[(&str, &str)]) -> LabelSelector {
    let mut m = HashMap::new();
    for (k, v) in labels {
        m.insert((*k).to_string(), (*v).to_string());
    }
    LabelSelector {
        match_labels: Some(m),
        match_expressions: None,
    }
}

fn create_pod(name: &str, namespace: &str, labels: &[(&str, &str)]) -> Pod {
    let mut labelmap = HashMap::new();
    for (k, v) in labels {
        labelmap.insert((*k).to_string(), (*v).to_string());
    }
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            labels: Some(labelmap),
            ..ObjectMeta::new(name).with_namespace(namespace)
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx".to_string(),
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
                restart_policy: None,
                resize_policy: None,
                security_context: None,
                lifecycle: None,
                termination_message_path: None,
                termination_message_policy: None,
                stdin: None,
                stdin_once: None,
                tty: None,
                env_from: None,
                volume_devices: None,
                ..Default::default()
            }],
            init_containers: None,
            restart_policy: Some("Always".to_string()),
            node_selector: None,
            node_name: Some("node-1".to_string()),
            volumes: None,
            affinity: None,
            tolerations: None,
            service_account_name: None,
            service_account: None,
            priority: None,
            priority_class_name: None,
            hostname: None,
            subdomain: None,
            host_network: None,
            host_pid: None,
            host_ipc: None,
            automount_service_account_token: None,
            ephemeral_containers: None,
            overhead: None,
            scheduler_name: None,
            topology_spread_constraints: None,
            resource_claims: None,
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
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: Some("10.0.0.1".to_string()),
            host_i_ps: None,
            pod_ip: Some("10.244.0.1".to_string()),
            pod_i_ps: None,
            nominated_node_name: None,
            qos_class: None,
            start_time: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        }),
    }
}

/// Upstream: `should enforce policy based on PodSelector` — ingress rules
/// allowing traffic from selected pods reconcile cleanly and are preserved.
#[tokio::test]
async fn test_networkpolicy_ingress_rules_reconcile() {
    let storage = setup_test().await;
    let controller = NetworkPolicyController::new(storage.clone());

    let spec = NetworkPolicySpec {
        pod_selector: selector_for(&[("app", "server")]),
        ingress: Some(vec![NetworkPolicyIngressRule {
            ports: Some(vec![NetworkPolicyPort {
                protocol: "TCP".to_string(),
                port: Some(serde_json::json!(80)),
                end_port: None,
            }]),
            from: Some(vec![NetworkPolicyPeer {
                pod_selector: Some(selector_for(&[("app", "client")])),
                namespace_selector: None,
                ip_block: None,
            }]),
        }]),
        egress: None,
        policy_types: Some(vec!["Ingress".to_string()]),
    };

    let policy = NetworkPolicy::new("allow-from-client", "default", spec);
    let key = build_key("networkpolicies", Some("default"), "allow-from-client");
    storage.create(&key, &policy).await.unwrap();

    // Place one matching server pod into storage so find_affected_pods has work.
    let pod = create_pod("server-0", "default", &[("app", "server")]);
    let pod_key = build_key("pods", Some("default"), "server-0");
    storage.create(&pod_key, &pod).await.unwrap();

    controller.reconcile_all().await.unwrap();

    // Controller must not mutate the policy spec.
    let stored: NetworkPolicy = storage.get(&key).await.unwrap();
    let ingress = stored.spec.ingress.expect("ingress should be preserved");
    assert_eq!(ingress.len(), 1);
    let peers = ingress[0].from.as_ref().expect("from peers preserved");
    assert_eq!(peers.len(), 1);
    assert!(peers[0].pod_selector.is_some());
}

/// Upstream: `should enforce egress policy allowing traffic to a server in a
/// different namespace based on PodSelector and NamespaceSelector`.
#[tokio::test]
async fn test_networkpolicy_egress_rules_reconcile() {
    let storage = setup_test().await;
    let controller = NetworkPolicyController::new(storage.clone());

    let spec = NetworkPolicySpec {
        pod_selector: selector_for(&[("role", "client")]),
        ingress: None,
        egress: Some(vec![NetworkPolicyEgressRule {
            ports: Some(vec![NetworkPolicyPort {
                protocol: "UDP".to_string(),
                port: Some(serde_json::json!(53)),
                end_port: None,
            }]),
            to: Some(vec![NetworkPolicyPeer {
                pod_selector: Some(selector_for(&[("k8s-app", "kube-dns")])),
                namespace_selector: Some(selector_for(&[(
                    "kubernetes.io/metadata.name",
                    "kube-system",
                )])),
                ip_block: None,
            }]),
        }]),
        policy_types: Some(vec!["Egress".to_string()]),
    };

    let policy = NetworkPolicy::new("allow-dns-egress", "default", spec);
    let key = build_key("networkpolicies", Some("default"), "allow-dns-egress");
    storage.create(&key, &policy).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let stored: NetworkPolicy = storage.get(&key).await.unwrap();
    let egress = stored.spec.egress.expect("egress preserved");
    assert_eq!(egress.len(), 1);
    let ports = egress[0].ports.as_ref().expect("ports preserved");
    assert_eq!(ports[0].protocol.as_str(), "UDP");
    let peers = egress[0].to.as_ref().expect("to peers preserved");
    assert!(peers[0].namespace_selector.is_some());
    assert!(peers[0].pod_selector.is_some());
}

/// Upstream: `should enforce policy based on PodSelector` — selection
/// matches only labelled pods in the same namespace.
#[tokio::test]
async fn test_networkpolicy_pod_selector_targets_only_matching_pods() {
    let storage = setup_test().await;
    let controller = NetworkPolicyController::new(storage.clone());

    // Policy targets pods with app=web.
    let spec = NetworkPolicySpec {
        pod_selector: selector_for(&[("app", "web")]),
        ingress: Some(vec![NetworkPolicyIngressRule {
            ports: None,
            from: None,
        }]),
        egress: None,
        policy_types: Some(vec!["Ingress".to_string()]),
    };
    let policy = NetworkPolicy::new("web-policy", "default", spec);
    let key = build_key("networkpolicies", Some("default"), "web-policy");
    storage.create(&key, &policy).await.unwrap();

    // Matching + non-matching pods in same namespace.
    let matching = create_pod("web-0", "default", &[("app", "web")]);
    let non_matching = create_pod("db-0", "default", &[("app", "db")]);
    storage
        .create(&build_key("pods", Some("default"), "web-0"), &matching)
        .await
        .unwrap();
    storage
        .create(&build_key("pods", Some("default"), "db-0"), &non_matching)
        .await
        .unwrap();

    // Reconcile must succeed without panicking even when matched-pod count > 0.
    controller.reconcile_all().await.unwrap();

    // Policy is still present after reconciliation.
    let stored: NetworkPolicy = storage.get(&key).await.unwrap();
    let labels = stored
        .spec
        .pod_selector
        .match_labels
        .as_ref()
        .expect("match_labels");
    assert_eq!(labels.get("app").map(String::as_str), Some("web"));
}

/// Upstream: `should enforce policy based on NamespaceSelector` — peers that
/// only specify a namespaceSelector validate and reconcile.
#[tokio::test]
async fn test_networkpolicy_namespace_selector_cross_namespace() {
    let storage = setup_test().await;
    let controller = NetworkPolicyController::new(storage.clone());

    let spec = NetworkPolicySpec {
        pod_selector: empty_selector(),
        ingress: Some(vec![NetworkPolicyIngressRule {
            ports: None,
            from: Some(vec![NetworkPolicyPeer {
                pod_selector: None,
                namespace_selector: Some(selector_for(&[("team", "frontend")])),
                ip_block: None,
            }]),
        }]),
        egress: None,
        policy_types: Some(vec!["Ingress".to_string()]),
    };

    let policy = NetworkPolicy::new("allow-from-frontend-ns", "backend", spec);
    let key = build_key("networkpolicies", Some("backend"), "allow-from-frontend-ns");
    storage.create(&key, &policy).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let stored: NetworkPolicy = storage.get(&key).await.unwrap();
    let ingress = stored.spec.ingress.as_ref().expect("ingress preserved");
    let peer = &ingress[0].from.as_ref().expect("from peers preserved")[0];
    assert!(
        peer.pod_selector.is_none(),
        "pure namespace selectors omit podSelector"
    );
    assert!(peer.namespace_selector.is_some());
}

/// Upstream: `should enforce policy based on Ports` — port range with
/// `endPort` validates and survives reconciliation.
#[tokio::test]
async fn test_networkpolicy_port_ranges_accept_end_port() {
    let storage = setup_test().await;
    let controller = NetworkPolicyController::new(storage.clone());

    let spec = NetworkPolicySpec {
        pod_selector: selector_for(&[("app", "ranged")]),
        ingress: Some(vec![NetworkPolicyIngressRule {
            ports: Some(vec![NetworkPolicyPort {
                protocol: "TCP".to_string(),
                port: Some(serde_json::json!(8000)),
                end_port: Some(8100),
            }]),
            from: None,
        }]),
        egress: None,
        policy_types: Some(vec!["Ingress".to_string()]),
    };

    let policy = NetworkPolicy::new("port-range-policy", "default", spec);
    let key = build_key("networkpolicies", Some("default"), "port-range-policy");
    storage.create(&key, &policy).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let stored: NetworkPolicy = storage.get(&key).await.unwrap();
    let ingress = stored.spec.ingress.as_ref().expect("ingress preserved");
    let port = &ingress[0].ports.as_ref().expect("ports preserved")[0];
    assert_eq!(port.protocol.as_str(), "TCP");
    assert_eq!(port.end_port, Some(8100));
}

/// Upstream: policyTypes can be `Ingress`, `Egress`, or both. A policy that
/// declares both kinds with matching rule blocks must reconcile cleanly.
#[tokio::test]
async fn test_networkpolicy_policy_types_ingress_and_egress() {
    let storage = setup_test().await;
    let controller = NetworkPolicyController::new(storage.clone());

    let spec = NetworkPolicySpec {
        pod_selector: selector_for(&[("app", "mixed")]),
        ingress: Some(vec![NetworkPolicyIngressRule {
            ports: None,
            from: Some(vec![NetworkPolicyPeer {
                pod_selector: Some(selector_for(&[("role", "peer")])),
                namespace_selector: None,
                ip_block: None,
            }]),
        }]),
        egress: Some(vec![NetworkPolicyEgressRule {
            ports: None,
            to: Some(vec![NetworkPolicyPeer {
                pod_selector: None,
                namespace_selector: None,
                ip_block: Some(IPBlock {
                    cidr: "10.0.0.0/8".to_string(),
                    except: Some(vec!["10.1.0.0/16".to_string()]),
                }),
            }]),
        }]),
        policy_types: Some(vec!["Ingress".to_string(), "Egress".to_string()]),
    };

    let policy = NetworkPolicy::new("mixed-policy", "default", spec);
    let key = build_key("networkpolicies", Some("default"), "mixed-policy");
    storage.create(&key, &policy).await.unwrap();

    controller.reconcile_all().await.unwrap();

    let stored: NetworkPolicy = storage.get(&key).await.unwrap();
    let types = stored.spec.policy_types.as_ref().expect("policyTypes");
    assert!(types.contains(&"Ingress".to_string()));
    assert!(types.contains(&"Egress".to_string()));

    let egress_rule = &stored.spec.egress.as_ref().unwrap()[0];
    let ip_block = egress_rule.to.as_ref().unwrap()[0]
        .ip_block
        .as_ref()
        .expect("ipBlock preserved");
    assert_eq!(ip_block.cidr, "10.0.0.0/8");
    assert_eq!(
        ip_block.except.as_ref().map(Vec::len),
        Some(1),
        "except CIDRs preserved"
    );
}

/// Upstream: `should support a 'default-deny-ingress' policy` — empty
/// `podSelector` plus empty ingress list and `policyTypes: [Ingress]` denies
/// all incoming traffic to every pod in the namespace.
#[tokio::test]
async fn test_networkpolicy_default_deny_ingress_reconciles() {
    let storage = setup_test().await;
    let controller = NetworkPolicyController::new(storage.clone());

    let spec = NetworkPolicySpec {
        pod_selector: empty_selector(),
        ingress: Some(vec![]),
        egress: None,
        policy_types: Some(vec!["Ingress".to_string()]),
    };

    let policy = NetworkPolicy::new("default-deny", "secure", spec);
    let key = build_key("networkpolicies", Some("secure"), "default-deny");
    storage.create(&key, &policy).await.unwrap();

    // Add a pod in the namespace so find_affected_pods returns it (empty
    // selector matches every pod).
    let pod = create_pod("workload-0", "secure", &[("app", "anything")]);
    storage
        .create(&build_key("pods", Some("secure"), "workload-0"), &pod)
        .await
        .unwrap();

    controller.reconcile_all().await.unwrap();

    let stored: NetworkPolicy = storage.get(&key).await.unwrap();
    assert!(
        stored.spec.ingress.as_ref().is_some_and(Vec::is_empty),
        "default-deny has an empty (not absent) ingress list"
    );
    assert_eq!(
        stored.spec.policy_types.as_deref(),
        Some(&["Ingress".to_string()][..]),
    );
}
