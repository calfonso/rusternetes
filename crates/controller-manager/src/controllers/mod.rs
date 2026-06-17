pub mod apiservice;
pub mod cascade_tests;
pub mod cert_authority;
pub mod certificate_signing_request;
pub mod crd;
pub mod cronjob;
pub mod daemonset;
pub mod deployment;
pub mod dynamic_provisioner;
pub mod endpoints;
pub mod endpointslice;
pub mod events;
pub mod garbage_collector;
pub mod hpa;
pub mod hpa_metrics_client;
pub mod hpa_replica_calculator;
pub mod ingress;
pub mod job;
pub mod limitrange;
pub mod loadbalancer;
pub mod namespace;
pub mod network_policy;
pub mod node;
pub mod node_ipam;
pub mod pod_disruption_budget;
pub mod priorityclass;
pub mod pv_binder;
pub mod pvc;
pub mod replicaset;
pub mod replicationcontroller;
pub mod resource_quota;
#[allow(dead_code)]
pub mod resourceclaim;
pub mod service;
pub mod serviceaccount;
pub mod statefulset;
pub mod storage_class;
pub mod taint_eviction;
pub mod ttl_controller;
#[allow(dead_code)]
pub mod volume_attachment;
pub mod volume_expansion;
pub mod volume_snapshot;
pub mod vpa;

/// Propagate the pod's ServiceAccount `imagePullSecrets` onto a
/// controller-created pod spec (#1084). Controllers write pods straight to
/// storage and bypass the api-server admission path, so without this the SA's
/// pull secrets never reach DaemonSet/ReplicaSet/StatefulSet/Job pods.
///
/// Resolves the SA named by the spec (default `"default"`) and applies the
/// same semantics as admission (PR #1083): copy only when the pod declares no
/// secrets of its own, regardless of automount. A missing SA is a no-op.
pub async fn propagate_sa_image_pull_secrets<S: rusternetes_storage::Storage>(
    storage: &S,
    namespace: &str,
    spec: &mut rusternetes_common::resources::PodSpec,
) {
    let sa_name = spec.service_account_name.as_deref().unwrap_or("default");
    let sa_key = format!("/registry/serviceaccounts/{}/{}", namespace, sa_name);
    if let Ok(sa) = storage
        .get::<rusternetes_common::resources::ServiceAccount>(&sa_key)
        .await
    {
        rusternetes_common::serviceaccount::propagate_image_pull_secrets(
            spec,
            sa.image_pull_secrets.as_deref(),
        );
    }
}

/// Check ResourceQuota before creating a pod in a namespace.
/// Returns Ok(()) if quota allows, Err with quota exceeded message otherwise.
pub async fn check_resource_quota<S: rusternetes_storage::Storage>(
    storage: &S,
    namespace: &str,
) -> anyhow::Result<()> {
    let quota_prefix = format!("/registry/resourcequotas/{}/", namespace);
    let quotas: Vec<serde_json::Value> = storage.list(&quota_prefix).await.unwrap_or_default();
    for quota in &quotas {
        if let Some(hard) = quota.pointer("/spec/hard") {
            for limit_key in ["pods", "count/pods"] {
                if let Some(limit_str) = hard.get(limit_key).and_then(|v| v.as_str()) {
                    let limit: i64 = limit_str.parse().unwrap_or(i64::MAX);
                    let pod_prefix = format!("/registry/pods/{}/", namespace);
                    let current: Vec<serde_json::Value> =
                        storage.list(&pod_prefix).await.unwrap_or_default();
                    // Only count active pods (not Failed/Succeeded/terminating)
                    let active_count = current
                        .iter()
                        .filter(|p| {
                            let phase = p
                                .pointer("/status/phase")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            let terminating = p.pointer("/metadata/deletionTimestamp").is_some();
                            !terminating && phase != "Failed" && phase != "Succeeded"
                        })
                        .count();
                    if active_count as i64 >= limit {
                        return Err(anyhow::anyhow!(
                            "exceeded quota: {}, requested: 1, used: {}, limited: {}",
                            limit_key,
                            active_count,
                            limit_str
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}
