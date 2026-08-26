pub mod apiservice;
pub mod cascade_tests;
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
pub mod ingress;
pub mod job;
pub mod loadbalancer;
pub mod namespace;
pub mod network_policy;
pub mod node;
pub mod pod_disruption_budget;
pub mod pv_binder;
pub mod replicaset;
pub mod replicationcontroller;
pub mod resource_quota;
#[allow(dead_code)]
pub mod resourceclaim;
pub mod service;
pub mod serviceaccount;
pub mod statefulset;
pub mod taint_eviction;
pub mod ttl_controller;
pub mod volume_expansion;
pub mod volume_snapshot;
pub mod vpa;

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
