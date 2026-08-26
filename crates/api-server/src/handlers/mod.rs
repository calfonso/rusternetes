pub mod admission_webhook;
pub mod apply;
pub mod authentication;
pub mod authorization;
pub mod certificates;
pub mod componentstatus;
pub mod configmap;
pub mod controllerrevision;
pub mod crd;
pub mod cronjob;
pub mod csidriver;
pub mod csinode;
pub mod csistoragecapacity;
pub mod custom_metrics;
pub mod custom_resource;
pub mod daemonset;
pub mod defaults;
pub mod deployment;
pub mod deviceclass;
pub mod discovery;
pub mod dryrun;
pub mod endpoints;
pub mod endpointslice;
pub mod event;
#[allow(dead_code)]
pub mod filtering;
pub mod finalizers;
pub mod flowcontrol;
pub mod generic;
pub mod generic_patch;
#[allow(dead_code)]
pub mod health;
pub mod horizontalpodautoscaler;
pub mod ingress;
pub mod ingressclass;
pub mod ipaddress;
pub mod job;
pub mod lease;
pub mod lifecycle;
pub mod limitrange;
pub mod metrics;
pub mod namespace;
pub mod networkpolicy;
pub mod node;
pub mod openapi;
pub mod persistentvolume;
pub mod persistentvolumeclaim;
pub mod pod;
pub mod pod_subresources;
pub mod poddisruptionbudget;
pub mod podtemplate;
pub mod priorityclass;
pub mod proxy;
pub mod rbac;
pub mod replicaset;
pub mod replicationcontroller;
pub mod resourceclaim;
pub mod resourceclaimtemplate;
pub mod resourcequota;
pub mod resourceslice;
pub mod runtimeclass;
pub mod scale;
pub mod secret;
pub mod service;
pub mod service_account;
pub mod servicecidr;
pub mod statefulset;
pub mod status;
pub mod storageclass;
pub mod table;
pub mod validating_admission_policy;
pub mod validation;
pub mod volumeattachment;
pub mod volumeattributesclass;
pub mod volumesnapshot;
pub mod volumesnapshotclass;
pub mod volumesnapshotcontent;
pub mod watch;

/// Compute the list-level resourceVersion from the max item resourceVersion.
/// This uses etcd mod_revisions (from individual items) rather than timestamps.
/// Using timestamps causes LIST+WATCH failures because watches start from a
/// revision that etcd never reaches.
pub fn list_resource_version<T: serde::Serialize>(items: &[T]) -> String {
    let mut max_rv: i64 = 0;
    for item in items {
        if let Ok(v) = serde_json::to_value(item) {
            if let Some(rv_str) = v
                .get("metadata")
                .and_then(|m| m.get("resourceVersion"))
                .and_then(|r| r.as_str())
            {
                if let Ok(rv) = rv_str.parse::<i64>() {
                    if rv > max_rv {
                        max_rv = rv;
                    }
                }
            }
        }
    }
    if max_rv > 0 {
        max_rv.to_string()
    } else {
        "1".to_string()
    }
}
