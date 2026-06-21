use rusternetes_common::resources::deployment::{DeploymentStrategy, RollingUpdateDeployment};
use rusternetes_common::resources::workloads::{
    DaemonSetUpdateStrategy, RollingUpdateDaemonSet, RollingUpdateStatefulSetStrategy,
    StatefulSetUpdateStrategy,
};
/// Shared defaulting functions matching K8s API server defaulting.
///
/// K8s applies defaults through registered scheme defaulting functions.
/// SetDefaults_PodSpec runs on ALL PodSpecs (including templates in workloads).
/// SetDefaults_Container runs on ALL containers.
/// SetDefaults_Probe runs on ALL probes.
///
/// K8s ref: pkg/apis/core/v1/defaults.go
/// K8s ref: pkg/apis/apps/v1/defaults.go
use rusternetes_common::resources::PodSpec;

/// Returns `true` when an `Option<String>` is `None` or `Some("")`.
/// Mirrors the upstream Go check `if obj.<Field> == ""` for non-pointer
/// `string` fields in PodSpec — protobuf-decoded payloads from client-go
/// reach us as `Some("")` rather than `None` because the Go wire format
/// emits zero-valued strings.
fn is_unset_or_empty(s: &Option<String>) -> bool {
    s.as_deref().is_none_or(str::is_empty)
}

/// Apply K8s defaults to a PodSpec.
/// Matches SetDefaults_PodSpec from pkg/apis/core/v1/defaults.go
pub fn apply_pod_spec_defaults(spec: &mut PodSpec) {
    // K8s: SetDefaults_PodSpec
    if is_unset_or_empty(&spec.dns_policy) {
        spec.dns_policy = Some("ClusterFirst".to_string());
    }
    if is_unset_or_empty(&spec.restart_policy) {
        spec.restart_policy = Some("Always".to_string());
    }
    if spec.termination_grace_period_seconds.is_none() {
        spec.termination_grace_period_seconds = Some(30);
    }
    if is_unset_or_empty(&spec.scheduler_name) {
        spec.scheduler_name = Some("default-scheduler".to_string());
    }
    // K8s defaults securityContext to empty struct (not nil).
    // This matters for byte-level comparisons like DaemonSet ControllerRevision Match().
    // K8s ref: pkg/apis/core/v1/defaults.go:222-224
    if spec.security_context.is_none() {
        spec.security_context =
            Some(rusternetes_common::resources::pod::PodSecurityContext::default());
    }

    // K8s: SetDefaults_Container (runs on all containers in the spec)
    for container in &mut spec.containers {
        apply_container_defaults(container);
    }
    if let Some(ref mut init_containers) = spec.init_containers {
        for container in init_containers {
            apply_container_defaults(container);
        }
    }
}

/// Apply K8s defaults to a Container.
/// Matches SetDefaults_Container from pkg/apis/core/v1/defaults.go
fn apply_container_defaults(container: &mut rusternetes_common::resources::Container) {
    if container.termination_message_path.is_none() {
        container.termination_message_path = Some("/dev/termination-log".to_string());
    }
    if container.termination_message_policy.is_none() {
        container.termination_message_policy = Some("File".to_string());
    }
    if container.image_pull_policy.is_none() {
        if container.image.contains(":latest") || !container.image.contains(':') {
            container.image_pull_policy = Some("Always".to_string());
        } else {
            container.image_pull_policy = Some("IfNotPresent".to_string());
        }
    }

    // K8s: SetDefaults_Container — default port protocol to TCP
    // K8s ref: pkg/apis/core/v1/defaults.go — SetDefaults_Container
    if let Some(ref mut ports) = container.ports {
        for port in ports.iter_mut() {
            if port.protocol.is_none() {
                port.protocol = Some("TCP".to_string());
            }
        }
    }

    // K8s: SetDefaults_Probe (runs on all probes)
    if let Some(ref mut probe) = container.liveness_probe {
        apply_probe_defaults(probe);
    }
    if let Some(ref mut probe) = container.readiness_probe {
        apply_probe_defaults(probe);
    }
    if let Some(ref mut probe) = container.startup_probe {
        apply_probe_defaults(probe);
    }
}

/// Apply K8s defaults to a Probe.
/// Matches SetDefaults_Probe from pkg/apis/core/v1/defaults.go
fn apply_probe_defaults(probe: &mut rusternetes_common::resources::pod::Probe) {
    if probe.timeout_seconds.is_none() || probe.timeout_seconds == Some(0) {
        probe.timeout_seconds = Some(1);
    }
    if probe.period_seconds.is_none() || probe.period_seconds == Some(0) {
        probe.period_seconds = Some(10);
    }
    if probe.success_threshold.is_none() || probe.success_threshold == Some(0) {
        probe.success_threshold = Some(1);
    }
    if probe.failure_threshold.is_none() || probe.failure_threshold == Some(0) {
        probe.failure_threshold = Some(3);
    }
}

/// Apply K8s defaults to a PodTemplateSpec (used by workload resources).
/// This is called for DaemonSet, Deployment, StatefulSet, ReplicaSet, Job, CronJob.
pub fn apply_pod_template_defaults(template: &mut rusternetes_common::resources::PodTemplateSpec) {
    apply_pod_spec_defaults(&mut template.spec);
}

/// Apply DaemonSet-specific defaults.
/// Matches SetDefaults_DaemonSet from pkg/apis/apps/v1/defaults.go
pub fn apply_daemonset_defaults(ds: &mut rusternetes_common::resources::DaemonSet) {
    if ds.spec.update_strategy.is_none() {
        ds.spec.update_strategy = Some(DaemonSetUpdateStrategy {
            strategy_type: Some("RollingUpdate".to_string()),
            rolling_update: Some(RollingUpdateDaemonSet {
                max_unavailable: Some("1".to_string()),
                max_surge: Some("0".to_string()),
            }),
        });
    } else if let Some(ref mut strategy) = ds.spec.update_strategy {
        if strategy.strategy_type.is_none() {
            strategy.strategy_type = Some("RollingUpdate".to_string());
        }
        if strategy.strategy_type.as_deref() == Some("RollingUpdate") {
            if strategy.rolling_update.is_none() {
                strategy.rolling_update = Some(RollingUpdateDaemonSet {
                    max_unavailable: Some("1".to_string()),
                    max_surge: Some("0".to_string()),
                });
            } else if let Some(ref mut ru) = strategy.rolling_update {
                if ru.max_unavailable.is_none() {
                    ru.max_unavailable = Some("1".to_string());
                }
                if ru.max_surge.is_none() {
                    ru.max_surge = Some("0".to_string());
                }
            }
        }
    }
    if ds.spec.revision_history_limit.is_none() {
        ds.spec.revision_history_limit = Some(10);
    }
    apply_pod_template_defaults(&mut ds.spec.template);
}

/// Apply Deployment-specific defaults.
/// Matches SetDefaults_Deployment from pkg/apis/apps/v1/defaults.go
pub fn apply_deployment_defaults(deploy: &mut rusternetes_common::resources::Deployment) {
    if deploy.spec.replicas.is_none() {
        deploy.spec.replicas = Some(1);
    }
    if deploy.spec.strategy.is_none() {
        deploy.spec.strategy = Some(DeploymentStrategy {
            strategy_type: "RollingUpdate".to_string(),
            rolling_update: Some(RollingUpdateDeployment {
                max_unavailable: Some(serde_json::json!("25%")),
                max_surge: Some(serde_json::json!("25%")),
            }),
        });
    } else if let Some(ref mut strategy) = deploy.spec.strategy {
        if strategy.strategy_type.is_empty() {
            strategy.strategy_type = "RollingUpdate".to_string();
        }
        if strategy.strategy_type == "RollingUpdate" {
            if strategy.rolling_update.is_none() {
                strategy.rolling_update = Some(RollingUpdateDeployment {
                    max_unavailable: Some(serde_json::json!("25%")),
                    max_surge: Some(serde_json::json!("25%")),
                });
            } else if let Some(ref mut ru) = strategy.rolling_update {
                if ru.max_unavailable.is_none() {
                    ru.max_unavailable = Some(serde_json::json!("25%"));
                }
                if ru.max_surge.is_none() {
                    ru.max_surge = Some(serde_json::json!("25%"));
                }
            }
        }
    }
    if deploy.spec.revision_history_limit.is_none() {
        deploy.spec.revision_history_limit = Some(10);
    }
    if deploy.spec.progress_deadline_seconds.is_none() {
        deploy.spec.progress_deadline_seconds = Some(600);
    }
    apply_pod_template_defaults(&mut deploy.spec.template);
}

/// Apply StatefulSet-specific defaults.
/// Matches SetDefaults_StatefulSet from pkg/apis/apps/v1/defaults.go
pub fn apply_statefulset_defaults(ss: &mut rusternetes_common::resources::StatefulSet) {
    if ss.spec.pod_management_policy.is_none() {
        ss.spec.pod_management_policy = Some("OrderedReady".to_string());
    }
    if ss.spec.update_strategy.is_none() {
        ss.spec.update_strategy = Some(StatefulSetUpdateStrategy {
            strategy_type: Some("RollingUpdate".to_string()),
            rolling_update: Some(RollingUpdateStatefulSetStrategy {
                partition: Some(0),
                max_unavailable: None,
            }),
        });
    } else if let Some(ref mut strategy) = ss.spec.update_strategy {
        if strategy.strategy_type.is_none() {
            strategy.strategy_type = Some("RollingUpdate".to_string());
        }
        if strategy.strategy_type.as_deref() == Some("RollingUpdate") {
            if strategy.rolling_update.is_none() {
                strategy.rolling_update = Some(RollingUpdateStatefulSetStrategy {
                    partition: Some(0),
                    max_unavailable: None,
                });
            } else if let Some(ref mut ru) = strategy.rolling_update {
                if ru.partition.is_none() {
                    ru.partition = Some(0);
                }
            }
        }
    }
    if ss.spec.replicas.is_none() {
        ss.spec.replicas = Some(1);
    }
    if ss.spec.revision_history_limit.is_none() {
        ss.spec.revision_history_limit = Some(10);
    }
    apply_pod_template_defaults(&mut ss.spec.template);
}

/// Apply ReplicaSet-specific defaults.
/// Matches SetDefaults_ReplicaSet from pkg/apis/apps/v1/defaults.go
pub fn apply_replicaset_defaults(rs: &mut rusternetes_common::resources::ReplicaSet) {
    // ReplicaSet.spec.replicas defaults via serde `default = "default_one_replica"`,
    // so no need to set here. Apply pod template defaults.
    apply_pod_template_defaults(&mut rs.spec.template);
}

/// Apply Job-specific defaults.
/// Matches SetDefaults_Job from pkg/apis/batch/v1/defaults.go
pub fn apply_job_defaults(job: &mut rusternetes_common::resources::Job) {
    apply_job_defaults_to_spec(&mut job.spec);

    // K8s: copy template labels to job labels if job has no labels
    if job.metadata.labels.is_none()
        || job
            .metadata
            .labels
            .as_ref()
            .map(|l| l.is_empty())
            .unwrap_or(true)
    {
        if let Some(ref tmeta) = job.spec.template.metadata {
            if let Some(ref labels) = tmeta.labels {
                if !labels.is_empty() {
                    job.metadata.labels = Some(labels.clone());
                }
            }
        }
    }
}

/// Apply CronJob-specific defaults.
/// Matches SetDefaults_CronJob from pkg/apis/batch/v1/defaults.go
pub fn apply_cronjob_defaults(cj: &mut rusternetes_common::resources::CronJob) {
    if cj.spec.concurrency_policy.is_none() {
        cj.spec.concurrency_policy = Some("Allow".to_string());
    }
    if cj.spec.suspend.is_none() {
        cj.spec.suspend = Some(false);
    }
    if cj.spec.successful_jobs_history_limit.is_none() {
        cj.spec.successful_jobs_history_limit = Some(3);
    }
    if cj.spec.failed_jobs_history_limit.is_none() {
        cj.spec.failed_jobs_history_limit = Some(1);
    }
    // Apply job defaults to the job template
    apply_job_defaults_to_spec(&mut cj.spec.job_template.spec);
}

/// Apply Job spec defaults (without the Job-level wrapper)
fn apply_job_defaults_to_spec(spec: &mut rusternetes_common::resources::JobSpec) {
    if spec.completions.is_none() && spec.parallelism.is_none() {
        spec.completions = Some(1);
        spec.parallelism = Some(1);
    }
    if spec.parallelism.is_none() {
        spec.parallelism = Some(1);
    }
    if spec.backoff_limit.is_none() {
        // Upstream SetDefaults_Job: when backoffLimitPerIndex is set, the
        // overall backoffLimit defaults to MaxInt32 (per-index limits govern
        // instead); otherwise it defaults to 6.
        spec.backoff_limit = Some(if spec.backoff_limit_per_index.is_some() {
            i32::MAX
        } else {
            6
        });
    }
    if spec.completion_mode.is_none() {
        spec.completion_mode = Some("NonIndexed".to_string());
    }
    if spec.suspend.is_none() {
        spec.suspend = Some(false);
    }
    // Upstream SetDefaults_Job: manualSelector defaults to false.
    if spec.manual_selector.is_none() {
        spec.manual_selector = Some(false);
    }
    apply_pod_template_defaults(&mut spec.template);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Container, PodSpec, PodTemplateSpec};

    #[test]
    fn test_apply_pod_spec_defaults() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "nginx:1.19".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        apply_pod_spec_defaults(&mut spec);

        assert_eq!(spec.dns_policy.as_deref(), Some("ClusterFirst"));
        assert_eq!(spec.restart_policy.as_deref(), Some("Always"));
        assert_eq!(spec.termination_grace_period_seconds, Some(30));
        assert_eq!(spec.scheduler_name.as_deref(), Some("default-scheduler"));

        let c = &spec.containers[0];
        assert_eq!(
            c.termination_message_path.as_deref(),
            Some("/dev/termination-log")
        );
        assert_eq!(c.termination_message_policy.as_deref(), Some("File"));
        assert_eq!(c.image_pull_policy.as_deref(), Some("IfNotPresent"));
    }

    #[test]
    fn test_container_defaults_latest_tag() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "nginx:latest".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        apply_pod_spec_defaults(&mut spec);
        assert_eq!(
            spec.containers[0].image_pull_policy.as_deref(),
            Some("Always")
        );
    }

    #[test]
    fn test_container_defaults_no_tag() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "nginx".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        apply_pod_spec_defaults(&mut spec);
        assert_eq!(
            spec.containers[0].image_pull_policy.as_deref(),
            Some("Always")
        );
    }

    /// Regression: protobuf-encoded PodSpecs from client-go arrive with
    /// `schedulerName: ""` for an unset Go `string` field (no `omitempty`
    /// over the protobuf wire — zero values are still emitted). Upstream
    /// `SetDefaults_PodSpec` checks `if obj.SchedulerName == ""`, so an
    /// empty string is also defaulted to `v1.DefaultSchedulerName`. The
    /// rusternetes check used `Option::is_none()` only and missed
    /// `Some("")`, which made the scheduler silently skip such pods
    /// (their canonical name stayed `""` ≠ `"default-scheduler"`).
    /// Surfaced via hydrophone after PR #134.
    #[test]
    fn test_empty_scheduler_name_defaults_to_default_scheduler() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx:1.27".to_string(),
                ..Default::default()
            }],
            scheduler_name: Some(String::new()),
            ..Default::default()
        };
        apply_pod_spec_defaults(&mut spec);
        assert_eq!(spec.scheduler_name.as_deref(), Some("default-scheduler"));
    }

    #[test]
    fn test_empty_dns_policy_defaults_to_cluster_first() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx:1.27".to_string(),
                ..Default::default()
            }],
            dns_policy: Some(String::new()),
            ..Default::default()
        };
        apply_pod_spec_defaults(&mut spec);
        assert_eq!(spec.dns_policy.as_deref(), Some("ClusterFirst"));
    }

    #[test]
    fn test_empty_restart_policy_defaults_to_always() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "c".to_string(),
                image: "nginx:1.27".to_string(),
                ..Default::default()
            }],
            restart_policy: Some(String::new()),
            ..Default::default()
        };
        apply_pod_spec_defaults(&mut spec);
        assert_eq!(spec.restart_policy.as_deref(), Some("Always"));
    }

    #[test]
    fn test_does_not_override_existing() {
        let mut spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "nginx:1.19".to_string(),
                image_pull_policy: Some("Never".to_string()),
                termination_message_path: Some("/custom/path".to_string()),
                ..Default::default()
            }],
            dns_policy: Some("None".to_string()),
            restart_policy: Some("OnFailure".to_string()),
            termination_grace_period_seconds: Some(60),
            scheduler_name: Some("my-scheduler".to_string()),
            ..Default::default()
        };

        apply_pod_spec_defaults(&mut spec);

        assert_eq!(spec.dns_policy.as_deref(), Some("None"));
        assert_eq!(spec.restart_policy.as_deref(), Some("OnFailure"));
        assert_eq!(spec.termination_grace_period_seconds, Some(60));
        assert_eq!(spec.scheduler_name.as_deref(), Some("my-scheduler"));
        assert_eq!(
            spec.containers[0].image_pull_policy.as_deref(),
            Some("Never")
        );
        assert_eq!(
            spec.containers[0].termination_message_path.as_deref(),
            Some("/custom/path")
        );
    }

    #[test]
    fn test_probe_defaults() {
        use rusternetes_common::resources::pod::{HTTPGetAction, Probe};

        let mut spec = PodSpec {
            containers: vec![Container {
                name: "test".to_string(),
                image: "nginx:1.19".to_string(),
                liveness_probe: Some(Probe {
                    http_get: Some(HTTPGetAction {
                        path: Some("/health".to_string()),
                        port: rusternetes_common::resources::IntOrString::Int(8080),
                        host: None,
                        scheme: None,
                        http_headers: None,
                    }),
                    tcp_socket: None,
                    exec: None,
                    grpc: None,
                    initial_delay_seconds: None,
                    timeout_seconds: None,
                    period_seconds: None,
                    success_threshold: None,
                    failure_threshold: None,
                    termination_grace_period_seconds: None,
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        apply_pod_spec_defaults(&mut spec);

        let probe = spec.containers[0].liveness_probe.as_ref().unwrap();
        assert_eq!(probe.timeout_seconds, Some(1));
        assert_eq!(probe.period_seconds, Some(10));
        assert_eq!(probe.success_threshold, Some(1));
        assert_eq!(probe.failure_threshold, Some(3));
    }

    #[test]
    fn test_pod_template_defaults_applied() {
        let mut template = PodTemplateSpec {
            metadata: None,
            spec: PodSpec {
                containers: vec![Container {
                    name: "test".to_string(),
                    image: "busybox".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        };

        apply_pod_template_defaults(&mut template);

        assert_eq!(template.spec.dns_policy.as_deref(), Some("ClusterFirst"));
        assert_eq!(template.spec.restart_policy.as_deref(), Some("Always"));
        assert_eq!(
            template.spec.scheduler_name.as_deref(),
            Some("default-scheduler")
        );
        assert_eq!(template.spec.termination_grace_period_seconds, Some(30));
        assert_eq!(
            template.spec.containers[0].image_pull_policy.as_deref(),
            Some("Always")
        );
    }
}
