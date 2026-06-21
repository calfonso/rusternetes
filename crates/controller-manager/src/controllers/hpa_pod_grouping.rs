//! HPA pod grouping for the initial-readiness / CPU-initialization window.
//!
//! Port of the CPU branch of upstream
//! `pkg/controller/podautoscaler/replica_calculator.go::groupPods`: when
//! computing CPU utilization, pods that are not yet usefully measurable — not
//! running, missing metrics, or unready/just-started within the
//! `cpuInitializationPeriod` / `delayOfInitialReadinessStatus` windows — are
//! excluded so a cold-start CPU spike doesn't trigger a scale-up.
//!
//! Scope note: this implements the *classification* (which pods count). The
//! upstream `calcPlainMetricReplicas` value-rebalancing (assigning assumed
//! usage to missing/unready pods to bias the ratio) is a separate refinement
//! and is not done here — we simply average over the ready pods. `now` and the
//! windows are injected so the logic is unit-testable.

use std::collections::HashSet;

use chrono::{DateTime, Duration, Utc};
use rusternetes_common::resources::pod::Pod;
use rusternetes_common::types::Phase;

use super::hpa_metrics_client::PodMetricsInfo;

/// Upstream `--horizontal-pod-autoscaler-cpu-initialization-period` default.
pub fn cpu_initialization_period() -> Duration {
    Duration::minutes(5)
}

/// Upstream `--horizontal-pod-autoscaler-initial-readiness-delay` default.
pub fn initial_readiness_delay() -> Duration {
    Duration::seconds(30)
}

/// The metrics reporting window. Upstream carries this per-sample
/// (`metric.Window`); rusternetes' `PodMetric` doesn't, so we use the standard
/// metrics-server resolution as a fixed window — it is effectively constant in
/// real deployments.
pub fn metric_window() -> Duration {
    Duration::seconds(30)
}

/// Names of the pods whose CPU metric should count toward utilization, mirroring
/// upstream `groupPods` (CPU branch): excludes deleting/Failed/Pending pods,
/// pods missing metrics, and unready/just-initializing pods.
pub fn ready_cpu_pods(
    pods: &[Pod],
    metrics: &PodMetricsInfo,
    now: DateTime<Utc>,
    cpu_init: Duration,
    readiness_delay: Duration,
    window: Duration,
) -> HashSet<String> {
    let mut ready = HashSet::new();
    for pod in pods {
        let name = &pod.metadata.name;
        // Deleting or Failed pods are ignored entirely.
        if pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        let Some(status) = pod.status.as_ref() else {
            continue;
        };
        match status.phase {
            Some(Phase::Failed) => continue,
            // Pending pods are unready.
            Some(Phase::Pending) => continue,
            _ => {}
        }
        // Pods without a metric reading are "missing", not ready.
        let Some(metric) = metrics.get(name) else {
            continue;
        };

        let ready_cond = status
            .conditions
            .as_ref()
            .and_then(|cs| cs.iter().find(|c| c.condition_type == "Ready"));

        let unready = match (ready_cond, status.start_time) {
            (Some(cond), Some(start)) => {
                if start + cpu_init > now {
                    // Still within the CPU initialization period: ignore the
                    // sample if the pod is unready, or if less than one metric
                    // window has elapsed since the last readiness transition.
                    let cond_false = cond.status == "False";
                    let window_not_elapsed = cond
                        .last_transition_time
                        .map(|lt| metric.timestamp < lt + window)
                        .unwrap_or(false);
                    cond_false || window_not_elapsed
                } else {
                    // Past the init period: ignore only if the pod is unready
                    // and has never been ready (still within the readiness delay
                    // of its start).
                    let cond_false = cond.status == "False";
                    cond_false
                        && cond
                            .last_transition_time
                            .map(|lt| start + readiness_delay > lt)
                            .unwrap_or(false)
                }
            }
            // No Ready condition or no start time → treat as unready.
            _ => true,
        };

        if !unready {
            ready.insert(name.clone());
        }
    }
    ready
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::pod::{PodCondition, PodStatus};

    fn metrics(names: &[&str]) -> PodMetricsInfo {
        super::super::hpa_metrics_client::FakeMetricsClient::pods_info(
            &names
                .iter()
                .map(|n| (*n, 0i64, Some(50)))
                .collect::<Vec<_>>(),
        )
    }

    fn pod(name: &str, phase: Phase, ready: Option<&str>, start_ago: Duration) -> Pod {
        let now = DateTime::from_timestamp(1_000_000, 0).unwrap();
        let conditions = ready.map(|s| {
            vec![PodCondition {
                condition_type: "Ready".to_string(),
                status: s.to_string(),
                reason: None,
                message: None,
                last_probe_time: None,
                last_transition_time: Some(now - start_ago),
                observed_generation: None,
            }]
        });
        let mut pod = Pod::new(name, Default::default());
        pod.status = Some(PodStatus {
            phase: Some(phase),
            start_time: Some(now - start_ago),
            conditions,
            ..Default::default()
        });
        pod
    }

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_000_000, 0).unwrap()
    }

    #[test]
    fn just_started_unready_pods_excluded() {
        // Running but Ready=False and started 10s ago (within cpuInit) → excluded.
        let pods = vec![
            pod("p1", Phase::Running, Some("False"), Duration::seconds(10)),
            pod("p2", Phase::Running, Some("False"), Duration::seconds(10)),
        ];
        let m = metrics(&["p1", "p2"]);
        let ready = ready_cpu_pods(
            &pods,
            &m,
            now(),
            cpu_initialization_period(),
            initial_readiness_delay(),
            metric_window(),
        );
        assert!(
            ready.is_empty(),
            "cold-start unready pods must be excluded: {ready:?}"
        );
    }

    #[test]
    fn long_ready_pods_counted() {
        // Ready=True since 10min ago, started 10min ago (past cpuInit) → counted.
        let pods = vec![pod(
            "p1",
            Phase::Running,
            Some("True"),
            Duration::minutes(10),
        )];
        let m = metrics(&["p1"]);
        let ready = ready_cpu_pods(
            &pods,
            &m,
            now(),
            cpu_initialization_period(),
            initial_readiness_delay(),
            metric_window(),
        );
        assert_eq!(ready.len(), 1);
        assert!(ready.contains("p1"));
    }

    #[test]
    fn pending_failed_missing_excluded() {
        let pods = vec![
            pod("pending", Phase::Pending, None, Duration::seconds(1)),
            pod("failed", Phase::Failed, Some("True"), Duration::minutes(10)),
            pod(
                "nometric",
                Phase::Running,
                Some("True"),
                Duration::minutes(10),
            ),
        ];
        // only "pending"/"failed" have... actually seed metric only for nometric? no:
        let m = metrics(&["pending", "failed"]); // "nometric" intentionally absent
        let ready = ready_cpu_pods(
            &pods,
            &m,
            now(),
            cpu_initialization_period(),
            initial_readiness_delay(),
            metric_window(),
        );
        assert!(
            ready.is_empty(),
            "pending/failed/missing all excluded: {ready:?}"
        );
    }
}
