//! Pure HPA replica math, ported from k8s
//! `pkg/controller/podautoscaler/replica_calculator.go`.
//!
//! All functions are side-effect-free and unit-testable.
//! `desired = ceil(current * usageRatio)` with the k8s tolerance band: if
//! `|ratio - 1| <= TOLERANCE`, keep current replicas (avoid thrashing).
#![allow(dead_code)]

use crate::controllers::hpa_metrics_client::PodMetricsInfo;

/// k8s default scale tolerance (10%).
pub const TOLERANCE: f64 = 0.1;

fn within_tolerance(ratio: f64) -> bool {
    (ratio - 1.0).abs() <= TOLERANCE
}

fn scale(current_replicas: i32, usage_ratio: f64) -> i32 {
    if within_tolerance(usage_ratio) {
        return current_replicas;
    }
    (current_replicas as f64 * usage_ratio).ceil() as i32
}

/// Average a per-pod metric and compute replicas vs a per-pod target
/// (Pods type, AverageValue). Errors if no pods reported.
pub fn get_metric_replicas(
    metrics: &PodMetricsInfo,
    current_replicas: i32,
    target_average_value: i64,
) -> anyhow::Result<(i32, i64)> {
    if metrics.is_empty() {
        anyhow::bail!("no metrics returned for any pods");
    }
    let sum: i64 = metrics.values().map(|m| m.value).sum();
    let avg = sum / metrics.len() as i64;
    if target_average_value <= 0 {
        anyhow::bail!("target average value must be positive");
    }
    let ratio = avg as f64 / target_average_value as f64;
    Ok((scale(current_replicas, ratio), avg))
}

/// Resource utilization (per-pod %). Errors if no pods reported.
pub fn get_resource_replicas(
    metrics: &PodMetricsInfo,
    current_replicas: i32,
    target_utilization: i32,
) -> anyhow::Result<(i32, i32)> {
    if metrics.is_empty() {
        anyhow::bail!("no metrics returned for any pods");
    }
    let utils: Vec<i32> = metrics.values().filter_map(|m| m.utilization).collect();
    if utils.is_empty() {
        anyhow::bail!("no utilization data for any pods");
    }
    let avg = utils.iter().sum::<i32>() / utils.len() as i32;
    if target_utilization <= 0 {
        anyhow::bail!("target utilization must be positive");
    }
    let ratio = avg as f64 / target_utilization as f64;
    Ok((scale(current_replicas, ratio), avg))
}

/// Object metric: single value vs target (Value) or per-pod average
/// (AverageValue).
pub fn get_object_metric_replicas(
    value: i64,
    current_replicas: i32,
    target_value: i64,
) -> anyhow::Result<(i32, i64)> {
    if target_value <= 0 {
        anyhow::bail!("target value must be positive");
    }
    let ratio = value as f64 / target_value as f64;
    Ok((scale(current_replicas, ratio), value))
}

/// External metric: sum of values vs target. For AverageValue, target is
/// per-pod, so desired = ceil(sum / targetPerPod).
pub fn get_external_metric_replicas(
    values: &[i64],
    target_average_value: i64,
) -> anyhow::Result<(i32, i64)> {
    if values.is_empty() {
        anyhow::bail!("no external metric values returned");
    }
    if target_average_value <= 0 {
        anyhow::bail!("target average value must be positive");
    }
    let sum: i64 = values.iter().sum();
    let desired = (sum as f64 / target_average_value as f64).ceil() as i32;
    Ok((desired.max(1), sum))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::hpa_metrics_client::FakeMetricsClient;

    #[test]
    fn pods_metric_doubles_replicas() {
        // 2 pods @ 200 each, target 100/pod -> ratio 2.0 -> 2*2=4
        let info = FakeMetricsClient::pods_info(&[("p1", 200, None), ("p2", 200, None)]);
        let (replicas, avg) = get_metric_replicas(&info, 2, 100).unwrap();
        assert_eq!(replicas, 4);
        assert_eq!(avg, 200);
    }

    #[test]
    fn within_tolerance_keeps_current() {
        // avg 105 vs target 100 -> ratio 1.05 (<=0.1 band) -> unchanged
        let info = FakeMetricsClient::pods_info(&[("p1", 105, None)]);
        let (replicas, _) = get_metric_replicas(&info, 3, 100).unwrap();
        assert_eq!(replicas, 3);
    }

    #[test]
    fn empty_pods_errors() {
        let info = PodMetricsInfo::new();
        assert!(get_metric_replicas(&info, 2, 100).is_err());
        assert!(get_resource_replicas(&info, 2, 80).is_err());
    }

    #[test]
    fn external_sum_over_per_pod_target() {
        // queue depth 90 total, target 30/pod -> ceil(90/30)=3
        let (replicas, sum) = get_external_metric_replicas(&[90], 30).unwrap();
        assert_eq!(replicas, 3);
        assert_eq!(sum, 90);
    }

    #[test]
    fn object_ratio() {
        // value 200, target 100, current 2 -> 4
        let (replicas, _) = get_object_metric_replicas(200, 2, 100).unwrap();
        assert_eq!(replicas, 4);
    }
}
