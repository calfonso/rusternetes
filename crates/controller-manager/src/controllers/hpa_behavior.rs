//! HPA `spec.behavior` scale rate policies.
//!
//! Port of upstream `pkg/controller/podautoscaler/horizontal.go`
//! (`convertDesiredReplicasWithBehaviorRate`,
//! `calculateScaleUpLimitWithScalingRules`,
//! `calculateScaleDownLimitWithBehaviors`, `getReplicasChangePerPeriod`,
//! `storeScaleEvent`). Given the metric-driven desired replica count, these
//! cap how fast the workload may scale per the configured `Pods`/`Percent`
//! policies over their `periodSeconds`, selecting the most/least permissive via
//! `selectPolicy` (`Max` default, `Min`, `Disabled`). A per-HPA history of
//! recent scale events feeds the period windows.
//!
//! Also implements `behavior.{scaleUp,scaleDown}.stabilizationWindowSeconds`
//! ([`stabilize_recommendation`] + [`RecommendationStore`], upstream
//! `stabilizeRecommendationWithBehaviors`). `now` is injected so the pure math
//! is unit-testable.

use std::collections::HashMap;
use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};
use rusternetes_common::resources::{HPAScalingRules, HorizontalPodAutoscalerBehavior};

/// One recorded scale change (absolute, non-negative), timestamped. Mirrors
/// upstream `timestampedScaleEvent`.
#[derive(Clone, Copy, Debug)]
pub struct ScaleEvent {
    pub change: i32,
    pub at: DateTime<Utc>,
}

/// Recent scale events for one HPA, split by direction: `(scale-up, scale-down)`.
pub type DirectionEvents = (Vec<ScaleEvent>, Vec<ScaleEvent>);

/// Sum of replica changes within `period_seconds` of `now`. Upstream
/// `getReplicasChangePerPeriod`.
fn change_in_period(events: &[ScaleEvent], period_seconds: i32, now: DateTime<Utc>) -> i32 {
    let cutoff = now - Duration::seconds(period_seconds as i64);
    events
        .iter()
        .filter(|e| e.at > cutoff)
        .map(|e| e.change)
        .sum()
}

/// Longest `periodSeconds` across a rule's policies (upstream
/// `getLongestPolicyPeriod`) — the window past which events can be pruned.
fn longest_period(rules: &HPAScalingRules) -> i32 {
    rules
        .policies
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|p| p.period_seconds)
        .max()
        .unwrap_or(0)
}

/// Upstream `calculateScaleUpLimitWithScalingRules`: the maximum replica count
/// the workload may reach this step under the scale-up rules.
fn scale_up_limit(
    current: i32,
    up: &[ScaleEvent],
    down: &[ScaleEvent],
    rules: &HPAScalingRules,
    now: DateTime<Utc>,
) -> i32 {
    let select = rules.select_policy.as_deref().unwrap_or("Max");
    if select == "Disabled" {
        return current;
    }
    let use_min = select == "Min";
    // For scale up: Min selects the lowest change, Max the highest.
    let mut result = if use_min { i32::MAX } else { i32::MIN };
    for p in rules.policies.as_deref().unwrap_or(&[]) {
        let added = change_in_period(up, p.period_seconds, now);
        let deleted = change_in_period(down, p.period_seconds, now);
        let period_start = current - added + deleted;
        let proposed = match p.policy_type.as_str() {
            "Pods" => period_start + p.value,
            "Percent" => (period_start as f64 * (1.0 + p.value as f64 / 100.0)).ceil() as i32,
            _ => continue,
        };
        result = if use_min {
            result.min(proposed)
        } else {
            result.max(proposed)
        };
    }
    result
}

/// Upstream `calculateScaleDownLimitWithBehaviors`: the minimum replica count
/// the workload may drop to this step under the scale-down rules.
fn scale_down_limit(
    current: i32,
    up: &[ScaleEvent],
    down: &[ScaleEvent],
    rules: &HPAScalingRules,
    now: DateTime<Utc>,
) -> i32 {
    let select = rules.select_policy.as_deref().unwrap_or("Max");
    if select == "Disabled" {
        return current;
    }
    let use_max = select == "Min";
    // For scale down: Min selects the lowest change (→ highest floor, use max);
    // the default selects the largest change (→ lowest floor, use min).
    let mut result = if use_max { i32::MIN } else { i32::MAX };
    for p in rules.policies.as_deref().unwrap_or(&[]) {
        let added = change_in_period(up, p.period_seconds, now);
        let deleted = change_in_period(down, p.period_seconds, now);
        let period_start = current - added + deleted;
        let proposed = match p.policy_type.as_str() {
            "Pods" => period_start - p.value,
            "Percent" => (period_start as f64 * (1.0 - p.value as f64 / 100.0)) as i32,
            _ => continue,
        };
        result = if use_max {
            result.max(proposed)
        } else {
            result.min(proposed)
        };
    }
    result
}

/// Upstream `convertDesiredReplicasWithBehaviorRate`: rate-limit `desired`
/// against `current` per the behavior policies and bound by min/max. Returns
/// the metric-driven `desired` unchanged when no rule constrains it (the caller
/// still applies the overall min/max clamp).
pub fn convert_with_behavior_rate(
    current: i32,
    desired: i32,
    min_replicas: i32,
    max_replicas: i32,
    behavior: &HorizontalPodAutoscalerBehavior,
    events: &DirectionEvents,
    now: DateTime<Utc>,
) -> i32 {
    let (up, down) = (events.0.as_slice(), events.1.as_slice());
    if desired > current {
        if let Some(rules) = behavior.scale_up.as_ref() {
            let mut limit = scale_up_limit(current, up, down, rules, now);
            // Don't scale up until the in-window events age out.
            if limit < current {
                limit = current;
            }
            let max_allowed = max_replicas.min(limit);
            if desired > max_allowed {
                return max_allowed;
            }
        }
    } else if desired < current {
        if let Some(rules) = behavior.scale_down.as_ref() {
            let mut limit = scale_down_limit(current, up, down, rules, now);
            if limit > current {
                limit = current;
            }
            let min_allowed = min_replicas.max(limit);
            if desired < min_allowed {
                return min_allowed;
            }
        }
    }
    desired
}

/// Per-HPA history of recent scale events, feeding the period windows. Mirrors
/// upstream's `scaleUpEvents` / `scaleDownEvents` maps; pruning past the longest
/// policy period replaces upstream's outdated-slot reuse with a plain retain.
#[derive(Default)]
pub struct ScaleEventStore {
    // key -> (scale-up events, scale-down events)
    inner: Mutex<HashMap<String, DirectionEvents>>,
}

impl ScaleEventStore {
    /// Snapshot of `(up, down)` events for `key` (empty if none recorded).
    pub fn snapshot(&self, key: &str) -> DirectionEvents {
        self.inner
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    /// Record a scale from `prev` → `new` replicas (upstream `storeScaleEvent`),
    /// pruning entries older than the relevant rule's longest policy period.
    /// No-op when `behavior` carries no rule for the direction or replicas are
    /// unchanged.
    pub fn record(
        &self,
        key: &str,
        behavior: &HorizontalPodAutoscalerBehavior,
        prev: i32,
        new: i32,
        now: DateTime<Utc>,
    ) {
        if new == prev {
            return;
        }
        let mut map = self.inner.lock().unwrap();
        let entry = map.entry(key.to_string()).or_default();
        if new > prev {
            let lp = behavior.scale_up.as_ref().map(longest_period).unwrap_or(0);
            let cutoff = now - Duration::seconds(lp as i64);
            entry.0.retain(|e| e.at > cutoff);
            entry.0.push(ScaleEvent {
                change: new - prev,
                at: now,
            });
        } else {
            let lp = behavior
                .scale_down
                .as_ref()
                .map(longest_period)
                .unwrap_or(0);
            let cutoff = now - Duration::seconds(lp as i64);
            entry.1.retain(|e| e.at > cutoff);
            entry.1.push(ScaleEvent {
                change: prev - new,
                at: now,
            });
        }
    }
}

/// Upstream `stabilizeRecommendationWithBehaviors`: clamp `desired` between the
/// *min* recommendation seen within the scale-up window and the *max* within the
/// scale-down window (each also bounded by `desired`), measured around
/// `current`. `recs` is the recent (unstabilized) recommendation history. This
/// holds the replica count at a recent high during a transient dip (downscale
/// stabilization) and at a recent low during a transient spike (upscale).
pub fn stabilize_recommendation(
    current: i32,
    desired: i32,
    up_window_s: i32,
    down_window_s: i32,
    recs: &[(DateTime<Utc>, i32)],
    now: DateTime<Utc>,
) -> i32 {
    let up_cutoff = now - Duration::seconds(up_window_s as i64);
    let down_cutoff = now - Duration::seconds(down_window_s as i64);
    let mut up_rec = desired;
    let mut down_rec = desired;
    for (t, r) in recs {
        if *t > up_cutoff {
            up_rec = up_rec.min(*r);
        }
        if *t > down_cutoff {
            down_rec = down_rec.max(*r);
        }
    }
    let mut rec = current;
    if rec < up_rec {
        rec = up_rec;
    }
    if rec > down_rec {
        rec = down_rec;
    }
    rec
}

/// A timestamped recommendation: `(observed_at, replicas)`. Mirrors upstream
/// `timestampedRecommendation`.
pub type Recommendation = (DateTime<Utc>, i32);

/// Per-HPA history of recent (unstabilized) recommendations feeding
/// [`stabilize_recommendation`]. Mirrors upstream's `recommendations` map of
/// `timestampedRecommendation`.
#[derive(Default)]
pub struct RecommendationStore {
    inner: Mutex<HashMap<String, Vec<Recommendation>>>,
}

impl RecommendationStore {
    /// Recent recommendations for `key` (empty if none).
    pub fn snapshot(&self, key: &str) -> Vec<Recommendation> {
        self.inner
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    /// Append a recommendation, pruning entries older than `max_window_s` (the
    /// longest of the two stabilization windows).
    pub fn record(&self, key: &str, recommendation: i32, max_window_s: i32, now: DateTime<Utc>) {
        let mut map = self.inner.lock().unwrap();
        let v = map.entry(key.to_string()).or_default();
        let cutoff = now - Duration::seconds(max_window_s as i64);
        v.retain(|(t, _)| *t >= cutoff);
        v.push((now, recommendation));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::HPAScalingPolicy;

    fn rules(select: &str, policies: Vec<HPAScalingPolicy>) -> HPAScalingRules {
        HPAScalingRules {
            stabilization_window_seconds: Some(0),
            select_policy: Some(select.to_string()),
            policies: Some(policies),
            tolerance: None,
        }
    }

    fn pods(value: i32, period: i32) -> HPAScalingPolicy {
        HPAScalingPolicy {
            policy_type: "Pods".to_string(),
            value,
            period_seconds: period,
        }
    }

    fn percent(value: i32, period: i32) -> HPAScalingPolicy {
        HPAScalingPolicy {
            policy_type: "Percent".to_string(),
            value,
            period_seconds: period,
        }
    }

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_000_000, 0).unwrap()
    }

    #[test]
    fn scale_up_pods_policy_caps_jump() {
        // 2 replicas, no prior events, +2 pods/60s → limit 4 (the #1105 case).
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Max", vec![pods(2, 60)])),
            scale_down: None,
        };
        assert_eq!(
            convert_with_behavior_rate(2, 17, 1, 20, &b, &(vec![], vec![]), now()),
            4
        );
    }

    #[test]
    fn scale_up_percent_policy() {
        // 10 replicas, +100%/60s → limit 20; desired 50 capped to min(max=100,20)=20.
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Max", vec![percent(100, 60)])),
            scale_down: None,
        };
        assert_eq!(
            convert_with_behavior_rate(10, 50, 1, 100, &b, &(vec![], vec![]), now()),
            20
        );
    }

    #[test]
    fn select_policy_max_takes_most_permissive() {
        // Pods +1 vs Percent +100% on 4 replicas → max(5, 8) = 8.
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Max", vec![pods(1, 60), percent(100, 60)])),
            scale_down: None,
        };
        assert_eq!(
            convert_with_behavior_rate(4, 100, 1, 100, &b, &(vec![], vec![]), now()),
            8
        );
    }

    #[test]
    fn select_policy_min_takes_least_permissive() {
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Min", vec![pods(1, 60), percent(100, 60)])),
            scale_down: None,
        };
        assert_eq!(
            convert_with_behavior_rate(4, 100, 1, 100, &b, &(vec![], vec![]), now()),
            5
        );
    }

    #[test]
    fn disabled_policy_blocks_scale_up() {
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Disabled", vec![pods(10, 60)])),
            scale_down: None,
        };
        // limit == current → capped to current.
        assert_eq!(
            convert_with_behavior_rate(3, 17, 1, 20, &b, &(vec![], vec![]), now()),
            3
        );
    }

    #[test]
    fn prior_events_in_window_reduce_headroom() {
        // 4 replicas, +2/60s policy, but already +2 added this window → period
        // start was 2, limit 4 == current → no further scale up.
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Max", vec![pods(2, 60)])),
            scale_down: None,
        };
        let up = vec![ScaleEvent {
            change: 2,
            at: now() - Duration::seconds(10),
        }];
        assert_eq!(
            convert_with_behavior_rate(4, 17, 1, 20, &b, &(up, vec![]), now()),
            4
        );
    }

    #[test]
    fn scale_down_pods_policy_caps_drop() {
        // 10 replicas, -3 pods/60s → floor 7; desired 1 capped to max(min=1,7)=7.
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: None,
            scale_down: Some(rules("Max", vec![pods(3, 60)])),
        };
        assert_eq!(
            convert_with_behavior_rate(10, 1, 1, 20, &b, &(vec![], vec![]), now()),
            7
        );
    }

    #[test]
    fn event_store_records_and_prunes() {
        let store = ScaleEventStore::default();
        let b = HorizontalPodAutoscalerBehavior {
            scale_up: Some(rules("Max", vec![pods(2, 60)])),
            scale_down: None,
        };
        // An old event (outside 60s) is pruned when a new one is recorded.
        store.record("ns/h", &b, 2, 3, now() - Duration::seconds(120));
        store.record("ns/h", &b, 3, 5, now());
        let (up, _down) = store.snapshot("ns/h");
        assert_eq!(up.len(), 1, "stale event pruned: {up:?}");
        assert_eq!(up[0].change, 2);
        assert_eq!(change_in_period(&up, 60, now()), 2);
    }

    #[test]
    fn stabilize_holds_recent_high_on_downscale() {
        // 8 replicas, metric now wants 2, but a recent recommendation of 8 is
        // within the 300s down-window → max(2,8)=8 → hold at 8.
        let recs = vec![(now() - Duration::seconds(30), 8)];
        assert_eq!(stabilize_recommendation(8, 2, 0, 300, &recs, now()), 8);
    }

    #[test]
    fn stabilize_allows_downscale_with_empty_history() {
        // No recent high → down_rec = desired → scale down proceeds.
        assert_eq!(stabilize_recommendation(8, 2, 0, 300, &[], now()), 2);
    }

    #[test]
    fn stabilize_holds_recent_low_on_upscale() {
        // 2 replicas, metric now wants 8, recent low of 2 within the up-window
        // → min(8,2)=2 → hold at 2 (don't chase a transient spike).
        let recs = vec![(now() - Duration::seconds(30), 2)];
        assert_eq!(stabilize_recommendation(2, 8, 300, 0, &recs, now()), 2);
    }

    #[test]
    fn stabilize_ignores_samples_outside_window() {
        // The high sample is older than the 300s down-window → not counted.
        let recs = vec![(now() - Duration::seconds(600), 8)];
        assert_eq!(stabilize_recommendation(8, 2, 0, 300, &recs, now()), 2);
    }

    #[test]
    fn recommendation_store_records_and_prunes() {
        let store = RecommendationStore::default();
        store.record("ns/h", 8, 300, now() - Duration::seconds(600));
        store.record("ns/h", 2, 300, now());
        let recs = store.snapshot("ns/h");
        assert_eq!(recs.len(), 1, "stale recommendation pruned: {recs:?}");
        assert_eq!(recs[0].1, 2);
    }
}
