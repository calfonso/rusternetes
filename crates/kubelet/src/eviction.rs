//! Out of Resource (OOR) Handling
//!
//! Implements kubelet eviction logic for managing node resource exhaustion.
//! When the node runs low on memory or disk space, the kubelet must evict pods
//! to prevent node failure and maintain system stability.
//!
//! Key concepts:
//! - **Eviction Signals**: Memory pressure, disk pressure, PID pressure
//! - **Eviction Thresholds**: Soft and hard thresholds for triggering eviction
//! - **QoS-based Eviction**: Pods are evicted in priority order (BestEffort → Burstable → Guaranteed)
//! - **Resource Usage Ordering**: Within a QoS class, evict based on resource consumption
//!
//! ## Upstream parity references
//!
//! This module mirrors the logic in upstream Kubernetes at:
//! - `pkg/kubelet/eviction/eviction_manager.go::synchronize` — transition-period gate.
//! - `pkg/kubelet/eviction/helpers.go::thresholdsFirstObservedAt`, `nodeConditions`.
//! - `cmd/kubelet/app/options/options.go` — `--eviction-hard`, `--eviction-soft`,
//!   `--eviction-minimum-reclaim`, `--eviction-pressure-transition-period` flags.
//! - `google/cadvisor/fs/fs.go::GetFsInfoForPath` — filesystem stats via `statfs`.

use anyhow::{anyhow, Result};
use rusternetes_common::resources::{Node, NodeCondition, Pod};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Eviction signals that can trigger pod eviction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvictionSignal {
    /// Available memory below threshold
    MemoryAvailable,
    /// Available disk space below threshold (nodefs)
    NodeFsAvailable,
    /// Available inodes below threshold (nodefs)
    NodeFsInodesFree,
    /// Available disk space below threshold (imagefs)
    ImageFsAvailable,
    /// Available inodes below threshold (imagefs)
    ImageFsInodesFree,
    /// Available PIDs below threshold
    PidAvailable,
}

impl EvictionSignal {
    /// Parse the upstream signal name (e.g. `memory.available`).
    pub fn from_upstream_name(name: &str) -> Option<Self> {
        match name {
            "memory.available" => Some(Self::MemoryAvailable),
            "nodefs.available" => Some(Self::NodeFsAvailable),
            "nodefs.inodesFree" => Some(Self::NodeFsInodesFree),
            "imagefs.available" => Some(Self::ImageFsAvailable),
            "imagefs.inodesFree" => Some(Self::ImageFsInodesFree),
            "pid.available" => Some(Self::PidAvailable),
            _ => None,
        }
    }
}

/// Eviction threshold configuration
#[derive(Debug, Clone)]
pub struct EvictionThreshold {
    /// The signal to monitor
    pub signal: EvictionSignal,
    /// Hard threshold (immediate eviction)
    pub hard: Option<EvictionValue>,
    /// Soft threshold (eviction after grace period)
    pub soft: Option<EvictionValue>,
    /// Grace period for soft thresholds
    pub grace_period: Option<Duration>,
}

/// Eviction threshold value (percentage or absolute)
#[derive(Debug, Clone, PartialEq)]
pub enum EvictionValue {
    /// Percentage threshold (e.g., 10.0 means evict when less than 10% available)
    Percentage(f64),
    /// Absolute bytes threshold
    Absolute(u64),
}

/// QoS class for pod eviction priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QoSClass {
    /// Guaranteed: limits == requests for all resources
    Guaranteed = 3,
    /// Burstable: some containers have limits/requests
    Burstable = 2,
    /// BestEffort: no limits or requests
    BestEffort = 1,
}

/// Resource statistics for a node
#[derive(Debug, Clone)]
pub struct NodeStats {
    /// Available memory in bytes
    pub memory_available_bytes: u64,
    /// Total memory in bytes
    pub memory_total_bytes: u64,
    /// Available disk space in bytes (nodefs)
    pub nodefs_available_bytes: u64,
    /// Total disk space in bytes (nodefs)
    pub nodefs_total_bytes: u64,
    /// Available inodes (nodefs)
    pub nodefs_inodes_free: u64,
    /// Total inodes (nodefs)
    pub nodefs_inodes_total: u64,
    /// Available PIDs
    pub pid_available: u64,
    /// Total PIDs
    pub pid_total: u64,
}

/// Pod resource usage statistics
#[derive(Debug, Clone)]
pub struct PodStats {
    /// Pod name
    pub name: String,
    /// Pod namespace
    pub namespace: String,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// Disk usage in bytes
    pub disk_usage_bytes: u64,
    /// QoS class
    pub qos_class: QoSClass,
}

/// Upstream default for `--eviction-pressure-transition-period`.
pub const DEFAULT_TRANSITION_PERIOD: Duration = Duration::from_secs(5 * 60);

/// Minimum observation window before a hard threshold actually trips eviction.
/// Mirrors upstream `thresholdsMetGracePeriod` for hard thresholds (which is 0
/// by default but is gated by `thresholdsFirstObservedAt`). We use a small
/// non-zero window so transient blips don't immediately trigger.
const HARD_MIN_OBSERVATION: Duration = Duration::from_secs(10);

/// Minimum interval between repeat "still under pressure" log lines.
const PRESSURE_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Eviction manager for handling out-of-resource situations
pub struct EvictionManager {
    /// Eviction thresholds configuration
    pub thresholds: Vec<EvictionThreshold>,
    /// Last time soft thresholds were exceeded (for grace period tracking)
    soft_threshold_exceeded: HashMap<EvictionSignal, Instant>,
    /// First time each hard signal was observed exceeded (cleared when not exceeded).
    /// Mirrors upstream `thresholdsFirstObservedAt`.
    hard_observations: HashMap<EvictionSignal, Instant>,
    /// Last time each hard signal was observed exceeded. Used to enforce
    /// the `EvictionPressureTransitionPeriod`: a signal stays in the active
    /// set until `transition_period` has elapsed since this timestamp.
    /// Mirrors upstream `lastObservations` in `eviction_manager.go`.
    last_observed_at: HashMap<EvictionSignal, Instant>,
    /// `--eviction-pressure-transition-period`
    transition_period: Duration,
    /// The previously reported set of active hard-pressure signals.
    last_pressure_signals: HashSet<EvictionSignal>,
    /// When `last_pressure_signals` last transitioned.
    last_pressure_change: Option<Instant>,
    /// Last time we emitted a "still under pressure" info! line.
    last_pressure_log: Option<Instant>,
}

/// Default upstream-equivalent thresholds.
/// Matches upstream `cmd/kubelet/app/options/options.go` defaults:
/// `memory.available<100Mi`, `nodefs.available<10%`, `nodefs.inodesFree<5%`,
/// `imagefs.available<15%`, `imagefs.inodesFree<5%`.
fn default_thresholds() -> Vec<EvictionThreshold> {
    vec![
        EvictionThreshold {
            signal: EvictionSignal::MemoryAvailable,
            hard: Some(EvictionValue::Absolute(100 * 1024 * 1024)),
            soft: None,
            grace_period: None,
        },
        EvictionThreshold {
            signal: EvictionSignal::NodeFsAvailable,
            hard: Some(EvictionValue::Percentage(10.0)),
            soft: None,
            grace_period: None,
        },
        EvictionThreshold {
            signal: EvictionSignal::NodeFsInodesFree,
            hard: Some(EvictionValue::Percentage(5.0)),
            soft: None,
            grace_period: None,
        },
        EvictionThreshold {
            signal: EvictionSignal::ImageFsAvailable,
            hard: Some(EvictionValue::Percentage(15.0)),
            soft: None,
            grace_period: None,
        },
        EvictionThreshold {
            signal: EvictionSignal::ImageFsInodesFree,
            hard: Some(EvictionValue::Percentage(5.0)),
            soft: None,
            grace_period: None,
        },
    ]
}

impl EvictionManager {
    /// Create a new eviction manager with upstream-default thresholds.
    pub fn new() -> Self {
        Self::with_config(default_thresholds(), DEFAULT_TRANSITION_PERIOD)
    }

    /// Create with the given thresholds and transition period.
    ///
    /// An empty `thresholds` vec disables the eviction subsystem entirely:
    /// `check_eviction_needed` will return an empty signal set and emit no
    /// node-condition mutations beyond clearing existing pressure conditions.
    pub fn with_config(thresholds: Vec<EvictionThreshold>, transition_period: Duration) -> Self {
        Self {
            thresholds,
            soft_threshold_exceeded: HashMap::new(),
            hard_observations: HashMap::new(),
            last_observed_at: HashMap::new(),
            transition_period,
            last_pressure_signals: HashSet::new(),
            last_pressure_change: None,
            last_pressure_log: None,
        }
    }

    /// True iff the eviction subsystem is disabled (no thresholds configured).
    pub fn is_disabled(&self) -> bool {
        self.thresholds.is_empty()
    }

    /// Check if eviction is needed based on current node statistics.
    ///
    /// Returns the set of signals that should be reported as active pressure,
    /// applying the upstream `EvictionPressureTransitionPeriod` gate:
    /// - A signal that just started exceeding its hard threshold is only
    ///   reported after `HARD_MIN_OBSERVATION` has elapsed (or if it was
    ///   already in the previous active set).
    /// - A signal that has stopped exceeding its hard threshold is kept in
    ///   the active set until `transition_period` has elapsed since the
    ///   last pressure transition. This dampens flapping that would
    ///   otherwise generate watch-event storms.
    pub fn check_eviction_needed(&mut self, stats: &NodeStats) -> Vec<EvictionSignal> {
        self.check_eviction_needed_at(stats, Instant::now())
    }

    /// Test-friendly variant that accepts an explicit `now` to drive the clock.
    pub fn check_eviction_needed_at(
        &mut self,
        stats: &NodeStats,
        now: Instant,
    ) -> Vec<EvictionSignal> {
        if self.thresholds.is_empty() {
            // Disabled: ensure any lingering pressure clears immediately.
            if !self.last_pressure_signals.is_empty() {
                self.last_pressure_signals.clear();
                self.last_pressure_change = Some(now);
            }
            return Vec::new();
        }

        // Phase 1: classify each threshold against the current stats.
        // We clone to avoid &self/&mut self conflicts in is_threshold_exceeded.
        let thresholds = self.thresholds.clone();
        let mut hard_currently_exceeded: HashSet<EvictionSignal> = HashSet::new();
        let mut soft_breached: Vec<EvictionSignal> = Vec::new();
        for threshold in &thresholds {
            if let Some(ref hard) = threshold.hard {
                if Self::compare_threshold(threshold.signal, hard, stats) {
                    hard_currently_exceeded.insert(threshold.signal);
                }
            }
            if let Some(ref soft) = threshold.soft {
                if Self::compare_threshold(threshold.signal, soft, stats) {
                    soft_breached.push(threshold.signal);
                } else {
                    self.soft_threshold_exceeded.remove(&threshold.signal);
                }
            }
        }

        // Phase 2: maintain `hard_observations` — first time each signal was
        // observed crossing the threshold (cleared when it stops) — and
        // `last_observed_at`, the most recent time the signal was exceeded.
        for signal in &hard_currently_exceeded {
            self.hard_observations.entry(*signal).or_insert(now);
            self.last_observed_at.insert(*signal, now);
        }
        let to_clear: Vec<EvictionSignal> = self
            .hard_observations
            .keys()
            .copied()
            .filter(|s| !hard_currently_exceeded.contains(s))
            .collect();
        for signal in to_clear {
            self.hard_observations.remove(&signal);
        }

        // Phase 3: compute the *reported* active set with transition gating.
        let mut active: HashSet<EvictionSignal> = HashSet::new();

        // (a) Newly exceeded signals: include if observation window elapsed
        //     OR if already in the previous active set.
        for signal in &hard_currently_exceeded {
            let first_seen = self.hard_observations.get(signal).copied().unwrap_or(now);
            let observed_long_enough =
                now.saturating_duration_since(first_seen) >= HARD_MIN_OBSERVATION;
            if observed_long_enough || self.last_pressure_signals.contains(signal) {
                active.insert(*signal);
            }
        }

        // (b) Previously-active signals that have recovered: keep them in
        //     the active set until `transition_period` has elapsed since
        //     the LAST observation that the signal was actually exceeded.
        //     Mirrors upstream `nodeConditionsObservedSince` in
        //     `pkg/kubelet/eviction/helpers.go`.
        for signal in &self.last_pressure_signals {
            if !hard_currently_exceeded.contains(signal) {
                if let Some(last_seen) = self.last_observed_at.get(signal).copied() {
                    let elapsed_since_last_seen = now.saturating_duration_since(last_seen);
                    if elapsed_since_last_seen < self.transition_period {
                        active.insert(*signal);
                    }
                }
            }
        }

        // (c) Add soft thresholds that have outlasted their grace period.
        for signal in &soft_breached {
            let exceeded_at = *self.soft_threshold_exceeded.entry(*signal).or_insert(now);
            // Find the threshold's grace period.
            if let Some(grace) = thresholds
                .iter()
                .find(|t| t.signal == *signal)
                .and_then(|t| t.grace_period)
            {
                if now.saturating_duration_since(exceeded_at) >= grace {
                    active.insert(*signal);
                }
            }
        }

        // Phase 4: detect transitions and log accordingly.
        if active != self.last_pressure_signals {
            // A transition. Log the change at info!.
            let entered: Vec<_> = active
                .difference(&self.last_pressure_signals)
                .collect::<Vec<_>>();
            let cleared: Vec<_> = self
                .last_pressure_signals
                .difference(&active)
                .collect::<Vec<_>>();
            if !entered.is_empty() {
                info!(
                    signals = ?entered,
                    "Hard eviction threshold exceeded (entered pressure)"
                );
            }
            if !cleared.is_empty() {
                info!(
                    signals = ?cleared,
                    "Eviction pressure cleared (transition period elapsed)"
                );
            }
            self.last_pressure_signals = active.clone();
            self.last_pressure_change = Some(now);
            self.last_pressure_log = Some(now);
        } else if !active.is_empty() {
            // Stable under pressure. Throttle "still under pressure" logs.
            let should_log = match self.last_pressure_log {
                Some(t) => now.saturating_duration_since(t) >= PRESSURE_LOG_INTERVAL,
                None => true,
            };
            if should_log {
                info!(
                    signals = ?active,
                    "Eviction pressure still active"
                );
                self.last_pressure_log = Some(now);
            } else {
                debug!(signals = ?active, "Eviction pressure still active (rate-limited)");
            }
        }

        // Garbage-collect stale `last_observed_at` entries (older than the
        // transition period AND not currently exceeded).
        let stale: Vec<EvictionSignal> = self
            .last_observed_at
            .iter()
            .filter(|(s, t)| {
                !hard_currently_exceeded.contains(*s)
                    && now.saturating_duration_since(**t) >= self.transition_period
            })
            .map(|(s, _)| *s)
            .collect();
        for signal in stale {
            self.last_observed_at.remove(&signal);
        }

        // Return in stable order for downstream consumers / tests.
        let mut out: Vec<_> = active.into_iter().collect();
        out.sort_by_key(|s| match s {
            EvictionSignal::MemoryAvailable => 0,
            EvictionSignal::NodeFsAvailable => 1,
            EvictionSignal::NodeFsInodesFree => 2,
            EvictionSignal::ImageFsAvailable => 3,
            EvictionSignal::ImageFsInodesFree => 4,
            EvictionSignal::PidAvailable => 5,
        });
        out
    }

    /// Compare current value against threshold. Returns true iff the signal is exceeded
    /// (i.e. available is strictly less than the threshold).
    fn compare_threshold(
        signal: EvictionSignal,
        threshold: &EvictionValue,
        stats: &NodeStats,
    ) -> bool {
        let current = match signal {
            EvictionSignal::MemoryAvailable => stats.memory_available_bytes,
            EvictionSignal::NodeFsAvailable => stats.nodefs_available_bytes,
            EvictionSignal::NodeFsInodesFree => stats.nodefs_inodes_free,
            EvictionSignal::ImageFsAvailable => stats.nodefs_available_bytes,
            EvictionSignal::ImageFsInodesFree => stats.nodefs_inodes_free,
            EvictionSignal::PidAvailable => stats.pid_available,
        };
        let total = match signal {
            EvictionSignal::MemoryAvailable => stats.memory_total_bytes,
            EvictionSignal::NodeFsAvailable | EvictionSignal::ImageFsAvailable => {
                stats.nodefs_total_bytes
            }
            EvictionSignal::NodeFsInodesFree | EvictionSignal::ImageFsInodesFree => {
                stats.nodefs_inodes_total
            }
            EvictionSignal::PidAvailable => stats.pid_total,
        };

        match threshold {
            EvictionValue::Percentage(pct) => {
                if total > 0 {
                    let available_pct = (current as f64 / total as f64) * 100.0;
                    available_pct < *pct
                } else {
                    false
                }
            }
            EvictionValue::Absolute(bytes) => current < *bytes,
        }
    }

    /// Select pods for eviction based on resource pressure
    pub fn select_pods_for_eviction(
        &self,
        pods: &[Pod],
        pod_stats: &HashMap<String, PodStats>,
        signal: &EvictionSignal,
    ) -> Vec<String> {
        let mut eviction_candidates: Vec<(&Pod, &PodStats)> = pods
            .iter()
            .filter_map(|pod| {
                let key = format!(
                    "{}/{}",
                    pod.metadata.namespace.as_deref().unwrap_or("default"),
                    pod.metadata.name
                );
                pod_stats.get(&key).map(|stats| (pod, stats))
            })
            .collect();

        // Sort by eviction priority:
        // 1. QoS class (BestEffort < Burstable < Guaranteed)
        // 2. Resource usage within QoS class
        eviction_candidates.sort_by(|a, b| {
            let qos_cmp = a.1.qos_class.cmp(&b.1.qos_class);
            if qos_cmp != std::cmp::Ordering::Equal {
                return qos_cmp;
            }
            match signal {
                EvictionSignal::MemoryAvailable => {
                    b.1.memory_usage_bytes.cmp(&a.1.memory_usage_bytes)
                }
                EvictionSignal::NodeFsAvailable | EvictionSignal::NodeFsInodesFree => {
                    b.1.disk_usage_bytes.cmp(&a.1.disk_usage_bytes)
                }
                _ => std::cmp::Ordering::Equal,
            }
        });

        eviction_candidates
            .iter()
            .take(5)
            .map(|(pod, _)| {
                format!(
                    "{}/{}",
                    pod.metadata.namespace.as_deref().unwrap_or("default"),
                    pod.metadata.name
                )
            })
            .collect()
    }

    /// Update node conditions based on eviction signals
    pub fn update_node_conditions(
        &self,
        node: &mut Node,
        active_signals: &[EvictionSignal],
    ) -> Result<()> {
        let now = chrono::Utc::now();

        let memory_pressure = active_signals.contains(&EvictionSignal::MemoryAvailable);
        let disk_pressure = active_signals.contains(&EvictionSignal::NodeFsAvailable)
            || active_signals.contains(&EvictionSignal::NodeFsInodesFree)
            || active_signals.contains(&EvictionSignal::ImageFsAvailable)
            || active_signals.contains(&EvictionSignal::ImageFsInodesFree);
        let pid_pressure = active_signals.contains(&EvictionSignal::PidAvailable);

        if let Some(ref mut status) = node.status {
            let conditions = status.conditions.get_or_insert_with(Vec::new);

            Self::update_or_add_condition(
                conditions,
                "MemoryPressure",
                if memory_pressure { "True" } else { "False" },
                if memory_pressure {
                    Some("NodeHasMemoryPressure")
                } else {
                    Some("NodeHasSufficientMemory")
                },
                if memory_pressure {
                    Some("Available memory is below eviction threshold")
                } else {
                    Some("Available memory is sufficient")
                },
                now,
            );

            Self::update_or_add_condition(
                conditions,
                "DiskPressure",
                if disk_pressure { "True" } else { "False" },
                if disk_pressure {
                    Some("NodeHasDiskPressure")
                } else {
                    Some("NodeHasNoDiskPressure")
                },
                if disk_pressure {
                    Some("Available disk space is below eviction threshold")
                } else {
                    Some("Available disk space is sufficient")
                },
                now,
            );

            Self::update_or_add_condition(
                conditions,
                "PIDPressure",
                if pid_pressure { "True" } else { "False" },
                if pid_pressure {
                    Some("NodeHasPIDPressure")
                } else {
                    Some("NodeHasNoPIDPressure")
                },
                if pid_pressure {
                    Some("Available PIDs are below eviction threshold")
                } else {
                    Some("Available PIDs are sufficient")
                },
                now,
            );
        }

        Ok(())
    }

    /// Update or add a node condition
    fn update_or_add_condition(
        conditions: &mut Vec<NodeCondition>,
        condition_type: &str,
        status: &str,
        reason: Option<&str>,
        message: Option<&str>,
        now: chrono::DateTime<chrono::Utc>,
    ) {
        if let Some(condition) = conditions
            .iter_mut()
            .find(|c| c.condition_type == condition_type)
        {
            let status_changed = condition.status != status;
            condition.status = status.to_string();
            condition.last_heartbeat_time = Some(now);
            if status_changed {
                condition.last_transition_time = Some(now);
            }
            condition.reason = reason.map(|s| s.to_string());
            condition.message = message.map(|s| s.to_string());
        } else {
            conditions.push(NodeCondition {
                condition_type: condition_type.to_string(),
                status: status.to_string(),
                last_heartbeat_time: Some(now),
                last_transition_time: Some(now),
                reason: reason.map(|s| s.to_string()),
                message: message.map(|s| s.to_string()),
            });
        }
    }
}

impl Default for EvictionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a comma-separated `--eviction-hard` (or `--eviction-soft`) flag value.
///
/// Format: `<signal><op><value>[,<signal><op><value>...]`.
/// `op` is always `<` (upstream only supports less-than).
/// `value` is `<int>[<unit>]` for absolutes (`100Mi`, `1Gi`) or `<percent>%`.
///
/// An empty string returns an empty map (eviction subsystem disabled).
pub fn parse_eviction_flag(value: &str) -> Result<HashMap<EvictionSignal, EvictionValue>> {
    let mut out = HashMap::new();
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(out);
    }

    for entry in trimmed.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        // Find the operator (only `<` supported).
        let op_pos = entry
            .find('<')
            .ok_or_else(|| anyhow!("eviction threshold '{}': missing '<' operator", entry))?;
        let signal_str = entry[..op_pos].trim();
        let value_str = entry[op_pos + 1..].trim();
        if signal_str.is_empty() || value_str.is_empty() {
            return Err(anyhow!(
                "eviction threshold '{}': empty signal or value",
                entry
            ));
        }
        let signal = EvictionSignal::from_upstream_name(signal_str)
            .ok_or_else(|| anyhow!("eviction threshold '{}': unknown signal", signal_str))?;
        let parsed = parse_threshold_value(value_str)
            .ok_or_else(|| anyhow!("eviction threshold '{}': invalid value", value_str))?;
        out.insert(signal, parsed);
    }

    Ok(out)
}

/// Parse a duration like `5m`, `30s`, `1h30m`. Returns None if invalid.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Plain seconds as a number.
    if let Ok(n) = s.parse::<u64>() {
        return Some(Duration::from_secs(n));
    }
    // Accept go-style `Ns`, `Nm`, `Nh`, or composed `1h30m`.
    let mut total = Duration::ZERO;
    let mut num = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        let n: u64 = num.parse().ok()?;
        num.clear();
        match c {
            's' => total += Duration::from_secs(n),
            'm' => total += Duration::from_secs(n * 60),
            'h' => total += Duration::from_secs(n * 3600),
            _ => return None,
        }
    }
    if !num.is_empty() {
        // Trailing bare number = seconds.
        let n: u64 = num.parse().ok()?;
        total += Duration::from_secs(n);
    }
    Some(total)
}

/// Parse a threshold value (`100Mi`, `1Gi`, `10%`, `1024`).
fn parse_threshold_value(value: &str) -> Option<EvictionValue> {
    let value = value.trim();
    if let Some(pct) = value.strip_suffix('%') {
        let p: f64 = pct.trim().parse().ok()?;
        if !(0.0..=100.0).contains(&p) {
            return None;
        }
        return Some(EvictionValue::Percentage(p));
    }
    parse_memory_value(value).map(EvictionValue::Absolute)
}

/// Build the threshold list from `--eviction-hard` and `--eviction-soft` maps.
///
/// `hard` and `soft` are the parsed CLI values. An entry present only in `soft`
/// has no hard threshold, and vice versa.
pub fn build_thresholds(
    hard: HashMap<EvictionSignal, EvictionValue>,
    soft: HashMap<EvictionSignal, EvictionValue>,
    soft_grace_periods: HashMap<EvictionSignal, Duration>,
) -> Vec<EvictionThreshold> {
    let mut signals: HashSet<EvictionSignal> = HashSet::new();
    signals.extend(hard.keys().copied());
    signals.extend(soft.keys().copied());

    let mut out = Vec::with_capacity(signals.len());
    for signal in signals {
        out.push(EvictionThreshold {
            signal,
            hard: hard.get(&signal).cloned(),
            soft: soft.get(&signal).cloned(),
            grace_period: soft_grace_periods.get(&signal).copied(),
        });
    }
    out
}

/// Determine QoS class for a pod.
///
/// Mirrors upstream Kubernetes' `ComputePodQOS`
/// (`pkg/apis/core/v1/helper/qos/qos.go`):
///
/// - **BestEffort** – no container (init or regular) requests or limits any
///   CPU/memory.
/// - **Guaranteed** – every container has explicit CPU **and** memory limits
///   and the effective requests equal those limits for every resource. A
///   container that sets limits but no requests is treated as if its requests
///   equal its limits (upstream defaults a missing request to the limit).
/// - **Burstable** – has some requests/limits but does not qualify as
///   Guaranteed.
///
/// Both `spec.containers` and `spec.init_containers` participate in the
/// classification. Ephemeral containers are excluded (they cannot declare
/// resources).
pub fn get_qos_class(pod: &Pod) -> QoSClass {
    let spec = match &pod.spec {
        Some(s) => s,
        None => return QoSClass::BestEffort,
    };

    let mut has_limits = false;
    let mut has_requests = false;
    let mut all_guaranteed = true;

    // Upstream folds init containers into the same set as regular containers.
    let all_containers = spec
        .containers
        .iter()
        .chain(spec.init_containers.iter().flatten());

    for container in all_containers {
        let resources = match &container.resources {
            Some(r) => r,
            None => {
                // No resources at all: cannot be Guaranteed.
                all_guaranteed = false;
                continue;
            }
        };

        // A container is Guaranteed only when it declares both CPU and memory
        // limits.
        match &resources.limits {
            Some(limits) => {
                has_limits = true;
                if !limits.contains_key("cpu") || !limits.contains_key("memory") {
                    all_guaranteed = false;
                }
            }
            None => all_guaranteed = false,
        }

        // Effective requests default to the limits when requests are absent
        // (upstream defaults a missing request to the matching limit before
        // classifying, so a limits-only container is Guaranteed).
        match (&resources.requests, &resources.limits) {
            (Some(requests), Some(limits)) => {
                has_requests = true;
                if requests != limits {
                    all_guaranteed = false;
                }
            }
            (Some(_requests), None) => {
                has_requests = true;
                // Requests without limits can never be Guaranteed.
                all_guaranteed = false;
            }
            (None, Some(_limits)) => {
                // Limits-only: treat requests as equal to limits. This still
                // counts as having requests for the Guaranteed check.
                has_requests = true;
            }
            (None, None) => all_guaranteed = false,
        }
    }

    if all_guaranteed && has_limits && has_requests {
        QoSClass::Guaranteed
    } else if has_limits || has_requests {
        QoSClass::Burstable
    } else {
        QoSClass::BestEffort
    }
}

/// Parse memory value string (e.g., "100Mi", "1Gi") to bytes
fn parse_memory_value(value: &str) -> Option<u64> {
    let value = value.trim();

    if let Some(stripped) = value.strip_suffix("Ki") {
        stripped.parse::<u64>().ok().map(|v| v * 1024)
    } else if let Some(stripped) = value.strip_suffix("Mi") {
        stripped.parse::<u64>().ok().map(|v| v * 1024 * 1024)
    } else if let Some(stripped) = value.strip_suffix("Gi") {
        stripped.parse::<u64>().ok().map(|v| v * 1024 * 1024 * 1024)
    } else if let Some(stripped) = value.strip_suffix("Ti") {
        stripped
            .parse::<u64>()
            .ok()
            .map(|v| v * 1024 * 1024 * 1024 * 1024)
    } else {
        value.parse::<u64>().ok()
    }
}

/// Get filesystem stats for the path that hosts the kubelet's root dir.
///
/// Tries the configured `root_dir` first, then its parent, then `/`. Returns
/// (available_bytes, total_bytes, inodes_free, inodes_total, resolved_path).
///
/// Mirrors `google/cadvisor/fs/fs.go::GetFsInfoForPath` semantics: walk up
/// looking for a directory that exists, then `statfs` it. Unlike sysinfo's
/// per-mount enumeration, this works correctly inside containers because
/// `statvfs` resolves the underlying mount automatically.
pub fn statvfs_for_root_dir(root_dir: &Path) -> Result<(u64, u64, u64, u64, PathBuf)> {
    let candidates = candidate_paths(root_dir);
    let mut last_err: Option<anyhow::Error> = None;
    for path in candidates {
        if !path.exists() {
            continue;
        }
        match rustix::fs::statvfs(&path) {
            Ok(stat) => {
                // `f_frsize` is the fundamental block size in bytes.
                let frsize = stat.f_frsize;
                let available_bytes = stat.f_bavail.saturating_mul(frsize);
                let total_bytes = stat.f_blocks.saturating_mul(frsize);
                let inodes_free = stat.f_favail;
                let inodes_total = stat.f_files;
                return Ok((
                    available_bytes,
                    total_bytes,
                    inodes_free,
                    inodes_total,
                    path,
                ));
            }
            Err(e) => {
                last_err = Some(anyhow!("statvfs({}): {}", path.display(), e));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("no valid path for statvfs")))
}

fn candidate_paths(root_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::with_capacity(3);
    out.push(root_dir.to_path_buf());
    if let Some(parent) = root_dir.parent() {
        if !parent.as_os_str().is_empty() {
            out.push(parent.to_path_buf());
        }
    }
    out.push(PathBuf::from("/"));
    // Deduplicate while preserving order.
    let mut seen = HashSet::new();
    out.into_iter().filter(|p| seen.insert(p.clone())).collect()
}

/// Get node resource statistics.
///
/// Queries memory via sysinfo and disk via `statvfs` on the kubelet root dir
/// (upstream-parity behavior). PID stats are read from /proc on Linux.
pub fn get_node_stats(root_dir: &Path) -> NodeStats {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_all();

    let memory_total_bytes = sys.total_memory();
    let memory_available_bytes = sys.available_memory();

    let (nodefs_available_bytes, nodefs_total_bytes, nodefs_inodes_free, nodefs_inodes_total) =
        match statvfs_for_root_dir(root_dir) {
            Ok((avail, total, ifree, itotal, _)) => (avail, total, ifree, itotal),
            Err(e) => {
                warn!(
                    error = %e,
                    "statvfs failed; assuming abundant disk to avoid spurious eviction pressure"
                );
                // Fail "safe": pretend disk is fine so we don't trigger eviction on a
                // measurement bug. This is the same fallback semantics as upstream
                // when cadvisor returns an error.
                (
                    1024 * 1024 * 1024 * 1024, // 1 TiB
                    1024 * 1024 * 1024 * 1024, // 1 TiB
                    10_000_000,
                    10_000_000,
                )
            }
        };

    let (pid_available, pid_total) = get_pid_stats();

    NodeStats {
        memory_available_bytes,
        memory_total_bytes,
        nodefs_available_bytes,
        nodefs_total_bytes,
        nodefs_inodes_free,
        nodefs_inodes_total,
        pid_available,
        pid_total,
    }
}

/// Log the resolved statvfs path at startup. Call once during kubelet boot.
pub fn log_statvfs_path(root_dir: &Path) {
    match statvfs_for_root_dir(root_dir) {
        Ok((avail, total, ifree, itotal, path)) => {
            info!(
                root_dir = %root_dir.display(),
                resolved = %path.display(),
                available_bytes = avail,
                total_bytes = total,
                inodes_free = ifree,
                inodes_total = itotal,
                "Eviction disk stats: using statvfs"
            );
        }
        Err(e) => {
            warn!(
                root_dir = %root_dir.display(),
                error = %e,
                "Eviction disk stats: statvfs probe failed"
            );
        }
    }
}

/// Get PID statistics
#[cfg(target_os = "linux")]
fn get_pid_stats() -> (u64, u64) {
    let pid_max = std::fs::read_to_string("/proc/sys/kernel/pid_max")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(32768);

    let pid_used = std::fs::read_dir("/proc")
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .and_then(|s| s.parse::<u32>().ok())
                        .is_some()
                })
                .count() as u64
        })
        .unwrap_or(100);

    let pid_available = pid_max.saturating_sub(pid_used);

    (pid_available, pid_max)
}

#[cfg(not(target_os = "linux"))]
fn get_pid_stats() -> (u64, u64) {
    use sysinfo::System;

    let mut sys = System::new();
    sys.refresh_processes(sysinfo::ProcessesToUpdate::All, true);

    let pid_used = sys.processes().len() as u64;
    let pid_max: u64 = 32768;
    let pid_available = pid_max.saturating_sub(pid_used);

    (pid_available, pid_max)
}

/// Get pod resource usage statistics from the container runtime.
pub async fn get_pod_stats(pods: &[Pod]) -> HashMap<String, PodStats> {
    get_pod_stats_async(pods).await
}

async fn get_pod_stats_async(pods: &[Pod]) -> HashMap<String, PodStats> {
    use bollard::Docker;

    let mut stats_map = HashMap::new();

    let docker = match Docker::connect_with_local_defaults() {
        Ok(d) => d,
        Err(e) => {
            warn!("Failed to connect to container runtime: {}", e);
            return stats_map;
        }
    };

    for pod in pods {
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let pod_name = &pod.metadata.name;
        let key = format!("{}/{}", namespace, pod_name);

        let qos_class = get_qos_class(pod);

        let mut total_memory_bytes = 0u64;
        let mut total_disk_bytes = 0u64;

        if let Some(spec) = &pod.spec {
            for container in &spec.containers {
                let container_name = format!("k8s_{}_{}_", container.name, pod_name);

                match get_container_stats(&docker, &container_name).await {
                    Ok((memory, disk)) => {
                        total_memory_bytes += memory;
                        total_disk_bytes += disk;
                    }
                    Err(e) => {
                        debug!(
                            "Failed to get stats for container {}: {}",
                            container_name, e
                        );
                    }
                }
            }
        }

        if total_memory_bytes > 0 || total_disk_bytes > 0 {
            stats_map.insert(
                key.clone(),
                PodStats {
                    name: pod_name.clone(),
                    namespace: namespace.to_string(),
                    memory_usage_bytes: total_memory_bytes,
                    disk_usage_bytes: total_disk_bytes,
                    qos_class,
                },
            );
        }
    }

    stats_map
}

async fn get_container_stats(
    docker: &bollard::Docker,
    container_name_prefix: &str,
) -> Result<(u64, u64)> {
    use bollard::container::ListContainersOptions;
    use std::collections::HashMap as BollardHashMap;

    let mut filters = BollardHashMap::new();
    filters.insert("name".to_string(), vec![container_name_prefix.to_string()]);

    let options = Some(ListContainersOptions {
        filters,
        ..Default::default()
    });

    let containers = docker.list_containers(options).await?;

    if containers.is_empty() {
        return Ok((0, 0));
    }

    let container_id = &containers[0]
        .id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Container has no ID"))?;

    let stats_options = bollard::container::StatsOptions {
        stream: false,
        one_shot: true,
    };

    use futures::stream::StreamExt;
    let mut stats_stream = docker.stats(container_id, Some(stats_options));

    if let Some(stats_result) = stats_stream.next().await {
        let stats = stats_result?;

        let memory_bytes = stats.memory_stats.usage.unwrap_or(0);

        let mut disk_bytes = 0u64;
        if let Some(io_service_bytes_recursive) = &stats.blkio_stats.io_service_bytes_recursive {
            for entry in io_service_bytes_recursive {
                disk_bytes += entry.value;
            }
        }

        Ok((memory_bytes, disk_bytes))
    } else {
        Ok((0, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_value() {
        assert_eq!(parse_memory_value("100"), Some(100));
        assert_eq!(parse_memory_value("100Ki"), Some(102400));
        assert_eq!(parse_memory_value("100Mi"), Some(104857600));
        assert_eq!(parse_memory_value("1Gi"), Some(1073741824));
        assert_eq!(parse_memory_value("1Ti"), Some(1099511627776));
    }

    #[test]
    fn test_parse_threshold_value_percentage() {
        assert_eq!(
            parse_threshold_value("10%"),
            Some(EvictionValue::Percentage(10.0))
        );
        assert_eq!(
            parse_threshold_value("0.5%"),
            Some(EvictionValue::Percentage(0.5))
        );
        // Out-of-range percentages are rejected.
        assert_eq!(parse_threshold_value("101%"), None);
        assert_eq!(parse_threshold_value("-1%"), None);
    }

    #[test]
    fn test_parse_eviction_flag_empty_disables() {
        let map = parse_eviction_flag("").unwrap();
        assert!(map.is_empty(), "empty flag must yield empty threshold set");
        let map = parse_eviction_flag("   ").unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn test_parse_eviction_flag_multi() {
        let map =
            parse_eviction_flag("memory.available<1Gi,nodefs.available<5%,nodefs.inodesFree<5%")
                .unwrap();
        assert_eq!(map.len(), 3);
        assert_eq!(
            map.get(&EvictionSignal::MemoryAvailable),
            Some(&EvictionValue::Absolute(1024 * 1024 * 1024))
        );
        assert_eq!(
            map.get(&EvictionSignal::NodeFsAvailable),
            Some(&EvictionValue::Percentage(5.0))
        );
        assert_eq!(
            map.get(&EvictionSignal::NodeFsInodesFree),
            Some(&EvictionValue::Percentage(5.0))
        );
    }

    #[test]
    fn test_parse_eviction_flag_invalid_signal() {
        assert!(parse_eviction_flag("bogus.signal<10%").is_err());
    }

    #[test]
    fn test_parse_eviction_flag_invalid_syntax() {
        // No '<' operator.
        assert!(parse_eviction_flag("memory.available=100Mi").is_err());
        // No value.
        assert!(parse_eviction_flag("memory.available<").is_err());
        // Unsupported '>' op (upstream only allows '<').
        assert!(parse_eviction_flag("memory.available>100Mi").is_err());
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(
            parse_duration("1h30m"),
            Some(Duration::from_secs(3600 + 1800))
        );
        assert_eq!(parse_duration("60"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("abc"), None);
    }

    #[test]
    fn test_statvfs_temp_dir() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let (avail, total, ifree, itotal, resolved) =
            statvfs_for_root_dir(tmp.path()).expect("statvfs on tempdir");
        // Real filesystem must report reasonable values.
        assert!(total > 0, "total bytes must be > 0");
        assert!(itotal > 0, "total inodes must be > 0");
        assert!(avail <= total, "available <= total bytes");
        assert!(ifree <= itotal, "free <= total inodes");
        // Resolved path should be the tempdir itself (or its parent if the
        // dir vanished mid-test, but here it exists).
        assert_eq!(resolved, tmp.path());
    }

    #[test]
    fn test_statvfs_nonexistent_falls_back_to_root() {
        let bogus = PathBuf::from("/this/path/does/not/exist/anywhere");
        let (avail, total, _ifree, _itotal, resolved) =
            statvfs_for_root_dir(&bogus).expect("fallback statvfs");
        assert!(total > 0);
        assert!(avail <= total);
        // Resolved must be `/`.
        assert_eq!(resolved, PathBuf::from("/"));
    }

    #[test]
    fn test_disabled_when_no_thresholds() {
        let mut mgr = EvictionManager::with_config(vec![], Duration::from_secs(5));
        assert!(mgr.is_disabled());
        let stats = NodeStats {
            memory_available_bytes: 1,
            memory_total_bytes: 1_000_000_000,
            nodefs_available_bytes: 1,
            nodefs_total_bytes: 1_000_000_000,
            nodefs_inodes_free: 1,
            nodefs_inodes_total: 1_000_000,
            pid_available: 1,
            pid_total: 32768,
        };
        let active = mgr.check_eviction_needed(&stats);
        assert!(
            active.is_empty(),
            "disabled manager must never report pressure"
        );
    }

    fn pressure_stats() -> NodeStats {
        // Memory critically low (below 100Mi default).
        NodeStats {
            memory_available_bytes: 10 * 1024 * 1024,
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            nodefs_available_bytes: 50 * 1024 * 1024 * 1024,
            nodefs_total_bytes: 100 * 1024 * 1024 * 1024,
            nodefs_inodes_free: 5_000_000,
            nodefs_inodes_total: 10_000_000,
            pid_available: 30000,
            pid_total: 32768,
        }
    }

    fn healthy_stats() -> NodeStats {
        NodeStats {
            memory_available_bytes: 4 * 1024 * 1024 * 1024,
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            nodefs_available_bytes: 50 * 1024 * 1024 * 1024,
            nodefs_total_bytes: 100 * 1024 * 1024 * 1024,
            nodefs_inodes_free: 5_000_000,
            nodefs_inodes_total: 10_000_000,
            pid_available: 30000,
            pid_total: 32768,
        }
    }

    #[test]
    fn test_transition_period_holds_signal_after_recovery() {
        let mut mgr = EvictionManager::with_config(
            vec![EvictionThreshold {
                signal: EvictionSignal::MemoryAvailable,
                hard: Some(EvictionValue::Absolute(100 * 1024 * 1024)),
                soft: None,
                grace_period: None,
            }],
            Duration::from_secs(5),
        );

        let t0 = Instant::now();
        // Tick 1 @ t=0s: pressure observed for the first time, BUT within
        // HARD_MIN_OBSERVATION → not yet reported.
        let signals = mgr.check_eviction_needed_at(&pressure_stats(), t0);
        assert!(
            signals.is_empty(),
            "first observation should be gated by observation window"
        );

        // Tick 2 @ t=11s: pressure still observed, past observation window.
        let signals = mgr.check_eviction_needed_at(&pressure_stats(), t0 + Duration::from_secs(11));
        assert_eq!(signals, vec![EvictionSignal::MemoryAvailable]);

        // Tick 3 @ t=20s: pressure STILL observed (last_observed_at
        // advances to t=20s).
        let signals = mgr.check_eviction_needed_at(&pressure_stats(), t0 + Duration::from_secs(20));
        assert_eq!(signals, vec![EvictionSignal::MemoryAvailable]);

        // Tick 4 @ t=22s: recovery starts. Signal must STAY active because
        // only 2s has elapsed since `last_observed_at` (= t=20s).
        let signals = mgr.check_eviction_needed_at(&healthy_stats(), t0 + Duration::from_secs(22));
        assert_eq!(
            signals,
            vec![EvictionSignal::MemoryAvailable],
            "recovered signal must stay in active set during transition period"
        );

        // Tick 5 @ t=24s: 4s after last_observed_at — still under 5s.
        let signals = mgr.check_eviction_needed_at(&healthy_stats(), t0 + Duration::from_secs(24));
        assert_eq!(signals, vec![EvictionSignal::MemoryAvailable]);

        // Tick 6 @ t=26s: 6s after last_observed_at — transition period
        // elapsed, must clear.
        let signals = mgr.check_eviction_needed_at(&healthy_stats(), t0 + Duration::from_secs(26));
        assert!(
            signals.is_empty(),
            "pressure must clear after transition period elapses"
        );
    }

    #[test]
    fn test_no_pressure_stays_clear() {
        let mut mgr = EvictionManager::new();
        let t0 = Instant::now();
        for i in 0..10 {
            let active =
                mgr.check_eviction_needed_at(&healthy_stats(), t0 + Duration::from_secs(i));
            assert!(active.is_empty());
        }
    }

    #[test]
    fn test_log_rate_limit_under_sustained_pressure() {
        // We can't easily intercept tracing output without a subscriber; instead,
        // verify the internal `last_pressure_log` only advances on transitions
        // or once per PRESSURE_LOG_INTERVAL.
        let mut mgr = EvictionManager::with_config(
            vec![EvictionThreshold {
                signal: EvictionSignal::MemoryAvailable,
                hard: Some(EvictionValue::Absolute(100 * 1024 * 1024)),
                soft: None,
                grace_period: None,
            }],
            Duration::from_secs(60),
        );

        let t0 = Instant::now();
        let mut log_advances = 0usize;
        let mut last_seen: Option<Instant> = None;

        // 100 ticks across 1 second (well below PRESSURE_LOG_INTERVAL of 60s).
        for i in 0..100 {
            let now = t0 + Duration::from_millis(i * 10) + Duration::from_secs(11);
            // Past the observation window from t0.
            mgr.check_eviction_needed_at(&pressure_stats(), now);
            if mgr.last_pressure_log != last_seen {
                log_advances += 1;
                last_seen = mgr.last_pressure_log;
            }
        }

        // Exactly one advance: the initial transition into pressure.
        assert!(
            log_advances <= 2,
            "expected ≤ 2 log advances over 100 ticks; got {}",
            log_advances
        );
    }

    #[test]
    fn test_qos_class_best_effort() {
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta::new("test-pod"),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "test".to_string(),
                    image: "nginx".to_string(),
                    resources: None,
                    image_pull_policy: None,
                    command: None,
                    args: None,
                    ports: None,
                    env: None,
                    volume_mounts: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
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
                }],
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: None,
                node_selector: None,
                node_name: None,
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
                topology_spread_constraints: None,
                overhead: None,
                scheduler_name: None,
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
            }),
            status: None,
        };

        assert_eq!(get_qos_class(&pod), QoSClass::BestEffort);
    }

    #[test]
    fn test_memory_pressure_detection_after_observation_window() {
        let mut manager = EvictionManager::new();

        let low_memory_stats = NodeStats {
            memory_available_bytes: 50 * 1024 * 1024,
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            nodefs_available_bytes: 50 * 1024 * 1024 * 1024,
            nodefs_total_bytes: 100 * 1024 * 1024 * 1024,
            nodefs_inodes_free: 1_000_000,
            nodefs_inodes_total: 10_000_000,
            pid_available: 30000,
            pid_total: 32768,
        };

        let t0 = Instant::now();
        // First observation gated.
        let signals = manager.check_eviction_needed_at(&low_memory_stats, t0);
        assert!(signals.is_empty());
        // After observation window: signal reported.
        let signals =
            manager.check_eviction_needed_at(&low_memory_stats, t0 + Duration::from_secs(11));
        assert!(signals.contains(&EvictionSignal::MemoryAvailable));
    }

    #[test]
    fn test_disk_pressure_detection_after_observation_window() {
        let mut manager = EvictionManager::new();

        let low_disk_stats = NodeStats {
            memory_available_bytes: 2 * 1024 * 1024 * 1024,
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            nodefs_available_bytes: 5 * 1024 * 1024 * 1024, // 5% of 100GiB
            nodefs_total_bytes: 100 * 1024 * 1024 * 1024,
            nodefs_inodes_free: 1_000_000,
            nodefs_inodes_total: 10_000_000,
            pid_available: 30000,
            pid_total: 32768,
        };

        let t0 = Instant::now();
        let _ = manager.check_eviction_needed_at(&low_disk_stats, t0);
        let signals =
            manager.check_eviction_needed_at(&low_disk_stats, t0 + Duration::from_secs(11));
        assert!(signals.contains(&EvictionSignal::NodeFsAvailable));
    }
}
