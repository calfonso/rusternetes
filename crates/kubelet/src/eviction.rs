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

use anyhow::Result;
use rusternetes_common::resources::{Node, NodeCondition, Pod};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Eviction signals that can trigger pod eviction
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    pub grace_period: Option<std::time::Duration>,
}

/// Eviction threshold value (percentage or absolute)
#[derive(Debug, Clone)]
pub enum EvictionValue {
    /// Percentage threshold (e.g., 85% means evict when less than 15% available)
    Percentage(f64),
    /// Absolute value threshold (e.g., "1Gi" for memory, "5%" for disk)
    Absolute(String),
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

/// Eviction manager for handling out-of-resource situations
pub struct EvictionManager {
    /// Eviction thresholds configuration
    pub thresholds: Vec<EvictionThreshold>,
    /// Last time soft thresholds were exceeded (for grace period tracking)
    soft_threshold_exceeded: HashMap<EvictionSignal, std::time::Instant>,
}

impl EvictionManager {
    /// Create a new eviction manager with default thresholds
    pub fn new() -> Self {
        let thresholds = vec![
            // Memory pressure: evict when less than 100Mi available
            EvictionThreshold {
                signal: EvictionSignal::MemoryAvailable,
                hard: Some(EvictionValue::Absolute("100Mi".to_string())),
                soft: Some(EvictionValue::Absolute("500Mi".to_string())),
                grace_period: Some(std::time::Duration::from_secs(90)),
            },
            // Disk pressure (nodefs): evict when less than 10% available
            EvictionThreshold {
                signal: EvictionSignal::NodeFsAvailable,
                hard: Some(EvictionValue::Percentage(10.0)),
                soft: Some(EvictionValue::Percentage(15.0)),
                grace_period: Some(std::time::Duration::from_secs(120)),
            },
            // Inode pressure: evict when less than 5% inodes free
            EvictionThreshold {
                signal: EvictionSignal::NodeFsInodesFree,
                hard: Some(EvictionValue::Percentage(5.0)),
                soft: Some(EvictionValue::Percentage(10.0)),
                grace_period: Some(std::time::Duration::from_secs(120)),
            },
        ];

        Self {
            thresholds,
            soft_threshold_exceeded: HashMap::new(),
        }
    }

    /// Check if eviction is needed based on current node statistics
    pub fn check_eviction_needed(&mut self, stats: &NodeStats) -> Vec<EvictionSignal> {
        let mut signals = Vec::new();

        // Clone thresholds to avoid borrow checker issues
        let thresholds = self.thresholds.clone();
        for threshold in &thresholds {
            if self.is_threshold_exceeded(threshold, stats) {
                signals.push(threshold.signal.clone());
            }
        }

        signals
    }

    /// Check if a threshold is exceeded
    fn is_threshold_exceeded(&mut self, threshold: &EvictionThreshold, stats: &NodeStats) -> bool {
        let current_value = self.get_current_value(&threshold.signal, stats);

        // Check hard threshold first
        if let Some(ref hard) = threshold.hard {
            if self.compare_threshold(&threshold.signal, current_value, hard, stats) {
                info!(
                    "Hard eviction threshold exceeded for {:?}",
                    threshold.signal
                );
                return true;
            }
        }

        // Check soft threshold with grace period
        if let Some(ref soft) = threshold.soft {
            if self.compare_threshold(&threshold.signal, current_value, soft, stats) {
                let now = std::time::Instant::now();
                let exceeded_time = self
                    .soft_threshold_exceeded
                    .entry(threshold.signal.clone())
                    .or_insert(now);

                if let Some(grace_period) = threshold.grace_period {
                    if now.duration_since(*exceeded_time) >= grace_period {
                        warn!(
                            "Soft eviction threshold exceeded for {:?} past grace period",
                            threshold.signal
                        );
                        return true;
                    } else {
                        debug!(
                            "Soft threshold exceeded for {:?}, within grace period",
                            threshold.signal
                        );
                    }
                }
            } else {
                // Reset soft threshold timer if condition cleared
                self.soft_threshold_exceeded.remove(&threshold.signal);
            }
        }

        false
    }

    /// Get current value for an eviction signal
    fn get_current_value(&self, signal: &EvictionSignal, stats: &NodeStats) -> f64 {
        match signal {
            EvictionSignal::MemoryAvailable => stats.memory_available_bytes as f64,
            EvictionSignal::NodeFsAvailable => stats.nodefs_available_bytes as f64,
            EvictionSignal::NodeFsInodesFree => stats.nodefs_inodes_free as f64,
            EvictionSignal::ImageFsAvailable => 0.0, // Not implemented
            EvictionSignal::ImageFsInodesFree => 0.0, // Not implemented
            EvictionSignal::PidAvailable => stats.pid_available as f64,
        }
    }

    /// Compare current value against threshold
    fn compare_threshold(
        &self,
        signal: &EvictionSignal,
        current: f64,
        threshold: &EvictionValue,
        stats: &NodeStats,
    ) -> bool {
        match threshold {
            EvictionValue::Percentage(pct) => {
                let total = match signal {
                    EvictionSignal::MemoryAvailable => stats.memory_total_bytes as f64,
                    EvictionSignal::NodeFsAvailable => stats.nodefs_total_bytes as f64,
                    EvictionSignal::NodeFsInodesFree => stats.nodefs_inodes_total as f64,
                    EvictionSignal::PidAvailable => stats.pid_total as f64,
                    _ => 0.0,
                };

                if total > 0.0 {
                    let available_pct = (current / total) * 100.0;
                    available_pct < *pct
                } else {
                    false
                }
            }
            EvictionValue::Absolute(value) => {
                // Parse absolute value (e.g., "100Mi", "1Gi")
                let threshold_bytes = parse_memory_value(value).unwrap_or(0);
                current < threshold_bytes as f64
            }
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
            // First compare QoS class (lower QoS gets evicted first)
            let qos_cmp = a.1.qos_class.cmp(&b.1.qos_class);
            if qos_cmp != std::cmp::Ordering::Equal {
                return qos_cmp;
            }

            // Within same QoS class, sort by resource usage
            match signal {
                EvictionSignal::MemoryAvailable => {
                    // Evict pods using more memory first
                    b.1.memory_usage_bytes.cmp(&a.1.memory_usage_bytes)
                }
                EvictionSignal::NodeFsAvailable | EvictionSignal::NodeFsInodesFree => {
                    // Evict pods using more disk first
                    b.1.disk_usage_bytes.cmp(&a.1.disk_usage_bytes)
                }
                _ => std::cmp::Ordering::Equal,
            }
        });

        // Return pod keys for eviction (start with lowest priority)
        eviction_candidates
            .iter()
            .take(5) // Evict at most 5 pods per iteration
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

        // Determine which conditions should be set
        let memory_pressure = active_signals.contains(&EvictionSignal::MemoryAvailable);
        let disk_pressure = active_signals.contains(&EvictionSignal::NodeFsAvailable)
            || active_signals.contains(&EvictionSignal::NodeFsInodesFree);
        let pid_pressure = active_signals.contains(&EvictionSignal::PidAvailable);

        // Update or add conditions
        if let Some(ref mut status) = node.status {
            let conditions = status.conditions.get_or_insert_with(Vec::new);

            // Update MemoryPressure condition
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

            // Update DiskPressure condition
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

            // Update PIDPressure condition
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
            // Update existing condition
            let status_changed = condition.status != status;
            condition.status = status.to_string();
            condition.last_heartbeat_time = Some(now);
            if status_changed {
                condition.last_transition_time = Some(now);
            }
            condition.reason = reason.map(|s| s.to_string());
            condition.message = message.map(|s| s.to_string());
        } else {
            // Add new condition
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

/// Determine QoS class for a pod
pub fn get_qos_class(pod: &Pod) -> QoSClass {
    let spec = match &pod.spec {
        Some(s) => s,
        None => return QoSClass::BestEffort,
    };

    let mut has_limits = false;
    let mut has_requests = false;
    let mut all_guaranteed = true;

    for container in &spec.containers {
        if let Some(ref resources) = container.resources {
            if let Some(ref limits) = resources.limits {
                has_limits = true;
                // Check if limits are set for CPU and memory
                if !limits.contains_key("cpu") || !limits.contains_key("memory") {
                    all_guaranteed = false;
                }
            } else {
                all_guaranteed = false;
            }

            if let Some(ref requests) = resources.requests {
                has_requests = true;
                // Check if requests == limits for guaranteed
                if let Some(ref limits) = resources.limits {
                    if requests != limits {
                        all_guaranteed = false;
                    }
                } else {
                    all_guaranteed = false;
                }
            } else {
                all_guaranteed = false;
            }
        } else {
            all_guaranteed = false;
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

/// Get node resource statistics
/// Queries actual system resources using sysinfo crate
pub fn get_node_stats() -> NodeStats {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_all();

    // Get memory stats
    let memory_total_bytes = sys.total_memory();
    let memory_available_bytes = sys.available_memory();

    // Get disk stats for root filesystem
    let disks = sysinfo::Disks::new_with_refreshed_list();
    let (nodefs_available_bytes, nodefs_total_bytes) = if let Some(root_disk) = disks
        .iter()
        .find(|d| d.mount_point().to_str() == Some("/"))
        .or_else(|| disks.iter().next())
    {
        (root_disk.available_space(), root_disk.total_space())
    } else {
        // Fallback if no disk found
        (100 * 1024 * 1024 * 1024, 200 * 1024 * 1024 * 1024)
    };

    // Inode stats: not directly available from sysinfo, use estimates
    // On most filesystems, 1 inode per ~16KB is common
    let estimated_inodes_total = nodefs_total_bytes / 16384;
    let estimated_inodes_free = nodefs_available_bytes / 16384;

    // PID stats: read from system or estimate
    let (pid_available, pid_total) = get_pid_stats();

    NodeStats {
        memory_available_bytes,
        memory_total_bytes,
        nodefs_available_bytes,
        nodefs_total_bytes,
        nodefs_inodes_free: estimated_inodes_free,
        nodefs_inodes_total: estimated_inodes_total,
        pid_available,
        pid_total,
    }
}

/// Get PID statistics
#[cfg(target_os = "linux")]
fn get_pid_stats() -> (u64, u64) {
    // Read from /proc/sys/kernel/pid_max
    let pid_max = std::fs::read_to_string("/proc/sys/kernel/pid_max")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(32768);

    // Count running processes
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

/// Get PID statistics (macOS/other)
#[cfg(not(target_os = "linux"))]
fn get_pid_stats() -> (u64, u64) {
    use sysinfo::System;

    let mut sys = System::new();
    sys.refresh_processes(sysinfo::ProcessesToUpdate::All, true);

    let pid_used = sys.processes().len() as u64;
    let pid_max: u64 = 32768; // Default limit on macOS and other systems
    let pid_available = pid_max.saturating_sub(pid_used);

    (pid_available, pid_max)
}

/// Get pod resource usage statistics
/// Queries the container runtime for actual resource usage
pub async fn get_pod_stats(pods: &[Pod]) -> HashMap<String, PodStats> {
    get_pod_stats_async(pods).await
}

/// Async implementation of pod stats gathering
async fn get_pod_stats_async(pods: &[Pod]) -> HashMap<String, PodStats> {
    use bollard::Docker;

    let mut stats_map = HashMap::new();

    // Connect to Docker/Podman
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

        // Get QoS class for this pod
        let qos_class = get_qos_class(pod);

        // Aggregate resource usage across all containers in the pod
        let mut total_memory_bytes = 0u64;
        let mut total_disk_bytes = 0u64;

        if let Some(spec) = &pod.spec {
            for container in &spec.containers {
                // Container name in runtime format: k8s_<container>_<pod>_<namespace>_<uid>_<attempt>
                // For simplicity, we'll try to find containers by pod name prefix
                let container_name = format!("k8s_{}_{}_", container.name, pod_name);

                // Try to get container stats
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

        // Only add to map if we got some stats
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

/// Get resource stats for a single container
async fn get_container_stats(
    docker: &bollard::Docker,
    container_name_prefix: &str,
) -> Result<(u64, u64)> {
    use bollard::container::ListContainersOptions;
    use std::collections::HashMap as BollardHashMap;

    // List all containers and find one matching our prefix
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

    // Get the first matching container
    let container_id = &containers[0]
        .id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Container has no ID"))?;

    // Get container stats (one-shot, not streaming)
    let stats_options = bollard::container::StatsOptions {
        stream: false,
        one_shot: true,
    };

    use futures::stream::StreamExt;
    let mut stats_stream = docker.stats(container_id, Some(stats_options));

    if let Some(stats_result) = stats_stream.next().await {
        let stats = stats_result?;

        // Extract memory usage
        let memory_bytes = stats.memory_stats.usage.unwrap_or(0);

        // Extract disk usage (from blkio stats)
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
    fn test_memory_pressure_detection() {
        let mut manager = EvictionManager::new();

        // Low memory scenario
        let low_memory_stats = NodeStats {
            memory_available_bytes: 50 * 1024 * 1024, // 50 MiB (below 100Mi threshold)
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            nodefs_available_bytes: 50 * 1024 * 1024 * 1024,
            nodefs_total_bytes: 100 * 1024 * 1024 * 1024,
            nodefs_inodes_free: 1_000_000,
            nodefs_inodes_total: 10_000_000,
            pid_available: 30000,
            pid_total: 32768,
        };

        let signals = manager.check_eviction_needed(&low_memory_stats);
        assert!(signals.contains(&EvictionSignal::MemoryAvailable));
    }

    #[test]
    fn test_disk_pressure_detection() {
        let mut manager = EvictionManager::new();

        // Low disk scenario (5% available = below 10% threshold)
        let low_disk_stats = NodeStats {
            memory_available_bytes: 2 * 1024 * 1024 * 1024,
            memory_total_bytes: 8 * 1024 * 1024 * 1024,
            nodefs_available_bytes: 5 * 1024 * 1024 * 1024, // 5 GiB available
            nodefs_total_bytes: 100 * 1024 * 1024 * 1024,   // 100 GiB total (5%)
            nodefs_inodes_free: 1_000_000,
            nodefs_inodes_total: 10_000_000,
            pid_available: 30000,
            pid_total: 32768,
        };

        let signals = manager.check_eviction_needed(&low_disk_stats);
        assert!(signals.contains(&EvictionSignal::NodeFsAvailable));
    }
}
