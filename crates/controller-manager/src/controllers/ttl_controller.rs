// TTL Controller - Automatic cleanup of completed Jobs based on TTL
//
// Implements:
// - TTL after finished for Jobs
// - Automatic deletion of finished Jobs after specified time
// - Cleanup of associated Pods

use chrono::{DateTime, Duration, Utc};
use futures::StreamExt;
use rusternetes_common::resources::workloads::Job;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use tokio::time::{sleep, Duration as TokioDuration};
use tracing::{debug, error, info, warn};

/// TTL Controller for automatic cleanup of finished Jobs
#[allow(dead_code)]
pub struct TTLController<S: Storage> {
    storage: Arc<S>,
    /// How often to check for expired Jobs
    check_interval: TokioDuration,
}

impl<S: Storage + 'static> TTLController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            check_interval: TokioDuration::from_secs(60), // Check every 60 seconds
        }
    }

    /// Watch-based run loop. Watches jobs as primary resource.
    /// Falls back to periodic resync every 30s.
    pub async fn run(self: Arc<Self>) {
        info!("Starting TTL Controller");

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("jobs", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    sleep(self.check_interval).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }

    /// Check all Jobs and cleanup expired ones
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (Some(parts[1]), parts[2]),
                2 => (None, parts[1]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("jobs", ns, name);
            match self.storage.get::<Job>(&storage_key).await {
                Ok(job) => match self.check_job_ttl(&job).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        error!("TTL check failed for {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self.storage.list::<Job>("/registry/jobs/").await {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("jobs/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list jobs for enqueue: {}", e);
            }
        }
    }

    /// Check a single Job's TTL and clean up if expired.
    async fn check_job_ttl(&self, job: &Job) -> rusternetes_common::Result<()> {
        let now = Utc::now();
        if let Some(ttl_seconds) = self.get_ttl_seconds_after_finished(job) {
            if self.should_cleanup(job, ttl_seconds, now).await {
                self.cleanup_job(job).await?;
                info!(
                    "TTL cleanup: deleted job {}/{}",
                    job.metadata.namespace.as_deref().unwrap_or("default"),
                    job.metadata.name
                );
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn check_and_cleanup(&self) -> rusternetes_common::Result<()> {
        debug!("Checking for expired Jobs");

        // List all Jobs across all namespaces
        let prefix = build_prefix("jobs", None);
        let jobs: Vec<Job> = self.storage.list(&prefix).await?;

        let now = Utc::now();
        let mut cleaned_count = 0;

        for job in jobs {
            if let Some(ttl_seconds) = self.get_ttl_seconds_after_finished(&job) {
                if self.should_cleanup(&job, ttl_seconds, now).await {
                    if let Err(e) = self.cleanup_job(&job).await {
                        error!(
                            "Failed to cleanup job {}/{}: {}",
                            job.metadata.namespace.as_deref().unwrap_or("default"),
                            job.metadata.name,
                            e
                        );
                    } else {
                        cleaned_count += 1;
                    }
                }
            }
        }

        if cleaned_count > 0 {
            info!("Cleaned up {} expired Jobs", cleaned_count);
        }

        Ok(())
    }

    /// Get the effective `ttlSecondsAfterFinished` for a Job.
    ///
    /// Upstream's ttl-after-finished controller reads the typed
    /// `JobSpec.TTLSecondsAfterFinished` field
    /// (`pkg/controller/ttlafterfinished`: `needsCleanup` /
    /// `getFinishAndExpireTime`). We prefer that field, falling back to the
    /// legacy `ttlSecondsAfterFinished` annotation for objects that only carry
    /// it there.
    pub fn get_ttl_seconds_after_finished(&self, job: &Job) -> Option<i32> {
        if let Some(ttl) = job.spec.ttl_seconds_after_finished {
            return Some(ttl);
        }
        job.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("ttlSecondsAfterFinished"))
            .and_then(|v| v.parse().ok())
    }

    /// Check if a Job should be cleaned up
    async fn should_cleanup(&self, job: &Job, ttl_seconds: i32, now: DateTime<Utc>) -> bool {
        // Job must be finished (Complete or Failed)
        if !self.is_job_finished(job) {
            return false;
        }

        // Get the finish time
        if let Some(finish_time) = self.get_job_finish_time(job) {
            let ttl_duration = Duration::seconds(ttl_seconds as i64);
            let expiry_time = finish_time + ttl_duration;

            if now >= expiry_time {
                info!(
                    "Job {}/{} has exceeded TTL ({} seconds after finish at {})",
                    job.metadata.namespace.as_deref().unwrap_or("default"),
                    job.metadata.name,
                    ttl_seconds,
                    finish_time
                );
                return true;
            }
        }

        false
    }

    /// Check if a Job is finished
    fn is_job_finished(&self, job: &Job) -> bool {
        if let Some(status) = &job.status {
            if let Some(conditions) = &status.conditions {
                return conditions.iter().any(|c| {
                    matches!(c.condition_type.as_str(), "Complete" | "Failed") && c.status == "True"
                });
            }
        }
        false
    }

    /// Get the finish time of a Job
    fn get_job_finish_time(&self, job: &Job) -> Option<DateTime<Utc>> {
        if let Some(status) = &job.status {
            if let Some(conditions) = &status.conditions {
                // Find the Complete or Failed condition
                for condition in conditions {
                    if matches!(condition.condition_type.as_str(), "Complete" | "Failed")
                        && condition.status == "True"
                    {
                        return condition.last_transition_time;
                    }
                }
            }
        }
        None
    }

    /// Cleanup a Job and its associated Pods
    async fn cleanup_job(&self, job: &Job) -> rusternetes_common::Result<()> {
        let namespace = job.metadata.namespace.as_deref().unwrap_or("default");
        let name = &job.metadata.name;

        info!("Cleaning up Job: {}/{}", namespace, name);

        // Delete associated Pods first (cascade deletion)
        if let Err(e) = self.delete_job_pods(namespace, &job.metadata.uid).await {
            warn!(
                "Failed to delete pods for job {}/{}: {}",
                namespace, name, e
            );
        }

        // Delete the Job itself
        let key = build_key("jobs", Some(namespace), name);
        self.storage.delete(&key).await?;

        info!("Successfully cleaned up Job: {}/{}", namespace, name);
        Ok(())
    }

    /// Delete all Pods owned by a Job
    async fn delete_job_pods(
        &self,
        namespace: &str,
        job_uid: &str,
    ) -> rusternetes_common::Result<()> {
        use rusternetes_common::resources::Pod;

        let prefix = build_prefix("pods", Some(namespace));
        let pods: Vec<Pod> = self.storage.list(&prefix).await?;

        for pod in pods {
            // Check if this Pod is owned by the Job
            if let Some(owner_refs) = &pod.metadata.owner_references {
                if owner_refs.iter().any(|o| o.uid == job_uid) {
                    let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                    if let Err(e) = self.storage.delete(&pod_key).await {
                        warn!("Failed to delete pod {}: {}", pod.metadata.name, e);
                    } else {
                        debug!("Deleted pod {}", pod.metadata.name);
                    }
                }
            }
        }

        Ok(())
    }
}

/// Job spec extension for TTL
#[allow(dead_code)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobSpecWithTTL {
    /// Duration in seconds the job can be active before the system tries to terminate it
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_deadline_seconds: Option<i64>,

    /// Clean up finished Jobs (complete or failed) after this time (in seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_seconds_after_finished: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rusternetes_common::resources::workloads::{
        JobCondition, JobSpec, JobStatus, PodTemplateSpec,
    };
    use rusternetes_common::types::ObjectMeta;
    use std::collections::HashMap;

    fn create_test_job(name: &str, namespace: &str, ttl_seconds: i32) -> Job {
        let mut annotations = HashMap::new();
        annotations.insert(
            "ttlSecondsAfterFinished".to_string(),
            ttl_seconds.to_string(),
        );

        Job {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Job".to_string(),
                api_version: "batch/v1".to_string(),
            },
            metadata: ObjectMeta::new(name)
                .with_namespace(namespace)
                .with_labels(HashMap::new())
                .with_annotations(annotations),
            spec: JobSpec {
                template: PodTemplateSpec {
                    metadata: None,
                    spec: rusternetes_common::resources::pod::PodSpec {
                        containers: vec![],
                        init_containers: None,
                        volumes: None,
                        restart_policy: Some("Never".to_string()),
                        node_name: None,
                        node_selector: None,
                        service_account_name: None,
                        service_account: None,
                        hostname: None,
                        subdomain: None,
                        host_network: None,
                        host_pid: None,
                        host_ipc: None,
                        affinity: None,
                        tolerations: None,
                        priority: None,
                        priority_class_name: None,
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
                    },
                },
                completions: Some(1),
                parallelism: Some(1),
                backoff_limit: Some(3),
                active_deadline_seconds: None,
                selector: None,
                manual_selector: None,
                suspend: None,
                ttl_seconds_after_finished: None,
                completion_mode: None,
                backoff_limit_per_index: None,
                max_failed_indexes: None,
                pod_failure_policy: None,
                pod_replacement_policy: None,
                success_policy: None,
                managed_by: None,
            },
            status: Some(JobStatus {
                active: Some(0),
                succeeded: Some(1),
                failed: Some(0),
                conditions: Some(vec![JobCondition {
                    condition_type: "Complete".to_string(),
                    status: "True".to_string(),
                    last_probe_time: Some(Utc::now()),
                    last_transition_time: Some(Utc::now() - Duration::seconds(120)),
                    reason: Some("JobComplete".to_string()),
                    message: Some("Job completed successfully".to_string()),
                }]),
                start_time: None,
                completion_time: None,
                ready: None,
                terminating: None,
                completed_indexes: None,
                failed_indexes: None,
                uncounted_terminated_pods: None,
                observed_generation: None,
            }),
        }
    }

    #[tokio::test]
    async fn test_ttl_controller_identifies_finished_job() {
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let controller =
            TTLController::<rusternetes_storage::memory::MemoryStorage>::new(storage.clone());

        let job = create_test_job("test-job", "default", 60);
        assert!(controller.is_job_finished(&job));
    }

    #[tokio::test]
    async fn test_ttl_controller_gets_finish_time() {
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let controller =
            TTLController::<rusternetes_storage::memory::MemoryStorage>::new(storage.clone());

        let job = create_test_job("test-job", "default", 60);
        let finish_time = controller.get_job_finish_time(&job);
        assert!(finish_time.is_some());
    }

    #[tokio::test]
    async fn test_ttl_seconds_parsing() {
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let controller =
            TTLController::<rusternetes_storage::memory::MemoryStorage>::new(storage.clone());

        let job = create_test_job("test-job", "default", 100);
        let ttl = controller.get_ttl_seconds_after_finished(&job);
        assert_eq!(ttl, Some(100));
    }

    #[tokio::test]
    async fn test_should_cleanup_expired_job() {
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let controller =
            TTLController::<rusternetes_storage::memory::MemoryStorage>::new(storage.clone());

        // Create a job that finished 120 seconds ago with 60 second TTL
        let job = create_test_job("test-job", "default", 60);
        let now = Utc::now();

        // Should be cleaned up since it finished 120 seconds ago and TTL is 60 seconds
        let should_cleanup = controller.should_cleanup(&job, 60, now).await;
        assert!(should_cleanup);
    }

    #[tokio::test]
    async fn test_should_not_cleanup_recent_job() {
        let storage = Arc::new(rusternetes_storage::memory::MemoryStorage::new());
        let controller =
            TTLController::<rusternetes_storage::memory::MemoryStorage>::new(storage.clone());

        // Create a job that just finished with 3600 second TTL
        let mut job = create_test_job("test-job", "default", 3600);

        // Update the finish time to be very recent
        if let Some(ref mut status) = job.status {
            if let Some(ref mut conditions) = status.conditions {
                if let Some(condition) = conditions.first_mut() {
                    condition.last_transition_time = Some(Utc::now() - Duration::seconds(10));
                }
            }
        }

        let now = Utc::now();

        // Should NOT be cleaned up since it just finished and TTL is 1 hour
        let should_cleanup = controller.should_cleanup(&job, 3600, now).await;
        assert!(!should_cleanup);
    }
}
