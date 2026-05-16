use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::workloads::{Job, JobCondition, JobStatus};
use rusternetes_common::resources::{Pod, PodStatus};
use rusternetes_common::types::{OwnerReference, Phase};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

pub struct JobController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> JobController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting JobController (watch-based)");
        let retry_interval = Duration::from_secs(5);

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            // Initial full reconciliation
            self.enqueue_all(&queue).await;

            // Watch for changes to Jobs AND Pods
            let prefix = "/registry/jobs/";
            let watch_result = self.storage.watch(prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };

            let pod_prefix = build_prefix("pods", None);
            let mut pod_watch = match self.storage.watch(&pod_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish pod watch: {}, retrying in {:?}",
                        e, retry_interval
                    );
                    time::sleep(retry_interval).await;
                    continue;
                }
            };

            // Periodic full resync as safety net
            let mut resync = tokio::time::interval(Duration::from_secs(5));
            resync.tick().await; // consume first immediate tick

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
                    event = pod_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                self.enqueue_owner_job(&queue, &ev).await;
                            }
                            Some(Err(e)) => {
                                warn!("Pod watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Pod watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
            // Watch broke — loop back to re-establish
        }
    }
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("jobs", Some(ns), name);
            match self.storage.get::<Job>(&storage_key).await {
                Ok(resource) => {
                    let mut resource = resource;
                    match self.reconcile(&mut resource).await {
                        Ok(()) => queue.forget(&key).await,
                        Err(e) => {
                            error!("Failed to reconcile {}: {}", key, e);
                            queue.requeue_rate_limited(key.clone()).await;
                        }
                    }
                }
                Err(_) => {
                    // Resource was deleted — nothing to reconcile
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
                    let key = {
                        let ns = item.metadata.namespace.as_deref().unwrap_or("");
                        format!("jobs/{}/{}", ns, item.metadata.name)
                    };
                    queue.add(key).await;
                }
            }
            Err(e) => {
                error!("Failed to list jobs for enqueue: {}", e);
            }
        }
    }

    /// When a pod changes, check its ownerReferences for a Job owner
    /// and enqueue that Job for reconciliation.
    async fn enqueue_owner_job(&self, queue: &WorkQueue, event: &rusternetes_storage::WatchEvent) {
        let pod_key = extract_key(event);
        let parts: Vec<&str> = pod_key.splitn(3, '/').collect();
        let ns = match parts.get(1) {
            Some(ns) => *ns,
            None => return,
        };

        let storage_key = format!("/registry/{}", pod_key);
        match self.storage.get::<Pod>(&storage_key).await {
            Ok(pod) => {
                if let Some(refs) = &pod.metadata.owner_references {
                    for owner_ref in refs {
                        if owner_ref.kind == "Job" {
                            queue.add(format!("jobs/{}/{}", ns, owner_ref.name)).await;
                        }
                    }
                }
            }
            Err(_) => {
                // Pod deleted — enqueue all Jobs in this namespace
                if let Ok(items) = self
                    .storage
                    .list::<Job>(&build_prefix("jobs", Some(ns)))
                    .await
                {
                    for job in &items {
                        queue
                            .add(format!("jobs/{}/{}", ns, job.metadata.name))
                            .await;
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        let jobs: Vec<Job> = self.storage.list("/registry/jobs/").await?;

        for mut job in jobs {
            if let Err(e) = self.reconcile(&mut job).await {
                error!("Failed to reconcile Job {}: {}", job.metadata.name, e);
            }
        }

        Ok(())
    }

    async fn reconcile(&self, job: &mut Job) -> Result<()> {
        let name = &job.metadata.name;
        let namespace = job.metadata.namespace.as_ref().unwrap();

        // Skip reconciliation for Jobs being deleted — GC handles pod cleanup
        if job.metadata.is_being_deleted() {
            return Ok(());
        }

        debug!("Reconciling Job {}/{}", namespace, name);

        // For completed/failed jobs, still update terminating count
        // (pods may still be shutting down after job completion).
        // K8s ref: pkg/controller/job/job_controller.go — syncJob continues
        // to update status.terminating for completed jobs.
        if let Some(ref status) = job.status {
            if let Some(ref conditions) = status.conditions {
                let is_finished = conditions.iter().any(|c| {
                    (c.condition_type == "Complete" || c.condition_type == "Failed")
                        && c.status == "True"
                });
                if is_finished {
                    // Still update terminating count for finished jobs
                    let pod_prefix = format!("/registry/pods/{}/", namespace);
                    let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;
                    let job_uid = &job.metadata.uid;
                    let terminating = all_pods
                        .iter()
                        .filter(|p| {
                            let owned = p.metadata.owner_references.as_ref().is_some_and(|refs| {
                                refs.iter().any(|r| r.uid == *job_uid && r.kind == "Job")
                            });
                            let is_terminating = p.metadata.deletion_timestamp.is_some()
                                && !matches!(
                                    p.status.as_ref().and_then(|s| s.phase.as_ref()),
                                    Some(Phase::Succeeded) | Some(Phase::Failed)
                                );
                            owned && is_terminating
                        })
                        .count() as i32;
                    // Update terminating count if it changed
                    if status.terminating != Some(terminating) {
                        let key = build_key("jobs", Some(namespace), name);
                        // Re-read for fresh resourceVersion to avoid CAS conflict
                        if let Ok(mut fresh_job) = self.storage.get::<Job>(&key).await {
                            if fresh_job.status.as_ref().and_then(|s| s.terminating)
                                != Some(terminating)
                            {
                                if let Some(ref mut s) = fresh_job.status {
                                    s.terminating = Some(terminating);
                                }
                                let _ = self.storage.update(&key, &fresh_job).await;
                            }
                        }
                    }
                    return Ok(());
                }
            }
        }

        let completions = job.spec.completions.unwrap_or(1);
        let parallelism = job.spec.parallelism.unwrap_or(1);
        let backoff_limit = job.spec.backoff_limit.unwrap_or(6);

        // Get current pods for this Job
        let pod_prefix = format!("/registry/pods/{}/", namespace);
        let all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;

        // Find pods owned by this Job via ownerReferences (authoritative),
        // or matching selector labels (for orphan adoption).
        // Also fall back to job-name label matching for backwards compatibility.
        let job_uid = &job.metadata.uid;
        let selector_labels = job
            .spec
            .selector
            .as_ref()
            .and_then(|s| s.match_labels.as_ref());
        let job_pods: Vec<Pod> = all_pods
            .into_iter()
            .filter(|pod| {
                let owned_by_ref = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| &r.uid == job_uid))
                    .unwrap_or(false);
                if owned_by_ref {
                    return true;
                }
                // Check if the pod is an orphan (no controller ownerRef) that matches our selector
                let has_any_controller = pod
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| r.controller.unwrap_or(false)))
                    .unwrap_or(false);
                if has_any_controller {
                    return false; // Pod is owned by another controller, skip
                }
                // Match by selector labels (primary) or job-name label (fallback)
                let pod_labels = pod.metadata.labels.as_ref();
                let matches_selector = selector_labels
                    .map(|sel| {
                        pod_labels
                            .map(|pl| sel.iter().all(|(k, v)| pl.get(k) == Some(v)))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                let matches_job_name = pod_labels
                    .and_then(|labels| labels.get("job-name"))
                    .map(|j| j == name)
                    .unwrap_or(false);
                matches_selector || matches_job_name
            })
            .collect();

        // Release pods whose labels no longer match the Job selector
        let selector = job
            .spec
            .selector
            .as_ref()
            .and_then(|s| s.match_labels.as_ref());
        if selector.is_none() && !job_pods.is_empty() {
            debug!(
                "Job {}/{} has no matchLabels selector, skipping release check for {} pods",
                namespace,
                name,
                job_pods.len()
            );
        }
        for pod in &job_pods {
            if let Some(sel) = selector {
                let labels = pod.metadata.labels.as_ref();
                let matches = labels.is_some_and(|l| sel.iter().all(|(k, v)| l.get(k) == Some(v)));
                if !matches {
                    // Pod no longer matches selector — release it by removing ownerReference
                    // Use CAS retry to handle concurrent updates
                    let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                    for _ in 0..3 {
                        match self.storage.get::<Pod>(&pod_key).await {
                            Ok(mut fresh_pod) => {
                                if let Some(ref mut refs) = fresh_pod.metadata.owner_references {
                                    refs.retain(|r| &r.uid != job_uid);
                                }
                                match self.storage.update(&pod_key, &fresh_pod).await {
                                    Ok(_) => {
                                        info!(
                                            "Released pod {} from job {}/{} (labels no longer match)",
                                            pod.metadata.name, namespace, name
                                        );
                                        break;
                                    }
                                    Err(e) => {
                                        warn!(
                                            "CAS retry releasing pod {}: {}",
                                            pod.metadata.name, e
                                        );
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    continue;
                }
            }
        }

        // Adopt orphaned pods — re-add ownerReference if pod matches by label but not by ownerRef
        for pod in &job_pods {
            let has_owner_ref = pod
                .metadata
                .owner_references
                .as_ref()
                .map(|refs| refs.iter().any(|r| &r.uid == job_uid))
                .unwrap_or(false);
            if !has_owner_ref {
                let mut adopted_pod = pod.clone();
                let owner_ref = rusternetes_common::types::OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: name.to_string(),
                    uid: job.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                };
                adopted_pod
                    .metadata
                    .owner_references
                    .get_or_insert_with(Vec::new)
                    .push(owner_ref);
                let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                if let Err(e) = self.storage.update(&pod_key, &adopted_pod).await {
                    tracing::warn!("Failed to adopt pod {}: {}", pod.metadata.name, e);
                } else {
                    info!(
                        "Adopted orphaned pod {} for job {}/{}",
                        pod.metadata.name, namespace, name
                    );
                }
            }
        }

        let is_indexed = job.spec.completion_mode.as_deref() == Some("Indexed");

        let mut active = 0;
        let mut succeeded = 0;
        let mut failed = 0;
        let mut ready = 0i32;

        for pod in job_pods.iter() {
            if let Some(status) = &pod.status {
                match &status.phase {
                    Some(Phase::Running) | Some(Phase::Pending) => active += 1,
                    Some(Phase::Succeeded) => succeeded += 1,
                    Some(Phase::Failed) => failed += 1,
                    _ => {}
                }
                // Count pods with Ready condition = True
                if let Some(conditions) = &status.conditions {
                    if conditions
                        .iter()
                        .any(|c| c.condition_type == "Ready" && c.status == "True")
                    {
                        ready += 1;
                    }
                }
            }
        }

        // Handle suspended jobs: delete all active pods and set active to 0
        if job.spec.suspend.unwrap_or(false) {
            if active > 0 {
                for pod in job_pods.iter() {
                    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                    if matches!(phase, Some(Phase::Running) | Some(Phase::Pending)) {
                        let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                        let _ = self.storage.delete(&pod_key).await;
                        info!(
                            "Suspended job {}/{}: deleted active pod {}",
                            namespace, name, pod.metadata.name
                        );
                    }
                }
            }
            // Preserve existing start_time
            let existing_start_time = job.status.as_ref().and_then(|s| s.start_time);
            let existing_conditions = job.status.as_ref().and_then(|s| s.conditions.clone());
            job.status = Some(JobStatus {
                active: Some(0),
                succeeded: Some(succeeded),
                failed: Some(failed),
                conditions: existing_conditions,
                start_time: existing_start_time,
                completion_time: None,
                ready: Some(ready),
                terminating: None,
                completed_indexes: None,
                failed_indexes: None,
                uncounted_terminated_pods: None,
                observed_generation: job.metadata.generation,
            });
            let key = format!("/registry/jobs/{}/{}", namespace, name);
            // Refresh RV before update to avoid CAS conflict
            if let Ok(mut fresh) = self.storage.get::<Job>(&key).await {
                if fresh.status != job.status {
                    fresh.status = job.status.clone();
                    self.storage.update(&key, &fresh).await?;
                }
            }
            return Ok(());
        }

        // Handle activeDeadlineSeconds — fail the job if it has been active too long
        if let Some(deadline) = job.spec.active_deadline_seconds {
            if let Some(start) = job.status.as_ref().and_then(|s| s.start_time) {
                let elapsed = chrono::Utc::now()
                    .signed_duration_since(start)
                    .num_seconds();
                if elapsed > deadline {
                    warn!(
                        "Job {}/{} exceeded activeDeadlineSeconds ({} > {})",
                        namespace, name, elapsed, deadline
                    );
                    // Delete all active pods
                    for pod in job_pods.iter() {
                        let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                        if matches!(phase, Some(Phase::Running) | Some(Phase::Pending)) {
                            let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                            let _ = self.storage.delete(&pod_key).await;
                        }
                    }
                    job.status = Some(JobStatus {
                        active: Some(0),
                        succeeded: Some(succeeded),
                        failed: Some(failed),
                        conditions: Some(vec![JobCondition {
                            condition_type: "Failed".to_string(),
                            status: "True".to_string(),
                            last_probe_time: Some(chrono::Utc::now()),
                            last_transition_time: Some(chrono::Utc::now()),
                            reason: Some("DeadlineExceeded".to_string()),
                            message: Some(format!(
                                "Job was active longer than specified deadline of {} seconds",
                                deadline
                            )),
                        }]),
                        start_time: job.status.as_ref().and_then(|s| s.start_time),
                        completion_time: Some(chrono::Utc::now()),
                        ready: Some(ready),
                        terminating: None,
                        completed_indexes: None,
                        failed_indexes: None,
                        uncounted_terminated_pods: None,
                        observed_generation: job.metadata.generation,
                    });
                    let key = format!("/registry/jobs/{}/{}", namespace, name);
                    // Refresh RV before update to avoid CAS conflict
                    if let Ok(mut fresh) = self.storage.get::<Job>(&key).await {
                        if fresh.status != job.status {
                            fresh.status = job.status.clone();
                            self.storage.update(&key, &fresh).await?;
                        }
                    }
                    return Ok(());
                }
            }
        }

        // For Indexed completion mode, track which indexes have completed
        let completed_indexes: Option<String> = if is_indexed {
            let mut indexes: Vec<i32> = Vec::new();
            for pod in job_pods.iter() {
                if let Some(status) = &pod.status {
                    if matches!(&status.phase, Some(Phase::Succeeded)) {
                        if let Some(idx) = get_pod_index(pod) {
                            indexes.push(idx);
                        }
                    }
                }
            }
            indexes.sort();
            indexes.dedup();
            if indexes.is_empty() {
                None
            } else {
                Some(format_index_ranges(&indexes))
            }
        } else {
            None
        };

        // Build a set of indexes that failed due to FailIndex podFailurePolicy
        let mut fail_index_set: HashSet<i32> = HashSet::new();

        // Check podFailurePolicy — if a failed pod matches a FailJob rule, fail immediately
        // Also check for FailIndex rules
        let mut pod_failure_policy_triggered = false;
        let mut pod_failure_message = String::new();
        let mut ignored_pods: HashSet<String> = HashSet::new();
        if let Some(ref policy) = job.spec.pod_failure_policy {
            if let Some(rules) = policy.get("rules").and_then(|r| r.as_array()) {
                for pod in job_pods.iter() {
                    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                    // Check ALL terminated pods (Failed AND Succeeded with non-zero exit).
                    // K8s podFailurePolicy evaluates against any pod with terminated containers,
                    // not just Failed phase pods. A pod can be Succeeded but have containers
                    // that exited with non-zero codes (if other containers succeeded).
                    if !matches!(phase, Some(Phase::Failed) | Some(Phase::Succeeded)) {
                        continue;
                    }
                    // Get container exit codes
                    let exit_codes: Vec<i32> = pod
                        .status
                        .as_ref()
                        .and_then(|s| s.container_statuses.as_ref())
                        .map(|cs| {
                            cs.iter()
                                .filter_map(|c| match &c.state {
                                    Some(
                                        rusternetes_common::resources::ContainerState::Terminated {
                                            exit_code,
                                            ..
                                        },
                                    ) => Some(*exit_code),
                                    _ => None,
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    for rule in rules {
                        let action = rule.get("action").and_then(|a| a.as_str()).unwrap_or("");

                        let mut rule_matched = false;

                        // Check onExitCodes
                        if let Some(on_exit) = rule.get("onExitCodes") {
                            let operator = on_exit
                                .get("operator")
                                .and_then(|o| o.as_str())
                                .unwrap_or("In");
                            let values: Vec<i32> = on_exit
                                .get("values")
                                .and_then(|v| v.as_array())
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|v| v.as_i64().map(|i| i as i32))
                                        .collect()
                                })
                                .unwrap_or_default();
                            rule_matched = exit_codes.iter().any(|code| match operator {
                                "In" => values.contains(code),
                                "NotIn" => !values.contains(code),
                                _ => false,
                            });
                        }

                        // Check onPodConditions
                        if !rule_matched {
                            if let Some(on_conditions) = rule.get("onPodConditions") {
                                if let Some(conditions) = on_conditions.as_array() {
                                    let pod_conditions =
                                        pod.status.as_ref().and_then(|s| s.conditions.as_ref());
                                    rule_matched = conditions.iter().any(|cond| {
                                        let ctype =
                                            cond.get("type").and_then(|t| t.as_str()).unwrap_or("");
                                        let cstatus = cond
                                            .get("status")
                                            .and_then(|s| s.as_str())
                                            .unwrap_or("True");
                                        pod_conditions
                                            .map(|pcs| {
                                                pcs.iter().any(|pc| {
                                                    pc.condition_type == ctype
                                                        && pc.status == cstatus
                                                })
                                            })
                                            .unwrap_or(false)
                                    });
                                }
                            }
                        }

                        if rule_matched {
                            match action {
                                "FailJob" => {
                                    pod_failure_policy_triggered = true;
                                    pod_failure_message =
                                        "Pod failed with exit code matching FailJob rule"
                                            .to_string();
                                    break;
                                }
                                "FailIndex" => {
                                    if is_indexed {
                                        if let Some(idx) = get_pod_index(pod) {
                                            fail_index_set.insert(idx);
                                        }
                                    }
                                    break;
                                }
                                "Ignore" => {
                                    ignored_pods.insert(pod.metadata.name.clone());
                                    break;
                                }
                                _ => {
                                    // "Count" and other actions: count normally against backoff
                                    break;
                                }
                            }
                        }
                    }
                    if pod_failure_policy_triggered {
                        break;
                    }
                }
            }
        }

        // Subtract ignored pods from the failed count — pods matching an "Ignore"
        // pod failure policy rule should not count against the backoff limit.
        let ignored_failed_count = ignored_pods.len() as i32;
        failed -= ignored_failed_count;
        if failed < 0 {
            failed = 0;
        }

        // Track failed indexes for backoffLimitPerIndex
        let backoff_limit_per_index = job.spec.backoff_limit_per_index;
        let mut backoff_failed_index_set: HashSet<i32> = HashSet::new();

        if is_indexed && backoff_limit_per_index.is_some() {
            let per_index_limit = backoff_limit_per_index.unwrap_or(0);
            // Count failures per index
            let mut failures_per_index: HashMap<i32, i32> = HashMap::new();
            for pod in job_pods.iter() {
                if let Some(status) = &pod.status {
                    if matches!(&status.phase, Some(Phase::Failed)) {
                        if let Some(index) = get_pod_index(pod) {
                            *failures_per_index.entry(index).or_insert(0) += 1;
                        }
                    }
                }
            }
            for (idx, count) in &failures_per_index {
                if *count > per_index_limit {
                    backoff_failed_index_set.insert(*idx);
                }
            }
        }

        // Merge FailIndex and backoff-per-index failed sets
        let all_failed_index_set: HashSet<i32> = fail_index_set
            .union(&backoff_failed_index_set)
            .copied()
            .collect();

        let failed_indexes: Option<String> = if !all_failed_index_set.is_empty() {
            let mut sorted: Vec<i32> = all_failed_index_set.iter().copied().collect();
            sorted.sort();
            Some(format_index_ranges(&sorted))
        } else {
            None
        };

        // For Indexed mode, K8s sets status.succeeded to the count of unique
        // succeeded indexes (NOT raw succeeded pod count); status.failed
        // excludes pods whose index has already succeeded. K8s ref:
        //   pkg/controller/job/job_controller.go — status.Succeeded =
        //   succeededIndexes.total().
        let succeeded_index_set: HashSet<i32> = if is_indexed {
            collect_indexes_in_phase(job_pods.iter(), Phase::Succeeded)
        } else {
            HashSet::new()
        };
        let succeeded_index_count = if is_indexed {
            succeeded_index_set.len() as i32
        } else {
            succeeded
        };

        if is_indexed {
            succeeded = succeeded_index_count;
            failed = job_pods
                .iter()
                .filter(|p| {
                    matches!(
                        p.status.as_ref().and_then(|s| s.phase.as_ref()),
                        Some(Phase::Failed)
                    ) && !ignored_pods.contains(&p.metadata.name)
                        && !get_pod_index(p).is_some_and(|i| succeeded_index_set.contains(&i))
                })
                .count() as i32;
        }

        info!(
            "Job {}/{}: active={}, succeeded={}, failed={}, target={}",
            namespace, name, active, succeeded, failed, completions
        );

        // Check if Job is complete
        // For indexed jobs, check number of distinct succeeded indexes
        let is_complete = if is_indexed {
            succeeded_index_count >= completions
        } else {
            succeeded >= completions
        };

        // Check maxFailedIndexes — if the number of failed indexes exceeds this limit, fail the job
        let max_failed_indexes_exceeded = if is_indexed {
            if let Some(max_failed) = job.spec.max_failed_indexes {
                let failed_index_count = all_failed_index_set.len() as i32;
                // Also count unique indexes with only failed pods (no succeeded) when no backoffLimitPerIndex
                if backoff_limit_per_index.is_none() && fail_index_set.is_empty() {
                    let mut failed_idx_set: HashSet<i32> = HashSet::new();
                    for pod in job_pods.iter() {
                        if matches!(
                            pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                            Some(Phase::Failed)
                        ) {
                            if let Some(index) = get_pod_index(pod) {
                                failed_idx_set.insert(index);
                            }
                        }
                    }
                    failed_idx_set.len() as i32 > max_failed
                } else {
                    failed_index_count > max_failed
                }
            } else {
                false
            }
        } else {
            false
        };

        // For backoffLimitPerIndex, job fails when all indexes are either succeeded or failed
        let is_failed = pod_failure_policy_triggered
            || max_failed_indexes_exceeded
            || if backoff_limit_per_index.is_some() && is_indexed {
                let completed_count = succeeded_index_count;
                let failed_count = all_failed_index_set.len() as i32;
                (completed_count + failed_count) >= completions
            } else {
                failed > backoff_limit
            };

        // Preserve the existing start_time if the job was already started
        let existing_start_time = job.status.as_ref().and_then(|s| s.start_time);

        // Set start_time when the job first has any pods (active, succeeded, or failed)
        let start_time = if active > 0 || succeeded > 0 || failed > 0 {
            Some(existing_start_time.unwrap_or_else(chrono::Utc::now))
        } else {
            existing_start_time
        };

        // Check successPolicy — if defined and criteria met, mark job complete
        let success_policy_met = if let Some(ref policy) = job.spec.success_policy {
            if let Some(rules) = policy.get("rules").and_then(|r| r.as_array()) {
                rules.iter().any(|rule| {
                    let indexes_ok = if let Some(succeeded_indexes_str) =
                        rule.get("succeededIndexes").and_then(|s| s.as_str())
                    {
                        // Parse required indexes and check they are all in completed set
                        let completed = completed_indexes.as_deref().unwrap_or("");
                        let completed_set: HashSet<i32> = parse_index_ranges(completed);
                        let required_set: HashSet<i32> = parse_index_ranges(succeeded_indexes_str);
                        required_set.is_subset(&completed_set)
                    } else {
                        true // No index constraint
                    };

                    let count_ok =
                        if let Some(count) = rule.get("succeededCount").and_then(|c| c.as_i64()) {
                            succeeded_index_count >= count as i32
                        } else {
                            true // No count constraint
                        };

                    // If rule has neither succeededIndexes nor succeededCount, match on all completions
                    let has_criteria = rule.get("succeededIndexes").is_some()
                        || rule.get("succeededCount").is_some();
                    if has_criteria {
                        indexes_ok && count_ok
                    } else {
                        succeeded_index_count >= completions
                    }
                })
            } else {
                false
            }
        } else {
            false
        };

        if success_policy_met {
            info!("Job {}/{} met success policy criteria", namespace, name);

            // Terminate remaining active pods and count how many we're terminating
            let mut _terminating_count = 0i32;
            for pod in job_pods.iter() {
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                if matches!(phase, Some(Phase::Running) | Some(Phase::Pending)) {
                    let pod_key = build_key("pods", Some(namespace), &pod.metadata.name);
                    let mut term_pod = pod.clone();
                    term_pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
                    let _ = self.storage.update(&pod_key, &term_pod).await;
                    _terminating_count += 1;
                }
            }

            // For indexed jobs, succeeded = number of unique succeeded indexes,
            // not the total succeeded pod count.
            // K8s ref: pkg/controller/job/job_controller.go — status.Succeeded = succeededIndexes.total()
            let succeeded_count = if is_indexed {
                completed_indexes
                    .as_ref()
                    .map(|ci| parse_index_ranges(ci).len() as i32)
                    .unwrap_or(0)
            } else {
                succeeded
            };

            job.status = Some(JobStatus {
                active: Some(0),
                succeeded: Some(succeeded_count),
                failed: Some(failed),
                conditions: Some(vec![
                    JobCondition {
                        condition_type: "SuccessCriteriaMet".to_string(),
                        status: "True".to_string(),
                        last_probe_time: Some(chrono::Utc::now()),
                        last_transition_time: Some(chrono::Utc::now()),
                        reason: Some("SuccessPolicy".to_string()),
                        message: Some("Job met success policy criteria".to_string()),
                    },
                    JobCondition {
                        condition_type: "Complete".to_string(),
                        status: "True".to_string(),
                        last_probe_time: Some(chrono::Utc::now()),
                        last_transition_time: Some(chrono::Utc::now()),
                        reason: Some("SuccessPolicy".to_string()),
                        message: Some("Job completed via success policy".to_string()),
                    },
                ]),
                start_time,
                completion_time: Some(chrono::Utc::now()),
                ready: Some(0), // Job is complete, no ready pods
                // K8s sets terminating to 0 when the job completes, even if pods
                // are still being cleaned up. The job status should reflect the
                // final state, not the transitional state.
                terminating: Some(0),
                completed_indexes: completed_indexes.clone(),
                failed_indexes: failed_indexes.clone(),
                uncounted_terminated_pods: None,
                observed_generation: job.metadata.generation,
            });
            let key = format!("/registry/jobs/{}/{}", namespace, name);
            // Refresh RV before update to avoid CAS conflict
            if let Ok(mut fresh) = self.storage.get::<Job>(&key).await {
                if fresh.status != job.status {
                    fresh.status = job.status.clone();
                    self.storage.update(&key, &fresh).await?;
                }
            }
            return Ok(());
        }

        if is_complete {
            info!("Job {}/{} completed successfully", namespace, name);
            job.status = Some(JobStatus {
                active: Some(0),
                succeeded: Some(succeeded),
                failed: Some(failed),
                conditions: Some(vec![JobCondition {
                    condition_type: "Complete".to_string(),
                    status: "True".to_string(),
                    last_probe_time: Some(chrono::Utc::now()),
                    last_transition_time: Some(chrono::Utc::now()),
                    reason: Some("CompletionsReached".to_string()),
                    message: Some("Job completed successfully".to_string()),
                }]),
                start_time,
                completion_time: Some(chrono::Utc::now()),
                // K8s sets ready and terminating to 0 when a job completes.
                // The test expects non-nil pointer to 0, not nil (omitted).
                ready: Some(0),
                terminating: Some(0),
                completed_indexes: completed_indexes.clone(),
                failed_indexes: failed_indexes.clone(),
                uncounted_terminated_pods: None,
                observed_generation: job.metadata.generation,
            });
        } else if is_failed {
            warn!(
                "Job {}/{} failed after {} failures",
                namespace, name, failed
            );

            // Determine failure reason
            let (reason, message) = if pod_failure_policy_triggered {
                ("PodFailurePolicy".to_string(), pod_failure_message.clone())
            } else if max_failed_indexes_exceeded {
                (
                    "MaxFailedIndexesExceeded".to_string(),
                    "Job has exceeded the maximum number of failed indexes".to_string(),
                )
            } else if backoff_limit_per_index.is_some() && is_indexed {
                (
                    "FailedIndexes".to_string(),
                    format!(
                        "Job has failed indexes: {}",
                        failed_indexes.as_deref().unwrap_or("")
                    ),
                )
            } else {
                (
                    "BackoffLimitExceeded".to_string(),
                    format!("Job has reached backoff limit of {}", backoff_limit),
                )
            };

            job.status = Some(JobStatus {
                active: Some(0),
                succeeded: Some(succeeded),
                failed: Some(failed),
                conditions: Some(vec![JobCondition {
                    condition_type: "Failed".to_string(),
                    status: "True".to_string(),
                    last_probe_time: Some(chrono::Utc::now()),
                    last_transition_time: Some(chrono::Utc::now()),
                    reason: Some(reason),
                    message: Some(message),
                }]),
                start_time,
                completion_time: Some(chrono::Utc::now()),
                // K8s sets ready and terminating to 0 when a job is terminal.
                ready: Some(0),
                terminating: Some(0),
                completed_indexes: completed_indexes.clone(),
                failed_indexes: failed_indexes.clone(),
                uncounted_terminated_pods: None,
                observed_generation: job.metadata.generation,
            });
        } else {
            // Re-list pods right before creating to minimize race window where
            // two parallel reconciliations both see "need 1 more pod" and both create one.
            let fresh_all_pods: Vec<Pod> = self.storage.list(&pod_prefix).await?;
            let fresh_job_pods: Vec<&Pod> = fresh_all_pods
                .iter()
                .filter(|pod| {
                    pod.metadata
                        .owner_references
                        .as_ref()
                        .map(|refs| refs.iter().any(|r| &r.uid == job_uid))
                        .unwrap_or(false)
                        || pod
                            .metadata
                            .labels
                            .as_ref()
                            .and_then(|labels| labels.get("job-name"))
                            .map(|j| j == name)
                            .unwrap_or(false)
                })
                .collect();

            let mut fresh_active = 0i32;
            let mut fresh_succeeded = 0i32;
            for pod in fresh_job_pods.iter() {
                if let Some(status) = &pod.status {
                    match &status.phase {
                        Some(Phase::Running) | Some(Phase::Pending) => fresh_active += 1,
                        Some(Phase::Succeeded) => fresh_succeeded += 1,
                        _ => {}
                    }
                }
            }

            // Calculate how many new pods to create using fresh counts
            let pods_needed = std::cmp::min(
                parallelism - fresh_active,
                completions - fresh_succeeded - fresh_active,
            );

            if pods_needed > 0 {
                // For Indexed mode, find which indexes still need pods
                let indexes_to_create: Vec<i32> = if is_indexed {
                    // Track indexes that already have active or succeeded pods
                    let mut active_or_succeeded_indexes: HashSet<i32> = HashSet::new();
                    for pod in fresh_job_pods.iter() {
                        let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                        if matches!(
                            phase,
                            Some(Phase::Running) | Some(Phase::Pending) | Some(Phase::Succeeded)
                        ) {
                            if let Some(idx) = get_pod_index(pod) {
                                active_or_succeeded_indexes.insert(idx);
                            }
                        }
                    }
                    (0..completions)
                        .filter(|i| {
                            // Skip indexes that already have active or succeeded pods
                            if active_or_succeeded_indexes.contains(i) {
                                return false;
                            }
                            // Skip indexes that are permanently failed (backoffLimitPerIndex or FailIndex)
                            if all_failed_index_set.contains(i) {
                                return false;
                            }
                            true
                        })
                        .take(pods_needed as usize)
                        .collect()
                } else {
                    (0..pods_needed).collect()
                };

                for (i, idx) in indexes_to_create.iter().enumerate() {
                    match self.create_pod(job, namespace, *idx, is_indexed).await {
                        Ok(_) => {
                            info!(
                                "Created pod for Job {}/{} ({}/{})",
                                namespace,
                                name,
                                fresh_job_pods.len() + i + 1,
                                completions
                            );
                        }
                        Err(e) => {
                            let err_str = format!("{}", e);
                            if err_str.contains("already exists")
                                || err_str.contains("AlreadyExists")
                            {
                                debug!(
                                    "Pod already exists for Job {}/{}, skipping",
                                    namespace, name
                                );
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }

                // Re-count pods after creation to get accurate status
                let all_pods_after: Vec<Pod> = self.storage.list(&pod_prefix).await?;
                let job_pods_after: Vec<Pod> = all_pods_after
                    .into_iter()
                    .filter(|pod| {
                        pod.metadata
                            .labels
                            .as_ref()
                            .and_then(|labels| labels.get("job-name"))
                            .map(|j| j == name)
                            .unwrap_or(false)
                    })
                    .collect();

                // Recalculate counts. For Indexed jobs apply the same unique-index
                // semantics as above so status.succeeded/failed stay consistent.
                active = 0;
                succeeded = 0;
                failed = 0;

                let after_succeeded_idx: HashSet<i32> = if is_indexed {
                    collect_indexes_in_phase(job_pods_after.iter(), Phase::Succeeded)
                } else {
                    HashSet::new()
                };

                for pod in job_pods_after.iter() {
                    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                    match phase {
                        Some(Phase::Running) | Some(Phase::Pending) => active += 1,
                        Some(Phase::Succeeded) if !is_indexed => succeeded += 1,
                        Some(Phase::Failed) => {
                            if ignored_pods.contains(&pod.metadata.name) {
                                continue;
                            }
                            // For Indexed jobs, skip Failed pods on an already-succeeded index.
                            if is_indexed
                                && get_pod_index(pod)
                                    .is_some_and(|i| after_succeeded_idx.contains(&i))
                            {
                                continue;
                            }
                            failed += 1;
                        }
                        _ => {}
                    }
                }
                if is_indexed {
                    succeeded = after_succeeded_idx.len() as i32;
                }
            }

            // Update status — but preserve conditions and completion_time if job
            // was already completed (e.g. by SuccessPolicy). Otherwise the regular
            // update path overwrites the completion status.
            let existing_conditions = job.status.as_ref().and_then(|s| s.conditions.clone());
            let existing_completion = job.status.as_ref().and_then(|s| s.completion_time);
            let already_complete = existing_conditions
                .as_ref()
                .map(|c| {
                    c.iter().any(|cond| {
                        (cond.condition_type == "Complete"
                            || cond.condition_type == "SuccessCriteriaMet")
                            && cond.status == "True"
                    })
                })
                .unwrap_or(false);

            if !already_complete {
                job.status = Some(JobStatus {
                    active: Some(active),
                    succeeded: Some(succeeded),
                    failed: Some(failed),
                    conditions: existing_conditions,
                    start_time,
                    completion_time: existing_completion,
                    ready: Some(ready),
                    terminating: None,
                    completed_indexes: completed_indexes.clone(),
                    failed_indexes: failed_indexes.clone(),
                    uncounted_terminated_pods: None,
                    observed_generation: job.metadata.generation,
                });
            }
        }

        // Save updated status
        let key = format!("/registry/jobs/{}/{}", namespace, name);
        let has_complete = job
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "Complete" && cond.status == "True")
            })
            .unwrap_or(false);
        let has_failed = job
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "Failed" && cond.status == "True")
            })
            .unwrap_or(false);
        if has_complete || has_failed {
            info!(
                "Job {}/{} status update: complete={}, failed={}, conditions={:?}",
                namespace,
                name,
                has_complete,
                has_failed,
                job.status.as_ref().and_then(|s| s.conditions.as_ref())
            );
        }
        // Refresh resourceVersion before update to avoid CAS conflicts.
        // The job's RV may be stale from the list() at the start of reconcile_all().
        // Other components (API server, kubelet) may have modified the job since then.
        let status_to_save = job.status.clone();
        for attempt in 0..3 {
            match self.storage.get::<Job>(&key).await {
                Ok(mut fresh_job) => {
                    // Only write status if it actually changed to avoid unnecessary
                    // storage writes that trigger watch events and cause feedback loops
                    if fresh_job.status == status_to_save {
                        break;
                    }
                    fresh_job.status = status_to_save.clone();
                    match self.storage.update(&key, &fresh_job).await {
                        Ok(_) => {
                            if has_complete || has_failed {
                                info!(
                                    "Job {}/{} status update persisted (attempt {})",
                                    namespace,
                                    name,
                                    attempt + 1
                                );
                            }
                            break;
                        }
                        Err(e) => {
                            warn!(
                                "Job {}/{} status update CAS conflict (attempt {}): {}",
                                namespace,
                                name,
                                attempt + 1,
                                e
                            );
                            if attempt == 2 {
                                return Err(e.into());
                            }
                        }
                    }
                }
                Err(e) => {
                    // Job was deleted between list and update
                    debug!("Job {}/{} no longer exists: {}", namespace, name, e);
                    break;
                }
            }
        }

        Ok(())
    }

    async fn create_pod(
        &self,
        job: &Job,
        namespace: &str,
        index: i32,
        is_indexed: bool,
    ) -> Result<()> {
        let job_name = &job.metadata.name;
        let pod_name = format!(
            "{}-{}",
            job_name,
            uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
        );

        // Create pod from template
        let template = &job.spec.template;
        let mut labels = template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.clone())
            .unwrap_or_default();
        labels.insert("job-name".to_string(), job_name.clone());
        labels.insert("controller-uid".to_string(), job.metadata.uid.clone());
        if is_indexed {
            labels.insert(
                "batch.kubernetes.io/job-completion-index".to_string(),
                index.to_string(),
            );
        }

        let mut annotations = template
            .metadata
            .as_ref()
            .and_then(|m| m.annotations.clone())
            .unwrap_or_default();
        if is_indexed {
            annotations.insert(
                "batch.kubernetes.io/job-completion-index".to_string(),
                index.to_string(),
            );
        }

        let mut spec = template.spec.clone();

        // Respect the template's restart policy.
        // For restartPolicy: OnFailure, the kubelet will restart failed containers in-place,
        // allowing the pod to eventually succeed without the Job controller creating new pods.
        // For restartPolicy: Never (or if not set), the Job controller handles retries.
        if spec.restart_policy.is_none() {
            spec.restart_policy = Some("Never".to_string());
        }

        // For Indexed mode, set hostname to {job-name}-{index} (K8s convention)
        if is_indexed {
            spec.hostname = Some(format!("{}-{}", job_name, index));
        }

        // For Indexed mode, inject JOB_COMPLETION_INDEX env var into all containers
        if is_indexed {
            for container in &mut spec.containers {
                let env = container.env.get_or_insert_with(Vec::new);
                if !env.iter().any(|e| e.name == "JOB_COMPLETION_INDEX") {
                    env.push(rusternetes_common::resources::EnvVar {
                        name: "JOB_COMPLETION_INDEX".to_string(),
                        value: Some(index.to_string()),
                        value_from: None,
                    });
                }
            }
        }

        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: pod_name.clone(),
                generate_name: None,
                generation: None,
                managed_fields: None,
                namespace: Some(namespace.to_string()),
                labels: Some(labels),
                annotations: Some(annotations),
                uid: uuid::Uuid::new_v4().to_string(),
                creation_timestamp: Some(chrono::Utc::now()),
                deletion_timestamp: None,
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: job_name.clone(),
                    uid: job.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
            },
            spec: Some(spec),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                message: None,
                reason: None,
                pod_ip: None,
                host_ip: None,
                host_i_ps: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
            }),
        };

        // Check ResourceQuota before creating pod
        super::check_resource_quota(&*self.storage, namespace).await?;

        let key = format!("/registry/pods/{}/{}", namespace, pod_name);
        self.storage.create(&key, &pod).await?;

        Ok(())
    }
}

/// Extract the completion index from a pod owned by an Indexed Job.
/// Looks for `batch.kubernetes.io/job-completion-index` on annotations,
/// then labels, then the `JOB_COMPLETION_INDEX` env var on the first container.
fn get_pod_index(pod: &Pod) -> Option<i32> {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("batch.kubernetes.io/job-completion-index"))
        .and_then(|v| v.parse::<i32>().ok())
        .or_else(|| {
            pod.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("batch.kubernetes.io/job-completion-index"))
                .and_then(|v| v.parse::<i32>().ok())
        })
        .or_else(|| {
            pod.spec.as_ref().and_then(|s| {
                s.containers.first().and_then(|c| {
                    c.env.as_ref().and_then(|envs| {
                        envs.iter()
                            .find(|e| e.name == "JOB_COMPLETION_INDEX")
                            .and_then(|e| e.value.as_ref())
                            .and_then(|v| v.parse::<i32>().ok())
                    })
                })
            })
        })
}

/// Collect the set of unique completion indexes whose pods are in the given
/// phase. Used for K8s-compatible Indexed Job status accounting.
fn collect_indexes_in_phase<'a, I>(pods: I, phase: Phase) -> HashSet<i32>
where
    I: IntoIterator<Item = &'a Pod>,
{
    let mut set = HashSet::new();
    for pod in pods {
        if pod.status.as_ref().and_then(|s| s.phase.as_ref()) == Some(&phase) {
            if let Some(idx) = get_pod_index(pod) {
                set.insert(idx);
            }
        }
    }
    set
}

/// Parse index ranges like "0,1,3-5" into a set of integers {0, 1, 3, 4, 5}
fn parse_index_ranges(s: &str) -> HashSet<i32> {
    let mut set = HashSet::new();
    if s.is_empty() {
        return set;
    }
    for part in s.split(',') {
        let part = part.trim();
        if part.contains('-') {
            let bounds: Vec<&str> = part.split('-').collect();
            if bounds.len() == 2 {
                if let (Ok(start), Ok(end)) = (
                    bounds[0].trim().parse::<i32>(),
                    bounds[1].trim().parse::<i32>(),
                ) {
                    for i in start..=end {
                        set.insert(i);
                    }
                }
            }
        } else if let Ok(idx) = part.parse::<i32>() {
            set.insert(idx);
        }
    }
    set
}

/// Format a sorted, deduped list of indexes into compressed ranges: [0, 1, 2, 5] -> "0-2,5"
fn format_index_ranges(indexes: &[i32]) -> String {
    if indexes.is_empty() {
        return String::new();
    }
    let mut ranges: Vec<String> = Vec::new();
    let mut start = indexes[0];
    let mut end = indexes[0];
    for &idx in &indexes[1..] {
        if idx == end + 1 {
            end = idx;
        } else {
            if start == end {
                ranges.push(start.to_string());
            } else {
                ranges.push(format!("{}-{}", start, end));
            }
            start = idx;
            end = idx;
        }
    }
    if start == end {
        ranges.push(start.to_string());
    } else {
        ranges.push(format!("{}-{}", start, end));
    }
    ranges.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::workloads::{Job, JobSpec, PodTemplateSpec};
    use rusternetes_common::resources::{
        Container, ContainerState, ContainerStatus, Pod, PodCondition, PodSpec, PodStatus,
    };
    use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    fn test_container() -> Container {
        Container {
            name: "test".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources: None,
            volume_mounts: None,
            volume_devices: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            image_pull_policy: None,
            security_context: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            resize_policy: None,
            restart_policy: None,
        }
    }

    fn make_job(name: &str, namespace: &str, completions: i32, parallelism: i32) -> Job {
        Job {
            type_meta: TypeMeta {
                kind: "Job".to_string(),
                api_version: "batch/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                uid: "job-uid-1".to_string(),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: JobSpec {
                template: PodTemplateSpec {
                    metadata: None,
                    spec: PodSpec {
                        containers: vec![test_container()],
                        ..Default::default()
                    },
                },
                completions: Some(completions),
                parallelism: Some(parallelism),
                backoff_limit: Some(6),
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
            status: None,
        }
    }

    fn make_pod(name: &str, namespace: &str, phase: Phase, job_name: &str, job_uid: &str) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: name.to_string(),
                namespace: Some(namespace.to_string()),
                uid: format!("pod-uid-{}", name),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("job-name".to_string(), job_name.to_string());
                    m
                }),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: job_name.to_string(),
                    uid: job_uid.to_string(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(phase),
                ..Default::default()
            }),
        }
    }

    fn make_indexed_pod(
        name: &str,
        namespace: &str,
        phase: Phase,
        job_name: &str,
        job_uid: &str,
        index: i32,
    ) -> Pod {
        let mut pod = make_pod(name, namespace, phase, job_name, job_uid);
        pod.metadata.annotations = Some({
            let mut m = HashMap::new();
            m.insert(
                "batch.kubernetes.io/job-completion-index".to_string(),
                index.to_string(),
            );
            m
        });
        if let Some(ref mut labels) = pod.metadata.labels {
            labels.insert(
                "batch.kubernetes.io/job-completion-index".to_string(),
                index.to_string(),
            );
        }
        pod
    }

    fn make_failed_pod_with_exit_code(
        name: &str,
        namespace: &str,
        job_name: &str,
        job_uid: &str,
        index: i32,
        exit_code: i32,
    ) -> Pod {
        let mut pod = make_indexed_pod(name, namespace, Phase::Failed, job_name, job_uid, index);
        if let Some(ref mut status) = pod.status {
            status.container_statuses = Some(vec![ContainerStatus {
                name: "test".to_string(),
                ready: false,
                restart_count: 0,
                state: Some(ContainerState::Terminated {
                    exit_code,
                    signal: None,
                    reason: Some("Error".to_string()),
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                }),
                last_state: None,
                image: Some("busybox".to_string()),
                image_id: None,
                container_id: None,
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                volume_mounts: None,
                stop_signal: None,
                user: None,
            }]);
        }
        pod
    }

    #[test]
    fn test_pods_needed_calculation() {
        let parallelism = 3;
        let completions = 10;
        let active = 2;
        let succeeded = 5;

        let pods_needed = std::cmp::min(
            parallelism - active,             // 1 (can run 1 more in parallel)
            completions - succeeded - active, // 3 (need 3 more to complete)
        );

        assert_eq!(pods_needed, 1);
    }

    #[test]
    fn test_job_completion() {
        let completions = 5;
        let succeeded = 5;
        assert!(succeeded >= completions);
    }

    #[test]
    fn test_backoff_limit_exceeded() {
        let backoff_limit = 6;
        let failed = 7;
        assert!(failed > backoff_limit);
    }

    #[test]
    fn test_parse_index_ranges() {
        assert_eq!(parse_index_ranges(""), HashSet::new());
        assert_eq!(
            parse_index_ranges("0"),
            [0].into_iter().collect::<HashSet<i32>>()
        );
        assert_eq!(
            parse_index_ranges("0,1,2"),
            [0, 1, 2].into_iter().collect::<HashSet<i32>>()
        );
        assert_eq!(
            parse_index_ranges("0-3"),
            [0, 1, 2, 3].into_iter().collect::<HashSet<i32>>()
        );
        assert_eq!(
            parse_index_ranges("0,2-4,7"),
            [0, 2, 3, 4, 7].into_iter().collect::<HashSet<i32>>()
        );
    }

    #[test]
    fn test_format_index_ranges() {
        assert_eq!(format_index_ranges(&[]), "");
        assert_eq!(format_index_ranges(&[0]), "0");
        assert_eq!(format_index_ranges(&[0, 1, 2]), "0-2");
        assert_eq!(format_index_ranges(&[0, 1, 2, 5]), "0-2,5");
        assert_eq!(format_index_ranges(&[0, 2, 3, 4, 7, 8]), "0,2-4,7-8");
    }

    #[tokio::test]
    async fn test_backoff_limit_per_index_tracks_failures() {
        let storage = Arc::new(MemoryStorage::new());

        // Create an indexed job with backoffLimitPerIndex=1, 3 completions
        let mut job = make_job("test-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit_per_index = Some(1);
        job.spec.backoff_limit = Some(100); // high so global limit doesn't kick in

        let job_key = "/registry/jobs/default/test-job";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "test-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();

        // Index 1 failed twice (exceeds backoffLimitPerIndex=1)
        let pod1a = make_indexed_pod(
            "pod-1a",
            "default",
            Phase::Failed,
            "test-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1a", &pod1a)
            .await
            .unwrap();
        let pod1b = make_indexed_pod(
            "pod-1b",
            "default",
            Phase::Failed,
            "test-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1b", &pod1b)
            .await
            .unwrap();

        // Index 2 succeeded
        let pod2 = make_indexed_pod(
            "pod-2",
            "default",
            Phase::Succeeded,
            "test-job",
            "job-uid-1",
            2,
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // Re-read the job
        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Job should be failed because: succeeded(0,2) + failed(1) = 3 = completions
        // Index 1 exceeded backoffLimitPerIndex
        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Failed" && c.status == "True"),
            "Job should be marked as Failed"
        );

        // Failed indexes should contain index 1
        assert!(status.failed_indexes.is_some());
        let fi = parse_index_ranges(status.failed_indexes.as_deref().unwrap());
        assert!(fi.contains(&1), "Index 1 should be in failed_indexes");

        // Completed indexes should contain 0 and 2
        assert!(status.completed_indexes.is_some());
        let ci = parse_index_ranges(status.completed_indexes.as_deref().unwrap());
        assert!(
            ci.contains(&0) && ci.contains(&2),
            "Completed indexes should have 0 and 2"
        );
    }

    #[tokio::test]
    async fn test_backoff_limit_per_index_no_retry_for_exhausted_index() {
        let storage = Arc::new(MemoryStorage::new());

        // Create an indexed job with backoffLimitPerIndex=0, 3 completions
        let mut job = make_job("test-job2", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit_per_index = Some(0);
        job.spec.backoff_limit = Some(100);

        let job_key = "/registry/jobs/default/test-job2";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 failed once (exceeds limit of 0)
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Failed,
            "test-job2",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();

        // Index 1 running
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Running,
            "test-job2",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // After reconciliation, no new pod should be created for index 0
        // Check that no new pod with index 0 was created
        let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let index_0_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("batch.kubernetes.io/job-completion-index"))
                    .map(|v| v == "0")
                    .unwrap_or(false)
            })
            .collect();

        // Should still be just the one failed pod for index 0, no retry
        assert_eq!(
            index_0_pods.len(),
            1,
            "Should not create a retry pod for exhausted index 0"
        );
    }

    #[tokio::test]
    async fn test_pod_failure_policy_fail_index() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("failindex-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit = Some(100);
        // Set up a FailIndex policy for exit code 42
        job.spec.pod_failure_policy = Some(serde_json::json!({
            "rules": [
                {
                    "action": "FailIndex",
                    "onExitCodes": {
                        "operator": "In",
                        "values": [42]
                    }
                }
            ]
        }));

        let job_key = "/registry/jobs/default/failindex-job";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "failindex-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();

        // Index 1 failed with exit code 42 -> should trigger FailIndex
        let pod1 =
            make_failed_pod_with_exit_code("pod-1", "default", "failindex-job", "job-uid-1", 1, 42);
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Index 2 succeeded
        let pod2 = make_indexed_pod(
            "pod-2",
            "default",
            Phase::Succeeded,
            "failindex-job",
            "job-uid-1",
            2,
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Failed indexes should include index 1
        assert!(status.failed_indexes.is_some());
        let fi = parse_index_ranges(status.failed_indexes.as_deref().unwrap());
        assert!(
            fi.contains(&1),
            "Index 1 should be in failed_indexes due to FailIndex policy"
        );

        // No new pod should be created for index 1
        let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let index_1_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("batch.kubernetes.io/job-completion-index"))
                    .map(|v| v == "1")
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(
            index_1_pods.len(),
            1,
            "No retry pod for FailIndex-ed index 1"
        );
    }

    #[tokio::test]
    async fn test_pod_failure_policy_fail_job() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("failjob-job", "default", 3, 3);
        job.spec.backoff_limit = Some(100);
        job.spec.pod_failure_policy = Some(serde_json::json!({
            "rules": [
                {
                    "action": "FailJob",
                    "onExitCodes": {
                        "operator": "In",
                        "values": [99]
                    }
                }
            ]
        }));

        let job_key = "/registry/jobs/default/failjob-job";
        storage.create(job_key, &job).await.unwrap();

        // One pod failed with exit code 99 -> should trigger FailJob
        let pod = make_failed_pod_with_exit_code(
            "pod-fail",
            "default",
            "failjob-job",
            "job-uid-1",
            0,
            99,
        );
        storage
            .create("/registry/pods/default/pod-fail", &pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Failed"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("PodFailurePolicy")),
            "Job should be Failed due to PodFailurePolicy"
        );
    }

    #[tokio::test]
    async fn test_local_restart_completion() {
        let storage = Arc::new(MemoryStorage::new());

        // Job with 2 completions, template has restartPolicy: OnFailure
        let mut job = make_job("restart-job", "default", 2, 2);
        job.spec.template.spec.restart_policy = Some("OnFailure".to_string());

        let job_key = "/registry/jobs/default/restart-job";
        storage.create(job_key, &job).await.unwrap();

        // Pod 1 succeeded (was restarted locally, restart_count > 0, now succeeded)
        let mut pod1 = make_pod(
            "pod-1",
            "default",
            Phase::Succeeded,
            "restart-job",
            "job-uid-1",
        );
        if let Some(ref mut status) = pod1.status {
            status.container_statuses = Some(vec![ContainerStatus {
                name: "test".to_string(),
                ready: false,
                restart_count: 3, // restarted 3 times before succeeding
                state: Some(ContainerState::Terminated {
                    exit_code: 0,
                    signal: None,
                    reason: Some("Completed".to_string()),
                    message: None,
                    started_at: None,
                    finished_at: None,
                    container_id: None,
                }),
                last_state: None,
                image: Some("busybox".to_string()),
                image_id: None,
                container_id: None,
                started: None,
                allocated_resources: None,
                allocated_resources_status: None,
                resources: None,
                volume_mounts: None,
                stop_signal: None,
                user: None,
            }]);
        }
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Pod 2 succeeded
        let pod2 = make_pod(
            "pod-2",
            "default",
            Phase::Succeeded,
            "restart-job",
            "job-uid-1",
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Job should be Complete when locally restarted pods succeed"
        );
        assert_eq!(status.succeeded, Some(2));
    }

    #[tokio::test]
    async fn test_restart_policy_on_failure_preserved() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("onfailure-job", "default", 1, 1);
        job.spec.template.spec.restart_policy = Some("OnFailure".to_string());

        let job_key = "/registry/jobs/default/onfailure-job";
        storage.create(job_key, &job).await.unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // Check that the created pod preserved the OnFailure restart policy
        let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let job_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|p| {
                p.metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("job-name"))
                    .map(|v| v == "onfailure-job")
                    .unwrap_or(false)
            })
            .collect();

        assert_eq!(job_pods.len(), 1);
        let restart_policy = job_pods[0].spec.as_ref().unwrap().restart_policy.as_deref();
        assert_eq!(
            restart_policy,
            Some("OnFailure"),
            "Pod should preserve OnFailure restart policy from template"
        );
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_indexes() {
        let storage = Arc::new(MemoryStorage::new());

        // Indexed job with 5 completions, successPolicy requiring indexes 0-1
        let mut job = make_job("sp-job", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(serde_json::json!({
            "rules": [
                {
                    "succeededIndexes": "0-1"
                }
            ]
        }));

        let job_key = "/registry/jobs/default/sp-job";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 and 1 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Succeeded,
            "sp-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Indexes 2-4 are still pending
        let pod2 = make_indexed_pod("pod-2", "default", Phase::Pending, "sp-job", "job-uid-1", 2);
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();
        let pod3 = make_indexed_pod("pod-3", "default", Phase::Pending, "sp-job", "job-uid-1", 3);
        storage
            .create("/registry/pods/default/pod-3", &pod3)
            .await
            .unwrap();
        let pod4 = make_indexed_pod("pod-4", "default", Phase::Pending, "sp-job", "job-uid-1", 4);
        storage
            .create("/registry/pods/default/pod-4", &pod4)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Job should be complete via successPolicy even though indexes 2-4 are pending
        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Job should be Complete via successPolicy"
        );
        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet" && c.status == "True"),
            "SuccessCriteriaMet condition should be set"
        );
        assert!(status.completion_time.is_some());
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_count() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-count-job", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(serde_json::json!({
            "rules": [
                {
                    "succeededCount": 2
                }
            ]
        }));

        let job_key = "/registry/jobs/default/sp-count-job";
        storage.create(job_key, &job).await.unwrap();

        // 2 indexes succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-count-job",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Succeeded,
            "sp-count-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        // Others still pending
        let pod2 = make_indexed_pod(
            "pod-2",
            "default",
            Phase::Pending,
            "sp-count-job",
            "job-uid-1",
            2,
        );
        storage
            .create("/registry/pods/default/pod-2", &pod2)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        assert!(
            status
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Job should be Complete via succeededCount successPolicy"
        );
    }

    #[tokio::test]
    async fn test_success_policy_not_met_yet() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-notyet", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(serde_json::json!({
            "rules": [
                {
                    "succeededIndexes": "0-2"
                }
            ]
        }));

        let job_key = "/registry/jobs/default/sp-notyet";
        storage.create(job_key, &job).await.unwrap();

        // Only index 0 succeeded — need 0, 1, 2
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-notyet",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Running,
            "sp-notyet",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // Should NOT be complete yet
        let is_complete = status
            .conditions
            .as_ref()
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "Complete" && cond.status == "True")
            })
            .unwrap_or(false);
        assert!(
            !is_complete,
            "Job should NOT be complete when not all required indexes succeeded"
        );
    }

    #[tokio::test]
    async fn test_pod_failure_policy_ignore_action() {
        let storage = Arc::new(MemoryStorage::new());

        // Create an indexed job with backoffLimit=0 and a pod failure policy
        // that ignores pods with DisruptionTarget condition
        let mut job = make_job("ignore-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.backoff_limit = Some(0); // Would fail immediately if the pod is counted
        job.spec.pod_failure_policy = Some(serde_json::json!({
            "rules": [
                {
                    "action": "Ignore",
                    "onPodConditions": [
                        {
                            "type": "DisruptionTarget",
                            "status": "True"
                        }
                    ]
                }
            ]
        }));

        let job_key = "/registry/jobs/default/ignore-job";
        storage.create(job_key, &job).await.unwrap();

        // Create a failed pod with a DisruptionTarget condition — should be ignored
        let mut pod0 = make_indexed_pod(
            "pod-0-fail",
            "default",
            Phase::Failed,
            "ignore-job",
            "job-uid-1",
            0,
        );
        if let Some(ref mut status) = pod0.status {
            status.conditions = Some(vec![PodCondition {
                condition_type: "DisruptionTarget".to_string(),
                status: "True".to_string(),
                reason: Some("EvictionByEvictionAPI".to_string()),
                message: None,
                last_transition_time: None,
                observed_generation: None,
            }]);
        }
        storage
            .create("/registry/pods/default/pod-0-fail", &pod0)
            .await
            .unwrap();

        // Index 1 running
        let pod1 = make_indexed_pod(
            "pod-1",
            "default",
            Phase::Running,
            "ignore-job",
            "job-uid-1",
            1,
        );
        storage
            .create("/registry/pods/default/pod-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();

        // The job should NOT have a Failed condition — the ignored pod shouldn't
        // trigger backoff limit exceeded
        let has_failed = status
            .conditions
            .as_ref()
            .map(|c| {
                c.iter()
                    .any(|cond| cond.condition_type == "Failed" && cond.status == "True")
            })
            .unwrap_or(false);
        assert!(
            !has_failed,
            "Job should NOT be Failed — the pod with DisruptionTarget should be ignored"
        );

        // status.failed should not count the ignored pod
        assert_eq!(
            status.failed,
            Some(0),
            "Ignored pod should not be counted in status.failed"
        );
    }

    #[tokio::test]
    async fn test_adopt_matching_orphans() {
        let storage = Arc::new(MemoryStorage::new());

        // Create a job with a selector that matches controller-uid
        let mut job = make_job("adopt-job", "default", 2, 2);
        let job_uid = "adopt-uid-123";
        job.metadata.uid = job_uid.to_string();
        // Set up a selector with controller-uid (like the API server auto-generates)
        let mut match_labels = HashMap::new();
        match_labels.insert("controller-uid".to_string(), job_uid.to_string());
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(match_labels),
            match_expressions: None,
        });
        // Also set template labels to include controller-uid
        job.spec.template.metadata = Some(ObjectMeta {
            labels: Some({
                let mut m = HashMap::new();
                m.insert("controller-uid".to_string(), job_uid.to_string());
                m.insert("job-name".to_string(), "adopt-job".to_string());
                m
            }),
            ..Default::default()
        });

        let job_key = "/registry/jobs/default/adopt-job";
        storage.create(job_key, &job).await.unwrap();

        // Create an orphan pod that has matching labels but NO ownerReference
        let orphan_pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "orphan-pod-1".to_string(),
                namespace: Some("default".to_string()),
                uid: "orphan-uid-1".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("controller-uid".to_string(), job_uid.to_string());
                    m.insert("job-name".to_string(), "adopt-job".to_string());
                    m
                }),
                owner_references: None, // No ownerReference — orphan
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Succeeded),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/orphan-pod-1", &orphan_pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The orphan pod should now have an ownerReference to the job
        let updated_pod: Pod = storage
            .get("/registry/pods/default/orphan-pod-1")
            .await
            .unwrap();
        let has_owner_ref = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.uid == job_uid && r.kind == "Job"))
            .unwrap_or(false);
        assert!(
            has_owner_ref,
            "Orphan pod should be adopted with ownerReference pointing to the job"
        );
    }

    #[tokio::test]
    async fn test_release_non_matching_pods() {
        let storage = Arc::new(MemoryStorage::new());

        // Create a job with a selector
        let mut job = make_job("release-job", "default", 2, 2);
        let job_uid = "release-uid-456";
        job.metadata.uid = job_uid.to_string();
        let mut match_labels = HashMap::new();
        match_labels.insert("controller-uid".to_string(), job_uid.to_string());
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(match_labels),
            match_expressions: None,
        });

        let job_key = "/registry/jobs/default/release-job";
        storage.create(job_key, &job).await.unwrap();

        // Create a pod that has an ownerReference to this job but WRONG labels
        let non_matching_pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "wrong-label-pod".to_string(),
                namespace: Some("default".to_string()),
                uid: "wrong-uid-1".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("controller-uid".to_string(), "different-uid".to_string());
                    m.insert("job-name".to_string(), "release-job".to_string());
                    m
                }),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: "release-job".to_string(),
                    uid: job_uid.to_string(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/wrong-label-pod", &non_matching_pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The non-matching pod should have had its ownerReference removed (released)
        let updated_pod: Pod = storage
            .get("/registry/pods/default/wrong-label-pod")
            .await
            .unwrap();
        let still_owned = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.uid == job_uid))
            .unwrap_or(false);
        assert!(
            !still_owned,
            "Pod with non-matching labels should be released (ownerReference removed)"
        );
    }

    #[tokio::test]
    async fn test_do_not_adopt_pods_owned_by_another_controller() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("adopt-job2", "default", 2, 2);
        let job_uid = "adopt-uid-789";
        job.metadata.uid = job_uid.to_string();
        let mut match_labels = HashMap::new();
        match_labels.insert("controller-uid".to_string(), job_uid.to_string());
        job.spec.selector = Some(rusternetes_common::types::LabelSelector {
            match_labels: Some(match_labels),
            match_expressions: None,
        });

        let job_key = "/registry/jobs/default/adopt-job2";
        storage.create(job_key, &job).await.unwrap();

        // Create a pod with matching labels but already owned by ANOTHER controller
        let owned_pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "other-owned-pod".to_string(),
                namespace: Some("default".to_string()),
                uid: "other-uid-1".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("controller-uid".to_string(), job_uid.to_string());
                    m.insert("job-name".to_string(), "adopt-job2".to_string());
                    m
                }),
                owner_references: Some(vec![OwnerReference {
                    api_version: "batch/v1".to_string(),
                    kind: "Job".to_string(),
                    name: "other-job".to_string(),
                    uid: "other-job-uid".to_string(),
                    controller: Some(true), // Already owned by another controller
                    block_owner_deletion: Some(true),
                }]),
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/other-owned-pod", &owned_pod)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The pod should NOT be adopted — it's owned by another controller
        let updated_pod: Pod = storage
            .get("/registry/pods/default/other-owned-pod")
            .await
            .unwrap();
        let adopted_by_us = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| refs.iter().any(|r| r.uid == job_uid))
            .unwrap_or(false);
        assert!(
            !adopted_by_us,
            "Pod owned by another controller should NOT be adopted"
        );
    }

    #[tokio::test]
    async fn test_auto_selector_adoption_without_explicit_selector() {
        let storage = Arc::new(MemoryStorage::new());

        // Job WITHOUT an explicit selector (backwards compat — old-style)
        let mut job = make_job("legacy-job", "default", 1, 1);
        job.metadata.uid = "legacy-uid".to_string();
        // No selector set — relies on job-name label

        let job_key = "/registry/jobs/default/legacy-job";
        storage.create(job_key, &job).await.unwrap();

        // Create orphan pod with just job-name label, no ownerRef
        let orphan = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "legacy-orphan".to_string(),
                namespace: Some("default".to_string()),
                uid: "legacy-orphan-uid".to_string(),
                labels: Some({
                    let mut m = HashMap::new();
                    m.insert("job-name".to_string(), "legacy-job".to_string());
                    m
                }),
                owner_references: None,
                creation_timestamp: Some(chrono::Utc::now()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![test_container()],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Succeeded),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/legacy-orphan", &orphan)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        // The orphan should be adopted via job-name label fallback
        let updated_pod: Pod = storage
            .get("/registry/pods/default/legacy-orphan")
            .await
            .unwrap();
        let adopted = updated_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.uid == "legacy-uid" && r.kind == "Job")
            })
            .unwrap_or(false);
        assert!(
            adopted,
            "Orphan pod should be adopted via job-name label fallback"
        );
    }

    #[tokio::test]
    async fn test_success_policy_all_indexes_succeeded() {
        // Test 47: "with successPolicy should succeeded when all indexes succeeded"
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-all-job", "default", 3, 3);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(serde_json::json!({
            "rules": [
                {
                    "succeededIndexes": "0-2"
                }
            ]
        }));

        let job_key = "/registry/jobs/default/sp-all-job";
        storage.create(job_key, &job).await.unwrap();

        // All indexes succeed
        for i in 0..3 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Succeeded,
                "sp-all-job",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();
        let conditions = status.conditions.as_ref().unwrap();

        // Should have SuccessCriteriaMet condition
        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("SuccessPolicy")),
            "Should have SuccessCriteriaMet condition with reason SuccessPolicy"
        );

        // Should have Complete condition
        assert!(
            conditions.iter().any(|c| c.condition_type == "Complete"
                && c.status == "True"
                && c.reason.as_deref() == Some("SuccessPolicy")),
            "Should have Complete condition with reason SuccessPolicy"
        );

        assert!(status.completion_time.is_some());
        assert_eq!(status.succeeded, Some(3));
        // completedIndexes should list all three
        assert_eq!(status.completed_indexes.as_deref(), Some("0-2"));
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_count_rule() {
        // Test 48: "with successPolicy succeededCount rule"
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-count2", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(serde_json::json!({
            "rules": [
                {
                    "succeededCount": 3
                }
            ]
        }));

        let job_key = "/registry/jobs/default/sp-count2";
        storage.create(job_key, &job).await.unwrap();

        // 3 indexes succeed, 2 still running
        for i in 0..3 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Succeeded,
                "sp-count2",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }
        for i in 3..5 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Running,
                "sp-count2",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();
        let conditions = status.conditions.as_ref().unwrap();

        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("SuccessPolicy")),
            "SuccessCriteriaMet should be set when succeededCount rule met"
        );
        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "Complete" && c.status == "True"),
            "Complete condition should be set"
        );
        // K8s sets terminating to 0 when the job completes, even if pods
        // are still being cleaned up. The job status reflects the final state.
        assert_eq!(status.terminating, Some(0));
        assert_eq!(status.active, Some(0));
    }

    #[tokio::test]
    async fn test_success_policy_succeeded_indexes_rule() {
        // Test 49: "with successPolicy succeededIndexes rule"
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-idx-rule", "default", 5, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        // Only indexes 0 and 4 need to succeed
        job.spec.success_policy = Some(serde_json::json!({
            "rules": [
                {
                    "succeededIndexes": "0,4"
                }
            ]
        }));

        let job_key = "/registry/jobs/default/sp-idx-rule";
        storage.create(job_key, &job).await.unwrap();

        // Index 0 and 4 succeeded
        let pod0 = make_indexed_pod(
            "pod-0",
            "default",
            Phase::Succeeded,
            "sp-idx-rule",
            "job-uid-1",
            0,
        );
        storage
            .create("/registry/pods/default/pod-0", &pod0)
            .await
            .unwrap();
        let pod4 = make_indexed_pod(
            "pod-4",
            "default",
            Phase::Succeeded,
            "sp-idx-rule",
            "job-uid-1",
            4,
        );
        storage
            .create("/registry/pods/default/pod-4", &pod4)
            .await
            .unwrap();

        // Indexes 1-3 still running
        for i in 1..4 {
            let pod = make_indexed_pod(
                &format!("pod-{}", i),
                "default",
                Phase::Running,
                "sp-idx-rule",
                "job-uid-1",
                i,
            );
            storage
                .create(&format!("/registry/pods/default/pod-{}", i), &pod)
                .await
                .unwrap();
        }

        let controller = JobController::new(storage.clone());
        let mut job: Job = storage.get(job_key).await.unwrap();
        controller.reconcile(&mut job).await.unwrap();

        let updated_job: Job = storage.get(job_key).await.unwrap();
        let status = updated_job.status.unwrap();
        let conditions = status.conditions.as_ref().unwrap();

        assert!(
            conditions
                .iter()
                .any(|c| c.condition_type == "SuccessCriteriaMet"
                    && c.status == "True"
                    && c.reason.as_deref() == Some("SuccessPolicy")),
            "SuccessCriteriaMet should be set when required indexes succeeded"
        );
        assert!(
            conditions.iter().any(|c| c.condition_type == "Complete"
                && c.status == "True"
                && c.reason.as_deref() == Some("SuccessPolicy")),
            "Complete condition should have reason SuccessPolicy"
        );
        // K8s sets terminating to 0 when the job completes
        assert_eq!(status.terminating, Some(0));
        assert_eq!(status.active, Some(0));
        assert_eq!(status.succeeded, Some(2));
        assert!(status.completion_time.is_some());
    }

    /// Test that when a Job completes via successPolicy, the status has
    /// terminating=0 (not the count of pods being terminated).
    /// K8s ref: test/e2e/apps/job.go:596 checks terminating==0
    #[tokio::test]
    async fn test_success_policy_sets_terminating_zero() {
        let storage = Arc::new(MemoryStorage::new());

        let mut job = make_job("sp-job", "default", 2, 5);
        job.spec.completion_mode = Some("Indexed".to_string());
        job.spec.success_policy = Some(serde_json::json!({
            "rules": [{"succeededCount": 1}]
        }));
        storage
            .create("/registry/jobs/default/sp-job", &job)
            .await
            .unwrap();

        let job_uid = job.metadata.uid.clone();

        // Create one succeeded pod (index 0)
        let mut pod = make_pod("sp-job-0", "default", Phase::Succeeded, "sp-job", &job_uid);
        pod.metadata.labels.as_mut().unwrap().insert(
            "batch.kubernetes.io/job-completion-index".to_string(),
            "0".to_string(),
        );
        storage
            .create("/registry/pods/default/sp-job-0", &pod)
            .await
            .unwrap();

        // Create one running pod (index 1) that will need termination
        let mut pod1 = make_pod("sp-job-1", "default", Phase::Running, "sp-job", &job_uid);
        pod1.metadata.labels.as_mut().unwrap().insert(
            "batch.kubernetes.io/job-completion-index".to_string(),
            "1".to_string(),
        );
        storage
            .create("/registry/pods/default/sp-job-1", &pod1)
            .await
            .unwrap();

        let controller = JobController::new(storage.clone());
        controller.reconcile_all().await.unwrap();

        let updated_job: Job = storage.get("/registry/jobs/default/sp-job").await.unwrap();
        let status = updated_job.status.unwrap();

        // Job should be complete via success policy
        assert!(
            status.conditions.as_ref().is_some_and(|c| c
                .iter()
                .any(|cond| cond.condition_type == "SuccessCriteriaMet" && cond.status == "True")),
            "Job should have SuccessCriteriaMet condition"
        );

        // terminating MUST be 0, not the count of pods being terminated
        assert_eq!(
            status.terminating,
            Some(0),
            "terminating should be 0 when job completes via successPolicy"
        );

        // ready should be 0
        assert_eq!(status.ready, Some(0));
    }
}
