use chrono::Utc;
use futures::StreamExt;
use rusternetes_common::resources::{
    Event, EventSeries, EventSource, EventType, ObjectReference, Pod,
};
use rusternetes_storage::{build_prefix, Storage, WorkQueue, RECONCILE_ALL_SENTINEL};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// EventsController creates events for pod lifecycle changes
pub struct EventsController<S: Storage> {
    storage: Arc<S>,
    sync_interval: Duration,
}

impl<S: Storage + 'static> EventsController<S> {
    /// Minimum time that must elapse between two recorded occurrences of an
    /// otherwise-identical event before the polling reconciler bumps its
    /// `count`. Prevents per-resync inflation while still letting genuinely
    /// recurring events accumulate.
    const EVENT_DEDUP_MIN_INTERVAL: chrono::Duration = chrono::Duration::seconds(60);

    pub fn new(storage: Arc<S>, sync_interval_secs: u64) -> Self {
        Self {
            storage,
            sync_interval: Duration::from_secs(sync_interval_secs),
        }
    }

    /// Watch-based run loop. Watches events as primary resource.
    /// Falls back to periodic resync every 30s.
    pub async fn run(self: Arc<Self>) {
        println!(
            "Starting Events controller (sync interval: {:?})",
            self.sync_interval
        );

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            queue.add(RECONCILE_ALL_SENTINEL.into()).await;

            let prefix = build_prefix("events", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    eprintln!("Failed to establish watch: {}, retrying", e);
                    sleep(self.sync_interval).await;
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
                            Some(Ok(_)) => {
                                queue.add(RECONCILE_ALL_SENTINEL.into()).await;
                            }
                            Some(Err(e)) => {
                                eprintln!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                eprintln!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        queue.add(RECONCILE_ALL_SENTINEL.into()).await;
                    }
                }
            }
        }
    }

    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let result = self.reconcile_all().await.map_err(|e| e.to_string());
            match result {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    eprintln!("reconcile_all error: {}", e);
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// Reconcile all pods and create events for lifecycle changes
    pub async fn reconcile_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Get all pods
        let pods: Vec<Pod> = self.storage.list("/registry/pods/").await?;

        for pod in pods {
            if let Err(e) = self.reconcile_pod_events(&pod).await {
                eprintln!(
                    "Error reconciling events for pod {}: {}",
                    &pod.metadata.name, e
                );
            }
        }

        // Clean up old events (older than 1 hour)
        self.cleanup_old_events().await?;

        Ok(())
    }

    /// Reconcile events for a single pod
    async fn reconcile_pod_events(&self, pod: &Pod) -> Result<(), Box<dyn std::error::Error>> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let pod_uid = &pod.metadata.uid;

        // Create object reference for the pod
        let pod_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some(namespace.to_string()),
            name: Some(pod_name.to_string()),
            uid: Some(pod_uid.to_string()),
            api_version: Some("v1".to_string()),
            resource_version: pod.metadata.resource_version.clone(),
            field_path: None,
        };

        // Check pod phase and create appropriate events
        if let Some(status) = &pod.status {
            use rusternetes_common::types::Phase;

            match &status.phase {
                Some(Phase::Pending) => {
                    // Check if pod is scheduled
                    if let Some(spec) = &pod.spec {
                        if spec.node_name.as_deref().is_some_and(|n| !n.is_empty()) {
                            self.create_event_if_new(
                                namespace,
                                &pod_ref,
                                "Scheduled",
                                &format!(
                                    "Successfully assigned {} to {}",
                                    pod_name,
                                    spec.node_name.as_deref().unwrap_or("node")
                                ),
                                EventType::Normal,
                                Some("scheduler".to_string()),
                            )
                            .await?;
                        }
                    }
                }
                Some(Phase::Running) => {
                    self.create_event_if_new(
                        namespace,
                        &pod_ref,
                        "Started",
                        &format!("Started pod {}", pod_name),
                        EventType::Normal,
                        Some("kubelet".to_string()),
                    )
                    .await?;

                    // Create events for container statuses
                    if let Some(container_statuses) = &status.container_statuses {
                        for container_status in container_statuses {
                            // Check for running containers
                            if matches!(&container_status.state, Some(state) if matches!(state, rusternetes_common::resources::ContainerState::Running { .. }))
                            {
                                self.create_event_if_new(
                                    namespace,
                                    &pod_ref,
                                    "Pulled",
                                    &format!(
                                        "Container image \\\"{}\\\" already present on machine",
                                        container_status.image.as_deref().unwrap_or("unknown")
                                    ),
                                    EventType::Normal,
                                    Some("kubelet".to_string()),
                                )
                                .await?;

                                self.create_event_if_new(
                                    namespace,
                                    &pod_ref,
                                    "Created",
                                    &format!("Created container {}", container_status.name),
                                    EventType::Normal,
                                    Some("kubelet".to_string()),
                                )
                                .await?;

                                self.create_event_if_new(
                                    namespace,
                                    &pod_ref,
                                    "Started",
                                    &format!("Started container {}", container_status.name),
                                    EventType::Normal,
                                    Some("kubelet".to_string()),
                                )
                                .await?;
                            }

                            // Check for restarts
                            if container_status.restart_count > 0 {
                                self.create_event_if_new(
                                    namespace,
                                    &pod_ref,
                                    "BackOff",
                                    &format!(
                                        "Back-off restarting failed container {} in pod {}",
                                        container_status.name, pod_name
                                    ),
                                    EventType::Warning,
                                    Some("kubelet".to_string()),
                                )
                                .await?;
                            }
                        }
                    }
                }
                Some(Phase::Succeeded) => {
                    // Pods that succeeded went through Running — emit Started
                    // event if not already emitted. Tests that watch for "Started"
                    // events may miss the Running phase if the pod completes quickly.
                    self.create_event_if_new(
                        namespace,
                        &pod_ref,
                        "Started",
                        &format!("Started pod {}", pod_name),
                        EventType::Normal,
                        Some("kubelet".to_string()),
                    )
                    .await?;
                    self.create_event_if_new(
                        namespace,
                        &pod_ref,
                        "Completed",
                        &format!("Pod {} completed successfully", pod_name),
                        EventType::Normal,
                        Some("kubelet".to_string()),
                    )
                    .await?;
                }
                Some(Phase::Failed) => {
                    // Pods that failed may have started containers — emit Started
                    if status.container_statuses.as_ref().is_some_and(|cs| {
                        cs.iter().any(|c| {
                            matches!(
                                c.state,
                                Some(
                                    rusternetes_common::resources::ContainerState::Terminated { .. }
                                )
                            )
                        })
                    }) {
                        self.create_event_if_new(
                            namespace,
                            &pod_ref,
                            "Started",
                            &format!("Started pod {}", pod_name),
                            EventType::Normal,
                            Some("kubelet".to_string()),
                        )
                        .await?;
                    }
                    let reason = status.reason.as_deref().unwrap_or("Unknown");
                    let message = status.message.as_deref().unwrap_or("Pod failed");
                    self.create_event_if_new(
                        namespace,
                        &pod_ref,
                        "Failed",
                        &format!("Pod {} failed: {} - {}", pod_name, reason, message),
                        EventType::Warning,
                        Some("kubelet".to_string()),
                    )
                    .await?;
                }
                Some(Phase::Unknown) => {}
                Some(Phase::Active) => {
                    // Active phase for namespaces - not relevant for pod events
                }
                Some(Phase::Terminating) => {
                    // Terminating phase for namespaces - not relevant for pod events
                }
                None => {
                    // Pod has no phase set
                }
            }
        }

        Ok(())
    }

    /// Create an event if it doesn't already exist (deduplicate)
    async fn create_event_if_new(
        &self,
        namespace: &str,
        involved_object: &ObjectReference,
        reason: &str,
        message: &str,
        event_type: EventType,
        component: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Generate event name based on involved object and reason
        let event_name = Event::generate_name(involved_object, reason);

        // Check if event already exists
        let key = format!("/registry/events/{}/{}", namespace, event_name);
        if let Ok(mut existing_event) = self.storage.get::<Event>(&key).await {
            // Event already exists — this is a recurring occurrence of the
            // same (object, reason) event. Mirror upstream's eventLogger
            // de-duplication: bump `count`, advance `lastTimestamp`, and keep
            // the events.k8s.io `series` in lock-step so the event no longer
            // appears stuck at count:1.
            //
            // Guard against the polling nature of this reconciler: the loop
            // re-evaluates unchanged pod state every resync (~30s). Without a
            // throttle we would inflate the count on every pass even though
            // nothing genuinely recurred, recreating the I/O / log-spam
            // pressure the previous skip-only logic was protecting against.
            // We therefore only bump when either:
            //   - the message changed (genuinely new information), or
            //   - at least `EVENT_DEDUP_MIN_INTERVAL` has elapsed since the
            //     last recorded occurrence (a real, distinct recurrence).
            let now = Utc::now();
            let message_changed = existing_event.message != message;
            let interval_elapsed = existing_event
                .last_timestamp
                .map(|t| now - t >= Self::EVENT_DEDUP_MIN_INTERVAL)
                .unwrap_or(true);

            if !message_changed && !interval_elapsed {
                // Too soon and nothing changed — treat as the same observation.
                return Ok(());
            }

            existing_event.count = existing_event.count.saturating_add(1);
            existing_event.last_timestamp = Some(now);
            existing_event.message = message.to_string();
            // Keep the series shape (events.k8s.io/v1) consistent with count.
            existing_event.series = Some(EventSeries {
                count: existing_event.count,
                last_observed_time: now,
            });

            if let Err(e) = self.storage.update(&key, &existing_event).await {
                eprintln!(
                    "Failed to bump count for recurring event {}/{}: {}",
                    namespace, event_name, e
                );
            }
            return Ok(());
        }

        // Create new event
        let mut event = Event::new(
            event_name.clone(),
            namespace.to_string(),
            involved_object.clone(),
            reason.to_string(),
            message.to_string(),
            event_type,
        );

        // Set component if provided
        if let Some(comp) = component {
            event.source = EventSource {
                component: comp,
                host: None,
            };
        }

        // Store event
        self.storage.create(&key, &event).await?;

        println!("Created event: {} - {} - {}", namespace, reason, message);

        Ok(())
    }

    /// Clean up events older than 1 hour
    async fn cleanup_old_events(&self) -> Result<(), Box<dyn std::error::Error>> {
        let all_events: Vec<Event> = self.storage.list("/registry/events/").await?;
        let one_hour_ago = Utc::now() - chrono::Duration::hours(1);

        for event in all_events {
            // Use last_timestamp, falling back to creation_timestamp
            let event_time = event.last_timestamp.or(event.metadata.creation_timestamp);
            if event_time.is_some_and(|t| t < one_hour_ago) {
                let namespace = event.metadata.namespace.as_deref().unwrap_or("default");
                let name = &event.metadata.name;
                let key = format!("/registry/events/{}/{}", namespace, name);

                if let Err(e) = self.storage.delete(&key).await {
                    eprintln!("Failed to delete old event {}: {}", name, e);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Container, PodSpec, PodStatus};
    use rusternetes_common::types::ObjectMeta;

    fn create_test_pod(name: &str, namespace: &str, phase: &str, node_name: Option<String>) -> Pod {
        use rusternetes_common::types::{Phase, TypeMeta};

        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec: Some(PodSpec {
                init_containers: None,
                containers: vec![Container {
                    name: "test-container".to_string(),
                    image: "nginx:latest".to_string(),
                    image_pull_policy: None,
                    ports: None,
                    env: None,
                    volume_mounts: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    resources: None,
                    working_dir: None,
                    command: None,
                    args: None,
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
                restart_policy: None,
                node_selector: None,
                node_name,
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
            }),
            status: Some(PodStatus {
                phase: Some(match phase {
                    "Pending" => Phase::Pending,
                    "Running" => Phase::Running,
                    "Succeeded" => Phase::Succeeded,
                    "Failed" => Phase::Failed,
                    _ => Phase::Unknown,
                }),
                message: None,
                reason: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                host_ip: None,
                host_i_ps: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
            }),
        }
    }

    #[test]
    fn test_event_creation() {
        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("test-pod".to_string()),
            uid: Some("abc123".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        let event = Event::new(
            "test-event".to_string(),
            "default".to_string(),
            obj_ref,
            "Started".to_string(),
            "Pod started successfully".to_string(),
            EventType::Normal,
        );

        assert_eq!(event.reason, "Started");
        assert_eq!(event.message, "Pod started successfully");
        assert_eq!(event.count, 1);
    }

    #[tokio::test]
    async fn test_recurring_event_bumps_count_and_series() {
        use rusternetes_storage::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let controller = EventsController::new(storage.clone(), 5);

        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("recurring-pod".to_string()),
            uid: Some("uid-recur-1".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        // First emission creates the event at count 1.
        controller
            .create_event_if_new(
                "default",
                &obj_ref,
                "BackOff",
                "Back-off restarting failed container",
                EventType::Warning,
                Some("kubelet".to_string()),
            )
            .await
            .unwrap();

        let event_name = Event::generate_name(&obj_ref, "BackOff");
        let key = format!("/registry/events/default/{}", event_name);
        let after_first: Event = storage.get(&key).await.unwrap();
        assert_eq!(after_first.count, 1, "first emission should be count 1");
        assert!(
            after_first.series.is_none(),
            "no series before a recurrence"
        );

        // Second emission with a changed message is a genuine recurrence and
        // must bump the count (not stay stuck at 1) and populate the series.
        controller
            .create_event_if_new(
                "default",
                &obj_ref,
                "BackOff",
                "Back-off restarting failed container (attempt 2)",
                EventType::Warning,
                Some("kubelet".to_string()),
            )
            .await
            .unwrap();

        let after_second: Event = storage.get(&key).await.unwrap();
        assert!(
            after_second.count >= 2,
            "recurring event must reach count >= 2, got {}",
            after_second.count
        );
        let series = after_second
            .series
            .as_ref()
            .expect("series must be populated on a recurrence");
        assert_eq!(
            series.count, after_second.count,
            "series.count must track the event count"
        );
        assert!(
            after_second.last_timestamp.is_some(),
            "lastTimestamp must be advanced on a recurrence"
        );
    }

    #[tokio::test]
    async fn test_succeeded_pod_emits_started_event() {
        use rusternetes_common::resources::ContainerState;
        use rusternetes_common::types::Phase;
        use rusternetes_storage::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let controller = EventsController::new(storage.clone(), 5);

        let mut pod = create_test_pod(
            "quick-pod",
            "default",
            "Succeeded",
            Some("node-1".to_string()),
        );
        if let Some(ref mut status) = pod.status {
            status.phase = Some(Phase::Succeeded);
            status.container_statuses =
                Some(vec![rusternetes_common::resources::ContainerStatus {
                    name: "main".to_string(),
                    ready: false,
                    restart_count: 0,
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
                    started: Some(false),
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]);
        }
        storage
            .create("/registry/pods/default/quick-pod", &pod)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let events: Vec<Event> = storage.list("/registry/events/default/").await.unwrap();

        let has_started = events.iter().any(|e| e.reason == "Started");
        let has_completed = events.iter().any(|e| e.reason == "Completed");

        assert!(
            has_started,
            "Succeeded pod should have Started event. Events: {:?}",
            events.iter().map(|e| &e.reason).collect::<Vec<_>>()
        );
        assert!(has_completed, "Succeeded pod should have Completed event");
    }

    #[tokio::test]
    async fn test_failed_pod_with_containers_emits_started_event() {
        use rusternetes_common::resources::ContainerState;
        use rusternetes_common::types::Phase;
        use rusternetes_storage::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let controller = EventsController::new(storage.clone(), 5);

        let mut pod = create_test_pod("fail-pod", "default", "Failed", Some("node-1".to_string()));
        if let Some(ref mut status) = pod.status {
            status.phase = Some(Phase::Failed);
            status.reason = Some("Error".to_string());
            status.message = Some("Container exited with error".to_string());
            status.container_statuses =
                Some(vec![rusternetes_common::resources::ContainerStatus {
                    name: "main".to_string(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState::Terminated {
                        exit_code: 1,
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
                    started: Some(false),
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]);
        }
        storage
            .create("/registry/pods/default/fail-pod", &pod)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let events: Vec<Event> = storage.list("/registry/events/default/").await.unwrap();

        let has_started = events.iter().any(|e| e.reason == "Started");
        let has_failed = events.iter().any(|e| e.reason == "Failed");

        assert!(
            has_started,
            "Failed pod with terminated containers should have Started event. Events: {:?}",
            events.iter().map(|e| &e.reason).collect::<Vec<_>>()
        );
        assert!(has_failed, "Failed pod should have Failed event");
    }
}
