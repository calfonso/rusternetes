/// APIService Availability Controller
///
/// Watches APIService resources and updates their Available condition
/// based on whether the backing service has ready endpoints.
///
/// K8s ref: staging/src/k8s.io/kube-aggregator/pkg/controllers/status/remote/remote_available_controller.go
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::EndpointSlice;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, warn};

pub struct APIServiceAvailabilityController<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage + 'static> APIServiceAvailabilityController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Work-queue-based run loop. Watch events enqueue resource keys;
    /// a worker task reconciles one APIService at a time with deduplication
    /// and exponential backoff on failures.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let queue = WorkQueue::new();

        // Spawn worker
        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        // Spawn secondary watch for endpointslices — changes to endpoints
        // affect APIService availability
        let ep_queue = queue.clone();
        let ep_self = Arc::clone(&self);
        tokio::spawn(async move {
            ep_self.watch_endpointslices(ep_queue).await;
        });

        // Spawn secondary watch for services
        let svc_queue = queue.clone();
        let svc_self = Arc::clone(&self);
        tokio::spawn(async move {
            svc_self.watch_services(svc_queue).await;
        });

        // Primary watch loop: enqueue keys from APIService watch events
        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("apiservices", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("APIService watch failed: {}, retrying", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(Duration::from_secs(30));
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
                                warn!("APIService watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("APIService watch stream ended, reconnecting");
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

    /// Enqueue all existing APIService keys for reconciliation.
    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self
            .storage
            .list::<serde_json::Value>("/registry/apiservices/")
            .await
        {
            Ok(apiservices) => {
                for apiservice in &apiservices {
                    if let Some(name) = apiservice
                        .pointer("/metadata/name")
                        .and_then(|v| v.as_str())
                    {
                        queue.add(format!("apiservices/{}", name)).await;
                    }
                }
            }
            Err(e) => {
                error!("Failed to list apiservices for enqueue: {}", e);
            }
        }
    }

    /// Watch endpointslices and enqueue all APIServices when endpoints change,
    /// since endpoint changes can affect APIService availability.
    async fn watch_endpointslices(&self, queue: WorkQueue) {
        loop {
            let prefix = build_prefix("endpointslices", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "EndpointSlice watch failed for apiservice controller: {}",
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            loop {
                match watch.next().await {
                    Some(Ok(_)) => {
                        // An endpoint changed — re-check all APIServices
                        self.enqueue_all(&queue).await;
                    }
                    Some(Err(e)) => {
                        warn!("EndpointSlice watch error in apiservice controller: {}", e);
                        break;
                    }
                    None => {
                        warn!("EndpointSlice watch ended in apiservice controller, reconnecting");
                        break;
                    }
                }
            }
        }
    }

    /// Watch services and enqueue all APIServices when services change.
    async fn watch_services(&self, queue: WorkQueue) {
        loop {
            let prefix = build_prefix("services", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    error!("Service watch failed for apiservice controller: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            loop {
                match watch.next().await {
                    Some(Ok(_)) => {
                        self.enqueue_all(&queue).await;
                    }
                    Some(Err(e)) => {
                        warn!("Service watch error in apiservice controller: {}", e);
                        break;
                    }
                    None => {
                        warn!("Service watch ended in apiservice controller, reconnecting");
                        break;
                    }
                }
            }
        }
    }

    /// Worker loop: pulls keys from the queue and reconciles one at a time.
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let name = key.strip_prefix("apiservices/").unwrap_or(&key);
            let storage_key = build_key("apiservices", None, name);

            match self.storage.get::<serde_json::Value>(&storage_key).await {
                Ok(apiservice) => match self.reconcile_one(&apiservice).await {
                    Ok(()) => {
                        queue.forget(&key).await;
                    }
                    Err(e) => {
                        debug!("Failed to reconcile APIService {}: {}", name, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    // APIService was deleted — nothing to reconcile
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn reconcile_one(&self, apiservice: &serde_json::Value) -> Result<()> {
        let name = apiservice
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if name.is_empty() {
            return Ok(());
        }

        // Only process APIServices with a backing service (remote, not local)
        let svc_name = match apiservice
            .pointer("/spec/service/name")
            .and_then(|v| v.as_str())
        {
            Some(n) => n.to_string(),
            None => {
                // Local APIService (no service backing) — always available
                self.update_condition(
                    name,
                    "True",
                    "Local",
                    "Local APIService is always available",
                )
                .await?;
                return Ok(());
            }
        };
        let svc_ns = apiservice
            .pointer("/spec/service/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();

        // Check if the backing service exists
        let svc_key = rusternetes_storage::build_key("services", Some(&svc_ns), &svc_name);
        let svc = match self.storage.get::<serde_json::Value>(&svc_key).await {
            Ok(s) => s,
            Err(_) => {
                self.update_condition(
                    name,
                    "False",
                    "ServiceNotFound",
                    &format!("service/{} in \"{}\" is not present", svc_name, svc_ns),
                )
                .await?;
                return Ok(());
            }
        };

        // Get the service port from APIService spec
        let apiservice_port = apiservice
            .pointer("/spec/service/port")
            .and_then(|v| v.as_i64())
            .unwrap_or(443) as i32;

        // Find the matching port name in the service
        let mut port_name = String::new();
        let mut found_port = false;
        if let Some(ports) = svc.pointer("/spec/ports").and_then(|v| v.as_array()) {
            for port in ports {
                let p = port.get("port").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                if p == apiservice_port {
                    found_port = true;
                    port_name = port
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    break;
                }
            }
        }

        if !found_port {
            self.update_condition(
                name,
                "False",
                "ServicePortError",
                &format!(
                    "service/{} in \"{}\" is not listening on port {}",
                    svc_name, svc_ns, apiservice_port
                ),
            )
            .await?;
            return Ok(());
        }

        // Check EndpointSlices for the service
        let ep_prefix = build_prefix("endpointslices", Some(&svc_ns));
        let all_slices: Vec<EndpointSlice> =
            self.storage.list(&ep_prefix).await.unwrap_or_default();

        // Filter slices belonging to this service
        let service_slices: Vec<&EndpointSlice> = all_slices
            .iter()
            .filter(|slice| {
                slice
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("kubernetes.io/service-name"))
                    .map(|sn| sn == &svc_name)
                    .unwrap_or(false)
            })
            .collect();

        if service_slices.is_empty() {
            // No endpoint slices — check Endpoints resource as fallback
            let ep_key = rusternetes_storage::build_key("endpoints", Some(&svc_ns), &svc_name);
            if let Ok(ep) = self
                .storage
                .get::<rusternetes_common::resources::Endpoints>(&ep_key)
                .await
            {
                let has_ready = ep.subsets.iter().any(|s| {
                    s.addresses
                        .as_ref()
                        .map(|addrs| !addrs.is_empty())
                        .unwrap_or(false)
                });
                if has_ready {
                    self.update_condition(name, "True", "Passed", "all checks passed")
                        .await?;
                    return Ok(());
                }
            }

            self.update_condition(
                name,
                "False",
                "EndpointsNotFound",
                &format!(
                    "cannot find endpointslices for service/{} in \"{}\"",
                    svc_name, svc_ns
                ),
            )
            .await?;
            return Ok(());
        }

        // Check for at least one ready endpoint with the matching port
        let mut has_active = false;
        'outer: for slice in &service_slices {
            let has_ready_endpoint = slice.endpoints.iter().any(|ep| {
                ep.conditions
                    .as_ref()
                    .map(|c| c.ready.unwrap_or(true)) // nil ready = ready
                    .unwrap_or(true)
            });
            if !has_ready_endpoint {
                continue;
            }
            // Check if the slice has the matching port
            for ep_port in &slice.ports {
                let ep_port_name = ep_port.name.as_deref().unwrap_or("");
                if ep_port_name == port_name && ep_port.port.is_some() {
                    has_active = true;
                    break 'outer;
                }
            }
        }

        if has_active {
            self.update_condition(name, "True", "Passed", "all checks passed")
                .await?;
        } else {
            self.update_condition(
                name,
                "False",
                "MissingEndpoints",
                &format!(
                    "endpointslices for service/{} in \"{}\" have no addresses with port name \"{}\"",
                    svc_name, svc_ns, port_name
                ),
            )
            .await?;
        }

        Ok(())
    }

    async fn update_condition(
        &self,
        apiservice_name: &str,
        status: &str,
        reason: &str,
        message: &str,
    ) -> Result<()> {
        let key = rusternetes_storage::build_key("apiservices", None, apiservice_name);
        let mut apiservice: serde_json::Value = match self.storage.get(&key).await {
            Ok(v) => v,
            Err(_) => return Ok(()), // APIService was deleted
        };

        // Check if the condition already matches — skip update if unchanged
        if let Some(conditions) = apiservice
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
        {
            for c in conditions {
                if c.get("type").and_then(|v| v.as_str()) == Some("Available")
                    && c.get("status").and_then(|v| v.as_str()) == Some(status)
                    && c.get("reason").and_then(|v| v.as_str()) == Some(reason)
                {
                    // Already up to date
                    return Ok(());
                }
            }
        }

        let now = chrono::Utc::now().to_rfc3339();
        let condition = serde_json::json!({
            "type": "Available",
            "status": status,
            "lastTransitionTime": now,
            "reason": reason,
            "message": message,
        });

        // Update the status conditions
        if let Some(status_obj) = apiservice.get_mut("status") {
            if let Some(conditions) = status_obj.get_mut("conditions") {
                if let Some(arr) = conditions.as_array_mut() {
                    // Replace existing Available condition
                    let mut found = false;
                    for c in arr.iter_mut() {
                        if c.get("type").and_then(|v| v.as_str()) == Some("Available") {
                            *c = condition.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        arr.push(condition.clone());
                    }
                }
            } else {
                status_obj["conditions"] = serde_json::json!([condition]);
            }
        } else {
            apiservice["status"] = serde_json::json!({
                "conditions": [condition]
            });
        }

        debug!(
            "Updating APIService {} Available condition: {} ({})",
            apiservice_name, status, reason
        );
        match self.storage.update(&key, &apiservice).await {
            Ok(_) => {}
            Err(rusternetes_common::Error::Conflict(_)) => {
                // Another writer raced — re-read and retry once.
                if let Ok(mut fresh) = self.storage.get::<serde_json::Value>(&key).await {
                    if let Some(status_obj) = fresh.get_mut("status") {
                        if let Some(conditions) = status_obj
                            .get_mut("conditions")
                            .and_then(|v| v.as_array_mut())
                        {
                            let mut found = false;
                            for c in conditions.iter_mut() {
                                if c.get("type").and_then(|v| v.as_str()) == Some("Available") {
                                    *c = condition.clone();
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                conditions.push(condition);
                            }
                        } else {
                            status_obj["conditions"] = serde_json::json!([condition]);
                        }
                    } else {
                        fresh["status"] = serde_json::json!({ "conditions": [condition] });
                    }
                    if let Err(e) = self.storage.update(&key, &fresh).await {
                        error!(
                            error = %e,
                            apiservice = apiservice_name,
                            "failed to update apiservice after conflict retry"
                        );
                    }
                }
            }
            Err(e) => {
                error!(
                    error = %e,
                    apiservice = apiservice_name,
                    "failed to update apiservice"
                );
            }
        }
        Ok(())
    }
}
