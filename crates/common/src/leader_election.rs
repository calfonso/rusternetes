use crate::{Error, Result};
use etcd_client::{Client, Compare, CompareOp, LeaseGrantOptions, TxnOp};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};

/// Leader election state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderState {
    /// This instance is the leader
    Leader,
    /// This instance is a follower
    Follower,
    /// Leadership state is unknown (initial state or connection lost)
    Unknown,
}

/// Configuration for leader election
#[derive(Debug, Clone)]
pub struct LeaderElectionConfig {
    /// Unique identifier for this instance
    pub identity: String,
    /// etcd key used for leader election
    pub lock_key: String,
    /// How long the lease should last (seconds)
    pub lease_duration: u64,
    /// How often to renew the lease (seconds, should be < lease_duration)
    pub renew_interval: u64,
    /// How often to check leadership status (seconds)
    pub retry_interval: u64,
}

impl Default for LeaderElectionConfig {
    fn default() -> Self {
        Self {
            identity: uuid::Uuid::new_v4().to_string(),
            lock_key: "/rusternetes/leader".to_string(),
            lease_duration: 15,
            renew_interval: 5,
            retry_interval: 2,
        }
    }
}

/// LeaderElector manages leader election using etcd
pub struct LeaderElector {
    client: Arc<Mutex<Client>>,
    config: LeaderElectionConfig,
    state: Arc<RwLock<LeaderState>>,
    lease_id: Arc<Mutex<Option<i64>>>,
    shutdown: Arc<RwLock<bool>>,
}

impl LeaderElector {
    /// Create a new LeaderElector
    pub async fn new(endpoints: Vec<String>, config: LeaderElectionConfig) -> Result<Self> {
        let client = Client::connect(endpoints, None)
            .await
            .map_err(|e| Error::Storage(format!("Failed to connect to etcd: {}", e)))?;

        info!(
            identity = %config.identity,
            lock_key = %config.lock_key,
            "Leader election client initialized"
        );

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            config,
            state: Arc::new(RwLock::new(LeaderState::Unknown)),
            lease_id: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(RwLock::new(false)),
        })
    }

    /// Get the current leader state
    pub async fn get_state(&self) -> LeaderState {
        self.state.read().await.clone()
    }

    /// Check if this instance is the leader
    pub async fn is_leader(&self) -> bool {
        matches!(self.get_state().await, LeaderState::Leader)
    }

    /// Get the current leader identity
    pub async fn get_leader(&self) -> Result<Option<String>> {
        let mut client = self.client.lock().await;

        let resp = client
            .get(self.config.lock_key.clone(), None)
            .await
            .map_err(|e| Error::Storage(format!("Failed to get leader: {}", e)))?;

        if let Some(kv) = resp.kvs().first() {
            let leader = kv
                .value_str()
                .map_err(|e| Error::Storage(format!("Invalid UTF-8 in leader value: {}", e)))?
                .to_string();
            Ok(Some(leader))
        } else {
            Ok(None)
        }
    }

    /// Run the leader election loop
    pub async fn run(&self) -> Result<()> {
        info!(identity = %self.config.identity, "Starting leader election");

        loop {
            // Check if we should shutdown
            if *self.shutdown.read().await {
                info!("Leader election shutting down");
                self.release_leadership().await?;
                break;
            }

            match self.attempt_leadership().await {
                Ok(true) => {
                    // We acquired or maintained leadership
                    self.run_as_leader().await?;
                }
                Ok(false) => {
                    // We are a follower
                    self.run_as_follower().await?;
                }
                Err(e) => {
                    error!(error = %e, "Error in leader election");
                    *self.state.write().await = LeaderState::Unknown;
                    sleep(Duration::from_secs(self.config.retry_interval)).await;
                }
            }
        }

        Ok(())
    }

    /// Attempt to acquire leadership
    async fn attempt_leadership(&self) -> Result<bool> {
        let mut client = self.client.lock().await;

        // Create or renew lease
        let lease_id = self.get_or_create_lease(&mut client).await?;

        // Try to acquire the lock using a transaction
        // Only set the key if it doesn't exist (version == 0) or if we already own it
        let txn = etcd_client::Txn::new()
            .when(vec![
                // Either the key doesn't exist
                Compare::version(self.config.lock_key.clone(), CompareOp::Equal, 0),
            ])
            .and_then(vec![TxnOp::put(
                self.config.lock_key.clone(),
                self.config.identity.clone(),
                Some(etcd_client::PutOptions::new().with_lease(lease_id)),
            )])
            .or_else(vec![TxnOp::get(self.config.lock_key.clone(), None)]);

        let txn_resp = client
            .txn(txn)
            .await
            .map_err(|e| Error::Storage(format!("Failed to execute transaction: {}", e)))?;

        if txn_resp.succeeded() {
            // We acquired leadership!
            let old_state = self.state.read().await.clone();
            *self.state.write().await = LeaderState::Leader;

            if old_state != LeaderState::Leader {
                info!(
                    identity = %self.config.identity,
                    "🎉 Acquired leadership"
                );
            }

            Ok(true)
        } else {
            // Someone else is the leader - check who it is
            let get_resp = client
                .get(self.config.lock_key.clone(), None)
                .await
                .map_err(|e| Error::Storage(format!("Failed to get current leader: {}", e)))?;

            if let Some(kv) = get_resp.kvs().first() {
                let current_leader = kv.value_str().ok().unwrap_or("unknown");

                // Check if the current leader is us (key exists from previous run)
                if current_leader == self.config.identity {
                    // We already own the lock, update it with our lease
                    client
                        .put(
                            self.config.lock_key.clone(),
                            self.config.identity.clone(),
                            Some(etcd_client::PutOptions::new().with_lease(lease_id)),
                        )
                        .await
                        .map_err(|e| Error::Storage(format!("Failed to update lock: {}", e)))?;

                    *self.state.write().await = LeaderState::Leader;
                    debug!("Refreshed leadership");
                    return Ok(true);
                }

                let old_state = self.state.read().await.clone();
                *self.state.write().await = LeaderState::Follower;

                if old_state == LeaderState::Leader {
                    warn!(
                        identity = %self.config.identity,
                        leader = %current_leader,
                        "Lost leadership"
                    );
                } else if old_state == LeaderState::Unknown {
                    debug!(
                        identity = %self.config.identity,
                        leader = %current_leader,
                        "Running as follower"
                    );
                }
            }

            Ok(false)
        }
    }

    /// Get or create a lease
    async fn get_or_create_lease(&self, client: &mut Client) -> Result<i64> {
        let mut lease_id_guard = self.lease_id.lock().await;

        // Check if we have an existing lease
        if let Some(lease_id) = *lease_id_guard {
            // Try to keep it alive
            match client.lease_keep_alive(lease_id).await {
                Ok(_) => {
                    debug!(lease_id = lease_id, "Renewed lease");
                    return Ok(lease_id);
                }
                Err(e) => {
                    warn!(
                        lease_id = lease_id,
                        error = %e,
                        "Failed to renew lease, creating new one"
                    );
                }
            }
        }

        // Create a new lease
        let lease = client
            .lease_grant(
                self.config.lease_duration as i64,
                Some(LeaseGrantOptions::new()),
            )
            .await
            .map_err(|e| Error::Storage(format!("Failed to create lease: {}", e)))?;

        let new_lease_id = lease.id();
        *lease_id_guard = Some(new_lease_id);
        debug!(lease_id = new_lease_id, "Created new lease");

        Ok(new_lease_id)
    }

    /// Run the leader loop - renew lease periodically
    async fn run_as_leader(&self) -> Result<()> {
        let mut renew_timer = interval(Duration::from_secs(self.config.renew_interval));
        renew_timer.tick().await; // First tick completes immediately

        loop {
            renew_timer.tick().await;

            // Check if we should shutdown
            if *self.shutdown.read().await {
                return Ok(());
            }

            // Verify we're still the leader and renew lease
            match self.attempt_leadership().await {
                Ok(true) => {
                    // Still the leader, continue
                    continue;
                }
                Ok(false) => {
                    // Lost leadership
                    warn!("Lost leadership, returning to election loop");
                    return Ok(());
                }
                Err(e) => {
                    error!(error = %e, "Error maintaining leadership");
                    *self.state.write().await = LeaderState::Unknown;
                    return Ok(());
                }
            }
        }
    }

    /// Run the follower loop - check for leader changes
    async fn run_as_follower(&self) -> Result<()> {
        sleep(Duration::from_secs(self.config.retry_interval)).await;
        Ok(())
    }

    /// Release leadership (called on shutdown)
    async fn release_leadership(&self) -> Result<()> {
        if !self.is_leader().await {
            return Ok(());
        }

        info!(identity = %self.config.identity, "Releasing leadership");

        let mut client = self.client.lock().await;

        // Only delete if we're the current leader
        let txn = etcd_client::Txn::new()
            .when(vec![Compare::value(
                self.config.lock_key.clone(),
                CompareOp::Equal,
                self.config.identity.as_bytes(),
            )])
            .and_then(vec![TxnOp::delete(self.config.lock_key.clone(), None)])
            .or_else(vec![]);

        let txn_resp = client
            .txn(txn)
            .await
            .map_err(|e| Error::Storage(format!("Failed to release leadership: {}", e)))?;

        if txn_resp.succeeded() {
            info!("Successfully released leadership");
        }

        // Revoke lease
        if let Some(lease_id) = *self.lease_id.lock().await {
            if let Err(e) = client.lease_revoke(lease_id).await {
                warn!(error = %e, "Failed to revoke lease");
            }
        }

        *self.state.write().await = LeaderState::Unknown;
        *self.lease_id.lock().await = None;

        Ok(())
    }

    /// Initiate shutdown
    pub async fn shutdown(&self) {
        *self.shutdown.write().await = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        GenericImage, ImageExt, TestcontainersError,
    };

    // On runners without a Docker socket (e.g. the ARC self-hosted runners
    // that back this repo's `cargo nextest` workflow), `start_etcd` returns
    // `None` and the test prints a skip message and exits 0 instead of
    // panicking. Don't promote this back to `expect("...")` — that's the
    // regression that turned this PR's nextest job red in the first place.
    // Mirrors the soft-skip pattern in `crates/storage/src/etcd.rs`.

    /// Detects whether the host has a usable Docker (or Docker-compatible)
    /// socket. We treat any `Client(Init(...))` failure from testcontainers
    /// as "no Docker" — that variant wraps the bollard connect error, which
    /// fires both for a missing `/var/run/docker.sock` and for a refused
    /// connection to a custom `DOCKER_HOST`. Either way the test cannot run.
    fn is_docker_unavailable(err: &TestcontainersError) -> bool {
        matches!(
            err,
            TestcontainersError::Client(testcontainers::core::client::ClientError::Init(_))
        )
    }

    /// Spawn an ephemeral single-node etcd container and return both the
    /// container handle and the client endpoint string. The handle MUST be
    /// kept alive for the duration of the test — dropping it stops the
    /// container.
    ///
    /// Returns `None` when Docker isn't reachable so callers can soft-skip
    /// rather than fail the whole nextest job on Docker-less runners.
    async fn start_etcd() -> Option<(testcontainers::ContainerAsync<GenericImage>, String)> {
        // `etcd` logs `ready to serve client requests` on stderr once the
        // client port is listening. Wait for that marker so the test can
        // connect immediately on `start()` returning.
        let result = GenericImage::new("quay.io/coreos/etcd", "v3.5.17")
            .with_exposed_port(2379.tcp())
            .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
            .with_cmd([
                "etcd",
                "--listen-client-urls",
                "http://0.0.0.0:2379",
                "--advertise-client-urls",
                "http://0.0.0.0:2379",
            ])
            .start()
            .await;

        let container = match result {
            Ok(c) => c,
            Err(e) if is_docker_unavailable(&e) => {
                eprintln!("skipping leader_election integration test: Docker unavailable ({e})");
                return None;
            }
            Err(e) => panic!("failed to start etcd test container: {e}"),
        };

        let port = container
            .get_host_port_ipv4(2379)
            .await
            .expect("etcd container did not expose client port");

        let endpoint = format!("http://127.0.0.1:{port}");
        Some((container, endpoint))
    }

    #[tokio::test]
    async fn test_leader_election() {
        // Spin up an isolated etcd instance; previously this test required a
        // long-running etcd at localhost:2379 and was `#[ignore]`d.
        let Some((_etcd, endpoint)) = start_etcd().await else {
            return;
        };
        let endpoints = vec![endpoint];

        let config1 = LeaderElectionConfig {
            identity: "instance-1".to_string(),
            lock_key: "/test/leader".to_string(),
            lease_duration: 5,
            renew_interval: 2,
            retry_interval: 1,
        };

        let config2 = LeaderElectionConfig {
            identity: "instance-2".to_string(),
            lock_key: "/test/leader".to_string(),
            lease_duration: 5,
            renew_interval: 2,
            retry_interval: 1,
        };

        let elector1 = Arc::new(
            LeaderElector::new(endpoints.clone(), config1)
                .await
                .unwrap(),
        );
        let elector2 = Arc::new(
            LeaderElector::new(endpoints.clone(), config2)
                .await
                .unwrap(),
        );

        // Start both electors
        let elector1_clone = elector1.clone();
        let handle1 = tokio::spawn(async move { elector1_clone.run().await });

        let elector2_clone = elector2.clone();
        let handle2 = tokio::spawn(async move { elector2_clone.run().await });

        // Wait for leader election
        sleep(Duration::from_secs(3)).await;

        // Exactly one should be leader
        let is_leader_1 = elector1.is_leader().await;
        let is_leader_2 = elector2.is_leader().await;
        assert!(is_leader_1 ^ is_leader_2, "Exactly one should be leader");

        // Shutdown
        elector1.shutdown().await;
        elector2.shutdown().await;

        let _ = tokio::join!(handle1, handle2);
    }
}
