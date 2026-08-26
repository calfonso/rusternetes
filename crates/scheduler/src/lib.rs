pub mod advanced;
#[allow(dead_code)]
pub mod framework;
#[allow(dead_code)]
pub mod plugins;
pub mod scheduler;

use rusternetes_storage::StorageBackend;
use std::sync::Arc;
use tracing::info;

/// Configuration for the scheduler component.
pub struct SchedulerConfig {
    pub interval: u64,
}

/// Run the scheduler component.
///
/// This is the main entry point for embedding the scheduler in the all-in-one binary.
/// Runs the scheduling loop until the process is terminated.
pub async fn run(storage: Arc<StorageBackend>, config: SchedulerConfig) -> anyhow::Result<()> {
    info!("Starting Rusternetes Scheduler");

    let scheduler = Arc::new(scheduler::Scheduler::new(storage, config.interval));
    scheduler.run().await?;

    Ok(())
}
