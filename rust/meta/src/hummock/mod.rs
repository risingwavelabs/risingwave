mod compaction;
mod compactor_manager;
mod hummock_manager;
#[cfg(test)]
mod hummock_manager_tests;
#[cfg(test)]
mod integration_tests;
mod level_handler;
#[cfg(test)]
mod mock_hummock_meta_client;
mod model;
#[cfg(test)]
mod test_utils;

use std::sync::Arc;
use std::time::Duration;

pub use compactor_manager::*;
pub use hummock_manager::*;
use tracing::error;

use crate::storage::MetaStore;

const COMPACT_TRIGGER_INTERVAL: Duration = Duration::from_secs(10);
/// Starts a worker to conditionally trigger compaction.
pub async fn start_compaction_loop<S>(
    hummock_manager_ref: Arc<HummockManager<S>>,
    compactor_manager_ref: Arc<CompactorManager>,
) where
    S: MetaStore,
{
    let hummock_manager_ref = Arc::downgrade(&hummock_manager_ref);
    let compactor_manager_ref = Arc::downgrade(&compactor_manager_ref);
    loop {
        tokio::time::sleep(COMPACT_TRIGGER_INTERVAL).await;

        // Get strong references to CompactorManager and HummockManager
        let compactor_manager_ref = match compactor_manager_ref.upgrade() {
            None => {
                return;
            }
            Some(compactor_manager_ref) => compactor_manager_ref,
        };
        let hummock_manager_ref = match hummock_manager_ref.upgrade() {
            None => {
                return;
            }
            Some(hummock_manager_ref) => hummock_manager_ref,
        };

        // Get a compact task and assign it to a compactor
        let compact_task = match hummock_manager_ref.get_compact_task().await {
            Ok(Some(compact_task)) => compact_task,
            Ok(None) => {
                continue;
            }
            Err(e) => {
                error!("failed to get_compact_task {}", e);
                continue;
            }
        };
        if !compactor_manager_ref
            .try_assign_compact_task(compact_task.clone())
            .await
        {
            // TODO #546: Cancel a task only requires task_id. compact_task.clone() can be
            // avoided.
            if let Err(e) = hummock_manager_ref
                .report_compact_task(compact_task, false)
                .await
            {
                error!("failed to report_compact_task {}", e);
            }
        }
    }
}
