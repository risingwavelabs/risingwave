use std::sync::Arc;
use std::time::Duration;

use risingwave_pb::hummock::{vacuum_task, VacuumTask};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use crate::hummock::{CompactorManager, HummockManager};
use crate::storage::MetaStore;

const VACUUM_TRIGGER_INTERVAL: Duration = Duration::from_secs(10);
const VACUUM_RETRY_INTERVAL: Duration = Duration::from_secs(10);

pub struct VacuumTrigger<S> {
    hummock_manager_ref: Arc<HummockManager<S>>,
    compactor_manager_ref: Arc<CompactorManager>,
}

impl<S> VacuumTrigger<S>
where
    S: MetaStore,
{
    pub fn new(
        hummock_manager_ref: Arc<HummockManager<S>>,
        compactor_manager_ref: Arc<CompactorManager>,
    ) -> Self {
        Self {
            hummock_manager_ref,
            compactor_manager_ref,
        }
    }

    /// Start a worker to periodically vacuum hummock
    pub async fn start_vacuum_trigger(
        vacuum: VacuumTrigger<S>,
    ) -> (JoinHandle<()>, UnboundedSender<()>)
    where
        S: MetaStore,
    {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async move {
            let mut min_trigger_interval = tokio::time::interval(VACUUM_TRIGGER_INTERVAL);
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_trigger_interval.tick() => {},
                    // Shutdown vacuum
                    _ = shutdown_rx.recv() => {
                        tracing::info!("vacuum is shutting down");
                        return;
                    }
                }
                match Self::vacuum_trigger_loop(&vacuum, &mut shutdown_rx).await {
                    Ok(should_continue) => {
                        if !should_continue {
                            tracing::info!("vacuum is shutting down");
                            return;
                        }
                    }
                    Err(err) => {
                        tracing::warn!("vacuum err {}", err);
                    }
                }
            }
        });
        (join_handle, shutdown_tx)
    }

    /// Return false if shutdown has been signalled.
    async fn vacuum_trigger_loop(
        vacuum: &VacuumTrigger<S>,
        shutdown_rx: &mut UnboundedReceiver<()>,
    ) -> risingwave_common::error::Result<bool> {
        let version_ids = vacuum.hummock_manager_ref.list_version_ids_asc().await?;
        // Iterate version ids in ascending order. Skip the greatest version id.
        for version_id in version_ids
            .iter()
            .take(std::cmp::max(0, version_ids.len() - 1))
        {
            let mut min_retry_interval = tokio::time::interval(VACUUM_RETRY_INTERVAL);
            'retry: loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_retry_interval.tick() => {},
                    // Shutdown vacuum
                    _ = shutdown_rx.recv() => {
                        tracing::info!("vacuum is shutting down");
                        return Ok(false);
                    }
                }

                let pin_count = vacuum
                    .hummock_manager_ref
                    .get_version_pin_count(*version_id)
                    .await?;
                if pin_count > 0 {
                    // Wait until the smallest version is not referenced.
                    continue 'retry;
                }

                // Vacuum this version in two steps. The two steps are not atomic, but they are both retryable.
                let ssts_to_delete = vacuum
                    .hummock_manager_ref
                    .get_ssts_to_delete(*version_id)
                    .await?;
                if !ssts_to_delete.is_empty() {
                    // Step 1. Delete SSTs in object store.
                    // TODO: Currently We reuse the compactor node as vacuum node.
                    // TODO: Currently we don't block step 2 until vacuum task is completed by assigned worker in step 1. This can leads to orphan SSTs if the vacuum task fails.
                    tracing::debug!("try to vacuum {} tracked SSTs", ssts_to_delete.len());
                    if !vacuum
                        .compactor_manager_ref
                        .try_assign_compact_task(
                            None,
                            Some(VacuumTask {
                                task: Some(vacuum_task::Task::Tracked(
                                    vacuum_task::VacuumTrackedData {
                                        sstable_ids: ssts_to_delete,
                                    },
                                )),
                            }),
                        )
                        .await
                    {
                        // Wait until there is an available vacuum worker.
                        continue 'retry;
                    }
                }

                // Step 2. Delete version in meta store.
                // TODO delete in batch
                vacuum
                    .hummock_manager_ref
                    .delete_version(*version_id)
                    .await?;
                break 'retry;
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::hummock::test_utils::setup_compute_env;
    use crate::hummock::{CompactorManager, VacuumTrigger};

    #[tokio::test]
    async fn test_shutdown_vacuum() {
        let (_env, hummock_manager, _cluster_manager, _worker_node) = setup_compute_env(80).await;
        let compactor_manager = Arc::new(CompactorManager::new());
        let vacuum = VacuumTrigger::new(hummock_manager, compactor_manager);
        let (join_handle, shutdown_sender) = VacuumTrigger::start_vacuum_trigger(vacuum).await;
        shutdown_sender.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
