use std::sync::Arc;
use std::time::{Duration, SystemTime};

use itertools::Itertools;
use risingwave_pb::hummock::{vacuum_task, VacuumTask};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use crate::hummock::model::INVALID_CREATE_TIMESTAMP;
use crate::hummock::{CompactorManager, HummockManager};
use crate::storage::MetaStore;

const VACUUM_TRIGGER_INTERVAL: Duration = Duration::from_secs(10);
const ORPHAN_SST_RETENTION_INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);

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
                if let Err(err) = Self::vacuum_tracked_data(&vacuum).await {
                    tracing::warn!("vacuum tracked data err {}", err);
                }
                if let Err(err) =
                    Self::vacuum_orphan_data(&vacuum, ORPHAN_SST_RETENTION_INTERVAL).await
                {
                    tracing::warn!("vacuum orphan data err {}", err);
                }
            }
        });
        (join_handle, shutdown_tx)
    }

    /// Return number of versions to delete.
    /// A tracked SST can be deleted when:
    /// - The SST is contained only in the smallest version.
    /// - And the smallest version is not the greatest version at the same time. We never vacuum the
    ///   greatest version.
    /// - And the smallest version is not being referenced, and we know it won't be referenced in
    ///   the future because only greatest version can be newly referenced.
    async fn vacuum_tracked_data(
        vacuum: &VacuumTrigger<S>,
    ) -> risingwave_common::error::Result<u64> {
        let mut vacuum_count: u64 = 0;
        let version_ids = vacuum.hummock_manager_ref.list_version_ids_asc().await?;
        // Iterate version ids in ascending order. Skip the greatest version id.
        for version_id in version_ids
            .iter()
            .take(std::cmp::max(0, version_ids.len() - 1))
        {
            let pin_count = vacuum
                .hummock_manager_ref
                .get_version_pin_count(*version_id)
                .await?;
            if pin_count > 0 {
                // The smallest version is still referenced.
                return Ok(vacuum_count);
            }

            // Vacuum tracked data in two steps. The two steps are not atomic, but they are
            // both retryable.
            let ssts_to_delete = vacuum
                .hummock_manager_ref
                .get_ssts_to_delete(*version_id)
                .await?;
            // Step 1. Delete SSTs in object store.
            // TODO: Currently We reuse the compactor node as vacuum node.
            // TODO: Currently we don't block step 2 until vacuum task is completed by
            // assigned worker in step 1. This can leads to orphan SSTs if the vacuum task
            // fails.
            if !ssts_to_delete.is_empty() {
                tracing::debug!(
                    "try to vacuum {} tracked SSTs, {:?}",
                    ssts_to_delete.len(),
                    ssts_to_delete
                );
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
                    // No compactor is available
                    return Ok(vacuum_count);
                }
            }

            // Step 2. Delete version in meta store.
            // TODO delete in batch
            vacuum
                .hummock_manager_ref
                .delete_version(*version_id)
                .await?;
            vacuum_count += 1;
        }
        Ok(vacuum_count)
    }

    /// Return number of orphan SSTs to delete.
    /// An orphan SST can be deleted when:
    /// - The SST is not tracked in meta, the `meta_create_timestamp` is not set.
    /// - And the SST has existed longer than `ORPHAN_SST_RETENTION_INTERVAL`.
    async fn vacuum_orphan_data(
        vacuum: &VacuumTrigger<S>,
        orphan_sst_retention_interval: Duration,
    ) -> risingwave_common::error::Result<u64> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let ssts_to_delete = vacuum
            .hummock_manager_ref
            .list_sstable_id_infos_asc()
            .await?
            .into_iter()
            .filter(|sstable_id_info| {
                sstable_id_info.meta_create_timestamp == INVALID_CREATE_TIMESTAMP
                    && std::cmp::max(now - sstable_id_info.id_create_timestamp, 0)
                        >= orphan_sst_retention_interval.as_secs()
            })
            .map(|sstable_id_info| sstable_id_info.id)
            .collect_vec();

        if ssts_to_delete.is_empty() {
            return Ok(0);
        }

        let ssts_to_delete_count = ssts_to_delete.len() as u64;
        if !vacuum
            .compactor_manager_ref
            .try_assign_compact_task(
                None,
                Some(VacuumTask {
                    task: Some(vacuum_task::Task::Orphan(vacuum_task::VaccumOrphanData {
                        // The sstable id doesn't necessarily have a counterpart SST file in S3, but
                        // it's OK trying to delete it.
                        sstable_ids: ssts_to_delete,
                    })),
                }),
            )
            .await
        {
            return Ok(0);
        }

        Ok(ssts_to_delete_count)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::hummock::test_utils::{generate_test_tables, setup_compute_env};
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

    #[tokio::test]
    async fn test_vacuum_tracked_data() {
        let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = Arc::new(CompactorManager::default());
        let vacuum = VacuumTrigger::new(hummock_manager.clone(), compactor_manager.clone());

        // Current state: {v0: []}
        // No tracked data to vacuum.
        assert_eq!(
            VacuumTrigger::vacuum_tracked_data(&vacuum).await.unwrap(),
            0
        );

        // Increase version by 2.
        let mut epoch: u64 = 1;
        let mut table_id = 1;
        let (test_tables, _) = generate_test_tables(epoch, &mut table_id);
        hummock_manager
            .add_tables(context_id, test_tables.clone(), epoch)
            .await
            .unwrap();
        hummock_manager.commit_epoch(epoch).await.unwrap();
        // Current state: {v0: [], v1: [test_tables uncommitted], v2: [test_tables]}

        // Vacuum v0, v1
        assert_eq!(
            VacuumTrigger::vacuum_tracked_data(&vacuum).await.unwrap(),
            2
        );
        // Current state: {v2: [test_tables]}

        // Simulate a compaction and increase version by 1.
        let mut compact_task = hummock_manager.get_compact_task().await.unwrap().unwrap();
        let (test_tables_2, _) = generate_test_tables(epoch, &mut table_id);
        compact_task.sorted_output_ssts = test_tables_2;
        hummock_manager
            .report_compact_task(compact_task, true)
            .await
            .unwrap();
        // Current state: {v2: [test_tables], v3: [test_tables_2, test_tables to_delete]}

        // Increase version by 1.
        epoch += 1;
        let (test_tables_3, _) = generate_test_tables(epoch, &mut table_id);
        hummock_manager
            .add_tables(context_id, test_tables_3.clone(), epoch)
            .await
            .unwrap();
        // Current state: {v2: [test_tables], v3: [test_tables_2, to_delete:test_tables], v4:
        // [test_tables_2, test_tables_3 uncommitted]}

        // Vacuum v2. v3 is not vacuumed because no vacuum node is available.
        assert_eq!(
            VacuumTrigger::vacuum_tracked_data(&vacuum).await.unwrap(),
            1
        );
        // Current state: {v3: [test_tables_2, to_delete:test_tables], v4: [test_tables_2,
        // test_tables_3 uncommitted]}

        let _receiver = compactor_manager.add_compactor().await;
        // Vacuum v3
        assert_eq!(
            VacuumTrigger::vacuum_tracked_data(&vacuum).await.unwrap(),
            1
        );
        // Current state: {v4: [test_tables_2, test_tables_3 uncommitted]}
    }

    #[tokio::test]
    async fn test_vacuum_orphan_data() {
        let (_env, hummock_manager, _cluster_manager, _worker_node) = setup_compute_env(80).await;
        let compactor_manager = Arc::new(CompactorManager::default());
        let vacuum = VacuumTrigger::new(hummock_manager.clone(), compactor_manager.clone());
        // 1. acquire 2 sstable ids.
        hummock_manager.get_new_table_id().await.unwrap();
        hummock_manager.get_new_table_id().await.unwrap();
        // 2. no expired sstable id.
        assert_eq!(
            VacuumTrigger::vacuum_orphan_data(&vacuum, Duration::from_secs(60))
                .await
                .unwrap(),
            0
        );
        // 3. 2 expired sstable id but no vacuum node
        assert_eq!(
            VacuumTrigger::vacuum_orphan_data(&vacuum, Duration::from_secs(0))
                .await
                .unwrap(),
            0
        );
        let _receiver = compactor_manager.add_compactor().await;
        // 4. 2 expired sstable id
        assert_eq!(
            VacuumTrigger::vacuum_orphan_data(&vacuum, Duration::from_secs(0))
                .await
                .unwrap(),
            2
        );
    }
}
