use std::cmp::Ordering;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};
use risingwave_pb::hummock::{
    HummockContextPinnedSnapshot, HummockContextPinnedVersion, HummockSnapshot, SstableInfo,
};
use risingwave_storage::hummock::{HummockContextId, FIRST_VERSION_ID, INVALID_EPOCH};

use crate::cluster::StoredClusterManager;
use crate::hummock::test_utils::*;
use crate::hummock::HummockManager;
use crate::manager::{MetaSrvEnv, NotificationManager};
use crate::model::MetadataModel;
use crate::rpc::metrics::MetaMetrics;
use crate::storage::MemStore;

async fn setup_compute_env(
    port: i32,
) -> (
    MetaSrvEnv<MemStore>,
    Arc<HummockManager<MemStore>>,
    Arc<StoredClusterManager<MemStore>>,
    WorkerNode,
) {
    let env = MetaSrvEnv::for_test().await;
    let hummock_manager = Arc::new(
        HummockManager::new(env.clone(), Arc::new(MetaMetrics::new()))
            .await
            .unwrap(),
    );
    let cluster_manager = Arc::new(
        StoredClusterManager::new(
            env.clone(),
            Some(hummock_manager.clone()),
            Arc::new(NotificationManager::new()),
        )
        .await
        .unwrap(),
    );
    let fake_host_address = HostAddress {
        host: "127.0.0.1".to_string(),
        port,
    };
    let (worker_node, _) = cluster_manager
        .add_worker_node(fake_host_address, WorkerType::ComputeNode)
        .await
        .unwrap();
    (env, hummock_manager, cluster_manager, worker_node)
}

#[tokio::test]
async fn test_hummock_pin_unpin() -> Result<()> {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let version_id = FIRST_VERSION_ID;
    let epoch = INVALID_EPOCH;

    assert!(HummockContextPinnedVersion::list(&*env.meta_store_ref())
        .await?
        .is_empty());
    for _ in 0..2 {
        let hummock_version = hummock_manager.pin_version(context_id).await.unwrap();
        assert_eq!(version_id, hummock_version.id);
        assert_eq!(2, hummock_version.levels.len());
        assert_eq!(0, hummock_version.levels[0].table_ids.len());
        assert_eq!(0, hummock_version.levels[1].table_ids.len());

        let pinned_versions = HummockContextPinnedVersion::list(&*env.meta_store_ref()).await?;
        assert_eq!(pinned_versions.len(), 1);
        assert_eq!(pinned_versions[0].context_id, context_id);
        assert_eq!(pinned_versions[0].version_id.len(), 1);
        assert_eq!(pinned_versions[0].version_id[0], version_id);
    }

    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_version(context_id, version_id)
            .await
            .unwrap();
        assert!(HummockContextPinnedVersion::list(&*env.meta_store_ref())
            .await?
            .is_empty());
    }

    assert!(HummockContextPinnedSnapshot::list(&*env.meta_store_ref())
        .await?
        .is_empty());
    for _ in 0..2 {
        let pin_result = hummock_manager.pin_snapshot(context_id).await.unwrap();
        assert_eq!(pin_result.epoch, epoch);
        let pinned_snapshots = HummockContextPinnedSnapshot::list(&*env.meta_store_ref()).await?;
        assert_eq!(pinned_snapshots.len(), 1);
        assert_eq!(pinned_snapshots[0].context_id, context_id);
        assert_eq!(pinned_snapshots[0].snapshot_id.len(), 1);
        assert_eq!(pinned_snapshots[0].snapshot_id[0], pin_result.epoch);
    }
    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot(context_id, HummockSnapshot { epoch })
            .await
            .unwrap();
        assert!(HummockContextPinnedSnapshot::list(&*env.meta_store_ref())
            .await?
            .is_empty());
    }

    Ok(())
}

#[tokio::test]
async fn test_hummock_compaction_task() -> Result<()> {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    // No compaction task available.
    let task = hummock_manager.get_compact_task().await?;
    assert_eq!(task, None);

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let mut table_id = 1;
    let original_tables = generate_test_tables(epoch, &mut table_id);
    hummock_manager
        .add_tables(context_id, original_tables.clone(), epoch)
        .await
        .unwrap();
    hummock_manager.commit_epoch(epoch).await.unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager.get_compact_task().await.unwrap().unwrap();
    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );
    assert_eq!(compact_task.get_task_id(), 1);

    // Cancel the task and succeed.
    assert!(hummock_manager
        .report_compact_task(compact_task.clone(), false)
        .await
        .unwrap());
    // Cancel the task and told the task is not found, which may have been processed previously.
    assert!(!hummock_manager
        .report_compact_task(compact_task.clone(), false)
        .await
        .unwrap());

    // Get a compaction task.
    let compact_task = hummock_manager.get_compact_task().await.unwrap().unwrap();
    assert_eq!(compact_task.get_task_id(), 2);
    // Finish the task and succeed.
    assert!(hummock_manager
        .report_compact_task(compact_task.clone(), true)
        .await
        .unwrap());
    // Finish the task and told the task is not found, which may have been processed previously.
    assert!(!hummock_manager
        .report_compact_task(compact_task.clone(), true)
        .await
        .unwrap());

    Ok(())
}

#[tokio::test]
async fn test_hummock_table() -> Result<()> {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    let epoch: u64 = 1;
    let mut table_id = 1;
    let original_tables = generate_test_tables(epoch, &mut table_id);
    hummock_manager
        .add_tables(context_id, original_tables.clone(), epoch)
        .await
        .unwrap();
    hummock_manager.commit_epoch(epoch).await.unwrap();

    // Confirm tables are successfully added
    let fetched_tables = SstableInfo::list(&*env.meta_store_ref())
        .await?
        .into_iter()
        .sorted_by_key(|t| t.id)
        .collect_vec();
    assert_eq!(original_tables, fetched_tables);

    let pinned_version = hummock_manager.pin_version(context_id).await?;
    assert_eq!(
        Ordering::Equal,
        pinned_version
            .levels
            .iter()
            .flat_map(|level| level.table_ids.iter())
            .copied()
            .sorted()
            .cmp(original_tables.iter().map(|ot| ot.id).sorted())
    );

    // Confirm tables got are equal to original tables
    assert_eq!(
        get_sorted_sstable_ids(&original_tables),
        get_sorted_committed_sstable_ids(&pinned_version)
    );

    Ok(())
}

#[tokio::test]
async fn test_hummock_transaction() -> Result<()> {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let mut table_id = 1;
    let mut committed_tables = vec![];

    // Add and commit tables in epoch1.
    // BEFORE:  umcommitted_epochs = [], committed_epochs = []
    // RUNNING: umcommitted_epochs = [epoch1], committed_epochs = []
    // AFTER:   umcommitted_epochs = [], committed_epochs = [epoch1]
    let epoch1: u64 = 1;
    {
        // Add tables in epoch1
        let tables_in_epoch1 = generate_test_tables(epoch1, &mut table_id);
        hummock_manager
            .add_tables(context_id, tables_in_epoch1.clone(), epoch1)
            .await
            .unwrap();

        // Get tables before committing epoch1. No tables should be returned.
        let mut pinned_version = hummock_manager.pin_version(context_id).await?;
        let uncommitted_epoch = pinned_version.uncommitted_epochs.first_mut().unwrap();
        assert_eq!(epoch1, uncommitted_epoch.epoch);
        assert_eq!(pinned_version.max_committed_epoch, INVALID_EPOCH);
        uncommitted_epoch.table_ids.sort_unstable();
        let table_ids_in_epoch1: Vec<u64> = tables_in_epoch1.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch1, uncommitted_epoch.table_ids);
        assert!(get_sorted_committed_sstable_ids(&pinned_version).is_empty());

        hummock_manager
            .unpin_version(context_id, pinned_version.id)
            .await?;

        // Commit epoch1
        hummock_manager.commit_epoch(epoch1).await.unwrap();
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let pinned_version = hummock_manager.pin_version(context_id).await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );

        hummock_manager
            .unpin_version(context_id, pinned_version.id)
            .await?;
    }

    // Add and commit tables in epoch2.
    // BEFORE:  umcommitted_epochs = [], committed_epochs = [epoch1]
    // RUNNING: umcommitted_epochs = [epoch2], committed_epochs = [epoch1]
    // AFTER:   umcommitted_epochs = [], committed_epochs = [epoch1, epoch2]
    let epoch2 = epoch1 + 1;
    {
        // Add tables in epoch2
        let tables_in_epoch2 = generate_test_tables(epoch2, &mut table_id);
        hummock_manager
            .add_tables(context_id, tables_in_epoch2.clone(), epoch2)
            .await
            .unwrap();

        // Get tables before committing epoch2. tables_in_epoch1 should be returned and
        // tables_in_epoch2 should be invisible.
        let mut pinned_version = hummock_manager.pin_version(context_id).await?;
        let uncommitted_epoch = pinned_version.uncommitted_epochs.first_mut().unwrap();
        assert_eq!(epoch2, uncommitted_epoch.epoch);
        uncommitted_epoch.table_ids.sort_unstable();
        let table_ids_in_epoch2: Vec<u64> = tables_in_epoch2.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch2, uncommitted_epoch.table_ids);
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, pinned_version.id)
            .await?;

        // Commit epoch2
        hummock_manager.commit_epoch(epoch2).await.unwrap();
        committed_tables.extend(tables_in_epoch2);

        // Get tables after committing epoch2. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let pinned_version = hummock_manager.pin_version(context_id).await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch2);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, pinned_version.id)
            .await?;
    }

    // Add tables in epoch3 and epoch4. Abort epoch3, commit epoch4.
    // BEFORE:  umcommitted_epochs = [], committed_epochs = [epoch1, epoch2]
    // RUNNING: umcommitted_epochs = [epoch3, epoch4], committed_epochs = [epoch1, epoch2]
    // AFTER:   umcommitted_epochs = [], committed_epochs = [epoch1, epoch2, epoch4]
    let epoch3 = epoch2 + 1;
    let epoch4 = epoch3 + 1;
    {
        // Add tables in epoch3 and epoch4
        let tables_in_epoch3 = generate_test_tables(epoch3, &mut table_id);
        hummock_manager
            .add_tables(context_id, tables_in_epoch3.clone(), epoch3)
            .await
            .unwrap();
        let tables_in_epoch4 = generate_test_tables(epoch4, &mut table_id);
        hummock_manager
            .add_tables(context_id, tables_in_epoch4.clone(), epoch4)
            .await
            .unwrap();

        // Get tables before committing epoch3 and epoch4. tables_in_epoch1 and tables_in_epoch2
        // should be returned
        let mut pinned_version = hummock_manager.pin_version(context_id).await?;
        let uncommitted_epoch3 = pinned_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch3)
            .unwrap();
        uncommitted_epoch3.table_ids.sort_unstable();
        let table_ids_in_epoch3: Vec<u64> = tables_in_epoch3.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch3, uncommitted_epoch3.table_ids);
        let uncommitted_epoch4 = pinned_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch4)
            .unwrap();
        uncommitted_epoch4.table_ids.sort_unstable();
        let table_ids_in_epoch4: Vec<u64> = tables_in_epoch4.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch4, uncommitted_epoch4.table_ids);
        assert_eq!(pinned_version.max_committed_epoch, epoch2);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, pinned_version.id)
            .await?;

        // Abort epoch3
        hummock_manager.abort_epoch(epoch3).await.unwrap();

        // Get tables after aborting epoch3. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let mut pinned_version = hummock_manager.pin_version(context_id).await?;
        assert!(pinned_version
            .uncommitted_epochs
            .iter_mut()
            .all(|e| e.epoch != epoch3));
        let uncommitted_epoch4 = pinned_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch4)
            .unwrap();
        uncommitted_epoch4.table_ids.sort_unstable();
        assert_eq!(table_ids_in_epoch4, uncommitted_epoch4.table_ids);
        assert_eq!(pinned_version.max_committed_epoch, epoch2);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, pinned_version.id)
            .await?;

        // Commit epoch4
        hummock_manager.commit_epoch(epoch4).await.unwrap();
        committed_tables.extend(tables_in_epoch4);

        // Get tables after committing epoch4. tables_in_epoch1, tables_in_epoch2, tables_in_epoch4
        // should be returned.
        let pinned_version = hummock_manager.pin_version(context_id).await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch4);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, pinned_version.id)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_release_context_resource() -> Result<()> {
    let (env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(1).await;
    let context_id_1 = worker_node.id;

    let fake_host_address_2 = HostAddress {
        host: "127.0.0.1".to_string(),
        port: 2,
    };
    let (worker_node_2, _) = cluster_manager
        .add_worker_node(fake_host_address_2, WorkerType::ComputeNode)
        .await
        .unwrap();
    let context_id_2 = worker_node_2.id;

    assert_eq!(
        HummockContextPinnedVersion::list(&*env.meta_store_ref())
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        HummockContextPinnedSnapshot::list(&*env.meta_store_ref())
            .await
            .unwrap()
            .len(),
        0
    );
    hummock_manager.pin_version(context_id_1).await.unwrap();
    hummock_manager.pin_version(context_id_2).await.unwrap();
    hummock_manager.pin_snapshot(context_id_1).await.unwrap();
    hummock_manager.pin_snapshot(context_id_2).await.unwrap();
    assert_eq!(
        HummockContextPinnedVersion::list(&*env.meta_store_ref())
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        HummockContextPinnedSnapshot::list(&*env.meta_store_ref())
            .await
            .unwrap()
            .len(),
        2
    );
    hummock_manager
        .release_context_resource(context_id_1)
        .await
        .unwrap();
    let pinned_versions = HummockContextPinnedVersion::list(&*env.meta_store_ref())
        .await
        .unwrap();
    assert_eq!(pinned_versions.len(), 1);
    assert_eq!(pinned_versions[0].context_id, context_id_2);
    let pinned_snapshots = HummockContextPinnedSnapshot::list(&*env.meta_store_ref())
        .await
        .unwrap();
    assert_eq!(pinned_snapshots.len(), 1);
    assert_eq!(pinned_snapshots[0].context_id, context_id_2);
    // it's OK to call again
    hummock_manager
        .release_context_resource(context_id_1)
        .await
        .unwrap();
    hummock_manager
        .release_context_resource(context_id_2)
        .await
        .unwrap();
    assert_eq!(
        HummockContextPinnedVersion::list(&*env.meta_store_ref())
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        HummockContextPinnedSnapshot::list(&*env.meta_store_ref())
            .await
            .unwrap()
            .len(),
        0
    );
    Ok(())
}

#[tokio::test]
async fn test_context_id_validation() {
    let (_env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
    let invalid_context_id = HummockContextId::MAX;
    let context_id = worker_node.id;
    let epoch: u64 = 1;
    let mut table_id = 1;
    let original_tables = generate_test_tables(epoch, &mut table_id);

    // Invalid context id is rejected.
    let error = hummock_manager
        .add_tables(invalid_context_id, original_tables.clone(), epoch)
        .await
        .unwrap_err();
    assert!(matches!(error.inner(), ErrorCode::InternalError(_)));
    assert_eq!(error.to_string(), "internal error: transaction aborted");

    // Valid context id is accepted.
    hummock_manager
        .add_tables(context_id, original_tables.clone(), epoch)
        .await
        .unwrap();

    hummock_manager.pin_version(context_id).await.unwrap();
    // Pin multiple times is OK.
    hummock_manager.pin_version(context_id).await.unwrap();

    // Remove the node from cluster will invalidate context id.
    cluster_manager
        .delete_worker_node(worker_node.host.unwrap())
        .await
        .unwrap();
    // Invalid context id is rejected.
    let error = hummock_manager.pin_version(context_id).await.unwrap_err();
    assert!(matches!(error.inner(), ErrorCode::InternalError(_)));
    assert_eq!(error.to_string(), "internal error: transaction aborted");
}
