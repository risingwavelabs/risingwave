use std::cmp::Ordering;

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::{
    HummockPinnedSnapshot, HummockPinnedVersion, HummockSnapshot, HummockVersion,
    HummockVersionRefId,
};
use risingwave_storage::hummock::{
    HummockContextId, FIRST_VERSION_ID, INVALID_EPOCH, INVALID_VERSION_ID,
};

use crate::hummock::model::CurrentHummockVersionId;
use crate::hummock::test_utils::*;
use crate::model::MetadataModel;

fn pin_versions_sum(pin_versions: &[HummockPinnedVersion]) -> usize {
    pin_versions.iter().map(|p| p.version_id.len()).sum()
}

fn pin_snapshots_sum(pin_snapshots: &[HummockPinnedSnapshot]) -> usize {
    pin_snapshots.iter().map(|p| p.snapshot_id.len()).sum()
}

#[tokio::test]
async fn test_hummock_pin_unpin() -> Result<()> {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let version_id = FIRST_VERSION_ID;
    let epoch = INVALID_EPOCH;

    assert!(HummockPinnedVersion::list(&*env.meta_store_ref())
        .await?
        .is_empty());
    for _ in 0..2 {
        let hummock_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap();
        assert_eq!(version_id, hummock_version.id);
        assert_eq!(2, hummock_version.levels.len());
        assert_eq!(0, hummock_version.levels[0].table_ids.len());
        assert_eq!(0, hummock_version.levels[1].table_ids.len());

        let pinned_versions = HummockPinnedVersion::list(&*env.meta_store_ref()).await?;
        assert_eq!(pin_versions_sum(&pinned_versions), 1);
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
        assert_eq!(
            pin_versions_sum(&HummockPinnedVersion::list(&*env.meta_store_ref()).await?),
            0
        );
    }

    assert_eq!(
        pin_snapshots_sum(&HummockPinnedSnapshot::list(&*env.meta_store_ref()).await?),
        0
    );
    for _ in 0..2 {
        let pin_result = hummock_manager
            .pin_snapshot(context_id, u64::MAX)
            .await
            .unwrap();
        assert_eq!(pin_result.epoch, epoch);
        let pinned_snapshots = HummockPinnedSnapshot::list(&*env.meta_store_ref()).await?;
        assert_eq!(pin_snapshots_sum(&pinned_snapshots), 1);
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
        assert_eq!(
            pin_snapshots_sum(&HummockPinnedSnapshot::list(&*env.meta_store_ref()).await?),
            0
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_hummock_compaction_task() -> Result<()> {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    // No compaction task available.
    let task = hummock_manager.get_compact_task().await?;
    assert_eq!(task, None);

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let table_id = 1;
    let (original_tables, _) = generate_test_tables(epoch, (table_id..table_id + 2).collect());
    hummock_manager
        .add_tables(context_id, original_tables.clone(), epoch)
        .await
        .unwrap();
    hummock_manager.commit_epoch(epoch).await.unwrap();

    // check safe epoch in hummock verison
    let version_id1 = CurrentHummockVersionId::get(env.meta_store_ref().as_ref())
        .await?
        .unwrap();
    let hummock_version1 = HummockVersion::select(
        env.meta_store_ref().as_ref(),
        &HummockVersionRefId {
            id: version_id1.id(),
        },
    )
    .await?
    .unwrap();

    // safe epoch should be INVALID before success compaction
    assert_eq!(INVALID_EPOCH, hummock_version1.safe_epoch);

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

    // check safe epoch in hummock verison
    let version_id2 = CurrentHummockVersionId::get(env.meta_store_ref().as_ref())
        .await?
        .unwrap();

    let hummock_version2 = HummockVersion::select(
        env.meta_store_ref().as_ref(),
        &HummockVersionRefId {
            id: version_id2.id(),
        },
    )
    .await?
    .unwrap();

    // safe epoch should still be INVALID since comapction task is canceled
    assert_eq!(INVALID_EPOCH, hummock_version2.safe_epoch);

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

    // check safe epoch in hummock verison after success compaction
    let version_id3 = CurrentHummockVersionId::get(env.meta_store_ref().as_ref())
        .await?
        .unwrap();

    let hummock_version3 = HummockVersion::select(
        env.meta_store_ref().as_ref(),
        &HummockVersionRefId {
            id: version_id3.id(),
        },
    )
    .await?
    .unwrap();

    // Since there is no pinned epochs, the safe epoch in version should be max_committed_epoch
    assert_eq!(epoch, hummock_version3.safe_epoch);

    Ok(())
}

#[tokio::test]
async fn test_hummock_table() -> Result<()> {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    let epoch: u64 = 1;
    let table_id = 1;
    let (original_tables, _) = generate_test_tables(epoch, (table_id..table_id + 2).collect());
    hummock_manager
        .add_tables(context_id, original_tables.clone(), epoch)
        .await
        .unwrap();
    hummock_manager.commit_epoch(epoch).await.unwrap();

    let pinned_version = hummock_manager.pin_version(context_id, u64::MAX).await?;
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
        let (tables_in_epoch1, _) =
            generate_test_tables(epoch1, (table_id..table_id + 2).collect());
        table_id += 2;
        hummock_manager
            .add_tables(context_id, tables_in_epoch1.clone(), epoch1)
            .await
            .unwrap();

        // Get tables before committing epoch1. No tables should be returned.
        let mut pinned_version = hummock_manager.pin_version(context_id, u64::MAX).await?;
        let uncommitted_epoch = pinned_version.uncommitted_epochs.first_mut().unwrap();
        assert_eq!(epoch1, uncommitted_epoch.epoch);
        assert_eq!(pinned_version.max_committed_epoch, INVALID_EPOCH);
        uncommitted_epoch.tables.sort_unstable_by_key(|e| e.id);
        assert_eq!(tables_in_epoch1, uncommitted_epoch.tables);
        assert!(get_sorted_committed_sstable_ids(&pinned_version).is_empty());

        hummock_manager
            .unpin_version(context_id, pinned_version.id)
            .await?;

        // Commit epoch1
        hummock_manager.commit_epoch(epoch1).await.unwrap();
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let pinned_version = hummock_manager.pin_version(context_id, u64::MAX).await?;
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
        let (tables_in_epoch2, _) =
            generate_test_tables(epoch2, (table_id..table_id + 2).collect());
        table_id += 2;
        hummock_manager
            .add_tables(context_id, tables_in_epoch2.clone(), epoch2)
            .await
            .unwrap();

        // Get tables before committing epoch2. tables_in_epoch1 should be returned and
        // tables_in_epoch2 should be invisible.
        let mut pinned_version = hummock_manager.pin_version(context_id, u64::MAX).await?;
        let uncommitted_epoch = pinned_version.uncommitted_epochs.first_mut().unwrap();
        assert_eq!(epoch2, uncommitted_epoch.epoch);
        uncommitted_epoch.tables.sort_unstable_by_key(|e| e.id);
        assert_eq!(tables_in_epoch2, uncommitted_epoch.tables);
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
        let pinned_version = hummock_manager.pin_version(context_id, u64::MAX).await?;
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
        let (tables_in_epoch3, _) =
            generate_test_tables(epoch3, (table_id..table_id + 2).collect());
        table_id += 2;
        hummock_manager
            .add_tables(context_id, tables_in_epoch3.clone(), epoch3)
            .await
            .unwrap();
        let (tables_in_epoch4, _) =
            generate_test_tables(epoch4, (table_id..table_id + 2).collect());
        hummock_manager
            .add_tables(context_id, tables_in_epoch4.clone(), epoch4)
            .await
            .unwrap();

        // Get tables before committing epoch3 and epoch4. tables_in_epoch1 and tables_in_epoch2
        // should be returned
        let mut pinned_version = hummock_manager.pin_version(context_id, u64::MAX).await?;
        let uncommitted_epoch3 = pinned_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch3)
            .unwrap();
        uncommitted_epoch3.tables.sort_unstable_by_key(|e| e.id);
        assert_eq!(tables_in_epoch3, uncommitted_epoch3.tables);
        let uncommitted_epoch4 = pinned_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch4)
            .unwrap();
        uncommitted_epoch4.tables.sort_unstable_by_key(|e| e.id);
        assert_eq!(tables_in_epoch4, uncommitted_epoch4.tables);
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
        let mut pinned_version = hummock_manager.pin_version(context_id, u64::MAX).await?;
        assert!(pinned_version
            .uncommitted_epochs
            .iter_mut()
            .all(|e| e.epoch != epoch3));
        let uncommitted_epoch4 = pinned_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch4)
            .unwrap();
        uncommitted_epoch4.tables.sort_unstable_by_key(|e| e.id);
        assert_eq!(tables_in_epoch4, uncommitted_epoch4.tables);
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
        let pinned_version = hummock_manager.pin_version(context_id, u64::MAX).await?;
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
        pin_versions_sum(
            &HummockPinnedVersion::list(&*env.meta_store_ref())
                .await
                .unwrap()
        ),
        0
    );
    assert_eq!(
        pin_snapshots_sum(
            &HummockPinnedSnapshot::list(&*env.meta_store_ref())
                .await
                .unwrap()
        ),
        0
    );
    hummock_manager
        .pin_version(context_id_1, u64::MAX)
        .await
        .unwrap();
    hummock_manager
        .pin_version(context_id_2, u64::MAX)
        .await
        .unwrap();
    hummock_manager
        .pin_snapshot(context_id_1, u64::MAX)
        .await
        .unwrap();
    hummock_manager
        .pin_snapshot(context_id_2, u64::MAX)
        .await
        .unwrap();
    assert_eq!(
        pin_versions_sum(
            &HummockPinnedVersion::list(&*env.meta_store_ref())
                .await
                .unwrap()
        ),
        2
    );
    assert_eq!(
        pin_snapshots_sum(
            &HummockPinnedSnapshot::list(&*env.meta_store_ref())
                .await
                .unwrap()
        ),
        2
    );
    hummock_manager
        .release_context_resource(context_id_1)
        .await
        .unwrap();
    let pinned_versions = HummockPinnedVersion::list(&*env.meta_store_ref())
        .await
        .unwrap();
    assert_eq!(pin_versions_sum(&pinned_versions), 1);
    assert_eq!(pinned_versions[0].context_id, context_id_2);
    let pinned_snapshots = HummockPinnedSnapshot::list(&*env.meta_store_ref())
        .await
        .unwrap();
    assert_eq!(pin_snapshots_sum(&pinned_snapshots), 1);
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
        pin_versions_sum(
            &HummockPinnedVersion::list(env.meta_store_ref().as_ref())
                .await
                .unwrap()
        ),
        0
    );
    assert_eq!(
        pin_snapshots_sum(
            &HummockPinnedSnapshot::list(&*env.meta_store_ref())
                .await
                .unwrap()
        ),
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
    let table_id = 1;
    let (original_tables, _) = generate_test_tables(epoch, (table_id..table_id + 2).collect());

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

    hummock_manager
        .pin_version(context_id, u64::MAX)
        .await
        .unwrap();
    // Pin multiple times is OK.
    hummock_manager
        .pin_version(context_id, u64::MAX)
        .await
        .unwrap();

    // Remove the node from cluster will invalidate context id.
    cluster_manager
        .delete_worker_node(worker_node.host.unwrap())
        .await
        .unwrap();
    // Invalid context id is rejected.
    let error = hummock_manager
        .pin_version(context_id, u64::MAX)
        .await
        .unwrap_err();
    assert!(matches!(error.inner(), ErrorCode::InternalError(_)));
    assert_eq!(error.to_string(), "internal error: transaction aborted");
}

#[tokio::test]
async fn test_hummock_manager_basic() {
    let (_env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(1).await;
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

    // test list_version_ids_asc
    assert_eq!(
        hummock_manager.list_version_ids_asc().await.unwrap().len(),
        1
    );

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let table_id = 1;
    let (original_tables, _) = generate_test_tables(epoch, (table_id..table_id + 2).collect());
    hummock_manager
        .add_tables(context_id_1, original_tables.clone(), epoch)
        .await
        .unwrap();

    // test list_version_ids_asc
    assert_eq!(
        hummock_manager.list_version_ids_asc().await.unwrap(),
        vec![FIRST_VERSION_ID, FIRST_VERSION_ID + 1]
    );

    // test get_version_pin_count
    assert_eq!(
        hummock_manager
            .get_version_pin_count(FIRST_VERSION_ID + 1)
            .await
            .unwrap(),
        0
    );
    for _ in 0..2 {
        let version = hummock_manager
            .pin_version(context_id_1, u64::MAX)
            .await
            .unwrap();
        assert_eq!(version.id, FIRST_VERSION_ID + 1);
        assert_eq!(
            hummock_manager
                .get_version_pin_count(FIRST_VERSION_ID + 1)
                .await
                .unwrap(),
            1
        );
    }

    for _ in 0..2 {
        let version = hummock_manager
            .pin_version(context_id_2, u64::MAX)
            .await
            .unwrap();
        assert_eq!(version.id, FIRST_VERSION_ID + 1);
        assert_eq!(
            hummock_manager
                .get_version_pin_count(FIRST_VERSION_ID + 1)
                .await
                .unwrap(),
            2
        );
    }

    // test get_ssts_to_delete
    assert!(hummock_manager
        .get_ssts_to_delete(FIRST_VERSION_ID)
        .await
        .unwrap()
        .is_empty());
    assert!(hummock_manager
        .get_ssts_to_delete(FIRST_VERSION_ID + 1)
        .await
        .unwrap()
        .is_empty());
    // even nonexistent version
    assert!(hummock_manager
        .get_ssts_to_delete(FIRST_VERSION_ID + 100)
        .await
        .unwrap()
        .is_empty());

    // test delete_version
    hummock_manager
        .delete_version(FIRST_VERSION_ID)
        .await
        .unwrap();
    assert_eq!(
        hummock_manager.list_version_ids_asc().await.unwrap(),
        vec![FIRST_VERSION_ID + 1]
    );
}

#[tokio::test]
async fn test_retryable_pin_version() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    // Pin a version with smallest last_pin
    // [ v0 ] -> [ v0:pinned ]
    let version = hummock_manager
        .pin_version(context_id, INVALID_VERSION_ID)
        .await
        .unwrap();
    assert_eq!(version.id, FIRST_VERSION_ID);

    let mut epoch: u64 = 1;
    // [ v0:pinned, v1, v2 ]
    for _ in 0..2 {
        let (test_tables, _) = generate_test_tables(
            epoch,
            vec![
                hummock_manager.get_new_table_id().await.unwrap(),
                hummock_manager.get_new_table_id().await.unwrap(),
            ],
        );
        // Increase the version
        hummock_manager
            .add_tables(context_id, test_tables.clone(), epoch)
            .await
            .unwrap();
        epoch += 1;
    }

    // Retry and results the same version pinned.
    // [ v0:pinned, v1, v2 ] -> [ v0:pinned, v1, v2 ]
    let version_retry = hummock_manager
        .pin_version(context_id, INVALID_VERSION_ID)
        .await
        .unwrap();
    assert_eq!(version_retry.id, version.id);

    // Use correct last_pin to pin newer version.
    // [ v0:pinned, v1, v2 ] -> [ v0:pinned, v1, v2:pinned ]
    let version_2 = hummock_manager
        .pin_version(context_id, version.id)
        .await
        .unwrap();
    assert_eq!(version_2.id, version.id + 2);

    // [ v0:pinned, v1, v2:pinned ] -> [ v0:pinned, v1, v2:pinned, v3, v4 ]
    for _ in 0..2 {
        let (test_tables, _) = generate_test_tables(
            epoch,
            vec![
                hummock_manager.get_new_table_id().await.unwrap(),
                hummock_manager.get_new_table_id().await.unwrap(),
            ],
        );
        // Increase the version
        hummock_manager
            .add_tables(context_id, test_tables.clone(), epoch)
            .await
            .unwrap();
        epoch += 1;
    }

    // Retry and results the same version pinned.
    // [ v0:pinned, v1, v2:pinned, v3, v4 ] -> [ v0:pinned, v1, v2:pinned, v3, v4 ]
    let version_2_retry = hummock_manager
        .pin_version(context_id, version.id)
        .await
        .unwrap();
    assert_eq!(version_2_retry.id, version_2.id);

    // Use None as last_pin to pin greatest version
    // [ v0:pinned, v1, v2:pinned, v3, v4 ] -> [ v0:pinned, v1, v2:pinned, v3, v4:pinned ]
    let version_3 = hummock_manager
        .pin_version(context_id, u64::MAX)
        .await
        .unwrap();
    assert_eq!(version_3.id, version.id + 4);
}

#[tokio::test]
async fn test_retryable_pin_snapshot() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    let mut epoch: u64 = 1;
    let (test_tables, _) = generate_test_tables(
        epoch,
        vec![
            hummock_manager.get_new_table_id().await.unwrap(),
            hummock_manager.get_new_table_id().await.unwrap(),
        ],
    );
    hummock_manager
        .add_tables(context_id, test_tables.clone(), epoch)
        .await
        .unwrap();
    // [ ] -> [ e0 ]
    hummock_manager.commit_epoch(epoch).await.unwrap();
    epoch += 1;

    // Pin a snapshot with smallest last_pin
    // [ e0 ] -> [ e0:pinned ]
    let snapshot = hummock_manager
        .pin_snapshot(context_id, INVALID_EPOCH)
        .await
        .unwrap();
    assert_eq!(snapshot.epoch, epoch - 1);

    let (test_tables, _) = generate_test_tables(
        epoch,
        vec![
            hummock_manager.get_new_table_id().await.unwrap(),
            hummock_manager.get_new_table_id().await.unwrap(),
        ],
    );
    hummock_manager
        .add_tables(context_id, test_tables.clone(), epoch)
        .await
        .unwrap();
    // [ e0:pinned ] -> [ e0:pinned, e1 ]
    hummock_manager.commit_epoch(epoch).await.unwrap();
    epoch += 1;

    // Retry and results the same snapshot pinned.
    // [ e0:pinned, e1 ] -> [ e0:pinned, e1 ]
    let snapshot_retry = hummock_manager
        .pin_snapshot(context_id, INVALID_EPOCH)
        .await
        .unwrap();
    assert_eq!(snapshot_retry.epoch, snapshot.epoch);

    // Use correct last_pin to pin newer snapshot.
    // [ e0:pinned, e1 ] -> [ e0:pinned, e1:pinned ]
    let snapshot_2 = hummock_manager
        .pin_snapshot(context_id, snapshot.epoch)
        .await
        .unwrap();
    assert_eq!(snapshot_2.epoch, snapshot.epoch + 1);

    for _ in 0..2 {
        let (test_tables, _) = generate_test_tables(
            epoch,
            vec![
                hummock_manager.get_new_table_id().await.unwrap(),
                hummock_manager.get_new_table_id().await.unwrap(),
            ],
        );
        hummock_manager
            .add_tables(context_id, test_tables.clone(), epoch)
            .await
            .unwrap();
        hummock_manager.commit_epoch(epoch).await.unwrap();
        epoch += 1;
    }
    // [ e0:pinned, e1:pinned ] -> [ e0:pinned, e1:pinned, e2, e3 ]

    // Retry and results the same snapshot pinned.
    // [ e0:pinned, e1:pinned, e2, e3 ] -> [ e0:pinned, e1:pinned, e2, e3 ]
    let snapshot_2_retry = hummock_manager
        .pin_snapshot(context_id, snapshot.epoch)
        .await
        .unwrap();
    assert_eq!(snapshot_2.epoch, snapshot_2_retry.epoch);

    // Use u64::MAX as last_pin to pin greatest snapshot
    // [ e0:pinned, e1:pinned, e2, e3 ] -> [ e0:pinned, e1:pinned, e2, e3:pinned ]
    let snapshot_3 = hummock_manager
        .pin_snapshot(context_id, u64::MAX)
        .await
        .unwrap();
    assert_eq!(snapshot_3.epoch, snapshot_2.epoch + 2);
}
