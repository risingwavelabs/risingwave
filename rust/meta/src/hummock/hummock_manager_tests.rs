use std::cmp::Ordering;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_pb::hummock::{
    HummockContextPinnedSnapshot, HummockContextPinnedVersion, HummockSnapshot, SstableInfo,
};
use risingwave_storage::hummock::{FIRST_VERSION_ID, INVALID_EPOCH};

use crate::hummock::test_utils::*;
use crate::hummock::HummockManager;
use crate::manager::MetaSrvEnv;
use crate::model::MetadataUserCfModel;
use crate::storage::DEFAULT_COLUMN_FAMILY_ID;

#[tokio::test]
async fn test_hummock_pin_unpin() -> Result<()> {
    let env = MetaSrvEnv::for_test().await;
    let hummock_manager = HummockManager::new(env.clone()).await?;
    let context_id = 0;
    let version_id = FIRST_VERSION_ID;
    let epoch = INVALID_EPOCH;

    assert!(HummockContextPinnedVersion::list_with_cf_suffix(
        &*env.meta_store_ref(),
        DEFAULT_COLUMN_FAMILY_ID
    )
    .await?
    .is_empty());
    for _ in 0..2 {
        let hummock_version = hummock_manager
            .pin_version(context_id, DEFAULT_COLUMN_FAMILY_ID)
            .await
            .unwrap();
        assert_eq!(version_id, hummock_version.id);
        assert_eq!(2, hummock_version.levels.len());
        assert_eq!(0, hummock_version.levels[0].table_ids.len());
        assert_eq!(0, hummock_version.levels[1].table_ids.len());

        let pinned_versions = HummockContextPinnedVersion::list_with_cf_suffix(
            &*env.meta_store_ref(),
            DEFAULT_COLUMN_FAMILY_ID,
        )
        .await?;
        assert_eq!(pinned_versions.len(), 1);
        assert_eq!(pinned_versions[0].context_id, context_id);
        assert_eq!(pinned_versions[0].version_id.len(), 1);
        assert_eq!(pinned_versions[0].version_id[0], version_id);
    }

    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_version(context_id, DEFAULT_COLUMN_FAMILY_ID, version_id)
            .await
            .unwrap();
        assert!(HummockContextPinnedVersion::list_with_cf_suffix(
            &*env.meta_store_ref(),
            DEFAULT_COLUMN_FAMILY_ID
        )
        .await?
        .is_empty());
    }

    assert!(HummockContextPinnedSnapshot::list_with_cf_suffix(
        &*env.meta_store_ref(),
        DEFAULT_COLUMN_FAMILY_ID
    )
    .await?
    .is_empty());
    for _ in 0..2 {
        let pin_result = hummock_manager
            .pin_snapshot(context_id, DEFAULT_COLUMN_FAMILY_ID)
            .await
            .unwrap();
        assert_eq!(pin_result.epoch, epoch);
        let pinned_snapshots = HummockContextPinnedSnapshot::list_with_cf_suffix(
            &*env.meta_store_ref(),
            DEFAULT_COLUMN_FAMILY_ID,
        )
        .await?;
        assert_eq!(pinned_snapshots.len(), 1);
        assert_eq!(pinned_snapshots[0].context_id, context_id);
        assert_eq!(pinned_snapshots[0].snapshot_id.len(), 1);
        assert_eq!(pinned_snapshots[0].snapshot_id[0], pin_result.epoch);
    }
    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot(
                context_id,
                DEFAULT_COLUMN_FAMILY_ID,
                HummockSnapshot { epoch },
            )
            .await
            .unwrap();
        assert!(HummockContextPinnedSnapshot::list_with_cf_suffix(
            &*env.meta_store_ref(),
            DEFAULT_COLUMN_FAMILY_ID
        )
        .await?
        .is_empty());
    }

    Ok(())
}

#[tokio::test]
async fn test_hummock_get_compact_task() -> Result<()> {
    let env = MetaSrvEnv::for_test().await;
    let hummock_manager = HummockManager::new(env.clone()).await?;
    let context_id = 0;

    let task = hummock_manager
        .get_compact_task(context_id, DEFAULT_COLUMN_FAMILY_ID)
        .await?;
    assert_eq!(task, None);

    let epoch: u64 = 1;
    let mut table_id = 1;
    let original_tables = generate_test_tables(epoch, &mut table_id);
    hummock_manager
        .add_tables(
            context_id,
            DEFAULT_COLUMN_FAMILY_ID,
            original_tables.clone(),
            epoch,
        )
        .await
        .unwrap();
    hummock_manager
        .commit_epoch(DEFAULT_COLUMN_FAMILY_ID, epoch)
        .await
        .unwrap();

    let task = hummock_manager
        .get_compact_task(context_id, DEFAULT_COLUMN_FAMILY_ID)
        .await?;
    let compact_task = task.unwrap();
    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );
    assert_eq!(compact_task.get_task_id(), 1);

    Ok(())
}

#[tokio::test]
async fn test_hummock_table() -> Result<()> {
    let env = MetaSrvEnv::for_test().await;
    let hummock_manager = HummockManager::new(env.clone()).await?;
    let context_id = 0;

    let epoch: u64 = 1;
    let mut table_id = 1;
    let original_tables = generate_test_tables(epoch, &mut table_id);
    hummock_manager
        .add_tables(
            context_id,
            DEFAULT_COLUMN_FAMILY_ID,
            original_tables.clone(),
            epoch,
        )
        .await
        .unwrap();
    hummock_manager
        .commit_epoch(DEFAULT_COLUMN_FAMILY_ID, epoch)
        .await
        .unwrap();

    // Confirm tables are successfully added
    let fetched_tables =
        SstableInfo::list_with_cf_suffix(&*env.meta_store_ref(), DEFAULT_COLUMN_FAMILY_ID)
            .await?
            .into_iter()
            .sorted_by_key(|t| t.id)
            .collect_vec();
    assert_eq!(original_tables, fetched_tables);

    let pinned_version = hummock_manager
        .pin_version(context_id, DEFAULT_COLUMN_FAMILY_ID)
        .await?;
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
    let env = MetaSrvEnv::for_test().await;
    let hummock_manager = HummockManager::new(env.clone()).await?;
    let context_id = 0;
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
            .add_tables(
                context_id,
                DEFAULT_COLUMN_FAMILY_ID,
                tables_in_epoch1.clone(),
                epoch1,
            )
            .await
            .unwrap();

        // Get tables before committing epoch1. No tables should be returned.
        let mut pinned_version = hummock_manager
            .pin_version(context_id, DEFAULT_COLUMN_FAMILY_ID)
            .await?;
        let uncommitted_epoch = pinned_version.uncommitted_epochs.first_mut().unwrap();
        assert_eq!(epoch1, uncommitted_epoch.epoch);
        assert_eq!(pinned_version.max_committed_epoch, INVALID_EPOCH);
        uncommitted_epoch.table_ids.sort_unstable();
        let table_ids_in_epoch1: Vec<u64> = tables_in_epoch1.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch1, uncommitted_epoch.table_ids);
        assert!(get_sorted_committed_sstable_ids(&pinned_version).is_empty());

        hummock_manager
            .unpin_version(context_id, DEFAULT_COLUMN_FAMILY_ID, pinned_version.id)
            .await?;

        // Commit epoch1
        hummock_manager
            .commit_epoch(DEFAULT_COLUMN_FAMILY_ID, epoch1)
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let pinned_version = hummock_manager
            .pin_version(context_id, DEFAULT_COLUMN_FAMILY_ID)
            .await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );

        hummock_manager
            .unpin_version(context_id, DEFAULT_COLUMN_FAMILY_ID, pinned_version.id)
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
            .add_tables(
                context_id,
                DEFAULT_COLUMN_FAMILY_ID,
                tables_in_epoch2.clone(),
                epoch2,
            )
            .await
            .unwrap();

        // Get tables before committing epoch2. tables_in_epoch1 should be returned and
        // tables_in_epoch2 should be invisible.
        let mut pinned_version = hummock_manager
            .pin_version(context_id, DEFAULT_COLUMN_FAMILY_ID)
            .await?;
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
            .unpin_version(context_id, DEFAULT_COLUMN_FAMILY_ID, pinned_version.id)
            .await?;

        // Commit epoch2
        hummock_manager
            .commit_epoch(DEFAULT_COLUMN_FAMILY_ID, epoch2)
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch2);

        // Get tables after committing epoch2. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let pinned_version = hummock_manager
            .pin_version(context_id, DEFAULT_COLUMN_FAMILY_ID)
            .await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch2);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, DEFAULT_COLUMN_FAMILY_ID, pinned_version.id)
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
            .add_tables(
                context_id,
                DEFAULT_COLUMN_FAMILY_ID,
                tables_in_epoch3.clone(),
                epoch3,
            )
            .await
            .unwrap();
        let tables_in_epoch4 = generate_test_tables(epoch4, &mut table_id);
        hummock_manager
            .add_tables(
                context_id,
                DEFAULT_COLUMN_FAMILY_ID,
                tables_in_epoch4.clone(),
                epoch4,
            )
            .await
            .unwrap();

        // Get tables before committing epoch3 and epoch4. tables_in_epoch1 and tables_in_epoch2
        // should be returned
        let mut pinned_version = hummock_manager
            .pin_version(context_id, DEFAULT_COLUMN_FAMILY_ID)
            .await?;
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
            .unpin_version(context_id, DEFAULT_COLUMN_FAMILY_ID, pinned_version.id)
            .await?;

        // Abort epoch3
        hummock_manager
            .abort_epoch(DEFAULT_COLUMN_FAMILY_ID, epoch3)
            .await
            .unwrap();

        // Get tables after aborting epoch3. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let mut pinned_version = hummock_manager
            .pin_version(context_id, DEFAULT_COLUMN_FAMILY_ID)
            .await?;
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
            .unpin_version(context_id, DEFAULT_COLUMN_FAMILY_ID, pinned_version.id)
            .await?;

        // Commit epoch4
        hummock_manager
            .commit_epoch(DEFAULT_COLUMN_FAMILY_ID, epoch4)
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch4);

        // Get tables after committing epoch4. tables_in_epoch1, tables_in_epoch2, tables_in_epoch4
        // should be returned.
        let pinned_version = hummock_manager
            .pin_version(context_id, DEFAULT_COLUMN_FAMILY_ID)
            .await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch4);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, DEFAULT_COLUMN_FAMILY_ID, pinned_version.id)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_release_context_resource() -> Result<()> {
    let env = MetaSrvEnv::for_test().await;
    let hummock_manager = Arc::new(HummockManager::new(env.clone()).await?);
    let context_id_1 = 1;
    let context_id_2 = 2;
    assert_eq!(
        HummockContextPinnedVersion::list_with_cf_suffix(
            &*env.meta_store_ref(),
            DEFAULT_COLUMN_FAMILY_ID
        )
        .await
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        HummockContextPinnedSnapshot::list_with_cf_suffix(
            &*env.meta_store_ref(),
            DEFAULT_COLUMN_FAMILY_ID
        )
        .await
        .unwrap()
        .len(),
        0
    );
    hummock_manager
        .pin_version(context_id_1, DEFAULT_COLUMN_FAMILY_ID)
        .await
        .unwrap();
    hummock_manager
        .pin_version(context_id_2, DEFAULT_COLUMN_FAMILY_ID)
        .await
        .unwrap();
    hummock_manager
        .pin_snapshot(context_id_1, DEFAULT_COLUMN_FAMILY_ID)
        .await
        .unwrap();
    hummock_manager
        .pin_snapshot(context_id_2, DEFAULT_COLUMN_FAMILY_ID)
        .await
        .unwrap();
    assert_eq!(
        HummockContextPinnedVersion::list_with_cf_suffix(
            &*env.meta_store_ref(),
            DEFAULT_COLUMN_FAMILY_ID
        )
        .await
        .unwrap()
        .len(),
        2
    );
    assert_eq!(
        HummockContextPinnedSnapshot::list_with_cf_suffix(
            &*env.meta_store_ref(),
            DEFAULT_COLUMN_FAMILY_ID
        )
        .await
        .unwrap()
        .len(),
        2
    );
    hummock_manager
        .release_all_context_resource(context_id_1)
        .await
        .unwrap();
    let pinned_versions = HummockContextPinnedVersion::list_with_cf_suffix(
        &*env.meta_store_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
    )
    .await
    .unwrap();
    assert_eq!(pinned_versions.len(), 1);
    assert_eq!(pinned_versions[0].context_id, context_id_2);
    let pinned_snapshots = HummockContextPinnedSnapshot::list_with_cf_suffix(
        &*env.meta_store_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
    )
    .await
    .unwrap();
    assert_eq!(pinned_snapshots.len(), 1);
    assert_eq!(pinned_snapshots[0].context_id, context_id_2);
    // it's OK to call again
    hummock_manager
        .release_all_context_resource(context_id_1)
        .await
        .unwrap();
    hummock_manager
        .release_all_context_resource(context_id_2)
        .await
        .unwrap();
    assert_eq!(
        HummockContextPinnedVersion::list_with_cf_suffix(
            &*env.meta_store_ref(),
            DEFAULT_COLUMN_FAMILY_ID
        )
        .await
        .unwrap()
        .len(),
        0
    );
    assert_eq!(
        HummockContextPinnedSnapshot::list_with_cf_suffix(
            &*env.meta_store_ref(),
            DEFAULT_COLUMN_FAMILY_ID
        )
        .await
        .unwrap()
        .len(),
        0
    );
    Ok(())
}
