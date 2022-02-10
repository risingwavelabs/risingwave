use std::cmp::Ordering;

use assert_matches::assert_matches;
use itertools::Itertools;
use prost::Message;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::hummock::{HummockVersion, KeyRange, SstableInfo};
use risingwave_storage::hummock::key::key_with_epoch;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    HummockSSTableId, HummockSnapshotId, HummockVersionId, SSTableBuilder, SSTableBuilderOptions,
    INVALID_EPOCH,
};

use super::*;
use crate::hummock;
use crate::manager::{MetaSrvEnv, SINGLE_VERSION_EPOCH};

async fn create_hummock_manager(
    env: MetaSrvEnv,
    hummock_config: &hummock::Config,
) -> Result<HummockManager> {
    let instance = HummockManager::new(env, hummock_config.clone()).await?;
    Ok(instance)
}

#[tokio::test]
async fn test_hummock_pin_unpin() -> Result<()> {
    let hummock_config = hummock::Config {
        context_ttl: 1000,
        context_check_interval: 300,
    };
    let sled_root = tempfile::tempdir().unwrap();
    let env = MetaSrvEnv::for_test_with_sled(sled_root).await;
    let hummock_manager = create_hummock_manager(env.clone(), &hummock_config).await?;
    let context_id = 0;

    let version_id = env
        .meta_store()
        .get_cf(
            HUMMOCK_DEFAULT_CF_NAME,
            HUMMOCK_VERSION_ID_LEY.as_bytes(),
            SINGLE_VERSION_EPOCH,
        )
        .await?;
    let version_id = HummockVersionId::from_be_bytes(version_id.try_into().unwrap());
    assert_eq!(0, version_id);

    for _ in 0..3 {
        let (version_id_0, hummock_version) =
            hummock_manager.pin_version(context_id).await.unwrap();
        assert_eq!(version_id, version_id_0);
        assert_eq!(2, hummock_version.levels.len());
        assert_eq!(0, hummock_version.levels[0].table_ids.len());
        assert_eq!(0, hummock_version.levels[1].table_ids.len());
    }

    for _ in 0..3 {
        hummock_manager
            .unpin_version(context_id, version_id)
            .await
            .unwrap();
    }

    let unpin_result = hummock_manager.unpin_version(context_id, version_id).await;
    assert!(unpin_result.is_err());
    assert_matches!(
        unpin_result.unwrap_err().inner(),
        ErrorCode::ItemNotFound(_)
    );

    hummock_manager.pin_version(context_id).await.unwrap();

    let pin_result = hummock_manager.pin_snapshot(context_id).await.unwrap();
    hummock_manager
        .unpin_snapshot(context_id, pin_result)
        .await
        .unwrap();

    Ok(())
}

/// Generate keys like `001_key_test_00002` with timestamp `epoch`.
fn iterator_test_key_of_epoch(table: u64, idx: usize, ts: HummockSnapshotId) -> Vec<u8> {
    // key format: {prefix_index}_version
    key_with_epoch(
        format!("{:03}_key_test_{:05}", table, idx)
            .as_bytes()
            .to_vec(),
        ts,
    )
}

fn get_sorted_sstable_ids(sstables: &[SstableInfo]) -> Vec<HummockSSTableId> {
    sstables.iter().map(|table| table.id).sorted().collect_vec()
}

fn get_sorted_committed_sstable_ids(hummock_version: HummockVersion) -> Vec<HummockSSTableId> {
    hummock_version
        .levels
        .iter()
        .flat_map(|level| level.table_ids.clone())
        .sorted()
        .collect_vec()
}

fn generate_test_tables(epoch: u64, table_id: &mut u64) -> Vec<SstableInfo> {
    // Tables to add
    let opt = SSTableBuilderOptions {
        bloom_false_positive: 0.1,
        block_size: 4096,
        table_capacity: 0,
        checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
    };

    let mut tables = vec![];
    for i in 0..2 {
        let mut b = SSTableBuilder::new(opt.clone());
        let kv_pairs = vec![
            (i, HummockValue::Put(b"test".to_vec())),
            (i * 10, HummockValue::Put(b"test".to_vec())),
        ];
        for kv in kv_pairs {
            b.add(&iterator_test_key_of_epoch(*table_id, kv.0, epoch), kv.1);
        }
        let (_data, meta) = b.finish();
        tables.push(SstableInfo {
            id: *table_id,
            key_range: Some(KeyRange {
                left: meta.smallest_key,
                right: meta.largest_key,
                inf: false,
            }),
        });
        (*table_id) += 1;
    }
    tables
}

#[tokio::test]
async fn test_hummock_table() -> Result<()> {
    let hummock_config = hummock::Config {
        context_ttl: 1000,
        context_check_interval: 300,
    };
    let sled_root = tempfile::tempdir().unwrap();
    let env = MetaSrvEnv::for_test_with_sled(sled_root).await;
    let hummock_manager = create_hummock_manager(env.clone(), &hummock_config).await?;
    let context_id = 0;

    let epoch: u64 = 1;
    let mut table_id = 1;
    let original_tables = generate_test_tables(epoch, &mut table_id);
    hummock_manager
        .add_tables(context_id, original_tables.clone(), epoch)
        .await
        .unwrap();
    hummock_manager
        .commit_epoch(context_id, epoch)
        .await
        .unwrap();

    // Confirm tables are successfully added
    let fetched_tables = env.meta_store().list_cf(HUMMOCK_TABLE_CF_NAME).await?;
    let fetched_tables: Vec<SstableInfo> = fetched_tables
        .iter()
        .map(|v| -> SstableInfo { SstableInfo::decode(v.as_slice()).unwrap() })
        .sorted_by_key(|t| t.id)
        .collect();
    assert_eq!(original_tables, fetched_tables);

    let (_, pinned_version) = hummock_manager.pin_version(context_id).await?;
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
        get_sorted_committed_sstable_ids(pinned_version)
    );

    // TODO should use strong cases after compactor is ready so that real compact_tasks are
    // reported.
    let compact_task = hummock_manager.get_compact_task(context_id).await?;
    hummock_manager
        .report_compact_task(context_id, compact_task.clone(), true)
        .await
        .unwrap();
    hummock_manager
        .report_compact_task(context_id, compact_task.clone(), false)
        .await
        .unwrap();

    Ok(())
}

#[tokio::test]
async fn test_hummock_transaction() -> Result<()> {
    let hummock_config = hummock::Config {
        context_ttl: 1000,
        context_check_interval: 300,
    };
    let sled_root = tempfile::tempdir().unwrap();
    let env = MetaSrvEnv::for_test_with_sled(sled_root).await;
    let hummock_manager = create_hummock_manager(env.clone(), &hummock_config).await?;
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
            .add_tables(context_id, tables_in_epoch1.clone(), epoch1)
            .await
            .unwrap();

        // Get tables before committing epoch1. No tables should be returned.
        let (pinned_version_id, mut pinned_version) =
            hummock_manager.pin_version(context_id).await?;
        let uncommitted_epoch = pinned_version.uncommitted_epochs.first_mut().unwrap();
        assert_eq!(epoch1, uncommitted_epoch.epoch);
        assert_eq!(pinned_version.max_committed_epoch, INVALID_EPOCH);
        uncommitted_epoch.table_ids.sort_unstable();
        let table_ids_in_epoch1: Vec<u64> = tables_in_epoch1.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch1, uncommitted_epoch.table_ids);
        assert!(get_sorted_committed_sstable_ids(pinned_version).is_empty());

        hummock_manager
            .unpin_version(context_id, pinned_version_id)
            .await?;

        // Commit epoch1
        hummock_manager
            .commit_epoch(context_id, epoch1)
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let (pinned_version_id, pinned_version) = hummock_manager.pin_version(context_id).await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(pinned_version)
        );

        hummock_manager
            .unpin_version(context_id, pinned_version_id)
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
        let (pinned_version_id, mut pinned_version) =
            hummock_manager.pin_version(context_id).await?;
        let uncommitted_epoch = pinned_version.uncommitted_epochs.first_mut().unwrap();
        assert_eq!(epoch2, uncommitted_epoch.epoch);
        uncommitted_epoch.table_ids.sort_unstable();
        let table_ids_in_epoch2: Vec<u64> = tables_in_epoch2.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch2, uncommitted_epoch.table_ids);
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, pinned_version_id)
            .await?;

        // Commit epoch2
        hummock_manager
            .commit_epoch(context_id, epoch2)
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch2);

        // Get tables after committing epoch2. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let (pinned_version_id, pinned_version) = hummock_manager.pin_version(context_id).await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch2);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, pinned_version_id)
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
        let (pinned_version_id, mut pinned_version) =
            hummock_manager.pin_version(context_id).await?;
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
            get_sorted_committed_sstable_ids(pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, pinned_version_id)
            .await?;

        // Abort epoch3
        hummock_manager
            .abort_epoch(context_id, epoch3)
            .await
            .unwrap();

        // Get tables after aborting epoch3. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let (pinned_version_id, mut pinned_version) =
            hummock_manager.pin_version(context_id).await?;
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
            get_sorted_committed_sstable_ids(pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, pinned_version_id)
            .await?;

        // Commit epoch4
        hummock_manager
            .commit_epoch(context_id, epoch4)
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch4);

        // Get tables after committing epoch4. tables_in_epoch1, tables_in_epoch2, tables_in_epoch4
        // should be returned.
        let (pinned_version_id, pinned_version) = hummock_manager.pin_version(context_id).await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch4);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, pinned_version_id)
            .await?;
    }
    Ok(())
}
