use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;

use assert_matches::assert_matches;
use itertools::Itertools;
use prost::Message;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::hummock::Table;
use risingwave_storage::hummock::key::key_with_ts;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{TableBuilder, TableBuilderOptions};
use tokio::task::JoinHandle;

use super::*;
use crate::hummock;
use crate::manager::{MetaSrvEnv, SINGLE_VERSION_EPOCH};

async fn create_hummock_manager(
    env: MetaSrvEnv,
    hummock_config: &hummock::Config,
) -> Result<(Arc<DefaultHummockManager>, JoinHandle<Result<()>>)> {
    let (instance, join_handle) = DefaultHummockManager::new(env, hummock_config.clone()).await?;
    Ok((instance, join_handle))
}

#[tokio::test]
async fn test_hummock_context_management() -> Result<()> {
    let hummock_config = hummock::Config {
        context_ttl: 1000,
        context_check_interval: 300,
    };
    let env = MetaSrvEnv::for_test_with_sled().await;
    let (hummock_manager, ..) = create_hummock_manager(env, &hummock_config).await?;
    let context = hummock_manager.create_hummock_context().await?;
    let invalidate = hummock_manager
        .invalidate_hummock_context(context.identifier)
        .await;
    assert!(invalidate.is_ok());

    let context2 = hummock_manager.create_hummock_context().await?;
    // context still valid after sleeping ttl/2
    tokio::time::sleep(Duration::from_millis(num_integer::Integer::div_ceil(
        &(hummock_config.context_ttl),
        &2,
    )))
    .await;
    let context2_refreshed = hummock_manager
        .refresh_hummock_context(context2.identifier)
        .await;
    assert!(context2_refreshed.is_ok());

    // context timeout
    tokio::time::sleep(Duration::from_millis(
        hummock_config.context_ttl + hummock_config.context_check_interval,
    ))
    .await;
    let context2_refreshed = hummock_manager
        .refresh_hummock_context(context2.identifier)
        .await;
    assert!(context2_refreshed.is_err());

    Ok(())
}

#[tokio::test]
async fn test_hummock_pin_unpin() -> Result<()> {
    let hummock_config = hummock::Config {
        context_ttl: 1000,
        context_check_interval: 300,
    };
    let env = MetaSrvEnv::for_test_with_sled().await;
    let (hummock_manager, _) = create_hummock_manager(env.clone(), &hummock_config).await?;
    let context = hummock_manager.create_hummock_context().await?;
    let manager_config = env.config();

    let version_id = env
        .meta_store()
        .get_cf(
            manager_config.get_hummock_default_cf(),
            manager_config.get_hummock_version_id_key().as_bytes(),
            SINGLE_VERSION_EPOCH,
        )
        .await?;
    let version_id = HummockVersionId::from_be_bytes(version_id.try_into().unwrap());
    assert_eq!(0, version_id);

    for _ in 0..3 {
        let pin_result = hummock_manager.pin_version(context.identifier).await;
        assert!(pin_result.is_ok());
        let (version_id_0, hummock_version) = pin_result.unwrap();
        assert_eq!(version_id, version_id_0);
        assert_eq!(2, hummock_version.levels.len());
        assert_eq!(0, hummock_version.levels[0].table_ids.len());
        assert_eq!(0, hummock_version.levels[1].table_ids.len());
    }

    for _ in 0..3 {
        let unpin_result = hummock_manager
            .unpin_version(context.identifier, version_id)
            .await;
        assert!(unpin_result.is_ok());
    }

    let unpin_result = hummock_manager
        .unpin_version(context.identifier, version_id)
        .await;
    assert!(unpin_result.is_err());
    assert_matches!(
        unpin_result.unwrap_err().inner(),
        ErrorCode::ItemNotFound(_)
    );

    let pin_result = hummock_manager.pin_version(context.identifier).await;
    assert!(pin_result.is_ok());

    let pin_result = hummock_manager.pin_snapshot(context.identifier).await;
    assert!(pin_result.is_ok());
    assert!(hummock_manager
        .unpin_snapshot(context.identifier, pin_result?)
        .await
        .is_ok());

    Ok(())
}

/// Generate keys like `001_key_test_00002` with timestamp `ts`.
pub fn iterator_test_key_of_ts(table: u64, idx: usize, ts: HummockSnapshotId) -> Vec<u8> {
    // key format: {prefix_index}_version
    key_with_ts(
        format!("{:03}_key_test_{:05}", table, idx)
            .as_bytes()
            .to_vec(),
        ts,
    )
}

fn generate_test_tables(epoch: u64, table_id: &mut u64) -> Vec<Table> {
    // Tables to add
    let opt = TableBuilderOptions {
        bloom_false_positive: 0.1,
        block_size: 4096,
        table_capacity: 0,
        checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
    };

    let mut tables = vec![];
    for i in 0..2 {
        let mut b = TableBuilder::new(opt.clone());
        let kv_pairs = vec![
            (i, HummockValue::Put(b"test".to_vec())),
            (i * 10, HummockValue::Put(b"test".to_vec())),
        ];
        for kv in kv_pairs {
            b.add(&iterator_test_key_of_ts(*table_id, kv.0, epoch), kv.1);
        }
        let (_data, meta) = b.finish();
        tables.push(Table {
            id: *table_id,
            meta: Some(meta),
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
    let env = MetaSrvEnv::for_test_with_sled().await;
    let (hummock_manager, _) = create_hummock_manager(env.clone(), &hummock_config).await?;
    let context = hummock_manager.create_hummock_context().await?;
    let manager_config = env.config();

    let epoch: u64 = 1;
    let mut table_id = 1;
    let original_tables: Vec<Table> = generate_test_tables(epoch, &mut table_id);
    let result = hummock_manager
        .add_tables(context.identifier, original_tables.clone(), epoch)
        .await;
    assert!(result.is_ok());
    let version_id = result.unwrap();
    assert_eq!(1, version_id);
    let result = hummock_manager
        .commit_epoch(context.identifier, epoch)
        .await;
    assert!(result.is_ok());

    // Confirm tables are successfully added
    let fetched_tables = env
        .meta_store()
        .list_cf(manager_config.get_hummock_table_cf())
        .await?;
    let fetched_tables: Vec<Table> = fetched_tables
        .iter()
        .map(|t| -> Table { Table::decode(t.as_slice()).unwrap() })
        .sorted_by_key(|t| t.id)
        .collect();
    assert_eq!(original_tables, fetched_tables);

    let (pinned_version_id, pinned_version) =
        hummock_manager.pin_version(context.identifier).await?;
    assert_eq!(version_id + 1, pinned_version_id);
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
    let got_tables: Vec<Table> = hummock_manager
        .get_tables(context.identifier, pinned_version)
        .await
        .map(|tv| tv.into_iter().sorted_by_key(|t| t.id).collect())?;
    assert_eq!(original_tables, got_tables);

    // TODO should use strong cases after compactor is ready so that real compact_tasks are
    // reported.
    let compact_task = hummock_manager.get_compact_task(context.identifier).await?;
    assert!(hummock_manager
        .report_compact_task(context.identifier, compact_task.clone(), true)
        .await
        .is_ok());
    assert!(hummock_manager
        .report_compact_task(context.identifier, compact_task.clone(), false)
        .await
        .is_ok());

    Ok(())
}

#[tokio::test]
async fn test_hummock_transaction() -> Result<()> {
    let hummock_config = hummock::Config {
        context_ttl: 1000,
        context_check_interval: 300,
    };
    let env = MetaSrvEnv::for_test_with_sled().await;
    let (hummock_manager, _) = create_hummock_manager(env.clone(), &hummock_config).await?;
    let context = hummock_manager.create_hummock_context().await?;
    let mut table_id = 1;
    let mut committed_tables = vec![];

    // Add and commit tables in epoch1.
    // BEFORE:  umcommitted_epochs = [], committed_epochs = []
    // RUNNING: umcommitted_epochs = [epoch1], committed_epochs = []
    // AFTER:   umcommitted_epochs = [], committed_epochs = [epoch1]
    let epoch1: u64 = 1;
    {
        // Add tables in epoch1
        let tables_in_epoch1: Vec<Table> = generate_test_tables(epoch1, &mut table_id);
        let result = hummock_manager
            .add_tables(context.identifier, tables_in_epoch1.clone(), epoch1)
            .await;
        assert!(result.is_ok());

        // Get tables before committing epoch1. No tables should be returned.
        let (pinned_version_id, mut pinned_version) =
            hummock_manager.pin_version(context.identifier).await?;
        let uncommitted_epoch = pinned_version.uncommitted_epochs.first_mut().unwrap();
        assert_eq!(epoch1, uncommitted_epoch.epoch);
        assert_eq!(pinned_version.max_committed_epoch, INVALID_EPOCH);
        uncommitted_epoch.table_ids.sort_unstable();
        let table_ids_in_epoch1: Vec<u64> = tables_in_epoch1.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch1, uncommitted_epoch.table_ids);
        let got_tables: Vec<Table> = hummock_manager
            .get_tables(context.identifier, pinned_version)
            .await
            .map(|tv| tv.into_iter().sorted_by_key(|t| t.id).collect())?;
        assert!(got_tables.is_empty());

        hummock_manager
            .unpin_version(context.identifier, pinned_version_id)
            .await?;

        // Commit epoch1
        let result = hummock_manager
            .commit_epoch(context.identifier, epoch1)
            .await;
        assert!(result.is_ok());
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let (pinned_version_id, pinned_version) =
            hummock_manager.pin_version(context.identifier).await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        let got_tables: Vec<Table> = hummock_manager
            .get_tables(context.identifier, pinned_version.clone())
            .await
            .map(|tv| tv.into_iter().sorted_by_key(|t| t.id).collect())?;
        assert_eq!(committed_tables, got_tables);
        hummock_manager
            .unpin_version(context.identifier, pinned_version_id)
            .await?;
    }

    // Add and commit tables in epoch2.
    // BEFORE:  umcommitted_epochs = [], committed_epochs = [epoch1]
    // RUNNING: umcommitted_epochs = [epoch2], committed_epochs = [epoch1]
    // AFTER:   umcommitted_epochs = [], committed_epochs = [epoch1, epoch2]
    let epoch2 = epoch1 + 1;
    {
        // Add tables in epoch2
        let tables_in_epoch2: Vec<Table> = generate_test_tables(epoch2, &mut table_id);
        let result = hummock_manager
            .add_tables(context.identifier, tables_in_epoch2.clone(), epoch2)
            .await;
        assert!(result.is_ok());

        // Get tables before committing epoch2. tables_in_epoch1 should be returned and
        // tables_in_epoch2 should be invisible.
        let (pinned_version_id, mut pinned_version) =
            hummock_manager.pin_version(context.identifier).await?;
        let uncommitted_epoch = pinned_version.uncommitted_epochs.first_mut().unwrap();
        assert_eq!(epoch2, uncommitted_epoch.epoch);
        uncommitted_epoch.table_ids.sort_unstable();
        let table_ids_in_epoch2: Vec<u64> = tables_in_epoch2.iter().map(|t| t.id).collect();
        assert_eq!(table_ids_in_epoch2, uncommitted_epoch.table_ids);
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        let got_tables: Vec<Table> = hummock_manager
            .get_tables(context.identifier, pinned_version)
            .await
            .map(|tv| tv.into_iter().sorted_by_key(|t| t.id).collect())?;
        assert_eq!(committed_tables, got_tables);
        hummock_manager
            .unpin_version(context.identifier, pinned_version_id)
            .await?;

        // Commit epoch2
        let result = hummock_manager
            .commit_epoch(context.identifier, epoch2)
            .await;
        assert!(result.is_ok());
        committed_tables.extend(tables_in_epoch2);

        // Get tables after committing epoch2. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let (pinned_version_id, pinned_version) =
            hummock_manager.pin_version(context.identifier).await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch2);
        let got_tables: Vec<Table> = hummock_manager
            .get_tables(context.identifier, pinned_version)
            .await
            .map(|tv| tv.into_iter().sorted_by_key(|t| t.id).collect())?;
        assert_eq!(committed_tables, got_tables);
        hummock_manager
            .unpin_version(context.identifier, pinned_version_id)
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
        let tables_in_epoch3: Vec<Table> = generate_test_tables(epoch3, &mut table_id);
        let result = hummock_manager
            .add_tables(context.identifier, tables_in_epoch3.clone(), epoch3)
            .await;
        assert!(result.is_ok());
        let tables_in_epoch4: Vec<Table> = generate_test_tables(epoch4, &mut table_id);
        let result = hummock_manager
            .add_tables(context.identifier, tables_in_epoch4.clone(), epoch4)
            .await;
        assert!(result.is_ok());

        // Get tables before committing epoch3 and epoch4. tables_in_epoch1 and tables_in_epoch2
        // should be returned
        let (pinned_version_id, mut pinned_version) =
            hummock_manager.pin_version(context.identifier).await?;
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
        let got_tables: Vec<Table> = hummock_manager
            .get_tables(context.identifier, pinned_version)
            .await
            .map(|tv| tv.into_iter().sorted_by_key(|t| t.id).collect())?;
        assert_eq!(committed_tables, got_tables);
        hummock_manager
            .unpin_version(context.identifier, pinned_version_id)
            .await?;

        // Abort epoch3
        let result = hummock_manager
            .abort_epoch(context.identifier, epoch3)
            .await;
        assert!(result.is_ok());

        // Get tables after aborting epoch3. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let (pinned_version_id, mut pinned_version) =
            hummock_manager.pin_version(context.identifier).await?;
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
        let got_tables: Vec<Table> = hummock_manager
            .get_tables(context.identifier, pinned_version)
            .await
            .map(|tv| tv.into_iter().sorted_by_key(|t| t.id).collect())?;
        assert_eq!(committed_tables, got_tables);
        hummock_manager
            .unpin_version(context.identifier, pinned_version_id)
            .await?;

        // Commit epoch4
        let result = hummock_manager
            .commit_epoch(context.identifier, epoch4)
            .await;
        assert!(result.is_ok());
        committed_tables.extend(tables_in_epoch4);

        // Get tables after committing epoch4. tables_in_epoch1, tables_in_epoch2, tables_in_epoch4
        // should be returned.
        let (pinned_version_id, pinned_version) =
            hummock_manager.pin_version(context.identifier).await?;
        assert!(pinned_version.uncommitted_epochs.is_empty());
        assert_eq!(pinned_version.max_committed_epoch, epoch4);
        let got_tables: Vec<Table> = hummock_manager
            .get_tables(context.identifier, pinned_version)
            .await
            .map(|tv| tv.into_iter().sorted_by_key(|t| t.id).collect())?;
        assert_eq!(committed_tables, got_tables);
        hummock_manager
            .unpin_version(context.identifier, pinned_version_id)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn test_hummock_context_tracker_shutdown() -> Result<()> {
    let hummock_config = hummock::Config {
        context_ttl: 1000,
        context_check_interval: 300,
    };
    let env = MetaSrvEnv::for_test_with_sled().await;
    let (hummock_manager_ref, join_handle) = create_hummock_manager(env, &hummock_config).await?;
    drop(hummock_manager_ref);
    let result = join_handle.await;
    assert!(result.is_ok());

    Ok(())
}
