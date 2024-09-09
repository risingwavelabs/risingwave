// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(test)]

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashMap;

use itertools::Itertools;
use prometheus::Registry;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::{test_epoch, EpochExt, INVALID_EPOCH};
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::get_compaction_group_ssts;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::{to_prost_table_stats_map, TableStats, TableStatsMap};
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockContextId, HummockEpoch, HummockSstableObjectId, HummockVersionId,
    LocalSstableInfo, FIRST_VERSION_ID,
};
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::{HummockPinnedSnapshot, HummockPinnedVersion, HummockSnapshot};
use risingwave_pb::meta::add_worker_node_request::Property;

use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::compaction::selector::{default_compaction_selector, ManualCompactionOption};
use crate::hummock::error::Error;
use crate::hummock::test_utils::*;
use crate::hummock::{HummockManager, HummockManagerRef};
use crate::manager::{MetaSrvEnv, MetaStoreImpl, WorkerId};
use crate::model::MetadataModel;
use crate::rpc::metrics::MetaMetrics;

fn pin_versions_sum(pin_versions: &[HummockPinnedVersion]) -> usize {
    pin_versions.iter().len()
}

fn pin_snapshots_epoch(pin_snapshots: &[HummockPinnedSnapshot]) -> Vec<u64> {
    pin_snapshots
        .iter()
        .map(|p| p.minimal_pinned_snapshot)
        .collect_vec()
}

fn gen_sstable_info(sst_id: u64, idx: usize, table_ids: Vec<u32>) -> SstableInfo {
    SstableInfo {
        sst_id,
        key_range: KeyRange {
            left: iterator_test_key_of_epoch(1, idx, 1).into(),
            right: iterator_test_key_of_epoch(1, idx, 1).into(),
            right_exclusive: false,
        },
        table_ids,
        object_id: sst_id,
        min_epoch: 20,
        max_epoch: 20,
        file_size: 100,
        sst_size: 100,
        ..Default::default()
    }
}

fn gen_local_sstable_info(sst_id: u64, idx: usize, table_ids: Vec<u32>) -> LocalSstableInfo {
    LocalSstableInfo {
        sst_info: gen_sstable_info(sst_id, idx, table_ids),
        table_stats: Default::default(),
    }
}
fn get_compaction_group_object_ids(
    version: &HummockVersion,
    group_id: CompactionGroupId,
) -> Vec<HummockSstableObjectId> {
    get_compaction_group_ssts(version, group_id)
        .map(|(object_id, _)| object_id)
        .collect_vec()
}

async fn list_pinned_snapshot_from_meta_store(env: &MetaSrvEnv) -> Vec<HummockPinnedSnapshot> {
    match env.meta_store_ref() {
        MetaStoreImpl::Kv(meta_store) => HummockPinnedSnapshot::list(meta_store).await.unwrap(),
        MetaStoreImpl::Sql(sql_meta_store) => {
            use risingwave_meta_model_v2::hummock_pinned_snapshot;
            use sea_orm::EntityTrait;
            hummock_pinned_snapshot::Entity::find()
                .all(&sql_meta_store.conn)
                .await
                .unwrap()
                .into_iter()
                .map(Into::into)
                .collect()
        }
    }
}

async fn list_pinned_version_from_meta_store(env: &MetaSrvEnv) -> Vec<HummockPinnedVersion> {
    match env.meta_store_ref() {
        MetaStoreImpl::Kv(meta_store) => HummockPinnedVersion::list(meta_store).await.unwrap(),
        MetaStoreImpl::Sql(sql_meta_store) => {
            use risingwave_meta_model_v2::hummock_pinned_version;
            use sea_orm::EntityTrait;
            hummock_pinned_version::Entity::find()
                .all(&sql_meta_store.conn)
                .await
                .unwrap()
                .into_iter()
                .map(Into::into)
                .collect()
        }
    }
}

#[tokio::test]
async fn test_unpin_snapshot_before() {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let epoch = test_epoch(0);

    for _ in 0..2 {
        let pin_result = hummock_manager.pin_snapshot(context_id).await.unwrap();
        assert_eq!(pin_result.committed_epoch, epoch);
        let pinned_snapshots = list_pinned_snapshot_from_meta_store(&env).await;
        assert_eq!(pinned_snapshots[0].context_id, context_id);
        assert_eq!(
            pinned_snapshots[0].minimal_pinned_snapshot,
            pin_result.committed_epoch
        );
    }

    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot_before(
                context_id,
                HummockSnapshot {
                    committed_epoch: epoch,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            pin_snapshots_epoch(&list_pinned_snapshot_from_meta_store(&env).await),
            vec![epoch]
        );
    }

    let epoch2 = epoch.next_epoch();
    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot_before(
                context_id,
                HummockSnapshot {
                    committed_epoch: epoch2,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            pin_snapshots_epoch(&list_pinned_snapshot_from_meta_store(&env).await),
            vec![epoch]
        );
    }
}

#[tokio::test]
async fn test_hummock_compaction_task() {
    let (_, hummock_manager, _, _worker_node) = setup_compute_env(80).await;
    let sst_num = 2;

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let original_tables = generate_test_sstables_with_table_id(
        epoch,
        1,
        get_sst_ids(&hummock_manager, sst_num).await,
    );
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&original_tables),
    )
    .await
    .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);
    assert_eq!(compact_task.task_id, 2);

    // Cancel the task and succeed.
    assert!(hummock_manager
        .cancel_compact_task(compact_task.task_id, TaskStatus::ManualCanceled)
        .await
        .unwrap());

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(compact_task.task_id, 3);
    // Finish the task and succeed.

    assert!(hummock_manager
        .report_compact_task(compact_task.task_id, TaskStatus::Success, vec![], None)
        .await
        .unwrap());
}

#[tokio::test]
async fn test_hummock_table() {
    let (_env, hummock_manager, _cluster_manager, _worker_node) = setup_compute_env(80).await;

    let epoch = test_epoch(1);
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&original_tables),
    )
    .await
    .unwrap();

    let pinned_version = hummock_manager.get_current_version().await;
    let levels =
        pinned_version.get_compaction_group_levels(StaticCompactionGroupId::StateDefault.into());
    assert_eq!(
        Ordering::Equal,
        levels
            .l0
            .sub_levels
            .iter()
            .chain(levels.levels.iter())
            .flat_map(|level| level.table_infos.iter())
            .map(|info| info.object_id)
            .sorted()
            .cmp(original_tables.iter().map(|ot| ot.object_id).sorted())
    );

    // Confirm tables got are equal to original tables
    assert_eq!(
        get_sorted_object_ids(&original_tables),
        get_sorted_committed_object_ids(&pinned_version)
    );
}

#[tokio::test]
async fn test_hummock_transaction() {
    let (_env, hummock_manager, _cluster_manager, _worker_node) = setup_compute_env(80).await;
    let mut committed_tables = vec![];

    // Add and commit tables in epoch1.
    // BEFORE:  committed_epochs = []
    // AFTER:   committed_epochs = [epoch1]
    let epoch1 = test_epoch(1);
    {
        // Add tables in epoch1
        let tables_in_epoch1 = generate_test_tables(epoch1, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &tables_in_epoch1,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        // Get tables before committing epoch1. No tables should be returned.
        let current_version = hummock_manager.get_current_version().await;
        assert_eq!(
            current_version.visible_table_committed_epoch(),
            INVALID_EPOCH
        );
        assert!(get_sorted_committed_object_ids(&current_version).is_empty());

        // Commit epoch1
        commit_from_meta_node(
            hummock_manager.borrow(),
            epoch1,
            to_local_sstable_info(&tables_in_epoch1),
        )
        .await
        .unwrap();
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let current_version = hummock_manager.get_current_version().await;
        assert_eq!(current_version.visible_table_committed_epoch(), epoch1);
        assert_eq!(
            get_sorted_object_ids(&committed_tables),
            get_sorted_committed_object_ids(&current_version)
        );
    }

    // Add and commit tables in epoch2.
    // BEFORE:  committed_epochs = [epoch1]
    // AFTER:   committed_epochs = [epoch1, epoch2]
    let epoch2 = epoch1.next_epoch();
    {
        // Add tables in epoch2
        let tables_in_epoch2 = generate_test_tables(epoch2, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &tables_in_epoch2,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        // Get tables before committing epoch2. tables_in_epoch1 should be returned and
        // tables_in_epoch2 should be invisible.
        let current_version = hummock_manager.get_current_version().await;
        assert_eq!(current_version.visible_table_committed_epoch(), epoch1);
        assert_eq!(
            get_sorted_object_ids(&committed_tables),
            get_sorted_committed_object_ids(&current_version)
        );

        // Commit epoch2
        commit_from_meta_node(
            hummock_manager.borrow(),
            epoch2,
            to_local_sstable_info(&tables_in_epoch2),
        )
        .await
        .unwrap();
        committed_tables.extend(tables_in_epoch2);

        // Get tables after committing epoch2. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let current_version = hummock_manager.get_current_version().await;
        assert_eq!(current_version.visible_table_committed_epoch(), epoch2);
        assert_eq!(
            get_sorted_object_ids(&committed_tables),
            get_sorted_committed_object_ids(&current_version)
        );
    }
}

#[tokio::test]
async fn test_release_context_resource() {
    let (env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(1).await;
    let context_id_1 = worker_node.id;

    let fake_host_address_2 = HostAddress {
        host: "127.0.0.1".to_string(),
        port: 2,
    };
    let fake_parallelism = 4;
    let worker_node_2 = cluster_manager
        .add_worker_node(
            WorkerType::ComputeNode,
            fake_host_address_2,
            Property {
                worker_node_parallelism: fake_parallelism,
                is_streaming: true,
                is_serving: true,
                is_unschedulable: false,
                internal_rpc_host_addr: "".to_string(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2.id;

    assert_eq!(
        pin_versions_sum(&list_pinned_version_from_meta_store(&env).await),
        0
    );
    hummock_manager.pin_version(context_id_1).await.unwrap();
    hummock_manager.pin_version(context_id_2).await.unwrap();
    hummock_manager.pin_snapshot(context_id_1).await.unwrap();
    hummock_manager.pin_snapshot(context_id_2).await.unwrap();
    assert_eq!(
        pin_versions_sum(&list_pinned_version_from_meta_store(&env).await),
        2
    );
    assert_eq!(list_pinned_version_from_meta_store(&env).await.len(), 2);
    hummock_manager
        .release_contexts(&vec![context_id_1])
        .await
        .unwrap();
    let pinned_versions = list_pinned_version_from_meta_store(&env).await;
    assert_eq!(pin_versions_sum(&pinned_versions), 1);
    assert_eq!(pinned_versions[0].context_id, context_id_2);
    let pinned_snapshots = list_pinned_snapshot_from_meta_store(&env).await;
    assert_eq!(pinned_snapshots[0].context_id, context_id_2);
    // it's OK to call again
    hummock_manager
        .release_contexts(&vec![context_id_1])
        .await
        .unwrap();
    hummock_manager
        .release_contexts(&vec![context_id_2])
        .await
        .unwrap();
    assert_eq!(
        pin_versions_sum(&list_pinned_version_from_meta_store(&env).await),
        0
    );
}

#[tokio::test]
async fn test_context_id_validation() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let invalid_context_id = HummockContextId::MAX;
    let context_id = worker_node.id;

    // Invalid context id is rejected.
    let error = hummock_manager
        .pin_version(invalid_context_id)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidContext(_)));

    // Valid context id is accepted.
    hummock_manager.pin_version(context_id).await.unwrap();
    // Pin multiple times is OK.
    hummock_manager.pin_version(context_id).await.unwrap();
}

#[tokio::test]
async fn test_hummock_manager_basic() {
    let (_env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(1).await;
    let context_id_1 = worker_node.id;

    let fake_host_address_2 = HostAddress {
        host: "127.0.0.1".to_string(),
        port: 2,
    };
    let fake_parallelism = 4;
    let worker_node_2 = cluster_manager
        .add_worker_node(
            WorkerType::ComputeNode,
            fake_host_address_2,
            Property {
                worker_node_parallelism: fake_parallelism,
                is_streaming: true,
                is_serving: true,
                is_unschedulable: false,
                internal_rpc_host_addr: "".to_string(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2.id;

    // initial version id
    assert_eq!(
        hummock_manager.get_current_version().await.id,
        FIRST_VERSION_ID
    );

    let mut epoch = test_epoch(1);
    let mut register_log_count = 0;
    let mut commit_log_count = 0;
    let commit_one = |epoch: HummockEpoch, hummock_manager: HummockManagerRef| async move {
        let original_tables =
            generate_test_tables(test_epoch(epoch), get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &original_tables,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        commit_from_meta_node(
            hummock_manager.borrow(),
            epoch,
            to_local_sstable_info(&original_tables),
        )
        .await
        .unwrap();
    };

    commit_one(epoch, hummock_manager.clone()).await;
    register_log_count += 1;
    commit_log_count += 1;
    epoch.inc_epoch();

    let init_version_id = FIRST_VERSION_ID;

    // increased version id
    assert_eq!(
        hummock_manager.get_current_version().await.id,
        init_version_id + commit_log_count + register_log_count
    );

    // min pinned version id if no clients
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );
    for _ in 0..2 {
        hummock_manager
            .unpin_version_before(context_id_1, HummockVersionId::MAX)
            .await
            .unwrap();
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            HummockVersionId::MAX
        );

        // should pin latest because u64::MAX
        let version = hummock_manager.pin_version(context_id_1).await.unwrap();
        assert_eq!(
            version.id,
            init_version_id + commit_log_count + register_log_count
        );
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            init_version_id + commit_log_count + register_log_count
        );
    }

    commit_one(epoch, hummock_manager.clone()).await;
    commit_log_count += 1;
    register_log_count += 1;

    for _ in 0..2 {
        // should pin latest because deltas cannot contain INVALID_EPOCH
        let version = hummock_manager.pin_version(context_id_2).await.unwrap();
        assert_eq!(
            version.id,
            init_version_id + commit_log_count + register_log_count
        );
        // pinned by context_id_1
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await + 2,
            init_version_id + commit_log_count + register_log_count,
        );
    }
    // objects_to_delete is always empty because no compaction is ever invoked.
    assert!(hummock_manager.get_objects_to_delete().is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (0, 0)
    );
    assert_eq!(
        hummock_manager.create_version_checkpoint(1).await.unwrap(),
        commit_log_count + register_log_count
    );
    assert!(hummock_manager.get_objects_to_delete().is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        ((commit_log_count + register_log_count) as usize, 0)
    );
    hummock_manager
        .unpin_version_before(context_id_1, HummockVersionId::MAX)
        .await
        .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        init_version_id + commit_log_count + register_log_count
    );
    hummock_manager
        .unpin_version_before(context_id_2, HummockVersionId::MAX)
        .await
        .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );
}

#[tokio::test]
async fn test_pin_snapshot_response_lost() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    let mut epoch = test_epoch(1);
    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ ] -> [ e0 ]
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&test_tables),
    )
    .await
    .unwrap();
    epoch.inc_epoch();

    // Pin a snapshot with smallest last_pin
    // [ e0 ] -> [ e0:pinned ]
    let mut epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    let prev_epoch = epoch.prev_epoch();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, prev_epoch);

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0:pinned ] -> [ e0:pinned, e1 ]
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&test_tables),
    )
    .await
    .unwrap();
    epoch.inc_epoch();

    // Assume the response of the previous rpc is lost.
    // [ e0:pinned, e1 ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    let prev_epoch = epoch.prev_epoch();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, prev_epoch);

    // Assume the response of the previous rpc is lost.
    // [ e0, e1:pinned ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(
        epoch_recorded_in_frontend.committed_epoch,
        epoch.prev_epoch()
    );

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0, e1:pinned ] -> [ e0, e1:pinned, e2 ]
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&test_tables),
    )
    .await
    .unwrap();
    epoch.inc_epoch();

    // Use correct snapshot id.
    // [ e0, e1:pinned, e2 ] -> [ e0, e1:pinned, e2:pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(
        epoch_recorded_in_frontend.committed_epoch,
        epoch.prev_epoch()
    );

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0, e1:pinned, e2:pinned ] -> [ e0, e1:pinned, e2:pinned, e3 ]
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&test_tables),
    )
    .await
    .unwrap();
    epoch.inc_epoch();

    // Use u64::MAX as epoch to pin greatest snapshot
    // [ e0, e1:pinned, e2:pinned, e3 ] -> [ e0, e1:pinned, e2:pinned, e3::pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(
        epoch_recorded_in_frontend.committed_epoch,
        epoch.prev_epoch()
    );
}

#[tokio::test]
async fn test_print_compact_task() {
    let (_, hummock_manager, _cluster_manager, _) = setup_compute_env(80).await;
    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let original_tables =
        generate_test_sstables_with_table_id(epoch, 1, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&original_tables),
    )
    .await
    .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);

    let s = compact_task_to_string(&compact_task);
    assert!(s.contains("Compaction task id: 1, group-id: 2, type: Dynamic, target level: 0"));
}

#[tokio::test]
async fn test_invalid_sst_id() {
    let (_, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let epoch = test_epoch(1);
    let ssts = generate_test_tables(epoch, vec![1]);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &ssts,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    let ssts = to_local_sstable_info(&ssts);
    // reject due to invalid context id
    let sst_to_worker = ssts
        .iter()
        .map(|LocalSstableInfo { sst_info, .. }| (sst_info.object_id, WorkerId::MAX))
        .collect();
    let error = hummock_manager
        .commit_epoch_for_test(epoch, ssts.clone(), sst_to_worker)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidSst(1)));

    let sst_to_worker = ssts
        .iter()
        .map(|LocalSstableInfo { sst_info, .. }| (sst_info.object_id, context_id))
        .collect();
    hummock_manager
        .commit_epoch_for_test(epoch, ssts, sst_to_worker)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_trigger_manual_compaction() {
    let (_, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    {
        let option = ManualCompactionOption::default();
        // to check no compaction task
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;

        assert_eq!(
            "trigger_manual_compaction No compactor is available. compaction_group 2",
            result.err().unwrap().to_string()
        );
    }

    // No compaction task available.
    let compactor_manager_ref = hummock_manager.compactor_manager_ref_for_test();
    let receiver = compactor_manager_ref.add_compactor(context_id);
    {
        let option = ManualCompactionOption::default();
        // to check no compaction task
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;

        assert_eq!(
            "trigger_manual_compaction No compaction_task is available. compaction_group 2",
            result.err().unwrap().to_string()
        );
    }

    // Generate data for compaction task
    let _ = add_test_tables(&hummock_manager, context_id).await;
    {
        // to check compactor send task fail
        drop(receiver);
        {
            let option = ManualCompactionOption::default();
            let result = hummock_manager
                .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
                .await;
            assert!(result.is_err());
        }
    }

    compactor_manager_ref.remove_compactor(context_id);
    let _receiver = compactor_manager_ref.add_compactor(context_id);

    {
        let option = ManualCompactionOption {
            level: 6,
            key_range: KeyRange::default(),
            ..Default::default()
        };

        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;
        assert!(result.is_ok());
    }

    {
        let option = ManualCompactionOption::default();
        // all sst pending , test no compaction avail
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;
        assert!(result.is_err());
    }
}

// This is a non-deterministic test
#[cfg(madsim)]
#[tokio::test]
async fn test_hummock_compaction_task_heartbeat() {
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::HummockManager;

    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_num = 2;

    let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
    let _tx = compactor_manager.add_compactor(context_id);

    let (join_handle, shutdown_tx) = HummockManager::hummock_timer_task(hummock_manager.clone());

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let original_tables = generate_test_sstables_with_table_id(
        epoch,
        1,
        get_sst_ids(&hummock_manager, sst_num).await,
    );
    register_table_ids_to_compaction_group(
        &hummock_manager,
        &[1],
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&original_tables),
    )
    .await
    .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);
    assert_eq!(compact_task.task_id, 2);

    for i in 0..10 {
        // send heartbeats to the task over 2.5 seconds
        let req = CompactTaskProgress {
            task_id: compact_task.task_id,
            num_ssts_sealed: i + 1,
            ..Default::default()
        };
        compactor_manager.update_task_heartbeats(&vec![req]);
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    // Cancel the task immediately and succeed.
    assert!(hummock_manager
        .report_compact_task(
            compact_task.task_id,
            TaskStatus::ExecuteFailed,
            vec![],
            None
        )
        .await
        .unwrap());

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(compact_task.task_id, 3);

    // Cancel the task after heartbeat has triggered and fail.

    // do not send heartbeats to the task for 30s seconds (ttl = 1s, heartbeat check freq. = 1s)
    // default_interval = 30s
    tokio::time::sleep(std::time::Duration::from_secs(32)).await;

    assert!(!hummock_manager
        .report_compact_task(
            compact_task.task_id,
            TaskStatus::ExecuteFailed,
            vec![],
            None
        )
        .await
        .unwrap());
    shutdown_tx.send(()).unwrap();
    join_handle.await.unwrap();
}

// This is a non-deterministic test
#[cfg(madsim)]
#[tokio::test]
async fn test_hummock_compaction_task_heartbeat_removal_on_node_removal() {
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::HummockManager;
    let (_env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_num = 2;

    let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
    let _tx = compactor_manager.add_compactor(context_id);

    let (join_handle, shutdown_tx) = HummockManager::hummock_timer_task(hummock_manager.clone());

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let original_tables = generate_test_sstables_with_table_id(
        epoch,
        1,
        get_sst_ids(&hummock_manager, sst_num).await,
    );
    register_table_ids_to_compaction_group(
        &hummock_manager,
        &[1],
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    commit_from_meta_node(
        hummock_manager.borrow(),
        epoch,
        to_local_sstable_info(&original_tables),
    )
    .await
    .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);
    assert_eq!(compact_task.task_id, 2);

    // send heartbeats to the task immediately
    let req = CompactTaskProgress {
        task_id: compact_task.task_id,
        num_ssts_sealed: 1,
        num_ssts_uploaded: 1,
        ..Default::default()
    };
    compactor_manager.update_task_heartbeats(&vec![req.clone()]);

    // Removing the node from cluster will invalidate context id.
    cluster_manager
        .delete_worker_node(worker_node.host.unwrap())
        .await
        .unwrap();
    hummock_manager
        .release_contexts([context_id])
        .await
        .unwrap();

    shutdown_tx.send(()).unwrap();
    join_handle.await.unwrap();
}

#[tokio::test]
async fn test_extend_objects_to_delete() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let _pinned_version1 = hummock_manager.pin_version(context_id).await.unwrap();
    let sst_infos = add_test_tables(hummock_manager.as_ref(), context_id).await;
    let max_committed_object_id = sst_infos
        .iter()
        .map(|ssts| {
            ssts.iter()
                .max_by_key(|s| s.object_id)
                .map(|s| s.object_id)
                .unwrap()
        })
        .max()
        .unwrap();
    let orphan_sst_num = 10;
    let all_object_ids = sst_infos
        .iter()
        .flatten()
        .map(|s| s.object_id)
        .chain(max_committed_object_id + 1..=max_committed_object_id + orphan_sst_num)
        .collect_vec();
    assert!(hummock_manager.get_objects_to_delete().is_empty());
    assert_eq!(
        hummock_manager
            .extend_objects_to_delete_from_scan(&all_object_ids)
            .await,
        orphan_sst_num as usize
    );
    assert_eq!(
        hummock_manager.get_objects_to_delete().len(),
        orphan_sst_num as usize
    );

    // Checkpoint
    assert_eq!(
        hummock_manager.create_version_checkpoint(1).await.unwrap(),
        6
    );
    assert_eq!(
        hummock_manager.get_objects_to_delete().len(),
        orphan_sst_num as usize
    );
    // since version1 is still pinned, the sst removed in compaction can not be reclaimed.
    assert_eq!(
        hummock_manager
            .extend_objects_to_delete_from_scan(&all_object_ids)
            .await,
        orphan_sst_num as usize
    );
    let objects_to_delete = hummock_manager.get_objects_to_delete();
    assert_eq!(objects_to_delete.len(), orphan_sst_num as usize);
    let pinned_version2: HummockVersion = hummock_manager.pin_version(context_id).await.unwrap();
    let objects_to_delete = hummock_manager.get_objects_to_delete();
    assert_eq!(
        objects_to_delete.len(),
        orphan_sst_num as usize,
        "{:?}",
        objects_to_delete
    );
    hummock_manager
        .unpin_version_before(context_id, pinned_version2.id)
        .await
        .unwrap();
    let objects_to_delete = hummock_manager.get_objects_to_delete();
    assert_eq!(
        objects_to_delete.len(),
        orphan_sst_num as usize,
        "{:?}",
        objects_to_delete
    );
    // version1 is unpin, but version2 is pinned, and version2 is the checkpoint version.
    // stale objects are combined in the checkpoint of version2, so no sst to reclaim
    assert_eq!(
        hummock_manager
            .extend_objects_to_delete_from_scan(&all_object_ids)
            .await,
        orphan_sst_num as usize
    );
    let objects_to_delete = hummock_manager.get_objects_to_delete();
    assert_eq!(objects_to_delete.len(), orphan_sst_num as usize);
    let new_epoch = pinned_version2.visible_table_committed_epoch().next_epoch();
    hummock_manager
        .commit_epoch_for_test(
            new_epoch,
            Vec::<LocalSstableInfo>::new(),
            Default::default(),
        )
        .await
        .unwrap();
    let pinned_version3: HummockVersion = hummock_manager.pin_version(context_id).await.unwrap();
    assert_eq!(new_epoch, pinned_version3.visible_table_committed_epoch());
    hummock_manager
        .unpin_version_before(context_id, pinned_version3.id)
        .await
        .unwrap();
    // version3 is the min pinned, and sst removed in compaction can be reclaimed, because they were tracked
    // in the stale objects of version2 checkpoint
    assert_eq!(
        hummock_manager
            .extend_objects_to_delete_from_scan(&all_object_ids)
            .await,
        orphan_sst_num as usize + 3
    );
    let objects_to_delete = hummock_manager.get_objects_to_delete();
    assert_eq!(objects_to_delete.len(), orphan_sst_num as usize + 3);
}

#[tokio::test]
async fn test_version_stats() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let init_stats = hummock_manager.get_version_stats().await;
    assert!(init_stats.table_stats.is_empty());

    // Commit epoch
    let epoch = test_epoch(1);
    register_table_ids_to_compaction_group(
        &hummock_manager,
        &[1, 2, 3],
        StaticCompactionGroupId::StateDefault as _,
    )
    .await;
    let table_stats_change = TableStats {
        total_key_size: 1000,
        total_value_size: 100,
        total_key_count: 10,

        total_compressed_size: 1024 * 1024,
    };
    let ssts_with_table_ids = vec![vec![1, 2], vec![2, 3]];
    let sst_ids = get_sst_ids(&hummock_manager, ssts_with_table_ids.len() as _).await;
    let ssts = ssts_with_table_ids
        .into_iter()
        .enumerate()
        .map(|(idx, table_ids)| LocalSstableInfo {
            sst_info: SstableInfo {
                object_id: sst_ids[idx],
                sst_id: sst_ids[idx],
                key_range: KeyRange {
                    left: iterator_test_key_of_epoch(1, 1, 1).into(),
                    right: iterator_test_key_of_epoch(1, 1, 1).into(),
                    right_exclusive: false,
                },
                file_size: 1024 * 1024 * 1024,
                table_ids: table_ids.clone(),
                sst_size: 1024 * 1024 * 1024,
                ..Default::default()
            },
            table_stats: table_ids
                .iter()
                .map(|table_id| (*table_id, table_stats_change.clone()))
                .collect(),
        })
        .collect_vec();
    let sst_to_worker = ssts
        .iter()
        .map(|LocalSstableInfo { sst_info, .. }| (sst_info.object_id, worker_node.id))
        .collect();
    hummock_manager
        .commit_epoch_for_test(epoch, ssts, sst_to_worker)
        .await
        .unwrap();

    let stats_after_commit = hummock_manager.get_version_stats().await;
    assert_eq!(stats_after_commit.table_stats.len(), 3);
    let table1_stats = stats_after_commit.table_stats.get(&1).unwrap();
    let table2_stats = stats_after_commit.table_stats.get(&2).unwrap();
    let table3_stats = stats_after_commit.table_stats.get(&3).unwrap();
    assert_eq!(table1_stats.total_key_count, 10);
    assert_eq!(table1_stats.total_value_size, 100);
    assert_eq!(table1_stats.total_key_size, 1000);
    assert_eq!(table2_stats.total_key_count, 20);
    assert_eq!(table2_stats.total_value_size, 200);
    assert_eq!(table2_stats.total_key_size, 2000);
    assert_eq!(table3_stats.total_key_count, 10);
    assert_eq!(table3_stats.total_value_size, 100);
    assert_eq!(table3_stats.total_key_size, 1000);

    // Report compaction
    hummock_manager
        .compactor_manager_ref_for_test()
        .add_compactor(worker_node.id);

    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    // compact_task.task_status = TaskStatus::Success as _;
    let compact_table_stats_change = TableStatsMap::from([
        (
            2,
            TableStats {
                total_key_size: -1000,
                total_value_size: -100,
                total_key_count: -10,
                total_compressed_size: 0, // unused
            },
        ),
        (
            3,
            TableStats {
                total_key_size: -1000,
                total_value_size: -100,
                total_key_count: -10,
                total_compressed_size: 0, // unused
            },
        ),
    ]);
    hummock_manager
        .report_compact_task(
            compact_task.task_id,
            TaskStatus::Success,
            vec![],
            Some(to_prost_table_stats_map(compact_table_stats_change)),
        )
        .await
        .unwrap();
    let stats_after_compact = hummock_manager.get_version_stats().await;
    let compact_table1_stats = stats_after_compact.table_stats.get(&1).unwrap();
    let compact_table2_stats = stats_after_compact.table_stats.get(&2).unwrap();
    let compact_table3_stats = stats_after_compact.table_stats.get(&3).unwrap();
    assert_eq!(compact_table1_stats, table1_stats);
    assert_eq!(compact_table2_stats.total_key_count, 10);
    assert_eq!(compact_table2_stats.total_value_size, 100);
    assert_eq!(compact_table2_stats.total_key_size, 1000);
    assert_eq!(compact_table3_stats.total_key_count, 0);
    assert_eq!(compact_table3_stats.total_value_size, 0);
    assert_eq!(compact_table3_stats.total_key_size, 0);
}

#[tokio::test]
async fn test_split_compaction_group_on_commit() {
    let (_env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    hummock_manager
        .register_table_ids_for_test(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(101, 3)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: KeyRange::default(),
            table_ids: vec![100, 101],
            min_epoch: 20,
            max_epoch: 20,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        },
        table_stats: HashMap::from([
            (
                100,
                TableStats {
                    total_compressed_size: 50,
                    ..Default::default()
                },
            ),
            (
                101,
                TableStats {
                    total_compressed_size: 50,
                    ..Default::default()
                },
            ),
        ]),
    };
    hummock_manager
        .commit_epoch_for_test(30, vec![sst_1], HashMap::from([(10, context_id)]))
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 2);
    assert_eq!(
        get_compaction_group_object_ids(&current_version, 2),
        vec![10]
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, 3),
        vec![10]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(2)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![100]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(3)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![101]
    );
}

#[tokio::test]
async fn test_split_compaction_group_on_demand_basic() {
    let (_env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let original_groups = hummock_manager
        .get_current_version()
        .await
        .levels
        .keys()
        .cloned()
        .sorted()
        .collect_vec();
    assert_eq!(original_groups, vec![2, 3]);

    let err = hummock_manager
        .split_compaction_group(100, &[0], 0)
        .await
        .unwrap_err();
    assert_eq!("compaction group error: invalid group 100", err.to_string());

    let err = hummock_manager
        .split_compaction_group(2, &[100], 0)
        .await
        .unwrap_err();
    assert_eq!(
        "compaction group error: table 100 doesn't in group 2",
        err.to_string()
    );

    hummock_manager
        .register_table_ids_for_test(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(101, 2)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(100, 1, 20).into(),
                right: iterator_test_key_of_epoch(100, 100, 20).into(),
                right_exclusive: false,
            },
            table_ids: vec![100],
            min_epoch: 20,
            max_epoch: 20,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    let sst_2 = LocalSstableInfo {
        sst_info: SstableInfo {
            object_id: 11,
            sst_id: 11,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(100, 101, 20).into(),
                right: iterator_test_key_of_epoch(101, 100, 20).into(),
                right_exclusive: false,
            },
            table_ids: vec![100, 101],
            min_epoch: 20,
            max_epoch: 20,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    hummock_manager
        .commit_epoch_for_test(
            30,
            vec![sst_1, sst_2],
            HashMap::from([(10, context_id), (11, context_id)]),
        )
        .await
        .unwrap();

    let err = hummock_manager
        .split_compaction_group(2, &[100, 101], 0)
        .await
        .unwrap_err();
    assert_eq!(
        "compaction group error: invalid split attempt for group 2: all member tables are moved",
        err.to_string()
    );

    // Now group 2 has member tables [100,101,102], so split [100, 101] can succeed even though
    // there is no data of 102.
    hummock_manager
        .register_table_ids_for_test(&[(102, 2)])
        .await
        .unwrap();

    hummock_manager
        .split_compaction_group(2, &[100, 101], 0)
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 3);
    let new_group_id = current_version.levels.keys().max().cloned().unwrap();
    assert!(new_group_id > StaticCompactionGroupId::End as u64);
    assert_eq!(
        get_compaction_group_object_ids(&current_version, 2),
        Vec::<u64>::new()
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, new_group_id),
        vec![10, 11]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(2)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![102]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(new_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .sorted()
            .collect_vec(),
        vec![100, 101]
    );
}

#[tokio::test]
async fn test_split_compaction_group_on_demand_non_trivial() {
    let (_env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_1 = LocalSstableInfo {
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: KeyRange::default(),
            table_ids: vec![100, 101],
            min_epoch: 20,
            max_epoch: 20,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    hummock_manager
        .register_table_ids_for_test(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(101, 2)])
        .await
        .unwrap();
    hummock_manager
        .commit_epoch_for_test(30, vec![sst_1], HashMap::from([(10, context_id)]))
        .await
        .unwrap();

    hummock_manager
        .split_compaction_group(2, &[100], 0)
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 3);
    let new_group_id = current_version.levels.keys().max().cloned().unwrap();
    assert!(new_group_id > StaticCompactionGroupId::End as u64);
    assert_eq!(
        get_compaction_group_object_ids(&current_version, 2),
        vec![10]
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, new_group_id),
        vec![10]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(2)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![101]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(new_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![100]
    );
}

#[tokio::test]
async fn test_split_compaction_group_trivial_expired() {
    let (_env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let original_groups = hummock_manager
        .get_current_version()
        .await
        .levels
        .keys()
        .cloned()
        .sorted()
        .collect_vec();
    assert_eq!(original_groups, vec![2, 3]);
    hummock_manager.compactor_manager.add_compactor(context_id);

    hummock_manager
        .register_table_ids_for_test(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(101, 2)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(100, 1, 20).into(),
                right: iterator_test_key_of_epoch(100, 100, 20).into(),
                right_exclusive: false,
            },
            table_ids: vec![100],
            min_epoch: 20,
            max_epoch: 20,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    let sst_2 = LocalSstableInfo {
        sst_info: SstableInfo {
            object_id: 11,
            sst_id: 11,
            table_ids: vec![101],
            min_epoch: 20,
            max_epoch: 20,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(101, 1, 20).into(),
                right: iterator_test_key_of_epoch(101, 100, 20).into(),
                right_exclusive: false,
            },
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    let mut sst_3 = sst_2.clone();
    let mut sst_4 = sst_1.clone();
    sst_3.sst_info.sst_id = 8;
    sst_3.sst_info.object_id = 8;
    sst_4.sst_info.sst_id = 9;
    sst_4.sst_info.object_id = 9;
    hummock_manager
        .commit_epoch_for_test(
            30,
            vec![sst_1, sst_2, sst_3, sst_4],
            HashMap::from([
                (10, context_id),
                (11, context_id),
                (9, context_id),
                (8, context_id),
            ]),
        )
        .await
        .unwrap();

    // Now group 2 has member tables [100,101,102], so split [100, 101] can succeed even though
    // there is no data of 102.
    hummock_manager
        .register_table_ids_for_test(&[(102, 2)])
        .await
        .unwrap();
    let task = hummock_manager
        .get_compact_task(2, &mut default_compaction_selector())
        .await
        .unwrap()
        .unwrap();

    hummock_manager
        .split_compaction_group(2, &[100], 0)
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    let new_group_id = current_version.levels.keys().max().cloned().unwrap();
    assert_eq!(current_version.levels.len(), 3);
    assert!(new_group_id > StaticCompactionGroupId::End as u64);
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(2)
            .iter()
            .map(|table_id| table_id.table_id)
            .sorted()
            .collect_vec(),
        vec![101, 102]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(new_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![100]
    );

    let task2 = hummock_manager
        .get_compact_task(new_group_id, &mut default_compaction_selector())
        .await
        .unwrap()
        .unwrap();

    let ret = hummock_manager
        .report_compact_task(
            task2.task_id,
            TaskStatus::Success,
            vec![SstableInfo {
                object_id: 12,
                sst_id: 12,
                key_range: KeyRange::default(),
                table_ids: vec![100],
                min_epoch: 20,
                max_epoch: 20,
                file_size: 100,
                sst_size: 100,
                ..Default::default()
            }],
            None,
        )
        .await
        .unwrap();
    assert!(ret);
    let ret = hummock_manager
        .report_compact_task(task.task_id, TaskStatus::Success, vec![], None)
        .await
        .unwrap();
    // the task has been canceled
    assert!(!ret);
}

async fn get_manual_compact_task(
    hummock_manager: &HummockManager,
    context_id: HummockContextId,
) -> CompactTask {
    hummock_manager.compactor_manager.add_compactor(context_id);
    hummock_manager
        .manual_get_compact_task(
            2,
            ManualCompactionOption {
                level: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn test_split_compaction_group_on_demand_bottom_levels() {
    let (_env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    hummock_manager
        .register_table_ids_for_test(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(101, 2)])
        .await
        .unwrap();

    let sst_1 = LocalSstableInfo {
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(1, 1, 1).into(),
                right: iterator_test_key_of_epoch(1, 1, 1).into(),
                right_exclusive: false,
            },
            table_ids: vec![100, 101],
            min_epoch: 20,
            max_epoch: 20,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    hummock_manager
        .commit_epoch_for_test(30, vec![sst_1.clone()], HashMap::from([(10, context_id)]))
        .await
        .unwrap();
    // Construct data via manual compaction
    let compaction_task = get_manual_compact_task(&hummock_manager, context_id).await;
    let base_level: usize = 6;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 1);
    assert_eq!(compaction_task.target_level, base_level as u32);

    assert!(hummock_manager
        .report_compact_task(
            compaction_task.task_id,
            TaskStatus::Success,
            vec![
                SstableInfo {
                    object_id: 11,
                    sst_id: 11,
                    table_ids: vec![100, 101],
                    key_range: KeyRange {
                        left: iterator_test_key_of_epoch(1, 1, 1).into(),
                        right: iterator_test_key_of_epoch(1, 1, 1).into(),
                        right_exclusive: false,
                    },
                    file_size: 100,
                    sst_size: 100,
                    ..Default::default()
                },
                SstableInfo {
                    object_id: 12,
                    sst_id: 12,
                    table_ids: vec![100],
                    key_range: KeyRange {
                        left: iterator_test_key_of_epoch(1, 2, 2).into(),
                        right: iterator_test_key_of_epoch(1, 2, 2).into(),
                        right_exclusive: false,
                    },
                    file_size: 100,
                    sst_size: 100,
                    ..Default::default()
                },
            ],
            None,
        )
        .await
        .unwrap());
    let current_version = hummock_manager.get_current_version().await;
    assert!(current_version
        .get_compaction_group_levels(2)
        .l0
        .sub_levels
        .is_empty());
    assert_eq!(
        current_version.get_compaction_group_levels(2).levels[base_level - 1]
            .table_infos
            .len(),
        2
    );

    hummock_manager
        .split_compaction_group(2, &[100], 0)
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    let new_group_id = current_version.levels.keys().max().cloned().unwrap();
    assert_eq!(
        current_version.get_compaction_group_levels(2).levels[base_level - 1]
            .table_infos
            .len(),
        1
    );

    assert_eq!(
        current_version.get_compaction_group_levels(2).levels[base_level - 1].table_infos[0]
            .object_id,
        sst_1.sst_info.object_id + 1,
    );
    assert_eq!(
        current_version.get_compaction_group_levels(2).levels[base_level - 1].table_infos[0]
            .table_ids,
        vec![101]
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(new_group_id)
            .levels[base_level - 1]
            .table_infos
            .len(),
        2
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(new_group_id)
            .levels[base_level - 1]
            .table_infos[0]
            .table_ids,
        vec![100]
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(new_group_id)
            .levels[base_level - 1]
            .table_infos[1]
            .table_ids,
        vec![100]
    );
}

#[tokio::test]
async fn test_compaction_task_expiration_due_to_split_group() {
    let (_env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    hummock_manager
        .register_table_ids_for_test(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(101, 2)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(1, 1, 1).into(),
                right: iterator_test_key_of_epoch(1, 1, 1).into(),
                right_exclusive: false,
            },
            table_ids: vec![100, 101],
            min_epoch: 20,
            max_epoch: 20,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    let sst_2 = LocalSstableInfo {
        sst_info: SstableInfo {
            object_id: 11,
            sst_id: 11,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(1, 1, 1).into(),
                right: iterator_test_key_of_epoch(1, 1, 1).into(),
                right_exclusive: false,
            },
            table_ids: vec![101],
            min_epoch: 20,
            max_epoch: 20,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    hummock_manager
        .commit_epoch_for_test(
            30,
            vec![sst_1, sst_2],
            HashMap::from([(10, context_id), (11, context_id)]),
        )
        .await
        .unwrap();

    let compaction_task = get_manual_compact_task(&hummock_manager, context_id).await;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 2);
    hummock_manager
        .split_compaction_group(2, &[100], 0)
        .await
        .unwrap();

    let version_1 = hummock_manager.get_current_version().await;
    // compaction_task.task_status = TaskStatus::Success.into();
    assert!(!hummock_manager
        .report_compact_task(compaction_task.task_id, TaskStatus::Success, vec![], None)
        .await
        .unwrap());
    let version_2 = hummock_manager.get_current_version().await;
    assert_eq!(
        version_1, version_2,
        "version should not change because compaction task has been cancelled"
    );

    let compaction_task = get_manual_compact_task(&hummock_manager, context_id).await;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 2);
    hummock_manager
        .report_compact_task(compaction_task.task_id, TaskStatus::Success, vec![], None)
        .await
        .unwrap();

    let version_3 = hummock_manager.get_current_version().await;
    assert_ne!(
        version_2, version_3,
        "version should change because compaction task has succeeded"
    );
}

#[tokio::test]
async fn test_move_tables_between_compaction_group() {
    let (_env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    hummock_manager
        .register_table_ids_for_test(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(101, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(102, 2)])
        .await
        .unwrap();
    let sst_1 = gen_local_sstable_info(10, 1, vec![100, 101, 102]);
    hummock_manager
        .commit_epoch_for_test(30, vec![sst_1.clone()], HashMap::from([(10, context_id)]))
        .await
        .unwrap();
    // Construct data via manual compaction
    let compaction_task = get_manual_compact_task(&hummock_manager, context_id).await;
    let base_level: usize = 6;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 1);
    assert_eq!(compaction_task.target_level, base_level as u32);
    assert!(hummock_manager
        .report_compact_task(
            compaction_task.task_id,
            TaskStatus::Success,
            vec![
                gen_sstable_info(11, 1, vec![100]),
                gen_sstable_info(12, 2, vec![100, 101]),
                gen_sstable_info(13, 3, vec![101, 102]),
            ],
            None,
        )
        .await
        .unwrap());
    let sst_2 = gen_local_sstable_info(14, 1, vec![101, 102]);
    hummock_manager
        .commit_epoch_for_test(31, vec![sst_2.clone()], HashMap::from([(14, context_id)]))
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(
        current_version.get_compaction_group_levels(2).levels[base_level - 1]
            .table_infos
            .len(),
        3
    );

    hummock_manager
        .split_compaction_group(2, &[100], 0)
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    let new_group_id = current_version.levels.keys().max().cloned().unwrap();
    assert_eq!(
        current_version.get_compaction_group_levels(2).levels[base_level - 1]
            .table_infos
            .len(),
        2
    );

    let level = &current_version
        .get_compaction_group_levels(new_group_id)
        .levels[base_level - 1];
    assert_eq!(level.table_infos[0].table_ids, vec![100]);
    assert_eq!(level.table_infos[1].table_ids, vec![100]);
    assert_eq!(level.table_infos.len(), 2);
}

#[tokio::test]
async fn test_gc_stats() {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(1)
        .level0_max_compact_file_number(130)
        .level0_sub_level_compact_level_count(1)
        .level0_overlapping_sub_level_compact_level_count(1)
        .build();
    let registry = Registry::new();
    let (_env, hummock_manager, _, worker_node) =
        setup_compute_env_with_metric(80, config, Some(MetaMetrics::for_test(&registry))).await;
    let context_id = worker_node.id;
    let assert_eq_gc_stats = |stale_object_size,
                              stale_object_count,
                              old_version_object_size,
                              old_version_object_count,
                              current_version_object_count,
                              current_version_object_size| {
        assert_eq!(
            hummock_manager.metrics.stale_object_size.get(),
            stale_object_size
        );
        assert_eq!(
            hummock_manager.metrics.stale_object_count.get(),
            stale_object_count
        );
        assert_eq!(
            hummock_manager.metrics.old_version_object_size.get(),
            old_version_object_size
        );
        assert_eq!(
            hummock_manager.metrics.old_version_object_count.get(),
            old_version_object_count
        );
        assert_eq!(
            hummock_manager.metrics.current_version_object_count.get(),
            current_version_object_count
        );
        assert_eq!(
            hummock_manager.metrics.current_version_object_size.get(),
            current_version_object_size
        );
    };
    assert_eq_gc_stats(0, 0, 0, 0, 0, 0);
    assert_eq!(
        hummock_manager.create_version_checkpoint(0).await.unwrap(),
        0
    );

    hummock_manager.pin_version(context_id).await.unwrap();
    let _ = add_test_tables(&hummock_manager, context_id).await;
    assert_eq_gc_stats(0, 0, 0, 0, 0, 0);
    assert_ne!(
        hummock_manager.create_version_checkpoint(0).await.unwrap(),
        0
    );

    assert_eq_gc_stats(0, 0, 6, 3, 2, 4);
    hummock_manager
        .unpin_version_before(context_id, HummockVersionId::MAX)
        .await
        .unwrap();

    assert_eq_gc_stats(0, 0, 6, 3, 2, 4);
    assert_eq!(
        hummock_manager.create_version_checkpoint(0).await.unwrap(),
        0
    );
    assert_eq_gc_stats(6, 3, 0, 0, 2, 4);
}

#[tokio::test]
async fn test_partition_level() {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(3)
        .level0_sub_level_compact_level_count(3)
        .level0_overlapping_sub_level_compact_level_count(3)
        .build();
    let registry = Registry::new();
    let (env, hummock_manager, _, worker_node) =
        setup_compute_env_with_metric(80, config.clone(), Some(MetaMetrics::for_test(&registry)))
            .await;
    let context_id = worker_node.id;

    hummock_manager
        .register_table_ids_for_test(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(101, 2)])
        .await
        .unwrap();
    let sst_1 = gen_local_sstable_info(10, 1, vec![100, 101]);
    hummock_manager
        .commit_epoch_for_test(30, vec![sst_1.clone()], HashMap::from([(10, context_id)]))
        .await
        .unwrap();
    // Construct data via manual compaction
    let compaction_task = get_manual_compact_task(&hummock_manager, context_id).await;
    let base_level: usize = 6;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 1);
    assert_eq!(compaction_task.target_level, base_level as u32);
    assert!(hummock_manager
        .report_compact_task(
            compaction_task.task_id,
            TaskStatus::Success,
            vec![
                gen_sstable_info(11, 1, vec![100]),
                gen_sstable_info(12, 2, vec![101]),
            ],
            None,
        )
        .await
        .unwrap());

    hummock_manager
        .split_compaction_group(2, &[100], env.opts.partition_vnode_count)
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;

    let new_group_id = current_version.levels.keys().max().cloned().unwrap();
    assert_eq!(
        current_version
            .get_compaction_group_levels(new_group_id)
            .levels[base_level - 1]
            .table_infos
            .len(),
        1
    );

    let mut global_sst_id = 13;
    const MB: u64 = 1024 * 1024;
    let mut selector = default_compaction_selector();
    for epoch in 31..100 {
        let mut sst = gen_local_sstable_info(global_sst_id, 10, vec![100]);
        sst.sst_info.file_size = 10 * MB;
        sst.sst_info.sst_size = 10 * MB;
        sst.sst_info.uncompressed_file_size = 10 * MB;

        hummock_manager
            .commit_epoch_for_test(
                epoch,
                vec![sst],
                HashMap::from([(global_sst_id, context_id)]),
            )
            .await
            .unwrap();
        global_sst_id += 1;
        if let Some(task) = hummock_manager
            .get_compact_task(new_group_id, &mut selector)
            .await
            .unwrap()
        {
            let mut sst = gen_sstable_info(global_sst_id, 10, vec![100]);
            sst.file_size = task
                .input_ssts
                .iter()
                .map(|level| {
                    level
                        .table_infos
                        .iter()
                        .map(|sst| sst.sst_size)
                        .sum::<u64>()
                })
                .sum::<u64>();
            sst.sst_size = sst.file_size;
            global_sst_id += 1;
            let ret = hummock_manager
                .report_compact_task(task.task_id, TaskStatus::Success, vec![sst], None)
                .await
                .unwrap();
            assert!(ret);
        }
    }
    let current_version = hummock_manager.get_current_version().await;
    let group = current_version.get_compaction_group_levels(new_group_id);
    for sub_level in &group.l0.sub_levels {
        if sub_level.total_file_size > config.sub_level_max_compaction_bytes {
            assert!(sub_level.vnode_partition_count > 0);
        }
    }
}

#[tokio::test]
async fn test_unregister_moved_table() {
    let (_env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let original_groups = hummock_manager
        .get_current_version()
        .await
        .levels
        .keys()
        .cloned()
        .sorted()
        .collect_vec();
    assert_eq!(
        original_groups,
        vec![
            StaticCompactionGroupId::StateDefault as u64,
            StaticCompactionGroupId::MaterializedView as u64
        ]
    );

    hummock_manager
        .register_table_ids_for_test(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids_for_test(&[(101, 2)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(100, 1, 20).into(),
                right: iterator_test_key_of_epoch(100, 100, 20).into(),
                right_exclusive: false,
            },
            table_ids: vec![100],
            min_epoch: 20,
            max_epoch: 20,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    let sst_2 = LocalSstableInfo {
        sst_info: SstableInfo {
            object_id: 11,
            sst_id: 11,
            key_range: KeyRange {
                left: iterator_test_key_of_epoch(100, 101, 20).into(),
                right: iterator_test_key_of_epoch(101, 100, 20).into(),
                right_exclusive: false,
            },
            table_ids: vec![100, 101],
            min_epoch: 20,
            max_epoch: 20,
            file_size: 100,
            sst_size: 100,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    hummock_manager
        .commit_epoch_for_test(
            30,
            vec![sst_1, sst_2],
            HashMap::from([(10, context_id), (11, context_id)]),
        )
        .await
        .unwrap();

    let new_group_id = hummock_manager
        .split_compaction_group(2, &[100], 0)
        .await
        .unwrap();
    assert_ne!(new_group_id, 2);
    assert!(new_group_id > StaticCompactionGroupId::End as u64);

    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(
        new_group_id,
        current_version.levels.keys().max().cloned().unwrap()
    );
    assert_eq!(current_version.levels.len(), 3);
    assert_eq!(
        get_compaction_group_object_ids(&current_version, 2),
        vec![11]
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, new_group_id),
        vec![10, 11]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(2)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![101]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(new_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![100]
    );

    hummock_manager
        .unregister_table_ids([TableId::new(100)])
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 2);
    assert!(!current_version.levels.contains_key(&new_group_id));
    assert_eq!(
        get_compaction_group_object_ids(&current_version, 2),
        vec![11]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(2)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![101]
    );
}
