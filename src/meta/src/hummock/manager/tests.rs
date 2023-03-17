// Copyright 2023 RisingWave Labs
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

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    get_compaction_group_ids, get_compaction_group_ssts, HummockVersionExt,
};
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::table_stats::{to_prost_table_stats_map, TableStats, TableStatsMap};
use risingwave_hummock_sdk::{
    CompactionGroupId, ExtendedSstableInfo, HummockContextId, HummockEpoch, HummockSstableId,
    HummockSstableObjectId, HummockVersionId, LocalSstableInfo, FIRST_VERSION_ID,
};
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::version_update_payload::Payload;
use risingwave_pb::hummock::{
    CompactTask, HummockPinnedSnapshot, HummockPinnedVersion, HummockSnapshot, HummockVersion,
    KeyRange, SstableInfo,
};

use crate::hummock::compaction::{default_level_selector, ManualCompactionOption};
use crate::hummock::error::Error;
use crate::hummock::test_utils::*;
use crate::hummock::{
    start_compaction_scheduler, CompactionScheduler, HummockManager, HummockManagerRef,
};
use crate::manager::WorkerId;
use crate::model::MetadataModel;
use crate::storage::{MemStore, MetaStore};

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
        id: sst_id,
        key_range: Some(KeyRange {
            left: iterator_test_key_of_epoch(1, idx, 1),
            right: iterator_test_key_of_epoch(1, idx, 1),
            right_exclusive: false,
        }),
        table_ids,
        divide_version: 0,
        min_epoch: 20,
        max_epoch: 20,
        ..Default::default()
    }
}

fn gen_extend_sstable_info(
    sst_id: u64,
    group_id: u64,
    idx: usize,
    table_ids: Vec<u32>,
) -> ExtendedSstableInfo {
    ExtendedSstableInfo {
        compaction_group_id: group_id,
        sst_info: gen_sstable_info(sst_id, idx, table_ids),
        table_stats: Default::default(),
    }
}
fn get_compaction_group_object_ids(
    version: &HummockVersion,
    group_id: CompactionGroupId,
) -> Vec<HummockSstableObjectId> {
    get_compaction_group_ssts(version, group_id)
        .into_iter()
        .map(|(object_id, _)| object_id)
        .collect_vec(
}

#[tokio::test]
async fn test_unpin_snapshot_before() {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let epoch = 0;

    for _ in 0..2 {
        let pin_result = hummock_manager.pin_snapshot(context_id).await.unwrap();
        assert_eq!(pin_result.committed_epoch, epoch);
        let pinned_snapshots = HummockPinnedSnapshot::list(env.meta_store()).await.unwrap();
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
                    current_epoch: epoch,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            pin_snapshots_epoch(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
            vec![epoch]
        );
    }

    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot_before(
                context_id,
                HummockSnapshot {
                    committed_epoch: epoch + 1,
                    current_epoch: epoch + 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            pin_snapshots_epoch(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
            vec![epoch]
        );
    }
}

#[tokio::test]
async fn test_hummock_compaction_task() {
    let (_, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let sst_num = 2;

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_level_selector(),
        )
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, sst_num).await);
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
    let mut compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_level_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    // Get the compactor and assign task.
    let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
    compactor_manager.add_compactor(worker_node.id, u64::MAX);
    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();
    assert_eq!(compactor.context_id(), worker_node.id);
    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );
    assert_eq!(compact_task.get_task_id(), 2);

    // Cancel the task and succeed.
    assert!(hummock_manager
        .cancel_compact_task(&mut compact_task, TaskStatus::ManualCanceled)
        .await
        .unwrap());
    // Cancel a non-existent task and succeed.
    assert!(hummock_manager
        .cancel_compact_task(&mut compact_task, TaskStatus::ManualCanceled)
        .await
        .unwrap());

    // Get compactor.
    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    // Get a compaction task.
    let mut compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_level_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();
    assert_eq!(compact_task.get_task_id(), 3);
    // Finish the task and succeed.
    compact_task.set_task_status(TaskStatus::Success);

    assert!(hummock_manager
        .report_compact_task(compactor.context_id(), &mut compact_task, None)
        .await
        .unwrap());
    // Finish the task and told the task is not found, which may have been processed previously.
    assert!(!hummock_manager
        .report_compact_task(compactor.context_id(), &mut compact_task, None)
        .await
        .unwrap());
}

#[tokio::test]
async fn test_hummock_table() {
    let (_env, hummock_manager, _cluster_manager, _worker_node) = setup_compute_env(80).await;

    let epoch: u64 = 1;
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
            .as_ref()
            .unwrap()
            .sub_levels
            .iter()
            .chain(levels.levels.iter())
            .flat_map(|level| level.table_infos.iter())
            .map(|info| info.get_object_id())
            .sorted()
            .cmp(original_tables.iter().map(|ot| ot.get_object_id()).sorted())
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
    let epoch1: u64 = 1;
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
        assert_eq!(current_version.max_committed_epoch, INVALID_EPOCH);
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
        assert_eq!(current_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_object_ids(&committed_tables),
            get_sorted_committed_object_ids(&current_version)
        );
    }

    // Add and commit tables in epoch2.
    // BEFORE:  committed_epochs = [epoch1]
    // AFTER:   committed_epochs = [epoch1, epoch2]
    let epoch2 = epoch1 + 1;
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
        assert_eq!(current_version.max_committed_epoch, epoch1);
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
        assert_eq!(current_version.max_committed_epoch, epoch2);
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
            fake_parallelism,
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2.id;

    assert_eq!(
        pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
        0
    );
    hummock_manager.pin_version(context_id_1).await.unwrap();
    hummock_manager.pin_version(context_id_2).await.unwrap();
    hummock_manager.pin_snapshot(context_id_1).await.unwrap();
    hummock_manager.pin_snapshot(context_id_2).await.unwrap();
    assert_eq!(
        pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
        2
    );
    assert_eq!(
        HummockPinnedSnapshot::list(env.meta_store())
            .await
            .unwrap()
            .len(),
        2
    );
    hummock_manager
        .release_contexts(&vec![context_id_1])
        .await
        .unwrap();
    let pinned_versions = HummockPinnedVersion::list(env.meta_store()).await.unwrap();
    assert_eq!(pin_versions_sum(&pinned_versions), 1);
    assert_eq!(pinned_versions[0].context_id, context_id_2);
    let pinned_snapshots = HummockPinnedSnapshot::list(env.meta_store()).await.unwrap();
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
        pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
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
            fake_parallelism,
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2.id;

    // initial version id
    assert_eq!(
        hummock_manager.get_current_version().await.id,
        FIRST_VERSION_ID
    );

    let mut epoch = 1;
    let mut register_log_count = 0;
    let mut commit_log_count = 0;
    let commit_one = |epoch: HummockEpoch, hummock_manager: HummockManagerRef<MemStore>| async move {
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
    };

    commit_one(epoch, hummock_manager.clone()).await;
    register_log_count += 1;
    commit_log_count += 1;
    epoch += 1;

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
            .unpin_version_before(context_id_1, u64::MAX)
            .await
            .unwrap();
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            HummockVersionId::MAX
        );

        // should pin latest because u64::MAX
        let version = match hummock_manager.pin_version(context_id_1).await.unwrap() {
            Payload::VersionDeltas(_) => {
                unreachable!("should get full version")
            }
            Payload::PinnedVersion(version) => version,
        };
        assert_eq!(
            version.get_id(),
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
        let version = match hummock_manager.pin_version(context_id_2).await.unwrap() {
            Payload::VersionDeltas(_) => {
                unreachable!("should get full version")
            }
            Payload::PinnedVersion(version) => version,
        };
        assert_eq!(
            version.get_id(),
            init_version_id + commit_log_count + register_log_count
        );
        // pinned by context_id_1
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            init_version_id + commit_log_count + register_log_count - 2,
        );
    }

    // objects_to_delete is always empty because no compaction is ever invoked.
    assert!(hummock_manager.get_objects_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (0, 0)
    );
    assert_eq!(
        hummock_manager.proceed_version_checkpoint().await.unwrap(),
        commit_log_count + register_log_count - 2
    );
    assert!(hummock_manager.get_objects_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        ((commit_log_count + register_log_count - 2) as usize, 0)
    );

    hummock_manager
        .unpin_version_before(context_id_1, u64::MAX)
        .await
        .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        init_version_id + commit_log_count + register_log_count
    );
    assert!(hummock_manager.get_objects_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (0, 0)
    );
    assert_eq!(
        hummock_manager.proceed_version_checkpoint().await.unwrap(),
        2
    );
    assert!(hummock_manager.get_objects_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .delete_version_deltas(usize::MAX)
            .await
            .unwrap(),
        (2, 0)
    );

    hummock_manager
        .unpin_version_before(context_id_2, u64::MAX)
        .await
        .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );

    assert_eq!(
        hummock_manager.proceed_version_checkpoint().await.unwrap(),
        0
    );
}

#[tokio::test]
async fn test_pin_snapshot_response_lost() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    let mut epoch: u64 = 1;
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
    epoch += 1;

    // Pin a snapshot with smallest last_pin
    // [ e0 ] -> [ e0:pinned ]
    let mut epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, epoch - 1);

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
    epoch += 1;

    // Assume the response of the previous rpc is lost.
    // [ e0:pinned, e1 ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, epoch - 1);

    // Assume the response of the previous rpc is lost.
    // [ e0, e1:pinned ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, epoch - 1);

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
    epoch += 1;

    // Use correct snapshot id.
    // [ e0, e1:pinned, e2 ] -> [ e0, e1:pinned, e2:pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, epoch - 1);

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
    epoch += 1;

    // Use u64::MAX as epoch to pin greatest snapshot
    // [ e0, e1:pinned, e2:pinned, e3 ] -> [ e0, e1:pinned, e2:pinned, e3::pinned ]
    epoch_recorded_in_frontend = hummock_manager.pin_snapshot(context_id).await.unwrap();
    assert_eq!(epoch_recorded_in_frontend.committed_epoch, epoch - 1);
}

#[tokio::test]
async fn test_print_compact_task() {
    let (_, hummock_manager, _cluster_manager, _) = setup_compute_env(80).await;
    // Add some sstables and commit.
    let epoch: u64 = 1;
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

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_level_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );

    let s = compact_task_to_string(&compact_task);
    assert!(s.contains("Compaction task id: 1, group-id: 2, target level: 0"));
}

#[tokio::test]
async fn test_invalid_sst_id() {
    let (_, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let epoch = 1;
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
        .map(|LocalSstableInfo { sst_info, .. }| (sst_info.get_object_id(), WorkerId::MAX))
        .collect();
    let error = hummock_manager
        .commit_epoch(epoch, ssts.clone(), sst_to_worker)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidSst(1)));

    let sst_to_worker = ssts
        .iter()
        .map(|LocalSstableInfo { sst_info, .. }| (sst_info.get_object_id(), context_id))
        .collect();
    hummock_manager
        .commit_epoch(epoch, ssts, sst_to_worker)
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
    let receiver = compactor_manager_ref.add_compactor(context_id, u64::MAX);
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
    let _receiver = compactor_manager_ref.add_compactor(context_id, u64::MAX);

    {
        let option = ManualCompactionOption {
            level: 6,
            key_range: KeyRange {
                ..Default::default()
            },
            ..Default::default()
        };

        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;
        assert!(result.is_ok());
    }

    let task_id: u64 = 4;
    let compact_task = hummock_manager
        .compaction_task_from_assignment_for_test(task_id)
        .await
        .unwrap()
        .compact_task
        .unwrap();
    assert_eq!(task_id, compact_task.task_id);

    {
        let option = ManualCompactionOption::default();
        // all sst pending , test no compaction avail
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn test_trigger_compaction_deterministic() {
    let (env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    // No compaction task available.
    let compactor_manager_ref = hummock_manager.compactor_manager_ref_for_test();
    let compaction_scheduler = Arc::new(CompactionScheduler::new(
        env.clone(),
        hummock_manager.clone(),
        compactor_manager_ref.clone(),
    ));

    let _ = compactor_manager_ref.add_compactor(context_id, u64::MAX);
    let (_handle, shutdown_tx) = start_compaction_scheduler(compaction_scheduler);

    // Generate data for compaction task
    let _ = add_test_tables(&hummock_manager, context_id).await;

    let cur_version = hummock_manager.get_current_version().await;
    let compaction_groups = get_compaction_group_ids(&cur_version);

    let ret = hummock_manager
        .trigger_compaction_deterministic(cur_version.id, compaction_groups)
        .await;
    assert!(ret.is_ok());
    shutdown_tx
        .send(())
        .expect("shutdown compaction scheduler error");
}

// This is a non-deterministic test
#[cfg(madsim)]
#[tokio::test]
async fn test_hummock_compaction_task_heartbeat() {
    use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::HummockManager;
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_num = 2;

    let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
    let _tx = compactor_manager.add_compactor(context_id, 100);
    let (join_handle, shutdown_tx) =
        HummockManager::start_compaction_heartbeat(hummock_manager.clone()).await;

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_level_selector(),
        )
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, sst_num).await);
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

    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    // Get a compaction task.
    let mut compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_level_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();

    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );
    assert_eq!(compact_task.get_task_id(), 2);
    // send task
    compactor
        .send_task(Task::CompactTask(compact_task.clone()))
        .await
        .unwrap();

    for i in 0..10 {
        // send heartbeats to the task over 2.5 seconds
        let req = CompactTaskProgress {
            task_id: compact_task.task_id,
            num_ssts_sealed: i + 1,
            num_ssts_uploaded: 0,
        };
        compactor_manager.update_task_heartbeats(context_id, &vec![req]);
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    // Cancel the task immediately and succeed.
    compact_task.set_task_status(TaskStatus::ExecuteFailed);

    assert!(hummock_manager
        .report_compact_task(context_id, &mut compact_task, None)
        .await
        .unwrap());

    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    // Get a compaction task.
    let mut compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_level_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();
    assert_eq!(compact_task.get_task_id(), 3);
    // send task
    compactor
        .send_task(Task::CompactTask(compact_task.clone()))
        .await
        .unwrap();

    // do not send heartbeats to the task for 2.5 seconds (ttl = 1s, heartbeat check freq. = 1s)
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

    // Cancel the task after heartbeat has triggered and fail.
    compact_task.set_task_status(TaskStatus::ExecuteFailed);
    assert!(!hummock_manager
        .report_compact_task(context_id, &mut compact_task, None)
        .await
        .unwrap());
    shutdown_tx.send(()).unwrap();
    join_handle.await.unwrap();
}

// This is a non-deterministic test
#[cfg(madsim)]
#[tokio::test]
async fn test_hummock_compaction_task_heartbeat_removal_on_node_removal() {
    use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::HummockManager;
    let (_env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_num = 2;

    let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
    let _tx = compactor_manager.add_compactor(context_id, 100);
    let (join_handle, shutdown_tx) =
        HummockManager::start_compaction_heartbeat(hummock_manager.clone()).await;

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_level_selector(),
        )
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, sst_num).await);
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

    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_level_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();
    assert_eq!(
        compact_task
            .get_input_ssts()
            .first()
            .unwrap()
            .get_level_idx(),
        0
    );
    assert_eq!(compact_task.get_task_id(), 2);
    // send task
    compactor
        .send_task(Task::CompactTask(compact_task.clone()))
        .await
        .unwrap();

    // send heartbeats to the task immediately
    let req = CompactTaskProgress {
        task_id: compact_task.task_id,
        num_ssts_sealed: 1,
        num_ssts_uploaded: 1,
    };
    compactor_manager.update_task_heartbeats(context_id, &vec![req.clone()]);

    // Removing the node from cluster will invalidate context id.
    cluster_manager
        .delete_worker_node(worker_node.host.unwrap())
        .await
        .unwrap();
    hummock_manager
        .release_contexts([context_id])
        .await
        .unwrap();

    // Check that no heartbeats exist for the relevant context.
    assert!(!compactor_manager.purge_heartbeats_for_context(worker_node.id));

    shutdown_tx.send(()).unwrap();
    join_handle.await.unwrap();
}

#[tokio::test]
async fn test_extend_objects_to_delete() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_infos = add_test_tables(hummock_manager.as_ref(), context_id).await;
    let max_committed_object_id = sst_infos
        .iter()
        .map(|ssts| {
            ssts.iter()
                .max_by_key(|s| s.get_object_id())
                .map(|s| s.get_object_id())
                .unwrap()
        })
        .max()
        .unwrap();
    let orphan_sst_num = 10;
    let orphan_object_ids = sst_infos
        .iter()
        .flatten()
        .map(|s| s.get_object_id())
        .chain(max_committed_object_id + 1..=max_committed_object_id + orphan_sst_num)
        .collect_vec();
    assert!(hummock_manager.get_objects_to_delete().await.is_empty());
    assert_eq!(
        hummock_manager
            .extend_objects_to_delete_from_scan(&orphan_object_ids)
            .await,
        orphan_sst_num as usize
    );
    assert_eq!(
        hummock_manager.get_objects_to_delete().await.len(),
        orphan_sst_num as usize
    );

    // Checkpoint
    assert_eq!(
        hummock_manager.proceed_version_checkpoint().await.unwrap(),
        6
    );
    assert_eq!(
        hummock_manager
            .extend_objects_to_delete_from_scan(&orphan_object_ids)
            .await,
        orphan_sst_num as usize
    );
    assert_eq!(
        hummock_manager.get_objects_to_delete().await.len(),
        orphan_sst_num as usize + 3
    );
}

#[tokio::test]
async fn test_version_stats() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let init_stats = hummock_manager.get_version_stats().await;
    assert!(init_stats.table_stats.is_empty());

    // Commit epoch
    let epoch = 1;
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
    };
    let ssts_with_table_ids = vec![vec![1, 2], vec![2, 3]];
    let sst_ids = get_sst_ids(&hummock_manager, ssts_with_table_ids.len() as _).await;
    let ssts = ssts_with_table_ids
        .into_iter()
        .enumerate()
        .map(|(idx, table_ids)| LocalSstableInfo {
            compaction_group_id: StaticCompactionGroupId::StateDefault as _,
            sst_info: SstableInfo {
                object_id: sst_ids[idx],
                sst_id: sst_ids[idx],
                key_range: Some(KeyRange {
                    left: iterator_test_key_of_epoch(1, 1, 1),
                    right: iterator_test_key_of_epoch(1, 1, 1),
                    right_exclusive: false,
                }),
                file_size: 1024 * 1024 * 1024,
                table_ids: table_ids.clone(),
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
        .map(|LocalSstableInfo { sst_info, .. }| (sst_info.get_object_id(), worker_node.id))
        .collect();
    hummock_manager
        .commit_epoch(epoch, ssts, sst_to_worker)
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
        .add_compactor(worker_node.id, u64::MAX);
    let compactor = hummock_manager.get_idle_compactor().await.unwrap();
    let mut compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_level_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, compactor.context_id())
        .await
        .unwrap();
    compact_task.task_status = TaskStatus::Success as _;
    let compact_table_stats_change = TableStatsMap::from([
        (
            2,
            TableStats {
                total_key_size: -1000,
                total_value_size: -100,
                total_key_count: -10,
            },
        ),
        (
            3,
            TableStats {
                total_key_size: -1000,
                total_value_size: -100,
                total_key_count: -10,
            },
        ),
    ]);
    hummock_manager
        .report_compact_task(
            worker_node.id,
            &mut compact_task,
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
        .register_table_ids(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids(&[(101, 3)])
        .await
        .unwrap();
    let sst_1 = ExtendedSstableInfo {
        compaction_group_id: 2,
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: None,
            table_ids: vec![100, 101],
            min_epoch: 20,
            max_epoch: 20,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    hummock_manager
        .commit_epoch(30, vec![sst_1], HashMap::from([(10, context_id)]))
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
            .get_compaction_group_levels(2)
            .member_table_ids,
        vec![100]
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(3)
            .member_table_ids,
        vec![101]
    );
    let branched_ssts = hummock_manager
        .versioning
        .read(&["", "", ""])
        .await
        .branched_ssts
        .clone();
    assert_eq!(branched_ssts.len(), 1);
    assert_eq!(branched_ssts.values().next().unwrap().len(), 2);
    assert_ne!(
        branched_ssts
            .values()
            .next()
            .unwrap()
            .get(&2)
            .cloned()
            .unwrap(),
        branched_ssts
            .values()
            .next()
            .unwrap()
            .get(&3)
            .cloned()
            .unwrap(),
    );
}

async fn get_branched_ssts<S: MetaStore>(
    hummock_manager: &HummockManager<S>,
) -> BTreeMap<HummockSstableObjectId, BTreeMap<CompactionGroupId, Vec<HummockSstableId>>> {
    hummock_manager
        .versioning
        .read(&["", "", ""])
        .await
        .branched_ssts
        .clone()
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
        .split_compaction_group(100, &[0])
        .await
        .unwrap_err();
    assert_eq!("compaction group error: invalid group 100", err.to_string());

    hummock_manager
        .split_compaction_group(2, &[])
        .await
        .unwrap();

    let err = hummock_manager
        .split_compaction_group(2, &[100])
        .await
        .unwrap_err();
    assert_eq!(
        "compaction group error: table 100 doesn't in group 2",
        err.to_string()
    );

    hummock_manager
        .register_table_ids(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids(&[(101, 2)])
        .await
        .unwrap();
    let sst_1 = ExtendedSstableInfo {
        compaction_group_id: 2,
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: None,
            table_ids: vec![100],
            min_epoch: 20,
            max_epoch: 20,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    let sst_2 = ExtendedSstableInfo {
        compaction_group_id: 2,
        sst_info: SstableInfo {
            object_id: 11,
            sst_id: 11,
            key_range: None,
            table_ids: vec![100, 101],
            min_epoch: 20,
            max_epoch: 20,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    hummock_manager
        .commit_epoch(
            30,
            vec![sst_1, sst_2],
            HashMap::from([(10, context_id), (11, context_id)]),
        )
        .await
        .unwrap();

    let err = hummock_manager
        .split_compaction_group(2, &[100, 101])
        .await
        .unwrap_err();
    assert_eq!(
        "compaction group error: invalid split attempt for group 2: all member tables are moved",
        err.to_string()
    );

    // Now group 2 has member tables [100,101,102], so split [100, 101] can succeed even though
    // there is no data of 102.
    hummock_manager
        .register_table_ids(&[(102, 2)])
        .await
        .unwrap();

    hummock_manager
        .split_compaction_group(2, &[100, 101])
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 3);
    let new_group_id = current_version.levels.keys().max().cloned().unwrap();
    assert!(new_group_id > StaticCompactionGroupId::End as u64);
    assert!(
        get_compaction_group_object_ids(&current_version, 2).is_empty(),
        "SST 10, 11 has been moved to new_group completely."
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, new_group_id),
        vec![10, 11]
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(2)
            .member_table_ids,
        vec![102]
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(new_group_id)
            .member_table_ids,
        vec![100, 101]
    );
    let branched_ssts = get_branched_ssts(&hummock_manager).await;
    assert_eq!(branched_ssts.len(), 2);
    for object_id in [10, 11] {
        assert_eq!(branched_ssts.get(&object_id).unwrap().len(), 1);
        assert_ne!(
            branched_ssts
                .get(&object_id)
                .unwrap()
                .get(&new_group_id)
                .cloned()
                .unwrap(),
            vec![object_id],
            "trivial adjust should also generate a new SST id"
        );
    }
}

#[tokio::test]
async fn test_split_compaction_group_on_demand_non_trivial() {
    let (_env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_1 = ExtendedSstableInfo {
        compaction_group_id: 2,
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: None,
            table_ids: vec![100, 101],
            min_epoch: 20,
            max_epoch: 20,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    hummock_manager
        .register_table_ids(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids(&[(101, 2)])
        .await
        .unwrap();
    hummock_manager
        .commit_epoch(30, vec![sst_1], HashMap::from([(10, context_id)]))
        .await
        .unwrap();

    hummock_manager
        .split_compaction_group(2, &[100])
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
            .get_compaction_group_levels(2)
            .member_table_ids,
        vec![101]
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(new_group_id)
            .member_table_ids,
        vec![100]
    );
    let branched_ssts = get_branched_ssts(&hummock_manager).await;
    assert_eq!(branched_ssts.len(), 1);
    assert_eq!(branched_ssts.get(&10).unwrap().len(), 2);
    let sst_ids = branched_ssts.get(&10).unwrap().get(&2).cloned().unwrap();
    assert_ne!(sst_ids, vec![10]);
    assert_ne!(
        branched_ssts
            .get(&10)
            .unwrap()
            .get(&new_group_id)
            .cloned()
            .unwrap(),
        sst_ids,
    );
    assert_ne!(
        branched_ssts
            .get(&10)
            .unwrap()
            .get(&new_group_id)
            .cloned()
            .unwrap(),
        vec![10],
    );
}

async fn get_manual_compact_task<S: MetaStore>(
    hummock_manager: &HummockManager<S>,
    context_id: HummockContextId,
) -> CompactTask {
    hummock_manager
        .compactor_manager
        .add_compactor(context_id, 1);
    let compaction_task = hummock_manager
        .manual_get_compact_task(
            2,
            ManualCompactionOption {
                level: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compaction_task, context_id)
        .await
        .unwrap();
    compaction_task
}

#[tokio::test]
async fn test_split_compaction_group_on_demand_bottom_levels() {
    let (_env, hummock_manager, _, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    hummock_manager
        .register_table_ids(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids(&[(101, 2)])
        .await
        .unwrap();

    let sst_1 = ExtendedSstableInfo {
        compaction_group_id: 2,
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: Some(KeyRange {
                left: iterator_test_key_of_epoch(1, 1, 1),
                right: iterator_test_key_of_epoch(1, 1, 1),
                right_exclusive: false,
            }),
            table_ids: vec![100, 101],
            min_epoch: 20,
            max_epoch: 20,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    hummock_manager
        .commit_epoch(30, vec![sst_1.clone()], HashMap::from([(10, context_id)]))
        .await
        .unwrap();
    // Construct data via manual compaction
    let mut compaction_task = get_manual_compact_task(&hummock_manager, context_id).await;
    let base_level: usize = 6;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 1);
    assert_eq!(compaction_task.target_level, base_level as u32);
    compaction_task.sorted_output_ssts = vec![
        SstableInfo {
            object_id: sst_1.sst_info.get_object_id() + 1,
            sst_id: sst_1.sst_info.get_object_id() + 1,
            table_ids: vec![100, 101],
            key_range: Some(KeyRange {
                left: iterator_test_key_of_epoch(1, 1, 1),
                right: iterator_test_key_of_epoch(1, 1, 1),
                right_exclusive: false,
            }),
            ..Default::default()
        },
        SstableInfo {
            object_id: sst_1.sst_info.get_object_id() + 2,
            sst_id: sst_1.sst_info.get_object_id() + 2,
            table_ids: vec![100],
            key_range: Some(KeyRange {
                left: iterator_test_key_of_epoch(1, 2, 2),
                right: iterator_test_key_of_epoch(1, 2, 2),
                right_exclusive: false,
            }),
            ..Default::default()
        },
    ];
    compaction_task.task_status = TaskStatus::Success.into();
    assert!(hummock_manager
        .report_compact_task(context_id, &mut compaction_task, None)
        .await
        .unwrap());
    let current_version = hummock_manager.get_current_version().await;
    assert!(current_version
        .get_compaction_group_levels(2)
        .l0
        .as_ref()
        .unwrap()
        .sub_levels
        .is_empty());
    assert_eq!(
        current_version.get_compaction_group_levels(2).levels[base_level - 1]
            .table_infos
            .len(),
        2
    );

    hummock_manager
        .split_compaction_group(2, &[100])
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

    let branched_ssts = hummock_manager.get_branched_ssts_info().await;
    assert_eq!(branched_ssts.len(), 2);
    let info = branched_ssts.get(&11).unwrap();
    assert_eq!(info.keys().cloned().collect_vec(), vec![2, new_group_id]);

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
        .register_table_ids(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids(&[(101, 2)])
        .await
        .unwrap();
    let sst_1 = ExtendedSstableInfo {
        compaction_group_id: 2,
        sst_info: SstableInfo {
            object_id: 10,
            sst_id: 10,
            key_range: Some(KeyRange {
                left: iterator_test_key_of_epoch(1, 1, 1),
                right: iterator_test_key_of_epoch(1, 1, 1),
                right_exclusive: false,
            }),
            table_ids: vec![100, 101],
            min_epoch: 20,
            max_epoch: 20,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    let sst_2 = ExtendedSstableInfo {
        compaction_group_id: 2,
        sst_info: SstableInfo {
            object_id: 11,
            sst_id: 11,
            key_range: Some(KeyRange {
                left: iterator_test_key_of_epoch(1, 1, 1),
                right: iterator_test_key_of_epoch(1, 1, 1),
                right_exclusive: false,
            }),
            table_ids: vec![101],
            min_epoch: 20,
            max_epoch: 20,
            ..Default::default()
        },
        table_stats: Default::default(),
    };
    hummock_manager
        .commit_epoch(
            30,
            vec![sst_1, sst_2],
            HashMap::from([(10, context_id), (11, context_id)]),
        )
        .await
        .unwrap();

    let mut compaction_task = get_manual_compact_task(&hummock_manager, context_id).await;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 2);
    hummock_manager
        .split_compaction_group(2, &[100])
        .await
        .unwrap();

    let version_1 = hummock_manager.get_current_version().await;
    compaction_task.task_status = TaskStatus::Success.into();
    assert!(hummock_manager
        .report_compact_task(context_id, &mut compaction_task, None)
        .await
        .unwrap());
    let version_2 = hummock_manager.get_current_version().await;
    assert_eq!(
        version_1, version_2,
        "version should not change because compaction task has been cancelled"
    );

    let mut compaction_task = get_manual_compact_task(&hummock_manager, context_id).await;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 2);
    compaction_task.task_status = TaskStatus::Success.into();
    assert!(hummock_manager
        .report_compact_task(context_id, &mut compaction_task, None)
        .await
        .unwrap());
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
        .register_table_ids(&[(100, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids(&[(101, 2)])
        .await
        .unwrap();
    hummock_manager
        .register_table_ids(&[(102, 2)])
        .await
        .unwrap();
    let sst_1 = gen_extend_sstable_info(10, 2, 1, vec![100, 101, 102]);
    hummock_manager
        .commit_epoch(30, vec![sst_1.clone()], HashMap::from([(10, context_id)]))
        .await
        .unwrap();
    // Construct data via manual compaction
    let mut compaction_task = get_manual_compact_task(&hummock_manager, context_id).await;
    let base_level: usize = 6;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 1);
    assert_eq!(compaction_task.target_level, base_level as u32);
    compaction_task.sorted_output_ssts = vec![
        gen_sstable_info(11, 1, vec![100]),
        gen_sstable_info(12, 2, vec![100, 101]),
        gen_sstable_info(13, 3, vec![101, 102]),
    ];
    compaction_task.task_status = TaskStatus::Success.into();
    assert!(hummock_manager
        .report_compact_task(context_id, &mut compaction_task, None)
        .await
        .unwrap());
    let sst_2 = gen_extend_sstable_info(14, 2, 1, vec![101, 102]);
    hummock_manager
        .commit_epoch(31, vec![sst_2.clone()], HashMap::from([(14, context_id)]))
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
        .split_compaction_group(2, &[100])
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

    assert_eq!(
        current_version
            .get_compaction_group_levels(new_group_id)
            .levels[base_level - 1]
            .table_infos
            .len(),
        2
    );

    let branched_ssts = hummock_manager.get_branched_ssts_info().await;
    assert_eq!(branched_ssts.len(), 2);
    let info = branched_ssts.get(&12).unwrap();
    let groups = info.keys().sorted().cloned().collect_vec();
    assert_eq!(groups, vec![2, new_group_id]);
    hummock_manager
        .move_state_table_to_compaction_group(2, &[101], Some(new_group_id), false)
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(
        current_version
            .get_compaction_group_levels(new_group_id)
            .levels[base_level - 1]
            .table_infos
            .len(),
        3
    );
    assert_eq!(
        current_version.get_compaction_group_levels(2).levels[base_level - 1]
            .table_infos
            .len(),
        1
    );
    let branched_ssts = hummock_manager.get_branched_ssts_info().await;
    assert_eq!(branched_ssts.len(), 4);
    let info = branched_ssts.get(&12).unwrap();
    assert_eq!(
        info.keys().cloned().sorted().collect_vec(),
        vec![new_group_id]
    );
    let info = branched_ssts.get(&14).unwrap();
    assert_eq!(
        info.keys().cloned().sorted().collect_vec(),
        vec![2, new_group_id]
    );
}
