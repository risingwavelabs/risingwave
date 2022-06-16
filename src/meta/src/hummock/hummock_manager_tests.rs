// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::time::Duration;

use itertools::Itertools;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::{
    HummockContextId, HummockSSTableId, FIRST_VERSION_ID, INVALID_VERSION_ID,
};
use risingwave_pb::common::{HostAddress, ParallelUnitType, WorkerType};
use risingwave_pb::hummock::{
    HummockPinnedSnapshot, HummockPinnedVersion, HummockSnapshot, HummockVersion,
};

use crate::hummock::error::Error;
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
async fn test_hummock_pin_unpin() {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let version_id = FIRST_VERSION_ID;
    let epoch = INVALID_EPOCH;

    assert!(HummockPinnedVersion::list(env.meta_store())
        .await
        .unwrap()
        .is_empty());
    for _ in 0..2 {
        let hummock_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap();
        assert_eq!(version_id, hummock_version.id);
        assert_eq!(7, hummock_version.levels.len());
        assert_eq!(0, hummock_version.levels[0].table_infos.len());
        assert_eq!(0, hummock_version.levels[1].table_infos.len());

        let pinned_versions = HummockPinnedVersion::list(env.meta_store()).await.unwrap();
        assert_eq!(pin_versions_sum(&pinned_versions), 1);
        assert_eq!(pinned_versions[0].context_id, context_id);
        assert_eq!(pinned_versions[0].version_id.len(), 1);
        assert_eq!(pinned_versions[0].version_id[0], version_id);
    }

    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_version(context_id, vec![version_id])
            .await
            .unwrap();
        assert_eq!(
            pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
            0
        );
    }

    assert_eq!(
        pin_snapshots_sum(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
        0
    );
    for _ in 0..2 {
        let pin_result = hummock_manager
            .pin_snapshot(context_id, u64::MAX)
            .await
            .unwrap();
        assert_eq!(pin_result.epoch, epoch);
        let pinned_snapshots = HummockPinnedSnapshot::list(env.meta_store()).await.unwrap();
        assert_eq!(pin_snapshots_sum(&pinned_snapshots), 1);
        assert_eq!(pinned_snapshots[0].context_id, context_id);
        assert_eq!(pinned_snapshots[0].snapshot_id.len(), 1);
        assert_eq!(pinned_snapshots[0].snapshot_id[0], pin_result.epoch);
    }
    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot(context_id, vec![HummockSnapshot { epoch }])
            .await
            .unwrap();
        assert_eq!(
            pin_snapshots_sum(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
            0
        );
    }
}

#[tokio::test]
async fn test_unpin_snapshot_before() {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let epoch = 0;

    for _ in 0..2 {
        let pin_result = hummock_manager
            .pin_snapshot(context_id, u64::MAX)
            .await
            .unwrap();
        assert_eq!(pin_result.epoch, epoch);
        let pinned_snapshots = HummockPinnedSnapshot::list(env.meta_store()).await.unwrap();
        assert_eq!(pin_snapshots_sum(&pinned_snapshots), 1);
        assert_eq!(pinned_snapshots[0].context_id, context_id);
        assert_eq!(pinned_snapshots[0].snapshot_id.len(), 1);
        assert_eq!(pinned_snapshots[0].snapshot_id[0], pin_result.epoch);
    }

    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot_before(context_id, HummockSnapshot { epoch })
            .await
            .unwrap();
        assert_eq!(
            pin_snapshots_sum(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
            1
        );
    }

    // unpin nonexistent target will not return error
    for _ in 0..3 {
        hummock_manager
            .unpin_snapshot_before(context_id, HummockSnapshot { epoch: epoch + 1 })
            .await
            .unwrap();
        assert_eq!(
            pin_snapshots_sum(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
            0
        );
    }
}

#[tokio::test]
async fn test_hummock_compaction_task() {
    let (env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_num = 2usize;

    // Construct vnode mappings for generating compaction tasks.
    let parallel_units = cluster_manager
        .list_parallel_units(Some(ParallelUnitType::Hash))
        .await;
    env.hash_mapping_manager()
        .build_fragment_hash_mapping(1, &parallel_units);
    for table_id in 1..sst_num + 2 {
        env.hash_mapping_manager()
            .set_fragment_state_table(1, table_id as u32);
    }

    // No compaction task available.
    let task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap();
    assert_eq!(task, None);

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, sst_num).await);
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&original_tables))
        .await
        .unwrap();

    // check safe epoch in hummock version
    let version_id1 = CurrentHummockVersionId::get(env.meta_store())
        .await
        .unwrap()
        .unwrap();
    let hummock_version1 = HummockVersion::select(env.meta_store(), &version_id1.id())
        .await
        .unwrap()
        .unwrap();

    // safe epoch should be INVALID before success compaction
    assert_eq!(INVALID_EPOCH, hummock_version1.safe_epoch);

    // Get a compaction task.
    let mut compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, context_id, async { true })
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
    // In the test case, we assume that each SST contains data of 2 relational tables, and
    // one of them overlaps with the previous SST. So there will be one more relational tables
    // (for vnode mapping) than SSTs.
    assert_eq!(compact_task.get_vnode_mappings().len(), sst_num + 1);

    // Cancel the task and succeed.
    compact_task.task_status = false;
    assert!(hummock_manager
        .report_compact_task(&compact_task)
        .await
        .unwrap());
    // Cancel the task and told the task is not found, which may have been processed previously.
    assert!(!hummock_manager
        .report_compact_task(&compact_task)
        .await
        .unwrap());

    // check safe epoch in hummock version
    let version_id2 = CurrentHummockVersionId::get(env.meta_store())
        .await
        .unwrap()
        .unwrap();

    let hummock_version2 = HummockVersion::select(env.meta_store(), &version_id2.id())
        .await
        .unwrap()
        .unwrap();

    // safe epoch should still be INVALID since comapction task is canceled
    assert_eq!(INVALID_EPOCH, hummock_version2.safe_epoch);

    // Get a compaction task.
    let mut compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
        .await
        .unwrap()
        .unwrap();
    hummock_manager
        .assign_compaction_task(&compact_task, context_id, async { true })
        .await
        .unwrap();
    assert_eq!(compact_task.get_task_id(), 3);
    // Finish the task and succeed.
    compact_task.task_status = true;

    assert!(hummock_manager
        .report_compact_task(&compact_task)
        .await
        .unwrap());
    // Finish the task and told the task is not found, which may have been processed previously.
    assert!(!hummock_manager
        .report_compact_task(&compact_task)
        .await
        .unwrap());

    // check safe epoch in hummock version after success compaction
    let version_id3 = CurrentHummockVersionId::get(env.meta_store())
        .await
        .unwrap()
        .unwrap();

    let hummock_version3 = HummockVersion::select(env.meta_store(), &version_id3.id())
        .await
        .unwrap()
        .unwrap();

    // Since there is no pinned epochs, the safe epoch in version should be max_committed_epoch
    assert_eq!(epoch, hummock_version3.safe_epoch);
}

#[tokio::test]
async fn test_hummock_table() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&original_tables))
        .await
        .unwrap();

    let pinned_version = hummock_manager
        .pin_version(context_id, u64::MAX)
        .await
        .unwrap();
    assert_eq!(
        Ordering::Equal,
        pinned_version
            .levels
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .map(|info| info.id)
            .sorted()
            .cmp(original_tables.iter().map(|ot| ot.id).sorted())
    );

    // Confirm tables got are equal to original tables
    assert_eq!(
        get_sorted_sstable_ids(&original_tables),
        get_sorted_committed_sstable_ids(&pinned_version)
    );
}

#[tokio::test]
async fn test_hummock_transaction() {
    let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let mut committed_tables = vec![];

    // Add and commit tables in epoch1.
    // BEFORE:  committed_epochs = []
    // AFTER:   committed_epochs = [epoch1]
    let epoch1: u64 = 1;
    {
        // Add tables in epoch1
        let tables_in_epoch1 = generate_test_tables(epoch1, get_sst_ids(&hummock_manager, 2).await);
        // Get tables before committing epoch1. No tables should be returned.
        let pinned_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap();
        assert_eq!(pinned_version.max_committed_epoch, INVALID_EPOCH);
        assert!(get_sorted_committed_sstable_ids(&pinned_version).is_empty());

        hummock_manager
            .unpin_version(context_id, vec![pinned_version.id])
            .await
            .unwrap();

        // Commit epoch1
        hummock_manager
            .commit_epoch(epoch1, to_local_sstable_info(&tables_in_epoch1))
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let pinned_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap();
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );

        hummock_manager
            .unpin_version(context_id, vec![pinned_version.id])
            .await
            .unwrap();
    }

    // Add and commit tables in epoch2.
    // BEFORE:  committed_epochs = [epoch1]
    // AFTER:   committed_epochs = [epoch1, epoch2]
    let epoch2 = epoch1 + 1;
    {
        // Add tables in epoch2
        let tables_in_epoch2 = generate_test_tables(epoch2, get_sst_ids(&hummock_manager, 2).await);
        // Get tables before committing epoch2. tables_in_epoch1 should be returned and
        // tables_in_epoch2 should be invisible.
        let pinned_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap();
        assert_eq!(pinned_version.max_committed_epoch, epoch1);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, vec![pinned_version.id])
            .await
            .unwrap();

        // Commit epoch2
        hummock_manager
            .commit_epoch(epoch2, to_local_sstable_info(&tables_in_epoch2))
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch2);

        // Get tables after committing epoch2. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let pinned_version = hummock_manager
            .pin_version(context_id, u64::MAX)
            .await
            .unwrap();
        assert_eq!(pinned_version.max_committed_epoch, epoch2);
        assert_eq!(
            get_sorted_sstable_ids(&committed_tables),
            get_sorted_committed_sstable_ids(&pinned_version)
        );
        hummock_manager
            .unpin_version(context_id, vec![pinned_version.id])
            .await
            .unwrap();
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
    let (worker_node_2, _) = cluster_manager
        .add_worker_node(fake_host_address_2, WorkerType::ComputeNode)
        .await
        .unwrap();
    let context_id_2 = worker_node_2.id;

    assert_eq!(
        pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
        0
    );
    assert_eq!(
        pin_snapshots_sum(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
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
        pin_versions_sum(&HummockPinnedVersion::list(env.meta_store()).await.unwrap()),
        2
    );
    assert_eq!(
        pin_snapshots_sum(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
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
    assert_eq!(pin_snapshots_sum(&pinned_snapshots), 1);
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
    assert_eq!(
        pin_snapshots_sum(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
        0
    );
}

#[tokio::test]
async fn test_context_id_validation() {
    let (_env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
    let invalid_context_id = HummockContextId::MAX;
    let context_id = worker_node.id;
    let epoch: u64 = 1;
    let _original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);

    // Invalid context id is rejected.
    let error = hummock_manager
        .pin_version(invalid_context_id, u64::MAX)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidContext(_)));

    // Valid context id is accepted.
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
    assert!(matches!(error, Error::InvalidContext(_)));
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
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&original_tables))
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
        .delete_versions(&[FIRST_VERSION_ID])
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
        let test_tables = generate_test_tables(
            epoch,
            vec![
                hummock_manager.get_new_table_id().await.unwrap(),
                hummock_manager.get_new_table_id().await.unwrap(),
            ],
        );
        // Increase the version
        hummock_manager
            .commit_epoch(epoch, to_local_sstable_info(&test_tables))
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
        let test_tables = generate_test_tables(
            epoch,
            vec![
                hummock_manager.get_new_table_id().await.unwrap(),
                hummock_manager.get_new_table_id().await.unwrap(),
            ],
        );
        // Increase the version
        hummock_manager
            .commit_epoch(epoch, to_local_sstable_info(&test_tables))
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
async fn test_pin_snapshot_response_lost() {
    let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;

    let mut epoch: u64 = 1;
    let test_tables = generate_test_tables(
        epoch,
        vec![
            hummock_manager.get_new_table_id().await.unwrap(),
            hummock_manager.get_new_table_id().await.unwrap(),
        ],
    );
    // [ ] -> [ e0 ]
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&test_tables))
        .await
        .unwrap();
    epoch += 1;

    // Pin a snapshot with smallest last_pin
    // [ e0 ] -> [ e0:pinned ]
    let mut epoch_recorded_in_frontend = hummock_manager
        .pin_snapshot(context_id, INVALID_EPOCH)
        .await
        .unwrap()
        .epoch;
    assert_eq!(epoch_recorded_in_frontend, epoch - 1);
    assert_eq!(
        pin_snapshots_sum(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
        1
    );

    let test_tables = generate_test_tables(
        epoch,
        vec![
            hummock_manager.get_new_table_id().await.unwrap(),
            hummock_manager.get_new_table_id().await.unwrap(),
        ],
    );
    // [ e0:pinned ] -> [ e0:pinned, e1 ]
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&test_tables))
        .await
        .unwrap();
    epoch += 1;

    // Assume the response of the previous rpc is lost.
    // [ e0:pinned, e1 ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .pin_snapshot(context_id, INVALID_EPOCH)
        .await
        .unwrap()
        .epoch;
    assert_eq!(epoch_recorded_in_frontend, epoch - 1);
    assert_eq!(
        pin_snapshots_sum(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
        1
    );

    // Assume the response of the previous rpc is lost.
    // [ e0, e1:pinned ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .pin_snapshot(context_id, INVALID_EPOCH)
        .await
        .unwrap()
        .epoch;
    assert_eq!(epoch_recorded_in_frontend, epoch - 1);

    let test_tables = generate_test_tables(
        epoch,
        vec![
            hummock_manager.get_new_table_id().await.unwrap(),
            hummock_manager.get_new_table_id().await.unwrap(),
        ],
    );
    // [ e0, e1:pinned ] -> [ e0, e1:pinned, e2 ]
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&test_tables))
        .await
        .unwrap();
    epoch += 1;

    // Use correct snapshot id.
    // [ e0, e1:pinned, e2 ] -> [ e0, e1:pinned, e2:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .pin_snapshot(context_id, epoch_recorded_in_frontend)
        .await
        .unwrap()
        .epoch;
    assert_eq!(epoch_recorded_in_frontend, epoch - 1);
    assert_eq!(
        pin_snapshots_sum(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
        2
    );

    let test_tables = generate_test_tables(
        epoch,
        vec![
            hummock_manager.get_new_table_id().await.unwrap(),
            hummock_manager.get_new_table_id().await.unwrap(),
        ],
    );
    // [ e0, e1:pinned, e2:pinned ] -> [ e0, e1:pinned, e2:pinned, e3 ]
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&test_tables))
        .await
        .unwrap();
    epoch += 1;

    // Use u64::MAX as epoch to pin greatest snapshot
    // [ e0, e1:pinned, e2:pinned, e3 ] -> [ e0, e1:pinned, e2:pinned, e3::pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .pin_snapshot(context_id, u64::MAX)
        .await
        .unwrap()
        .epoch;
    assert_eq!(epoch_recorded_in_frontend, epoch - 1);
    assert_eq!(
        pin_snapshots_sum(&HummockPinnedSnapshot::list(env.meta_store()).await.unwrap()),
        3
    );
}

#[tokio::test]
async fn test_print_compact_task() {
    let (_, hummock_manager, _cluster_manager, _) = setup_compute_env(80).await;
    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&original_tables))
        .await
        .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(StaticCompactionGroupId::StateDefault.into())
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
    assert!(s.contains("Compaction task id: 1, target level: 6"));
}

#[tokio::test]
async fn test_invalid_sst_id() {
    let (_, hummock_manager, _cluster_manager, _) = setup_compute_env(80).await;
    let epoch = 1;
    let ssts = generate_test_tables(epoch, vec![HummockSSTableId::MAX]);
    let error = hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&ssts))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InternalError(_)));
}

#[tokio::test]
async fn test_mark_orphan_ssts() {
    let (_, hummock_manager, _cluster_manager, _) = setup_compute_env(80).await;
    let epoch = 1;

    // No SST is marked.
    let marked = hummock_manager
        .mark_orphan_ssts(Duration::from_secs(3600))
        .await
        .unwrap();
    assert!(marked.is_empty());

    let ssts = generate_test_tables(
        epoch,
        vec![hummock_manager.get_new_table_id().await.unwrap()],
    );
    let marked = hummock_manager
        .mark_orphan_ssts(Duration::from_secs(0))
        .await
        .unwrap();
    // The SST is marked.
    assert_eq!(marked.len(), 1);
    assert_eq!(
        marked.first().as_ref().unwrap().id,
        ssts.first().as_ref().unwrap().id
    );
    // Cannot commit_epoch for marked SST ids.
    let error = hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&ssts))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InternalError(_)));
}

#[tokio::test]
async fn test_trigger_manual_compaction() {
    let (env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
    let context_id = worker_node.id;
    let sst_num = 2usize;

    // Construct vnode mappings for generating compaction tasks.
    let parallel_units = cluster_manager
        .list_parallel_units(Some(ParallelUnitType::Hash))
        .await;
    env.hash_mapping_manager()
        .build_fragment_hash_mapping(1, &parallel_units);
    for table_id in 1..sst_num + 2 {
        env.hash_mapping_manager()
            .set_fragment_state_table(1, table_id as u32);
    }

    {
        // to check no compactor
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into())
            .await;

        assert_eq!(
            "internal error: trigger_manual_compaction No compactor is available. compaction_group 2",
            result.err().unwrap().to_string()
        );
    }

    // No compaction task available.
    let compactor_manager_ref = hummock_manager.compactor_manager_ref_for_test();
    let receiver = compactor_manager_ref.add_compactor(context_id);
    {
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into())
            .await;
        assert_eq!("internal error: trigger_manual_compaction No compaction_task is available. compaction_group 2", result.err().unwrap().to_string());
    }

    // Add some sstables and commit.
    let epoch: u64 = 1;
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, sst_num).await);
    hummock_manager
        .commit_epoch(epoch, to_local_sstable_info(&original_tables))
        .await
        .unwrap();

    // check safe epoch in hummock version
    let version_id1 = CurrentHummockVersionId::get(env.meta_store())
        .await
        .unwrap()
        .unwrap();
    let hummock_version1 = HummockVersion::select(env.meta_store(), &version_id1.id())
        .await
        .unwrap()
        .unwrap();

    // safe epoch should be INVALID before success compaction
    assert_eq!(INVALID_EPOCH, hummock_version1.safe_epoch);

    {
        // to check compactor send task fail
        drop(receiver);
        {
            let result = hummock_manager
                .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into())
                .await;
            assert!(result.is_err());
        }
    }

    compactor_manager_ref.remove_compactor(context_id);
    let _receiver = compactor_manager_ref.add_compactor(context_id);

    {
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into())
            .await;
        assert!(result.is_ok());
    }

    let task_id: u64 = 3;
    let compact_task = hummock_manager
        .compaction_task_from_assignment_for_test(task_id)
        .await
        .unwrap()
        .compact_task
        .unwrap();
    assert_eq!(task_id, compact_task.task_id);

    {
        // all sst pending , test no compaction avail
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into())
            .await;
        assert!(result.is_err());
    }
}
