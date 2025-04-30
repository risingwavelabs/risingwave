// Copyright 2025 RisingWave Labs
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

#![cfg(any(test, feature = "test"))]

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::key::key_with_epoch;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::level::Levels;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
use risingwave_hummock_sdk::table_watermark::TableWatermarks;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionStateTableInfo};
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockEpoch, HummockSstableObjectId, LocalSstableInfo, SyncResult,
};
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::worker_node::Property;
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::CompactionConfig;
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_rpc_client::HummockMetaClient;

use crate::controller::catalog::CatalogController;
use crate::controller::cluster::{ClusterController, ClusterControllerRef};
use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::compaction::selector::{LocalSelectorStatistic, default_compaction_selector};
use crate::hummock::compaction::{CompactionDeveloperConfig, CompactionSelectorContext};
use crate::hummock::level_handler::LevelHandler;
pub use crate::hummock::manager::CommitEpochInfo;
use crate::hummock::model::CompactionGroup;
use crate::hummock::{CompactorManager, HummockManager, HummockManagerRef};
use crate::manager::MetaSrvEnv;
use crate::rpc::metrics::MetaMetrics;

pub fn to_local_sstable_info(ssts: &[SstableInfo]) -> Vec<LocalSstableInfo> {
    ssts.iter()
        .map(|sst| LocalSstableInfo::for_test(sst.clone()))
        .collect_vec()
}

// This function has 3 phases:
// 1. add 3 ssts to
// 2. trigger a compaction and replace the input from phase 1 with the 1 new sst
// 3. add 1 new sst
// Please make sure the function do what you want before using it.
pub async fn add_test_tables(
    hummock_manager: &HummockManager,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    compaction_group_id: CompactionGroupId,
) -> Vec<Vec<SstableInfo>> {
    // Increase version by 2.

    use risingwave_common::util::epoch::EpochExt;

    let mut epoch = test_epoch(1);
    let sstable_ids = get_sst_ids(hummock_manager, 3).await;
    let test_tables = generate_test_sstables_with_table_id(epoch, 1, sstable_ids);
    register_sstable_infos_to_compaction_group(hummock_manager, &test_tables, compaction_group_id)
        .await;
    let test_local_tables = to_local_sstable_info(&test_tables);
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: test_local_tables,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Simulate a compaction and increase version by 1.
    let test_tables_2 = generate_test_tables(epoch, get_sst_ids(hummock_manager, 1).await);
    register_sstable_infos_to_compaction_group(
        hummock_manager,
        &test_tables_2,
        compaction_group_id,
    )
    .await;
    let mut compact_task = hummock_manager
        .get_compact_task(compaction_group_id, &mut default_compaction_selector())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        compact_task
            .input_ssts
            .iter()
            .map(|i| i.table_infos.len())
            .sum::<usize>(),
        3
    );

    compact_task.target_level = 6;
    hummock_manager
        .report_compact_task_for_test(
            compact_task.task_id,
            Some(compact_task),
            TaskStatus::Success,
            test_tables_2.clone(),
            None,
        )
        .await
        .unwrap();
    // Increase version by 1.
    epoch.inc_epoch();
    let test_tables_3 = generate_test_tables(epoch, get_sst_ids(hummock_manager, 1).await);
    register_sstable_infos_to_compaction_group(
        hummock_manager,
        &test_tables_3,
        compaction_group_id,
    )
    .await;
    let test_local_tables_3 = to_local_sstable_info(&test_tables_3);
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: test_local_tables_3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    vec![test_tables, test_tables_2, test_tables_3]
}

pub fn generate_test_sstables_with_table_id(
    epoch: u64,
    table_id: u32,
    sst_ids: Vec<HummockSstableObjectId>,
) -> Vec<SstableInfo> {
    let mut sst_info = vec![];
    for (i, sst_id) in sst_ids.into_iter().enumerate() {
        let object_size = 2;
        sst_info.push(
            SstableInfoInner {
                object_id: sst_id,
                sst_id,
                key_range: KeyRange {
                    left: Bytes::from(key_with_epoch(
                        format!("{:03}\0\0_key_test_{:05}", table_id, i + 1)
                            .as_bytes()
                            .to_vec(),
                        epoch,
                    )),
                    right: Bytes::from(key_with_epoch(
                        format!("{:03}\0\0_key_test_{:05}", table_id, (i + 1) * 10)
                            .as_bytes()
                            .to_vec(),
                        epoch,
                    )),
                    right_exclusive: false,
                },
                file_size: object_size,
                table_ids: vec![table_id],
                uncompressed_file_size: object_size,
                max_epoch: epoch,
                sst_size: object_size,
                ..Default::default()
            }
            .into(),
        );
    }
    sst_info
}

pub fn generate_test_tables(epoch: u64, sst_ids: Vec<HummockSstableObjectId>) -> Vec<SstableInfo> {
    let mut sst_info = vec![];
    for (i, sst_id) in sst_ids.into_iter().enumerate() {
        let object_size = 2;
        sst_info.push(
            SstableInfoInner {
                object_id: sst_id,
                sst_id,
                key_range: KeyRange {
                    left: Bytes::from(iterator_test_key_of_epoch(sst_id, i + 1, epoch)),
                    right: Bytes::from(iterator_test_key_of_epoch(sst_id, (i + 1) * 10, epoch)),
                    right_exclusive: false,
                },
                file_size: object_size,
                table_ids: vec![sst_id as u32, sst_id as u32 * 10000],
                uncompressed_file_size: object_size,
                max_epoch: epoch,
                sst_size: object_size,
                ..Default::default()
            }
            .into(),
        );
    }
    sst_info
}

pub async fn register_sstable_infos_to_compaction_group(
    compaction_group_manager_ref: &HummockManager,
    sstable_infos: &[SstableInfo],
    compaction_group_id: CompactionGroupId,
) {
    let table_ids = sstable_infos
        .iter()
        .flat_map(|sstable_info| &sstable_info.table_ids)
        .sorted()
        .dedup()
        .cloned()
        .collect_vec();
    register_table_ids_to_compaction_group(
        compaction_group_manager_ref,
        &table_ids,
        compaction_group_id,
    )
    .await;
}

pub async fn register_table_ids_to_compaction_group(
    hummock_manager_ref: &HummockManager,
    table_ids: &[u32],
    compaction_group_id: CompactionGroupId,
) {
    hummock_manager_ref
        .register_table_ids_for_test(
            &table_ids
                .iter()
                .map(|table_id| (*table_id, compaction_group_id))
                .collect_vec(),
        )
        .await
        .unwrap();
}

pub async fn unregister_table_ids_from_compaction_group(
    hummock_manager_ref: &HummockManager,
    table_ids: &[u32],
) {
    hummock_manager_ref
        .unregister_table_ids(table_ids.iter().map(|table_id| TableId::new(*table_id)))
        .await
        .unwrap();
}

/// Generate keys like `001_key_test_00002` with timestamp `epoch`.
pub fn iterator_test_key_of_epoch(
    table: HummockSstableObjectId,
    idx: usize,
    ts: HummockEpoch,
) -> Vec<u8> {
    // key format: {prefix_index}_version
    key_with_epoch(
        format!("{:03}\0\0_key_test_{:05}", table, idx)
            .as_bytes()
            .to_vec(),
        ts,
    )
}

pub fn get_sorted_object_ids(sstables: &[SstableInfo]) -> Vec<HummockSstableObjectId> {
    sstables
        .iter()
        .map(|table| table.object_id)
        .sorted()
        .collect_vec()
}

pub fn get_sorted_committed_object_ids(
    hummock_version: &HummockVersion,
    compaction_group_id: CompactionGroupId,
) -> Vec<HummockSstableObjectId> {
    let levels = match hummock_version.levels.get(&compaction_group_id) {
        Some(levels) => levels,
        None => return vec![],
    };
    levels
        .levels
        .iter()
        .chain(levels.l0.sub_levels.iter())
        .flat_map(|levels| levels.table_infos.iter().map(|info| info.object_id))
        .sorted()
        .collect_vec()
}

pub async fn setup_compute_env_with_config(
    port: i32,
    config: CompactionConfig,
) -> (
    MetaSrvEnv,
    HummockManagerRef,
    ClusterControllerRef,
    WorkerId,
) {
    setup_compute_env_with_metric(port, config, None).await
}

pub async fn setup_compute_env_with_metric(
    port: i32,
    config: CompactionConfig,
    meta_metric: Option<MetaMetrics>,
) -> (
    MetaSrvEnv,
    HummockManagerRef,
    ClusterControllerRef,
    WorkerId,
) {
    let env = MetaSrvEnv::for_test().await;
    let cluster_ctl = Arc::new(
        ClusterController::new(env.clone(), Duration::from_secs(1))
            .await
            .unwrap(),
    );
    let catalog_ctl = Arc::new(CatalogController::new(env.clone()).await.unwrap());

    let compactor_manager = Arc::new(CompactorManager::for_test());

    let (compactor_streams_change_tx, _compactor_streams_change_rx) =
        tokio::sync::mpsc::unbounded_channel();

    let (iceberg_compactor_streams_change_tx, _compactor_streams_change_rx) =
        tokio::sync::mpsc::unbounded_channel();

    let hummock_manager = HummockManager::with_config(
        env.clone(),
        cluster_ctl.clone(),
        catalog_ctl,
        Arc::new(meta_metric.unwrap_or_default()),
        compactor_manager,
        config,
        compactor_streams_change_tx,
        iceberg_compactor_streams_change_tx,
    )
    .await;

    let fake_host_address = HostAddress {
        host: "127.0.0.1".to_owned(),
        port,
    };
    let fake_parallelism = 4;
    let worker_id = cluster_ctl
        .add_worker(
            WorkerType::ComputeNode,
            fake_host_address,
            Property {
                is_streaming: true,
                is_serving: true,
                is_unschedulable: false,
                parallelism: fake_parallelism as _,
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .unwrap();
    (env, hummock_manager, cluster_ctl, worker_id)
}

pub async fn setup_compute_env(
    port: i32,
) -> (
    MetaSrvEnv,
    HummockManagerRef,
    ClusterControllerRef,
    WorkerId,
) {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(1)
        .level0_max_compact_file_number(130)
        .level0_sub_level_compact_level_count(1)
        .level0_overlapping_sub_level_compact_level_count(1)
        .build();
    setup_compute_env_with_config(port, config).await
}

pub async fn get_sst_ids(
    hummock_manager: &HummockManager,
    number: u32,
) -> Vec<HummockSstableObjectId> {
    let range = hummock_manager.get_new_sst_ids(number).await.unwrap();
    (range.start_id..range.end_id).collect_vec()
}

pub async fn add_ssts(
    epoch: HummockEpoch,
    hummock_manager: &HummockManager,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
) -> Vec<SstableInfo> {
    let table_ids = get_sst_ids(hummock_manager, 3).await;
    let test_tables = generate_test_sstables_with_table_id(test_epoch(epoch), 1, table_ids);
    let ssts = to_local_sstable_info(&test_tables);
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: ssts,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    test_tables
}

pub fn compaction_selector_context<'a>(
    group: &'a CompactionGroup,
    levels: &'a Levels,
    member_table_ids: &'a BTreeSet<TableId>,
    level_handlers: &'a mut [LevelHandler],
    selector_stats: &'a mut LocalSelectorStatistic,
    table_id_to_options: &'a HashMap<u32, TableOption>,
    developer_config: Arc<CompactionDeveloperConfig>,
    table_watermarks: &'a HashMap<TableId, Arc<TableWatermarks>>,
    state_table_info: &'a HummockVersionStateTableInfo,
) -> CompactionSelectorContext<'a> {
    CompactionSelectorContext {
        group,
        levels,
        member_table_ids,
        level_handlers,
        selector_stats,
        table_id_to_options,
        developer_config,
        table_watermarks,
        state_table_info,
    }
}

pub async fn get_compaction_group_id_by_table_id(
    hummock_manager_ref: HummockManagerRef,
    table_id: u32,
) -> u64 {
    let version = hummock_manager_ref.get_current_version().await;
    let mapping = version.state_table_info.build_table_compaction_group_id();
    *mapping.get(&(table_id.into())).unwrap()
}
