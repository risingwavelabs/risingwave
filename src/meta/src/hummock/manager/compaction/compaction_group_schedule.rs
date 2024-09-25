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

use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::DerefMut;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compact_task::ReportTask;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::TableGroupInfo;
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::version::{GroupDelta, GroupDeltas};
use risingwave_hummock_sdk::{can_concat, CompactionGroupId};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::{PbGroupMerge, PbStateTableInfoDelta};
use thiserror_ext::AsReport;

use crate::hummock::error::{Error, Result};
use crate::hummock::manager::transaction::HummockVersionTransaction;
use crate::hummock::manager::{commit_multi_var, HummockManager};
use crate::hummock::metrics_utils::remove_compaction_group_in_sst_stat;

impl HummockManager {
    /// Splits a compaction group into two. The new one will contain `table_ids`.
    /// Returns the newly created compaction group id.
    pub async fn split_compaction_group(
        &self,
        parent_group_id: CompactionGroupId,
        table_ids: &[StateTableId],
        partition_vnode_count: u32,
    ) -> Result<CompactionGroupId> {
        let result = self
            .move_state_table_to_compaction_group(parent_group_id, table_ids, partition_vnode_count)
            .await?;

        Ok(result)
    }

    pub async fn merge_compaction_group(
        &self,
        group_1: CompactionGroupId,
        group_2: CompactionGroupId,
    ) -> Result<()> {
        let compaction_guard = self.compaction.write().await;
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        // Validate parameters.
        if !versioning.current_version.levels.contains_key(&group_1) {
            return Err(Error::CompactionGroup(format!("invalid group {}", group_1)));
        }

        if !versioning.current_version.levels.contains_key(&group_2) {
            return Err(Error::CompactionGroup(format!("invalid group {}", group_2)));
        }

        let state_table_info = versioning.current_version.state_table_info.clone();
        let mut member_table_ids_1 = state_table_info
            .compaction_group_member_table_ids(group_1)
            .iter()
            .cloned()
            .collect_vec();

        let mut member_table_ids_2 = state_table_info
            .compaction_group_member_table_ids(group_2)
            .iter()
            .cloned()
            .collect_vec();

        debug_assert!(!member_table_ids_1.is_empty());
        debug_assert!(!member_table_ids_2.is_empty());
        assert!(member_table_ids_1.is_sorted());
        assert!(member_table_ids_2.is_sorted());

        // Make sure `member_table_ids_1` is smaller than `member_table_ids_2`
        let (left_group_id, right_group_id) =
            if member_table_ids_1.first().unwrap() < member_table_ids_2.first().unwrap() {
                (group_1, group_2)
            } else {
                std::mem::swap(&mut member_table_ids_1, &mut member_table_ids_2);
                (group_2, group_1)
            };

        // We can only merge two groups with non-overlapping member table ids
        if member_table_ids_1.last().unwrap() >= member_table_ids_2.first().unwrap() {
            return Err(Error::CompactionGroup(format!(
                "invalid merge group_1 {} group_2 {}",
                left_group_id, right_group_id
            )));
        }

        let combined_member_table_ids = member_table_ids_1
            .iter()
            .chain(member_table_ids_2.iter())
            .collect_vec();
        assert!(combined_member_table_ids.is_sorted());

        // check duplicated sst_id
        let mut sst_id_set = HashSet::new();
        for sst_id in versioning
            .current_version
            .get_sst_ids_by_group_id(left_group_id)
            .chain(
                versioning
                    .current_version
                    .get_sst_ids_by_group_id(right_group_id),
            )
        {
            if !sst_id_set.insert(sst_id) {
                return Err(Error::CompactionGroup(format!(
                    "invalid merge group_1 {} group_2 {} duplicated sst_id {}",
                    left_group_id, right_group_id, sst_id
                )));
            }
        }

        // check branched sst on non-overlap level
        {
            let left_levels = versioning
                .current_version
                .get_compaction_group_levels(group_1);

            let right_levels = versioning
                .current_version
                .get_compaction_group_levels(group_2);

            // we can not check the l0 sub level, because the sub level id will be rewritten when merge
            // This check will ensure that other non-overlapping level ssts can be concat and that the key_range is correct.
            let max_level = std::cmp::max(left_levels.levels.len(), right_levels.levels.len());
            for level_idx in 1..=max_level {
                let left_level = left_levels.get_level(level_idx);
                let right_level = right_levels.get_level(level_idx);
                if left_level.table_infos.is_empty() || right_level.table_infos.is_empty() {
                    continue;
                }

                let left_last_sst = left_level.table_infos.last().unwrap().clone();
                let right_first_sst = right_level.table_infos.first().unwrap().clone();
                let left_sst_id = left_last_sst.sst_id;
                let right_sst_id = right_first_sst.sst_id;
                let left_obj_id = left_last_sst.object_id;
                let right_obj_id = right_first_sst.object_id;

                // Since the sst key_range within a group is legal, we only need to check the ssts adjacent to the two groups.
                if !can_concat(&[left_last_sst, right_first_sst]) {
                    return Err(Error::CompactionGroup(format!(
                        "invalid merge group_1 {} group_2 {} level_idx {} left_last_sst_id {} right_first_sst_id {} left_obj_id {} right_obj_id {}",
                        left_group_id, right_group_id, level_idx, left_sst_id, right_sst_id, left_obj_id, right_obj_id
                    )));
                }
            }
        }

        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            self.env.notification_manager(),
            &self.metrics,
        );
        let mut new_version_delta = version.new_delta();

        let target_compaction_group_id = {
            // merge right_group_id to left_group_id and remove right_group_id
            new_version_delta.group_deltas.insert(
                left_group_id,
                GroupDeltas {
                    group_deltas: vec![GroupDelta::GroupMerge(PbGroupMerge {
                        left_group_id,
                        right_group_id,
                    })],
                },
            );
            left_group_id
        };

        // TODO: remove compaciton group_id from state_table_info
        // rewrite compaction_group_id for all tables
        new_version_delta.with_latest_version(|version, new_version_delta| {
            for table_id in combined_member_table_ids {
                let table_id = TableId::new(table_id.table_id());
                let info = version
                    .state_table_info
                    .info()
                    .get(&table_id)
                    .expect("have check exist previously");
                assert!(new_version_delta
                    .state_table_info_delta
                    .insert(
                        table_id,
                        PbStateTableInfoDelta {
                            committed_epoch: info.committed_epoch,
                            compaction_group_id: target_compaction_group_id,
                        }
                    )
                    .is_none());
            }
        });

        {
            let mut compaction_group_manager = self.compaction_group_manager.write().await;
            let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();

            // for metrics reclaim
            {
                let right_group_max_level = new_version_delta
                    .latest_version()
                    .get_compaction_group_levels(right_group_id)
                    .levels
                    .len();

                remove_compaction_group_in_sst_stat(
                    &self.metrics,
                    right_group_id,
                    right_group_max_level,
                );
            }

            new_version_delta.pre_apply();

            // remove right_group_id
            compaction_groups_txn.remove(right_group_id);
            commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;
        }

        // Instead of handling DeltaType::GroupConstruct for time travel, simply enforce a version snapshot.
        versioning.mark_next_time_travel_version_snapshot();

        // cancel tasks
        let mut canceled_tasks = vec![];
        // after merge, all tasks in right_group_id should be canceled
        // otherwise, pending size calculation by level handler will make some mistake
        for task_assignment in compaction_guard.compact_task_assignment.values() {
            if let Some(task) = task_assignment.compact_task.as_ref() {
                let need_cancel = task.compaction_group_id == right_group_id;
                if need_cancel {
                    canceled_tasks.push(ReportTask {
                        task_id: task.task_id,
                        task_status: TaskStatus::ManualCanceled,
                        table_stats_change: HashMap::default(),
                        sorted_output_ssts: vec![],
                    });
                }
            }
        }

        drop(versioning_guard);
        drop(compaction_guard);
        self.report_compact_tasks(canceled_tasks).await?;

        self.metrics
            .merge_compaction_group_count
            .with_label_values(&[&left_group_id.to_string()])
            .inc();

        Ok(())
    }

    pub async fn try_split_compaction_group(
        &self,
        table_write_throughput: &HashMap<u32, VecDeque<u64>>,
        checkpoint_secs: u64,
        group: &TableGroupInfo,
        created_tables: &HashSet<u32>,
    ) {
        // split high throughput table to dedicated compaction group
        for (table_id, table_size) in &group.table_statistic {
            self.try_move_table_to_dedicated_cg(
                table_write_throughput,
                table_id,
                table_size,
                !created_tables.contains(table_id),
                checkpoint_secs,
                group.group_id,
                group.group_size,
            )
            .await;
        }
    }

    pub async fn try_move_table_to_dedicated_cg(
        &self,
        table_write_throughput: &HashMap<u32, VecDeque<u64>>,
        table_id: &u32,
        table_size: &u64,
        is_creating_table: bool,
        checkpoint_secs: u64,
        parent_group_id: u64,
        group_size: u64,
    ) {
        let default_group_id: CompactionGroupId = StaticCompactionGroupId::StateDefault.into();
        let mv_group_id: CompactionGroupId = StaticCompactionGroupId::MaterializedView.into();
        let partition_vnode_count = self.env.opts.partition_vnode_count;
        let window_size =
            self.env.opts.table_info_statistic_history_times / (checkpoint_secs as usize);

        let mut is_high_write_throughput = false;
        let mut is_low_write_throughput = true;
        if let Some(history) = table_write_throughput.get(table_id) {
            if history.len() >= window_size {
                is_high_write_throughput = history.iter().all(|throughput| {
                    *throughput / checkpoint_secs > self.env.opts.table_write_throughput_threshold
                });
                is_low_write_throughput = history.iter().any(|throughput| {
                    *throughput / checkpoint_secs < self.env.opts.min_table_split_write_throughput
                });
            }
        }

        let state_table_size = *table_size;

        // 1. Avoid splitting a creating table
        // 2. Avoid splitting a is_low_write_throughput creating table
        // 3. Avoid splitting a non-high throughput medium-sized table
        if is_creating_table
            || (is_low_write_throughput)
            || (state_table_size < self.env.opts.min_table_split_size && !is_high_write_throughput)
        {
            return;
        }

        // do not split a large table and a small table because it would increase IOPS
        // of small table.
        if parent_group_id != default_group_id && parent_group_id != mv_group_id {
            let rest_group_size = group_size - state_table_size;
            if rest_group_size < state_table_size
                && rest_group_size < self.env.opts.min_table_split_size
            {
                return;
            }
        }

        let ret = self
            .move_state_table_to_compaction_group(
                parent_group_id,
                &[*table_id],
                partition_vnode_count,
            )
            .await;
        match ret {
            Ok(new_group_id) => {
                tracing::info!("move state table [{}] from group-{} to group-{} success table_vnode_partition_count {:?}", table_id, parent_group_id, new_group_id, partition_vnode_count);
            }
            Err(e) => {
                tracing::info!(
                    error = %e.as_report(),
                    "failed to move state table [{}] from group-{}",
                    table_id,
                    parent_group_id,
                )
            }
        }
    }
}
