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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::DerefMut;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::compact_task::ReportTask;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::TableGroupInfo;
use risingwave_hummock_sdk::compaction_group::{
    group_split, StateTableId, StaticCompactionGroupId,
};
use risingwave_hummock_sdk::version::{GroupDelta, GroupDeltas};
use risingwave_hummock_sdk::{can_concat, CompactionGroupId};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
use risingwave_pb::hummock::{
    CompatibilityVersion, PbGroupConstruct, PbGroupMerge, PbStateTableInfoDelta,
};
use thiserror_ext::AsReport;

use crate::hummock::error::{Error, Result};
use crate::hummock::manager::transaction::HummockVersionTransaction;
use crate::hummock::manager::{commit_multi_var, HummockManager};
use crate::hummock::metrics_utils::remove_compaction_group_in_sst_stat;
use crate::hummock::sequence::{next_compaction_group_id, next_sstable_object_id};

impl HummockManager {
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
        let compact_task_assignments =
            compaction_guard.get_compact_task_assignments_by_group_id(right_group_id);
        compact_task_assignments
            .into_iter()
            .for_each(|task_assignment| {
                if let Some(task) = task_assignment.compact_task.as_ref() {
                    assert_eq!(task.compaction_group_id, right_group_id);
                    canceled_tasks.push(ReportTask {
                        task_id: task.task_id,
                        task_status: TaskStatus::ManualCanceled,
                        table_stats_change: HashMap::default(),
                        sorted_output_ssts: vec![],
                    });
                }
            });

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
            .move_state_tables_to_dedicated_compaction_group(
                parent_group_id,
                &[*table_id],
                partition_vnode_count,
            )
            .await;
        match ret {
            Ok((target_compaction_group_id, cg_id_to_table_ids)) => {
                tracing::info!(
                    "split state table [{}] success (source_group_id {} target_group_id {})  cg_id_to_table_ids {:?}",
                    table_id,
                    parent_group_id,
                    target_compaction_group_id,
                    cg_id_to_table_ids
                );
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

impl HummockManager {
    /// Split `table_ids` to a dedicated compaction group.(will be split by the `table_id` and `vnode`.)
    /// Returns the compaction group id containing the `table_ids` and the mapping of compaction group id to table ids.
    /// The split will follow the following rules
    /// 1. ssts with `key_range.left` greater than `split_key` will be split to the right group
    /// 2. the sst containing `split_key` will be split into two separate ssts and their `key_range` will be changed `sst_1`: [`sst.key_range.left`, `split_key`) `sst_2`: [`split_key`, `sst.key_range.right`]
    /// 3. currently only `vnode` 0 and `vnode` max is supported. (Due to the above rule, vnode max will be rewritten as `table_id` + 1, `vnode` 0)
    ///     `parent_group_id`: the `group_id` to split
    ///     `split_table_ids`: the `table_ids` to split, now we still support to split multiple tables to one group at once, pass `split_table_ids` for per `split` operation for checking
    ///     `table_id_to_split`: the `table_id` to split
    ///     `vnode_to_split`: the `vnode` to split
    ///     `partition_vnode_count`: the partition count for the single table group if need
    async fn split_compaction_group_impl(
        &self,
        parent_group_id: CompactionGroupId,
        split_table_ids: &[StateTableId],
        table_id_to_split: StateTableId,
        vnode_to_split: VirtualNode,
        partition_vnode_count: u32,
    ) -> Result<Vec<(CompactionGroupId, Vec<StateTableId>)>> {
        let mut result = vec![];
        let compaction_guard = self.compaction.write().await;
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        // Validate parameters.
        if !versioning
            .current_version
            .levels
            .contains_key(&parent_group_id)
        {
            return Err(Error::CompactionGroup(format!(
                "invalid group {}",
                parent_group_id
            )));
        }

        let member_table_ids = versioning
            .current_version
            .state_table_info
            .compaction_group_member_table_ids(parent_group_id);

        if !member_table_ids.contains(&TableId::new(table_id_to_split)) {
            return Err(Error::CompactionGroup(format!(
                "table {} doesn't in group {}",
                table_id_to_split, parent_group_id
            )));
        }

        let split_full_key = group_split::build_split_full_key(table_id_to_split, vnode_to_split);

        // change to vec for partition
        let table_ids = member_table_ids
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec();
        // avoid decode split_key when caller is aware of the table_id and vnode
        let (table_ids_left, table_ids_right) =
            group_split::split_table_ids_with_table_id_and_vnode(
                &table_ids,
                split_full_key.user_key.table_id.table_id(),
                split_full_key.user_key.get_vnode_id(),
            );
        if table_ids_left.is_empty() || table_ids_right.is_empty() {
            // not need to split group if all tables are in the same side
            if !table_ids_left.is_empty() {
                result.push((parent_group_id, table_ids_left));
            }

            if !table_ids_right.is_empty() {
                result.push((parent_group_id, table_ids_right));
            }
            return Ok(result);
        }

        result.push((parent_group_id, table_ids_left));

        let split_key: Bytes = split_full_key.encode().into();

        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            self.env.notification_manager(),
            &self.metrics,
        );
        let mut new_version_delta = version.new_delta();

        let split_sst_count = new_version_delta
            .latest_version()
            .count_new_ssts_in_group_split(parent_group_id, split_key.clone());

        let new_sst_start_id = next_sstable_object_id(&self.env, split_sst_count).await?;
        let (new_compaction_group_id, config) = {
            // All NewCompactionGroup pairs are mapped to one new compaction group.
            let new_compaction_group_id = next_compaction_group_id(&self.env).await?;
            // The new config will be persisted later.
            let config = self
                .compaction_group_manager
                .read()
                .await
                .default_compaction_config()
                .as_ref()
                .clone();

            #[expect(deprecated)]
            // fill the deprecated field with default value
            new_version_delta.group_deltas.insert(
                new_compaction_group_id,
                GroupDeltas {
                    group_deltas: vec![GroupDelta::GroupConstruct(PbGroupConstruct {
                        group_config: Some(config.clone()),
                        group_id: new_compaction_group_id,
                        parent_group_id,
                        new_sst_start_id,
                        table_ids: vec![],
                        version: CompatibilityVersion::SplitGroupByTableId as i32, // for compatibility
                        split_key: Some(split_key.into()),
                    })],
                },
            );
            (new_compaction_group_id, config)
        };

        new_version_delta.with_latest_version(|version, new_version_delta| {
            for table_id in &table_ids_right {
                let table_id = TableId::new(*table_id);
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
                            safe_epoch: info.safe_epoch,
                            compaction_group_id: new_compaction_group_id,
                        }
                    )
                    .is_none());
            }
        });

        result.push((new_compaction_group_id, table_ids_right));

        {
            let mut compaction_group_manager = self.compaction_group_manager.write().await;
            let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();
            compaction_groups_txn
                .create_compaction_groups(new_compaction_group_id, Arc::new(config));

            // check if need to update the compaction config for the single table group and guarantee the operation atomicity
            // `partition_vnode_count` only works inside a table, to avoid a lot of slicing sst, we only enable it in groups with high throughput and only one table.
            // The target `table_ids` might be split to an existing group, so we need to try to update its config
            for (cg_id, table_ids) in &result {
                // check the split_tables had been place to the dedicated compaction group
                if table_ids.len() == 1 && table_ids == split_table_ids {
                    if let Err(err) = compaction_groups_txn.update_compaction_config(
                        &[*cg_id],
                        &[MutableConfig::SplitWeightByVnode(partition_vnode_count)],
                    ) {
                        tracing::error!(
                            error = %err.as_report(),
                            "failed to update compaction config for group-{}",
                            cg_id
                        );
                    }
                }
            }

            new_version_delta.pre_apply();
            commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;
        }
        // Instead of handling DeltaType::GroupConstruct for time travel, simply enforce a version snapshot.
        versioning.mark_next_time_travel_version_snapshot();
        let mut canceled_tasks = vec![];
        let compact_task_assignments =
            compaction_guard.get_compact_task_assignments_by_group_id(parent_group_id);
        compact_task_assignments
            .into_iter()
            .for_each(|task_assignment| {
                if let Some(task) = task_assignment.compact_task.as_ref() {
                    let need_cancel = HummockManager::is_compact_task_expired(
                        &task.into(),
                        &versioning.current_version,
                    );
                    if need_cancel {
                        canceled_tasks.push(ReportTask {
                            task_id: task.task_id,
                            task_status: TaskStatus::ManualCanceled,
                            table_stats_change: HashMap::default(),
                            sorted_output_ssts: vec![],
                        });
                    }
                }
            });

        drop(versioning_guard);
        drop(compaction_guard);
        self.report_compact_tasks(canceled_tasks).await?;

        self.metrics
            .split_compaction_group_count
            .with_label_values(&[&parent_group_id.to_string()])
            .inc();

        Ok(result)
    }

    /// Split `table_ids` to a dedicated compaction group.
    /// Returns the compaction group id containing the `table_ids` and the mapping of compaction group id to table ids.
    pub async fn move_state_tables_to_dedicated_compaction_group(
        &self,
        parent_group_id: CompactionGroupId,
        table_ids: &[StateTableId],
        partition_vnode_count: u32,
    ) -> Result<(
        CompactionGroupId,
        BTreeMap<CompactionGroupId, Vec<StateTableId>>,
    )> {
        if table_ids.is_empty() {
            return Err(Error::CompactionGroup(
                "table_ids must not be empty".to_string(),
            ));
        }

        if !table_ids.is_sorted() {
            return Err(Error::CompactionGroup(
                "table_ids must be sorted".to_string(),
            ));
        }

        let parent_table_ids = {
            let versioning_guard = self.versioning.read().await;
            versioning_guard
                .current_version
                .state_table_info
                .compaction_group_member_table_ids(parent_group_id)
                .iter()
                .map(|table_id| table_id.table_id)
                .collect_vec()
        };

        if parent_table_ids == table_ids {
            return Err(Error::CompactionGroup(format!(
                "invalid split attempt for group {}: all member tables are moved",
                parent_group_id
            )));
        }

        fn check_table_ids_valid(cg_id_to_table_ids: &BTreeMap<u64, Vec<u32>>) {
            // 1. table_ids in different cg are sorted.
            {
                cg_id_to_table_ids
                    .iter()
                    .for_each(|(_cg_id, table_ids)| assert!(table_ids.is_sorted()));
            }

            // 2.table_ids in different cg are non-overlapping
            {
                let mut table_table_ids_vec = cg_id_to_table_ids.values().cloned().collect_vec();
                table_table_ids_vec.sort_by(|a, b| a[0].cmp(&b[0]));
                assert!(table_table_ids_vec.concat().is_sorted());
            }

            // 3.table_ids belong to one and only one cg.
            {
                let mut all_table_ids = HashSet::new();
                for table_ids in cg_id_to_table_ids.values() {
                    for table_id in table_ids {
                        assert!(all_table_ids.insert(*table_id));
                    }
                }
            }
        }

        // move [3,4,5,6]
        // [1,2,3,4,5,6,7,8,9,10] -> [1,2] [3,4,5,6] [7,8,9,10]
        // split key
        // 1. table_id = 3, vnode = 0, epoch = MAX
        // 2. table_id = 7, vnode = 0, epoch = MAX

        // The new compaction group id is always generate on the right side
        // Hence, we return the first compaction group id as the result
        // split 1
        let mut cg_id_to_table_ids: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        let table_id_to_split = *table_ids.first().unwrap();
        let mut target_compaction_group_id = 0;
        let result_vec = self
            .split_compaction_group_impl(
                parent_group_id,
                table_ids,
                table_id_to_split,
                VirtualNode::ZERO,
                partition_vnode_count,
            )
            .await?;
        assert!(result_vec.len() <= 2);
        for (cg_id, table_ids) in result_vec {
            if table_ids.contains(&table_id_to_split) {
                target_compaction_group_id = cg_id;
            }
            cg_id_to_table_ids.insert(cg_id, table_ids);
        }
        check_table_ids_valid(&cg_id_to_table_ids);

        // split 2
        // See the example above and the split rule in `split_compaction_group_impl`.
        let table_id_to_split = *table_ids.last().unwrap();
        let result_vec = self
            .split_compaction_group_impl(
                target_compaction_group_id,
                table_ids,
                table_id_to_split,
                VirtualNode::MAX,
                partition_vnode_count,
            )
            .await?;
        assert!(result_vec.len() <= 2);
        for (cg_id, table_ids) in result_vec {
            if table_ids.contains(&table_id_to_split) {
                target_compaction_group_id = cg_id;
            }
            cg_id_to_table_ids.insert(cg_id, table_ids);
        }
        check_table_ids_valid(&cg_id_to_table_ids);

        self.metrics
            .move_state_table_count
            .with_label_values(&[&parent_group_id.to_string()])
            .inc();

        Ok((target_compaction_group_id, cg_id_to_table_ids))
    }
}
