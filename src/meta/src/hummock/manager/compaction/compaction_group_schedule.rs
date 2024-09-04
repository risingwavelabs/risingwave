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
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::compact_task::ReportTask;
use risingwave_hummock_sdk::compaction_group::group_split::split_table_ids;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    get_compaction_group_ids, TableGroupInfo,
};
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use risingwave_hummock_sdk::version::{GroupDelta, GroupDeltas};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::compact_task::{self, TaskStatus};
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

pub const VNODE_SPLIT_TO_RIGHT: VirtualNode = VirtualNode::ZERO;
pub const VNODE_SPLIT_TO_LEFT: VirtualNode = VirtualNode::MAX;

pub(super) fn is_vnode_split_to_right(vnode: VirtualNode) -> bool {
    vnode == VNODE_SPLIT_TO_RIGHT
}

pub(super) fn is_vnode_split_to_left(vnode: VirtualNode) -> bool {
    vnode == VNODE_SPLIT_TO_LEFT
}

// By default, the split key is constructed with vnode = 0 and epoch = 0, so that we can split table_id to the right group
pub(crate) fn build_split_key(
    mut table_id: StateTableId,
    mut vnode: VirtualNode,
    table_ids: &Vec<u32>,
) -> Bytes {
    if is_vnode_split_to_left(vnode) {
        // Modify `table_id` to `next_table_id` to satisfy the `split_to_right`` rule, so that the `table_id`` originally passed in will be split to left.
        table_id = table_ids[table_ids.partition_point(|x| *x <= table_id)]; // use next table_id
        vnode = VNODE_SPLIT_TO_RIGHT;
    }

    Bytes::from(FullKey::new(TableId::from(table_id), TableKey(vnode.to_be_bytes()), 0).encode())
}

pub fn build_split_key_with_table_id(table_id: StateTableId) -> Bytes {
    Bytes::from(
        FullKey::new(
            TableId::from(table_id),
            TableKey(VNODE_SPLIT_TO_RIGHT.to_be_bytes()),
            0,
        )
        .encode(),
    )
}

impl HummockManager {
    /// Splits a compaction group into two. The new one will contain `table_ids`.
    /// Returns the newly created compaction group id.
    pub async fn split_compaction_group(
        &self,
        parent_group_id: CompactionGroupId,
        table_ids: &[StateTableId],
    ) -> Result<CompactionGroupId> {
        if !table_ids.is_sorted() {
            return Err(Error::CompactionGroup(
                "table_ids must be sorted".to_string(),
            ));
        }

        let result = self
            .move_state_tables_to_dedicated_compaction_group(parent_group_id, table_ids)
            .await?;

        Ok(result)
    }

    async fn split_compaction_group_impl(
        &self,
        parent_group_id: CompactionGroupId,
        table_id: StateTableId,
        vnode: VirtualNode,
    ) -> Result<CompactionGroupId> {
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

        if !member_table_ids.contains(&TableId::new(table_id)) {
            return Err(Error::CompactionGroup(format!(
                "table {} doesn't in group {}",
                table_id, parent_group_id
            )));
        }

        let table_ids = member_table_ids
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec();

        // not need to split
        if table_id == *table_ids.first().unwrap() && is_vnode_split_to_right(vnode) {
            return Ok(parent_group_id);
        }

        if table_id == *table_ids.last().unwrap() && is_vnode_split_to_left(vnode) {
            return Ok(parent_group_id);
        }

        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            self.env.notification_manager(),
            &self.metrics,
        );
        let mut new_version_delta = version.new_delta();

        let split_key = build_split_key(table_id, vnode, &table_ids);

        let (_, table_ids_right) = split_table_ids(&table_ids, split_key.clone());

        let split_sst_count = new_version_delta
            .latest_version()
            .count_new_ssts_in_group_split(parent_group_id, split_key.clone());

        tracing::info!(
            "LI)K parent_group_id: {}, split_sst_count: {}",
            parent_group_id,
            split_sst_count
        );

        let new_sst_start_id = next_sstable_object_id(&self.env, split_sst_count).await?;
        let (new_group, target_compaction_group_id) = {
            {
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
                            version: CompatibilityVersion::NoMemberTableIds as i32,
                            split_key: Some(split_key.into()),
                        })],
                    },
                );
                ((new_compaction_group_id, config), new_compaction_group_id)
            }
        };

        let (new_compaction_group_id, config) = new_group;
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
        {
            let mut compaction_group_manager = self.compaction_group_manager.write().await;
            let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();
            compaction_groups_txn
                .create_compaction_groups(new_compaction_group_id, Arc::new(config));

            new_version_delta.pre_apply();
            commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;
        }
        // Instead of handling DeltaType::GroupConstruct for time travel, simply enforce a version snapshot.
        versioning.mark_next_time_travel_version_snapshot();
        let mut canceled_tasks = vec![];
        for task_assignment in compaction_guard.compact_task_assignment.values() {
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
        }

        drop(versioning_guard);
        drop(compaction_guard);
        self.report_compact_tasks(canceled_tasks).await?;

        // Don't trigger compactions if we enable deterministic compaction
        if !self.env.opts.compaction_deterministic_test {
            // commit_epoch may contains SSTs from any compaction group
            self.try_send_compaction_request(parent_group_id, compact_task::TaskType::SpaceReclaim);
            self.try_send_compaction_request(
                target_compaction_group_id,
                compact_task::TaskType::SpaceReclaim,
            );
        }

        self.metrics
            .move_state_table_count
            .with_label_values(&[&parent_group_id.to_string()])
            .inc();

        Ok(target_compaction_group_id)
    }

    pub async fn move_state_tables_to_dedicated_compaction_group(
        &self,
        parent_group_id: CompactionGroupId,
        table_ids: &[StateTableId],
    ) -> Result<CompactionGroupId> {
        if table_ids.is_empty() {
            return Ok(parent_group_id);
        }

        if !table_ids.is_sorted() {
            return Err(Error::CompactionGroup(
                "table_ids must be sorted".to_string(),
            ));
        }

        // move [3,4,5,6]
        // [1,2,3,4,5,6,7,8,9,10] -> [1,2] [3,4,5,6] [7,8,9,10]
        // split key
        // 1. table_id = 3, vnode = 0, epoch = 0
        // 2. table_id = 7, vnode = 0, epoch = 0
        let table_ids = table_ids.iter().cloned().unique().collect_vec();

        // The new compaction group id is always generate on the right side
        // Hence, we return the first compaction group id as the result

        // split 1
        let target_compaction_group_id = self
            .split_compaction_group_impl(
                parent_group_id,
                *table_ids.first().unwrap(),
                VNODE_SPLIT_TO_RIGHT,
            )
            .await?;

        // split 2
        self.split_compaction_group_impl(
            target_compaction_group_id,
            *table_ids.last().unwrap(),
            VNODE_SPLIT_TO_LEFT,
        )
        .await?;

        Ok(target_compaction_group_id)
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
        let member_table_ids_1 = state_table_info
            .compaction_group_member_table_ids(group_1)
            .iter()
            .cloned()
            .collect_vec();

        let member_table_ids_2 = state_table_info
            .compaction_group_member_table_ids(group_2)
            .iter()
            .cloned()
            .collect_vec();

        let mut combine_1 = member_table_ids_1.clone();
        combine_1.extend_from_slice(&member_table_ids_2);

        let mut combine_2 = member_table_ids_2;
        combine_2.extend_from_slice(&member_table_ids_1);

        if !combine_1.is_sorted() && !combine_2.is_sorted() {
            return Err(Error::CompactionGroup(format!(
                "invalid merge group_1 {} group_2 {}",
                group_1, group_2
            )));
        }

        let mut left_group_id = group_1;
        let mut right_group_id = group_2;
        let combine_member_table_ids = if combine_1.is_sorted() {
            combine_1
        } else {
            std::mem::swap(&mut left_group_id, &mut right_group_id);
            combine_2
        };

        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            self.env.notification_manager(),
            &self.metrics,
        );
        let mut new_version_delta = version.new_delta();

        let target_compaction_group_id = {
            let mut config = self
                .compaction_group_manager
                .read()
                .await
                .try_get_compaction_group_config(group_1)
                .unwrap()
                .compaction_config()
                .deref()
                .clone();

            {
                // update config
                config.split_weight_by_vnode = 0;
            }

            // merge right_group_id to left_group_id and remove right_group_id
            new_version_delta.group_deltas.insert(
                left_group_id,
                GroupDeltas {
                    group_deltas: vec![GroupDelta::GroupMerge(PbGroupMerge {
                        group_config: Some(config.clone()),
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
            for table_id in combine_member_table_ids {
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
                            safe_epoch: info.safe_epoch,
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

            // purge right_group_id
            compaction_groups_txn.purge(HashSet::from_iter(get_compaction_group_ids(
                version.latest_version(),
            )));
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

        Ok(())
    }

    pub async fn try_split_compaction_group(
        &self,
        table_write_throughput: &HashMap<u32, VecDeque<u64>>,
        checkpoint_secs: u64,
        group: &TableGroupInfo,
    ) {
        // split high throughput table to dedicated compaction group
        for (table_id, table_size) in &group.table_statistic {
            self.try_move_table_to_dedicated_cg(
                table_write_throughput,
                table_id,
                table_size,
                checkpoint_secs,
                group.group_id,
            )
            .await;
        }

        // split the huge group to multiple groups
        self.try_split_huge_compaction_group(group).await;
    }

    pub async fn try_move_table_to_dedicated_cg(
        &self,
        table_write_throughput: &HashMap<u32, VecDeque<u64>>,
        table_id: &u32,
        table_size: &u64,
        checkpoint_secs: u64,
        parent_group_id: u64,
    ) {
        let partition_vnode_count = self.env.opts.partition_vnode_count;
        let is_high_write_throughput = is_table_high_write_throughput(
            table_write_throughput,
            *table_id,
            checkpoint_secs,
            self.env.opts.table_info_statistic_history_times / 4,
            self.env.opts.table_write_throughput_threshold,
        );

        let state_table_size = *table_size;
        // do not split a small table to dedicated compaction group
        // do not split a table to dedicated compaction group if it is not high write throughput
        if state_table_size < self.env.opts.min_table_split_size || !is_high_write_throughput {
            return;
        }

        let ret = self
            .move_state_tables_to_dedicated_compaction_group(parent_group_id, &[*table_id])
            .await;
        match ret {
            Ok(new_group_id) => {
                tracing::info!("move state table [{}] from group-{} to group-{} success table_vnode_partition_count {:?}", table_id, parent_group_id, new_group_id, partition_vnode_count);

                // update compaction config for target_compaction_group_id
                let mut compaction_group_manager = self.compaction_group_manager.write().await;
                let mut compaction_groups_txn =
                    compaction_group_manager.start_compaction_groups_txn();
                if let Err(err) = compaction_groups_txn.update_compaction_config(
                    &[new_group_id],
                    &[MutableConfig::SplitWeightByVnode(partition_vnode_count)],
                ) {
                    tracing::error!(
                        error = %err.as_report(),
                        "failed to update compaction config for group-{}",
                        new_group_id
                    );
                }
                if let Err(err) = commit_multi_var!(self.meta_store_ref(), compaction_groups_txn) {
                    tracing::error!(
                        error = %err.as_report(),
                        "failed to update compaction config for group-{}",
                        new_group_id
                    );
                }

                self.metrics
                    .split_compaction_group_count
                    .with_label_values(&[&parent_group_id.to_string()])
                    .inc();
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

    pub async fn try_split_huge_compaction_group(&self, group: &TableGroupInfo) {
        // split the huge group to multiple groups
        let compaction_group_config = {
            let compaction_group_manager = self.compaction_group_manager.read().await;
            compaction_group_manager
                .try_get_compaction_group_config(group.group_id)
                .unwrap()
        };

        let is_huge_hybrid_group = group.group_size
            > (compaction_group_config.max_estimated_group_size() * 8 / 10)
            && group.table_statistic.len() > 1;
        if is_huge_hybrid_group {
            let mut accumulated_size = 0;
            let mut table_ids = Vec::default();
            assert!(group.table_statistic.keys().is_sorted());

            for (table_id, table_size) in &group.table_statistic {
                accumulated_size += table_size;
                table_ids.push(*table_id);
                if accumulated_size * 2 > group.group_size {
                    let ret = self
                        .move_state_tables_to_dedicated_compaction_group(group.group_id, &table_ids)
                        .await;
                    match ret {
                        Ok(new_group_id) => {
                            tracing::info!(
                                "move state table {:?} from group-{} to group-{} success",
                                table_ids,
                                group.group_id,
                                new_group_id
                            );

                            self.metrics
                                .split_compaction_group_count
                                .with_label_values(&[&group.group_id.to_string()])
                                .inc();
                            return;
                        }
                        Err(e) => {
                            tracing::info!(
                                error = %e.as_report(),
                                "failed to move state table {:?} from group-{}",
                                table_ids,
                                group.group_id
                            );
                        }
                    }
                }
            }
        }
    }

    pub async fn try_merge_compaction_group(
        &self,
        table_write_throughput: &HashMap<u32, VecDeque<u64>>,
        group: &TableGroupInfo,
        next_group: &TableGroupInfo,
        checkpoint_secs: u64,
        created_tables: &HashSet<u32>,
    ) -> Result<()> {
        if group.table_statistic.is_empty() || next_group.table_statistic.is_empty() {
            return Err(Error::CompactionGroup(format!(
                "group-{} or group-{} is empty",
                group.group_id, next_group.group_id
            )));
        }

        fn check_is_creating_compaction_group(
            group: &TableGroupInfo,
            created_tables: &HashSet<u32>,
        ) -> bool {
            let table_id = group.table_statistic.keys().next().unwrap();
            !created_tables.contains(table_id)
        }

        fn check_is_low_write_throughput_compaction_group(
            table_write_throughput: &HashMap<u32, VecDeque<u64>>,
            checkpoint_secs: u64,
            window_size: usize,
            threshold: u64,
            group: &TableGroupInfo,
        ) -> bool {
            group.table_statistic.keys().all(|table_id| {
                is_table_low_write_throughput(
                    table_write_throughput,
                    *table_id,
                    checkpoint_secs,
                    window_size,
                    threshold,
                )
            })
        }

        // do not merge the compaction group which is creating
        if check_is_creating_compaction_group(group, created_tables) {
            tracing::info!("Not Merge creating group {}", group.group_id);
            return Err(Error::CompactionGroup(format!(
                "group-{} is creating",
                group.group_id
            )));
        }

        // do not merge high throughput group
        let window_size =
            self.env.opts.table_info_statistic_history_times / (checkpoint_secs as usize);

        // merge the group which is low write throughput
        if !check_is_low_write_throughput_compaction_group(
            table_write_throughput,
            checkpoint_secs,
            window_size,
            self.env.opts.table_write_throughput_threshold,
            group,
        ) {
            tracing::info!("Not Merge high throughput group {}", group.group_id);
            return Err(Error::CompactionGroup(format!(
                "group-{} is high throughput",
                group.group_id
            )));
        }

        async fn check_is_huge_group(
            hummock_manager: &HummockManager,
            group: &TableGroupInfo,
        ) -> bool {
            let group_config = hummock_manager
                .compaction_group_manager
                .read()
                .await
                .try_get_compaction_group_config(group.group_id)
                .unwrap();

            group.group_size > (group_config.max_estimated_group_size() * 3 / 10)
        }

        if check_is_huge_group(self, group).await {
            tracing::info!(
                "Not Merge huge group {} group_size {}",
                group.group_id,
                group.group_size
            );
            return Err(Error::CompactionGroup(format!(
                "group-{} is huge group_size {}",
                group.group_id, group.group_size
            )));
        }

        if check_is_creating_compaction_group(next_group, created_tables) {
            tracing::info!("Not Merge creating group {}", next_group.group_id);
            return Err(Error::CompactionGroup(format!(
                "right group-{} is creating",
                next_group.group_id
            )));
        }

        if !check_is_low_write_throughput_compaction_group(
            table_write_throughput,
            checkpoint_secs,
            window_size,
            self.env.opts.table_write_throughput_threshold,
            next_group,
        ) {
            tracing::info!("Not Merge high throughput group {}", next_group.group_id);
            return Err(Error::CompactionGroup(format!(
                "right group-{} is high throughput",
                next_group.group_id
            )));
        }

        if check_is_huge_group(self, next_group).await {
            tracing::info!("Not Merge huge group {}", next_group.group_id);
            return Err(Error::CompactionGroup(format!(
                "group-{} is huge",
                next_group.group_id
            )));
        }

        match self
            .merge_compaction_group(group.group_id, next_group.group_id)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    "merge group-{} to group-{}",
                    next_group.group_id,
                    group.group_id,
                );

                self.metrics
                    .merge_compaction_group_count
                    .with_label_values(&[&group.group_id.to_string()])
                    .inc();
            }
            Err(e) => {
                tracing::info!(
                    error = %e.as_report(),
                    "failed to merge group-{}  group-{}",
                    next_group.group_id,
                    group.group_id,
                )
            }
        }

        Ok(())
    }
}

pub fn is_table_high_write_throughput(
    table_write_throughput: &HashMap<u32, VecDeque<u64>>,
    table_id: StateTableId,
    checkpoint_secs: u64,
    window_size: usize,
    threshold: u64,
) -> bool {
    if let Some(history) = table_write_throughput.get(&table_id) {
        if history.len() >= window_size {
            // Determine if 1/2 of the values in the interval exceed the threshold.
            let mut high_write_throughput_count = 0;
            for throughput in history.iter().take(history.len() - window_size) {
                if *throughput / checkpoint_secs > threshold {
                    high_write_throughput_count += 1;
                }
            }

            return high_write_throughput_count * 2 > window_size;
        }
    }

    false
}

pub fn is_table_low_write_throughput(
    table_write_throughput: &HashMap<u32, VecDeque<u64>>,
    table_id: StateTableId,
    checkpoint_secs: u64,
    window_size: usize,
    threshold: u64,
) -> bool {
    if let Some(history) = table_write_throughput.get(&table_id) {
        // Determine if 2/3 of the values in the interval below the threshold.
        let mut low_write_throughput_count = 0;
        for throughput in history.iter().take(history.len() - window_size) {
            if *throughput / checkpoint_secs < threshold {
                low_write_throughput_count += 1;
            }
        }

        return low_write_throughput_count * 3 > window_size;
    }

    false
}
