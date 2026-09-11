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

//! Automatic group scheduling. Policy checks use scheduling snapshots; topology changes
//! remain in the transaction helpers, with normalization kept as its own operation.

use std::collections::HashSet;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use thiserror_ext::AsReport;

use super::CompactionGroupStatistic;
use crate::hummock::error::Result;
use crate::hummock::manager::HummockManager;
use crate::hummock::table_write_throughput_statistic::TableWriteThroughputStatisticManager;

mod normalize;
mod policy;
mod topology;

#[cfg(test)]
mod tests;

// Preserve the existing log target when moving implementation into private modules.
const TRACE_TARGET: &str = module_path!();

impl HummockManager {
    /// Split the compaction group if the group is too large or contains high throughput tables.
    pub async fn try_split_compaction_group(
        &self,
        table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
        group: CompactionGroupStatistic,
    ) {
        if group
            .compaction_group_config
            .compaction_config
            .disable_auto_group_scheduling
            .unwrap_or(false)
        {
            return;
        }
        // split high throughput table to dedicated compaction group
        let mut refresh_groups = false;
        for table_id in group.table_statistic.keys() {
            refresh_groups |= self
                .try_move_high_throughput_table_to_dedicated_cg(
                    table_write_throughput_statistic_manager,
                    *table_id,
                )
                .await;
        }

        let group_max_size = (group.compaction_group_config.max_estimated_group_size() as f64
            * self.env.opts.split_group_size_ratio) as u64;
        if !refresh_groups
            && (group.table_statistic.len() < 2 || group.group_size <= group_max_size)
        {
            return;
        }

        // Refresh only groups that may split. Hot-table attempts can move members even when a
        // later step fails; size-based plans also need current sizes and config before splitting.
        let table_ids = group.table_statistic.keys().copied().collect_vec();
        for current in self
            .calculate_compaction_group_statistic_for_tables(&table_ids)
            .await
        {
            if !current
                .compaction_group_config
                .compaction_config
                .disable_auto_group_scheduling
                .unwrap_or(false)
            {
                self.try_split_huge_compaction_group(current).await;
            }
        }
    }

    /// Try to move the high throughput table to a dedicated compaction group.
    /// Returns whether the caller needs to refresh membership after inspecting a hot table.
    pub async fn try_move_high_throughput_table_to_dedicated_cg(
        &self,
        table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
        table_id: TableId,
    ) -> bool {
        let mut table_throughput = table_write_throughput_statistic_manager
            .get_table_throughput_descending(
                table_id,
                self.env.opts.table_stat_throuput_window_seconds_for_split as i64,
            )
            .peekable();

        if table_throughput.peek().is_none() {
            return false;
        }

        let is_high_write_throughput = policy::is_table_high_write_throughput(
            table_throughput,
            self.env.opts.table_high_write_throughput_threshold,
            self.env
                .opts
                .table_stat_high_write_throughput_ratio_for_split,
        );

        // do not split a table to dedicated compaction group if it is not high write throughput
        if !is_high_write_throughput {
            return false;
        }

        let parent_group_id = self
            .on_current_version(|version| {
                let group_id = version
                    .state_table_info
                    .info()
                    .get(&table_id)?
                    .compaction_group_id;
                (version
                    .state_table_info
                    .compaction_group_member_table_ids(group_id)
                    .len()
                    > 1)
                .then_some(group_id)
            })
            .await;
        let Some(parent_group_id) = parent_group_id else {
            return true;
        };

        let ret = self
            .move_state_tables_to_dedicated_compaction_group(
                parent_group_id,
                &[table_id],
                Some(self.env.opts.partition_vnode_count),
            )
            .await;
        match ret {
            Ok(split_result) => {
                tracing::info!(
                    "split state table [{}] from group-{} success table_vnode_partition_count {:?} split result {:?}",
                    table_id,
                    parent_group_id,
                    self.env.opts.partition_vnode_count,
                    split_result
                );
            }
            Err(e) => {
                tracing::info!(
                    error = %e.as_report(),
                    "failed to split state table [{}] from group-{}",
                    table_id,
                    parent_group_id,
                )
            }
        }
        true
    }

    pub async fn try_split_huge_compaction_group(&self, group: CompactionGroupStatistic) {
        let Some(table_ids) =
            policy::split_huge_group_table_ids(&group, self.env.opts.split_group_size_ratio)
        else {
            return;
        };
        let ret = self
            .move_state_tables_to_dedicated_compaction_group(group.group_id, &table_ids, None)
            .await;
        match ret {
            Ok(split_result) => {
                tracing::info!("split_huge_compaction_group success {:?}", split_result);
                self.metrics
                    .split_compaction_group_count
                    .with_label_values(&[&group.group_id.to_string()])
                    .inc();
            }
            Err(e) => {
                tracing::error!(
                    error = %e.as_report(),
                    "failed to split_huge_compaction_group table {:?} from group-{}",
                    table_ids,
                    group.group_id
                );
            }
        }
    }

    pub async fn try_merge_compaction_group(
        &self,
        table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
        group: &CompactionGroupStatistic,
        next_group: &CompactionGroupStatistic,
        created_tables: &HashSet<TableId>,
    ) -> Result<CompactionGroupStatistic> {
        policy::validate_merge_snapshot(
            group,
            next_group,
            created_tables,
            table_write_throughput_statistic_manager,
            &self.env.opts,
        )?;
        {
            // Only candidates accepted by the snapshot reach the current-version checks.
            // Release this read guard before the merge transaction takes the write lock.
            let versioning = self
                .versioning
                .read_with_process_name("validate_group_merge")
                .await;
            policy::validate_merge_current_version(
                group,
                next_group,
                &self.env.opts,
                &versioning.current_version,
            )?;
        }

        let result = self
            .merge_compaction_group_impl(group.group_id, next_group.group_id, None)
            .await;

        match &result {
            Ok(survivor) => {
                tracing::info!(
                    "merge groups {} and {} into group-{}",
                    group.group_id,
                    next_group.group_id,
                    survivor.group_id,
                );

                self.metrics
                    .merge_compaction_group_count
                    .with_label_values(&[&survivor.group_id.to_string()])
                    .inc();
            }
            Err(e) => {
                tracing::info!(
                    error = %e.as_report(),
                    "failed to merge group-{} group-{}",
                    next_group.group_id,
                    group.group_id,
                );
            }
        }

        result
    }
}
