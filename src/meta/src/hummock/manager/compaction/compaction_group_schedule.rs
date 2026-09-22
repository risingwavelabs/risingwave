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

//! Automatic group scheduling. Policies inspect observations and current state;
//! topology changes and normalization retain their own transaction boundaries.

use std::collections::HashSet;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use thiserror_ext::AsReport;

use super::CompactionGroupStatistic;
use crate::hummock::error::Result;
use crate::hummock::manager::HummockManager;
use crate::hummock::table_write_throughput_statistic::TableWriteThroughputStatisticManager;

mod merge_policy;
mod normalize;
mod topology;

#[cfg(test)]
mod tests;

// Preserve the log target of transaction and normalization code moved below this module.
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
        let mut hot_tables = group
            .table_statistic
            .keys()
            .copied()
            .filter(|&table_id| {
                table_write_throughput_statistic_manager
                    .latest_table_throughput(table_id)
                    .is_some_and(|rate| rate > self.env.opts.table_high_write_throughput_threshold)
            })
            .peekable();
        let group_max_size = (group.compaction_group_config.max_estimated_group_size() as f64
            * self.env.opts.split_group_size_ratio) as u64;
        if hot_tables.peek().is_none()
            && (group.table_statistic.len() < 2 || group.group_size <= group_max_size)
        {
            return;
        }

        for table_id in hot_tables {
            self.try_move_high_throughput_table_to_dedicated_cg(table_id)
                .await;
        }

        // Plan size-based splits from current groups, not the snapshot used to select hot tables.
        // Refresh even after a failed move: its first split may already have committed.
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

    /// Try to isolate a table already selected as hot by the scheduler.
    async fn try_move_high_throughput_table_to_dedicated_cg(&self, table_id: TableId) {
        // An earlier hot-table split in this round may have moved this table to a new group.
        // Resolve its current parent instead of using the scheduler's original group snapshot.
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
            // The table was removed or is already in a single-table group, so no move is needed.
            return;
        };

        let ret = self
            .move_state_tables_to_dedicated_compaction_group_impl(
                parent_group_id,
                &[table_id],
                Some(self.env.opts.partition_vnode_count),
                true,
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
    }

    pub async fn try_split_huge_compaction_group(&self, group: CompactionGroupStatistic) {
        let group_max_size = (group.compaction_group_config.max_estimated_group_size() as f64
            * self.env.opts.split_group_size_ratio) as u64;
        let is_huge_hybrid_group =
            group.group_size > group_max_size && group.table_statistic.len() > 1; // avoid split single table group
        if is_huge_hybrid_group {
            let mut accumulated_size = 0;
            let mut table_ids = Vec::default();
            for (table_id, table_size) in &group.table_statistic {
                accumulated_size += table_size;
                table_ids.push(*table_id);
                // split if the accumulated size is greater than half of the group size
                // avoid split a small table to dedicated compaction group and trigger multiple merge
                let remaining_size = group.group_size.saturating_sub(accumulated_size);
                if accumulated_size > group_max_size / 2
                    && remaining_size > 0
                    && table_ids.len() < group.table_statistic.len()
                {
                    let ret = self
                        .move_state_tables_to_dedicated_compaction_group_impl(
                            group.group_id,
                            &table_ids,
                            None,
                            true,
                        )
                        .await;
                    match ret {
                        Ok(split_result) => {
                            tracing::info!(
                                "split_huge_compaction_group success {:?}",
                                split_result
                            );
                            self.metrics
                                .split_compaction_group_count
                                .with_label_values(&[&group.group_id.to_string()])
                                .inc();
                            return;
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e.as_report(),
                                "failed to split_huge_compaction_group table {:?} from group-{}",
                                table_ids,
                                group.group_id
                            );

                            return;
                        }
                    }
                }
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
        merge_policy::validate_group_merge(
            group,
            next_group,
            created_tables,
            table_write_throughput_statistic_manager,
            &self.env.opts,
        )?;

        let result = self
            .merge_compaction_group_impl(
                group.group_id,
                next_group.group_id,
                Some(created_tables),
                true,
            )
            .await;

        match &result {
            Ok(survivor) => {
                tracing::info!(
                    "merge groups {} and {} into group-{}",
                    group.group_id,
                    next_group.group_id,
                    survivor.group_id,
                );
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
