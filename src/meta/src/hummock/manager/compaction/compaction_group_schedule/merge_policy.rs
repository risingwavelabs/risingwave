// Copyright 2026 RisingWave Labs
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

//! Shared merge predicates. Locking and topology mutations belong to `HummockManager`.

use std::collections::HashSet;

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::version::HummockVersion;

use super::CompactionGroupStatistic;
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::compaction::GroupStateValidator;
use crate::hummock::model::CompactionGroup;
use crate::hummock::table_write_throughput_statistic::TableWriteThroughputStatisticManager;
use crate::manager::MetaOpts;

pub(super) fn check_is_low_write_throughput(
    table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
    mut table_ids: impl Iterator<Item = TableId>,
    opts: &MetaOpts,
) -> bool {
    let now = tokio::time::Instant::now();
    let threshold = opts
        .table_low_write_throughput_threshold
        .min(opts.table_high_write_throughput_threshold);
    table_ids.all(|table_id| {
        // The peak dominates the current rate used by split. Require both thresholds so
        // custom threshold orderings cannot make a table simultaneously hot and cold.
        table_write_throughput_statistic_manager
            .max_write_throughput(table_id, now)
            .is_some_and(|peak| peak <= threshold)
    })
}

pub(super) fn validate_group_merge(
    group: &CompactionGroupStatistic,
    next_group: &CompactionGroupStatistic,
    created_tables: &HashSet<TableId>,
    table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
    opts: &MetaOpts,
) -> Result<()> {
    if group.table_statistic.is_empty() || next_group.table_statistic.is_empty() {
        return Err(Error::CompactionGroup(format!(
            "group-{} or group-{} is empty",
            group.group_id, next_group.group_id
        )));
    }

    // BTreeMap keys already provide sorted endpoints; no allocation or sorting is needed.
    let mut left = &group.table_statistic;
    let mut right = &next_group.table_statistic;
    if left.first_key_value().unwrap().0 > right.first_key_value().unwrap().0 {
        std::mem::swap(&mut left, &mut right);
    }
    if left.last_key_value().unwrap().0 >= right.first_key_value().unwrap().0 {
        return Err(Error::CompactionGroup(format!(
            "group-{} and group-{} have overlapping table id ranges, not mergeable",
            group.group_id, next_group.group_id
        )));
    }

    validate_group_config(
        &group.compaction_group_config,
        &next_group.compaction_group_config,
        group.group_size.saturating_add(next_group.group_size),
        opts,
    )?;

    for candidate in [group, next_group] {
        if candidate
            .table_statistic
            .keys()
            .any(|table_id| !created_tables.contains(table_id))
        {
            return Err(Error::CompactionGroup(format!(
                "Cannot merge groups {} and {}: group {} contains creating tables",
                group.group_id, next_group.group_id, candidate.group_id
            )));
        }
        if !check_is_low_write_throughput(
            table_write_throughput_statistic_manager,
            candidate.table_statistic.keys().copied(),
            opts,
        ) {
            return Err(Error::CompactionGroup(format!(
                "Cannot merge groups {} and {}: group {} without sufficiently observed low throughput",
                group.group_id, next_group.group_id, candidate.group_id
            )));
        }
    }

    Ok(())
}

pub(super) fn validate_group_config(
    group: &CompactionGroup,
    next_group: &CompactionGroup,
    combined_size: u64,
    opts: &MetaOpts,
) -> Result<()> {
    // TODO: remove this check after refactor group id
    if (group.group_id == StaticCompactionGroupId::StateDefault
        && next_group.group_id == StaticCompactionGroupId::MaterializedView)
        || (group.group_id == StaticCompactionGroupId::MaterializedView
            && next_group.group_id == StaticCompactionGroupId::StateDefault)
    {
        return Err(Error::CompactionGroup(format!(
            "group-{} and group-{} are both StaticCompactionGroupId",
            group.group_id, next_group.group_id
        )));
    }

    if group
        .compaction_config
        .disable_auto_group_scheduling
        .unwrap_or(false)
        || next_group
            .compaction_config
            .disable_auto_group_scheduling
            .unwrap_or(false)
    {
        return Err(Error::CompactionGroup(format!(
            "group-{} or group-{} disable_auto_group_scheduling",
            group.group_id, next_group.group_id
        )));
    }

    // This per-table setting is reset after merge; every other config field must match.
    let mut left = group.compaction_config.as_ref().clone();
    let mut right = next_group.compaction_config.as_ref().clone();
    left.split_weight_by_vnode = 0;
    right.split_weight_by_vnode = 0;
    if left != right {
        let left_config = group.compaction_config.as_ref();
        let right_config = next_group.compaction_config.as_ref();

        tracing::warn!(
            group_id = %group.group_id,
            next_group_id = %next_group.group_id,
            left_config = ?left_config,
            right_config = ?right_config,
            "compaction config semantic mismatch detected while merging compaction groups"
        );

        return Err(Error::CompactionGroup(format!(
            "Cannot merge group {} and next_group {} with different compaction config (split_weight_by_vnode is excluded from comparison). left_config: {:?}, right_config: {:?}",
            group.group_id, next_group.group_id, left_config, right_config
        )));
    }

    let size_limit = (group.max_estimated_group_size() as f64 * opts.split_group_size_ratio) as u64;
    if combined_size > size_limit {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge huge groups {} and {}: combined_size {} size_limit {}",
            group.group_id, next_group.group_id, combined_size, size_limit
        )));
    }
    Ok(())
}

pub(super) fn validate_group_levels(
    group: &CompactionGroup,
    next_group: &CompactionGroup,
    opts: &MetaOpts,
    version: &HummockVersion,
) -> Result<()> {
    let group_levels = version.levels.get(&group.group_id).ok_or_else(|| {
        Error::CompactionGroup(format!(
            "cannot merge compaction group {} because it does not exist",
            group.group_id
        ))
    })?;
    let next_group_levels = version.levels.get(&next_group.group_id).ok_or_else(|| {
        Error::CompactionGroup(format!(
            "cannot merge compaction group {} because it does not exist",
            next_group.group_id
        ))
    })?;
    for (candidate, levels) in [(group, group_levels), (next_group, next_group_levels)] {
        let state = GroupStateValidator::group_state(levels, &candidate.compaction_config);
        if state.is_write_stop() || state.is_emergency() {
            return Err(Error::CompactionGroup(format!(
                "Cannot merge groups {} and {}: write limit on group {}",
                group.group_id, next_group.group_id, candidate.group_id
            )));
        }
    }

    // check whether the group is in the write stop state after merge
    let l0_sub_level_count_after_merge =
        group_levels.l0.sub_levels.len() + next_group_levels.l0.sub_levels.len();
    if GroupStateValidator::write_stop_sub_level_count(
        (l0_sub_level_count_after_merge as f64 * opts.compaction_group_merge_dimension_threshold)
            as usize,
        &group.compaction_config,
    ) {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge write limit group {} next group {}, will trigger write stop after merge",
            group.group_id, next_group.group_id
        )));
    }

    let l0_file_count_after_merge = group_levels
        .l0
        .sub_levels
        .iter()
        .chain(next_group_levels.l0.sub_levels.iter())
        .map(|level| level.table_infos.len())
        .sum::<usize>();
    if GroupStateValidator::write_stop_l0_file_count(
        (l0_file_count_after_merge as f64 * opts.compaction_group_merge_dimension_threshold)
            as usize,
        &group.compaction_config,
    ) {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge write limit next group {} group {}, will trigger write stop after merge",
            next_group.group_id, group.group_id
        )));
    }

    let l0_size_after_merge =
        group_levels.l0.total_file_size + next_group_levels.l0.total_file_size;

    if GroupStateValidator::write_stop_l0_size(
        (l0_size_after_merge as f64 * opts.compaction_group_merge_dimension_threshold) as u64,
        &group.compaction_config,
    ) {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge write limit next group {} group {}, will trigger write stop after merge",
            next_group.group_id, group.group_id
        )));
    }

    // check whether the group is in the emergency state after merge
    if GroupStateValidator::emergency_l0_file_count(
        (l0_file_count_after_merge as f64 * opts.compaction_group_merge_dimension_threshold)
            as usize,
        &group.compaction_config,
    ) {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge emergency group {} next group {}, will trigger emergency after merge",
            group.group_id, next_group.group_id
        )));
    }
    Ok(())
}
