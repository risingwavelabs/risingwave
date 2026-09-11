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

//! Synchronous scheduling checks. Snapshot policy runs before current-version safety checks;
//! callers own the locks. Throughput checks retain their clock reads and short-circuit order.

use std::collections::HashSet;

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::version::HummockVersion;

use super::super::GroupStateValidator;
use super::CompactionGroupStatistic;
use crate::hummock::error::{Error, Result};
use crate::hummock::table_write_throughput_statistic::{
    TableWriteThroughputStatistic, TableWriteThroughputStatisticManager,
};
use crate::manager::MetaOpts;

/// Check if two groups have compatible compaction configs for merging.
/// Ignores `split_weight_by_vnode` since it's per-table and will be reset after merge.
fn is_merge_compatible_by_semantics(
    group: &CompactionGroupStatistic,
    next_group: &CompactionGroupStatistic,
) -> bool {
    let (mut left, mut right) = (
        group
            .compaction_group_config
            .compaction_config
            .as_ref()
            .clone(),
        next_group
            .compaction_group_config
            .compaction_config
            .as_ref()
            .clone(),
    );
    left.split_weight_by_vnode = 0;
    right.split_weight_by_vnode = 0;
    left == right
}

/// Check if the table is high write throughput with the given threshold and ratio.
pub(super) fn is_table_high_write_throughput(
    table_throughput: impl Iterator<Item = &TableWriteThroughputStatistic>,
    threshold: u64,
    high_write_throughput_ratio: f64,
) -> bool {
    let mut sample_size = 0;
    let mut high_write_throughput_count = 0;
    for statistic in table_throughput {
        sample_size += 1;
        if statistic.throughput > threshold {
            high_write_throughput_count += 1;
        }
    }

    high_write_throughput_count as f64 > sample_size as f64 * high_write_throughput_ratio
}

fn is_table_low_write_throughput(
    table_throughput: impl Iterator<Item = &TableWriteThroughputStatistic>,
    threshold: u64,
    low_write_throughput_ratio: f64,
) -> bool {
    let mut sample_size = 0;
    let mut low_write_throughput_count = 0;
    for statistic in table_throughput {
        sample_size += 1;
        if statistic.throughput <= threshold {
            low_write_throughput_count += 1;
        }
    }

    low_write_throughput_count as f64 > sample_size as f64 * low_write_throughput_ratio
}

fn check_is_low_write_throughput_compaction_group(
    table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
    group: &CompactionGroupStatistic,
    opts: &MetaOpts,
) -> bool {
    let now = chrono::Utc::now().timestamp();
    let merge_window = opts.table_stat_throuput_window_seconds_for_merge as i64;
    group.table_statistic.keys().all(|&table_id| {
        // Reuse the split predicate: long-window coldness cannot override a current hotspot.
        !is_table_high_write_throughput(
            table_write_throughput_statistic_manager.get_table_throughput_descending_at(
                table_id,
                opts.table_stat_throuput_window_seconds_for_split as i64,
                now,
            ),
            opts.table_high_write_throughput_threshold,
            opts.table_stat_high_write_throughput_ratio_for_split,
        ) && table_write_throughput_statistic_manager.observed_window_secs(
            table_id,
            merge_window,
            now,
        ) as f64
            > merge_window as f64 * opts.table_stat_low_write_throughput_ratio_for_merge
            && is_table_low_write_throughput(
                table_write_throughput_statistic_manager.get_table_throughput_descending_at(
                    table_id,
                    merge_window,
                    now,
                ),
                opts.table_low_write_throughput_threshold,
                opts.table_stat_low_write_throughput_ratio_for_merge,
            )
    })
}

fn check_is_creating_compaction_group(
    group: &CompactionGroupStatistic,
    created_tables: &HashSet<TableId>,
) -> bool {
    group
        .table_statistic
        .keys()
        .any(|table_id| !created_tables.contains(table_id))
}

/// Reject candidates using the scheduling snapshot before acquiring a version lock.
pub(super) fn validate_merge_snapshot(
    group: &CompactionGroupStatistic,
    next_group: &CompactionGroupStatistic,
    created_tables: &HashSet<TableId>,
    table_write_throughput_statistic_manager: &TableWriteThroughputStatisticManager,
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

    if group.table_statistic.is_empty() || next_group.table_statistic.is_empty() {
        return Err(Error::CompactionGroup(format!(
            "group-{} or group-{} is empty",
            group.group_id, next_group.group_id
        )));
    }

    // The statistics use BTreeMap, so the range check needs no key copies or sorting.
    // Keep this check before configuration and throughput rejection.
    {
        let mut range_1 = (
            group.table_statistic.first_key_value().unwrap().0,
            group.table_statistic.last_key_value().unwrap().0,
        );
        let mut range_2 = (
            next_group.table_statistic.first_key_value().unwrap().0,
            next_group.table_statistic.last_key_value().unwrap().0,
        );
        if range_1.0 > range_2.0 {
            std::mem::swap(&mut range_1, &mut range_2);
        }
        if range_1.1 >= range_2.0 {
            return Err(Error::CompactionGroup(format!(
                "group-{} and group-{} have overlapping table id ranges, not mergeable",
                group.group_id, next_group.group_id
            )));
        }
    }

    if group
        .compaction_group_config
        .compaction_config
        .disable_auto_group_scheduling
        .unwrap_or(false)
        || next_group
            .compaction_group_config
            .compaction_config
            .disable_auto_group_scheduling
            .unwrap_or(false)
    {
        return Err(Error::CompactionGroup(format!(
            "group-{} or group-{} disable_auto_group_scheduling",
            group.group_id, next_group.group_id
        )));
    }

    // Keep merge compatibility as a feature, but ignore split_weight_by_vnode, because it is
    // only used for per-table split behavior and will be reset after merge.
    if !is_merge_compatible_by_semantics(group, next_group) {
        let left_config = group.compaction_group_config.compaction_config.as_ref();
        let right_config = next_group
            .compaction_group_config
            .compaction_config
            .as_ref();

        tracing::warn!(target: super::TRACE_TARGET,
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

    // do not merge the compaction group which is creating
    if check_is_creating_compaction_group(group, created_tables) {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge creating group {} next_group {}",
            group.group_id, next_group.group_id
        )));
    }

    // do not merge high throughput group
    if !check_is_low_write_throughput_compaction_group(
        table_write_throughput_statistic_manager,
        group,
        opts,
    ) {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge group {} next_group {} without sufficiently observed low throughput",
            group.group_id, next_group.group_id
        )));
    }

    let size_limit = (group.compaction_group_config.max_estimated_group_size() as f64
        * opts.split_group_size_ratio) as u64;

    if (group.group_size + next_group.group_size) > size_limit {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge huge group {} group_size {} next_group {} next_group_size {} size_limit {}",
            group.group_id,
            group.group_size,
            next_group.group_id,
            next_group.group_size,
            size_limit
        )));
    }

    if check_is_creating_compaction_group(next_group, created_tables) {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge creating group {} next group {}",
            group.group_id, next_group.group_id
        )));
    }

    if !check_is_low_write_throughput_compaction_group(
        table_write_throughput_statistic_manager,
        next_group,
        opts,
    ) {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge group {} next group {} without sufficiently observed low throughput",
            group.group_id, next_group.group_id
        )));
    }

    Ok(())
}

/// Check current L0 safety while the caller holds the version read lock.
pub(super) fn validate_merge_current_version(
    group: &CompactionGroupStatistic,
    next_group: &CompactionGroupStatistic,
    opts: &MetaOpts,
    version: &HummockVersion,
) -> Result<()> {
    let levels = &version.levels;
    if !levels.contains_key(&group.group_id) {
        return Err(Error::CompactionGroup(format!(
            "cannot merge compaction group {} because it does not exist",
            group.group_id
        )));
    }

    if !levels.contains_key(&next_group.group_id) {
        return Err(Error::CompactionGroup(format!(
            "cannot merge next compaction group {} because it does not exist",
            next_group.group_id
        )));
    }

    let group_levels = version.get_compaction_group_levels(group.group_id);

    let next_group_levels = version.get_compaction_group_levels(next_group.group_id);

    let group_config = group.compaction_group_config.compaction_config.as_ref();
    let next_group_config = next_group
        .compaction_group_config
        .compaction_config
        .as_ref();
    let group_state = GroupStateValidator::group_state(group_levels, group_config);

    if group_state.is_write_stop() || group_state.is_emergency() {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge write limit group {} next group {}",
            group.group_id, next_group.group_id
        )));
    }

    let next_group_state = GroupStateValidator::group_state(next_group_levels, next_group_config);

    if next_group_state.is_write_stop() || next_group_state.is_emergency() {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge write limit next group {} group {}",
            next_group.group_id, group.group_id
        )));
    }

    // check whether the group is in the write stop state after merge
    let l0_sub_level_count_after_merge =
        group_levels.l0.sub_levels.len() + next_group_levels.l0.sub_levels.len();
    if GroupStateValidator::write_stop_sub_level_count(
        (l0_sub_level_count_after_merge as f64 * opts.compaction_group_merge_dimension_threshold)
            as usize,
        group_config,
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
        group_config,
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
        group_config,
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
        group_config,
    ) {
        return Err(Error::CompactionGroup(format!(
            "Cannot merge emergency group {} next group {}, will trigger emergency after merge",
            group.group_id, next_group.group_id
        )));
    }
    Ok(())
}

/// Select the first size-based split boundary, preserving table order and the size checks.
pub(super) fn split_huge_group_table_ids(
    group: &CompactionGroupStatistic,
    split_group_size_ratio: f64,
) -> Option<Vec<TableId>> {
    let group_max_size = (group.compaction_group_config.max_estimated_group_size() as f64
        * split_group_size_ratio) as u64;
    if group.group_size <= group_max_size || group.table_statistic.len() <= 1 {
        return None;
    }
    let mut accumulated_size = 0;
    let mut table_ids = Vec::default();
    for (table_id, table_size) in &group.table_statistic {
        accumulated_size += table_size;
        table_ids.push(*table_id);
        // split if the accumulated size is greater than half of the group size
        // avoid split a small table to dedicated compaction group and trigger multiple merge
        assert!(table_ids.is_sorted());
        let remaining_size = group.group_size.saturating_sub(accumulated_size);
        if accumulated_size > group_max_size / 2
            && remaining_size > 0
            && table_ids.len() < group.table_statistic.len()
        {
            return Some(table_ids);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::super::tests::group;

    #[test]
    fn test_merge_snapshot_table_ranges() {
        use std::collections::HashSet;

        use super::{Error, MetaOpts, TableWriteThroughputStatisticManager};

        let opts = MetaOpts::test(false);
        let stats = TableWriteThroughputStatisticManager::new(240);
        let cases: &[(&[u32], &[u32], bool)] = &[
            (&[1], &[2], false),
            (&[2], &[1], false),
            (&[1, 2], &[3, 4], false),
            (&[3, 4], &[1, 2], false),
            (&[1, 3], &[2, 4], true),
            (&[2, 4], &[1, 3], true),
            (&[1, 2], &[2, 3], true),
            (&[2], &[2], true),
        ];
        for &(left, right, overlap) in cases {
            let left = group(10.into(), left, true);
            let right = group(11.into(), right, true);
            let Err(Error::CompactionGroup(reason)) =
                super::validate_merge_snapshot(&left, &right, &HashSet::new(), &stats, &opts)
            else {
                panic!("overlap or disabled scheduling must reject the pair");
            };
            assert_eq!(
                reason,
                if overlap {
                    "group-10 and group-11 have overlapping table id ranges, not mergeable"
                } else {
                    "group-10 or group-11 disable_auto_group_scheduling"
                },
            );
        }
    }

    #[test]
    fn test_huge_split_selects_first_valid_prefix() {
        use risingwave_common::catalog::TableId;
        use risingwave_pb::hummock::CompactionConfig;

        use crate::hummock::model::CompactionGroup;

        // Preserve strict thresholds, nonempty remainder, and the use of the supplied total
        // even when concurrent statistics made it differ from the sum of member sizes.
        let cases: &[(&[u64], u64, f64, Option<usize>)] = &[
            (&[], 20, 1.0, None),
            (&[20], 20, 1.0, None),
            (&[6, 4], 10, 1.0, None),
            (&[6, 5], 11, 1.0, Some(1)),
            (&[5, 6], 11, 1.0, None),
            (&[5, 1, 5], 11, 1.0, Some(2)),
            (&[0, 6, 5], 11, 1.0, Some(2)),
            (&[11, 0], 11, 1.0, None),
            (&[6, 0], 12, 1.0, Some(1)),
            (&[0, 0], 12, 1.0, None),
            (&[6, 5], 11, 2.0, None),
            (&[3, 4], 7, 0.5, Some(1)),
        ];
        for &(sizes, total, ratio, prefix_len) in cases {
            let mut group = group(10.into(), &[], false);
            group.group_size = total;
            group.table_statistic = sizes
                .iter()
                .enumerate()
                .map(|(i, &size)| (TableId::new(100 + i as u32), size))
                .collect();
            group.compaction_group_config = CompactionGroup::new(
                group.group_id,
                CompactionConfig {
                    max_level: 1,
                    max_bytes_for_level_base: 10,
                    ..Default::default()
                },
            );
            let expected = prefix_len.map(|len| {
                (0..len)
                    .map(|i| TableId::new(100 + i as u32))
                    .collect::<Vec<_>>()
            });
            assert_eq!(
                super::split_huge_group_table_ids(&group, ratio),
                expected,
                "sizes={sizes:?}, total={total}, ratio={ratio}",
            );
        }
    }

    #[test]
    fn test_merge_rejects_recent_hot_and_unobserved_tables() {
        use std::sync::Arc;

        use super::super::TableWriteThroughputStatisticManager;
        use crate::manager::MetaOpts;
        let opts = Arc::new(MetaOpts::test(false));
        let group = group(10.into(), &[100], false);
        let now = chrono::Utc::now().timestamp();
        let mut stats = TableWriteThroughputStatisticManager::new(240);
        for age in (0..240).rev() {
            let throughput = if age < 60 {
                opts.table_high_write_throughput_threshold + 1
            } else {
                0
            };
            stats.add_table_throughput_with_ts(100.into(), throughput, now - age, 1);
        }
        assert!(super::is_table_high_write_throughput(
            stats.get_table_throughput_descending(100.into(), 60),
            opts.table_high_write_throughput_threshold,
            opts.table_stat_high_write_throughput_ratio_for_split
        ));
        assert!(
            !super::check_is_low_write_throughput_compaction_group(&stats, &group, &opts),
            "a group that would split immediately must not merge"
        );
    }

    #[test]
    fn test_merge_requires_observed_cold_window() {
        use std::sync::Arc;

        use super::super::TableWriteThroughputStatisticManager;
        use crate::manager::MetaOpts;
        let opts = Arc::new(MetaOpts::test(false));
        let group = group(10.into(), &[100, 101], false);
        let now = chrono::Utc::now().timestamp();
        let mut stats = TableWriteThroughputStatisticManager::new(240);
        for age in (0..240).rev() {
            stats.add_table_throughput_with_ts(100.into(), 0, now - age, 1);
        }
        assert!(
            !super::check_is_low_write_throughput_compaction_group(&stats, &group, &opts),
            "every member needs observations"
        );
        stats.add_table_throughput_with_ts(101.into(), 0, now, 1);
        assert!(
            !super::check_is_low_write_throughput_compaction_group(&stats, &group, &opts),
            "one commit after a pause is insufficient"
        );
        let mut stats = TableWriteThroughputStatisticManager::new(240);
        for age in (0..240).rev() {
            for table in [100, 101] {
                stats.add_table_throughput_with_ts(table.into(), 0, now - age, 1);
            }
        }
        assert!(super::check_is_low_write_throughput_compaction_group(
            &stats, &group, &opts
        ));
    }
}
