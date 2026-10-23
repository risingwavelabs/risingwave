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

use std::collections::BTreeMap;

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactionConfig;

use super::CompactionGroupStatistic;
use crate::hummock::model::CompactionGroup;
use crate::hummock::test_utils::advance_time;

pub(super) fn group(
    group_id: CompactionGroupId,
    table_ids: &[u32],
    disable_auto_group_scheduling: bool,
) -> CompactionGroupStatistic {
    let config = CompactionConfig {
        disable_auto_group_scheduling: Some(disable_auto_group_scheduling),
        ..Default::default()
    };
    CompactionGroupStatistic {
        group_id,
        group_size: 0,
        table_statistic: table_ids
            .iter()
            .copied()
            .map(|table_id| (table_id.into(), 0_u64))
            .collect::<BTreeMap<_, _>>(),
        compaction_group_config: CompactionGroup::new(group_id, config),
    }
}

#[tokio::test(start_paused = true)]
async fn test_merge_rejects_recent_hot_and_unobserved_tables() {
    use super::{TableWriteThroughputStatisticManager, merge_policy};
    use crate::manager::MetaOpts;
    let mut opts = MetaOpts::test(false);
    let table_ids = [TableId::new(100)];
    let now = tokio::time::Instant::now();
    let mut stats = TableWriteThroughputStatisticManager::new(240);
    for age in (0..=240).rev() {
        let throughput = if age < 60 {
            opts.table_high_write_throughput_threshold + 1
        } else {
            0
        };
        stats.record_commit(
            100.into(),
            throughput,
            now - std::time::Duration::from_secs(age),
        );
    }
    assert!(
        stats.latest_table_throughput(100.into()).unwrap()
            > opts.table_high_write_throughput_threshold
    );
    assert!(!merge_policy::check_is_low_write_throughput(
        &stats,
        table_ids.into_iter(),
        &opts
    ));
    // Once a successful cold sample arrives, the old peak must stop merging without
    // continuing to classify the current load as hot.
    advance_time(std::time::Duration::from_secs(1)).await;
    stats.record_commit(100.into(), 0, tokio::time::Instant::now());
    assert_eq!(stats.latest_table_throughput(100.into()), Some(0));
    // Reversed custom thresholds must not admit the retained hot observations either.
    opts.table_low_write_throughput_threshold = opts.table_high_write_throughput_threshold + 1;
    assert!(!merge_policy::check_is_low_write_throughput(
        &stats,
        table_ids.into_iter(),
        &opts
    ));
}

#[test]
fn test_merge_requires_observed_cold_window() {
    use super::{TableWriteThroughputStatisticManager, merge_policy};
    use crate::manager::MetaOpts;
    let mut opts = MetaOpts::test(false);
    let table_ids = [TableId::new(100), TableId::new(101)];
    let now = tokio::time::Instant::now();
    let mut stats = TableWriteThroughputStatisticManager::new(240);
    for age in (0..=240).rev() {
        stats.record_commit(100.into(), 0, now - std::time::Duration::from_secs(age));
    }
    assert!(
        !merge_policy::check_is_low_write_throughput(&stats, table_ids.into_iter(), &opts),
        "every member needs observations"
    );
    stats.record_commit(101.into(), 0, now);
    assert!(
        !merge_policy::check_is_low_write_throughput(&stats, table_ids.into_iter(), &opts),
        "one commit after a pause is insufficient"
    );
    let mut stats = TableWriteThroughputStatisticManager::new(240);
    // Both fast commits and commits farther apart than the history window prove idleness.
    for (table, seconds) in [(100, 1), (101, 300)] {
        for age in (0..=240_u64.div_ceil(seconds)).rev() {
            stats.record_commit(
                table.into(),
                0,
                now - std::time::Duration::from_secs(age * seconds),
            );
        }
    }
    assert!(merge_policy::check_is_low_write_throughput(
        &stats,
        table_ids.into_iter(),
        &opts
    ));
    opts.table_high_write_throughput_threshold = 0;
    assert!(
        merge_policy::check_is_low_write_throughput(&stats, table_ids.into_iter(), &opts),
        "zero throughput is still idle when the split threshold is zero"
    );
}
