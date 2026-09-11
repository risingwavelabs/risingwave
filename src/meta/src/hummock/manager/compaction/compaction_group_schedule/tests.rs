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

use std::collections::BTreeMap;

use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactionConfig;

use super::CompactionGroupStatistic;
use crate::hummock::model::CompactionGroup;

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

#[tokio::test]
#[cfg(not(madsim))]
async fn test_merge_validation_order_and_lock_boundary() {
    use std::collections::HashSet;
    use std::task::Poll;

    use super::TableWriteThroughputStatisticManager;
    use crate::hummock::error::Error;
    use crate::hummock::test_utils::setup_compute_env;

    let (_, manager, _, _) = setup_compute_env(80).await;
    let mut stats = TableWriteThroughputStatisticManager::new(240);
    let now = chrono::Utc::now().timestamp();
    for age in (0..240).rev() {
        stats.add_table_throughput_with_ts(100.into(), 0, now - age, 1);
    }
    let created = HashSet::from([100.into()]);
    let left = group(10.into(), &[100], false);
    let right = group(11.into(), &[101], false);
    let mut oversized = left.clone();
    oversized.group_size = 1;
    // Each pair violates multiple rules. Keep the first rejection and its exact message,
    // and ensure snapshot rejection does not acquire the version lock.
    let cases = [
        (
            group(2.into(), &[], true),
            group(3.into(), &[], true),
            "group-2 and group-3 are both StaticCompactionGroupId",
        ),
        (
            group(10.into(), &[], true),
            right.clone(),
            "group-10 or group-11 is empty",
        ),
        (
            group(10.into(), &[100, 102], true),
            right.clone(),
            "group-10 and group-11 have overlapping table id ranges, not mergeable",
        ),
        (
            group(10.into(), &[100], true),
            right.clone(),
            "group-10 or group-11 disable_auto_group_scheduling",
        ),
        (
            group(10.into(), &[99], false),
            right.clone(),
            "Cannot merge creating group 10 next_group 11",
        ),
        (
            oversized,
            right.clone(),
            "Cannot merge huge group 10 group_size 1 next_group 11 next_group_size 0 size_limit 0",
        ),
        (
            left.clone(),
            right.clone(),
            "Cannot merge creating group 10 next group 11",
        ),
    ];
    let versioning = manager.versioning.write().await;
    for (left, right, expected) in cases {
        let merge = tokio::task::unconstrained(
            manager.try_merge_compaction_group(&stats, &left, &right, &created),
        );
        tokio::pin!(merge);
        let Poll::Ready(Err(Error::CompactionGroup(actual))) = futures::poll!(merge.as_mut())
        else {
            panic!("snapshot rejection must complete without the version lock: {expected}");
        };
        assert_eq!(actual, expected);
    }

    let created = HashSet::from([100.into(), 101.into()]);
    for age in (0..240).rev() {
        stats.add_table_throughput_with_ts(101.into(), 0, now - age, 1);
    }
    let merge = tokio::task::unconstrained(
        manager.try_merge_compaction_group(&stats, &left, &right, &created),
    );
    tokio::pin!(merge);
    assert!(futures::poll!(merge.as_mut()).is_pending());
    drop(versioning);
    let Err(Error::CompactionGroup(actual)) = merge.await else {
        panic!("current-version validation must reject the missing group");
    };
    assert_eq!(
        actual,
        "cannot merge compaction group 10 because it does not exist"
    );
}
