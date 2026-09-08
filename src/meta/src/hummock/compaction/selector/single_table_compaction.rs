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

use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::level::OverlappingLevel;
use risingwave_pb::hummock::CompactionConfig;

use super::level_selector::SCORE_BASE;
use crate::hummock::compaction::vnode_partition::L0VnodePartitionView;
use crate::hummock::level_handler::LevelHandler;

/// The immutable table metadata required by the partition-aware L0 strategy.
///
/// The manager only constructs this value for a compaction group containing exactly one table.
/// Keeping it as a distinct type prevents the generic selector from inferring single-table
/// eligibility from partial inputs.
#[derive(Clone, Copy, Debug)]
pub struct SingleTableCompactionGroup {
    table_id: TableId,
    vnode_count: usize,
}

impl SingleTableCompactionGroup {
    pub(crate) fn new(table_id: TableId, vnode_count: usize) -> Self {
        Self {
            table_id,
            vnode_count,
        }
    }

    /// Build partition-local candidates from the current L0 and pending state.
    ///
    /// `None` means that the current L0 cannot be represented by the fixed vnode layout and the
    /// caller must use the legacy global L0 strategy. `Some([])` means the partition strategy is
    /// active but no partition has reached the configured depth threshold.
    pub(super) fn build_l0_candidates(
        self,
        config: &CompactionConfig,
        l0: &OverlappingLevel,
        l0_handler: &LevelHandler,
    ) -> Option<Vec<SingleTableL0Candidate>> {
        let partition_view = L0VnodePartitionView::build(
            self.table_id,
            self.vnode_count,
            config.split_weight_by_vnode as usize,
            l0,
            l0_handler,
        )?;
        let min_l0_level_count = config.level0_sub_level_compact_level_count as usize;
        let partitions = partition_view
            .into_partitions(min_l0_level_count as u64, SCORE_BASE)
            .filter(|(score, partition_l0)| {
                *score > SCORE_BASE && !partition_l0.sub_levels.is_empty()
            })
            .map(|(score, partition_l0)| SingleTableL0Partition {
                score,
                l0: Arc::new(partition_l0),
            })
            .collect::<Vec<_>>();

        let Some(global_l0_score) = partitions.iter().map(|partition| partition.score).max() else {
            return Some(vec![]);
        };

        let mut candidates = Vec::with_capacity(partitions.len() * 2);
        for partition in &partitions {
            candidates.push(SingleTableL0Candidate {
                score: global_l0_score.saturating_add(1),
                partition_score: partition.score,
                picker_type: SingleTableL0PickerType::ToBase,
                l0: partition.l0.clone(),
                min_l0_level_count,
            });
        }
        for partition in partitions {
            candidates.push(SingleTableL0Candidate {
                score: global_l0_score,
                partition_score: partition.score,
                picker_type: SingleTableL0PickerType::Intra,
                l0: partition.l0,
                min_l0_level_count: 1,
            });
        }
        Some(candidates)
    }
}

struct SingleTableL0Partition {
    score: u64,
    l0: Arc<OverlappingLevel>,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum SingleTableL0PickerType {
    ToBase,
    Intra,
}

#[derive(Debug)]
pub(super) struct SingleTableL0Candidate {
    pub(super) score: u64,
    pub(super) partition_score: u64,
    pub(super) picker_type: SingleTableL0PickerType,
    pub(super) l0: Arc<OverlappingLevel>,
    pub(super) min_l0_level_count: usize,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    use bytes::Bytes;
    use itertools::Itertools;
    use risingwave_common::hash::VirtualNode;
    use risingwave_hummock_sdk::HummockCompactionTaskId;
    use risingwave_hummock_sdk::key::{FullKey, TableKey};
    use risingwave_hummock_sdk::key_range::KeyRange;
    use risingwave_hummock_sdk::level::{Levels, OverlappingLevel};
    use risingwave_hummock_sdk::sstable_info::SstableInfo;
    use risingwave_hummock_sdk::version::HummockVersionStateTableInfo;
    use risingwave_pb::hummock::CompactionConfig;

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::in_progress_compaction::InProgressCompactionView;
    use crate::hummock::compaction::selector::level_selector::DynamicLevelSelector;
    use crate::hummock::compaction::selector::tests::{
        generate_l0_nonoverlapping_multi_sublevels, generate_l0_nonoverlapping_sublevels,
        generate_level, generate_table,
    };
    use crate::hummock::compaction::selector::{
        CompactionSelector, CompactionSelectorContext, LocalSelectorStatistic,
    };
    use crate::hummock::compaction::{CompactionDeveloperConfig, CompactionTask};
    use crate::hummock::level_handler::LevelHandler;
    use crate::hummock::model::CompactionGroup;

    fn vnode_key(vnode: usize) -> Bytes {
        FullKey::new(
            TableId::new(1),
            TableKey(VirtualNode::from_index(vnode).to_be_bytes().to_vec()),
            u64::MAX,
        )
        .encode()
        .into()
    }

    fn vnode_sst(id: u64, left_vnode: usize, right_vnode: usize) -> SstableInfo {
        let mut sst = generate_table(id, 1, 0, 1, id);
        let mut inner = sst.get_inner();
        inner.key_range = KeyRange {
            left: vnode_key(left_vnode),
            right: vnode_key(right_vnode),
            right_exclusive: true,
        };
        sst.set_inner(inner);
        sst
    }

    fn two_partition_l0(first_depth: usize, second_depth: usize) -> OverlappingLevel {
        generate_l0_nonoverlapping_multi_sublevels(
            (0..std::cmp::max(first_depth, second_depth))
                .map(|level| {
                    let mut ssts = vec![];
                    if level < first_depth {
                        ssts.push(vnode_sst(level as u64 + 1, 0, 32));
                    }
                    if level < second_depth {
                        ssts.push(vnode_sst(level as u64 + 101, 32, 64));
                    }
                    ssts
                })
                .collect(),
        )
    }

    fn pick_compaction(
        task_id: HummockCompactionTaskId,
        group: &CompactionGroup,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        in_progress_compactions: &InProgressCompactionView,
    ) -> Option<CompactionTask> {
        DynamicLevelSelector::default().pick_compaction(
            task_id,
            CompactionSelectorContext {
                group,
                levels,
                member_table_ids: &BTreeSet::from([TableId::new(1)]),
                single_table_compaction_group: Some(SingleTableCompactionGroup::new(
                    TableId::new(1),
                    256,
                )),
                level_handlers,
                selector_stats: &mut LocalSelectorStatistic::default(),
                table_id_to_options: &HashMap::default(),
                developer_config: Arc::new(CompactionDeveloperConfig::default()),
                table_watermarks: &HashMap::default(),
                state_table_info: &HummockVersionStateTableInfo::empty(),
                in_progress_compactions,
            },
        )
    }

    #[test]
    fn local_depth_does_not_sum_across_partitions() {
        let config = CompactionConfig {
            split_weight_by_vnode: 8,
            ..CompactionConfigBuilder::new()
                .level0_sub_level_compact_level_count(3)
                .build()
        };
        let l0 = generate_l0_nonoverlapping_sublevels(
            (0..8)
                .map(|partition| {
                    vnode_sst(partition as u64 + 1, partition * 32, (partition + 1) * 32)
                })
                .collect(),
        );
        let candidates = SingleTableCompactionGroup::new(TableId::new(1), 256)
            .build_l0_candidates(&config, &l0, &LevelHandler::new(0))
            .unwrap();
        assert!(candidates.is_empty());
    }

    #[test]
    fn pending_partition_does_not_block_another_partition() {
        let config = CompactionConfig {
            split_weight_by_vnode: 8,
            ..CompactionConfigBuilder::new()
                .max_level(1)
                .max_bytes_for_level_base(1024)
                .level0_sub_level_compact_level_count(2)
                .build()
        };
        let group = CompactionGroup::new(1, config);
        let levels = Levels {
            levels: vec![generate_level(1, vec![])],
            l0: generate_l0_nonoverlapping_multi_sublevels(
                (1..=3)
                    .map(|id| vec![vnode_sst(id, 0, 32), vnode_sst(id + 100, 32, 64)])
                    .collect(),
            ),
            ..Default::default()
        };
        let mut handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];
        handlers[0].add_pending_task(
            100,
            0,
            levels
                .l0
                .sub_levels
                .iter()
                .map(|level| &level.table_infos[0]),
        );

        let task = pick_compaction(
            1,
            &group,
            &levels,
            &mut handlers,
            &InProgressCompactionView::default(),
        )
        .unwrap();
        let selected_l0_ids = task
            .input
            .input_levels
            .iter()
            .filter(|level| level.level_idx == 0)
            .flat_map(|level| level.table_infos.iter())
            .map(|sst| sst.sst_id.as_raw_id())
            .collect_vec();
        assert!(!selected_l0_ids.is_empty());
        assert!(selected_l0_ids.iter().all(|sst_id| *sst_id > 100));
    }

    #[test]
    fn to_base_prefers_the_deeper_partition() {
        let config = CompactionConfig {
            split_weight_by_vnode: 8,
            ..CompactionConfigBuilder::new()
                .max_level(1)
                .max_bytes_for_level_base(1024)
                .level0_sub_level_compact_level_count(2)
                .build()
        };
        let group = CompactionGroup::new(1, config);
        let levels = Levels {
            levels: vec![generate_level(1, vec![])],
            l0: two_partition_l0(4, 2),
            ..Default::default()
        };
        let mut handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let task = pick_compaction(
            1,
            &group,
            &levels,
            &mut handlers,
            &InProgressCompactionView::default(),
        )
        .unwrap();
        assert_eq!(task.input.target_level, 1);
        assert!(
            task.input
                .input_levels
                .iter()
                .filter(|level| level.level_idx == 0)
                .flat_map(|level| level.table_infos.iter())
                .all(|sst| sst.sst_id.as_raw_id() < 100)
        );
    }

    #[test]
    fn intra_is_used_only_after_partition_to_base_is_blocked() {
        let config = CompactionConfig {
            split_weight_by_vnode: 8,
            ..CompactionConfigBuilder::new()
                .max_level(1)
                .max_bytes_for_level_base(1024)
                .level0_sub_level_compact_level_count(2)
                .build()
        };
        let group = CompactionGroup::new(1, config);
        let levels = Levels {
            levels: vec![generate_level(
                1,
                vec![vnode_sst(1000, 0, 32), vnode_sst(1001, 32, 64)],
            )],
            l0: two_partition_l0(4, 2),
            ..Default::default()
        };
        let mut handlers = vec![LevelHandler::new(0), LevelHandler::new(1)];
        handlers[1].add_pending_task(100, 1, &levels.levels[0].table_infos);

        let task = pick_compaction(
            1,
            &group,
            &levels,
            &mut handlers,
            &InProgressCompactionView::default(),
        )
        .unwrap();
        assert_eq!(task.input.target_level, 0);
        assert!(
            task.input
                .input_levels
                .iter()
                .flat_map(|level| level.table_infos.iter())
                .all(|sst| sst.sst_id.as_raw_id() < 100)
        );
    }
}
