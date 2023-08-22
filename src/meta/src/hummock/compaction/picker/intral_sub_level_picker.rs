use std::sync::Arc;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey};

use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{CompactionConfig, InputLevel, LevelType};

use crate::hummock::compaction::create_overlap_strategy;
use crate::hummock::compaction::picker::min_overlap_compaction_picker::NonOverlapSubLevelPicker;
use crate::hummock::compaction::picker::{CompactionInput, CompactionPicker, LocalPickerStatistic};
use crate::hummock::level_handler::LevelHandler;

pub struct IntraSubLevelPicker {
    config: Arc<CompactionConfig>,
}

impl IntraSubLevelPicker {
    pub fn new(config: Arc<CompactionConfig>) -> Self {
        Self { config }
    }
}

impl CompactionPicker for IntraSubLevelPicker {
    fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let l0 = levels.l0.as_ref().unwrap();
        let overlap_strategy = create_overlap_strategy(self.config.compaction_mode());

        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type() != LevelType::Nonoverlapping || level_handlers[0].is_level_all_pending_compact(level) {
                continue;
            }

            if level.partition_vnode_count == 0 && level.total_file_size > self.config.sub_level_max_compaction_bytes {
                continue;
            }

            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.sub_level_max_compaction_bytes,
            );

            if level.partition_vnode_count == 0 {

            } else {
                assert_eq!(levels.member_table_ids.len(), 1);
                let mut partition_vnode_count: usize = 1;
                while partition_vnode_count * 2 <= (level.partition_vnode_count as usize) {
                    partition_vnode_count *= 2;
                }
                let partition_size = VirtualNode::COUNT / partition_vnode_count;
                for partition_id in 0..partition_vnode_count {
                    let smallest_vnode = partition_id * partition_size;
                    let largest_vnode = (partition_id + 1) * partition_size;
                    let smallest_table_key = UserKey::prefix_of_vnode(levels.member_table_ids[0], VirtualNode::from_index(smallest_vnode));
                    let largest_table_key = UserKey::prefix_of_vnode(levels.member_table_ids[0], VirtualNode::from_index(largest_vnode));
                    for sub_level in &l0.sub_levels[idx..] {
                        let idx = sub_level.table_infos.partition_point(|sst| {
                          FullKey::decode(&sst.key_range.as_ref().unwrap().right).user_key.lt(&smallest_table_key.as_ref())
                        });
                        if idx >= sub_level.table_infos.len() {
                            continue;
                        }
                        if sub_level.table_infos[idx].table_ids.
                    }
                }
            }



                // This limitation would keep our write-amplification no more than
                // ln(max_compaction_bytes/flush_level_bytes) /
                // ln(self.config.level0_sub_level_compact_level_count/2) Here we only use half
                // of level0_sub_level_compact_level_count just for convenient.
                let is_write_amp_large =
                    max_level_size * self.config.level0_sub_level_compact_level_count as u64 / 2
                        >= input.total_file_size;

                if (is_write_amp_large
                    || input.sstable_infos.len() < tier_sub_level_compact_level_count)
                    && input.total_file_count < self.config.level0_max_compact_file_number as usize
                {
                    skip_by_write_amp = true;
                    continue;
                }

                let mut select_level_inputs = Vec::with_capacity(input.sstable_infos.len());
                for level_select_sst in input.sstable_infos {
                    if level_select_sst.is_empty() {
                        continue;
                    }
                    select_level_inputs.push(InputLevel {
                        level_idx: 0,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: level_select_sst,
                    });
                }
                select_level_inputs.reverse();
                return Some(CompactionInput {
                    input_levels: select_level_inputs,
                    target_level: 0,
                    target_sub_level_id: level.sub_level_id,
                });
            }

            if skip_by_write_amp {
                stats.skip_by_write_amp_limit += 1;
            }
        }
    }
}
