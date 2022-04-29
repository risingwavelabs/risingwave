use std::collections::HashMap;
use std::intrinsics::roundf64;
use std::sync::Arc;
use itertools::Itertools;
use risingwave_pb::hummock::Level;

use crate::hummock::compaction::compaction_picker::{CompactionPicker, SizeOverlapPicker};
use crate::hummock::compaction::{CompactionConfig, SearchResult};
use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
use crate::hummock::compaction::tier_compaction_picker::TierCompactionPicker;
use crate::hummock::level_handler::LevelHandler;


pub trait LevelSelector {
    fn select_level(
        &self,
        task_id: u64,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Box<dyn CompactionPicker>;

    fn pick_compaction(
        &self,
        task_id: u64,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        let picker = self.select_level(task_id, levels, level_handlers);
        picker.pick_compaction(levels, level_handlers)
    }

    fn apply_compact_result(&mut self, levels: &[Level]);
}

// TODO: Set these configurations by meta rpc
pub struct DynamicLevelSelector {
    config: Arc<CompactionConfig>,
    level_max_bytes: Vec<u64>,
    base_level: usize,
}

struct SelectorContext {
    // score and level idx.
    level_scores: Vec<(u64, usize)>,
}

impl DynamicLevelSelector {
    pub fn new(
        config: Arc<CompactionConfig>,
    ) -> Self {
        DynamicLevelSelector {
            base_level: config.max_level,
            level_max_bytes: vec![0u64; config.max_level as usize + 1];
            config,
        }
    }

    fn create_compaction_picker(
        &self,
        level: usize,
        task_id: u64,
    ) -> Box<dyn CompactionPicker> {
        if level == 0 {
            Box::new(TierCompactionPicker::new(task_id, self.config.clone(), Box::new(RangeOverlapStrategy::default())))
        } else {
            Box::new(SizeOverlapPicker{})
        }
    }

    fn get_priority_levels(&self, levels: &[Level], handlers: &mut [LevelHandler]) -> Vec<(u64, usize)> {
        todo!();
        vec![]
    }
}

impl LevelSelector for DynamicLevelSelector {
    fn select_level(
        &self,
        task_id: u64,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Box<dyn CompactionPicker> {
        let level_scores = self.get_priority_levels(levels, level_handlers);
        self.create_compaction_picker(level_scores[0].1, task_id)
    }

    fn pick_compaction(
        &self,
        task_id: u64,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        let level_scores = self.get_priority_levels(levels, level_handlers);
        for (score, level_idx) in level_scores {
            if score <= 100 {
                return None;
            }
            let picker = self.create_compaction_picker(level_idx);
            if let Some(ret) = picker.pick_compaction(levels, level_handlers) {
                return Some(ret);
            }
        }
        None
    }


    fn apply_compact_result(&mut self, levels: &[Level]) {
        let mut first_non_empty_level = 0;
        let mut max_level_size = 0;

        for level  in levels.iter() {
            let mut total_file_size = 0;
            for table in &level.table_infos {
                total_file_size += table.file_size;
            }
            if level.level_idx > 0 {
                if total_file_size > 0 && first_non_empty_level == 0 {
                    first_non_empty_level = level.level_idx as usize;
                }
                max_level_size = std::cmp::max(max_level_size, total_file_size);
            }
        }

        self.level_max_bytes.resize(self.config.max_level as usize + 1, u64::MAX);

        if max_level_size == 0 {
            // Use the bottommost level.
            self.base_level = self.config.max_level;
            return;
        }

        let l0_size = scores[0].0;

        let base_bytes_max = std::cmp::max(self.config.max_bytes_for_level_base, l0_size);
        let base_bytes_min = base_bytes_max / self.config.max_bytes_for_level_multiplier;

        let mut cur_level_size = max_level_size;
        for _ in first_non_empty_level..self.config.max_level {
            cur_level_size /= self.config.max_bytes_for_level_multiplier;
        }

        let mut base_level_size = if cur_level_size <= base_bytes_min {
            // Case 1. If we make target size of last level to be max_level_size,
            // target size of the first non-empty level would be smaller than
            // base_bytes_min. We set it be base_bytes_min.
            self.base_level = first_non_empty_level;
            base_bytes_min + 1
        } else {
            self.base_level = first_non_empty_level;
            while self.base_level > 1 && cur_level_size > base_bytes_min {
                self.base_level -= 1;
                cur_level_size /= self.config.max_bytes_for_level_multiplier;
            }
            std::cmp::min(base_bytes_max, cur_level_size)
        };

        let mut level_multiplier = self.config.max_bytes_for_level_multiplier as f64;

        if l0_size > base_level_size && levels[0].table_infos.len() > self.config.level0_max_file_number {
            // We adjust the base level according to actual L0 size, and adjust
            // the level multiplier accordingly, when the number of L0 files reaches twice the level0_max_file_number.
            // We don't do this otherwise to keep the LSM-tree structure stable
            // unless the L0 compation is backlogged.
            base_level_size = l0_size;
            if base_level == self.config.max_level {
                // There is only two level (L0 and L1).
                level_multiplier = 1.0;
            } else {
                unsafe {
                    level_multiplier =
                        roundf64(std::intrinsics::powf64(max_level_size as f64 / (base_level_size as f64), 1.0 / (self.config.max_level - base_level) as f64));
                }
            }
        }

        let mut level_size = base_level_size;
        for i in self.base_level..=self.config.max_level {
            self.level_max_bytes[i] = std::cmp::max(level_size, base_bytes_max);
            level_size = (level_size as f64 * level_multiplier) as u64;
        }
    }
}
