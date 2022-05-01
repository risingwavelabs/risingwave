use std::sync::Arc;

use risingwave_pb::hummock::Level;

use crate::hummock::compaction::compaction_picker::{CompactionPicker, SizeOverlapPicker};
use crate::hummock::compaction::overlap_strategy::{OverlapStrategy, RangeOverlapStrategy};
use crate::hummock::compaction::tier_compaction_picker::TierCompactionPicker;
use crate::hummock::compaction::{CompactionConfig, SearchResult};
use crate::hummock::level_handler::LevelHandler;

pub trait LevelSelector: Sync + Send {
    fn select_level(
        &mut self,
        task_id: u64,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Box<dyn CompactionPicker>;

    fn pick_compaction(
        &mut self,
        task_id: u64,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        let picker = self.select_level(task_id, levels, level_handlers);
        picker.pick_compaction(levels, level_handlers)
    }

    fn name(&self) -> &'static str;
}

#[derive(Default)]
pub struct SelectContext {
    level_max_bytes: Vec<u64>,
    base_level: usize,
    score_levels: Vec<(u64, usize)>,
}

// TODO: Set these configurations by meta rpc
pub struct DynamicLevelSelector {
    config: Arc<CompactionConfig>,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl Default for DynamicLevelSelector {
    fn default() -> Self {
        DynamicLevelSelector::new(
            Arc::new(CompactionConfig::default()),
            Arc::new(RangeOverlapStrategy::default()),
        )
    }
}

impl DynamicLevelSelector {
    pub fn new(config: Arc<CompactionConfig>, overlap_strategy: Arc<dyn OverlapStrategy>) -> Self {
        DynamicLevelSelector {
            config,
            overlap_strategy,
        }
    }

    fn create_compaction_picker(
        &self,
        level: usize,
        base_level: usize,
        task_id: u64,
    ) -> Box<dyn CompactionPicker> {
        if level == 0 {
            Box::new(TierCompactionPicker::new(
                task_id,
                base_level,
                self.config.clone(),
                self.overlap_strategy.clone(),
            ))
        } else {
            Box::new(SizeOverlapPicker::new(
                task_id,
                level,
                self.overlap_strategy.clone(),
            ))
        }
    }

    // TODO: calculate this scores in apply compact result.
    fn calculate_level_base_score(&self, levels: &[Level]) -> SelectContext {
        let mut first_non_empty_level = 0;
        let mut max_level_size = 0;
        let mut ctx = SelectContext::default();

        let mut l0_size = 0;
        for level in levels.iter() {
            let mut total_file_size = 0;
            for table in &level.table_infos {
                total_file_size += table.file_size;
            }
            if level.level_idx > 0 {
                if total_file_size > 0 && first_non_empty_level == 0 {
                    first_non_empty_level = level.level_idx as usize;
                }
                max_level_size = std::cmp::max(max_level_size, total_file_size);
            } else {
                l0_size = max_level_size;
            }
        }

        ctx.level_max_bytes
            .resize(self.config.max_level as usize + 1, u64::MAX);

        if max_level_size == 0 {
            // Use the bottommost level.
            ctx.base_level = self.config.max_level;
            return ctx;
        }

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
            ctx.base_level = first_non_empty_level;
            base_bytes_min + 1
        } else {
            ctx.base_level = first_non_empty_level;
            while ctx.base_level > 1 && cur_level_size > base_bytes_min {
                ctx.base_level -= 1;
                cur_level_size /= self.config.max_bytes_for_level_multiplier;
            }
            std::cmp::min(base_bytes_max, cur_level_size)
        };

        let mut level_multiplier = self.config.max_bytes_for_level_multiplier as f64;

        if l0_size > base_level_size
            && levels[0].table_infos.len() > self.config.level0_max_file_number
        {
            // We adjust the base level according to actual L0 size, and adjust
            // the level multiplier accordingly, when the number of L0 files reaches twice the
            // level0_max_file_number. We don't do this otherwise to keep the LSM-tree
            // structure stable unless the L0 compation is backlogged.
            base_level_size = l0_size;
            if ctx.base_level == self.config.max_level {
                // There is only two level (L0 and L1).
                level_multiplier = 1.0;
            } else {
                let size_multiplier = max_level_size as f64 / (base_level_size as f64);
                level_multiplier =
                    size_multiplier.powf(1.0 / (self.config.max_level - ctx.base_level) as f64);
            }
        }

        let mut level_size = base_level_size;
        for i in ctx.base_level..=self.config.max_level {
            ctx.level_max_bytes[i] = std::cmp::max(level_size, base_bytes_max);
            level_size = (level_size as f64 * level_multiplier) as u64;
        }
        ctx
    }

    fn get_priority_levels(
        &self,
        levels: &[Level],
        handlers: &mut [LevelHandler],
    ) -> SelectContext {
        let mut ctx = self.calculate_level_base_score(levels);

        // The bottommost level can not be input level.
        for level in &levels[..self.config.max_level] {
            let level_idx = level.level_idx as usize;
            let mut total_size = 0;
            let mut idle_file_count = 0;
            for table in &level.table_infos {
                if !handlers[level_idx].is_pending_compact(&table.id) {
                    total_size += table.file_size;
                    idle_file_count += 1;
                }
            }
            if total_size == 0 {
                continue;
            }
            if level_idx == 0 {
                let score = std::cmp::max(
                    total_size * 100 / self.config.max_bytes_for_level_base,
                    idle_file_count * 100 / self.config.level0_trigger_number as u64,
                );
                ctx.score_levels.push((score, 0));
            } else {
                ctx.score_levels
                    .push((total_size * 100 / ctx.level_max_bytes[level_idx], level_idx));
            }
        }

        // sort reverse to pick the largest one.
        ctx.score_levels.sort_by(|a, b| b.0.cmp(&a.0));
        ctx
    }
}

impl LevelSelector for DynamicLevelSelector {
    fn select_level(
        &mut self,
        task_id: u64,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Box<dyn CompactionPicker> {
        let ctx = self.get_priority_levels(levels, level_handlers);
        self.create_compaction_picker(ctx.score_levels[0].1, ctx.base_level, task_id)
    }

    fn pick_compaction(
        &mut self,
        task_id: u64,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        let ctx = self.get_priority_levels(levels, level_handlers);
        for (score, level_idx) in ctx.score_levels {
            if score <= 100 {
                return None;
            }
            let picker = self.create_compaction_picker(level_idx, ctx.base_level, task_id);
            if let Some(ret) = picker.pick_compaction(levels, level_handlers) {
                return Some(ret);
            }
        }
        None
    }

    fn name(&self) -> &'static str {
        "DynamicLevelSelector"
    }
}
