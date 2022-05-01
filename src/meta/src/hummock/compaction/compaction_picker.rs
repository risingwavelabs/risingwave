use std::sync::Arc;

use risingwave_pb::hummock::Level;

use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::{CompactionConfig, SearchResult};
use crate::hummock::level_handler::LevelHandler;

pub trait CompactionPicker {
    fn pick_compaction(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult>;
}

pub struct SizeOverlapPicker {
    compact_task_id: u64,
    config: Arc<CompactionConfig>,
    overlap_strategy: Box<dyn OverlapStrategy>,
    level: usize,
}

impl SizeOverlapPicker {
    pub fn new(
        compact_task_id: u64,
        level: usize,
        config: Arc<CompactionConfig>,
        overlap_strategy: Box<dyn OverlapStrategy>,
    ) -> SizeOverlapPicker {
        SizeOverlapPicker {
            compact_task_id,
            config,
            overlap_strategy,
            level,
        }
    }
}

impl CompactionPicker for SizeOverlapPicker {
    fn pick_compaction(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        None
    }
}
