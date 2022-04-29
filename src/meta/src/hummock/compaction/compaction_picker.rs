

use risingwave_pb::hummock::Level;

use crate::hummock::compaction::SearchResult;
use crate::hummock::level_handler::LevelHandler;

pub trait CompactionPicker {
    fn pick_compaction(
        &self,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult>;
}

pub struct SizeOverlapPicker {
}

impl CompactionPicker for SizeOverlapPicker {
    fn pick_compaction(&self, levels: &[Level], level_handlers: &mut [LevelHandler]) -> Option<SearchResult> {
        None
    }
}