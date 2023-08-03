use std::sync::Arc;

use risingwave_pb::hummock::Level;

use crate::hummock::compaction::overlap_strategy::{OverlapInfo, OverlapStrategy};
use crate::hummock::compaction::picker::min_overlap_compaction_picker::SubLevelSstables;
use crate::hummock::level_handler::LevelHandler;
pub const MAX_LEVEL_COUNT: usize = 32;

pub struct L0IncludeSstPicker {
    overlap_info: Box<dyn OverlapInfo>,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    max_compact_size: u64,
}

impl L0IncludeSstPicker {
    pub fn new(
        overlap_info: Box<dyn OverlapInfo>,
        overlap_strategy: Arc<dyn OverlapStrategy>,
        max_compact_size: u64,
    ) -> Self {
        Self {
            overlap_info,
            overlap_strategy,
            max_compact_size,
        }
    }

    pub fn pick_tables(
        &self,
        sub_levels: &[Level],
        level_handler: &LevelHandler,
    ) -> SubLevelSstables {
        let mut overlaps: Vec<Box<dyn OverlapInfo>> = vec![];
        let mut ret = SubLevelSstables::default();
        for level in sub_levels {
            if ret.total_file_size > self.max_compact_size
                || ret.sstable_infos.len() >= MAX_LEVEL_COUNT
            {
                break;
            }
            let mut range = self.overlap_info.check_multiple_include(&level.table_infos);
            for overlap in &overlaps {
                let old_range = overlap.check_multiple_include(&level.table_infos);
                range.start = std::cmp::max(range.start, old_range.start);
                range.end = std::cmp::min(range.end, old_range.end);
            }
            if range.start >= range.end {
                break;
            }
            let pending_compact = range
                .any(|index| level_handler.is_pending_compact(&level.table_infos[index].sst_id));
            if pending_compact {
                break;
            }
            let mut overlap = self.overlap_strategy.create_overlap_info();
            ret.sstable_infos
                .push(level.table_infos[range.clone()].to_vec());
            for index in range {
                ret.total_file_count += 1;
                ret.total_file_size += level.table_infos[index].file_size;
                let key_range = level.table_infos[index].key_range.as_ref().unwrap();
                if index > 0 && index + 1 < level.table_infos.len() {
                    overlap.update_key_range(key_range);
                    continue;
                }
                let mut key_range = key_range.clone();
                if index == 0 {
                    key_range.left.clear();
                }
                if index + 1 == level.table_infos.len() {
                    key_range.right.clear();
                }
                overlap.update_key_range(&key_range);
            }
            overlaps.push(overlap);
        }
        ret
    }
}
