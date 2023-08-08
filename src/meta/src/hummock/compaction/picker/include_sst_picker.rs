// Copyright 2023 RisingWave Labs
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

use risingwave_pb::hummock::Level;

use crate::hummock::compaction::overlap_strategy::{OverlapInfo, OverlapStrategy};
use crate::hummock::compaction::picker::min_overlap_compaction_picker::SubLevelSstables;
use crate::hummock::level_handler::LevelHandler;
pub const MAX_LEVEL_COUNT: usize = 32;

pub struct L0IncludeSstPicker {
    overlap_strategy: Arc<dyn OverlapStrategy>,
    max_compact_size: u64,
    max_file_count: u64,
}

impl L0IncludeSstPicker {
    pub fn new(
        overlap_strategy: Arc<dyn OverlapStrategy>,
        max_compact_size: u64,
        max_file_count: u64,
    ) -> Self {
        Self {
            overlap_strategy,
            max_compact_size,
            max_file_count,
        }
    }

    pub fn pick_tables(
        &self,
        overlap_info: &dyn OverlapInfo,
        sub_levels: &[Level],
        level_handler: &LevelHandler,
    ) -> SubLevelSstables {
        let mut overlaps: Vec<Box<dyn OverlapInfo>> = vec![];
        let mut ret = SubLevelSstables::default();
        for level in sub_levels {
            if ret.total_file_size > self.max_compact_size
                || ret.sstable_infos.len() >= MAX_LEVEL_COUNT
                || ret.total_file_count as u64 > self.max_file_count
            {
                break;
            }
            let mut range = overlap_info.check_multiple_include(&level.table_infos);
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
