// Copyright 2025 RisingWave Labs
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

mod base_level_compaction_picker;
mod emergency_compaction_picker;
mod intra_compaction_picker;
mod manual_compaction_picker;
mod min_overlap_compaction_picker;
mod space_reclaim_compaction_picker;
mod tier_compaction_picker;
mod tombstone_reclaim_compaction_picker;
mod trivial_move_compaction_picker;
mod ttl_reclaim_compaction_picker;

mod compaction_task_validator;
mod vnode_watermark_picker;

pub use base_level_compaction_picker::LevelCompactionPicker;
pub use compaction_task_validator::{CompactionTaskValidator, ValidationRuleType};
pub use emergency_compaction_picker::EmergencyCompactionPicker;
pub use intra_compaction_picker::IntraCompactionPicker;
pub use manual_compaction_picker::ManualCompactionPicker;
pub use min_overlap_compaction_picker::MinOverlappingPicker;
use risingwave_hummock_sdk::level::{InputLevel, Levels};
pub use space_reclaim_compaction_picker::{SpaceReclaimCompactionPicker, SpaceReclaimPickerState};
pub use tier_compaction_picker::TierCompactionPicker;
pub use tombstone_reclaim_compaction_picker::{
    TombstoneReclaimCompactionPicker, TombstoneReclaimPickerState,
};
pub use trivial_move_compaction_picker::TrivialMovePicker;
pub use ttl_reclaim_compaction_picker::{TtlPickerState, TtlReclaimCompactionPicker};
pub use vnode_watermark_picker::VnodeWatermarkCompactionPicker;

use crate::hummock::level_handler::LevelHandler;

#[derive(Default, Debug)]
pub struct LocalPickerStatistic {
    pub skip_by_write_amp_limit: u64,
    pub skip_by_count_limit: u64,
    pub skip_by_pending_files: u64,
    pub skip_by_overlapping: u64,
}

#[derive(Default, Debug)]
pub struct CompactionInput {
    pub input_levels: Vec<InputLevel>,
    pub target_level: usize,
    pub target_sub_level_id: u64,
    pub select_input_size: u64,
    pub target_input_size: u64,
    pub total_file_count: u64,
    pub vnode_partition_count: u32,
}

impl CompactionInput {
    pub fn add_pending_task(&self, task_id: u64, level_handlers: &mut [LevelHandler]) {
        let mut has_l0 = false;
        for level in &self.input_levels {
            if level.level_idx != 0 {
                level_handlers[level.level_idx as usize].add_pending_task(
                    task_id,
                    self.target_level,
                    &level.table_infos,
                );
            } else {
                has_l0 = true;
            }
        }
        if has_l0 {
            let table_infos = self
                .input_levels
                .iter()
                .filter(|level| level.level_idx == 0)
                .flat_map(|level| level.table_infos.iter());
            level_handlers[0].add_pending_task(task_id, self.target_level, table_infos);
        }
    }
}

pub trait CompactionPicker {
    fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput>;
}
