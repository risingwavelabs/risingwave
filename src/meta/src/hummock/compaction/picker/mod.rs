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

mod base_level_compaction_picker;
mod manual_compaction_picker;
mod min_overlap_compaction_picker;
mod space_reclaim_compaction_picker;
mod tier_compaction_picker;
mod ttl_reclaim_compaction_picker;

pub use base_level_compaction_picker::LevelCompactionPicker;
pub use manual_compaction_picker::ManualCompactionPicker;
pub use min_overlap_compaction_picker::MinOverlappingPicker;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::InputLevel;
pub use space_reclaim_compaction_picker::{SpaceReclaimCompactionPicker, SpaceReclaimPickerState};
pub use tier_compaction_picker::TierCompactionPicker;
pub use ttl_reclaim_compaction_picker::{TtlPickerState, TtlReclaimCompactionPicker};

use crate::hummock::level_handler::LevelHandler;

#[derive(Default)]
pub struct LocalPickerStatistic {
    skip_by_write_amp_limit: bool,
    skip_by_count_limit: bool,
    skip_by_pending_files: bool,
    skip_by_overlapping: bool,
    skip_by_trivial_move: bool,

    pub select_level: usize,
    pub target_level: usize,
    pub task_label: String,
}

impl LocalPickerStatistic {
    fn new(select_level: usize, target_level: usize, task_label: String) -> Self {
        Self {
            skip_by_write_amp_limit: false,
            skip_by_count_limit: false,
            skip_by_pending_files: false,
            skip_by_overlapping: false,
            skip_by_trivial_move: false,
            select_level,
            target_level,
            task_label,
        }
    }

    fn set_skip_by_write_amp_limit(&mut self) {
        self.skip_by_write_amp_limit = true;
    }

    fn set_skip_by_count_limit(&mut self) {
        self.skip_by_count_limit = true;
    }

    fn set_skip_by_pending_files(&mut self) {
        self.skip_by_pending_files = true;
    }

    fn set_skip_by_overlapping(&mut self) {
        self.skip_by_overlapping = true;
    }

    fn set_skip_by_trivial_move(&mut self) {
        self.skip_by_trivial_move = true;
    }

    pub fn skip_by_write_amp_limit(&self) -> bool {
        self.skip_by_write_amp_limit
    }

    pub fn skip_by_count_limit(&self) -> bool {
        self.skip_by_count_limit
    }

    pub fn skip_by_pending_files(&self) -> bool {
        self.skip_by_pending_files
    }

    pub fn skip_by_overlapping(&self) -> bool {
        self.skip_by_overlapping
    }

    pub fn skip_by_trivial_move(&self) -> bool {
        self.skip_by_trivial_move
    }
}

pub struct CompactionInput {
    pub input_levels: Vec<InputLevel>,
    pub target_level: usize,
    pub target_sub_level_id: u64,
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
    ) -> (Option<CompactionInput>, Vec<LocalPickerStatistic>);
}
