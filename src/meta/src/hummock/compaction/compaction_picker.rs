// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_pb::hummock::Level;

use crate::hummock::compaction::SearchResult;
use crate::hummock::level_handler::LevelHandler;

pub trait CompactionPicker {
    /// Decides if a compaction is needed after excluding files already in compaction. This method
    /// is expected to be lightweight compared with `pick_compaction_task`.
    fn need_compaction(&self, levels: Vec<Level>) -> bool;
    /// Tries to pick a compaction task.
    fn try_pick_compaction(
        &self,
        levels: Vec<Level>,
        level_handlers: &mut [LevelHandler],
        compact_task_id: u64,
    ) -> Option<SearchResult>;
}
