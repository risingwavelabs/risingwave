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

use crate::hummock::compaction::compaction_picker::CompactionPicker;
use crate::hummock::compaction::SearchResult;
use crate::hummock::level_handler::LevelHandler;

/// Picks files from common level which will be assembled according to compaction groups later.
// TODO: A dedicated picker for common level is not mandatory. We can remove this if any other
// picker is qualified.
pub struct GroupCompactionPicker {}

impl CompactionPicker for GroupCompactionPicker {
    fn need_compaction(&self, _levels: Vec<Level>) -> bool {
        todo!()
    }

    fn try_pick_compaction(
        &self,
        _levels: Vec<Level>,
        _level_handlers: &mut [LevelHandler],
        _compact_task_id: u64,
    ) -> Option<SearchResult> {
        todo!()
    }
}
