// Copyright 2022 RisingWave Labs
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

use std::collections::HashMap;

use risingwave_hummock_sdk::level::Level;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::{HummockCompactionTaskId, HummockSstableId};
use risingwave_pb::hummock::level_handler::RunningCompactTask;

use crate::hummock::in_progress_compaction::PendingInputSstIndex;

#[derive(Clone, Debug, PartialEq)]
pub struct LevelHandler {
    level: u32,
    /// Per-level input-side pending view used by picker checks and metrics.
    pending_inputs: PendingInputSstIndex,
}

impl LevelHandler {
    pub fn new(level: u32) -> Self {
        Self {
            level,
            pending_inputs: PendingInputSstIndex::default(),
        }
    }

    pub fn get_level(&self) -> u32 {
        self.level
    }

    pub fn remove_task(&mut self, target_task_id: u64) {
        self.pending_inputs.remove_task(target_task_id);
    }

    pub fn is_pending_compact(&self, sst_id: &HummockSstableId) -> bool {
        self.pending_inputs.contains_sst(sst_id)
    }

    pub fn pending_task_id_by_sst(
        &self,
        sst_id: &HummockSstableId,
    ) -> Option<HummockCompactionTaskId> {
        self.pending_inputs.task_id_by_sst(sst_id)
    }

    pub fn is_level_pending_compact(&self, level: &Level) -> bool {
        self.pending_inputs.contains_any_sst_in_level(level)
    }

    pub fn is_level_all_pending_compact(&self, level: &Level) -> bool {
        self.pending_inputs.contains_all_ssts_in_level(level)
    }

    pub fn add_pending_task<'a>(
        &mut self,
        task_id: u64,
        target_level: usize,
        ssts: impl IntoIterator<Item = &'a SstableInfo>,
    ) {
        self.pending_inputs
            .add_pending_task(task_id, target_level as u32, ssts);
    }

    pub fn pending_file_count(&self) -> usize {
        self.pending_inputs.pending_file_count()
    }

    pub fn pending_file_size(&self) -> u64 {
        self.pending_inputs.pending_file_size()
    }

    pub fn pending_output_file_size(&self, target_level: u32) -> u64 {
        self.pending_inputs.pending_output_file_size(target_level)
    }

    pub fn pending_tasks_ids(&self) -> Vec<u64> {
        self.pending_inputs.task_ids()
    }

    pub fn pending_tasks(&self) -> &[RunningCompactTask] {
        self.pending_inputs.tasks()
    }

    pub fn compacting_files(&self) -> &HashMap<HummockSstableId, HummockCompactionTaskId> {
        self.pending_inputs.sst_to_task()
    }

    #[cfg(test)]
    pub(crate) fn test_add_pending_sst(&mut self, sst_id: HummockSstableId, task_id: u64) {
        self.pending_inputs.test_add_pending_sst(sst_id, task_id);
    }
}

impl From<&LevelHandler> for risingwave_pb::hummock::LevelHandler {
    fn from(lh: &LevelHandler) -> Self {
        risingwave_pb::hummock::LevelHandler {
            level: lh.level,
            tasks: lh.pending_tasks().to_vec(),
        }
    }
}

impl From<&risingwave_pb::hummock::LevelHandler> for LevelHandler {
    fn from(lh: &risingwave_pb::hummock::LevelHandler) -> Self {
        Self {
            pending_inputs: PendingInputSstIndex::from_tasks(lh.tasks.clone()),
            level: lh.level,
        }
    }
}
