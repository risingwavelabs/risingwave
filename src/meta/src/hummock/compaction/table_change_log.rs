// Copyright 2026 RisingWave Labs
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

use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_meta_model::TableId;

/// Tracker dedicated to table change log compaction tasks.
///
/// This structure is deliberately simpler than the general-purpose `CompactStatus` and
/// `CompactTaskAssignment`, which are overkill for the needs of table change log compactions.
/// Unlike those, this tracker is not persisted to the meta store and only lives in memory.
///
/// Currently, at most one compaction task is tracked per table. Support for tracking multiple
/// concurrent tasks per table can be added in the future as needed.

#[derive(Default)]
pub struct TableChangeLogCompactionTaskTracker {
    task_tracker: HashMap<TableId, CompactTask>,
    task_id_to_table_id: HashMap<HummockCompactionTaskId, TableId>,
}

impl TableChangeLogCompactionTaskTracker {
    pub fn add_task(&mut self, table_id: TableId, task: &CompactTask) {
        self.task_tracker.insert(table_id, task.clone());
        self.task_id_to_table_id.insert(task.task_id, table_id);
    }

    pub fn remove_task(&mut self, task_id: HummockCompactionTaskId) -> Option<CompactTask> {
        let table_id = self.task_id_to_table_id.remove(&task_id)?;
        self.task_tracker.remove(&table_id)
    }

    pub fn is_pending(&self, table_id: &TableId) -> bool {
        self.task_tracker.contains_key(table_id)
    }
}
