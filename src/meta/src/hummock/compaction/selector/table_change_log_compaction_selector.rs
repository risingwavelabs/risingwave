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

use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_hummock_sdk::change_log::TableChangeLog;

use crate::hummock::compaction::picker::TableChangeLogCompactionPicker;
use crate::hummock::compaction::{CompactionSelector, CompactionSelectorContext, CompactionTask};

#[derive(Default)]
pub struct TableChangeLogCompactionSelector {
    /// The index of the next table to try picking compaction for, to achieve round-robin selection between tables.
    next_table_index: usize,
}

impl CompactionSelector for TableChangeLogCompactionSelector {
    fn pick_compaction(
        &mut self,
        _task_id: HummockCompactionTaskId,
        context: CompactionSelectorContext<'_>,
    ) -> Option<CompactionTask> {
        let CompactionSelectorContext {
            table_change_log,
            developer_config,
            compacted_table_change_logs,
            table_change_log_compaction_task_tracker,
            ..
        } = context;
        let picker = TableChangeLogCompactionPicker::new(
            developer_config.table_change_log_dirty_ratio,
            developer_config.table_change_log_min_compaction_size_dirty_part,
            developer_config.table_change_log_max_compaction_size_dirty_part,
            developer_config.table_change_log_max_compaction_sst_count_dirty_part,
            developer_config.table_change_log_max_compaction_size_clean_part,
        );
        let next_table_index = if self.next_table_index < table_change_log.len() {
            self.next_table_index
        } else {
            0
        };
        tracing::debug!(
            next_table_index,
            "Triggered table change log compaction selector."
        );
        for (index, (table_id, table_change_log)) in
            table_change_log.iter().enumerate().skip(next_table_index)
        {
            if table_change_log_compaction_task_tracker.is_pending(table_id) {
                continue;
            }
            let task = picker.pick_compaction(
                *table_id,
                table_change_log,
                compacted_table_change_logs
                    .get(table_id)
                    .unwrap_or(&TableChangeLog::new(vec![])),
            );
            if let Some(task) = task {
                self.next_table_index = index + 1;
                return Some(task);
            }
        }
        self.next_table_index = 0;
        None
    }

    fn name(&self) -> &'static str {
        "TableChangeLogCompaction"
    }

    fn task_type(&self) -> risingwave_pb::hummock::compact_task::TaskType {
        risingwave_pb::hummock::compact_task::TaskType::TableChangeLog
    }
}
