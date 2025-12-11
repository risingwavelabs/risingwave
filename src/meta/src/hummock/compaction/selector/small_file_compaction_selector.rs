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

use std::collections::HashMap;

use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_pb::hummock::compact_task;

use super::{CompactionSelector, DynamicLevelSelectorCore};
use crate::hummock::compaction::picker::{SmallFileCompactionPicker, SmallFilePickerState};
use crate::hummock::compaction::selector::CompactionSelectorContext;
use crate::hummock::compaction::{CompactionTask, create_compaction_task};

#[derive(Default)]
pub struct SmallFileCompactionSelector {
    state: HashMap<u64, SmallFilePickerState>,
}

impl CompactionSelector for SmallFileCompactionSelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        context: CompactionSelectorContext<'_>,
    ) -> Option<CompactionTask> {
        let CompactionSelectorContext {
            group,
            levels,
            level_handlers,
            developer_config,
            ..
        } = context;

        // Check if small file compaction is enabled
        let small_file_threshold = group.compaction_config.small_file_size_threshold?;

        if small_file_threshold == 0 {
            return None;
        }

        let dynamic_level_core =
            DynamicLevelSelectorCore::new(group.compaction_config.clone(), developer_config);
        let ctx = dynamic_level_core.calculate_level_base_size(levels);

        let picker = SmallFileCompactionPicker::new(small_file_threshold);

        let state = self.state.entry(group.group_id).or_default();

        let compaction_input = picker.pick_compaction(levels, level_handlers, state)?;
        compaction_input.add_pending_task(task_id, level_handlers);

        Some(create_compaction_task(
            group.compaction_config.as_ref(),
            compaction_input,
            ctx.base_level,
            self.task_type(),
        ))
    }

    fn name(&self) -> &'static str {
        "SmallFileCompaction"
    }

    fn task_type(&self) -> compact_task::TaskType {
        compact_task::TaskType::SmallFile
    }
}
