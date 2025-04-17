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

use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_pb::hummock::compact_task;

use super::{CompactionSelector, DynamicLevelSelectorCore};
use crate::hummock::compaction::picker::{EmergencyCompactionPicker, LocalPickerStatistic};
use crate::hummock::compaction::selector::CompactionSelectorContext;
use crate::hummock::compaction::{CompactionTask, create_compaction_task};

#[derive(Default)]
pub struct EmergencySelector {}

impl CompactionSelector for EmergencySelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        context: CompactionSelectorContext<'_>,
    ) -> Option<CompactionTask> {
        let CompactionSelectorContext {
            group,
            levels,
            level_handlers,
            selector_stats,
            developer_config,
            ..
        } = context;
        let dynamic_level_core = DynamicLevelSelectorCore::new(
            group.compaction_config.clone(),
            developer_config.clone(),
        );
        let ctx = dynamic_level_core.calculate_level_base_size(levels);
        let picker = EmergencyCompactionPicker::new(
            ctx.base_level,
            group.compaction_config.clone(),
            developer_config,
        );

        let mut stats = LocalPickerStatistic::default();
        if let Some(compaction_input) = picker.pick_compaction(levels, level_handlers, &mut stats) {
            compaction_input.add_pending_task(task_id, level_handlers);

            return Some(create_compaction_task(
                group.compaction_config.as_ref(),
                compaction_input,
                ctx.base_level,
                self.task_type(),
            ));
        }

        selector_stats.skip_picker.push((0, ctx.base_level, stats));

        None
    }

    fn name(&self) -> &'static str {
        "EmergencyCompaction"
    }

    fn task_type(&self) -> compact_task::TaskType {
        compact_task::TaskType::Emergency
    }
}
