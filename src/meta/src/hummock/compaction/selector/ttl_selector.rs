//  Copyright 2025 RisingWave Labs
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

use std::collections::HashMap;

use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_pb::hummock::compact_task;

use super::{CompactionSelector, DynamicLevelSelectorCore};
use crate::hummock::compaction::picker::{TtlPickerState, TtlReclaimCompactionPicker};
use crate::hummock::compaction::selector::CompactionSelectorContext;
use crate::hummock::compaction::{CompactionTask, create_compaction_task};

#[derive(Default)]
pub struct TtlCompactionSelector {
    state: HashMap<u64, TtlPickerState>,
}

impl CompactionSelector for TtlCompactionSelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        context: CompactionSelectorContext<'_>,
    ) -> Option<CompactionTask> {
        let CompactionSelectorContext {
            group,
            levels,
            level_handlers,
            table_id_to_options,
            developer_config,
            ..
        } = context;
        let dynamic_level_core =
            DynamicLevelSelectorCore::new(group.compaction_config.clone(), developer_config);
        let ctx = dynamic_level_core.calculate_level_base_size(levels);
        let picker = TtlReclaimCompactionPicker::new(table_id_to_options);
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
        "TtlCompaction"
    }

    fn task_type(&self) -> compact_task::TaskType {
        compact_task::TaskType::Ttl
    }
}
