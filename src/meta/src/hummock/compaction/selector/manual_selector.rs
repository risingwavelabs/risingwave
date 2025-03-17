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

use std::collections::HashSet;

use bytes::Bytes;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{HummockCompactionTaskId, HummockSstableId};
use risingwave_pb::hummock::compact_task;

use super::{CompactionSelector, DynamicLevelSelectorCore};
use crate::hummock::compaction::picker::{
    CompactionPicker, LocalPickerStatistic, ManualCompactionPicker,
};
use crate::hummock::compaction::selector::CompactionSelectorContext;
use crate::hummock::compaction::{CompactionTask, create_compaction_task, create_overlap_strategy};

#[derive(Clone, Debug, PartialEq)]
pub struct ManualCompactionOption {
    /// Filters out SSTs to pick. Has no effect if empty.
    pub sst_ids: Vec<HummockSstableId>,
    /// Filters out SSTs to pick.
    pub key_range: KeyRange,
    /// Filters out SSTs to pick. Has no effect if empty.
    pub internal_table_id: HashSet<StateTableId>,
    /// Input level.
    pub level: usize,
}

impl Default for ManualCompactionOption {
    fn default() -> Self {
        Self {
            sst_ids: vec![],
            key_range: KeyRange {
                left: Bytes::default(),
                right: Bytes::default(),
                right_exclusive: false,
            },
            internal_table_id: HashSet::default(),
            level: 1,
        }
    }
}

pub struct ManualCompactionSelector {
    option: ManualCompactionOption,
}

impl ManualCompactionSelector {
    pub fn new(option: ManualCompactionOption) -> Self {
        Self { option }
    }
}

impl CompactionSelector for ManualCompactionSelector {
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
        let dynamic_level_core =
            DynamicLevelSelectorCore::new(group.compaction_config.clone(), developer_config);
        let overlap_strategy = create_overlap_strategy(group.compaction_config.compaction_mode());
        let ctx = dynamic_level_core.calculate_level_base_size(levels);
        let (mut picker, base_level) = {
            let target_level = if self.option.level == 0 {
                ctx.base_level
            } else if self.option.level == group.compaction_config.max_level as usize {
                self.option.level
            } else {
                self.option.level + 1
            };
            if self.option.level > 0 && self.option.level < ctx.base_level {
                return None;
            }
            (
                ManualCompactionPicker::new(overlap_strategy, self.option.clone(), target_level),
                ctx.base_level,
            )
        };

        let compaction_input =
            picker.pick_compaction(levels, level_handlers, &mut LocalPickerStatistic::default())?;
        compaction_input.add_pending_task(task_id, level_handlers);

        Some(create_compaction_task(
            group.compaction_config.as_ref(),
            compaction_input,
            base_level,
            self.task_type(),
        ))
    }

    fn name(&self) -> &'static str {
        "ManualCompactionSelector"
    }

    fn task_type(&self) -> compact_task::TaskType {
        compact_task::TaskType::Manual
    }
}
