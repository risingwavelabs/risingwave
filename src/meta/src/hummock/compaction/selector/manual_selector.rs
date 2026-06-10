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
    /// Output level. Defaults to the implicit manual compaction target if unset.
    pub target_level: Option<usize>,
    /// When true, skip manual compaction if any task is pending in the compaction group.
    pub exclusive: bool,
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
            target_level: None,
            exclusive: false,
        }
    }
}

pub struct ManualCompactionSelector {
    option: ManualCompactionOption,
    blocked_by_pending: bool,
    validation_error: Option<String>,
}

impl ManualCompactionSelector {
    pub fn new(option: ManualCompactionOption) -> Self {
        Self {
            option,
            blocked_by_pending: false,
            validation_error: None,
        }
    }

    pub fn blocked_by_pending(&self) -> bool {
        self.blocked_by_pending
    }

    pub fn validation_error(&self) -> Option<&str> {
        self.validation_error.as_deref()
    }
}

impl CompactionSelector for ManualCompactionSelector {
    fn pick_compaction(
        &mut self,
        _task_id: HummockCompactionTaskId,
        context: CompactionSelectorContext<'_>,
    ) -> Option<CompactionTask> {
        let CompactionSelectorContext {
            group,
            levels,
            level_handlers,
            developer_config,
            ..
        } = context;
        self.blocked_by_pending = false;
        self.validation_error = None;

        let dynamic_level_core =
            DynamicLevelSelectorCore::new(group.compaction_config.clone(), developer_config);
        let overlap_strategy = create_overlap_strategy(group.compaction_config.compaction_mode());
        let ctx = dynamic_level_core.calculate_level_base_size(levels);
        let (mut picker, base_level) = {
            let max_level = group.compaction_config.max_level as usize;
            if self.option.level > max_level {
                self.validation_error = Some(format!(
                    "level {} exceeds max_level {}",
                    self.option.level, max_level
                ));
                return None;
            }
            let target_level = self.option.target_level.unwrap_or_else(|| {
                if self.option.level == 0 {
                    if self.option.sst_ids.is_empty() {
                        ctx.base_level
                    } else {
                        0
                    }
                } else if self.option.level == group.compaction_config.max_level as usize {
                    self.option.level
                } else {
                    self.option.level + 1
                }
            });
            if target_level > max_level {
                self.validation_error = Some(format!(
                    "target_level {} exceeds max_level {}",
                    target_level, max_level
                ));
                return None;
            }
            if self.option.level == 0 {
                let expected_target_level = if self.option.sst_ids.is_empty() {
                    ctx.base_level
                } else {
                    0
                };
                if target_level != expected_target_level {
                    self.validation_error = Some(format!(
                        "target_level for L0 must be {}, got {}",
                        expected_target_level, target_level
                    ));
                    return None;
                }
            }
            if self.option.level > 0
                && target_level != self.option.level
                && target_level != self.option.level + 1
            {
                self.validation_error = Some(format!(
                    "target_level for L{} must be {} or {}, got {}",
                    self.option.level,
                    self.option.level,
                    self.option.level + 1,
                    target_level
                ));
                return None;
            }
            if self.option.level > 0 && self.option.level < ctx.base_level {
                return None;
            }
            (
                ManualCompactionPicker::new(overlap_strategy, self.option.clone(), target_level),
                ctx.base_level,
            )
        };

        let compaction_input =
            picker.pick_compaction(levels, level_handlers, &mut LocalPickerStatistic::default());

        if compaction_input.is_none()
            && self.option.exclusive
            && level_handlers
                .iter()
                .any(|level_handler| level_handler.pending_file_count() > 0)
        {
            self.blocked_by_pending = true;
        }

        let compaction_input = compaction_input?;

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
