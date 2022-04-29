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

mod group_compaction_picker;

use std::fmt::{Display, Formatter};

use risingwave_pb::hummock::Level;

use crate::hummock::compaction::compaction_picker::CompactionPicker;
use crate::hummock::compaction::tier_compaction_picker::TierCompactionPicker;
use crate::hummock::compaction::SearchResult;
use crate::hummock::compaction_group::group_compaction_picker::GroupCompactionPicker;
use crate::hummock::level_handler::LevelHandler;

#[derive(Debug, Clone)]
struct Prefix([u8; 4]);

#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct CompactionGroupId(u64);

pub const DEFAULT_COMPACTION_GROUP_ID: CompactionGroupId = CompactionGroupId(0);

pub enum CompactionPickerImpl {
    Group(GroupCompactionPicker),
    Tiered(TierCompactionPicker),
}

pub struct CompactionGroup {
    group_id: CompactionGroupId,
    #[allow(dead_code)]
    prefixes: Vec<Prefix>,
    compaction_picker: CompactionPickerImpl,
    /// If `is_scheduled`, no need to notify scheduler again. Scheduler will reschedule it if
    /// necessary, e.g. more compaction task available.
    is_scheduled: bool,
}

impl CompactionGroup {
    pub fn group_id(&self) -> &CompactionGroupId {
        &self.group_id
    }

    pub fn compaction_picker(&self) -> &CompactionPickerImpl {
        &self.compaction_picker
    }

    pub fn is_scheduled(&self) -> bool {
        self.is_scheduled
    }

    pub fn set_is_scheduled(&mut self, is_scheduled: bool) {
        self.is_scheduled = is_scheduled;
    }
}

impl From<u64> for CompactionGroupId {
    fn from(cg_id: u64) -> Self {
        Self(cg_id)
    }
}

impl From<&CompactionGroupId> for u64 {
    fn from(cg_id: &CompactionGroupId) -> Self {
        cg_id.0
    }
}

impl Display for CompactionGroupId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl CompactionGroup {
    pub fn new(cg_id: CompactionGroupId, compaction_picker: CompactionPickerImpl) -> Self {
        CompactionGroup {
            group_id: cg_id,
            prefixes: vec![],
            compaction_picker,
            is_scheduled: false,
        }
    }
}

impl CompactionPicker for CompactionPickerImpl {
    fn need_compaction(&self, levels: Vec<Level>) -> bool {
        match self {
            CompactionPickerImpl::Group(picker) => picker.need_compaction(levels),
            CompactionPickerImpl::Tiered(picker) => picker.need_compaction(levels),
        }
    }

    fn try_pick_compaction(
        &self,
        levels: Vec<Level>,
        level_handlers: &mut [LevelHandler],
        compact_task_id: u64,
    ) -> Option<SearchResult> {
        match self {
            CompactionPickerImpl::Group(picker) => {
                picker.try_pick_compaction(levels, level_handlers, compact_task_id)
            }
            CompactionPickerImpl::Tiered(picker) => {
                picker.pick_compaction(levels, level_handlers, compact_task_id)
            }
        }
    }
}
