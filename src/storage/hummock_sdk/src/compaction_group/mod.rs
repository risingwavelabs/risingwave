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

pub mod hummock_version_ext;

use parse_display::Display;

use crate::CompactionGroupId;

pub type StateTableId = u32;

/// A compaction task's `StaticCompactionGroupId` indicates the compaction group that all its input
/// SSTs belong to.
#[derive(FromPrimitive, Display)]
pub enum StaticCompactionGroupId {
    /// Create a new compaction group.
    NewCompactionGroup = 0,
    /// All shared buffer local compaction task goes to here. Meta service will never see this
    /// value. Note that currently we've restricted the compaction task's input by `via
    /// compact_shared_buffer_by_compaction_group`
    SharedBuffer = 1,
    /// All states goes to here by default.
    StateDefault = 2,
    /// All MVs goes to here.
    MaterializedView = 3,
    /// Larger than any `StaticCompactionGroupId`.
    End = 4,
}

impl From<StaticCompactionGroupId> for CompactionGroupId {
    fn from(cg: StaticCompactionGroupId) -> Self {
        cg as CompactionGroupId
    }
}
