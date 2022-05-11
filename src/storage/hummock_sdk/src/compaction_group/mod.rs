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

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq)]
pub struct CompactionGroupId(u64);

impl From<u64> for CompactionGroupId {
    fn from(u: u64) -> Self {
        Self(u)
    }
}

impl From<CompactionGroupId> for u64 {
    fn from(c: CompactionGroupId) -> Self {
        c.0
    }
}

#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct Prefix(u32);

impl From<u32> for Prefix {
    fn from(u: u32) -> Self {
        Self(u)
    }
}

impl From<Prefix> for u32 {
    fn from(p: Prefix) -> Self {
        p.0
    }
}

#[allow(dead_code)]
struct CompactionGroup {
    group_id: CompactionGroupId,
    prefixes: Vec<Prefix>,
    /// If `is_scheduled`, no need to notify scheduler again. Scheduler will reschedule it if
    /// necessary, e.g. more compaction task available.
    is_scheduled: bool,
}
