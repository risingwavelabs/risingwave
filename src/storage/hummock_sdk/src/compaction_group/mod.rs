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

use std::borrow::Borrow;

use crate::CompactionGroupId;

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq)]
pub struct Prefix([u8; 4]);

impl From<[u8; 4]> for Prefix {
    fn from(u: [u8; 4]) -> Self {
        Self(u)
    }
}

impl From<u32> for Prefix {
    fn from(u: u32) -> Self {
        let u: [u8; 4] = u.to_be_bytes();
        u.into()
    }
}

impl From<Prefix> for Vec<u8> {
    fn from(p: Prefix) -> Self {
        p.borrow().into()
    }
}

impl From<&Prefix> for Vec<u8> {
    fn from(p: &Prefix) -> Self {
        p.0.to_vec()
    }
}

/// A compaction task's `StaticCompactionGroupId` indicates the compaction group that all its input
/// SSTs belong to.
pub enum StaticCompactionGroupId {
    /// All shared buffer local compaction task goes to here.
    SharedBuffer = 1,
    /// All unregistered state goes to here.
    StateDefault = 2,
    // TODO: all registered MV goes to here.
    MaterializedView = 3,
}

impl From<StaticCompactionGroupId> for CompactionGroupId {
    fn from(cg: StaticCompactionGroupId) -> Self {
        cg as CompactionGroupId
    }
}
