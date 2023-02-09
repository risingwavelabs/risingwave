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

use crate::manager::{IdCategory, IdCategoryType, IdGeneratorManager};
use crate::storage::MetaStore;
use crate::MetaResult;

/// A wrapper to distinguish global ID generated by the [`IdGeneratorManager`] and the local ID from
/// the frontend.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub(super) struct GlobalId<const TYPE: IdCategoryType>(u32);

impl<const TYPE: IdCategoryType> GlobalId<TYPE> {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_global_id(&self) -> u32 {
        self.0
    }
}

/// Utility for converting local IDs into pre-allocated global IDs by adding an `offset`.
///
/// This requires the local IDs exactly a permutation of the range `[0, len)`.
#[derive(Clone, Copy, Debug)]
pub(super) struct GlobalIdGen<const TYPE: IdCategoryType> {
    offset: u32,
    len: u32,
}

impl<const TYPE: IdCategoryType> GlobalIdGen<TYPE> {
    /// Pre-allocate a range of IDs with the given `len` and return the generator.
    pub async fn new<S: MetaStore>(id_gen: &IdGeneratorManager<S>, len: u64) -> MetaResult<Self> {
        let offset = id_gen.generate_interval::<TYPE>(len).await?;
        Ok(Self {
            offset: offset as u32,
            len: len as u32,
        })
    }

    /// Convert local id to global id. Panics if `id >= len`.
    pub fn to_global_id(self, local_id: u32) -> GlobalId<TYPE> {
        assert!(
            local_id < self.len,
            "id {} is out of range (len: {})",
            local_id,
            self.len
        );
        GlobalId(local_id + self.offset)
    }

    /// Returns the length of this ID generator.
    pub fn len(&self) -> u32 {
        self.len
    }
}

pub(super) type GlobalFragmentId = GlobalId<{ IdCategory::Fragment }>;
pub(super) type GlobalFragmentIdGen = GlobalIdGen<{ IdCategory::Fragment }>;

pub(super) type GlobalTableIdGen = GlobalIdGen<{ IdCategory::Table }>;

pub(super) type GlobalActorId = GlobalId<{ IdCategory::Actor }>;
pub(super) type GlobalActorIdGen = GlobalIdGen<{ IdCategory::Actor }>;

/// A list of actor IDs.
#[derive(Debug, Clone)]
pub(super) struct GlobalActorIds(pub Vec<GlobalActorId>);

impl GlobalActorIds {
    pub fn as_global_ids(&self) -> Vec<u32> {
        self.0.iter().map(|x| x.as_global_id()).collect()
    }
}
