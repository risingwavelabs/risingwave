// Copyright 2024 RisingWave Labs
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

use crate::controller::id::{
    IdCategory, IdCategoryType, IdGeneratorManager as SqlIdGeneratorManager,
};

/// A wrapper to distinguish global ID generated by the [`SqlIdGeneratorManager`] and the local ID from
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

impl<const TYPE: IdCategoryType> From<u32> for GlobalId<TYPE> {
    fn from(id: u32) -> Self {
        Self(id)
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
    pub fn new(id_gen: &SqlIdGeneratorManager, len: u64) -> Self {
        let offset = id_gen.generate_interval::<TYPE>(len);
        Self {
            offset: offset as u32,
            len: len as u32,
        }
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

/// Extension for converting a slice of [`GlobalActorId`] to a vector of global IDs.
#[easy_ext::ext(GlobalFragmentIdsExt)]
pub(super) impl<A: AsRef<[GlobalActorId]>> A {
    fn as_global_ids(&self) -> Vec<u32> {
        self.as_ref().iter().map(|x| x.as_global_id()).collect()
    }
}
