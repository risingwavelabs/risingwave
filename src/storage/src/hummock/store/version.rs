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

use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;

use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_pb::hummock::{HummockVersion, HummockVersionDelta};

use super::{GetFutureTrait, IterFutureTrait, ReadOptions};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockResult, HummockStateStoreIter};
use crate::table::streaming_table::mem_table::MemTable;

/// Data not committed to Hummock. There are two types of staging data:
/// - Immutable memtable: data that has been written into local state store but not persisted.
/// - Uncommitted SST: data that has been uploaded to persistent storage but not committed to
///   hummock version.
pub enum StagingData {
    ImmMem(Arc<MemTable>),
    Sst(LocalSstableInfo),
}

pub enum VersionUpdate {
    /// We will do in-place update if a `OrderIdx` is provided.
    /// Otherwise, a new staging data entry will be added.
    Staging(StagingData, Option<OrderIdx>),
    CommittedDelta(HummockVersionDelta),
    CommittedSnapshot(HummockVersion),
}

pub type OrderIdx = u32;

/// `OrderIdx` serves two purposes:
/// - Represent ordering of the uncommitted data so that we can do early-stop for point get.
/// - Use as an identifier to uncommitted data so that we can do in-place update.
pub type StagingVersion = BTreeMap<OrderIdx, StagingData>;

// TODO: use a custom data structure to allow in-place update instead of proto
pub type CommittedVersion = HummockVersion;

/// A container of information required for reading from hummock.
#[allow(unused)]
pub struct HummockReadVersion {
    /// Local version for staging data.
    staging: StagingVersion,

    /// Remote version for committed data.
    committed: CommittedVersion,

    sstable_store: SstableStoreRef,
}

#[allow(unused)]
impl HummockReadVersion {
    /// Updates the read version with `VersionUpdate`.
    /// A `OrderIdx` that can uniquely identify the newly added entry will be returned.
    pub fn update(&mut self, info: VersionUpdate) -> HummockResult<OrderIdx> {
        unimplemented!()
    }

    /// Point gets a value from the state store based on the read version.
    fn get(&self, key: &[u8], epoch: u64, read_options: ReadOptions) -> impl GetFutureTrait<'_> {
        async move { unimplemented!() }
    }

    /// Opens and returns an iterator for a given `key_range` based on the read version.
    fn iter<R, B>(
        &self,
        key_range: R,
        epoch: u64,
        read_options: ReadOptions,
    ) -> impl IterFutureTrait<'_, HummockStateStoreIter, R, B>
    where
        R: 'static + Send + RangeBounds<B>,
        B: 'static + Send + AsRef<[u8]>,
    {
        async move { unimplemented!() }
    }
}
