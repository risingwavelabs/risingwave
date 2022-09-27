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

use std::collections::VecDeque;
use std::ops::Bound;

use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch};
use risingwave_pb::hummock::{HummockVersion, HummockVersionDelta, SstableInfo};

// use super::memtable::Memtable;
use super::{GetFutureTrait, ReadOptions};
use crate::hummock::local_version::PinnedVersion;
use crate::hummock::shared_buffer::shared_buffer_batch::{SharedBufferBatch, SharedBufferBatchId};
use crate::hummock::utils::{filter_single_sst, range_overlap};
use crate::hummock::HummockResult;

type ImmutableMemtable = SharedBufferBatch;

// TODO: refine to use use a custom data structure Memtable
type ImmId = SharedBufferBatchId;

/// Data not committed to Hummock. There are two types of staging data:
/// - Immutable memtable: data that has been written into local state store but not persisted.
/// - Uncommitted SST: data that has been uploaded to persistent storage but not committed to
///   hummock version.

#[derive(Clone)]
pub struct StagingSstableInfo {
    sst_info: SstableInfo,
    /// Epochs whose data are included in the Sstable. The newer epoch comes first.
    /// The field must not be empty.
    epochs: Vec<HummockEpoch>,
    compaction_group_id: CompactionGroupId,
    #[allow(dead_code)]
    imm_ids: Vec<ImmId>,
}

#[derive(Clone)]
pub enum StagingData {
    // ImmMem(Arc<Memtable>),
    ImmMem(ImmutableMemtable),
    Sst(StagingSstableInfo),
}

pub enum VersionUpdate {
    /// a new staging data entry will be added.
    Staging(StagingData),
    CommittedDelta(HummockVersionDelta),
    CommittedSnapshot(HummockVersion),
}

#[allow(unused)]
pub struct StagingVersion {
    imm: VecDeque<ImmutableMemtable>,
    sst: VecDeque<StagingSstableInfo>,
}

impl StagingVersion {
    pub fn prune_overlap<'a>(
        &'a self,
        epoch: HummockEpoch,
        compaction_group_id: Option<CompactionGroupId>,
        key_range: &'a (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> (
        impl Iterator<Item = &ImmutableMemtable> + 'a,
        impl Iterator<Item = &SstableInfo> + 'a,
    ) {
        let overlapped_batches = self.imm.iter().filter(move |batch| {
            compaction_group_id
                .map(|group_id| group_id == batch.compaction_group_id())
                .unwrap_or(true)
                && batch.epoch() <= epoch
                && range_overlap(key_range, batch.start_user_key(), batch.end_user_key())
        });
        let overlapped_ssts = self
            .sst
            .iter()
            .filter(move |staging_sst| {
                compaction_group_id
                    .map(|group_id| group_id == staging_sst.compaction_group_id)
                    .unwrap_or(true)
                    && *staging_sst.epochs.last().expect("epochs not empty") <= epoch
                    && filter_single_sst(&staging_sst.sst_info, key_range)
            })
            .map(|staging_sst| &staging_sst.sst_info);
        (overlapped_batches, overlapped_ssts)
    }
}

// TODO: use a custom data structure to allow in-place update instead of proto
pub type CommittedVersion = PinnedVersion;

/// A container of information required for reading from hummock.
#[allow(unused)]
pub struct HummockReadVersion {
    /// Local version for staging data.
    staging: StagingVersion,

    /// Remote version for committed data.
    committed: CommittedVersion,
}

#[allow(unused)]
impl HummockReadVersion {
    /// Updates the read version with `VersionUpdate`.
    /// A `OrderIdx` that can uniquely identify the newly added entry will be returned.
    pub fn update(&mut self, info: VersionUpdate) -> HummockResult<()> {
        unimplemented!()
    }

    /// Point gets a value from the state store based on the read version.
    pub fn get(
        &self,
        key: &[u8],
        epoch: HummockEpoch,
        read_options: ReadOptions,
    ) -> impl GetFutureTrait<'_> {
        async move { unimplemented!() }
    }

    pub fn staging(&self) -> &StagingVersion {
        &self.staging
    }

    pub fn committed(&self) -> &CommittedVersion {
        &self.committed
    }
}
