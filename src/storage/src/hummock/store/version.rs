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
use risingwave_pb::hummock::{HummockVersionDelta, SstableInfo};

use super::memtable::{ImmId, ImmutableMemtable};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::utils::{filter_single_sst, range_overlap};

// TODO: use a custom data structure to allow in-place update instead of proto
// pub type CommittedVersion = HummockVersion;

pub type CommittedVersion = PinnedVersion;

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
    CommittedSnapshot(CommittedVersion),
}

pub struct StagingVersion {
    pub imm: VecDeque<ImmutableMemtable>,
    pub sst: VecDeque<StagingSstableInfo>,
}

impl StagingVersion {
    pub fn prune_overlap<'a>(
        &'a self,
        epoch: HummockEpoch,
        compaction_group_id: CompactionGroupId,
        key_range: &'a (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> (
        impl Iterator<Item = &ImmutableMemtable> + 'a,
        impl Iterator<Item = &SstableInfo> + 'a,
    ) {
        let overlapped_imms = self.imm.iter().filter(move |imm| {
            compaction_group_id == imm.compaction_group_id()
                && imm.epoch() <= epoch
                && range_overlap(key_range, imm.start_user_key(), imm.end_user_key())
        });

        let overlapped_ssts = self
            .sst
            .iter()
            .filter(move |staging_sst| {
                compaction_group_id == staging_sst.compaction_group_id
                    && *staging_sst.epochs.last().expect("epochs not empty") <= epoch
                    && filter_single_sst(&staging_sst.sst_info, key_range)
            })
            .map(|staging_sst| &staging_sst.sst_info);
        (overlapped_imms, overlapped_ssts)
    }
}

/// A container of information required for reading from hummock.
pub struct HummockReadVersion {
    /// Local version for staging data.
    staging: StagingVersion,

    /// Remote version for committed data.
    committed: CommittedVersion,
}

impl HummockReadVersion {
    pub fn new(committed_version: CommittedVersion) -> Self {
        // before build `HummockReadVersion`, we need to get the a initial version which obtained
        // from meta. want this initialization after version is initialized (now with
        // notification), so add a assert condition to guarantee correct initialization order
        assert!(committed_version.is_valid());
        Self {
            staging: StagingVersion {
                imm: VecDeque::default(),
                sst: VecDeque::default(),
            },

            committed: committed_version,
        }
    }

    /// Updates the read version with `VersionUpdate`.
    /// A `OrderIdx` that can uniquely identify the newly added entry will be returned.
    pub fn update(&mut self, info: VersionUpdate) {
        match info {
            VersionUpdate::Staging(staging) => match staging {
                StagingData::ImmMem(imm) => self.staging.imm.push_front(imm),
                StagingData::Sst(staging_sst) => {
                    assert!(self.staging.imm.len() >= staging_sst.imm_ids.len());
                    assert!(staging_sst
                        .imm_ids
                        .is_sorted_by(|batch_id1, batch_id2| batch_id2.partial_cmp(batch_id1)));
                    for clear_imm_id in staging_sst.imm_ids.iter().rev() {
                        let item = self.staging.imm.back().unwrap();
                        assert_eq!(*clear_imm_id, item.batch_id());
                        self.staging.imm.pop_back();
                    }

                    self.staging.sst.push_front(staging_sst);
                }
            },

            VersionUpdate::CommittedDelta(_) => {
                unimplemented!()
            }

            VersionUpdate::CommittedSnapshot(committed_version) => {
                let max_committed_epoch = committed_version.max_committed_epoch();
                self.committed = committed_version;

                {
                    // TODO: remove it when support update staging local_sst
                    self.staging
                        .imm
                        .retain(|imm| imm.epoch() > max_committed_epoch);
                    self.staging.sst.retain(|sst| {
                        sst.epochs.first().expect("epochs not empty") > &max_committed_epoch
                    });

                    // check epochs.last() > MCE
                    assert!(self.staging.sst.iter().all(|sst| {
                        sst.epochs.last().expect("epochs not empty") > &max_committed_epoch
                    }));
                }
            }
        }
    }

    pub fn staging(&self) -> &StagingVersion {
        &self.staging
    }

    pub fn committed(&self) -> &CommittedVersion {
        &self.committed
    }
}

impl StagingSstableInfo {
    pub fn new(
        sst_info: SstableInfo,
        epochs: Vec<HummockEpoch>,
        compaction_group_id: CompactionGroupId,
        imm_ids: Vec<ImmId>,
    ) -> Self {
        // the epochs are sorted from higher epoch to lower epoch
        assert!(epochs.is_sorted_by(|epoch1, epoch2| epoch2.partial_cmp(epoch1)));
        Self {
            sst_info,
            epochs,
            compaction_group_id,
            imm_ids,
        }
    }
}
