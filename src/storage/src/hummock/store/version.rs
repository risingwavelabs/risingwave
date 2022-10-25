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

use std::collections::{HashSet, VecDeque};
use std::ops::Bound;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::{HummockVersionDelta, SstableInfo};

use super::memtable::{ImmId, ImmutableMemtable};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::utils::{check_subset_preserve_order, filter_single_sst, range_overlap};

// TODO: use a custom data structure to allow in-place update instead of proto
// pub type CommittedVersion = HummockVersion;

pub type CommittedVersion = PinnedVersion;

/// Data not committed to Hummock. There are two types of staging data:
/// - Immutable memtable: data that has been written into local state store but not persisted.
/// - Uncommitted SST: data that has been uploaded to persistent storage but not committed to
///   hummock version.

#[derive(Clone, Debug)]
pub struct StagingSstableInfo {
    sstable_infos: Vec<SstableInfo>,
    /// Epochs whose data are included in the Sstable. The newer epoch comes first.
    /// The field must not be empty.
    epochs: Vec<HummockEpoch>,
    #[allow(dead_code)]
    imm_ids: Vec<ImmId>,
}

impl StagingSstableInfo {
    pub fn new(
        sstable_infos: Vec<SstableInfo>,
        epochs: Vec<HummockEpoch>,
        imm_ids: Vec<ImmId>,
    ) -> Self {
        // the epochs are sorted from higher epoch to lower epoch
        assert!(epochs.is_sorted_by(|epoch1, epoch2| epoch2.partial_cmp(epoch1)));
        Self {
            sstable_infos,
            epochs,
            imm_ids,
        }
    }

    pub fn sstable_infos(&self) -> &Vec<SstableInfo> {
        &self.sstable_infos
    }
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
    // newer data comes first
    // Note: Currently, building imm and writing to staging version is not atomic, and therefore
    // imm of smaller batch id may be added later than one with greater batch id
    pub imm: VecDeque<ImmutableMemtable>,
    // newer data comes first
    pub sst: VecDeque<StagingSstableInfo>,
}

impl StagingVersion {
    pub fn prune_overlap<'a>(
        &'a self,
        epoch: HummockEpoch,
        table_id: TableId,
        key_range: &'a (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> (
        impl Iterator<Item = &ImmutableMemtable> + 'a,
        impl Iterator<Item = &SstableInfo> + 'a,
    ) {
        let overlapped_imms = self.imm.iter().filter(move |imm| {
            imm.epoch() <= epoch
                && range_overlap(key_range, imm.start_user_key(), imm.end_user_key())
        });

        let overlapped_ssts = self
            .sst
            .iter()
            .filter(move |staging_sst| {
                *staging_sst.epochs.last().expect("epochs not empty") <= epoch
            })
            .flat_map(move |staging_sst| {
                staging_sst
                    .sstable_infos
                    .iter()
                    .filter(move |sstable| filter_single_sst(sstable, table_id, key_range))
            });
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
                // TODO: add a check to ensure that the added batch id of added imm is greater than
                // the batch id of imm at the front
                StagingData::ImmMem(imm) => self.staging.imm.push_front(imm),
                StagingData::Sst(staging_sst) => {
                    // TODO: enable this stricter check after each streaming table owns a read
                    // version. assert!(self.staging.imm.len() >=
                    // staging_sst.imm_ids.len()); assert!(staging_sst
                    //     .imm_ids
                    //     .is_sorted_by(|batch_id1, batch_id2| batch_id2.partial_cmp(batch_id1)));
                    // assert!(
                    //     check_subset_preserve_order(
                    //         staging_sst.imm_ids.iter().cloned(),
                    //         self.staging.imm.iter().map(|imm| imm.batch_id()),
                    //     ),
                    //     "the imm id of staging sstable info not preserve the imm order. staging
                    // sst imm ids: {:?}, current imm ids: {:?}",
                    //     staging_sst.imm_ids.iter().collect_vec(),
                    //     self.staging.imm.iter().map(|imm| imm.batch_id()).collect_vec()
                    // );
                    // for clear_imm_id in staging_sst.imm_ids.iter().rev() {
                    //     let item = self.staging.imm.back().unwrap();
                    //     assert_eq!(*clear_imm_id, item.batch_id());
                    //     self.staging.imm.pop_back();
                    // }

                    debug_assert!(
                        check_subset_preserve_order(
                            staging_sst.imm_ids.iter().cloned().sorted(),
                            self.staging.imm.iter().map(|imm| imm.batch_id()).sorted()
                        ),
                        "the set of imm ids in the staging_sst {:?} is not a subset of current staging imms {:?}",
                        staging_sst.imm_ids.iter().cloned().sorted().collect_vec(),
                        self.staging.imm.iter().map(|imm| imm.batch_id()).sorted().collect_vec(),
                    );

                    let imm_id_set: HashSet<ImmId> =
                        HashSet::from_iter(staging_sst.imm_ids.iter().cloned());
                    self.staging
                        .imm
                        .retain(|imm| !imm_id_set.contains(&imm.batch_id()));

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
