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

use std::cmp::Ordering;
use std::collections::vec_deque::VecDeque;
use std::collections::HashSet;
use std::iter::once;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use minitrace::future::FutureExt;
use minitrace::Span;
use parking_lot::RwLock;
use risingwave_common::buffer::{cache_may_stale, Bitmap};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{
    bound_table_key_range, FullKey, TableKey, TableKeyRange, UserKey,
};
use risingwave_hummock_sdk::key_range::KeyRangeCommon;
use risingwave_hummock_sdk::{
    HummockEpoch, HummockSstableObjectId, HummockSubLevelId, LocalSstableInfo,
};
use risingwave_pb::hummock::{HummockVersionDelta, Level, LevelType, SstableInfo};
use sync_point::sync_point;

use super::memtable::{ImmId, ImmutableMemtable};
use super::state_store::StagingDataIterator;
use crate::error::StorageResult;
use crate::hummock::iterator::{
    ConcatIterator, ForwardMergeRangeIterator, HummockIteratorUnion, OrderedMergeIteratorInner,
    UnorderedMergeIteratorInner, UserIterator,
};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::store::state_store::HummockStorageIterator;
use crate::hummock::utils::{
    check_subset_preserve_order, filter_single_sst, prune_nonoverlapping_ssts,
    prune_overlapping_ssts, range_overlap, search_sst_idx,
};
use crate::hummock::{
    get_from_batch, get_from_sstable_info, hit_sstable_bloom_filter, Sstable,
    SstableDeleteRangeIterator, SstableIterator,
};
use crate::monitor::{
    GetLocalMetricsGuard, HummockStateStoreMetrics, MayExistLocalMetricsGuard, StoreLocalStatistic,
};
use crate::store::{gen_min_epoch, ReadOptions, StateStoreIterExt, StreamTypeOfIter};

// TODO: use a custom data structure to allow in-place update instead of proto
// pub type CommittedVersion = HummockVersion;

pub type CommittedVersion = PinnedVersion;
pub type ReadVersionTuple = (
    Vec<ImmutableMemtable>,
    Vec<SstableInfo>,
    CommittedVersion,
    Option<Arc<PrunedVersion>>,
);

/// Data not committed to Hummock. There are two types of staging data:
/// - Immutable memtable: data that has been written into local state store but not persisted.
/// - Uncommitted SST: data that has been uploaded to persistent storage but not committed to
///   hummock version.

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StagingSstableInfo {
    // newer data comes first
    sstable_infos: Vec<LocalSstableInfo>,
    /// Epochs whose data are included in the Sstable. The newer epoch comes first.
    /// The field must not be empty.
    epochs: Vec<HummockEpoch>,
    imm_ids: Vec<ImmId>,
    imm_size: usize,
}

impl StagingSstableInfo {
    pub fn new(
        sstable_infos: Vec<LocalSstableInfo>,
        epochs: Vec<HummockEpoch>,
        imm_ids: Vec<ImmId>,
        imm_size: usize,
    ) -> Self {
        // the epochs are sorted from higher epoch to lower epoch
        assert!(epochs.is_sorted_by(|epoch1, epoch2| epoch2.partial_cmp(epoch1)));
        Self {
            sstable_infos,
            epochs,
            imm_ids,
            imm_size,
        }
    }

    pub fn sstable_infos(&self) -> &Vec<LocalSstableInfo> {
        &self.sstable_infos
    }

    pub fn imm_size(&self) -> usize {
        self.imm_size
    }

    pub fn epochs(&self) -> &Vec<HummockEpoch> {
        &self.epochs
    }

    pub fn imm_ids(&self) -> &Vec<ImmId> {
        &self.imm_ids
    }
}

#[derive(Clone)]
pub enum StagingData {
    // ImmMem(Arc<Memtable>),
    ImmMem(ImmutableMemtable),
    MergedImmMem(ImmutableMemtable),
    Sst(StagingSstableInfo),
}

pub enum VersionUpdate {
    /// a new staging data entry will be added.
    Staging(StagingData),
    CommittedDelta(HummockVersionDelta),
    CommittedSnapshot(CommittedVersion),
}

#[derive(Clone)]
pub struct StagingVersion {
    // newer data comes first
    // Note: Currently, building imm and writing to staging version is not atomic, and therefore
    // imm of smaller batch id may be added later than one with greater batch id
    pub imm: VecDeque<ImmutableMemtable>,

    // Separate queue for merged imm to ease the management of imm and merged imm.
    // Newer merged imm comes first
    pub merged_imm: VecDeque<ImmutableMemtable>,

    // newer data comes first
    pub sst: VecDeque<StagingSstableInfo>,
}

impl StagingVersion {
    /// Get the overlapping `imm`s and `sst`s that overlap respectively with `table_key_range` and
    /// the user key range derived from `table_id`, `epoch` and `table_key_range`.
    pub fn prune_overlap<'a>(
        &'a self,
        min_epoch_exclusive: HummockEpoch,
        max_epoch_inclusive: HummockEpoch,
        table_id: TableId,
        table_key_range: &'a TableKeyRange,
    ) -> (
        impl Iterator<Item = &ImmutableMemtable> + 'a,
        impl Iterator<Item = &SstableInfo> + 'a,
    ) {
        let (ref left, ref right) = table_key_range;
        let left = left.as_ref().map(|key| TableKey(key.0.as_ref()));
        let right = right.as_ref().map(|key| TableKey(key.0.as_ref()));
        let overlapped_imms = self
            .imm
            .iter()
            .chain(self.merged_imm.iter())
            .filter(move |imm| {
                // retain imm which is overlapped with (min_epoch_exclusive, max_epoch_inclusive]
                imm.min_epoch() <= max_epoch_inclusive
                    && imm.table_id == table_id
                    && imm.min_epoch() > min_epoch_exclusive
                    && range_overlap(
                        &(left, right),
                        &imm.start_table_key(),
                        imm.end_table_key().as_ref(),
                    )
            });

        // TODO: Remove duplicate sst based on sst id
        let overlapped_ssts = self
            .sst
            .iter()
            .filter(move |staging_sst| {
                let sst_min_epoch = *staging_sst.epochs.first().expect("epochs not empty");
                let sst_max_epoch = *staging_sst.epochs.last().expect("epochs not empty");
                assert!(
                    sst_max_epoch <= min_epoch_exclusive || sst_min_epoch > min_epoch_exclusive
                );
                sst_max_epoch <= max_epoch_inclusive && sst_min_epoch > min_epoch_exclusive
            })
            .flat_map(move |staging_sst| {
                // TODO: sstable info should be concat-able after each streaming table owns a read
                // version. May use concat sstable iter instead in some cases.
                staging_sst
                    .sstable_infos
                    .iter()
                    .map(|sstable| &sstable.sst_info)
                    .filter(move |sstable| filter_single_sst(sstable, table_id, table_key_range))
            });
        (overlapped_imms, overlapped_ssts)
    }
}

/// `(SST positions, SST object ids)`
/// We can track ssts in pruned version by `SST object id` because there won't be two SSTs which
/// both include the `table_id` of this read version and share the same `SST object id`.
type PrunedVersionLevel = (Vec<usize>, Vec<HummockSstableObjectId>);
pub struct PrunedVersion {
    table_id: TableId,
    /// index is only for data with `epoch >= scope`
    scope: HummockEpoch,
    /// mapping from table view position to `(sub_level_id, (SST positions, SST object ids))`
    levels: Vec<(HummockSubLevelId, PrunedVersionLevel)>,
}

impl PrunedVersion {
    fn new(table_id: TableId, max_committed_epoch: HummockEpoch) -> Self {
        Self {
            table_id,
            scope: max_committed_epoch + 1,
            levels: vec![],
        }
    }

    fn scope(&self) -> HummockEpoch {
        self.scope
    }

    fn get_indices(
        &self,
        table_view_position: usize,
        sub_level_id: HummockSubLevelId,
    ) -> Option<&Vec<usize>> {
        if sub_level_id >= self.scope {
            Some(&self.levels[table_view_position].1 .0)
        } else {
            None
        }
    }

    /// Construct index for one sub level in `HummockVersion` by local staging SST object ids.
    fn construct_index(
        &self,
        sub_level: &Level,
        object_ids: impl Iterator<Item = HummockSstableObjectId>,
    ) -> PrunedVersionLevel {
        // hexff means '0xff'
        let mut hexff_indices = object_ids
            .map(|object_id| (usize::MAX, object_id))
            .collect_vec();
        hexff_indices.sort_by_key(|(_usize_max, object_id)| *object_id);
        for (index, table_info) in sub_level.get_table_infos().iter().enumerate() {
            if let Ok(position) = hexff_indices.binary_search_by_key(&table_info.object_id, |(_, object_id)| *object_id) && table_info.get_table_ids().binary_search(&self.table_id.table_id).is_ok() {
                hexff_indices[position].0 = index;
            }
        }
        hexff_indices.retain(|(index, _)| *index != usize::MAX);
        hexff_indices.sort();
        hexff_indices.into_iter().unzip()
    }

    fn apply(
        &self,
        committed_version: &CommittedVersion,
        mut cleaned_staging_ssts: Vec<(HummockEpoch, Vec<HummockSstableObjectId>)>,
    ) -> Self {
        cleaned_staging_ssts.sort_by_key(|(epoch, _)| *epoch);

        let committed_l0_sub_levels = committed_version.l0_sublevels(self.table_id).collect_vec();
        // Fresh current `self`. (Because some sub levels in `self` might have been compacted.)
        let mut sub_level_asc_iter = committed_l0_sub_levels.iter().rev().peekable();
        let mut new_levels = vec![];
        for (old_sub_level_id, (_, old_object_ids)) in self
            .levels
            .iter()
            .rev()
            .filter(|(_, (indices, _))| !indices.is_empty())
        {
            while let Some(sub_level) = sub_level_asc_iter.peek() && sub_level.sub_level_id < *old_sub_level_id {
                sub_level_asc_iter.next();
            }
            match sub_level_asc_iter.peek() {
                Some(sub_level) => {
                    if sub_level.sub_level_id == *old_sub_level_id
                        && sub_level.level_type() != LevelType::Nonoverlapping
                    {
                        let new_indices =
                            self.construct_index(sub_level, old_object_ids.iter().copied());
                        new_levels.push((*old_sub_level_id, new_indices));

                        sub_level_asc_iter.next();
                    }
                }
                None => break,
            }
        }

        // Add new sub levels in the latest `HummockVersion` into `self`.
        let mut levels_to_extend = vec![];
        for (idx, sub_level) in committed_l0_sub_levels.iter().enumerate() {
            while let Some((epoch, _)) = cleaned_staging_ssts.last() && epoch > &sub_level.sub_level_id {
                cleaned_staging_ssts.pop();
            }
            if cleaned_staging_ssts.is_empty() {
                break;
            }

            let next_epoch = committed_l0_sub_levels
                .get(idx + 1)
                .map(|sub_level| sub_level.get_sub_level_id());
            let left_len = if let Some(next_epoch) = next_epoch {
                let mut left_len = cleaned_staging_ssts.len();
                while left_len > 0 && cleaned_staging_ssts[left_len - 1].0 > next_epoch {
                    left_len -= 1;
                }
                left_len
            } else {
                0
            };
            let candidate_ssts = cleaned_staging_ssts.split_off(left_len);

            if sub_level.level_type() != LevelType::Nonoverlapping {
                let committed_indices = self.construct_index(
                    sub_level,
                    candidate_ssts
                        .into_iter()
                        .flat_map(|(_, object_ids)| object_ids),
                );
                levels_to_extend.push((sub_level.get_sub_level_id(), committed_indices));
            }
        }

        new_levels.extend(levels_to_extend.into_iter().rev());

        // Build result.
        let mut ret = vec![];
        for sub_level in committed_l0_sub_levels {
            let sub_level_id = sub_level.get_sub_level_id();
            if sub_level_id < self.scope {
                break;
            }
            ret.push(if let Some((new_sub_level_id, _)) = new_levels.last() && *new_sub_level_id == sub_level_id {
                new_levels.pop().unwrap()
            } else {
                (sub_level_id, (vec![], vec![]))
            });
        }

        Self {
            table_id: self.table_id,
            scope: self.scope,
            levels: ret,
        }
    }
}

/// A container of information required for reading from hummock.
pub struct HummockReadVersion {
    table_id: TableId,

    is_singleton: bool,
    vnodes: Arc<Bitmap>,

    /// Local version for staging data.
    staging: StagingVersion,

    /// Remote version for committed data.
    committed: CommittedVersion,

    committed_index: Option<Arc<PrunedVersion>>,
}

impl HummockReadVersion {
    pub fn new(
        table_id: TableId,
        is_singleton: bool,
        vnodes: Arc<Bitmap>,
        committed_version: CommittedVersion,
    ) -> Self {
        // before build `HummockReadVersion`, we need to get the a initial version which obtained
        // from meta. want this initialization after version is initialized (now with
        // notification), so add a assert condition to guarantee correct initialization order
        assert!(committed_version.is_valid());
        Self {
            table_id,

            is_singleton,
            vnodes,

            staging: StagingVersion {
                imm: VecDeque::default(),
                merged_imm: Default::default(),
                sst: VecDeque::default(),
            },

            committed: committed_version,

            committed_index: None,
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clone_for_test(&self) -> Self {
        Self {
            table_id: self.table_id,
            is_singleton: self.is_singleton,
            vnodes: self.vnodes.clone(),
            staging: self.staging().clone(),
            committed: self.committed().clone(),
            committed_index: None,
        }
    }

    /// Updates the read version with `VersionUpdate`.
    /// There will be three data types to be processed
    /// `VersionUpdate::Staging`
    ///     - `StagingData::ImmMem` -> Insert into memory's `staging_imm`
    ///     - `StagingData::Sst` -> Update the sst to memory's `staging_sst` and remove the
    ///       corresponding `staging_imms` according to the `batch_id`
    /// `VersionUpdate::CommittedDelta` -> Unimplemented yet
    /// `VersionUpdate::CommittedSnapshot` -> Update `committed_version` , and clean up related
    /// `staging_sst` and `staging_imm` in memory according to epoch
    pub fn update(&mut self, info: VersionUpdate) {
        match info {
            VersionUpdate::Staging(staging) => match staging {
                // TODO: add a check to ensure that the added batch id of added imm is greater than
                // the batch id of imm at the front
                StagingData::ImmMem(imm) => {
                    if let Some(item) = self.staging.imm.front() {
                        // check batch_id order from newest to old
                        debug_assert!(item.batch_id() < imm.batch_id());
                    }

                    self.staging.imm.push_front(imm)
                }
                StagingData::MergedImmMem(merged_imm) => {
                    if let Some(item) = self.staging.merged_imm.front() {
                        // check batch_id order from newest to old
                        debug_assert!(item.batch_id() < merged_imm.batch_id());
                    }
                    self.add_merged_imm(merged_imm);
                }
                StagingData::Sst(mut staging_sst) => {
                    // The following properties must be ensured:
                    // 1) self.staging.imm is sorted by imm id descendingly
                    // 2) staging_sst.imm_ids preserves the imm id partial
                    //    ordering of the participating read version imms. Example:
                    //    If staging_sst contains two read versions r1: [i1, i3] and  r2: [i2, i4],
                    //    then [i2, i1, i3, i4] is valid while [i3, i1, i2, i4] is invalid.
                    // 3) The intersection between staging_sst.imm_ids and self.staging.imm
                    //    are always the suffix of self.staging.imm

                    // Check 1)
                    debug_assert!(self
                        .staging
                        .imm
                        .iter()
                        .chain(self.staging.merged_imm.iter())
                        .rev()
                        .is_sorted_by_key(|imm| imm.batch_id()));

                    // Calculate intersection
                    let staging_imm_ids_from_imms: HashSet<u64> = self
                        .staging
                        .imm
                        .iter()
                        .chain(self.staging.merged_imm.iter())
                        .map(|imm| imm.batch_id())
                        .collect();

                    // intersected batch_id order from oldest to newest
                    let intersect_imm_ids = staging_sst
                        .imm_ids
                        .iter()
                        .rev()
                        .copied()
                        .filter(|id| staging_imm_ids_from_imms.contains(id))
                        .collect_vec();

                    if !intersect_imm_ids.is_empty() {
                        // Check 2)
                        debug_assert!(check_subset_preserve_order(
                            intersect_imm_ids.iter().copied(),
                            self.staging
                                .imm
                                .iter()
                                .chain(self.staging.merged_imm.iter())
                                .map(|imm| imm.batch_id())
                                .rev(),
                        ));

                        // Check 3) and replace imms with a staging sst
                        for imm_id in &intersect_imm_ids {
                            if let Some(merged_imm) = self.staging.merged_imm.back() {
                                if *imm_id == merged_imm.batch_id() {
                                    self.staging.merged_imm.pop_back();
                                }
                            } else if let Some(imm) = self.staging.imm.back() {
                                if *imm_id == imm.batch_id() {
                                    self.staging.imm.pop_back();
                                }
                            } else {
                                let local_imm_ids = self
                                    .staging
                                    .imm
                                    .iter()
                                    .map(|imm| imm.batch_id())
                                    .collect_vec();

                                let merged_imm_ids = self
                                    .staging
                                    .merged_imm
                                    .iter()
                                    .map(|imm| imm.batch_id())
                                    .collect_vec();
                                unreachable!(
                                    "should not reach here staging_sst.size {},
                                    staging_sst.imm_ids {:?},
                                    staging_sst.epochs {:?},
                                    local_imm_ids {:?},
                                    merged_imm_ids {:?},
                                    intersect_imm_ids {:?}",
                                    staging_sst.imm_size,
                                    staging_sst.imm_ids,
                                    staging_sst.epochs,
                                    local_imm_ids,
                                    merged_imm_ids,
                                    intersect_imm_ids,
                                );
                            }
                        }
                        staging_sst.sstable_infos.retain(|local_sstable_info| {
                            local_sstable_info
                                .sst_info
                                .get_table_ids()
                                .binary_search(&self.table_id.table_id)
                                .is_ok()
                        });
                        self.staging.sst.push_front(staging_sst);
                    }
                }
            },

            VersionUpdate::CommittedDelta(_) => {
                unimplemented!()
            }

            VersionUpdate::CommittedSnapshot(committed_version) => {
                let cleaned_ssts = self.clean_staging_ssts(committed_version);

                if let Some(committed_index) = self.committed_index.as_deref() {
                    self.committed_index = Some(Arc::new(
                        committed_index.apply(self.committed(), cleaned_ssts),
                    ));
                }
            }
        }
    }

    fn clean_staging_ssts(
        &mut self,
        committed_version: PinnedVersion,
    ) -> Vec<(HummockEpoch, Vec<HummockSstableObjectId>)> {
        let old_max_committed_epoch = self.committed().max_committed_epoch();
        self.committed = committed_version;
        let new_max_committed_epoch = self.committed().max_committed_epoch();

        let mut min_acceptable_staging_epoch = old_max_committed_epoch + 1;
        if let Some(pruned_version) = self.committed_index.as_deref() && pruned_version.scope() > min_acceptable_staging_epoch {
            min_acceptable_staging_epoch = pruned_version.scope();
        }

        self.staging
            .imm
            .retain(|imm| imm.min_epoch() > new_max_committed_epoch);

        self.staging
            .merged_imm
            .retain(|merged_imm| merged_imm.min_epoch() > new_max_committed_epoch);

        let mut cleaned_ssts = vec![];
        self.staging.sst.retain(|sst| {
            let newest_epoch = *sst.epochs.first().expect("epochs not empty");
            if newest_epoch > new_max_committed_epoch {
                true
            } else {
                assert!(newest_epoch >= min_acceptable_staging_epoch);
                cleaned_ssts.push((
                    newest_epoch,
                    sst.sstable_infos()
                        .iter()
                        .map(|local_sstable_info| local_sstable_info.sst_info.get_object_id())
                        .collect_vec(),
                ));
                false
            }
        });

        // check epochs.last() > MCE
        assert!(self.staging.sst.iter().all(|sst| {
            sst.epochs.last().expect("epochs not empty") > &new_max_committed_epoch
        }));

        cleaned_ssts
    }

    pub fn rewind(&mut self) {
        self.committed_index = if self.is_singleton {
            None
        } else {
            let max_committed_epoch = self.committed().max_committed_epoch();
            Some(Arc::new(PrunedVersion::new(
                self.table_id,
                max_committed_epoch,
            )))
        };
    }

    pub fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) {
        let cache_may_stale = cache_may_stale(&self.vnodes, &new_vnodes);

        if cache_may_stale {
            self.rewind();
        }

        self.vnodes = new_vnodes;
    }

    pub fn staging(&self) -> &StagingVersion {
        &self.staging
    }

    pub fn committed(&self) -> &CommittedVersion {
        &self.committed
    }

    pub fn committed_index(&self) -> Option<&Arc<PrunedVersion>> {
        self.committed_index.as_ref()
    }

    pub fn clear_uncommitted(&mut self) {
        self.staging.imm.clear();
        self.staging.merged_imm.clear();
        self.staging.sst.clear();
    }

    pub fn add_merged_imm(&mut self, merged_imm: ImmutableMemtable) {
        let staging_imm_count = self.staging.imm.len();
        let merged_imm_ids = merged_imm.get_imm_ids();

        #[cfg(debug_assertions)]
        {
            // check the suffix `merged_imm_ids.len()` imms in staging.imm are the same as
            // `merged_imm_ids`
            let diff = staging_imm_count - merged_imm_ids.len();
            let mut count: usize = 0;
            for (i, imm) in self.staging.imm.iter().skip(diff).enumerate() {
                count += 1;
                assert_eq!(
                    imm.batch_id(),
                    merged_imm_ids[i],
                    "merged_imm_ids: {:?}",
                    merged_imm_ids
                );
            }
            assert_eq!(count, merged_imm_ids.len());
        }
        self.staging
            .imm
            .truncate(staging_imm_count - merged_imm_ids.len());

        // add the newly merged imm into front
        self.staging.merged_imm.push_front(merged_imm);
    }
}

pub fn read_filter_for_batch(
    epoch: HummockEpoch, // for check
    table_id: TableId,
    key_range: &TableKeyRange,
    read_version_vec: Vec<Arc<RwLock<HummockReadVersion>>>,
) -> StorageResult<ReadVersionTuple> {
    assert!(!read_version_vec.is_empty());
    let read_version_guard_vec = read_version_vec
        .iter()
        .map(|read_version| read_version.read())
        .collect_vec();
    let mut imm_vec = Vec::default();
    let mut sst_vec = Vec::default();
    // to get max_mce with lock_guard to avoid losing committed_data since the read_version
    // update is asynchronous
    let (lastst_committed_version, max_mce) = {
        let committed_version = read_version_guard_vec
            .iter()
            .max_by_key(|read_version| read_version.committed().max_committed_epoch())
            .unwrap()
            .committed();

        (
            committed_version.clone(),
            committed_version.max_committed_epoch(),
        )
    };

    // only filter the staging data that epoch greater than max_mce to avoid data duplication
    let (min_epoch, max_epoch) = (max_mce, epoch);

    // prune imm and sst with max_mce
    for read_version_guard in read_version_guard_vec {
        let (imm_iter, sst_iter) = read_version_guard
            .staging()
            .prune_overlap(min_epoch, max_epoch, table_id, key_range);

        imm_vec.extend(imm_iter.cloned().collect_vec());
        sst_vec.extend(sst_iter.cloned().collect_vec());
    }

    // TODO: dedup the same `SstableInfo` before introduce new uploader

    Ok((imm_vec, sst_vec, lastst_committed_version, None))
}

pub fn read_filter_for_local(
    epoch: HummockEpoch,
    table_id: TableId,
    table_key_range: &TableKeyRange,
    read_version: Arc<RwLock<HummockReadVersion>>,
) -> StorageResult<ReadVersionTuple> {
    let read_version_guard = read_version.read();
    let (imm_iter, sst_iter) =
        read_version_guard
            .staging()
            .prune_overlap(0, epoch, table_id, table_key_range);

    Ok((
        imm_iter.cloned().collect_vec(),
        sst_iter.cloned().collect_vec(),
        read_version_guard.committed().clone(),
        read_version_guard.committed_index().cloned(),
    ))
}

#[derive(Clone)]
pub struct HummockVersionReader {
    sstable_store: SstableStoreRef,

    /// Statistics
    state_store_metrics: Arc<HummockStateStoreMetrics>,
}

/// use `HummockVersionReader` to reuse `get` and `iter` implement for both `batch_query` and
/// `streaming_query`
impl HummockVersionReader {
    pub fn new(
        sstable_store: SstableStoreRef,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
    ) -> Self {
        Self {
            sstable_store,
            state_store_metrics,
        }
    }

    pub fn stats(&self) -> &Arc<HummockStateStoreMetrics> {
        &self.state_store_metrics
    }
}

const SLOW_ITER_FETCH_META_DURATION_SECOND: f64 = 5.0;

impl HummockVersionReader {
    pub async fn get(
        &self,
        table_key: TableKey<Bytes>,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: ReadVersionTuple,
    ) -> StorageResult<Option<Bytes>> {
        let (imms, uncommitted_ssts, committed_version, committed_index) = read_version_tuple;
        let min_epoch = gen_min_epoch(epoch, read_options.retention_seconds.as_ref());
        let mut stats_guard =
            GetLocalMetricsGuard::new(self.state_store_metrics.clone(), read_options.table_id);
        stats_guard.local_stats.found_key = true;

        // 1. read staging data
        for imm in &imms {
            // skip imm that only holding out-of-date data
            if imm.max_epoch() < min_epoch {
                continue;
            }

            if let Some((data, data_epoch)) = get_from_batch(
                imm,
                TableKey(table_key.as_ref()),
                epoch,
                &read_options,
                &mut stats_guard.local_stats,
            ) {
                return Ok(if data_epoch < min_epoch {
                    None
                } else {
                    data.into_user_value()
                });
            }
        }

        // 2. order guarantee: imm -> sst
        let dist_key_hash = read_options.prefix_hint.as_ref().map(|dist_key| {
            Sstable::hash_for_bloom_filter(dist_key.as_ref(), read_options.table_id.table_id())
        });

        let full_key = FullKey::new(read_options.table_id, TableKey(table_key.clone()), epoch);
        for local_sst in &uncommitted_ssts {
            stats_guard.local_stats.sub_iter_count += 1;
            if let Some((data, data_epoch)) = get_from_sstable_info(
                self.sstable_store.clone(),
                local_sst,
                full_key.to_ref(),
                &read_options,
                dist_key_hash,
                &mut stats_guard.local_stats,
            )
            .await?
            {
                return Ok(if data_epoch < min_epoch {
                    None
                } else {
                    data.into_user_value()
                });
            }
        }

        // 3. read from committed_version sst file
        // Because SST meta records encoded key range,
        // the filter key needs to be encoded as well.
        assert!(committed_version.is_valid());
        for (table_view_position, level) in
            committed_version.levels(read_options.table_id).enumerate()
        {
            if level.table_infos.is_empty() {
                continue;
            }

            match level.level_type() {
                LevelType::Overlapping | LevelType::Unspecified => {
                    // TODO: Use an elegant way to avoid duplicate code.
                    let sst_indices = committed_index.as_deref().and_then(|pruned_version| {
                        pruned_version.get_indices(table_view_position, level.get_sub_level_id())
                    });

                    let single_table_key_range = table_key.clone()..=table_key.clone();

                    if let Some(sst_indices) = sst_indices {
                        let sstable_infos = prune_overlapping_ssts(
                            sst_indices
                                .iter()
                                .map(|sst_index| &level.table_infos[*sst_index]),
                            read_options.table_id,
                            &single_table_key_range,
                        );
                        for sstable_info in sstable_infos {
                            stats_guard.local_stats.sub_iter_count += 1;
                            if let Some((data, data_epoch)) = get_from_sstable_info(
                                self.sstable_store.clone(),
                                sstable_info,
                                full_key.to_ref(),
                                &read_options,
                                dist_key_hash,
                                &mut stats_guard.local_stats,
                            )
                            .await?
                            {
                                return Ok(if data_epoch < min_epoch {
                                    None
                                } else {
                                    data.into_user_value()
                                });
                            }
                        }
                    } else {
                        let sstable_infos = prune_overlapping_ssts(
                            level.table_infos.iter(),
                            read_options.table_id,
                            &single_table_key_range,
                        );
                        for sstable_info in sstable_infos {
                            stats_guard.local_stats.sub_iter_count += 1;
                            if let Some((data, data_epoch)) = get_from_sstable_info(
                                self.sstable_store.clone(),
                                sstable_info,
                                full_key.to_ref(),
                                &read_options,
                                dist_key_hash,
                                &mut stats_guard.local_stats,
                            )
                            .await?
                            {
                                return Ok(if data_epoch < min_epoch {
                                    None
                                } else {
                                    data.into_user_value()
                                });
                            }
                        }
                    }
                }
                LevelType::Nonoverlapping => {
                    let mut table_info_idx =
                        search_sst_idx(&level.table_infos, full_key.user_key.as_ref());
                    if table_info_idx == 0 {
                        continue;
                    }
                    table_info_idx = table_info_idx.saturating_sub(1);
                    let ord = level.table_infos[table_info_idx]
                        .key_range
                        .as_ref()
                        .unwrap()
                        .compare_right_with_user_key(full_key.user_key.as_ref());
                    // the case that the key falls into the gap between two ssts
                    if ord == Ordering::Less {
                        sync_point!("HUMMOCK_V2::GET::SKIP_BY_NO_FILE");
                        continue;
                    }

                    stats_guard.local_stats.sub_iter_count += 1;
                    if let Some((data, data_epoch)) = get_from_sstable_info(
                        self.sstable_store.clone(),
                        &level.table_infos[table_info_idx],
                        full_key.to_ref(),
                        &read_options,
                        dist_key_hash,
                        &mut stats_guard.local_stats,
                    )
                    .await?
                    {
                        return Ok(if data_epoch < min_epoch {
                            None
                        } else {
                            data.into_user_value()
                        });
                    }
                }
            }
        }
        stats_guard.local_stats.found_key = false;
        Ok(None)
    }

    pub async fn iter(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: ReadVersionTuple,
    ) -> StorageResult<StreamTypeOfIter<HummockStorageIterator>> {
        let table_id_string = read_options.table_id.to_string();
        let table_id_label = table_id_string.as_str();
        let (imms, uncommitted_ssts, committed, committed_index) = read_version_tuple;

        let mut local_stats = StoreLocalStatistic::default();
        let mut staging_iters = Vec::with_capacity(imms.len() + uncommitted_ssts.len());
        let mut delete_range_iter = ForwardMergeRangeIterator::new(epoch);
        local_stats.staging_imm_iter_count = imms.len() as u64;
        for imm in imms {
            if imm.has_range_tombstone() && !read_options.ignore_range_tombstone {
                delete_range_iter.add_batch_iter(imm.delete_range_iter());
            }
            staging_iters.push(HummockIteratorUnion::First(imm.into_forward_iter()));
        }
        let mut staging_sst_iter_count = 0;
        // encode once
        let bloom_filter_prefix_hash = read_options
            .prefix_hint
            .as_ref()
            .map(|hint| Sstable::hash_for_bloom_filter(hint, read_options.table_id.table_id()));

        for sstable_info in &uncommitted_ssts {
            let table_holder = self
                .sstable_store
                .sstable(sstable_info, &mut local_stats)
                .in_span(Span::enter_with_local_parent("get_sstable"))
                .await?;

            if !table_holder
                .value()
                .meta
                .monotonic_tombstone_events
                .is_empty()
                && !read_options.ignore_range_tombstone
            {
                delete_range_iter
                    .add_sst_iter(SstableDeleteRangeIterator::new(table_holder.clone()));
            }
            if let Some(prefix_hash) = bloom_filter_prefix_hash.as_ref() {
                if !hit_sstable_bloom_filter(table_holder.value(), *prefix_hash, &mut local_stats) {
                    continue;
                }
            }

            staging_sst_iter_count += 1;
            staging_iters.push(HummockIteratorUnion::Second(SstableIterator::new(
                table_holder,
                self.sstable_store.clone(),
                Arc::new(SstableIteratorReadOptions::from_read_options(&read_options)),
            )));
        }
        local_stats.staging_sst_iter_count = staging_sst_iter_count;
        let staging_iter: StagingDataIterator = OrderedMergeIteratorInner::new(staging_iters);

        // 2. build iterator from committed
        // Because SST meta records encoded key range,
        // the filter key range needs to be encoded as well.
        let user_key_range = bound_table_key_range(read_options.table_id, &table_key_range);
        let user_key_range_ref = (
            user_key_range.0.as_ref().map(UserKey::as_ref),
            user_key_range.1.as_ref().map(UserKey::as_ref),
        );
        let mut non_overlapping_iters = Vec::new();
        let mut overlapping_iters = Vec::new();
        let mut overlapping_iter_count = 0;
        let timer = self
            .state_store_metrics
            .iter_fetch_meta_duration
            .with_label_values(&[table_id_label])
            .start_timer();

        let mut sst_read_options = SstableIteratorReadOptions::from_read_options(&read_options);
        if read_options.prefetch_options.exhaust_iter {
            sst_read_options.must_iterated_end_user_key =
                Some(user_key_range.1.map(|key| key.cloned()));
        }
        let sst_read_options = Arc::new(sst_read_options);
        for (table_view_position, level) in committed.levels(read_options.table_id).enumerate() {
            if level.table_infos.is_empty() {
                continue;
            }

            if level.level_type == LevelType::Nonoverlapping as i32 {
                let table_infos = prune_nonoverlapping_ssts(&level.table_infos, user_key_range_ref);
                let sstables = table_infos
                    .filter(|sstable_info| {
                        sstable_info
                            .table_ids
                            .binary_search(&read_options.table_id.table_id)
                            .is_ok()
                    })
                    .cloned()
                    .collect_vec();
                if sstables.is_empty() {
                    continue;
                }
                if sstables.len() > 1 {
                    delete_range_iter.add_concat_iter(sstables.clone(), self.sstable_store.clone());
                    non_overlapping_iters.push(ConcatIterator::new(
                        sstables,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                    ));
                } else {
                    let sstable = self
                        .sstable_store
                        .sstable(&sstables[0], &mut local_stats)
                        .in_span(Span::enter_with_local_parent("get_sstable"))
                        .await?;
                    if !sstable.value().meta.monotonic_tombstone_events.is_empty()
                        && !read_options.ignore_range_tombstone
                    {
                        delete_range_iter
                            .add_sst_iter(SstableDeleteRangeIterator::new(sstable.clone()));
                    }
                    if let Some(dist_hash) = bloom_filter_prefix_hash.as_ref() {
                        if !hit_sstable_bloom_filter(sstable.value(), *dist_hash, &mut local_stats)
                        {
                            continue;
                        }
                    }
                    overlapping_iters.push(SstableIterator::new(
                        sstable,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                    ));
                }
            } else {
                // TODO: Use an elegant way to avoid duplicate code.
                let sst_indices = committed_index.as_deref().and_then(|pruned_version| {
                    pruned_version.get_indices(table_view_position, level.get_sub_level_id())
                });

                // Overlapping
                let fetch_meta_req = if let Some(sst_indices) = sst_indices {
                    prune_overlapping_ssts(
                        sst_indices
                            .iter()
                            .map(|sst_index| &level.table_infos[*sst_index]),
                        read_options.table_id,
                        &table_key_range,
                    )
                    .rev()
                    .collect_vec()
                } else {
                    prune_overlapping_ssts(
                        level.table_infos.iter(),
                        read_options.table_id,
                        &table_key_range,
                    )
                    .rev()
                    .collect_vec()
                };
                if fetch_meta_req.is_empty() {
                    continue;
                }
                for sstable_info in fetch_meta_req {
                    let sstable = self
                        .sstable_store
                        .sstable(sstable_info, &mut local_stats)
                        .in_span(Span::enter_with_local_parent("get_sstable"))
                        .await?;
                    assert_eq!(sstable_info.get_object_id(), sstable.value().id);
                    if !sstable.value().meta.monotonic_tombstone_events.is_empty()
                        && !read_options.ignore_range_tombstone
                    {
                        delete_range_iter
                            .add_sst_iter(SstableDeleteRangeIterator::new(sstable.clone()));
                    }
                    if let Some(dist_hash) = bloom_filter_prefix_hash.as_ref() {
                        if !hit_sstable_bloom_filter(sstable.value(), *dist_hash, &mut local_stats)
                        {
                            continue;
                        }
                    }
                    overlapping_iters.push(SstableIterator::new(
                        sstable,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                    ));
                    overlapping_iter_count += 1;
                }
            }
        }
        let fetch_meta_duration_sec = timer.stop_and_record();
        if fetch_meta_duration_sec > SLOW_ITER_FETCH_META_DURATION_SECOND {
            tracing::warn!("Fetching meta while creating an iter to read table_id {:?} at epoch {:?} is slow: duration = {:?}s, cache unhits = {:?}.",
                table_id_string, epoch, fetch_meta_duration_sec, local_stats.cache_meta_block_miss);
            self.state_store_metrics
                .iter_slow_fetch_meta_cache_unhits
                .set(local_stats.cache_meta_block_miss as i64);
        }
        local_stats.overlapping_iter_count = overlapping_iter_count;
        local_stats.non_overlapping_iter_count = non_overlapping_iters.len() as u64;

        // 3. build user_iterator
        let merge_iter = UnorderedMergeIteratorInner::new(
            once(HummockIteratorUnion::First(staging_iter))
                .chain(
                    overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Second),
                )
                .chain(
                    non_overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Third),
                ),
        );

        let user_key_range = (
            user_key_range.0.map(|key| key.cloned()),
            user_key_range.1.map(|key| key.cloned()),
        );

        // the epoch_range left bound for iterator read
        let min_epoch = gen_min_epoch(epoch, read_options.retention_seconds.as_ref());
        let mut user_iter = UserIterator::new(
            merge_iter,
            user_key_range,
            epoch,
            min_epoch,
            Some(committed),
            delete_range_iter,
        );
        user_iter
            .rewind()
            .in_span(Span::enter_with_local_parent("rewind"))
            .await?;
        local_stats.found_key = user_iter.is_valid();
        local_stats.sub_iter_count = local_stats.staging_imm_iter_count
            + local_stats.staging_sst_iter_count
            + local_stats.overlapping_iter_count
            + local_stats.non_overlapping_iter_count;

        Ok(HummockStorageIterator::new(
            user_iter,
            self.state_store_metrics.clone(),
            read_options.table_id,
            local_stats,
        )
        .into_stream())
    }

    // Note: this method will not check the kv tomestones and delete range tomestones
    pub async fn may_exist(
        &self,
        table_key_range: TableKeyRange,
        read_options: ReadOptions,
        read_version_tuple: ReadVersionTuple,
    ) -> StorageResult<bool> {
        let table_id = read_options.table_id;
        let (imms, uncommitted_ssts, committed_version, committed_index) = read_version_tuple;
        let mut stats_guard =
            MayExistLocalMetricsGuard::new(self.state_store_metrics.clone(), table_id);

        // 1. check staging data
        for imm in &imms {
            if imm.range_exists(&table_key_range) {
                return Ok(true);
            }
        }

        let user_key_range = bound_table_key_range(read_options.table_id, &table_key_range);
        let user_key_range_ref = (
            user_key_range.0.as_ref().map(UserKey::as_ref),
            user_key_range.1.as_ref().map(UserKey::as_ref),
        );
        let bloom_filter_prefix_hash = if let Some(prefix_hint) = read_options.prefix_hint {
            Sstable::hash_for_bloom_filter(&prefix_hint, table_id.table_id)
        } else {
            // only use `table_key_range` to see whether all SSTs are filtered out
            // without looking at bloom filter because prefix_hint is not provided
            if !uncommitted_ssts.is_empty() {
                // uncommitted_ssts is already pruned by `table_key_range` so no extra check is
                // needed.
                return Ok(true);
            }
            for (table_view_position, level) in committed_version.levels(table_id).enumerate() {
                match level.level_type() {
                    LevelType::Overlapping | LevelType::Unspecified => {
                        // TODO: Use an elegant way to avoid duplicate code.
                        let sst_indices = committed_index.as_deref().and_then(|pruned_version| {
                            pruned_version
                                .get_indices(table_view_position, level.get_sub_level_id())
                        });

                        if let Some(sst_indices) = sst_indices {
                            let mut pruned_ssts_iter = prune_overlapping_ssts(
                                sst_indices
                                    .iter()
                                    .map(|sst_index| &level.table_infos[*sst_index]),
                                table_id,
                                &table_key_range,
                            );
                            if pruned_ssts_iter.next().is_some() {
                                return Ok(true);
                            }
                        } else {
                            let mut pruned_ssts_iter = prune_overlapping_ssts(
                                level.table_infos.iter(),
                                table_id,
                                &table_key_range,
                            );
                            if pruned_ssts_iter.next().is_some() {
                                return Ok(true);
                            }
                        }
                    }
                    LevelType::Nonoverlapping => {
                        if prune_nonoverlapping_ssts(&level.table_infos, user_key_range_ref)
                            .next()
                            .is_some()
                        {
                            return Ok(true);
                        }
                    }
                }
            }
            return Ok(false);
        };

        // 2. order guarantee: imm -> sst
        for local_sst in &uncommitted_ssts {
            stats_guard.local_stats.may_exist_check_sstable_count += 1;
            if hit_sstable_bloom_filter(
                self.sstable_store
                    .sstable(local_sst, &mut stats_guard.local_stats)
                    .await?
                    .value(),
                bloom_filter_prefix_hash,
                &mut stats_guard.local_stats,
            ) {
                return Ok(true);
            }
        }

        // 3. read from committed_version sst file
        // Because SST meta records encoded key range,
        // the filter key needs to be encoded as well.
        assert!(committed_version.is_valid());
        for (table_view_position, level) in committed_version.levels(table_id).enumerate() {
            if level.table_infos.is_empty() {
                continue;
            }
            match level.level_type() {
                LevelType::Overlapping | LevelType::Unspecified => {
                    // TODO: Use an elegant way to avoid duplicate code.
                    let sst_indices = committed_index.as_deref().and_then(|pruned_version| {
                        pruned_version.get_indices(table_view_position, level.get_sub_level_id())
                    });

                    if let Some(sst_indices) = sst_indices {
                        let sstable_infos = prune_overlapping_ssts(
                            sst_indices
                                .iter()
                                .map(|sst_index| &level.table_infos[*sst_index]),
                            table_id,
                            &table_key_range,
                        );
                        for sstable_info in sstable_infos {
                            stats_guard.local_stats.may_exist_check_sstable_count += 1;
                            if hit_sstable_bloom_filter(
                                self.sstable_store
                                    .sstable(sstable_info, &mut stats_guard.local_stats)
                                    .await?
                                    .value(),
                                bloom_filter_prefix_hash,
                                &mut stats_guard.local_stats,
                            ) {
                                return Ok(true);
                            }
                        }
                    } else {
                        let sstable_infos = prune_overlapping_ssts(
                            level.table_infos.iter(),
                            table_id,
                            &table_key_range,
                        );
                        for sstable_info in sstable_infos {
                            stats_guard.local_stats.may_exist_check_sstable_count += 1;
                            if hit_sstable_bloom_filter(
                                self.sstable_store
                                    .sstable(sstable_info, &mut stats_guard.local_stats)
                                    .await?
                                    .value(),
                                bloom_filter_prefix_hash,
                                &mut stats_guard.local_stats,
                            ) {
                                return Ok(true);
                            }
                        }
                    }
                }
                LevelType::Nonoverlapping => {
                    let table_infos =
                        prune_nonoverlapping_ssts(&level.table_infos, user_key_range_ref);

                    for table_info in table_infos {
                        stats_guard.local_stats.may_exist_check_sstable_count += 1;
                        if hit_sstable_bloom_filter(
                            self.sstable_store
                                .sstable(table_info, &mut stats_guard.local_stats)
                                .await?
                                .value(),
                            bloom_filter_prefix_hash,
                            &mut stats_guard.local_stats,
                        ) {
                            return Ok(true);
                        }
                    }
                }
            }
        }

        Ok(false)
    }
}
