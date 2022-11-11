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

use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};
use std::iter::once;
use std::ops::Bound::{Excluded, Included};
use std::ops::{Deref, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use minitrace::future::FutureExt;
use minitrace::Span;
use parking_lot::RwLock;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{
    bound_table_key_range, user_key, FullKey, TableKey, TableKeyRange, UserKey,
};
use risingwave_hummock_sdk::{can_concat, HummockEpoch};
use risingwave_pb::hummock::{HummockVersionDelta, LevelType, SstableInfo};

use super::memtable::{ImmId, ImmutableMemtable};
use super::state_store::StagingDataIterator;
use crate::error::StorageResult;
use crate::hummock::iterator::{
    ConcatIterator, HummockIteratorUnion, OrderedMergeIteratorInner, UnorderedMergeIteratorInner,
    UserIterator,
};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::store::state_store::HummockStorageIterator;
use crate::hummock::utils::{
    check_subset_preserve_order, filter_single_sst, prune_ssts, range_overlap, search_sst_idx,
};
use crate::hummock::{
    get_from_batch, get_from_sstable_info, hit_sstable_bloom_filter, SstableIterator,
};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};
use crate::store::{gen_min_epoch, ReadOptions};

// TODO: use a custom data structure to allow in-place update instead of proto
// pub type CommittedVersion = HummockVersion;

pub type CommittedVersion = PinnedVersion;

/// Data not committed to Hummock. There are two types of staging data:
/// - Immutable memtable: data that has been written into local state store but not persisted.
/// - Uncommitted SST: data that has been uploaded to persistent storage but not committed to
///   hummock version.

#[derive(Clone, Debug)]
pub struct StagingSstableInfo {
    // newer data comes first
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

#[derive(Clone)]
pub struct StagingVersion {
    // newer data comes first
    // Note: Currently, building imm and writing to staging version is not atomic, and therefore
    // imm of smaller batch id may be added later than one with greater batch id
    pub imm: VecDeque<ImmutableMemtable>,
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
        let overlapped_imms = self.imm.iter().filter(move |imm| {
            imm.epoch() <= max_epoch_inclusive
                && imm.table_id == table_id
                && imm.epoch() > min_epoch_exclusive
                && range_overlap(table_key_range, imm.start_table_key(), imm.end_table_key())
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
                    .filter(move |sstable| filter_single_sst(sstable, table_id, table_key_range))
            });
        (overlapped_imms, overlapped_ssts)
    }
}

#[derive(Clone)]
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

    pub fn clear_uncommitted(&mut self) {
        self.staging.imm.clear();
        self.staging.sst.clear();
    }
}

pub fn read_filter_for_batch(
    epoch: HummockEpoch, // for check
    table_id: TableId,
    key_range: &TableKeyRange,
    read_version_vec: Vec<Arc<RwLock<HummockReadVersion>>>,
) -> StorageResult<(Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion)> {
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

    Ok((imm_vec, sst_vec, lastst_committed_version))
}

pub fn read_filter_for_local(
    epoch: HummockEpoch,
    table_id: TableId,
    table_key_range: &TableKeyRange,
    read_version: Arc<RwLock<HummockReadVersion>>,
) -> StorageResult<(Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion)> {
    let read_version_guard = read_version.read();
    let (imm_iter, sst_iter) =
        read_version_guard
            .staging()
            .prune_overlap(0, epoch, table_id, table_key_range);

    Ok((
        imm_iter.cloned().collect_vec(),
        sst_iter.cloned().collect_vec(),
        read_version_guard.committed().clone(),
    ))
}

#[derive(Clone)]
pub struct HummockVersionReader {
    sstable_store: SstableStoreRef,

    /// Statistics
    stats: Arc<StateStoreMetrics>,
}

/// use `HummockVersionReader` to reuse `get` and `iter` implement for both `batch_query` and
/// `streaming_query`
impl HummockVersionReader {
    pub fn new(sstable_store: SstableStoreRef, stats: Arc<StateStoreMetrics>) -> Self {
        Self {
            sstable_store,
            stats,
        }
    }
}

impl HummockVersionReader {
    pub async fn get<'a>(
        &'a self,
        table_key: TableKey<&'a [u8]>,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: (Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion),
    ) -> StorageResult<Option<Bytes>> {
        let mut table_counts = 0;
        let mut local_stats = StoreLocalStatistic::default();
        let (imms, uncommitted_ssts, committed_version) = read_version_tuple;

        // 1. read staging data
        for imm in &imms {
            if let Some(data) = get_from_batch(imm, table_key, &mut local_stats) {
                return Ok(data.into_user_value());
            }
        }

        // 2. order guarantee: imm -> sst
        let full_key = FullKey::new(read_options.table_id, table_key, epoch);
        for local_sst in &uncommitted_ssts {
            table_counts += 1;

            if let Some(data) = get_from_sstable_info(
                self.sstable_store.clone(),
                local_sst,
                full_key,
                read_options.check_bloom_filter,
                &mut local_stats,
            )
            .await?
            {
                return Ok(data.into_user_value());
            }
        }

        // 3. read from committed_version sst file
        // Because SST meta records encoded key range,
        // the filter key needs to be encoded as well.
        let encoded_user_key = full_key.user_key.encode();
        assert!(committed_version.is_valid());
        for level in committed_version.levels(read_options.table_id) {
            if level.table_infos.is_empty() {
                continue;
            }
            match level.level_type() {
                LevelType::Overlapping | LevelType::Unspecified => {
                    let sstable_infos = prune_ssts(
                        level.table_infos.iter(),
                        read_options.table_id,
                        &(table_key..=table_key),
                    );
                    for sstable_info in sstable_infos {
                        table_counts += 1;
                        if let Some(v) = get_from_sstable_info(
                            self.sstable_store.clone(),
                            sstable_info,
                            full_key,
                            read_options.check_bloom_filter,
                            &mut local_stats,
                        )
                        .await?
                        {
                            // todo add global stat to report
                            local_stats.report(self.stats.as_ref());
                            return Ok(v.into_user_value());
                        }
                    }
                }
                LevelType::Nonoverlapping => {
                    let mut table_info_idx = level.table_infos.partition_point(|table| {
                        let ord = user_key(&table.key_range.as_ref().unwrap().left)
                            .cmp(encoded_user_key.as_ref());
                        ord == Ordering::Less || ord == Ordering::Equal
                    });
                    if table_info_idx == 0 {
                        continue;
                    }
                    table_info_idx = table_info_idx.saturating_sub(1);
                    let ord = user_key(
                        &level.table_infos[table_info_idx]
                            .key_range
                            .as_ref()
                            .unwrap()
                            .right,
                    )
                    .cmp(encoded_user_key.as_ref());
                    // the case that the key falls into the gap between two ssts
                    if ord == Ordering::Less {
                        continue;
                    }

                    table_counts += 1;
                    if let Some(v) = get_from_sstable_info(
                        self.sstable_store.clone(),
                        &level.table_infos[table_info_idx],
                        full_key,
                        read_options.check_bloom_filter,
                        &mut local_stats,
                    )
                    .await?
                    {
                        local_stats.report(self.stats.as_ref());
                        return Ok(v.into_user_value());
                    }
                }
            }
        }

        local_stats.report(self.stats.as_ref());
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["sub-iter"])
            .observe(table_counts as f64);

        Ok(None)
    }

    pub async fn iter(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: (Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion),
    ) -> StorageResult<HummockStorageIterator> {
        let (imms, uncommitted_ssts, committed) = read_version_tuple;

        let mut local_stats = StoreLocalStatistic::default();
        let mut staging_iters = Vec::with_capacity(imms.len() + uncommitted_ssts.len());
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["staging-imm-iter"])
            .observe(imms.len() as f64);
        staging_iters.extend(
            imms.into_iter()
                .map(|imm| HummockIteratorUnion::First(imm.into_forward_iter())),
        );
        let mut staging_sst_iter_count = 0;
        for sstable_info in &uncommitted_ssts {
            let table_holder = self
                .sstable_store
                .sstable(sstable_info, &mut local_stats)
                .in_span(Span::enter_with_local_parent("get_sstable"))
                .await?;
            if let Some(prefix) = read_options.prefix_hint.as_ref() {
                if !hit_sstable_bloom_filter(
                    table_holder.value(),
                    UserKey::for_test(read_options.table_id, prefix)
                        .encode()
                        .as_slice(),
                    &mut local_stats,
                ) {
                    continue;
                }
            }
            staging_sst_iter_count += 1;
            staging_iters.push(HummockIteratorUnion::Second(SstableIterator::new(
                table_holder,
                self.sstable_store.clone(),
                Arc::new(SstableIteratorReadOptions::default()),
            )));
        }
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["staging-sst-iter"])
            .observe(staging_sst_iter_count as f64);
        let staging_iter: StagingDataIterator = OrderedMergeIteratorInner::new(staging_iters);

        // 2. build iterator from committed
        // Because SST meta records encoded key range,
        // the filter key range needs to be encoded as well.
        let user_key_range = bound_table_key_range(read_options.table_id, &table_key_range);
        let encoded_user_key_range = (
            user_key_range.0.as_ref().map(UserKey::encode),
            user_key_range.1.as_ref().map(UserKey::encode),
        );
        let mut non_overlapping_iters = Vec::new();
        let mut overlapping_iters = Vec::new();
        let mut overlapping_iter_count = 0;
        for level in committed.levels(read_options.table_id) {
            let table_infos = prune_ssts(
                level.table_infos.iter(),
                read_options.table_id,
                &table_key_range,
            );
            if table_infos.is_empty() {
                continue;
            }

            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&table_infos));
                let start_table_idx = match encoded_user_key_range.start_bound() {
                    Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                    _ => 0,
                };
                let end_table_idx = match encoded_user_key_range.end_bound() {
                    Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                    _ => table_infos.len().saturating_sub(1),
                };
                assert!(start_table_idx < table_infos.len() && end_table_idx < table_infos.len());
                let matched_table_infos = &table_infos[start_table_idx..=end_table_idx];

                let mut sstables = vec![];
                for sstable_info in matched_table_infos {
                    if let Some(bloom_filter_key) = read_options.prefix_hint.as_ref() {
                        let sstable = self
                            .sstable_store
                            .sstable(sstable_info, &mut local_stats)
                            .in_span(Span::enter_with_local_parent("get_sstable"))
                            .await?;

                        if hit_sstable_bloom_filter(
                            sstable.value(),
                            UserKey::for_test(read_options.table_id, bloom_filter_key)
                                .encode()
                                .as_slice(),
                            &mut local_stats,
                        ) {
                            sstables.push((*sstable_info).clone());
                        }
                    } else {
                        sstables.push((*sstable_info).clone());
                    }
                }

                non_overlapping_iters.push(ConcatIterator::new(
                    sstables,
                    self.sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                ));
            } else {
                // Overlapping
                let mut iters = Vec::new();
                for table_info in table_infos.into_iter().rev() {
                    let sstable = self
                        .sstable_store
                        .sstable(table_info, &mut local_stats)
                        .in_span(Span::enter_with_local_parent("get_sstable"))
                        .await?;
                    if let Some(bloom_filter_key) = read_options.prefix_hint.as_ref() {
                        if !hit_sstable_bloom_filter(
                            sstable.value(),
                            UserKey::for_test(read_options.table_id, bloom_filter_key)
                                .encode()
                                .as_slice(),
                            &mut local_stats,
                        ) {
                            continue;
                        }
                    }

                    iters.push(SstableIterator::new(
                        sstable,
                        self.sstable_store.clone(),
                        Arc::new(SstableIteratorReadOptions::default()),
                    ));
                    overlapping_iter_count += 1;
                }
                overlapping_iters.push(OrderedMergeIteratorInner::new(iters));
            }
        }
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["committed-overlapping-iter"])
            .observe(overlapping_iter_count as f64);
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["committed-non-overlapping-iter"])
            .observe(non_overlapping_iters.len() as f64);

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

        // the epoch_range left bound for iterator read
        let min_epoch = gen_min_epoch(epoch, read_options.retention_seconds.as_ref());
        let mut user_iter = UserIterator::new(
            merge_iter,
            user_key_range,
            epoch,
            min_epoch,
            Some(committed),
        );
        user_iter
            .rewind()
            .in_span(Span::enter_with_local_parent("rewind"))
            .await?;
        local_stats.report(self.stats.deref());
        Ok(HummockStorageIterator::new(user_iter, self.stats.clone()))
    }
}
