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

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::mem::swap;
use std::ops::{Deref, RangeBounds};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    add_new_sub_level, summarize_level_deltas, HummockLevelsExt, HummockVersionExt,
    LevelDeltasSummary,
};
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, HummockVersionId, LocalSstableInfo};
use risingwave_pb::hummock::{HummockVersion, HummockVersionDelta, Level};
use tokio::sync::mpsc::UnboundedSender;

use super::shared_buffer::SharedBuffer;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::shared_buffer::{
    to_order_sorted, KeyIndexSharedBufferBatch, OrderSortedUncommittedData, UncommittedData,
};
use crate::hummock::utils::{check_subset_preserve_order, filter_single_sst, range_overlap};

#[derive(Clone)]
pub struct LocalVersion {
    replicated_batches: BTreeMap<HummockEpoch, KeyIndexSharedBufferBatch>,
    shared_buffer: BTreeMap<HummockEpoch, SharedBuffer>,
    pinned_version: PinnedVersion,
    local_related_version: PinnedVersion,
    pub version_ids_in_use: BTreeSet<HummockVersionId>,
    // TODO: save uncommitted data that needs to be flushed to disk.
    /// Save uncommitted data that needs to be synced or finished syncing.
    /// We need to save data in reverse order of epoch,
    /// because we will traverse `sync_uncommitted_data` in the forward direction and return the
    /// key when we find it
    pub sync_uncommitted_data: VecDeque<(Vec<HummockEpoch>, SyncUncommittedData)>,
    max_sync_epoch: u64,
}

#[derive(Debug, Clone)]
pub enum SyncUncommittedData {
    /// Before we start syncing, we need to mv the shared buffer to `sync_uncommitted_data` and
    /// wait for flush task to complete
    AwaitFlush(BTreeMap<HummockEpoch, SharedBuffer>),
    /// We may start syncing after the flush task complete
    Syncing(OrderSortedUncommittedData),
    /// After we finish syncing, we changed `Syncing` to `Synced`.
    Synced(Vec<LocalSstableInfo>),
    /// A temporary enum value. We may use `std::mem::swap` to take the ownership of current stage,
    /// do something and set the state to a new valid stage
    _Temporary,
}

// state transition
impl SyncUncommittedData {
    fn new(shared_buffer_data: BTreeMap<HummockEpoch, SharedBuffer>) -> Self {
        SyncUncommittedData::AwaitFlush(shared_buffer_data)
    }

    fn start_syncing(&mut self) -> (OrderSortedUncommittedData, usize) {
        let mut current_stage = SyncUncommittedData::_Temporary;
        swap(&mut current_stage, self);
        let (new_state, task_payload, task_size) = match current_stage {
            SyncUncommittedData::AwaitFlush(shared_buffer_data) => {
                let mut sync_size = 0;
                let mut all_uncommitted_data = vec![];
                for (_, shared_buffer) in shared_buffer_data {
                    if let Some((uncommitted_data, size)) = shared_buffer.into_uncommitted_data() {
                        all_uncommitted_data.push(uncommitted_data);
                        sync_size += size;
                    };
                }
                // Data of smaller epoch was added first. Take a `reverse` to make the data of
                // greater epoch appear first.
                all_uncommitted_data.reverse();
                let task_payload = all_uncommitted_data
                    .into_iter()
                    .flat_map(to_order_sorted)
                    .collect_vec();
                (
                    SyncUncommittedData::Syncing(task_payload.clone()),
                    task_payload,
                    sync_size,
                )
            }
            invalid_stage => {
                unreachable!("start syncing from an invalid stage: {:?}", invalid_stage)
            }
        };
        *self = new_state;
        (task_payload, task_size)
    }

    fn synced(&mut self, ssts: Vec<LocalSstableInfo>) {
        *self = SyncUncommittedData::Synced(ssts);
    }
}

impl SyncUncommittedData {
    pub fn get_overlap_data<R, B>(
        &self,
        key_range: &R,
        epoch: HummockEpoch,
    ) -> OrderSortedUncommittedData
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        match self {
            SyncUncommittedData::AwaitFlush(shared_buffer_data) => shared_buffer_data
                .range(..=epoch)
                .rev() // take rev so that data of newer epoch comes first
                .flat_map(|(_, shared_buffer)| shared_buffer.get_overlap_data(key_range))
                .collect(),
            SyncUncommittedData::Syncing(task) => task
                .iter()
                .map(|order_vec_data| {
                    order_vec_data
                        .iter()
                        .filter(|data| match data {
                            UncommittedData::Batch(batch) => {
                                batch.epoch() <= epoch
                                    && range_overlap(
                                        key_range,
                                        batch.start_user_key(),
                                        batch.end_user_key(),
                                    )
                            }
                            UncommittedData::Sst((_, info)) => filter_single_sst(info, key_range),
                        })
                        .cloned()
                        .collect_vec()
                })
                .collect_vec(),
            SyncUncommittedData::Synced(ssts) => vec![ssts
                .iter()
                .filter(|(_, info)| filter_single_sst(info, key_range))
                .map(|info| UncommittedData::Sst(info.clone()))
                .collect()],
            SyncUncommittedData::_Temporary => {
                unreachable!()
            }
        }
    }
}

impl LocalVersion {
    pub fn new(
        version: HummockVersion,
        unpin_worker_tx: UnboundedSender<HummockVersionId>,
    ) -> Self {
        let mut version_ids_in_use = BTreeSet::new();
        version_ids_in_use.insert(version.id);
        let local_related_version = version.clone();
        let pinned_version = PinnedVersion::new(version, unpin_worker_tx);
        let local_related_version =
            pinned_version.new_local_related_pin_version(local_related_version);
        Self {
            replicated_batches: BTreeMap::default(),
            shared_buffer: BTreeMap::default(),
            pinned_version,
            local_related_version,
            version_ids_in_use,
            sync_uncommitted_data: Default::default(),
            max_sync_epoch: 0,
        }
    }

    pub fn pinned_version(&self) -> &PinnedVersion {
        &self.pinned_version
    }

    /// Advance the `max_sync_epoch` to at least `new_epoch`.
    ///
    /// Return `Some(prev max_sync_epoch)` if `new_epoch > max_sync_epoch`
    /// Return `None` if `new_epoch <= max_sync_epoch`
    pub fn advance_max_sync_epoch(&mut self, new_epoch: HummockEpoch) -> Option<Vec<HummockEpoch>> {
        if self.max_sync_epoch > new_epoch {
            None
        } else {
            let mut shared_buffer_to_sync = self.shared_buffer.split_off(&(new_epoch + 1));
            // After `split_off`, epochs greater than `epoch` will be in `shared_buffer_to_sync`. We
            // use a swap to reach the expected setting.
            swap(&mut shared_buffer_to_sync, &mut self.shared_buffer);
            let epochs = if shared_buffer_to_sync.is_empty() {
                Vec::new()
            } else {
                self.add_sync_data(shared_buffer_to_sync)
            };
            self.max_sync_epoch = new_epoch;
            Some(epochs)
        }
    }

    pub fn get_max_sync_epoch(&self) -> HummockEpoch {
        self.max_sync_epoch
    }

    pub fn get_mut_shared_buffer(&mut self, epoch: HummockEpoch) -> Option<&mut SharedBuffer> {
        self.shared_buffer.get_mut(&epoch).or_else(|| {
            for &mut (_, ref mut sync_data) in &mut self.sync_uncommitted_data {
                if let SyncUncommittedData::AwaitFlush(shared_buffer_data) = sync_data {
                    if let Some(shared_buffer) = shared_buffer_data.get_mut(&epoch) {
                        return Some(shared_buffer);
                    }
                }
            }
            None
        })
    }

    #[cfg(any(test, feature = "test"))]
    pub fn get_shared_buffer(&self, epoch: HummockEpoch) -> Option<&SharedBuffer> {
        self.shared_buffer.get(&epoch)
    }

    pub fn add_sync_data(
        &mut self,
        shared_buffer_data: BTreeMap<HummockEpoch, SharedBuffer>,
    ) -> Vec<HummockEpoch> {
        let epochs = shared_buffer_data.keys().rev().cloned().collect_vec(); // newer epoch comes first
        if let Some((prev_sync_epochs, _)) = self.sync_uncommitted_data.front() {
            let current_min_epoch = epochs.last().expect("non empty");
            let prev_max_epoch = prev_sync_epochs.first().expect("non empty");
            assert!(
                current_min_epoch > prev_max_epoch,
                "newly added sync data must be newer than prev data: {:?} {:?}",
                epochs,
                prev_sync_epochs
            );
        }
        self.sync_uncommitted_data
            .push_front((epochs.clone(), SyncUncommittedData::new(shared_buffer_data)));
        epochs
    }

    pub fn start_syncing(
        &mut self,
        sync_epochs: Vec<HummockEpoch>,
    ) -> (OrderSortedUncommittedData, usize) {
        let &mut (_, ref mut stage) = self
            .sync_uncommitted_data
            .iter_mut()
            .find(|(epochs, _)| epochs == &sync_epochs)
            .expect("should find");
        stage.start_syncing()
    }

    pub fn data_synced(&mut self, sync_epochs: Vec<HummockEpoch>, ssts: Vec<LocalSstableInfo>) {
        let &mut (_, ref mut stage) = self
            .sync_uncommitted_data
            .iter_mut()
            .find(|(epochs, _)| epochs == &sync_epochs)
            .expect("should find");
        stage.synced(ssts);
    }

    #[cfg(any(test, feature = "test"))]
    pub fn iter_shared_buffer(&self) -> impl Iterator<Item = (&HummockEpoch, &SharedBuffer)> {
        self.shared_buffer.iter()
    }

    pub fn iter_mut_unsynced_shared_buffer(
        &mut self,
    ) -> impl Iterator<Item = (&HummockEpoch, &mut SharedBuffer)> {
        self.shared_buffer.range_mut(self.max_sync_epoch + 1..)
    }

    pub fn new_shared_buffer(
        &mut self,
        epoch: HummockEpoch,
        global_upload_task_size: Arc<AtomicUsize>,
    ) -> &mut SharedBuffer {
        self.shared_buffer
            .entry(epoch)
            .or_insert_with(|| SharedBuffer::new(global_upload_task_size))
    }

    /// Returns epochs cleaned from shared buffer.
    pub fn set_pinned_version(
        &mut self,
        new_pinned_version: HummockVersion,
        version_deltas: Option<Vec<HummockVersionDelta>>,
    ) -> Vec<HummockEpoch> {
        let new_max_committed_epoch = new_pinned_version.max_committed_epoch;
        if self.pinned_version.max_committed_epoch() < new_max_committed_epoch {
            self.replicated_batches
                .retain(|epoch, _| *epoch > new_pinned_version.max_committed_epoch);
            assert!(self
                .shared_buffer
                .iter()
                .all(|(epoch, _)| *epoch > new_pinned_version.max_committed_epoch));
        }

        self.version_ids_in_use.insert(new_pinned_version.id);

        let new_pinned_version = self.pinned_version.new_pin_version(new_pinned_version);

        let cleaned_epoch =
            match version_deltas {
                Some(version_deltas) => {
                    let mut new_local_related_version = self.local_related_version.version();
                    let mut clean_epochs = Vec::new();
                    for delta in version_deltas {
                        assert_eq!(new_local_related_version.id, delta.prev_id);
                        clean_epochs.extend(self.apply_version_delta_local_related(
                            &mut new_local_related_version,
                            &delta,
                        ));
                    }
                    assert_eq!(
                        clean_epochs.len(),
                        clean_epochs.iter().sorted().dedup().count(),
                        "some epochs are clean in two version delta: {:?}",
                        clean_epochs
                    );
                    self.local_related_version =
                        new_pinned_version.new_local_related_pin_version(new_local_related_version);
                    clean_epochs
                }
                None => {
                    let cleaned_epochs = self
                        .clear_committed_data(new_max_committed_epoch)
                        .into_iter()
                        .flat_map(|(epochs, _)| epochs.into_iter())
                        .sorted()
                        .dedup()
                        .collect_vec();
                    self.local_related_version = new_pinned_version
                        .new_local_related_pin_version(new_pinned_version.version());
                    cleaned_epochs
                }
            };

        // update pinned version
        self.pinned_version = new_pinned_version;

        cleaned_epoch
    }

    pub fn read_filter<R, B>(
        this: &RwLock<Self>,
        read_epoch: HummockEpoch,
        key_range: &R,
    ) -> ReadVersion
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        use parking_lot::RwLockReadGuard;
        let (pinned_version, (replicated_batches, shared_buffer_data, sync_uncommitted_data)) = {
            let guard = this.read();
            let smallest_uncommitted_epoch = guard.pinned_version.max_committed_epoch() + 1;
            let pinned_version = guard.pinned_version.clone();
            (
                pinned_version,
                if read_epoch >= smallest_uncommitted_epoch {
                    let replicated_batches = guard
                        .replicated_batches
                        .range(smallest_uncommitted_epoch..=read_epoch)
                        .rev() // Important: order by epoch descendingly
                        .map(|(_, key_indexed_batches)| {
                            key_indexed_batches
                                .range((
                                    key_range.start_bound().map(|b| b.as_ref().to_vec()),
                                    std::ops::Bound::Unbounded,
                                ))
                                .filter(|(_, batch)| {
                                    range_overlap(
                                        key_range,
                                        batch.start_user_key(),
                                        batch.end_user_key(),
                                    )
                                })
                                .map(|(_, batches)| batches.clone())
                                .collect_vec()
                        })
                        .collect_vec();

                    let shared_buffer_data = guard
                        .shared_buffer
                        .range(smallest_uncommitted_epoch..=read_epoch)
                        .rev() // Important: order by epoch descendingly
                        .map(|(_, shared_buffer)| shared_buffer.get_overlap_data(key_range))
                        .collect();
                    let sync_data: Vec<OrderSortedUncommittedData> = guard
                        .sync_uncommitted_data
                        .iter()
                        .filter(|&node| {
                            node.0.last().le(&Some(&read_epoch))
                                && node.0.last().ge(&Some(&smallest_uncommitted_epoch))
                        })
                        .map(|(_, value)| value.get_overlap_data(key_range, read_epoch))
                        .collect();
                    RwLockReadGuard::unlock_fair(guard);
                    (replicated_batches, shared_buffer_data, sync_data)
                } else {
                    RwLockReadGuard::unlock_fair(guard);
                    (Vec::new(), Vec::new(), Vec::new())
                },
            )
        };

        ReadVersion {
            replicated_batches,
            shared_buffer_data,
            pinned_version,
            sync_uncommitted_data,
        }
    }

    pub fn replicate_batch(&mut self, epoch: HummockEpoch, batch: SharedBufferBatch) {
        self.replicated_batches
            .entry(epoch)
            .or_default()
            .insert(batch.end_user_key().to_vec(), batch);
    }

    pub fn clear_shared_buffer(&mut self) -> Vec<HummockEpoch> {
        let mut cleaned_epoch = self.shared_buffer.keys().cloned().collect_vec();
        for (epochs, _) in &self.sync_uncommitted_data {
            for epoch in epochs {
                cleaned_epoch.push(*epoch);
            }
        }
        self.sync_uncommitted_data.clear();
        self.shared_buffer.clear();
        self.replicated_batches.clear();
        cleaned_epoch
    }

    pub fn clear_committed_data(
        &mut self,
        max_committed_epoch: HummockEpoch,
    ) -> Vec<(Vec<HummockEpoch>, Vec<LocalSstableInfo>)> {
        match self.sync_uncommitted_data.iter().position(|(epoch, data)| {
            let min_epoch = *epoch.last().expect("epoch list should not be empty");
            let max_epoch = *epoch.first().expect("epoch list should not be empty");
            assert!(
                max_epoch <= max_committed_epoch || min_epoch > max_committed_epoch,
                "new_max_committed_epoch {} lays within max_epoch {} and min_epoch {} of data {:?}",
                max_committed_epoch,
                max_epoch,
                min_epoch,
                data,
            );
            max_epoch <= max_committed_epoch
        }) {
            Some(index) => self
                .sync_uncommitted_data
                .drain(index..)
                .map(|(epoch, data)| {
                    (
                        epoch,
                        match data {
                            SyncUncommittedData::Synced(ssts) => ssts,
                            invalid_stage => {
                                unreachable!("expect synced. Now is {:?}", invalid_stage)
                            }
                        },
                    )
                })
                .collect_vec(),
            None => vec![],
        }
    }

    fn apply_version_delta_local_related(
        &mut self,
        version: &mut HummockVersion,
        version_delta: &HummockVersionDelta,
    ) -> Vec<HummockEpoch> {
        assert!(version.max_committed_epoch <= version_delta.max_committed_epoch);
        let (clean_epochs, mut compaction_group_synced_ssts) =
            if version.max_committed_epoch < version_delta.max_committed_epoch {
                let (clean_epochs, synced_ssts) = self
                    .clear_committed_data(version_delta.max_committed_epoch)
                    .into_iter()
                    .fold(
                        (Vec::new(), Vec::new()),
                        |(mut clean_epochs, mut synced_ssts), (epochs, ssts)| {
                            clean_epochs.extend(epochs);
                            synced_ssts.extend(ssts);
                            (clean_epochs, synced_ssts)
                        },
                    );
                let clean_epochs = clean_epochs.into_iter().sorted().dedup().collect();
                let mut compaction_group_ssts: HashMap<_, Vec<_>> = HashMap::new();
                for (compaction_group_id, sst) in synced_ssts {
                    compaction_group_ssts
                        .entry(compaction_group_id)
                        .or_default()
                        .push(sst);
                }
                (clean_epochs, Some(compaction_group_ssts))
            } else {
                (Vec::new(), None)
            };

        for (compaction_group_id, level_deltas) in &version_delta.level_deltas {
            let levels = version
                .levels
                .get_mut(compaction_group_id)
                .expect("compaction group id should exist");

            let summary = summarize_level_deltas(level_deltas);

            match &mut compaction_group_synced_ssts {
                Some(compaction_group_ssts) => {
                    // The version delta is generated from a `commit_epoch` call.
                    let LevelDeltasSummary {
                        delete_sst_levels,
                        delete_sst_ids_set,
                        insert_sst_level_id,
                        insert_sub_level_id,
                        insert_table_infos,
                    } = summary;
                    assert!(
                        delete_sst_levels.is_empty() && delete_sst_ids_set.is_empty(),
                        "there should not be any sst deleted in a commit_epoch call. Epoch: {}",
                        version_delta.max_committed_epoch
                    );
                    assert_eq!(
                        0, insert_sst_level_id,
                        "an commit_epoch call should always insert sst into L0, but not insert to {}",
                        insert_sst_level_id
                    );
                    if let Some(ssts) = compaction_group_ssts.remove(compaction_group_id) {
                        assert!(
                            check_subset_preserve_order(
                                ssts.iter().map(|info| info.id),
                                insert_table_infos.iter().map(|info| info.id)
                            ),
                            "order of local synced ssts is not preserved in the global inserted sst. local ssts: {:?}, global: {:?}",
                            ssts.iter().map(|info| info.id).collect_vec(),
                            insert_table_infos.iter().map(|info| info.id).collect_vec()
                        );
                        add_new_sub_level(levels.l0.as_mut().unwrap(), insert_sub_level_id, ssts);
                    }
                }
                None => {
                    // The version delta is generated from a compaction
                    levels.apply_compact_ssts(summary, true);
                }
            }
        }
        version.id = version_delta.id;
        version.max_committed_epoch = version_delta.max_committed_epoch;
        version.max_current_epoch = version_delta.max_current_epoch;
        version.safe_epoch = version_delta.safe_epoch;

        clean_epochs
    }
}

struct PinnedVersionGuard {
    version_id: HummockVersionId,
    unpin_worker_tx: UnboundedSender<HummockVersionId>,
}

impl Drop for PinnedVersionGuard {
    fn drop(&mut self) {
        self.unpin_worker_tx.send(self.version_id).ok();
    }
}

#[derive(Clone)]
pub struct PinnedVersion {
    version: Arc<HummockVersion>,
    guard: Arc<PinnedVersionGuard>,
}

impl PinnedVersion {
    fn new(version: HummockVersion, unpin_worker_tx: UnboundedSender<HummockVersionId>) -> Self {
        let version_id = version.id;
        PinnedVersion {
            version: Arc::new(version),
            guard: Arc::new(PinnedVersionGuard {
                version_id,
                unpin_worker_tx,
            }),
        }
    }

    fn new_pin_version(&self, version: HummockVersion) -> Self {
        assert!(
            version.id > self.version.id,
            "pinning a older version {}. Current is {}",
            version.id,
            self.version.id
        );
        let version_id = version.id;
        PinnedVersion {
            version: Arc::new(version),
            guard: Arc::new(PinnedVersionGuard {
                version_id,
                unpin_worker_tx: self.guard.unpin_worker_tx.clone(),
            }),
        }
    }

    fn new_local_related_pin_version(&self, version: HummockVersion) -> Self {
        assert_eq!(
            self.version.id, version.id,
            "local related version {} to pin not equal to current version id {}",
            version.id, self.version.id
        );
        PinnedVersion {
            version: Arc::new(version),
            guard: self.guard.clone(),
        }
    }

    pub fn id(&self) -> HummockVersionId {
        self.version.id
    }

    pub fn levels(&self, compaction_group_id: Option<CompactionGroupId>) -> Vec<&Level> {
        match compaction_group_id {
            None => self.version.get_combined_levels(),
            Some(compaction_group_id) => {
                let levels = self
                    .version
                    .get_compaction_group_levels(compaction_group_id);
                let mut ret = vec![];
                ret.extend(levels.l0.as_ref().unwrap().sub_levels.iter().rev());
                ret.extend(levels.levels.iter());
                ret
            }
        }
    }

    pub fn max_committed_epoch(&self) -> u64 {
        self.version.max_committed_epoch
    }

    pub fn max_current_epoch(&self) -> u64 {
        self.version.max_current_epoch
    }

    pub fn safe_epoch(&self) -> u64 {
        self.version.safe_epoch
    }

    /// ret value can't be used as `HummockVersion`. it must be modified with delta
    pub fn version(&self) -> HummockVersion {
        self.version.deref().clone()
    }
}

pub struct ReadVersion {
    // The replicated batches are sorted by epoch descendingly
    pub replicated_batches: Vec<Vec<SharedBufferBatch>>,
    // The shared buffers are sorted by epoch descendingly
    pub shared_buffer_data: Vec<OrderSortedUncommittedData>,
    pub pinned_version: PinnedVersion,
    pub sync_uncommitted_data: Vec<OrderSortedUncommittedData>,
}
