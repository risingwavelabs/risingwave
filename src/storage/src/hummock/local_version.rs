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

use std::assert_matches::assert_matches;
use std::collections::{BTreeMap, HashMap};
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
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockEpoch, HummockVersionId, LocalSstableInfo, INVALID_VERSION_ID,
};
use risingwave_pb::hummock::{HummockVersion, HummockVersionDelta, Level};
use tokio::sync::mpsc::UnboundedSender;

use super::shared_buffer::SharedBuffer;
use crate::hummock::shared_buffer::{to_order_sorted, OrderSortedUncommittedData, UncommittedData};
use crate::hummock::utils::{check_subset_preserve_order, filter_single_sst, range_overlap};

#[derive(Clone)]
pub struct LocalVersion {
    shared_buffer: BTreeMap<HummockEpoch, SharedBuffer>,
    pinned_version: PinnedVersion,
    local_related_version: PinnedVersion,
    // TODO: save uncommitted data that needs to be flushed to disk.
    /// Save uncommitted data that needs to be synced or finished syncing.
    /// We need to save data in reverse order of epoch,
    /// because we will traverse `sync_uncommitted_data` in the forward direction and return the
    /// key when we find it
    pub sync_uncommitted_data: BTreeMap<HummockEpoch, SyncUncommittedData>,
    max_sync_epoch: HummockEpoch,
    /// The max readable epoch, and epochs smaller than it will not be written again.
    sealed_epoch: HummockEpoch,
}

#[derive(Debug, Clone)]
pub enum PinVersionAction {
    Pin(HummockVersionId),
    Unpin(HummockVersionId),
}

#[derive(Debug, Clone)]
pub enum SyncUncommittedDataStage {
    /// Before we start syncing, we need to mv the shared buffer to `sync_uncommitted_data` and
    /// wait for flush task to complete
    CheckpointEpochSealed(BTreeMap<HummockEpoch, SharedBuffer>),
    /// Task payload when we start syncing
    Syncing(OrderSortedUncommittedData),
    /// After we finish syncing, we changed `Syncing` to `Synced`.
    Synced(Vec<LocalSstableInfo>),
}

#[derive(Debug, Clone)]
pub struct SyncUncommittedData {
    #[allow(dead_code)]
    sync_epoch: HummockEpoch,
    /// The previous `max_sync_epoch` when we start syncing `sync_epoch` and advance to a new
    /// `max_sync_epoch`.
    prev_max_sync_epoch: HummockEpoch,
    // newer epochs come first
    epochs: Vec<HummockEpoch>,
    stage: SyncUncommittedDataStage,
}

// state transition
impl SyncUncommittedData {
    fn new(
        sync_epoch: HummockEpoch,
        prev_max_sync_epoch: HummockEpoch,
        shared_buffer_data: BTreeMap<HummockEpoch, SharedBuffer>,
    ) -> Self {
        let epochs = shared_buffer_data.keys().rev().cloned().collect_vec(); // newer epoch comes first
        SyncUncommittedData {
            sync_epoch,
            prev_max_sync_epoch,
            epochs,
            stage: SyncUncommittedDataStage::CheckpointEpochSealed(shared_buffer_data),
        }
    }

    fn start_syncing(&mut self) -> (OrderSortedUncommittedData, usize) {
        let (new_stage, task_payload, task_size) = match &mut self.stage {
            SyncUncommittedDataStage::CheckpointEpochSealed(shared_buffer_data) => {
                let mut sync_size = 0;
                let mut all_uncommitted_data = vec![];
                for (_, shared_buffer) in shared_buffer_data.drain_filter(|_, _| true) {
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
                    SyncUncommittedDataStage::Syncing(task_payload.clone()),
                    task_payload,
                    sync_size,
                )
            }
            invalid_stage => {
                unreachable!("start syncing from an invalid stage: {:?}", invalid_stage)
            }
        };
        self.stage = new_stage;
        (task_payload, task_size)
    }

    fn synced(&mut self, ssts: Vec<LocalSstableInfo>) {
        assert_matches!(self.stage, SyncUncommittedDataStage::Syncing(_));
        self.stage = SyncUncommittedDataStage::Synced(ssts);
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
        match &self.stage {
            SyncUncommittedDataStage::CheckpointEpochSealed(shared_buffer_data) => {
                shared_buffer_data
                    .range(..=epoch)
                    .rev() // take rev so that data of newer epoch comes first
                    .flat_map(|(_, shared_buffer)| shared_buffer.get_overlap_data(key_range))
                    .collect()
            }
            SyncUncommittedDataStage::Syncing(task) => task
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
            SyncUncommittedDataStage::Synced(ssts) => vec![ssts
                .iter()
                .filter(|(_, info)| filter_single_sst(info, key_range))
                .map(|info| UncommittedData::Sst(info.clone()))
                .collect()],
        }
    }
}

impl LocalVersion {
    pub fn new(
        version: HummockVersion,
        pinned_version_manager_tx: UnboundedSender<PinVersionAction>,
    ) -> Self {
        let local_related_version = version.clone();
        let pinned_version = PinnedVersion::new(version, pinned_version_manager_tx);
        let local_related_version =
            pinned_version.new_local_related_pin_version(local_related_version);
        Self {
            shared_buffer: BTreeMap::default(),
            pinned_version,
            local_related_version,
            sync_uncommitted_data: Default::default(),
            max_sync_epoch: 0,
            sealed_epoch: 0,
        }
    }

    pub fn seal_epoch(&mut self, epoch: HummockEpoch, is_checkpoint: bool) {
        // TODO: remove it when non-checkpoint barrier is enabled
        assert!(is_checkpoint, "current seal_epoch must be a checkpoint");
        assert!(
            epoch > self.sealed_epoch,
            "sealed epoch not advance. new epoch: {}, current {}",
            epoch,
            self.sealed_epoch
        );
        self.sealed_epoch = epoch;
        self.advance_max_sync_epoch(epoch)
    }

    pub fn get_sealed_epoch(&self) -> HummockEpoch {
        self.sealed_epoch
    }

    pub fn pinned_version(&self) -> &PinnedVersion {
        &self.pinned_version
    }

    /// Advance the `max_sync_epoch` to at least `new_epoch`.
    ///
    /// Return `Some(prev max_sync_epoch)` if `new_epoch > max_sync_epoch`
    /// Return `None` if `new_epoch <= max_sync_epoch`
    pub fn advance_max_sync_epoch(&mut self, new_epoch: HummockEpoch) {
        assert!(
            new_epoch > self.max_sync_epoch,
            "max sync epoch not advance. new epoch: {}, current max sync epoch {}",
            new_epoch,
            self.max_sync_epoch
        );
        let last_epoch = self.max_sync_epoch;
        let mut shared_buffer_to_sync = self.shared_buffer.split_off(&(new_epoch + 1));
        // After `split_off`, epochs greater than `epoch` will be in `shared_buffer_to_sync`. We
        // want epoch with `epoch > new_sync_epoch` to stay in `self.shared_buffer`, so we
        // use a swap to reach the expected setting.
        swap(&mut shared_buffer_to_sync, &mut self.shared_buffer);
        let insert_result = self.sync_uncommitted_data.insert(
            new_epoch,
            SyncUncommittedData::new(new_epoch, last_epoch, shared_buffer_to_sync),
        );
        assert_matches!(insert_result, None);
        self.max_sync_epoch = new_epoch;
    }

    pub fn get_prev_max_sync_epoch(&self, epoch: HummockEpoch) -> Option<HummockEpoch> {
        assert!(
            epoch <= self.max_sync_epoch,
            "call get prev max sync epoch on unsynced epoch: {}. max_sync_epoch {}",
            epoch,
            self.max_sync_epoch
        );
        self.sync_uncommitted_data
            .get(&epoch)
            .map(|data| data.prev_max_sync_epoch)
    }

    pub fn get_max_sync_epoch(&self) -> HummockEpoch {
        self.max_sync_epoch
    }

    pub fn get_mut_shared_buffer(&mut self, epoch: HummockEpoch) -> Option<&mut SharedBuffer> {
        if epoch > self.max_sync_epoch {
            self.shared_buffer.get_mut(&epoch)
        } else {
            for sync_data in self.sync_uncommitted_data.values_mut() {
                if let SyncUncommittedDataStage::CheckpointEpochSealed(shared_buffer_data) =
                    &mut sync_data.stage
                {
                    if let Some(shared_buffer) = shared_buffer_data.get_mut(&epoch) {
                        return Some(shared_buffer);
                    }
                }
            }
            None
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn get_shared_buffer(&self, epoch: HummockEpoch) -> Option<&SharedBuffer> {
        if epoch > self.max_sync_epoch {
            self.shared_buffer.get(&epoch)
        } else {
            for sync_data in self.sync_uncommitted_data.values() {
                if let SyncUncommittedDataStage::CheckpointEpochSealed(shared_buffer_data) =
                    &sync_data.stage
                {
                    if let Some(shared_buffer) = shared_buffer_data.get(&epoch) {
                        return Some(shared_buffer);
                    }
                }
            }
            None
        }
    }

    pub fn start_syncing(
        &mut self,
        sync_epoch: HummockEpoch,
    ) -> (OrderSortedUncommittedData, usize) {
        let data = self
            .sync_uncommitted_data
            .get_mut(&sync_epoch)
            .expect("should find");
        data.start_syncing()
    }

    pub fn data_synced(&mut self, sync_epoch: HummockEpoch, ssts: Vec<LocalSstableInfo>) {
        let data = self
            .sync_uncommitted_data
            .get_mut(&sync_epoch)
            .expect("should find");
        data.synced(ssts);
    }

    #[cfg(any(test, feature = "test"))]
    pub fn iter_shared_buffer(&self) -> impl Iterator<Item = (&HummockEpoch, &SharedBuffer)> {
        self.shared_buffer.iter().chain(
            self.sync_uncommitted_data
                .iter()
                .filter_map(|(_, data)| match &data.stage {
                    SyncUncommittedDataStage::CheckpointEpochSealed(shared_buffer_data) => {
                        Some(shared_buffer_data)
                    }
                    _ => None,
                })
                .flatten(),
        )
    }

    pub fn iter_mut_unsynced_shared_buffer(
        &mut self,
    ) -> impl Iterator<Item = (&HummockEpoch, &mut SharedBuffer)> {
        self.shared_buffer.iter_mut()
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
            assert!(self
                .shared_buffer
                .iter()
                .all(|(epoch, _)| *epoch > new_pinned_version.max_committed_epoch));
        }

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
        let (pinned_version, (shared_buffer_data, sync_uncommitted_data)) = {
            let guard = this.read();
            let smallest_uncommitted_epoch = guard.pinned_version.max_committed_epoch() + 1;
            let pinned_version = guard.pinned_version.clone();
            (
                pinned_version,
                if read_epoch >= smallest_uncommitted_epoch {
                    let shared_buffer_data = guard
                        .shared_buffer
                        .range(smallest_uncommitted_epoch..=read_epoch)
                        .rev() // Important: order by epoch descendingly
                        .map(|(_, shared_buffer)| shared_buffer.get_overlap_data(key_range))
                        .collect();
                    let sync_data: Vec<OrderSortedUncommittedData> = guard
                        .sync_uncommitted_data
                        .iter()
                        .rev() // Take rev so that newer epoch comes first
                        .filter(|(_, data)| {
                            if let Some(&min_epoch) = data.epochs.last() {
                                min_epoch <= read_epoch
                            } else {
                                false
                            }
                        })
                        .map(|(_, value)| value.get_overlap_data(key_range, read_epoch))
                        .collect();
                    RwLockReadGuard::unlock_fair(guard);
                    (shared_buffer_data, sync_data)
                } else {
                    RwLockReadGuard::unlock_fair(guard);
                    (Vec::new(), Vec::new())
                },
            )
        };

        ReadVersion {
            shared_buffer_data,
            pinned_version,
            sync_uncommitted_data,
        }
    }

    pub fn clear_shared_buffer(&mut self) -> Vec<HummockEpoch> {
        let mut cleaned_epoch = self.shared_buffer.keys().cloned().collect_vec();
        for data in self.sync_uncommitted_data.values() {
            for epoch in &data.epochs {
                cleaned_epoch.push(*epoch);
            }
        }
        self.sync_uncommitted_data.clear();
        self.shared_buffer.clear();
        cleaned_epoch
    }

    pub fn clear_committed_data(
        &mut self,
        max_committed_epoch: HummockEpoch,
    ) -> Vec<(Vec<HummockEpoch>, Vec<LocalSstableInfo>)> {
        match self.sync_uncommitted_data
            .iter()
            .rev() // Take rev so that newer epochs come first
            .find(|(sync_epoch, data)| {
            if data.epochs.is_empty() {
                **sync_epoch <= max_committed_epoch
            } else {
                let min_epoch = *data.epochs.last().expect("epoch list should not be empty");
                let max_epoch = *data.epochs.first().expect("epoch list should not be empty");
                assert!(
                    max_epoch <= max_committed_epoch || min_epoch > max_committed_epoch,
                    "new_max_committed_epoch {} lays within max_epoch {} and min_epoch {} of data {:?}",
                    max_committed_epoch,
                    max_epoch,
                    min_epoch,
                    data,
                );
                max_epoch <= max_committed_epoch
            }
        }) {
            Some((&sync_epoch, _)) => {
                let mut synced_ssts = self
                    .sync_uncommitted_data
                    .drain_filter(|&epoch, _| epoch <= sync_epoch)
                    .map(|(_, data)| {
                        (
                            data.epochs,
                            match data.stage {
                                SyncUncommittedDataStage::Synced(ssts) => ssts,
                                invalid_stage => {
                                    unreachable!("expect synced. Now is {:?}", invalid_stage)
                                }
                            },
                        )
                    })
                    .collect_vec();
                synced_ssts.reverse(); // Take reverse so that newer epoch comes first
                synced_ssts
            },
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
        version.safe_epoch = version_delta.safe_epoch;

        clean_epochs
    }
}

struct PinnedVersionGuard {
    version_id: HummockVersionId,
    pinned_version_manager_tx: UnboundedSender<PinVersionAction>,
}

impl PinnedVersionGuard {
    /// Creates a new `PinnedVersionGuard` and send a pin request to `pinned_version_worker`.
    fn new(
        version_id: HummockVersionId,
        pinned_version_manager_tx: UnboundedSender<PinVersionAction>,
    ) -> Self {
        if pinned_version_manager_tx
            .send(PinVersionAction::Pin(version_id))
            .is_err()
        {
            tracing::warn!("failed to send req pin version id{}", version_id);
        }

        Self {
            version_id,
            pinned_version_manager_tx,
        }
    }
}

impl Drop for PinnedVersionGuard {
    fn drop(&mut self) {
        if self
            .pinned_version_manager_tx
            .send(PinVersionAction::Unpin(self.version_id))
            .is_err()
        {
            tracing::warn!("failed to send req unpin version id:{}", self.version_id);
        }
    }
}

#[derive(Clone)]
pub struct PinnedVersion {
    version: Arc<HummockVersion>,
    guard: Arc<PinnedVersionGuard>,
}

impl PinnedVersion {
    fn new(
        version: HummockVersion,
        pinned_version_manager_tx: UnboundedSender<PinVersionAction>,
    ) -> Self {
        let version_id = version.id;

        PinnedVersion {
            version: Arc::new(version),
            guard: Arc::new(PinnedVersionGuard::new(
                version_id,
                pinned_version_manager_tx,
            )),
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
            guard: Arc::new(PinnedVersionGuard::new(
                version_id,
                self.guard.pinned_version_manager_tx.clone(),
            )),
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

    pub fn is_valid(&self) -> bool {
        self.version.id != INVALID_VERSION_ID
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

    pub fn safe_epoch(&self) -> u64 {
        self.version.safe_epoch
    }

    /// ret value can't be used as `HummockVersion`. it must be modified with delta
    pub fn version(&self) -> HummockVersion {
        self.version.deref().clone()
    }
}

pub struct ReadVersion {
    // The shared buffers are sorted by epoch descendingly
    pub shared_buffer_data: Vec<OrderSortedUncommittedData>,
    pub pinned_version: PinnedVersion,
    pub sync_uncommitted_data: Vec<OrderSortedUncommittedData>,
}
