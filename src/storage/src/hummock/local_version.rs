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
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeBounds;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, HummockVersionId, LocalSstableInfo};
use risingwave_pb::hummock::{HummockVersion, Level};
use tokio::sync::mpsc::UnboundedSender;

use super::shared_buffer::SharedBuffer;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::shared_buffer::{
    KeyIndexSharedBufferBatch, OrderSortedUncommittedData, UncommittedData,
};
use crate::hummock::utils::{filter_single_sst, range_overlap};

#[derive(Debug, Clone)]
pub struct LocalVersion {
    replicated_batches: BTreeMap<HummockEpoch, KeyIndexSharedBufferBatch>,
    shared_buffer: BTreeMap<HummockEpoch, SharedBuffer>,
    pinned_version: Arc<PinnedVersion>,
    pub version_ids_in_use: BTreeSet<HummockVersionId>,
    // TODO: save uncommitted data that needs to be flushed to disk.
    /// Save uncommitted data that needs to be synced or finished syncing.
    pub sync_uncommitted_data: Vec<(Vec<HummockEpoch>, SyncUncommittedData)>,
    max_sync_epoch: u64,
}

#[derive(Debug, Clone)]
pub enum SyncUncommittedData {
    /// Before we start syncing, we need to mv data from shared buffer to `sync_uncommitted_data`
    /// as `Syncing`.
    Syncing(OrderSortedUncommittedData),
    /// After we finish syncing, we changed `Syncing` to `Synced`.
    Synced(Vec<LocalSstableInfo>),
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
        Self {
            replicated_batches: BTreeMap::default(),
            shared_buffer: BTreeMap::default(),
            pinned_version: Arc::new(PinnedVersion::new(version, unpin_worker_tx)),
            version_ids_in_use,
            sync_uncommitted_data: Default::default(),
            max_sync_epoch: 0,
        }
    }

    pub fn pinned_version(&self) -> &Arc<PinnedVersion> {
        &self.pinned_version
    }

    pub fn swap_max_sync_epoch(&mut self, epoch: HummockEpoch) -> Option<HummockEpoch> {
        if self.max_sync_epoch > epoch {
            None
        } else {
            let last_epoch = self.max_sync_epoch;
            self.max_sync_epoch = epoch;
            Some(last_epoch)
        }
    }

    pub fn get_max_sync_epoch(&self) -> HummockEpoch {
        self.max_sync_epoch
    }

    pub fn get_mut_shared_buffer(&mut self, epoch: HummockEpoch) -> Option<&mut SharedBuffer> {
        self.shared_buffer.get_mut(&epoch)
    }

    pub fn get_shared_buffer(&self, epoch: HummockEpoch) -> Option<&SharedBuffer> {
        self.shared_buffer.get(&epoch)
    }

    /// Returns all shared buffer less than or equal to epoch
    pub fn drain_shared_buffer<'a, R>(
        &'a mut self,
        epoch_range: R,
    ) -> impl Iterator<Item = (HummockEpoch, SharedBuffer)> + 'a
    where
        R: RangeBounds<u64> + 'a,
    {
        self.shared_buffer
            .drain_filter(move |epoch, _| epoch_range.contains(epoch))
    }

    /// Returns all shared buffer less than or equal to epoch
    pub fn scan_shared_buffer<R>(
        &self,
        epoch_range: R,
    ) -> impl Iterator<Item = (&HummockEpoch, &SharedBuffer)>
    where
        R: RangeBounds<u64>,
    {
        self.shared_buffer.range(epoch_range)
    }

    pub fn add_sync_state(
        &mut self,
        sync_epoch: Vec<HummockEpoch>,
        sync_uncommitted_data: SyncUncommittedData,
    ) {
        let node = self
            .sync_uncommitted_data
            .iter_mut()
            .find(|(epoch, _)| epoch == &sync_epoch);
        match &node {
            None => {
                assert_matches!(sync_uncommitted_data, SyncUncommittedData::Syncing(_));
                if let Some(last) = self.sync_uncommitted_data.last() {
                    assert!(
                        last.0.first().lt(&sync_epoch.first()),
                        "last epoch:{:?} >= sync epoch:{:?}",
                        last,
                        sync_epoch
                    );
                }
                self.sync_uncommitted_data
                    .push((sync_epoch, sync_uncommitted_data));
                return;
            }
            Some((_, SyncUncommittedData::Syncing(_))) => {
                assert_matches!(sync_uncommitted_data, SyncUncommittedData::Synced(_));
            }
            Some((_, SyncUncommittedData::Synced(_))) => {
                panic!("sync over, can't modify uncommitted sst state");
            }
        }
        *node.unwrap() = (sync_epoch, sync_uncommitted_data);
    }

    pub fn iter_shared_buffer(&self) -> impl Iterator<Item = (&HummockEpoch, &SharedBuffer)> {
        self.shared_buffer.iter()
    }

    pub fn iter_mut_shared_buffer(
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
    pub fn set_pinned_version(&mut self, new_pinned_version: HummockVersion) -> Vec<HummockEpoch> {
        // Clean shared buffer and uncommitted ssts below (<=) new max committed epoch
        let mut cleaned_epoch = vec![];
        if self.pinned_version.max_committed_epoch() < new_pinned_version.max_committed_epoch {
            for (epochs, _) in &self.sync_uncommitted_data {
                for epoch in epochs {
                    if *epoch <= new_pinned_version.max_committed_epoch {
                        cleaned_epoch.push(*epoch);
                    }
                }
            }

            self.replicated_batches
                .retain(|epoch, _| *epoch > new_pinned_version.max_committed_epoch);
            assert!(self
                .shared_buffer
                .iter()
                .all(|(epoch, _)| *epoch > new_pinned_version.max_committed_epoch));
            self.sync_uncommitted_data.retain(|(epoch, _)| {
                epoch
                    .last()
                    .gt(&Some(&new_pinned_version.max_committed_epoch))
            });
        }

        self.version_ids_in_use.insert(new_pinned_version.id);

        // update pinned version
        self.pinned_version = Arc::new(PinnedVersion {
            version: new_pinned_version,
            unpin_worker_tx: self.pinned_version.unpin_worker_tx.clone(),
        });
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
                            node.0.first().le(&Some(&read_epoch))
                                && node.0.first().ge(&Some(&smallest_uncommitted_epoch))
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
}

#[derive(Debug)]
pub struct PinnedVersion {
    version: HummockVersion,
    unpin_worker_tx: UnboundedSender<HummockVersionId>,
}

impl Drop for PinnedVersion {
    fn drop(&mut self) {
        self.unpin_worker_tx.send(self.version.id).ok();
    }
}

impl PinnedVersion {
    fn new(
        version: HummockVersion,
        unpin_worker_tx: UnboundedSender<HummockVersionId>,
    ) -> PinnedVersion {
        PinnedVersion {
            version,
            unpin_worker_tx,
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
        self.version.clone()
    }
}

pub struct ReadVersion {
    // The replicated batches are sorted by epoch descendingly
    pub replicated_batches: Vec<Vec<SharedBufferBatch>>,
    // The shared buffers are sorted by epoch descendingly
    pub shared_buffer_data: Vec<OrderSortedUncommittedData>,
    pub pinned_version: Arc<PinnedVersion>,
    pub sync_uncommitted_data: Vec<OrderSortedUncommittedData>,
}
