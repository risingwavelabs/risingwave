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
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, HummockVersionId};
use risingwave_pb::hummock::{HummockVersion, Level};
use tokio::sync::mpsc::UnboundedSender;

use super::shared_buffer::SharedBuffer;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::shared_buffer::{
    KeyIndexedUncommittedData, OrderSortedUncommittedData, UncommittedData,
};
use crate::hummock::utils::{filter_single_sst, range_overlap};

#[derive(Debug, Clone)]
pub struct LocalVersion {
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
    Syncing(Vec<KeyIndexedUncommittedData>),
    /// After we finish syncing, we changed `Syncing` to `Synced`.
    Synced(Vec<UncommittedData>),
}

impl SyncUncommittedData {
    pub fn get_overlap_data<R, B>(&self, key_range: &R) -> OrderSortedUncommittedData
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        match self {
            SyncUncommittedData::Syncing(task) => {
                let local_data_iter = task
                    .iter()
                    .flatten()
                    .filter(|(_, data)| match data {
                        UncommittedData::Batch(batch) => {
                            range_overlap(key_range, batch.start_user_key(), batch.end_user_key())
                        }
                        UncommittedData::Sst((_, info)) => filter_single_sst(info, key_range),
                    })
                    .map(|((_, order_index), data)| (*order_index, data.clone()));

                let mut uncommitted_data = BTreeMap::new();
                for (order_index, data) in local_data_iter {
                    uncommitted_data
                        .entry(order_index)
                        .or_insert_with(Vec::new)
                        .push(data);
                }
                uncommitted_data.into_values().rev().collect()
            }
            SyncUncommittedData::Synced(ssts) => vec![ssts
                .iter()
                .filter(|data| match data {
                    UncommittedData::Batch(_) => {
                        panic!("sync uncommitted states can't save batch")
                    }
                    UncommittedData::Sst((_, info)) => filter_single_sst(info, key_range),
                })
                .cloned()
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

    pub fn get_mut_shared_buffer(&mut self, epoch: HummockEpoch) -> Option<&mut SharedBuffer> {
        self.shared_buffer.get_mut(&epoch)
    }

    pub fn get_shared_buffer(&self, epoch: HummockEpoch) -> Option<&SharedBuffer> {
        self.shared_buffer.get(&epoch)
    }

    /// Returns all shared buffer less than or equal to epoch
    pub fn scan_mut_shared_buffer<R>(
        &mut self,
        epoch_range: R,
    ) -> impl Iterator<Item = (&HummockEpoch, &mut SharedBuffer)>
    where
        R: RangeBounds<u64>,
    {
        self.shared_buffer.range_mut(epoch_range)
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
            cleaned_epoch.append(
                &mut self
                    .shared_buffer
                    .keys()
                    .filter(|e| **e <= new_pinned_version.max_committed_epoch)
                    .cloned()
                    .collect_vec(),
            );
            self.shared_buffer
                .retain(|epoch, _| epoch > &new_pinned_version.max_committed_epoch);
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
        let (pinned_version, (shared_buffer_datas, sync_uncommitted_datas)) = {
            let guard = this.read();
            let smallest_uncommitted_epoch = guard.pinned_version.max_committed_epoch() + 1;
            let pinned_version = guard.pinned_version.clone();
            (
                pinned_version,
                if read_epoch >= smallest_uncommitted_epoch {
                    let result = guard
                        .shared_buffer
                        .range(smallest_uncommitted_epoch..=read_epoch)
                        .rev() // Important: order by epoch descendingly
                        .map(|(_, shared_buffer)| shared_buffer.get_overlap_data(key_range))
                        .collect();
                    let result_sync: Vec<OrderSortedUncommittedData> = guard
                        .sync_uncommitted_data
                        .iter()
                        .filter(|&node| {
                            node.0.first().le(&Some(&read_epoch))
                                && node.0.first().ge(&Some(&smallest_uncommitted_epoch))
                        })
                        .map(|(_, value)| value.get_overlap_data(key_range))
                        .collect();
                    RwLockReadGuard::unlock_fair(guard);
                    (result, result_sync)
                } else {
                    RwLockReadGuard::unlock_fair(guard);
                    (Vec::new(), Vec::new())
                },
            )
        };

        ReadVersion {
            shared_buffer_datas,
            pinned_version,
            sync_uncommitted_datas,
        }
    }

    pub fn clear_shared_buffer(&mut self) -> Vec<HummockEpoch> {
        let cleaned_epochs = self.shared_buffer.keys().cloned().collect_vec();
        self.shared_buffer.clear();
        cleaned_epochs
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
    /// The shared buffer is sorted by epoch descendingly
    pub shared_buffer_datas: Vec<(Vec<SharedBufferBatch>, OrderSortedUncommittedData)>,
    pub pinned_version: Arc<PinnedVersion>,
    pub sync_uncommitted_datas: Vec<OrderSortedUncommittedData>,
}
