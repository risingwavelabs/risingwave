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
use std::sync::Arc;

use parking_lot::lock_api::ArcRwLockReadGuard;
use parking_lot::{RawRwLock, RwLock};
use risingwave_hummock_sdk::{HummockEpoch, HummockVersionId};
use risingwave_pb::hummock::{HummockVersion, Level, SstableInfo};
use tokio::sync::mpsc::UnboundedSender;

use super::shared_buffer::SharedBuffer;

pub type UncommittedSsts = BTreeMap<HummockEpoch, Vec<SstableInfo>>;

#[derive(Debug, Clone)]
pub struct LocalVersion {
    shared_buffer: BTreeMap<HummockEpoch, Arc<RwLock<SharedBuffer>>>,
    uncommitted_ssts: UncommittedSsts,
    pinned_version: Arc<PinnedVersion>,
}

impl LocalVersion {
    pub fn new(
        version: HummockVersion,
        unpin_worker_tx: UnboundedSender<HummockVersionId>,
    ) -> Self {
        Self {
            shared_buffer: BTreeMap::default(),
            uncommitted_ssts: UncommittedSsts::default(),
            pinned_version: Arc::new(PinnedVersion::new(version, unpin_worker_tx)),
        }
    }

    pub fn pinned_version(&self) -> &Arc<PinnedVersion> {
        &self.pinned_version
    }

    pub fn get_shared_buffer(&self, epoch: HummockEpoch) -> Option<&Arc<RwLock<SharedBuffer>>> {
        self.shared_buffer.get(&epoch)
    }

    pub fn iter_shared_buffer(
        &self,
    ) -> impl Iterator<Item = (&HummockEpoch, &Arc<RwLock<SharedBuffer>>)> {
        self.shared_buffer.iter()
    }

    pub fn new_shared_buffer(&mut self, epoch: HummockEpoch) -> Arc<RwLock<SharedBuffer>> {
        self.shared_buffer
            .entry(epoch)
            .or_insert_with(|| Arc::new(RwLock::new(SharedBuffer::default())))
            .clone()
    }

    pub fn add_uncommitted_ssts(&mut self, epoch: HummockEpoch, ssts: Vec<SstableInfo>) {
        self.uncommitted_ssts.entry(epoch).or_default().extend(ssts);
    }

    pub fn set_pinned_version(&mut self, new_pinned_version: HummockVersion) {
        // Clean shared buffer and uncommitted ssts below (<=) new max committed epoch
        if self.pinned_version.max_committed_epoch() < new_pinned_version.max_committed_epoch {
            self.uncommitted_ssts = self
                .uncommitted_ssts
                .split_off(&(new_pinned_version.max_committed_epoch + 1));
            let mut buffer_to_release = self
                .shared_buffer
                .split_off(&(new_pinned_version.max_committed_epoch + 1));
            // buffer_to_release = older part, self.shared_buffer = new part
            std::mem::swap(&mut buffer_to_release, &mut self.shared_buffer);
        }

        // update pinned version
        self.pinned_version = Arc::new(PinnedVersion {
            version: new_pinned_version,
            unpin_worker_tx: self.pinned_version.unpin_worker_tx.clone(),
        });
    }

    pub fn read_version(&self, read_epoch: HummockEpoch) -> ReadVersion {
        let smallest_uncommitted_epoch = self.pinned_version.max_committed_epoch() + 1;
        ReadVersion {
            shared_buffer: if read_epoch >= smallest_uncommitted_epoch {
                self.shared_buffer
                    .range(smallest_uncommitted_epoch..=read_epoch)
                    .rev() // Important: order by epoch descendingly
                    .map(|e| e.1.read_arc())
                    .collect()
            } else {
                Vec::new()
            },
            uncommitted_ssts: if read_epoch >= smallest_uncommitted_epoch {
                self.uncommitted_ssts
                    .range(smallest_uncommitted_epoch..=read_epoch)
                    .rev() // Important: order by epoch descendingly
                    .flat_map(|s| s.1.clone())
                    .collect()
            } else {
                Vec::new()
            },
            pinned_version: self.pinned_version.clone(),
        }
    }

    pub fn get_uncommitted_ssts(&self) -> UncommittedSsts {
        self.uncommitted_ssts.clone()
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

    pub fn levels(&self) -> &Vec<Level> {
        &self.version.levels
    }

    pub fn max_committed_epoch(&self) -> u64 {
        self.version.max_committed_epoch
    }

    pub fn safe_epoch(&self) -> u64 {
        self.version.safe_epoch
    }

    #[cfg(test)]
    pub fn version(&self) -> HummockVersion {
        self.version.clone()
    }
}

pub struct ReadVersion {
    pub shared_buffer: Vec<ArcRwLockReadGuard<RawRwLock, SharedBuffer>>,
    pub uncommitted_ssts: Vec<SstableInfo>,
    pub pinned_version: Arc<PinnedVersion>,
}
