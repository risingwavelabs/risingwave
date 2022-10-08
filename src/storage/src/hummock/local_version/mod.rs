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

use risingwave_hummock_sdk::{HummockEpoch, LocalSstableInfo};

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::shared_buffer::{OrderSortedUncommittedData, SharedBuffer};
use crate::hummock::HummockResult;

mod flush_controller;
pub mod local_version_impl;
pub mod local_version_manager;
pub mod pinned_version;

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

#[derive(Debug)]
pub enum SyncUncommittedDataStage {
    /// Before we start syncing, we need to mv the shared buffer to `sync_uncommitted_data` and
    /// wait for flush task to complete
    CheckpointEpochSealed(Arc<BTreeMap<HummockEpoch, SharedBuffer>>),
    InMemoryMerge(Vec<SharedBufferBatch>),
    /// Task payload when we start syncing
    Syncing(Vec<Vec<SharedBufferBatch>>),
    /// After we finish syncing, we changed `Syncing` to `Synced`.
    Synced(Vec<LocalSstableInfo>, usize),
}

#[derive(Debug)]
pub struct SyncUncommittedData {
    #[allow(dead_code)]
    sync_epoch: HummockEpoch,
    // newer epochs come first
    epochs: Vec<HummockEpoch>,
    stage: SyncUncommittedDataStage,
    ret: HummockResult<()>,
}

pub struct ReadVersion {
    // The shared buffers are sorted by epoch descendingly
    pub shared_buffer_data: Vec<Vec<SharedBufferBatch>>,
    pub pinned_version: PinnedVersion,
    pub sync_uncommitted_data: Vec<OrderSortedUncommittedData>,
}
