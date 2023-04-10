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

use std::collections::BTreeMap;
use std::sync::Arc;

use risingwave_common::util::epoch::Epoch;
use risingwave_pb::hummock::HummockSnapshot;
use tokio::sync::Mutex;

use crate::hummock::HummockManagerRef;
use crate::manager::META_NODE_ID;
use crate::storage::MetaStore;
use crate::MetaResult;

pub(super) type SnapshotManagerRef<S> = Arc<SnapshotManager<S>>;

/// A simple manager for pinning and unpinning snapshots for mview creation in barrier manager.
pub(super) struct SnapshotManager<S: MetaStore> {
    hummock_manager: HummockManagerRef<S>,

    /// The snapshot pinned for each epoch. Note that the epoch of each DDL is unique.
    snapshots: Mutex<BTreeMap<Epoch, HummockSnapshot>>,
}

impl<S: MetaStore> SnapshotManager<S> {
    pub fn new(hummock_manager: HummockManagerRef<S>) -> Self {
        Self {
            hummock_manager,
            snapshots: Default::default(),
        }
    }

    pub async fn unpin_all(&self) -> MetaResult<()> {
        let mut snapshots = self.snapshots.lock().await;
        self.hummock_manager.unpin_snapshot(META_NODE_ID).await?;
        snapshots.clear();
        Ok(())
    }
}
