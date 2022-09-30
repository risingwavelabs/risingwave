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

    /// Pin the exact snapshot for the given epoch.
    pub async fn pin(&self, prev_epoch: Epoch) -> MetaResult<()> {
        let snapshot = self.hummock_manager.pin_snapshot(META_NODE_ID).await?;

        // We require the caller to ensure the "exactness".
        assert_eq!(snapshot.committed_epoch, prev_epoch.0);
        assert_eq!(snapshot.current_epoch, prev_epoch.0);

        let mut snapshots = self.snapshots.lock().await;
        if let Some((&last_epoch, _)) = snapshots.last_key_value() {
            assert!(last_epoch < prev_epoch);
        }
        snapshots.try_insert(prev_epoch, snapshot).unwrap();

        Ok(())
    }

    /// Unpin the snapshot for the given epoch.
    pub async fn unpin(&self, prev_epoch: Epoch) -> MetaResult<()> {
        let mut snapshots = self.snapshots.lock().await;
        let min_pinned_epoch = *snapshots.first_key_value().expect("no snapshot to unpin").0;
        snapshots.remove(&prev_epoch).expect("snapshot not exists");

        match snapshots.first_key_value() {
            // The watermark unchanged, do nothing.
            Some((&new_min_pinned_epoch, _)) if new_min_pinned_epoch == min_pinned_epoch => {}

            // The watermark bumped, unpin the snapshot before the new watermark.
            Some((_, min_snapshot)) => {
                self.hummock_manager
                    .unpin_snapshot_before(META_NODE_ID, min_snapshot.clone())
                    .await?
            }

            // No snapshot left, unpin all snapshots.
            None => self.hummock_manager.unpin_snapshot(META_NODE_ID).await?,
        }

        Ok(())
    }
}
