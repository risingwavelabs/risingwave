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

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::error::Result;
use tokio::sync::Mutex;

use crate::meta_client::FrontendMetaClient;

/// Cache of hummock snapshot in meta.
pub struct HummockSnapshotManager {
    core: Mutex<HummockSnapshotManagerCore>,
    meta_client: Arc<dyn FrontendMetaClient>,
}
pub type HummockSnapshotManagerRef = Arc<HummockSnapshotManager>;

impl HummockSnapshotManager {
    pub fn new(meta_client: Arc<dyn FrontendMetaClient>) -> Self {
        Self {
            core: Mutex::new(HummockSnapshotManagerCore::new()),
            meta_client,
        }
    }

    pub async fn get_epoch(&self) -> Result<u64> {
        let mut core_guard = self.core.lock().await;
        if core_guard.is_outdated {
            let epoch = self
                .meta_client
                .pin_snapshot(core_guard.last_pinned)
                .await?;
            core_guard.is_outdated = false;
            core_guard.last_pinned = epoch;
            core_guard.reference_number.insert(epoch, 1);
        } else {
            let last_pinned = core_guard.last_pinned;
            *core_guard.reference_number.get_mut(&last_pinned).unwrap() += 1;
        }
        Ok(core_guard.last_pinned)
    }

    pub async fn unpin_snapshot(&self, epoch: u64) -> Result<()> {
        let mut core_guard = self.core.lock().await;
        let reference_number = core_guard.reference_number.get_mut(&epoch);
        if let Some(reference_number) = reference_number {
            *reference_number -= 1;
            if *reference_number == 0 {
                self.meta_client.unpin_snapshot(epoch).await?;
                core_guard.reference_number.remove(&epoch);
                if epoch == core_guard.last_pinned {
                    core_guard.is_outdated = true;
                }
            }
        }
        Ok(())
    }

    /// Used in `ObserverManager`.
    pub async fn update_snapshot_status(&self, epoch: u64) {
        let mut core_guard = self.core.lock().await;
        if core_guard.last_pinned < epoch {
            core_guard.is_outdated = true;
        }
    }
}

#[derive(Default)]
struct HummockSnapshotManagerCore {
    is_outdated: bool,
    last_pinned: u64,
    /// Record the number of references of snapshot. Send an `unpin_snapshot` RPC when the number
    /// drops to 0.
    reference_number: HashMap<u64, u32>,
}

impl HummockSnapshotManagerCore {
    fn new() -> Self {
        Self {
            // Initialize by setting `is_outdated` to `true`.
            is_outdated: true,
            ..Default::default()
        }
    }
}
