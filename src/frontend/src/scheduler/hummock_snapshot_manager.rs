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
            core: Mutex::new(HummockSnapshotManagerCore::default()),
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
            core_guard.quote_number.insert(epoch, 1);
        } else {
            let last_pinned = core_guard.last_pinned;
            *core_guard.quote_number.get_mut(&last_pinned).unwrap() += 1;
        }
        Ok(core_guard.last_pinned)
    }

    pub async fn unpin_snapshot(&self, epoch: u64) -> Result<()> {
        let mut core_guard = self.core.lock().await;
        let quote_number = core_guard.quote_number.get_mut(&epoch);
        if let Some(quote_number) = quote_number {
            *quote_number -= 1;
            if *quote_number == 0 {
                self.meta_client.unpin_snapshot(epoch).await?;
                core_guard.quote_number.remove(&epoch);
                core_guard.is_outdated = epoch == core_guard.last_pinned;
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
    quote_number: HashMap<u64, u32>,
}
