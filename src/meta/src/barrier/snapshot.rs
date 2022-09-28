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

pub(super) struct SnapshotManager<S: MetaStore> {
    hummock_manager: HummockManagerRef<S>,

    snapshots: Mutex<BTreeMap<Epoch, HummockSnapshot>>,
}

impl<S: MetaStore> SnapshotManager<S> {
    pub fn new(hummock_manager: HummockManagerRef<S>) -> Self {
        Self {
            hummock_manager,
            snapshots: Default::default(),
        }
    }

    pub async fn pin(&self, prev_epoch: Epoch) -> MetaResult<()> {
        let snapshot = self.hummock_manager.pin_snapshot(META_NODE_ID).await?;
        assert_eq!(snapshot.committed_epoch, prev_epoch.0);
        assert_eq!(snapshot.current_epoch, prev_epoch.0);

        let mut snapshots = self.snapshots.lock().await;
        if let Some((&last_epoch, _)) = snapshots.last_key_value() {
            assert!(last_epoch < prev_epoch);
        }
        snapshots.try_insert(prev_epoch, snapshot).unwrap();

        Ok(())
    }

    pub async fn unpin(&self, prev_epoch: Epoch) -> MetaResult<()> {
        let mut snapshots = self.snapshots.lock().await;
        let min_pinned_epoch = *snapshots.first_key_value().expect("no snapshot to unpin").0;
        snapshots.remove(&prev_epoch).expect("snapshot not exists");

        match snapshots.first_key_value() {
            Some((&new_min_pinned_epoch, _)) if new_min_pinned_epoch == min_pinned_epoch => {}
            Some((_, min_snapshot)) => {
                self.hummock_manager
                    .unpin_snapshot_before(META_NODE_ID, min_snapshot.clone())
                    .await?
            }
            None => self.hummock_manager.unpin_snapshot(META_NODE_ID).await?,
        }

        Ok(())
    }
}
