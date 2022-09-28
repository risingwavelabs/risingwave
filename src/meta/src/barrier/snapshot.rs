use std::collections::BTreeMap;

use risingwave_common::util::epoch::Epoch;
use risingwave_pb::hummock::HummockSnapshot;
use tokio::sync::Mutex;

use crate::hummock::HummockManager;
use crate::manager::META_NODE_ID;
use crate::storage::MetaStore;
use crate::MetaResult;

pub struct SnapshotManager<S: MetaStore> {
    hummock_manager: HummockManager<S>,

    snapshots: Mutex<BTreeMap<Epoch, HummockSnapshot>>,
}

impl<S: MetaStore> SnapshotManager<S> {
    pub async fn pin(&self, epoch: Epoch) -> MetaResult<()> {
        let snapshot = self.hummock_manager.pin_snapshot(META_NODE_ID).await?;
        assert_eq!(snapshot.committed_epoch, epoch.0);
        assert_eq!(snapshot.current_epoch, epoch.0);

        let mut snapshots = self.snapshots.lock().await;
        if let Some((&last_epoch, _)) = snapshots.last_key_value() {
            assert!(last_epoch < epoch);
        }
        snapshots.try_insert(epoch, snapshot).unwrap();

        Ok(())
    }

    pub async fn unpin(&self, epoch: Epoch) -> MetaResult<()> {
        let mut snapshots = self.snapshots.lock().await;
        let min_pinned_epoch = *snapshots.first_key_value().expect("no snapshot to unpin").0;
        snapshots.remove(&epoch).expect("snapshot not exists");

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
