use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;

use parking_lot::Mutex as PLMutex;
use tokio::sync::Mutex;

use super::key_range::KeyRange;
use super::{HummockError, HummockResult, Table};

#[derive(Clone)]
enum Level {
    Leveling(Vec<u64>),
    Tiering(Vec<u64>),
}
#[derive(Clone)]
struct TableStat {
    key_range: KeyRange,
    table_id: u64,
    compact_task: Option<u64>,
}
#[derive(Clone)]
enum LevelHandler {
    Leveling(Vec<TableStat>),
    Tiering(Vec<TableStat>),
}

/// We store the full information of a snapshot in one [`Snapshot`] object. In the future, we should
/// implement a MVCC structure for this.
#[derive(Clone, Default)]
struct Snapshot {
    /// Table IDs in this snapshot. We **only store ID** in snapshot, we need to get the actual
    /// objects from version manager later.
    levels: Vec<Level>,
}

#[derive(Default)]
struct VersionManagerInner {
    /// To make things easy, we store the full snapshot of each epoch. In the future, we will use a
    /// MVCC structure for this, and only record changes compared with last epoch.
    status: HashMap<u64, Arc<Snapshot>>,

    /// TableId -> Object mapping
    tables: HashMap<u64, Arc<Table>>,

    /// Reference count of each epoch.
    ref_cnt: BTreeMap<u64, usize>,

    /// Deletion to apply in each epoch.
    table_deletion_to_apply: BTreeMap<u64, Vec<u64>>,

    /// Current epoch number.
    epoch: u64,
}

/// Manages the state history of the storage engine and vacuum the stale files in storage.
///
/// Generally, when a query starts, it will take a snapshot and store the state of the
/// LSM at the time of starting. As the query is running, new Tables are added and old Tables
/// will no longer be used. So how do we know that we can safely remove a Table file?
///
/// [`VersionManager`] manages all Tables in a multi-version way. Everytime there are some
/// changes in the storage engine, [`VersionManager`] should be notified about this change,
/// and handle out a epoch number for that change. For example,
///
/// * (epoch 0) Table 1, 2
/// * (engine) add Table 3, remove Table 1
/// * (epoch 1) Table 2, 3
///
/// Each history state will be associated with an epoch number, which will be used by
/// snapshots. When a snapshot is taken, it will "pin" an epoch number. Tables logically
/// deleted after that epoch won't be deleted physically until the snapshot "unpins" the
/// epoch number.
///
/// Therefore, [`VersionManager`] is the manifest manager of the whole storage system,
/// which reads and writes manifest, manages all in-storage files and vacuum them when no
/// snapshot holds the corresponding epoch of the file.
///
/// The design choice of separating [`VersionManager`] out of the storage engine is a
/// preparation for a distributed storage engine. In such distributed engine, there will
/// generally be some kind of "`MetadataManager`" which does all of the things that our
/// [`VersionManager`] do.
pub struct VersionManager {
    /// Inner structure of [`VersionManager`]. This structure is protected by a parking lot Mutex,
    /// so as to support quick lock and unlock.
    inner: PLMutex<VersionManagerInner>,

    /// Current compaction status.
    compact_status: Mutex<Vec<LevelHandler>>,

    /// Notify the vacuum to apply changes from one epoch.
    tx: tokio::sync::mpsc::UnboundedSender<()>,

    /// Receiver of the vacuum.
    rx: PLMutex<Option<tokio::sync::mpsc::UnboundedReceiver<()>>>,
}

impl VersionManager {
    pub fn new() -> Self {
        let mut init_epoch_vm = VersionManagerInner::default();
        *init_epoch_vm
            .ref_cnt
            .entry(init_epoch_vm.epoch)
            .or_default() += 1;
        init_epoch_vm.status.insert(
            init_epoch_vm.epoch,
            Arc::new(Snapshot {
                levels: vec![Level::Tiering(vec![])],
            }),
        );

        let vec_handler_having_l0 = vec![LevelHandler::Tiering(vec![])];

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
            inner: PLMutex::new(init_epoch_vm),
            compact_status: Mutex::new(vec_handler_having_l0),
            tx,
            rx: PLMutex::new(Some(rx)),
        }
    }

    /// Pin a snapshot of one epoch, so that all files at this epoch won't be deleted.
    fn pin(&self) -> (u64, Arc<Snapshot>) {
        let mut inner = self.inner.lock();
        let epoch = inner.epoch;
        *inner.ref_cnt.entry(epoch).or_default() += 1;
        (epoch, inner.status.get(&epoch).unwrap().clone())
    }

    /// Unpin a snapshot of one epoch. When reference counter becomes 0, files might be vacuumed.
    fn unpin_inner(&self, inner: &mut VersionManagerInner, epoch: u64) {
        let ref_cnt = inner
            .ref_cnt
            .get_mut(&epoch)
            .expect("epoch not registered!");
        *ref_cnt -= 1;
        if *ref_cnt == 0 {
            inner.ref_cnt.remove(&epoch).unwrap();

            if epoch != inner.epoch {
                // TODO: precisely pass the epoch number that can be vacuum.
                self.tx.send(()).unwrap();
            }
        }
    }

    fn unpin(&self, option_inner: Option<&mut VersionManagerInner>, epoch: u64) {
        match option_inner {
            Some(inner) => {
                self.unpin_inner(inner, epoch);
            }
            None => {
                self.unpin_inner(&mut self.inner.lock(), epoch);
            }
        }
    }

    /// Add a L0 SST and return a new epoch number
    pub async fn add_l0_sst(&self, table: Table) -> HummockResult<u64> {
        let table_id = table.id;
        let smallest_ky = Bytes::copy_from_slice(&table.meta.smallest_key);
        let largest_ky = Bytes::copy_from_slice(&table.meta.largest_key);

        // Hold the compact status lock so that no one else could add/drop SST or search compaction
        // plan.
        let mut compact_status = self.compact_status.lock().await;

        let epoch;

        {
            // Hold the inner lock, so as to apply the changes to the current status, and add new
            // L0 SST to the LSM. This lock is released before triggering searching for compaction
            // plan.
            let mut inner = self.inner.lock();
            let old_epoch = inner.epoch;

            // Get snapshot of latest version.
            let mut snapshot = inner
                .status
                .get(&old_epoch)
                .map(|x| x.as_ref().clone())
                .unwrap();

            if inner.tables.insert(table_id, Arc::new(table)).is_some() {
                return Err(HummockError::ObjectIoError(String::from(
                    "Table ID to be created already exists.",
                )));
            }
            match snapshot.levels.first_mut().unwrap() {
                Level::Tiering(vec_tier) => {
                    vec_tier.push(table_id);
                }
                Level::Leveling(_) => {
                    unimplemented!();
                }
            }

            // Add epoch number and make the modified snapshot available.
            inner.epoch += 1;
            epoch = inner.epoch;
            *inner.ref_cnt.entry(epoch).or_default() += 1;
            inner.status.insert(epoch, Arc::new(snapshot));

            self.unpin(Some(&mut inner), old_epoch);
        }

        match compact_status.first_mut().unwrap() {
            LevelHandler::Tiering(vec_tier) => {
                vec_tier.push(TableStat {
                    key_range: KeyRange::new(smallest_ky, largest_ky),
                    table_id,
                    compact_task: None,
                });
            }
            LevelHandler::Leveling(_) => {
                unimplemented!();
            }
        }

        Ok(epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::InMemObjectStore;
    use risingwave_pb::hummock::TableMeta;

    #[tokio::test]
    async fn test_version_manager() -> HummockResult<()> {
        let version_manager = VersionManager::new();
        let epoch0 = version_manager.pin();
        assert_eq!(epoch0.0, 0);
        let e0_l0 = epoch0.1.levels.first().unwrap();
        match e0_l0 {
            Level::Tiering(table_ids) => {
                assert!(table_ids.is_empty());
            }
            Level::Leveling(_) => {
                panic!();
            }
        }
        let test_table_id = 42;
        let current_epoch_id = version_manager
            .add_l0_sst(
                Table::load(
                    test_table_id,
                    Arc::new(InMemObjectStore::default()),
                    String::from(""),
                    TableMeta::default(),
                )
                .await?,
            )
            .await?;
        assert_eq!(current_epoch_id, 1);
        let epoch1 = version_manager.pin();
        assert_eq!(epoch1.0, 1);
        let e1_l0 = epoch1.1.levels.first().unwrap();
        match e1_l0 {
            Level::Tiering(table_ids) => {
                assert_eq!(*table_ids, vec![test_table_id]);
            }
            Level::Leveling(_) => {
                panic!();
            }
        }
        Ok(())
    }
}
