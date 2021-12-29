use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::iter::once;
use std::ops::AddAssign;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use itertools::{EitherOrBoth, Itertools};
use parking_lot::Mutex as PLMutex;
use tokio::sync::Mutex;

use super::key_range::KeyRange;
use super::level_handler::{LevelHandler, TableStat};
use super::{HummockError, HummockResult, Table};
use crate::hummock::key::Timestamp;
use crate::hummock::{user_key, FullKey};

#[derive(Clone)]
pub enum Level {
    /// Leveling Indicates no key overlays in this level
    Leveling(Vec<u64>),
    Tiering(Vec<u64>),
}
pub struct LevelEntry {
    level_idx: u8,
    pub level: Level,
}

/// We store the full information of a snapshot in one [`Snapshot`] object. In the future, we should
/// implement a MVCC structure for this.
#[derive(Clone, Default)]
pub struct Snapshot {
    /// Table IDs in this snapshot. We **only store ID** in snapshot, we need to get the actual
    /// objects from version manager later.
    pub levels: Vec<Level>,
}
struct CompactStatus {
    level_handlers: Vec<LevelHandler>,
    next_compact_task_id: u64,
}
pub struct CompactTask {
    /// SSTs to be compacted, which will be removed from LSM after compaction
    pub input_ssts: Vec<LevelEntry>,
    /// In ideal case, the compaction will generate `splits.len()` tables which have key range
    /// corresponding to that in [`splits`], respectively
    pub splits: Vec<KeyRange>,
    /// low watermark in 'ts-aware compaction'
    pub watermark: Timestamp,
    /// compacion output, which will be added to [`target_level`] of LSM after compaction
    pub sorted_output_ssts: Vec<Table>,
    /// task id assigned by hummock storage service
    task_id: u64,
    /// compacion output will be added to [`target_level`] of LSM after compaction
    target_level: u8,

    pub is_target_ultimate_and_leveling: bool,
}

struct VersionManagerInner {
    /// To make things easy, we store the full snapshot of each epoch. In the future, we will use a
    /// MVCC structure for this, and only record changes compared with last epoch.
    status: HashMap<u64, Arc<Snapshot>>,

    /// TableId -> Object mapping
    tables: BTreeMap<u64, Arc<Table>>,

    /// Reference count of each epoch.
    ref_cnt: BTreeMap<u64, usize>,

    /// Deletion to apply in each epoch.
    table_deletion_to_apply: BTreeMap<u64, Vec<u64>>,

    /// Current epoch number.
    epoch: u64,

    /// Notify the vacuum of outer struct [`VersionManager`] to apply changes from one epoch.
    tx: tokio::sync::mpsc::UnboundedSender<()>,
}

impl VersionManagerInner {
    /// Unpin a snapshot of one epoch. When reference counter becomes 0, files might be vacuumed.
    pub fn unpin(&mut self, epoch: u64) {
        let ref_cnt = self.ref_cnt.get_mut(&epoch).expect("epoch not registered!");
        *ref_cnt -= 1;
        if *ref_cnt == 0 {
            self.ref_cnt.remove(&epoch).unwrap();

            if epoch != self.epoch {
                // TODO: precisely pass the epoch number that can be vacuum.
                self.tx.send(()).unwrap();
            }
        }
    }

    /// Add the given `snapshot` and increase the epoch. Return the latest epoch number.
    pub fn inc_epoch(&mut self, snapshot: Arc<Snapshot>) -> u64 {
        self.epoch += 1;
        self.ref_cnt.entry(self.epoch).or_default().add_assign(1);
        self.status.insert(self.epoch, snapshot);

        self.epoch
    }
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
    compact_status: Mutex<CompactStatus>,

    /// Notify the vacuum to apply changes from one epoch.
    tx: tokio::sync::mpsc::UnboundedSender<()>,

    /// Receiver of the vacuum.
    rx: PLMutex<Option<tokio::sync::mpsc::UnboundedReceiver<()>>>,

    /// Next timestamp
    next_ts: AtomicU64,

    /// Next table id
    next_table_id: AtomicU64,
}

impl VersionManager {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut init_epoch_vm = VersionManagerInner {
            status: HashMap::new(),
            tables: BTreeMap::new(),
            ref_cnt: BTreeMap::new(),
            table_deletion_to_apply: BTreeMap::new(),
            epoch: 0,
            tx: tx.clone(),
        };
        *init_epoch_vm
            .ref_cnt
            .entry(init_epoch_vm.epoch)
            .or_default() += 1;
        init_epoch_vm.status.insert(
            init_epoch_vm.epoch,
            Arc::new(Snapshot {
                levels: vec![Level::Tiering(vec![]), Level::Leveling(vec![])],
            }),
        );

        let vec_handler_having_l0 = vec![
            LevelHandler::Tiering(vec![], vec![]),
            LevelHandler::Leveling(vec![], vec![]),
        ];

        Self {
            inner: PLMutex::new(init_epoch_vm),
            compact_status: Mutex::new(CompactStatus {
                level_handlers: vec_handler_having_l0,
                next_compact_task_id: 1,
            }),
            tx,
            rx: PLMutex::new(Some(rx)),
            next_ts: 1.into(),
            next_table_id: 1.into(),
        }
    }

    // Mark it `async` to emulate a remote service
    pub async fn generate_ts(&self) -> u64 {
        self.next_ts.fetch_add(1, Ordering::SeqCst)
    }

    // TODO: This function maybe removed in the future.
    pub fn latest_ts(&self) -> u64 {
        self.next_ts
            .load(Ordering::Acquire)
            .checked_sub(1)
            .expect("ts should not be 0")
    }

    // Mark it `async` to emulate a remote service
    pub async fn generate_table_id(&self) -> u64 {
        self.next_table_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn pick_few_tables(&self, table_ids: &[u64]) -> HummockResult<Vec<Arc<Table>>> {
        let mut ret = Vec::with_capacity(table_ids.len());
        let inner = self.inner.lock();
        for table_id in table_ids {
            match inner.tables.get(table_id) {
                Some(table) => {
                    ret.push(table.clone());
                }
                None => {
                    return Err(HummockError::ObjectIoError(String::from(
                        "Table Not Exist.",
                    )));
                }
            }
        }
        Ok(ret)
    }

    /// Pin a snapshot of one epoch, so that all files at this epoch won't be deleted.
    pub fn pin(&self) -> (u64, Arc<Snapshot>) {
        let mut inner = self.inner.lock();
        let epoch = inner.epoch;
        *inner.ref_cnt.entry(epoch).or_default() += 1;
        (epoch, inner.status.get(&epoch).unwrap().clone())
    }

    pub fn unpin(&self, epoch: u64) {
        self.inner.lock().unpin(epoch);
    }

    /// Get the iterators on the underlying tables.
    /// Caller should be expected to pin a snapshot to get a consistent view.
    pub fn tables(&self, snapshot: Arc<Snapshot>) -> HummockResult<Vec<Arc<Table>>> {
        let mut out: Vec<Arc<Table>> = Vec::new();
        for level in &snapshot.levels {
            match level {
                Level::Tiering(table_ids) => {
                    let mut tables = self.pick_few_tables(table_ids)?;
                    out.append(&mut tables);
                }
                Level::Leveling(table_ids) => {
                    let mut tables = self.pick_few_tables(table_ids)?;
                    out.append(&mut tables);
                }
            }
        }

        Ok(out)
    }

    /// Add some L0 SSTs and return the latest epoch number
    pub async fn add_l0_ssts(&self, tables: impl IntoIterator<Item = Table>) -> HummockResult<u64> {
        let tables = tables.into_iter().collect_vec();

        let stats = tables
            .iter()
            .map(|table| TableStat {
                key_range: KeyRange::new(
                    Bytes::copy_from_slice(&table.meta.smallest_key),
                    Bytes::copy_from_slice(&table.meta.largest_key),
                ),
                table_id: table.id,
                compact_task: None,
            })
            .collect_vec();

        // Hold the compact status lock so that no one else could add/drop SST or search compaction
        // plan.
        let mut compact_status = self.compact_status.lock().await;

        let epoch = {
            // Hold the inner lock, so as to apply the changes to the current status, and add new
            // L0 SST to the LSM. This lock is released before triggering searching for compaction
            // plan.
            let mut inner = self.inner.lock();
            let old_epoch = inner.epoch;

            if tables.is_empty() {
                // No tables to add, simply return the old_epoch.
                return Ok(old_epoch);
            }

            // Get snapshot of latest version.
            let mut snapshot = inner
                .status
                .get(&old_epoch)
                .map(|x| x.as_ref().clone())
                .unwrap();

            for table in tables {
                let table_id = table.id;

                match inner.tables.entry(table_id) {
                    Entry::Vacant(v) => {
                        v.insert(Arc::new(table));
                    }
                    Entry::Occupied(_) => {
                        return Err(HummockError::ObjectIoError(
                            "Table ID to be created already exists.".to_owned(),
                        ))
                    }
                }

                match snapshot.levels.first_mut().unwrap() {
                    Level::Tiering(vec_tier) => {
                        vec_tier.push(table_id);
                    }
                    Level::Leveling(_) => {
                        unimplemented!();
                    }
                }
            }

            inner.unpin(old_epoch);

            // Add epoch number and make the modified snapshot available.
            inner.inc_epoch(Arc::new(snapshot))
        };

        match compact_status.level_handlers.first_mut().unwrap() {
            LevelHandler::Tiering(vec_tier, _) => {
                for stat in stats {
                    let insert_point = vec_tier.partition_point(
                        |TableStat {
                             key_range: other_key_range,
                             ..
                         }| { other_key_range <= &stat.key_range },
                    );
                    vec_tier.insert(insert_point, stat);
                }
            }
            LevelHandler::Leveling(_, _) => {
                panic!("L0 must be Tiering.");
            }
        }

        Ok(epoch)
    }

    /// Add a L0 SST and return a new epoch number
    pub async fn add_single_l0_sst(&self, table: Table) -> HummockResult<u64> {
        self.add_l0_ssts(once(table)).await
    }

    /// We assume that SSTs will only be deleted in compaction, otherwise `get_compact_task` need to
    /// `pin`
    pub async fn get_compact_task(&self) -> HummockResult<Option<CompactTask>> {
        let select_level = 0u8;

        enum SearchResult {
            Found(Vec<u64>, Vec<u64>, Vec<KeyRange>),
            NotFound,
        }

        let mut found = SearchResult::NotFound;
        let mut compact_status = self.compact_status.lock().await;
        let next_task_id = compact_status.next_compact_task_id;
        let (prior, posterior) = compact_status
            .level_handlers
            .split_at_mut(select_level as usize + 1);
        let (prior, posterior) = (prior.last_mut().unwrap(), posterior.first_mut().unwrap());
        let is_select_level_leveling = matches!(prior, LevelHandler::Leveling(_, _));
        let target_level = select_level + 1;
        let is_target_level_leveling = matches!(posterior, LevelHandler::Leveling(_, _));
        match prior {
            LevelHandler::Tiering(l_n, compacting_key_ranges)
            | LevelHandler::Leveling(l_n, compacting_key_ranges) => {
                let mut sst_idx = 0;
                let l_n_len = l_n.len();
                while sst_idx < l_n_len {
                    let mut next_sst_idx = sst_idx + 1;
                    let TableStat {
                        key_range: sst_key_range,
                        table_id,
                        ..
                    } = &l_n[sst_idx];
                    let mut select_level_inputs = vec![*table_id];
                    let key_range;
                    let mut tier_key_range;
                    if !is_select_level_leveling {
                        tier_key_range = sst_key_range.clone();

                        next_sst_idx = sst_idx;
                        for (
                            delta_idx,
                            TableStat {
                                key_range: other_key_range,
                                table_id: other_table_id,
                                ..
                            },
                        ) in l_n[sst_idx + 1..].iter().enumerate()
                        {
                            if user_key(&other_key_range.left) <= user_key(&tier_key_range.right) {
                                select_level_inputs.push(*other_table_id);
                                tier_key_range.full_key_extend(other_key_range);
                            } else {
                                next_sst_idx = sst_idx + 1 + delta_idx;
                                break;
                            }
                        }
                        if next_sst_idx == sst_idx {
                            next_sst_idx = l_n_len;
                        }

                        key_range = &tier_key_range;
                    } else {
                        key_range = sst_key_range;
                    }

                    let mut is_select_idle = true;
                    for TableStat { compact_task, .. } in &l_n[sst_idx..next_sst_idx] {
                        if compact_task.is_some() {
                            is_select_idle = false;
                            break;
                        }
                    }

                    if is_select_idle {
                        let insert_point =
                            compacting_key_ranges.partition_point(|(ongoing_key_range, _)| {
                                user_key(&ongoing_key_range.right) < user_key(&key_range.left)
                            });
                        if insert_point >= compacting_key_ranges.len()
                            || user_key(&compacting_key_ranges[insert_point].0.left)
                                > user_key(&key_range.right)
                        {
                            match posterior {
                                LevelHandler::Tiering(_, _) => unimplemented!(),
                                LevelHandler::Leveling(l_n_suc, _) => {
                                    let mut overlap_all_idle = true;
                                    // TODO: use pointer last time to avoid binary search
                                    let overlap_begin = l_n_suc.partition_point(|table_status| {
                                        user_key(&table_status.key_range.right)
                                            < user_key(&key_range.left)
                                    });
                                    let mut overlap_end = overlap_begin;
                                    let l_n_suc_len = l_n_suc.len();
                                    while overlap_end < l_n_suc_len
                                        && user_key(&l_n_suc[overlap_end].key_range.left)
                                            <= user_key(&key_range.right)
                                    {
                                        if l_n_suc[overlap_end].compact_task.is_some() {
                                            overlap_all_idle = false;
                                            break;
                                        }
                                        overlap_end += 1;
                                    }
                                    if overlap_all_idle {
                                        compacting_key_ranges.insert(
                                            insert_point,
                                            (key_range.clone(), next_task_id),
                                        );

                                        let mut suc_table_ids =
                                            Vec::with_capacity(overlap_end - overlap_begin);

                                        let mut splits =
                                            Vec::with_capacity(overlap_end - overlap_begin);
                                        splits.push(KeyRange::new(Bytes::new(), Bytes::new()));
                                        let mut key_split_append = |key_before_last: &Bytes| {
                                            splits.last_mut().unwrap().right =
                                                key_before_last.clone();
                                            splits.push(KeyRange::new(
                                                key_before_last.clone(),
                                                Bytes::new(),
                                            ));
                                        };

                                        let mut overlap_idx = overlap_begin;
                                        while overlap_idx < overlap_end {
                                            l_n_suc[overlap_idx].compact_task = Some(next_task_id);
                                            suc_table_ids.push(l_n_suc[overlap_idx].table_id);
                                            if overlap_idx > overlap_begin {
                                                // TODO: We do not need to add splits every time. We
                                                // can add every K SSTs.
                                                key_split_append(
                                                    &FullKey::from_user_key_slice(
                                                        user_key(
                                                            &l_n_suc[overlap_idx].key_range.left,
                                                        ),
                                                        Timestamp::MAX,
                                                    )
                                                    .into_inner()
                                                    .into(),
                                                );
                                            }
                                            overlap_idx += 1;
                                        }

                                        found = SearchResult::Found(
                                            select_level_inputs,
                                            suc_table_ids,
                                            splits,
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    sst_idx = next_sst_idx;
                }
                match &found {
                    SearchResult::Found(select_ln_ids, _, _) => {
                        let mut select_ln_iter = select_ln_ids.iter();
                        if let Some(first_id) = select_ln_iter.next() {
                            let mut current_id = first_id;
                            for TableStat {
                                table_id,
                                compact_task,
                                ..
                            } in l_n
                            {
                                if table_id == current_id {
                                    *compact_task = Some(next_task_id);
                                    match select_ln_iter.next() {
                                        Some(next_id) => {
                                            current_id = next_id;
                                        }
                                        None => break,
                                    }
                                }
                            }
                        }
                    }
                    SearchResult::NotFound => {}
                }
            }
        }

        let task = match found {
            SearchResult::Found(select_ln_ids, select_lnsuc_ids, splits) => {
                compact_status.next_compact_task_id += 1;
                CompactTask {
                    input_ssts: vec![
                        LevelEntry {
                            level_idx: select_level,
                            level: if is_select_level_leveling {
                                Level::Leveling(select_ln_ids)
                            } else {
                                Level::Tiering(select_ln_ids)
                            },
                        },
                        LevelEntry {
                            level_idx: target_level,
                            level: if is_target_level_leveling {
                                Level::Leveling(select_lnsuc_ids)
                            } else {
                                Level::Tiering(select_lnsuc_ids)
                            },
                        },
                    ],
                    splits,
                    watermark: Timestamp::MAX,
                    sorted_output_ssts: vec![],
                    task_id: next_task_id,
                    target_level,
                    is_target_ultimate_and_leveling: target_level as usize
                        == compact_status.level_handlers.len() - 1
                        && is_target_level_leveling,
                }
                .into()
            }
            SearchResult::NotFound => None,
        };

        Ok(task)
    }

    #[allow(clippy::needless_collect)]
    pub async fn report_compact_task(&self, compact_task: CompactTask, result: HummockResult<()>) {
        let output_table_compact_entries: Vec<_> = compact_task
            .sorted_output_ssts
            .iter()
            .map(|table| TableStat {
                key_range: KeyRange::new(
                    Bytes::copy_from_slice(&table.meta.smallest_key),
                    Bytes::copy_from_slice(&table.meta.largest_key),
                ),
                table_id: table.id,
                compact_task: None,
            })
            .collect();
        let mut compact_status = self.compact_status.lock().await;
        match result {
            Ok(()) => {
                let mut delete_table_ids = vec![];
                for LevelEntry { level_idx, .. } in compact_task.input_ssts {
                    delete_table_ids.extend(
                        compact_status.level_handlers[level_idx as usize]
                            .pop_task_input(compact_task.task_id)
                            .into_iter(),
                    );
                }
                match &mut compact_status.level_handlers[compact_task.target_level as usize] {
                    LevelHandler::Tiering(l_n, _) | LevelHandler::Leveling(l_n, _) => {
                        let old_ln = std::mem::take(l_n);
                        *l_n = itertools::merge_join_by(
                            old_ln,
                            output_table_compact_entries,
                            |l, r| l.key_range.cmp(&r.key_range),
                        )
                        .flat_map(|either_or_both| match either_or_both {
                            EitherOrBoth::Both(a, b) => vec![a, b].into_iter(),
                            EitherOrBoth::Left(a) => vec![a].into_iter(),
                            EitherOrBoth::Right(b) => vec![b].into_iter(),
                        })
                        .collect();
                    }
                }
                {
                    let mut inner = self.inner.lock();
                    let old_epoch = inner.epoch;

                    let snapshot = Snapshot {
                        levels: compact_status
                            .level_handlers
                            .iter()
                            .map(|level_handler| match level_handler {
                                LevelHandler::Tiering(l_n, _) => Level::Tiering(
                                    l_n.iter()
                                        .map(|TableStat { table_id, .. }| *table_id)
                                        .collect(),
                                ),
                                LevelHandler::Leveling(l_n, _) => Level::Leveling(
                                    l_n.iter()
                                        .map(|TableStat { table_id, .. }| *table_id)
                                        .collect(),
                                ),
                            })
                            .collect(),
                    };

                    inner.tables.extend(
                        compact_task
                            .sorted_output_ssts
                            .into_iter()
                            .map(|table| (table.id, Arc::new(table))),
                    );

                    // Add epoch number and make the modified snapshot available.
                    inner.epoch += 1;
                    let epoch = inner.epoch;
                    *inner.ref_cnt.entry(epoch).or_default() += 1;
                    inner.status.insert(epoch, Arc::new(snapshot));
                    inner
                        .table_deletion_to_apply
                        .insert(epoch, delete_table_ids);

                    inner.unpin(old_epoch);
                }
            }
            Err(_) => {
                // TODO: loop only in input levels
                for level_handler in &mut compact_status.level_handlers {
                    level_handler.unassign_task(compact_task.task_id);
                }
            }
        }
    }
}

pub struct ScopedUnpinSnapshot {
    vm: Arc<VersionManager>,
    epoch: u64,
    snapshot: Arc<Snapshot>,
}

impl ScopedUnpinSnapshot {
    pub fn from_version_manager(vm: Arc<VersionManager>) -> Self {
        let p = vm.pin();
        Self {
            vm,
            epoch: p.0,
            snapshot: p.1,
        }
    }

    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.clone()
    }
}

impl Drop for ScopedUnpinSnapshot {
    fn drop(&mut self) {
        self.vm.unpin(self.epoch);
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::hummock::TableMeta;

    use super::*;
    use crate::object::InMemObjectStore;

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
            .add_single_l0_sst(
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
