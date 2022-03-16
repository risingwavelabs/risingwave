use std::collections::BTreeMap;
use std::sync::Arc;

use itertools::{enumerate, Itertools};
use prometheus::core::{AtomicF64, GenericGauge};
use prost::Message;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::hummock::hummock_version::HummockVersionRefId;
use risingwave_pb::hummock::{
    CompactMetrics, CompactTask, HummockContextRefId, HummockPinnedSnapshot, HummockPinnedVersion,
    HummockSnapshot, HummockStaleSstables, HummockVersion, Level, LevelType, SstableIdInfo,
    SstableInfo, SstableRefId, TableSetStatistics, UncommittedEpoch,
};
use risingwave_storage::hummock::{
    HummockContextId, HummockEpoch, HummockRefCount, HummockSSTableId, HummockVersionId,
    INVALID_EPOCH,
};
use tokio::sync::{Mutex, RwLock};

use crate::hummock::compaction::CompactStatus;
use crate::hummock::level_handler::{LevelHandler, SSTableStat};
use crate::hummock::model::{
    sstable_id_info, CurrentHummockVersionId, HummockPinnedSnapshotExt, HummockPinnedVersionExt,
    INVALID_TIMESTAMP,
};
use crate::manager::{IdCategory, IdGeneratorManagerRef, MetaSrvEnv};
use crate::model::{MetadataModel, Transactional, Worker};
use crate::rpc::metrics::MetaMetrics;
use crate::storage::{Error, MetaStore, Transaction};

// Update to states are performed as follow:
// - Call load_meta_store_state when creating HummockManager (on boot or on meta-node leadership
//   change).
// - Read in-mem states and make copies of them.
// - Make changes on the copies.
// - Commit the changes via meta store transaction.
// - If transaction succeeds, update in-mem states with the copies.
pub struct HummockManager<S> {
    meta_store_ref: Arc<S>,
    id_gen_manager_ref: IdGeneratorManagerRef<S>,
    // When trying to locks compaction and versioning at the same time, compaction lock should
    // be requested before versioning lock.
    compaction: Mutex<Compaction>,
    versioning: RwLock<Versioning>,

    metrics: Arc<MetaMetrics>,
}

struct Compaction {
    compact_status: CompactStatus,
}

struct Versioning {
    current_version_id: CurrentHummockVersionId,
    hummock_versions: BTreeMap<HummockVersionId, HummockVersion>,
    pinned_versions: BTreeMap<HummockContextId, HummockPinnedVersion>,
    pinned_snapshots: BTreeMap<HummockContextId, HummockPinnedSnapshot>,
    stale_sstables: BTreeMap<HummockVersionId, HummockStaleSstables>,
    sstable_id_infos: BTreeMap<HummockSSTableId, SstableIdInfo>,
}

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>, metrics: Arc<MetaMetrics>) -> Result<HummockManager<S>> {
        let instance = HummockManager {
            meta_store_ref: env.meta_store_ref(),
            id_gen_manager_ref: env.id_gen_manager_ref(),
            versioning: RwLock::new(Versioning {
                current_version_id: CurrentHummockVersionId::new(),
                hummock_versions: Default::default(),
                pinned_versions: Default::default(),
                pinned_snapshots: Default::default(),
                stale_sstables: Default::default(),
                sstable_id_infos: Default::default(),
            }),
            compaction: Mutex::new(Compaction {
                compact_status: CompactStatus::new(),
            }),
            metrics,
        };

        instance.load_meta_store_state().await?;

        Ok(instance)
    }

    /// load state from meta store.
    async fn load_meta_store_state(&self) -> Result<()> {
        let mut compaction_guard = self.compaction.lock().await;
        compaction_guard.compact_status = CompactStatus::get(self.meta_store_ref.as_ref())
            .await?
            .unwrap_or_else(CompactStatus::new);

        let mut versioning_guard = self.versioning.write().await;
        versioning_guard.current_version_id =
            CurrentHummockVersionId::get(self.meta_store_ref.as_ref())
                .await?
                .unwrap_or_else(CurrentHummockVersionId::new);

        versioning_guard.hummock_versions = HummockVersion::list(self.meta_store_ref.as_ref())
            .await?
            .into_iter()
            .map(|version| (version.id, version))
            .collect();

        // Insert the initial version.
        if versioning_guard.hummock_versions.is_empty() {
            let init_version = HummockVersion {
                id: versioning_guard.current_version_id.id(),
                levels: vec![
                    Level {
                        level_type: LevelType::Overlapping as i32,
                        table_ids: vec![],
                    },
                    Level {
                        level_type: LevelType::Nonoverlapping as i32,
                        table_ids: vec![],
                    },
                ],
                uncommitted_epochs: vec![],
                max_committed_epoch: INVALID_EPOCH,
            };
            init_version.insert(self.meta_store_ref.as_ref()).await?;
            versioning_guard
                .hummock_versions
                .insert(init_version.id, init_version);
        }

        versioning_guard.pinned_versions = HummockPinnedVersion::list(self.meta_store_ref.as_ref())
            .await?
            .into_iter()
            .map(|p| (p.context_id, p))
            .collect();
        versioning_guard.pinned_snapshots =
            HummockPinnedSnapshot::list(self.meta_store_ref.as_ref())
                .await?
                .into_iter()
                .map(|p| (p.context_id, p))
                .collect();

        versioning_guard.stale_sstables = HummockStaleSstables::list(self.meta_store_ref.as_ref())
            .await?
            .into_iter()
            .map(|s| (s.version_id, s))
            .collect();

        versioning_guard.sstable_id_infos = SstableIdInfo::list(self.meta_store_ref.as_ref())
            .await?
            .into_iter()
            .map(|s| (s.id, s))
            .collect();

        Ok(())
    }

    /// We use worker node id as the `context_id`.
    /// If the `context_id` is provided, the transaction will abort if the `context_id` is not
    /// valid, which means the worker node is not a valid member of the cluster.
    async fn commit_trx(
        &self,
        meta_store_ref: &S,
        mut trx: Transaction,
        context_id: Option<HummockContextId>,
    ) -> Result<()> {
        if let Some(context_id) = context_id {
            // Get the worker's key in meta store
            let workers = Worker::list(meta_store_ref)
                .await?
                .into_iter()
                .filter(|worker| worker.worker_id() == context_id)
                .collect_vec();
            assert!(workers.len() <= 1);
            if let Some(worker) = workers.first() {
                trx.check_exists(Worker::cf_name(), worker.key()?.encode_to_vec());
            } else {
                // The worker is not found in cluster.
                return Err(Error::TransactionAbort().into());
            }
        }
        meta_store_ref.txn(trx).await.map_err(Into::into)
    }

    pub async fn pin_version(&self, context_id: HummockContextId) -> Result<HummockVersion> {
        let mut versioning_guard = self.versioning.write().await;
        let version_id = versioning_guard.current_version_id.id();
        // pin the version
        let mut context_pinned_version_copy = versioning_guard
            .pinned_versions
            .get(&context_id)
            .cloned()
            .unwrap_or(HummockPinnedVersion {
                context_id,
                version_id: vec![],
            });
        context_pinned_version_copy.pin_version(version_id);
        let mut transaction = Transaction::default();
        context_pinned_version_copy.upsert_in_transaction(&mut transaction)?;
        self.commit_trx(self.meta_store_ref.as_ref(), transaction, Some(context_id))
            .await?;

        // Update in-mem state after transaction succeeds.
        versioning_guard.pinned_versions.insert(
            context_pinned_version_copy.context_id,
            context_pinned_version_copy,
        );

        let ret = Ok(versioning_guard
            .hummock_versions
            .get(&version_id)
            .unwrap()
            .clone());

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        ret
    }

    pub async fn unpin_version(
        &self,
        context_id: HummockContextId,
        pinned_version_id: HummockVersionId,
    ) -> Result<()> {
        let mut versioning_guard = self.versioning.write().await;
        let mut transaction = Transaction::default();
        let mut context_pinned_version_copy =
            match versioning_guard.pinned_versions.get(&context_id).cloned() {
                None => {
                    return Ok(());
                }
                Some(context_pinned_version) => context_pinned_version,
            };
        context_pinned_version_copy.unpin_version(pinned_version_id);
        context_pinned_version_copy.upsert_in_transaction(&mut transaction)?;
        self.commit_trx(self.meta_store_ref.as_ref(), transaction, Some(context_id))
            .await?;
        // Update in-mem state after transaction succeeds.
        versioning_guard.pinned_versions.insert(
            context_pinned_version_copy.context_id,
            context_pinned_version_copy,
        );

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    fn trigger_sst_stat(&self, compact_status: &CompactStatus) {
        let reduce_compact_cnt = |compacting_key_ranges: &Vec<(
            risingwave_storage::hummock::key_range::KeyRange,
            u64,
            u64,
        )>| {
            compacting_key_ranges
                .iter()
                .fold(0, |accum, elem| accum + elem.2)
        };
        for (idx, level_handler) in enumerate(compact_status.level_handlers.iter()) {
            let (sst_num, compact_cnt) = match level_handler {
                LevelHandler::Nonoverlapping(ssts, compacting_key_ranges) => {
                    (ssts.len(), reduce_compact_cnt(compacting_key_ranges))
                }
                LevelHandler::Overlapping(ssts, compacting_key_ranges) => {
                    (ssts.len(), reduce_compact_cnt(compacting_key_ranges))
                }
            };
            let level_label = String::from("L") + &idx.to_string();
            self.metrics
                .level_sst_num
                .get_metric_with_label_values(&[&level_label])
                .unwrap()
                .set(sst_num as i64);
            self.metrics
                .level_compact_cnt
                .get_metric_with_label_values(&[&level_label])
                .unwrap()
                .set(compact_cnt as i64);
        }
    }

    fn single_level_stat<T: FnMut(String) -> prometheus::Result<GenericGauge<AtomicF64>>>(
        mut metric_vec: T,
        level_stat: &TableSetStatistics,
    ) {
        let level_label = String::from("L") + &level_stat.level_idx.to_string();
        metric_vec(level_label).unwrap().add(level_stat.size_gb);
    }

    fn trigger_rw_stat(&self, compact_metrics: &CompactMetrics) {
        Self::single_level_stat(
            |label| {
                self.metrics
                    .level_compact_read_curr
                    .get_metric_with_label_values(&[&label])
            },
            compact_metrics.read_level_n.as_ref().unwrap(),
        );
        Self::single_level_stat(
            |label| {
                self.metrics
                    .level_compact_read_next
                    .get_metric_with_label_values(&[&label])
            },
            compact_metrics.read_level_nplus1.as_ref().unwrap(),
        );
        Self::single_level_stat(
            |label| {
                self.metrics
                    .level_compact_write
                    .get_metric_with_label_values(&[&label])
            },
            compact_metrics.write.as_ref().unwrap(),
        );
    }

    pub async fn add_tables(
        &self,
        context_id: HummockContextId,
        sstables: Vec<SstableInfo>,
        epoch: HummockEpoch,
    ) -> Result<HummockVersion> {
        let stats = sstables.iter().map(SSTableStat::from).collect_vec();

        let mut sst_id_infos_update_vec = vec![];
        let mut compaction_guard = self.compaction.lock().await;
        let mut compact_status_copy = compaction_guard.compact_status.clone();
        match compact_status_copy.level_handlers.first_mut().unwrap() {
            LevelHandler::Overlapping(vec_tier, _) => {
                for stat in stats {
                    let insert_point = vec_tier.partition_point(
                        |SSTableStat {
                             key_range: other_key_range,
                             ..
                         }| { other_key_range <= &stat.key_range },
                    );
                    vec_tier.insert(insert_point, stat);
                }
            }
            LevelHandler::Nonoverlapping(_, _) => {
                panic!("L0 must be Tiering.");
            }
        }
        let mut transaction = Transaction::default();
        compact_status_copy.update_in_transaction(&mut transaction);

        let mut versioning_guard = self.versioning.write().await;
        let mut current_version_id_copy = versioning_guard.current_version_id.clone();
        let mut hummock_version_copy = versioning_guard
            .hummock_versions
            .get(&current_version_id_copy.id())
            .unwrap()
            .clone();

        // Track SST in meta.
        for sst_id in sstables.iter().map(|s| s.id) {
            match versioning_guard.sstable_id_infos.get(&sst_id).cloned() {
                None => {
                    #[cfg(not(test))]
                    return Err(ErrorCode::MetaError(format!(
                        "invalid sst id {}, may have been vacuumed",
                        sst_id
                    ))
                    .into());
                }
                Some(mut sst_id_info) => {
                    if sst_id_info.meta_create_timestamp != INVALID_TIMESTAMP {
                        // This is a duplicate request.
                        return Ok(hummock_version_copy);
                    }
                    sst_id_info.meta_create_timestamp = sstable_id_info::get_timestamp_now();
                    sst_id_info.upsert_in_transaction(&mut transaction)?;
                    sst_id_infos_update_vec.push(sst_id_info);
                }
            }
        }

        // check whether the epoch is valid
        // TODO: return error instead of panic
        if epoch <= hummock_version_copy.max_committed_epoch {
            panic!(
                "Epoch {} <= max_committed_epoch {}",
                epoch, hummock_version_copy.max_committed_epoch
            );
        }

        // create new_version by adding tables in UncommittedEpoch
        match hummock_version_copy
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch)
        {
            Some(uncommitted_epoch) => {
                sstables
                    .iter()
                    .for_each(|t| uncommitted_epoch.table_ids.push(t.id));
            }
            None => hummock_version_copy
                .uncommitted_epochs
                .push(UncommittedEpoch {
                    epoch,
                    table_ids: sstables.iter().map(|t| t.id).collect(),
                }),
        };
        current_version_id_copy.increase();
        current_version_id_copy.upsert_in_transaction(&mut transaction)?;
        hummock_version_copy.id = current_version_id_copy.id();
        hummock_version_copy.upsert_in_transaction(&mut transaction)?;

        // the trx contain update for both tables and compact_status
        self.commit_trx(self.meta_store_ref.as_ref(), transaction, Some(context_id))
            .await?;

        self.trigger_sst_stat(&compact_status_copy);

        // Update in-mem state after transaction succeeds.
        compaction_guard.compact_status = compact_status_copy;
        versioning_guard.current_version_id = current_version_id_copy;
        versioning_guard
            .hummock_versions
            .insert(hummock_version_copy.id, hummock_version_copy.clone());
        for sst_id_info_update in sst_id_infos_update_vec {
            versioning_guard
                .sstable_id_infos
                .insert(sst_id_info_update.id, sst_id_info_update);
        }

        #[cfg(test)]
        {
            drop(versioning_guard);
            drop(compaction_guard);
            self.check_state_consistency().await;
        }

        Ok(hummock_version_copy)
    }

    pub async fn pin_snapshot(&self, context_id: HummockContextId) -> Result<HummockSnapshot> {
        let mut versioning_guard = self.versioning.write().await;

        // Use the max_committed_epoch in storage as the snapshot ts so only committed changes are
        // visible in the snapshot.
        let version_id = versioning_guard.current_version_id.id();
        let max_committed_epoch = versioning_guard
            .hummock_versions
            .get(&version_id)
            .unwrap()
            .max_committed_epoch;
        let mut context_pinned_snapshot_copy = versioning_guard
            .pinned_snapshots
            .get(&context_id)
            .cloned()
            .unwrap_or(HummockPinnedSnapshot {
                context_id,
                snapshot_id: vec![],
            });
        context_pinned_snapshot_copy.pin_snapshot(max_committed_epoch);
        let mut transaction = Transaction::default();
        context_pinned_snapshot_copy.upsert_in_transaction(&mut transaction)?;
        self.commit_trx(self.meta_store_ref.as_ref(), transaction, Some(context_id))
            .await?;

        // Update in-mem state after transaction succeeds.
        versioning_guard.pinned_snapshots.insert(
            context_pinned_snapshot_copy.context_id,
            context_pinned_snapshot_copy,
        );

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(HummockSnapshot {
            epoch: max_committed_epoch,
        })
    }

    pub async fn unpin_snapshot(
        &self,
        context_id: HummockContextId,
        hummock_snapshot: HummockSnapshot,
    ) -> Result<()> {
        let mut versioning_guard = self.versioning.write().await;

        let mut context_pinned_snapshot_copy =
            match versioning_guard.pinned_snapshots.get(&context_id).cloned() {
                None => {
                    return Ok(());
                }
                Some(context_pinned_snapshot) => context_pinned_snapshot,
            };
        let mut transaction = Transaction::default();
        context_pinned_snapshot_copy.unpin_snapshot(hummock_snapshot.epoch);
        context_pinned_snapshot_copy.upsert_in_transaction(&mut transaction)?;
        self.commit_trx(self.meta_store_ref.as_ref(), transaction, Some(context_id))
            .await?;

        // Update in-mem state after transaction succeeds.
        versioning_guard.pinned_snapshots.insert(
            context_pinned_snapshot_copy.context_id,
            context_pinned_snapshot_copy,
        );

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    pub async fn get_compact_task(&self) -> Result<Option<CompactTask>> {
        let mut compaction_guard = self.compaction.lock().await;
        let mut compact_status_copy = compaction_guard.compact_status.clone();
        let compact_task = compact_status_copy.get_compact_task();
        let ret = match compact_task {
            None => Ok(None),
            Some(mut compact_task) => {
                let mut transaction = Transaction::default();
                compact_status_copy.update_in_transaction(&mut transaction);
                self.commit_trx(self.meta_store_ref.as_ref(), transaction, None)
                    .await?;
                compact_task.watermark = {
                    let versioning_guard = self.versioning.read().await;
                    let current_version_id = versioning_guard.current_version_id.id();
                    let max_committed_epoch = versioning_guard
                        .hummock_versions
                        .get(&current_version_id)
                        .unwrap()
                        .max_committed_epoch;
                    versioning_guard
                        .pinned_snapshots
                        .values()
                        .flat_map(|v| v.snapshot_id.clone())
                        .fold(max_committed_epoch, std::cmp::min)
                };
                // Update in-mem state after transaction succeeds.
                compaction_guard.compact_status = compact_status_copy;
                Ok(Some(compact_task))
            }
        };

        #[cfg(test)]
        {
            drop(compaction_guard);
            self.check_state_consistency().await;
        }

        ret
    }

    /// `report_compact_task` is retryable. `task_id` in `compact_task` parameter is used as the
    /// idempotency key. Return Ok(false) to indicate the `task_id` is not found, which may have
    /// been processed previously.
    pub async fn report_compact_task(
        &self,
        mut compact_task: CompactTask,
        task_result: bool,
    ) -> Result<bool> {
        let output_table_compact_entries: Vec<_> = compact_task
            .sorted_output_ssts
            .iter()
            .map(SSTableStat::from)
            .collect();

        let mut sst_id_info_update_vec = vec![];
        // Extract info for logging.
        let compact_task_id = compact_task.task_id;
        let input_sst_ids = compact_task
            .input_ssts
            .iter()
            .flat_map(|v| v.level.as_ref().unwrap().table_ids.clone())
            .collect_vec();
        let output_sst_ids = compact_task
            .sorted_output_ssts
            .iter()
            .map(|s| s.id)
            .collect_vec();

        let compact_task_metrics = compact_task.metrics.take();
        let mut compaction_guard = self.compaction.lock().await;
        let mut transaction = Transaction::default();
        let mut compact_status_copy = compaction_guard.compact_status.clone();
        let delete_table_ids = match compact_status_copy.report_compact_task(
            output_table_compact_entries,
            compact_task,
            task_result,
        ) {
            None => {
                // The task is not found.
                return Ok(false);
            }
            Some(delete_table_ids) => delete_table_ids,
        };
        compact_status_copy.update_in_transaction(&mut transaction);
        if task_result {
            // The compact task is finished.
            let mut versioning_guard = self.versioning.write().await;
            let mut current_version_id_copy = versioning_guard.current_version_id.clone();
            let old_version = versioning_guard
                .hummock_versions
                .get(&current_version_id_copy.id())
                .unwrap();
            current_version_id_copy.increase();
            current_version_id_copy.upsert_in_transaction(&mut transaction)?;
            let new_version = HummockVersion {
                id: current_version_id_copy.id(),
                levels: compact_status_copy
                    .level_handlers
                    .iter()
                    .map(|level_handler| match level_handler {
                        LevelHandler::Overlapping(l_n, _) => Level {
                            level_type: LevelType::Overlapping as i32,
                            table_ids: l_n
                                .iter()
                                .map(|SSTableStat { table_id, .. }| *table_id)
                                .collect(),
                        },
                        LevelHandler::Nonoverlapping(l_n, _) => Level {
                            level_type: LevelType::Nonoverlapping as i32,
                            table_ids: l_n
                                .iter()
                                .map(|SSTableStat { table_id, .. }| *table_id)
                                .collect(),
                        },
                    })
                    .collect(),
                uncommitted_epochs: old_version.uncommitted_epochs.clone(),
                max_committed_epoch: old_version.max_committed_epoch,
            };

            new_version.upsert_in_transaction(&mut transaction)?;
            let mut stale_sstables_copy = versioning_guard
                .stale_sstables
                .get(&old_version.id)
                .cloned()
                .unwrap_or(HummockStaleSstables {
                    version_id: old_version.id,
                    id: vec![],
                });
            stale_sstables_copy.id.extend(delete_table_ids);
            stale_sstables_copy.upsert_in_transaction(&mut transaction)?;

            for sst_id in &output_sst_ids {
                match versioning_guard.sstable_id_infos.get(sst_id).cloned() {
                    None => {
                        return Err(ErrorCode::MetaError(format!(
                            "invalid sst id {}, may have been vacuumed",
                            sst_id
                        ))
                        .into());
                    }
                    Some(mut sst_id_info) => {
                        sst_id_info.meta_create_timestamp = sstable_id_info::get_timestamp_now();
                        sst_id_info.upsert_in_transaction(&mut transaction)?;
                        sst_id_info_update_vec.push(sst_id_info);
                    }
                }
            }

            self.commit_trx(self.meta_store_ref.as_ref(), transaction, None)
                .await?;

            // Update in-mem state after transaction succeeds. after transaction succeeds.
            compaction_guard.compact_status = compact_status_copy;
            versioning_guard.current_version_id = current_version_id_copy;
            versioning_guard
                .hummock_versions
                .insert(new_version.id, new_version);
            versioning_guard
                .stale_sstables
                .insert(stale_sstables_copy.version_id, stale_sstables_copy);
            for sst_id_info_update in sst_id_info_update_vec {
                versioning_guard
                    .sstable_id_infos
                    .insert(sst_id_info_update.id, sst_id_info_update);
            }

            tracing::debug!(
                "Finish hummock compaction task id {}, compact {} SSTs {:?} to {} SSTs {:?}",
                compact_task_id,
                input_sst_ids.len(),
                input_sst_ids,
                output_sst_ids.len(),
                output_sst_ids,
            );
        } else {
            // The compact task is cancelled.
            self.commit_trx(self.meta_store_ref.as_ref(), transaction, None)
                .await?;

            // Update in-mem state after transaction succeeds. after transaction succeeds.
            compaction_guard.compact_status = compact_status_copy;

            tracing::debug!("Cancel hummock compaction task id {}", compact_task_id);
        }

        self.trigger_sst_stat(&compaction_guard.compact_status);
        self.trigger_rw_stat(compact_task_metrics.as_ref().unwrap());

        #[cfg(test)]
        {
            drop(compaction_guard);
            self.check_state_consistency().await;
        }

        Ok(true)
    }

    pub async fn commit_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        let mut versioning_guard = self.versioning.write().await;
        let mut transaction = Transaction::default();
        let mut current_version_id_copy = versioning_guard.current_version_id.clone();
        let old_version_id = current_version_id_copy.increase();
        let new_version_id = current_version_id_copy.id();
        current_version_id_copy.upsert_in_transaction(&mut transaction)?;
        let mut hummock_version_copy = versioning_guard
            .hummock_versions
            .get(&old_version_id)
            .unwrap()
            .clone();
        // TODO: return error instead of panic
        if epoch <= hummock_version_copy.max_committed_epoch {
            panic!(
                "Epoch {} <= max_committed_epoch {}",
                epoch, hummock_version_copy.max_committed_epoch
            );
        }

        // TODO #447: the epoch should fail and rollback if any precedent epoch is uncommitted.
        // get tables in the committing epoch
        if let Some(idx) = hummock_version_copy
            .uncommitted_epochs
            .iter()
            .position(|e| e.epoch == epoch)
        {
            let uncommitted_epoch = &hummock_version_copy.uncommitted_epochs[idx];

            // commit tables by moving them into level0
            let version_first_level = hummock_version_copy.levels.first_mut().unwrap();
            match version_first_level.get_level_type()? {
                LevelType::Overlapping => {
                    uncommitted_epoch
                        .table_ids
                        .iter()
                        .for_each(|id| version_first_level.table_ids.push(*id));
                }
                LevelType::Nonoverlapping => {
                    unimplemented!()
                }
            };

            // remove the epoch from uncommitted_epochs and update max_committed_epoch
            hummock_version_copy.uncommitted_epochs.swap_remove(idx);
        }
        // Create a new_version, possibly merely to bump up the version id and max_committed_epoch.
        hummock_version_copy.max_committed_epoch = epoch;
        hummock_version_copy.id = new_version_id;
        hummock_version_copy.upsert_in_transaction(&mut transaction)?;
        self.commit_trx(self.meta_store_ref.as_ref(), transaction, None)
            .await?;
        // Update in-mem state after transaction succeeds.
        versioning_guard.current_version_id = current_version_id_copy;
        versioning_guard
            .hummock_versions
            .insert(hummock_version_copy.id, hummock_version_copy);
        tracing::trace!("new committed epoch {}", epoch);

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    pub async fn abort_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        let mut versioning_guard = self.versioning.write().await;
        let mut transaction = Transaction::default();
        let mut current_version_id_copy = versioning_guard.current_version_id.clone();
        let mut sst_id_info_update_vec = vec![];
        let old_version_id = current_version_id_copy.increase();
        let new_version_id = current_version_id_copy.id();
        current_version_id_copy.upsert_in_transaction(&mut transaction)?;
        let mut hummock_version_copy = versioning_guard
            .hummock_versions
            .get(&old_version_id)
            .unwrap()
            .clone();

        // get tables in the committing epoch
        let ret = match hummock_version_copy
            .uncommitted_epochs
            .iter()
            .position(|e| e.epoch == epoch)
        {
            Some(idx) => {
                let uncommitted_epoch = &hummock_version_copy.uncommitted_epochs[idx];

                // remove tables of the aborting epoch
                for sst_id in &uncommitted_epoch.table_ids {
                    if let Some(mut sst_id_info) =
                        versioning_guard.sstable_id_infos.get(sst_id).cloned()
                    {
                        sst_id_info.meta_delete_timestamp = sstable_id_info::get_timestamp_now();
                        sst_id_info.upsert_in_transaction(&mut transaction)?;
                        sst_id_info_update_vec.push(sst_id_info);
                    }
                }
                hummock_version_copy.uncommitted_epochs.swap_remove(idx);

                // create new_version
                hummock_version_copy.id = new_version_id;
                hummock_version_copy.upsert_in_transaction(&mut transaction)?;

                self.commit_trx(self.meta_store_ref.as_ref(), transaction, None)
                    .await?;
                // Update in-mem state after transaction succeeds.
                versioning_guard.current_version_id = current_version_id_copy;
                versioning_guard
                    .hummock_versions
                    .insert(hummock_version_copy.id, hummock_version_copy);
                for sst_id_info_update in sst_id_info_update_vec {
                    versioning_guard
                        .sstable_id_infos
                        .insert(sst_id_info_update.id, sst_id_info_update);
                }
                Ok(())
            }
            None => Ok(()),
        };

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        ret
    }

    pub async fn get_new_table_id(&self) -> Result<HummockSSTableId> {
        // TODO id_gen_manager generates u32, we need u64
        let sstable_id = self
            .id_gen_manager_ref
            .generate::<{ IdCategory::HummockSSTableId }>()
            .await
            .map(|id| id as HummockSSTableId)?;

        let mut versioning_guard = self.versioning.write().await;
        let new_sst_id_info = SstableIdInfo {
            id: sstable_id,
            id_create_timestamp: sstable_id_info::get_timestamp_now(),
            meta_create_timestamp: INVALID_TIMESTAMP,
            meta_delete_timestamp: INVALID_TIMESTAMP,
        };
        new_sst_id_info.insert(self.meta_store_ref.as_ref()).await?;

        // Update in-mem state after transaction succeeds.
        versioning_guard
            .sstable_id_infos
            .insert(new_sst_id_info.id, new_sst_id_info);

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(sstable_id)
    }

    pub async fn release_context_resource(&self, context_id: HummockContextId) -> Result<()> {
        let mut versioning_guard = self.versioning.write().await;
        let mut transaction = Transaction::default();
        let pinned_version = versioning_guard.pinned_versions.get(&context_id);
        let mut to_commit = false;
        if let Some(pinned_version) = pinned_version {
            HummockPinnedVersion::delete_in_transaction(
                HummockContextRefId {
                    id: pinned_version.context_id,
                },
                &mut transaction,
            )?;
            to_commit = true;
        }
        let pinned_snapshot = versioning_guard.pinned_snapshots.get(&context_id);
        if let Some(pinned_snapshot) = pinned_snapshot {
            HummockPinnedSnapshot::delete_in_transaction(
                HummockContextRefId {
                    id: pinned_snapshot.context_id,
                },
                &mut transaction,
            )?;
            to_commit = true;
        }
        if !to_commit {
            return Ok(());
        }
        self.commit_trx(self.meta_store_ref.as_ref(), transaction, None)
            .await?;

        // Update in-mem state after transaction succeeds.
        versioning_guard.pinned_versions.remove(&context_id);
        versioning_guard.pinned_snapshots.remove(&context_id);

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    /// List version ids in ascending order. TODO: support limit parameter
    pub async fn list_version_ids_asc(&self) -> Result<Vec<HummockVersionId>> {
        let versioning_guard = self.versioning.read().await;
        let version_ids = versioning_guard
            .hummock_versions
            .keys()
            .cloned()
            .collect_vec();
        Ok(version_ids)
    }

    /// Get the reference count of given version id
    pub async fn get_version_pin_count(
        &self,
        version_id: HummockVersionId,
    ) -> Result<HummockRefCount> {
        let versioning_guard = self.versioning.read().await;
        let count = versioning_guard
            .pinned_versions
            .values()
            .filter(|version_pin| version_pin.version_id.contains(&version_id))
            .count();
        Ok(count as HummockRefCount)
    }

    /// Get the `SSTable` ids which are guaranteed not to be included after `version_id`, thus they
    /// can be deleted if all versions LE than `version_id` are not referenced.
    pub async fn get_ssts_to_delete(
        &self,
        version_id: HummockVersionId,
    ) -> Result<Vec<HummockSSTableId>> {
        let versioning_guard = self.versioning.read().await;
        Ok(versioning_guard
            .stale_sstables
            .get(&version_id)
            .map(|s| s.id.clone())
            .unwrap_or_default())
    }

    /// Delete metadata of the given `version_id`
    pub async fn delete_version(&self, version_id: HummockVersionId) -> Result<()> {
        let mut sst_id_info_update_vec = vec![];
        let mut versioning_guard = self.versioning.write().await;
        let mut transaction = Transaction::default();
        // Delete record in HummockVersion if any.
        if versioning_guard.hummock_versions.get(&version_id).is_none() {
            return Ok(());
        }
        HummockVersion::delete_in_transaction(
            HummockVersionRefId { id: version_id },
            &mut transaction,
        )?;
        // Delete record in HummockTablesToDelete if any.
        let stale_sstables = versioning_guard.stale_sstables.get(&version_id);
        if let Some(ssts_to_delete) = stale_sstables {
            // Delete tracked sstables.
            for sst_id in &ssts_to_delete.id {
                if let Some(mut sst_id_info) =
                    versioning_guard.sstable_id_infos.get(sst_id).cloned()
                {
                    sst_id_info.meta_delete_timestamp = sstable_id_info::get_timestamp_now();
                    sst_id_info.upsert_in_transaction(&mut transaction)?;
                    sst_id_info_update_vec.push(sst_id_info);
                }
            }
            HummockStaleSstables::delete_in_transaction(
                HummockVersionRefId {
                    id: ssts_to_delete.version_id,
                },
                &mut transaction,
            )?;
        }

        for version_pin in versioning_guard.pinned_versions.values() {
            assert!(
                !version_pin.version_id.contains(&version_id),
                "version still referenced shouldn't be deleted."
            );
        }

        self.commit_trx(self.meta_store_ref.as_ref(), transaction, None)
            .await?;

        // Update in-mem state after transaction succeeds.
        versioning_guard.hummock_versions.remove(&version_id);
        versioning_guard.stale_sstables.remove(&version_id);
        for sst_id_info_update in sst_id_info_update_vec {
            versioning_guard
                .sstable_id_infos
                .insert(sst_id_info_update.id, sst_id_info_update);
        }

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    // TODO: use proc macro to call check_state_consistency
    #[cfg(test)]
    pub async fn check_state_consistency(&self) {
        let get_state = || async {
            let compaction_guard = self.compaction.lock().await;
            let versioning_guard = self.versioning.read().await;
            let compact_status_copy = compaction_guard.compact_status.clone();
            let current_version_id_copy = versioning_guard.current_version_id.clone();
            let hummmock_versions_copy = versioning_guard.hummock_versions.clone();
            let pinned_versions_copy = versioning_guard.pinned_versions.clone();
            let pinned_snapshots_copy = versioning_guard.pinned_snapshots.clone();
            let stale_sstables_copy = versioning_guard.stale_sstables.clone();
            let sst_id_infos_copy = versioning_guard.sstable_id_infos.clone();
            (
                compact_status_copy,
                current_version_id_copy,
                hummmock_versions_copy,
                pinned_versions_copy,
                pinned_snapshots_copy,
                stale_sstables_copy,
                sst_id_infos_copy,
            )
        };
        let mem_state = get_state().await;
        self.load_meta_store_state()
            .await
            .expect("Failed to load state from meta store");
        let loaded_state = get_state().await;
        assert_eq!(
            mem_state, loaded_state,
            "hummock in-mem state is inconsistent with meta store state",
        );
    }

    pub async fn list_sstable_id_infos(&self) -> Result<Vec<SstableIdInfo>> {
        let versioning_guard = self.versioning.read().await;
        Ok(versioning_guard
            .sstable_id_infos
            .values()
            .cloned()
            .collect_vec())
    }

    pub async fn delete_sstable_ids(&self, sst_ids: impl AsRef<[HummockSSTableId]>) -> Result<()> {
        let mut versioning_guard = self.versioning.write().await;
        let mut transaction = Transaction::default();
        for sst_id in sst_ids.as_ref() {
            SstableIdInfo::delete_in_transaction(SstableRefId { id: *sst_id }, &mut transaction)?;
        }
        self.commit_trx(self.meta_store_ref.as_ref(), transaction, None)
            .await?;

        // Update in-mem state after transaction succeeds.
        for sst_id in sst_ids.as_ref() {
            versioning_guard.sstable_id_infos.remove(sst_id);
        }

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }
}
