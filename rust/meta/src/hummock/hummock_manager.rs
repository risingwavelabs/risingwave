use std::sync::Arc;

use itertools::{enumerate, Itertools};
use prost::Message;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::hummock::hummock_version::HummockVersionRefId;
use risingwave_pb::hummock::{
    CompactTask, HummockContextPinnedSnapshot, HummockContextPinnedVersion, HummockContextRefId,
    HummockSnapshot, HummockTablesToDelete, HummockVersion, Level, LevelType, SstableInfo,
    SstableRefId, UncommittedEpoch,
};
use risingwave_storage::hummock::{
    HummockContextId, HummockEpoch, HummockRefCount, HummockSSTableId, HummockVersionId,
    INVALID_EPOCH,
};
use tokio::sync::{Mutex, RwLock};

use crate::hummock::compaction::CompactStatus;
use crate::hummock::level_handler::{LevelHandler, SSTableStat};
use crate::hummock::model::{
    CurrentHummockVersionId, HummockContextPinnedSnapshotExt, HummockContextPinnedVersionExt,
};
use crate::manager::{IdCategory, IdGeneratorManagerRef, MetaSrvEnv};
use crate::model::{MetadataModel, Transactional, Worker};
use crate::rpc::metrics::MetaMetrics;
use crate::storage::{Error, MetaStore, Transaction};

pub struct HummockManager<S> {
    id_gen_manager_ref: IdGeneratorManagerRef<S>,
    // 1. When trying to locks compaction and versioning at the same time, compaction lock should
    // be requested before versioning lock.
    // 2. The two meta_store_ref from versioning and compaction point to the same meta store
    // instance. By convention we always use the one from compaction to get and commit meta store
    // transaction when both are available.
    // 3. TODO: Currently we don't prevent a meta store
    // transaction from living longer than the compaction guard or versioning guard it was
    // requested from. We need to fix it.
    compaction: Mutex<Compaction<S>>,
    versioning: RwLock<Versioning<S>>,

    metrics: Arc<MetaMetrics>,
}

struct Compaction<S> {
    meta_store_ref: Arc<S>,
}

struct Versioning<S> {
    meta_store_ref: Arc<S>,
}

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>, metrics: Arc<MetaMetrics>) -> Result<HummockManager<S>> {
        let instance = HummockManager {
            id_gen_manager_ref: env.id_gen_manager_ref(),
            versioning: RwLock::new(Versioning {
                meta_store_ref: env.meta_store_ref(),
            }),
            compaction: Mutex::new(Compaction {
                meta_store_ref: env.meta_store_ref(),
            }),
            metrics,
        };

        instance.initialize_meta().await?;

        Ok(instance)
    }

    /// Restore the table related data in meta store to a consistent state.
    async fn initialize_meta(&self) -> Result<()> {
        let compaction_guard = self.compaction.lock().await;
        let versioning_guard = self.versioning.write().await;
        // initialize metadata only if CurrentHummockVersionId is not found in metastore
        match CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref()).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                if !matches!(err.inner(), ErrorCode::ItemNotFound(_)) {
                    return Err(err);
                }
                true
            }
        };

        let mut transaction = Transaction::default();

        let compact_status = CompactStatus::new();
        compact_status.update_in_transaction(&mut transaction);

        let init_version_id = CurrentHummockVersionId::new();
        init_version_id.update_in_transaction(&mut transaction);

        let init_version = &HummockVersion {
            id: init_version_id.id(),
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
        init_version.upsert_in_transaction(&mut transaction)?;

        // TODO #546: Cancel all compact_tasks

        self.commit_trx(compaction_guard.meta_store_ref.as_ref(), transaction, None)
            .await
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
        let versioning_guard = self.versioning.write().await;
        let version_id = CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref())
            .await?
            .id();
        let hummock_version = HummockVersion::select(
            &*versioning_guard.meta_store_ref,
            &HummockVersionRefId { id: version_id },
        )
        .await?
        .unwrap();
        // pin the version
        let mut context_pinned_version = HummockContextPinnedVersion::select(
            &*versioning_guard.meta_store_ref,
            &HummockContextRefId { id: context_id },
        )
        .await?
        .unwrap_or(HummockContextPinnedVersion {
            context_id,
            version_id: vec![],
        });
        context_pinned_version.pin_version(version_id);
        let mut transaction = Transaction::default();
        context_pinned_version.update_in_transaction(&mut transaction)?;
        self.commit_trx(
            versioning_guard.meta_store_ref.as_ref(),
            transaction,
            Some(context_id),
        )
        .await?;

        Ok(hummock_version)
    }

    pub async fn unpin_version(
        &self,
        context_id: HummockContextId,
        pinned_version_id: HummockVersionId,
    ) -> Result<()> {
        let versioning_guard = self.versioning.write().await;
        let mut transaction = Transaction::default();
        let mut context_pinned_version = match HummockContextPinnedVersion::select(
            &*versioning_guard.meta_store_ref,
            &HummockContextRefId { id: context_id },
        )
        .await?
        {
            None => {
                return Ok(());
            }
            Some(context_pinned_version) => context_pinned_version,
        };
        context_pinned_version.unpin_version(pinned_version_id);
        context_pinned_version.update_in_transaction(&mut transaction)?;
        self.commit_trx(
            versioning_guard.meta_store_ref.as_ref(),
            transaction,
            Some(context_id),
        )
        .await
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

    pub async fn add_tables(
        &self,
        context_id: HummockContextId,
        tables: Vec<SstableInfo>,
        epoch: HummockEpoch,
    ) -> Result<HummockVersion> {
        let stats = tables.iter().map(SSTableStat::from).collect_vec();

        // Hold the compact status lock so that no one else could add/drop SST or search compaction
        // plan.
        let compaction_guard = self.compaction.lock().await;
        let mut compact_status =
            CompactStatus::get(compaction_guard.meta_store_ref.as_ref()).await?;
        match compact_status.level_handlers.first_mut().unwrap() {
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
        // update compact_status
        compact_status.update_in_transaction(&mut transaction);

        let versioning_guard = self.versioning.write().await;
        let mut current_version_id =
            CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref()).await?;
        let old_version_id = current_version_id.id();
        let mut hummock_version = HummockVersion::select(
            &*versioning_guard.meta_store_ref,
            &HummockVersionRefId { id: old_version_id },
        )
        .await?
        .unwrap();

        let current_tables = SstableInfo::list(&*versioning_guard.meta_store_ref).await?;
        if tables
            .iter()
            .any(|t| current_tables.iter().any(|ct| ct.id == t.id))
        {
            // Retry an add_tables request is OK if the original request has completed successfully.
            return Ok(hummock_version);
        }

        // check whether the epoch is valid
        // TODO: return error instead of panic
        // TODO: the validation is temporarily disabled until
        // the new barrier manager design is integrated
        // if epoch <= hummock_version.max_committed_epoch {
        //   panic!(
        //     "Epoch {} <= max_committed_epoch {}",
        //     epoch, hummock_version.max_committed_epoch
        //   );
        // }

        // add tables
        for table in &tables {
            table.upsert_in_transaction(&mut transaction)?
        }

        // create new_version by adding tables in UncommittedEpoch
        match hummock_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch)
        {
            Some(uncommitted_epoch) => {
                tables
                    .iter()
                    .for_each(|t| uncommitted_epoch.table_ids.push(t.id));
            }
            None => hummock_version.uncommitted_epochs.push(UncommittedEpoch {
                epoch,
                table_ids: tables.iter().map(|t| t.id).collect(),
            }),
        };
        current_version_id.increase();
        current_version_id.update_in_transaction(&mut transaction);
        hummock_version.id = current_version_id.id();
        hummock_version.upsert_in_transaction(&mut transaction)?;

        // the trx contain update for both tables and compact_status
        self.commit_trx(
            compaction_guard.meta_store_ref.as_ref(),
            transaction,
            Some(context_id),
        )
        .await?;

        self.trigger_sst_stat(&compact_status);

        Ok(hummock_version)
    }

    pub async fn pin_snapshot(&self, context_id: HummockContextId) -> Result<HummockSnapshot> {
        let versioning_guard = self.versioning.write().await;

        // Use the max_committed_epoch in storage as the snapshot ts so only committed changes are
        // visible in the snapshot.
        let version_id = CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref())
            .await?
            .id();
        let version = HummockVersion::select(
            &*versioning_guard.meta_store_ref,
            &HummockVersionRefId { id: version_id },
        )
        .await?
        .unwrap();
        let max_committed_epoch = version.max_committed_epoch;
        let mut context_pinned_snapshot = HummockContextPinnedSnapshot::select(
            &*versioning_guard.meta_store_ref,
            &HummockContextRefId { id: context_id },
        )
        .await?
        .unwrap_or(HummockContextPinnedSnapshot {
            context_id,
            snapshot_id: vec![],
        });
        context_pinned_snapshot.pin_snapshot(max_committed_epoch);
        let mut transaction = Transaction::default();
        context_pinned_snapshot.update_in_transaction(&mut transaction)?;
        self.commit_trx(
            versioning_guard.meta_store_ref.as_ref(),
            transaction,
            Some(context_id),
        )
        .await?;
        Ok(HummockSnapshot {
            epoch: max_committed_epoch,
        })
    }

    pub async fn unpin_snapshot(
        &self,
        context_id: HummockContextId,
        hummock_snapshot: HummockSnapshot,
    ) -> Result<()> {
        let versioning_guard = self.versioning.write().await;

        let mut context_pinned_snapshot = match HummockContextPinnedSnapshot::select(
            &*versioning_guard.meta_store_ref,
            &HummockContextRefId { id: context_id },
        )
        .await?
        {
            None => {
                return Ok(());
            }
            Some(context_pinned_snapshot) => context_pinned_snapshot,
        };
        let mut transaction = Transaction::default();
        context_pinned_snapshot.unpin_snapshot(hummock_snapshot.epoch);
        context_pinned_snapshot.update_in_transaction(&mut transaction)?;
        self.commit_trx(
            versioning_guard.meta_store_ref.as_ref(),
            transaction,
            Some(context_id),
        )
        .await
    }

    pub async fn get_compact_task(&self) -> Result<Option<CompactTask>> {
        let watermark = {
            let versioning_guard = self.versioning.read().await;
            let current_version_id =
                CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref())
                    .await?
                    .id();
            let version_data = HummockVersion::select(
                &*versioning_guard.meta_store_ref,
                &HummockVersionRefId {
                    id: current_version_id,
                },
            )
            .await?
            .unwrap();
            HummockContextPinnedSnapshot::list(&*versioning_guard.meta_store_ref)
                .await?
                .iter()
                .flat_map(|v| v.snapshot_id.clone())
                .fold(version_data.max_committed_epoch, std::cmp::min)
        };
        let compaction_guard = self.compaction.lock().await;
        let mut compact_status =
            CompactStatus::get(compaction_guard.meta_store_ref.as_ref()).await?;
        let compact_task = compact_status.get_compact_task();
        match compact_task {
            None => Ok(None),
            Some(mut compact_task) => {
                let mut transaction = Transaction::default();
                compact_status.update_in_transaction(&mut transaction);
                self.commit_trx(compaction_guard.meta_store_ref.as_ref(), transaction, None)
                    .await?;
                compact_task.watermark = watermark;
                Ok(Some(compact_task))
            }
        }
    }

    /// `report_compact_task` is retryable. `task_id` in `compact_task` parameter is used as the
    /// idempotency key. Return Ok(false) to indicate the `task_id` is not found, which may have
    /// been processed previously.
    pub async fn report_compact_task(
        &self,
        compact_task: CompactTask,
        task_result: bool,
    ) -> Result<bool> {
        let output_table_compact_entries: Vec<_> = compact_task
            .sorted_output_ssts
            .iter()
            .map(SSTableStat::from)
            .collect();
        let compaction_guard = self.compaction.lock().await;
        let mut transaction = Transaction::default();
        let mut compact_status =
            CompactStatus::get(compaction_guard.meta_store_ref.as_ref()).await?;

        let compact_task_id = compact_task.task_id;
        let input_sst_count: usize = compact_task
            .input_ssts
            .iter()
            .map(|v| v.level.as_ref().unwrap().table_ids.len())
            .sum();
        let output_sst_count = compact_task.sorted_output_ssts.len();

        let (sorted_output_ssts, delete_table_ids) = match compact_status.report_compact_task(
            output_table_compact_entries,
            compact_task,
            task_result,
        ) {
            None => {
                // The task is not found.
                return Ok(false);
            }
            Some((sorted_output_ssts, delete_table_ids)) => (sorted_output_ssts, delete_table_ids),
        };
        compact_status.update_in_transaction(&mut transaction);
        let versioning_guard = self.versioning.write().await;
        if task_result {
            let mut current_version_id =
                CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref()).await?;
            let old_version_id = current_version_id.increase();
            let new_version_id = current_version_id.id();
            current_version_id.update_in_transaction(&mut transaction);
            let old_version = HummockVersion::select(
                &*versioning_guard.meta_store_ref,
                &HummockVersionRefId { id: old_version_id },
            )
            .await?
            .unwrap();
            let mut version = HummockVersion {
                id: new_version_id,
                levels: compact_status
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
                uncommitted_epochs: old_version.uncommitted_epochs,
                max_committed_epoch: old_version.max_committed_epoch,
            };

            for table in sorted_output_ssts {
                table.upsert_in_transaction(&mut transaction)?
            }

            version.id = new_version_id;
            version.upsert_in_transaction(&mut transaction)?;
            let mut tables_to_delete = HummockTablesToDelete::select(
                &*versioning_guard.meta_store_ref,
                &HummockVersionRefId { id: old_version_id },
            )
            .await?
            .unwrap_or(HummockTablesToDelete {
                version_id: new_version_id,
                id: vec![],
            });
            tables_to_delete.id.extend(delete_table_ids);
            if tables_to_delete.id.is_empty() {
                HummockTablesToDelete::delete_in_transaction(
                    HummockVersionRefId {
                        id: tables_to_delete.version_id,
                    },
                    &mut transaction,
                )?;
            } else {
                tables_to_delete.upsert_in_transaction(&mut transaction)?;
            }
        }
        self.commit_trx(compaction_guard.meta_store_ref.as_ref(), transaction, None)
            .await?;
        if task_result {
            tracing::debug!(
                "Finish hummock compaction task id {}, compact {} SSTs to {} SSTs",
                compact_task_id,
                input_sst_count,
                output_sst_count
            );
        } else {
            tracing::debug!("Cancel hummock compaction task id {}", compact_task_id);
        }
        self.trigger_sst_stat(&compact_status);
        Ok(true)
    }

    pub async fn commit_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        let versioning_guard = self.versioning.write().await;
        let mut transaction = Transaction::default();
        let mut current_version_id =
            CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref()).await?;
        let old_version_id = current_version_id.increase();
        let new_version_id = current_version_id.id();
        current_version_id.update_in_transaction(&mut transaction);
        let mut hummock_version = HummockVersion::select(
            &*versioning_guard.meta_store_ref,
            &HummockVersionRefId { id: old_version_id },
        )
        .await?
        .unwrap();
        // TODO: return error instead of panic
        if epoch <= hummock_version.max_committed_epoch {
            panic!(
                "Epoch {} <= max_committed_epoch {}",
                epoch, hummock_version.max_committed_epoch
            );
        }

        // TODO #447: the epoch should fail and rollback if any precedent epoch is uncommitted.
        // get tables in the committing epoch
        if let Some(idx) = hummock_version
            .uncommitted_epochs
            .iter()
            .position(|e| e.epoch == epoch)
        {
            let uncommitted_epoch = &hummock_version.uncommitted_epochs[idx];

            // commit tables by moving them into level0
            let version_first_level = hummock_version.levels.first_mut().unwrap();
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
            hummock_version.uncommitted_epochs.swap_remove(idx);
        }
        // Create a new_version, possibly merely to bump up the version id and max_committed_epoch.
        hummock_version.max_committed_epoch = epoch;
        hummock_version.id = new_version_id;
        hummock_version.upsert_in_transaction(&mut transaction)?;
        self.commit_trx(versioning_guard.meta_store_ref.as_ref(), transaction, None)
            .await?;
        tracing::trace!("new committed epoch {}", epoch);
        Ok(())
    }

    pub async fn abort_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        let versioning_guard = self.versioning.write().await;
        let mut transaction = Transaction::default();
        let mut current_version_id =
            CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref()).await?;
        let old_version_id = current_version_id.increase();
        let new_version_id = current_version_id.id();
        current_version_id.update_in_transaction(&mut transaction);
        let mut hummock_version = HummockVersion::select(
            &*versioning_guard.meta_store_ref,
            &HummockVersionRefId { id: old_version_id },
        )
        .await?
        .unwrap();

        // get tables in the committing epoch
        match hummock_version
            .uncommitted_epochs
            .iter()
            .position(|e| e.epoch == epoch)
        {
            Some(idx) => {
                let uncommitted_epoch = &hummock_version.uncommitted_epochs[idx];

                // remove tables of the aborting epoch
                for table_id in &uncommitted_epoch.table_ids {
                    SstableInfo::delete_in_transaction(
                        SstableRefId { id: *table_id },
                        &mut transaction,
                    )?;
                }
                hummock_version.uncommitted_epochs.swap_remove(idx);

                // create new_version
                hummock_version.id = new_version_id;
                hummock_version.upsert_in_transaction(&mut transaction)?;

                self.commit_trx(versioning_guard.meta_store_ref.as_ref(), transaction, None)
                    .await
            }
            None => Ok(()),
        }
    }

    pub async fn get_new_table_id(&self) -> Result<HummockSSTableId> {
        // TODO id_gen_manager generates u32, we need u64
        self.id_gen_manager_ref
            .generate::<{ IdCategory::HummockSSTableId }>()
            .await
            .map(|id| id as HummockSSTableId)
    }

    pub async fn release_context_resource(&self, context_id: HummockContextId) -> Result<()> {
        let versioning_guard = self.versioning.write().await;
        let mut transaction = Transaction::default();
        let pinned_version = HummockContextPinnedVersion::select(
            &*versioning_guard.meta_store_ref,
            &HummockContextRefId { id: context_id },
        )
        .await?;
        let mut to_commit = false;
        if let Some(pinned_version) = pinned_version {
            HummockContextPinnedVersion::delete_in_transaction(
                HummockContextRefId {
                    id: pinned_version.context_id,
                },
                &mut transaction,
            )?;
            to_commit = true;
        }
        let pinned_snapshot = HummockContextPinnedSnapshot::select(
            &*versioning_guard.meta_store_ref,
            &HummockContextRefId { id: context_id },
        )
        .await?;
        if let Some(pinned_snapshot) = pinned_snapshot {
            HummockContextPinnedSnapshot::delete_in_transaction(
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
        self.commit_trx(versioning_guard.meta_store_ref.as_ref(), transaction, None)
            .await
    }

    /// List version ids in ascending order. TODO: support limit parameter
    pub async fn list_version_ids_asc(&self) -> Result<Vec<HummockVersionId>> {
        let versioning_guard = self.versioning.read().await;
        let version_ids = HummockVersion::list(versioning_guard.meta_store_ref.as_ref())
            .await?
            .iter()
            .map(|version| version.id)
            .sorted()
            .collect_vec();
        Ok(version_ids)
    }

    /// Get the reference count of given version id
    pub async fn get_version_pin_count(
        &self,
        version_id: HummockVersionId,
    ) -> Result<HummockRefCount> {
        let versioning_guard = self.versioning.read().await;
        let version_pins =
            HummockContextPinnedVersion::list(versioning_guard.meta_store_ref.as_ref()).await?;
        let count = version_pins
            .iter()
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
        let ssts_to_delete = HummockTablesToDelete::select(
            versioning_guard.meta_store_ref.as_ref(),
            &HummockVersionRefId { id: version_id },
        )
        .await?;
        match ssts_to_delete {
            None => Ok(vec![]),
            Some(ssts) => Ok(ssts.id),
        }
    }

    /// Delete metadata of the given `version_id`
    pub async fn delete_version(&self, version_id: HummockVersionId) -> Result<()> {
        let versioning_guard = self.versioning.write().await;
        let mut transaction = Transaction::default();
        let key = HummockVersionRefId { id: version_id };
        // Delete record in HummockVersion if any.
        let version =
            HummockVersion::select(versioning_guard.meta_store_ref.as_ref(), &key).await?;
        if let Some(version) = version {
            HummockVersion::delete_in_transaction(
                HummockVersionRefId { id: version.id },
                &mut transaction,
            )?;
        }
        // Delete record in HummockTablesToDelete if any.
        let ssts_to_delete =
            HummockTablesToDelete::select(versioning_guard.meta_store_ref.as_ref(), &key).await?;
        if let Some(ssts_to_delete) = ssts_to_delete {
            // Delete tracked sstables.
            for sst_id in ssts_to_delete.id {
                SstableInfo::delete_in_transaction(SstableRefId { id: sst_id }, &mut transaction)?;
            }
            HummockTablesToDelete::delete_in_transaction(
                HummockVersionRefId {
                    id: ssts_to_delete.version_id,
                },
                &mut transaction,
            )?;
        }
        // Delete record in HummockContextPinnedVersion if any.
        let version_pins =
            HummockContextPinnedVersion::list(versioning_guard.meta_store_ref.as_ref()).await?;
        for mut version_pin in version_pins {
            if version_pin.version_id.contains(&version_id) {
                version_pin.unpin_version(version_id);
                version_pin.update_in_transaction(&mut transaction)?;
            }
        }

        self.commit_trx(versioning_guard.meta_store_ref.as_ref(), transaction, None)
            .await
    }
}
