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

use std::borrow::{Borrow, BorrowMut};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound::{Excluded, Included};
use std::ops::DerefMut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use fail::fail_point;
use function_name::named;
use itertools::Itertools;
use prost::Message;
use risingwave_common::monitor::rwlock::MonitoredRwLock;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    add_new_sub_level, HummockVersionExt,
};
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockCompactionTaskId, HummockContextId, HummockEpoch, HummockSstableId,
    HummockVersionId, LocalSstableInfo, SstIdRange, FIRST_VERSION_ID, INVALID_VERSION_ID,
};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{
    pin_version_response, CompactTask, CompactTaskAssignment, HummockPinnedSnapshot,
    HummockPinnedVersion, HummockSnapshot, HummockVersion, HummockVersionDelta,
    HummockVersionDeltas, Level, LevelDelta, LevelType, OverlappingLevel, ValidationTask,
};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::MetaLeaderInfo;
use tokio::sync::oneshot::Sender;
use tokio::sync::{Notify, RwLockReadGuard, RwLockWriteGuard};
use tokio::task::JoinHandle;

use crate::hummock::compaction::{CompactStatus, ManualCompactionOption};
use crate::hummock::compaction_group::manager::CompactionGroupManagerRef;
use crate::hummock::compaction_scheduler::CompactionRequestChannelRef;
use crate::hummock::error::{Error, Result};
use crate::hummock::metrics_utils::{
    trigger_pin_unpin_snapshot_state, trigger_pin_unpin_version_state, trigger_sst_stat,
    trigger_version_stat,
};
use crate::hummock::CompactorManagerRef;
use crate::manager::{ClusterManagerRef, IdCategory, LocalNotification, MetaSrvEnv, META_NODE_ID};
use crate::model::{BTreeMapTransaction, MetadataModel, ValTransaction, VarTransaction};
use crate::rpc::metrics::MetaMetrics;
use crate::rpc::{META_CF_NAME, META_LEADER_KEY};
use crate::storage::{MetaStore, Transaction};

mod context;
mod gc;
#[cfg(test)]
mod tests;
mod versioning;
use versioning::*;
mod compaction;
use compaction::*;

// Update to states are performed as follow:
// - Initialize ValTransaction for the meta state to update
// - Make changes on the ValTransaction.
// - Call `commit_multi_var` to commit the changes via meta store transaction. If transaction
//   succeeds, the in-mem state will be updated by the way.
pub struct HummockManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    cluster_manager: ClusterManagerRef<S>,
    compaction_group_manager: CompactionGroupManagerRef<S>,
    // When trying to locks compaction and versioning at the same time, compaction lock should
    // be requested before versioning lock.
    compaction: MonitoredRwLock<Compaction>,
    versioning: MonitoredRwLock<Versioning>,
    max_committed_epoch: AtomicU64,
    max_current_epoch: AtomicU64,

    metrics: Arc<MetaMetrics>,

    // `compaction_request_channel` is used to schedule a compaction for specified
    // CompactionGroupId
    compaction_request_channel: parking_lot::RwLock<Option<CompactionRequestChannelRef>>,
    compaction_resume_notifier: parking_lot::RwLock<Option<Arc<Notify>>>,

    compactor_manager: CompactorManagerRef,
}

pub type HummockManagerRef<S> = Arc<HummockManager<S>>;

/// Commit multiple `ValTransaction`s to state store and upon success update the local in-mem state
/// by the way
/// After called, the `ValTransaction` will be dropped.
macro_rules! commit_multi_var {
    ($hummock_mgr:expr, $context_id:expr, $($val_txn:expr),*) => {
        {
            async {
                let mut trx = Transaction::default();
                // Apply the change in `ValTransaction` to trx
                $(
                    $val_txn.apply_to_txn(&mut trx)?;
                )*
                // Commit to state store
                $hummock_mgr.commit_trx($hummock_mgr.env.meta_store(), trx, $context_id, $hummock_mgr.env.get_leader_info())
                .await?;
                // Upon successful commit, commit the change to local in-mem state
                $(
                    $val_txn.commit();
                )*
                Result::Ok(())
            }.await
        }
    };
}
pub(crate) use commit_multi_var;

/// Acquire read lock of the lock with `lock_name`.
/// The macro will use macro `function_name` to get the name of the function of method that calls
/// the lock, and therefore, anyone to call this macro should ensured that the caller method has the
/// macro #[named]
macro_rules! read_lock {
    ($hummock_mgr:expr, $lock_name:ident) => {
        async {
            $hummock_mgr
                .$lock_name
                .read(&[function_name!(), stringify!($lock_name), "read"])
                .await
        }
    };
}
pub(crate) use read_lock;
use risingwave_pb::common::WorkerType;
use risingwave_pb::hummock::pin_version_response::Payload;

/// Acquire write lock of the lock with `lock_name`.
/// The macro will use macro `function_name` to get the name of the function of method that calls
/// the lock, and therefore, anyone to call this macro should ensured that the caller method has the
/// macro #[named]
macro_rules! write_lock {
    ($hummock_mgr:expr, $lock_name:ident) => {
        async {
            $hummock_mgr
                .$lock_name
                .write(&[function_name!(), stringify!($lock_name), "write"])
                .await
        }
    };
}
pub(crate) use write_lock;

macro_rules! start_measure_real_process_timer {
    ($hummock_mgr:expr) => {
        $hummock_mgr
            .metrics
            .hummock_manager_real_process_time
            .with_label_values(&[function_name!()])
            .start_timer()
    };
}
pub(crate) use start_measure_real_process_timer;

use super::Compactor;

static CACEL_STATUS_SET: LazyLock<HashSet<TaskStatus>> = LazyLock::new(|| {
    [
        TaskStatus::ManualCanceled,
        TaskStatus::SendFailCanceled,
        TaskStatus::AssignFailCanceled,
        TaskStatus::HeartbeatCanceled,
    ]
    .into_iter()
    .collect()
});

#[derive(Debug)]
pub enum CompactionResumeTrigger {
    /// The addition (re-subscription) of compactors
    CompactorAddition { context_id: HummockContextId },
    /// A compaction task is reported when all compactors are not idle.
    TaskReport { original_task_num: usize },
}

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        metrics: Arc<MetaMetrics>,
        compaction_group_manager: CompactionGroupManagerRef<S>,
        compactor_manager: CompactorManagerRef,
    ) -> Result<HummockManager<S>> {
        let instance = HummockManager {
            env,
            versioning: MonitoredRwLock::new(
                metrics.hummock_manager_lock_time.clone(),
                Default::default(),
            ),
            compaction: MonitoredRwLock::new(
                metrics.hummock_manager_lock_time.clone(),
                Default::default(),
            ),
            metrics,
            cluster_manager,
            compaction_group_manager,
            compaction_request_channel: parking_lot::RwLock::new(None),
            compaction_resume_notifier: parking_lot::RwLock::new(None),
            compactor_manager,
            max_committed_epoch: AtomicU64::new(0),
            max_current_epoch: AtomicU64::new(0),
        };

        instance.load_meta_store_state().await?;
        instance.release_invalid_contexts().await?;
        instance.cancel_unassigned_compaction_task().await?;
        // Release snapshots pinned by meta on restarting.
        instance.release_contexts([META_NODE_ID]).await?;
        Ok(instance)
    }

    pub async fn start_compaction_heartbeat(
        hummock_manager: Arc<Self>,
    ) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let compactor_manager = hummock_manager.compactor_manager.clone();
        let join_handle = tokio::spawn(async move {
            let mut min_interval = tokio::time::interval(std::time::Duration::from_millis(1000));
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_interval.tick() => {
                    },
                    // Shutdown
                    _ = &mut shutdown_rx => {
                        tracing::info!("Compaction heartbeat checker is stopped");
                        return;
                    }
                }
                // TODO: add metrics to track expired tasks
                for (context_id, mut task) in compactor_manager.get_expired_tasks() {
                    tracing::info!("Task with task_id {} with context_id {context_id} has expired due to lack of visible progress", task.task_id);
                    if let Some(compactor) = compactor_manager.get_compactor(context_id) {
                        // Forcefully cancel the task so that it terminates early on the compactor
                        // node.
                        let _ = compactor.cancel_task(task.task_id).await;
                        tracing::info!("CancelTask operation for task_id {} has been sent to node with context_id {context_id}", task.task_id);
                    }

                    if let Err(e) = hummock_manager
                        .cancel_compact_task(&mut task, TaskStatus::HeartbeatCanceled)
                        .await
                    {
                        tracing::error!("Attempt to remove compaction task due to elapsed heartbeat failed. We will continue to track its heartbeat
                            until we can successfully report its status. {context_id}, task_id: {}, ERR: {e:?}", task.task_id);
                    }
                }
            }
        });
        (join_handle, shutdown_tx)
    }

    /// Load state from meta store.
    #[named]
    async fn load_meta_store_state(&self) -> Result<()> {
        let mut compaction_guard = write_lock!(self, compaction).await;
        let mut versioning_guard = write_lock!(self, versioning).await;
        self.load_meta_store_state_impl(
            compaction_guard.borrow_mut(),
            versioning_guard.borrow_mut(),
        )
        .await
    }

    /// Load state from meta store.
    async fn load_meta_store_state_impl(
        &self,
        compaction_guard: &mut RwLockWriteGuard<'_, Compaction>,
        versioning_guard: &mut RwLockWriteGuard<'_, Versioning>,
    ) -> Result<()> {
        let compaction_statuses = CompactStatus::list(self.env.meta_store())
            .await?
            .into_iter()
            .map(|cg| (cg.compaction_group_id(), cg))
            .collect::<BTreeMap<CompactionGroupId, CompactStatus>>();
        if !compaction_statuses.is_empty() {
            compaction_guard.compaction_statuses = compaction_statuses;
        } else {
            // Initialize compact status for each compaction group
            let mut compaction_statuses =
                BTreeMapTransaction::new(&mut compaction_guard.compaction_statuses);
            for compaction_group in self.compaction_group_manager.compaction_groups().await {
                let compact_status = CompactStatus::new(
                    compaction_group.group_id(),
                    compaction_group.compaction_config().max_level,
                );
                compaction_statuses.insert(compact_status.compaction_group_id(), compact_status);
            }
            commit_multi_var!(self, None, compaction_statuses)?;
        }
        compaction_guard.compact_task_assignment =
            CompactTaskAssignment::list(self.env.meta_store())
                .await?
                .into_iter()
                .map(|assigned| (assigned.key().unwrap(), assigned))
                .collect();

        let versions = HummockVersion::list(self.env.meta_store()).await?;

        let hummock_version_deltas: BTreeMap<_, _> =
            HummockVersionDelta::list(self.env.meta_store())
                .await?
                .into_iter()
                .map(|version_delta| (version_delta.id, version_delta))
                .collect();

        // Insert the initial version.
        let mut redo_state = if versions.is_empty() {
            let mut init_version = HummockVersion {
                id: FIRST_VERSION_ID,
                levels: Default::default(),
                max_committed_epoch: INVALID_EPOCH,
                safe_epoch: INVALID_EPOCH,
            };
            // Initialize independent levels via corresponding compaction group' config.
            for compaction_group in self.compaction_group_manager.compaction_groups().await {
                let mut levels = vec![];
                for l in 0..compaction_group.compaction_config().max_level {
                    levels.push(Level {
                        level_idx: (l + 1) as u32,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: vec![],
                        total_file_size: 0,
                        sub_level_id: 0,
                    });
                }
                init_version.levels.insert(
                    compaction_group.group_id(),
                    Levels {
                        levels,
                        l0: Some(OverlappingLevel {
                            sub_levels: vec![],
                            total_file_size: 0,
                        }),
                    },
                );
            }
            init_version.insert(self.env.meta_store()).await?;
            init_version
        } else {
            versions.first().unwrap().clone()
        };
        versioning_guard.checkpoint_version = redo_state.clone();

        for version_delta in hummock_version_deltas.values() {
            if version_delta.prev_id == redo_state.id {
                redo_state.apply_version_delta(version_delta);
            }
        }
        self.max_committed_epoch
            .store(redo_state.max_committed_epoch, Ordering::Relaxed);
        self.max_current_epoch
            .fetch_max(redo_state.max_committed_epoch, Ordering::Relaxed);

        versioning_guard.current_version = redo_state;
        versioning_guard.hummock_version_deltas = hummock_version_deltas;

        versioning_guard.pinned_versions = HummockPinnedVersion::list(self.env.meta_store())
            .await?
            .into_iter()
            .map(|p| (p.context_id, p))
            .collect();
        versioning_guard.pinned_snapshots = HummockPinnedSnapshot::list(self.env.meta_store())
            .await?
            .into_iter()
            .map(|p| (p.context_id, p))
            .collect();

        let checkpoint_id = versioning_guard.checkpoint_version.id;
        versioning_guard.ssts_to_delete.clear();
        versioning_guard.extend_ssts_to_delete_from_deltas(..=checkpoint_id);
        let preserved_deltas: HashSet<HummockVersionId> =
            HashSet::from_iter(versioning_guard.ssts_to_delete.values().cloned());
        versioning_guard.deltas_to_delete = versioning_guard
            .hummock_version_deltas
            .keys()
            .cloned()
            .filter(|id| {
                *id <= versioning_guard.checkpoint_version.id && !preserved_deltas.contains(id)
            })
            .collect_vec();

        Ok(())
    }

    /// We use worker node id as the `context_id`.
    /// If the `context_id` is provided, the transaction will abort if the `context_id` is not
    /// valid, which means the worker node is not a valid member of the cluster.
    /// This operation is protected by mutex of compaction, so that other thread can not
    /// call `release_contexts` even if it has removed `context_id` from cluster manager.
    async fn commit_trx(
        &self,
        meta_store: &S,
        mut trx: Transaction,
        context_id: Option<HummockContextId>,
        info: MetaLeaderInfo,
    ) -> Result<()> {
        if let Some(context_id) = context_id {
            if context_id == META_NODE_ID {
                // Using the preserved meta id is allowed.
            } else if !self.check_context(context_id).await {
                // The worker is not found in cluster.
                return Err(Error::InvalidContext(context_id));
            }
        }
        trx.check_equal(
            META_CF_NAME.to_owned(),
            META_LEADER_KEY.as_bytes().to_vec(),
            info.encode_to_vec(),
        );
        meta_store.txn(trx).await.map_err(Into::into)
    }

    /// Pin the current greatest hummock version. The pin belongs to `context_id`
    /// and will be unpinned when `context_id` is invalidated.
    #[named]
    pub async fn pin_version(
        &self,
        context_id: HummockContextId,
    ) -> Result<pin_version_response::Payload> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let versioning = versioning_guard.deref_mut();
        let mut pinned_versions = BTreeMapTransaction::new(&mut versioning.pinned_versions);
        let mut context_pinned_version = pinned_versions.new_entry_txn_or_default(
            context_id,
            HummockPinnedVersion {
                context_id,
                min_pinned_id: INVALID_VERSION_ID,
            },
        );
        let version_id = versioning.current_version.id;
        let ret = Payload::PinnedVersion(versioning.current_version.clone());
        if context_pinned_version.min_pinned_id == INVALID_VERSION_ID
            || context_pinned_version.min_pinned_id > version_id
        {
            context_pinned_version.min_pinned_id = version_id;
            commit_multi_var!(self, Some(context_id), context_pinned_version)?;
            trigger_pin_unpin_version_state(&self.metrics, &versioning.pinned_versions);
        }

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(ret)
    }

    /// Unpin all pins which belongs to `context_id` and has an id which is older than
    /// `unpin_before`. All versions >= `unpin_before` will be treated as if they are all pinned by
    /// this `context_id` so they will not be vacummed.
    #[named]
    pub async fn unpin_version_before(
        &self,
        context_id: HummockContextId,
        unpin_before: HummockVersionId,
    ) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let versioning = versioning_guard.deref_mut();
        let mut pinned_versions = BTreeMapTransaction::new(&mut versioning.pinned_versions);
        let mut context_pinned_version = pinned_versions.new_entry_txn_or_default(
            context_id,
            HummockPinnedVersion {
                context_id,
                min_pinned_id: 0,
            },
        );
        context_pinned_version.min_pinned_id = unpin_before;
        commit_multi_var!(self, Some(context_id), context_pinned_version)?;
        trigger_pin_unpin_version_state(&self.metrics, &versioning.pinned_versions);

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    /// Make sure `max_committed_epoch` is pinned and return it.
    #[named]
    pub async fn pin_snapshot(&self, context_id: HummockContextId) -> Result<HummockSnapshot> {
        let max_committed_epoch = self.max_committed_epoch.load(Ordering::Relaxed);
        let max_current_epoch = self.max_current_epoch.load(Ordering::Relaxed);
        let mut guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let mut pinned_snapshots = BTreeMapTransaction::new(&mut guard.pinned_snapshots);
        let mut context_pinned_snapshot = pinned_snapshots.new_entry_txn_or_default(
            context_id,
            HummockPinnedSnapshot {
                context_id,
                minimal_pinned_snapshot: INVALID_EPOCH,
            },
        );
        if context_pinned_snapshot.minimal_pinned_snapshot == INVALID_EPOCH {
            context_pinned_snapshot.minimal_pinned_snapshot = max_committed_epoch;
            commit_multi_var!(self, Some(context_id), context_pinned_snapshot)?;
            trigger_pin_unpin_snapshot_state(&self.metrics, &guard.pinned_snapshots);
        }
        Ok(HummockSnapshot {
            committed_epoch: max_committed_epoch,
            current_epoch: max_current_epoch,
        })
    }

    pub fn get_last_epoch(&self) -> Result<HummockSnapshot> {
        let max_committed_epoch = self.max_committed_epoch.load(Ordering::Relaxed);
        let max_current_epoch = self.max_current_epoch.load(Ordering::Relaxed);
        Ok(HummockSnapshot {
            committed_epoch: max_committed_epoch,
            current_epoch: max_current_epoch,
        })
    }

    #[named]
    pub async fn unpin_snapshot(&self, context_id: HummockContextId) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let mut pinned_snapshots = BTreeMapTransaction::new(&mut versioning_guard.pinned_snapshots);
        let release_snapshot = pinned_snapshots.remove(context_id);
        if release_snapshot.is_some() {
            commit_multi_var!(self, Some(context_id), pinned_snapshots)?;
            trigger_pin_unpin_snapshot_state(&self.metrics, &versioning_guard.pinned_snapshots);
        }

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    /// Unpin all snapshots smaller than specified epoch for current context.
    #[named]
    pub async fn unpin_snapshot_before(
        &self,
        context_id: HummockContextId,
        hummock_snapshot: HummockSnapshot,
    ) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        // Use the max_committed_epoch in storage as the snapshot ts so only committed changes are
        // visible in the snapshot.
        let max_committed_epoch = versioning_guard.current_version.max_committed_epoch;
        // Ensure the unpin will not clean the latest one.
        let snapshot_committed_epoch = hummock_snapshot.committed_epoch;
        #[cfg(not(test))]
        {
            assert!(snapshot_committed_epoch <= max_committed_epoch);
        }
        let last_read_epoch = std::cmp::min(snapshot_committed_epoch, max_committed_epoch);

        let mut pinned_snapshots = BTreeMapTransaction::new(&mut versioning_guard.pinned_snapshots);
        let mut context_pinned_snapshot = pinned_snapshots.new_entry_txn_or_default(
            context_id,
            HummockPinnedSnapshot {
                context_id,
                minimal_pinned_snapshot: INVALID_EPOCH,
            },
        );

        // Unpin the snapshots pinned by meta but frontend doesn't know. Also equal to unpin all
        // epochs below specific watermark.
        if context_pinned_snapshot.minimal_pinned_snapshot < last_read_epoch
            || context_pinned_snapshot.minimal_pinned_snapshot == INVALID_EPOCH
        {
            context_pinned_snapshot.minimal_pinned_snapshot = last_read_epoch;
            commit_multi_var!(self, Some(context_id), context_pinned_snapshot)?;
            trigger_pin_unpin_snapshot_state(&self.metrics, &versioning_guard.pinned_snapshots);
        }

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    #[named]
    pub async fn get_compact_task_impl(
        &self,
        compaction_group_id: CompactionGroupId,
        manual_compaction_option: Option<ManualCompactionOption>,
    ) -> Result<Option<CompactTask>> {
        let mut compaction_guard = write_lock!(self, compaction).await;
        let compaction = compaction_guard.deref_mut();
        let start_time = Instant::now();
        // StoredIdGenerator already implements ids pre-allocation by ID_PREALLOCATE_INTERVAL.
        let task_id = self
            .env
            .id_gen_manager()
            .generate::<{ IdCategory::HummockCompactionTask }>()
            .await?;
        if !compaction
            .compaction_statuses
            .contains_key(&compaction_group_id)
        {
            let group_config = self
                .compaction_group_manager()
                .compaction_group(compaction_group_id)
                .await
                .ok_or(Error::InvalidCompactionGroup(compaction_group_id))?;
            let mut compact_statuses =
                BTreeMapTransaction::new(&mut compaction.compaction_statuses);
            let new_compact_status = compact_statuses.new_entry_insert_txn(
                compaction_group_id,
                CompactStatus::new(
                    compaction_group_id,
                    group_config.compaction_config().max_level,
                ),
            );
            commit_multi_var!(self, None, new_compact_status)?;
        }
        let mut compact_status = VarTransaction::new(
            compaction
                .compaction_statuses
                .get_mut(&compaction_group_id)
                .ok_or(Error::InvalidCompactionGroup(compaction_group_id))?,
        );
        let (current_version, watermark) = {
            let versioning_guard = read_lock!(self, versioning).await;
            let max_committed_epoch = versioning_guard.current_version.max_committed_epoch;
            let watermark = versioning_guard
                .pinned_snapshots
                .values()
                .map(|v| v.minimal_pinned_snapshot)
                .fold(max_committed_epoch, std::cmp::min);
            (versioning_guard.current_version.clone(), watermark)
        };
        let can_trivial_move = manual_compaction_option.is_none();
        let compact_task = compact_status.get_compact_task(
            current_version.get_compaction_group_levels(compaction_group_id),
            task_id as HummockCompactionTaskId,
            compaction_group_id,
            manual_compaction_option,
            self.get_compaction_config(compaction_group_id).await,
        );
        let mut compact_task = match compact_task {
            None => {
                return Ok(None);
            }
            Some(task) => task,
        };
        compact_task.watermark = watermark;

        if CompactStatus::is_trivial_move_task(&compact_task) && can_trivial_move {
            compact_task.sorted_output_ssts = compact_task.input_ssts[0].table_infos.clone();
            // this task has been finished and `trivial_move_task` does not need to be schedule.
            compact_task.set_task_status(TaskStatus::Success);
            self.report_compact_task_impl(None, &compact_task, Some(compaction_guard))
                .await?;
            tracing::info!(
                "TrivialMove for compaction group {}: pick up {} tables in level {} to compact to target_level {}  cost time: {:?}",
                compaction_group_id,
                compact_task.input_ssts[0].table_infos.len(),
                compact_task.input_ssts[0].level_idx,
                compact_task.target_level,
                start_time.elapsed()
            );
        } else {
            let existing_table_ids_from_meta = self
                .compaction_group_manager
                .internal_table_ids_by_compaction_group_id(compaction_group_id)
                .await?;

            // to get all relational table_id from sst_info
            let table_ids = compact_task
                .input_ssts
                .iter()
                .flat_map(|level| {
                    level
                        .table_infos
                        .iter()
                        .flat_map(|sst_info| sst_info.table_ids.iter().cloned())
                        .collect_vec()
                })
                .collect::<HashSet<u32>>();
            for table_id in table_ids {
                // to found exist table_id from
                if existing_table_ids_from_meta.contains(&table_id) {
                    compact_task.existing_table_ids.push(table_id);
                }
            }

            // build table_options
            let compaction_group = self
                .compaction_group_manager
                .compaction_group(compaction_group_id)
                .await
                .unwrap();
            compact_task.table_options = compaction_group
                .table_id_to_options()
                .iter()
                .filter(|id_to_option| compact_task.existing_table_ids.contains(id_to_option.0))
                .map(|id_to_option| (*id_to_option.0, id_to_option.1.into()))
                .collect();
            compact_task.current_epoch_time = Epoch::now().0;

            compact_task.compaction_filter_mask = self
                .get_compaction_config(compact_status.compaction_group_id())
                .await
                .compaction_filter_mask;
            commit_multi_var!(self, None, compact_status)?;

            // this task has been finished.
            compact_task.set_task_status(TaskStatus::Pending);

            trigger_sst_stat(
                &self.metrics,
                Some(
                    compaction
                        .compaction_statuses
                        .get(&compaction_group_id)
                        .ok_or(Error::InvalidCompactionGroup(compaction_group_id))?,
                ),
                &current_version,
                compaction_group_id,
            );

            tracing::trace!(
                "For compaction group {}: pick up {} tables in level {} to compact.  cost time: {:?}",
                compaction_group_id,
                compact_task.input_ssts[0].table_infos.len(),
                compact_task.input_ssts[0].level_idx,
                start_time.elapsed()
            );
            drop(compaction_guard);
        }
        #[cfg(test)]
        {
            self.check_state_consistency().await;
        }

        Ok(Some(compact_task))
    }

    /// Cancels a compaction task no matter it's assigned or unassigned.
    pub async fn cancel_compact_task(
        &self,
        compact_task: &mut CompactTask,
        task_status: TaskStatus,
    ) -> Result<bool> {
        compact_task.set_task_status(task_status);
        fail_point!("fp_cancel_compact_task", |_| Err(Error::MetaStore(
            anyhow::anyhow!("failpoint metastore err")
        )));
        self.cancel_compact_task_impl(compact_task).await
    }

    pub async fn cancel_compact_task_impl(&self, compact_task: &CompactTask) -> Result<bool> {
        assert!(CACEL_STATUS_SET.contains(&compact_task.task_status()));
        self.report_compact_task_impl(None, compact_task, None)
            .await
    }

    pub async fn get_compact_task(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> Result<Option<CompactTask>> {
        fail_point!("fp_get_compact_task", |_| Err(Error::MetaStore(
            anyhow::anyhow!("failpoint metastore error")
        )));
        while let Some(task) = self
            .get_compact_task_impl(compaction_group_id, None)
            .await?
        {
            if let TaskStatus::Pending = task.task_status() {
                return Ok(Some(task));
            }
            assert!(CompactStatus::is_trivial_move_task(&task));
        }
        Ok(None)
    }

    pub async fn manual_get_compact_task(
        &self,
        compaction_group_id: CompactionGroupId,
        manual_compaction_option: ManualCompactionOption,
    ) -> Result<Option<CompactTask>> {
        self.get_compact_task_impl(compaction_group_id, Some(manual_compaction_option))
            .await
    }

    #[named]
    pub async fn get_idle_compactor(&self) -> Option<Arc<Compactor>> {
        let compaction_guard = read_lock!(self, compaction).await;
        // Calculate the number of tasks assigned to each compactor.
        let mut compactor_assigned_task_num = HashMap::new();
        compaction_guard
            .compact_task_assignment
            .values()
            .for_each(|assignment| {
                compactor_assigned_task_num
                    .entry(assignment.context_id)
                    .and_modify(|n| *n += 1)
                    .or_insert(1);
            });
        drop(compaction_guard);
        self.compactor_manager
            .next_idle_compactor(&compactor_assigned_task_num)
    }

    /// Assign a compaction task to the compactor identified by `assignee_context_id`.
    #[named]
    pub async fn assign_compaction_task(
        &self,
        compact_task: &CompactTask,
        assignee_context_id: HummockContextId,
    ) -> Result<()> {
        fail_point!("assign_compaction_task_fail", |_| Err(anyhow::anyhow!(
            "assign_compaction_task_fail"
        )
        .into()));
        let mut compaction_guard = write_lock!(self, compaction).await;
        let _timer = start_measure_real_process_timer!(self);

        // Assign the task.
        let compaction = compaction_guard.deref_mut();
        let mut compact_task_assignment =
            BTreeMapTransaction::new(&mut compaction.compact_task_assignment);
        if let Some(assignment) = compact_task_assignment.get(&compact_task.task_id) {
            return Err(Error::CompactionTaskAlreadyAssigned(
                compact_task.task_id,
                assignment.context_id,
            ));
        }
        compact_task_assignment.insert(
            compact_task.task_id,
            CompactTaskAssignment {
                compact_task: Some(compact_task.clone()),
                context_id: assignee_context_id,
            },
        );
        commit_multi_var!(self, Some(assignee_context_id), compact_task_assignment)?;
        // Update compaction scheudle policy.
        self.compactor_manager
            .assign_compact_task(assignee_context_id, compact_task)?;

        // Initiate heartbeat for the task to track its progress.
        self.compactor_manager
            .initiate_task_heartbeat(assignee_context_id, compact_task.clone());

        #[cfg(test)]
        {
            drop(compaction_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    pub async fn report_compact_task(
        &self,
        context_id: HummockContextId,
        compact_task: &CompactTask,
    ) -> Result<bool> {
        let ret = self
            .report_compact_task_impl(Some(context_id), compact_task, None)
            .await?;

        Ok(ret)
    }

    /// Finishes or cancels a compaction task, according to `task_status`.
    ///
    /// If `context_id` is not None, its validity will be checked when writing meta store.
    /// Its ownership of the task is checked as well.
    ///
    /// Return Ok(false) indicates either the task is not found,
    /// or the task is not owned by `context_id` when `context_id` is not None.
    #[named]
    pub async fn report_compact_task_impl(
        &self,
        context_id: Option<HummockContextId>,
        compact_task: &CompactTask,
        compaction_guard: Option<RwLockWriteGuard<'_, Compaction>>,
    ) -> Result<bool> {
        let mut compaction_guard = match compaction_guard {
            None => write_lock!(self, compaction).await,
            Some(compaction_guard) => compaction_guard,
        };

        let deterministic_mode = self.env.opts.compaction_deterministic_test;
        let compaction = compaction_guard.deref_mut();
        let start_time = Instant::now();
        let mut compact_status = VarTransaction::new(
            compaction
                .compaction_statuses
                .get_mut(&compact_task.compaction_group_id)
                .ok_or(Error::InvalidCompactionGroup(
                    compact_task.compaction_group_id,
                ))?,
        );
        let assigned_task_num = compaction.compact_task_assignment.len();
        let mut compact_task_assignment =
            BTreeMapTransaction::new(&mut compaction.compact_task_assignment);
        let assignee_context_id = compact_task_assignment
            .remove(compact_task.task_id)
            .map(|assignment| assignment.context_id);

        // For context_id is None, there is no need to check the task assignment.
        if let Some(context_id) = context_id {
            match assignee_context_id {
                Some(id) => {
                    // Assignee id mismatch.
                    if id != context_id {
                        tracing::warn!(
                            "Wrong reporter {}. Compaction task {} is assigned to {}",
                            context_id,
                            compact_task.task_id,
                            *assignee_context_id.as_ref().unwrap(),
                        );
                        return Ok(false);
                    }
                }
                None => {
                    // The task is not found.
                    tracing::warn!("Compaction task {} not found", compact_task.task_id);
                    return Ok(false);
                }
            }
        }
        compact_status.report_compact_task(compact_task);
        let task_status = compact_task.task_status();
        debug_assert!(
            task_status != TaskStatus::Pending,
            "report pending compaction task"
        );
        if let TaskStatus::Success = task_status {
            // The compaction task is finished.
            let mut versioning_guard = write_lock!(self, versioning).await;
            let versioning = versioning_guard.deref_mut();
            let current_version = &mut versioning.current_version;
            let mut hummock_version_deltas =
                BTreeMapTransaction::new(&mut versioning.hummock_version_deltas);
            let version_delta = gen_version_delta(
                &mut hummock_version_deltas,
                current_version,
                compact_task,
                CompactStatus::is_trivial_move_task(compact_task),
                deterministic_mode,
            );

            commit_multi_var!(
                self,
                context_id,
                compact_status,
                compact_task_assignment,
                hummock_version_deltas
            )?;

            current_version.apply_version_delta(&version_delta);

            trigger_version_stat(&self.metrics, current_version);

            if deterministic_mode {
                self.env.notification_manager().notify_asynchronously(
                    WorkerType::RiseCtl,
                    Operation::Add,
                    Info::HummockVersionDeltas(risingwave_pb::hummock::HummockVersionDeltas {
                        version_deltas: vec![version_delta],
                    }),
                );
            }

            self.env
                .notification_manager()
                .notify_compute_asynchronously(
                    Operation::Add,
                    Info::HummockVersionDeltas(risingwave_pb::hummock::HummockVersionDeltas {
                        version_deltas: vec![versioning
                            .hummock_version_deltas
                            .last_key_value()
                            .unwrap()
                            .1
                            .clone()],
                    }),
                );
        } else {
            // The compaction task is cancelled or failed.
            commit_multi_var!(self, context_id, compact_status, compact_task_assignment)?;
        }

        let task_label = task_status.as_str_name();
        if let Some(context_id) = assignee_context_id {
            // A task heartbeat is removed IFF we report the task status of a task and it still has
            // a valid assignment, OR we remove the node context from our list of nodes,
            // in which case the associated heartbeats are forcefully purged.
            self.compactor_manager
                .remove_task_heartbeat(context_id, compact_task.task_id);
            // Also, if the task is already assigned, we need to update the compaction schedule
            // policy.
            self.compactor_manager
                .report_compact_task(context_id, compact_task);
            // Tell compaction scheduler to resume compaction if there's any compactor becoming
            // available.
            if assigned_task_num == self.compactor_manager.max_concurrent_task_number() {
                self.try_resume_compaction(CompactionResumeTrigger::TaskReport {
                    original_task_num: assigned_task_num,
                });
            }
            // Update compaction task count.
            //
            // A corner case is that the compactor is deleted
            // immediately after it reports the task and before the meta node handles
            // it. In that case, its host address will not be obtainable.
            if let Some(worker) = self.cluster_manager.get_worker_by_id(context_id).await {
                let host = worker.worker_node.host.unwrap();
                self.metrics
                    .compact_frequency
                    .with_label_values(&[
                        &format!("{}:{}", host.host, host.port),
                        &compact_task.compaction_group_id.to_string(),
                        task_label,
                    ])
                    .inc();
            }
        } else {
            // Update compaction task count. The task will be marked as `unassigned`.
            self.metrics
                .compact_frequency
                .with_label_values(&[
                    "unassigned",
                    &compact_task.compaction_group_id.to_string(),
                    task_label,
                ])
                .inc();
        }

        tracing::trace!(
            "Reported compaction task. {}. cost time: {:?}",
            compact_task_to_string(compact_task),
            start_time.elapsed(),
        );

        trigger_sst_stat(
            &self.metrics,
            Some(
                compaction
                    .compaction_statuses
                    .get(&compact_task.compaction_group_id)
                    .ok_or(Error::InvalidCompactionGroup(
                        compact_task.compaction_group_id,
                    ))?,
            ),
            read_lock!(self, versioning).await.current_version.borrow(),
            compact_task.compaction_group_id,
        );

        if !deterministic_mode {
            self.try_send_compaction_request(compact_task.compaction_group_id)?;
        }

        #[cfg(test)]
        {
            drop(compaction_guard);
            self.check_state_consistency().await;
        }

        Ok(true)
    }

    /// Caller should ensure `epoch` > `max_committed_epoch`
    #[named]
    pub async fn commit_epoch(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<LocalSstableInfo>,
        sst_to_context: HashMap<HummockSstableId, HummockContextId>,
    ) -> Result<()> {
        // Warn of table_ids that is not found in expected compaction group.
        // It indicates:
        // 1. Either these table_ids are never registered to any compaction group. This is FATAL
        // since compaction filter will remove these valid states incorrectly.
        // 2. Or the owners of these table_ids have been dropped, but their stale states are still
        // committed. This is OK since compaction filter will remove these stale states
        // later.
        for (compaction_group_id, sst) in &sstables {
            let compaction_group = self
                .compaction_group_manager
                .compaction_group(*compaction_group_id)
                .await
                .unwrap_or_else(|| panic!("compaction group {} exists", compaction_group_id));
            for table_id in sst
                .table_ids
                .iter()
                .filter(|t| !compaction_group.member_table_ids().contains(t))
            {
                tracing::warn!(
                    "table {} doesn't belong to expected compaction group {}",
                    table_id,
                    compaction_group_id
                );
            }
        }

        if self.env.opts.enable_committed_sst_sanity_check {
            async {
                if sstables.is_empty() {
                    return;
                }
                let compactor = match self.compactor_manager.next_compactor() {
                    None => {
                        tracing::warn!(
                            "Skip committed SST sanity check due to no available worker"
                        );
                        return;
                    }
                    Some(compactor) => compactor,
                };
                let sst_infos = sstables.iter().map(|(_, sst)| sst.clone()).collect_vec();
                if compactor
                    .send_task(Task::ValidationTask(ValidationTask {
                        sst_infos,
                        sst_id_to_worker_id: sst_to_context.clone(),
                        epoch,
                    }))
                    .await
                    .is_err()
                {
                    tracing::warn!("Skip committed SST sanity check due to send failure");
                }
            }
            .await;
        }

        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        // Prevent commit new epochs if this flag is set
        if versioning_guard.disable_commit_epochs {
            return Ok(());
        }

        for (sst_id, context_id) in &sst_to_context {
            #[cfg(test)]
            {
                if *context_id == META_NODE_ID {
                    continue;
                }
            }
            if !self.check_context(*context_id).await {
                return Err(Error::InvalidSst(*sst_id));
            }
        }

        let old_version = versioning_guard.current_version.clone();
        let new_version_id = old_version.id + 1;
        let versioning = versioning_guard.deref_mut();
        let mut hummock_version_deltas =
            BTreeMapTransaction::new(&mut versioning.hummock_version_deltas);
        let mut new_version_delta = hummock_version_deltas.new_entry_insert_txn(
            new_version_id,
            HummockVersionDelta {
                prev_id: old_version.id,
                safe_epoch: old_version.safe_epoch,
                trivial_move: false,
                ..Default::default()
            },
        );
        let mut new_hummock_version = old_version;
        new_hummock_version.id = new_version_id;
        new_version_delta.id = new_version_id;

        if epoch <= new_hummock_version.max_committed_epoch {
            return Err(anyhow::anyhow!(
                "Epoch {} <= max_committed_epoch {}",
                epoch,
                new_hummock_version.max_committed_epoch
            )
            .into());
        }

        let mut modified_compaction_groups = vec![];
        // Append SSTs to a new version.
        for (compaction_group_id, sstables) in &sstables
            .into_iter()
            // the sort is stable sort, and will not change the order within compaction group.
            // Do a sort so that sst in the same compaction group can be consecutive
            .sorted_by_key(|(cg_id, _)| *cg_id)
            .group_by(|(cg_id, _)| *cg_id)
        {
            modified_compaction_groups.push(compaction_group_id);
            let group_sstables = sstables.into_iter().map(|(_, sst)| sst).collect_vec();
            let level_deltas = &mut new_version_delta
                .level_deltas
                .entry(compaction_group_id)
                .or_default()
                .level_deltas;
            let version_l0 = new_hummock_version
                .get_compaction_group_levels_mut(compaction_group_id)
                .l0
                .as_mut()
                .expect("Expect level 0 is not empty");
            let l0_sub_level_id = epoch;
            let level_delta = LevelDelta {
                level_idx: 0,
                inserted_table_infos: group_sstables.clone(),
                l0_sub_level_id,
                ..Default::default()
            };
            level_deltas.push(level_delta);

            add_new_sub_level(version_l0, l0_sub_level_id, group_sstables);
        }

        // Create a new_version, possibly merely to bump up the version id and max_committed_epoch.
        new_version_delta.max_committed_epoch = epoch;
        new_hummock_version.max_committed_epoch = epoch;
        commit_multi_var!(self, None, new_version_delta)?;
        versioning.current_version = new_hummock_version;
        self.max_committed_epoch.store(epoch, Ordering::Release);
        self.max_current_epoch.fetch_max(epoch, Ordering::Release);

        trigger_version_stat(&self.metrics, &versioning.current_version);
        for compaction_group_id in &modified_compaction_groups {
            trigger_sst_stat(
                &self.metrics,
                None,
                &versioning.current_version,
                *compaction_group_id,
            );
        }

        tracing::trace!("new committed epoch {}", epoch);

        self.env
            .notification_manager()
            .notify_frontend_asynchronously(
                Operation::Update, // Frontends don't care about operation.
                Info::HummockSnapshot(HummockSnapshot {
                    committed_epoch: epoch,
                    current_epoch: self.max_current_epoch.load(Ordering::Relaxed),
                }),
            );
        self.env
            .notification_manager()
            .notify_compute_asynchronously(
                Operation::Add,
                Info::HummockVersionDeltas(risingwave_pb::hummock::HummockVersionDeltas {
                    version_deltas: vec![versioning
                        .hummock_version_deltas
                        .last_key_value()
                        .unwrap()
                        .1
                        .clone()],
                }),
            );

        drop(versioning_guard);
        // Don't trigger compactions if we enable deterministic compaction
        if !self.env.opts.compaction_deterministic_test {
            // commit_epoch may contains SSTs from any compaction group
            for id in modified_compaction_groups {
                self.try_send_compaction_request(id)?;
            }
        }
        #[cfg(test)]
        {
            self.check_state_consistency().await;
        }
        Ok(())
    }

    /// We don't commit an epoch without checkpoint. We will only update the `max_current_epoch`.
    pub fn update_current_epoch(&self, max_current_epoch: HummockEpoch) -> Result<()> {
        // We only update `max_current_epoch`!
        let original_epoch = self
            .max_current_epoch
            .fetch_max(max_current_epoch, Ordering::Release);
        assert!(original_epoch < max_current_epoch);
        tracing::trace!("new current epoch {}", max_current_epoch);
        self.env
            .notification_manager()
            .notify_frontend_asynchronously(
                Operation::Update, // Frontends don't care about operation.
                Info::HummockSnapshot(HummockSnapshot {
                    committed_epoch: self.max_committed_epoch.load(Ordering::Relaxed),
                    current_epoch: max_current_epoch,
                }),
            );
        Ok(())
    }

    pub async fn get_new_sst_ids(&self, number: u32) -> Result<SstIdRange> {
        // TODO: refactor id generator to u64
        assert!(number <= (i32::MAX as u32), "number overflow");
        let start_id = self
            .env
            .id_gen_manager()
            .generate_interval::<{ IdCategory::HummockSstableId }>(number as i32)
            .await
            .map(|id| id as u64)?;
        assert!(start_id <= u64::MAX - number as u64, "SST id overflow");
        Ok(SstIdRange::new(start_id, start_id + number as u64))
    }

    /// Tries to checkpoint at min_pinned_version_id
    ///
    /// Returns the diff between new and old checkpoint id.
    #[named]
    pub async fn proceed_version_checkpoint(&self) -> Result<u64> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let min_pinned_version_id = versioning_guard.min_pinned_version_id();
        if min_pinned_version_id <= versioning_guard.checkpoint_version.id {
            return Ok(0);
        }
        let versioning = versioning_guard.deref_mut();
        let mut checkpoint = VarTransaction::new(&mut versioning.checkpoint_version);
        let old_checkpoint_id = checkpoint.id;
        let mut new_checkpoint_id = min_pinned_version_id;
        for (_, version_delta) in versioning
            .hummock_version_deltas
            .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
        {
            checkpoint.apply_version_delta(version_delta);
        }
        new_checkpoint_id = checkpoint.id;
        if new_checkpoint_id == old_checkpoint_id {
            return Ok(0);
        }
        commit_multi_var!(self, None, checkpoint)?;
        versioning.extend_ssts_to_delete_from_deltas((
            Excluded(old_checkpoint_id),
            Included(new_checkpoint_id),
        ));
        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }
        self.metrics
            .checkpoint_version_id
            .set(new_checkpoint_id as i64);
        Ok(new_checkpoint_id - old_checkpoint_id)
    }

    #[named]
    pub async fn get_min_pinned_version_id(&self) -> HummockVersionId {
        read_lock!(self, versioning).await.min_pinned_version_id()
    }

    // TODO: use proc macro to call check_state_consistency
    #[named]
    #[cfg(test)]
    pub async fn check_state_consistency(&self) {
        use std::borrow::Borrow;
        let mut compaction_guard = write_lock!(self, compaction).await;
        let mut versioning_guard = write_lock!(self, versioning).await;
        let get_state =
            |compaction_guard: &RwLockWriteGuard<'_, Compaction>,
             versioning_guard: &RwLockWriteGuard<'_, Versioning>| {
                let compact_statuses_copy = compaction_guard.compaction_statuses.clone();
                let compact_task_assignment_copy = compaction_guard.compact_task_assignment.clone();
                let pinned_versions_copy = versioning_guard.pinned_versions.clone();
                let pinned_snapshots_copy = versioning_guard.pinned_snapshots.clone();
                let checkpoint_version_copy = versioning_guard.checkpoint_version.clone();
                let hummock_version_deltas_copy = versioning_guard.hummock_version_deltas.clone();
                (
                    compact_statuses_copy,
                    compact_task_assignment_copy,
                    pinned_versions_copy,
                    pinned_snapshots_copy,
                    checkpoint_version_copy,
                    hummock_version_deltas_copy,
                )
            };
        let mem_state = get_state(compaction_guard.borrow(), versioning_guard.borrow());
        self.load_meta_store_state_impl(
            compaction_guard.borrow_mut(),
            versioning_guard.borrow_mut(),
        )
        .await
        .expect("Failed to load state from meta store");
        let loaded_state = get_state(compaction_guard.borrow(), versioning_guard.borrow());
        assert_eq!(
            mem_state, loaded_state,
            "hummock in-mem state is inconsistent with meta store state",
        );
    }

    /// Gets current version without pinning it.
    #[named]
    pub async fn get_current_version(&self) -> HummockVersion {
        read_lock!(self, versioning).await.current_version.clone()
    }

    /// Get version deltas from meta store
    pub async fn list_version_deltas(
        &self,
        start_id: u64,
        num_limit: u32,
    ) -> Result<HummockVersionDeltas> {
        let ordered_version_deltas: BTreeMap<_, _> =
            HummockVersionDelta::list(self.env.meta_store())
                .await?
                .into_iter()
                .map(|version_delta| (version_delta.id, version_delta))
                .collect();

        let version_deltas = ordered_version_deltas
            .into_iter()
            .filter(|(id, _)| *id >= start_id)
            .map(|(_, v)| v)
            .take(num_limit as _)
            .collect();
        Ok(HummockVersionDeltas { version_deltas })
    }

    #[named]
    pub async fn get_read_guard(&self) -> RwLockReadGuard<'_, Versioning> {
        read_lock!(self, versioning).await
    }

    /// Reset current version to empty
    #[named]
    pub async fn reset_current_version(&self) -> Result<HummockVersion> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        // Reset current version to empty
        let mut init_version = HummockVersion {
            id: FIRST_VERSION_ID,
            levels: Default::default(),
            max_committed_epoch: INVALID_EPOCH,
            safe_epoch: INVALID_EPOCH,
        };

        // Initialize independent levels via corresponding compaction group' config.
        for compaction_group in self.compaction_group_manager.compaction_groups().await {
            let mut levels = vec![];
            for l in 0..compaction_group.compaction_config().max_level {
                levels.push(Level {
                    level_idx: (l + 1) as u32,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: vec![],
                    total_file_size: 0,
                    sub_level_id: 0,
                });
            }
            init_version.levels.insert(
                compaction_group.group_id(),
                Levels {
                    levels,
                    l0: Some(OverlappingLevel {
                        sub_levels: vec![],
                        total_file_size: 0,
                    }),
                },
            );
        }

        let old_version = versioning_guard.current_version.clone();
        versioning_guard.current_version = init_version;
        Ok(old_version)
    }

    /// Replay a version delta to current hummock version.
    /// Returns the `version_id`, `max_committed_epoch` of the new version and the modified
    /// compaction groups
    #[named]
    pub async fn replay_version_delta(
        &self,
        version_delta_id: HummockVersionId,
    ) -> Result<(HummockVersionId, HummockEpoch, Vec<CompactionGroupId>)> {
        let result = HummockVersionDelta::select(self.env.meta_store(), &version_delta_id).await?;
        // the version delta must exist
        assert!(result.is_some());

        let mut version_delta = result.unwrap();
        let mut versioning_guard = write_lock!(self, versioning).await;
        // ensure the version id is ascending after replay
        version_delta.id = versioning_guard.current_version.id + 1;
        version_delta.prev_id = version_delta.id - 1;
        versioning_guard
            .current_version
            .apply_version_delta(&version_delta);
        assert!(versioning_guard.current_version.id >= version_delta_id);

        let compaction_group_ids = version_delta.level_deltas.keys().cloned().collect_vec();
        let version_id = versioning_guard.current_version.id;
        let max_committed_epoch = versioning_guard.current_version.max_committed_epoch;

        // notify our testing tool
        self.env.notification_manager().notify_asynchronously(
            WorkerType::RiseCtl,
            Operation::Add,
            Info::HummockVersionDeltas(risingwave_pb::hummock::HummockVersionDeltas {
                version_deltas: vec![version_delta],
            }),
        );
        Ok((version_id, max_committed_epoch, compaction_group_ids))
    }

    #[named]
    pub async fn disable_commit_epoch(&self) -> HummockVersion {
        let mut versioning_guard = write_lock!(self, versioning).await;
        versioning_guard.disable_commit_epochs = true;
        versioning_guard.current_version.clone()
    }

    #[named]
    async fn get_compact_task_assignment_size(&self) -> usize {
        let compaction_guard = read_lock!(self, compaction).await;
        compaction_guard.compact_task_assignment.len()
    }

    /// Triggers compacitons to specified compaction groups.
    /// Returns the version deltas generated by compaction tasks
    #[cfg_attr(coverage, no_coverage)]
    pub async fn trigger_compaction_deterministic(
        &self,
        base_version_id: HummockVersionId,
        compaction_groups: Vec<CompactionGroupId>,
    ) -> Result<(HummockVersionId, HummockEpoch)> {
        tracing::info!(
            "Trigger compaction for version {}, groups {:?}",
            base_version_id,
            compaction_groups
        );
        let old_version = self.get_current_version().await;
        assert_eq!(
            base_version_id, old_version.id,
            "old_version epoch {}",
            old_version.max_committed_epoch
        );

        if compaction_groups.is_empty() {
            return Ok((old_version.id, old_version.max_committed_epoch));
        }

        let old_size = self.get_compact_task_assignment_size().await;
        for compaction_group in compaction_groups {
            self.try_send_compaction_request(compaction_group)?;
        }
        let (schedule_ok, task_num) = self.poll_compaction_schedule_status(old_size).await;
        let (compaction_ok, cur_version) = self
            .poll_compaction_tasks_status(schedule_ok, task_num, &old_version)
            .await;

        tracing::info!(
            "Compaction schedule_ok {}, task_num {} compaction_ok {}",
            schedule_ok,
            task_num,
            compaction_ok,
        );

        if !schedule_ok && !compaction_ok {
            return Ok((old_version.id, old_version.max_committed_epoch));
        }
        Ok((cur_version.id, cur_version.max_committed_epoch))
    }

    /// Poll the compaction task assignment to aware whether scheduling is success.
    /// Returns (whether scheduling is success, number of tasks)
    #[cfg_attr(coverage, no_coverage)]
    async fn poll_compaction_schedule_status(&self, old_size: usize) -> (bool, usize) {
        let poll_timeout = Duration::from_secs(2);
        let poll_interval = Duration::from_millis(20);
        let mut poll_duration_cnt = Duration::from_millis(0);
        let mut new_size = self.get_compact_task_assignment_size().await;
        let mut schedule_ok = false;
        loop {
            if new_size > old_size {
                schedule_ok = true;
                break;
            }

            if poll_duration_cnt >= poll_timeout {
                break;
            }
            tokio::time::sleep(poll_interval).await;
            poll_duration_cnt += poll_interval;
            new_size = self.get_compact_task_assignment_size().await;
        }
        (schedule_ok, new_size - old_size)
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn poll_compaction_tasks_status(
        &self,
        schedule_ok: bool,
        task_num: usize,
        old_version: &HummockVersion,
    ) -> (bool, HummockVersion) {
        // Polls current version to check whether its id become large,
        // which means compaction tasks have finished
        let poll_timeout = if schedule_ok {
            Duration::from_secs(120)
        } else {
            Duration::from_secs(5)
        };
        let poll_interval = Duration::from_millis(50);
        let mut duration_cnt = Duration::from_millis(0);
        let mut compaction_ok = false;
        let mut cur_version = self.get_current_version().await;
        loop {
            if (cur_version.id > old_version.id)
                && (cur_version.id - old_version.id >= task_num as u64)
            {
                tracing::info!("Collected all of compact tasks");
                compaction_ok = true;
                break;
            }
            if duration_cnt >= poll_timeout {
                break;
            }
            tokio::time::sleep(poll_interval).await;
            duration_cnt += poll_interval;
            cur_version = self.get_current_version().await;
        }
        (compaction_ok, cur_version)
    }

    pub fn init_compaction_scheduler(
        &self,
        sched_channel: CompactionRequestChannelRef,
        notifier: Arc<Notify>,
    ) {
        *self.compaction_request_channel.write() = Some(sched_channel);
        *self.compaction_resume_notifier.write() = Some(notifier);
    }

    /// Cancels pending compaction tasks which are not yet assigned to any compactor.
    #[named]
    async fn cancel_unassigned_compaction_task(&self) -> Result<()> {
        let mut compaction_guard = write_lock!(self, compaction).await;
        let compaction = compaction_guard.deref_mut();
        let mut compact_statuses = BTreeMapTransaction::new(&mut compaction.compaction_statuses);
        let mut cancelled_count = 0;
        let mut modified_group_status = vec![];
        for (group_id, compact_status) in compact_statuses.tree_ref().iter() {
            let mut compact_status = compact_status.clone();
            let count = compact_status.cancel_compaction_tasks_if(|pending_task_id| {
                !compaction
                    .compact_task_assignment
                    .contains_key(&pending_task_id)
            });
            if count > 0 {
                cancelled_count += count;
                modified_group_status.push((*group_id, compact_status));
            }
        }
        for (group_id, compact_status) in modified_group_status {
            compact_statuses.insert(group_id, compact_status);
        }
        if cancelled_count > 0 {
            commit_multi_var!(self, None, compact_statuses)?;
        }
        #[cfg(test)]
        {
            drop(compaction_guard);
            self.check_state_consistency().await;
        }
        Ok(())
    }

    /// Sends a compaction request to compaction scheduler.
    pub fn try_send_compaction_request(&self, compaction_group: CompactionGroupId) -> Result<bool> {
        if let Some(sender) = self.compaction_request_channel.read().as_ref() {
            sender
                .try_sched_compaction(compaction_group)
                .map_err(|e| Error::Internal(anyhow::anyhow!(e.to_string())))
        } else {
            Ok(false) // maybe this should be an Err, but we need this to be Ok for tests.
        }
    }

    /// Tell compaction scheduler to resume compaction.
    pub fn try_resume_compaction(&self, trigger: CompactionResumeTrigger) {
        tracing::debug!("resume compaction, trigger: {:?}", trigger);
        if let Some(notifier) = self.compaction_resume_notifier.read().as_ref() {
            notifier.notify_one();
        }
    }

    pub async fn trigger_manual_compaction(
        &self,
        compaction_group: CompactionGroupId,
        manual_compaction_option: ManualCompactionOption,
    ) -> Result<()> {
        let start_time = Instant::now();

        // 1. Get idle compactor.
        let compactor = match self.get_idle_compactor().await {
            Some(compactor) => compactor,
            None => {
                tracing::warn!("trigger_manual_compaction No compactor is available.");
                return Err(anyhow::anyhow!(
                    "trigger_manual_compaction No compactor is available. compaction_group {}",
                    compaction_group
                )
                .into());
            }
        };

        // 2. Get manual compaction task.
        let compact_task = self
            .manual_get_compact_task(compaction_group, manual_compaction_option)
            .await;
        let compact_task = match compact_task {
            Ok(Some(compact_task)) => compact_task,
            Ok(None) => {
                // No compaction task available.
                return Err(anyhow::anyhow!(
                    "trigger_manual_compaction No compaction_task is available. compaction_group {}",
                    compaction_group
                ).into());
            }
            Err(err) => {
                tracing::warn!("Failed to get compaction task: {:#?}.", err);
                return Err(anyhow::anyhow!(
                    "Failed to get compaction task: {:#?} compaction_group {}",
                    err,
                    compaction_group
                )
                .into());
            }
        };

        // Locally cancel task if fails to assign or send task.
        let locally_cancel_task = |compact_task| async {
            self.env
                .notification_manager()
                .notify_local_subscribers(LocalNotification::CompactionTaskNeedCancel(compact_task))
                .await;
            Err(Error::Internal(anyhow::anyhow!(
                "Failed to trigger_manual_compaction"
            )))
        };

        // 2. Assign the task to the previously picked compactor.
        if let Err(err) = self
            .assign_compaction_task(&compact_task, compactor.context_id())
            .await
        {
            tracing::warn!("Failed to assign compaction task to compactor: {:#?}", err);
            return locally_cancel_task(compact_task).await;
        };

        // 3. Send the task.
        if let Err(e) = compactor
            .send_task(Task::CompactTask(compact_task.clone()))
            .await
        {
            tracing::warn!(
                "Failed to send task {} to {}. {:#?}",
                compact_task.task_id,
                compactor.context_id(),
                e
            );
            return locally_cancel_task(compact_task).await;
        }

        tracing::info!(
            "Trigger manual compaction task. {}. cost time: {:?}",
            compact_task_to_string(&compact_task),
            start_time.elapsed(),
        );

        Ok(())
    }

    pub fn compactor_manager_ref_for_test(&self) -> CompactorManagerRef {
        self.compactor_manager.clone()
    }

    #[named]
    pub async fn compaction_task_from_assignment_for_test(
        &self,
        task_id: u64,
    ) -> Option<CompactTaskAssignment> {
        let compaction_guard = read_lock!(self, compaction).await;
        let assignment_ref = &compaction_guard.compact_task_assignment;
        assignment_ref.get(&task_id).cloned()
    }

    pub fn compaction_group_manager(&self) -> CompactionGroupManagerRef<S> {
        self.compaction_group_manager.clone()
    }

    pub fn cluster_manager(&self) -> &ClusterManagerRef<S> {
        &self.cluster_manager
    }
}

fn gen_version_delta<'a>(
    txn: &mut BTreeMapTransaction<'a, HummockVersionId, HummockVersionDelta>,
    old_version: &HummockVersion,
    compact_task: &CompactTask,
    trivial_move: bool,
    deterministic_mode: bool,
) -> HummockVersionDelta {
    let mut version_delta = HummockVersionDelta {
        prev_id: old_version.id,
        max_committed_epoch: old_version.max_committed_epoch,
        trivial_move,
        ..Default::default()
    };
    let level_deltas = &mut version_delta
        .level_deltas
        .entry(compact_task.compaction_group_id)
        .or_default()
        .level_deltas;
    for level in &compact_task.input_ssts {
        let level_delta = LevelDelta {
            level_idx: level.level_idx,
            removed_table_ids: level.table_infos.iter().map(|sst| sst.id).collect_vec(),
            ..Default::default()
        };
        level_deltas.push(level_delta);
    }
    let level_delta = LevelDelta {
        level_idx: compact_task.target_level,
        inserted_table_infos: compact_task.sorted_output_ssts.clone(),
        l0_sub_level_id: compact_task.target_sub_level_id,
        ..Default::default()
    };
    level_deltas.push(level_delta);
    version_delta.safe_epoch = std::cmp::max(old_version.safe_epoch, compact_task.watermark);
    version_delta.id = old_version.id + 1;
    // Don't persist version delta generated by compaction to meta store in deterministic mode.
    // Because it will overwrite existing version delta that has same ID generated in the data
    // ingestion phase.
    if !deterministic_mode {
        txn.insert(version_delta.id, version_delta.clone());
    }

    version_delta
}
