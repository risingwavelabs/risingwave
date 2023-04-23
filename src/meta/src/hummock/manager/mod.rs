// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::panic;
use std::borrow::{Borrow, BorrowMut};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::DerefMut;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use arc_swap::ArcSwap;
use fail::fail_point;
use function_name::named;
use itertools::Itertools;
use risingwave_common::monitor::rwlock::MonitoredRwLock;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    add_new_sub_level, build_version_delta_after_version, get_compaction_group_ids,
    try_get_compaction_group_id_by_table_id, BranchedSstInfo, HummockVersionExt,
    HummockVersionUpdateExt,
};
use risingwave_hummock_sdk::{
    version_checkpoint_path, CompactionGroupId, ExtendedSstableInfo, HummockCompactionTaskId,
    HummockContextId, HummockEpoch, HummockSstableId, HummockSstableObjectId, HummockVersionId,
    SstObjectIdRange, INVALID_VERSION_ID,
};
use risingwave_pb::hummock::compact_task::{self, TaskStatus};
use risingwave_pb::hummock::group_delta::DeltaType;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{
    version_update_payload, CompactTask, CompactTaskAssignment, CompactionConfig, GroupDelta,
    HummockPinnedSnapshot, HummockPinnedVersion, HummockSnapshot, HummockVersion,
    HummockVersionCheckpoint, HummockVersionDelta, HummockVersionDeltas, HummockVersionStats,
    IntraLevelDelta, LevelType, TableOption,
};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use tokio::sync::oneshot::Sender;
use tokio::sync::{Notify, RwLockWriteGuard};
use tokio::task::JoinHandle;

use crate::hummock::compaction::{
    CompactStatus, LocalSelectorStatistic, ManualCompactionOption, ScaleCompactorInfo,
};
use crate::hummock::compaction_scheduler::CompactionRequestChannelRef;
use crate::hummock::error::{Error, Result};
use crate::hummock::metrics_utils::{
    trigger_delta_log_stats, trigger_lsm_stat, trigger_pin_unpin_snapshot_state,
    trigger_pin_unpin_version_state, trigger_sst_stat, trigger_version_stat,
};
use crate::hummock::CompactorManagerRef;
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, IdCategory, LocalNotification, MetaSrvEnv, META_NODE_ID,
};
use crate::model::{
    BTreeMapEntryTransaction, BTreeMapTransaction, MetadataModel, ValTransaction, VarTransaction,
};
use crate::rpc::metrics::MetaMetrics;
use crate::storage::{MetaStore, Transaction};

mod compaction_group_manager;
mod context;
mod gc;
#[cfg(test)]
mod tests;
mod versioning;
pub use versioning::HummockVersionSafePoint;
use versioning::*;
mod checkpoint;
mod compaction;
mod worker;

use compaction::*;

type Snapshot = ArcSwap<HummockSnapshot>;

// Update to states are performed as follow:
// - Initialize ValTransaction for the meta state to update
// - Make changes on the ValTransaction.
// - Call `commit_multi_var` to commit the changes via meta store transaction. If transaction
//   succeeds, the in-mem state will be updated by the way.
pub struct HummockManager<S: MetaStore> {
    pub env: MetaSrvEnv<S>,
    cluster_manager: ClusterManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    // `CompactionGroupManager` manages `CompactionGroup`'s members.
    // Note that all hummock state store user should register to `CompactionGroupManager`. It
    // includes all state tables of streaming jobs except sink.
    compaction_group_manager: tokio::sync::RwLock<CompactionGroupManager>,
    // When trying to locks compaction and versioning at the same time, compaction lock should
    // be requested before versioning lock.
    compaction: MonitoredRwLock<Compaction>,
    versioning: MonitoredRwLock<Versioning>,
    latest_snapshot: Snapshot,

    metrics: Arc<MetaMetrics>,

    // `compaction_request_channel` is used to schedule a compaction for specified
    // CompactionGroupId
    compaction_request_channel: parking_lot::RwLock<Option<CompactionRequestChannelRef>>,
    compaction_resume_notifier: parking_lot::RwLock<Option<Arc<Notify>>>,
    compaction_tasks_to_cancel: parking_lot::Mutex<Vec<HummockCompactionTaskId>>,

    pub compactor_manager: CompactorManagerRef,
    event_sender: HummockManagerEventSender,

    object_store: ObjectStoreRef,
    version_checkpoint_path: String,
}

pub type HummockManagerRef<S> = Arc<HummockManager<S>>;

/// Commit multiple `ValTransaction`s to state store and upon success update the local in-mem state
/// by the way
/// After called, the `ValTransaction` will be dropped.
macro_rules! commit_multi_var {
    ($hummock_mgr:expr, $context_id:expr, $trx_extern_part:expr, $($val_txn:expr),*) => {
        {
            async {
                let mut trx = $trx_extern_part;
                // Apply the change in `ValTransaction` to trx
                $(
                    $val_txn.apply_to_txn(&mut trx)?;
                )*
                // Commit to state store
                $hummock_mgr.commit_trx($hummock_mgr.env.meta_store(), trx, $context_id)
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
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::table_stats::{
    add_prost_table_stats_map, purge_prost_table_stats, PbTableStatsMap,
};
use risingwave_object_store::object::{parse_remote_object_store, ObjectStoreRef};
use risingwave_pb::catalog::Table;
use risingwave_pb::hummock::version_update_payload::Payload;
use risingwave_pb::hummock::PbCompactionGroupInfo;
use risingwave_pb::meta::relation::RelationInfo;

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

use super::compaction::{LevelSelector, ManualCompactionSelector};
use super::Compactor;
use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::manager::compaction_group_manager::CompactionGroupManager;
use crate::hummock::manager::worker::HummockManagerEventSender;

pub static CANCEL_STATUS_SET: LazyLock<HashSet<TaskStatus>> = LazyLock::new(|| {
    [
        TaskStatus::ManualCanceled,
        TaskStatus::SendFailCanceled,
        TaskStatus::AssignFailCanceled,
        TaskStatus::HeartbeatCanceled,
        TaskStatus::InvalidGroupCanceled,
        TaskStatus::NoAvailResourceCanceled,
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
    pub(crate) async fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        catalog_manager: CatalogManagerRef<S>,
    ) -> Result<HummockManagerRef<S>> {
        let compaction_group_manager = Self::build_compaction_group_manager(&env).await?;
        Self::new_impl(
            env,
            cluster_manager,
            metrics,
            compactor_manager,
            compaction_group_manager,
            catalog_manager,
        )
        .await
    }

    #[cfg(any(test, feature = "test"))]
    pub(super) async fn with_config(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        config: CompactionConfig,
    ) -> HummockManagerRef<S> {
        use crate::manager::CatalogManager;
        let compaction_group_manager =
            Self::build_compaction_group_manager_with_config(&env, config)
                .await
                .unwrap();
        let catalog_manager = Arc::new(CatalogManager::new(env.clone()).await.unwrap());
        Self::new_impl(
            env,
            cluster_manager,
            metrics,
            compactor_manager,
            compaction_group_manager,
            catalog_manager,
        )
        .await
        .unwrap()
    }

    async fn new_impl(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        compaction_group_manager: tokio::sync::RwLock<CompactionGroupManager>,
        catalog_manager: CatalogManagerRef<S>,
    ) -> Result<HummockManagerRef<S>> {
        let sys_params = env.system_params_manager().get_params().await;
        let state_store_url = sys_params.state_store();
        let state_store_dir = sys_params.data_directory();
        let object_store = Arc::new(
            parse_remote_object_store(
                state_store_url.strip_prefix("hummock+").unwrap_or("memory"),
                metrics.object_store_metric.clone(),
                "Version Checkpoint",
            )
            .await,
        );
        let checkpoint_path = version_checkpoint_path(state_store_dir);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
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
            catalog_manager,
            compaction_group_manager,
            compaction_request_channel: parking_lot::RwLock::new(None),
            compaction_resume_notifier: parking_lot::RwLock::new(None),
            compaction_tasks_to_cancel: parking_lot::Mutex::new(vec![]),
            compactor_manager,
            latest_snapshot: ArcSwap::from_pointee(HummockSnapshot {
                committed_epoch: INVALID_EPOCH,
                current_epoch: INVALID_EPOCH,
            }),
            event_sender: tx,
            object_store,
            version_checkpoint_path: checkpoint_path,
        };
        let instance = Arc::new(instance);
        instance.start_worker(rx).await;
        instance.load_meta_store_state().await?;
        instance.release_invalid_contexts().await?;
        instance.cancel_unassigned_compaction_task().await?;
        // Release snapshots pinned by meta on restarting.
        instance.release_meta_context().await?;
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
                let mut split_cancel = {
                    let mut manager_cancel = hummock_manager.compaction_tasks_to_cancel.lock();
                    manager_cancel.drain(..).collect_vec()
                };
                split_cancel.sort();
                split_cancel.dedup();
                // TODO: add metrics to track expired tasks
                for (context_id, mut task) in compactor_manager.get_expired_tasks(split_cancel) {
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
        }
        compaction_guard.compact_task_assignment =
            CompactTaskAssignment::list(self.env.meta_store())
                .await?
                .into_iter()
                .map(|assigned| (assigned.key().unwrap(), assigned))
                .collect();

        let hummock_version_deltas: BTreeMap<_, _> =
            HummockVersionDelta::list(self.env.meta_store())
                .await?
                .into_iter()
                .map(|version_delta| (version_delta.id, version_delta))
                .collect();

        let mut redo_state = if self.need_init().await? {
            // For backward compatibility, try to read checkpoint from meta store.
            let versions = HummockVersion::list(self.env.meta_store()).await?;
            let checkpoint_version = if !versions.is_empty() {
                let checkpoint = versions.into_iter().next().unwrap();
                tracing::warn!(
                    "read hummock version checkpoint from meta store: {:#?}",
                    checkpoint
                );
                checkpoint
            } else {
                // As no record found in stores, create a initial version.
                let checkpoint = create_init_version();
                tracing::info!("init hummock version checkpoint");
                HummockVersionStats::default()
                    .insert(self.env.meta_store())
                    .await?;
                checkpoint
            };
            versioning_guard.checkpoint = HummockVersionCheckpoint {
                version: Some(checkpoint_version.clone()),
                stale_objects: Default::default(),
            };
            self.write_checkpoint(&versioning_guard.checkpoint).await?;
            self.mark_init().await?;
            checkpoint_version
        } else {
            // Read checkpoint from object store.
            versioning_guard.checkpoint = self.read_checkpoint().await?.expect("checkpoint exists");
            versioning_guard
                .checkpoint
                .version
                .as_ref()
                .cloned()
                .unwrap()
        };
        versioning_guard.version_stats = HummockVersionStats::list(self.env.meta_store())
            .await?
            .into_iter()
            .next()
            .expect("should contain exact one item");
        for version_delta in hummock_version_deltas.values() {
            if version_delta.prev_id == redo_state.id {
                redo_state.apply_version_delta(version_delta);
            }
        }
        self.latest_snapshot.store(
            HummockSnapshot {
                committed_epoch: redo_state.max_committed_epoch,
                current_epoch: redo_state.max_committed_epoch,
            }
            .into(),
        );
        versioning_guard.current_version = redo_state;
        versioning_guard.branched_ssts = versioning_guard.current_version.build_branched_sst_info();
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

        versioning_guard.objects_to_delete.clear();
        versioning_guard.mark_objects_for_deletion();

        let all_group_ids = get_compaction_group_ids(&versioning_guard.current_version);
        let configs = self
            .compaction_group_manager
            .read()
            .await
            .get_compaction_group_configs(&all_group_ids);
        versioning_guard.write_limit =
            calc_new_write_limits(configs, HashMap::new(), &versioning_guard.current_version);
        tracing::info!("Hummock stopped write: {:#?}", versioning_guard.write_limit);

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
        trx: Transaction,
        context_id: Option<HummockContextId>,
    ) -> Result<()> {
        if let Some(context_id) = context_id {
            if context_id == META_NODE_ID {
                // Using the preserved meta id is allowed.
            } else if !self.check_context(context_id).await {
                // The worker is not found in cluster.
                return Err(Error::InvalidContext(context_id));
            }
        }

        meta_store.txn(trx).await.map_err(Into::into)
    }

    /// Pin the current greatest hummock version. The pin belongs to `context_id`
    /// and will be unpinned when `context_id` is invalidated.
    #[named]
    pub async fn pin_version(
        &self,
        context_id: HummockContextId,
    ) -> Result<version_update_payload::Payload> {
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
            commit_multi_var!(
                self,
                Some(context_id),
                Transaction::default(),
                context_pinned_version
            )?;
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
        commit_multi_var!(
            self,
            Some(context_id),
            Transaction::default(),
            context_pinned_version
        )?;
        trigger_pin_unpin_version_state(&self.metrics, &versioning.pinned_versions);

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    #[named]
    pub async fn pin_specific_snapshot(
        &self,
        context_id: HummockContextId,
        epoch: HummockEpoch,
    ) -> Result<HummockSnapshot> {
        let snapshot = self.latest_snapshot.load();
        let mut guard = write_lock!(self, versioning).await;
        let mut pinned_snapshots = BTreeMapTransaction::new(&mut guard.pinned_snapshots);
        let mut context_pinned_snapshot = pinned_snapshots.new_entry_txn_or_default(
            context_id,
            HummockPinnedSnapshot {
                context_id,
                minimal_pinned_snapshot: INVALID_EPOCH,
            },
        );
        let epoch_to_pin = std::cmp::min(epoch, snapshot.committed_epoch);
        if context_pinned_snapshot.minimal_pinned_snapshot == INVALID_EPOCH {
            context_pinned_snapshot.minimal_pinned_snapshot = epoch_to_pin;
            commit_multi_var!(
                self,
                Some(context_id),
                Transaction::default(),
                context_pinned_snapshot
            )?;
        }
        Ok(HummockSnapshot::clone(&snapshot))
    }

    /// Make sure `max_committed_epoch` is pinned and return it.
    #[named]
    pub async fn pin_snapshot(&self, context_id: HummockContextId) -> Result<HummockSnapshot> {
        let snapshot = self.latest_snapshot.load();
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
            context_pinned_snapshot.minimal_pinned_snapshot = snapshot.committed_epoch;
            commit_multi_var!(
                self,
                Some(context_id),
                Transaction::default(),
                context_pinned_snapshot
            )?;
            trigger_pin_unpin_snapshot_state(&self.metrics, &guard.pinned_snapshots);
        }
        Ok(HummockSnapshot::clone(&snapshot))
    }

    pub fn get_last_epoch(&self) -> Result<HummockSnapshot> {
        let snapshot = self.latest_snapshot.load();
        Ok(HummockSnapshot::clone(&snapshot))
    }

    #[named]
    pub async fn unpin_snapshot(&self, context_id: HummockContextId) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let mut pinned_snapshots = BTreeMapTransaction::new(&mut versioning_guard.pinned_snapshots);
        let release_snapshot = pinned_snapshots.remove(context_id);
        if release_snapshot.is_some() {
            commit_multi_var!(
                self,
                Some(context_id),
                Transaction::default(),
                pinned_snapshots
            )?;
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
            commit_multi_var!(
                self,
                Some(context_id),
                Transaction::default(),
                context_pinned_snapshot
            )?;
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
        selector: &mut Box<dyn LevelSelector>,
    ) -> Result<Option<CompactTask>> {
        // TODO: `get_all_table_options` will hold catalog_manager async lock, to avoid holding the
        // lock in compaction_guard, take out all table_options in advance there may be a
        // waste of resources here, need to add a more efficient filter in catalog_manager
        let all_table_id_to_option = self.catalog_manager.get_all_table_options().await;

        let mut compaction_guard = write_lock!(self, compaction).await;
        let compaction = compaction_guard.deref_mut();
        let compaction_statuses = &mut compaction.compaction_statuses;

        let start_time = Instant::now();
        // StoredIdGenerator already implements ids pre-allocation by ID_PREALLOCATE_INTERVAL.
        let task_id = self
            .env
            .id_gen_manager()
            .generate::<{ IdCategory::HummockCompactionTask }>()
            .await?;

        let group_config = self
            .compaction_group_manager
            .read()
            .await
            .get_compaction_group_config(compaction_group_id);
        self.precheck_compaction_group(
            compaction_group_id,
            compaction_statuses,
            &group_config.compaction_config,
        )
        .await?;

        let mut compact_status = match compaction.compaction_statuses.get_mut(&compaction_group_id)
        {
            Some(c) => VarTransaction::new(c),
            None => {
                return Ok(None);
            }
        };
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
        if current_version.levels.get(&compaction_group_id).is_none() {
            // compaction group has been deleted.
            return Ok(None);
        }

        let can_trivial_move = matches!(selector.task_type(), compact_task::TaskType::Dynamic);

        let mut stats = LocalSelectorStatistic::default();
        let member_table_ids = &current_version
            .get_compaction_group_levels(compaction_group_id)
            .member_table_ids;
        let table_id_to_option: HashMap<u32, _> = all_table_id_to_option
            .into_iter()
            .filter(|(table_id, _)| member_table_ids.contains(table_id))
            .collect();

        let compact_task = compact_status.get_compact_task(
            current_version.get_compaction_group_levels(compaction_group_id),
            task_id as HummockCompactionTaskId,
            &group_config,
            &mut stats,
            selector,
            table_id_to_option.clone(),
        );
        stats.report_to_metrics(compaction_group_id, self.metrics.as_ref());
        let mut compact_task = match compact_task {
            None => {
                return Ok(None);
            }
            Some(task) => task,
        };
        compact_task.watermark = watermark;
        compact_task.existing_table_ids = current_version
            .levels
            .get(&compaction_group_id)
            .unwrap()
            .member_table_ids
            .clone();

        if CompactStatus::is_trivial_move_task(&compact_task) && can_trivial_move {
            compact_task.sorted_output_ssts = compact_task.input_ssts[0].table_infos.clone();
            // this task has been finished and `trivial_move_task` does not need to be schedule.
            compact_task.set_task_status(TaskStatus::Success);
            self.report_compact_task_impl(None, &mut compact_task, Some(compaction_guard), None)
                .await?;
            tracing::debug!(
                "TrivialMove for compaction group {}: pick up {} tables in level {} to compact to target_level {}  cost time: {:?}",
                compaction_group_id,
                compact_task.input_ssts[0].table_infos.len(),
                compact_task.input_ssts[0].level_idx,
                compact_task.target_level,
                start_time.elapsed()
            );
        } else {
            compact_task.table_options = table_id_to_option
                .into_iter()
                .filter_map(|(table_id, table_option)| {
                    if compact_task.existing_table_ids.contains(&table_id) {
                        return Some((table_id, TableOption::from(&table_option)));
                    }

                    None
                })
                .collect();
            compact_task.current_epoch_time = Epoch::now().0;
            compact_task.compaction_filter_mask =
                group_config.compaction_config.compaction_filter_mask;
            commit_multi_var!(self, None, Transaction::default(), compact_status)?;

            // this task has been finished.
            compact_task.set_task_status(TaskStatus::Pending);

            trigger_sst_stat(
                &self.metrics,
                compaction.compaction_statuses.get(&compaction_group_id),
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

    pub async fn cancel_compact_task_impl(&self, compact_task: &mut CompactTask) -> Result<bool> {
        assert!(CANCEL_STATUS_SET.contains(&compact_task.task_status()));
        self.report_compact_task_impl(None, compact_task, None, None)
            .await
    }

    // need mutex protect
    async fn precheck_compaction_group(
        &self,
        compaction_group_id: CompactionGroupId,
        compaction_statuses: &mut BTreeMap<CompactionGroupId, CompactStatus>,
        compaction_config: &CompactionConfig,
    ) -> Result<()> {
        if !compaction_statuses.contains_key(&compaction_group_id) {
            let mut compact_statuses = BTreeMapTransaction::new(compaction_statuses);
            let new_compact_status = compact_statuses.new_entry_insert_txn(
                compaction_group_id,
                CompactStatus::new(compaction_group_id, compaction_config.max_level),
            );
            commit_multi_var!(self, None, Transaction::default(), new_compact_status)?;
        }

        Ok(())
    }

    pub async fn get_compact_task(
        &self,
        compaction_group_id: CompactionGroupId,
        selector: &mut Box<dyn LevelSelector>,
    ) -> Result<Option<CompactTask>> {
        fail_point!("fp_get_compact_task", |_| Err(Error::MetaStore(
            anyhow::anyhow!("failpoint metastore error")
        )));

        while let Some(task) = self
            .get_compact_task_impl(compaction_group_id, selector)
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
        let mut selector: Box<dyn LevelSelector> =
            Box::new(ManualCompactionSelector::new(manual_compaction_option));
        self.get_compact_task(compaction_group_id, &mut selector)
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
        commit_multi_var!(
            self,
            Some(assignee_context_id),
            Transaction::default(),
            compact_task_assignment
        )?;
        // Update compaction schedule policy.
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

    fn is_compact_task_expired(
        compact_task: &CompactTask,
        branched_ssts: &BTreeMap<HummockSstableObjectId, BranchedSstInfo>,
    ) -> bool {
        for input_level in compact_task.get_input_ssts() {
            for table_info in input_level.get_table_infos() {
                if let Some(mp) = branched_ssts.get(&table_info.object_id) {
                    if mp
                        .get(&compact_task.compaction_group_id)
                        .map_or(true, |sst_id| *sst_id != table_info.sst_id)
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    pub async fn report_compact_task(
        &self,
        context_id: HummockContextId,
        compact_task: &mut CompactTask,
        table_stats_change: Option<PbTableStatsMap>,
    ) -> Result<bool> {
        let ret = self
            .report_compact_task_impl(Some(context_id), compact_task, None, table_stats_change)
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
        compact_task: &mut CompactTask,
        compaction_guard: Option<RwLockWriteGuard<'_, Compaction>>,
        table_stats_change: Option<PbTableStatsMap>,
    ) -> Result<bool> {
        let mut compaction_guard = match compaction_guard {
            None => write_lock!(self, compaction).await,
            Some(compaction_guard) => compaction_guard,
        };
        let deterministic_mode = self.env.opts.compaction_deterministic_test;
        let compaction = compaction_guard.deref_mut();
        let start_time = Instant::now();
        let original_keys = compaction.compaction_statuses.keys().cloned().collect_vec();
        let mut compact_statuses = BTreeMapTransaction::new(&mut compaction.compaction_statuses);
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

        match compact_statuses.get_mut(compact_task.compaction_group_id) {
            Some(mut compact_status) => {
                compact_status.report_compact_task(compact_task);
            }
            None => {
                compact_task.set_task_status(TaskStatus::InvalidGroupCanceled);
            }
        }

        debug_assert!(
            compact_task.task_status() != TaskStatus::Pending,
            "report pending compaction task"
        );
        {
            // The compaction task is finished.
            let mut versioning_guard = write_lock!(self, versioning).await;
            let versioning = versioning_guard.deref_mut();
            let current_version = &mut versioning.current_version;
            // purge stale compact_status
            for group_id in original_keys {
                if !current_version.levels.contains_key(&group_id) {
                    compact_statuses.remove(group_id);
                }
            }
            let is_success = if let TaskStatus::Success = compact_task.task_status() {
                // if member_table_ids changes, the data of sstable may stale.
                let is_expired =
                    Self::is_compact_task_expired(compact_task, &versioning.branched_ssts);
                if is_expired {
                    compact_task.set_task_status(TaskStatus::InputOutdatedCanceled);
                    false
                } else {
                    assert!(current_version
                        .levels
                        .contains_key(&compact_task.compaction_group_id));
                    true
                }
            } else {
                false
            };
            if is_success {
                let mut hummock_version_deltas =
                    BTreeMapTransaction::new(&mut versioning.hummock_version_deltas);
                let mut branched_ssts = BTreeMapTransaction::new(&mut versioning.branched_ssts);
                let version_delta = gen_version_delta(
                    &mut hummock_version_deltas,
                    &mut branched_ssts,
                    current_version,
                    compact_task,
                    CompactStatus::is_trivial_move_task(compact_task),
                    deterministic_mode,
                );
                let mut version_stats = VarTransaction::new(&mut versioning.version_stats);
                if let Some(table_stats_change) = table_stats_change {
                    add_prost_table_stats_map(&mut version_stats.table_stats, &table_stats_change);
                }

                commit_multi_var!(
                    self,
                    context_id,
                    Transaction::default(),
                    compact_statuses,
                    compact_task_assignment,
                    hummock_version_deltas,
                    version_stats
                )?;
                branched_ssts.commit_memory();
                current_version.apply_version_delta(&version_delta);

                trigger_version_stat(&self.metrics, current_version, &versioning.version_stats);
                trigger_delta_log_stats(&self.metrics, versioning.hummock_version_deltas.len());

                if !deterministic_mode {
                    self.notify_last_version_delta(versioning);
                }
            } else {
                // The compaction task is cancelled or failed.
                commit_multi_var!(
                    self,
                    context_id,
                    Transaction::default(),
                    compact_statuses,
                    compact_task_assignment
                )?;
            }
        }

        let task_status = compact_task.task_status();
        let task_status_label = task_status.as_str_name();
        let task_type_label = compact_task.task_type().as_str_name();
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
                        task_type_label,
                        task_status_label,
                    ])
                    .inc();
            }
        } else {
            // There are two cases where assignee_context_id is not available
            // 1. compactor does not exist
            // 2. trivial_move

            let label = if CompactStatus::is_trivial_move_task(compact_task) {
                // TODO: only support can_trivial_move in DynamicLevelCompcation, will check
                // task_type next PR
                "trivial-move"
            } else {
                "unassigned"
            };

            self.metrics
                .compact_frequency
                .with_label_values(&[
                    label,
                    &compact_task.compaction_group_id.to_string(),
                    task_type_label,
                    task_status_label,
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
            compaction
                .compaction_statuses
                .get(&compact_task.compaction_group_id),
            read_lock!(self, versioning).await.current_version.borrow(),
            compact_task.compaction_group_id,
        );

        if !deterministic_mode
            && matches!(compact_task.task_type(), compact_task::TaskType::Dynamic)
        {
            // only try send Dynamic compaction
            self.try_send_compaction_request(
                compact_task.compaction_group_id,
                compact_task::TaskType::Dynamic,
            );
        }

        if task_status == TaskStatus::Success {
            self.try_update_write_limits(&[compact_task.compaction_group_id])
                .await;
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
        sstables: Vec<impl Into<ExtendedSstableInfo>>,
        sst_to_context: HashMap<HummockSstableObjectId, HummockContextId>,
    ) -> Result<Option<HummockSnapshot>> {
        let mut sstables = sstables.into_iter().map(|s| s.into()).collect_vec();
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        // Prevent commit new epochs if this flag is set
        if versioning_guard.disable_commit_epochs {
            return Ok(None);
        }

        let versioning = versioning_guard.deref_mut();
        self.commit_epoch_sanity_check(
            epoch,
            &sstables,
            &sst_to_context,
            &versioning.current_version,
        )
        .await?;

        // Consume and aggregate table stats.
        let mut table_stats_change = PbTableStatsMap::default();
        for s in &mut sstables {
            add_prost_table_stats_map(&mut table_stats_change, &std::mem::take(&mut s.table_stats));
        }

        let old_version = &versioning.current_version;
        let mut new_version_delta = BTreeMapEntryTransaction::new_insert(
            &mut versioning.hummock_version_deltas,
            old_version.id + 1,
            build_version_delta_after_version(old_version),
        );
        new_version_delta.max_committed_epoch = epoch;
        let mut new_hummock_version = old_version.clone();
        new_hummock_version.id = new_version_delta.id;
        let mut incorrect_ssts = vec![];
        let mut new_sst_id_number = 0;
        for ExtendedSstableInfo {
            compaction_group_id,
            sst_info: sst,
            ..
        } in &mut sstables
        {
            let is_sst_belong_to_group_declared = match old_version.levels.get(compaction_group_id)
            {
                Some(compaction_group) => sst
                    .table_ids
                    .iter()
                    .all(|t| compaction_group.member_table_ids.contains(t)),
                None => false,
            };
            if !is_sst_belong_to_group_declared {
                let mut group_table_ids: BTreeMap<_, Vec<u32>> = BTreeMap::new();
                for table_id in sst.get_table_ids() {
                    match try_get_compaction_group_id_by_table_id(
                        &versioning.current_version,
                        *table_id,
                    ) {
                        Some(compaction_group_id) => {
                            group_table_ids
                                .entry(compaction_group_id)
                                .or_default()
                                .push(*table_id);
                        }
                        None => {
                            tracing::warn!(
                                "table {} in SST {} doesn't belong to any compaction group",
                                table_id,
                                sst.get_object_id(),
                            );
                        }
                    }
                }
                let is_trivial_adjust = group_table_ids.len() == 1
                    && group_table_ids.first_key_value().unwrap().1.len()
                        == sst.get_table_ids().len();
                if is_trivial_adjust {
                    *compaction_group_id = *group_table_ids.first_key_value().unwrap().0;
                    // is_sst_belong_to_group_declared = true;
                } else {
                    new_sst_id_number += group_table_ids.len();
                    incorrect_ssts.push((std::mem::take(sst), group_table_ids));
                    *compaction_group_id =
                        StaticCompactionGroupId::NewCompactionGroup as CompactionGroupId;
                }
            }
        }
        let mut new_sst_id = self
            .env
            .id_gen_manager()
            .generate_interval::<{ IdCategory::HummockSstableId }>(new_sst_id_number as u64)
            .await?;
        let mut branched_ssts = BTreeMapTransaction::new(&mut versioning.branched_ssts);
        let original_sstables = std::mem::take(&mut sstables);
        sstables.reserve_exact(original_sstables.len() - incorrect_ssts.len() + new_sst_id_number);
        let mut incorrect_ssts = incorrect_ssts.into_iter();
        for original_sstable in original_sstables {
            if original_sstable.compaction_group_id
                == StaticCompactionGroupId::NewCompactionGroup as CompactionGroupId
            {
                let (sst, group_table_ids) = incorrect_ssts.next().unwrap();
                let mut branch_groups = HashMap::new();
                for (group_id, _match_ids) in group_table_ids {
                    let mut branch_sst = sst.clone();
                    branch_sst.sst_id = new_sst_id;
                    sstables.push(ExtendedSstableInfo::with_compaction_group(
                        group_id, branch_sst,
                    ));
                    branch_groups.insert(group_id, new_sst_id);
                    new_sst_id += 1;
                }
                if !branch_groups.is_empty() {
                    branched_ssts.insert(sst.get_object_id(), branch_groups);
                }
            } else {
                sstables.push(original_sstable);
            }
        }

        let mut modified_compaction_groups = vec![];
        // Append SSTs to a new version.
        for (compaction_group_id, sstables) in &sstables
            .into_iter()
            // the sort is stable sort, and will not change the order within compaction group.
            // Do a sort so that sst in the same compaction group can be consecutive
            .sorted_by_key(
                |ExtendedSstableInfo {
                     compaction_group_id,
                     ..
                 }| *compaction_group_id,
            )
            .group_by(
                |ExtendedSstableInfo {
                     compaction_group_id,
                     ..
                 }| *compaction_group_id,
            )
        {
            modified_compaction_groups.push(compaction_group_id);
            let group_sstables = sstables
                .into_iter()
                .map(|ExtendedSstableInfo { sst_info, .. }| sst_info)
                .collect_vec();
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(compaction_group_id)
                .or_default()
                .group_deltas;
            let version_l0 = new_hummock_version
                .get_compaction_group_levels_mut(compaction_group_id)
                .l0
                .as_mut()
                .expect("Expect level 0 is not empty");
            let l0_sub_level_id = epoch;
            let group_delta = GroupDelta {
                delta_type: Some(DeltaType::IntraLevel(IntraLevelDelta {
                    level_idx: 0,
                    inserted_table_infos: group_sstables.clone(),
                    l0_sub_level_id,
                    ..Default::default()
                })),
            };
            group_deltas.push(group_delta);

            add_new_sub_level(
                version_l0,
                l0_sub_level_id,
                LevelType::Overlapping,
                group_sstables,
            );
        }

        // Create a new_version, possibly merely to bump up the version id and max_committed_epoch.
        new_hummock_version.max_committed_epoch = epoch;

        // Apply stats changes.
        let mut version_stats = VarTransaction::new(&mut versioning.version_stats);
        add_prost_table_stats_map(&mut version_stats.table_stats, &table_stats_change);
        purge_prost_table_stats(&mut version_stats.table_stats, &new_hummock_version);

        commit_multi_var!(
            self,
            None,
            Transaction::default(),
            new_version_delta,
            version_stats
        )?;
        branched_ssts.commit_memory();
        versioning.current_version = new_hummock_version;

        let snapshot = HummockSnapshot {
            committed_epoch: epoch,
            current_epoch: epoch,
        };
        let prev_snapshot = self.latest_snapshot.swap(snapshot.clone().into());
        assert!(prev_snapshot.committed_epoch < epoch);
        assert!(prev_snapshot.current_epoch < epoch);

        trigger_version_stat(
            &self.metrics,
            &versioning.current_version,
            &versioning.version_stats,
        );
        for compaction_group_id in &modified_compaction_groups {
            trigger_sst_stat(
                &self.metrics,
                None,
                &versioning.current_version,
                *compaction_group_id,
            );
        }

        tracing::trace!("new committed epoch {}", epoch);

        self.notify_last_version_delta(versioning);
        trigger_delta_log_stats(&self.metrics, versioning.hummock_version_deltas.len());

        drop(versioning_guard);
        // Don't trigger compactions if we enable deterministic compaction
        if !self.env.opts.compaction_deterministic_test {
            // commit_epoch may contains SSTs from any compaction group
            for id in &modified_compaction_groups {
                self.try_send_compaction_request(*id, compact_task::TaskType::Dynamic);
            }
        }
        if !modified_compaction_groups.is_empty() {
            self.try_update_write_limits(&modified_compaction_groups)
                .await;
        }
        #[cfg(test)]
        {
            self.check_state_consistency().await;
        }
        Ok(Some(snapshot))
    }

    /// We don't commit an epoch without checkpoint. We will only update the `max_current_epoch`.
    pub fn update_current_epoch(&self, max_current_epoch: HummockEpoch) -> HummockSnapshot {
        // We only update `max_current_epoch`!
        let prev_snapshot = self.latest_snapshot.rcu(|snapshot| HummockSnapshot {
            committed_epoch: snapshot.committed_epoch,
            current_epoch: max_current_epoch,
        });
        assert!(prev_snapshot.current_epoch < max_current_epoch);

        tracing::trace!("new current epoch {}", max_current_epoch);
        HummockSnapshot {
            committed_epoch: prev_snapshot.committed_epoch,
            current_epoch: max_current_epoch,
        }
    }

    pub async fn get_new_sst_ids(&self, number: u32) -> Result<SstObjectIdRange> {
        let start_id = self
            .env
            .id_gen_manager()
            .generate_interval::<{ IdCategory::HummockSstableId }>(number as u64)
            .await?;
        Ok(SstObjectIdRange::new(start_id, start_id + number as u64))
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
        // We don't check `checkpoint` because it's allowed to update its in memory state without
        // persisting to object store.
        let get_state =
            |compaction_guard: &RwLockWriteGuard<'_, Compaction>,
             versioning_guard: &RwLockWriteGuard<'_, Versioning>| {
                let compact_statuses_copy = compaction_guard.compaction_statuses.clone();
                let compact_task_assignment_copy = compaction_guard.compact_task_assignment.clone();
                let pinned_versions_copy = versioning_guard.pinned_versions.clone();
                let pinned_snapshots_copy = versioning_guard.pinned_snapshots.clone();
                let hummock_version_deltas_copy = versioning_guard.hummock_version_deltas.clone();
                let version_stats_copy = versioning_guard.version_stats.clone();
                let branched_ssts = versioning_guard.branched_ssts.clone();
                (
                    (
                        compact_statuses_copy,
                        compact_task_assignment_copy,
                        pinned_versions_copy,
                        pinned_snapshots_copy,
                        hummock_version_deltas_copy,
                        version_stats_copy,
                    ),
                    branched_ssts,
                )
            };
        let (mem_state, branched_ssts) =
            get_state(compaction_guard.borrow(), versioning_guard.borrow());
        self.load_meta_store_state_impl(
            compaction_guard.borrow_mut(),
            versioning_guard.borrow_mut(),
        )
        .await
        .expect("Failed to load state from meta store");
        let (loaded_state, load_branched_ssts) =
            get_state(compaction_guard.borrow(), versioning_guard.borrow());
        assert_eq!(branched_ssts, load_branched_ssts);
        assert_eq!(
            mem_state, loaded_state,
            "hummock in-mem state is inconsistent with meta store state",
        );
    }

    /// Gets current version without pinning it.
    /// Should not be called inside [`HummockManager`], because it requests locks internally.
    #[named]
    pub async fn get_current_version(&self) -> HummockVersion {
        read_lock!(self, versioning).await.current_version.clone()
    }

    /// Gets branched sstable infos
    /// Should not be called inside [`HummockManager`], because it requests locks internally.
    #[named]
    pub async fn get_branched_ssts_info(&self) -> BTreeMap<HummockSstableId, BranchedSstInfo> {
        read_lock!(self, versioning).await.branched_ssts.clone()
    }

    /// Get version deltas from meta store
    #[cfg_attr(coverage, no_coverage)]
    pub async fn list_version_deltas(
        &self,
        start_id: u64,
        num_limit: u32,
        committed_epoch_limit: HummockEpoch,
    ) -> Result<HummockVersionDeltas> {
        let ordered_version_deltas: BTreeMap<_, _> =
            HummockVersionDelta::list(self.env.meta_store())
                .await?
                .into_iter()
                .map(|version_delta| (version_delta.id, version_delta))
                .collect();

        let version_deltas = ordered_version_deltas
            .into_iter()
            .filter(|(id, delta)| {
                *id >= start_id && delta.max_committed_epoch <= committed_epoch_limit
            })
            .map(|(_, v)| v)
            .take(num_limit as _)
            .collect();
        Ok(HummockVersionDeltas { version_deltas })
    }

    pub async fn init_metadata_for_version_replay(
        &self,
        table_catalogs: Vec<Table>,
        compaction_groups: Vec<PbCompactionGroupInfo>,
    ) -> Result<()> {
        for table in &table_catalogs {
            table.insert(self.env.meta_store()).await?;
        }
        for group in &compaction_groups {
            assert!(
                group.id == StaticCompactionGroupId::NewCompactionGroup as u64
                    || (group.id >= StaticCompactionGroupId::StateDefault as u64
                        && group.id <= StaticCompactionGroupId::MaterializedView as u64),
                "compaction group id should be either NewCompactionGroup to create new one, or predefined static ones."
            );
        }

        for group in &compaction_groups {
            let mut pairs = vec![];
            for table_id in group.member_table_ids.clone() {
                pairs.push((table_id as StateTableId, group.id));
            }
            let group_config = group.compaction_config.clone().unwrap();
            self.compaction_group_manager
                .write()
                .await
                .init_compaction_config_for_replay(group.id, group_config, self.env.meta_store())
                .await
                .unwrap();
            self.register_table_ids(&pairs).await?;
            tracing::info!("Registered table ids {:?}", pairs);
        }

        // Notify that tables have created
        for table in table_catalogs {
            self.env
                .notification_manager()
                .notify_hummock_relation_info(Operation::Add, RelationInfo::Table(table.clone()))
                .await;
            self.env
                .notification_manager()
                .notify_compactor_relation_info(Operation::Add, RelationInfo::Table(table))
                .await;
        }

        tracing::info!("Inited compaction groups:");
        for group in compaction_groups {
            tracing::info!("{:?}", group);
        }
        Ok(())
    }

    /// Replay a version delta to current hummock version.
    /// Returns the `version_id`, `max_committed_epoch` of the new version and the modified
    /// compaction groups
    #[named]
    pub async fn replay_version_delta(
        &self,
        mut version_delta: HummockVersionDelta,
    ) -> Result<(HummockVersion, Vec<CompactionGroupId>)> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        // ensure the version id is ascending after replay
        version_delta.id = versioning_guard.current_version.id + 1;
        version_delta.prev_id = version_delta.id - 1;
        versioning_guard
            .current_version
            .apply_version_delta(&version_delta);

        let version_new = versioning_guard.current_version.clone();
        let compaction_group_ids = version_delta.group_deltas.keys().cloned().collect_vec();
        Ok((version_new, compaction_group_ids))
    }

    #[named]
    pub async fn disable_commit_epoch(&self) -> HummockVersion {
        let mut versioning_guard = write_lock!(self, versioning).await;
        versioning_guard.disable_commit_epochs = true;
        versioning_guard.current_version.clone()
    }

    /// Triggers compacitons to specified compaction groups.
    /// Don't wait for compaction finish
    pub async fn trigger_compaction_deterministic(
        &self,
        _base_version_id: HummockVersionId,
        compaction_groups: Vec<CompactionGroupId>,
    ) -> Result<()> {
        let old_version = self.get_current_version().await;
        tracing::info!(
            "Trigger compaction for version {}, epoch {}, groups {:?}",
            old_version.id,
            old_version.max_committed_epoch,
            compaction_groups
        );

        if compaction_groups.is_empty() {
            return Ok(());
        }
        for compaction_group in compaction_groups {
            self.try_send_compaction_request(compaction_group, compact_task::TaskType::Dynamic);
        }
        Ok(())
    }

    pub fn init_compaction_scheduler(
        &self,
        sched_channel: CompactionRequestChannelRef,
        notifier: Option<Arc<Notify>>,
    ) {
        *self.compaction_request_channel.write() = Some(sched_channel);
        *self.compaction_resume_notifier.write() = notifier;
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
            commit_multi_var!(self, None, Transaction::default(), compact_statuses)?;
        }
        #[cfg(test)]
        {
            drop(compaction_guard);
            self.check_state_consistency().await;
        }
        Ok(())
    }

    /// Sends a compaction request to compaction scheduler.
    pub fn try_send_compaction_request(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
    ) -> bool {
        if let Some(sender) = self.compaction_request_channel.read().as_ref() {
            match sender.try_sched_compaction(compaction_group, task_type) {
                Ok(_) => true,
                Err(e) => {
                    tracing::error!(
                        "failed to send compaction request for compaction group {}. {}",
                        compaction_group,
                        e
                    );
                    false
                }
            }
        } else {
            tracing::warn!("compaction_request_channel is not initialized");
            false
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
        let locally_cancel_task = |mut compact_task: CompactTask, task_status: TaskStatus| async move {
            compact_task.set_task_status(task_status);
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
            return locally_cancel_task(compact_task, TaskStatus::AssignFailCanceled).await;
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
            return locally_cancel_task(compact_task, TaskStatus::SendFailCanceled).await;
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

    pub fn cluster_manager(&self) -> &ClusterManagerRef<S> {
        &self.cluster_manager
    }

    pub async fn report_scale_compactor_info(&self) {
        let info = self.get_scale_compactor_info().await;
        let suggest_scale_out_core = info.scale_out_cores();
        self.metrics
            .scale_compactor_core_num
            .set(suggest_scale_out_core as i64);

        tracing::debug!(
            "report_scale_compactor_info {:?} suggest_scale_out_core {:?}",
            info,
            suggest_scale_out_core
        );
    }

    #[named]
    pub async fn get_scale_compactor_info(&self) -> ScaleCompactorInfo {
        let total_cpu_core = self.compactor_manager.total_cpu_core_num();
        let total_running_cpu_core = self.compactor_manager.total_running_cpu_core_num();
        let version = {
            let guard = read_lock!(self, versioning).await;
            guard.current_version.clone()
        };
        let mut global_info = ScaleCompactorInfo {
            total_cores: total_cpu_core as u64,
            running_cores: total_running_cpu_core as u64,
            ..Default::default()
        };

        let compaction = read_lock!(self, compaction).await;
        for (group_id, status) in &compaction.compaction_statuses {
            if let Some(levels) = version.levels.get(group_id) {
                let cg = self
                    .compaction_group_manager
                    .read()
                    .await
                    .get_compaction_group_config(*group_id);
                let info = status.get_compaction_info(levels, cg.compaction_config());
                global_info.add(&info);
                tracing::debug!("cg {} info {:?}", group_id, info);
            }
        }
        global_info
    }

    fn notify_last_version_delta(&self, versioning: &Versioning) {
        self.env
            .notification_manager()
            .notify_hummock_without_version(
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
    }

    #[named]
    pub async fn start_lsm_stat_report(hummock_manager: Arc<Self>) -> (JoinHandle<()>, Sender<()>) {
        use crate::hummock::model::CompactionGroup;
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let mut min_interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_interval.tick() => {
                    },
                    // Shutdown
                    _ = &mut shutdown_rx => {
                        tracing::info!("Lsm stat reporter is stopped");
                        return;
                    }
                }

                {
                    let id_to_config = hummock_manager.get_compaction_group_map().await;
                    let current_version = {
                        let mut versioning_guard =
                            write_lock!(hummock_manager.as_ref(), versioning).await;
                        versioning_guard.deref_mut().current_version.clone()
                    };

                    let compaction_group_ids_from_version =
                        get_compaction_group_ids(&current_version);
                    let default_config = CompactionConfigBuilder::new().build();
                    for compaction_group_id in &compaction_group_ids_from_version {
                        let compaction_group_config = id_to_config
                            .get(compaction_group_id)
                            .cloned()
                            .unwrap_or_else(|| {
                                CompactionGroup::new(*compaction_group_id, default_config.clone())
                            });

                        trigger_lsm_stat(
                            &hummock_manager.metrics,
                            compaction_group_config.compaction_config(),
                            current_version
                                .get_compaction_group_levels(compaction_group_config.group_id()),
                            compaction_group_config.group_id(),
                        )
                    }
                }

                {
                    hummock_manager.report_scale_compactor_info().await;
                }
            }
        });
        (join_handle, shutdown_tx)
    }
}

fn drop_sst(
    branched_ssts: &mut BTreeMapTransaction<'_, HummockSstableObjectId, BranchedSstInfo>,
    group_id: CompactionGroupId,
    object_id: HummockSstableObjectId,
    sst_id: HummockSstableId,
) -> bool {
    match branched_ssts.get_mut(object_id) {
        Some(mut entry) => {
            // if group_id not exist, it would not pass the stale check before.
            let removed_sst_id = entry.get(&group_id).unwrap();
            assert_eq!(*removed_sst_id, sst_id);
            entry.remove(&group_id);
            if entry.is_empty() {
                branched_ssts.remove(object_id);
                true
            } else {
                false
            }
        }
        None => true,
    }
}

fn gen_version_delta<'a>(
    txn: &mut BTreeMapTransaction<'a, HummockVersionId, HummockVersionDelta>,
    branched_ssts: &mut BTreeMapTransaction<'a, HummockSstableObjectId, BranchedSstInfo>,
    old_version: &HummockVersion,
    compact_task: &CompactTask,
    trivial_move: bool,
    deterministic_mode: bool,
) -> HummockVersionDelta {
    let mut version_delta = HummockVersionDelta {
        id: old_version.id + 1,
        prev_id: old_version.id,
        max_committed_epoch: old_version.max_committed_epoch,
        trivial_move,
        ..Default::default()
    };
    let group_deltas = &mut version_delta
        .group_deltas
        .entry(compact_task.compaction_group_id)
        .or_default()
        .group_deltas;
    let mut gc_object_ids = vec![];
    for level in &compact_task.input_ssts {
        let group_delta = GroupDelta {
            delta_type: Some(DeltaType::IntraLevel(IntraLevelDelta {
                level_idx: level.level_idx,
                removed_table_ids: level
                    .table_infos
                    .iter()
                    .map(|sst| {
                        let object_id = sst.get_object_id();
                        let sst_id = sst.get_sst_id();
                        if !trivial_move
                            && drop_sst(
                                branched_ssts,
                                compact_task.compaction_group_id,
                                object_id,
                                sst_id,
                            )
                        {
                            gc_object_ids.push(object_id);
                        }
                        sst_id
                    })
                    .collect_vec(),
                ..Default::default()
            })),
        };
        group_deltas.push(group_delta);
    }
    let group_delta = GroupDelta {
        delta_type: Some(DeltaType::IntraLevel(IntraLevelDelta {
            level_idx: compact_task.target_level,
            inserted_table_infos: compact_task.sorted_output_ssts.clone(),
            l0_sub_level_id: compact_task.target_sub_level_id,
            ..Default::default()
        })),
    };
    group_deltas.push(group_delta);
    version_delta.gc_object_ids.append(&mut gc_object_ids);
    version_delta.safe_epoch = std::cmp::max(old_version.safe_epoch, compact_task.watermark);
    // Don't persist version delta generated by compaction to meta store in deterministic mode.
    // Because it will override existing version delta that has same ID generated in the data
    // ingestion phase.
    if !deterministic_mode {
        txn.insert(version_delta.id, version_delta.clone());
    }

    version_delta
}
