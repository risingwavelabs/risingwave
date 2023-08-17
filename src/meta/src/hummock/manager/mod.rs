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

use std::borrow::BorrowMut;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime};

use arc_swap::ArcSwap;
use bytes::Bytes;
use fail::fail_point;
use function_name::named;
use futures::future::Either;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_common::monitor::rwlock::MonitoredRwLock;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_common::util::{pending_on_none, select_all};
use risingwave_hummock_sdk::compact::{compact_task_to_string, statistics_compact_task};
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    build_version_delta_after_version, get_compaction_group_ids,
    try_get_compaction_group_id_by_table_id, BranchedSstInfo, HummockLevelsExt, HummockVersionExt,
    HummockVersionUpdateExt,
};
use risingwave_hummock_sdk::{
    version_checkpoint_path, CompactionGroupId, ExtendedSstableInfo, HummockCompactionTaskId,
    HummockContextId, HummockEpoch, HummockSstableId, HummockSstableObjectId, HummockVersionId,
    SstObjectIdRange, INVALID_VERSION_ID,
};
use risingwave_pb::hummock::compact_task::{self, TaskStatus, TaskType};
use risingwave_pb::hummock::group_delta::DeltaType;
use risingwave_pb::hummock::subscribe_compaction_event_request::{
    Event as RequestEvent, HeartBeat, PullTask, ReportTask,
};
use risingwave_pb::hummock::subscribe_compaction_event_response::{
    Event as ResponseEvent, PullTaskAck,
};
use risingwave_pb::hummock::{
    version_update_payload, CompactTask, CompactTaskAssignment, CompactionConfig, GroupDelta,
    HummockPinnedSnapshot, HummockPinnedVersion, HummockSnapshot, HummockVersion,
    HummockVersionCheckpoint, HummockVersionDelta, HummockVersionDeltas, HummockVersionStats,
    IntraLevelDelta, SubscribeCompactionEventRequest, TableOption,
};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tokio::sync::RwLockWriteGuard;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::IntervalStream;
use tonic::Streaming;
use tracing::warn;

use crate::hummock::compaction::{CompactStatus, LocalSelectorStatistic, ManualCompactionOption};
use crate::hummock::error::{Error, Result};
use crate::hummock::metrics_utils::{
    trigger_delta_log_stats, trigger_lsm_stat, trigger_mv_stat, trigger_pin_unpin_snapshot_state,
    trigger_pin_unpin_version_state, trigger_split_stat, trigger_sst_stat, trigger_version_stat,
    trigger_write_stop_stats,
};
use crate::hummock::{CompactorManagerRef, TASK_NORMAL};
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, FragmentManagerRef, IdCategory, MetaSrvEnv, META_NODE_ID,
};
use crate::model::{
    BTreeMapEntryTransaction, BTreeMapTransaction, ClusterId, MetadataModel, ValTransaction,
    VarTransaction,
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
const HISTORY_TABLE_INFO_WINDOW_SIZE: usize = 16;

// Update to states are performed as follow:
// - Initialize ValTransaction for the meta state to update
// - Make changes on the ValTransaction.
// - Call `commit_multi_var` to commit the changes via meta store transaction. If transaction
//   succeeds, the in-mem state will be updated by the way.
pub struct HummockManager<S: MetaStore> {
    pub env: MetaSrvEnv<S>,
    pub cluster_manager: ClusterManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,

    fragment_manager: FragmentManagerRef<S>,
    // `CompactionGroupManager` manages `CompactionGroup`'s members.
    // Note that all hummock state store user should register to `CompactionGroupManager`. It
    // includes all state tables of streaming jobs except sink.
    compaction_group_manager: tokio::sync::RwLock<CompactionGroupManager>,
    // When trying to locks compaction and versioning at the same time, compaction lock should
    // be requested before versioning lock.
    compaction: MonitoredRwLock<Compaction>,
    versioning: MonitoredRwLock<Versioning>,
    latest_snapshot: Snapshot,

    pub metrics: Arc<MetaMetrics>,

    pub compactor_manager: CompactorManagerRef,
    event_sender: HummockManagerEventSender,

    object_store: ObjectStoreRef,
    version_checkpoint_path: String,
    pause_version_checkpoint: AtomicBool,
    history_table_throughput: parking_lot::RwLock<HashMap<u32, VecDeque<u64>>>,

    // for compactor
    // `compactor_streams_change_tx` is used to pass the mapping from `context_id` to event_stream
    // and is maintained in memory. All event_streams are consumed through a separate event loop
    compactor_streams_change_tx: UnboundedSender<(u32, Streaming<SubscribeCompactionEventRequest>)>,

    // `compaction_state` will record the types of compact tasks that can be triggered in `hummock`
    // and suggest types with a certain priority.
    pub compaction_state: CompactionState,
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
use risingwave_object_store::object::{parse_remote_object_store, ObjectError, ObjectStoreRef};
use risingwave_pb::catalog::Table;
use risingwave_pb::hummock::level_handler::RunningCompactTask;
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

use super::compaction::{
    DynamicLevelSelector, LevelSelector, ManualCompactionSelector, SpaceReclaimCompactionSelector,
    TtlCompactionSelector,
};
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
        fragment_manager: FragmentManagerRef<S>,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        catalog_manager: CatalogManagerRef<S>,
        compactor_streams_change_tx: UnboundedSender<(
            u32,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> Result<HummockManagerRef<S>> {
        let compaction_group_manager = Self::build_compaction_group_manager(&env).await?;
        Self::new_impl(
            env,
            cluster_manager,
            fragment_manager,
            metrics,
            compactor_manager,
            compaction_group_manager,
            catalog_manager,
            compactor_streams_change_tx,
        )
        .await
    }

    #[cfg(any(test, feature = "test"))]
    pub(super) async fn with_config(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        config: CompactionConfig,
        compactor_streams_change_tx: UnboundedSender<(
            u32,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
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
            fragment_manager,
            metrics,
            compactor_manager,
            compaction_group_manager,
            catalog_manager,
            compactor_streams_change_tx,
        )
        .await
        .unwrap()
    }

    async fn new_impl(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        compaction_group_manager: tokio::sync::RwLock<CompactionGroupManager>,
        catalog_manager: CatalogManagerRef<S>,
        compactor_streams_change_tx: UnboundedSender<(
            u32,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> Result<HummockManagerRef<S>> {
        let sys_params_manager = env.system_params_manager();
        let sys_params = sys_params_manager.get_params().await;
        let state_store_url = sys_params.state_store();
        let state_store_dir: &str = sys_params.data_directory();
        let deterministic_mode = env.opts.compaction_deterministic_test;
        let object_store = Arc::new(
            parse_remote_object_store(
                state_store_url.strip_prefix("hummock+").unwrap_or("memory"),
                metrics.object_store_metric.clone(),
                "Version Checkpoint",
            )
            .await,
        );
        // Make sure data dir is not used by another cluster.
        // Skip this check in e2e compaction test, which needs to start a secondary cluster with
        // same bucket
        if env.cluster_first_launch() && !deterministic_mode {
            write_exclusive_cluster_id(
                state_store_dir,
                env.cluster_id().clone(),
                object_store.clone(),
            )
            .await?;

            // config bucket lifecycle for new cluster.
            if let risingwave_object_store::object::ObjectStoreImpl::S3(s3) = object_store.as_ref()
                && !env.opts.do_not_config_object_storage_lifecycle
            {
                s3.inner().configure_bucket_lifecycle().await;
            }
        }
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
            fragment_manager,
            compaction_group_manager,
            // compaction_request_channel: parking_lot::RwLock::new(None),
            compactor_manager,
            latest_snapshot: ArcSwap::from_pointee(HummockSnapshot {
                committed_epoch: INVALID_EPOCH,
                current_epoch: INVALID_EPOCH,
            }),
            event_sender: tx,
            object_store,
            version_checkpoint_path: checkpoint_path,
            pause_version_checkpoint: AtomicBool::new(false),
            history_table_throughput: parking_lot::RwLock::new(HashMap::default()),
            compactor_streams_change_tx,
            compaction_state: CompactionState::new(),
        };
        let instance = Arc::new(instance);
        instance.start_worker(rx).await;
        instance.load_meta_store_state().await?;
        instance.release_invalid_contexts().await?;
        // Release snapshots pinned by meta on restarting.
        instance.release_meta_context().await?;
        Ok(instance)
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
                let default_compaction_config = self
                    .compaction_group_manager
                    .read()
                    .await
                    .default_compaction_config();
                let checkpoint = create_init_version(default_compaction_config);
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
            versioning_guard.checkpoint = self.read_checkpoint().await?;
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
            .write()
            .await
            .get_or_insert_compaction_group_configs(
                &all_group_ids.collect_vec(),
                self.env.meta_store(),
            )
            .await?;
        versioning_guard.write_limit =
            calc_new_write_limits(configs, HashMap::new(), &versioning_guard.current_version);
        trigger_write_stop_stats(&self.metrics, &versioning_guard.write_limit);
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

    pub fn latest_snapshot(&self) -> HummockSnapshot {
        let snapshot = self.latest_snapshot.load();
        HummockSnapshot::clone(&snapshot)
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

        // When the last table of a compaction group is deleted, the compaction group (and its
        // config) is destroyed as well. Then a compaction task for this group may come later and
        // cannot find its config.
        let group_config = match self
            .compaction_group_manager
            .read()
            .await
            .try_get_compaction_group_config(compaction_group_id)
        {
            Some(config) => config,
            None => return Ok(None),
        };
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
        let is_trivial_reclaim = CompactStatus::is_trivial_reclaim(&compact_task);
        let is_trivial_move = CompactStatus::is_trivial_move_task(&compact_task);

        if is_trivial_reclaim {
            compact_task.set_task_status(TaskStatus::Success);
            self.report_compact_task_impl(&mut compact_task, &mut compaction_guard, None)
                .await?;
            tracing::debug!(
                "TrivialReclaim for compaction group {}: remove {} sstables, cost time: {:?}",
                compaction_group_id,
                compact_task
                    .input_ssts
                    .iter()
                    .map(|level| level.table_infos.len())
                    .sum::<usize>(),
                start_time.elapsed()
            );
        } else if is_trivial_move && can_trivial_move {
            compact_task.sorted_output_ssts = compact_task.input_ssts[0].table_infos.clone();
            // this task has been finished and `trivial_move_task` does not need to be schedule.
            compact_task.set_task_status(TaskStatus::Success);
            self.report_compact_task_impl(&mut compact_task, &mut compaction_guard, None)
                .await?;
            tracing::debug!(
                "TrivialMove for compaction group {}: pick up {} sstables in level {} to compact
            to target_level {}  cost time: {:?}",
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

            let mut compact_task_assignment =
                BTreeMapTransaction::new(&mut compaction.compact_task_assignment);
            compact_task_assignment.insert(
                compact_task.task_id,
                CompactTaskAssignment {
                    compact_task: Some(compact_task.clone()),
                    context_id: META_NODE_ID, // deprecated
                },
            );

            // We are using a single transaction to ensure that each task has progress when it is
            // created.
            commit_multi_var!(
                self,
                None,
                Transaction::default(),
                compact_status,
                compact_task_assignment
            )?;

            // Initiate heartbeat for the task to track its progress.
            self.compactor_manager
                .initiate_task_heartbeat(compact_task.clone());

            // this task has been finished.
            compact_task.set_task_status(TaskStatus::Pending);

            trigger_sst_stat(
                &self.metrics,
                compaction.compaction_statuses.get(&compaction_group_id),
                &current_version,
                compaction_group_id,
            );

            let compact_task_statistics = statistics_compact_task(&compact_task);

            let level_type_label = format!(
                "L{}->L{}",
                compact_task.input_ssts[0].level_idx,
                compact_task.input_ssts.last().unwrap().level_idx,
            );

            let level_count = compact_task.input_ssts.len();
            if compact_task.input_ssts[0].level_idx == 0 {
                self.metrics
                    .l0_compact_level_count
                    .with_label_values(&[&compaction_group_id.to_string(), &level_type_label])
                    .observe(level_count as _);
            }

            self.metrics
                .compact_task_size
                .with_label_values(&[&compaction_group_id.to_string(), &level_type_label])
                .observe(compact_task_statistics.total_file_size as _);

            self.metrics
                .compact_task_size
                .with_label_values(&[
                    &compaction_group_id.to_string(),
                    &format!("{} uncompressed", level_type_label),
                ])
                .observe(compact_task_statistics.total_uncompressed_file_size as _);

            self.metrics
                .compact_task_file_count
                .with_label_values(&[&compaction_group_id.to_string(), &level_type_label])
                .observe(compact_task_statistics.total_file_count as _);

            tracing::trace!(
                    "For compaction group {}: pick up {} {} sub_level in level {} to compact to target {}. cost time: {:?} compact_task_statistics {:?}",
                    compaction_group_id,
                    level_count,
                    compact_task.input_ssts[0].level_type().as_str_name(),
                    compact_task.input_ssts[0].level_idx,
                    compact_task.target_level,
                    start_time.elapsed(),
                    compact_task_statistics
                );
        }

        #[cfg(test)]
        {
            drop(compaction_guard);
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

    #[named]
    pub async fn cancel_compact_task_impl(&self, compact_task: &mut CompactTask) -> Result<bool> {
        assert!(CANCEL_STATUS_SET.contains(&compact_task.task_status()));
        let mut compaction_guard = write_lock!(self, compaction).await;
        let ret = self
            .report_compact_task_impl(compact_task, &mut compaction_guard, None)
            .await?;
        #[cfg(test)]
        {
            drop(compaction_guard);
            self.check_state_consistency().await;
        }
        Ok(ret)
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
            assert!(
                CompactStatus::is_trivial_move_task(&task)
                    || CompactStatus::is_trivial_reclaim(&task)
            );
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

    #[named]
    pub async fn report_compact_task(
        &self,
        compact_task: &mut CompactTask,
        table_stats_change: Option<PbTableStatsMap>,
    ) -> Result<bool> {
        let mut guard = write_lock!(self, compaction).await;
        let ret = self
            .report_compact_task_impl(compact_task, &mut guard, table_stats_change)
            .await?;
        #[cfg(test)]
        {
            drop(guard);
            self.check_state_consistency().await;
        }
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
        compact_task: &mut CompactTask,
        compaction_guard: &mut RwLockWriteGuard<'_, Compaction>,
        table_stats_change: Option<PbTableStatsMap>,
    ) -> Result<bool> {
        let deterministic_mode = self.env.opts.compaction_deterministic_test;
        let compaction = compaction_guard.deref_mut();
        let start_time = Instant::now();
        let original_keys = compaction.compaction_statuses.keys().cloned().collect_vec();
        let mut compact_statuses = BTreeMapTransaction::new(&mut compaction.compaction_statuses);
        let mut compact_task_assignment =
            BTreeMapTransaction::new(&mut compaction.compact_task_assignment);

        let is_trivial_reclaim = CompactStatus::is_trivial_reclaim(compact_task);
        let is_trivial_move = CompactStatus::is_trivial_move_task(compact_task);

        // remove task_assignment
        if compact_task_assignment
            .remove(compact_task.task_id)
            .is_none()
            && !(is_trivial_reclaim || is_trivial_move)
        {
            return Ok(false);
        }

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
            let input_sst_ids: HashSet<u64> = compact_task
                .input_ssts
                .iter()
                .flat_map(|level| level.table_infos.iter().map(|sst| sst.sst_id))
                .collect();
            let input_level_ids: Vec<u32> = compact_task
                .input_ssts
                .iter()
                .map(|level| level.level_idx)
                .collect();
            let is_success = if let TaskStatus::Success = compact_task.task_status() {
                // if member_table_ids changes, the data of sstable may stale.
                let is_expired =
                    Self::is_compact_task_expired(compact_task, &versioning.branched_ssts);
                if is_expired {
                    compact_task.set_task_status(TaskStatus::InputOutdatedCanceled);
                    false
                } else {
                    let group = current_version
                        .levels
                        .get(&compact_task.compaction_group_id)
                        .unwrap();
                    let input_exist =
                        group.check_deleted_sst_exist(&input_level_ids, input_sst_ids);
                    if !input_exist {
                        compact_task.set_task_status(TaskStatus::InvalidGroupCanceled);
                        warn!(
                            "The task may be expired because of group split, task:\n {:?}",
                            compact_task_to_string(compact_task)
                        );
                    }
                    input_exist
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
                    deterministic_mode,
                );
                let mut version_stats = VarTransaction::new(&mut versioning.version_stats);
                if let Some(table_stats_change) = &table_stats_change {
                    add_prost_table_stats_map(&mut version_stats.table_stats, table_stats_change);
                }

                // apply version delta before we persist this change. If it causes panic we can
                // recover to a correct state after restarting meta-node.
                current_version.apply_version_delta(&version_delta);
                commit_multi_var!(
                    self,
                    None,
                    Transaction::default(),
                    compact_statuses,
                    compact_task_assignment,
                    hummock_version_deltas,
                    version_stats
                )?;
                branched_ssts.commit_memory();

                trigger_version_stat(&self.metrics, current_version, &versioning.version_stats);
                trigger_delta_log_stats(&self.metrics, versioning.hummock_version_deltas.len());
                self.notify_stats(&versioning.version_stats);

                if !deterministic_mode {
                    self.notify_last_version_delta(versioning);
                }
            } else {
                // The compaction task is cancelled or failed.
                commit_multi_var!(
                    self,
                    None,
                    Transaction::default(),
                    compact_statuses,
                    compact_task_assignment
                )?;
            }
        }

        let task_status = compact_task.task_status();
        let task_status_label = task_status.as_str_name();
        let task_type_label = compact_task.task_type().as_str_name();

        let label = if is_trivial_reclaim {
            "trivial-space-reclaim"
        } else if is_trivial_move {
            // TODO: only support can_trivial_move in DynamicLevelCompcation, will check
            // task_type next PR
            "trivial-move"
        } else {
            self.compactor_manager
                .remove_task_heartbeat(compact_task.task_id);
            "normal"
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
            &read_lock!(self, versioning).await.current_version,
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
        }

        // Create a new_version, possibly merely to bump up the version id and max_committed_epoch.
        new_hummock_version.apply_version_delta(new_version_delta.deref());

        // Apply stats changes.
        let mut version_stats = VarTransaction::new(&mut versioning.version_stats);
        add_prost_table_stats_map(&mut version_stats.table_stats, &table_stats_change);
        purge_prost_table_stats(&mut version_stats.table_stats, &new_hummock_version);
        for (table_id, stats) in &table_stats_change {
            let table_id_str = table_id.to_string();
            let stats_value =
                std::cmp::max(0, stats.total_key_size + stats.total_value_size) / 1024 / 1024;
            self.metrics
                .table_write_throughput
                .with_label_values(&[table_id_str.as_str()])
                .inc_by(stats_value as u64);
        }

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
        self.notify_stats(&versioning.version_stats);
        let mut table_groups = HashMap::<u32, usize>::default();
        for group in versioning.current_version.levels.values() {
            for table_id in &group.member_table_ids {
                table_groups.insert(*table_id, group.member_table_ids.len());
            }
        }
        drop(versioning_guard);
        // Don't trigger compactions if we enable deterministic compaction
        if !self.env.opts.compaction_deterministic_test {
            // commit_epoch may contains SSTs from any compaction group
            for id in &modified_compaction_groups {
                self.try_send_compaction_request(*id, compact_task::TaskType::Dynamic);
            }
            if !table_stats_change.is_empty() {
                table_stats_change.retain(|table_id, _| {
                    table_groups
                        .get(table_id)
                        .map(|table_count| *table_count > 1)
                        .unwrap_or(false)
                });
            }
            if !table_stats_change.is_empty() {
                self.collect_table_write_throughput(table_stats_change);
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
        let (mem_state, branched_ssts) = get_state(&compaction_guard, &versioning_guard);
        self.load_meta_store_state_impl(
            compaction_guard.borrow_mut(),
            versioning_guard.borrow_mut(),
        )
        .await
        .expect("Failed to load state from meta store");
        let (loaded_state, load_branched_ssts) = get_state(&compaction_guard, &versioning_guard);
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

    /// Sends a compaction request.
    pub fn try_send_compaction_request(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
    ) -> bool {
        match self
            .compaction_state
            .try_sched_compaction(compaction_group, task_type)
        {
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
    }

    pub async fn trigger_manual_compaction(
        &self,
        compaction_group: CompactionGroupId,
        manual_compaction_option: ManualCompactionOption,
    ) -> Result<()> {
        let start_time = Instant::now();

        // 1. Get idle compactor.
        let compactor = match self.compactor_manager.next_compactor() {
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
                )
                .into());
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

        // 3. send task to compactor
        let compact_task_string = compact_task_to_string(&compact_task);
        if let Err(e) = compactor.send_event(ResponseEvent::CompactTask(compact_task)) {
            // TODO: shall we need to cancel on meta ?
            return Err(anyhow::anyhow!(
                "Failed to trigger compaction task: {:#?} compaction_group {}",
                e,
                compaction_group
            )
            .into());
        }

        tracing::info!(
            "Trigger manual compaction task. {}. cost time: {:?}",
            &compact_task_string,
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

    fn notify_stats(&self, stats: &HummockVersionStats) {
        self.env
            .notification_manager()
            .notify_frontend_without_version(Operation::Update, Info::HummockStats(stats.clone()));
    }

    #[named]
    pub async fn hummock_timer_task(hummock_manager: Arc<Self>) -> (JoinHandle<()>, Sender<()>) {
        use futures::{FutureExt, StreamExt};

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            const CHECK_PENDING_TASK_PERIOD_SEC: u64 = 300;
            const STAT_REPORT_PERIOD_SEC: u64 = 20;
            const COMPACTION_HEARTBEAT_PERIOD_SEC: u64 = 1;

            pub enum HummockTimerEvent {
                GroupSplit,
                CheckDeadTask,
                Report,
                CompactionHeartBeat,

                DynamicCompactionTrigger,
                SpaceReclaimCompactionTrigger,
                TtlCompactionTrigger,

                FullGc,
            }
            let mut check_compact_trigger_interval =
                tokio::time::interval(Duration::from_secs(CHECK_PENDING_TASK_PERIOD_SEC));
            check_compact_trigger_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            check_compact_trigger_interval.reset();

            let check_compact_trigger = IntervalStream::new(check_compact_trigger_interval)
                .map(|_| HummockTimerEvent::CheckDeadTask);

            let mut stat_report_interval =
                tokio::time::interval(std::time::Duration::from_secs(STAT_REPORT_PERIOD_SEC));
            stat_report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            stat_report_interval.reset();
            let stat_report_trigger =
                IntervalStream::new(stat_report_interval).map(|_| HummockTimerEvent::Report);

            let mut compaction_heartbeat_interval = tokio::time::interval(
                std::time::Duration::from_secs(COMPACTION_HEARTBEAT_PERIOD_SEC),
            );
            compaction_heartbeat_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            compaction_heartbeat_interval.reset();
            let compaction_heartbeat_trigger = IntervalStream::new(compaction_heartbeat_interval)
                .map(|_| HummockTimerEvent::CompactionHeartBeat);

            let mut min_trigger_interval = tokio::time::interval(Duration::from_secs(
                hummock_manager.env.opts.periodic_compaction_interval_sec,
            ));
            min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            min_trigger_interval.reset();
            let dynamic_tick_trigger = IntervalStream::new(min_trigger_interval)
                .map(|_| HummockTimerEvent::DynamicCompactionTrigger);

            let mut min_space_reclaim_trigger_interval =
                tokio::time::interval(Duration::from_secs(
                    hummock_manager
                        .env
                        .opts
                        .periodic_space_reclaim_compaction_interval_sec,
                ));
            min_space_reclaim_trigger_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            min_space_reclaim_trigger_interval.reset();
            let space_reclaim_trigger = IntervalStream::new(min_space_reclaim_trigger_interval)
                .map(|_| HummockTimerEvent::SpaceReclaimCompactionTrigger);

            let mut min_ttl_reclaim_trigger_interval = tokio::time::interval(Duration::from_secs(
                hummock_manager
                    .env
                    .opts
                    .periodic_ttl_reclaim_compaction_interval_sec,
            ));
            min_ttl_reclaim_trigger_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            min_ttl_reclaim_trigger_interval.reset();
            let ttl_reclaim_trigger = IntervalStream::new(min_ttl_reclaim_trigger_interval)
                .map(|_| HummockTimerEvent::TtlCompactionTrigger);

            let mut full_gc_interval = tokio::time::interval(Duration::from_secs(
                hummock_manager.env.opts.full_gc_interval_sec,
            ));
            full_gc_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            full_gc_interval.reset();
            let full_gc_trigger =
                IntervalStream::new(full_gc_interval).map(|_| HummockTimerEvent::FullGc);

            let mut triggers: Vec<BoxStream<'static, HummockTimerEvent>> = vec![
                Box::pin(check_compact_trigger),
                Box::pin(stat_report_trigger),
                Box::pin(compaction_heartbeat_trigger),
                Box::pin(dynamic_tick_trigger),
                Box::pin(space_reclaim_trigger),
                Box::pin(ttl_reclaim_trigger),
                Box::pin(full_gc_trigger),
            ];

            let periodic_check_split_group_interval_sec = hummock_manager
                .env
                .opts
                .periodic_split_compact_group_interval_sec;

            if periodic_check_split_group_interval_sec > 0 {
                let mut split_group_trigger_interval = tokio::time::interval(Duration::from_secs(
                    periodic_check_split_group_interval_sec,
                ));
                split_group_trigger_interval
                    .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                split_group_trigger_interval.reset();

                let split_group_trigger = IntervalStream::new(split_group_trigger_interval)
                    .map(|_| HummockTimerEvent::GroupSplit);
                triggers.push(Box::pin(split_group_trigger));
            }

            let event_stream = select_all(triggers);
            use futures::pin_mut;
            pin_mut!(event_stream);

            let shutdown_rx_shared = shutdown_rx.shared();

            tracing::info!(
                "Hummock timer task tracing [GroupSplit interval {} sec] [CheckDeadTask interval {} sec] [Report interval {} sec] [CompactionHeartBeat interval {} sec]",
                    periodic_check_split_group_interval_sec, CHECK_PENDING_TASK_PERIOD_SEC, STAT_REPORT_PERIOD_SEC, COMPACTION_HEARTBEAT_PERIOD_SEC
            );

            loop {
                let item =
                    futures::future::select(event_stream.next(), shutdown_rx_shared.clone()).await;

                match item {
                    Either::Left((event, _)) => {
                        if let Some(event) = event {
                            match event {
                                HummockTimerEvent::CheckDeadTask => {
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager.check_dead_task().await;
                                }

                                HummockTimerEvent::GroupSplit => {
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager.on_handle_check_split_multi_group().await;
                                }

                                HummockTimerEvent::Report => {
                                    let (
                                        current_version,
                                        id_to_config,
                                        branched_sst,
                                        version_stats,
                                    ) = {
                                        let versioning_guard =
                                            read_lock!(hummock_manager.as_ref(), versioning).await;

                                        let configs =
                                            hummock_manager.get_compaction_group_map().await;
                                        let versioning_deref = versioning_guard;
                                        (
                                            versioning_deref.current_version.clone(),
                                            configs,
                                            versioning_deref.branched_ssts.clone(),
                                            versioning_deref.version_stats.clone(),
                                        )
                                    };

                                    if let Some(mv_id_to_all_table_ids) = hummock_manager
                                        .fragment_manager
                                        .get_mv_id_to_internal_table_ids_mapping()
                                    {
                                        trigger_mv_stat(
                                            &hummock_manager.metrics,
                                            &version_stats,
                                            mv_id_to_all_table_ids,
                                        );
                                    }

                                    for compaction_group_id in
                                        get_compaction_group_ids(&current_version)
                                    {
                                        let compaction_group_config =
                                            &id_to_config[&compaction_group_id];

                                        let group_levels = current_version
                                            .get_compaction_group_levels(
                                                compaction_group_config.group_id(),
                                            );

                                        trigger_split_stat(
                                            &hummock_manager.metrics,
                                            compaction_group_config.group_id(),
                                            group_levels.member_table_ids.len(),
                                            &branched_sst,
                                        );

                                        trigger_lsm_stat(
                                            &hummock_manager.metrics,
                                            compaction_group_config.compaction_config(),
                                            group_levels,
                                            compaction_group_config.group_id(),
                                        )
                                    }
                                }

                                HummockTimerEvent::CompactionHeartBeat => {
                                    let compactor_manager =
                                        hummock_manager.compactor_manager.clone();

                                    // TODO: add metrics to track expired tasks
                                    const INTERVAL_SEC: u64 = 30;
                                    // The cancel task has two paths
                                    // 1. compactor heartbeat cancels the expired task based on task
                                    // progress (meta + compactor)
                                    // 2. meta periodically scans the task and performs a cancel on
                                    // the meta side for tasks that are not updated by heartbeat

                                    // So the reason for setting Interval is to let compactor be
                                    // responsible for canceling the corresponding task as much as
                                    // possible by relaxing the conditions for detection on the meta
                                    // side, and meta is just used as a last resort to clean up the
                                    // tasks that compactor has expired.

                                    //
                                    for mut task in
                                        compactor_manager.get_expired_tasks(Some(INTERVAL_SEC))
                                    {
                                        if let Err(e) = hummock_manager
                                            .cancel_compact_task(
                                                &mut task,
                                                TaskStatus::HeartbeatCanceled,
                                            )
                                            .await
                                        {
                                            tracing::error!("Attempt to remove compaction task due to elapsed heartbeat failed. We will continue to track its heartbeat
                                                until we can successfully report its status. task_id: {}, ERR: {e:?}", task.task_id);
                                        }
                                    }
                                }

                                HummockTimerEvent::DynamicCompactionTrigger => {
                                    // Disable periodic trigger for compaction_deterministic_test.
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager
                                        .on_handle_trigger_multi_group(
                                            compact_task::TaskType::Dynamic,
                                        )
                                        .await;
                                }

                                HummockTimerEvent::SpaceReclaimCompactionTrigger => {
                                    // Disable periodic trigger for compaction_deterministic_test.
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager
                                        .on_handle_trigger_multi_group(
                                            compact_task::TaskType::SpaceReclaim,
                                        )
                                        .await;
                                }

                                HummockTimerEvent::TtlCompactionTrigger => {
                                    // Disable periodic trigger for compaction_deterministic_test.
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager
                                        .on_handle_trigger_multi_group(compact_task::TaskType::Ttl)
                                        .await;
                                }

                                HummockTimerEvent::FullGc => {
                                    if hummock_manager
                                        .start_full_gc(Duration::from_secs(3600))
                                        .is_ok()
                                    {
                                        tracing::info!("Start full GC from meta node.");
                                    }
                                }
                            }
                        }
                    }

                    Either::Right((_, _shutdown)) => {
                        tracing::info!("Hummock timer loop is stopped");
                        break;
                    }
                }
            }
        });
        (join_handle, shutdown_tx)
    }

    #[named]
    pub async fn check_dead_task(&self) {
        const MAX_COMPACTION_L0_MULTIPLIER: u64 = 32;
        const MAX_COMPACTION_DURATION_SEC: u64 = 20 * 60;
        let (groups, configs) = {
            let versioning_guard = read_lock!(self, versioning).await;
            let g = versioning_guard
                .current_version
                .levels
                .iter()
                .map(|(id, group)| {
                    (
                        *id,
                        group
                            .l0
                            .as_ref()
                            .unwrap()
                            .sub_levels
                            .iter()
                            .map(|level| level.total_file_size)
                            .sum::<u64>(),
                    )
                })
                .collect_vec();
            let c = self.get_compaction_group_map().await;
            (g, c)
        };
        let mut slowdown_groups: HashMap<u64, u64> = HashMap::default();
        {
            for (group_id, l0_file_size) in groups {
                let group = &configs[&group_id];
                if l0_file_size
                    > MAX_COMPACTION_L0_MULTIPLIER
                        * group.compaction_config.max_bytes_for_level_base
                {
                    slowdown_groups.insert(group_id, l0_file_size);
                }
            }
        }
        if slowdown_groups.is_empty() {
            return;
        }
        let mut pending_tasks: HashMap<u64, (u64, usize, RunningCompactTask)> = HashMap::default();
        {
            let compaction_guard = read_lock!(self, compaction).await;
            for group_id in slowdown_groups.keys() {
                if let Some(status) = compaction_guard.compaction_statuses.get(group_id) {
                    for (idx, level_handler) in status.level_handlers.iter().enumerate() {
                        let tasks = level_handler.get_pending_tasks().to_vec();
                        if tasks.is_empty() {
                            continue;
                        }
                        for task in tasks {
                            pending_tasks.insert(task.task_id, (*group_id, idx, task));
                        }
                    }
                }
            }
        }
        let task_ids = pending_tasks.keys().cloned().collect_vec();
        let task_infos = self
            .compactor_manager
            .check_tasks_status(&task_ids, Duration::from_secs(MAX_COMPACTION_DURATION_SEC));
        for (task_id, (compact_time, status)) in task_infos {
            if status == TASK_NORMAL {
                continue;
            }
            if let Some((group_id, level_id, task)) = pending_tasks.get(&task_id) {
                let group_size = *slowdown_groups.get(group_id).unwrap();
                warn!("COMPACTION SLOW: the task-{} of group-{}(size: {}MB) level-{} has not finished after {:?}, {}, it may cause pending sstable files({:?}) blocking other task.",
                    task_id, *group_id,group_size / 1024 / 1024,*level_id, compact_time, status, task.ssts);
            }
        }
    }

    fn collect_table_write_throughput(&self, table_stats: PbTableStatsMap) {
        let mut table_infos = self.history_table_throughput.write();
        for (table_id, stat) in table_stats {
            let throughput = (stat.total_value_size + stat.total_key_size) as u64;
            let entry = table_infos.entry(table_id).or_default();
            entry.push_back(throughput);
            if entry.len() > HISTORY_TABLE_INFO_WINDOW_SIZE {
                entry.pop_front();
            }
        }
    }

    async fn on_handle_check_split_multi_group(&self) {
        let table_write_throughput = self.history_table_throughput.read().clone();
        let mut group_infos = self.calculate_compaction_group_statistic().await;
        group_infos.sort_by_key(|group| group.group_size);
        group_infos.reverse();
        let group_size_limit = self.env.opts.split_group_size_limit;
        let default_group_id: CompactionGroupId = StaticCompactionGroupId::StateDefault.into();
        let mv_group_id: CompactionGroupId = StaticCompactionGroupId::MaterializedView.into();
        let mut partition_vnode_count = self.env.opts.partition_vnode_count;
        for group in &group_infos {
            if group.table_statistic.len() == 1 {
                continue;
            }

            for (table_id, table_size) in &group.table_statistic {
                let mut is_high_write_throughput = false;
                let mut is_low_write_throughput = true;
                if let Some(history) = table_write_throughput.get(table_id) {
                    if history.len() >= HISTORY_TABLE_INFO_WINDOW_SIZE {
                        let window_total_size = history.iter().sum::<u64>();
                        is_high_write_throughput = history.iter().all(|throughput| {
                            *throughput > self.env.opts.table_write_throughput_threshold
                        });
                        is_low_write_throughput = window_total_size
                            < (HISTORY_TABLE_INFO_WINDOW_SIZE as u64)
                                * self.env.opts.min_table_split_write_throughput;
                    }
                }
                let state_table_size = *table_size;

                if state_table_size < self.env.opts.min_table_split_size
                    && !is_high_write_throughput
                {
                    continue;
                }

                let parent_group_id = group.group_id;
                let mut target_compact_group_id = None;
                let mut allow_split_by_table = false;
                if state_table_size < self.env.opts.split_group_size_limit
                    && is_low_write_throughput
                {
                    // do not split a large table and a small table because it would increase IOPS
                    // of small table.
                    if parent_group_id != default_group_id && parent_group_id != mv_group_id {
                        let rest_group_size = group.group_size - state_table_size;
                        if rest_group_size < state_table_size
                            && rest_group_size < self.env.opts.min_table_split_size
                        {
                            continue;
                        }
                    } else {
                        for group in &group_infos {
                            // do not move to mv group or state group
                            if !group.split_by_table || group.group_id == mv_group_id
                                || group.group_id == default_group_id
                                || group.group_id == parent_group_id
                                // do not move state-table to a large group.
                                || group.group_size + state_table_size > group_size_limit
                                // do not move state-table from group A to group B if this operation would make group B becomes larger than A.
                                || group.group_size + state_table_size > group.group_size - state_table_size
                            {
                                continue;
                            }
                            target_compact_group_id = Some(group.group_id);
                        }
                        allow_split_by_table = true;
                        partition_vnode_count = 1;
                    }
                }

                let ret = self
                    .move_state_table_to_compaction_group(
                        parent_group_id,
                        &[*table_id],
                        target_compact_group_id,
                        allow_split_by_table,
                        partition_vnode_count,
                    )
                    .await;
                match ret {
                    Ok(_) => {
                        tracing::info!(
                        "move state table [{}] from group-{} to group-{:?} success, Allow split by table: {}",
                        table_id, parent_group_id, target_compact_group_id, allow_split_by_table
                    );
                        return;
                    }
                    Err(e) => {
                        tracing::info!(
                        "failed to move state table [{}] from group-{} to group-{:?} because {:?}",
                        table_id, parent_group_id, target_compact_group_id, e
                    )
                    }
                }
            }
        }
    }

    pub async fn compaction_event_loop(
        hummock_manager: Arc<Self>,
        mut compactor_streams_change_rx: UnboundedReceiver<(
            u32,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> (JoinHandle<()>, Sender<()>) {
        let mut compactor_request_streams = FuturesUnordered::new();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let shutdown_rx_shared = shutdown_rx.shared();
        let mut compaction_selectors = init_selectors();

        let join_handle = tokio::spawn(async move {
            let push_stream =
                |context_id: u32,
                 stream: Streaming<SubscribeCompactionEventRequest>,
                 compactor_request_streams: &mut FuturesUnordered<_>| {
                    let future = stream
                        .into_future()
                        .map(move |stream_future| (context_id, stream_future));

                    compactor_request_streams.push(future);
                };

            let mut event_loop_iteration_now = Instant::now();
            loop {
                let shutdown_rx_shared = shutdown_rx_shared.clone();

                // report
                hummock_manager
                    .metrics
                    .compaction_event_loop_iteration_latency
                    .observe(event_loop_iteration_now.elapsed().as_millis() as _);
                event_loop_iteration_now = Instant::now();

                tokio::select! {
                    _ = shutdown_rx_shared => {
                        return;
                    },

                    compactor_stream = compactor_streams_change_rx.recv() => {
                        if let Some((context_id, stream)) = compactor_stream {
                            tracing::info!("compactor {} enters the cluster", context_id);
                            push_stream(context_id, stream, &mut compactor_request_streams);
                        }
                    },

                    result = pending_on_none(compactor_request_streams.next()) => {
                        let mut compactor_alive = true;
                        let (context_id, compactor_stream_req) = result;
                        let (event, create_at, stream) = match compactor_stream_req {
                            (Some(Ok(req)), stream) => {
                                (req.event.unwrap(), req.create_at, stream)
                            }

                            (Some(Err(err)), _stream) => {
                                tracing::warn!("compactor {} leaving the cluster with err {:?}", context_id, err);
                                hummock_manager.compactor_manager
                                    .remove_compactor(context_id);
                                continue
                            }

                            _ => {
                                tracing::warn!("compactor {} leaving the cluster", context_id);
                                hummock_manager.compactor_manager
                                    .remove_compactor(context_id);
                                continue
                            },
                        };

                        {
                            const MAX_CONSUMED_LATENCY_MS: u64 = 500;
                            let consumed_latency_ms = SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("Clock may have gone backwards")
                                .as_millis()
                                as u64
                            - create_at;
                            hummock_manager.metrics
                                .compaction_event_consumed_latency
                                .observe(consumed_latency_ms as _);
                            if consumed_latency_ms > MAX_CONSUMED_LATENCY_MS {
                                tracing::warn!(
                                    "Compaction event {:?} takes too long create_at {} consumed_latency_ms {}",
                                    event,
                                    create_at,
                                    consumed_latency_ms
                                );
                            }
                        }

                        match event {
                            RequestEvent::PullTask(PullTask {
                                pull_task_count,
                            }) => {
                                assert_ne!(0, pull_task_count);
                                if let Some(compactor) = hummock_manager.compactor_manager.get_compactor(context_id) {
                                    if let Some((group, task_type)) = hummock_manager.auto_pick_compaction_group_and_type().await {
                                        let selector: &mut Box<dyn LevelSelector> = compaction_selectors.get_mut(&task_type).unwrap();
                                        for _ in 0..pull_task_count {
                                            let compact_task =
                                                hummock_manager
                                                .get_compact_task(group, selector)
                                                .await;

                                            match compact_task {
                                                Ok(Some(compact_task)) => {
                                                    let task_id = compact_task.task_id;
                                                    if let Err(e) = compactor.send_event(
                                                        ResponseEvent::CompactTask(compact_task)
                                                    ) {
                                                        tracing::warn!(
                                                            "Failed to send task {} to {}. {:#?}",
                                                            task_id,
                                                            compactor.context_id(),
                                                            e
                                                        );

                                                        compactor_alive = false;
                                                        break;
                                                    }
                                                },
                                                Ok(None) => {
                                                    // no compact_task to be picked
                                                    hummock_manager.compaction_state.unschedule(group, task_type);
                                                    break;
                                                }
                                                Err(err) => {
                                                    tracing::warn!("Failed to get compaction task: {:#?}.", err);
                                                    break;
                                                }
                                            };
                                        }
                                    }

                                    // ack to compactor
                                    if compactor_alive {
                                        if let Err(e) = compactor.send_event(ResponseEvent::PullTaskAck(PullTaskAck {})){
                                            tracing::warn!(
                                                "Failed to send ask to {}. {:#?}",
                                                context_id,
                                                e
                                            );

                                            compactor_alive = false;
                                        }
                                    }
                                } else {
                                    compactor_alive = false;

                                }
                            },

                            RequestEvent::ReportTask(ReportTask {
                                compact_task,
                                table_stats_change
                            }) => {
                                if let Some(mut compact_task) = compact_task {
                                    if let Err(e) =  hummock_manager
                                        .report_compact_task(&mut compact_task, Some(table_stats_change))
                                       .await {
                                        tracing::error!("report compact_tack fail {e:?}");
                                    }
                                }
                            },

                            RequestEvent::HeartBeat(HeartBeat {
                                progress,
                            }) => {
                                let compactor_manager = hummock_manager.compactor_manager.clone();
                                let cancel_tasks = compactor_manager.update_task_heartbeats(&progress);

                                // TODO: task cancellation can be batched
                                for mut task in cancel_tasks {
                                    tracing::info!(
                                        "Task with task_id {} with context_id {} has expired due to lack of visible progress",
                                        context_id,
                                        task.task_id
                                    );

                                    if let Err(e) =
                                        hummock_manager
                                        .cancel_compact_task(&mut task, TaskStatus::HeartbeatCanceled)
                                        .await
                                    {
                                        tracing::error!("Attempt to remove compaction task due to elapsed heartbeat failed. We will continue to track its heartbeat
                                                        until we can successfully report its status. task_id: {}, ERR: {e:?}", task.task_id);
                                    }

                                    if let Some(compactor) = compactor_manager.get_compactor(context_id) {
                                        // Forcefully cancel the task so that it terminates
                                        // early on the compactor
                                        // node.
                                        let _ = compactor.cancel_task(task.task_id);
                                        tracing::info!(
                                            "CancelTask operation for task_id {} has been sent to node with context_id {}",
                                            context_id,
                                            task.task_id
                                        );
                                    } else {
                                        compactor_alive = false;
                                    }
                                }
                            },

                            RequestEvent::Register(_) => {
                                unreachable!()
                            }
                        }


                        if compactor_alive {
                            push_stream(context_id, stream, &mut compactor_request_streams);
                        } else {
                            tracing::warn!("compactor {} leaving the cluster since it's not alive", context_id);
                            hummock_manager.compactor_manager
                                .remove_compactor(context_id);
                        }
                    }
                }
            }
        });

        (join_handle, shutdown_tx)
    }

    pub fn add_compactor_stream(
        &self,
        context_id: u32,
        req_stream: Streaming<SubscribeCompactionEventRequest>,
    ) {
        self.compactor_streams_change_tx
            .send((context_id, req_stream))
            .unwrap();
    }

    async fn on_handle_trigger_multi_group(&self, task_type: compact_task::TaskType) {
        for cg_id in self.compaction_group_ids().await {
            if let Err(e) = self.compaction_state.try_sched_compaction(cg_id, task_type) {
                tracing::warn!(
                    "Failed to schedule {:?} compaction for compaction group {}. {}",
                    task_type,
                    cg_id,
                    e
                );
            }
        }
    }

    pub async fn auto_pick_compaction_group_and_type(
        &self,
    ) -> Option<(CompactionGroupId, compact_task::TaskType)> {
        use rand::prelude::SliceRandom;
        use rand::thread_rng;
        let mut compaction_group_ids = self.compaction_group_ids().await;
        compaction_group_ids.shuffle(&mut thread_rng());

        for cg_id in compaction_group_ids {
            if let Some(pick_type) = self.compaction_state.auto_pick_type(cg_id) {
                return Some((cg_id, pick_type));
            }
        }

        None
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
    deterministic_mode: bool,
) -> HummockVersionDelta {
    let trivial_move = CompactStatus::is_trivial_move_task(compact_task);

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

async fn write_exclusive_cluster_id(
    state_store_dir: &str,
    cluster_id: ClusterId,
    object_store: ObjectStoreRef,
) -> Result<()> {
    const CLUSTER_ID_DIR: &str = "cluster_id";
    const CLUSTER_ID_NAME: &str = "0";
    let cluster_id_dir = format!("{}/{}/", state_store_dir, CLUSTER_ID_DIR);
    let cluster_id_full_path = format!("{}{}", cluster_id_dir, CLUSTER_ID_NAME);
    match object_store.read(&cluster_id_full_path, None).await {
        Ok(cluster_id) => Err(ObjectError::internal(format!(
            "Data directory is already used by another cluster with id {:?}, path {}.",
            String::from_utf8(cluster_id.to_vec()).unwrap(),
            cluster_id_full_path,
        ))
        .into()),
        Err(e) => {
            if e.is_object_not_found_error() {
                object_store
                    .upload(&cluster_id_full_path, Bytes::from(String::from(cluster_id)))
                    .await?;
                return Ok(());
            }
            Err(e.into())
        }
    }
}

fn init_selectors() -> HashMap<compact_task::TaskType, Box<dyn LevelSelector>> {
    let mut compaction_selectors: HashMap<compact_task::TaskType, Box<dyn LevelSelector>> =
        HashMap::default();
    compaction_selectors.insert(
        compact_task::TaskType::Dynamic,
        Box::<DynamicLevelSelector>::default(),
    );
    compaction_selectors.insert(
        compact_task::TaskType::SpaceReclaim,
        Box::<SpaceReclaimCompactionSelector>::default(),
    );
    compaction_selectors.insert(
        compact_task::TaskType::Ttl,
        Box::<TtlCompactionSelector>::default(),
    );
    compaction_selectors
}

type CompactionRequestChannelItem = (CompactionGroupId, compact_task::TaskType);
use tokio::sync::mpsc::error::SendError;

#[derive(Debug, Default)]
pub struct CompactionState {
    scheduled: Mutex<HashSet<(CompactionGroupId, compact_task::TaskType)>>,
}

impl CompactionState {
    pub fn new() -> Self {
        Self {
            scheduled: Default::default(),
        }
    }

    /// Enqueues only if the target is not yet in queue.
    pub fn try_sched_compaction(
        &self,
        compaction_group: CompactionGroupId,
        task_type: TaskType,
    ) -> std::result::Result<bool, SendError<CompactionRequestChannelItem>> {
        let mut guard = self.scheduled.lock();
        let key = (compaction_group, task_type);
        if guard.contains(&key) {
            return Ok(false);
        }
        guard.insert(key);
        Ok(true)
    }

    pub fn unschedule(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
    ) {
        self.scheduled.lock().remove(&(compaction_group, task_type));
    }

    pub fn auto_pick_type(&self, group: CompactionGroupId) -> Option<TaskType> {
        let guard = self.scheduled.lock();
        if guard.contains(&(group, compact_task::TaskType::SpaceReclaim)) {
            Some(compact_task::TaskType::SpaceReclaim)
        } else if guard.contains(&(group, compact_task::TaskType::Ttl)) {
            Some(compact_task::TaskType::Ttl)
        } else if guard.contains(&(group, compact_task::TaskType::Dynamic)) {
            Some(compact_task::TaskType::Dynamic)
        } else {
            None
        }
    }
}
