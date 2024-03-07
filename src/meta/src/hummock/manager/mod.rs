// Copyright 2024 RisingWave Labs
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

use anyhow::Context;
use arc_swap::ArcSwap;
use bytes::Bytes;
use fail::fail_point;
use function_name::named;
use futures::future::Either;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_common::config::default::compaction_config;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::monitor::rwlock::MonitoredRwLock;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_hummock_sdk::compact::{compact_task_to_string, statistics_compact_task};
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    build_version_delta_after_version, get_compaction_group_ids,
    get_table_compaction_group_id_mapping, try_get_compaction_group_id_by_table_id,
    BranchedSstInfo, HummockLevelsExt,
};
use risingwave_hummock_sdk::version::HummockVersionDelta;
use risingwave_hummock_sdk::{
    version_archive_dir, version_checkpoint_path, CompactionGroupId, ExtendedSstableInfo,
    HummockCompactionTaskId, HummockContextId, HummockEpoch, HummockSstableId,
    HummockSstableObjectId, HummockVersionId, SstObjectIdRange, INVALID_VERSION_ID,
};
use risingwave_meta_model_v2::{
    compaction_status, compaction_task, hummock_pinned_snapshot, hummock_pinned_version,
    hummock_version_delta, hummock_version_stats,
};
use risingwave_pb::hummock::compact_task::{self, TaskStatus, TaskType};
use risingwave_pb::hummock::group_delta::DeltaType;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config;
use risingwave_pb::hummock::subscribe_compaction_event_request::{
    Event as RequestEvent, HeartBeat, ReportTask,
};
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::{
    CompactTask, CompactTaskAssignment, CompactionConfig, GroupDelta, HummockPinnedSnapshot,
    HummockPinnedVersion, HummockSnapshot, HummockVersionStats, IntraLevelDelta,
    PbCompactionGroupInfo, SstableInfo, SubscribeCompactionEventRequest, TableOption,
};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use rw_futures_util::{pending_on_none, select_all};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tokio::sync::RwLockWriteGuard;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::IntervalStream;
use tonic::Streaming;
use tracing::warn;

use crate::hummock::compaction::selector::{
    DynamicLevelSelector, LocalSelectorStatistic, ManualCompactionOption, ManualCompactionSelector,
    SpaceReclaimCompactionSelector, TombstoneCompactionSelector, TtlCompactionSelector,
};
use crate::hummock::compaction::{CompactStatus, CompactionDeveloperConfig};
use crate::hummock::error::{Error, Result};
use crate::hummock::metrics_utils::{
    build_compact_task_level_type_metrics_label, trigger_delta_log_stats, trigger_local_table_stat,
    trigger_lsm_stat, trigger_mv_stat, trigger_pin_unpin_snapshot_state,
    trigger_pin_unpin_version_state, trigger_split_stat, trigger_sst_stat, trigger_version_stat,
    trigger_write_stop_stats,
};
use crate::hummock::sequence::next_compaction_task_id;
use crate::hummock::{CompactorManagerRef, TASK_NORMAL};
#[cfg(any(test, feature = "test"))]
use crate::manager::{ClusterManagerRef, FragmentManagerRef};
use crate::manager::{MetaSrvEnv, MetadataManager, TableId, META_NODE_ID};
use crate::model::{
    BTreeMapEntryTransaction, BTreeMapEntryTransactionWrapper, BTreeMapTransaction,
    BTreeMapTransactionWrapper, ClusterId, MetadataModel, MetadataModelError, ValTransaction,
    VarTransaction, VarTransactionWrapper,
};
use crate::rpc::metrics::MetaMetrics;
use crate::storage::MetaStore;

mod compaction_group_manager;
mod context;
mod gc;
#[cfg(test)]
mod tests;
mod versioning;
pub use versioning::HummockVersionSafePoint;
use versioning::*;
pub(crate) mod checkpoint;
mod compaction;
pub mod sequence;
mod utils;
mod worker;

use compaction::*;
pub(crate) use utils::*;

type Snapshot = ArcSwap<HummockSnapshot>;
const HISTORY_TABLE_INFO_STATISTIC_TIME: usize = 240;

// Update to states are performed as follow:
// - Initialize ValTransaction for the meta state to update
// - Make changes on the ValTransaction.
// - Call `commit_multi_var` to commit the changes via meta store transaction. If transaction
//   succeeds, the in-mem state will be updated by the way.
pub struct HummockManager {
    pub env: MetaSrvEnv,

    metadata_manager: MetadataManager,
    /// Lock order: compaction, versioning, compaction_group_manager.
    /// - Lock compaction first, then versioning, and finally compaction_group_manager.
    /// - This order should be strictly followed to prevent deadlock.
    compaction: MonitoredRwLock<Compaction>,
    versioning: MonitoredRwLock<Versioning>,
    /// `CompactionGroupManager` manages compaction configs for compaction groups.
    compaction_group_manager: tokio::sync::RwLock<CompactionGroupManager>,
    latest_snapshot: Snapshot,

    pub metrics: Arc<MetaMetrics>,

    pub compactor_manager: CompactorManagerRef,
    event_sender: HummockManagerEventSender,

    object_store: ObjectStoreRef,
    version_checkpoint_path: String,
    version_archive_dir: String,
    pause_version_checkpoint: AtomicBool,
    history_table_throughput: parking_lot::RwLock<HashMap<u32, VecDeque<u64>>>,

    // for compactor
    // `compactor_streams_change_tx` is used to pass the mapping from `context_id` to event_stream
    // and is maintained in memory. All event_streams are consumed through a separate event loop
    compactor_streams_change_tx: UnboundedSender<(u32, Streaming<SubscribeCompactionEventRequest>)>,

    // `compaction_state` will record the types of compact tasks that can be triggered in `hummock`
    // and suggest types with a certain priority.
    pub compaction_state: CompactionState,

    // Record the partition corresponding to the table in each group (accepting delays)
    // The compactor will refer to this structure to determine how to cut the boundaries of sst.
    // Currently, we update it in a couple of scenarios
    // 1. throughput and size are checked periodically and calculated according to the rules
    // 2. A new group is created (split)
    // 3. split_weight_by_vnode is modified for an existing group. (not supported yet)
    // Tips:
    // 1. When table_id does not exist in the current structure, compactor will not cut the boundary
    // 2. When partition count <=1, compactor will still use table_id as the cutting boundary of sst
    // 3. Modify the special configuration item hybrid_vnode_count = 0 to remove the table_id in hybrid cg and no longer perform alignment cutting.
    group_to_table_vnode_partition:
        parking_lot::RwLock<HashMap<CompactionGroupId, BTreeMap<TableId, u32>>>,
}

pub type HummockManagerRef = Arc<HummockManager>;

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
use risingwave_object_store::object::{build_remote_object_store, ObjectError, ObjectStoreRef};
use risingwave_pb::catalog::Table;
use risingwave_pb::hummock::level_handler::RunningCompactTask;
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

use crate::hummock::manager::compaction_group_manager::CompactionGroupManager;
use crate::hummock::manager::worker::HummockManagerEventSender;

pub static CANCEL_STATUS_SET: LazyLock<HashSet<TaskStatus>> = LazyLock::new(|| {
    [
        TaskStatus::ManualCanceled,
        TaskStatus::SendFailCanceled,
        TaskStatus::AssignFailCanceled,
        TaskStatus::HeartbeatCanceled,
        TaskStatus::InvalidGroupCanceled,
        TaskStatus::NoAvailMemoryResourceCanceled,
        TaskStatus::NoAvailCpuResourceCanceled,
    ]
    .into_iter()
    .collect()
});

pub struct CommitEpochInfo {
    pub sstables: Vec<ExtendedSstableInfo>,
    pub new_table_watermarks: HashMap<risingwave_common::catalog::TableId, TableWatermarks>,
    pub sst_to_context: HashMap<HummockSstableObjectId, HummockContextId>,
}

impl CommitEpochInfo {
    pub fn new(
        sstables: Vec<ExtendedSstableInfo>,
        new_table_watermarks: HashMap<risingwave_common::catalog::TableId, TableWatermarks>,
        sst_to_context: HashMap<HummockSstableObjectId, HummockContextId>,
    ) -> Self {
        Self {
            sstables,
            new_table_watermarks,
            sst_to_context,
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub(crate) fn for_test(
        sstables: Vec<impl Into<ExtendedSstableInfo>>,
        sst_to_context: HashMap<HummockSstableObjectId, HummockContextId>,
    ) -> Self {
        Self::new(
            sstables.into_iter().map(Into::into).collect(),
            HashMap::new(),
            sst_to_context,
        )
    }
}

impl HummockManager {
    pub async fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        compactor_streams_change_tx: UnboundedSender<(
            u32,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> Result<HummockManagerRef> {
        let compaction_group_manager = Self::build_compaction_group_manager(&env).await?;
        Self::new_impl(
            env,
            metadata_manager,
            metrics,
            compactor_manager,
            compaction_group_manager,
            compactor_streams_change_tx,
        )
        .await
    }

    #[cfg(any(test, feature = "test"))]
    pub(super) async fn with_config(
        env: MetaSrvEnv,
        cluster_manager: ClusterManagerRef,
        fragment_manager: FragmentManagerRef,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        config: CompactionConfig,
        compactor_streams_change_tx: UnboundedSender<(
            u32,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> HummockManagerRef {
        use crate::manager::CatalogManager;
        let compaction_group_manager =
            Self::build_compaction_group_manager_with_config(&env, config)
                .await
                .unwrap();
        let catalog_manager = Arc::new(CatalogManager::new(env.clone()).await.unwrap());
        let metadata_manager =
            MetadataManager::new_v1(cluster_manager, catalog_manager, fragment_manager);
        Self::new_impl(
            env,
            metadata_manager,
            metrics,
            compactor_manager,
            compaction_group_manager,
            compactor_streams_change_tx,
        )
        .await
        .unwrap()
    }

    async fn new_impl(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        compaction_group_manager: tokio::sync::RwLock<CompactionGroupManager>,
        compactor_streams_change_tx: UnboundedSender<(
            u32,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> Result<HummockManagerRef> {
        let sys_params = env.system_params_reader().await;
        let state_store_url = sys_params.state_store();
        let state_store_dir: &str = sys_params.data_directory();
        let deterministic_mode = env.opts.compaction_deterministic_test;
        let object_store = Arc::new(
            build_remote_object_store(
                state_store_url.strip_prefix("hummock+").unwrap_or("memory"),
                metrics.object_store_metric.clone(),
                "Version Checkpoint",
                ObjectStoreConfig::default(),
            )
            .await,
        );
        // Make sure data dir is not used by another cluster.
        // Skip this check in e2e compaction test, which needs to start a secondary cluster with
        // same bucket
        if !deterministic_mode {
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
                let is_bucket_expiration_configured =
                    s3.inner().configure_bucket_lifecycle(state_store_dir).await;
                if is_bucket_expiration_configured {
                    return Err(ObjectError::internal("Cluster cannot start with object expiration configured for bucket because RisingWave data will be lost when object expiration kicks in.
                    Please disable object expiration and restart the cluster.")
                    .into());
                }
            }
        }
        let version_checkpoint_path = version_checkpoint_path(state_store_dir);
        let version_archive_dir = version_archive_dir(state_store_dir);
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
            metadata_manager,
            compaction_group_manager,
            // compaction_request_channel: parking_lot::RwLock::new(None),
            compactor_manager,
            latest_snapshot: ArcSwap::from_pointee(HummockSnapshot {
                committed_epoch: INVALID_EPOCH,
                current_epoch: INVALID_EPOCH,
            }),
            event_sender: tx,
            object_store,
            version_checkpoint_path,
            version_archive_dir,
            pause_version_checkpoint: AtomicBool::new(false),
            history_table_throughput: parking_lot::RwLock::new(HashMap::default()),
            compactor_streams_change_tx,
            compaction_state: CompactionState::new(),
            group_to_table_vnode_partition: parking_lot::RwLock::new(HashMap::default()),
        };
        let instance = Arc::new(instance);
        instance.start_worker(rx).await;
        instance.load_meta_store_state().await?;
        instance.release_invalid_contexts().await?;
        // Release snapshots pinned by meta on restarting.
        instance.release_meta_context().await?;
        Ok(instance)
    }

    fn sql_meta_store(&self) -> Option<SqlMetaStore> {
        self.env.sql_meta_store()
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
        use sea_orm::EntityTrait;
        let sql_meta_store = self.sql_meta_store();
        let compaction_statuses: BTreeMap<CompactionGroupId, CompactStatus> = match &sql_meta_store
        {
            None => CompactStatus::list(self.env.meta_store_checked())
                .await?
                .into_iter()
                .map(|cg| (cg.compaction_group_id(), cg))
                .collect(),
            Some(sql_meta_store) => compaction_status::Entity::find()
                .all(&sql_meta_store.conn)
                .await
                .map_err(MetadataModelError::from)?
                .into_iter()
                .map(|m| (m.compaction_group_id as CompactionGroupId, m.into()))
                .collect(),
        };
        if !compaction_statuses.is_empty() {
            compaction_guard.compaction_statuses = compaction_statuses;
        }

        compaction_guard.compact_task_assignment = match &sql_meta_store {
            None => CompactTaskAssignment::list(self.env.meta_store_checked())
                .await?
                .into_iter()
                .map(|assigned| (assigned.key().unwrap(), assigned))
                .collect(),
            Some(sql_meta_store) => compaction_task::Entity::find()
                .all(&sql_meta_store.conn)
                .await
                .map_err(MetadataModelError::from)?
                .into_iter()
                .map(|m| (m.id as HummockCompactionTaskId, m.into()))
                .collect(),
        };

        let hummock_version_deltas: BTreeMap<HummockVersionId, HummockVersionDelta> =
            match &sql_meta_store {
                None => HummockVersionDelta::list(self.env.meta_store_checked())
                    .await?
                    .into_iter()
                    .map(|version_delta| (version_delta.id, version_delta))
                    .collect(),
                Some(sql_meta_store) => {
                    use risingwave_pb::hummock::PbHummockVersionDelta;
                    hummock_version_delta::Entity::find()
                        .all(&sql_meta_store.conn)
                        .await
                        .map_err(MetadataModelError::from)?
                        .into_iter()
                        .map(|m| {
                            (
                                m.id as HummockVersionId,
                                HummockVersionDelta::from_persisted_protobuf(
                                    &PbHummockVersionDelta::from(m),
                                ),
                            )
                        })
                        .collect()
                }
            };

        let checkpoint = self.try_read_checkpoint().await?;
        let mut redo_state = if let Some(c) = checkpoint {
            versioning_guard.checkpoint = c;
            versioning_guard.checkpoint.version.clone()
        } else {
            let default_compaction_config = self
                .compaction_group_manager
                .read()
                .await
                .default_compaction_config();
            let checkpoint_version = create_init_version(default_compaction_config);
            tracing::info!("init hummock version checkpoint");
            versioning_guard.checkpoint = HummockVersionCheckpoint {
                version: checkpoint_version.clone(),
                stale_objects: Default::default(),
            };
            self.write_checkpoint(&versioning_guard.checkpoint).await?;
            checkpoint_version
        };
        for version_delta in hummock_version_deltas.values() {
            if version_delta.prev_id == redo_state.id {
                redo_state.apply_version_delta(version_delta);
            }
        }
        versioning_guard.version_stats = match &sql_meta_store {
            None => HummockVersionStats::list(self.env.meta_store_checked())
                .await?
                .into_iter()
                .next(),
            Some(sql_meta_store) => hummock_version_stats::Entity::find()
                .one(&sql_meta_store.conn)
                .await
                .map_err(MetadataModelError::from)?
                .map(HummockVersionStats::from),
        }
        .unwrap_or_else(|| HummockVersionStats {
            // version_stats.hummock_version_id is always 0 in meta store.
            hummock_version_id: 0,
            ..Default::default()
        });

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

        versioning_guard.pinned_versions = match &sql_meta_store {
            None => HummockPinnedVersion::list(self.env.meta_store_checked())
                .await?
                .into_iter()
                .map(|p| (p.context_id, p))
                .collect(),
            Some(sql_meta_store) => hummock_pinned_version::Entity::find()
                .all(&sql_meta_store.conn)
                .await
                .map_err(MetadataModelError::from)?
                .into_iter()
                .map(|m| (m.context_id as HummockContextId, m.into()))
                .collect(),
        };

        versioning_guard.pinned_snapshots = match &sql_meta_store {
            None => HummockPinnedSnapshot::list(self.env.meta_store_checked())
                .await?
                .into_iter()
                .map(|p| (p.context_id, p))
                .collect(),
            Some(sql_meta_store) => hummock_pinned_snapshot::Entity::find()
                .all(&sql_meta_store.conn)
                .await
                .map_err(MetadataModelError::from)?
                .into_iter()
                .map(|m| (m.context_id as HummockContextId, m.into()))
                .collect(),
        };

        versioning_guard.objects_to_delete.clear();
        versioning_guard.mark_objects_for_deletion();

        self.initial_compaction_group_config_after_load(versioning_guard)
            .await?;

        Ok(())
    }

    /// Caller should hold `versioning` lock, to sync with `HummockManager::release_contexts`.
    async fn check_context_with_meta_node(&self, context_id: HummockContextId) -> Result<()> {
        if context_id == META_NODE_ID {
            // Using the preserved meta id is allowed.
        } else if !self.check_context(context_id).await? {
            // The worker is not found in cluster.
            return Err(Error::InvalidContext(context_id));
        }
        Ok(())
    }

    /// Pin the current greatest hummock version. The pin belongs to `context_id`
    /// and will be unpinned when `context_id` is invalidated.
    #[named]
    pub async fn pin_version(&self, context_id: HummockContextId) -> Result<HummockVersion> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        self.check_context_with_meta_node(context_id).await?;
        let _timer = start_measure_real_process_timer!(self);
        let versioning = versioning_guard.deref_mut();
        let mut pinned_versions = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut versioning.pinned_versions,)
        );
        let mut context_pinned_version = pinned_versions.new_entry_txn_or_default(
            context_id,
            HummockPinnedVersion {
                context_id,
                min_pinned_id: INVALID_VERSION_ID,
            },
        );
        let version_id = versioning.current_version.id;
        let ret = versioning.current_version.clone();
        if context_pinned_version.min_pinned_id == INVALID_VERSION_ID
            || context_pinned_version.min_pinned_id > version_id
        {
            context_pinned_version.min_pinned_id = version_id;
            commit_multi_var!(
                self.env.meta_store(),
                self.sql_meta_store(),
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
    /// this `context_id` so they will not be vacuumed.
    #[named]
    pub async fn unpin_version_before(
        &self,
        context_id: HummockContextId,
        unpin_before: HummockVersionId,
    ) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        self.check_context_with_meta_node(context_id).await?;
        let _timer = start_measure_real_process_timer!(self);
        let versioning = versioning_guard.deref_mut();
        let mut pinned_versions = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut versioning.pinned_versions,)
        );
        let mut context_pinned_version = pinned_versions.new_entry_txn_or_default(
            context_id,
            HummockPinnedVersion {
                context_id,
                min_pinned_id: 0,
            },
        );
        assert!(
            context_pinned_version.min_pinned_id <= unpin_before,
            "val must be monotonically non-decreasing. old = {}, new = {}.",
            context_pinned_version.min_pinned_id,
            unpin_before
        );
        context_pinned_version.min_pinned_id = unpin_before;
        commit_multi_var!(
            self.env.meta_store(),
            self.sql_meta_store(),
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
        self.check_context_with_meta_node(context_id).await?;
        let mut pinned_snapshots = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut guard.pinned_snapshots,)
        );
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
                self.env.meta_store(),
                self.sql_meta_store(),
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
        self.check_context_with_meta_node(context_id).await?;
        let _timer = start_measure_real_process_timer!(self);
        let mut pinned_snapshots = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut guard.pinned_snapshots,)
        );
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
                self.env.meta_store(),
                self.sql_meta_store(),
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
        self.check_context_with_meta_node(context_id).await?;
        let _timer = start_measure_real_process_timer!(self);
        let mut pinned_snapshots = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut versioning_guard.pinned_snapshots,)
        );
        let release_snapshot = pinned_snapshots.remove(context_id);
        if release_snapshot.is_some() {
            commit_multi_var!(
                self.env.meta_store(),
                self.sql_meta_store(),
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
        self.check_context_with_meta_node(context_id).await?;
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

        let mut pinned_snapshots = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut versioning_guard.pinned_snapshots,)
        );
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
                self.env.meta_store(),
                self.sql_meta_store(),
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

    /// Cancels a compaction task no matter it's assigned or unassigned.
    pub async fn cancel_compact_task(&self, task_id: u64, task_status: TaskStatus) -> Result<bool> {
        fail_point!("fp_cancel_compact_task", |_| Err(Error::MetaStore(
            anyhow::anyhow!("failpoint metastore err")
        )));
        self.cancel_compact_task_impl(task_id, task_status).await
    }

    pub async fn cancel_compact_task_impl(
        &self,
        task_id: u64,
        task_status: TaskStatus,
    ) -> Result<bool> {
        assert!(CANCEL_STATUS_SET.contains(&task_status));
        let rets = self
            .report_compact_tasks(vec![ReportTask {
                task_id,
                task_status: task_status as i32,
                sorted_output_ssts: vec![],
                table_stats_change: HashMap::default(),
            }])
            .await?;
        #[cfg(test)]
        {
            self.check_state_consistency().await;
        }
        Ok(rets[0])
    }

    pub async fn get_compact_tasks(
        &self,
        mut compaction_groups: Vec<CompactionGroupId>,
        max_task_count: usize,
        selector: &mut Box<dyn CompactionSelector>,
    ) -> Result<(Vec<CompactTask>, Vec<CompactTask>)> {
        fail_point!("fp_get_compact_task", |_| Err(Error::MetaStore(
            anyhow::anyhow!("failpoint metastore error")
        )));
        loop {
            let (normal_tasks, trivial_tasks) = self
                .get_compact_tasks_impl(
                    std::mem::take(&mut compaction_groups),
                    max_task_count,
                    selector,
                )
                .await?;
            if !normal_tasks.is_empty() {
                return Ok((normal_tasks, trivial_tasks));
            } else if trivial_tasks.is_empty() {
                return Ok((vec![], vec![]));
            }
            // only select groups which could generate more tasks.
            for t in &trivial_tasks {
                compaction_groups.push(t.compaction_group_id);
            }
        }
    }

    pub async fn get_compact_task(
        &self,
        compaction_group_id: CompactionGroupId,
        selector: &mut Box<dyn CompactionSelector>,
    ) -> Result<Option<CompactTask>> {
        fail_point!("fp_get_compact_task", |_| Err(Error::MetaStore(
            anyhow::anyhow!("failpoint metastore error")
        )));

        loop {
            let (mut normal_tasks, trivial_tasks) = self
                .get_compact_tasks_impl(vec![compaction_group_id], 1, selector)
                .await?;
            if !normal_tasks.is_empty() {
                return Ok(normal_tasks.pop());
            } else if trivial_tasks.is_empty() {
                return Ok(None);
            }
        }
    }

    pub async fn manual_get_compact_task(
        &self,
        compaction_group_id: CompactionGroupId,
        manual_compaction_option: ManualCompactionOption,
    ) -> Result<Option<CompactTask>> {
        let mut selector: Box<dyn CompactionSelector> =
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

    pub async fn report_compact_task(
        &self,
        task_id: u64,
        task_status: TaskStatus,
        sorted_output_ssts: Vec<SstableInfo>,
        table_stats_change: Option<PbTableStatsMap>,
    ) -> Result<bool> {
        let rets = self
            .report_compact_tasks(vec![ReportTask {
                task_id,
                task_status: task_status as i32,
                sorted_output_ssts,
                table_stats_change: table_stats_change.unwrap_or_default(),
            }])
            .await?;
        Ok(rets[0])
    }

    /// Finishes or cancels a compaction task, according to `task_status`.
    ///
    /// If `context_id` is not None, its validity will be checked when writing meta store.
    /// Its ownership of the task is checked as well.
    ///
    /// Return Ok(false) indicates either the task is not found,
    /// or the task is not owned by `context_id` when `context_id` is not None.
    #[named]
    pub async fn report_compact_tasks(&self, report_tasks: Vec<ReportTask>) -> Result<Vec<bool>> {
        let mut guard = write_lock!(self, compaction).await;
        let deterministic_mode = self.env.opts.compaction_deterministic_test;
        let compaction = guard.deref_mut();
        let start_time = Instant::now();
        let original_keys = compaction.compaction_statuses.keys().cloned().collect_vec();
        let mut compact_statuses = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut compaction.compaction_statuses,)
        );
        let mut rets = vec![false; report_tasks.len()];
        let mut compact_task_assignment = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut compaction.compact_task_assignment,)
        );
        // The compaction task is finished.
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        let mut current_version = versioning.current_version.clone();
        // purge stale compact_status
        for group_id in original_keys {
            if !current_version.levels.contains_key(&group_id) {
                compact_statuses.remove(group_id);
            }
        }
        let mut tasks = vec![];

        let mut hummock_version_deltas = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut versioning.hummock_version_deltas,)
        );
        let mut branched_ssts = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut versioning.branched_ssts)
        );

        let mut version_stats = create_trx_wrapper!(
            self.sql_meta_store(),
            VarTransactionWrapper,
            VarTransaction::new(&mut versioning.version_stats)
        );
        let mut success_count = 0;
        let last_version_id = current_version.id;
        for (idx, task) in report_tasks.into_iter().enumerate() {
            rets[idx] = true;
            let mut compact_task = match compact_task_assignment.remove(task.task_id) {
                Some(compact_task) => compact_task.compact_task.unwrap(),
                None => {
                    tracing::warn!("{}", format!("compact task {} not found", task.task_id));
                    rets[idx] = false;
                    continue;
                }
            };

            {
                // apply result
                compact_task.task_status = task.task_status;
                compact_task.sorted_output_ssts = task.sorted_output_ssts;
            }

            match compact_statuses.get_mut(compact_task.compaction_group_id) {
                Some(mut compact_status) => {
                    compact_status.report_compact_task(&compact_task);
                }
                None => {
                    compact_task.set_task_status(TaskStatus::InvalidGroupCanceled);
                }
            }

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
                    Self::is_compact_task_expired(&compact_task, branched_ssts.tree_ref());
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
                            compact_task_to_string(&compact_task)
                        );
                    }
                    input_exist
                }
            } else {
                false
            };
            if is_success {
                success_count += 1;
                let version_delta = gen_version_delta(
                    &mut hummock_version_deltas,
                    &mut branched_ssts,
                    &current_version,
                    &compact_task,
                    deterministic_mode,
                );
                // apply version delta before we persist this change. If it causes panic we can
                // recover to a correct state after restarting meta-node.
                current_version.apply_version_delta(&version_delta);
                if purge_prost_table_stats(&mut version_stats.table_stats, &current_version) {
                    self.metrics.version_stats.reset();
                    versioning.local_metrics.clear();
                }
                add_prost_table_stats_map(&mut version_stats.table_stats, &task.table_stats_change);
                trigger_local_table_stat(
                    &self.metrics,
                    &mut versioning.local_metrics,
                    &version_stats,
                    &task.table_stats_change,
                );
            }
            tasks.push(compact_task);
        }
        if success_count > 0 {
            commit_multi_var!(
                self.env.meta_store(),
                self.sql_meta_store(),
                compact_statuses,
                compact_task_assignment,
                hummock_version_deltas,
                version_stats
            )?;
            branched_ssts.commit_memory();

            trigger_version_stat(&self.metrics, &current_version);
            trigger_delta_log_stats(&self.metrics, versioning.hummock_version_deltas.len());
            self.notify_stats(&versioning.version_stats);
            versioning.current_version = current_version;

            if !deterministic_mode {
                self.notify_version_deltas(versioning, last_version_id);
            }

            self.metrics
                .compact_task_batch_count
                .with_label_values(&["batch_report_task"])
                .observe(success_count as f64);
        } else {
            // The compaction task is cancelled or failed.
            commit_multi_var!(
                self.env.meta_store(),
                self.sql_meta_store(),
                compact_statuses,
                compact_task_assignment
            )?;
        }
        let mut success_groups = vec![];
        for compact_task in tasks {
            let task_status = compact_task.task_status();
            let task_status_label = task_status.as_str_name();
            let task_type_label = compact_task.task_type().as_str_name();

            self.compactor_manager
                .remove_task_heartbeat(compact_task.task_id);

            self.metrics
                .compact_frequency
                .with_label_values(&[
                    "normal",
                    &compact_task.compaction_group_id.to_string(),
                    task_type_label,
                    task_status_label,
                ])
                .inc();

            tracing::trace!(
                "Reported compaction task. {}. cost time: {:?}",
                compact_task_to_string(&compact_task),
                start_time.elapsed(),
            );

            trigger_sst_stat(
                &self.metrics,
                compaction
                    .compaction_statuses
                    .get(&compact_task.compaction_group_id),
                &versioning_guard.current_version,
                compact_task.compaction_group_id,
            );

            if !deterministic_mode
                && (matches!(compact_task.task_type(), compact_task::TaskType::Dynamic)
                    || matches!(compact_task.task_type(), compact_task::TaskType::Emergency))
            {
                // only try send Dynamic compaction
                self.try_send_compaction_request(
                    compact_task.compaction_group_id,
                    compact_task::TaskType::Dynamic,
                );
            }

            if task_status == TaskStatus::Success {
                success_groups.push(compact_task.compaction_group_id);
            }
        }
        drop(versioning_guard);
        if !success_groups.is_empty() {
            self.try_update_write_limits(&success_groups).await;
        }
        Ok(rets)
    }

    /// Caller should ensure `epoch` > `max_committed_epoch`
    #[named]
    pub async fn commit_epoch(
        &self,
        epoch: HummockEpoch,
        commit_info: CommitEpochInfo,
    ) -> Result<Option<HummockSnapshot>> {
        let CommitEpochInfo {
            mut sstables,
            new_table_watermarks,
            sst_to_context,
        } = commit_info;
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
        let mut new_version_delta = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapEntryTransactionWrapper,
            BTreeMapEntryTransaction::new_insert(
                &mut versioning.hummock_version_deltas,
                old_version.id + 1,
                build_version_delta_after_version(old_version),
            )
        );
        new_version_delta.max_committed_epoch = epoch;
        new_version_delta.new_table_watermarks = new_table_watermarks;
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
        let mut new_sst_id = next_sstable_object_id(&self.env, new_sst_id_number).await?;
        let mut branched_ssts = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut versioning.branched_ssts)
        );
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
        let mut version_stats = create_trx_wrapper!(
            self.sql_meta_store(),
            VarTransactionWrapper,
            VarTransaction::new(&mut versioning.version_stats)
        );
        add_prost_table_stats_map(&mut version_stats.table_stats, &table_stats_change);
        if purge_prost_table_stats(&mut version_stats.table_stats, &new_hummock_version) {
            self.metrics.version_stats.reset();
            versioning.local_metrics.clear();
        }

        trigger_local_table_stat(
            &self.metrics,
            &mut versioning.local_metrics,
            &version_stats,
            &table_stats_change,
        );
        commit_multi_var!(
            self.env.meta_store(),
            self.sql_meta_store(),
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
        let start_id = next_sstable_object_id(&self.env, number).await?;
        Ok(SstObjectIdRange::new(start_id, start_id + number as u64))
    }

    #[named]
    pub async fn get_min_pinned_version_id(&self) -> HummockVersionId {
        read_lock!(self, versioning).await.min_pinned_version_id()
    }

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
    ///
    /// Note: this method can hurt performance because it will clone a large object.
    #[named]
    pub async fn get_current_version(&self) -> HummockVersion {
        read_lock!(self, versioning).await.current_version.clone()
    }

    #[named]
    pub async fn get_current_max_committed_epoch(&self) -> HummockEpoch {
        read_lock!(self, versioning)
            .await
            .current_version
            .max_committed_epoch
    }

    /// Gets branched sstable infos
    /// Should not be called inside [`HummockManager`], because it requests locks internally.
    #[named]
    pub async fn get_branched_ssts_info(&self) -> BTreeMap<HummockSstableId, BranchedSstInfo> {
        read_lock!(self, versioning).await.branched_ssts.clone()
    }

    #[named]
    /// Gets the mapping from table id to compaction group id
    pub async fn get_table_compaction_group_id_mapping(
        &self,
    ) -> HashMap<StateTableId, CompactionGroupId> {
        get_table_compaction_group_id_mapping(&read_lock!(self, versioning).await.current_version)
    }

    /// Get version deltas from meta store
    #[cfg_attr(coverage, coverage(off))]
    #[named]
    pub async fn list_version_deltas(
        &self,
        start_id: u64,
        num_limit: u32,
        committed_epoch_limit: HummockEpoch,
    ) -> Result<Vec<HummockVersionDelta>> {
        let versioning = read_lock!(self, versioning).await;
        let version_deltas = versioning
            .hummock_version_deltas
            .range(start_id..)
            .map(|(_id, delta)| delta)
            .filter(|delta| delta.max_committed_epoch <= committed_epoch_limit)
            .take(num_limit as _)
            .cloned()
            .collect();
        Ok(version_deltas)
    }

    pub async fn init_metadata_for_version_replay(
        &self,
        table_catalogs: Vec<Table>,
        compaction_groups: Vec<PbCompactionGroupInfo>,
    ) -> Result<()> {
        for table in &table_catalogs {
            table.insert(self.env.meta_store_checked()).await?;
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
                    error = %e.as_report(),
                    "failed to send compaction request for compaction group {}",
                    compaction_group,
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
                tracing::warn!(error = %err.as_report(), "Failed to get compaction task");

                return Err(anyhow::anyhow!(err)
                    .context(format!(
                        "Failed to get compaction task for compaction_group {}",
                        compaction_group,
                    ))
                    .into());
            }
        };

        // 3. send task to compactor
        let compact_task_string = compact_task_to_string(&compact_task);
        // TODO: shall we need to cancel on meta ?
        compactor
            .send_event(ResponseEvent::CompactTask(compact_task))
            .with_context(|| {
                format!(
                    "Failed to trigger compaction task for compaction_group {}",
                    compaction_group,
                )
            })?;

        tracing::info!(
            "Trigger manual compaction task. {}. cost time: {:?}",
            &compact_task_string,
            start_time.elapsed(),
        );

        Ok(())
    }

    #[cfg(any(test, feature = "test"))]
    pub fn compactor_manager_ref_for_test(&self) -> CompactorManagerRef {
        self.compactor_manager.clone()
    }

    #[cfg(any(test, feature = "test"))]
    #[named]
    pub async fn compaction_task_from_assignment_for_test(
        &self,
        task_id: u64,
    ) -> Option<CompactTaskAssignment> {
        let compaction_guard = read_lock!(self, compaction).await;
        let assignment_ref = &compaction_guard.compact_task_assignment;
        assignment_ref.get(&task_id).cloned()
    }

    #[cfg(any(test, feature = "test"))]
    #[named]
    pub async fn report_compact_task_for_test(
        &self,
        task_id: u64,
        compact_task: Option<CompactTask>,
        task_status: TaskStatus,
        sorted_output_ssts: Vec<SstableInfo>,
        table_stats_change: Option<PbTableStatsMap>,
    ) -> Result<()> {
        if let Some(task) = compact_task {
            let mut guard = write_lock!(self, compaction).await;
            guard.compact_task_assignment.insert(
                task_id,
                CompactTaskAssignment {
                    compact_task: Some(task),
                    context_id: 0,
                },
            );
        }

        // In the test, the contents of the compact task may have been modified directly, while the contents of compact_task_assignment were not modified.
        // So we pass the modified compact_task directly into the `report_compact_task_impl`
        self.report_compact_tasks(vec![ReportTask {
            task_id,
            task_status: task_status as i32,
            sorted_output_ssts,
            table_stats_change: table_stats_change.unwrap_or_default(),
        }])
        .await?;
        Ok(())
    }

    pub fn metadata_manager(&self) -> &MetadataManager {
        &self.metadata_manager
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
                        .to_protobuf()],
                }),
            );
    }

    fn notify_version_deltas(&self, versioning: &Versioning, last_version_id: u64) {
        let start_version_id = last_version_id + 1;
        let version_deltas = versioning
            .hummock_version_deltas
            .range(start_version_id..)
            .map(|(_, delta)| delta.to_protobuf())
            .collect_vec();
        self.env
            .notification_manager()
            .notify_hummock_without_version(
                Operation::Add,
                Info::HummockVersionDeltas(risingwave_pb::hummock::HummockVersionDeltas {
                    version_deltas,
                }),
            );
    }

    fn notify_stats(&self, stats: &HummockVersionStats) {
        self.env
            .notification_manager()
            .notify_frontend_without_version(Operation::Update, Info::HummockStats(stats.clone()));
    }

    #[named]
    pub async fn get_compact_tasks_impl(
        &self,
        compaction_groups: Vec<CompactionGroupId>,
        max_task_count: usize,
        selector: &mut Box<dyn CompactionSelector>,
    ) -> crate::hummock::error::Result<(Vec<CompactTask>, Vec<CompactTask>)> {
        // TODO: `get_all_table_options` will hold catalog_manager async lock, to avoid holding the
        // lock in compaction_guard, take out all table_options in advance there may be a
        // waste of resources here, need to add a more efficient filter in catalog_manager
        let deterministic_mode = self.env.opts.compaction_deterministic_test;
        let all_table_id_to_option = self
            .metadata_manager
            .get_all_table_options()
            .await
            .map_err(|err| Error::MetaStore(err.into()))?;

        let mut compaction_guard = write_lock!(self, compaction).await;
        let compaction = compaction_guard.deref_mut();

        let start_time = Instant::now();

        let mut compaction_statuses = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut compaction.compaction_statuses)
        );

        let mut compact_task_assignment = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut compaction.compact_task_assignment)
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
        let mut trivial_tasks = vec![];
        let mut pick_tasks = vec![];
        for compaction_group_id in compaction_groups {
            if current_version.levels.get(&compaction_group_id).is_none() {
                continue;
            }
            if pick_tasks.len() >= max_task_count {
                break;
            }
            // StoredIdGenerator already implements ids pre-allocation by ID_PREALLOCATE_INTERVAL.
            let task_id = next_compaction_task_id(&self.env).await?;

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
                None => continue,
            };
            if !compaction_statuses.contains_key(&compaction_group_id) {
                compaction_statuses.insert(
                    compaction_group_id,
                    CompactStatus::new(
                        compaction_group_id,
                        group_config.compaction_config.max_level,
                    ),
                );
            }
            let mut compact_status = compaction_statuses.get_mut(compaction_group_id).unwrap();

            let can_trivial_move = matches!(selector.task_type(), TaskType::Dynamic)
                || matches!(selector.task_type(), TaskType::Emergency);

            let mut stats = LocalSelectorStatistic::default();
            let member_table_ids = &current_version
                .get_compaction_group_levels(compaction_group_id)
                .member_table_ids;

            let mut table_id_to_option: HashMap<u32, _> = HashMap::default();

            for table_id in member_table_ids {
                if let Some(opts) = all_table_id_to_option.get(table_id) {
                    table_id_to_option.insert(*table_id, *opts);
                }
            }

            let compact_task = compact_status.get_compact_task(
                current_version.get_compaction_group_levels(compaction_group_id),
                task_id as HummockCompactionTaskId,
                &group_config,
                &mut stats,
                selector,
                table_id_to_option.clone(),
                Arc::new(CompactionDeveloperConfig::new_from_meta_opts(
                    &self.env.opts,
                )),
            );

            stats.report_to_metrics(compaction_group_id, self.metrics.as_ref());
            let compact_task = match compact_task {
                None => {
                    continue;
                }
                Some(task) => task,
            };
            let target_level_id = compact_task.input.target_level as u32;

            let compression_algorithm = match compact_task.compression_algorithm.as_str() {
                "Lz4" => 1,
                "Zstd" => 2,
                _ => 0,
            };
            let vnode_partition_count = compact_task.input.vnode_partition_count;
            use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;

            let mut compact_task = CompactTask {
                input_ssts: compact_task.input.input_levels,
                splits: vec![risingwave_pb::hummock::KeyRange::inf()],
                watermark,
                sorted_output_ssts: vec![],
                task_id,
                target_level: target_level_id,
                // only gc delete keys in last level because there may be older version in more bottom
                // level.
                gc_delete_keys: current_version
                    .get_compaction_group_levels(compaction_group_id)
                    .is_last_level(target_level_id),
                base_level: compact_task.base_level as u32,
                task_status: TaskStatus::Pending as i32,
                compaction_group_id: group_config.group_id,
                existing_table_ids: member_table_ids.clone(),
                compression_algorithm,
                target_file_size: compact_task.target_file_size,
                table_options: table_id_to_option
                    .into_iter()
                    .filter_map(|(table_id, table_option)| {
                        if member_table_ids.contains(&table_id) {
                            return Some((table_id, TableOption::from(&table_option)));
                        }

                        None
                    })
                    .collect(),
                current_epoch_time: Epoch::now().0,
                compaction_filter_mask: group_config.compaction_config.compaction_filter_mask,
                target_sub_level_id: compact_task.input.target_sub_level_id,
                task_type: compact_task.compaction_task_type as i32,
                split_weight_by_vnode: compact_task.input.vnode_partition_count,
                ..Default::default()
            };

            let is_trivial_reclaim = CompactStatus::is_trivial_reclaim(&compact_task);
            let is_trivial_move = CompactStatus::is_trivial_move_task(&compact_task);
            if is_trivial_reclaim {
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
                compact_task.set_task_status(TaskStatus::Success);
                compact_status.report_compact_task(&compact_task);
                trivial_tasks.push(compact_task);
            } else if is_trivial_move && can_trivial_move {
                tracing::debug!(
                "TrivialMove for compaction group {}: pick up {} sstables in level {} to compact to target_level {} cost time: {:?} input {:?}",
                compaction_group_id,
                compact_task.input_ssts[0].table_infos.len(),
                compact_task.input_ssts[0].level_idx,
                compact_task.target_level,
                start_time.elapsed(),
                compact_task.input_ssts
            );
                // this task has been finished and `trivial_move_task` does not need to be schedule.
                compact_task.set_task_status(TaskStatus::Success);
                compact_task.sorted_output_ssts = compact_task.input_ssts[0].table_infos.clone();
                compact_status.report_compact_task(&compact_task);
                trivial_tasks.push(compact_task);
            } else {
                let mut table_to_vnode_partition = match self
                    .group_to_table_vnode_partition
                    .read()
                    .get(&compaction_group_id)
                {
                    Some(table_to_vnode_partition) => table_to_vnode_partition.clone(),
                    None => BTreeMap::default(),
                };
                table_to_vnode_partition
                    .retain(|table_id, _| compact_task.existing_table_ids.contains(table_id));

                if group_config.compaction_config.split_weight_by_vnode > 0 {
                    for table_id in &compact_task.existing_table_ids {
                        compact_task
                            .table_vnode_partition
                            .entry(*table_id)
                            .or_insert(vnode_partition_count);
                    }
                }
                compact_task.table_watermarks =
                    current_version.safe_epoch_table_watermarks(&compact_task.existing_table_ids);

                compact_task_assignment.insert(
                    compact_task.task_id,
                    CompactTaskAssignment {
                        compact_task: Some(compact_task.clone()),
                        context_id: META_NODE_ID, // deprecated
                    },
                );

                pick_tasks.push(compact_task);
            }
        }

        if !trivial_tasks.is_empty() {
            let mut versioning_guard = write_lock!(self, versioning).await;
            let versioning = versioning_guard.deref_mut();
            let mut hummock_version_deltas = create_trx_wrapper!(
                self.sql_meta_store(),
                BTreeMapTransactionWrapper,
                BTreeMapTransaction::new(&mut versioning.hummock_version_deltas,)
            );
            let mut branched_ssts = create_trx_wrapper!(
                self.sql_meta_store(),
                BTreeMapTransactionWrapper,
                BTreeMapTransaction::new(&mut versioning.branched_ssts)
            );
            let mut current_version = versioning.current_version.clone();
            let last_apply_version_id = current_version.id;
            for compact_task in &trivial_tasks {
                let version_delta = gen_version_delta(
                    &mut hummock_version_deltas,
                    &mut branched_ssts,
                    &current_version,
                    compact_task,
                    deterministic_mode,
                );
                current_version.apply_version_delta(&version_delta);
            }
            commit_multi_var!(
                self.env.meta_store(),
                self.sql_meta_store(),
                compaction_statuses,
                compact_task_assignment,
                hummock_version_deltas
            )?;
            branched_ssts.commit_memory();

            trigger_version_stat(&self.metrics, &current_version);
            trigger_delta_log_stats(&self.metrics, versioning.hummock_version_deltas.len());
            self.notify_stats(&versioning.version_stats);
            versioning.current_version = current_version;
            self.notify_version_deltas(versioning, last_apply_version_id);
            self.metrics
                .compact_frequency
                .with_label_values(&[
                    "trivial-space-reclaim",
                    "several_group",
                    selector.task_type().as_str_name(),
                    "SUCCESS",
                ])
                .inc_by(trivial_tasks.len() as u64);
            self.metrics
                .compact_task_batch_count
                .with_label_values(&["batch_trivial_move"])
                .observe(trivial_tasks.len() as f64);
        } else {
            // We are using a single transaction to ensure that each task has progress when it is
            // created.
            commit_multi_var!(
                self.env.meta_store(),
                self.sql_meta_store(),
                compaction_statuses,
                compact_task_assignment
            )?;
        }
        if !pick_tasks.is_empty() {
            self.metrics
                .compact_task_batch_count
                .with_label_values(&["batch_get_compact_task"])
                .observe(pick_tasks.len() as f64);
        }

        for compact_task in &mut pick_tasks {
            let compaction_group_id = compact_task.compaction_group_id;

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
            let compact_task_statistics = statistics_compact_task(compact_task);

            let level_type_label = build_compact_task_level_type_metrics_label(
                compact_task.input_ssts[0].level_idx as usize,
                compact_task.input_ssts.last().unwrap().level_idx as usize,
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
        Ok((pick_tasks, trivial_tasks))
    }

    #[named]
    pub fn hummock_timer_task(hummock_manager: Arc<Self>) -> (JoinHandle<()>, Sender<()>) {
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
                CompactionHeartBeatExpiredCheck,

                DynamicCompactionTrigger,
                SpaceReclaimCompactionTrigger,
                TtlCompactionTrigger,
                TombstoneCompactionTrigger,

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
                .map(|_| HummockTimerEvent::CompactionHeartBeatExpiredCheck);

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

            let mut tombstone_reclaim_trigger_interval =
                tokio::time::interval(Duration::from_secs(
                    hummock_manager
                        .env
                        .opts
                        .periodic_tombstone_reclaim_compaction_interval_sec,
                ));
            tombstone_reclaim_trigger_interval
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tombstone_reclaim_trigger_interval.reset();
            let tombstone_reclaim_trigger = IntervalStream::new(tombstone_reclaim_trigger_interval)
                .map(|_| HummockTimerEvent::TombstoneCompactionTrigger);

            let mut triggers: Vec<BoxStream<'static, HummockTimerEvent>> = vec![
                Box::pin(check_compact_trigger),
                Box::pin(stat_report_trigger),
                Box::pin(compaction_heartbeat_trigger),
                Box::pin(dynamic_tick_trigger),
                Box::pin(space_reclaim_trigger),
                Box::pin(ttl_reclaim_trigger),
                Box::pin(full_gc_trigger),
                Box::pin(tombstone_reclaim_trigger),
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
                                        .metadata_manager
                                        .get_job_id_to_internal_table_ids_mapping()
                                        .await
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

                                HummockTimerEvent::CompactionHeartBeatExpiredCheck => {
                                    let compactor_manager =
                                        hummock_manager.compactor_manager.clone();

                                    // TODO: add metrics to track expired tasks
                                    // The cancel task has two paths
                                    // 1. compactor heartbeat cancels the expired task based on task
                                    // progress (meta + compactor)
                                    // 2. meta periodically scans the task and performs a cancel on
                                    // the meta side for tasks that are not updated by heartbeat
                                    for task in compactor_manager.get_heartbeat_expired_tasks() {
                                        if let Err(e) = hummock_manager
                                            .cancel_compact_task(
                                                task.task_id,
                                                TaskStatus::HeartbeatCanceled,
                                            )
                                            .await
                                        {
                                            tracing::error!(
                                                task_id = task.task_id,
                                                error = %e.as_report(),
                                                "Attempt to remove compaction task due to elapsed heartbeat failed. We will continue to track its heartbeat
                                                until we can successfully report its status",
                                            );
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

                                HummockTimerEvent::TombstoneCompactionTrigger => {
                                    // Disable periodic trigger for compaction_deterministic_test.
                                    if hummock_manager.env.opts.compaction_deterministic_test {
                                        continue;
                                    }

                                    hummock_manager
                                        .on_handle_trigger_multi_group(
                                            compact_task::TaskType::Tombstone,
                                        )
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
            if entry.len() > HISTORY_TABLE_INFO_STATISTIC_TIME {
                entry.pop_front();
            }
        }
    }

    /// * For compaction group with only one single state-table, do not change it again.
    /// * For state-table which only write less than `HISTORY_TABLE_INFO_WINDOW_SIZE` times, do not
    ///   change it. Because we need more statistic data to decide split strategy.
    /// * For state-table with low throughput which write no more than
    ///   `min_table_split_write_throughput` data, never split it.
    /// * For state-table whose size less than `min_table_split_size`, do not split it unless its
    ///   throughput keep larger than `table_write_throughput_threshold` for a long time.
    /// * For state-table whose throughput less than `min_table_split_write_throughput`, do not
    ///   increase it size of base-level.
    async fn on_handle_check_split_multi_group(&self) {
        let params = self.env.system_params_reader().await;
        let barrier_interval_ms = params.barrier_interval_ms() as u64;
        let checkpoint_secs = std::cmp::max(
            1,
            params.checkpoint_frequency() * barrier_interval_ms / 1000,
        );
        let created_tables = match self.metadata_manager.get_created_table_ids().await {
            Ok(created_tables) => created_tables,
            Err(err) => {
                tracing::warn!(error = %err.as_report(), "failed to fetch created table ids");
                return;
            }
        };
        let created_tables: HashSet<u32> = HashSet::from_iter(created_tables);
        let table_write_throughput = self.history_table_throughput.read().clone();
        let mut group_infos = self.calculate_compaction_group_statistic().await;
        group_infos.sort_by_key(|group| group.group_size);
        group_infos.reverse();
        const SPLIT_BY_TABLE: u32 = 1;

        let mut group_to_table_vnode_partition = self.group_to_table_vnode_partition.read().clone();
        for group in &group_infos {
            if group.table_statistic.len() == 1 {
                // no need to handle the separate compaciton group
                continue;
            }

            let mut table_vnode_partition_mappoing = group_to_table_vnode_partition
                .entry(group.group_id)
                .or_default();

            for (table_id, table_size) in &group.table_statistic {
                let rule = self
                    .calculate_table_align_rule(
                        &table_write_throughput,
                        table_id,
                        table_size,
                        !created_tables.contains(table_id),
                        checkpoint_secs,
                        group.group_id,
                        group.group_size,
                    )
                    .await;

                match rule {
                    TableAlignRule::NoOptimization => {
                        table_vnode_partition_mappoing.remove(table_id);
                        continue;
                    }

                    TableAlignRule::SplitByTable(table_id) => {
                        if self.env.opts.hybird_partition_vnode_count > 0 {
                            table_vnode_partition_mappoing.insert(table_id, SPLIT_BY_TABLE);
                        } else {
                            table_vnode_partition_mappoing.remove(&table_id);
                        }
                    }

                    TableAlignRule::SplitByVnode((table_id, vnode)) => {
                        if self.env.opts.hybird_partition_vnode_count > 0 {
                            table_vnode_partition_mappoing.insert(table_id, vnode);
                        } else {
                            table_vnode_partition_mappoing.remove(&table_id);
                        }
                    }

                    TableAlignRule::SplitToDedicatedCg((
                        new_group_id,
                        table_vnode_partition_count,
                    )) => {
                        let _ = table_vnode_partition_mappoing; // drop
                        group_to_table_vnode_partition
                            .insert(new_group_id, table_vnode_partition_count);

                        table_vnode_partition_mappoing = group_to_table_vnode_partition
                            .entry(group.group_id)
                            .or_default();
                    }
                }
            }
        }

        tracing::trace!(
            "group_to_table_vnode_partition {:?}",
            group_to_table_vnode_partition
        );

        // batch update group_to_table_vnode_partition
        *self.group_to_table_vnode_partition.write() = group_to_table_vnode_partition;
    }

    pub fn compaction_event_loop(
        hummock_manager: Arc<Self>,
        mut compactor_streams_change_rx: UnboundedReceiver<(
            u32,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> Vec<(JoinHandle<()>, Sender<()>)> {
        let mut compactor_request_streams = FuturesUnordered::new();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let (shutdown_tx_dedicated, shutdown_rx_dedicated) = tokio::sync::oneshot::channel();
        let shutdown_rx_shared = shutdown_rx.shared();
        let shutdown_rx_dedicated_shared = shutdown_rx_dedicated.shared();

        let (tx, rx) = unbounded_channel();

        let mut join_handle_vec = Vec::default();

        let hummock_manager_dedicated = hummock_manager.clone();
        let compact_task_event_handler_join_handle = tokio::spawn(async move {
            Self::compact_task_dedicated_event_handler(
                hummock_manager_dedicated,
                rx,
                shutdown_rx_dedicated_shared,
            )
            .await;
        });

        join_handle_vec.push((
            compact_task_event_handler_join_handle,
            shutdown_tx_dedicated,
        ));

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
                let hummock_manager = hummock_manager.clone();
                hummock_manager
                    .metrics
                    .compaction_event_loop_iteration_latency
                    .observe(event_loop_iteration_now.elapsed().as_millis() as _);
                event_loop_iteration_now = Instant::now();

                tokio::select! {
                    _ = shutdown_rx_shared => { return; },

                    compactor_stream = compactor_streams_change_rx.recv() => {
                        if let Some((context_id, stream)) = compactor_stream {
                            tracing::info!("compactor {} enters the cluster", context_id);
                            push_stream(context_id, stream, &mut compactor_request_streams);
                        }
                    },

                    result = pending_on_none(compactor_request_streams.next()) => {
                        let mut compactor_alive = true;

                        let (context_id, compactor_stream_req): (_, (std::option::Option<std::result::Result<SubscribeCompactionEventRequest, _>>, _)) = result;
                        let (event, create_at, stream) = match compactor_stream_req {
                            (Some(Ok(req)), stream) => {
                                (req.event.unwrap(), req.create_at, stream)
                            }

                            (Some(Err(err)), _stream) => {
                                tracing::warn!(error = %err.as_report(), "compactor stream {} poll with err, recv stream may be destroyed", context_id);
                                continue
                            }

                            _ => {
                                tracing::warn!("compactor stream {} poll err, recv stream may be destroyed", context_id);
                                continue
                            },
                        };

                        {
                            let consumed_latency_ms = SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("Clock may have gone backwards")
                                .as_millis()
                                as u64
                            - create_at;
                            hummock_manager.metrics
                                .compaction_event_consumed_latency
                                .observe(consumed_latency_ms as _);
                        }

                        match event {
                            RequestEvent::HeartBeat(HeartBeat {
                                progress,
                            }) => {
                                let compactor_manager = hummock_manager.compactor_manager.clone();
                                let cancel_tasks = compactor_manager.update_task_heartbeats(&progress);
                                if let Some(compactor) = compactor_manager.get_compactor(context_id) {
                                    // TODO: task cancellation can be batched
                                    for task in cancel_tasks {
                                        tracing::info!(
                                            "Task with group_id {} task_id {} with context_id {} has expired due to lack of visible progress",
                                            task.compaction_group_id,
                                            task.task_id,
                                            context_id,
                                        );

                                        if let Err(e) =
                                            hummock_manager
                                            .cancel_compact_task(task.task_id, TaskStatus::HeartbeatCanceled)
                                            .await
                                        {
                                            tracing::error!(
                                                task_id = task.task_id,
                                                error = %e.as_report(),
                                                "Attempt to remove compaction task due to elapsed heartbeat failed. We will continue to track its heartbeat
                                                until we can successfully report its status."
                                            );
                                        }

                                        // Forcefully cancel the task so that it terminates
                                        // early on the compactor
                                        // node.
                                        let _ = compactor.cancel_task(task.task_id);
                                        tracing::info!(
                                            "CancelTask operation for task_id {} has been sent to node with context_id {}",
                                            context_id,
                                            task.task_id
                                        );
                                    }
                                } else {
                                    // Determine the validity of the compactor streaming rpc. When the compactor no longer exists in the manager, the stream will be removed.
                                    // Tip: Connectivity to the compactor will be determined through the `send_event` operation. When send fails, it will be removed from the manager
                                    compactor_alive = false;
                                }
                            },

                            RequestEvent::Register(_) => {
                                unreachable!()
                            }

                            e @ (RequestEvent::PullTask(_) | RequestEvent::ReportTask(_)) => {
                                let _ = tx.send((context_id, e));
                            }
                        }

                        if compactor_alive {
                            push_stream(context_id, stream, &mut compactor_request_streams);
                        } else {
                            tracing::warn!("compactor stream {} error, send stream may be destroyed", context_id);
                        }
                    },
                }
            }
        });

        join_handle_vec.push((join_handle, shutdown_tx));

        join_handle_vec
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
                    error = %e.as_report(),
                    "Failed to schedule {:?} compaction for compaction group {}",
                    task_type,
                    cg_id,
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

    #[named]
    pub async fn auto_pick_compaction_groups_and_type(
        &self,
    ) -> (Vec<CompactionGroupId>, compact_task::TaskType) {
        use rand::prelude::SliceRandom;
        use rand::thread_rng;
        let mut compaction_group_ids = self.compaction_group_ids().await;
        compaction_group_ids.shuffle(&mut thread_rng());

        let versioning_guard = read_lock!(self, versioning).await;
        let versioning = versioning_guard.deref();

        let mut normal_groups = vec![];
        for cg_id in compaction_group_ids {
            if versioning.write_limit.contains_key(&cg_id) {
                let enable_emergency_picker = match self
                    .compaction_group_manager
                    .read()
                    .await
                    .try_get_compaction_group_config(cg_id)
                {
                    Some(config) => config.compaction_config.enable_emergency_picker,
                    None => {
                        unreachable!("compaction-group {} not exist", cg_id)
                    }
                };

                if enable_emergency_picker {
                    if normal_groups.is_empty() {
                        return (vec![cg_id], TaskType::Emergency);
                    } else {
                        break;
                    }
                }
            }
            if let Some(pick_type) = self.compaction_state.auto_pick_type(cg_id) {
                if pick_type == TaskType::Dynamic {
                    normal_groups.push(cg_id);
                } else {
                    return (vec![cg_id], pick_type);
                }
            }
        }
        (normal_groups, TaskType::Dynamic)
    }

    async fn calculate_table_align_rule(
        &self,
        table_write_throughput: &HashMap<u32, VecDeque<u64>>,
        table_id: &u32,
        table_size: &u64,
        is_creating_table: bool,
        checkpoint_secs: u64,
        parent_group_id: u64,
        group_size: u64,
    ) -> TableAlignRule {
        let default_group_id: CompactionGroupId = StaticCompactionGroupId::StateDefault.into();
        let mv_group_id: CompactionGroupId = StaticCompactionGroupId::MaterializedView.into();
        let partition_vnode_count = self.env.opts.partition_vnode_count;
        let hybrid_vnode_count: u32 = self.env.opts.hybird_partition_vnode_count;
        let window_size = HISTORY_TABLE_INFO_STATISTIC_TIME / (checkpoint_secs as usize);

        let mut is_high_write_throughput = false;
        let mut is_low_write_throughput = true;
        if let Some(history) = table_write_throughput.get(table_id) {
            if !is_creating_table {
                if history.len() >= window_size {
                    is_high_write_throughput = history.iter().all(|throughput| {
                        *throughput / checkpoint_secs
                            > self.env.opts.table_write_throughput_threshold
                    });
                    is_low_write_throughput = history.iter().any(|throughput| {
                        *throughput / checkpoint_secs
                            < self.env.opts.min_table_split_write_throughput
                    });
                }
            } else {
                // For creating table, relax the checking restrictions to make the data alignment behavior more sensitive.
                let sum = history.iter().sum::<u64>();
                is_low_write_throughput = sum
                    < self.env.opts.min_table_split_write_throughput
                        * history.len() as u64
                        * checkpoint_secs;
            }
        }

        let state_table_size = *table_size;
        let result = {
            // When in a hybrid compaction group, data from multiple state tables may exist in a single sst, and in order to make the data in the sub level more aligned, a proactive cut is made for the data.
            // https://github.com/risingwavelabs/risingwave/issues/13037
            // 1. In some scenario (like backfill), the creating state_table / mv may have high write throughput (creating table ). Therefore, we relax the limit of `is_low_write_throughput` and partition the table with high write throughput by vnode to improve the parallel efficiency of compaction.
            // Add: creating table is not allowed to be split
            // 2. For table with low throughput, partition by table_id to minimize amplification.
            // 3. When the write mode is changed (the above conditions are not met), the default behavior is restored
            if !is_low_write_throughput {
                TableAlignRule::SplitByVnode((*table_id, hybrid_vnode_count))
            } else if state_table_size > self.env.opts.cut_table_size_limit {
                TableAlignRule::SplitByTable(*table_id)
            } else {
                TableAlignRule::NoOptimization
            }
        };

        // 1. Avoid splitting a creating table
        // 2. Avoid splitting a is_low_write_throughput creating table
        // 3. Avoid splitting a non-high throughput medium-sized table
        if is_creating_table
            || (is_low_write_throughput)
            || (state_table_size < self.env.opts.min_table_split_size && !is_high_write_throughput)
        {
            return result;
        }

        // do not split a large table and a small table because it would increase IOPS
        // of small table.
        if parent_group_id != default_group_id && parent_group_id != mv_group_id {
            let rest_group_size = group_size - state_table_size;
            if rest_group_size < state_table_size
                && rest_group_size < self.env.opts.min_table_split_size
            {
                return result;
            }
        }

        let ret = self
            .move_state_table_to_compaction_group(
                parent_group_id,
                &[*table_id],
                None,
                partition_vnode_count,
            )
            .await;
        match ret {
            Ok((new_group_id, table_vnode_partition_count)) => {
                tracing::info!("move state table [{}] from group-{} to group-{} success table_vnode_partition_count {:?}", table_id, parent_group_id, new_group_id, table_vnode_partition_count);
                return TableAlignRule::SplitToDedicatedCg((
                    new_group_id,
                    table_vnode_partition_count,
                ));
            }
            Err(e) => {
                tracing::info!(
                    error = %e.as_report(),
                    "failed to move state table [{}] from group-{}",
                    table_id,
                    parent_group_id,
                )
            }
        }

        TableAlignRule::NoOptimization
    }

    async fn initial_compaction_group_config_after_load(
        &self,
        versioning_guard: &mut RwLockWriteGuard<'_, Versioning>,
    ) -> Result<()> {
        // 1. Due to version compatibility, we fix some of the configuration of older versions after hummock starts.
        let current_version = &versioning_guard.current_version;
        let all_group_ids = get_compaction_group_ids(current_version);
        let mut configs = self
            .compaction_group_manager
            .write()
            .await
            .get_or_insert_compaction_group_configs(
                &all_group_ids.collect_vec(),
                self.env.meta_store(),
            )
            .await?;

        // We've already lowered the default limit for write limit in PR-12183, and to prevent older clusters from continuing to use the outdated configuration, we've introduced a new logic to rewrite it in a uniform way.
        let mut rewrite_cg_ids = vec![];
        let mut restore_cg_to_partition_vnode: HashMap<u64, BTreeMap<u32, u32>> =
            HashMap::default();
        for (cg_id, compaction_group_config) in &mut configs {
            // update write limit
            let relaxed_default_write_stop_level_count = 1000;
            if compaction_group_config
                .compaction_config
                .level0_sub_level_compact_level_count
                == relaxed_default_write_stop_level_count
            {
                rewrite_cg_ids.push(*cg_id);
            }

            if let Some(levels) = current_version.levels.get(cg_id) {
                if levels.member_table_ids.len() == 1 {
                    restore_cg_to_partition_vnode.insert(
                        *cg_id,
                        vec![(
                            levels.member_table_ids[0],
                            compaction_group_config
                                .compaction_config
                                .split_weight_by_vnode,
                        )]
                        .into_iter()
                        .collect(),
                    );
                }
            }
        }

        if !rewrite_cg_ids.is_empty() {
            tracing::info!("Compaction group {:?} configs rewrite ", rewrite_cg_ids);

            // update meta store
            let result = self
                .compaction_group_manager
                .write()
                .await
                .update_compaction_config(
                    &rewrite_cg_ids,
                    &[
                        mutable_config::MutableConfig::Level0StopWriteThresholdSubLevelNumber(
                            compaction_config::level0_stop_write_threshold_sub_level_number(),
                        ),
                    ],
                    self.env.meta_store(),
                )
                .await?;

            // update memory
            for new_config in result {
                configs.insert(new_config.group_id(), new_config);
            }
        }

        versioning_guard.write_limit =
            calc_new_write_limits(configs, HashMap::new(), &versioning_guard.current_version);
        trigger_write_stop_stats(&self.metrics, &versioning_guard.write_limit);
        tracing::debug!("Hummock stopped write: {:#?}", versioning_guard.write_limit);

        {
            // 2. Restore the memory data structure according to the memory of the compaction group config.
            let mut group_to_table_vnode_partition = self.group_to_table_vnode_partition.write();
            for (cg_id, table_vnode_partition) in restore_cg_to_partition_vnode {
                group_to_table_vnode_partition.insert(cg_id, table_vnode_partition);
            }
        }

        Ok(())
    }
}

// This structure describes how hummock handles sst switching in a compaction group. A better sst cut will result in better data alignment, which in turn will improve the efficiency of the compaction.
// By adopting certain rules, a better sst cut will lead to better data alignment and thus improve the efficiency of the compaction.
pub enum TableAlignRule {
    // The table_id is not optimized for alignment.
    NoOptimization,
    // Move the table_id to a separate compaction group. Currently, the system only supports separate compaction with one table.
    SplitToDedicatedCg((CompactionGroupId, BTreeMap<TableId, u32>)),
    // In the current group, partition the table's data according to the granularity of the vnode.
    SplitByVnode((TableId, u32)),
    // In the current group, partition the table's data at the granularity of the table.
    SplitByTable(TableId),
}

fn drop_sst(
    branched_ssts: &mut BTreeMapTransactionWrapper<'_, HummockSstableObjectId, BranchedSstInfo>,
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
    txn: &mut BTreeMapTransactionWrapper<'a, HummockVersionId, HummockVersionDelta>,
    branched_ssts: &mut BTreeMapTransactionWrapper<'a, HummockSstableObjectId, BranchedSstInfo>,
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
    let mut removed_table_ids_map: BTreeMap<u32, Vec<u64>> = BTreeMap::default();

    for level in &compact_task.input_ssts {
        let level_idx = level.level_idx;
        let mut removed_table_ids = level
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
            .collect_vec();

        removed_table_ids_map
            .entry(level_idx)
            .or_default()
            .append(&mut removed_table_ids);
    }

    for (level_idx, removed_table_ids) in removed_table_ids_map {
        let group_delta = GroupDelta {
            delta_type: Some(DeltaType::IntraLevel(IntraLevelDelta {
                level_idx,
                removed_table_ids,
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
            vnode_partition_count: compact_task.split_weight_by_vnode,
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
    match object_store.read(&cluster_id_full_path, ..).await {
        Ok(stored_cluster_id) => {
            let stored_cluster_id = String::from_utf8(stored_cluster_id.to_vec()).unwrap();
            if cluster_id.deref() == stored_cluster_id {
                return Ok(());
            }

            Err(ObjectError::internal(format!(
                "Data directory is already used by another cluster with id {:?}, path {}.",
                stored_cluster_id, cluster_id_full_path,
            ))
            .into())
        }
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

fn init_selectors() -> HashMap<compact_task::TaskType, Box<dyn CompactionSelector>> {
    let mut compaction_selectors: HashMap<compact_task::TaskType, Box<dyn CompactionSelector>> =
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
    compaction_selectors.insert(
        compact_task::TaskType::Tombstone,
        Box::<TombstoneCompactionSelector>::default(),
    );
    compaction_selectors.insert(
        compact_task::TaskType::Emergency,
        Box::<EmergencySelector>::default(),
    );
    compaction_selectors
}

type CompactionRequestChannelItem = (CompactionGroupId, compact_task::TaskType);
use risingwave_hummock_sdk::table_watermark::TableWatermarks;
use risingwave_hummock_sdk::version::HummockVersion;
use tokio::sync::mpsc::error::SendError;

use super::compaction::selector::EmergencySelector;
use super::compaction::CompactionSelector;
use crate::controller::SqlMetaStore;
use crate::hummock::manager::checkpoint::HummockVersionCheckpoint;
use crate::hummock::sequence::next_sstable_object_id;

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
        if guard.contains(&(group, compact_task::TaskType::Dynamic)) {
            Some(compact_task::TaskType::Dynamic)
        } else if guard.contains(&(group, compact_task::TaskType::SpaceReclaim)) {
            Some(compact_task::TaskType::SpaceReclaim)
        } else if guard.contains(&(group, compact_task::TaskType::Ttl)) {
            Some(compact_task::TaskType::Ttl)
        } else if guard.contains(&(group, compact_task::TaskType::Tombstone)) {
            Some(compact_task::TaskType::Tombstone)
        } else {
            None
        }
    }
}
