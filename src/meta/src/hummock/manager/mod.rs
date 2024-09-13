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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::monitor::MonitoredRwLock;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{
    version_archive_dir, version_checkpoint_path, CompactionGroupId, HummockCompactionTaskId,
    HummockContextId, HummockVersionId,
};
use risingwave_meta_model_v2::{
    compaction_status, compaction_task, hummock_pinned_snapshot, hummock_pinned_version,
    hummock_version_delta, hummock_version_stats,
};
use risingwave_pb::hummock::{
    HummockPinnedSnapshot, HummockPinnedVersion, HummockSnapshot, HummockVersionStats,
    PbCompactTaskAssignment, PbCompactionGroupInfo, SubscribeCompactionEventRequest,
};
use risingwave_pb::meta::subscribe_response::Operation;
use tokio::sync::mpsc::UnboundedSender;
use tonic::Streaming;

use crate::hummock::compaction::CompactStatus;
use crate::hummock::error::Result;
use crate::hummock::manager::checkpoint::HummockVersionCheckpoint;
use crate::hummock::manager::context::ContextInfo;
use crate::hummock::manager::gc::DeleteObjectTracker;
use crate::hummock::CompactorManagerRef;
use crate::manager::{MetaSrvEnv, MetaStoreImpl, MetadataManager};
use crate::model::{ClusterId, MetadataModel, MetadataModelError};
use crate::rpc::metrics::MetaMetrics;

mod context;
mod gc;
mod tests;
mod versioning;
pub use context::HummockVersionSafePoint;
use versioning::*;
pub(crate) mod checkpoint;
mod commit_epoch;
mod compaction;
pub mod sequence;
pub mod time_travel;
mod timer_task;
mod transaction;
mod utils;
mod worker;

pub use commit_epoch::{CommitEpochInfo, NewTableFragmentInfo};
use compaction::*;
pub use compaction::{check_cg_write_limit, WriteLimitType};
pub(crate) use utils::*;

type Snapshot = ArcSwap<HummockSnapshot>;

// Update to states are performed as follow:
// - Initialize ValTransaction for the meta state to update
// - Make changes on the ValTransaction.
// - Call `commit_multi_var` to commit the changes via meta store transaction. If transaction
//   succeeds, the in-mem state will be updated by the way.
pub struct HummockManager {
    pub env: MetaSrvEnv,

    metadata_manager: MetadataManager,
    /// Lock order: `compaction`, `versioning`, `compaction_group_manager`, `context_info`
    /// - Lock `compaction` first, then `versioning`, then `compaction_group_manager` and finally `context_info`.
    /// - This order should be strictly followed to prevent deadlock.
    compaction: MonitoredRwLock<Compaction>,
    versioning: MonitoredRwLock<Versioning>,
    /// `CompactionGroupManager` manages compaction configs for compaction groups.
    compaction_group_manager: MonitoredRwLock<CompactionGroupManager>,
    context_info: MonitoredRwLock<ContextInfo>,
    latest_snapshot: Snapshot,

    pub metrics: Arc<MetaMetrics>,

    pub compactor_manager: CompactorManagerRef,
    event_sender: HummockManagerEventSender,

    delete_object_tracker: DeleteObjectTracker,

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
}

pub type HummockManagerRef = Arc<HummockManager>;

use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_object_store::object::{build_remote_object_store, ObjectError, ObjectStoreRef};
use risingwave_pb::catalog::Table;
use risingwave_pb::meta::relation::RelationInfo;

macro_rules! start_measure_real_process_timer {
    ($hummock_mgr:expr, $func_name:literal) => {
        $hummock_mgr
            .metrics
            .hummock_manager_real_process_time
            .with_label_values(&[$func_name])
            .start_timer()
    };
}
pub(crate) use start_measure_real_process_timer;

use crate::hummock::manager::compaction_group_manager::CompactionGroupManager;
use crate::hummock::manager::worker::HummockManagerEventSender;

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
        let compaction_group_manager = CompactionGroupManager::new(&env).await?;
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
        cluster_manager: crate::manager::ClusterManagerRef,
        fragment_manager: crate::manager::FragmentManagerRef,
        metrics: Arc<MetaMetrics>,
        compactor_manager: CompactorManagerRef,
        config: risingwave_pb::hummock::CompactionConfig,
        compactor_streams_change_tx: UnboundedSender<(
            u32,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> HummockManagerRef {
        use crate::manager::CatalogManager;
        let compaction_group_manager = CompactionGroupManager::new_with_config(&env, config)
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
        compaction_group_manager: CompactionGroupManager,
        compactor_streams_change_tx: UnboundedSender<(
            u32,
            Streaming<SubscribeCompactionEventRequest>,
        )>,
    ) -> Result<HummockManagerRef> {
        let sys_params = env.system_params_reader().await;
        let state_store_url = sys_params.state_store();
        let state_store_dir: &str = sys_params.data_directory();
        let deterministic_mode = env.opts.compaction_deterministic_test;
        let mut object_store_config = env.opts.object_store_config.clone();
        // For fs and hdfs object store, operations are not always atomic.
        // We should manually enable atomicity guarantee by setting the atomic_write_dir config when building services.
        object_store_config.set_atomic_write_dir();
        let object_store = Arc::new(
            build_remote_object_store(
                state_store_url.strip_prefix("hummock+").unwrap_or("memory"),
                metrics.object_store_metric.clone(),
                "Version Checkpoint",
                Arc::new(object_store_config),
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
                "hummock_manager::versioning",
            ),
            compaction: MonitoredRwLock::new(
                metrics.hummock_manager_lock_time.clone(),
                Default::default(),
                "hummock_manager::compaction",
            ),
            compaction_group_manager: MonitoredRwLock::new(
                metrics.hummock_manager_lock_time.clone(),
                compaction_group_manager,
                "hummock_manager::compaction_group_manager",
            ),
            context_info: MonitoredRwLock::new(
                metrics.hummock_manager_lock_time.clone(),
                Default::default(),
                "hummock_manager::context_info",
            ),
            metrics,
            metadata_manager,
            // compaction_request_channel: parking_lot::RwLock::new(None),
            compactor_manager,
            latest_snapshot: ArcSwap::from_pointee(HummockSnapshot {
                committed_epoch: INVALID_EPOCH,
            }),
            event_sender: tx,
            delete_object_tracker: Default::default(),
            object_store,
            version_checkpoint_path,
            version_archive_dir,
            pause_version_checkpoint: AtomicBool::new(false),
            history_table_throughput: parking_lot::RwLock::new(HashMap::default()),
            compactor_streams_change_tx,
            compaction_state: CompactionState::new(),
        };
        let instance = Arc::new(instance);
        instance.init_time_travel_state().await?;
        instance.start_worker(rx).await;
        instance.load_meta_store_state().await?;
        instance.release_invalid_contexts().await?;
        // Release snapshots pinned by meta on restarting.
        instance.release_meta_context().await?;
        Ok(instance)
    }

    fn meta_store_ref(&self) -> &MetaStoreImpl {
        self.env.meta_store_ref()
    }

    /// Load state from meta store.
    async fn load_meta_store_state(&self) -> Result<()> {
        let mut compaction_guard = self.compaction.write().await;
        let mut versioning_guard = self.versioning.write().await;
        let mut context_info_guard = self.context_info.write().await;
        self.load_meta_store_state_impl(
            &mut compaction_guard,
            &mut versioning_guard,
            &mut context_info_guard,
        )
        .await
    }

    /// Load state from meta store.
    async fn load_meta_store_state_impl(
        &self,
        compaction_guard: &mut Compaction,
        versioning_guard: &mut Versioning,
        context_info: &mut ContextInfo,
    ) -> Result<()> {
        use sea_orm::EntityTrait;
        let meta_store = self.meta_store_ref();
        let compaction_statuses: BTreeMap<CompactionGroupId, CompactStatus> = match &meta_store {
            MetaStoreImpl::Kv(meta_store) => CompactStatus::list(meta_store)
                .await?
                .into_iter()
                .map(|cg| (cg.compaction_group_id(), cg))
                .collect(),
            MetaStoreImpl::Sql(sql_meta_store) => compaction_status::Entity::find()
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

        compaction_guard.compact_task_assignment = match &meta_store {
            MetaStoreImpl::Kv(meta_store) => PbCompactTaskAssignment::list(meta_store)
                .await?
                .into_iter()
                .map(|assigned| (assigned.key().unwrap(), assigned))
                .collect(),
            MetaStoreImpl::Sql(sql_meta_store) => compaction_task::Entity::find()
                .all(&sql_meta_store.conn)
                .await
                .map_err(MetadataModelError::from)?
                .into_iter()
                .map(|m| {
                    (
                        m.id as HummockCompactionTaskId,
                        PbCompactTaskAssignment::from(m),
                    )
                })
                .collect(),
        };

        let hummock_version_deltas: BTreeMap<HummockVersionId, HummockVersionDelta> =
            match &meta_store {
                MetaStoreImpl::Kv(meta_store) => HummockVersionDelta::list(meta_store)
                    .await?
                    .into_iter()
                    .map(|version_delta| (version_delta.id, version_delta))
                    .collect(),
                MetaStoreImpl::Sql(sql_meta_store) => {
                    use risingwave_pb::hummock::PbHummockVersionDelta;
                    hummock_version_delta::Entity::find()
                        .all(&sql_meta_store.conn)
                        .await
                        .map_err(MetadataModelError::from)?
                        .into_iter()
                        .map(|m| {
                            (
                                HummockVersionId::new(m.id as _),
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
            let checkpoint_version = HummockVersion::create_init_version(default_compaction_config);
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
        versioning_guard.version_stats = match &meta_store {
            MetaStoreImpl::Kv(meta_store) => HummockVersionStats::list(meta_store)
                .await?
                .into_iter()
                .next(),
            MetaStoreImpl::Sql(sql_meta_store) => hummock_version_stats::Entity::find()
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
                committed_epoch: redo_state.visible_table_committed_epoch(),
            }
            .into(),
        );
        versioning_guard.current_version = redo_state;
        versioning_guard.hummock_version_deltas = hummock_version_deltas;

        context_info.pinned_versions = match &meta_store {
            MetaStoreImpl::Kv(meta_store) => HummockPinnedVersion::list(meta_store)
                .await?
                .into_iter()
                .map(|p| (p.context_id, p))
                .collect(),
            MetaStoreImpl::Sql(sql_meta_store) => hummock_pinned_version::Entity::find()
                .all(&sql_meta_store.conn)
                .await
                .map_err(MetadataModelError::from)?
                .into_iter()
                .map(|m| (m.context_id as HummockContextId, m.into()))
                .collect(),
        };

        context_info.pinned_snapshots = match &meta_store {
            MetaStoreImpl::Kv(meta_store) => HummockPinnedSnapshot::list(meta_store)
                .await?
                .into_iter()
                .map(|p| (p.context_id, p))
                .collect(),
            MetaStoreImpl::Sql(sql_meta_store) => hummock_pinned_snapshot::Entity::find()
                .all(&sql_meta_store.conn)
                .await
                .map_err(MetadataModelError::from)?
                .into_iter()
                .map(|m| (m.context_id as HummockContextId, m.into()))
                .collect(),
        };

        self.delete_object_tracker.clear();
        // Not delete stale objects when archive or time travel is enabled
        if !self.env.opts.enable_hummock_data_archive && !self.time_travel_enabled().await {
            versioning_guard.mark_objects_for_deletion(context_info, &self.delete_object_tracker);
        }

        self.initial_compaction_group_config_after_load(
            versioning_guard,
            self.compaction_group_manager.write().await.deref_mut(),
        )
        .await?;

        Ok(())
    }

    pub async fn init_metadata_for_version_replay(
        &self,
        table_catalogs: Vec<Table>,
        compaction_groups: Vec<PbCompactionGroupInfo>,
    ) -> Result<()> {
        for table in &table_catalogs {
            table.insert(self.env.meta_store().as_kv()).await?;
        }
        for group in &compaction_groups {
            assert!(
                group.id == StaticCompactionGroupId::NewCompactionGroup as u64
                    || (group.id >= StaticCompactionGroupId::StateDefault as u64
                    && group.id <= StaticCompactionGroupId::MaterializedView as u64),
                "compaction group id should be either NewCompactionGroup to create new one, or predefined static ones."
            );
        }

        let mut compaction_group_manager = self.compaction_group_manager.write().await;
        let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();
        for group in &compaction_groups {
            let mut pairs = vec![];
            for table_id in group.member_table_ids.clone() {
                pairs.push((table_id as StateTableId, group.id));
            }
            let group_config = group.compaction_config.clone().unwrap();
            compaction_groups_txn.create_compaction_groups(group.id, Arc::new(group_config));

            self.register_table_ids_for_test(&pairs).await?;
            tracing::info!("Registered table ids {:?}", pairs);
        }

        commit_multi_var!(self.meta_store_ref(), compaction_groups_txn)?;

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
    pub async fn replay_version_delta(
        &self,
        mut version_delta: HummockVersionDelta,
    ) -> Result<(HummockVersion, Vec<CompactionGroupId>)> {
        let mut versioning_guard = self.versioning.write().await;
        // ensure the version id is ascending after replay
        version_delta.id = versioning_guard.current_version.next_version_id();
        version_delta.prev_id = versioning_guard.current_version.id;
        versioning_guard
            .current_version
            .apply_version_delta(&version_delta);

        let version_new = versioning_guard.current_version.clone();
        let compaction_group_ids = version_delta.group_deltas.keys().cloned().collect_vec();
        Ok((version_new, compaction_group_ids))
    }

    pub async fn disable_commit_epoch(&self) -> HummockVersion {
        let mut versioning_guard = self.versioning.write().await;
        versioning_guard.disable_commit_epochs = true;
        versioning_guard.current_version.clone()
    }

    pub fn metadata_manager(&self) -> &MetadataManager {
        &self.metadata_manager
    }

    pub fn object_store_media_type(&self) -> &'static str {
        self.object_store.media_type()
    }
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
