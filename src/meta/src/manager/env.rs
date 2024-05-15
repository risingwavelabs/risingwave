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

use std::ops::Deref;
use std::sync::Arc;

use risingwave_common::config::{
    CompactionConfig, DefaultParallelism, MetaBackend, ObjectStoreConfig,
};
use risingwave_common::session_config::SessionConfig;
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_meta_model_v2::prelude::Cluster;
use risingwave_pb::meta::SystemParams;
use risingwave_rpc_client::{StreamClientPool, StreamClientPoolRef};
use sea_orm::EntityTrait;

use super::{
    SessionParamsManager, SessionParamsManagerRef, SystemParamsManager, SystemParamsManagerRef,
};
use crate::controller::id::{
    IdGeneratorManager as SqlIdGeneratorManager, IdGeneratorManagerRef as SqlIdGeneratorManagerRef,
};
use crate::controller::session_params::{SessionParamsController, SessionParamsControllerRef};
use crate::controller::system_param::{SystemParamsController, SystemParamsControllerRef};
use crate::controller::SqlMetaStore;
use crate::hummock::sequence::SequenceGenerator;
use crate::manager::event_log::{start_event_log_manager, EventLogMangerRef};
use crate::manager::{
    IdGeneratorManager, IdGeneratorManagerRef, IdleManager, IdleManagerRef, NotificationManager,
    NotificationManagerRef,
};
use crate::model::ClusterId;
use crate::storage::{MetaStore, MetaStoreRef};
use crate::MetaResult;

#[derive(Clone)]
pub enum IdGenManagerImpl {
    Kv(IdGeneratorManagerRef),
    Sql(SqlIdGeneratorManagerRef),
}

impl IdGenManagerImpl {
    pub fn as_kv(&self) -> &IdGeneratorManagerRef {
        match self {
            IdGenManagerImpl::Kv(mgr) => mgr,
            _ => panic!("expect kv id generator manager"),
        }
    }
}

#[derive(Clone)]
pub enum MetaStoreImpl {
    Kv(MetaStoreRef),
    Sql(SqlMetaStore),
}

impl MetaStoreImpl {
    pub fn as_kv(&self) -> &MetaStoreRef {
        match self {
            MetaStoreImpl::Kv(mgr) => mgr,
            _ => panic!("expect kv meta store"),
        }
    }

    pub fn as_sql(&self) -> &SqlMetaStore {
        match self {
            MetaStoreImpl::Sql(mgr) => mgr,
            _ => panic!("expect sql meta store"),
        }
    }

    pub fn backend(&self) -> MetaBackend {
        match self {
            MetaStoreImpl::Kv(meta_store) => meta_store.meta_store_type(),
            MetaStoreImpl::Sql(_) => MetaBackend::Sql,
        }
    }
}

#[derive(Clone)]
pub enum SystemParamsManagerImpl {
    Kv(SystemParamsManagerRef),
    Sql(SystemParamsControllerRef),
}

#[derive(Clone)]
pub enum SessionParamsManagerImpl {
    Kv(SessionParamsManagerRef),
    Sql(SessionParamsControllerRef),
}

/// [`MetaSrvEnv`] is the global environment in Meta service. The instance will be shared by all
/// kind of managers inside Meta.
#[derive(Clone)]
pub struct MetaSrvEnv {
    /// id generator manager.
    id_gen_manager_impl: IdGenManagerImpl,

    /// system param manager.
    system_param_manager_impl: SystemParamsManagerImpl,

    /// session param manager.
    session_param_manager_impl: SessionParamsManagerImpl,

    /// meta store.
    meta_store_impl: MetaStoreImpl,

    /// notification manager.
    notification_manager: NotificationManagerRef,

    /// stream client pool memorization.
    stream_client_pool: StreamClientPoolRef,

    /// idle status manager.
    idle_manager: IdleManagerRef,

    event_log_manager: EventLogMangerRef,

    /// Unique identifier of the cluster.
    cluster_id: ClusterId,

    pub hummock_seq: Option<Arc<SequenceGenerator>>,

    /// options read by all services
    pub opts: Arc<MetaOpts>,
}

/// Options shared by all meta service instances
#[derive(Clone, serde::Serialize)]
pub struct MetaOpts {
    /// Whether to enable the recovery of the cluster. If disabled, the meta service will exit on
    /// abnormal cases.
    pub enable_recovery: bool,
    /// Whether to disable the auto-scaling feature.
    pub disable_automatic_parallelism_control: bool,
    /// The number of streaming jobs per scaling operation.
    pub parallelism_control_batch_size: usize,
    /// The period of parallelism control trigger.
    pub parallelism_control_trigger_period_sec: u64,
    /// The first delay of parallelism control.
    pub parallelism_control_trigger_first_delay_sec: u64,
    /// The maximum number of barriers in-flight in the compute nodes.
    pub in_flight_barrier_nums: usize,
    /// After specified seconds of idle (no mview or flush), the process will be exited.
    /// 0 for infinite, process will never be exited due to long idle time.
    pub max_idle_ms: u64,
    /// Whether run in compaction detection test mode
    pub compaction_deterministic_test: bool,
    /// Default parallelism of units for all streaming jobs.
    pub default_parallelism: DefaultParallelism,

    /// Interval of invoking a vacuum job, to remove stale metadata from meta store and objects
    /// from object store.
    pub vacuum_interval_sec: u64,
    /// The spin interval inside a vacuum job. It avoids the vacuum job monopolizing resources of
    /// meta node.
    pub vacuum_spin_interval_ms: u64,
    /// Interval of hummock version checkpoint.
    pub hummock_version_checkpoint_interval_sec: u64,
    pub enable_hummock_data_archive: bool,
    /// The minimum delta log number a new checkpoint should compact, otherwise the checkpoint
    /// attempt is rejected. Greater value reduces object store IO, meanwhile it results in
    /// more loss of in memory `HummockVersionCheckpoint::stale_objects` state when meta node is
    /// restarted.
    pub min_delta_log_num_for_hummock_version_checkpoint: u64,
    /// Objects within `min_sst_retention_time_sec` won't be deleted by hummock full GC, even they
    /// are dangling.
    pub min_sst_retention_time_sec: u64,
    /// Interval of automatic hummock full GC.
    pub full_gc_interval_sec: u64,
    /// The spin interval when collecting global GC watermark in hummock
    pub collect_gc_watermark_spin_interval_sec: u64,
    /// Enable sanity check when SSTs are committed
    pub enable_committed_sst_sanity_check: bool,
    /// Schedule compaction for all compaction groups with this interval.
    pub periodic_compaction_interval_sec: u64,
    /// Interval of reporting the number of nodes in the cluster.
    pub node_num_monitor_interval_sec: u64,

    /// The Prometheus endpoint for Meta Dashboard Service.
    /// The Dashboard service uses this in the following ways:
    /// 1. Query Prometheus for relevant metrics to find Stream Graph Bottleneck, and display it.
    /// 2. Provide cluster diagnostics, at `/api/monitor/diagnose` to troubleshoot cluster.
    /// These are just examples which show how the Meta Dashboard Service queries Prometheus.
    pub prometheus_endpoint: Option<String>,

    /// The additional selector used when querying Prometheus.
    pub prometheus_selector: Option<String>,

    /// The VPC id of the cluster.
    pub vpc_id: Option<String>,

    /// A usable security group id to assign to a vpc endpoint
    pub security_group_id: Option<String>,

    /// Default tag for the endpoint created when creating a privatelink connection.
    /// Will be appended to the tags specified in the `tags` field in with clause in `create
    /// connection`.
    pub privatelink_endpoint_default_tags: Option<Vec<(String, String)>>,

    /// Schedule `space_reclaim_compaction` for all compaction groups with this interval.
    pub periodic_space_reclaim_compaction_interval_sec: u64,

    /// telemetry enabled in config file or not
    pub telemetry_enabled: bool,
    /// Schedule `ttl_reclaim_compaction` for all compaction groups with this interval.
    pub periodic_ttl_reclaim_compaction_interval_sec: u64,

    /// Schedule `tombstone_reclaim_compaction` for all compaction groups with this interval.
    pub periodic_tombstone_reclaim_compaction_interval_sec: u64,

    /// Schedule `split_compaction_group` for all compaction groups with this interval.
    pub periodic_split_compact_group_interval_sec: u64,

    /// The size limit to split a large compaction group.
    pub split_group_size_limit: u64,
    /// The size limit to move a state-table to other group.
    pub min_table_split_size: u64,

    /// Whether config object storage bucket lifecycle to purge stale data.
    pub do_not_config_object_storage_lifecycle: bool,

    pub partition_vnode_count: u32,

    /// threshold of high write throughput of state-table, unit: B/sec
    pub table_write_throughput_threshold: u64,
    /// threshold of low write throughput of state-table, unit: B/sec
    pub min_table_split_write_throughput: u64,

    pub compaction_task_max_heartbeat_interval_secs: u64,
    pub compaction_task_max_progress_interval_secs: u64,
    pub compaction_config: Option<CompactionConfig>,

    /// The size limit to split a state-table to independent sstable.
    pub cut_table_size_limit: u64,

    /// hybird compaction group config
    ///
    /// `hybird_partition_vnode_count` determines the granularity of vnodes in the hybrid compaction group for SST alignment.
    /// When `hybird_partition_vnode_count` > 0, in hybrid compaction group
    /// - Tables with high write throughput will be split at vnode granularity
    /// - Tables with high size tables will be split by table granularity
    /// When `hybird_partition_vnode_count` = 0,no longer be special alignment operations for the hybird compaction group
    pub hybird_partition_vnode_count: u32,

    pub event_log_enabled: bool,
    pub event_log_channel_max_size: u32,
    pub advertise_addr: String,
    /// The number of traces to be cached in-memory by the tracing collector
    /// embedded in the meta node.
    pub cached_traces_num: u32,
    /// The maximum memory usage in bytes for the tracing collector embedded
    /// in the meta node.
    pub cached_traces_memory_limit_bytes: usize,

    /// l0 picker whether to select trivial move task
    pub enable_trivial_move: bool,

    /// l0 multi level picker whether to check the overlap accuracy between sub levels
    pub enable_check_task_level_overlap: bool,
    pub enable_dropped_column_reclaim: bool,
    pub object_store_config: ObjectStoreConfig,

    /// The maximum number of trivial move tasks to be picked in a single loop
    pub max_trivial_move_task_count_per_loop: usize,

    /// The maximum number of times to probe for `PullTaskEvent`
    pub max_get_task_probe_times: usize,
}

impl MetaOpts {
    /// Default opts for testing. Some tests need `enable_recovery=true`
    pub fn test(enable_recovery: bool) -> Self {
        Self {
            enable_recovery,
            disable_automatic_parallelism_control: false,
            parallelism_control_batch_size: 1,
            parallelism_control_trigger_period_sec: 10,
            parallelism_control_trigger_first_delay_sec: 30,
            in_flight_barrier_nums: 40,
            max_idle_ms: 0,
            compaction_deterministic_test: false,
            default_parallelism: DefaultParallelism::Full,
            vacuum_interval_sec: 30,
            vacuum_spin_interval_ms: 0,
            hummock_version_checkpoint_interval_sec: 30,
            enable_hummock_data_archive: false,
            min_delta_log_num_for_hummock_version_checkpoint: 1,
            min_sst_retention_time_sec: 3600 * 24 * 7,
            full_gc_interval_sec: 3600 * 24 * 7,
            collect_gc_watermark_spin_interval_sec: 5,
            enable_committed_sst_sanity_check: false,
            periodic_compaction_interval_sec: 60,
            node_num_monitor_interval_sec: 10,
            prometheus_endpoint: None,
            prometheus_selector: None,
            vpc_id: None,
            security_group_id: None,
            privatelink_endpoint_default_tags: None,
            periodic_space_reclaim_compaction_interval_sec: 60,
            telemetry_enabled: false,
            periodic_ttl_reclaim_compaction_interval_sec: 60,
            periodic_tombstone_reclaim_compaction_interval_sec: 60,
            periodic_split_compact_group_interval_sec: 60,
            split_group_size_limit: 5 * 1024 * 1024 * 1024,
            min_table_split_size: 2 * 1024 * 1024 * 1024,
            table_write_throughput_threshold: 128 * 1024 * 1024,
            min_table_split_write_throughput: 64 * 1024 * 1024,
            do_not_config_object_storage_lifecycle: true,
            partition_vnode_count: 32,
            compaction_task_max_heartbeat_interval_secs: 0,
            compaction_task_max_progress_interval_secs: 1,
            compaction_config: None,
            cut_table_size_limit: 1024 * 1024 * 1024,
            hybird_partition_vnode_count: 4,
            event_log_enabled: false,
            event_log_channel_max_size: 1,
            advertise_addr: "".to_string(),
            cached_traces_num: 1,
            cached_traces_memory_limit_bytes: usize::MAX,
            enable_trivial_move: true,
            enable_check_task_level_overlap: true,
            enable_dropped_column_reclaim: false,
            object_store_config: ObjectStoreConfig::default(),
            max_trivial_move_task_count_per_loop: 256,
            max_get_task_probe_times: 5,
        }
    }
}

impl MetaSrvEnv {
    pub async fn new(
        opts: MetaOpts,
        init_system_params: SystemParams,
        init_session_config: SessionConfig,
        meta_store_impl: MetaStoreImpl,
    ) -> MetaResult<Self> {
        let notification_manager =
            Arc::new(NotificationManager::new(meta_store_impl.clone()).await);
        let idle_manager = Arc::new(IdleManager::new(opts.max_idle_ms));
        let stream_client_pool = Arc::new(StreamClientPool::default());
        let event_log_manager = Arc::new(start_event_log_manager(
            opts.event_log_enabled,
            opts.event_log_channel_max_size,
        ));

        let env = match &meta_store_impl {
            MetaStoreImpl::Kv(meta_store) => {
                let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
                let (cluster_id, cluster_first_launch) =
                    if let Some(id) = ClusterId::from_meta_store(meta_store).await? {
                        (id, false)
                    } else {
                        (ClusterId::new(), true)
                    };
                let system_params_manager = Arc::new(
                    SystemParamsManager::new(
                        meta_store.clone(),
                        notification_manager.clone(),
                        init_system_params.clone(),
                        cluster_first_launch,
                    )
                    .await?,
                );
                let session_params_manager = Arc::new(
                    SessionParamsManager::new(
                        meta_store.clone(),
                        init_session_config.clone(),
                        notification_manager.clone(),
                        cluster_first_launch,
                    )
                    .await?,
                );

                // Persist params before starting services so that invalid params that cause meta node
                // to crash will not be persisted.
                system_params_manager.flush_params().await?;
                session_params_manager.flush_params().await?;
                cluster_id.put_at_meta_store(meta_store).await?;

                Self {
                    id_gen_manager_impl: IdGenManagerImpl::Kv(id_gen_manager),
                    system_param_manager_impl: SystemParamsManagerImpl::Kv(system_params_manager),
                    session_param_manager_impl: SessionParamsManagerImpl::Kv(
                        session_params_manager,
                    ),
                    meta_store_impl: meta_store_impl.clone(),
                    notification_manager,
                    stream_client_pool,
                    idle_manager,
                    event_log_manager,
                    cluster_id,
                    hummock_seq: None,
                    opts: opts.into(),
                }
            }
            MetaStoreImpl::Sql(sql_meta_store) => {
                let cluster_id = Cluster::find()
                    .one(&sql_meta_store.conn)
                    .await?
                    .map(|c| c.cluster_id.to_string().into())
                    .unwrap();
                let system_param_controller = Arc::new(
                    SystemParamsController::new(
                        sql_meta_store.clone(),
                        notification_manager.clone(),
                        init_system_params,
                    )
                    .await?,
                );
                let session_param_controller = Arc::new(
                    SessionParamsController::new(
                        sql_meta_store.clone(),
                        notification_manager.clone(),
                        init_session_config,
                    )
                    .await?,
                );
                Self {
                    id_gen_manager_impl: IdGenManagerImpl::Sql(Arc::new(
                        SqlIdGeneratorManager::new(&sql_meta_store.conn).await?,
                    )),
                    system_param_manager_impl: SystemParamsManagerImpl::Sql(
                        system_param_controller,
                    ),
                    session_param_manager_impl: SessionParamsManagerImpl::Sql(
                        session_param_controller,
                    ),
                    meta_store_impl: meta_store_impl.clone(),
                    notification_manager,
                    stream_client_pool,
                    idle_manager,
                    event_log_manager,
                    cluster_id,
                    hummock_seq: Some(Arc::new(SequenceGenerator::new(
                        sql_meta_store.conn.clone(),
                    ))),
                    opts: opts.into(),
                }
            }
        };
        Ok(env)
    }

    pub fn meta_store_ref(&self) -> MetaStoreImpl {
        self.meta_store_impl.clone()
    }

    pub fn meta_store(&self) -> &MetaStoreImpl {
        &self.meta_store_impl
    }

    pub fn id_gen_manager(&self) -> &IdGenManagerImpl {
        &self.id_gen_manager_impl
    }

    pub fn notification_manager_ref(&self) -> NotificationManagerRef {
        self.notification_manager.clone()
    }

    pub fn notification_manager(&self) -> &NotificationManager {
        self.notification_manager.deref()
    }

    pub fn idle_manager_ref(&self) -> IdleManagerRef {
        self.idle_manager.clone()
    }

    pub fn idle_manager(&self) -> &IdleManager {
        self.idle_manager.deref()
    }

    pub async fn system_params_reader(&self) -> SystemParamsReader {
        match &self.system_param_manager_impl {
            SystemParamsManagerImpl::Kv(mgr) => mgr.get_params().await,
            SystemParamsManagerImpl::Sql(mgr) => mgr.get_params().await,
        }
    }

    pub fn system_params_manager_impl_ref(&self) -> SystemParamsManagerImpl {
        self.system_param_manager_impl.clone()
    }

    pub fn session_params_manager_impl_ref(&self) -> SessionParamsManagerImpl {
        self.session_param_manager_impl.clone()
    }

    pub fn stream_client_pool_ref(&self) -> StreamClientPoolRef {
        self.stream_client_pool.clone()
    }

    pub fn stream_client_pool(&self) -> &StreamClientPool {
        self.stream_client_pool.deref()
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn event_log_manager_ref(&self) -> EventLogMangerRef {
        self.event_log_manager.clone()
    }
}

#[cfg(any(test, feature = "test"))]
impl MetaSrvEnv {
    // Instance for test.
    pub async fn for_test() -> Self {
        Self::for_test_opts(MetaOpts::test(false)).await
    }

    // Instance for test with sql meta store.
    #[cfg(not(madsim))]
    pub async fn for_test_with_sql_meta_store() -> Self {
        Self::new(
            MetaOpts::test(false),
            risingwave_common::system_param::system_params_for_test(),
            Default::default(),
            MetaStoreImpl::Sql(SqlMetaStore::for_test().await),
        )
        .await
        .unwrap()
    }

    pub async fn for_test_opts(opts: MetaOpts) -> Self {
        use crate::storage::{MemStore, MetaStoreBoxExt};

        Self::new(
            opts,
            risingwave_common::system_param::system_params_for_test(),
            Default::default(),
            MetaStoreImpl::Kv(MemStore::default().into_ref()),
        )
        .await
        .unwrap()
    }
}
