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

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

#[cfg(any(test, feature = "test"))]
use prost::Message;
use risingwave_pb::meta::MetaLeaderInfo;
#[cfg(any(test, feature = "test"))]
use risingwave_pb::meta::MetaLeaseInfo;
use risingwave_rpc_client::{StreamClientPool, StreamClientPoolRef};

use crate::manager::{
    IdGeneratorManager, IdGeneratorManagerRef, IdleManager, IdleManagerRef, NotificationManager,
    NotificationManagerRef,
};
#[cfg(any(test, feature = "test"))]
use crate::rpc::{META_CF_NAME, META_LEADER_KEY, META_LEASE_KEY};
#[cfg(any(test, feature = "test"))]
use crate::storage::MemStore;
use crate::storage::MetaStore;

/// [`MetaSrvEnv`] is the global environment in Meta service. The instance will be shared by all
/// kind of managers inside Meta.
#[derive(Clone)]
pub struct MetaSrvEnv<S>
where
    S: MetaStore,
{
    /// id generator manager.
    id_gen_manager: IdGeneratorManagerRef<S>,

    /// meta store.
    meta_store: Arc<S>,

    /// notification manager.
    notification_manager: NotificationManagerRef,

    /// stream client pool memorization.
    stream_client_pool: StreamClientPoolRef,

    /// idle status manager.
    idle_manager: IdleManagerRef,

    info: MetaLeaderInfo,

    /// options read by all services
    pub opts: Arc<MetaOpts>,
}

/// Options shared by all meta service instances
#[derive(Clone)]
pub struct MetaOpts {
    /// Whether to enable the recovery of the cluster. If disabled, the meta service will exit on
    /// abnormal cases.
    pub enable_recovery: bool,
    /// The interval of periodic barrier.
    pub barrier_interval: Duration,
    /// The maximum number of barriers in-flight in the compute nodes.
    pub in_flight_barrier_nums: usize,
    /// Whether to enable the minimal scheduling strategy, that is, only schedule the streaming
    /// fragment on one parallel unit per compute node.
    pub minimal_scheduling: bool,

    /// After specified seconds of idle (no mview or flush), the process will be exited.
    /// 0 for infinite, process will never be exited due to long idle time.
    pub max_idle_ms: u64,

    pub checkpoint_frequency: usize,

    /// Interval of GC metadata in meta store and stale SSTs in object store.
    pub vacuum_interval_sec: u64,
    /// Threshold used by worker node to filter out new SSTs when scanning object store.
    pub min_sst_retention_time_sec: u64,
    /// The spin interval when collecting global GC watermark in hummock
    pub collect_gc_watermark_spin_interval_sec: u64,
    /// Enable sanity check when SSTs are committed
    pub enable_committed_sst_sanity_check: bool,
    /// Schedule compaction for all compaction groups with this interval.
    pub periodic_compaction_interval_sec: u64,
    /// Interval of reporting the number of nodes in the cluster.
    pub node_num_monitor_interval_sec: u64,
    /// Seconds compaction scheduler should stall when there is no available compactor.
    pub no_available_compactor_stall_sec: u64,
}

impl Default for MetaOpts {
    fn default() -> Self {
        Self {
            enable_recovery: false,
            barrier_interval: Duration::from_millis(250),
            in_flight_barrier_nums: 40,
            minimal_scheduling: false,
            max_idle_ms: 0,
            checkpoint_frequency: 10,
            vacuum_interval_sec: 30,
            min_sst_retention_time_sec: 3600 * 24 * 7,
            collect_gc_watermark_spin_interval_sec: 5,
            enable_committed_sst_sanity_check: false,
            periodic_compaction_interval_sec: 60,
            node_num_monitor_interval_sec: 10,
            no_available_compactor_stall_sec: 5,
        }
    }
}

impl MetaOpts {
    /// some test need `enable_recovery=true`
    #[cfg(test)]
    pub fn test(enable_recovery: bool) -> Self {
        Self {
            enable_recovery,
            barrier_interval: Duration::from_millis(250),
            max_idle_ms: 0,
            in_flight_barrier_nums: 40,
            checkpoint_frequency: 10,
            ..Default::default()
        }
    }
}

impl<S> MetaSrvEnv<S>
where
    S: MetaStore,
{
    pub async fn new(opts: MetaOpts, meta_store: Arc<S>, info: MetaLeaderInfo) -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let stream_client_pool = Arc::new(StreamClientPool::default());
        let notification_manager = Arc::new(NotificationManager::new());
        let idle_manager = Arc::new(IdleManager::new(opts.max_idle_ms));

        Self {
            id_gen_manager,
            meta_store,
            notification_manager,
            stream_client_pool,
            idle_manager,
            info,
            opts: opts.into(),
        }
    }

    pub fn meta_store_ref(&self) -> Arc<S> {
        self.meta_store.clone()
    }

    pub fn meta_store(&self) -> &S {
        self.meta_store.deref()
    }

    pub fn id_gen_manager_ref(&self) -> IdGeneratorManagerRef<S> {
        self.id_gen_manager.clone()
    }

    pub fn id_gen_manager(&self) -> &IdGeneratorManager<S> {
        self.id_gen_manager.deref()
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

    pub fn stream_client_pool_ref(&self) -> StreamClientPoolRef {
        self.stream_client_pool.clone()
    }

    pub fn stream_client_pool(&self) -> &StreamClientPool {
        self.stream_client_pool.deref()
    }

    pub fn get_leader_info(&self) -> MetaLeaderInfo {
        self.info.clone()
    }
}

#[cfg(any(test, feature = "test"))]
impl MetaSrvEnv<MemStore> {
    // Instance for test.
    pub async fn for_test() -> Self {
        Self::for_test_opts(MetaOpts::default().into()).await
    }

    pub async fn for_test_opts(opts: Arc<MetaOpts>) -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let leader_info = MetaLeaderInfo {
            lease_id: 0,
            node_address: "".to_string(),
        };
        let lease_info = MetaLeaseInfo {
            leader: Some(leader_info.clone()),
            lease_register_time: 0,
            lease_expire_time: 10,
        };
        let meta_store = Arc::new(MemStore::default());
        meta_store
            .put_cf(
                META_CF_NAME,
                META_LEADER_KEY.as_bytes().to_vec(),
                leader_info.encode_to_vec(),
            )
            .await
            .unwrap();
        meta_store
            .put_cf(
                META_CF_NAME,
                META_LEASE_KEY.as_bytes().to_vec(),
                lease_info.encode_to_vec(),
            )
            .await
            .unwrap();
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let notification_manager = Arc::new(NotificationManager::new());
        let stream_client_pool = Arc::new(StreamClientPool::default());
        let idle_manager = Arc::new(IdleManager::disabled());

        Self {
            id_gen_manager,
            meta_store,
            notification_manager,
            stream_client_pool,
            idle_manager,
            info: leader_info,
            opts,
        }
    }
}
