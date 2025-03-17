// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use hytra::TrAdder;
use risingwave_common::config::StreamingConfig;
use risingwave_common::system_param::local_manager::LocalSystemParamsManagerRef;
use risingwave_common::util::addr::HostAddr;
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_dml::dml_manager::DmlManagerRef;
use risingwave_rpc_client::{ComputeClientPoolRef, MetaClient};
use risingwave_storage::StateStoreImpl;

pub(crate) type WorkerNodeId = u32;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone, Debug)]
pub struct StreamEnvironment {
    /// Endpoint the stream manager listens on.
    server_addr: HostAddr,

    /// Streaming related configurations.
    config: Arc<StreamingConfig>,

    /// Current worker node id.
    worker_id: WorkerNodeId,

    /// State store for table scanning.
    state_store: StateStoreImpl,

    /// Manages dml information.
    dml_manager: DmlManagerRef,

    /// Read the latest system parameters.
    system_params_manager: LocalSystemParamsManagerRef,

    /// Metrics for source.
    source_metrics: Arc<SourceMetrics>,

    /// Total memory usage in stream.
    total_mem_val: Arc<TrAdder<i64>>,

    /// Meta client. Use `None` for test only
    meta_client: Option<MetaClient>,

    /// Compute client pool for streaming gRPC exchange.
    client_pool: ComputeClientPoolRef,
}

impl StreamEnvironment {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        server_addr: HostAddr,
        config: Arc<StreamingConfig>,
        worker_id: WorkerNodeId,
        state_store: StateStoreImpl,
        dml_manager: DmlManagerRef,
        system_params_manager: LocalSystemParamsManagerRef,
        source_metrics: Arc<SourceMetrics>,
        meta_client: MetaClient,
        client_pool: ComputeClientPoolRef,
    ) -> Self {
        StreamEnvironment {
            server_addr,
            config,
            worker_id,
            state_store,
            dml_manager,
            system_params_manager,
            source_metrics,
            total_mem_val: Arc::new(TrAdder::new()),
            meta_client: Some(meta_client),
            client_pool,
        }
    }

    // Create an instance for testing purpose.
    #[cfg(test)]
    pub fn for_test() -> Self {
        use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
        use risingwave_dml::dml_manager::DmlManager;
        use risingwave_rpc_client::ComputeClientPool;
        use risingwave_storage::monitor::MonitoredStorageMetrics;
        StreamEnvironment {
            server_addr: "127.0.0.1:2333".parse().unwrap(),
            config: Arc::new(StreamingConfig::default()),
            worker_id: WorkerNodeId::default(),
            state_store: StateStoreImpl::shared_in_memory_store(Arc::new(
                MonitoredStorageMetrics::unused(),
            )),
            dml_manager: Arc::new(DmlManager::for_test()),
            system_params_manager: Arc::new(LocalSystemParamsManager::for_test()),
            source_metrics: Arc::new(SourceMetrics::default()),
            total_mem_val: Arc::new(TrAdder::new()),
            meta_client: None,
            client_pool: Arc::new(ComputeClientPool::for_test()),
        }
    }

    pub fn server_address(&self) -> &HostAddr {
        &self.server_addr
    }

    pub fn config(&self) -> &Arc<StreamingConfig> {
        &self.config
    }

    pub fn worker_id(&self) -> WorkerNodeId {
        self.worker_id
    }

    pub fn state_store(&self) -> StateStoreImpl {
        self.state_store.clone()
    }

    pub fn dml_manager_ref(&self) -> DmlManagerRef {
        self.dml_manager.clone()
    }

    pub fn system_params_manager_ref(&self) -> LocalSystemParamsManagerRef {
        self.system_params_manager.clone()
    }

    pub fn source_metrics(&self) -> Arc<SourceMetrics> {
        self.source_metrics.clone()
    }

    pub fn total_mem_usage(&self) -> Arc<TrAdder<i64>> {
        self.total_mem_val.clone()
    }

    pub fn meta_client(&self) -> Option<MetaClient> {
        self.meta_client.clone()
    }

    pub fn client_pool(&self) -> ComputeClientPoolRef {
        self.client_pool.clone()
    }
}
