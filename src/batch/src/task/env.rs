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

use std::sync::Arc;

use risingwave_common::config::BatchConfig;
use risingwave_common::util::addr::HostAddr;
use risingwave_rpc_client::ComputeClientPoolRef;
use risingwave_source::{TableSourceManager, TableSourceManagerRef};
use risingwave_storage::StateStoreImpl;

use crate::executor::BatchTaskMetrics;
use crate::task::BatchManager;

pub(crate) type WorkerNodeId = u32;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct BatchEnvironment {
    /// Endpoint the batch task manager listens on.
    server_addr: HostAddr,

    /// Reference to the task manager.
    task_manager: Arc<BatchManager>,

    /// Reference to the source manager. This is used to query the sources.
    source_manager: TableSourceManagerRef,

    /// Batch related configurations.
    config: Arc<BatchConfig>,

    /// Current worker node id.
    worker_id: WorkerNodeId,

    /// State store for table scanning.
    state_store: StateStoreImpl,

    /// Task level metrics.
    task_metrics: Arc<BatchTaskMetrics>,

    /// Compute client pool for grpc exchange.
    client_pool: ComputeClientPoolRef,
}

impl BatchEnvironment {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_manager: TableSourceManagerRef,
        task_manager: Arc<BatchManager>,
        server_addr: HostAddr,
        config: Arc<BatchConfig>,
        worker_id: WorkerNodeId,
        state_store: StateStoreImpl,
        task_metrics: Arc<BatchTaskMetrics>,
        client_pool: ComputeClientPoolRef,
    ) -> Self {
        BatchEnvironment {
            server_addr,
            task_manager,
            source_manager,
            config,
            worker_id,
            state_store,
            task_metrics,
            client_pool,
        }
    }

    // Create an instance for testing purpose.
    #[cfg(test)]
    pub fn for_test() -> Self {
        use risingwave_rpc_client::ComputeClientPool;
        use risingwave_storage::monitor::StateStoreMetrics;

        BatchEnvironment {
            task_manager: Arc::new(BatchManager::new(None)),
            server_addr: "127.0.0.1:5688".parse().unwrap(),
            source_manager: std::sync::Arc::new(TableSourceManager::default()),
            config: Arc::new(BatchConfig::default()),
            worker_id: WorkerNodeId::default(),
            state_store: StateStoreImpl::shared_in_memory_store(Arc::new(
                StateStoreMetrics::unused(),
            )),
            task_metrics: Arc::new(BatchTaskMetrics::for_test()),
            client_pool: Arc::new(ComputeClientPool::default()),
        }
    }

    pub fn server_address(&self) -> &HostAddr {
        &self.server_addr
    }

    pub fn task_manager(&self) -> Arc<BatchManager> {
        self.task_manager.clone()
    }

    pub fn source_manager(&self) -> &TableSourceManager {
        &*self.source_manager
    }

    pub fn source_manager_ref(&self) -> TableSourceManagerRef {
        self.source_manager.clone()
    }

    pub fn config(&self) -> &BatchConfig {
        self.config.as_ref()
    }

    pub fn worker_id(&self) -> WorkerNodeId {
        self.worker_id
    }

    pub fn state_store(&self) -> StateStoreImpl {
        self.state_store.clone()
    }

    pub fn task_metrics(&self) -> Arc<BatchTaskMetrics> {
        self.task_metrics.clone()
    }

    pub fn client_pool(&self) -> ComputeClientPoolRef {
        self.client_pool.clone()
    }
}
