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

use prometheus::core::Atomic;
use risingwave_common::catalog::SysCatalogReaderRef;
use risingwave_common::config::BatchConfig;
use risingwave_common::memory::MemoryContext;
use risingwave_common::metrics::TrAdderAtomic;
use risingwave_common::util::addr::{HostAddr, is_local_address};
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_dml::dml_manager::DmlManagerRef;
use risingwave_rpc_client::ComputeClientPoolRef;
use risingwave_storage::StateStoreImpl;

use crate::error::Result;
use crate::monitor::{BatchMetrics, BatchMetricsInner, BatchSpillMetrics};
use crate::task::{BatchEnvironment, TaskOutput, TaskOutputId};
use crate::worker_manager::worker_node_manager::WorkerNodeManagerRef;

/// Context for batch task execution.
///
/// This context is specific to one task execution, and should *not* be shared by different tasks.
pub trait BatchTaskContext: Send + Sync + 'static {
    /// Get task output identified by `task_output_id`.
    ///
    /// Returns error if the task of `task_output_id` doesn't run in same worker as current task.
    fn get_task_output(&self, task_output_id: TaskOutputId) -> Result<TaskOutput>;

    /// Get system catalog reader, used to read system table.
    fn catalog_reader(&self) -> SysCatalogReaderRef;

    /// Whether `peer_addr` is in same as current task.
    fn is_local_addr(&self, peer_addr: &HostAddr) -> bool;

    fn dml_manager(&self) -> DmlManagerRef;

    fn state_store(&self) -> StateStoreImpl;

    /// Get batch metrics.
    /// None indicates that not collect task metrics.
    fn batch_metrics(&self) -> Option<BatchMetrics>;

    fn spill_metrics(&self) -> Arc<BatchSpillMetrics>;

    /// Get compute client pool. This is used in grpc exchange to avoid creating new compute client
    /// for each grpc call.
    fn client_pool(&self) -> ComputeClientPoolRef;

    /// Get config for batch environment
    fn get_config(&self) -> &BatchConfig;

    fn source_metrics(&self) -> Arc<SourceMetrics>;

    fn create_executor_mem_context(&self, executor_id: &str) -> MemoryContext;

    fn worker_node_manager(&self) -> Option<WorkerNodeManagerRef>;
}

/// Batch task context on compute node.
#[derive(Clone)]
pub struct ComputeNodeContext {
    env: BatchEnvironment,

    batch_metrics: BatchMetrics,

    mem_context: MemoryContext,
}

impl BatchTaskContext for ComputeNodeContext {
    fn get_task_output(&self, task_output_id: TaskOutputId) -> Result<TaskOutput> {
        self.env
            .task_manager()
            .take_output(&task_output_id.to_prost())
    }

    fn catalog_reader(&self) -> SysCatalogReaderRef {
        unimplemented!("not supported in distributed mode")
    }

    fn is_local_addr(&self, peer_addr: &HostAddr) -> bool {
        is_local_address(self.env.server_address(), peer_addr)
    }

    fn dml_manager(&self) -> DmlManagerRef {
        self.env.dml_manager_ref()
    }

    fn state_store(&self) -> StateStoreImpl {
        self.env.state_store()
    }

    fn batch_metrics(&self) -> Option<BatchMetrics> {
        Some(self.batch_metrics.clone())
    }

    fn spill_metrics(&self) -> Arc<BatchSpillMetrics> {
        self.env.spill_metrics()
    }

    fn client_pool(&self) -> ComputeClientPoolRef {
        self.env.client_pool()
    }

    fn get_config(&self) -> &BatchConfig {
        self.env.config()
    }

    fn source_metrics(&self) -> Arc<SourceMetrics> {
        self.env.source_metrics()
    }

    fn create_executor_mem_context(&self, _executor_id: &str) -> MemoryContext {
        let counter = TrAdderAtomic::new(0);
        MemoryContext::new(Some(self.mem_context.clone()), counter)
    }

    fn worker_node_manager(&self) -> Option<WorkerNodeManagerRef> {
        None
    }
}

impl ComputeNodeContext {
    pub fn for_test() -> Arc<dyn BatchTaskContext> {
        Arc::new(Self {
            env: BatchEnvironment::for_test(),
            batch_metrics: BatchMetricsInner::for_test(),
            mem_context: MemoryContext::none(),
        })
    }

    pub fn create(env: BatchEnvironment) -> Arc<dyn BatchTaskContext> {
        let mem_context = env.task_manager().memory_context_ref();
        let batch_metrics = Arc::new(BatchMetricsInner::new(
            env.task_manager().metrics(),
            env.executor_metrics(),
            env.iceberg_scan_metrics(),
        ));
        Arc::new(Self {
            env,
            batch_metrics,
            mem_context,
        })
    }
}
