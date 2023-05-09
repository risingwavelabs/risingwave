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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use prometheus::IntGauge;
use risingwave_common::catalog::SysCatalogReaderRef;
use risingwave_common::config::BatchConfig;
use risingwave_common::error::Result;
use risingwave_common::memory::MemoryContext;
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_rpc_client::ComputeClientPoolRef;
use risingwave_source::dml_manager::DmlManagerRef;
use risingwave_storage::StateStoreImpl;

use super::TaskId;
use crate::monitor::{BatchMetricsWithTaskLabels, BatchMetricsWithTaskLabelsInner};
use crate::task::{BatchEnvironment, TaskOutput, TaskOutputId};

/// Context for batch task execution.
///
/// This context is specific to one task execution, and should *not* be shared by different tasks.
pub trait BatchTaskContext: Clone + Send + Sync + 'static {
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
    fn batch_metrics(&self) -> Option<BatchMetricsWithTaskLabels>;

    /// Get compute client pool. This is used in grpc exchange to avoid creating new compute client
    /// for each grpc call.
    fn client_pool(&self) -> ComputeClientPoolRef;

    /// Get config for batch environment
    fn get_config(&self) -> &BatchConfig;

    fn source_metrics(&self) -> Arc<SourceMetrics>;

    fn store_mem_usage(&self, val: usize);

    fn mem_usage(&self) -> usize;

    fn create_executor_mem_context(&self, executor_id: &str) -> MemoryContext;
}

/// Batch task context on compute node.
#[derive(Clone)]
pub struct ComputeNodeContext {
    env: BatchEnvironment,
    // None: Local mode don't record metrics.
    batch_metrics: Option<BatchMetricsWithTaskLabels>,

    mem_context: MemoryContext,

    // Last mem usage value. Init to be 0. Should be the last value of `cur_mem_val`.
    last_mem_val: Arc<AtomicUsize>,
    // How many memory bytes have been used in this task for the latest report value. Will be moved
    // to `last_mem_val` if new value comes in.
    cur_mem_val: Arc<AtomicUsize>,
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

    fn batch_metrics(&self) -> Option<BatchMetricsWithTaskLabels> {
        self.batch_metrics.clone()
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

    fn store_mem_usage(&self, val: usize) {
        // Record the last mem val.
        // Calculate the difference between old val and new value, and apply the diff to total
        // memory usage value.
        let old_value = self.cur_mem_val.load(Ordering::Relaxed);
        self.last_mem_val.store(old_value, Ordering::Relaxed);
        let diff = val as i64 - old_value as i64;
        self.env.task_manager().apply_mem_diff(diff);

        self.cur_mem_val.store(val, Ordering::Relaxed);
    }

    fn mem_usage(&self) -> usize {
        self.cur_mem_val.load(Ordering::Relaxed)
    }

    fn create_executor_mem_context(&self, executor_id: &str) -> MemoryContext {
        if let Some(metrics) = &self.batch_metrics {
            let mut labels = metrics.task_labels();
            labels.push(executor_id);
            let executor_mem_usage =
                metrics.create_collector_for_mem_usage(vec![executor_id.to_string()]);
            MemoryContext::new(Some(self.mem_context.clone()), executor_mem_usage)
        } else {
            MemoryContext::default()
        }
    }
}

impl ComputeNodeContext {
    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            env: BatchEnvironment::for_test(),
            batch_metrics: None,
            cur_mem_val: Arc::new(0.into()),
            last_mem_val: Arc::new(0.into()),
            mem_context: MemoryContext::default(),
        }
    }

    pub fn new(env: BatchEnvironment, task_id: TaskId) -> Self {
        let batch_mem_context = env.task_manager().memory_context_ref();
        let batch_metrics = Arc::new(BatchMetricsWithTaskLabelsInner::new(
            env.task_metrics(),
            env.executor_metrics(),
            task_id,
        ));
        let mem_context = MemoryContext::new(
            Some(batch_mem_context),
            batch_metrics
                .get_task_metrics()
                .task_mem_usage
                .with_label_values(&batch_metrics.task_labels()),
        );
        Self {
            env,
            batch_metrics: Some(batch_metrics),
            cur_mem_val: Arc::new(0.into()),
            last_mem_val: Arc::new(0.into()),
            mem_context,
        }
    }

    pub fn new_for_local(env: BatchEnvironment) -> Self {
        Self {
            env,
            batch_metrics: None,
            cur_mem_val: Arc::new(0.into()),
            last_mem_val: Arc::new(0.into()),
            // Leave it for now, it should be None
            mem_context: MemoryContext::new(None, IntGauge::new("test", "test").unwrap()),
        }
    }

    pub fn mem_usage(&self) -> usize {
        self.cur_mem_val.load(Ordering::Relaxed)
    }
}
