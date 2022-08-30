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

use std::collections::HashMap;
use std::sync::Arc;

use prometheus::core::Collector;
use risingwave_common::catalog::SysCatalogReaderRef;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_rpc_client::ComputeClientPoolRef;
use risingwave_source::SourceManagerRef;
use risingwave_storage::StateStoreImpl;

use super::TaskId;
use crate::executor::{BatchMetrics, BatchTaskMetrics};
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
    fn catalog_reader_ref(&self) -> Option<SysCatalogReaderRef>;

    fn try_get_catalog_reader_ref(&self) -> Result<SysCatalogReaderRef> {
        Ok(self
            .catalog_reader_ref()
            .ok_or_else(|| InternalError("Sys catalog reader not found".to_string()))?)
    }

    /// Whether `peer_addr` is in same as current task.
    fn is_local_addr(&self, peer_addr: &HostAddr) -> bool;

    fn source_manager_ref(&self) -> Option<SourceManagerRef>;

    fn try_get_source_manager_ref(&self) -> Result<SourceManagerRef> {
        Ok(self
            .source_manager_ref()
            .ok_or_else(|| InternalError("Source manager not found".to_string()))?)
    }

    fn state_store(&self) -> Option<StateStoreImpl>;

    fn try_get_state_store(&self) -> Result<StateStoreImpl> {
        Ok(self
            .state_store()
            .ok_or_else(|| InternalError("State store not found".to_string()))?)
    }

    /// None indicates that not collect batch metrics.
    fn stats(&self) -> Option<Arc<BatchMetrics>>;

    /// get task level metrics.
    /// None indicates that not collect task metrics.
    fn get_task_metrics(&self) -> Option<BatchTaskMetrics>;

    // Each task execution has its own label:
    // QueryID, StageId, TaskId
    fn task_labels(&self) -> HashMap<String, String>;

    fn register(&self, c: Box<dyn Collector>) -> Result<()>;

    fn unregister(&self, c: Box<dyn Collector>);

    /// Get compute client pool. This is used in grpc exchange to avoid creating new compute client
    /// for each grpc call.
    fn client_pool(&self) -> ComputeClientPoolRef;
}

/// Batch task context on compute node.
#[derive(Clone)]
pub struct ComputeNodeContext {
    id: TaskId,
    env: BatchEnvironment,
    task_metrics: BatchTaskMetrics,
}

impl BatchTaskContext for ComputeNodeContext {
    fn get_task_output(&self, task_output_id: TaskOutputId) -> Result<TaskOutput> {
        self.env
            .task_manager()
            .take_output(&task_output_id.to_prost())
    }

    fn catalog_reader_ref(&self) -> Option<SysCatalogReaderRef> {
        None
    }

    fn is_local_addr(&self, peer_addr: &HostAddr) -> bool {
        is_local_address(self.env.server_address(), peer_addr)
    }

    fn source_manager_ref(&self) -> Option<SourceManagerRef> {
        Some(self.env.source_manager_ref())
    }

    fn state_store(&self) -> Option<StateStoreImpl> {
        Some(self.env.state_store())
    }

    fn stats(&self) -> Option<Arc<BatchMetrics>> {
        Some(self.env.stats())
    }

    fn get_task_metrics(&self) -> Option<BatchTaskMetrics> {
        Some(self.task_metrics.clone())
    }

    fn client_pool(&self) -> ComputeClientPoolRef {
        self.env.client_pool()
    }

    fn task_labels(&self) -> HashMap<String, String> {
        HashMap::from([
            ("query_id".to_string(), self.id.query_id.clone()),
            ("stage_id".to_string(), self.id.stage_id.to_string()),
            ("task_id".to_string(), self.id.task_id.to_string()),
        ])
    }

    fn register(&self, c: Box<dyn Collector>) -> Result<()> {
        self.task_metrics.register(c)?;
        Ok(())
    }

    fn unregister(&self, c: Box<dyn Collector>) {
        self.task_metrics.unregister(c)
    }
}

impl ComputeNodeContext {
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        Self {
            env: BatchEnvironment::for_test(),
            task_metrics: BatchTaskMetrics::for_test(),
            id: TaskId::default(),
        }
    }

    pub fn new(env: BatchEnvironment, task_id: TaskId) -> Self {
        let task_metrics = env.create_task_metrics(task_id.clone());
        Self {
            env,
            task_metrics,
            id: task_id,
        }
    }
}
