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

use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_source::SourceManagerRef;
use risingwave_storage::StateStoreImpl;

use crate::executor::BatchMetrics;
use crate::task::{BatchEnvironment, TaskId, TaskOutput, TaskOutputId};

/// Context for batch task execution.
///
/// This context is specific to one task execution, and should *not* be shared by different tasks.
pub trait BatchTaskContext: Clone + Send + Sync + 'static {
    /// Get task output identified by `task_output_id`.
    ///
    /// Returns error if the task of `task_output_id` doesn't run in same worker as current task.
    fn get_task_output(&self, task_output_id: TaskOutputId) -> Result<TaskOutput<Self>>;

    /// Whether `peer_addr` is in same as current task.
    fn is_local_addr(&self, peer_addr: &HostAddr) -> bool;

    /// Get task error identified by `task_id`.
    fn get_task_error(&self, task_id: TaskId) -> Result<Option<RwError>>;

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

    fn stats(&self) -> Arc<BatchMetrics>;

    /// Returns error when `task_id` doesn't belong to current task runtime.
    fn try_get_error(&self, task_id: &TaskId) -> Result<Option<RwError>>;
}

/// Batch task context on compute node.
#[derive(Clone)]
pub struct ComputeNodeContext {
    env: BatchEnvironment,
}

impl BatchTaskContext for ComputeNodeContext {
    fn get_task_output(&self, task_output_id: TaskOutputId) -> Result<TaskOutput<Self>> {
        self.env
            .task_manager()
            .take_output(&task_output_id.to_prost())
    }

    fn is_local_addr(&self, peer_addr: &HostAddr) -> bool {
        is_local_address(self.env.server_address(), peer_addr)
    }

    fn get_task_error(&self, task_id: TaskId) -> Result<Option<RwError>> {
        self.env.task_manager().get_error(&task_id)
    }

    fn source_manager_ref(&self) -> Option<SourceManagerRef> {
        Some(self.env.source_manager_ref())
    }

    fn state_store(&self) -> Option<StateStoreImpl> {
        Some(self.env.state_store())
    }

    fn stats(&self) -> Arc<BatchMetrics> {
        self.env.stats()
    }

    fn try_get_error(&self, task_id: &TaskId) -> Result<Option<RwError>> {
        self.env.task_manager().get_error(task_id)
    }
}

impl ComputeNodeContext {
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        Self {
            env: BatchEnvironment::for_test(),
        }
    }

    pub fn new(env: BatchEnvironment) -> Self {
        Self { env }
    }
}
