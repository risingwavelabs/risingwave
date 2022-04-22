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

use std::collections::{hash_map, HashMap};
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_common::error::ErrorCode::{self, TaskNotFound};
use risingwave_common::error::{Result, RwError};
use risingwave_pb::plan::{PlanFragment, TaskId as ProstTaskId, TaskOutputId as ProstOutputId};

use crate::task::env::BatchEnvironment;
use crate::task::{BatchTaskExecution, TaskId, TaskOutput};

/// `BatchManager` is responsible for managing all batch tasks.
#[derive(Clone)]
pub struct BatchManager {
    /// Every task id has a corresponding task execution.
    tasks: Arc<Mutex<HashMap<TaskId, Box<BatchTaskExecution>>>>,
}

impl BatchManager {
    pub fn new() -> Self {
        BatchManager {
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn fire_task(
        &self,
        env: BatchEnvironment,
        tid: &ProstTaskId,
        plan: PlanFragment,
        epoch: u64,
    ) -> Result<()> {
        trace!("Received task id: {:?}, plan: {:?}", tid, plan);
        let task = BatchTaskExecution::new(tid, plan, env, epoch)?;
        let task_id = task.get_task_id().clone();

        task.async_execute()?;
        if let hash_map::Entry::Vacant(e) = self.tasks.lock().entry(task_id.clone()) {
            e.insert(Box::new(task));
            Ok(())
        } else {
            Err(ErrorCode::InternalError(format!(
                "can not create duplicate task with the same id: {:?}",
                task_id,
            ))
            .into())
        }
    }

    pub fn take_output(&self, output_id: &ProstOutputId) -> Result<TaskOutput> {
        let task_id = TaskId::from(output_id.get_task_id()?);
        debug!("Trying to take output of: {:?}", output_id);
        self.tasks
            .lock()
            .get(&task_id)
            .ok_or(TaskNotFound)?
            .get_task_output(output_id)
    }

    #[cfg(test)]
    pub fn remove_task(&self, sid: &ProstTaskId) -> Result<Option<Box<BatchTaskExecution>>> {
        let task_id = TaskId::from(sid);
        match self.tasks.lock().remove(&task_id) {
            Some(t) => Ok(Some(t)),
            None => Err(TaskNotFound.into()),
        }
    }

    /// Returns error if task is not running.
    pub fn check_if_task_running(&self, task_id: &TaskId) -> Result<()> {
        match self.tasks.lock().get(task_id) {
            Some(task) => task.check_if_running(),
            None => Err(TaskNotFound.into()),
        }
    }

    pub fn get_error(&self, task_id: &TaskId) -> Result<Option<RwError>> {
        Ok(self
            .tasks
            .lock()
            .get(task_id)
            .ok_or(TaskNotFound)?
            .get_error())
    }
}

impl Default for BatchManager {
    fn default() -> Self {
        BatchManager::new()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::plan::exchange_info::DistributionMode;
    use risingwave_pb::plan::plan_node::NodeBody;
    use risingwave_pb::plan::TaskOutputId as ProstTaskOutputId;
    use tonic::Code;

    use crate::task::{BatchEnvironment, BatchManager, TaskId};

    #[test]
    fn test_task_not_found() {
        let manager = BatchManager::new();
        let task_id = TaskId {
            task_id: 0,
            stage_id: 0,
            query_id: "abc".to_string(),
        };

        assert_eq!(
            manager
                .check_if_task_running(&task_id)
                .unwrap_err()
                .to_grpc_status()
                .code(),
            Code::Internal
        );

        let output_id = ProstTaskOutputId {
            task_id: Some(risingwave_pb::plan::TaskId {
                stage_id: 0,
                task_id: 0,
                query_id: "".to_owned(),
            }),
            output_id: 0,
        };
        match manager.take_output(&output_id) {
            Err(e) => assert_eq!(e.to_grpc_status().code(), Code::Internal),
            Ok(_) => unreachable!(),
        };
    }

    #[tokio::test]
    async fn test_task_id_conflict() {
        use risingwave_pb::plan::*;

        let manager = BatchManager::new();
        let plan = PlanFragment {
            root: Some(PlanNode {
                children: vec![],
                identity: "".to_string(),
                node_body: Some(NodeBody::Values(ValuesNode {
                    tuples: vec![],
                    fields: vec![],
                })),
            }),
            exchange_info: Some(ExchangeInfo {
                mode: DistributionMode::Single as i32,
                distribution: None,
            }),
        };
        let env = BatchEnvironment::for_test();
        let task_id = TaskId {
            ..Default::default()
        };
        manager
            .fire_task(env.clone(), &task_id, plan.clone(), 0)
            .unwrap();
        let err = manager.fire_task(env, &task_id, plan, 0).unwrap_err();
        assert!(err
            .to_string()
            .contains("can not create duplicate task with the same id"));
    }
}
