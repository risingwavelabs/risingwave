use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use risingwave_common::error::ErrorCode::TaskNotFound;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::plan::{PlanFragment, TaskId as ProstTaskId, TaskSinkId as ProstSinkId};

use crate::task::env::BatchEnvironment;
use crate::task::task::{BatchTaskExecution, TaskId};
use crate::task::TaskSink;

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
        let task = BatchTaskExecution::new(tid, plan, env, epoch)?;

        task.async_execute()?;
        self.tasks
            .lock()
            .unwrap()
            .entry(task.get_task_id().clone())
            .or_insert_with(|| Box::new(task));
        Ok(())
    }

    pub fn take_sink(&self, sink_id: &ProstSinkId) -> Result<TaskSink> {
        let task_id = TaskId::try_from(sink_id.get_task_id()?)?;
        self.tasks
            .lock()
            .unwrap()
            .get(&task_id)
            .ok_or(TaskNotFound)?
            .get_task_sink(sink_id)
    }

    #[cfg(test)]
    pub fn remove_task(&self, sid: &ProstTaskId) -> Result<Option<Box<BatchTaskExecution>>> {
        let task_id = TaskId::try_from(sid)?;
        match self.tasks.lock().unwrap().remove(&task_id) {
            Some(t) => Ok(Some(t)),
            None => Err(TaskNotFound.into()),
        }
    }

    pub fn check_if_task_running(&self, task_id: &TaskId) -> Result<()> {
        match self.tasks.lock().unwrap().get(task_id) {
            Some(task) => task.check_if_running(),
            None => Err(TaskNotFound.into()),
        }
    }

    pub fn get_error(&self, task_id: &TaskId) -> Result<Option<RwError>> {
        return self
            .tasks
            .lock()
            .unwrap()
            .get(task_id)
            .ok_or(TaskNotFound)?
            .get_error();
    }
}

impl Default for BatchManager {
    fn default() -> Self {
        BatchManager::new()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::plan::TaskSinkId as ProstTaskSinkId;
    use tonic::Code;

    use crate::task::{BatchManager, TaskId};

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

        let sink_id = ProstTaskSinkId {
            task_id: Some(risingwave_pb::plan::TaskId {
                stage_id: 0,
                task_id: 0,
                query_id: "".to_owned(),
            }),
            sink_id: 0,
        };
        match manager.take_sink(&sink_id) {
            Err(e) => assert_eq!(e.to_grpc_status().code(), Code::Internal),
            Ok(_) => unreachable!(),
        };
    }
}
