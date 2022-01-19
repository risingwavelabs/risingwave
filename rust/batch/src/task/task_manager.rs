use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use risingwave_common::error::ErrorCode::TaskNotFound;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::plan::{PlanFragment, TaskId as ProstTaskId, TaskSinkId as ProstSinkId};

use crate::task::env::BatchTaskEnv;
use crate::task::task::{TaskExecution, TaskId};
use crate::task::TaskSink;

#[derive(Clone)]
pub struct TaskManager {
    tasks: Arc<Mutex<HashMap<TaskId, Box<TaskExecution>>>>,
}

impl TaskManager {
    pub fn new() -> Self {
        TaskManager {
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn fire_task(
        &self,
        env: BatchTaskEnv,
        tid: &ProstTaskId,
        plan: PlanFragment,
    ) -> Result<()> {
        let task = TaskExecution::new(tid, plan, env);

        task.async_execute()?;
        self.tasks
            .lock()
            .unwrap()
            .entry(task.get_task_id().clone())
            .or_insert_with(|| Box::new(task));
        Ok(())
    }

    pub fn take_sink(&self, sink_id: &ProstSinkId) -> Result<TaskSink> {
        let task_id = TaskId::from(sink_id.get_task_id());
        self.tasks
            .lock()
            .unwrap()
            .get(&task_id)
            .ok_or(TaskNotFound)?
            .get_task_sink(sink_id)
    }

    #[cfg(test)]
    pub fn remove_task(&self, sid: &ProstTaskId) -> Result<Option<Box<TaskExecution>>> {
        let task_id = TaskId::from(sid);
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

impl Default for TaskManager {
    fn default() -> Self {
        TaskManager::new()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::plan::{QueryId, StageId, TaskSinkId as ProstTaskSinkId};
    use tonic::Code;

    use crate::task::test_utils::{ResultChecker, TestRunner};
    use crate::task::{TaskId, TaskManager};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_select_all() {
        let mut runner = TestRunner::new();
        runner
            .prepare_table()
            .create_table_int32s(3)
            .insert_i32s(&[1, 4, 2])
            .insert_i32s(&[2, 3, 3])
            .insert_i32s(&[3, 4, 4])
            .insert_i32s(&[4, 3, 5])
            .run()
            .await;

        let res = runner
            .prepare_scan()
            .scan_all()
            .await
            .run_and_collect_single_output()
            .await;
        ResultChecker::new()
            .add_i64_column(false, &[0, 1, 2, 3])
            .add_i32_column(false, &[1, 2, 3, 4])
            .add_i32_column(false, &[4, 3, 4, 3])
            .add_i32_column(false, &[2, 3, 4, 5])
            .check_result(&res);
    }

    #[test]
    fn test_task_not_found() {
        let manager = TaskManager::new();
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
                stage_id: Some(StageId {
                    query_id: Some(QueryId {
                        trace_id: "".to_string(),
                    }),
                    stage_id: 0,
                }),
                task_id: 0,
            }),
            sink_id: 0,
        };
        match manager.take_sink(&sink_id) {
            Err(e) => assert_eq!(e.to_grpc_status().code(), Code::Internal),
            Ok(_) => unreachable!(),
        };
    }
}
