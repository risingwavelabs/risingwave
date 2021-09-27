use crate::error::{ErrorCode, Result, RwError};
use crate::task::env::GlobalTaskEnv;
use crate::task::task::{TaskExecution, TaskId};
use crate::task::TaskSink;
use rayon::{ThreadPool, ThreadPoolBuilder};
use risingwave_proto::plan::PlanFragment;
use risingwave_proto::task_service::{TaskId as ProtoTaskId, TaskSinkId as ProtoSinkId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct TaskManager {
    worker_pool: Arc<ThreadPool>,
    tasks: Arc<Mutex<HashMap<TaskId, Box<TaskExecution>>>>,
}

impl TaskManager {
    pub fn new() -> Self {
        let worker_pool = Arc::new(ThreadPoolBuilder::default().num_threads(4).build().unwrap());
        TaskManager {
            worker_pool,
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn fire_task(
        &self,
        env: GlobalTaskEnv,
        tid: &ProtoTaskId,
        plan: PlanFragment,
    ) -> Result<()> {
        let task = TaskExecution::new(tid, plan, env);
        task.async_execute(self.worker_pool.clone())?;
        self.tasks
            .lock()
            .unwrap()
            .entry(task.get_task_id().clone())
            .or_insert_with(|| Box::new(task));
        Ok(())
    }

    pub fn take_sink(&self, sink_id: &ProtoSinkId) -> Result<TaskSink> {
        let task_id = TaskId::from(sink_id.get_task_id());
        return self
            .tasks
            .lock()
            .unwrap()
            .get(&task_id)
            .ok_or_else(|| ErrorCode::InternalError(format!("task {:?} is not running", task_id)))?
            .get_task_sink(sink_id);
    }

    // NOTE: For now, draining out data from the task will block the grpc thread.
    // By design, `take_data` is an future, but for the safe of the unsafety to mix other
    // future runtimes (tokio, eg.) with grpc-rs reactor, we still implement it
    // in an blocking way.
    pub fn remove_task(&self, sid: &ProtoTaskId) -> Result<Option<Box<TaskExecution>>> {
        let task_id = TaskId::from(sid);
        match self.tasks.lock().unwrap().remove(&task_id) {
            Some(t) => Ok(Some(t)),
            None => {
                return Err(ErrorCode::InternalError(format!("No such task {:?}", &task_id)).into())
            }
        }
    }

    pub fn check_if_task_running(&self, task_id: &TaskId) -> Result<()> {
        match self.tasks.lock().unwrap().get(task_id) {
            Some(task) => task.check_if_running(),
            None => {
                return Err(ErrorCode::InternalError(format!(
                    "There is not such task:{:?} in task manager.",
                    task_id
                ))
                .into())
            }
        }
    }

    pub fn get_error(&self, task_id: &TaskId) -> Result<Option<RwError>> {
        return self
            .tasks
            .lock()
            .unwrap()
            .get(task_id)
            .ok_or_else(|| ErrorCode::InternalError(format!("task {:?} is not running", task_id)))?
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
    use crate::task::test_utils::{ResultChecker, TestRunner};
    use crate::task::{GlobalTaskEnv, TaskManager};
    use risingwave_proto::plan::{PlanFragment, PlanNode_PlanNodeType};
    use risingwave_proto::task_service::TaskId as ProtoTaskId;

    #[test]
    fn test_select_all() {
        let mut runner = TestRunner::new();
        runner
            .prepare_table()
            .create_table_int32s(3)
            .insert_i32s(&[1, 4, 2])
            .insert_i32s(&[2, 3, 3])
            .insert_i32s(&[3, 4, 4])
            .insert_i32s(&[4, 3, 5])
            .run();

        let res = runner.prepare_scan().scan_all().run();
        ResultChecker::new()
            .add_i32_column(false, &[1, 2, 3, 4])
            .add_i32_column(false, &[4, 3, 4, 3])
            .add_i32_column(false, &[2, 3, 4, 5])
            .check_result(res);
    }
    #[test]
    fn test_bad_node_type() {
        let env = GlobalTaskEnv::for_test();
        let manager = TaskManager::new();
        let mut plan = PlanFragment::default();
        plan.mut_root()
            .set_node_type(PlanNode_PlanNodeType::INVALID);
        assert!(manager
            .fire_task(env, &ProtoTaskId::default(), plan)
            .is_err());
    }
}
