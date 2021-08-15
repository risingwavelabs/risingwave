use crate::error::{ErrorCode, Result};
use crate::task::task::{TaskExecution, TaskId};
use rayon::{ThreadPool, ThreadPoolBuilder};
use risingwave_proto::plan::PlanFragment;
use risingwave_proto::task_service::{TaskId as ProtoTaskId, TaskSinkId as ProtoSinkId};
use std::collections::HashMap;
use std::sync::Mutex;

pub(crate) struct TaskManager {
    worker_pool: ThreadPool,
    tasks: Mutex<HashMap<TaskId, Box<TaskExecution>>>,
}

impl TaskManager {
    pub(crate) fn new() -> Self {
        let worker_pool = ThreadPoolBuilder::default().num_threads(2).build().unwrap();
        TaskManager {
            worker_pool,
            tasks: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn fire_task(&mut self, tid: &ProtoTaskId, plan: PlanFragment) -> Result<()> {
        let tsk = TaskExecution::new(tid, plan);
        self.tasks
            .lock()
            .unwrap()
            .entry(tsk.get_task_id().clone())
            .or_insert_with(|| Box::new(tsk))
            .async_execute(&mut self.worker_pool)?;
        Ok(())
    }

    // NOTE: For now, draining out data from the task will block the grpc thread.
    // By design, `take_data` is an future, but since it's unsafe to mix other
    // future runtimes (tokio, eg.) with grpc-rs reactor, we
    pub(crate) fn take_task(&mut self, sid: &ProtoSinkId) -> Result<Box<TaskExecution>> {
        let task_id = TaskId::from(sid.get_task_id());
        let tsk = match self.tasks.lock().unwrap().remove(&task_id) {
            Some(t) => t,
            None => {
                return Err(
                    ErrorCode::InternalError(format!("No such task {:?}", &task_id)).into(),
                );
            }
        };
        Ok(tsk)
    }
}
