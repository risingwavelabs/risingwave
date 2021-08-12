use std::collections::HashMap;
use std::sync::Mutex;

use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::error::{ErrorCode, RwError};
use crate::task::task::{TaskExecution, TaskId};
use risingwave_proto::plan::PlanFragment;
use risingwave_proto::task_service::TaskId as ProtoTaskId;

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

    pub(crate) fn fire_task(
        &mut self,
        tid: &ProtoTaskId,
        plan: PlanFragment,
    ) -> Result<(), RwError> {
        let tsk = TaskExecution::new(tid, plan)?;
        let tsk_ref = Box::new(tsk);
        {
            let mut tasks = match self.tasks.lock() {
                Ok(t) => t,
                Err(e) => return Err(RwError::from(ErrorCode::InternalError(format!("{}", e)))),
            };
            tasks
                .entry(tsk_ref.get_task_id().clone())
                .or_insert(tsk_ref);
        }
        Ok(())
    }
}
