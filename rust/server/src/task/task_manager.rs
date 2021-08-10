use std::collections::HashMap;
use std::sync::Mutex;

use rayon::ThreadPool;

use crate::error::Result;
use crate::task::task::TaskExecutionRef;
use risingwave_proto::task_service::CreateTaskRequest;
use risingwave_proto::task_service::TaskId;

pub(crate) struct TaskManager {
    query_task_pool: ThreadPool,
    task_executions: Mutex<HashMap<TaskId, TaskExecutionRef>>,
}

impl TaskManager {
    pub(crate) fn create_task(_request: &CreateTaskRequest) -> Result<()> {
        unimplemented!()
    }
}
