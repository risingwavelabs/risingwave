use std::sync::Arc;
use std::sync::RwLock;

use risingwave_proto::task_service::TaskId;

pub(in crate::task) type TaskExecutionRef = Arc<TaskExecution>;

pub(in crate) enum TaskState {
  PENDING,
  RUNNING,
  BLOCKING,
  FINISHED,
  FAILED,
}

struct TaskExecutionInner {
  task_id: TaskId,
  state: TaskState,
  // sinks: Vec<Option<TaskSinkReader<>>>
}

pub(in crate::task) struct TaskExecution {
  inner: RwLock<TaskExecutionInner>,
}

// impl TaskExecutionInner {
//     fn new()
// }
