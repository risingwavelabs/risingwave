use std::sync::Mutex;

use crate::error::RwError;
use crate::executor::{transform_plan_tree, BoxedExecutor};
use risingwave_proto::plan::PlanFragment;
use risingwave_proto::task_service::TaskId as ProtoTaskId;
use risingwave_proto::task_service::TaskInfo_TaskStatus as TaskStatus;

#[derive(PartialEq, Eq, Hash, Clone)]
pub(crate) struct TaskId {
    pub task_id: u32,
    pub stage_id: u32,
    pub query_id: String,
}

pub(in crate) enum TaskState {
    Pending,
    Running,
    Blocking,
    Finished,
    Failed,
}

impl From<&ProtoTaskId> for TaskId {
    fn from(proto: &ProtoTaskId) -> Self {
        TaskId {
            task_id: proto.get_task_id(),
            stage_id: proto.get_stage_id().get_stage_id(),
            query_id: String::from(proto.get_stage_id().get_query_id().get_trace_id()),
        }
    }
}

pub(in crate::task) struct TaskExecution {
    task_id: TaskId,
    plan: PlanFragment,
    root: BoxedExecutor,

    state: Mutex<TaskStatus>,
}

impl TaskExecution {
    pub fn new(proto_tid: &ProtoTaskId, plan: PlanFragment) -> Result<Self, RwError> {
        let root_exec = transform_plan_tree(&plan)?;
        Ok(TaskExecution {
            task_id: TaskId::from(proto_tid),
            plan,
            root: root_exec,
            state: Mutex::new(TaskStatus::PENDING),
        })
    }

    pub fn get_task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// `execute` and `get_data` run in separate threads.
    /// `get_data` consumes the data produced by `execute`.
    pub fn execute(&mut self) {
        let mut state = self.state.lock().unwrap();
        *state = TaskStatus::RUNNING;
    }
}
