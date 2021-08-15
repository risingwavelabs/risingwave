use std::sync::Mutex;

use crate::error::{ErrorCode, Result, RwError};
use crate::executor::{transform_plan_tree, BoxedExecutor, ExecutorResult};
use crate::service::ExchangeWriteStream;
use crate::storage::{StorageManager, StorageManagerRef};
use crate::task::channel::{create_output_channel, BoxChanReceiver, BoxChanSender};
use futures::SinkExt;
use grpcio::WriteFlags;
use rayon::ThreadPool;
use risingwave_proto::common::Status;
use risingwave_proto::plan::PlanFragment;
use risingwave_proto::task_service::TaskInfo_TaskStatus as TaskStatus;
use risingwave_proto::task_service::{TaskData, TaskId as ProtoTaskId};

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
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

pub(crate) struct TaskExecution {
    task_id: TaskId,
    plan: PlanFragment,
    state: Mutex<TaskStatus>,
    receiver: Option<BoxChanReceiver>,

    // The execution failure.
    failure: Mutex<Option<RwError>>,
}

impl TaskExecution {
    pub fn new(proto_tid: &ProtoTaskId, plan: PlanFragment) -> Self {
        TaskExecution {
            task_id: TaskId::from(proto_tid),
            plan,
            state: Mutex::new(TaskStatus::PENDING),
            receiver: Option::None,
            failure: Mutex::new(Option::None),
        }
    }

    pub fn get_task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// `get_data` consumes the data produced by `async_execute`.
    pub fn async_execute(&mut self, worker_pool: &mut ThreadPool) -> Result<()> {
        {
            let mut state = self.state.lock().unwrap();
            *state = TaskStatus::RUNNING;
        }
        let exec = transform_plan_tree(&self.plan)?;
        let (sender, receiver) = create_output_channel(self.plan.get_shuffle_info())?;
        self.receiver = Some(receiver);
        worker_pool.install(|| {
            futures::executor::block_on(self.execute(exec, sender));
        });
        Ok(())
    }

    async fn execute(&mut self, root: BoxedExecutor, sender: BoxChanSender) {
        if let Err(e) = TaskExecution::try_execute(root, sender).await {
            let mut failure = self.failure.lock().unwrap();
            *failure = Some(e);
        }
    }

    async fn try_execute(mut root: BoxedExecutor, mut sender: BoxChanSender) -> Result<()> {
        root.init()?;
        loop {
            let exec_res = root.execute()?;
            let chunk = match exec_res {
                ExecutorResult::Done => {
                    break;
                }
                ExecutorResult::Batch(chunk) => chunk,
            };
            sender.send(chunk).await?;
        }
        root.clean()?;
        Ok(())
    }

    // Completely drain out of the data from executor.
    pub async fn take_data(
        &mut self,
        sink_id: u32,
        stream: &mut ExchangeWriteStream,
    ) -> Result<()> {
        {
            let state = self.state.lock().unwrap();
            if *state != TaskStatus::RUNNING {
                return Err(ErrorCode::InternalError(format!(
                    "task {:?} is not running",
                    self.get_task_id()
                ))
                .into());
            }
        }
        let mut receiver = self.receiver.take().unwrap();
        loop {
            let chunk = match receiver.recv(sink_id).await {
                None => {
                    break;
                }
                Some(c) => c,
            };
            let pb = chunk.to_protobuf()?;
            let mut task_data = TaskData::new();
            task_data.set_status(Status::default());
            task_data.set_record_batch(pb);
            let res = stream.send((task_data, WriteFlags::default())).await;
            res.map_err(|e| {
                RwError::from(ErrorCode::GrpcError(
                    "failed to send TaskData".to_string(),
                    e,
                ))
            })?;
        }
        let possible_err = self.failure.lock().unwrap().clone();
        if let Some(err) = possible_err {
            return Err(err);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct TaskContext {
    storage_manager: StorageManagerRef,
}

impl TaskContext {
    pub(crate) fn storage_manager(&self) -> &dyn StorageManager {
        &*self.storage_manager
    }

    pub(crate) fn storage_manager_ref(&self) -> StorageManagerRef {
        self.storage_manager.clone()
    }
}
