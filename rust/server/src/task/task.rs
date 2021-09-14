use std::sync::{Arc, Mutex};

use crate::array2::DataChunkRef;
use crate::error::{ErrorCode, Result, RwError};
use crate::executor::{BoxedExecutor, ExecutorBuilder, ExecutorResult};
use crate::service::ExchangeWriter;
use crate::task::channel::{create_output_channel, BoxChanReceiver, BoxChanSender};
use crate::task::GlobalTaskEnv;
use crate::util::{json_to_pretty_string, JsonFormatter};
use rayon::ThreadPool;
use risingwave_proto::common::Status;
use risingwave_proto::plan::PlanFragment;
use risingwave_proto::task_service::TaskInfo_TaskStatus as TaskStatus;
use risingwave_proto::task_service::{TaskData, TaskId as ProtoTaskId};

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct TaskId {
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

pub struct TaskExecution {
    task_id: TaskId,
    plan: PlanFragment,
    state: Mutex<TaskStatus>,
    receiver: Option<BoxChanReceiver>,
    env: GlobalTaskEnv,

    // The execution failure.
    failure: Mutex<Option<RwError>>,
}

impl TaskExecution {
    pub fn new(proto_tid: &ProtoTaskId, plan: PlanFragment, env: GlobalTaskEnv) -> Self {
        TaskExecution {
            task_id: TaskId::from(proto_tid),
            plan,
            state: Mutex::new(TaskStatus::PENDING),
            receiver: Option::None,
            env,
            failure: Mutex::new(Option::None),
        }
    }

    pub fn get_task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// `get_data` consumes the data produced by `async_execute`.
    pub fn async_execute(&mut self, worker_pool: Arc<ThreadPool>) -> Result<()> {
        {
            let mut state = self.state.lock().unwrap();
            *state = TaskStatus::RUNNING;
        }
        debug!(
            "Prepare executing plan [{:?}]: {}",
            self.task_id,
            json_to_pretty_string(&self.plan.to_json()?)?
        );
        let exec = ExecutorBuilder::new(self.plan.get_root(), self.env.clone()).build()?;
        let (sender, receiver) = create_output_channel(self.plan.get_shuffle_info())?;
        self.receiver = Some(receiver);
        worker_pool.install(|| {
            debug!("Executing plan [{:?}]", self.task_id);
            async_std::task::block_on(self.execute(exec, sender));
        });
        Ok(())
    }

    async fn execute(&mut self, root: BoxedExecutor, sender: BoxChanSender) {
        if let Err(e) = TaskExecution::try_execute(root, sender).await {
            error!("Execution failed: {} [task_id={:?}]", &e, &self.task_id);
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
    pub async fn take_data(&mut self, sink_id: u32, writer: &mut dyn ExchangeWriter) -> Result<()> {
        self.check_if_running()?;
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
            writer.write(task_data).await?;
        }
        let possible_err = self.failure.lock().unwrap().clone();
        if let Some(err) = possible_err {
            return Err(err);
        }
        Ok(())
    }

    pub async fn direct_take_data(&mut self, sink_id: u32) -> Result<Option<DataChunkRef>> {
        self.check_if_running()?;
        let mut receiver = self.receiver.take().unwrap();
        let res = receiver.recv(sink_id).await;
        self.receiver = Some(receiver);
        Ok(res)
    }

    fn check_if_running(&self) -> Result<()> {
        let state = self.state.lock().unwrap();
        if *state != TaskStatus::RUNNING {
            return Err(ErrorCode::InternalError(format!(
                "task {:?} is not running",
                self.get_task_id()
            ))
            .into());
        }
        Ok(())
    }
}
