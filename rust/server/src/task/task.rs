use risingwave_common::array::DataChunk;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use crate::executor::{BoxedExecutor, ExecutorBuilder};
use crate::rpc::service::exchange_service::ExchangeWriter;
use crate::task::channel::{create_output_channel, BoxChanReceiver, BoxChanSender};
use crate::task::GlobalTaskEnv;
use crate::task::TaskManager;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::util::{json_to_pretty_string, JsonFormatter};
use risingwave_pb::plan::PlanFragment;
use risingwave_pb::task_service::task_info::TaskStatus;
use risingwave_pb::task_service::TaskSinkId as ProstSinkId;
use risingwave_pb::task_service::{GetDataResponse, TaskId as ProstTaskId};

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct TaskId {
    pub task_id: u32,
    pub stage_id: u32,
    pub query_id: String,
}

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct TaskSinkId {
    pub task_id: TaskId,
    pub sink_id: u32,
}

/// More compact formatter compared to derived `fmt::Debug`.
impl Debug for TaskSinkId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "TaskSinkId {{ query_id: \"{}\", stage_id: {}, task_id: {}, sink_id: {} }}",
            self.task_id.query_id, self.task_id.stage_id, self.task_id.task_id, self.sink_id
        ))
    }
}

pub(in crate) enum TaskState {
    Pending,
    Running,
    Blocking,
    Finished,
    Failed,
}

impl From<&ProstTaskId> for TaskId {
    fn from(prost: &ProstTaskId) -> Self {
        TaskId {
            task_id: prost.get_task_id(),
            stage_id: prost.get_stage_id().get_stage_id(),
            query_id: String::from(prost.get_stage_id().get_query_id().get_trace_id()),
        }
    }
}

impl From<&ProstSinkId> for TaskSinkId {
    fn from(prost: &ProstSinkId) -> Self {
        TaskSinkId {
            task_id: TaskId::from(prost.get_task_id()),
            sink_id: prost.get_sink_id(),
        }
    }
}

pub struct TaskSink {
    task_manager: Arc<TaskManager>,
    receiver: BoxChanReceiver,
    sink_id: ProstSinkId,
}

impl TaskSink {
    /// Writes the data in serialized format to `ExchangeWriter`.
    pub async fn take_data(&mut self, writer: &mut dyn ExchangeWriter) -> Result<()> {
        let task_id = TaskId::from(self.sink_id.get_task_id());
        self.task_manager.check_if_task_running(&task_id)?;
        loop {
            let chunk = match self.receiver.recv().await? {
                None => {
                    break;
                }
                Some(c) => c,
            };
            let pb = chunk.to_protobuf()?;
            let resp = GetDataResponse {
                record_batch: Some(pb),
                ..Default::default()
            };
            writer.write(resp).await?;
        }
        let possible_err = self.task_manager.get_error(&task_id)?;
        if let Some(err) = possible_err {
            return Err(err);
        }
        Ok(())
    }

    /// Directly takes data without serialization.
    pub async fn direct_take_data(&mut self) -> Result<Option<DataChunk>> {
        let task_id = TaskId::from(self.sink_id.get_task_id());
        self.task_manager.check_if_task_running(&task_id)?;
        self.receiver.recv().await
    }
}

pub struct TaskExecution {
    task_id: TaskId,
    plan: PlanFragment,
    state: Mutex<TaskStatus>,
    receivers: Mutex<Vec<Option<BoxChanReceiver>>>,
    env: GlobalTaskEnv,
    // The execution failure.
    failure: Arc<Mutex<Option<RwError>>>,
}

impl TaskExecution {
    pub fn new(prost_tid: &ProstTaskId, plan: PlanFragment, env: GlobalTaskEnv) -> Self {
        TaskExecution {
            task_id: TaskId::from(prost_tid),
            plan,
            state: Mutex::new(TaskStatus::Pending),
            receivers: Mutex::new(Vec::new()),
            env,
            failure: Arc::new(Mutex::new(None)),
        }
    }

    pub fn get_task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// `get_data` consumes the data produced by `async_execute`.
    pub fn async_execute(&self) -> Result<()> {
        debug!(
            "Prepare executing plan [{:?}]: {}",
            self.task_id,
            json_to_pretty_string(&self.plan.to_json()?)?
        );
        *self.state.lock().unwrap() = TaskStatus::Running;
        let exec = ExecutorBuilder::new(
            self.plan.root.as_ref().unwrap(),
            &self.task_id.clone(),
            self.env.clone(),
        )
        .build()?;
        let (sender, receivers) = create_output_channel(self.plan.get_exchange_info())?;
        self.receivers
            .lock()
            .unwrap()
            .extend(receivers.into_iter().map(Some));
        let failure = self.failure.clone();
        let task_id = self.task_id.clone();
        tokio::spawn(async move {
            debug!("Executing plan [{:?}]", task_id);
            if let Err(e) = TaskExecution::try_execute(exec, sender).await {
                // Prints the entire backtrace of error.
                error!("Execution failed [{:?}]: {:?}", &task_id, &e);
                *failure.lock().unwrap() = Some(e);
            }
        });
        Ok(())
    }

    async fn try_execute(mut root: BoxedExecutor, mut sender: BoxChanSender) -> Result<()> {
        root.open().await?;
        loop {
            match root.next().await? {
                Option::None => {
                    sender.send(None).await?;
                    break;
                }
                Option::Some(chunk) => {
                    // It is wasteful to send a chunk with cardinality == 0.
                    if chunk.cardinality() > 0 {
                        sender.send(Some(chunk)).await?;
                    }
                }
            };
        }
        root.close().await?;
        Ok(())
    }

    pub fn get_task_sink(&self, sink_id: &ProstSinkId) -> Result<TaskSink> {
        let task_id = TaskId::from(sink_id.get_task_id());
        let receiver = self.receivers.lock().unwrap()[sink_id.get_sink_id() as usize]
            .take()
            .ok_or_else(|| {
                ErrorCode::InternalError(format!(
                    "Task{:?}'s sink{} has already been taken.",
                    task_id,
                    sink_id.get_sink_id(),
                ))
            })?;
        let task_sink = TaskSink {
            task_manager: self.env.task_manager(),
            receiver,
            sink_id: sink_id.clone(),
        };
        Ok(task_sink)
    }

    pub fn get_error(&self) -> Result<Option<RwError>> {
        Ok(self.failure.lock().unwrap().clone())
    }

    pub fn check_if_running(&self) -> Result<()> {
        if *self.state.lock().unwrap() != TaskStatus::Running {
            return Err(ErrorCode::InternalError(format!(
                "task {:?} is not running",
                self.get_task_id()
            ))
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_sink_id_debug() {
        let task_id = TaskId {
            task_id: 1,
            stage_id: 2,
            query_id: "abc".to_string(),
        };
        let task_sink_id = TaskSinkId {
            task_id,
            sink_id: 3,
        };
        assert_eq!(
            format!("{:?}", task_sink_id),
            "TaskSinkId { query_id: \"abc\", stage_id: 2, task_id: 1, sink_id: 3 }"
        );
    }
}
