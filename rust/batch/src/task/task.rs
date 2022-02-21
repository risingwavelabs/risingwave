use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use risingwave_common::array::DataChunk;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::plan::{
    PlanFragment, QueryId as ProstQueryId, StageId as ProstStageId, TaskId as ProstTaskId,
    TaskSinkId as ProstSinkId,
};
use risingwave_pb::task_service::task_info::TaskStatus;
use risingwave_pb::task_service::GetDataResponse;
use tracing_futures::Instrument;

use crate::executor::{BoxedExecutor, ExecutorBuilder};
use crate::rpc::service::exchange::ExchangeWriter;
use crate::task::channel::{create_output_channel, BoxChanReceiver, BoxChanSender};
use crate::task::{BatchEnvironment, BatchManager};

#[derive(PartialEq, Eq, Hash, Clone, Debug, Default)]
pub struct TaskId {
    pub task_id: u32,
    pub stage_id: u32,
    pub query_id: String,
}

#[derive(PartialEq, Eq, Hash, Clone, Default)]
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

impl TryFrom<&ProstTaskId> for TaskId {
    type Error = RwError;
    fn try_from(prost: &ProstTaskId) -> Result<Self> {
        Ok(TaskId {
            task_id: prost.get_task_id(),
            stage_id: prost.get_stage_id()?.get_stage_id(),
            query_id: String::from(prost.get_stage_id()?.get_query_id()?.get_trace_id()),
        })
    }
}

impl TaskId {
    pub fn to_prost(&self) -> ProstTaskId {
        ProstTaskId {
            task_id: self.task_id,
            stage_id: Some(ProstStageId {
                query_id: Some(ProstQueryId {
                    trace_id: self.query_id.clone(),
                }),
                stage_id: self.stage_id,
            }),
        }
    }
}

impl TryFrom<&ProstSinkId> for TaskSinkId {
    type Error = RwError;
    fn try_from(prost: &ProstSinkId) -> Result<Self> {
        Ok(TaskSinkId {
            task_id: TaskId::try_from(prost.get_task_id()?)?,
            sink_id: prost.get_sink_id(),
        })
    }
}

impl TaskSinkId {
    pub fn to_prost(&self) -> ProstSinkId {
        ProstSinkId {
            task_id: Some(self.task_id.to_prost()),
            sink_id: self.sink_id,
        }
    }
}

pub struct TaskSink {
    task_manager: Arc<BatchManager>,
    receiver: BoxChanReceiver,
    sink_id: TaskSinkId,
}

impl TaskSink {
    /// Writes the data in serialized format to `ExchangeWriter`.
    pub async fn take_data(&mut self, writer: &mut dyn ExchangeWriter) -> Result<()> {
        let task_id = self.sink_id.task_id.clone();
        self.task_manager.check_if_task_running(&task_id)?;
        loop {
            match self.receiver.recv().await {
                // Received some data
                Ok(Some(chunk)) => {
                    let chunk = chunk.compact()?;
                    trace!("Task sink: {:?}, data: {:?}", self.sink_id, chunk);
                    let pb = chunk.to_protobuf()?;
                    let resp = GetDataResponse {
                        record_batch: Some(pb),
                        ..Default::default()
                    };
                    writer.write(resp).await?;
                }
                // Reached EOF
                Ok(None) => {
                    break;
                }
                // Error happened
                Err(e) => {
                    let possible_err = self.task_manager.get_error(&task_id)?;
                    return if let Some(err) = possible_err {
                        // Task error
                        Err(err)
                    } else {
                        // Channel error
                        Err(e)
                    };
                }
            }
        }
        Ok(())
    }

    /// Directly takes data without serialization.
    pub async fn direct_take_data(&mut self) -> Result<Option<DataChunk>> {
        let task_id = self.sink_id.task_id.clone();
        self.task_manager.check_if_task_running(&task_id)?;
        self.receiver.recv().await
    }

    pub fn id(&self) -> &TaskSinkId {
        &self.sink_id
    }
}

/// `BatchTaskExecution` represents a single task execution.
pub struct BatchTaskExecution {
    /// Task id.
    task_id: TaskId,

    /// Inner plan to execute.
    plan: PlanFragment,

    /// Task state.
    state: Mutex<TaskStatus>,

    /// Receivers data of the task.   
    receivers: Mutex<Vec<Option<BoxChanReceiver>>>,

    /// Global environment of task execution.
    env: BatchEnvironment,

    /// The execution failure.
    failure: Arc<Mutex<Option<RwError>>>,
}

impl BatchTaskExecution {
    pub fn new(prost_tid: &ProstTaskId, plan: PlanFragment, env: BatchEnvironment) -> Result<Self> {
        Ok(BatchTaskExecution {
            task_id: TaskId::try_from(prost_tid)?,
            plan,
            state: Mutex::new(TaskStatus::Pending),
            receivers: Mutex::new(Vec::new()),
            env,
            failure: Arc::new(Mutex::new(None)),
        })
    }

    pub fn get_task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// `get_data` consumes the data produced by `async_execute`.
    pub fn async_execute(&self) -> Result<()> {
        trace!(
            "Prepare executing plan [{:?}]: {}",
            self.task_id,
            serde_json::to_string_pretty(self.plan.get_root()?).unwrap()
        );
        *self.state.lock().unwrap() = TaskStatus::Running;
        let exec = ExecutorBuilder::new(
            self.plan.root.as_ref().unwrap(),
            &self.task_id.clone(),
            self.env.clone(),
        )
        .build()?;

        let (sender, receivers) = create_output_channel(self.plan.get_exchange_info()?)?;
        self.receivers
            .lock()
            .unwrap()
            .extend(receivers.into_iter().map(Some));
        let failure = self.failure.clone();
        let task_id = self.task_id.clone();
        tokio::spawn(async move {
            trace!("Executing plan [{:?}]", task_id);
            let mut sender = sender;

            let task_id_cloned = task_id.clone();

            let join_handle = tokio::spawn(async move {
                // We should only pass a reference of sender to execution because we should only
                // close it after task error has been set.
                if let Err(e) = BatchTaskExecution::try_execute(exec, &mut sender)
                    .instrument(tracing::trace_span!(
                      "batch_execute",
                      task_id = ?task_id.task_id,
                      stage_id = ?task_id.stage_id,
                      query_id = ?task_id.query_id,
                    ))
                    .await
                {
                    // Prints the entire backtrace of error.
                    error!("Execution failed [{:?}]: {:?}", &task_id, &e);
                    *failure.lock().unwrap() = Some(e);
                }
            });

            if let Err(join_error) = join_handle.await {
                if join_error.is_panic() {
                    error!("Batch task {:?} panic!", task_id_cloned);
                }
            }
        });
        Ok(())
    }

    async fn try_execute(mut root: BoxedExecutor, sender: &mut BoxChanSender) -> Result<()> {
        root.open().await?;
        while let Some(chunk) = root.next().await? {
            if chunk.cardinality() > 0 {
                sender.send(Some(chunk)).await?;
            }
        }
        sender.send(None).await?;
        root.close().await?;
        Ok(())
    }

    pub fn get_task_sink(&self, sink_id: &ProstSinkId) -> Result<TaskSink> {
        let task_id = TaskId::try_from(sink_id.get_task_id()?)?;
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
            sink_id: sink_id.try_into()?,
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
