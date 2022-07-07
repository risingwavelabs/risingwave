// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use futures::StreamExt;
use parking_lot::Mutex;
use risingwave_common::array::DataChunk;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::batch_plan::{
    PlanFragment, TaskId as ProstTaskId, TaskOutputId as ProstOutputId,
};
use risingwave_pb::task_service::task_info::TaskStatus;
use risingwave_pb::task_service::GetDataResponse;
use tokio::sync::oneshot::{Receiver, Sender};
use tracing_futures::Instrument;

use crate::executor::{BoxedExecutor, ExecutorBuilder};
use crate::rpc::service::exchange::ExchangeWriter;
use crate::task::channel::{create_output_channel, ChanReceiverImpl, ChanSenderImpl};
use crate::task::BatchTaskContext;

#[derive(PartialEq, Eq, Hash, Clone, Debug, Default)]
pub struct TaskId {
    pub task_id: u32,
    pub stage_id: u32,
    pub query_id: String,
}

#[derive(PartialEq, Eq, Hash, Clone, Default)]
pub struct TaskOutputId {
    pub task_id: TaskId,
    pub output_id: u32,
}

/// More compact formatter compared to derived `fmt::Debug`.
impl Debug for TaskOutputId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "TaskOutputId {{ query_id: \"{}\", stage_id: {}, task_id: {}, output_id: {} }}",
            self.task_id.query_id, self.task_id.stage_id, self.task_id.task_id, self.output_id
        ))
    }
}

pub(crate) enum TaskState {
    Pending,
    Running,
    Blocking,
    Finished,
    Failed,
}

impl From<&ProstTaskId> for TaskId {
    fn from(prost: &ProstTaskId) -> Self {
        TaskId {
            task_id: prost.task_id,
            stage_id: prost.stage_id,
            query_id: prost.query_id.clone(),
        }
    }
}

impl TaskId {
    pub fn to_prost(&self) -> ProstTaskId {
        ProstTaskId {
            task_id: self.task_id,
            stage_id: self.stage_id,
            query_id: self.query_id.clone(),
        }
    }
}

impl TryFrom<&ProstOutputId> for TaskOutputId {
    type Error = RwError;

    fn try_from(prost: &ProstOutputId) -> Result<Self> {
        Ok(TaskOutputId {
            task_id: TaskId::from(prost.get_task_id()?),
            output_id: prost.get_output_id(),
        })
    }
}

impl TaskOutputId {
    pub fn to_prost(&self) -> ProstOutputId {
        ProstOutputId {
            task_id: Some(self.task_id.to_prost()),
            output_id: self.output_id,
        }
    }
}

pub struct TaskOutput {
    receiver: ChanReceiverImpl,
    output_id: TaskOutputId,
    failure: Arc<Mutex<Option<RwError>>>,
}

impl TaskOutput {
    /// Writes the data in serialized format to `ExchangeWriter`.
    pub async fn take_data(&mut self, writer: &mut dyn ExchangeWriter) -> Result<()> {
        loop {
            match self.receiver.recv().await {
                // Received some data
                Ok(Some(chunk)) => {
                    trace!(
                        "Task output id: {:?}, data len: {:?}",
                        self.output_id,
                        chunk.cardinality()
                    );
                    let pb = chunk.to_protobuf().await?;
                    let resp = GetDataResponse {
                        status: Default::default(),
                        record_batch: Some(pb),
                    };
                    writer.write(resp).await?;
                }
                // Reached EOF
                Ok(None) => {
                    break;
                }
                // Error happened
                Err(e) => {
                    let possible_err = self.failure.lock().clone();
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
        Ok(self.receiver.recv().await?.map(|c| c.into_data_chunk()))
    }

    pub fn id(&self) -> &TaskOutputId {
        &self.output_id
    }
}

/// `BatchTaskExecution` represents a single task execution.
pub struct BatchTaskExecution<C> {
    /// Task id.
    task_id: TaskId,

    /// Inner plan to execute.
    plan: PlanFragment,

    /// Task state.
    state: Mutex<TaskStatus>,

    /// Receivers data of the task.   
    receivers: Mutex<Vec<Option<ChanReceiverImpl>>>,

    /// Context for task execution
    context: C,

    /// The execution failure.
    failure: Arc<Mutex<Option<RwError>>>,

    /// Shutdown signal sender.
    shutdown_tx: Mutex<Option<Sender<u64>>>,

    epoch: u64,
}

impl<C: BatchTaskContext> BatchTaskExecution<C> {
    pub fn new(
        prost_tid: &ProstTaskId,
        plan: PlanFragment,
        context: C,
        epoch: u64,
    ) -> Result<Self> {
        Ok(Self {
            task_id: TaskId::from(prost_tid),
            plan,
            state: Mutex::new(TaskStatus::Pending),
            receivers: Mutex::new(Vec::new()),
            context,
            failure: Arc::new(Mutex::new(None)),
            epoch,
            shutdown_tx: Mutex::new(None),
        })
    }

    pub fn get_task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// `async_execute` executes the task in background, it spawns a tokio coroutine and returns
    /// immediately. The result produced by the task will be sent to one or more channels, according
    /// to a particular shuffling strategy. For example, in hash shuffling, the result will be
    /// hash partitioned across multiple channels.
    /// To obtain the result, one must pick one of the channels to consume via [`TaskOutputId`]. As
    /// such, parallel consumers are able to consume the result idependently.
    pub async fn async_execute(self: Arc<Self>) -> Result<()> {
        trace!(
            "Prepare executing plan [{:?}]: {}",
            self.task_id,
            serde_json::to_string_pretty(self.plan.get_root()?).unwrap()
        );
        *self.state.lock() = TaskStatus::Running;
        let exec = ExecutorBuilder::new(
            self.plan.root.as_ref().unwrap(),
            &self.task_id,
            self.context.clone(),
            self.epoch,
        )
        .build()
        .await?;

        let (sender, receivers) = create_output_channel(self.plan.get_exchange_info()?)?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<u64>();
        *self.shutdown_tx.lock() = Some(shutdown_tx);
        self.receivers
            .lock()
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
                if let Err(e) = self
                    .try_execute(exec, &mut sender, shutdown_rx)
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
                    *failure.lock() = Some(e);
                    *self.state.lock() = TaskStatus::Failed;
                }
            });

            if let Err(join_error) = join_handle.await && join_error.is_panic() {
                error!("Batch task {:?} panic!", task_id_cloned);
            }
        });
        Ok(())
    }

    pub async fn try_execute(
        &self,
        root: BoxedExecutor,
        sender: &mut ChanSenderImpl,
        mut shutdown_rx: Receiver<u64>,
    ) -> Result<()> {
        let mut data_chunk_stream = root.execute();
        loop {
            tokio::select! {
                // We prioritize abort signal over normal data chunks.
                biased;
                _ = &mut shutdown_rx => {
                    sender.send(None).await?;
                    *self.state.lock() = TaskStatus::Aborted;
                    break;
                }
                res = data_chunk_stream.next() => {
                    match res {
                        Some(data_chunk) => {
                            sender.send(Some(data_chunk?)).await?;
                        }
                        None => {
                            trace!("data chunk stream shuts down");
                            sender.send(None).await?;
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn abort_task(&self) -> Result<()> {
        let sender = self.shutdown_tx.lock().take().ok_or_else(|| {
            ErrorCode::InternalError(format!(
                "Task{:?}'s shutdown channel does not exist. \
                    Either the task has been aborted once, \
                    or the channel has neven been initialized.",
                self.task_id
            ))
        })?;
        *self.state.lock() = TaskStatus::Aborting;
        sender.send(0).map_err(|err| {
            ErrorCode::InternalError(format!(
                "Task{:?};s shutdown channel send error:{:?}",
                self.task_id, err
            ))
            .into()
        })
    }

    pub fn get_task_output(&self, output_id: &ProstOutputId) -> Result<TaskOutput> {
        let task_id = TaskId::from(output_id.get_task_id()?);
        let receiver = self.receivers.lock()[output_id.get_output_id() as usize]
            .take()
            .ok_or_else(|| {
                ErrorCode::InternalError(format!(
                    "Task{:?}'s output{} has already been taken.",
                    task_id,
                    output_id.get_output_id(),
                ))
            })?;
        let task_output = TaskOutput {
            receiver,
            output_id: output_id.try_into()?,
            failure: self.failure.clone(),
        };
        Ok(task_output)
    }

    pub fn get_error(&self) -> Option<RwError> {
        self.failure.lock().clone()
    }

    pub fn check_if_running(&self) -> Result<()> {
        if *self.state.lock() != TaskStatus::Running {
            return Err(ErrorCode::InternalError(format!(
                "task {:?} is not running",
                self.get_task_id()
            ))
            .into());
        }
        Ok(())
    }

    pub fn check_if_aborted(&self) -> Result<bool> {
        match *self.state.lock() {
            TaskStatus::Aborted => Ok(true),
            TaskStatus::Finished => Err(ErrorCode::InternalError(format!(
                "task {:?} has been finished",
                self.get_task_id()
            ))
            .into()),
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_output_id_debug() {
        let task_id = TaskId {
            task_id: 1,
            stage_id: 2,
            query_id: "abc".to_string(),
        };
        let task_output_id = TaskOutputId {
            task_id,
            output_id: 3,
        };
        assert_eq!(
            format!("{:?}", task_output_id),
            "TaskOutputId { query_id: \"abc\", stage_id: 2, task_id: 1, output_id: 3 }"
        );
    }
}
