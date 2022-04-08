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

use parking_lot::Mutex;
use risingwave_common::array::DataChunk;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::plan::{PlanFragment, TaskId as ProstTaskId, TaskOutputId as ProstOutputId};
use risingwave_pb::task_service::task_info::TaskStatus;
use risingwave_pb::task_service::GetDataResponse;
use tracing_futures::Instrument;

use crate::executor::{BoxedExecutor, ExecutorBuilder};
use crate::rpc::service::exchange::ExchangeWriter;
use crate::task::channel::{create_output_channel, ChanReceiverImpl, ChanSenderImpl};
use crate::task::{BatchEnvironment, BatchManager};

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
    task_manager: Arc<BatchManager>,
    receiver: ChanReceiverImpl,
    output_id: TaskOutputId,
}

impl TaskOutput {
    /// Writes the data in serialized format to `ExchangeWriter`.
    pub async fn take_data(&mut self, writer: &mut dyn ExchangeWriter) -> Result<()> {
        let task_id = self.output_id.task_id.clone();
        self.task_manager.check_if_task_running(&task_id)?;
        loop {
            match self.receiver.recv().await {
                // Received some data
                Ok(Some(chunk)) => {
                    let chunk = chunk.compact()?;
                    trace!(
                        "Task output id: {:?}, data len: {:?}",
                        self.output_id,
                        chunk.cardinality()
                    );
                    let pb = chunk.to_protobuf();
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
        let task_id = self.output_id.task_id.clone();
        self.task_manager.check_if_task_running(&task_id)?;
        self.receiver.recv().await
    }

    pub fn id(&self) -> &TaskOutputId {
        &self.output_id
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
    receivers: Mutex<Vec<Option<ChanReceiverImpl>>>,

    /// Global environment of task execution.
    env: BatchEnvironment,

    /// The execution failure.
    failure: Arc<Mutex<Option<RwError>>>,

    epoch: u64,
}

impl BatchTaskExecution {
    pub fn new(
        prost_tid: &ProstTaskId,
        plan: PlanFragment,
        env: BatchEnvironment,
        epoch: u64,
    ) -> Result<Self> {
        Ok(BatchTaskExecution {
            task_id: TaskId::from(prost_tid),
            plan,
            state: Mutex::new(TaskStatus::Pending),
            receivers: Mutex::new(Vec::new()),
            env,
            failure: Arc::new(Mutex::new(None)),
            epoch,
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
    pub fn async_execute(&self) -> Result<()> {
        trace!(
            "Prepare executing plan [{:?}]: {}",
            self.task_id,
            serde_json::to_string_pretty(self.plan.get_root()?).unwrap()
        );
        *self.state.lock() = TaskStatus::Running;
        let exec = ExecutorBuilder::new(
            self.plan.root.as_ref().unwrap(),
            &self.task_id.clone(),
            self.env.clone(),
            self.epoch,
        )
        .build()?;

        let (sender, receivers) = create_output_channel(self.plan.get_exchange_info()?)?;
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
                    *failure.lock() = Some(e);
                }
            });

            if let Err(join_error) = join_handle.await && join_error.is_panic() {
                error!("Batch task {:?} panic!", task_id_cloned);
            }
        });
        Ok(())
    }

    async fn try_execute(mut root: BoxedExecutor, sender: &mut ChanSenderImpl) -> Result<()> {
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
            task_manager: self.env.task_manager(),
            receiver,
            output_id: output_id.try_into()?,
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
