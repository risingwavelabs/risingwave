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
use minitrace::prelude::*;
use parking_lot::Mutex;
use risingwave_common::array::DataChunk;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::batch_plan::{
    PlanFragment, TaskId as ProstTaskId, TaskOutputId as ProstOutputId,
};
use risingwave_pb::task_service::task_info::TaskStatus;
use risingwave_pb::task_service::{GetDataResponse, TaskInfo, TaskInfoResponse};
use tokio::runtime::Runtime;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio_metrics::TaskMonitor;

use crate::error::BatchError::SenderError;
use crate::error::{BatchError, Result as BatchResult};
use crate::executor::{BoxedExecutor, ExecutorBuilder};
use crate::rpc::service::exchange::ExchangeWriter;
use crate::rpc::service::task_service::TaskInfoResponseResult;
use crate::task::channel::{create_output_channel, ChanReceiverImpl, ChanSenderImpl};
use crate::task::BatchTaskContext;

// Now we will only at most have 2 status for each status channel. Running -> Failed or Finished.
const TASK_STATUS_BUFFER_SIZE: usize = 2;

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
    /// Write the data in serialized format to `ExchangeWriter`.
    /// Return whether the data stream is finished.
    async fn take_data_inner(
        &mut self,
        writer: &mut dyn ExchangeWriter,
        at_most_num: Option<usize>,
    ) -> Result<bool> {
        let mut cnt: usize = 0;
        let limited = at_most_num.is_some();
        let at_most_num = at_most_num.unwrap_or(usize::MAX);
        loop {
            if limited && cnt >= at_most_num {
                return Ok(false);
            }
            match self.receiver.recv().await {
                // Received some data
                Ok(Some(chunk)) => {
                    trace!(
                        "Task output id: {:?}, data len: {:?}",
                        self.output_id,
                        chunk.cardinality()
                    );
                    let pb = chunk.to_protobuf().await;
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
                    let possible_err = self.failure.lock().take();
                    return if let Some(err) = possible_err {
                        // Task error
                        Err(err)
                    } else {
                        // Channel error
                        Err(e)
                    };
                }
            }
            cnt += 1;
        }
        Ok(true)
    }

    /// Take at most num data and write the data in serialized format to `ExchangeWriter`.
    /// Return whether the data stream is finished.
    pub async fn take_data_with_num(
        &mut self,
        writer: &mut dyn ExchangeWriter,
        num: usize,
    ) -> Result<bool> {
        self.take_data_inner(writer, Some(num)).await
    }

    /// Take all data and write the data in serialized format to `ExchangeWriter`.
    pub async fn take_data(&mut self, writer: &mut dyn ExchangeWriter) -> Result<()> {
        let finish = self.take_data_inner(writer, None).await?;
        assert!(finish);
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

    /// State receivers. Will be moved out by `.state_receivers()`. Returned back to client.
    /// This is a hack, cuz there is no easy way to get out the receiver.
    state_rx: Mutex<Option<tokio::sync::mpsc::Receiver<TaskInfoResponseResult>>>,

    epoch: u64,

    /// Runtime for the batch tasks.
    runtime: &'static Runtime,
}

impl<C: BatchTaskContext> BatchTaskExecution<C> {
    pub fn new(
        prost_tid: &ProstTaskId,
        plan: PlanFragment,
        context: C,
        epoch: u64,
        runtime: &'static Runtime,
    ) -> Result<Self> {
        let task_id = TaskId::from(prost_tid);
        Ok(Self {
            task_id,
            plan,
            state: Mutex::new(TaskStatus::Pending),
            receivers: Mutex::new(Vec::new()),
            failure: Arc::new(Mutex::new(None)),
            epoch,
            shutdown_tx: Mutex::new(None),
            state_rx: Mutex::new(None),
            context,
            runtime,
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
    /// such, parallel consumers are able to consume the result independently.
    pub async fn async_execute(self: Arc<Self>) -> Result<()> {
        trace!(
            "Prepare executing plan [{:?}]: {}",
            self.task_id,
            serde_json::to_string_pretty(self.plan.get_root()?).unwrap()
        );

        let exec = ExecutorBuilder::new(
            self.plan.root.as_ref().unwrap(),
            &self.task_id,
            self.context.clone(),
            self.epoch,
        )
        .build()
        .await?;

        // Init shutdown channel and data receivers.
        let (sender, receivers) = create_output_channel(
            self.plan.get_exchange_info()?,
            self.context
                .get_config()
                .developer
                .batch_output_channel_size,
        )?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<u64>();
        *self.shutdown_tx.lock() = Some(shutdown_tx);
        self.receivers
            .lock()
            .extend(receivers.into_iter().map(Some));
        let failure = self.failure.clone();
        let task_id = self.task_id.clone();

        // After we init the output receivers, it's must safe to schedule next stage -- able to send
        // TaskStatus::Running here.
        let (mut state_tx, state_rx) = tokio::sync::mpsc::channel(TASK_STATUS_BUFFER_SIZE);
        // Init the state receivers. Swap out later.
        *self.state_rx.lock() = Some(state_rx);
        self.change_state_notify(TaskStatus::Running, &mut state_tx)
            .await?;

        // Clone `self` to make compiler happy because of the move block.
        let t_1 = self.clone();
        let t_2 = self.clone();
        // Spawn task for real execution.
        self.runtime.spawn(async move {
            trace!("Executing plan [{:?}]", task_id);
            let mut sender = sender;
            let mut state_tx = state_tx;
            let task_metrics = t_1.context.get_task_metrics();

            let task = |task_id: TaskId| async move {
                // We should only pass a reference of sender to execution because we should only
                // close it after task error has been set.
                if let Err(e) = t_1
                    .try_execute(exec, &mut sender, shutdown_rx, &mut state_tx)
                    .in_span({
                        let mut span = Span::enter_with_local_parent("batch_execute");
                        span.add_property(|| ("task_id", task_id.task_id.to_string()));
                        span.add_property(|| ("stage_id", task_id.stage_id.to_string()));
                        span.add_property(|| ("query_id", task_id.query_id.to_string()));
                        span
                    })
                    .await
                {
                    // Prints the entire backtrace of error.
                    error!("Execution failed [{:?}]: {:?}", &task_id, &e);
                    *failure.lock() = Some(e);
                    if let Err(_e) = t_1
                        .change_state_notify(TaskStatus::Failed, &mut state_tx)
                        .await
                    {
                        // It's possible to send fail. Same reason in `.try_execute`.
                    }
                }
            };

            if let Some(task_metrics) = task_metrics {
                let monitor = TaskMonitor::new();
                let join_handle = t_2.runtime.spawn(monitor.instrument(task(task_id.clone())));
                if let Err(join_error) = join_handle.await && join_error.is_panic() {
                    error!("Batch task {:?} panic!", task_id);
                }
                let cumulative = monitor.cumulative();
                task_metrics
                    .task_first_poll_delay
                    .set(cumulative.total_first_poll_delay.as_secs_f64());
                task_metrics
                    .task_fast_poll_duration
                    .set(cumulative.total_fast_poll_duration.as_secs_f64());
                task_metrics
                    .task_idle_duration
                    .set(cumulative.total_idle_duration.as_secs_f64());
                task_metrics
                    .task_poll_duration
                    .set(cumulative.total_poll_duration.as_secs_f64());
                task_metrics
                    .task_scheduled_duration
                    .set(cumulative.total_scheduled_duration.as_secs_f64());
                task_metrics
                    .task_slow_poll_duration
                    .set(cumulative.total_slow_poll_duration.as_secs_f64());
                task_metrics.clear_record();
            } else {
                let join_handle = t_2.runtime.spawn(task(task_id.clone()));
                if let Err(join_error) = join_handle.await && join_error.is_panic() {
                    error!("Batch task {:?} panic!", task_id);
                }
            }
        });
        Ok(())
    }

    /// Change state and notify frontend for task status.
    pub async fn change_state_notify(
        &self,
        task_status: TaskStatus,
        state_tx: &mut tokio::sync::mpsc::Sender<TaskInfoResponseResult>,
    ) -> BatchResult<()> {
        self.change_state(task_status);
        // Notify frontend the task status.
        state_tx
            .send(Ok(TaskInfoResponse {
                task_info: Some(TaskInfo {
                    task_id: Some(TaskId::default().to_prost()),
                    task_status: task_status.into(),
                }),
                // TODO: Fill the real status.
                ..Default::default()
            }))
            .await
            .map_err(|_| SenderError)
    }

    pub fn change_state(&self, task_status: TaskStatus) {
        *self.state.lock() = task_status;
    }

    pub async fn try_execute(
        &self,
        root: BoxedExecutor,
        sender: &mut ChanSenderImpl,
        mut shutdown_rx: Receiver<u64>,
        state_tx: &mut tokio::sync::mpsc::Sender<TaskInfoResponseResult>,
    ) -> Result<()> {
        let mut data_chunk_stream = root.execute();
        let mut state = TaskStatus::Unspecified;
        loop {
            tokio::select! {
            // We prioritize abort signal over normal data chunks.
            biased;
            _ = &mut shutdown_rx => {
                state = TaskStatus::Aborted;
                break;
            }
            res = data_chunk_stream.next() => {
                if let Some(data_chunk) = res {
                if let Err(e) = sender.send(Some(data_chunk?)).await {
                        match e {
                            BatchError::SenderError => {
                                // This is possible since when we have limit executor in parent
                                // stage, it may early stop receiving data from downstream, which
                                // leads to close of channel.
                                warn!("Task receiver closed!");
                                break;
                            },
                            x => {
                                return Err(InternalError(format!("Failed to send data: {:?}", x)))?;
                            }
                        }
                    }
                } else {
                    state = TaskStatus::Finished;
                    break;
                }
            }
            }
        }

        *self.state.lock() = state;
        if let Err(e) = sender.send(None).await {
            match e {
                BatchError::SenderError => {
                    // This is possible since when we have limit executor in parent
                    // stage, it may early stop receiving data from downstream, which
                    // leads to close of channel.
                    warn!("Task receiver closed when sending None!");
                }
                x => {
                    return Err(InternalError(format!("Failed to send data: {:?}", x)))?;
                }
            }
        }
        if let Err(_e) = self.change_state_notify(state, state_tx).await {}
        Ok(())
    }

    pub fn abort_task(&self) {
        if let Some(sender) = self.shutdown_tx.lock().take() {
            // No need to set state to be Aborted here cuz it will be set by shutdown receiver.
            // Stop task execution.
            if sender.send(0).is_err() {
                warn!("The task has already died before this request, so the abort did no-op")
            } else {
                info!("Abort task {:?} done", self.task_id);
            }
        };
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

    pub fn state_receiver(&self) -> tokio::sync::mpsc::Receiver<TaskInfoResponseResult> {
        self.state_rx
            .lock()
            .take()
            .expect("The state receivers must have been inited!")
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
