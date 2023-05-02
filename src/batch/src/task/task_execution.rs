// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Debug, Formatter};
#[cfg(enable_task_local_alloc)]
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
#[cfg(enable_task_local_alloc)]
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use minitrace::prelude::*;
use parking_lot::Mutex;
use risingwave_common::array::DataChunk;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_pb::batch_plan::{PbTaskId, PbTaskOutputId, PlanFragment};
use risingwave_pb::common::BatchQueryEpoch;
use risingwave_pb::task_service::task_info_response::TaskStatus;
use risingwave_pb::task_service::{GetDataResponse, TaskInfoResponse};
use tokio_metrics::TaskMonitor;

use crate::error::BatchError::SenderError;
use crate::error::{to_rw_error, BatchError, Result as BatchResult};
use crate::executor::{BoxedExecutor, ExecutorBuilder};
use crate::rpc::service::exchange::ExchangeWriter;
use crate::rpc::service::task_service::TaskInfoResponseResult;
use crate::task::channel::{create_output_channel, ChanReceiverImpl, ChanSenderImpl};
use crate::task::BatchTaskContext;

// Now we will only at most have 2 status for each status channel. Running -> Failed or Finished.
pub const TASK_STATUS_BUFFER_SIZE: usize = 2;

/// A special version for batch allocation stat, passed in another task `context` C to report task
/// mem usage 0 bytes at the end.
#[cfg(enable_task_local_alloc)]
pub async fn allocation_stat_for_batch<Fut, T, F, C>(
    future: Fut,
    interval: Duration,
    mut report: F,
    context: C,
) -> T
where
    Fut: Future<Output = T>,
    F: FnMut(usize),
    C: BatchTaskContext,
{
    use task_stats_alloc::{TaskLocalBytesAllocated, BYTES_ALLOCATED};

    BYTES_ALLOCATED
        .scope(TaskLocalBytesAllocated::new(), async move {
            // The guard has the same lifetime as the counter so that the counter will keep positive
            // in the whole scope. When the scope exits, the guard is released, so the counter can
            // reach zero eventually and then `drop` itself.
            let _guard = Box::new(114514);
            let monitor = async move {
                let mut interval = tokio::time::interval(interval);
                loop {
                    interval.tick().await;
                    BYTES_ALLOCATED.with(|bytes| report(bytes.val()));
                }
            };
            let output = tokio::select! {
                biased;
                _ = monitor => unreachable!(),
                output = future => {
                    // NOTE: Report bytes allocated when the actor ends. We simply report 0 here,
                    // assuming that all memory allocated by this batch task will be freed at some
                    // time. Maybe we should introduce a better monitoring strategy for batch memory
                    // usage.
                    BYTES_ALLOCATED.with(|_| context.store_mem_usage(0));
                    output
                },
            };
            output
        })
        .await
}

/// Send batch task status (local/distributed) to frontend.
///
///
/// Local mode use `StateReporter::Local`, Distributed mode use `StateReporter::Distributed` to send
/// status (Failed/Finished) update. `StateReporter::Mock` is only used in test and do not takes any
/// effect. Local sender only report Failed update, Distributed sender will also report
/// Finished/Pending/Starting/Aborted etc.
pub enum StateReporter {
    Distributed(tokio::sync::mpsc::Sender<TaskInfoResponseResult>),
    Mock(),
}

impl StateReporter {
    pub async fn send(&mut self, val: TaskInfoResponse) -> BatchResult<()> {
        match self {
            Self::Distributed(s) => s.send(Ok(val)).await.map_err(|_| SenderError),
            Self::Mock() => Ok(()),
        }
    }

    pub fn new_with_dist_sender(s: tokio::sync::mpsc::Sender<TaskInfoResponseResult>) -> Self {
        Self::Distributed(s)
    }

    pub fn new_with_test() -> Self {
        Self::Mock()
    }
}

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

impl From<&PbTaskId> for TaskId {
    fn from(prost: &PbTaskId) -> Self {
        TaskId {
            task_id: prost.task_id,
            stage_id: prost.stage_id,
            query_id: prost.query_id.clone(),
        }
    }
}

impl TaskId {
    pub fn to_prost(&self) -> PbTaskId {
        PbTaskId {
            task_id: self.task_id,
            stage_id: self.stage_id,
            query_id: self.query_id.clone(),
        }
    }
}

impl TryFrom<&PbTaskOutputId> for TaskOutputId {
    type Error = RwError;

    fn try_from(prost: &PbTaskOutputId) -> Result<Self> {
        Ok(TaskOutputId {
            task_id: TaskId::from(prost.get_task_id()?),
            output_id: prost.get_output_id(),
        })
    }
}

impl TaskOutputId {
    pub fn to_prost(&self) -> PbTaskOutputId {
        PbTaskOutputId {
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
        writer: &mut impl ExchangeWriter,
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
                        record_batch: Some(pb),
                    };
                    writer.write(Ok(resp)).await?;
                }
                // Reached EOF
                Ok(None) => {
                    break;
                }
                // Error happened
                Err(e) => {
                    writer.write(Err(tonic::Status::from(&*e))).await?;
                    break;
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
        writer: &mut impl ExchangeWriter,
        num: usize,
    ) -> Result<bool> {
        self.take_data_inner(writer, Some(num)).await
    }

    /// Take all data and write the data in serialized format to `ExchangeWriter`.
    pub async fn take_data(&mut self, writer: &mut impl ExchangeWriter) -> Result<()> {
        let finish = self.take_data_inner(writer, None).await?;
        assert!(finish);
        Ok(())
    }

    /// Directly takes data without serialization.
    pub async fn direct_take_data(&mut self) -> Result<Option<DataChunk>> {
        Ok(self
            .receiver
            .recv()
            .await
            .map_err(to_rw_error)?
            .map(|c| c.into_data_chunk()))
    }

    pub fn id(&self) -> &TaskOutputId {
        &self.output_id
    }
}

#[derive(Clone)]
pub enum ShutdownMsg {
    /// Used in init, it never occur in receiver later.
    Init,
    Abort(String),
    Cancel,
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

    /// Sender for sending chunks between different executors.
    sender: ChanSenderImpl,

    /// Context for task execution
    context: C,

    /// The execution failure.
    failure: Arc<Mutex<Option<RwError>>>,

    /// State receivers. Will be moved out by `.state_receivers()`. Returned back to client.
    /// This is a hack, cuz there is no easy way to get out the receiver.
    state_rx: Mutex<Option<tokio::sync::mpsc::Receiver<TaskInfoResponse>>>,

    epoch: BatchQueryEpoch,

    /// Runtime for the batch tasks.
    runtime: Arc<BackgroundShutdownRuntime>,

    shutdown_tx: tokio::sync::watch::Sender<ShutdownMsg>,
    shutdown_rx: tokio::sync::watch::Receiver<ShutdownMsg>,
}

impl<C: BatchTaskContext> BatchTaskExecution<C> {
    pub fn new(
        prost_tid: &PbTaskId,
        plan: PlanFragment,
        context: C,
        epoch: BatchQueryEpoch,
        runtime: Arc<BackgroundShutdownRuntime>,
    ) -> Result<Self> {
        let task_id = TaskId::from(prost_tid);

        let (sender, receivers) = create_output_channel(
            plan.get_exchange_info()?,
            context.get_config().developer.output_channel_size,
        )?;

        let mut rts = Vec::new();
        rts.extend(receivers.into_iter().map(Some));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(ShutdownMsg::Init);
        Ok(Self {
            task_id,
            plan,
            state: Mutex::new(TaskStatus::Pending),
            receivers: Mutex::new(rts),
            failure: Arc::new(Mutex::new(None)),
            epoch,
            state_rx: Mutex::new(None),
            context,
            runtime,
            sender,
            shutdown_tx,
            shutdown_rx,
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
    pub async fn async_execute(self: Arc<Self>, state_tx: Option<StateReporter>) -> Result<()> {
        let mut state_tx = state_tx;
        trace!(
            "Prepare executing plan [{:?}]: {}",
            self.task_id,
            serde_json::to_string_pretty(self.plan.get_root()?).unwrap()
        );

        let exec = ExecutorBuilder::new(
            self.plan.root.as_ref().unwrap(),
            &self.task_id,
            self.context.clone(),
            self.epoch.clone(),
            self.shutdown_rx.clone(),
        )
        .build()
        .await?;

        let sender = self.sender.clone();
        let _failure = self.failure.clone();
        let task_id = self.task_id.clone();

        // After we init the output receivers, it's must safe to schedule next stage -- able to send
        // TaskStatus::Running here.
        // Init the state receivers. Swap out later.
        self.change_state_notify(TaskStatus::Running, state_tx.as_mut(), None)
            .await?;

        // Clone `self` to make compiler happy because of the move block.
        let t_1 = self.clone();
        // Spawn task for real execution.
        let fut = async move {
            trace!("Executing plan [{:?}]", task_id);
            let sender = sender;
            let mut state_tx = state_tx;
            let batch_metrics = t_1.context.batch_metrics();

            let task = |task_id: TaskId| async move {
                // We should only pass a reference of sender to execution because we should only
                // close it after task error has been set.
                t_1.run(exec, sender, state_tx.as_mut())
                    .in_span({
                        let mut span = Span::enter_with_local_parent("batch_execute");
                        span.add_property(|| ("task_id", task_id.task_id.to_string()));
                        span.add_property(|| ("stage_id", task_id.stage_id.to_string()));
                        span.add_property(|| ("query_id", task_id.query_id.to_string()));
                        span
                    })
                    .await;
            };

            if let Some(batch_metrics) = batch_metrics {
                let monitor = TaskMonitor::new();
                let instrumented_task = AssertUnwindSafe(monitor.instrument(task(task_id.clone())));
                if let Err(error) = instrumented_task.catch_unwind().await {
                    error!("Batch task {:?} panic: {:?}", task_id, error);
                }
                let cumulative = monitor.cumulative();
                let labels = &batch_metrics.task_labels();
                let task_metrics = batch_metrics.get_task_metrics();
                task_metrics
                    .task_first_poll_delay
                    .with_label_values(labels)
                    .set(cumulative.total_first_poll_delay.as_secs_f64());
                task_metrics
                    .task_fast_poll_duration
                    .with_label_values(labels)
                    .set(cumulative.total_fast_poll_duration.as_secs_f64());
                task_metrics
                    .task_idle_duration
                    .with_label_values(labels)
                    .set(cumulative.total_idle_duration.as_secs_f64());
                task_metrics
                    .task_poll_duration
                    .with_label_values(labels)
                    .set(cumulative.total_poll_duration.as_secs_f64());
                task_metrics
                    .task_scheduled_duration
                    .with_label_values(labels)
                    .set(cumulative.total_scheduled_duration.as_secs_f64());
                task_metrics
                    .task_slow_poll_duration
                    .with_label_values(labels)
                    .set(cumulative.total_slow_poll_duration.as_secs_f64());
            } else if let Err(error) = AssertUnwindSafe(task(task_id.clone())).catch_unwind().await
            {
                error!("Batch task {:?} panic: {:?}", task_id, error);
            }
        };

        #[cfg(enable_task_local_alloc)]
        {
            // For every fired Batch Task, we will wrap it with allocation stats to report memory
            // estimation per task to `BatchManager`.
            let ctx1 = self.context.clone();
            let ctx2 = self.context.clone();
            let alloc_stat_wrap_fut = allocation_stat_for_batch(
                fut,
                Duration::from_millis(1000),
                move |bytes| {
                    ctx1.store_mem_usage(bytes);
                },
                ctx2,
            );
            self.runtime.spawn(alloc_stat_wrap_fut);
        }

        #[cfg(not(enable_task_local_alloc))]
        {
            self.runtime.spawn(fut);
        }

        Ok(())
    }

    /// Change state and notify frontend for task status via streaming GRPC.
    pub async fn change_state_notify(
        &self,
        task_status: TaskStatus,
        state_tx: Option<&mut StateReporter>,
        err_str: Option<String>,
    ) -> BatchResult<()> {
        self.change_state(task_status);
        // Notify frontend the task status.
        if let Some(reporter) = state_tx {
            reporter
                .send(TaskInfoResponse {
                    task_id: Some(self.task_id.to_prost()),
                    task_status: task_status.into(),
                    error_message: err_str.unwrap_or("".to_string()),
                })
                .await
        } else {
            Ok(())
        }
    }

    pub fn change_state(&self, task_status: TaskStatus) {
        *self.state.lock() = task_status;
    }

    async fn run(
        &self,
        root: BoxedExecutor,
        mut sender: ChanSenderImpl,
        state_tx: Option<&mut StateReporter>,
    ) {
        let mut data_chunk_stream = root.execute();
        let mut state;
        let mut error = None;
        loop {
            match data_chunk_stream.next().await {
                Some(Ok(data_chunk)) => {
                    if let Err(e) = sender.send(data_chunk).await {
                        match e {
                            BatchError::SenderError => {
                                // This is possible since when we have limit executor in parent
                                // stage, it may early stop receiving data from downstream, which
                                // leads to close of channel.
                                warn!("Task receiver closed!");
                                state = TaskStatus::Finished;
                                break;
                            }
                            x => {
                                error!("Failed to send data!");
                                error = Some(x);
                                state = TaskStatus::Failed;
                                break;
                            }
                        }
                    }
                }
                Some(Err(e)) => match self.shutdown_rx.borrow().clone() {
                    ShutdownMsg::Init => {
                        // There is no message received from shutdown channel, which means it caused
                        // task failed.
                        error!("Batch task failed: {:?}", e);
                        error = Some(BatchError::from(e));
                        state = TaskStatus::Failed;
                        break;
                    }
                    ShutdownMsg::Abort(_) => {
                        error = Some(BatchError::from(e));
                        state = TaskStatus::Aborted;
                        break;
                    }
                    ShutdownMsg::Cancel => {
                        state = TaskStatus::Cancelled;
                        break;
                    }
                },
                None => {
                    debug!("Batch task {:?} finished successfully.", self.task_id);
                    state = TaskStatus::Finished;
                    break;
                }
            }
        }

        let error = error.map(Arc::new);
        *self.failure.lock() = error.clone().map(to_rw_error);
        let err_str = error.as_ref().map(|e| format!("{:?}", e));
        if let Err(e) = sender.close(error).await {
            match e {
                SenderError => {
                    // This is possible since when we have limit executor in parent
                    // stage, it may early stop receiving data from downstream, which
                    // leads to close of channel.
                    warn!("Task receiver closed when sending None!");
                }
                _x => {
                    error!("Failed to close task output channel: {:?}", self.task_id);
                    state = TaskStatus::Failed;
                }
            }
        }

        if let Err(e) = self.change_state_notify(state, state_tx, err_str).await {
            warn!(
                "The status receiver in FE has closed so the status push is failed {:}",
                e
            );
        }
    }

    pub fn abort(&self, err_msg: String) {
        // No need to set state to be Aborted here cuz it will be set by shutdown receiver.
        // Stop task execution.
        if self.shutdown_tx.send(ShutdownMsg::Abort(err_msg)).is_err() {
            debug!("The task has already died before this request.")
        } else {
            info!("Abort task {:?} done", self.task_id);
        }
    }

    pub fn cancel(&self) {
        if self.shutdown_tx.send(ShutdownMsg::Cancel).is_err() {
            debug!("The task has already died before this request.");
        }
    }

    pub fn get_task_output(&self, output_id: &PbTaskOutputId) -> Result<TaskOutput> {
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

    pub fn state_receiver(&self) -> tokio::sync::mpsc::Receiver<TaskInfoResponse> {
        self.state_rx
            .lock()
            .take()
            .expect("The state receivers must have been inited!")
    }

    pub fn mem_usage(&self) -> usize {
        self.context.mem_usage()
    }

    /// Check the task status: whether has ended.
    pub fn is_end(&self) -> bool {
        let guard = self.state.lock();
        !(*guard == TaskStatus::Running || *guard == TaskStatus::Pending)
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
