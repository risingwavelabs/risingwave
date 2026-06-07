// Copyright 2026 RisingWave Labs
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

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_expr::expr_context::{capture_expr_context, expr_context_scope};
use tokio::sync::mpsc;

use super::{BoxedDataChunkStream, BoxedExecutor};
use crate::error::{BatchError, Result};
use crate::task::{ShutdownMsg, ShutdownToken};

/// The result of pushing a chunk into a downstream pipeline component.
///
/// This mirrors DuckDB's high-level `NEED_MORE_INPUT`/`FINISHED` flow control,
/// while leaving async blocking to Rust futures and bounded channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushStatus {
    NeedMoreInput,
    Finished,
}

impl PushStatus {
    pub fn is_finished(self) -> bool {
        matches!(self, Self::Finished)
    }
}

/// Per-task push execution context.
#[derive(Clone)]
pub struct PushContext {
    shutdown_rx: ShutdownToken,
    morsel_budget: usize,
    morsel_queue_capacity: usize,
    morsel_parallelism: usize,
    task_scope: Option<Arc<dyn PushTaskScope>>,
}

impl PushContext {
    pub const DEFAULT_MORSEL_BUDGET: usize = 64;
    pub const DEFAULT_MORSEL_PARALLELISM: usize = 1;
    pub const DEFAULT_MORSEL_QUEUE_CAPACITY: usize = 64;

    pub fn new(shutdown_rx: ShutdownToken) -> Self {
        Self {
            shutdown_rx,
            morsel_budget: Self::DEFAULT_MORSEL_BUDGET,
            morsel_queue_capacity: Self::DEFAULT_MORSEL_QUEUE_CAPACITY,
            morsel_parallelism: Self::DEFAULT_MORSEL_PARALLELISM,
            task_scope: None,
        }
    }

    pub fn with_morsel_budget(mut self, morsel_budget: usize) -> Self {
        self.morsel_budget = morsel_budget.max(1);
        self
    }

    pub fn with_morsel_queue_capacity(mut self, capacity: usize) -> Self {
        self.morsel_queue_capacity = capacity.max(1);
        self
    }

    pub fn with_morsel_parallelism(mut self, parallelism: usize) -> Self {
        self.morsel_parallelism = parallelism.max(1);
        self
    }

    pub fn with_task_scope(mut self, task_scope: Arc<dyn PushTaskScope>) -> Self {
        self.task_scope = Some(task_scope);
        self
    }

    pub fn morsel_budget(&self) -> usize {
        self.morsel_budget
    }

    pub fn morsel_queue_capacity(&self) -> usize {
        self.morsel_queue_capacity
    }

    pub fn morsel_parallelism(&self) -> usize {
        self.morsel_parallelism
    }

    pub fn task_scope(&self) -> Option<Arc<dyn PushTaskScope>> {
        self.task_scope.clone()
    }

    pub fn check_shutdown(&self) -> Result<()> {
        match self.shutdown_rx.message() {
            ShutdownMsg::Init => Ok(()),
            ShutdownMsg::Abort(reason) => Err(BatchError::Aborted(reason)),
            ShutdownMsg::Cancel => Err(BatchError::Aborted("cancelled".to_owned())),
        }
    }
}

/// Re-enters task-local execution contexts when a push pipeline is spawned.
pub trait PushTaskScope: Send + Sync + 'static {
    fn scope(&self, future: BoxFuture<'static, ()>) -> BoxFuture<'static, ()>;
}

/// A push sink consumes data chunks produced by a pipeline.
pub trait PushSink: Send {
    fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>>;

    fn finish<'a>(&'a mut self) -> BoxFuture<'a, Result<PushStatus>> {
        async { Ok(PushStatus::NeedMoreInput) }.boxed()
    }

    fn requires_input_order(&self) -> bool {
        true
    }
}

/// Scheduler-owned entrypoint for push execution.
///
/// Today this delegates to the root executor's push implementation. Keeping a
/// distinct scheduler object gives the pipeline DAG work one root-level owner
/// instead of scattering future graph execution across local/distributed task
/// runners.
pub struct PushQueryScheduler {
    context: PushContext,
}

impl PushQueryScheduler {
    pub fn new(context: PushContext) -> Self {
        Self { context }
    }

    pub async fn execute_root(
        &self,
        root: BoxedExecutor,
        sink: &mut dyn PushSink,
    ) -> Result<PushStatus> {
        root.execute_push(self.context.clone(), sink).await
    }

    pub async fn execute_pipeline_schedule(
        &self,
        schedule: PushPipelineSchedule,
        sink: &mut dyn PushSink,
    ) -> Result<PushStatus> {
        schedule.execute_inner(self.context.clone(), sink).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PushPipelineId(usize);

type PushPipelineAction = Box<
    dyn for<'sink> FnMut(
            PushContext,
            &'sink mut dyn PushSink,
        ) -> BoxFuture<'sink, Result<PushStatus>>
        + Send,
>;

struct PushPipelineTask {
    id: PushPipelineId,
    name: String,
    dependencies: Vec<PushPipelineId>,
    action: PushPipelineAction,
}

/// Task-local dependency graph for push pipeline boundaries.
///
/// This models DuckDB-style execution-level completion dependencies: a pipeline
/// becomes runnable only after all dependency pipelines have completed. The
/// current runner is intentionally single-sink and deterministic; the graph is
/// explicit so breaker operators can expose build/finalize/probe boundaries.
/// Parallel pipeline execution still requires separate sink-local state and a
/// later combine step, instead of sharing one `&mut dyn PushSink`.
pub struct PushPipelineSchedule {
    tasks: Vec<PushPipelineTask>,
}

impl PushPipelineSchedule {
    pub fn new() -> Self {
        Self { tasks: vec![] }
    }

    pub fn add_pipeline<F>(
        &mut self,
        name: impl Into<String>,
        dependencies: impl IntoIterator<Item = PushPipelineId>,
        action: F,
    ) -> PushPipelineId
    where
        F: for<'sink> FnMut(
                PushContext,
                &'sink mut dyn PushSink,
            ) -> BoxFuture<'sink, Result<PushStatus>>
            + Send
            + 'static,
    {
        let id = PushPipelineId(self.tasks.len());
        self.tasks.push(PushPipelineTask {
            id,
            name: name.into(),
            dependencies: dependencies.into_iter().collect(),
            action: Box::new(action),
        });
        id
    }

    pub fn add_dependency(
        &mut self,
        pipeline: PushPipelineId,
        dependency: PushPipelineId,
    ) -> Result<()> {
        let task_count = self.tasks.len();
        if pipeline.0 >= task_count || dependency.0 >= task_count {
            return Err(BatchError::Internal(anyhow::anyhow!(
                "invalid push pipeline dependency: {:?} depends on {:?}, but schedule has {} pipelines",
                pipeline,
                dependency,
                task_count
            )));
        }
        self.tasks[pipeline.0].dependencies.push(dependency);
        Ok(())
    }

    pub async fn execute(
        self,
        context: PushContext,
        sink: &mut dyn PushSink,
    ) -> Result<PushStatus> {
        PushQueryScheduler::new(context)
            .execute_pipeline_schedule(self, sink)
            .await
    }

    async fn execute_inner(
        mut self,
        context: PushContext,
        sink: &mut dyn PushSink,
    ) -> Result<PushStatus> {
        let task_count = self.tasks.len();
        if task_count == 0 {
            return sink.finish().await;
        }

        let mut ready = VecDeque::new();
        let mut remaining_dependencies = vec![0; task_count];
        let mut dependents = vec![Vec::new(); task_count];
        for task in &self.tasks {
            for dependency in &task.dependencies {
                if dependency.0 >= task_count {
                    return Err(BatchError::Internal(anyhow::anyhow!(
                        "invalid push pipeline dependency: {} depends on {:?}, but schedule has {} pipelines",
                        task.name,
                        dependency,
                        task_count
                    )));
                }
                remaining_dependencies[task.id.0] += 1;
                dependents[dependency.0].push(task.id.0);
            }
        }
        for (task_idx, remaining) in remaining_dependencies.iter().enumerate() {
            if *remaining == 0 {
                ready.push_back(task_idx);
            }
        }

        let mut completed = 0;
        while let Some(task_idx) = ready.pop_front() {
            context.check_shutdown()?;
            let status = (self.tasks[task_idx].action)(context.clone(), sink).await?;
            completed += 1;
            // Internal bookkeeping pipelines must return `NeedMoreInput`. A
            // `Finished` status means a pipeline that drives the downstream
            // sink observed early completion, so the remaining schedule is
            // skipped and the downstream sink is finalized once.
            if status.is_finished() {
                return sink.finish().await;
            }

            for dependent in &dependents[task_idx] {
                remaining_dependencies[*dependent] -= 1;
                if remaining_dependencies[*dependent] == 0 {
                    ready.push_back(*dependent);
                }
            }
        }

        if completed != task_count {
            let blocked = self
                .tasks
                .iter()
                .enumerate()
                .filter(|(idx, _)| remaining_dependencies[*idx] != 0)
                .map(|(_, task)| task.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(BatchError::Internal(anyhow::anyhow!(
                "cyclic push pipeline dependency involving: {}",
                blocked
            )));
        }

        sink.finish().await
    }
}

impl Default for PushPipelineSchedule {
    fn default() -> Self {
        Self::new()
    }
}

/// Forwards chunks to another sink but suppresses `finish`.
///
/// This is used by parent operators that execute multiple child push pipelines
/// and must finalize their downstream sink only after all children have run.
pub struct ForwardingNoFinishSink<'a> {
    sink: &'a mut dyn PushSink,
    finished: bool,
}

impl<'a> ForwardingNoFinishSink<'a> {
    pub fn new(sink: &'a mut dyn PushSink) -> Self {
        Self {
            sink,
            finished: false,
        }
    }
}

impl PushSink for ForwardingNoFinishSink<'_> {
    fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            let status = self.sink.push(chunk).await?;
            self.finished |= status.is_finished();
            Ok(status)
        }
        .boxed()
    }

    fn finish<'a>(&'a mut self) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            if self.finished {
                Ok(PushStatus::Finished)
            } else {
                Ok(PushStatus::NeedMoreInput)
            }
        }
        .boxed()
    }

    fn requires_input_order(&self) -> bool {
        self.sink.requires_input_order()
    }
}

/// A chunk-sized scheduling unit.
#[derive(Debug)]
pub struct Morsel {
    sequence: u64,
    chunk: DataChunk,
}

impl Morsel {
    pub fn new(sequence: u64, chunk: DataChunk) -> Self {
        Self { sequence, chunk }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn into_chunk(self) -> DataChunk {
        self.chunk
    }
}

/// A bounded morsel queue used by the task-local scheduler.
#[derive(Debug)]
pub struct MorselQueue {
    queue: VecDeque<Morsel>,
    next_sequence: u64,
    capacity: usize,
}

impl MorselQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
            next_sequence: 0,
            capacity,
        }
    }

    pub fn has_capacity(&self) -> bool {
        self.queue.len() < self.capacity
    }

    pub fn push_chunk(&mut self, chunk: DataChunk) -> Option<u64> {
        if !self.has_capacity() {
            return None;
        }
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.queue.push_back(Morsel::new(sequence, chunk));
        Some(sequence)
    }

    pub fn push_morsel(&mut self, morsel: Morsel) -> bool {
        if !self.has_capacity() {
            return false;
        }
        self.queue.push_back(morsel);
        true
    }

    pub fn pop(&mut self) -> Option<Morsel> {
        self.queue.pop_front()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }
}

struct MorselScheduler<'a, S> {
    source: &'a mut S,
    context: PushContext,
    queue: MorselQueue,
    source_exhausted: bool,
}

impl<'a, S> MorselScheduler<'a, S>
where
    S: MorselSource,
{
    fn new(source: &'a mut S, context: PushContext) -> Self {
        let queue_capacity = context.morsel_queue_capacity();
        Self {
            source,
            context,
            queue: MorselQueue::new(queue_capacity),
            source_exhausted: false,
        }
    }

    async fn next_morsel(&mut self) -> Result<Option<Morsel>> {
        if let Some(morsel) = self.queue.pop() {
            return Ok(Some(morsel));
        }

        while !self.source_exhausted && self.queue.has_capacity() {
            match self.source.next_morsel(&self.context).await? {
                Some(morsel) => {
                    assert!(self.queue.push_morsel(morsel));
                }
                None => {
                    self.source_exhausted = true;
                }
            }
        }

        Ok(self.queue.pop())
    }
}

/// A morsel source drives a pipeline by producing chunk-sized work units.
pub trait MorselSource: Send {
    fn next_morsel<'a>(
        &'a mut self,
        context: &'a PushContext,
    ) -> BoxFuture<'a, Result<Option<Morsel>>>;

    fn into_parallel_sources(self, _parallelism: usize) -> Vec<Self>
    where
        Self: Sized,
    {
        vec![self]
    }
}

pub fn split_morsel_source_items<T>(items: Vec<T>, parallelism: usize) -> Vec<Vec<T>> {
    let parallelism = parallelism.max(1).min(items.len().max(1));
    let mut partitions = (0..parallelism).map(|_| Vec::new()).collect::<Vec<_>>();
    for (idx, item) in items.into_iter().enumerate() {
        partitions[idx % parallelism].push(item);
    }
    partitions
        .into_iter()
        .filter(|items| !items.is_empty())
        .collect()
}

/// Output produced by one pipeline operator for a single input chunk.
pub struct PipelineOperatorOutput {
    chunks: Vec<DataChunk>,
    status: PushStatus,
}

impl PipelineOperatorOutput {
    pub fn new(chunks: Vec<DataChunk>, status: PushStatus) -> Self {
        Self { chunks, status }
    }

    pub fn one(chunk: DataChunk) -> Self {
        Self::new(vec![chunk], PushStatus::NeedMoreInput)
    }

    pub fn empty() -> Self {
        Self::new(vec![], PushStatus::NeedMoreInput)
    }

    pub fn finished(chunks: Vec<DataChunk>) -> Self {
        Self::new(chunks, PushStatus::Finished)
    }
}

/// A linear pipeline operator owned by a morsel driver.
///
/// Unlike [`PushSink`], this represents an operator inside a scheduler-owned
/// pipeline: the driver feeds chunks through each operator and owns the final
/// sink call. This is the foundation for moving simple pipelines away from
/// recursive child-to-sink execution.
pub trait BatchPipelineOperator: Send {
    fn execute<'a>(&'a mut self, chunk: DataChunk)
    -> BoxFuture<'a, Result<PipelineOperatorOutput>>;

    fn finish<'a>(&'a mut self) -> BoxFuture<'a, Result<PipelineOperatorOutput>> {
        async { Ok(PipelineOperatorOutput::empty()) }.boxed()
    }
}

/// Creates per-worker operators for parallel morsel drivers.
pub trait BatchPipelineOperatorFactory: Send + Sync {
    fn create(&self) -> Box<dyn BatchPipelineOperator>;
}

/// A collected linear operator chain.
///
/// `operators` is always available for the single-driver path. `factories`
/// remains present only while every operator in the chain can be recreated as
/// worker-local state for the parallel morsel driver.
pub struct BatchPipelineOperatorChain {
    operators: Vec<Box<dyn BatchPipelineOperator>>,
    factories: Option<Vec<Arc<dyn BatchPipelineOperatorFactory>>>,
}

impl BatchPipelineOperatorChain {
    pub fn empty() -> Self {
        Self {
            operators: vec![],
            factories: Some(vec![]),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    pub fn prepend_parallel(
        &mut self,
        operator: Box<dyn BatchPipelineOperator>,
        factory: Arc<dyn BatchPipelineOperatorFactory>,
    ) {
        self.operators.insert(0, operator);
        if let Some(factories) = &mut self.factories {
            factories.insert(0, factory);
        }
    }

    pub fn prepend_single(&mut self, operator: Box<dyn BatchPipelineOperator>) {
        self.operators.insert(0, operator);
        self.factories = None;
    }

    fn into_operators(self) -> Vec<Box<dyn BatchPipelineOperator>> {
        self.operators
    }

    fn into_driver_plan(self, parallelism: usize) -> BatchPipelineOperatorDriverPlan {
        if parallelism > 1
            && let Some(factories) = self.factories
        {
            BatchPipelineOperatorDriverPlan::Parallel(factories)
        } else {
            BatchPipelineOperatorDriverPlan::Single(self.operators)
        }
    }
}

enum BatchPipelineOperatorDriverPlan {
    Single(Vec<Box<dyn BatchPipelineOperator>>),
    Parallel(Vec<Arc<dyn BatchPipelineOperatorFactory>>),
}

/// Applies a driver-style operator chain to chunks produced by an executor
/// that is not yet a native morsel source.
pub struct PipelineOperatorPushSink<'a> {
    operators: Vec<Box<dyn BatchPipelineOperator>>,
    downstream: &'a mut dyn PushSink,
}

impl<'a> PipelineOperatorPushSink<'a> {
    pub fn new(operators: BatchPipelineOperatorChain, downstream: &'a mut dyn PushSink) -> Self {
        Self {
            operators: operators.into_operators(),
            downstream,
        }
    }
}

impl PushSink for PipelineOperatorPushSink<'_> {
    fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            let output = MorselPipelineDriver::<StreamMorselSource>::run_operators_from(
                &mut self.operators,
                0,
                vec![chunk],
            )
            .await?;
            let status = MorselPipelineDriver::<StreamMorselSource>::push_outputs(
                output.chunks,
                self.downstream,
            )
            .await?;
            if status.is_finished() || output.status.is_finished() {
                Ok(PushStatus::Finished)
            } else {
                Ok(PushStatus::NeedMoreInput)
            }
        }
        .boxed()
    }

    fn finish<'a>(&'a mut self) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            for operator_idx in 0..self.operators.len() {
                let output = self.operators[operator_idx].finish().await?;
                let output = MorselPipelineDriver::<StreamMorselSource>::run_operators_from(
                    &mut self.operators,
                    operator_idx + 1,
                    output.chunks,
                )
                .await?;
                let status = MorselPipelineDriver::<StreamMorselSource>::push_outputs(
                    output.chunks,
                    self.downstream,
                )
                .await?;
                if status.is_finished() || output.status.is_finished() {
                    return self.downstream.finish().await;
                }
            }
            self.downstream.finish().await
        }
        .boxed()
    }

    fn requires_input_order(&self) -> bool {
        self.downstream.requires_input_order()
    }
}

pub struct StreamMorselSource {
    stream: BoxedDataChunkStream,
    next_sequence: u64,
}

impl StreamMorselSource {
    pub fn new(stream: BoxedDataChunkStream) -> Self {
        Self {
            stream,
            next_sequence: 0,
        }
    }
}

impl MorselSource for StreamMorselSource {
    fn next_morsel<'a>(
        &'a mut self,
        context: &'a PushContext,
    ) -> BoxFuture<'a, Result<Option<Morsel>>> {
        async move {
            context.check_shutdown()?;
            let Some(chunk) = self.stream.next().await else {
                return Ok(None);
            };
            let sequence = self.next_sequence;
            self.next_sequence += 1;
            Ok(Some(Morsel::new(sequence, chunk?)))
        }
        .boxed()
    }
}

struct MorselSenderSink {
    sender: mpsc::Sender<Result<Morsel>>,
    next_sequence: u64,
    requires_input_order: bool,
}

impl PushSink for MorselSenderSink {
    fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            let sequence = self.next_sequence;
            self.next_sequence += 1;
            self.sender
                .send(Ok(Morsel::new(sequence, chunk)))
                .await
                .map_err(|_| BatchError::SenderError)?;
            Ok(PushStatus::NeedMoreInput)
        }
        .boxed()
    }

    fn requires_input_order(&self) -> bool {
        self.requires_input_order
    }
}

struct MorselDispatcher<T> {
    senders: Vec<mpsc::Sender<T>>,
    next_worker: Arc<AtomicUsize>,
}

impl<T> Clone for MorselDispatcher<T> {
    fn clone(&self) -> Self {
        Self {
            senders: self.senders.clone(),
            next_worker: self.next_worker.clone(),
        }
    }
}

impl<T> MorselDispatcher<T> {
    fn new(senders: Vec<mpsc::Sender<T>>) -> Self {
        Self {
            senders,
            next_worker: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn send(&self, mut item: T) -> bool {
        if self.senders.is_empty() {
            return false;
        }

        let start = self.next_worker.fetch_add(1, Ordering::Relaxed);
        let mut first_full = None;
        for offset in 0..self.senders.len() {
            let idx = (start + offset) % self.senders.len();
            match self.senders[idx].try_send(item) {
                Ok(()) => return true,
                Err(mpsc::error::TrySendError::Full(error)) => {
                    if first_full.is_none() {
                        first_full = Some(idx);
                    }
                    item = error;
                }
                Err(mpsc::error::TrySendError::Closed(error)) => {
                    item = error;
                }
            }
        }

        if let Some(idx) = first_full {
            match self.senders[idx].send(item).await {
                Ok(()) => return true,
                Err(_error) => {}
            }
        }

        false
    }
}

fn worker_channels<T>(
    parallelism: usize,
    capacity: usize,
) -> (MorselDispatcher<T>, Vec<mpsc::Receiver<T>>) {
    let mut senders = Vec::with_capacity(parallelism);
    let mut receivers = Vec::with_capacity(parallelism);
    for _ in 0..parallelism {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        senders.push(sender);
        receivers.push(receiver);
    }
    (MorselDispatcher::new(senders), receivers)
}

struct MorselDispatchSink {
    dispatcher: MorselDispatcher<Result<Morsel>>,
    next_sequence: u64,
}

impl PushSink for MorselDispatchSink {
    fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            let sequence = self.next_sequence;
            self.next_sequence += 1;
            if !self.dispatcher.send(Ok(Morsel::new(sequence, chunk))).await {
                return Err(BatchError::SenderError);
            }
            Ok(PushStatus::NeedMoreInput)
        }
        .boxed()
    }

    fn requires_input_order(&self) -> bool {
        false
    }
}

/// A morsel source backed by a push executor.
///
/// The child executor remains push-native: it is driven into a morsel channel
/// by a small sink, and the scheduler-owned morsel driver consumes that channel
/// with worker-local operator state. This is the forward path for breaker probe
/// pipelines that need morsel parallelism without falling back to a pull stream.
pub struct ExecutorMorselSource {
    executor: Option<BoxedExecutor>,
    receiver: Option<mpsc::Receiver<Result<Morsel>>>,
    handle: Option<tokio::task::JoinHandle<Result<()>>>,
    capacity: usize,
    requires_input_order: bool,
}

impl ExecutorMorselSource {
    pub fn new(executor: BoxedExecutor, capacity: usize) -> Self {
        Self {
            executor: Some(executor),
            receiver: None,
            handle: None,
            capacity,
            requires_input_order: true,
        }
    }

    fn start(&mut self, context: PushContext) {
        if self.receiver.is_some() {
            return;
        }

        let executor = self
            .executor
            .take()
            .expect("executor morsel source should be started once");
        let (sender, receiver) = mpsc::channel(self.capacity.max(1));
        let task_sender = sender.clone();
        let mut sink = MorselSenderSink {
            sender,
            next_sequence: 0,
            requires_input_order: self.requires_input_order,
        };
        let task_scope = context.task_scope();
        let expr_context = task_scope
            .is_none()
            .then(|| capture_expr_context().ok())
            .flatten();

        self.receiver = Some(receiver);
        self.handle = Some(tokio::spawn(async move {
            let exec = async move {
                if let Err(error) = executor.execute_push(context, &mut sink).await {
                    let _ = task_sender.send(Err(error)).await;
                }
            }
            .boxed();
            if let Some(task_scope) = task_scope {
                task_scope.scope(exec).await;
            } else if let Some(expr_context) = expr_context {
                expr_context_scope(expr_context, exec).await;
            } else {
                exec.await;
            }
            Ok(())
        }));
    }
}

impl MorselSource for ExecutorMorselSource {
    fn next_morsel<'a>(
        &'a mut self,
        context: &'a PushContext,
    ) -> BoxFuture<'a, Result<Option<Morsel>>> {
        async move {
            context.check_shutdown()?;
            self.start(context.clone());

            let receiver = self
                .receiver
                .as_mut()
                .expect("executor morsel source receiver should be initialized");
            match receiver.recv().await {
                Some(Ok(morsel)) => Ok(Some(morsel)),
                Some(Err(error)) => Err(error),
                None => {
                    if let Some(handle) = self.handle.take() {
                        match handle.await {
                            Ok(result) => result?,
                            Err(error) if error.is_cancelled() => {}
                            Err(error) => {
                                return Err(BatchError::Internal(anyhow::anyhow!(
                                    error.to_string()
                                )));
                            }
                        }
                    }
                    Ok(None)
                }
            }
        }
        .boxed()
    }

    fn into_parallel_sources(mut self, _parallelism: usize) -> Vec<Self> {
        self.requires_input_order = false;
        vec![self]
    }
}

/// Scheduler-owned task-local morsel pipeline driver.
///
/// A source emits morsels, the driver owns the linear operator chain, and only
/// the driver talks to the final sink. This is the execution shape we want for
/// real morsel-driven pipelines.
pub struct MorselPipelineDriver<S> {
    source: S,
    operators: Vec<Box<dyn BatchPipelineOperator>>,
    context: PushContext,
}

impl<S> MorselPipelineDriver<S>
where
    S: MorselSource,
{
    pub fn new(
        source: S,
        operators: Vec<Box<dyn BatchPipelineOperator>>,
        context: PushContext,
    ) -> Self {
        Self {
            source,
            operators,
            context,
        }
    }

    pub fn source_only(source: S, context: PushContext) -> Self {
        Self::new(source, vec![], context)
    }

    pub async fn execute(&mut self, sink: &mut dyn PushSink) -> Result<PushStatus> {
        let operators = &mut self.operators;
        let context = self.context.clone();
        let mut processed = 0;
        let mut scheduler = MorselScheduler::new(&mut self.source, context.clone());
        while let Some(morsel) = scheduler.next_morsel().await? {
            context.check_shutdown()?;
            let output = Self::run_operators_from(operators, 0, vec![morsel.into_chunk()]).await?;
            let status = Self::push_outputs(output.chunks, sink).await?;
            if status.is_finished() {
                return sink.finish().await;
            }
            if output.status.is_finished() {
                return sink.finish().await;
            }
            processed += 1;
            if processed >= context.morsel_budget() {
                tokio::task::yield_now().await;
                processed = 0;
            }
        }
        for operator_idx in 0..operators.len() {
            let output = operators[operator_idx].finish().await?;
            let output =
                Self::run_operators_from(operators, operator_idx + 1, output.chunks).await?;
            let status = Self::push_outputs(output.chunks, sink).await?;
            if status.is_finished() || output.status.is_finished() {
                return sink.finish().await;
            }
        }
        sink.finish().await
    }

    pub(crate) async fn run_operators_from(
        operators: &mut [Box<dyn BatchPipelineOperator>],
        start: usize,
        mut chunks: Vec<DataChunk>,
    ) -> Result<PipelineOperatorOutput> {
        let mut status = PushStatus::NeedMoreInput;
        for operator in operators.iter_mut().skip(start) {
            let mut next_chunks = Vec::new();
            for chunk in chunks {
                let output = operator.execute(chunk).await?;
                status = output.status;
                next_chunks.extend(output.chunks);
                if status.is_finished() {
                    break;
                }
            }
            chunks = next_chunks;
            if chunks.is_empty() || status.is_finished() {
                break;
            }
        }
        Ok(PipelineOperatorOutput::new(chunks, status))
    }

    pub(crate) async fn push_outputs(
        chunks: Vec<DataChunk>,
        sink: &mut dyn PushSink,
    ) -> Result<PushStatus> {
        for chunk in chunks {
            if sink.push(chunk).await?.is_finished() {
                return Ok(PushStatus::Finished);
            }
        }
        Ok(PushStatus::NeedMoreInput)
    }
}

/// Compatibility name for older tests and source-only pipelines.
pub type PipelineExecutor<S> = MorselPipelineDriver<S>;

/// Parallel scheduler-owned morsel pipeline driver.
///
/// This runs source producers, multiple worker-local operator chains, and a
/// single final sink. Operators are created per worker so worker state is
/// isolated. Ordered sinks receive chunks in source sequence order; unordered
/// sinks can consume worker outputs as soon as they complete.
pub struct ParallelMorselPipelineDriver<S> {
    source: S,
    operator_factories: Vec<Arc<dyn BatchPipelineOperatorFactory>>,
    context: PushContext,
}

impl<S> ParallelMorselPipelineDriver<S>
where
    S: MorselSource + 'static,
{
    pub fn new(
        source: S,
        operator_factories: Vec<Arc<dyn BatchPipelineOperatorFactory>>,
        context: PushContext,
    ) -> Self {
        Self {
            source,
            operator_factories,
            context,
        }
    }

    pub fn source_only(source: S, context: PushContext) -> Self {
        Self::new(source, vec![], context)
    }

    pub async fn execute(self, sink: &mut dyn PushSink) -> Result<PushStatus> {
        let Self {
            source,
            operator_factories,
            context,
        } = self;
        let parallelism = context.morsel_parallelism().max(1);
        if parallelism == 1 {
            let mut driver = MorselPipelineDriver::new(
                source,
                operator_factories
                    .into_iter()
                    .map(|factory| factory.create())
                    .collect(),
                context,
            );
            return driver.execute(sink).await;
        }

        let (output_tx, mut output_rx) = mpsc::channel(context.morsel_queue_capacity());
        let preserve_order = sink.requires_input_order();
        let sources = if preserve_order {
            vec![source]
        } else {
            source.into_parallel_sources(parallelism)
        };
        let (dispatcher, worker_receivers) =
            worker_channels(parallelism, context.morsel_queue_capacity());

        let producer_context = context.clone();
        let mut tasks = Vec::with_capacity(sources.len() + parallelism);
        for mut source in sources {
            let dispatcher = dispatcher.clone();
            let producer_context = producer_context.clone();
            let producer_output_tx = output_tx.clone();
            tasks.push(tokio::spawn(async move {
                let mut scheduler = MorselScheduler::new(&mut source, producer_context.clone());
                loop {
                    let next = match scheduler.next_morsel().await {
                        Ok(next) => next,
                        Err(error) => {
                            let _ = producer_output_tx
                                .send(ParallelDriverOutput::error(error))
                                .await;
                            break;
                        }
                    };
                    let Some(morsel) = next else {
                        break;
                    };
                    if !dispatcher.send(morsel).await {
                        break;
                    }
                }
                Ok::<_, BatchError>(())
            }));
        }
        drop(dispatcher);

        for mut rx in worker_receivers {
            let tx = output_tx.clone();
            let context = context.clone();
            let factories = operator_factories.clone();
            tasks.push(tokio::spawn(async move {
                let mut operators = factories
                    .into_iter()
                    .map(|factory| factory.create())
                    .collect::<Vec<_>>();
                loop {
                    let next = rx.recv().await;
                    let Some(morsel) = next else {
                        break;
                    };
                    context.check_shutdown()?;
                    let sequence = morsel.sequence();
                    let output = MorselPipelineDriver::<StreamMorselSource>::run_operators_from(
                        &mut operators,
                        0,
                        vec![morsel.into_chunk()],
                    )
                    .await?;
                    let driver_output = ParallelDriverOutput::ok(sequence, output);
                    if tx.send(driver_output).await.is_err() {
                        break;
                    }
                }
                Ok::<_, BatchError>(())
            }));
        }
        drop(output_tx);

        let mut next_sequence = 0;
        let mut pending = BTreeMap::new();
        while let Some(output) = output_rx.recv().await {
            let output = match output.into_result() {
                Ok(output) => output,
                Err(error) => {
                    abort_tasks(&tasks);
                    return Err(error);
                }
            };
            if !preserve_order {
                let output = output.output;
                let status =
                    MorselPipelineDriver::<StreamMorselSource>::push_outputs(output.chunks, sink)
                        .await?;
                if status.is_finished() || output.status.is_finished() {
                    abort_tasks(&tasks);
                    return sink.finish().await;
                }
                continue;
            }

            pending.insert(output.sequence, output.output);
            while let Some(output) = pending.remove(&next_sequence) {
                let status =
                    MorselPipelineDriver::<StreamMorselSource>::push_outputs(output.chunks, sink)
                        .await?;
                next_sequence += 1;
                if status.is_finished() || output.status.is_finished() {
                    abort_tasks(&tasks);
                    return sink.finish().await;
                }
            }
        }

        for task in tasks {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) if error.is_cancelled() => {}
                Err(error) => return Err(BatchError::Internal(anyhow::anyhow!(error.to_string()))),
            }
        }

        sink.finish().await
    }
}

struct ParallelDriverOutput {
    result: Result<ParallelDriverOutputData>,
}

impl ParallelDriverOutput {
    fn ok(sequence: u64, output: PipelineOperatorOutput) -> Self {
        Self {
            result: Ok(ParallelDriverOutputData { sequence, output }),
        }
    }

    fn error(error: BatchError) -> Self {
        Self { result: Err(error) }
    }

    fn into_result(self) -> Result<ParallelDriverOutputData> {
        self.result
    }
}

struct ParallelDriverOutputData {
    sequence: u64,
    output: PipelineOperatorOutput,
}

fn abort_tasks(tasks: &[tokio::task::JoinHandle<Result<()>>]) {
    for task in tasks {
        task.abort();
    }
}

/// Drives a stream-backed source into a push sink.
///
/// Some batch leaf executors still receive data from connector or storage APIs
/// that naturally expose streams. Keep them source-driven here instead of going
/// through the executor pull adapter.
pub async fn push_chunk_stream(
    stream: BoxedDataChunkStream,
    context: PushContext,
    sink: &mut dyn PushSink,
) -> Result<PushStatus> {
    push_chunk_stream_with_operators(stream, BatchPipelineOperatorChain::empty(), context, sink)
        .await
}

/// Drives a stream-backed source through a driver-owned operator chain.
pub async fn push_chunk_stream_with_operators(
    stream: BoxedDataChunkStream,
    operators: BatchPipelineOperatorChain,
    context: PushContext,
    sink: &mut dyn PushSink,
) -> Result<PushStatus> {
    if context.morsel_parallelism() == 1 {
        return push_chunk_stream_serial(stream, operators, context, sink).await;
    }

    let source = StreamMorselSource::new(stream);
    drive_morsel_source_with_operators(source, operators, context, sink).await
}

async fn push_chunk_stream_serial(
    stream: BoxedDataChunkStream,
    operators: BatchPipelineOperatorChain,
    context: PushContext,
    sink: &mut dyn PushSink,
) -> Result<PushStatus> {
    if operators.is_empty() {
        push_chunk_stream_direct(stream, context, sink).await
    } else {
        let mut pipeline_sink = PipelineOperatorPushSink::new(operators, sink);
        push_chunk_stream_direct(stream, context, &mut pipeline_sink).await
    }
}

async fn push_chunk_stream_direct(
    mut stream: BoxedDataChunkStream,
    context: PushContext,
    sink: &mut dyn PushSink,
) -> Result<PushStatus> {
    let mut processed = 0;
    while let Some(chunk) = stream.next().await {
        context.check_shutdown()?;
        if sink.push(chunk?).await?.is_finished() {
            return sink.finish().await;
        }
        processed += 1;
        if processed >= context.morsel_budget() {
            tokio::task::yield_now().await;
            processed = 0;
        }
    }
    sink.finish().await
}

/// Drives a native morsel source through a collected operator chain.
pub async fn drive_morsel_source_with_operators<S>(
    source: S,
    operators: BatchPipelineOperatorChain,
    context: PushContext,
    sink: &mut dyn PushSink,
) -> Result<PushStatus>
where
    S: MorselSource + 'static,
{
    if operators.is_empty() {
        ParallelMorselPipelineDriver::source_only(source, context)
            .execute(sink)
            .await
    } else {
        match operators.into_driver_plan(context.morsel_parallelism()) {
            BatchPipelineOperatorDriverPlan::Single(operators) => {
                MorselPipelineDriver::new(source, operators, context)
                    .execute(sink)
                    .await
            }
            BatchPipelineOperatorDriverPlan::Parallel(factories) => {
                ParallelMorselPipelineDriver::new(source, factories, context)
                    .execute(sink)
                    .await
            }
        }
    }
}

/// Drives a push executor into worker-local sinks and returns those local states.
///
/// This is the local/global sink-state hook used by breaker operators. Each
/// worker owns one local sink and consumes morsels from the scheduler queue.
/// The caller is responsible for combining the returned local states and
/// running the operator-specific finalize step.
pub async fn drive_push_executor_into_parallel_sinks<F, L>(
    executor: BoxedExecutor,
    context: PushContext,
    create_local_sink: F,
) -> Result<Vec<L>>
where
    F: Fn() -> L + Send + Sync + 'static,
    L: PushSink + Send + 'static,
{
    let parallelism = context.morsel_parallelism().max(1);
    if parallelism == 1 {
        let mut sink = create_local_sink();
        executor.execute_push(context, &mut sink).await?;
        sink.finish().await?;
        return Ok(vec![sink]);
    }

    let (dispatcher, worker_receivers) =
        worker_channels(parallelism, context.morsel_queue_capacity());
    let create_local_sink = Arc::new(create_local_sink);
    let producer_context = context.clone();
    let producer_dispatcher = dispatcher.clone();
    let producer = tokio::spawn(async move {
        let mut sink = MorselDispatchSink {
            dispatcher: producer_dispatcher.clone(),
            next_sequence: 0,
        };
        if let Err(error) = executor.execute_push(producer_context, &mut sink).await {
            let _ = producer_dispatcher.send(Err(error)).await;
        }
        Ok::<_, BatchError>(())
    });
    drop(dispatcher);

    let mut tasks = vec![];
    for mut rx in worker_receivers {
        let context = context.clone();
        let create_local_sink = create_local_sink.clone();
        tasks.push(tokio::spawn(async move {
            let mut sink = create_local_sink();
            loop {
                let next = rx.recv().await;
                let Some(morsel) = next else {
                    break;
                };
                let morsel = morsel?;
                context.check_shutdown()?;
                if sink.push(morsel.into_chunk()).await?.is_finished() {
                    break;
                }
            }
            sink.finish().await?;
            Ok::<_, BatchError>(sink)
        }));
    }

    match producer.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            abort_local_sink_tasks(&tasks);
            return Err(error);
        }
        Err(error) if error.is_cancelled() => {}
        Err(error) => {
            abort_local_sink_tasks(&tasks);
            return Err(BatchError::Internal(anyhow::anyhow!(error.to_string())));
        }
    }

    let mut sinks = Vec::with_capacity(tasks.len());
    for task in tasks {
        match task.await {
            Ok(Ok(sink)) => sinks.push(sink),
            Ok(Err(error)) => return Err(error),
            Err(error) if error.is_cancelled() => {}
            Err(error) => return Err(BatchError::Internal(anyhow::anyhow!(error.to_string()))),
        }
    }
    Ok(sinks)
}

fn abort_local_sink_tasks<L>(tasks: &[tokio::task::JoinHandle<Result<L>>]) {
    for task in tasks {
        task.abort();
    }
}

/// Test-only adapter for validating the old pull-to-push bridge behavior.
#[cfg(test)]
async fn execute_pull_stream_as_push(
    stream: BoxedDataChunkStream,
    context: PushContext,
    sink: &mut dyn PushSink,
) -> Result<PushStatus> {
    if context.morsel_parallelism() > 1 {
        return execute_pull_stream_as_push_parallel(stream, context, sink).await;
    }

    let source = StreamMorselSource::new(stream);
    PipelineExecutor::source_only(source, context)
        .execute(sink)
        .await
}

#[cfg(test)]
async fn execute_pull_stream_as_push_parallel(
    mut stream: BoxedDataChunkStream,
    context: PushContext,
    sink: &mut dyn PushSink,
) -> Result<PushStatus> {
    let (output_tx, mut output_rx) = mpsc::channel(context.morsel_queue_capacity());
    let (dispatcher, worker_receivers) = worker_channels(
        context.morsel_parallelism(),
        context.morsel_queue_capacity(),
    );

    let producer_context = context.clone();
    let producer_output_tx = output_tx.clone();
    let producer_dispatcher = dispatcher.clone();
    let producer = tokio::spawn(async move {
        let mut sequence = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = match producer_context.check_shutdown().and(chunk) {
                Ok(chunk) => chunk,
                Err(error) => {
                    let _ = producer_output_tx.send(Err(error)).await;
                    break;
                }
            };

            if !producer_dispatcher.send(Morsel::new(sequence, chunk)).await {
                break;
            }
            sequence += 1;
        }
    });
    drop(dispatcher);

    let mut tasks = vec![producer];
    for mut rx in worker_receivers {
        let tx = output_tx.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let next = rx.recv().await;
                let Some(morsel) = next else {
                    break;
                };
                if tx.send(Ok(morsel)).await.is_err() {
                    break;
                }
            }
        }));
    }
    drop(output_tx);

    let mut next_sequence = 0;
    let mut pending = BTreeMap::new();
    while let Some(morsel) = output_rx.recv().await {
        let morsel = morsel?;
        pending.insert(morsel.sequence(), morsel.into_chunk());
        while let Some(chunk) = pending.remove(&next_sequence) {
            let status = sink.push(chunk).await?;
            next_sequence += 1;
            if status.is_finished() {
                for task in tasks {
                    task.abort();
                }
                return sink.finish().await;
            }
        }
    }

    sink.finish().await
}

struct ChannelPushSink {
    sender: mpsc::Sender<Result<DataChunk>>,
}

impl PushSink for ChannelPushSink {
    fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            self.sender
                .send(Ok(chunk))
                .await
                .map_err(|_| BatchError::SenderError)?;
            Ok(PushStatus::NeedMoreInput)
        }
        .boxed()
    }
}

/// Compatibility bridge for algorithms that still consume ordered stream inputs.
#[try_stream(boxed, ok = DataChunk, error = BatchError)]
pub async fn execute_push_as_pull(executor: BoxedExecutor, context: PushContext, buffer: usize) {
    let (sender, mut receiver) = mpsc::channel(buffer.max(1));
    let mut sink = ChannelPushSink {
        sender: sender.clone(),
    };
    let task_scope = context.task_scope();
    let expr_context = task_scope
        .is_none()
        .then(|| capture_expr_context().ok())
        .flatten();

    tokio::spawn(async move {
        let exec = async move {
            if let Err(error) = executor.execute_push(context, &mut sink).await {
                let _ = sender.send(Err(error)).await;
            }
        }
        .boxed();
        if let Some(task_scope) = task_scope {
            task_scope.scope(exec).await;
        } else if let Some(expr_context) = expr_context {
            expr_context_scope(expr_context, exec).await;
        } else {
            exec.await;
        }
    });

    while let Some(data_chunk) = receiver.recv().await {
        yield data_chunk?;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::{StreamExt, stream};
    use risingwave_common::catalog::Schema;

    use super::*;
    use crate::executor::Executor;

    #[test]
    fn test_morsel_queue_is_bounded_and_ordered() {
        let mut queue = MorselQueue::new(2);
        assert_eq!(queue.push_chunk(DataChunk::new_dummy(1)), Some(0));
        assert_eq!(queue.push_chunk(DataChunk::new_dummy(1)), Some(1));
        assert_eq!(queue.push_chunk(DataChunk::new_dummy(1)), None);
        assert_eq!(queue.len(), 2);

        let first = queue.pop().unwrap();
        let second = queue.pop().unwrap();
        assert_eq!(first.sequence(), 0);
        assert_eq!(second.sequence(), 1);
        assert!(queue.pop().is_none());
    }

    struct CollectSink {
        cardinalities: Vec<usize>,
    }

    impl PushSink for CollectSink {
        fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>> {
            async move {
                self.cardinalities.push(chunk.cardinality());
                Ok(PushStatus::NeedMoreInput)
            }
            .boxed()
        }
    }

    struct UnorderedCollectSink {
        cardinalities: Vec<usize>,
    }

    impl PushSink for UnorderedCollectSink {
        fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>> {
            async move {
                self.cardinalities.push(chunk.cardinality());
                Ok(PushStatus::NeedMoreInput)
            }
            .boxed()
        }

        fn requires_input_order(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn test_pull_stream_as_push_adapter() {
        let stream = stream::iter(vec![
            Ok(DataChunk::new_dummy(2)),
            Ok(DataChunk::new_dummy(3)),
        ])
        .boxed();
        let mut sink = CollectSink {
            cardinalities: vec![],
        };

        let status = execute_pull_stream_as_push(
            stream,
            PushContext::new(ShutdownToken::empty()),
            &mut sink,
        )
        .await
        .unwrap();

        assert_eq!(status, PushStatus::NeedMoreInput);
        assert_eq!(sink.cardinalities, vec![2, 3]);
    }

    #[tokio::test]
    async fn test_morsel_scheduler_drives_bounded_queue() {
        let stream = stream::iter(vec![
            Ok(DataChunk::new_dummy(2)),
            Ok(DataChunk::new_dummy(3)),
            Ok(DataChunk::new_dummy(4)),
        ])
        .boxed();
        let context = PushContext::new(ShutdownToken::empty()).with_morsel_queue_capacity(2);
        let mut source = StreamMorselSource::new(stream);
        let mut scheduler = MorselScheduler::new(&mut source, context);

        let first = scheduler.next_morsel().await.unwrap().unwrap();
        let second = scheduler.next_morsel().await.unwrap().unwrap();
        let third = scheduler.next_morsel().await.unwrap().unwrap();

        assert_eq!(first.sequence(), 0);
        assert_eq!(second.sequence(), 1);
        assert_eq!(third.sequence(), 2);
        assert!(scheduler.next_morsel().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_push_pipeline_schedule_respects_dependencies() {
        use std::sync::Mutex as StdMutex;

        let order = Arc::new(StdMutex::new(vec![]));
        let mut schedule = PushPipelineSchedule::new();
        let first = {
            let order = order.clone();
            schedule.add_pipeline("first", [], move |_, _| {
                let order = order.clone();
                async move {
                    order.lock().unwrap().push("first");
                    Ok(PushStatus::NeedMoreInput)
                }
                .boxed()
            })
        };
        let second = {
            let order = order.clone();
            schedule.add_pipeline("second", [first], move |_, _| {
                let order = order.clone();
                async move {
                    order.lock().unwrap().push("second");
                    Ok(PushStatus::NeedMoreInput)
                }
                .boxed()
            })
        };
        {
            let order = order.clone();
            schedule.add_pipeline("third", [second], move |_, _| {
                let order = order.clone();
                async move {
                    order.lock().unwrap().push("third");
                    Ok(PushStatus::NeedMoreInput)
                }
                .boxed()
            });
        }
        let mut sink = CollectSink {
            cardinalities: vec![],
        };

        let status = schedule
            .execute(PushContext::new(ShutdownToken::empty()), &mut sink)
            .await
            .unwrap();

        assert_eq!(status, PushStatus::NeedMoreInput);
        assert_eq!(*order.lock().unwrap(), vec!["first", "second", "third"]);
    }

    #[tokio::test]
    async fn test_push_pipeline_schedule_rejects_cycles() {
        let mut schedule = PushPipelineSchedule::new();
        let first = schedule.add_pipeline("first", [], move |_, _| {
            async { Ok(PushStatus::NeedMoreInput) }.boxed()
        });
        let second = schedule.add_pipeline("second", [first], move |_, _| {
            async { Ok(PushStatus::NeedMoreInput) }.boxed()
        });
        schedule.add_dependency(first, second).unwrap();
        let mut sink = CollectSink {
            cardinalities: vec![],
        };

        let error = schedule
            .execute(PushContext::new(ShutdownToken::empty()), &mut sink)
            .await
            .unwrap_err();

        assert!(format!("{error:?}").contains("cyclic push pipeline dependency"));
    }

    #[tokio::test]
    async fn test_parallel_pull_stream_as_push_preserves_sequence() {
        let stream = stream::iter(vec![
            Ok(DataChunk::new_dummy(1)),
            Ok(DataChunk::new_dummy(2)),
            Ok(DataChunk::new_dummy(3)),
            Ok(DataChunk::new_dummy(4)),
        ])
        .boxed();
        let context = PushContext::new(ShutdownToken::empty())
            .with_morsel_queue_capacity(2)
            .with_morsel_parallelism(3);
        let mut sink = CollectSink {
            cardinalities: vec![],
        };

        let status = execute_pull_stream_as_push(stream, context, &mut sink)
            .await
            .unwrap();

        assert_eq!(status, PushStatus::NeedMoreInput);
        assert_eq!(sink.cardinalities, vec![1, 2, 3, 4]);
    }

    struct DoubleCardinalityOperator;

    impl BatchPipelineOperator for DoubleCardinalityOperator {
        fn execute<'a>(
            &'a mut self,
            chunk: DataChunk,
        ) -> BoxFuture<'a, Result<PipelineOperatorOutput>> {
            async move {
                Ok(PipelineOperatorOutput::one(DataChunk::new_dummy(
                    chunk.cardinality() * 2,
                )))
            }
            .boxed()
        }
    }

    struct DoubleCardinalityOperatorFactory;

    impl BatchPipelineOperatorFactory for DoubleCardinalityOperatorFactory {
        fn create(&self) -> Box<dyn BatchPipelineOperator> {
            Box::new(DoubleCardinalityOperator)
        }
    }

    struct DelayFirstOperator;

    impl BatchPipelineOperator for DelayFirstOperator {
        fn execute<'a>(
            &'a mut self,
            chunk: DataChunk,
        ) -> BoxFuture<'a, Result<PipelineOperatorOutput>> {
            async move {
                if chunk.cardinality() == 1 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Ok(PipelineOperatorOutput::one(chunk))
            }
            .boxed()
        }
    }

    struct DelayFirstOperatorFactory;

    impl BatchPipelineOperatorFactory for DelayFirstOperatorFactory {
        fn create(&self) -> Box<dyn BatchPipelineOperator> {
            Box::new(DelayFirstOperator)
        }
    }

    #[tokio::test]
    async fn test_morsel_pipeline_driver_runs_operator_chain() {
        let stream = stream::iter(vec![
            Ok(DataChunk::new_dummy(2)),
            Ok(DataChunk::new_dummy(3)),
        ])
        .boxed();
        let source = StreamMorselSource::new(stream);
        let mut driver = MorselPipelineDriver::new(
            source,
            vec![Box::new(DoubleCardinalityOperator)],
            PushContext::new(ShutdownToken::empty()),
        );
        let mut sink = CollectSink {
            cardinalities: vec![],
        };

        let status = driver.execute(&mut sink).await.unwrap();

        assert_eq!(status, PushStatus::NeedMoreInput);
        assert_eq!(sink.cardinalities, vec![4, 6]);
    }

    #[tokio::test]
    async fn test_parallel_morsel_pipeline_driver_preserves_sequence() {
        let stream = stream::iter(vec![
            Ok(DataChunk::new_dummy(1)),
            Ok(DataChunk::new_dummy(2)),
            Ok(DataChunk::new_dummy(3)),
            Ok(DataChunk::new_dummy(4)),
        ])
        .boxed();
        let source = StreamMorselSource::new(stream);
        let context = PushContext::new(ShutdownToken::empty())
            .with_morsel_queue_capacity(2)
            .with_morsel_parallelism(3);
        let driver = ParallelMorselPipelineDriver::new(
            source,
            vec![Arc::new(DoubleCardinalityOperatorFactory)],
            context,
        );
        let mut sink = CollectSink {
            cardinalities: vec![],
        };

        let status = driver.execute(&mut sink).await.unwrap();

        assert_eq!(status, PushStatus::NeedMoreInput);
        assert_eq!(sink.cardinalities, vec![2, 4, 6, 8]);
    }

    #[tokio::test]
    async fn test_parallel_morsel_pipeline_driver_skips_sequence_for_unordered_sink() {
        let stream = stream::iter(vec![
            Ok(DataChunk::new_dummy(1)),
            Ok(DataChunk::new_dummy(2)),
        ])
        .boxed();
        let source = StreamMorselSource::new(stream);
        let context = PushContext::new(ShutdownToken::empty())
            .with_morsel_queue_capacity(2)
            .with_morsel_parallelism(2);
        let driver = ParallelMorselPipelineDriver::new(
            source,
            vec![Arc::new(DelayFirstOperatorFactory)],
            context,
        );
        let mut sink = UnorderedCollectSink {
            cardinalities: vec![],
        };

        let status = driver.execute(&mut sink).await.unwrap();

        assert_eq!(status, PushStatus::NeedMoreInput);
        assert_eq!(sink.cardinalities, vec![2, 1]);
    }

    struct SplitCountingSource {
        chunks: VecDeque<DataChunk>,
        split_count: Arc<AtomicUsize>,
        next_sequence: u64,
    }

    impl MorselSource for SplitCountingSource {
        fn next_morsel<'a>(
            &'a mut self,
            _context: &'a PushContext,
        ) -> BoxFuture<'a, Result<Option<Morsel>>> {
            async move {
                let Some(chunk) = self.chunks.pop_front() else {
                    return Ok(None);
                };
                let sequence = self.next_sequence;
                self.next_sequence += 1;
                Ok(Some(Morsel::new(sequence, chunk)))
            }
            .boxed()
        }

        fn into_parallel_sources(self, parallelism: usize) -> Vec<Self> {
            let Self {
                chunks,
                split_count,
                next_sequence,
            } = self;
            if next_sequence != 0 {
                return vec![Self {
                    chunks,
                    split_count,
                    next_sequence,
                }];
            }

            let partitions = split_morsel_source_items(chunks.into_iter().collect(), parallelism);
            split_count.store(partitions.len(), Ordering::SeqCst);
            partitions
                .into_iter()
                .map(|chunks| Self {
                    chunks: chunks.into(),
                    split_count: split_count.clone(),
                    next_sequence: 0,
                })
                .collect()
        }
    }

    #[tokio::test]
    async fn test_unordered_parallel_driver_splits_sources() {
        let split_count = Arc::new(AtomicUsize::new(0));
        let source = SplitCountingSource {
            chunks: vec![
                DataChunk::new_dummy(1),
                DataChunk::new_dummy(2),
                DataChunk::new_dummy(3),
                DataChunk::new_dummy(4),
            ]
            .into(),
            split_count: split_count.clone(),
            next_sequence: 0,
        };
        let context = PushContext::new(ShutdownToken::empty())
            .with_morsel_queue_capacity(4)
            .with_morsel_parallelism(3);
        let driver = ParallelMorselPipelineDriver::source_only(source, context);
        let mut sink = UnorderedCollectSink {
            cardinalities: vec![],
        };

        let status = driver.execute(&mut sink).await.unwrap();

        assert_eq!(status, PushStatus::NeedMoreInput);
        assert_eq!(split_count.load(Ordering::SeqCst), 3);
        assert_eq!(sink.cardinalities.len(), 4);
    }

    struct FinishAfterFirstOperator {
        seen: bool,
    }

    impl BatchPipelineOperator for FinishAfterFirstOperator {
        fn execute<'a>(
            &'a mut self,
            chunk: DataChunk,
        ) -> BoxFuture<'a, Result<PipelineOperatorOutput>> {
            async move {
                assert!(!self.seen, "driver should stop after Finished");
                self.seen = true;
                Ok(PipelineOperatorOutput::finished(vec![chunk]))
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn test_morsel_pipeline_driver_stops_on_finished_operator() {
        let stream = stream::iter(vec![
            Ok(DataChunk::new_dummy(2)),
            Ok(DataChunk::new_dummy(3)),
        ])
        .boxed();
        let source = StreamMorselSource::new(stream);
        let mut driver = MorselPipelineDriver::new(
            source,
            vec![Box::new(FinishAfterFirstOperator { seen: false })],
            PushContext::new(ShutdownToken::empty()),
        );
        let mut sink = CollectSink {
            cardinalities: vec![],
        };

        let status = driver.execute(&mut sink).await.unwrap();

        assert_eq!(status, PushStatus::NeedMoreInput);
        assert_eq!(sink.cardinalities, vec![2]);
    }

    struct PushOnlyExecutor {
        schema: Schema,
    }

    impl Executor for PushOnlyExecutor {
        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn identity(&self) -> &str {
            "PushOnlyExecutor"
        }

        fn execute(self: Box<Self>) -> BoxedDataChunkStream {
            stream::empty().boxed()
        }

        fn execute_push<'a>(
            self: Box<Self>,
            _context: PushContext,
            sink: &'a mut dyn PushSink,
        ) -> BoxFuture<'a, Result<PushStatus>> {
            async move {
                sink.push(DataChunk::new_dummy(2)).await?;
                sink.push(DataChunk::new_dummy(3)).await?;
                sink.finish().await
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn test_push_as_pull_adapter() {
        let executor: BoxedExecutor = Box::new(PushOnlyExecutor {
            schema: Schema::default(),
        });

        let mut stream =
            execute_push_as_pull(executor, PushContext::new(ShutdownToken::empty()), 1);

        assert_eq!(stream.next().await.unwrap().unwrap().cardinality(), 2);
        assert_eq!(stream.next().await.unwrap().unwrap().cardinality(), 3);
        assert!(stream.next().await.is_none());
    }

    tokio::task_local! {
        static TEST_PUSH_TASK_SCOPE: &'static str;
    }

    struct TestPushTaskScope;

    impl PushTaskScope for TestPushTaskScope {
        fn scope(&self, future: BoxFuture<'static, ()>) -> BoxFuture<'static, ()> {
            TEST_PUSH_TASK_SCOPE.scope("set", future).boxed()
        }
    }

    struct ContextCheckingPushExecutor {
        schema: Schema,
    }

    impl Executor for ContextCheckingPushExecutor {
        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn identity(&self) -> &str {
            "ContextCheckingPushExecutor"
        }

        fn execute(self: Box<Self>) -> BoxedDataChunkStream {
            stream::empty().boxed()
        }

        fn execute_push<'a>(
            self: Box<Self>,
            _context: PushContext,
            sink: &'a mut dyn PushSink,
        ) -> BoxFuture<'a, Result<PushStatus>> {
            async move {
                let value = TEST_PUSH_TASK_SCOPE
                    .try_with(|value| *value)
                    .map_err(|_| BatchError::Internal(anyhow::anyhow!("task scope not set")))?;
                assert_eq!(value, "set");
                sink.push(DataChunk::new_dummy(1)).await?;
                sink.finish().await
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn test_push_as_pull_adapter_reenters_task_scope() {
        let executor: BoxedExecutor = Box::new(ContextCheckingPushExecutor {
            schema: Schema::default(),
        });
        let context =
            PushContext::new(ShutdownToken::empty()).with_task_scope(Arc::new(TestPushTaskScope));

        let mut stream = execute_push_as_pull(executor, context, 1);

        assert_eq!(stream.next().await.unwrap().unwrap().cardinality(), 1);
        assert!(stream.next().await.is_none());
    }
}
