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

use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_expr::expr_context::{capture_expr_context, expr_context_scope};
use tokio::sync::{Mutex, mpsc};

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

    pub fn morsel_budget(&self) -> usize {
        self.morsel_budget
    }

    pub fn morsel_queue_capacity(&self) -> usize {
        self.morsel_queue_capacity
    }

    pub fn morsel_parallelism(&self) -> usize {
        self.morsel_parallelism
    }

    pub fn check_shutdown(&self) -> Result<()> {
        match self.shutdown_rx.message() {
            ShutdownMsg::Init => Ok(()),
            ShutdownMsg::Abort(reason) => Err(BatchError::Aborted(reason)),
            ShutdownMsg::Cancel => Err(BatchError::Aborted("cancelled".to_owned())),
        }
    }
}

/// A push sink consumes data chunks produced by a pipeline.
pub trait PushSink: Send {
    fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>>;

    fn finish<'a>(&'a mut self) -> BoxFuture<'a, Result<PushStatus>> {
        async { Ok(PushStatus::NeedMoreInput) }.boxed()
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

    async fn run_operators_from(
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

    async fn push_outputs(chunks: Vec<DataChunk>, sink: &mut dyn PushSink) -> Result<PushStatus> {
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
/// This runs a single source producer, multiple worker-local operator chains,
/// and an ordered output merge. Operators are created per worker so worker state
/// is isolated. The final sink remains single-threaded and receives chunks in
/// source sequence order.
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
            mut source,
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

        let (morsel_tx, morsel_rx) = mpsc::channel(context.morsel_queue_capacity());
        let (output_tx, mut output_rx) = mpsc::channel(context.morsel_queue_capacity());
        let shared_rx = Arc::new(Mutex::new(morsel_rx));

        let producer_context = context.clone();
        let producer_output_tx = output_tx.clone();
        let producer = tokio::spawn(async move {
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
                if morsel_tx.send(morsel).await.is_err() {
                    break;
                }
            }
            Ok::<_, BatchError>(())
        });

        let mut tasks = vec![producer];
        for _ in 0..parallelism {
            let rx = shared_rx.clone();
            let tx = output_tx.clone();
            let context = context.clone();
            let factories = operator_factories.clone();
            tasks.push(tokio::spawn(async move {
                let mut operators = factories
                    .into_iter()
                    .map(|factory| factory.create())
                    .collect::<Vec<_>>();
                loop {
                    let next = {
                        let mut rx = rx.lock().await;
                        rx.recv().await
                    };
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
    let source = StreamMorselSource::new(stream);
    ParallelMorselPipelineDriver::source_only(source, context)
        .execute(sink)
        .await
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
    let (morsel_tx, morsel_rx) = mpsc::channel(context.morsel_queue_capacity());
    let (output_tx, mut output_rx) = mpsc::channel(context.morsel_queue_capacity());
    let shared_rx = Arc::new(Mutex::new(morsel_rx));

    let producer_context = context.clone();
    let producer_output_tx = output_tx.clone();
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

            if morsel_tx.send(Morsel::new(sequence, chunk)).await.is_err() {
                break;
            }
            sequence += 1;
        }
    });

    let mut tasks = vec![producer];
    for _ in 0..context.morsel_parallelism() {
        let rx = shared_rx.clone();
        let tx = output_tx.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let next = {
                    let mut rx = rx.lock().await;
                    rx.recv().await
                };
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
    let expr_context = capture_expr_context().ok();

    tokio::spawn(async move {
        let exec = async move {
            if let Err(error) = executor.execute_push(context, &mut sink).await {
                let _ = sender.send(Err(error)).await;
            }
        };
        if let Some(expr_context) = expr_context {
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
}
