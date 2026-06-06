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

use std::collections::VecDeque;

use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;

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
}

impl PushContext {
    pub const DEFAULT_MORSEL_BUDGET: usize = 64;

    pub fn new(shutdown_rx: ShutdownToken) -> Self {
        Self {
            shutdown_rx,
            morsel_budget: Self::DEFAULT_MORSEL_BUDGET,
        }
    }

    pub fn with_morsel_budget(mut self, morsel_budget: usize) -> Self {
        self.morsel_budget = morsel_budget.max(1);
        self
    }

    pub fn morsel_budget(&self) -> usize {
        self.morsel_budget
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

    pub fn pop(&mut self) -> Option<Morsel> {
        self.queue.pop_front()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }
}

/// A morsel source drives a pipeline by producing chunk-sized work units.
pub trait MorselSource: Send {
    fn next_morsel<'a>(
        &'a mut self,
        context: &'a PushContext,
    ) -> BoxFuture<'a, Result<Option<Morsel>>>;
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

/// Task-local pipeline executor. Sources emit morsels; the sink applies the
/// downstream operator chain and backpressures through async `push`.
pub struct PipelineExecutor<S> {
    source: S,
    context: PushContext,
}

impl<S> PipelineExecutor<S>
where
    S: MorselSource,
{
    pub fn new(source: S, context: PushContext) -> Self {
        Self { source, context }
    }

    pub async fn execute(&mut self, sink: &mut dyn PushSink) -> Result<PushStatus> {
        let mut processed = 0;
        while let Some(morsel) = self.source.next_morsel(&self.context).await? {
            self.context.check_shutdown()?;
            let status = sink.push(morsel.into_chunk()).await?;
            if status.is_finished() {
                return sink.finish().await;
            }
            processed += 1;
            if processed >= self.context.morsel_budget() {
                tokio::task::yield_now().await;
                processed = 0;
            }
        }
        sink.finish().await
    }
}

/// Adapter used while operators are migrated from pull to push.
pub async fn execute_pull_stream_as_push(
    stream: BoxedDataChunkStream,
    context: PushContext,
    sink: &mut dyn PushSink,
) -> Result<PushStatus> {
    let source = StreamMorselSource::new(stream);
    PipelineExecutor::new(source, context).execute(sink).await
}

struct ChannelPushSink {
    sender: tokio::sync::mpsc::Sender<Result<DataChunk>>,
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

/// Adapter used while pull consumers are migrated to consume push pipelines directly.
#[try_stream(boxed, ok = DataChunk, error = BatchError)]
pub async fn execute_push_as_pull(executor: BoxedExecutor, context: PushContext, buffer: usize) {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(buffer.max(1));
    let mut sink = ChannelPushSink {
        sender: sender.clone(),
    };

    tokio::spawn(async move {
        if let Err(error) = executor.execute_push(context, &mut sink).await {
            let _ = sender.send(Err(error)).await;
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
