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

use std::pin::Pin;
use std::task::Poll;

use async_stack_trace::StackTrace;
use either::Either;
use futures::stream::{select_with_strategy, BoxStream, PollNext, SelectWithStrategy};
use futures::{Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::bail;
use risingwave_source::BoxSourceWithStateStream;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::Barrier;

type SourceReaderMessage<T> = StreamExecutorResult<Either<Barrier, T>>;
type SourceReaderArm<T> = BoxStream<'static, SourceReaderMessage<T>>;
#[allow(type_alias_bounds)]
type SourceReaderStreamInner<T: Send + 'static> =
    SelectWithStrategy<SourceReaderArm<T>, SourceReaderArm<T>, impl FnMut(&mut ()) -> PollNext, ()>;

pub(super) struct SourceReaderStream<T: Send + 'static> {
    inner: SourceReaderStreamInner<T>,
    /// Whether the source stream is paused.
    paused: bool,
}

impl<T: Send + 'static> SourceReaderStream<T> {
    /// Receive barriers from barrier manager with the channel, error on channel close.
    #[try_stream(ok = Barrier, error = StreamExecutorError)]
    async fn barrier_receiver(mut rx: UnboundedReceiver<Barrier>) {
        while let Some(barrier) = rx.recv().stack_trace("source_recv_barrier").await {
            yield barrier;
        }
        bail!("barrier reader closed unexpectedly");
    }

    /// Receive chunks and states from the source reader, hang up on error.
    #[try_stream(ok = T, error = StreamExecutorError)]
    async fn source_stream(stream: BoxSourceWithStateStream<T>) {
        // TODO: support stack trace for Stream
        #[for_await]
        for chunk in stream {
            match chunk {
                Ok(chunk) => yield chunk,
                Err(err) => {
                    error!("hang up stream reader due to polling error: {}", err);
                    futures::future::pending().stack_trace("source_error").await
                }
            }
        }
    }

    /// Convert this reader to a stream.
    pub fn new(
        barrier_receiver: UnboundedReceiver<Barrier>,
        source_stream: BoxSourceWithStateStream<T>,
    ) -> Self {
        Self {
            inner: Self::new_inner(
                Self::barrier_receiver(barrier_receiver)
                    .map_ok(Either::Left)
                    .boxed(),
                Self::source_stream(source_stream)
                    .map_ok(Either::Right)
                    .boxed(),
            ),
            paused: false,
        }
    }

    fn new_inner(
        barrier_receiver_arm: SourceReaderArm<T>,
        source_stream_arm: SourceReaderArm<T>,
    ) -> SourceReaderStreamInner<T> {
        select_with_strategy(
            barrier_receiver_arm,
            source_stream_arm,
            // We prefer barrier on the left hand side over source chunks.
            |_: &mut ()| PollNext::Left,
        )
    }

    /// Replace the source stream with a new one for given `stream`. Used for split change.
    pub fn replace_source_stream(&mut self, source_stream: BoxSourceWithStateStream<T>) {
        // Take the barrier receiver arm.
        let barrier_receiver_arm = std::mem::replace(
            self.inner.get_mut().0,
            futures::stream::once(async { unreachable!("placeholder") }).boxed(),
        );

        // Note: create a new `SelectWithStrategy` instead of replacing the source stream arm here,
        // to ensure the internal state of the `SelectWithStrategy` is reset. (#6300)
        self.inner = Self::new_inner(
            barrier_receiver_arm,
            Self::source_stream(source_stream)
                .map_ok(Either::Right)
                .boxed(),
        );
    }

    /// Pause the source stream.
    pub fn pause_source(&mut self) {
        assert!(!self.paused, "already paused");
        self.paused = true;
    }

    /// Resume the source stream, panic if the source is not paused before.
    pub fn resume_source(&mut self) {
        assert!(self.paused, "not paused");
        self.paused = false;
    }
}

impl<T: Send> Stream for SourceReaderStream<T> {
    type Item = SourceReaderMessage<T>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        ctx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.paused {
            self.inner.get_mut().0.poll_next_unpin(ctx)
        } else {
            self.inner.poll_next_unpin(ctx)
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::{pin_mut, FutureExt};
    use risingwave_common::array::StreamChunk;
    use risingwave_source::TableSource;
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn test_pause_and_resume() {
        let (barrier_tx, barrier_rx) = mpsc::unbounded_channel();

        let table_source = TableSource::new(vec![]);
        let source_stream = table_source
            .stream_reader(vec![])
            .await
            .unwrap()
            .into_stream();

        let stream = SourceReaderStream::new(barrier_rx, source_stream);
        pin_mut!(stream);

        macro_rules! next {
            () => {
                stream
                    .next()
                    .now_or_never()
                    .flatten()
                    .map(|result| result.unwrap())
            };
        }

        // Write a chunk, and we should receive it.
        table_source.write_chunk(StreamChunk::default()).unwrap();
        assert_matches!(next!().unwrap(), Either::Right(_));
        // Write a barrier, and we should receive it.
        barrier_tx.send(Barrier::new_test_barrier(1)).unwrap();
        assert_matches!(next!().unwrap(), Either::Left(_));

        // Pause the stream.
        stream.pause_source();

        // Write a barrier.
        barrier_tx.send(Barrier::new_test_barrier(2)).unwrap();
        // Then write a chunk.
        table_source.write_chunk(StreamChunk::default()).unwrap();

        // We should receive the barrier.
        assert_matches!(next!().unwrap(), Either::Left(_));
        // We shouldn't receive the chunk.
        assert!(next!().is_none());

        // Resume the stream.
        stream.resume_source();
        // Then we can receive the chunk sent when the stream is paused.
        assert_matches!(next!().unwrap(), Either::Right(_));
    }
}
