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

use std::pin::Pin;
use std::task::Poll;

use async_stack_trace::StackTrace;
use either::Either;
use futures::stream::{select_with_strategy, BoxStream, PollNext, SelectWithStrategy};
use futures::{Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::bail;
use risingwave_connector::source::{BoxSourceWithStateStream, StreamChunkWithState};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::{Barrier, Message};

type ExecutorMessageStream = BoxStream<'static, StreamExecutorResult<Message>>;
type ReaderStreamData = StreamExecutorResult<Either<Message, StreamChunkWithState>>;
type ReaderArm = BoxStream<'static, ReaderStreamData>;
type ReaderStreamWithPauseInner =
    SelectWithStrategy<ReaderArm, ReaderArm, impl FnMut(&mut PollNext) -> PollNext, PollNext>;

/// `ReaderStreamWithPause` merges two streams, with one of them receiving barriers. When the
/// barrier requires the data stream
pub(super) struct ReaderStreamWithPause<const BIASED: bool> {
    inner: ReaderStreamWithPauseInner,
    /// Whether the source stream is paused.
    paused: bool,
}

impl<const BIASED: bool> ReaderStreamWithPause<BIASED> {
    /// Receive barriers from barrier manager with the channel, error on channel close.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn barrier_receiver(mut rx: UnboundedReceiver<Barrier>) {
        while let Some(barrier) = rx.recv().stack_trace("source_recv_barrier").await {
            yield Message::Barrier(barrier);
        }
        bail!("barrier reader closed unexpectedly");
    }

    /// Receive chunks and states from the reader. Hang up on error.
    #[try_stream(ok = StreamChunkWithState, error = StreamExecutorError)]
    async fn data_stream(stream: BoxSourceWithStateStream) {
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

    /// Construct a `ReaderStreamWithPause` with one stream receiving streaming messages and the
    /// other receiving data only.
    pub fn new_with_message_stream(
        message_stream: ExecutorMessageStream,
        data_stream: BoxSourceWithStateStream,
    ) -> Self {
        let message_stream_arm = message_stream.map_ok(Either::Left).boxed();
        let data_stream_arm = Self::data_stream(data_stream).map_ok(Either::Right).boxed();
        let inner = Self::new_inner(message_stream_arm, data_stream_arm);
        Self {
            inner,
            paused: false,
        }
    }

    /// Construct a `ReaderStreamWithPause` with one stream receiving barriers only and the other
    /// receiving data only.
    pub fn new_with_barrier_receiver(
        barrier_receiver: UnboundedReceiver<Barrier>,
        data_stream: BoxSourceWithStateStream,
    ) -> Self {
        let barrier_receiver_arm = Self::barrier_receiver(barrier_receiver)
            .map_ok(Either::Left)
            .boxed();
        let data_stream_arm = Self::data_stream(data_stream).map_ok(Either::Right).boxed();
        let inner = Self::new_inner(barrier_receiver_arm, data_stream_arm);
        Self {
            inner,
            paused: false,
        }
    }

    fn new_inner(message_stream: ReaderArm, data_stream: ReaderArm) -> ReaderStreamWithPauseInner {
        let strategy = if BIASED {
            |_: &mut PollNext| PollNext::Left
        } else {
            // The poll strategy is not biased: we poll the two streams in a round robin way.
            |last: &mut PollNext| last.toggle()
        };
        select_with_strategy(message_stream, data_stream, strategy)
    }

    /// Replace the data stream with a new one for given `stream`. Used for split change.
    pub fn replace_source_stream(&mut self, data_stream: BoxSourceWithStateStream) {
        // Take the barrier receiver arm.
        let barrier_receiver_arm = std::mem::replace(
            self.inner.get_mut().0,
            futures::stream::once(async { unreachable!("placeholder") }).boxed(),
        );

        // Note: create a new `SelectWithStrategy` instead of replacing the source stream arm here,
        // to ensure the internal state of the `SelectWithStrategy` is reset. (#6300)
        self.inner = Self::new_inner(
            barrier_receiver_arm,
            Self::data_stream(data_stream).map_ok(Either::Right).boxed(),
        );
    }

    /// Pause the data stream.
    pub fn pause_data_stream(&mut self) {
        assert!(!self.paused, "already paused");
        self.paused = true;
    }

    /// Resume the data stream. Panic if the data stream is not paused.
    pub fn resume_data_stream(&mut self) {
        assert!(self.paused, "not paused");
        self.paused = false;
    }
}

impl<const BIASED: bool> Stream for ReaderStreamWithPause<BIASED> {
    type Item = ReaderStreamData;

    fn poll_next(
        mut self: Pin<&mut Self>,
        ctx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.paused {
            // Note: It is safe here to poll the left arm even if it contains streaming messages
            // other than barriers: after the upstream executor sends a `Mutation::Pause`, there
            // should be no more message until a `Mutation::Update` and a 'Mutation::Resume`.
            self.inner.get_mut().0.poll_next_unpin(ctx)
        } else {
            // TODO: We may need to prioritize the data stream (right hand stream) after resuming
            // from the paused state.
            self.inner.poll_next_unpin(ctx)
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::{pin_mut, FutureExt};
    use risingwave_common::array::StreamChunk;
    use risingwave_source::TableDmlHandle;
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn test_pause_and_resume() {
        let (barrier_tx, barrier_rx) = mpsc::unbounded_channel();

        let table_dml_handle = TableDmlHandle::new(vec![]);
        let source_stream = table_dml_handle.stream_reader().into_stream();

        let stream =
            ReaderStreamWithPause::<true>::new_with_barrier_receiver(barrier_rx, source_stream);
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
        table_dml_handle
            .write_chunk(StreamChunk::default())
            .await
            .unwrap();
        assert_matches!(next!().unwrap(), Either::Right(_));
        // Write a barrier, and we should receive it.
        barrier_tx.send(Barrier::new_test_barrier(1)).unwrap();
        assert_matches!(next!().unwrap(), Either::Left(_));

        // Pause the stream.
        stream.pause_data_stream();

        // Write a barrier.
        barrier_tx.send(Barrier::new_test_barrier(2)).unwrap();
        // Then write a chunk.
        table_dml_handle
            .write_chunk(StreamChunk::default())
            .await
            .unwrap();

        // We should receive the barrier.
        assert_matches!(next!().unwrap(), Either::Left(_));
        // We shouldn't receive the chunk.
        assert!(next!().is_none());

        // Resume the stream.
        stream.resume_data_stream();
        // Then we can receive the chunk sent when the stream is paused.
        assert_matches!(next!().unwrap(), Either::Right(_));
    }
}
