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

use either::Either;
use futures::stream::{select_with_strategy, BoxStream, PollNext, SelectWithStrategy};
use futures::{Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::transaction::transaction_message::TxnMsg;
use risingwave_connector::source::BoxTxnMsgStream;

use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::Message;

type ExecutorMessageStream = BoxStream<'static, StreamExecutorResult<Message>>;

type DmlReaderData = StreamExecutorResult<Either<Message, TxnMsg>>;
type DmlReaderArm = BoxStream<'static, DmlReaderData>;
type DmlReaderWithPauseInner =
    SelectWithStrategy<DmlReaderArm, DmlReaderArm, impl FnMut(&mut PollNext) -> PollNext, PollNext>;

/// [`DmlReaderWithPause`] merges two streams, with one receiving barriers (and maybe other types
/// of messages) and the other receiving dml data only (no barrier). The merged stream can be paused
/// (`DmlReaderWithPause::pause_stream`) and resumed (`DmlReaderWithPause::resume_stream`).
/// A paused stream will not receive any data from either original stream until a barrier arrives
/// and the stream is resumed. The two streams will be polled in a round robin fashion.
pub(super) struct DmlReaderWithPause {
    inner: DmlReaderWithPauseInner,
    /// Whether the source stream is paused.
    paused: bool,
}

impl DmlReaderWithPause {
    #[try_stream(ok = TxnMsg, error = StreamExecutorError)]
    async fn data_stream(stream: BoxTxnMsgStream) {
        // TODO: support stack trace for Stream
        #[for_await]
        for txn_msg in stream {
            match txn_msg {
                Ok(txn_msg) => yield txn_msg,
                Err(err) => {
                    return Err(StreamExecutorError::dml_error(err));
                }
            }
        }
    }

    /// Construct a `DmlReaderWithPause` with one stream receiving barrier messages (and maybe
    /// other types of messages) and the other receiving data only (no barrier).
    pub fn new(message_stream: ExecutorMessageStream, data_stream: BoxTxnMsgStream) -> Self {
        let message_stream_arm = message_stream.map_ok(Either::Left).boxed();
        let data_stream_arm = Self::data_stream(data_stream).map_ok(Either::Right).boxed();
        let inner = Self::new_inner(message_stream_arm, data_stream_arm);
        Self {
            inner,
            paused: false,
        }
    }

    fn new_inner(
        message_stream: DmlReaderArm,
        data_stream: DmlReaderArm,
    ) -> DmlReaderWithPauseInner {
        let strategy = |last: &mut PollNext| last.toggle();
        select_with_strategy(message_stream, data_stream, strategy)
    }

    /// Pause the data stream.
    pub fn pause_stream(&mut self) {
        assert!(!self.paused, "already paused");
        self.paused = true;
    }

    /// Resume the data stream. Panic if the data stream is not paused.
    pub fn resume_stream(&mut self) {
        assert!(self.paused, "not paused");
        self.paused = false;
    }
}

impl Stream for DmlReaderWithPause {
    type Item = DmlReaderData;

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
            // TODO: We may need to prioritize the data stream (right-hand stream) after resuming
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
    use risingwave_common::transaction::transaction_message::TxnMsg;
    use risingwave_common::transaction::TxnId;
    use risingwave_source::TableDmlHandle;
    use tokio::sync::mpsc;

    use super::*;
    use crate::executor::{barrier_to_message_stream, Barrier};

    #[tokio::test]
    async fn test_pause_and_resume() {
        let (barrier_tx, barrier_rx) = mpsc::unbounded_channel();

        let table_dml_handle = TableDmlHandle::new(vec![]);
        let source_stream = table_dml_handle.stream_reader().into_stream();

        let barrier_stream = barrier_to_message_stream(barrier_rx).boxed();
        let stream = DmlReaderWithPause::new(barrier_stream, source_stream);
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
        const TEST_TRANSACTION_ID: TxnId = 1;
        table_dml_handle
            .write_txn_msg(TxnMsg::Begin(TEST_TRANSACTION_ID))
            .await
            .unwrap();
        table_dml_handle
            .write_txn_msg(TxnMsg::Data(TEST_TRANSACTION_ID, StreamChunk::default()))
            .await
            .unwrap();
        table_dml_handle
            .write_txn_msg(TxnMsg::End(TEST_TRANSACTION_ID))
            .await
            .unwrap();
        assert_matches!(next!().unwrap(), Either::Right(_));
        // Write a barrier, and we should receive it.
        barrier_tx.send(Barrier::new_test_barrier(1)).unwrap();
        assert_matches!(next!().unwrap(), Either::Left(_));

        // Pause the stream.
        stream.pause_stream();

        // Write a barrier.
        barrier_tx.send(Barrier::new_test_barrier(2)).unwrap();
        // Then write a chunk.
        table_dml_handle
            .write_txn_msg(TxnMsg::Begin(TEST_TRANSACTION_ID))
            .await
            .unwrap();
        table_dml_handle
            .write_txn_msg(TxnMsg::Data(TEST_TRANSACTION_ID, StreamChunk::default()))
            .await
            .unwrap();
        table_dml_handle
            .write_txn_msg(TxnMsg::End(TEST_TRANSACTION_ID))
            .await
            .unwrap();

        // We should receive the barrier.
        assert_matches!(next!().unwrap(), Either::Left(_));
        // We shouldn't receive the chunk.
        assert!(next!().is_none());

        // Resume the stream.
        stream.resume_stream();
        // Then we can receive the chunk sent when the stream is paused.
        assert_matches!(next!().unwrap(), Either::Right(_));
    }
}
