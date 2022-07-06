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

use either::Either;
use futures::stream::{select_with_strategy, BoxStream, PollNext, SelectWithStrategy};
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use pin_project::pin_project;
use risingwave_common::bail;
use risingwave_source::*;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::Barrier;

type SourceReaderMessage =
    Either<StreamExecutorResult<Barrier>, StreamExecutorResult<StreamChunkWithState>>;
type SourceReaderArm = BoxStream<'static, SourceReaderMessage>;
type SourceReaderStreamInner =
    SelectWithStrategy<SourceReaderArm, SourceReaderArm, impl FnMut(&mut ()) -> PollNext, ()>;

#[pin_project]
pub(super) struct SourceReaderStream {
    #[pin]
    inner: SourceReaderStreamInner,

    paused: Option<SourceReaderArm>,
}

impl SourceReaderStream {
    /// Receive barriers from barrier manager with the channel, error on channel close.
    #[try_stream(ok = Barrier, error = StreamExecutorError)]
    async fn barrier_receiver(mut rx: UnboundedReceiver<Barrier>) {
        while let Some(barrier) = rx.recv().await {
            yield barrier;
        }
        bail!("barrier reader closed unexpectedly");
    }

    /// Receive chunks and states from the source reader, hang up on error.
    #[try_stream(ok = StreamChunkWithState, error = StreamExecutorError)]
    async fn source_chunk_reader(mut reader: Box<SourceStreamReaderImpl>) {
        loop {
            match reader.next().await {
                Ok(chunk) => yield chunk,
                Err(err) => {
                    error!("hang up stream reader due to polling error: {}", err);
                    futures::future::pending().await
                }
            }
        }
    }

    /// Convert this reader to a stream.
    pub fn new(
        barrier_receiver: UnboundedReceiver<Barrier>,
        source_chunk_reader: Box<SourceStreamReaderImpl>,
    ) -> Self {
        let barrier_receiver = Self::barrier_receiver(barrier_receiver);
        let source_chunk_reader = Self::source_chunk_reader(source_chunk_reader);

        let inner = select_with_strategy(
            barrier_receiver.map(Either::Left).boxed(),
            source_chunk_reader.map(Either::Right).boxed(),
            // We prefer barrier on the left hand side over source chunks.
            |_: &mut ()| PollNext::Left,
        );

        Self {
            inner,
            paused: None,
        }
    }

    /// Replace the source chunk reader with a new one for given `stream`. Used for split
    /// change.
    pub fn replace_source_chunk_reader(&mut self, reader: Box<SourceStreamReaderImpl>) {
        if self.paused.is_some() {
            panic!("should not replace source chunk reader when paused");
        }
        *self.inner.get_mut().1 = Self::source_chunk_reader(reader).map(Either::Right).boxed();
    }

    #[expect(dead_code)]
    pub fn pause(&mut self) {
        if self.paused.is_some() {
            panic!("already paused");
        }
        let source_chunk_reader =
            std::mem::replace(self.inner.get_mut().1, futures::stream::pending().boxed());
        let _ = self.paused.insert(source_chunk_reader);
    }

    #[expect(dead_code)]
    pub fn resume(&mut self) {
        let source_chunk_reader = self.paused.take().expect("not paused");
        let _ = std::mem::replace(self.inner.get_mut().1, source_chunk_reader);
    }
}

impl Stream for SourceReaderStream {
    type Item = SourceReaderMessage;

    fn poll_next(
        self: Pin<&mut Self>,
        ctx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(ctx)
    }
}
