// Copyright 2025 RisingWave Labs
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

use std::time::Duration;

use anyhow::anyhow;
use either::Either;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::Op;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_common::util::retry::exponential_backoff;
use risingwave_connector::source::reader::desc::{SourceDesc, SourceDescBuilder};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{StreamSourceCore, barrier_to_message_stream};
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

/// Retry errors emitted by the listing stream without blocking barriers.
/// Stream construction errors propagate, including the initial build before the first barrier.
fn retry_list_stream<F, S>(
    mut build_stream: F,
) -> StreamExecutorResult<impl Stream<Item = StreamExecutorResult<StreamChunk>>>
where
    F: FnMut() -> StreamExecutorResult<S>,
    S: Stream<Item = StreamExecutorResult<StreamChunk>>,
{
    let mut stream = build_stream()?;
    Ok(async_stream::try_stream! {
        let new_backoff = || exponential_backoff(Duration::from_secs(1), 2, Duration::from_secs(10));
        let mut backoff = new_backoff();
        loop {
            let mut has_progress = false;
            let error = {
                pin_mut!(stream);
                loop {
                    match stream.next().await {
                        Some(Ok(chunk)) => {
                            has_progress = true;
                            yield chunk;
                        }
                        Some(Err(error)) => break error,
                        None => return,
                    }
                }
            };
            if has_progress {
                backoff = new_backoff();
            }
            let delay = backoff.next().unwrap();
            tracing::warn!(error = %error.as_report(), ?delay, "file listing failed, retrying");
            // Sleep in the data stream so barriers and pause/resume remain responsive.
            tokio::time::sleep(delay).await;
            stream = build_stream()?;
        }
    })
}

pub struct FsListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: StreamSourceCore<S>,

    /// Metrics for monitor.
    #[expect(dead_code)]
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// System parameter reader to read barrier interval
    #[expect(dead_code)]
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    #[expect(dead_code)]
    rate_limit_rps: Option<u32>,
}

impl<S: StateStore> FsListExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        system_params: SystemParamsReaderRef,
        rate_limit_rps: Option<u32>,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            system_params,
            rate_limit_rps,
        }
    }

    fn build_chunked_paginate_stream(
        source_desc: &SourceDesc,
    ) -> StreamExecutorResult<impl Stream<Item = StreamExecutorResult<StreamChunk>> + use<S>> {
        let stream = source_desc
            .source
            .get_source_list()
            .map_err(StreamExecutorError::connector_error)?
            .map_err(StreamExecutorError::connector_error);

        let processed_stream = stream.map(|item| match item {
            Ok(page_item) => {
                let row = (
                    Op::Insert,
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(page_item.name.into_boxed_str())),
                        Some(ScalarImpl::Timestamptz(page_item.timestamp)),
                        Some(ScalarImpl::Int64(page_item.size)),
                    ]),
                );
                Ok(StreamChunk::from_rows(
                    &[row],
                    &[DataType::Varchar, DataType::Timestamptz, DataType::Int64],
                ))
            }
            Err(e) => {
                tracing::error!(error = %e.as_report(), "Connector failed to list item");
                Err(e)
            }
        });

        Ok(processed_stream)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.actor_ctx.id,
                    self.stream_source_core.source_id
                )
            })?;

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder =
            self.stream_source_core.source_desc_builder.take().unwrap();
        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        let chunked_paginate_stream =
            retry_list_stream(move || Self::build_chunked_paginate_stream(&source_desc))?;

        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();
        let mut stream =
            StreamReaderWithPause::<true, _>::new(barrier_stream, chunked_paginate_stream);

        if barrier.is_pause_on_startup() {
            stream.pause_stream();
        }

        yield Message::Barrier(barrier);

        while let Some(msg) = stream.next().await {
            match msg? {
                // Barrier arrives.
                Either::Left(msg) => match &msg {
                    Message::Barrier(barrier) => {
                        if let Some(mutation) = barrier.mutation.as_deref() {
                            match mutation {
                                Mutation::Pause => stream.pause_stream(),
                                Mutation::Resume => stream.resume_stream(),
                                _ => (),
                            }
                        }

                        // Propagate the barrier.
                        yield msg;
                    }
                    // Only barrier can be received.
                    _ => unreachable!(),
                },
                // Chunked FsPage arrives.
                Either::Right(chunk) => {
                    yield Message::Chunk(chunk);
                }
            }
        }
    }
}

impl<S: StateStore> Execute for FsListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for FsListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsListExecutor")
            .field("source_id", &self.stream_source_core.source_id)
            .field("column_ids", &self.stream_source_core.column_ids)
            .finish()
    }
}

// These tests use Tokio's paused clock, which is not available under madsim.
#[cfg(all(test, not(madsim)))]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use assert_matches::assert_matches;
    use futures::{FutureExt, stream};
    use risingwave_common::util::epoch::test_epoch;
    use tokio::sync::mpsc;

    use super::*;

    fn list_error() -> StreamExecutorError {
        StreamExecutorError::connector_error(anyhow!("permanent listing error"))
    }

    #[tokio::test(start_paused = true)]
    async fn test_listing_errors_back_off() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let available = Arc::new(AtomicBool::new(false));
        let data_stream = retry_list_stream({
            let attempts = attempts.clone();
            let available = available.clone();
            move || {
                attempts.fetch_add(1, Ordering::SeqCst);
                if available.load(Ordering::SeqCst) {
                    Ok(stream::once(async { Ok(StreamChunk::default()) })
                        .chain(stream::pending())
                        .boxed())
                } else {
                    // An asynchronous listing-creation error is emitted by the stream.
                    Ok(stream::once(async { Err(list_error()) }).boxed())
                }
            }
        })
        .unwrap();
        let (barrier_tx, barrier_rx) = mpsc::unbounded_channel();
        let mut reader = StreamReaderWithPause::<true, _>::new(
            barrier_to_message_stream(barrier_rx).boxed(),
            data_stream,
        );

        assert!(reader.next().now_or_never().is_none());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        for (i, delay) in [1, 2, 4, 8, 10, 10].into_iter().enumerate() {
            // A pending retry must not prevent the executor from forwarding barriers.
            barrier_tx
                .send(Barrier::new_test_barrier(test_epoch(i as u64 + 1)))
                .unwrap();
            assert_matches!(
                reader.next().now_or_never(),
                Some(Some(Ok(Either::Left(Message::Barrier(_)))))
            );

            tokio::time::advance(Duration::from_secs(delay) - Duration::from_millis(1)).await;
            assert!(reader.next().now_or_never().is_none());
            assert_eq!(attempts.load(Ordering::SeqCst), i + 1);

            tokio::time::advance(Duration::from_millis(1)).await;
            assert!(reader.next().now_or_never().is_none());
            assert_eq!(attempts.load(Ordering::SeqCst), i + 2);
        }

        // Restoring storage while paused must not start another listing until resume.
        available.store(true, Ordering::SeqCst);
        reader.pause_stream();
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(reader.next().now_or_never().is_none());
        assert_eq!(attempts.load(Ordering::SeqCst), 7);
        barrier_tx
            .send(Barrier::new_test_barrier(test_epoch(7)))
            .unwrap();
        assert_matches!(
            reader.next().now_or_never(),
            Some(Some(Ok(Either::Left(Message::Barrier(_)))))
        );
        reader.resume_stream();
        assert_matches!(
            reader.next().now_or_never(),
            Some(Some(Ok(Either::Right(_))))
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn test_initial_stream_construction_error_propagates() {
        let result = retry_list_stream(|| {
            Err::<stream::Empty<StreamExecutorResult<StreamChunk>>, _>(list_error())
        });
        assert!(result.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn test_stream_reconstruction_error_propagates() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let stream = retry_list_stream({
            let attempts = attempts.clone();
            move || {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(stream::once(async { Err(list_error()) }))
                } else {
                    Err(StreamExecutorError::connector_error(anyhow!(
                        "construction failed"
                    )))
                }
            }
        })
        .unwrap();
        pin_mut!(stream);

        assert!(stream.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        let error = stream.next().now_or_never().unwrap().unwrap().unwrap_err();
        assert!(
            error
                .as_report()
                .to_string()
                .contains("construction failed")
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_matches!(stream.next().now_or_never(), Some(None));
    }

    #[tokio::test(start_paused = true)]
    async fn test_listing_progress_resets_backoff() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let stream = retry_list_stream({
            let attempts = attempts.clone();
            move || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                Ok(stream::iter(if attempt < 2 {
                    vec![Err(list_error())]
                } else {
                    vec![Ok(StreamChunk::default()), Err(list_error())]
                }))
            }
        })
        .unwrap();
        pin_mut!(stream);

        assert!(stream.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(stream.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_matches!(stream.next().now_or_never(), Some(Some(Ok(_))));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        // A page error after a successful item still waits before restarting the listing.
        assert!(stream.next().now_or_never().is_none());
        tokio::time::advance(Duration::from_millis(999)).await;
        assert!(stream.next().now_or_never().is_none());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_matches!(stream.next().now_or_never(), Some(Some(Ok(_))));
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }
}
