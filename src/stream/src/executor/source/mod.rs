// Copyright 2022 RisingWave Labs
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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use await_tree::InstrumentAwait;
use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::bail;
use risingwave_common::row::Row;
use risingwave_common_rate_limit::RateLimiter;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::source::{
    BoxSourceChunkStream, BoxSourceReaderEventStream, BoxStreamingFileSourceChunkStream,
    SourceColumnDesc, SourceReaderEvent, SplitId,
};
use risingwave_pb::plan_common::AdditionalColumn;
use risingwave_pb::plan_common::additional_column::ColumnType;
pub use state_table_handler::*;

mod executor_core;
pub use executor_core::StreamSourceCore;

mod reader_stream;

mod source_executor;
pub use source_executor::*;
mod dummy_source_executor;
pub use dummy_source_executor::*;
mod source_backfill_executor;
pub use source_backfill_executor::*;
mod fs_list_executor;
pub use fs_list_executor::*;
mod fs_fetch_executor;
pub use fs_fetch_executor::*;
mod iceberg_list_executor;
pub use iceberg_list_executor::*;
mod iceberg_fetch_executor;
pub use iceberg_fetch_executor::*;
mod batch_source; // For refreshable batch source executors
pub use batch_source::*;
mod source_backfill_state_table;
pub(crate) use source_backfill_state_table::BackfillStateTableHandler;

pub mod state_table_handler;
use futures_async_stream::try_stream;
use risingwave_common::util::retry::exponential_backoff;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_retry::strategy::jitter;

use crate::common::rate_limit::rate_limited_pieces;
use crate::executor::actor::{DropOnBlockingThread, spawn_blocking_drop_stream};
use crate::executor::error::StreamExecutorError;
use crate::executor::{Barrier, Message};

/// Receive barriers from barrier manager with the channel, error on channel close.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn barrier_to_message_stream(mut rx: UnboundedReceiver<Barrier>) {
    while let Some(barrier) = rx.recv().instrument_await("receive_barrier").await {
        yield Message::Barrier(barrier);
    }
    bail!("barrier reader closed unexpectedly");
}

pub fn get_split_offset_mapping_from_chunk(
    chunk: &StreamChunk,
    split_idx: usize,
    offset_idx: usize,
) -> Option<HashMap<SplitId, String>> {
    let mut split_offset_mapping = HashMap::new();
    // All rows (including those visible or invisible) will be used to update the source offset.
    for i in 0..chunk.capacity() {
        let (_, row, _) = chunk.row_at(i);
        let split_id = row.datum_at(split_idx).unwrap().into_utf8().into();
        let offset = row.datum_at(offset_idx).unwrap().into_utf8();
        split_offset_mapping.insert(split_id, offset.to_owned());
    }
    Some(split_offset_mapping)
}

/// Get the indices of the split, offset, and pulsar message id columns.
pub fn get_split_offset_col_idx(
    column_descs: &[SourceColumnDesc],
) -> (Option<usize>, Option<usize>, Option<usize>) {
    let mut split_idx = None;
    let mut offset_idx = None;
    let mut pulsar_message_id_idx = None;
    for (idx, column) in column_descs.iter().enumerate() {
        match column.additional_column {
            AdditionalColumn {
                column_type: Some(ColumnType::Partition(_) | ColumnType::Filename(_)),
            } => {
                split_idx = Some(idx);
            }
            AdditionalColumn {
                column_type: Some(ColumnType::Offset(_)),
            } => {
                offset_idx = Some(idx);
            }
            AdditionalColumn {
                column_type: Some(ColumnType::PulsarMessageIdData(_)),
            } => {
                pulsar_message_id_idx = Some(idx);
            }
            _ => (),
        }
    }
    (split_idx, offset_idx, pulsar_message_id_idx)
}

pub fn prune_additional_cols(
    chunk: &StreamChunk,
    to_prune_indices: &[usize],
    column_descs: &[SourceColumnDesc],
) -> StreamChunk {
    chunk.project(
        &(0..chunk.dimension())
            .filter(|&idx| !to_prune_indices.contains(&idx) || column_descs[idx].is_visible())
            .collect_vec(),
    )
}

#[try_stream(ok = StreamChunk, error = ConnectorError)]
pub async fn apply_rate_limit(stream: BoxSourceChunkStream, rate_limit_rps: Option<u32>) {
    if rate_limit_rps == Some(0) {
        // block the stream until the rate limit is reset
        let future = futures::future::pending::<()>();
        future.await;
        unreachable!();
    }

    let limiter = RateLimiter::new(
        rate_limit_rps
            .inspect(|limit| tracing::info!(rate_limit = limit, "rate limit applied"))
            .into(),
    );

    #[for_await]
    for chunk in stream {
        let chunk = chunk?;
        yield process_chunk(chunk, rate_limit_rps, &limiter).await;
    }
}

#[try_stream(ok = SourceReaderEvent, error = ConnectorError)]
pub async fn apply_rate_limit_to_source_reader_event(
    stream: BoxSourceReaderEventStream,
    rate_limit_rps: Option<u32>,
) {
    let mut stream = DropOnBlockingThread::new(stream);

    if rate_limit_rps == Some(0) {
        // block the stream until the rate limit is reset
        let future = futures::future::pending::<()>();
        future.await;
        unreachable!();
    }

    let limiter = RateLimiter::new(
        rate_limit_rps
            .inspect(|limit| tracing::info!(rate_limit = limit, "rate limit applied"))
            .into(),
    );

    let result = loop {
        let Some(event) = stream.get_mut().next().await else {
            break Ok(());
        };
        match event {
            Err(error) => break Err(error),
            Ok(SourceReaderEvent::DataChunk(chunk)) => {
                yield SourceReaderEvent::DataChunk(
                    process_chunk(chunk, rate_limit_rps, &limiter).await,
                )
            }
            Ok(SourceReaderEvent::SplitProgress(progress)) => {
                yield SourceReaderEvent::SplitProgress(progress)
            }
        }
    };

    // Some source clients perform synchronous cleanup when dropped. In particular,
    // librdkafka's consumer close may wait for broker-side timeouts. Keep that work off the
    // actor runtime so unrelated actors and barriers remain schedulable.
    spawn_blocking_drop_stream(stream.into_inner()).await;
    result?;
}

#[try_stream(ok = StreamChunk, error = ConnectorError)]
pub async fn source_reader_event_to_chunk_stream(stream: BoxSourceReaderEventStream) {
    #[for_await]
    for event in stream {
        match event? {
            SourceReaderEvent::DataChunk(chunk) => yield chunk,
            SourceReaderEvent::SplitProgress(_) => {}
        }
    }
}

/// Pace a file source reader with a limiter shared with its executor, so that a `Throttle`
/// mutation also applies to the file that is already being read.
#[try_stream(ok = Option<StreamChunk>, error = ConnectorError)]
pub async fn apply_shared_rate_limit_to_file_source_reader(
    stream: BoxStreamingFileSourceChunkStream,
    limiter: Arc<RateLimiter>,
) {
    #[for_await]
    for chunk in stream {
        match chunk? {
            Some(chunk) =>
            {
                #[for_await]
                for piece in rate_limited_pieces(&limiter, chunk) {
                    yield Some(piece);
                }
            }
            None => yield None,
        }
    }
}

#[try_stream(ok = Option<StreamChunk>, error = ConnectorError)]
pub async fn apply_rate_limit_with_for_streaming_file_source_reader(
    stream: BoxStreamingFileSourceChunkStream,
    rate_limit_rps: Option<u32>,
) {
    if rate_limit_rps == Some(0) {
        // block the stream until the rate limit is reset
        let future = futures::future::pending::<()>();
        future.await;
        unreachable!();
    }

    let limiter = RateLimiter::new(
        rate_limit_rps
            .inspect(|limit| tracing::info!(rate_limit = limit, "rate limit applied"))
            .into(),
    );

    #[for_await]
    for chunk in stream {
        let chunk_option = chunk?;
        match chunk_option {
            Some(chunk) => {
                let processed_chunk = process_chunk(chunk, rate_limit_rps, &limiter).await;
                yield Some(processed_chunk);
            }
            None => yield None,
        }
    }
}

async fn process_chunk(
    chunk: StreamChunk,
    rate_limit_rps: Option<u32>,
    limiter: &RateLimiter,
) -> StreamChunk {
    let chunk_size = chunk.capacity();

    if rate_limit_rps.is_none() || chunk_size == 0 {
        // no limit, or empty chunk
        return chunk;
    }

    let limit = rate_limit_rps.unwrap() as u64;
    let required_permits = chunk.rate_limit_permits();
    if required_permits > limit {
        // This should not happen after the mentioned PR.
        tracing::error!(
            chunk_size,
            required_permits,
            limit,
            "unexpected large chunk size"
        );
    }

    limiter.wait(required_permits).await;
    chunk
}

pub fn get_infinite_backoff_strategy() -> impl Iterator<Item = Duration> {
    const BASE_DELAY: Duration = Duration::from_secs(1);
    const BACKOFF_FACTOR: u64 = 2;
    const MAX_DELAY: Duration = Duration::from_secs(10);
    exponential_backoff(BASE_DELAY, BACKOFF_FACTOR, MAX_DELAY).map(jitter)
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, mpsc};
    use std::time::Instant;

    use futures::stream;
    use risingwave_connector::error::ConnectorResult;
    use tokio::sync::oneshot;

    use super::*;

    struct BlockingDrop {
        started: Option<oneshot::Sender<()>>,
        finish: mpsc::Receiver<()>,
    }

    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            self.started.take().unwrap().send(()).unwrap();
            self.finish.recv().unwrap();
        }
    }

    struct SleepingDrop {
        started: Option<oneshot::Sender<()>>,
        finished: Option<oneshot::Sender<()>>,
    }

    impl Drop for SleepingDrop {
        fn drop(&mut self) {
            let _ = self.started.take().unwrap().send(());
            std::thread::sleep(Duration::from_secs(1));
            let _ = self.finished.take().unwrap().send(());
        }
    }

    #[try_stream(ok = SourceReaderEvent, error = ConnectorError)]
    async fn failed_source_reader(_guard: Arc<Mutex<BlockingDrop>>) {
        return Err(ConnectorError::from(anyhow::anyhow!("test source failure")));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_failed_source_reader_cleanup_does_not_block_runtime() {
        let mut cleanup_tasks = Vec::new();
        let mut cleanup_started = Vec::new();
        let mut cleanup_finish = Vec::new();

        for _ in 0..4 {
            let (started_tx, started_rx) = oneshot::channel();
            let (finish_tx, finish_rx) = mpsc::channel();
            let guard = Arc::new(Mutex::new(BlockingDrop {
                started: Some(started_tx),
                finish: finish_rx,
            }));
            let stream = failed_source_reader(guard.clone())
                .map(move |item| {
                    // A terminating `try_stream` drops its own copy of the guard before returning
                    // the error. Model the Kafka stream's outer owner, which must retain the
                    // resource until `apply_rate_limit_to_source_reader_event` can move it to the
                    // blocking pool.
                    let _ = &guard;
                    item
                })
                .boxed();
            let mut stream = apply_rate_limit_to_source_reader_event(stream, None).boxed();

            cleanup_tasks.push(tokio::spawn(async move {
                assert!(stream.next().await.unwrap().is_err());
            }));
            cleanup_started.push(started_rx);
            cleanup_finish.push(finish_tx);
        }

        for started in cleanup_started {
            started.await.unwrap();
        }

        tokio::time::timeout(
            Duration::from_secs(1),
            tokio::time::sleep(Duration::from_millis(10)),
        )
        .await
        .expect("the runtime worker should remain schedulable during source cleanup");

        for finish in cleanup_finish {
            finish.send(()).unwrap();
        }
        for task in cleanup_tasks {
            task.await.unwrap();
        }
    }

    async fn assert_cancelled_source_reader_cleanup_does_not_block_runtime(
        rate_limit_rps: Option<u32>,
    ) {
        let (cleanup_started_tx, cleanup_started_rx) = oneshot::channel();
        let (cleanup_finished_tx, cleanup_finished_rx) = oneshot::channel();
        let guard = SleepingDrop {
            started: Some(cleanup_started_tx),
            finished: Some(cleanup_finished_tx),
        };
        let stream = stream::pending::<ConnectorResult<SourceReaderEvent>>()
            .map(move |item| {
                let _ = &guard;
                item
            })
            .boxed();
        let mut stream = apply_rate_limit_to_source_reader_event(stream, rate_limit_rps).boxed();
        let (poll_started_tx, poll_started_rx) = oneshot::channel();
        let stream_task = tokio::spawn(async move {
            poll_started_tx.send(()).unwrap();
            stream.next().await
        });

        // The send and first stream poll happen in the same task poll, so receiving this signal
        // means the source wrapper is suspended in either `stream.next()` or the zero-rate branch.
        poll_started_rx.await.unwrap();
        let cancel_started_at = Instant::now();
        stream_task.abort();
        let join_error = tokio::time::timeout(Duration::from_millis(500), stream_task)
            .await
            .expect("cancelling the source stream should not block the runtime worker")
            .unwrap_err();
        assert!(join_error.is_cancelled());
        assert!(
            cancel_started_at.elapsed() < Duration::from_millis(500),
            "cancelling the source stream waited for its blocking destructor"
        );

        cleanup_started_rx.await.unwrap();
        cleanup_finished_rx.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_cancelled_source_reader_cleanup_does_not_block_runtime() {
        tokio::join!(
            assert_cancelled_source_reader_cleanup_does_not_block_runtime(None),
            assert_cancelled_source_reader_cleanup_does_not_block_runtime(Some(0)),
        );
    }
}
