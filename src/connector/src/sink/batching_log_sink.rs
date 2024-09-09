// Copyright 2024 RisingWave Labs
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

use std::num::NonZeroU64;
use std::time::Instant;

use async_trait::async_trait;
use futures::future::{select, Either};
use rw_futures_util::drop_either_future;
use std::pin::pin;
use super::file_sink::opendal_sink::BatchingStrategy;
use super::log_store::DeliveryFutureManager;
use super::writer::AsyncTruncateSinkWriter;
use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::writer::SinkWriter;
use crate::sink::{LogSinker, Result, SinkLogReader, SinkMetrics};
pub const DEFAULT_COMMIT_CHECKPOINT_INTERVAL: u64 = 10;

pub fn default_commit_checkpoint_interval() -> u64 {
    DEFAULT_COMMIT_CHECKPOINT_INTERVAL
}

/// The `LogSinker` implementation used for commit-decoupled sinks (such as `Iceberg`, `DeltaLake` and `StarRocks`).
/// The concurrent/frequent commit capability of these sinks is poor, so by leveraging the decoupled log reader,
/// we delay the checkpoint barrier to make commits less frequent.
pub struct BatchingLogSinkerOf<W> {
    writer: W,
    batching_strategy: BatchingStrategy,
    sink_metrics: SinkMetrics,
    commit_checkpoint_interval: NonZeroU64,
}

impl<W> BatchingLogSinkerOf<W> {
    /// Create a log sinker with a commit checkpoint interval. The sinker should be used with a
    /// decouple log reader `KvLogStoreReader`.
    pub fn new(
        writer: W,
        batching_strategy: BatchingStrategy,
        sink_metrics: SinkMetrics,
        commit_checkpoint_interval: NonZeroU64,
    ) -> Self {
        BatchingLogSinkerOf {
            writer,
            batching_strategy,
            sink_metrics,
            commit_checkpoint_interval,
        }
    }
}

#[async_trait]
impl<W: SinkWriter<CommitMetadata = ()>> LogSinker for BatchingLogSinkerOf<W> {
    async fn consume_log_and_sink(self, log_reader: &mut impl SinkLogReader) -> Result<!> {
        let mut sink_writer = self.writer;
        let sink_metrics = self.sink_metrics;
        #[derive(Debug)]
        enum LogConsumerState {
            /// Mark that the log consumer is not initialized yet
            Uninitialized,

            /// Mark that a new epoch has begun.
            EpochBegun { curr_epoch: u64 },

            /// Mark that the consumer has just received a barrier
            BarrierReceived { prev_epoch: u64 },
        }

        let mut state = LogConsumerState::Uninitialized;

        let mut current_checkpoint: u64 = 0;
        let commit_checkpoint_interval = self.commit_checkpoint_interval;
        loop {
            let (epoch, item): (u64, LogStoreReadItem) = log_reader.next_item().await?;
            if let LogStoreReadItem::UpdateVnodeBitmap(_) = &item {
                match &state {
                    LogConsumerState::BarrierReceived { .. } => {}
                    _ => unreachable!(
                        "update vnode bitmap can be accepted only right after \
                    barrier, but current state is {:?}",
                        state
                    ),
                }
            }
            // begin_epoch when not previously began
            state = match state {
                LogConsumerState::Uninitialized => {
                    sink_writer.begin_epoch(epoch).await?;
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
                LogConsumerState::EpochBegun { curr_epoch } => {
                    assert!(
                        epoch >= curr_epoch,
                        "new epoch {} should not be below the current epoch {}",
                        epoch,
                        curr_epoch
                    );
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
                LogConsumerState::BarrierReceived { prev_epoch } => {
                    assert!(
                        epoch > prev_epoch,
                        "new epoch {} should be greater than prev epoch {}",
                        epoch,
                        prev_epoch
                    );
                    sink_writer.begin_epoch(epoch).await?;
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
            };
            match item {
                LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                    match sink_writer
                        .write_batch_and_try_finish(chunk, chunk_id)
                        .await
                    {
                        Err(e) => {
                            sink_writer.abort().await?;
                            return Err(e);
                        }
                        Ok(true) => {}
                        Ok(false) => {}
                    }
                }
                LogStoreReadItem::Barrier { is_checkpoint } => {
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun { curr_epoch } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };
                    if is_checkpoint {
                        current_checkpoint += 1;
                        if current_checkpoint >= commit_checkpoint_interval.get() {
                            let start_time = Instant::now();
                            sink_writer.barrier(true).await?;
                            sink_metrics
                                .sink_commit_duration_metrics
                                .observe(start_time.elapsed().as_millis() as f64);
                            log_reader.truncate(TruncateOffset::Barrier { epoch })?;
                            current_checkpoint = 0;
                        } else {
                            sink_writer.barrier(false).await?;
                        }
                    } else {
                        sink_writer.barrier(false).await?;
                    }
                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
                LogStoreReadItem::UpdateVnodeBitmap(vnode_bitmap) => {
                    sink_writer.update_vnode_bitmap(vnode_bitmap).await?;
                }
            }
        }
    }
}

pub struct BatchingAsyncTruncateLogSinkerOf<W: AsyncTruncateSinkWriter> {
    writer: W,
    future_manager: DeliveryFutureManager<W::DeliveryFuture>,
}

impl<W: AsyncTruncateSinkWriter> BatchingAsyncTruncateLogSinkerOf<W> {
    pub fn new(writer: W, max_future_count: usize) -> Self {
        BatchingAsyncTruncateLogSinkerOf {
            writer,
            future_manager: DeliveryFutureManager::new(max_future_count),
        }
    }
}

#[async_trait]
impl<W: AsyncTruncateSinkWriter> LogSinker for BatchingAsyncTruncateLogSinkerOf<W> {
    async fn consume_log_and_sink(mut self, log_reader: &mut impl SinkLogReader) -> Result<!> {
        loop {
            let select_result = drop_either_future(
                select(
                    pin!(log_reader.next_item()),
                    pin!(self.future_manager.next_truncate_offset()),
                )
                .await,
            );
            match select_result {
                Either::Left(item_result) => {
                    let (epoch, item) = item_result?;
                    match item {
                        LogStoreReadItem::StreamChunk { chunk_id, chunk } => {
                            let add_future = self.future_manager.start_write_chunk(epoch, chunk_id);
                            self.writer.write_chunk(chunk, add_future).await?;
                        }
                        LogStoreReadItem::Barrier { is_checkpoint } => {}
                        LogStoreReadItem::UpdateVnodeBitmap(_) => {}
                    }
                }
                Either::Right(offset_result) => {
                    let offset = offset_result?;
                    log_reader.truncate(offset)?;
                }
            }
        }
    }
}

#[easy_ext::ext(AsyncTruncateSinkWriterExt)]
impl<T> T
where
    T: AsyncTruncateSinkWriter + Sized,
{
    pub fn into_log_sinker(
        self,
        max_future_count: usize,
    ) -> BatchingAsyncTruncateLogSinkerOf<Self> {
        BatchingAsyncTruncateLogSinkerOf::new(self, max_future_count)
    }
}
