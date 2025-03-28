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

use std::future::{Future, Ready};
use std::pin::pin;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use futures::TryFuture;
use futures::future::{Either, select};
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use rw_futures_util::drop_either_future;

use crate::sink::encoder::SerTo;
use crate::sink::formatter::SinkFormatter;
use crate::sink::log_store::{
    DeliveryFutureManager, DeliveryFutureManagerAddFuture, LogStoreReadItem, TruncateOffset,
};
use crate::sink::{LogSinker, Result, SinkError, SinkLogReader, SinkWriterMetrics};

#[async_trait]
pub trait SinkWriter: Send + 'static {
    type CommitMetadata: Send = ();
    /// Begin a new epoch
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()>;

    /// Write a stream chunk to sink
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()>;

    /// Receive a barrier and mark the end of current epoch. When `is_checkpoint` is true, the sink
    /// writer should commit the current epoch.
    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata>;

    /// Clean up
    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    /// Update the vnode bitmap of current sink writer
    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }

    /// Return the starting read offset of the log store, which defaults to None.
    /// It may only rewind from an intermediate offset in the case of exactly once.
    fn rewind_start_offset(&mut self) -> Result<Option<u64>> {
        Ok(None)
    }
}

pub type DummyDeliveryFuture = Ready<std::result::Result<(), SinkError>>;

pub trait AsyncTruncateSinkWriter: Send + 'static {
    type DeliveryFuture: TryFuture<Ok = (), Error = SinkError> + Unpin + Send + 'static =
        DummyDeliveryFuture;

    fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    fn barrier(&mut self, _is_checkpoint: bool) -> impl Future<Output = Result<()>> + Send + '_ {
        async { Ok(()) }
    }
}

/// A free-form sink that may output in multiple formats and encodings. Examples include kafka,
/// kinesis, nats and redis.
///
/// The implementor specifies required key & value type (likely string or bytes), as well as how to
/// write a single pair. The provided `write_chunk` method would handle the interaction with a
/// `SinkFormatter`.
///
/// Currently kafka takes `&mut self` while kinesis takes `&self`. So we use `&mut self` in trait
/// but implement it for `&Kinesis`. This allows us to hold `&mut &Kinesis` and `&Kinesis`
/// simultaneously, preventing the schema clone issue propagating from kafka to kinesis.
pub trait FormattedSink {
    type K;
    type V;
    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()>;

    async fn write_chunk<F: SinkFormatter>(
        &mut self,
        chunk: StreamChunk,
        formatter: &F,
    ) -> Result<()>
    where
        F::K: SerTo<Self::K>,
        F::V: SerTo<Self::V>,
    {
        for r in formatter.format_chunk(&chunk) {
            let (event_key_object, event_object) = r?;

            self.write_one(
                event_key_object.map(SerTo::ser_to).transpose()?,
                event_object.map(SerTo::ser_to).transpose()?,
            )
            .await?;
        }

        Ok(())
    }
}

pub struct LogSinkerOf<W> {
    writer: W,
    sink_writer_metrics: SinkWriterMetrics,
}

impl<W> LogSinkerOf<W> {
    pub fn new(writer: W, sink_writer_metrics: SinkWriterMetrics) -> Self {
        LogSinkerOf {
            writer,
            sink_writer_metrics,
        }
    }
}

#[async_trait]
impl<W: SinkWriter<CommitMetadata = ()>> LogSinker for LogSinkerOf<W> {
    async fn consume_log_and_sink(self, mut log_reader: impl SinkLogReader) -> Result<!> {
        log_reader.start_from(None).await?;
        let mut sink_writer = self.writer;
        let metrics = self.sink_writer_metrics;
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

        loop {
            let (epoch, item): (u64, LogStoreReadItem) = log_reader.next_item().await?;
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
                LogStoreReadItem::StreamChunk { chunk, .. } => {
                    if let Err(e) = sink_writer.write_batch(chunk).await {
                        sink_writer.abort().await?;
                        return Err(e);
                    }
                }
                LogStoreReadItem::Barrier {
                    is_checkpoint,
                    new_vnode_bitmap,
                } => {
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun { curr_epoch } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };
                    if is_checkpoint {
                        let start_time = Instant::now();
                        sink_writer.barrier(true).await?;
                        metrics
                            .sink_commit_duration
                            .observe(start_time.elapsed().as_millis() as f64);
                        log_reader.truncate(TruncateOffset::Barrier { epoch })?;
                        if let Some(new_vnode_bitmap) = new_vnode_bitmap {
                            sink_writer.update_vnode_bitmap(new_vnode_bitmap).await?;
                        }
                    } else {
                        assert!(new_vnode_bitmap.is_none());
                        sink_writer.barrier(false).await?;
                    }
                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
            }
        }
    }
}

#[easy_ext::ext(SinkWriterExt)]
impl<T> T
where
    T: SinkWriter<CommitMetadata = ()> + Sized,
{
    pub fn into_log_sinker(self, sink_writer_metrics: SinkWriterMetrics) -> LogSinkerOf<Self> {
        LogSinkerOf {
            writer: self,
            sink_writer_metrics,
        }
    }
}

pub struct AsyncTruncateLogSinkerOf<W: AsyncTruncateSinkWriter> {
    writer: W,
    future_manager: DeliveryFutureManager<W::DeliveryFuture>,
}

impl<W: AsyncTruncateSinkWriter> AsyncTruncateLogSinkerOf<W> {
    pub fn new(writer: W, max_future_count: usize) -> Self {
        AsyncTruncateLogSinkerOf {
            writer,
            future_manager: DeliveryFutureManager::new(max_future_count),
        }
    }
}

#[async_trait]
impl<W: AsyncTruncateSinkWriter> LogSinker for AsyncTruncateLogSinkerOf<W> {
    async fn consume_log_and_sink(mut self, mut log_reader: impl SinkLogReader) -> Result<!> {
        log_reader.start_from(None).await?;
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
                        LogStoreReadItem::Barrier { is_checkpoint, .. } => {
                            self.writer.barrier(is_checkpoint).await?;
                            self.future_manager.add_barrier(epoch);
                        }
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
    pub fn into_log_sinker(self, max_future_count: usize) -> AsyncTruncateLogSinkerOf<Self> {
        AsyncTruncateLogSinkerOf::new(self, max_future_count)
    }
}
