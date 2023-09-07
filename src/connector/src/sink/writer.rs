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

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;

use crate::sink::log_store::{LogReader, LogStoreReadItem};
use crate::sink::{LogSinker, Result, SinkMetrics};

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
}

// TODO: remove this trait after KafkaSinkWriter implements SinkWriter
#[async_trait]
// An old version of SinkWriter for backward compatibility
pub trait SinkWriterV1: Send + 'static {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()>;

    // the following interface is for transactions, if not supported, return Ok(())
    // start a transaction with epoch number. Note that epoch number should be increasing.
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()>;

    // commits the current transaction and marks all messages in the transaction success.
    async fn commit(&mut self) -> Result<()>;

    // aborts the current transaction because some error happens. we should rollback to the last
    // commit point.
    async fn abort(&mut self) -> Result<()>;
}

pub struct SinkWriterV1Adapter<W: SinkWriterV1> {
    is_empty: bool,
    epoch: u64,
    inner: W,
}

impl<W: SinkWriterV1> SinkWriterV1Adapter<W> {
    pub(crate) fn new(inner: W) -> Self {
        Self {
            inner,
            is_empty: true,
            epoch: u64::MIN,
        }
    }
}

#[async_trait]
impl<W: SinkWriterV1> SinkWriter for SinkWriterV1Adapter<W> {
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = epoch;
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_empty {
            self.is_empty = false;
            self.inner.begin_epoch(self.epoch).await?;
        }
        self.inner.write_batch(chunk).await
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        if is_checkpoint {
            if !self.is_empty {
                self.inner.commit().await?
            }
            self.is_empty = true;
        }
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

pub struct LogSinkerOf<W: SinkWriter<CommitMetadata = ()>> {
    writer: W,
    sink_metrics: SinkMetrics,
}

impl<W: SinkWriter<CommitMetadata = ()>> LogSinkerOf<W> {
    pub fn new(writer: W, sink_metrics: SinkMetrics) -> Self {
        LogSinkerOf {
            writer,
            sink_metrics,
        }
    }
}

impl<W: SinkWriter<CommitMetadata = ()>> LogSinker for LogSinkerOf<W> {
    async fn consume_log_and_sink(self, mut log_reader: impl LogReader) -> Result<()> {
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

        log_reader.init().await?;

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
                LogStoreReadItem::StreamChunk(chunk) => {
                    if let Err(e) = sink_writer.write_batch(chunk).await {
                        sink_writer.abort().await?;
                        return Err(e);
                    }
                }
                LogStoreReadItem::Barrier { is_checkpoint } => {
                    if is_checkpoint {
                        let start_time = Instant::now();
                        sink_writer.barrier(true).await?;
                        sink_metrics
                            .sink_commit_duration_metrics
                            .observe(start_time.elapsed().as_millis() as f64);
                        log_reader.truncate().await?;
                    } else {
                        sink_writer.barrier(false).await?;
                    }
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun { curr_epoch } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };
                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
                LogStoreReadItem::UpdateVnodeBitmap(vnode_bitmap) => {
                    sink_writer.update_vnode_bitmap(vnode_bitmap).await?;
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
    pub fn into_log_sinker(self, sink_metrics: SinkMetrics) -> LogSinkerOf<Self> {
        LogSinkerOf {
            writer: self,
            sink_metrics,
        }
    }
}
