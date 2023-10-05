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

use std::cmp::Ordering;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::util::epoch::{EpochPair, INVALID_EPOCH};

use crate::sink::SinkMetrics;

pub type LogStoreResult<T> = Result<T, anyhow::Error>;
pub type ChunkId = usize;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum TruncateOffset {
    Chunk { epoch: u64, chunk_id: ChunkId },
    Barrier { epoch: u64 },
}

impl PartialOrd for TruncateOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let extract = |offset: &TruncateOffset| match offset {
            TruncateOffset::Chunk { epoch, chunk_id } => (*epoch, *chunk_id),
            TruncateOffset::Barrier { epoch } => (*epoch, usize::MAX),
        };
        let this = extract(self);
        let other = extract(other);
        this.partial_cmp(&other)
    }
}

impl TruncateOffset {
    pub fn next_chunk_id(&self) -> ChunkId {
        match self {
            TruncateOffset::Chunk { chunk_id, .. } => chunk_id + 1,
            TruncateOffset::Barrier { .. } => 0,
        }
    }

    pub fn epoch(&self) -> u64 {
        match self {
            TruncateOffset::Chunk { epoch, .. } | TruncateOffset::Barrier { epoch } => *epoch,
        }
    }

    pub fn check_next_item_epoch(&self, epoch: u64) -> LogStoreResult<()> {
        match self {
            TruncateOffset::Chunk {
                epoch: offset_epoch,
                ..
            } => {
                if epoch != *offset_epoch {
                    return Err(anyhow!(
                        "new item epoch {} not match current chunk offset epoch {}",
                        epoch,
                        offset_epoch
                    ));
                }
            }
            TruncateOffset::Barrier {
                epoch: offset_epoch,
            } => {
                if epoch <= *offset_epoch {
                    return Err(anyhow!(
                        "new item epoch {} not exceed barrier offset epoch {}",
                        epoch,
                        offset_epoch
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum LogStoreReadItem {
    StreamChunk {
        chunk: StreamChunk,
        chunk_id: ChunkId,
    },
    Barrier {
        is_checkpoint: bool,
    },
    UpdateVnodeBitmap(Arc<Bitmap>),
}

pub trait LogWriter: Send {
    /// Initialize the log writer with an epoch
    fn init(&mut self, epoch: EpochPair) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Write a stream chunk to the log writer
    fn write_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Mark current epoch as finished and sealed, and flush the unconsumed log data.
    fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        is_checkpoint: bool,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Update the vnode bitmap of the log writer
    fn update_vnode_bitmap(
        &mut self,
        new_vnodes: Arc<Bitmap>,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_;
}

pub trait LogReader: Send + Sized + 'static {
    /// Initialize the log reader. Usually function as waiting for log writer to be initialized.
    fn init(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_;

    /// Emit the next item.
    fn next_item(
        &mut self,
    ) -> impl Future<Output = LogStoreResult<(u64, LogStoreReadItem)>> + Send + '_;

    /// Mark that all items emitted so far have been consumed and it is safe to truncate the log
    /// from the current offset.
    fn truncate(
        &mut self,
        offset: TruncateOffset,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_;
}

pub trait LogStoreFactory: 'static {
    type Reader: LogReader + Send + 'static;
    type Writer: LogWriter + Send + 'static;

    fn build(self) -> impl Future<Output = (Self::Reader, Self::Writer)> + Send;
}

pub struct TransformChunkLogReader<F: Fn(StreamChunk) -> StreamChunk, R: LogReader> {
    f: F,
    inner: R,
}

impl<F: Fn(StreamChunk) -> StreamChunk + Send + 'static, R: LogReader> LogReader
    for TransformChunkLogReader<F, R>
{
    fn init(&mut self) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.inner.init()
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        let (epoch, item) = self.inner.next_item().await?;
        let item = match item {
            LogStoreReadItem::StreamChunk { chunk, chunk_id } => LogStoreReadItem::StreamChunk {
                chunk: (self.f)(chunk),
                chunk_id,
            },
            other => other,
        };
        Ok((epoch, item))
    }

    fn truncate(
        &mut self,
        offset: TruncateOffset,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        self.inner.truncate(offset)
    }
}

pub struct MonitoredLogReader<R: LogReader> {
    inner: R,
    read_epoch: u64,
    metrics: SinkMetrics,
}

impl<R: LogReader> LogReader for MonitoredLogReader<R> {
    async fn init(&mut self) -> LogStoreResult<()> {
        self.inner.init().await
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        self.inner.next_item().await.inspect(|(epoch, _)| {
            if self.read_epoch != *epoch {
                self.read_epoch = *epoch;
                self.metrics.log_store_latest_read_epoch.set(*epoch as _);
            }
        })
    }

    async fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        self.inner.truncate(offset).await
    }
}

#[easy_ext::ext(LogReaderExt)]
impl<T> T
where
    T: LogReader,
{
    pub fn transform_chunk<F: Fn(StreamChunk) -> StreamChunk + Sized>(
        self,
        f: F,
    ) -> TransformChunkLogReader<F, Self> {
        TransformChunkLogReader { f, inner: self }
    }

    pub fn monitored(self, metrics: SinkMetrics) -> MonitoredLogReader<T> {
        MonitoredLogReader {
            read_epoch: INVALID_EPOCH,
            inner: self,
            metrics,
        }
    }
}

pub struct MonitoredLogWriter<W: LogWriter> {
    inner: W,
    metrics: SinkMetrics,
}

impl<W: LogWriter> LogWriter for MonitoredLogWriter<W> {
    async fn init(&mut self, epoch: EpochPair) -> LogStoreResult<()> {
        self.metrics
            .log_store_first_write_epoch
            .set(epoch.curr as _);
        self.metrics
            .log_store_latest_write_epoch
            .set(epoch.curr as _);
        self.inner.init(epoch).await
    }

    async fn write_chunk(&mut self, chunk: StreamChunk) -> LogStoreResult<()> {
        self.metrics
            .log_store_write_rows
            .inc_by(chunk.cardinality() as _);
        self.inner.write_chunk(chunk).await
    }

    async fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        is_checkpoint: bool,
    ) -> LogStoreResult<()> {
        self.inner
            .flush_current_epoch(next_epoch, is_checkpoint)
            .await?;
        self.metrics
            .log_store_latest_write_epoch
            .set(next_epoch as _);
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) -> LogStoreResult<()> {
        self.inner.update_vnode_bitmap(new_vnodes).await
    }
}

#[easy_ext::ext(LogWriterExt)]
impl<T> T
where
    T: LogWriter + Sized,
{
    pub fn monitored(self, metrics: SinkMetrics) -> MonitoredLogWriter<T> {
        MonitoredLogWriter {
            inner: self,
            metrics,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sink::log_store::TruncateOffset;

    #[test]
    fn test_truncate_offset_cmp() {
        assert!(
            TruncateOffset::Barrier { epoch: 232 }
                < TruncateOffset::Chunk {
                    epoch: 233,
                    chunk_id: 1
                }
        );
        assert_eq!(
            TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 1
            },
            TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 1
            }
        );
        assert!(
            TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 1
            } < TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 2
            }
        );
        assert!(
            TruncateOffset::Barrier { epoch: 1 }
                > TruncateOffset::Chunk {
                    epoch: 1,
                    chunk_id: 2
                }
        );
        assert!(
            TruncateOffset::Chunk {
                epoch: 1,
                chunk_id: 2
            } < TruncateOffset::Barrier { epoch: 1 }
        );
        assert!(
            TruncateOffset::Chunk {
                epoch: 2,
                chunk_id: 2
            } > TruncateOffset::Barrier { epoch: 1 }
        );
        assert!(TruncateOffset::Barrier { epoch: 2 } > TruncateOffset::Barrier { epoch: 1 });
    }
}
