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
use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::{poll_fn, Future};
use std::sync::Arc;
use std::task::Poll;

use anyhow::anyhow;
use futures::{TryFuture, TryFutureExt};
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::util::epoch::EpochPair;

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

pub trait LogWriter {
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

#[easy_ext::ext(LogStoreTransformChunkLogReader)]
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
}

enum DeliveryFutureManagerItem<F> {
    Chunk {
        chunk_id: ChunkId,
        // earlier future at the front
        futures: VecDeque<F>,
    },
    Barrier,
}

pub struct DeliveryFutureManager<F> {
    future_count: usize,
    max_future_count: usize,
    // earlier items at the front
    items: VecDeque<(u64, DeliveryFutureManagerItem<F>)>,
}

impl<F> DeliveryFutureManager<F> {
    pub fn new(max_future_count: usize) -> Self {
        Self {
            future_count: 0,
            max_future_count,
            items: Default::default(),
        }
    }

    pub fn add_barrier(&mut self, epoch: u64) {
        if let Some((item_epoch, last_item)) = self.items.back() {
            match last_item {
                DeliveryFutureManagerItem::Chunk { .. } => {
                    assert_eq!(*item_epoch, epoch)
                }
                DeliveryFutureManagerItem::Barrier => {
                    assert!(
                        epoch > *item_epoch,
                        "new barrier epoch {} should be greater than prev barrier {}",
                        epoch,
                        item_epoch
                    );
                }
            }
        }
        self.items
            .push_back((epoch, DeliveryFutureManagerItem::Barrier));
    }

    pub fn start_write_chunk(
        &mut self,
        epoch: u64,
        chunk_id: ChunkId,
    ) -> DeliveryFutureManagerAddFuture<'_, F> {
        if let Some((item_epoch, item)) = self.items.back() {
            match item {
                DeliveryFutureManagerItem::Chunk {
                    chunk_id: item_chunk_id,
                    ..
                } => {
                    assert_eq!(epoch, *item_epoch);
                    assert!(
                        chunk_id > *item_chunk_id,
                        "new chunk id {} should be greater than prev chunk id {}",
                        chunk_id,
                        item_chunk_id
                    );
                }
                DeliveryFutureManagerItem::Barrier => {
                    assert!(
                        epoch > *item_epoch,
                        "new chunk epoch {} should be greater than prev barrier: {}",
                        epoch,
                        item_epoch
                    );
                }
            }
        }
        self.items.push_back((
            epoch,
            DeliveryFutureManagerItem::Chunk {
                chunk_id,
                futures: VecDeque::new(),
            },
        ));
        DeliveryFutureManagerAddFuture(self)
    }
}

pub struct DeliveryFutureManagerAddFuture<'a, F>(&'a mut DeliveryFutureManager<F>);

impl<'a, F: TryFuture<Ok = ()> + Unpin + 'static> DeliveryFutureManagerAddFuture<'a, F> {
    /// Add a new future to the latest started written chunk.
    /// The returned bool value indicate whether we have awaited on any previous futures.
    pub async fn add_future_may_await(&mut self, future: F) -> Result<bool, F::Error> {
        let mut has_await = false;
        while self.0.future_count >= self.0.max_future_count {
            self.await_one_delivery().await?;
            has_await = true;
        }
        match self.0.items.back_mut() {
            Some((_, DeliveryFutureManagerItem::Chunk { futures, .. })) => {
                futures.push_back(future);
                self.0.future_count += 1;
                Ok(has_await)
            }
            _ => unreachable!("should add future only after add a new chunk"),
        }
    }

    pub async fn await_one_delivery(&mut self) -> Result<(), F::Error> {
        for (_, item) in self.0.items.iter_mut().rev() {
            if let DeliveryFutureManagerItem::Chunk {futures, ..} = item && !futures.is_empty() {
                let mut delivery_future = futures.pop_front().expect("have checked non-empty");
                self.0.future_count -= 1;
                return poll_fn(|cx| delivery_future.try_poll_unpin(cx)).await;
            } else {
                continue;
            }
        }
        Ok(())
    }

    pub fn future_count(&self) -> usize {
        self.0.future_count
    }

    pub fn max_future_count(&self) -> usize {
        self.0.max_future_count
    }
}

impl<F: TryFuture<Ok = ()> + Unpin + 'static> DeliveryFutureManager<F> {
    pub fn next_truncate_offset(
        &mut self,
    ) -> impl Future<Output = Result<TruncateOffset, F::Error>> + '_ {
        poll_fn(move |cx| {
            let mut latest_offset: Option<TruncateOffset> = None;
            'outer: loop {
                if let Some((epoch, item)) = self.items.front_mut() {
                    match item {
                        DeliveryFutureManagerItem::Chunk { chunk_id, futures } => {
                            while let Some(future) = futures.front_mut() {
                                match future.try_poll_unpin(cx) {
                                    Poll::Ready(result) => match result {
                                        Ok(()) => {
                                            self.future_count -= 1;
                                            futures.pop_front();
                                        }
                                        Err(e) => {
                                            return Poll::Ready(Err(e));
                                        }
                                    },
                                    Poll::Pending => {
                                        break 'outer;
                                    }
                                }
                            }

                            // when we reach here, there must not be any pending or error future.
                            // Which means all futures of this stream chunk have been finished
                            assert!(futures.is_empty());
                            latest_offset = Some(TruncateOffset::Chunk {
                                epoch: *epoch,
                                chunk_id: *chunk_id,
                            });
                            self.items.pop_front().expect("items not empty");
                        }
                        DeliveryFutureManagerItem::Barrier => {
                            // Barrier will be yielded anyway
                            return Poll::Ready(Ok(TruncateOffset::Barrier { epoch: *epoch }));
                        }
                    }
                }
            }
            if let Some(offset) = latest_offset {
                Poll::Ready(Ok(offset))
            } else {
                Poll::Pending
            }
        })
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
