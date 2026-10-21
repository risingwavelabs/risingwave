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

use std::collections::VecDeque;
use std::mem::size_of;
use std::ops::DerefMut;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use parking_lot::{Mutex, MutexGuard};
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::sink::log_store::{ChunkId, LogStoreResult, TruncateOffset};
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use tokio::sync::Notify;

use crate::common::log_store_impl::kv_log_store::{
    KvLogStoreMetrics, ReaderTruncationOffsetType, SeqId,
};

#[derive(Clone)]
pub(crate) enum LogStoreBufferItem {
    StreamChunk {
        chunk: StreamChunk,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        flushed: bool,
        chunk_id: ChunkId,
    },

    Flushed {
        vnode_bitmap: Bitmap,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        chunk_id: ChunkId,
    },

    Barrier {
        is_checkpoint: bool,
        next_epoch: u64,
        schema_change: Option<PbSinkSchemaChange>,
        is_stop: bool,
    },
}

impl EstimateSize for LogStoreBufferItem {
    fn estimated_heap_size(&self) -> usize {
        match self {
            LogStoreBufferItem::StreamChunk { chunk, .. } => chunk.estimated_heap_size(),
            LogStoreBufferItem::Flushed { vnode_bitmap, .. } => vnode_bitmap.estimated_heap_size(),
            LogStoreBufferItem::Barrier { .. } => 0,
        }
    }

    fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.estimated_heap_size()
    }
}

/// Aggregated statistics of the items in a buffer queue, reported by the buffer metrics.
///
/// The statistics are maintained incrementally when an item enters or leaves a queue. Recomputing
/// them by scanning the queues on every buffer operation costs O(buffered items) per operation,
/// which can dominate the CPU of a sink when many items are buffered.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
struct BufferQueueStats {
    epoch_count: usize,
    row_count: usize,
    memory_bytes: usize,
}

impl BufferQueueStats {
    fn of_item(item: &LogStoreBufferItem) -> Self {
        let (epoch_count, row_count) = match item {
            LogStoreBufferItem::StreamChunk { chunk, .. } => (0, chunk.cardinality()),
            LogStoreBufferItem::Flushed {
                start_seq_id,
                end_seq_id,
                ..
            } => (0, (end_seq_id - start_seq_id) as usize),
            LogStoreBufferItem::Barrier { .. } => (1, 0),
        };
        Self {
            epoch_count,
            row_count,
            memory_bytes: item.estimated_size(),
        }
    }

    fn add(&mut self, item: &LogStoreBufferItem) {
        let stats = Self::of_item(item);
        self.epoch_count += stats.epoch_count;
        self.row_count += stats.row_count;
        self.memory_bytes += stats.memory_bytes;
    }

    fn remove(&mut self, item: &LogStoreBufferItem) {
        let stats = Self::of_item(item);
        debug_assert!(
            self.epoch_count >= stats.epoch_count
                && self.row_count >= stats.row_count
                && self.memory_bytes >= stats.memory_bytes,
            "buffer queue stats {:?} smaller than removed item stats {:?}",
            self,
            stats
        );
        // The stats only feed metrics, so never panic on underflow in release builds.
        self.epoch_count = self.epoch_count.saturating_sub(stats.epoch_count);
        self.row_count = self.row_count.saturating_sub(stats.row_count);
        self.memory_bytes = self.memory_bytes.saturating_sub(stats.memory_bytes);
    }
}

struct LogStoreBufferInner {
    /// Items not read by log reader. Newer item at the front
    unconsumed_queue: VecDeque<(u64, LogStoreBufferItem)>,
    /// Items already read by log reader by not truncated. Newer item at the front
    consumed_queue: VecDeque<(u64, LogStoreBufferItem)>,
    /// Stats of the items in `unconsumed_queue`. Must be updated together with the queue.
    unconsumed_stats: BufferQueueStats,
    /// Stats of the items in `consumed_queue`. Must be updated together with the queue.
    consumed_stats: BufferQueueStats,
    row_count: usize,
    max_row_count: usize,
    chunk_size: usize,

    truncation_list: VecDeque<ReaderTruncationOffsetType>,

    next_chunk_id: ChunkId,

    metrics: KvLogStoreMetrics,
}

impl LogStoreBufferInner {
    fn update_buffer_metrics(&self) {
        self.metrics
            .buffer_unconsumed_epoch_count
            .set(self.unconsumed_stats.epoch_count as _);
        self.metrics
            .buffer_unconsumed_row_count
            .set(self.unconsumed_stats.row_count as _);
        self.metrics
            .buffer_unconsumed_item_count
            .set(self.unconsumed_queue.len() as _);
        self.metrics
            .buffer_unconsumed_min_epoch
            .set_optional(self.unconsumed_queue.front().map(|(epoch, _)| *epoch as _));
        self.metrics
            .buffer_memory_bytes
            .set((self.unconsumed_stats.memory_bytes + self.consumed_stats.memory_bytes) as _);
    }

    fn push_unconsumed_front(&mut self, epoch: u64, item: LogStoreBufferItem) {
        self.unconsumed_stats.add(&item);
        self.unconsumed_queue.push_front((epoch, item));
    }

    fn push_unconsumed_back(&mut self, epoch: u64, item: LogStoreBufferItem) {
        self.unconsumed_stats.add(&item);
        self.unconsumed_queue.push_back((epoch, item));
    }

    fn pop_unconsumed_back(&mut self) -> Option<(u64, LogStoreBufferItem)> {
        let (epoch, item) = self.unconsumed_queue.pop_back()?;
        self.unconsumed_stats.remove(&item);
        Some((epoch, item))
    }

    fn push_consumed_front(&mut self, epoch: u64, item: LogStoreBufferItem) {
        self.consumed_stats.add(&item);
        self.consumed_queue.push_front((epoch, item));
    }

    fn pop_consumed_front(&mut self) -> Option<(u64, LogStoreBufferItem)> {
        let (epoch, item) = self.consumed_queue.pop_front()?;
        self.consumed_stats.remove(&item);
        Some((epoch, item))
    }

    fn pop_consumed_back(&mut self) -> Option<(u64, LogStoreBufferItem)> {
        let (epoch, item) = self.consumed_queue.pop_back()?;
        self.consumed_stats.remove(&item);
        Some((epoch, item))
    }

    fn can_add_stream_chunk(&self) -> bool {
        self.row_count < self.max_row_count
    }

    fn add_item(&mut self, epoch: u64, item: LogStoreBufferItem) {
        if let LogStoreBufferItem::StreamChunk { .. } = item {
            unreachable!("StreamChunk should call try_add_item")
        }
        if let LogStoreBufferItem::Barrier { .. } = &item {
            self.next_chunk_id = 0;
        }
        self.push_unconsumed_front(epoch, item);
        self.update_buffer_metrics();
    }

    pub(crate) fn try_add_stream_chunk(
        &mut self,
        epoch: u64,
        chunk: StreamChunk,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
    ) -> Option<StreamChunk> {
        if !self.can_add_stream_chunk() {
            Some(chunk)
        } else {
            let chunk_id = self.next_chunk_id;
            self.next_chunk_id += 1;
            self.row_count += chunk.cardinality();
            self.push_unconsumed_front(
                epoch,
                LogStoreBufferItem::StreamChunk {
                    chunk,
                    start_seq_id,
                    end_seq_id,
                    flushed: false,
                    chunk_id,
                },
            );
            self.update_buffer_metrics();
            None
        }
    }

    fn pop_item(&mut self) -> Option<(u64, LogStoreBufferItem)> {
        if let Some((epoch, item)) = self.pop_unconsumed_back() {
            self.push_consumed_front(epoch, item.clone());
            self.update_buffer_metrics();
            Some((epoch, item))
        } else {
            None
        }
    }

    fn add_flushed(
        &mut self,
        epoch: u64,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        new_vnode_bitmap: Bitmap,
    ) {
        let curr_chunk_size = (end_seq_id - start_seq_id + 1) as usize;
        if let Some((item_epoch, item)) = self.unconsumed_queue.front_mut()
            && let LogStoreBufferItem::Flushed {
                start_seq_id: prev_start_seq_id,
                end_seq_id: prev_end_seq_id,
                ..
            } = item
            && let prev_chunk_size = (*prev_end_seq_id - *prev_start_seq_id + 1) as usize
            && curr_chunk_size + prev_chunk_size <= self.chunk_size
        {
            assert!(
                *prev_end_seq_id < start_seq_id,
                "prev end_seq_id {} should be smaller than current start_seq_id {}",
                end_seq_id,
                start_seq_id
            );
            assert_eq!(
                epoch, *item_epoch,
                "epoch of newly added flushed item must be the same as the last flushed item"
            );
            // Merging changes the row count and possibly the bitmap size of the item, so remove
            // its stats before the update and add them back after it.
            self.unconsumed_stats.remove(item);
            let LogStoreBufferItem::Flushed {
                end_seq_id: prev_end_seq_id,
                vnode_bitmap,
                ..
            } = item
            else {
                unreachable!("matched as flushed item above")
            };
            *prev_end_seq_id = end_seq_id;
            *vnode_bitmap |= new_vnode_bitmap;
            self.unconsumed_stats.add(item);
        } else {
            let chunk_id = self.next_chunk_id;
            self.next_chunk_id += 1;
            self.add_item(
                epoch,
                LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    vnode_bitmap: new_vnode_bitmap,
                    chunk_id,
                },
            );
        }
        self.update_buffer_metrics();
    }

    fn add_truncate_offset(&mut self, (epoch, seq_id): ReaderTruncationOffsetType) {
        if let Some((prev_epoch, prev_seq_id)) = self.truncation_list.back_mut()
            && *prev_epoch == epoch
        {
            *prev_seq_id = seq_id;
        } else {
            self.truncation_list.push_back((epoch, seq_id));
        }
    }

    fn rewind(&mut self, log_store_rewind_start_epoch: Option<u64>) {
        let rewind_start_epoch = log_store_rewind_start_epoch.unwrap_or(0);
        while let Some((epoch, item)) = self.pop_consumed_front() {
            if epoch > rewind_start_epoch {
                self.push_unconsumed_back(epoch, item);
            }
        }
        self.update_buffer_metrics();
    }

    fn clear(&mut self) {
        self.consumed_queue.clear();
        self.unconsumed_queue.clear();
        self.consumed_stats = BufferQueueStats::default();
        self.unconsumed_stats = BufferQueueStats::default();
        self.next_chunk_id = 0;
        self.truncation_list.clear();
        self.row_count = 0;
    }
}

struct SharedMutex<T>(Arc<Mutex<T>>);

impl<T> Clone for SharedMutex<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> SharedMutex<T> {
    fn new(value: T) -> Self {
        Self(Arc::new(Mutex::new(value)))
    }

    fn inner(&self) -> MutexGuard<'_, T> {
        if let Some(guard) = self.0.try_lock() {
            guard
        } else {
            info!("fall back to lock");
            self.0.lock()
        }
    }
}

pub(crate) struct LogStoreBufferSender {
    buffer: SharedMutex<LogStoreBufferInner>,
    update_notify: Arc<Notify>,
    truncate_notify: Arc<Notify>,
}

impl LogStoreBufferSender {
    pub(crate) fn add_flushed(
        &self,
        epoch: u64,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        vnode_bitmap: Bitmap,
    ) {
        self.buffer
            .inner()
            .add_flushed(epoch, start_seq_id, end_seq_id, vnode_bitmap);
        self.update_notify.notify_waiters();
    }

    pub(crate) fn try_add_stream_chunk(
        &self,
        epoch: u64,
        chunk: StreamChunk,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
    ) -> Option<StreamChunk> {
        let ret = self
            .buffer
            .inner()
            .try_add_stream_chunk(epoch, chunk, start_seq_id, end_seq_id);
        if ret.is_none() {
            // notify when successfully add
            self.update_notify.notify_waiters();
        }
        ret
    }

    pub(crate) fn barrier(
        &self,
        epoch: u64,
        is_checkpoint: bool,
        next_epoch: u64,
        schema_change: Option<PbSinkSchemaChange>,
        is_stop: bool,
    ) {
        self.buffer.inner().add_item(
            epoch,
            LogStoreBufferItem::Barrier {
                is_checkpoint,
                next_epoch,
                schema_change,
                is_stop,
            },
        );
        self.update_notify.notify_waiters();
    }

    pub(crate) fn pop_truncation(&self, curr_epoch: u64) -> Option<ReaderTruncationOffsetType> {
        let mut inner = self.buffer.inner();
        let mut ret = None;
        while let Some((epoch, _)) = inner.truncation_list.front()
            && *epoch < curr_epoch
        {
            ret = inner.truncation_list.pop_front();
        }
        ret
    }

    pub(crate) async fn wait_for_barrier_truncation(
        &self,
        curr_epoch: u64,
    ) -> LogStoreResult<ReaderTruncationOffsetType> {
        loop {
            let notified = self.truncate_notify.notified();

            {
                let mut inner = self.buffer.inner();
                while let Some((epoch, seq_id)) = inner.truncation_list.pop_front() {
                    if epoch > curr_epoch {
                        // TODO: should panic, after we confirm the correctness
                        return Err(anyhow::anyhow!(
                            "truncation epoch {} should not be larger than current epoch {}",
                            epoch,
                            curr_epoch
                        ));
                    }
                    if epoch == curr_epoch && seq_id.is_none() {
                        return Ok((epoch, seq_id));
                    }
                }
            }

            notified
                .instrument_await("Wait For Barrier Truncation")
                .await;
        }
    }

    pub(crate) fn flush_all_unflushed(
        &mut self,
        mut flush_fn: impl FnMut(&StreamChunk, u64, SeqId, SeqId) -> LogStoreResult<()>,
    ) -> LogStoreResult<()> {
        let mut inner_guard = self.buffer.inner();
        let inner = inner_guard.deref_mut();
        for (epoch, item) in inner
            .unconsumed_queue
            .iter_mut()
            .chain(inner.consumed_queue.iter_mut())
        {
            if let LogStoreBufferItem::StreamChunk {
                chunk,
                start_seq_id,
                end_seq_id,
                flushed,
                ..
            } = item
            {
                if *flushed {
                    // Since we iterate from new data to old data, when we meet a flushed data, the
                    // rest should have been flushed.
                    break;
                }
                flush_fn(chunk, *epoch, *start_seq_id, *end_seq_id)?;
                *flushed = true;
            }
        }
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.buffer.inner().clear();
    }
}

pub(crate) struct LogStoreBufferReceiver {
    buffer: SharedMutex<LogStoreBufferInner>,
    update_notify: Arc<Notify>,
    truncate_notify: Arc<Notify>,
}

impl LogStoreBufferReceiver {
    pub(crate) async fn next_item(&self) -> (u64, LogStoreBufferItem) {
        let notified = self.update_notify.notified();
        if let Some(item) = { self.buffer.inner().pop_item() } {
            item
        } else {
            notified.instrument_await("Wait For New Buffer Item").await;
            self.buffer
                .inner()
                .pop_item()
                .expect("should get the item because notified")
        }
    }

    pub(crate) fn truncate_buffer(&mut self, offset: TruncateOffset) {
        let mut inner = self.buffer.inner();
        let mut latest_offset: Option<ReaderTruncationOffsetType> = None;
        while let Some((epoch, item)) = inner.consumed_queue.back() {
            let epoch = *epoch;
            match item {
                LogStoreBufferItem::StreamChunk {
                    chunk_id,
                    flushed,
                    end_seq_id,
                    chunk,
                    ..
                } => {
                    let chunk_offset = TruncateOffset::Chunk {
                        epoch,
                        chunk_id: *chunk_id,
                    };
                    let flushed = *flushed;
                    let end_seq_id = *end_seq_id;
                    if chunk_offset <= offset {
                        inner.row_count -= chunk.cardinality();
                        inner.pop_consumed_back();
                        if flushed {
                            latest_offset = Some((epoch, Some(end_seq_id)));
                        }
                    } else {
                        break;
                    }
                }
                LogStoreBufferItem::Flushed {
                    chunk_id,
                    end_seq_id,
                    ..
                } => {
                    let end_seq_id = *end_seq_id;
                    let chunk_offset = TruncateOffset::Chunk {
                        epoch,
                        chunk_id: *chunk_id,
                    };
                    if chunk_offset <= offset {
                        inner.pop_consumed_back();
                        latest_offset = Some((epoch, Some(end_seq_id)));
                    } else {
                        break;
                    }
                }
                LogStoreBufferItem::Barrier { .. } => {
                    let chunk_offset = TruncateOffset::Barrier { epoch };
                    if chunk_offset <= offset {
                        inner.pop_consumed_back();
                        latest_offset = Some((epoch, None));
                    } else {
                        break;
                    }
                }
            }
        }
        inner.update_buffer_metrics();
        if let Some(offset) = latest_offset {
            inner.add_truncate_offset(offset);
            self.truncate_notify.notify_waiters();
        }
    }

    pub(crate) fn truncate_historical(&mut self, epoch: u64) {
        let mut inner = self.buffer.inner();
        inner.add_truncate_offset((epoch, None));
        self.truncate_notify.notify_waiters();
    }

    pub(crate) fn rewind(&self, log_store_rewind_start_epoch: Option<u64>) {
        self.buffer.inner().rewind(log_store_rewind_start_epoch)
    }
}

pub(crate) fn new_log_store_buffer(
    max_row_count: usize,
    chunk_size: usize,
    metrics: KvLogStoreMetrics,
) -> (LogStoreBufferSender, LogStoreBufferReceiver) {
    let buffer = SharedMutex::new(LogStoreBufferInner {
        unconsumed_queue: VecDeque::new(),
        consumed_queue: VecDeque::new(),
        unconsumed_stats: BufferQueueStats::default(),
        consumed_stats: BufferQueueStats::default(),
        row_count: 0,
        max_row_count,
        chunk_size,
        truncation_list: VecDeque::new(),
        next_chunk_id: 0,
        metrics,
    });
    let update_notify = Arc::new(Notify::new());
    let truncate_notify = Arc::new(Notify::new());
    let tx = LogStoreBufferSender {
        buffer: buffer.clone(),
        update_notify: update_notify.clone(),
        truncate_notify: truncate_notify.clone(),
    };

    let rx = LogStoreBufferReceiver {
        buffer,
        update_notify,
        truncate_notify,
    };

    (tx, rx)
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::util::epoch::test_epoch;

    use super::*;

    /// Computes the buffer metrics by scanning the whole buffer, which is what the buffer did on
    /// every operation before the stats were maintained incrementally.
    fn scan_buffer_metrics(inner: &LogStoreBufferInner) -> (usize, usize, usize) {
        let mut epoch_count = 0;
        let mut row_count = 0;
        let mut memory_bytes = 0;
        for (_, item) in &inner.unconsumed_queue {
            match item {
                LogStoreBufferItem::StreamChunk { chunk, .. } => {
                    row_count += chunk.cardinality();
                }
                LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    ..
                } => {
                    row_count += (end_seq_id - start_seq_id) as usize;
                }
                LogStoreBufferItem::Barrier { .. } => {
                    epoch_count += 1;
                }
            }
            memory_bytes += item.estimated_size();
        }
        for (_, item) in &inner.consumed_queue {
            memory_bytes += item.estimated_size();
        }
        (epoch_count, row_count, memory_bytes)
    }

    /// Checks that the incrementally maintained metrics match a full scan of the buffer, and
    /// returns `(epoch_count, row_count, memory_bytes)`.
    fn check_buffer_metrics(rx: &LogStoreBufferReceiver) -> (usize, usize, usize) {
        let inner = rx.buffer.inner();
        let (epoch_count, row_count, memory_bytes) = scan_buffer_metrics(&inner);
        let metrics = &inner.metrics;
        assert_eq!(
            metrics.buffer_unconsumed_epoch_count.get(),
            epoch_count as i64
        );
        assert_eq!(metrics.buffer_unconsumed_row_count.get(), row_count as i64);
        assert_eq!(
            metrics.buffer_unconsumed_item_count.get(),
            inner.unconsumed_queue.len() as i64
        );
        assert_eq!(metrics.buffer_memory_bytes.get(), memory_bytes as i64);
        (epoch_count, row_count, memory_bytes)
    }

    #[tokio::test]
    async fn test_buffer_metrics_match_full_scan() {
        let (mut tx, mut rx) = new_log_store_buffer(1024, 4, KvLogStoreMetrics::for_test());
        let epoch1 = test_epoch(1);
        let epoch2 = test_epoch(2);

        assert!(
            tx.try_add_stream_chunk(
                epoch1,
                StreamChunk::from_pretty(
                    " I I
                    + 1 2
                    + 3 4",
                ),
                0,
                1,
            )
            .is_none()
        );
        assert_eq!(check_buffer_metrics(&rx).1, 2);
        assert!(
            tx.try_add_stream_chunk(
                epoch1,
                StreamChunk::from_pretty(
                    " I I
                    + 5 6
                    - 1 2
                    + 7 8",
                ),
                2,
                4,
            )
            .is_none()
        );
        assert_eq!(check_buffer_metrics(&rx).1, 5);

        // A new flushed item, since the newest item is a stream chunk.
        tx.add_flushed(epoch1, 5, 5, Bitmap::zeros(4));
        // Merged into the previous flushed item. The bitmap grows from no heap allocation.
        let before_merge = check_buffer_metrics(&rx);
        tx.add_flushed(
            epoch1,
            6,
            6,
            Bitmap::from_bool_slice(&[true, false, false, false]),
        );
        let after_merge = check_buffer_metrics(&rx);
        assert_eq!(after_merge.1, before_merge.1 + 1);
        assert!(after_merge.2 > before_merge.2);
        // Merged again. The bitmap becomes all ones and drops its heap allocation.
        tx.add_flushed(epoch1, 7, 7, Bitmap::ones(4));
        let after_merge_all_ones = check_buffer_metrics(&rx);
        assert!(after_merge_all_ones.2 < after_merge.2);
        // Exceeds the chunk size of the previous flushed item, so a new flushed item is added.
        tx.add_flushed(epoch1, 8, 9, Bitmap::zeros(4));
        check_buffer_metrics(&rx);
        tx.barrier(epoch1, true, epoch2, None, false);
        assert_eq!(check_buffer_metrics(&rx).0, 1);
        assert_eq!(rx.buffer.inner().unconsumed_queue.len(), 5);

        // Consume some items. They move from the unconsumed queue to the consumed queue.
        for _ in 0..3 {
            rx.next_item().await;
            check_buffer_metrics(&rx);
        }
        assert_eq!(rx.buffer.inner().consumed_queue.len(), 3);

        // Truncate the first chunk.
        rx.truncate_buffer(TruncateOffset::Chunk {
            epoch: epoch1,
            chunk_id: 0,
        });
        check_buffer_metrics(&rx);
        assert_eq!(rx.buffer.inner().consumed_queue.len(), 2);

        // Rewind moves the consumed items back to the unconsumed queue.
        rx.rewind(None);
        check_buffer_metrics(&rx);
        assert!(rx.buffer.inner().consumed_queue.is_empty());
        assert_eq!(rx.buffer.inner().unconsumed_queue.len(), 4);

        // Consume and truncate everything.
        for _ in 0..4 {
            rx.next_item().await;
            check_buffer_metrics(&rx);
        }
        rx.truncate_buffer(TruncateOffset::Barrier { epoch: epoch1 });
        assert_eq!(check_buffer_metrics(&rx), (0, 0, 0));
        {
            let inner = rx.buffer.inner();
            assert!(inner.consumed_queue.is_empty() && inner.unconsumed_queue.is_empty());
            assert_eq!(inner.consumed_stats, BufferQueueStats::default());
            assert_eq!(inner.unconsumed_stats, BufferQueueStats::default());
        }

        // Clearing the buffer also resets the stats.
        assert!(
            tx.try_add_stream_chunk(
                epoch2,
                StreamChunk::from_pretty(
                    " I I
                    + 1 2",
                ),
                10,
                10,
            )
            .is_none()
        );
        rx.next_item().await;
        tx.barrier(epoch2, true, test_epoch(3), None, false);
        check_buffer_metrics(&rx);
        tx.clear();
        let inner = rx.buffer.inner();
        assert_eq!(inner.consumed_stats, BufferQueueStats::default());
        assert_eq!(inner.unconsumed_stats, BufferQueueStats::default());
    }
}
