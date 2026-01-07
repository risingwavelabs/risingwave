use std::collections::VecDeque;

use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_connector::sink::log_store::ChunkId;
use risingwave_pb::id::ActorId;
use risingwave_storage::store::LocalStateStore;

use crate::common::log_store_impl::kv_log_store::SeqId;
use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferItem;
use crate::common::log_store_impl::kv_log_store::state::{
    LogStorePostSealCurrentEpoch, LogStoreWriteState,};
use crate::common::log_store_impl::kv_log_store::LogStoreVnodeProgress;
use crate::executor::{Barrier, StreamExecutorResult, SyncedKvLogStoreMetrics};

/// Buffer used by the synced log-store executors to coordinate write/read.
pub(crate) struct SyncedLogStoreBuffer {
    pub(crate) buffer: VecDeque<(u64, LogStoreBufferItem)>,
    pub(crate) current_size: usize,
    pub(crate) max_size: usize,
    pub(crate) max_chunk_size: usize,
    pub(crate) next_chunk_id: ChunkId,
    pub(crate) metrics: SyncedKvLogStoreMetrics,
    pub(crate) flushed_count: usize,
}

impl SyncedLogStoreBuffer {
    pub(crate) fn is_empty(&self) -> bool {
        self.current_size == 0
    }

    pub(crate) fn add_or_flush_chunk(
        &mut self,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        chunk: StreamChunk,
        epoch: u64,
    ) -> Option<StreamChunk> {
        let current_size = self.current_size;
        let chunk_size = chunk.cardinality();

        tracing::trace!(
            current_size,
            chunk_size,
            max_size = self.max_size,
            "checking chunk size"
        );
        let should_flush_chunk = current_size + chunk_size > self.max_size;
        if should_flush_chunk {
            tracing::trace!(start_seq_id, end_seq_id, epoch, "flushing chunk",);
            Some(chunk)
        } else {
            tracing::trace!(start_seq_id, end_seq_id, epoch, "buffering chunk",);
            self.add_chunk_to_buffer(chunk, start_seq_id, end_seq_id, epoch);
            None
        }
    }

    /// After flushing a chunk, we will preserve a `FlushedItem` inside the buffer.
    /// This doesn't contain any data, but it contains the metadata to read the flushed chunk.
    pub(crate) fn add_flushed_item_to_buffer(
        &mut self,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        new_vnode_bitmap: Bitmap,
        epoch: u64,
    ) {
        let new_chunk_size = (end_seq_id - start_seq_id + 1) as usize;

        if let Some((
            item_epoch,
            LogStoreBufferItem::Flushed {
                start_seq_id: prev_start_seq_id,
                end_seq_id: prev_end_seq_id,
                vnode_bitmap,
                ..
            },
        )) = self.buffer.back_mut()
            && let flushed_chunk_size = (*prev_end_seq_id - *prev_start_seq_id + 1) as usize
            && let projected_flushed_chunk_size = flushed_chunk_size + new_chunk_size
            && projected_flushed_chunk_size <= self.max_chunk_size
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
            *prev_end_seq_id = end_seq_id;
            *vnode_bitmap |= new_vnode_bitmap;
        } else {
            let chunk_id = self.next_chunk_id;
            self.next_chunk_id += 1;
            self.buffer.push_back((
                epoch,
                LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    vnode_bitmap: new_vnode_bitmap,
                    chunk_id,
                },
            ));
            self.flushed_count += 1;
            tracing::trace!(
                "adding flushed item to buffer: start_seq_id: {start_seq_id}, end_seq_id: {end_seq_id}, chunk_id: {chunk_id}"
            );
        }
        // FIXME(kwannoel): Seems these metrics are updated _after_ the flush info is reported.
        self.update_unconsumed_buffer_metrics();
    }

    fn add_chunk_to_buffer(
        &mut self,
        chunk: StreamChunk,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        epoch: u64,
    ) {
        let chunk_id = self.next_chunk_id;
        self.next_chunk_id += 1;
        self.current_size += chunk.cardinality();
        self.buffer.push_back((
            epoch,
            LogStoreBufferItem::StreamChunk {
                chunk,
                start_seq_id,
                end_seq_id,
                flushed: false,
                chunk_id,
            },
        ));
        self.update_unconsumed_buffer_metrics();
    }

    pub(crate) fn pop_front(&mut self) -> Option<(u64, LogStoreBufferItem)> {
        let item = self.buffer.pop_front();
        match &item {
            Some((_, LogStoreBufferItem::Flushed { .. })) => {
                self.flushed_count -= 1;
            }
            Some((_, LogStoreBufferItem::StreamChunk { chunk, .. })) => {
                self.current_size -= chunk.cardinality();
            }
            _ => {}
        }
        self.update_unconsumed_buffer_metrics();
        item
    }

    fn update_unconsumed_buffer_metrics(&self) {
        let mut epoch_count = 0;
        let mut row_count = 0;
        for (_, item) in &self.buffer {
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
        }
        self.metrics.buffer_unconsumed_epoch_count.set(epoch_count);
        self.metrics.buffer_unconsumed_row_count.set(row_count as _);
        self.metrics
            .buffer_unconsumed_item_count
            .set(self.buffer.len() as _);
        self.metrics.buffer_unconsumed_min_epoch.set(
            self.buffer
                .front()
                .map(|(epoch, _)| *epoch)
                .unwrap_or_default() as _,
        );
    }
}

/// Write barrier into log-store, flushing pending chunks when needed.
pub(crate) async fn write_barrier<'a, S: LocalStateStore>(
    actor_id: ActorId,
    write_state: &'a mut LogStoreWriteState<S>,
    barrier: Barrier,
    metrics: &SyncedKvLogStoreMetrics,
    progress: LogStoreVnodeProgress,
    buffer: &mut SyncedLogStoreBuffer,
) -> StreamExecutorResult<LogStorePostSealCurrentEpoch<'a, S>> {
    tracing::trace!(%actor_id, ?progress, "applying truncation");

    let epoch = barrier.epoch.prev;
    let mut writer = write_state.start_writer(false);
    writer.write_barrier(epoch, barrier.is_checkpoint())?;

    if barrier.is_checkpoint() {
        for (epoch, item) in buffer.buffer.iter_mut().rev() {
            match item {
                LogStoreBufferItem::StreamChunk {
                    chunk,
                    start_seq_id,
                    end_seq_id,
                    flushed,
                    ..
                } => {
                    if !*flushed {
                        writer.write_chunk(chunk, *epoch, *start_seq_id, *end_seq_id)?;
                        *flushed = true;
                    } else {
                        break;
                    }
                }
                LogStoreBufferItem::Flushed { .. } | LogStoreBufferItem::Barrier { .. } => {}
            }
        }
    }

    // Apply truncation
    let (flush_info, _) = writer.finish().await?;
    metrics
        .storage_write_count
        .inc_by(flush_info.flush_count as _);
    metrics
        .storage_write_size
        .inc_by(flush_info.flush_size as _);
    let post_seal = write_state.seal_current_epoch(barrier.epoch.curr, progress);

    // Add to buffer
    buffer.buffer.push_back((
        epoch,
        LogStoreBufferItem::Barrier {
            is_checkpoint: barrier.is_checkpoint(),
            next_epoch: barrier.epoch.curr,
            add_columns: None,
            is_stop: false,
        },
    ));
    buffer.next_chunk_id = 0;
    buffer.update_unconsumed_buffer_metrics();

    Ok(post_seal)
}
