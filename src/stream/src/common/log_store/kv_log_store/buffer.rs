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
use std::sync::Arc;

use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use tokio::sync::{oneshot, Notify};

use crate::common::log_store::kv_log_store::{ReaderTruncationOffsetType, SeqIdType};

pub(crate) enum LogStoreBufferItem {
    StreamChunk(StreamChunk),

    Flushed {
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
    },

    Barrier {
        is_checkpoint: bool,
        next_epoch: u64,
    },

    UpdateVnodes(Arc<Bitmap>),
}

struct LogStoreBufferInner {
    queue: VecDeque<LogStoreBufferItem>,
    stream_chunk_count: usize,
    max_stream_chunk_count: usize,

    updated_truncation: Option<ReaderTruncationOffsetType>,
}

impl LogStoreBufferInner {
    fn can_add_stream_chunk(&self) -> bool {
        self.stream_chunk_count < self.max_stream_chunk_count
    }

    fn add_item(&mut self, item: LogStoreBufferItem) {
        if let LogStoreBufferItem::StreamChunk(_) = &item {
            self.stream_chunk_count += 1;
            if self.stream_chunk_count > self.max_stream_chunk_count {
                warn!(
                    "stream chunk count {} exceeds max stream chunk count {}",
                    self.stream_chunk_count, self.max_stream_chunk_count
                );
            }
        }
        self.queue.push_front(item);
    }

    fn pop_item(&mut self) -> Option<LogStoreBufferItem> {
        let ret = self.queue.pop_back();
        if let Some(LogStoreBufferItem::StreamChunk(_)) = &ret {
            self.stream_chunk_count -= 1;
        }
        ret
    }

    fn add_flushed(&mut self, start_seq_id: SeqIdType, end_seq_id: SeqIdType) {
        if let Some(LogStoreBufferItem::Flushed {
            end_seq_id: prev_end_seq_id,
            ..
        }) = self.queue.front_mut()
        {
            assert!(
                *prev_end_seq_id < start_seq_id,
                "prev end_seq_id {} should be smaller than current start_seq_id {}",
                end_seq_id,
                start_seq_id
            );
            *prev_end_seq_id = end_seq_id;
        } else {
            self.add_item(LogStoreBufferItem::Flushed {
                start_seq_id,
                end_seq_id,
            });
        }
    }
}

pub(crate) struct LogStoreBufferSender {
    init_epoch_tx: Option<oneshot::Sender<u64>>,
    buffer: Arc<spin::Mutex<LogStoreBufferInner>>,
    update_notify: Arc<Notify>,
}

impl LogStoreBufferSender {
    fn buffer_inner(&self) -> spin::MutexGuard<'_, LogStoreBufferInner> {
        self.buffer.try_lock().expect("should be able to acquire the lock because there should not be any parallel contention")
    }

    pub(crate) fn init(&mut self, epoch: u64) {
        if let Err(e) = self
            .init_epoch_tx
            .take()
            .expect("should be Some in first init")
            .send(epoch)
        {
            error!("unable to send init epoch: {}", e);
        }
    }

    pub(crate) fn add_flushed(&self, start_seq_id: SeqIdType, end_seq_id: SeqIdType) {
        self.buffer_inner().add_flushed(start_seq_id, end_seq_id);
        self.update_notify.notify_waiters();
    }

    pub(crate) fn try_add_stream_chunk(&self, chunk: StreamChunk) -> Option<StreamChunk> {
        let mut buffer_inner = self.buffer_inner();
        if buffer_inner.can_add_stream_chunk() {
            buffer_inner.add_item(LogStoreBufferItem::StreamChunk(chunk));
            self.update_notify.notify_waiters();
            None
        } else {
            Some(chunk)
        }
    }

    pub(crate) fn barrier(&self, is_checkpoint: bool, next_epoch: u64) {
        self.buffer_inner().add_item(LogStoreBufferItem::Barrier {
            is_checkpoint,
            next_epoch,
        });
        self.update_notify.notify_waiters();
    }

    pub(crate) fn update_vnode(&self, vnode: Arc<Bitmap>) {
        self.buffer_inner()
            .add_item(LogStoreBufferItem::UpdateVnodes(vnode));
        self.update_notify.notify_waiters();
    }

    pub(crate) fn pop_truncation(&self) -> Option<ReaderTruncationOffsetType> {
        self.buffer_inner().updated_truncation.take()
    }
}

pub(crate) struct LogStoreBufferReceiver {
    init_epoch_rx: Option<oneshot::Receiver<u64>>,
    buffer: Arc<spin::Mutex<LogStoreBufferInner>>,
    update_notify: Arc<Notify>,
}

impl LogStoreBufferReceiver {
    pub(crate) async fn init(&mut self) -> u64 {
        self.init_epoch_rx
            .take()
            .expect("should be Some in first init")
            .await
            .expect("should get the first epoch")
    }

    pub(crate) async fn next_item(&self) -> LogStoreBufferItem {
        let notified = self.update_notify.notified();
        if let Some(item) = self
            .buffer
            .try_lock()
            .expect("should get the lock")
            .pop_item()
        {
            item
        } else {
            notified.await;
            self.buffer
                .try_lock()
                .expect("should get the lock")
                .pop_item()
                .expect("should get the item because notified")
        }
    }
}

pub(crate) fn new_log_store_buffer(
    max_stream_chunk_count: usize,
) -> (LogStoreBufferSender, LogStoreBufferReceiver) {
    let buffer = Arc::new(spin::Mutex::new(LogStoreBufferInner {
        queue: VecDeque::new(),
        stream_chunk_count: 0,
        max_stream_chunk_count,
        updated_truncation: None,
    }));
    let update_notify = Arc::new(Notify::new());
    let (init_epoch_tx, init_epoch_rx) = oneshot::channel();
    let tx = LogStoreBufferSender {
        init_epoch_tx: Some(init_epoch_tx),
        buffer: buffer.clone(),
        update_notify: update_notify.clone(),
    };

    let rx = LogStoreBufferReceiver {
        init_epoch_rx: Some(init_epoch_rx),
        buffer,
        update_notify,
    };

    (tx, rx)
}
