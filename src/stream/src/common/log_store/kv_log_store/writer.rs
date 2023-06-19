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

use std::future::Future;
use std::ops::Bound::Included;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_storage::store::LocalStateStore;

use crate::common::log_store::kv_log_store::buffer::LogStoreBufferSender;
use crate::common::log_store::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store::kv_log_store::{SeqIdType, FIRST_SEQ_ID};
use crate::common::log_store::{LogStoreResult, LogWriter};

pub struct KvLogStoreWriter<LS: LocalStateStore> {
    _table_id: TableId,

    seq_id: SeqIdType,

    state_store: LS,

    serde: LogStoreRowSerde,

    tx: LogStoreBufferSender,
}

impl<LS: LocalStateStore> KvLogStoreWriter<LS> {
    pub(crate) fn new(
        table_id: TableId,
        state_store: LS,
        serde: LogStoreRowSerde,
        tx: LogStoreBufferSender,
    ) -> Self {
        Self {
            _table_id: table_id,
            seq_id: FIRST_SEQ_ID,
            state_store,
            serde,
            tx,
        }
    }
}

impl<LS: LocalStateStore> LogWriter for KvLogStoreWriter<LS> {
    type FlushCurrentEpoch<'a> = impl Future<Output = LogStoreResult<()>> + 'a;
    type InitFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;
    type WriteChunkFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;

    fn init(&mut self, epoch: u64) -> Self::InitFuture<'_> {
        async move {
            self.state_store.init(epoch);
            self.seq_id = FIRST_SEQ_ID;
            self.tx.init(epoch);
            Ok(())
        }
    }

    fn write_chunk(&mut self, chunk: StreamChunk) -> Self::WriteChunkFuture<'_> {
        async move {
            if let Some(chunk) = self.tx.try_add_stream_chunk(chunk) {
                // When enter this branch, the chunk cannot be added directly, and should be add to
                // state store and flush
                let epoch = self.state_store.epoch();
                let start_seq_id = self.seq_id;
                let mut vnode_bitmap_builder = BitmapBuilder::zeroed(VirtualNode::COUNT);
                for (op, row) in chunk.rows() {
                    let (vnode, key, value) =
                        self.serde.serialize_data_row(epoch, self.seq_id, op, row);
                    vnode_bitmap_builder.set(vnode.to_index(), true);
                    self.state_store.insert(key, value, None)?;
                    self.seq_id += 1;
                }
                let end_seq_id = self.seq_id - 1;

                let mut delete_range = Vec::with_capacity(self.serde.vnodes().count_ones());
                if let Some(truncation_offset) = self.tx.pop_truncation() {
                    for vnode in self.serde.vnodes().iter_vnodes() {
                        let range_begin = Bytes::from(vnode.to_be_bytes().to_vec());
                        let range_end = self
                            .serde
                            .serialize_truncation_offset_watermark(vnode, truncation_offset);
                        delete_range.push((Included(range_begin), Included(range_end)));
                    }
                }
                self.state_store.flush(delete_range).await?;

                let vnode_bitmap = vnode_bitmap_builder.finish();
                self.tx.add_flushed(start_seq_id, end_seq_id, vnode_bitmap);
            }
            Ok(())
        }
    }

    fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        is_checkpoint: bool,
    ) -> Self::FlushCurrentEpoch<'_> {
        async move {
            let epoch = self.state_store.epoch();
            for vnode in self.serde.vnodes().iter_vnodes() {
                let (key, value) = self.serde.serialize_barrier(epoch, vnode, is_checkpoint);
                self.state_store.insert(key, value, None)?;
            }
            self.state_store.flush(Vec::new()).await?;
            self.state_store.seal_current_epoch(next_epoch);
            self.tx.barrier(is_checkpoint, next_epoch);
            self.seq_id = FIRST_SEQ_ID;
            Ok(())
        }
    }

    fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) {
        self.serde.update_vnode_bitmap(new_vnodes.clone());
        self.tx.update_vnode(new_vnodes);
    }
}
