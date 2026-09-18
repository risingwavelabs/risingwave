// Copyright 2026 RisingWave Labs
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

use std::ops::Range;

use await_tree::{InstrumentAwait, SpanExt};
use bytes::Bytes;
use fail::fail_point;
use risingwave_hummock_sdk::sstable_info::SstableInfo;

use crate::hummock::block_stream::BlockDataStream;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockResult, TableHolder};

/// Streams a caller-selected range of physical SST blocks, resuming at block boundaries on I/O
/// errors. Decoding and deciding which blocks may be copied belong to the consuming iterator.
pub(super) struct SstableBlockStream {
    pub(super) sstable: TableHolder,
    pub(super) sstable_info: SstableInfo,
    sstable_store: SstableStoreRef,
    block_stream: Option<BlockDataStream>,
    /// Absolute SST indices. Advance the start only after a complete block has been read.
    remaining_blocks: Range<usize>,
    io_retry_times: usize,
    max_io_retry_times: usize,
}

impl SstableBlockStream {
    pub(super) fn new(
        sstable: TableHolder,
        block_metas_range: Range<usize>,
        sstable_info: SstableInfo,
        sstable_store: SstableStoreRef,
        max_io_retry_times: usize,
    ) -> Self {
        Self {
            sstable,
            sstable_info,
            sstable_store,
            block_stream: None,
            remaining_blocks: block_metas_range,
            io_retry_times: 0,
            max_io_retry_times,
        }
    }

    pub(super) fn next_block_index(&self) -> usize {
        self.remaining_blocks.start
    }

    pub(super) fn has_next_block(&self) -> bool {
        !self.remaining_blocks.is_empty()
    }

    pub(super) async fn next_block(&mut self) -> HummockResult<Option<(Bytes, usize)>> {
        while self.has_next_block() {
            if self.block_stream.is_none() {
                // Opening a stream already uses the object store's initialization retry policy.
                // An exhausted initialization failure propagates without consuming this budget.
                self.block_stream = Some(
                    self.sstable_store
                        .get_stream_for_blocks(
                            self.sstable_info.object_id,
                            &self.sstable.meta.block_metas[self.remaining_blocks.clone()],
                        )
                        .instrument_await("stream_iter_get_stream".verbose())
                        .await?,
                );
            }

            match self.block_stream.as_mut().unwrap().next_block().await {
                Ok(Some(block)) => {
                    self.remaining_blocks.start += 1;
                    return Ok(Some(block));
                }
                Ok(None) => {
                    self.remaining_blocks.start = self.remaining_blocks.end;
                }
                Err(e) => {
                    if !e.is_object_error() || self.io_retry_times >= self.max_io_retry_times {
                        return Err(e);
                    }
                    // Discard any partial block and reopen at its original offset. The budget
                    // is cumulative for this SST iterator, not reset after each successful block.
                    self.block_stream = None;
                    self.io_retry_times += 1;
                    fail_point!("create_stream_err");
                    tracing::warn!(
                        object_id = %self.sstable_info.object_id,
                        sst_id = %self.sstable_info.sst_id,
                        meta_offset = self.sstable_info.meta_offset,
                        table_ids = ?self.sstable_info.table_ids,
                        block_index = self.next_block_index(),
                        io_retry_times = self.io_retry_times,
                        "retry create compactor block stream"
                    );
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
