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
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use fail::fail_point;
use risingwave_object_store::object::{MonitoredStreamingReader, ObjectError};

use super::BlockMeta;
use crate::hummock::pin_cache::PinCacheReadHandle;
use crate::hummock::{BlockHolder, HummockResult};

pub struct MemoryUsageTracker {
    total_usage: Arc<AtomicUsize>,
    usage: usize,
}

impl MemoryUsageTracker {
    pub fn new(total_usage: Arc<AtomicUsize>, usage: usize) -> Self {
        total_usage.fetch_add(usage, Ordering::SeqCst);
        Self { total_usage, usage }
    }
}

impl Drop for MemoryUsageTracker {
    fn drop(&mut self) {
        self.total_usage.fetch_sub(self.usage, Ordering::SeqCst);
    }
}

/// An iterator that reads the blocks of an SST step by step from a given stream of bytes.
pub struct BlockDataStream {
    buf_reader: MonitoredStreamingReader,

    /// The index of the next block. Note that `block_idx` is relative to the start index of the
    /// stream (and is compatible with `block_sizes`); it is not relative to the corresponding
    /// SST. That is, if streaming starts at block 2 of a given SST `T`, then `block_idx = 0`
    /// refers to the third block of `T`.
    block_idx: usize,

    /// The sizes of each block which the stream reads. The first number states the compressed size
    /// in the stream. The second number is the block's uncompressed size.  Note that the list does
    /// not contain the size of blocks which precede the first streamed block. That is, if
    /// streaming starts at block 2 of a given SST, then the list does not contain information
    /// about block 0 and block 1.
    block_sizes: Vec<(u32, u32)>,

    buf: Bytes,

    buff_offset: usize,

    local_route: Option<PinCacheReadHandle>,
}

impl BlockDataStream {
    /// Reads the blocks described by `block_metas` from a byte stream positioned at their start.
    /// The block index is relative to this slice, not to the full SST.
    /// Only retain the lengths needed to frame and decode blocks, without cloning their keys.
    pub fn new(
        // The stream that provides raw data.
        byte_stream: MonitoredStreamingReader,
        // Meta data of the SST that is streamed.
        block_metas: &[BlockMeta],
    ) -> Self {
        Self::new_with_local_route(byte_stream, block_metas, None)
    }

    pub(crate) fn new_with_local_route(
        byte_stream: MonitoredStreamingReader,
        block_metas: &[BlockMeta],
        local_route: Option<PinCacheReadHandle>,
    ) -> Self {
        Self {
            buf_reader: byte_stream,
            block_idx: 0,
            block_sizes: block_metas
                .iter()
                .map(|meta| (meta.len, meta.uncompressed_size))
                .collect(),
            buf: Bytes::default(),
            buff_offset: 0,
            local_route,
        }
    }

    /// Reads the next block from the stream and returns it. Returns `None` if there are no blocks
    /// left to read.
    pub async fn next_block(&mut self) -> HummockResult<Option<(Bytes, usize)>> {
        let result = self.next_block_inner().await;
        if result.is_err()
            && let Some(local_route) = &self.local_route
        {
            local_route.invalidate();
        }
        result
    }

    async fn next_block_inner(&mut self) -> HummockResult<Option<(Bytes, usize)>> {
        if self.block_idx >= self.block_sizes.len() {
            return Ok(None);
        }

        let (compressed_size, uncompressed_size) = self.block_sizes[self.block_idx];
        fail_point!("stream_read_err", |_| Err(ObjectError::internal(
            "stream read error"
        )
        .into()));
        let uncompressed_size = uncompressed_size as usize;
        let end = self.buff_offset + compressed_size as usize;
        let data = if end > self.buf.len() {
            let current_block = self.read_next_buf(compressed_size as usize).await?;
            self.buff_offset = 0;
            current_block
        } else {
            let data = self.buf.slice(self.buff_offset..end);
            self.buff_offset = end;
            data
        };

        self.block_idx += 1;
        Ok(Some((data, uncompressed_size)))
    }

    async fn read_next_buf(&mut self, read_size: usize) -> HummockResult<Bytes> {
        let mut read_buf = BytesMut::with_capacity(read_size);
        let start_pos = if self.buff_offset < self.buf.len() {
            read_buf.extend_from_slice(&self.buf[self.buff_offset..]);
            self.buf.len() - self.buff_offset
        } else {
            0
        };
        let mut rest = read_size - start_pos;
        while rest > 0 {
            let next_packet = self
                .buf_reader
                .read_bytes()
                .await
                .unwrap_or_else(|| Err(ObjectError::internal("read unexpected EOF")))?;
            let read_len = std::cmp::min(next_packet.len(), rest);
            read_buf.extend_from_slice(&next_packet[..read_len]);
            rest -= read_len;
            if rest == 0 {
                self.buf = next_packet.slice(read_len..);
                return Ok(read_buf.freeze());
            }
        }
        self.buf = Bytes::default();
        Ok(read_buf.freeze())
    }
}

/// Consecutive decoded blocks whose I/O has already completed in `SstableStore::prefetch_blocks`.
/// Consuming them is synchronous and infallible. The tracker is retained until the stream drops.
pub struct PrefetchBlockStream {
    blocks: VecDeque<BlockHolder>,
    /// SST index of the first remaining block, or the end index when exhausted.
    block_index: usize,
    _tracker: Option<MemoryUsageTracker>,
}

pub(super) enum PrefetchLookup {
    Hit(BlockHolder),
    /// The target precedes the remaining range. The stream is unchanged.
    BeforeStart,
    /// All buffered blocks were consumed without reaching the target.
    Exhausted,
}

impl PrefetchBlockStream {
    pub(super) fn new(
        blocks: VecDeque<BlockHolder>,
        block_index: usize,
        _tracker: Option<MemoryUsageTracker>,
    ) -> Self {
        Self {
            blocks,
            block_index,
            _tracker,
        }
    }

    /// Takes the block at the given SST index, discarding earlier buffered blocks.
    /// A backward lookup leaves the stream unchanged; a lookup past the end exhausts it.
    pub(super) fn take_block(&mut self, target: usize) -> PrefetchLookup {
        if target < self.block_index {
            return PrefetchLookup::BeforeStart;
        }
        while let Some(block) = self.blocks.pop_front() {
            let block_index = self.block_index;
            self.block_index += 1;
            if block_index == target {
                return PrefetchLookup::Hit(block);
            }
        }
        PrefetchLookup::Exhausted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::test_utils::test_key_of;
    use crate::hummock::{Block, BlockBuilder, BlockBuilderOptions};

    fn test_blocks() -> Vec<Arc<Block>> {
        (10..14)
            .map(|idx| {
                let mut builder = BlockBuilder::new(BlockBuilderOptions::default());
                builder.add_for_test(test_key_of(idx).to_ref(), b"value");
                let capacity = builder.uncompressed_block_size();
                Arc::new(Block::decode(Bytes::copy_from_slice(builder.build()), capacity).unwrap())
            })
            .collect()
    }

    #[test]
    fn test_prefetch_take_block_and_tracker_lifetime() {
        let blocks = test_blocks();
        let usage = Arc::new(AtomicUsize::new(7));
        let mut stream = PrefetchBlockStream::new(
            blocks
                .iter()
                .cloned()
                .map(BlockHolder::from_ref_block)
                .collect(),
            10,
            Some(MemoryUsageTracker::new(usage.clone(), 100)),
        );
        assert!(matches!(stream.take_block(9), PrefetchLookup::BeforeStart));
        let PrefetchLookup::Hit(first) = stream.take_block(10) else {
            panic!("missing first block");
        };
        assert!(std::ptr::eq(&*first, &*blocks[0]));
        // Neither rereading a consumed block nor seeking backward may consume future blocks.
        assert!(matches!(stream.take_block(10), PrefetchLookup::BeforeStart));
        let PrefetchLookup::Hit(skipped_to) = stream.take_block(12) else {
            panic!("missing block after a forward skip");
        };
        assert!(std::ptr::eq(&*skipped_to, &*blocks[2]));
        assert_eq!(Arc::strong_count(&blocks[1]), 1);
        assert!(matches!(stream.take_block(11), PrefetchLookup::BeforeStart));
        let PrefetchLookup::Hit(last) = stream.take_block(13) else {
            panic!("backward lookup consumed the remaining block");
        };
        assert!(std::ptr::eq(&*last, &*blocks[3]));
        assert!(matches!(stream.take_block(14), PrefetchLookup::Exhausted));
        assert!(matches!(
            stream.take_block(usize::MAX),
            PrefetchLookup::Exhausted
        ));
        assert_eq!(usage.load(Ordering::SeqCst), 107);
        drop(stream);
        assert_eq!(usage.load(Ordering::SeqCst), 7);
        // Returned holders continue to own their blocks after the stream and tracker drop.
        drop(blocks);
        assert!(!first.data().is_empty());
        assert!(!last.data().is_empty());
    }

    #[test]
    fn test_prefetch_take_block_past_end_and_empty() {
        let blocks = test_blocks();
        let mut stream = PrefetchBlockStream::new(
            blocks
                .iter()
                .cloned()
                .map(BlockHolder::from_ref_block)
                .collect(),
            10,
            None,
        );
        assert!(matches!(
            stream.take_block(usize::MAX),
            PrefetchLookup::Exhausted
        ));
        assert!(blocks.iter().all(|block| Arc::strong_count(block) == 1));
        assert!(matches!(stream.take_block(14), PrefetchLookup::Exhausted));
        assert!(matches!(stream.take_block(13), PrefetchLookup::BeforeStart));

        let mut empty = PrefetchBlockStream::new(VecDeque::new(), 10, None);
        assert!(matches!(empty.take_block(9), PrefetchLookup::BeforeStart));
        assert!(matches!(empty.take_block(10), PrefetchLookup::Exhausted));
        assert!(matches!(empty.take_block(11), PrefetchLookup::Exhausted));
    }
}

#[cfg(test)]
mod pin_cache_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_common::config::ObjectStoreConfig;
    use risingwave_hummock_sdk::HummockSstableObjectId;
    use risingwave_object_store::object::{InMemObjectStore, ObjectStore, ObjectStoreImpl};

    use super::{BlockDataStream, BlockMeta};
    use crate::hummock::pin_cache::PinCache;
    use crate::monitor::ObjectStoreMetrics;

    #[tokio::test]
    async fn test_stream_error_invalidates_local_route() {
        let remote_store = Arc::new(ObjectStoreImpl::InMem(
            InMemObjectStore::for_test().monitored(
                Arc::new(ObjectStoreMetrics::unused()),
                Arc::new(ObjectStoreConfig::default()),
            ),
        ));
        let local_store = Arc::new(ObjectStoreImpl::InMem(
            InMemObjectStore::for_test().monitored(
                Arc::new(ObjectStoreMetrics::unused()),
                Arc::new(ObjectStoreConfig::default()),
            ),
        ));
        let pin_cache = PinCache::new(local_store, 1024);
        let object_id = HummockSstableObjectId::from(1001);
        remote_store
            .upload("sst", Bytes::from_static(b"data"))
            .await
            .unwrap();
        pin_cache.replace_desired_objects(HashMap::from([(object_id, 4)]));
        pin_cache
            .pin_sst(remote_store, "sst".into(), object_id)
            .await
            .unwrap();

        let route = pin_cache.get(object_id).unwrap();
        let reader = route.streaming_read(..).await.unwrap();
        let mut stream = BlockDataStream::new_with_local_route(
            reader,
            &[BlockMeta {
                len: 5,
                ..Default::default()
            }],
            Some(route),
        );
        assert!(stream.next_block().await.is_err());
        assert!(pin_cache.get(object_id).is_none());
    }
}
