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

//! Adapters to and from the unchanged native SST and Foyer metadata formats.
//! Native keys borrow the input; Foyer retains only the previous/current full block key.
//! Both feed the same index builder without materializing the full raw block sequence.

use std::fmt::Formatter;

use bytes::Buf;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::key::FullKey;
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::super::{
    BlockMeta, BlockMetaRef, MonotonicDeleteEvent, NativeMetaTail, Sstable, SstableMeta,
    XorFilterReader, verified_meta_body,
};
use super::key_index::{CachedBlocks, CachedBlocksBuilder};
use super::{CachedBlockMeta, CachedSstableMeta};
use crate::hummock::HummockResult;

impl<'de> Deserialize<'de> for CachedBlocks {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BlocksVisitor;

        impl<'de> Visitor<'de> for BlocksVisitor {
            type Value = CachedBlocks;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a sequence of SST block metadata")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                // Match serde's cautious Vec preallocation for untrusted sequence lengths.
                let initial = seq
                    .size_hint()
                    .unwrap_or(0)
                    .min(1024 * 1024 / std::mem::size_of::<CachedBlockMeta>());
                let mut blocks = CachedBlocksBuilder::new(initial);
                let mut previous = Vec::new();
                while let Some(block) = seq.next_element::<BlockMeta>()? {
                    blocks.push(
                        CachedBlockMeta::from(&block),
                        &block.smallest_key,
                        &previous,
                    );
                    previous = block.smallest_key;
                }
                Ok(blocks.finish())
            }
        }

        deserializer.deserialize_seq(BlocksVisitor)
    }
}

// Keep the original field names/order and serde_bytes layout. Only block_metas uses a
// custom visitor, so deserialization retains at most the current and previous full key.
#[derive(Deserialize)]
pub(in super::super) struct DeserializedSstable {
    id: HummockSstableObjectId,
    meta: DeserializedMeta,
}

#[derive(Deserialize)]
struct DeserializedMeta {
    block_metas: CachedBlocks,
    #[serde(with = "serde_bytes")]
    bloom_filter: Vec<u8>,
    estimated_size: u32,
    key_count: u32,
    #[serde(with = "serde_bytes")]
    smallest_key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    largest_key: Vec<u8>,
    meta_offset: u64,
    monotonic_tombstone_events: Vec<MonotonicDeleteEvent>,
    version: u32,
}

impl From<DeserializedSstable> for Sstable {
    fn from(DeserializedSstable { id, meta }: DeserializedSstable) -> Self {
        let CachedBlocks { block_metas, keys } = meta.block_metas;
        let filter_reader = XorFilterReader::new(&meta.bloom_filter, block_metas.len());
        Self {
            id,
            meta: CachedSstableMeta {
                block_metas,
                keys,
                estimated_size: meta.estimated_size,
                key_count: meta.key_count,
                smallest_key: meta.smallest_key,
                largest_key: meta.largest_key,
                meta_offset: meta.meta_offset,
                monotonic_tombstone_events: meta.monotonic_tombstone_events,
                version: meta.version,
            },
            filter_reader,
            // The bytes determine whether a filter was serialized, as in the legacy decoder.
            skip_bloom_filter_in_serde: false,
        }
    }
}

impl Sstable {
    /// Decodes the existing native SST format directly into its cache representation.
    pub fn from_meta_bytes(
        id: HummockSstableObjectId,
        buf: &[u8],
        skip_bloom_filter_in_serde: bool,
    ) -> HummockResult<Self> {
        let (mut buf, version) = verified_meta_body(buf)?;

        let count = buf.get_u32_le() as usize;
        let mut blocks = CachedBlocksBuilder::new(count);
        let mut previous: &[u8] = &[];
        for _ in 0..count {
            let block = BlockMetaRef::decode(&mut buf, version);
            blocks.push(
                CachedBlockMeta {
                    offset: block.offset,
                    len: block.len,
                    uncompressed_size: block.uncompressed_size,
                    total_key_count: block.total_key_count,
                    stale_key_count: block.stale_key_count,
                    table_id: FullKey::decode(block.smallest_key).user_key.table_id,
                },
                block.smallest_key,
                previous,
            );
            previous = block.smallest_key;
        }
        let tail = NativeMetaTail::decode(&mut buf);
        let filter_reader = XorFilterReader::new(tail.bloom_filter, count);
        let CachedBlocks { block_metas, keys } = blocks.finish();
        Ok(Self {
            id,
            meta: CachedSstableMeta {
                block_metas,
                keys,
                estimated_size: tail.estimated_size,
                key_count: tail.key_count,
                smallest_key: tail.smallest_key,
                largest_key: tail.largest_key,
                meta_offset: tail.meta_offset,
                monotonic_tombstone_events: tail.monotonic_tombstone_events,
                version,
            },
            filter_reader,
            skip_bloom_filter_in_serde,
        })
    }
}

impl From<SstableMeta> for CachedSstableMeta {
    #[expect(deprecated)]
    fn from(meta: SstableMeta) -> Self {
        let mut blocks = CachedBlocksBuilder::new(meta.block_metas.len());
        let mut previous = Vec::new();
        for block in meta.block_metas {
            blocks.push(
                CachedBlockMeta::from(&block),
                &block.smallest_key,
                &previous,
            );
            previous = block.smallest_key;
        }
        let CachedBlocks { block_metas, keys } = blocks.finish();
        Self {
            block_metas,
            keys,
            estimated_size: meta.estimated_size,
            key_count: meta.key_count,
            smallest_key: meta.smallest_key,
            largest_key: meta.largest_key,
            meta_offset: meta.meta_offset,
            monotonic_tombstone_events: meta.monotonic_tombstone_events,
            version: meta.version,
        }
    }
}

impl CachedSstableMeta {
    /// Legacy bincode size, without reconstructing full keys or encoding the SST filter.
    pub(in super::super) fn serde_encoded_size(&self, filter_len: usize) -> usize {
        let blocks_size = 8 + self
            .keys
            .full_key_lengths()
            .map(|key_len| 8 + key_len + 5 * 4)
            .sum::<usize>();
        // Match the remaining MetaRef fields in their established bincode order. Byte slices
        // only contribute their length here, and tombstones keep their existing serde layout.
        let tail_size = bincode::serialized_size(&(
            serde_bytes::Bytes::new(&[]),
            self.estimated_size,
            self.key_count,
            serde_bytes::Bytes::new(&self.smallest_key),
            serde_bytes::Bytes::new(&self.largest_key),
            self.meta_offset,
            &self.monotonic_tombstone_events,
            self.version,
        ))
        .unwrap() as usize;
        blocks_size + tail_size + filter_len
    }

    pub(in super::super) fn serialize<S: Serializer>(
        &self,
        id: HummockSstableObjectId,
        filter: &[u8],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        // Preserve the existing Foyer/bincode field order and full-key sequence. Only one key
        // is reconstructed at a time, including bincode's serialized_size traversal.
        #[derive(Serialize)]
        struct MetaRef<'a> {
            block_metas: BlockMetasRef<'a>,
            bloom_filter: &'a serde_bytes::Bytes,
            estimated_size: u32,
            key_count: u32,
            smallest_key: &'a serde_bytes::Bytes,
            largest_key: &'a serde_bytes::Bytes,
            meta_offset: u64,
            monotonic_tombstone_events: &'a [MonotonicDeleteEvent],
            version: u32,
        }
        #[derive(Serialize)]
        struct SstableRef<'a> {
            id: HummockSstableObjectId,
            meta: MetaRef<'a>,
        }
        SstableRef {
            id,
            meta: MetaRef {
                block_metas: BlockMetasRef(self),
                bloom_filter: serde_bytes::Bytes::new(filter),
                estimated_size: self.estimated_size,
                key_count: self.key_count,
                smallest_key: serde_bytes::Bytes::new(&self.smallest_key),
                largest_key: serde_bytes::Bytes::new(&self.largest_key),
                meta_offset: self.meta_offset,
                monotonic_tombstone_events: &self.monotonic_tombstone_events,
                version: self.version,
            },
        }
        .serialize(serializer)
    }
}

struct BlockMetasRef<'a>(&'a CachedSstableMeta);

impl Serialize for BlockMetasRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.block_metas.len()))?;
        let mut raw = BlockMeta::default();
        let mut keys = self.0.keys.cursor();
        for block in &self.0.block_metas {
            keys.restore_next(block.table_id, &mut raw.smallest_key);
            raw.offset = block.offset;
            raw.len = block.len;
            raw.uncompressed_size = block.uncompressed_size;
            raw.total_key_count = block.total_key_count;
            raw.stale_key_count = block.stale_key_count;
            seq.serialize_element(&raw)?;
        }
        seq.end()
    }
}
