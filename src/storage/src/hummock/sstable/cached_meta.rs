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

use std::collections::HashSet;
use std::fmt::Formatter;
use std::ops::{Bound, Range};

use bytes::Buf;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::key::{FullKey, TABLE_PREFIX_LEN};
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::utils::get_length_prefixed_slice;
use super::{
    BlockMeta, MonotonicDeleteEvent, OLD_VERSION, Sstable, SstableMeta, XorFilterReader,
    verified_meta_body,
};
use crate::hummock::HummockResult;

const KEY_RESTART_INTERVAL: usize = 16;

/// The address and counters of a cached block. Keys belong to `CachedSstableMeta`'s prefix index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CachedBlockMeta {
    pub offset: u32,
    pub len: u32,
    pub uncompressed_size: u32,
    pub total_key_count: u32,
    pub stale_key_count: u32,
    table_id: TableId,
}

impl CachedBlockMeta {
    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

impl From<&BlockMeta> for CachedBlockMeta {
    fn from(block: &BlockMeta) -> Self {
        Self {
            offset: block.offset,
            len: block.len,
            uncompressed_size: block.uncompressed_size,
            total_key_count: block.total_key_count,
            stale_key_count: block.stale_key_count,
            table_id: block.table_id(),
        }
    }
}

/// Cache-resident metadata. Native SST encoding and writer metadata use `SstableMeta` instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CachedSstableMeta {
    pub block_metas: Vec<CachedBlockMeta>,
    keys: PrefixKeys,
    pub estimated_size: u32,
    pub key_count: u32,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub meta_offset: u64,
    pub monotonic_tombstone_events: Vec<MonotonicDeleteEvent>,
    pub version: u32,
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

/// Construction-only accumulator shared by native, Foyer and writer metadata paths.
struct CachedBlocksBuilder {
    block_metas: Vec<CachedBlockMeta>,
    data: Vec<u8>,
    restarts: Vec<usize>,
}

impl CachedBlocksBuilder {
    fn new(block_count: usize) -> Self {
        Self {
            block_metas: Vec::with_capacity(block_count),
            data: Vec::new(),
            restarts: Vec::with_capacity(block_count.div_ceil(KEY_RESTART_INTERVAL)),
        }
    }

    // Callers own the previous key so native decoding can borrow it from the input buffer.
    fn push(&mut self, block: CachedBlockMeta, key: &[u8], previous: &[u8]) {
        let shared = if self.block_metas.len().is_multiple_of(KEY_RESTART_INTERVAL) {
            self.restarts.push(self.data.len());
            0
        } else {
            common_prefix(previous, key)
        };
        let suffix = &key[shared..];
        self.data.extend_from_slice(&(shared as u32).to_le_bytes());
        self.data
            .extend_from_slice(&(suffix.len() as u32).to_le_bytes());
        self.data.extend_from_slice(suffix);
        self.block_metas.push(block);
    }

    fn finish(self) -> CachedBlocks {
        CachedBlocks {
            block_metas: self.block_metas,
            keys: PrefixKeys {
                data: self.data.into_boxed_slice(),
                restarts: self.restarts.into_boxed_slice(),
            },
        }
    }
}

struct CachedBlocks {
    block_metas: Vec<CachedBlockMeta>,
    keys: PrefixKeys,
}

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
pub(super) struct DeserializedSstable {
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
            let offset = buf.get_u32_le();
            let len = buf.get_u32_le();
            let uncompressed_size = buf.get_u32_le();
            let (total_key_count, stale_key_count) = if version == OLD_VERSION {
                (0, 0)
            } else {
                (buf.get_u32_le(), buf.get_u32_le())
            };
            let key = get_length_prefixed_slice(&mut buf);
            blocks.push(
                CachedBlockMeta {
                    offset,
                    len,
                    uncompressed_size,
                    total_key_count,
                    stale_key_count,
                    table_id: FullKey::decode(key).user_key.table_id,
                },
                key,
                previous,
            );
            previous = key;
        }
        let filter_data = get_length_prefixed_slice(&mut buf);
        let filter_reader = XorFilterReader::new(filter_data, count);
        let estimated_size = buf.get_u32_le();
        let key_count = buf.get_u32_le();
        let smallest_key = get_length_prefixed_slice(&mut buf).to_vec();
        let largest_key = get_length_prefixed_slice(&mut buf).to_vec();
        let tomb_event_count = buf.get_u32_le() as usize;
        let mut monotonic_tombstone_events = Vec::with_capacity(tomb_event_count);
        for _ in 0..tomb_event_count {
            monotonic_tombstone_events.push(MonotonicDeleteEvent::decode(&mut buf));
        }
        let meta_offset = buf.get_u64_le();
        if !monotonic_tombstone_events.is_empty() {
            tracing::warn!(
                count = monotonic_tombstone_events.len(),
                tables = ?monotonic_tombstone_events.iter()
                    .map(|event| event.event_key.left_user_key.table_id)
                    .collect::<HashSet<_>>(),
                "read non-empty range tombstones"
            );
        }
        let CachedBlocks { block_metas, keys } = blocks.finish();
        Ok(Self {
            id,
            meta: CachedSstableMeta {
                block_metas,
                keys,
                estimated_size,
                key_count,
                smallest_key,
                largest_key,
                meta_offset,
                monotonic_tombstone_events,
                version,
            },
            filter_reader,
            skip_bloom_filter_in_serde,
        })
    }
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let end = a.len().min(b.len());
    let mut len = 0;
    while len + 32 <= end && a[len..len + 32] == b[len..len + 32] {
        len += 32;
    }
    while len < end && a[len] == b[len] {
        len += 1;
    }
    len
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PrefixKeys {
    data: Box<[u8]>,
    restarts: Box<[usize]>,
}

impl PrefixKeys {
    fn entry(&self, offset: usize) -> (usize, &[u8], usize) {
        let shared = u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()) as usize;
        let len =
            u32::from_le_bytes(self.data[offset + 4..offset + 8].try_into().unwrap()) as usize;
        let end = offset + 8 + len;
        (shared, &self.data[offset + 8..end], end)
    }

    fn decode_next(&self, offset: usize, key: &mut Vec<u8>) -> usize {
        let (shared, suffix, end) = self.entry(offset);
        key.truncate(shared);
        key.extend_from_slice(suffix);
        end
    }
}

impl CachedSstableMeta {
    /// Restores one key into reader-owned scratch, without retaining decoded keys in the cache.
    pub fn block_smallest_key<'a>(&self, index: usize, key: &'a mut Vec<u8>) -> &'a [u8] {
        assert!(index < self.block_metas.len());
        let first = index / KEY_RESTART_INTERVAL * KEY_RESTART_INTERVAL;
        let mut offset = self.keys.restarts[index / KEY_RESTART_INTERVAL];
        key.clear();
        for _ in first..=index {
            offset = self.keys.decode_next(offset, key);
        }
        key
    }

    /// Returns the absolute index after the keys within `bound` in the requested range.
    pub fn partition_point(
        &self,
        range: Range<usize>,
        bound: Bound<FullKey<&[u8]>>,
        key: &mut Vec<u8>,
    ) -> usize {
        assert!(range.start <= range.end && range.end <= self.block_metas.len());
        if range.is_empty() {
            return range.start;
        }
        let (target, inclusive) = match bound {
            Bound::Included(target) => (target, true),
            Bound::Excluded(target) => (target, false),
            Bound::Unbounded => return range.end,
        };
        let before_bound = |key: FullKey<&[u8]>| {
            let order = key.cmp(&target);
            order.is_lt() || (inclusive && order.is_eq())
        };
        let first_group = range.start / KEY_RESTART_INTERVAL;
        let end_group = (range.end - 1) / KEY_RESTART_INTERVAL + 1;
        let first_key = if range.start.is_multiple_of(KEY_RESTART_INTERVAL) {
            self.keys.entry(self.keys.restarts[first_group]).1
        } else {
            self.block_smallest_key(range.start, key)
        };
        if !before_bound(FullKey::decode(first_key)) {
            return range.start;
        }
        let found = self.keys.restarts[first_group + 1..end_group].partition_point(|&offset| {
            let (_, complete, _) = self.keys.entry(offset);
            before_bound(FullKey::decode(complete))
        });
        let group = first_group + found;
        let first = group * KEY_RESTART_INTERVAL;
        let end = range.end.min(first + KEY_RESTART_INTERVAL);
        // The range start or selected restart was already accepted above. Decode through it
        // to initialize prefix reuse, without accepting keys before the requested range.
        let known_true = first.max(range.start);
        if known_true + 1 == end {
            return end;
        }
        let mut offset = self.keys.restarts[group];
        let target_table_key = target.user_key.table_key.0;
        let mut matched = 0;
        key.clear();
        for index in first..end {
            let (shared, suffix, next) = self.keys.entry(offset);
            key.truncate(shared);
            key.extend_from_slice(suffix);
            offset = next;
            if index < known_true {
                continue;
            }
            let current = FullKey::decode(key);
            let order = current.user_key.table_id.cmp(&target.user_key.table_id);
            let order = if order.is_eq() {
                let table_key = current.user_key.table_key.0;
                // Reuse only user-key bytes: an encoded shared prefix may extend into the
                // previous key's epoch when the next user key is longer.
                let reused = matched
                    .min(shared.saturating_sub(TABLE_PREFIX_LEN))
                    .min(table_key.len());
                matched = reused + common_prefix(&table_key[reused..], &target_table_key[reused..]);
                table_key
                    .get(matched)
                    .cmp(&target_table_key.get(matched))
                    .then_with(|| target.epoch_with_gap.cmp(&current.epoch_with_gap))
            } else {
                matched = 0;
                order
            };
            if index > known_true && (order.is_gt() || (!inclusive && order.is_eq())) {
                return index;
            }
        }
        end
    }

    /// Reconstructs an owned `BlockMeta`, allocating its key, for raw copying or inspection.
    pub fn block_meta(&self, index: usize) -> BlockMeta {
        let block = &self.block_metas[index];
        let mut smallest_key = Vec::new();
        self.block_smallest_key(index, &mut smallest_key);
        BlockMeta {
            smallest_key,
            offset: block.offset,
            len: block.len,
            uncompressed_size: block.uncompressed_size,
            total_key_count: block.total_key_count,
            stale_key_count: block.stale_key_count,
        }
    }

    pub(super) fn estimated_heap_size(&self) -> usize {
        self.block_metas.capacity() * std::mem::size_of::<CachedBlockMeta>()
            + self.keys.data.len()
            + std::mem::size_of_val(&*self.keys.restarts)
            + self.smallest_key.capacity()
            + self.largest_key.capacity()
            + self.monotonic_tombstone_events.capacity()
                * std::mem::size_of::<MonotonicDeleteEvent>()
    }

    /// Legacy bincode size, without reconstructing full keys or encoding the SST filter.
    pub(super) fn serde_encoded_size(&self, filter_len: usize) -> usize {
        let mut blocks_size = 8; // Vec length.
        let mut offset = 0;
        for _ in &self.block_metas {
            let (shared, suffix, next) = self.keys.entry(offset);
            blocks_size += 8 + shared + suffix.len() + 5 * 4;
            offset = next;
        }
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

    pub(super) fn serialize<S: Serializer>(
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
        #[derive(Serialize)]
        struct BlockRef<'a> {
            smallest_key: &'a serde_bytes::Bytes,
            offset: u32,
            len: u32,
            uncompressed_size: u32,
            total_key_count: u32,
            stale_key_count: u32,
        }
        let mut seq = serializer.serialize_seq(Some(self.0.block_metas.len()))?;
        let mut key = Vec::new();
        let mut offset = 0;
        for block in &self.0.block_metas {
            offset = self.0.keys.decode_next(offset, &mut key);
            seq.serialize_element(&BlockRef {
                smallest_key: serde_bytes::Bytes::new(&key),
                offset: block.offset,
                len: block.len,
                uncompressed_size: block.uncompressed_size,
                total_key_count: block.total_key_count,
                stale_key_count: block.stale_key_count,
            })?;
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use foyer::Code;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::EpochWithGap;
    use risingwave_hummock_sdk::key::TableKey;
    use risingwave_hummock_sdk::key::range_delete_backward_compatibility_serde_struct::{
        PointRange, UserKey,
    };

    use super::super::{SerdeSstable, VERSION};
    use super::*;

    #[test]
    fn test_cached_meta_long_keys_and_partial_ranges() {
        // Three epochs per user key put equal user keys on both sides of restart 16.
        let blocks: Vec<_> = (0..80)
            .map(|index| {
                let local = index % 40;
                let mut key = vec![b'x'; 70_000];
                key[..2].copy_from_slice(&[0, 1]);
                key.extend_from_slice(&(local / 3_u32).to_be_bytes());
                BlockMeta {
                    smallest_key: FullKey::for_test(
                        TableId::new(1 + index / 40),
                        key,
                        test_epoch((100 - local % 3).into()),
                    )
                    .encode(),
                    offset: index * 100,
                    len: 100,
                    uncompressed_size: 200,
                    total_key_count: 3,
                    stale_key_count: 1,
                }
            })
            .collect();
        let old_key_bytes: usize = blocks.iter().map(|block| block.smallest_key.len()).sum();
        let mut tombstone_key = Vec::new();
        tombstone_key.put_u32(1);
        tombstone_key.put_u32(9);
        tombstone_key.extend_from_slice(b"old-range");

        #[expect(deprecated)]
        let raw = SstableMeta {
            block_metas: blocks.clone(),
            smallest_key: blocks.first().unwrap().smallest_key.clone(),
            largest_key: blocks.last().unwrap().smallest_key.clone(),
            estimated_size: 80_000,
            key_count: 240,
            meta_offset: 8_000,
            version: VERSION,
            monotonic_tombstone_events: vec![MonotonicDeleteEvent {
                event_key: PointRange {
                    left_user_key: UserKey::decode_length_prefixed(&mut &tombstone_key[..]),
                    is_exclude_left_key: true,
                },
                new_epoch: test_epoch(90),
            }],
            ..Default::default()
        };
        let expected = Sstable::new(1.into(), raw.clone(), false);
        let decoded = Sstable::from_meta_bytes(1.into(), &raw.encode_to_bytes(), false).unwrap();
        assert_eq!(decoded.meta, expected.meta);
        let wire = bincode::serialize(&SerdeSstable {
            id: 1.into(),
            meta: raw,
        })
        .unwrap();
        let restored = Box::<Sstable>::decode(&mut &wire[..]).unwrap();
        assert_eq!(restored.meta, expected.meta);
        // Restoring an empty legacy filter produces a canonical XOR16 header on encoding.
        assert_eq!(
            expected.serde_encoded_size(),
            bincode::serialize(&expected).unwrap().len()
        );
        let meta = decoded.meta;
        assert!(meta.estimated_heap_size() < old_key_bytes / 8);
        let mut buffer = Vec::new();
        for (index, original) in blocks.iter().enumerate() {
            assert_eq!(meta.block_meta(index), *original);
            let target = FullKey::decode(&original.smallest_key);
            for range in [0..80, 5..21, 15..17, 17..64, 33..78, 80..80] {
                let expected = range.start
                    + blocks[range.clone()]
                        .partition_point(|block| FullKey::decode(&block.smallest_key) <= target);
                assert_eq!(
                    meta.partition_point(range.clone(), Bound::Included(target), &mut buffer),
                    expected
                );
                for inclusive in [true, false] {
                    let before_end = |key: FullKey<&[u8]>| {
                        key.user_key < target.user_key
                            || (inclusive && key.user_key == target.user_key)
                    };
                    let expected = range.start
                        + blocks[range.clone()].partition_point(|block| {
                            before_end(FullKey::decode(&block.smallest_key))
                        });
                    assert_eq!(
                        meta.partition_point(
                            range.clone(),
                            if inclusive {
                                Bound::Included(FullKey {
                                    user_key: target.user_key,
                                    epoch_with_gap: EpochWithGap::new_min_epoch(),
                                })
                            } else {
                                Bound::Excluded(FullKey {
                                    user_key: target.user_key,
                                    epoch_with_gap: EpochWithGap::new_max_epoch(),
                                })
                            },
                            &mut buffer,
                        ),
                        expected
                    );
                }
            }
        }
        // The first group's restart belongs to another table; evaluate only the requested range.
        assert_eq!(
            meta.partition_point(
                40..79,
                Bound::Included(FullKey::decode(&blocks[39].smallest_key)),
                &mut buffer
            ),
            40
        );
    }

    #[test]
    fn test_cached_meta_bound_search_with_variable_user_keys() {
        // The final version of "a" starts its epoch with 'b', so the encoded prefix shared
        // with "ab" includes epoch bytes. Those bytes are not reusable user-key matches.
        let epochs = [u64::MAX, 0x6200_0000_0000_0001, 0x6200_0000_0000_0000];
        let users: &[&[u8]] = &[b"", b"a", b"ab", b"abc", b"b", b"x"];
        let mut blocks = Vec::new();
        for table in 1..=3 {
            for &user in users {
                for epoch in epochs.into_iter().chain(user.is_empty().then_some(0)) {
                    blocks.push(BlockMeta {
                        smallest_key: FullKey::new_with_gap_epoch(
                            TableId::new(table),
                            TableKey(user),
                            EpochWithGap::from_u64(epoch),
                        )
                        .encode(),
                        ..Default::default()
                    });
                }
            }
        }
        let meta = CachedSstableMeta::from(SstableMeta {
            block_metas: blocks.clone(),
            ..Default::default()
        });
        let mut scratch = Vec::new();
        let count = blocks.len();
        let ranges = [
            0..count,
            1..count - 1,
            15..17,
            17..count - 2,
            19..35,
            31..33,
            32..33,
            0..1,
            count..count,
        ];
        for range in &ranges {
            assert_eq!(
                meta.partition_point(range.clone(), Bound::Unbounded, &mut scratch),
                range.end
            );
            for table in 0..=4 {
                for user in [b"".as_slice(), b"a", b"aa", b"ab", b"abb", b"abcd", b"z"] {
                    for epoch in [0, 1, epochs[1], epochs[2], u64::MAX] {
                        let target = FullKey::new_with_gap_epoch(
                            TableId::new(table),
                            TableKey(user),
                            EpochWithGap::from_u64(epoch),
                        );
                        for inclusive in [true, false] {
                            let expected = range.start
                                + blocks[range.clone()].partition_point(|block| {
                                    let key = FullKey::decode(&block.smallest_key);
                                    if inclusive {
                                        key <= target
                                    } else {
                                        key < target
                                    }
                                });
                            assert_eq!(
                                meta.partition_point(
                                    range.clone(),
                                    if inclusive {
                                        Bound::Included(target)
                                    } else {
                                        Bound::Excluded(target)
                                    },
                                    &mut scratch,
                                ),
                                expected,
                                "full bound: {range:?}, {target:?}, inclusive={inclusive}"
                            );
                        }
                    }
                    let target = FullKey::new_with_gap_epoch(
                        TableId::new(table),
                        TableKey(user),
                        EpochWithGap::new_min_epoch(),
                    );
                    for inclusive in [true, false] {
                        let expected = range.start
                            + blocks[range.clone()].partition_point(|block| {
                                let key = FullKey::decode(&block.smallest_key).user_key;
                                if inclusive {
                                    key <= target.user_key
                                } else {
                                    key < target.user_key
                                }
                            });
                        assert_eq!(
                            meta.partition_point(
                                range.clone(),
                                if inclusive {
                                    Bound::Included(target)
                                } else {
                                    Bound::Excluded(FullKey {
                                        epoch_with_gap: EpochWithGap::new_max_epoch(),
                                        ..target
                                    })
                                },
                                &mut scratch,
                            ),
                            expected,
                            "user bound: {range:?}, {target:?}, inclusive={inclusive}"
                        );
                    }
                }
            }
        }
    }
}
