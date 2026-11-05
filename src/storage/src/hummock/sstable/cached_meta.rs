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

use std::cmp::Ordering;
use std::fmt::Formatter;
use std::ops::{Bound, Range};

use bytes::Buf;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{EPOCH_LEN, FullKey, TABLE_PREFIX_LEN, TableKey, UserKey};
use risingwave_hummock_sdk::{EpochWithGap, HummockSstableObjectId};
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{
    BlockMeta, BlockMetaRef, MonotonicDeleteEvent, NativeMetaTail, Sstable, SstableMeta,
    XorFilterReader, verified_meta_body,
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
            // Two u32 headers and an uncompressed epoch are present in every entry.
            data: Vec::with_capacity(block_count * (8 + EPOCH_LEN)),
            restarts: Vec::with_capacity(block_count.div_ceil(KEY_RESTART_INTERVAL)),
        }
    }

    // Callers own the previous full key so native decoding can borrow it from the input.
    fn push(&mut self, block: CachedBlockMeta, key: &[u8], previous: &[u8]) {
        let table_key = FullKey::decode(key).user_key.table_key.0;
        let shared = if self.block_metas.len().is_multiple_of(KEY_RESTART_INTERVAL) {
            self.restarts.push(self.data.len());
            0
        } else {
            common_prefix(FullKey::decode(previous).user_key.table_key.0, table_key)
        };
        let suffix = &table_key[shared..];
        self.data.extend_from_slice(&(shared as u32).to_le_bytes());
        self.data
            .extend_from_slice(&(suffix.len() as u32).to_le_bytes());
        self.data.extend_from_slice(suffix);
        self.data.extend_from_slice(&key[key.len() - EPOCH_LEN..]);
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

/// Cache-only K16 encoding: shared TableKey length, suffix length, suffix, and an
/// uncompressed epoch. Table IDs live in CachedBlockMeta; neither can enter a shared prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
struct PrefixKeys {
    data: Box<[u8]>,
    restarts: Box<[usize]>,
}

impl PrefixKeys {
    fn entries(&self, offset: usize) -> impl Iterator<Item = KeyEntry<'_>> {
        let mut data = &self.data[offset..];
        std::iter::from_fn(move || (!data.is_empty()).then(|| KeyEntry::decode(&mut data)))
    }

    fn restart(&self, group: usize) -> KeyEntry<'_> {
        self.entries(self.restarts[group]).next().unwrap()
    }
}

/// One borrowed entry of the in-memory prefix encoding.
struct KeyEntry<'a> {
    shared: usize,
    suffix: &'a [u8],
    epoch: &'a [u8; EPOCH_LEN],
}

impl<'a> KeyEntry<'a> {
    fn decode(data: &mut &'a [u8]) -> Self {
        let shared = data.get_u32_le() as usize;
        let len = data.get_u32_le() as usize;
        let (suffix, rest) = data.split_at(len);
        let (epoch, rest) = rest.split_at(EPOCH_LEN);
        *data = rest;
        Self {
            shared,
            suffix,
            epoch: epoch.try_into().unwrap(),
        }
    }

    fn epoch(&self) -> EpochWithGap {
        EpochWithGap::from_u64(u64::from_be_bytes(*self.epoch))
    }

    /// Apply a TableKey delta to a full-key buffer. This also replaces its table ID and epoch.
    fn restore(&self, table_id: TableId, key: &mut Vec<u8>) {
        key.resize(TABLE_PREFIX_LEN + self.shared, 0);
        key[..TABLE_PREFIX_LEN].copy_from_slice(&table_id.as_raw_id().to_be_bytes());
        key.extend_from_slice(self.suffix);
        key.extend_from_slice(self.epoch);
    }

    fn restart_key(&self, table_id: TableId) -> FullKey<&'a [u8]> {
        debug_assert_eq!(self.shared, 0);
        FullKey {
            user_key: UserKey::new(table_id, TableKey(self.suffix)),
            epoch_with_gap: self.epoch(),
        }
    }
}

/// Compares successive TableKey deltas against one fixed target, without restoring keys.
struct TableKeyComparator<'a> {
    target: &'a [u8],
    matched: usize,
    order: Ordering,
}

impl<'a> TableKeyComparator<'a> {
    fn new(target: &'a [u8]) -> Self {
        Self {
            target,
            matched: 0,
            order: Ordering::Equal,
        }
    }

    fn compare(&mut self, entry: &KeyEntry<'_>) -> Ordering {
        if entry.shared <= self.matched {
            let target = &self.target[entry.shared..];
            let matched = common_prefix(entry.suffix, target);
            self.matched = entry.shared + matched;
            self.order = entry.suffix.get(matched).cmp(&target.get(matched));
        }
        // Otherwise the shared prefix retains the previous first difference, including
        // a target that ended there. A previous key that ended there cannot share more bytes.
        self.order
    }
}

impl CachedSstableMeta {
    /// Restores one key into reader-owned scratch, without retaining decoded keys in the cache.
    pub fn block_smallest_key<'a>(&self, index: usize, key: &'a mut Vec<u8>) -> &'a [u8] {
        assert!(index < self.block_metas.len());
        let group = index / KEY_RESTART_INTERVAL;
        let first = group * KEY_RESTART_INTERVAL;
        for entry in self
            .keys
            .entries(self.keys.restarts[group])
            .take(index - first + 1)
        {
            entry.restore(self.block_metas[index].table_id, key);
        }
        key
    }

    /// Compares a block boundary without allocating or restoring its full key.
    pub(super) fn compare_block_key(&self, index: usize, target: FullKey<&[u8]>) -> Ordering {
        let table_order = self.block_metas[index]
            .table_id
            .cmp(&target.user_key.table_id);
        if !table_order.is_eq() {
            return table_order;
        }
        let group = index / KEY_RESTART_INTERVAL;
        let first = group * KEY_RESTART_INTERVAL;
        let mut comparator = TableKeyComparator::new(target.user_key.table_key.0);
        let mut order = Ordering::Equal;
        for entry in self
            .keys
            .entries(self.keys.restarts[group])
            .take(index - first + 1)
        {
            order = comparator
                .compare(&entry)
                .then_with(|| target.epoch_with_gap.cmp(&entry.epoch()));
        }
        order
    }

    /// Returns the absolute index after the keys within `bound` in the requested range.
    pub fn partition_point(&self, range: Range<usize>, bound: Bound<FullKey<&[u8]>>) -> usize {
        assert!(range.start <= range.end && range.end <= self.block_metas.len());
        if range.is_empty() {
            return range.start;
        }
        let (target, inclusive) = match bound {
            Bound::Included(target) => (target, true),
            Bound::Excluded(target) => (target, false),
            Bound::Unbounded => return range.end,
        };
        let before_bound = |order: Ordering| order.is_lt() || (inclusive && order.is_eq());
        let first_group = range.start / KEY_RESTART_INTERVAL;
        let end_group = (range.end - 1) / KEY_RESTART_INTERVAL + 1;
        if !before_bound(self.compare_block_key(range.start, target)) {
            return range.start;
        }
        let mut left = first_group + 1;
        let mut right = end_group;
        while left < right {
            let group = left + (right - left) / 2;
            let first = group * KEY_RESTART_INTERVAL;
            let restart = self
                .keys
                .restart(group)
                .restart_key(self.block_metas[first].table_id);
            if before_bound(restart.cmp(&target)) {
                left = group + 1;
            } else {
                right = group;
            }
        }
        let group = left - 1;
        let first = group * KEY_RESTART_INTERVAL;
        let end = range.end.min(first + KEY_RESTART_INTERVAL);
        let known_true = first.max(range.start);
        if known_true + 1 == end {
            return end;
        }
        let mut comparator = TableKeyComparator::new(target.user_key.table_key.0);
        for (index, entry) in (first..end).zip(self.keys.entries(self.keys.restarts[group])) {
            // Always advance TableKey comparison, including entries before range.start and
            // entries from another table. Prefixes contain only TableKey bytes.
            let user_order = comparator.compare(&entry);
            if index <= known_true {
                continue;
            }
            let order = self.block_metas[index]
                .table_id
                .cmp(&target.user_key.table_id)
                .then(user_order)
                .then_with(|| target.epoch_with_gap.cmp(&entry.epoch()));
            if !before_bound(order) {
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
        let blocks_size = 8 + self
            .keys
            .entries(0)
            .map(|entry| {
                8 + TABLE_PREFIX_LEN + entry.shared + entry.suffix.len() + EPOCH_LEN + 5 * 4
            })
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
        let mut seq = serializer.serialize_seq(Some(self.0.block_metas.len()))?;
        let mut raw = BlockMeta::default();
        for (block, entry) in self.0.block_metas.iter().zip(self.0.keys.entries(0)) {
            entry.restore(block.table_id, &mut raw.smallest_key);
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
        for (index, original) in blocks.iter().enumerate() {
            assert_eq!(meta.block_meta(index), *original);
            let target = FullKey::decode(&original.smallest_key);
            for range in [0..80, 5..21, 15..17, 17..64, 33..78, 80..80] {
                let expected = range.start
                    + blocks[range.clone()]
                        .partition_point(|block| FullKey::decode(&block.smallest_key) <= target);
                assert_eq!(
                    meta.partition_point(range.clone(), Bound::Included(target)),
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
        // Growing prefix keys force a fresh suffix comparison after an equal/shorter key,
        // across complete restart groups. Epoch bytes must never enter the shared prefix.
        for len in 1..=33 {
            blocks.push(BlockMeta {
                smallest_key: FullKey::new_with_gap_epoch(
                    TableId::new(4),
                    TableKey(vec![b'a'; len]),
                    EpochWithGap::from_u64(0x6161_6161_6161_6161),
                )
                .encode(),
                ..Default::default()
            });
        }
        let meta = CachedSstableMeta::from(SstableMeta {
            block_metas: blocks.clone(),
            ..Default::default()
        });
        let count = blocks.len();
        for (index, block) in blocks.iter().enumerate() {
            let target = FullKey::decode(&block.smallest_key);
            for (other_index, other) in blocks.iter().enumerate() {
                assert_eq!(
                    meta.compare_block_key(other_index, target),
                    FullKey::decode(&other.smallest_key).cmp(&target),
                    "boundary {other_index}, target {index}"
                );
            }
            assert_eq!(
                meta.partition_point(0..count, Bound::Included(target)),
                index + 1
            );
            assert_eq!(
                meta.partition_point(0..count, Bound::Excluded(target)),
                index
            );
        }
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
                meta.partition_point(range.clone(), Bound::Unbounded),
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
