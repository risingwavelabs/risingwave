// Copyright 2022 RisingWave Labs
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

//! Hummock state store's SST builder, format and iterator

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
mod block;

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::ops::{BitXor, Bound, Range};

pub use block::*;
mod block_iterator;
pub use block_iterator::*;
mod xor_filter;
use serde::{Deserialize, Serialize};
pub use xor_filter::{
    BlockedXor8FilterBuilder, BlockedXor16FilterBuilder, Xor8FilterBuilder, Xor16FilterBuilder,
    XorFilterReader,
};
pub mod builder;
pub use builder::*;
pub mod writer;
use risingwave_common::catalog::TableId;
pub use writer::*;
mod forward_sstable_iterator;
pub mod multi_builder;

use bytes::{Buf, BufMut};
pub use forward_sstable_iterator::*;
use itertools::Itertools;
use tracing::warn;
mod backward_sstable_iterator;
pub use backward_sstable_iterator::*;
use risingwave_hummock_sdk::key::{FullKey, KeyPayloadType, UserKey, UserKeyRangeRef};
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableObjectId, KeyComparator};

mod filter;
mod utils;

pub use filter::{DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP, FilterBuilder, FilterBuilderOptions};
pub use utils::{CompressionAlgorithm, xxhash64_checksum, xxhash64_verify};
use utils::{get_length_prefixed_slice, put_length_prefixed_slice};
use xxhash_rust::xxh64;

use super::{HummockError, HummockResult};
use crate::hummock::CachePolicy;
use crate::store::ReadOptions;

const MAGIC: u32 = 0x5785ab73;
const OLD_VERSION: u32 = 1;
const VERSION: u32 = 2;
pub const PARTITIONED_META_VERSION: u32 = 3;
pub const META_SHARD_POLICY_FIXED_BLOCK_COUNT: u32 = 1;

/// Assume that watermark1 is 5, watermark2 is 7, watermark3 is 11, delete ranges
/// `{ [0, wmk1) in epoch1, [wmk1, wmk2) in epoch2, [wmk2, wmk3) in epoch3 }`
/// can be transformed into events below:
/// `{ <0, +epoch1> <wmk1, -epoch1> <wmk1, +epoch2> <wmk2, -epoch2> <wmk2, +epoch3> <wmk3,
/// -epoch3> }`
/// Then we can get monotonic events (they are in order by user key) as below:
/// `{ <0, epoch1>, <wmk1, epoch2>, <wmk2, epoch3>, <wmk3, +inf> }`
/// which means that delete range of [0, wmk1) is epoch1, delete range of [wmk1, wmk2) if epoch2,
/// etc. In this example, at the event key wmk1 (5), delete range changes from epoch1 to epoch2,
/// thus the `new epoch` is epoch2. epoch2 will be used from the event key wmk1 (5) and till the
/// next event key wmk2 (7) (not inclusive).
/// If there is no range deletes between current event key and next event key, `new_epoch` will be
/// `HummockEpoch::MAX`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct MonotonicDeleteEvent {
    pub event_key:
        risingwave_hummock_sdk::key::range_delete_backward_compatibility_serde_struct::PointRange,
    pub new_epoch: HummockEpoch,
}

impl MonotonicDeleteEvent {
    pub fn encode(&self, mut buf: impl BufMut) {
        self.event_key
            .left_user_key
            .encode_length_prefixed(&mut buf);
        buf.put_u8(if self.event_key.is_exclude_left_key {
            1
        } else {
            0
        });
        buf.put_u64_le(self.new_epoch);
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        use risingwave_hummock_sdk::key::range_delete_backward_compatibility_serde_struct::*;
        let user_key = UserKey::decode_length_prefixed(buf);
        let exclude_left_key_flag = buf.get_u8();
        let is_exclude_left_key = match exclude_left_key_flag {
            0 => false,
            1 => true,
            _ => panic!("exclusive flag should be either 0 or 1"),
        };
        let new_epoch = buf.get_u64_le();
        Self {
            event_key: PointRange {
                left_user_key: user_key,
                is_exclude_left_key,
            },
            new_epoch,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SerdeSstable {
    id: HummockSstableObjectId,
    meta: SstableMeta,
}

impl From<SerdeSstable> for Sstable {
    fn from(SerdeSstable { id, meta }: SerdeSstable) -> Self {
        // Set skip_bloom_filter_in_serde to false because the behavior
        // is determined by the serializer
        Sstable::new(id, meta, false)
    }
}

/// [`Sstable`] is a handle for accessing SST.
#[derive(Clone, Deserialize)]
#[serde(from = "SerdeSstable")]
pub struct Sstable {
    pub id: HummockSstableObjectId,
    pub meta: SstableMeta,
    #[serde(skip)]
    pub filter_reader: XorFilterReader,
    /// SST serde happens when an SST meta is written to meta disk cache.
    /// Excluding the SST filter from serde can reduce the meta disk cache entry size
    /// and reduce disk IO throughput at the cost of making the SST filter useless.
    #[serde(skip)]
    skip_bloom_filter_in_serde: bool,
}

impl Serialize for Sstable {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut serde_sstable = SerdeSstable {
            id: self.id,
            meta: self.meta.clone(),
        };
        if !self.skip_bloom_filter_in_serde {
            serde_sstable.meta.bloom_filter = self.filter_reader.encode_to_bytes();
        }
        serde_sstable.serialize(serializer)
    }
}

impl Debug for Sstable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sstable")
            .field("id", &self.id)
            .field("meta", &self.meta)
            .finish()
    }
}

impl Sstable {
    pub fn new(
        id: HummockSstableObjectId,
        mut meta: SstableMeta,
        skip_bloom_filter_in_serde: bool,
    ) -> Self {
        let filter_data = std::mem::take(&mut meta.bloom_filter);
        let filter_reader = XorFilterReader::new(&filter_data, &meta.block_metas);
        Self {
            id,
            meta,
            filter_reader,
            skip_bloom_filter_in_serde,
        }
    }

    #[inline(always)]
    pub fn has_filter(&self) -> bool {
        !self.filter_reader.is_empty()
    }

    pub fn calculate_block_info(&self, block_index: usize) -> (Range<usize>, usize) {
        let block_meta = &self.meta.block_metas[block_index];
        let range =
            block_meta.offset as usize..block_meta.offset as usize + block_meta.len as usize;
        let uncompressed_capacity = block_meta.uncompressed_size as usize;
        (range, uncompressed_capacity)
    }

    #[inline(always)]
    pub fn hash_for_filter(dist_key: &[u8], table_id: u32) -> u64 {
        let dist_key_hash = xxh64::xxh64(dist_key, 0);
        // congyi adds this because he aims to dedup keys in different tables
        (table_id as u64).bitxor(dist_key_hash)
    }

    #[inline(always)]
    pub fn may_match_hash(&self, user_key_range: &UserKeyRangeRef<'_>, hash: u64) -> bool {
        self.filter_reader
            .may_match(&self.meta.block_metas, user_key_range, hash)
    }

    #[inline(always)]
    pub fn block_count(&self) -> usize {
        self.meta.block_metas.len()
    }

    #[inline(always)]
    pub fn estimated_meta_cache_memory_weight(&self) -> usize {
        // This is for foyer's in-memory cache weighter. The disk tier uses foyer `Code`
        // serialization and `estimated_size` instead.
        std::mem::size_of::<Self>()
            + self.meta.estimated_heap_size()
            + self.filter_reader.estimated_heap_size()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionedSstableMeta {
    pub id: HummockSstableObjectId,
    pub index: MetaPartitionIndex,
}

impl PartitionedSstableMeta {
    pub fn new(id: HummockSstableObjectId, index: MetaPartitionIndex) -> Self {
        Self { id, index }
    }

    #[inline(always)]
    pub fn block_count(&self) -> usize {
        self.index.block_count as usize
    }

    #[inline(always)]
    pub fn estimate_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.index.estimated_heap_size()
    }

    #[inline(always)]
    pub fn estimated_size(&self) -> u32 {
        self.index.estimated_size
    }

    #[inline(always)]
    pub fn smallest_key(&self) -> &[u8] {
        &self.index.smallest_key
    }

    #[inline(always)]
    pub fn largest_key(&self) -> &[u8] {
        &self.index.largest_key
    }
}

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockMeta {
    pub smallest_key: Vec<u8>,
    pub offset: u32,
    pub len: u32,
    pub uncompressed_size: u32,
    pub total_key_count: u32,
    pub stale_key_count: u32,
}

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetaShardDesc {
    pub shard_idx: u32,
    pub first_block_idx: u32,
    pub block_count: u32,
    pub smallest_key: Vec<u8>,
    pub offset: u64,
    pub len: u32,
    pub checksum: u64,
}

impl MetaShardDesc {
    pub fn table_id(&self) -> TableId {
        FullKey::decode(&self.smallest_key).user_key.table_id
    }

    fn encode(&self, mut buf: impl BufMut) {
        buf.put_u32_le(self.first_block_idx);
        buf.put_u32_le(self.block_count);
        put_length_prefixed_slice(&mut buf, &self.smallest_key);
        buf.put_u64_le(self.offset);
        buf.put_u32_le(self.len);
        buf.put_u64_le(self.checksum);
    }

    fn decode(shard_idx: u32, buf: &mut &[u8]) -> Self {
        let first_block_idx = buf.get_u32_le();
        let block_count = buf.get_u32_le();
        let smallest_key = get_length_prefixed_slice(buf);
        let offset = buf.get_u64_le();
        let len = buf.get_u32_le();
        let checksum = buf.get_u64_le();
        Self {
            shard_idx,
            first_block_idx,
            block_count,
            smallest_key,
            offset,
            len,
            checksum,
        }
    }

    fn encoded_size(&self) -> usize {
        4 // first_block_idx
            + 4 // block_count
            + 4 // smallest_key len
            + self.smallest_key.len()
            + 8 // offset
            + 4 // len
            + 8 // checksum
    }

    fn estimated_heap_size(&self) -> usize {
        self.smallest_key.capacity()
    }
}

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetaPartitionIndex {
    pub estimated_size: u32,
    pub key_count: u32,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub block_count: u32,
    pub shard_count: u32,
    pub filter_type: u32,
    pub shard_policy: u32,
    pub shards: Vec<MetaShardDesc>,
}

impl MetaPartitionIndex {
    fn validate(&self, encoded_len: usize) -> HummockResult<()> {
        if self.shard_count as usize != self.shards.len() {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard_count mismatch: header {} actual {}",
                self.shard_count,
                self.shards.len()
            )));
        }
        if self.block_count == 0 {
            return Err(HummockError::decode_error(
                "partitioned meta index has no data block",
            ));
        }
        if self.shards.is_empty() {
            return Err(HummockError::decode_error(
                "partitioned meta index has no shard",
            ));
        }
        if self.smallest_key.is_empty() || self.largest_key.is_empty() {
            return Err(HummockError::decode_error(
                "partitioned meta index has empty boundary key",
            ));
        }
        if KeyComparator::compare_encoded_full_key(&self.smallest_key, &self.largest_key)
            == std::cmp::Ordering::Greater
        {
            return Err(HummockError::decode_error(
                "partitioned meta index has inverted boundary keys",
            ));
        }
        if self.shards[0].smallest_key != self.smallest_key {
            return Err(HummockError::decode_error(
                "partitioned meta first shard smallest key mismatch",
            ));
        }
        if self.estimated_size as usize <= encoded_len {
            return Err(HummockError::decode_error(format!(
                "partitioned meta estimated size {} too small for index {}",
                self.estimated_size, encoded_len
            )));
        }

        let index_offset = self.estimated_size as usize - encoded_len;
        let mut expected_first_block_idx = 0u32;
        let mut expected_shard_offset = None;
        let mut last_smallest_key: Option<&[u8]> = None;
        for (shard_idx, shard) in self.shards.iter().enumerate() {
            if shard.shard_idx as usize != shard_idx {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard idx mismatch: expected {} actual {}",
                    shard_idx, shard.shard_idx
                )));
            }
            if shard.block_count == 0 {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} has no block",
                    shard_idx
                )));
            }
            if shard.first_block_idx != expected_first_block_idx {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} first block mismatch: expected {} actual {}",
                    shard_idx, expected_first_block_idx, shard.first_block_idx
                )));
            }
            if shard.smallest_key.is_empty() {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} has empty smallest key",
                    shard_idx
                )));
            }
            if let Some(last_smallest_key) = last_smallest_key
                && KeyComparator::compare_encoded_full_key(last_smallest_key, &shard.smallest_key)
                    != std::cmp::Ordering::Less
            {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} smallest key is not strictly increasing",
                    shard_idx
                )));
            }

            let shard_offset = shard.offset as usize;
            let shard_end = shard_offset
                .checked_add(shard.len as usize)
                .ok_or_else(|| {
                    HummockError::decode_error(format!(
                        "partitioned meta shard {} offset overflow",
                        shard_idx
                    ))
                })?;
            if shard_end > index_offset {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} range {}..{} crosses index offset {}",
                    shard_idx, shard_offset, shard_end, index_offset
                )));
            }
            if let Some(expected_shard_offset) = expected_shard_offset
                && shard_offset != expected_shard_offset
            {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} offset mismatch: expected {} actual {}",
                    shard_idx, expected_shard_offset, shard_offset
                )));
            }
            expected_shard_offset = Some(shard_end);
            last_smallest_key = Some(&shard.smallest_key);
            expected_first_block_idx = expected_first_block_idx
                .checked_add(shard.block_count)
                .ok_or_else(|| {
                    HummockError::decode_error("partitioned meta block count overflow")
                })?;
        }
        if expected_first_block_idx != self.block_count {
            return Err(HummockError::decode_error(format!(
                "partitioned meta block count mismatch: header {} actual {}",
                self.block_count, expected_first_block_idx
            )));
        }
        Ok(())
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let encoded_size = self.encoded_size();
        let mut buf = Vec::with_capacity(encoded_size);
        self.encode_to(&mut buf);
        buf
    }

    pub fn encode_to(&self, mut buf: impl BufMut + AsRef<[u8]>) {
        let start = buf.as_ref().len();
        self.encode_body(&mut buf);
        let end = buf.as_ref().len();

        let checksum = xxhash64_checksum(&buf.as_ref()[start..end]);
        buf.put_u64_le(checksum);
        buf.put_u32_le(PARTITIONED_META_VERSION);
        buf.put_u32_le(MAGIC);
    }

    pub fn encode_body(&self, mut buf: impl BufMut) {
        buf.put_u32_le(self.estimated_size);
        buf.put_u32_le(self.key_count);
        put_length_prefixed_slice(&mut buf, &self.smallest_key);
        put_length_prefixed_slice(&mut buf, &self.largest_key);
        buf.put_u32_le(self.block_count);
        buf.put_u32_le(self.shard_count);
        buf.put_u32_le(self.filter_type);
        buf.put_u32_le(self.shard_policy);
        assert_eq!(self.shard_count as usize, self.shards.len());
        for shard in &self.shards {
            shard.encode(&mut buf);
        }
    }

    pub fn decode(buf: &[u8]) -> HummockResult<Self> {
        let version = decode_meta_footer_version(buf)?;
        if version != PARTITIONED_META_VERSION {
            return Err(HummockError::invalid_format_version(version));
        }

        let cursor = buf.len() - 16;
        let checksum = (&buf[cursor..cursor + 8]).get_u64_le();
        let buf = &mut &buf[..cursor];
        xxhash64_verify(buf, checksum)?;

        let estimated_size = buf.get_u32_le();
        let key_count = buf.get_u32_le();
        let smallest_key = get_length_prefixed_slice(buf);
        let largest_key = get_length_prefixed_slice(buf);
        let block_count = buf.get_u32_le();
        let shard_count = buf.get_u32_le();
        let filter_type = buf.get_u32_le();
        let shard_policy = buf.get_u32_le();
        let mut shards = Vec::with_capacity(shard_count as usize);
        for shard_idx in 0..shard_count {
            shards.push(MetaShardDesc::decode(shard_idx, buf));
        }
        if !buf.is_empty() {
            return Err(HummockError::decode_error(format!(
                "partitioned meta index has {} trailing bytes",
                buf.len()
            )));
        }

        let index = Self {
            estimated_size,
            key_count,
            smallest_key,
            largest_key,
            block_count,
            shard_count,
            filter_type,
            shard_policy,
            shards,
        };
        index.validate(cursor + 16)?;
        Ok(index)
    }

    pub fn encoded_size(&self) -> usize {
        4 // estimated_size
            + 4 // key_count
            + 4 // smallest_key len
            + self.smallest_key.len()
            + 4 // largest_key len
            + self.largest_key.len()
            + 4 // block_count
            + 4 // shard_count
            + 4 // filter_type
            + 4 // shard_policy
            + self
                .shards
                .iter()
                .map(MetaShardDesc::encoded_size)
                .sum::<usize>()
            + 8 // checksum
            + 4 // version
            + 4 // magic
    }

    fn estimated_heap_size(&self) -> usize {
        self.smallest_key.capacity()
            + self.largest_key.capacity()
            + self.shards.capacity() * std::mem::size_of::<MetaShardDesc>()
            + self
                .shards
                .iter()
                .map(MetaShardDesc::estimated_heap_size)
                .sum::<usize>()
    }

    pub fn locate_shard_by_key(&self, key: FullKey<&[u8]>) -> Option<&MetaShardDesc> {
        if self.shards.is_empty() {
            return None;
        }

        let shard_idx = self
            .shards
            .partition_point(|shard| FullKey::decode(&shard.smallest_key).le(&key))
            .saturating_sub(1);
        self.shards.get(shard_idx)
    }

    pub fn filter_candidate_shards_by_user_key(&self, key: FullKey<&[u8]>) -> Vec<&MetaShardDesc> {
        if self.shards.is_empty() {
            return vec![];
        }

        let first_not_less = self
            .shards
            .partition_point(|shard| FullKey::decode(&shard.smallest_key).user_key < key.user_key);
        let mut candidates = Vec::new();

        if first_not_less > 0 {
            candidates.push(&self.shards[first_not_less - 1]);
        }

        let mut shard_idx = first_not_less;
        while shard_idx < self.shards.len() {
            let shard_user_key = FullKey::decode(&self.shards[shard_idx].smallest_key).user_key;
            if shard_user_key != key.user_key {
                break;
            }
            candidates.push(&self.shards[shard_idx]);
            shard_idx += 1;
        }

        candidates
    }

    pub fn filter_candidate_shards_by_user_key_range(
        &self,
        user_key_range: &UserKeyRangeRef<'_>,
    ) -> Vec<&MetaShardDesc> {
        if self.shards.is_empty() {
            return vec![];
        }

        let start_idx = match user_key_range.0 {
            Bound::Unbounded => 0,
            Bound::Included(left) | Bound::Excluded(left) => self
                .shards
                .partition_point(|shard| FullKey::decode(&shard.smallest_key).user_key < left)
                .saturating_sub(1),
        };
        let end_idx_exclusive = match user_key_range.1 {
            Bound::Unbounded => self.shards.len(),
            Bound::Included(right) => self
                .shards
                .partition_point(|shard| FullKey::decode(&shard.smallest_key).user_key <= right),
            Bound::Excluded(right) => self
                .shards
                .partition_point(|shard| FullKey::decode(&shard.smallest_key).user_key < right),
        };

        if end_idx_exclusive <= start_idx {
            return vec![];
        }

        self.shards[start_idx..end_idx_exclusive].iter().collect()
    }

    pub fn block_range_for_table_id_range(
        &self,
        table_id_range: (TableId, TableId),
    ) -> Option<(usize, usize)> {
        if self.shards.is_empty() {
            return None;
        }

        let start_shard_idx = self
            .shards
            .partition_point(|shard| shard.table_id() < table_id_range.0)
            .saturating_sub(1);
        let end_shard_idx_exclusive = self
            .shards
            .partition_point(|shard| shard.table_id() <= table_id_range.1);
        if end_shard_idx_exclusive <= start_shard_idx {
            return None;
        }

        let start_shard = &self.shards[start_shard_idx];
        let end_shard = &self.shards[end_shard_idx_exclusive - 1];
        Some((
            start_shard.first_block_idx as usize,
            end_shard.first_block_idx as usize + end_shard.block_count as usize - 1,
        ))
    }
}

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetaShard {
    pub shard_idx: u32,
    pub first_block_idx: u32,
    pub block_metas: Vec<BlockMeta>,
    pub filter_type: u32,
    pub filter: Vec<u8>,
}

impl MetaShard {
    pub fn locate_block_by_key(&self, key: FullKey<&[u8]>) -> usize {
        self.block_metas
            .partition_point(|block_meta| FullKey::decode(&block_meta.smallest_key).le(&key))
            .saturating_sub(1)
    }

    pub fn encode_body(&self, mut buf: impl BufMut) {
        buf.put_u32_le(self.block_metas.len() as u32);
        for block_meta in &self.block_metas {
            block_meta.encode(&mut buf);
        }
        put_length_prefixed_slice(&mut buf, &self.filter);
    }

    pub fn encode_body_to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_body_size());
        self.encode_body(&mut buf);
        buf
    }

    pub fn decode_body(
        shard_idx: u32,
        first_block_idx: u32,
        expected_block_count: u32,
        expected_smallest_key: &[u8],
        filter_type: u32,
        mut buf: &[u8],
    ) -> HummockResult<Self> {
        let block_meta_count = buf.get_u32_le() as usize;
        if block_meta_count != expected_block_count as usize {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} block count mismatch: desc {} body {}",
                shard_idx, expected_block_count, block_meta_count
            )));
        }
        let mut block_metas = Vec::with_capacity(block_meta_count);
        for _ in 0..block_meta_count {
            block_metas.push(BlockMeta::decode(&mut buf));
        }
        let filter = get_length_prefixed_slice(&mut buf);
        if !buf.is_empty() {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} has {} trailing bytes",
                shard_idx,
                buf.len()
            )));
        }
        if block_metas.is_empty() {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} has no block meta",
                shard_idx
            )));
        }
        if block_metas[0].smallest_key != expected_smallest_key {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} smallest key mismatch",
                shard_idx
            )));
        }
        for (idx, (left, right)) in block_metas.iter().tuple_windows().enumerate() {
            if KeyComparator::compare_encoded_full_key(&left.smallest_key, &right.smallest_key)
                != std::cmp::Ordering::Less
            {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} block meta {} smallest key is not strictly increasing",
                    shard_idx,
                    idx + 1
                )));
            }
        }
        Ok(Self {
            shard_idx,
            first_block_idx,
            block_metas,
            filter_type,
            filter,
        })
    }

    pub fn decode_block_metas_body(
        shard_idx: u32,
        expected_block_count: u32,
        expected_smallest_key: &[u8],
        mut buf: &[u8],
    ) -> HummockResult<Vec<BlockMeta>> {
        let block_meta_count = buf.get_u32_le() as usize;
        if block_meta_count != expected_block_count as usize {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} block count mismatch: desc {} body {}",
                shard_idx, expected_block_count, block_meta_count
            )));
        }
        let mut block_metas = Vec::with_capacity(block_meta_count);
        for _ in 0..block_meta_count {
            block_metas.push(BlockMeta::decode(&mut buf));
        }
        let filter_len = buf.get_u32_le() as usize;
        if buf.len() < filter_len {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} filter length {} exceeds remaining bytes {}",
                shard_idx,
                filter_len,
                buf.len()
            )));
        }
        buf.advance(filter_len);
        if !buf.is_empty() {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} has {} trailing bytes",
                shard_idx,
                buf.len()
            )));
        }
        if block_metas.is_empty() {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} has no block meta",
                shard_idx
            )));
        }
        if block_metas[0].smallest_key != expected_smallest_key {
            return Err(HummockError::decode_error(format!(
                "partitioned meta shard {} smallest key mismatch",
                shard_idx
            )));
        }
        for (idx, (left, right)) in block_metas.iter().tuple_windows().enumerate() {
            if KeyComparator::compare_encoded_full_key(&left.smallest_key, &right.smallest_key)
                != std::cmp::Ordering::Less
            {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} block meta {} smallest key is not strictly increasing",
                    shard_idx,
                    idx + 1
                )));
            }
        }
        Ok(block_metas)
    }

    pub fn encoded_body_size(&self) -> usize {
        4 // block meta count
            + self
                .block_metas
                .iter()
                .map(BlockMeta::encoded_size)
                .sum::<usize>()
            + 4 // filter len
            + self.filter.len()
    }

    pub fn estimated_heap_size(&self) -> usize {
        self.block_metas.capacity() * std::mem::size_of::<BlockMeta>()
            + self
                .block_metas
                .iter()
                .map(BlockMeta::estimated_heap_size)
                .sum::<usize>()
            + self.filter.capacity()
    }
}

pub fn decode_meta_footer_version(buf: &[u8]) -> HummockResult<u32> {
    if buf.len() < 16 {
        return Err(HummockError::decode_error(format!(
            "sst meta footer too short: {}",
            buf.len()
        )));
    }

    let magic = (&buf[buf.len() - 4..]).get_u32_le();
    if magic != MAGIC {
        return Err(HummockError::magic_mismatch(MAGIC, magic));
    }

    Ok((&buf[buf.len() - 8..buf.len() - 4]).get_u32_le())
}

impl BlockMeta {
    /// Format:
    ///
    /// ```plain
    /// | offset (4B) | len (4B) | uncompressed size (4B) | smallest key len (4B) | smallest key |
    /// ```
    pub fn encode(&self, mut buf: impl BufMut) {
        buf.put_u32_le(self.offset);
        buf.put_u32_le(self.len);
        buf.put_u32_le(self.uncompressed_size);
        buf.put_u32_le(self.total_key_count);
        buf.put_u32_le(self.stale_key_count);
        put_length_prefixed_slice(buf, &self.smallest_key);
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        let offset = buf.get_u32_le();
        let len = buf.get_u32_le();
        let uncompressed_size = buf.get_u32_le();

        let total_key_count = buf.get_u32_le();
        let stale_key_count = buf.get_u32_le();
        let smallest_key = get_length_prefixed_slice(buf);
        Self {
            smallest_key,
            offset,
            len,
            uncompressed_size,
            total_key_count,
            stale_key_count,
        }
    }

    pub fn decode_from_v1(buf: &mut &[u8]) -> Self {
        let offset = buf.get_u32_le();
        let len = buf.get_u32_le();
        let uncompressed_size = buf.get_u32_le();
        let total_key_count = 0;
        let stale_key_count = 0;
        let smallest_key = get_length_prefixed_slice(buf);
        Self {
            smallest_key,
            offset,
            len,
            uncompressed_size,
            total_key_count,
            stale_key_count,
        }
    }

    #[inline]
    pub fn encoded_size(&self) -> usize {
        24 /* offset + len + key len + uncompressed size + total key count + stale key count */ + self.smallest_key.len()
    }

    pub fn table_id(&self) -> TableId {
        FullKey::decode(&self.smallest_key).user_key.table_id
    }

    fn estimated_heap_size(&self) -> usize {
        self.smallest_key.capacity()
    }
}

#[derive(Default, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct SstableMeta {
    pub block_metas: Vec<BlockMeta>,
    pub bloom_filter: Vec<u8>,
    pub estimated_size: u32,
    pub key_count: u32,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub meta_offset: u64,
    /// Assume that watermark1 is 5, watermark2 is 7, watermark3 is 11, delete ranges
    /// `{ [0, wmk1) in epoch1, [wmk1, wmk2) in epoch2, [wmk2, wmk3) in epoch3 }`
    /// can be transformed into events below:
    /// `{ <0, +epoch1> <wmk1, -epoch1> <wmk1, +epoch2> <wmk2, -epoch2> <wmk2, +epoch3> <wmk3,
    /// -epoch3> }`
    /// Then we can get monotonic events (they are in order by user key) as below:
    /// `{ <0, epoch1>, <wmk1, epoch2>, <wmk2, epoch3>, <wmk3, +inf> }`
    /// which means that delete range of [0, wmk1) is epoch1, delete range of [wmk1, wmk2) if
    /// epoch2, etc. In this example, at the event key wmk1 (5), delete range changes from
    /// epoch1 to epoch2, thus the `new epoch` is epoch2. epoch2 will be used from the event
    /// key wmk1 (5) and till the next event key wmk2 (7) (not inclusive).
    /// If there is no range deletes between current event key and next event key, `new_epoch` will
    /// be `HummockEpoch::MAX`.
    #[deprecated]
    pub monotonic_tombstone_events: Vec<MonotonicDeleteEvent>,
    /// Format version, for further compatibility.
    pub version: u32,
}

impl SstableMeta {
    /// Format:
    ///
    /// ```plain
    /// | N (4B) |
    /// | block meta 0 | ... | block meta N-1 |
    /// | SST filter len (4B) | SST filter |
    /// | estimated size (4B) | key count (4B) |
    /// | smallest key len (4B) | smallest key |
    /// | largest key len (4B) | largest key |
    /// | K (4B) |
    /// | tombstone-event 0 | ... | tombstone-event K-1 |
    /// | file offset of this meta block (8B) |
    /// | checksum (8B) | version (4B) | magic (4B) |
    /// ```
    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let encoded_size = self.encoded_size();
        let mut buf = Vec::with_capacity(encoded_size);
        self.encode_to(&mut buf);
        buf
    }

    pub fn encode_to(&self, mut buf: impl BufMut + AsRef<[u8]>) {
        let start = buf.as_ref().len();

        buf.put_u32_le(
            utils::checked_into_u32(self.block_metas.len()).unwrap_or_else(|_| {
                let tmp_full_key = FullKey::decode(&self.smallest_key);
                panic!(
                    "WARN overflow can't convert block_metas_len {} into u32 table {}",
                    self.block_metas.len(),
                    tmp_full_key.user_key.table_id,
                )
            }),
        );
        for block_meta in &self.block_metas {
            block_meta.encode(&mut buf);
        }
        put_length_prefixed_slice(&mut buf, &self.bloom_filter);
        buf.put_u32_le(self.estimated_size);
        buf.put_u32_le(self.key_count);
        put_length_prefixed_slice(&mut buf, &self.smallest_key);
        put_length_prefixed_slice(&mut buf, &self.largest_key);
        #[expect(deprecated)]
        buf.put_u32_le(
            utils::checked_into_u32(self.monotonic_tombstone_events.len()).unwrap_or_else(|_| {
                let tmp_full_key = FullKey::decode(&self.smallest_key);
                panic!(
                    "WARN overflow can't convert monotonic_tombstone_events_len {} into u32 table {}",
                    self.monotonic_tombstone_events.len(),
                    tmp_full_key.user_key.table_id,
                )
            }),
        );
        #[expect(deprecated)]
        for monotonic_tombstone_event in &self.monotonic_tombstone_events {
            monotonic_tombstone_event.encode(&mut buf);
        }
        buf.put_u64_le(self.meta_offset);

        let end = buf.as_ref().len();

        let checksum = xxhash64_checksum(&buf.as_ref()[start..end]);
        buf.put_u64_le(checksum);
        buf.put_u32_le(VERSION);
        buf.put_u32_le(MAGIC);
    }

    pub fn decode(buf: &[u8]) -> HummockResult<Self> {
        let mut cursor = buf.len();

        cursor -= 4;
        let magic = (&buf[cursor..]).get_u32_le();
        if magic != MAGIC {
            return Err(HummockError::magic_mismatch(MAGIC, magic));
        }

        cursor -= 4;
        let version = (&buf[cursor..cursor + 4]).get_u32_le();
        if version != VERSION && version != OLD_VERSION {
            return Err(HummockError::invalid_format_version(version));
        }

        cursor -= 8;
        let checksum = (&buf[cursor..cursor + 8]).get_u64_le();
        let buf = &mut &buf[..cursor];
        xxhash64_verify(buf, checksum)?;

        let block_meta_count = buf.get_u32_le() as usize;
        let mut block_metas = Vec::with_capacity(block_meta_count);
        if version == OLD_VERSION {
            for _ in 0..block_meta_count {
                block_metas.push(BlockMeta::decode_from_v1(buf));
            }
        } else {
            for _ in 0..block_meta_count {
                block_metas.push(BlockMeta::decode(buf));
            }
        }

        let bloom_filter = get_length_prefixed_slice(buf);
        let estimated_size = buf.get_u32_le();
        let key_count = buf.get_u32_le();
        let smallest_key = get_length_prefixed_slice(buf);
        let largest_key = get_length_prefixed_slice(buf);
        let tomb_event_count = buf.get_u32_le() as usize;
        let mut monotonic_tombstone_events = Vec::with_capacity(tomb_event_count);
        for _ in 0..tomb_event_count {
            let monotonic_tombstone_event = MonotonicDeleteEvent::decode(buf);
            monotonic_tombstone_events.push(monotonic_tombstone_event);
        }
        let meta_offset = buf.get_u64_le();

        if !monotonic_tombstone_events.is_empty() {
            warn!(
                count = monotonic_tombstone_events.len(),
                tables = ?monotonic_tombstone_events
                    .iter()
                    .map(|event| event.event_key.left_user_key.table_id)
                    .collect::<HashSet<_>>(),
                "read non-empty range tombstones");
        }

        #[expect(deprecated)]
        Ok(Self {
            block_metas,
            bloom_filter,
            estimated_size,
            key_count,
            smallest_key,
            largest_key,
            meta_offset,
            monotonic_tombstone_events,
            version,
        })
    }

    #[inline]
    pub fn encoded_size(&self) -> usize {
        4 // block meta count
            + self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.encoded_size())
            .sum::<usize>()
            + 4 // monotonic tombstone events len
            + 4 // SST filter len
            + self.bloom_filter.len()
            + 4 // estimated size
            + 4 // key count
            + 4 // key len
            + self.smallest_key.len()
            + 4 // key len
            + self.largest_key.len()
            + 8 // footer
            + 8 // checksum
            + 4 // version
            + 4 // magic
    }

    #[expect(
        deprecated,
        reason = "monotonic_tombstone_events is deprecated but still contributes to decoded meta heap size"
    )]
    fn estimated_heap_size(&self) -> usize {
        self.block_metas.capacity() * std::mem::size_of::<BlockMeta>()
            + self
                .block_metas
                .iter()
                .map(BlockMeta::estimated_heap_size)
                .sum::<usize>()
            + self.bloom_filter.capacity()
            + self.smallest_key.capacity()
            + self.largest_key.capacity()
            + self.monotonic_tombstone_events.capacity()
                * std::mem::size_of::<MonotonicDeleteEvent>()
    }
}

#[derive(Clone, Default)]
pub struct SstableIteratorReadOptions {
    pub cache_policy: CachePolicy,
    pub must_iterated_end_user_key: Option<Bound<UserKey<KeyPayloadType>>>,
    pub max_preload_retry_times: usize,
    pub prefetch_for_large_query: bool,
}

impl SstableIteratorReadOptions {
    pub fn from_read_options(read_options: &ReadOptions) -> Self {
        Self {
            cache_policy: read_options.cache_policy,
            must_iterated_end_user_key: None,
            max_preload_retry_times: 0,
            prefetch_for_large_query: read_options.prefetch_options.for_large_query,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::HummockValue;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, iterator_test_key_of,
    };
    use crate::hummock::test_utils::gen_test_sstable_data;

    #[test]
    fn test_sstable_meta_enc_dec() {
        #[expect(deprecated)]
        let meta = SstableMeta {
            block_metas: vec![
                BlockMeta {
                    smallest_key: b"0-smallest-key".to_vec(),
                    len: 100,
                    ..Default::default()
                },
                BlockMeta {
                    smallest_key: b"5-some-key".to_vec(),
                    offset: 100,
                    len: 100,
                    ..Default::default()
                },
            ],
            bloom_filter: b"0123456789".to_vec(),
            estimated_size: 123,
            key_count: 123,
            smallest_key: b"0-smallest-key".to_vec(),
            largest_key: b"9-largest-key".to_vec(),
            meta_offset: 123,
            monotonic_tombstone_events: vec![],
            version: VERSION,
        };
        let sz = meta.encoded_size();
        let buf = meta.encode_to_bytes();
        assert_eq!(sz, buf.len());
        let decoded_meta = SstableMeta::decode(&buf[..]).unwrap();
        assert_eq!(decoded_meta, meta);

        println!("buf: {}", buf.len());
    }

    #[test]
    fn test_partitioned_meta_enc_dec() {
        let smallest_key = iterator_test_key_of(0).encode();
        let largest_key = iterator_test_key_of(9).encode();
        let shard = MetaShard {
            shard_idx: 0,
            first_block_idx: 0,
            block_metas: vec![BlockMeta {
                smallest_key: smallest_key.clone(),
                offset: 0,
                len: 100,
                uncompressed_size: 200,
                total_key_count: 10,
                stale_key_count: 1,
            }],
            filter_type: 1,
            filter: b"filter".to_vec(),
        };
        let shard_body = shard.encode_body_to_bytes();
        let decoded_shard = MetaShard::decode_body(0, 0, 1, &smallest_key, 1, &shard_body).unwrap();
        assert_eq!(decoded_shard, shard);

        let index = MetaPartitionIndex {
            estimated_size: 1234,
            key_count: 10,
            smallest_key: smallest_key.clone(),
            largest_key,
            block_count: 1,
            shard_count: 1,
            filter_type: 1,
            shard_policy: META_SHARD_POLICY_FIXED_BLOCK_COUNT,
            shards: vec![MetaShardDesc {
                shard_idx: 0,
                first_block_idx: 0,
                block_count: 1,
                smallest_key,
                offset: 100,
                len: shard_body.len() as u32,
                checksum: xxhash64_checksum(&shard_body),
            }],
        };
        let encoded_index = index.encode_to_bytes();
        assert_eq!(
            decode_meta_footer_version(&encoded_index).unwrap(),
            PARTITIONED_META_VERSION
        );
        assert_eq!(MetaPartitionIndex::decode(&encoded_index).unwrap(), index);
    }

    #[tokio::test]
    async fn test_sstable_serde() {
        let (data, meta) = gen_test_sstable_data(
            default_builder_opt_for_test(),
            (0..100).clone().map(|x| {
                (
                    iterator_test_key_of(x),
                    HummockValue::put(format!("overlapped_new_{}", x).as_bytes().to_vec()),
                )
            }),
        )
        .await;
        let index = MetaPartitionIndex::decode(&data[meta.meta_offset as usize..]).unwrap();

        // Legacy SST serde.
        let sstable = Sstable::new(42.into(), meta, true);

        let buffer = bincode::serialize(&sstable).unwrap();

        let s: Sstable = bincode::deserialize(&buffer).unwrap();

        assert_eq!(s.id, sstable.id);
        assert_eq!(s.meta, sstable.meta);
        assert!(s.filter_reader.is_empty());

        // Partitioned SST meta serde.
        let partitioned_meta = PartitionedSstableMeta::new(42.into(), index);

        let buffer = bincode::serialize(&partitioned_meta).unwrap();

        let s: PartitionedSstableMeta = bincode::deserialize(&buffer).unwrap();

        assert_eq!(s.id, partitioned_meta.id);
        assert_eq!(s.index, partitioned_meta.index);
    }
}
