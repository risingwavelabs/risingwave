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

use std::cmp::Ordering;
use std::mem::size_of;
use std::ops::Bound;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use itertools::Itertools;
use risingwave_hummock_sdk::key::{FullKey, UserKeyRangeRef};
use risingwave_pb::hummock::PbSstableFilterType;
use xorf::{Filter, Xor8, Xor8Ref, Xor16, Xor16Ref};

use super::{
    DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP, FilterBuilder, FilterBuilderOptions, Sstable,
};
use crate::hummock::{BlockMeta, MemoryLimiter};

const FOOTER_XOR8: u8 = 254;
const FOOTER_XOR16: u8 = 255;
const FOOTER_BLOCKED_XOR16: u8 = 253;
const FOOTER_BLOCKED_XOR8: u8 = 252;
// Plain Xor filter bytes are encoded as seed, block length, fingerprints, and footer.
const PLAIN_XOR_FILTER_FIXED_LEN: usize =
    std::mem::size_of::<u64>() + std::mem::size_of::<u32>() + 1;
const BLOCKED_XOR_FILTER_ENTRY_LEN_PREFIX: usize = std::mem::size_of::<u32>();
const BLOCKED_XOR_FILTER_FOOTER_LEN: usize = std::mem::size_of::<u32>() + 1;
// The blocked filter payload is serialized incrementally; cap its initial reservation as well.
const MAX_BLOCKED_FILTER_DATA_PREALLOC_BYTES: usize = 4 * 1024 * 1024;

pub struct Xor16FilterBuilder {
    key_hash_entries: Vec<u64>,
}

pub struct Xor8FilterBuilder {
    key_hash_entries: Vec<u64>,
}

impl Xor8FilterBuilder {
    pub fn new(estimated_key_count: usize) -> Self {
        let capacity = filter_hash_prealloc_capacity(estimated_key_count);
        let key_hash_entries = if capacity > 0 {
            Vec::with_capacity(capacity)
        } else {
            vec![]
        };
        Self { key_hash_entries }
    }

    fn build_from_xor8(xor_filter: &Xor8) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 4 + xor_filter.fingerprints.len() + 1);
        buf.put_u64_le(xor_filter.seed);
        buf.put_u32_le(xor_filter.block_length as u32);
        buf.put_slice(xor_filter.fingerprints.as_ref());
        // Add footer to tell which kind of filter. 254 indicates a xor8 filter.
        buf.put_u8(FOOTER_XOR8);
        buf
    }
}

impl Xor16FilterBuilder {
    pub fn new(estimated_key_count: usize) -> Self {
        let capacity = filter_hash_prealloc_capacity(estimated_key_count);
        let key_hash_entries = if capacity > 0 {
            Vec::with_capacity(capacity)
        } else {
            vec![]
        };
        Self { key_hash_entries }
    }

    fn build_from_xor16(xor_filter: &Xor16) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 4 + xor_filter.fingerprints.len() * 2 + 1);
        buf.put_u64_le(xor_filter.seed);
        buf.put_u32_le(xor_filter.block_length as u32);
        xor_filter
            .fingerprints
            .iter()
            .for_each(|x| buf.put_u16_le(*x));
        // Add an extra byte so readers can distinguish tagged xor filter formats.
        buf.put_u8(FOOTER_XOR16);
        buf
    }
}

fn filter_hash_prealloc_capacity(estimated_key_count: usize) -> usize {
    filter_hash_prealloc_capacity_with_cap(
        estimated_key_count,
        DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP,
    )
}

fn filter_hash_prealloc_capacity_with_cap(
    estimated_key_count: usize,
    hash_prealloc_key_count_cap: usize,
) -> usize {
    estimated_key_count.min(hash_prealloc_key_count_cap)
}

/// Estimates the fingerprint count produced by `xorf` for a plain Xor filter.
///
/// This mirrors the capacity formula used by `xorf` when constructing `Xor8`/`Xor16`.
/// Builder-side size accounting uses it before the filter is actually built, so it is
/// approximate when later de-duplication removes repeated hashes.
fn approximate_xor_filter_fingerprint_count(key_count: usize) -> usize {
    let capacity = (1.23 * key_count as f64) as usize + 32;
    capacity / 3 * 3
}

/// Estimates the serialized byte length of a plain Xor filter before it is built.
fn approximate_plain_xor_filter_serialized_len(key_count: usize, fingerprint_size: usize) -> usize {
    let fingerprint_len = approximate_xor_filter_fingerprint_count(key_count);
    PLAIN_XOR_FILTER_FIXED_LEN + fingerprint_len * fingerprint_size
}

#[cfg(test)]
fn xor8_filter_serialized_len(filter: &Xor8) -> usize {
    PLAIN_XOR_FILTER_FIXED_LEN + filter.fingerprints.len()
}

#[cfg(test)]
fn xor16_filter_serialized_len(filter: &Xor16) -> usize {
    PLAIN_XOR_FILTER_FIXED_LEN + filter.fingerprints.len() * std::mem::size_of::<u16>()
}

/// Computes the serialized byte length of a blocked Xor filter.
///
/// `encoded_blocks_len` already contains all finished block entries, including their per-block
/// length prefixes. `pending_block_plain_filter_len` is the serialized length of the current
/// block-local plain Xor filter, if the filter builder already has pending keys for it.
/// Use `None` when there is no pending block-local filter to account for.
fn blocked_xor_filter_serialized_len(
    encoded_blocks_len: usize,
    pending_block_plain_filter_len: Option<usize>,
) -> usize {
    let pending_block_len = pending_block_plain_filter_len
        .map(|len| BLOCKED_XOR_FILTER_ENTRY_LEN_PREFIX + len)
        .unwrap_or(0);
    encoded_blocks_len + pending_block_len + BLOCKED_XOR_FILTER_FOOTER_LEN
}

impl FilterBuilder for Xor16FilterBuilder {
    fn add_key(&mut self, key: &[u8], table_id: u32) {
        self.key_hash_entries
            .push(Sstable::hash_for_filter(key, table_id));
    }

    fn approximate_len(&self) -> usize {
        approximate_plain_xor_filter_serialized_len(
            self.key_hash_entries.len(),
            std::mem::size_of::<u16>(),
        )
    }

    fn finish(&mut self, memory_limiter: Option<Arc<MemoryLimiter>>) -> Option<Vec<u8>> {
        self.key_hash_entries.sort_unstable();
        self.key_hash_entries.dedup();

        let _memory_tracker = memory_limiter.as_ref().map(|memory_limit| {
            memory_limit.must_require_memory(self.approximate_building_memory() as u64)
        });

        let xor_filter = Xor16::from(&self.key_hash_entries);
        self.key_hash_entries.clear();
        Some(Self::build_from_xor16(&xor_filter))
    }

    fn create(options: FilterBuilderOptions) -> Self {
        let capacity = filter_hash_prealloc_capacity_with_cap(
            options.estimated_key_count,
            options.hash_prealloc_key_count_cap,
        );
        Xor16FilterBuilder {
            key_hash_entries: Vec::with_capacity(capacity),
        }
    }

    fn approximate_building_memory(&self) -> usize {
        // related to https://github.com/ayazhafiz/xorf/blob/master/src/xor16.rs
        const XOR_MEMORY_PROPORTION: usize = 123;
        self.key_hash_entries.len() * XOR_MEMORY_PROPORTION
    }

    fn filter_type(&self) -> PbSstableFilterType {
        PbSstableFilterType::SstableFilterXor16
    }
}

impl FilterBuilder for Xor8FilterBuilder {
    fn add_key(&mut self, key: &[u8], table_id: u32) {
        self.key_hash_entries
            .push(Sstable::hash_for_filter(key, table_id));
    }

    fn finish(&mut self, memory_limiter: Option<Arc<MemoryLimiter>>) -> Option<Vec<u8>> {
        self.key_hash_entries.sort_unstable();
        self.key_hash_entries.dedup();

        let _memory_tracker = memory_limiter.as_ref().map(|memory_limit| {
            memory_limit.must_require_memory(self.approximate_building_memory() as u64)
        });

        let xor_filter = Xor8::from(&self.key_hash_entries);
        self.key_hash_entries.clear();
        Some(Self::build_from_xor8(&xor_filter))
    }

    fn approximate_len(&self) -> usize {
        approximate_plain_xor_filter_serialized_len(
            self.key_hash_entries.len(),
            std::mem::size_of::<u8>(),
        )
    }

    fn create(options: FilterBuilderOptions) -> Self {
        let capacity = filter_hash_prealloc_capacity_with_cap(
            options.estimated_key_count,
            options.hash_prealloc_key_count_cap,
        );
        Xor8FilterBuilder {
            key_hash_entries: Vec::with_capacity(capacity),
        }
    }

    fn approximate_building_memory(&self) -> usize {
        const XOR_MEMORY_PROPORTION: usize = 123;
        self.key_hash_entries.len() * XOR_MEMORY_PROPORTION
    }

    fn filter_type(&self) -> PbSstableFilterType {
        PbSstableFilterType::SstableFilterXor8
    }
}

pub struct BlockedXor16FilterBuilder {
    current: Xor16FilterBuilder,
    data: Vec<u8>,
    block_count: usize,
}

pub struct BlockedXor8FilterBuilder {
    current: Xor8FilterBuilder,
    data: Vec<u8>,
    block_count: usize,
}

const BLOCK_FILTER_HASH_PREALLOC_BYTES: usize = 16 * 1024;
// 16KiB means 2K key hashes.
const BLOCK_FILTER_HASH_PREALLOC_KEYS: usize = BLOCK_FILTER_HASH_PREALLOC_BYTES / size_of::<u64>();

fn blocked_filter_data_prealloc_capacity(
    options: FilterBuilderOptions,
    fingerprint_size: usize,
) -> usize {
    if options.estimated_key_count == 0 {
        return 0;
    }

    let estimated_block_count = estimated_blocked_filter_block_count(options);
    let estimated_keys_per_block = estimated_blocked_filter_keys_per_block(options);
    let estimated_block_filter_len =
        approximate_plain_xor_filter_serialized_len(estimated_keys_per_block, fingerprint_size);
    let estimated_data_len = estimated_block_count
        * (BLOCKED_XOR_FILTER_ENTRY_LEN_PREFIX + estimated_block_filter_len)
        + BLOCKED_XOR_FILTER_FOOTER_LEN;

    estimated_data_len.min(MAX_BLOCKED_FILTER_DATA_PREALLOC_BYTES)
}

fn estimated_blocked_filter_block_count(options: FilterBuilderOptions) -> usize {
    if options.estimated_block_count > 0 {
        options.estimated_block_count
    } else if options.estimated_key_count > 0 {
        options
            .estimated_key_count
            .div_ceil(BLOCK_FILTER_HASH_PREALLOC_KEYS)
            .max(1)
    } else {
        0
    }
}

fn estimated_blocked_filter_keys_per_block(options: FilterBuilderOptions) -> usize {
    let estimated_block_count = estimated_blocked_filter_block_count(options);
    if options.estimated_key_count == 0 || estimated_block_count == 0 {
        0
    } else {
        options.estimated_key_count.div_ceil(estimated_block_count)
    }
}

fn blocked_filter_current_hash_prealloc_capacity(options: FilterBuilderOptions) -> usize {
    estimated_blocked_filter_keys_per_block(options).min(BLOCK_FILTER_HASH_PREALLOC_KEYS)
}

impl BlockedXor16FilterBuilder {
    fn with_options(options: FilterBuilderOptions) -> Self {
        Self {
            current: Xor16FilterBuilder::new(blocked_filter_current_hash_prealloc_capacity(
                options,
            )),
            data: Vec::with_capacity(blocked_filter_data_prealloc_capacity(
                options,
                size_of::<u16>(),
            )),
            block_count: 0,
        }
    }
}

impl BlockedXor8FilterBuilder {
    fn with_options(options: FilterBuilderOptions) -> Self {
        Self {
            current: Xor8FilterBuilder::new(blocked_filter_current_hash_prealloc_capacity(options)),
            data: Vec::with_capacity(blocked_filter_data_prealloc_capacity(
                options,
                size_of::<u8>(),
            )),
            block_count: 0,
        }
    }
}

impl FilterBuilder for BlockedXor16FilterBuilder {
    fn add_key(&mut self, key: &[u8], table_id: u32) {
        self.current.add_key(key, table_id)
    }

    fn finish(&mut self, _memory_limiter: Option<Arc<MemoryLimiter>>) -> Option<Vec<u8>> {
        // Add footer to tell which kind of filter.
        self.data.put_u32_le(self.block_count as u32);
        self.data.put_u8(FOOTER_BLOCKED_XOR16);
        Some(std::mem::take(&mut self.data))
    }

    fn approximate_len(&self) -> usize {
        let pending_block_plain_filter_len =
            (!self.current.key_hash_entries.is_empty()).then(|| self.current.approximate_len());
        blocked_xor_filter_serialized_len(self.data.len(), pending_block_plain_filter_len)
    }

    fn create(options: FilterBuilderOptions) -> Self {
        BlockedXor16FilterBuilder::with_options(options)
    }

    fn switch_block(&mut self, memory_limiter: Option<Arc<MemoryLimiter>>) {
        let block = self
            .current
            .finish(memory_limiter)
            .expect("plain xor16 filter builder always emits filter data");
        self.data.put_u32_le(block.len() as u32);
        self.data.extend(block);
        self.block_count += 1;
    }

    fn approximate_building_memory(&self) -> usize {
        self.current.approximate_building_memory()
    }

    fn add_raw_data(&mut self, raw: Vec<u8>) {
        assert!(self.current.key_hash_entries.is_empty());
        self.data.put_u32_le(raw.len() as u32);
        self.data.extend(raw);
        self.block_count += 1;
    }

    fn support_blocked_raw_data(&self) -> bool {
        true
    }

    fn filter_type(&self) -> PbSstableFilterType {
        PbSstableFilterType::SstableFilterXor16
    }
}

impl FilterBuilder for BlockedXor8FilterBuilder {
    fn add_key(&mut self, key: &[u8], table_id: u32) {
        self.current.add_key(key, table_id)
    }

    fn finish(&mut self, _memory_limiter: Option<Arc<MemoryLimiter>>) -> Option<Vec<u8>> {
        self.data.put_u32_le(self.block_count as u32);
        self.data.put_u8(FOOTER_BLOCKED_XOR8);
        Some(std::mem::take(&mut self.data))
    }

    fn approximate_len(&self) -> usize {
        let pending_block_plain_filter_len =
            (!self.current.key_hash_entries.is_empty()).then(|| self.current.approximate_len());
        blocked_xor_filter_serialized_len(self.data.len(), pending_block_plain_filter_len)
    }

    fn create(options: FilterBuilderOptions) -> Self {
        BlockedXor8FilterBuilder::with_options(options)
    }

    fn switch_block(&mut self, memory_limiter: Option<Arc<MemoryLimiter>>) {
        let block = self
            .current
            .finish(memory_limiter)
            .expect("plain xor8 filter builder always emits filter data");
        self.data.put_u32_le(block.len() as u32);
        self.data.extend(block);
        self.block_count += 1;
    }

    fn approximate_building_memory(&self) -> usize {
        self.current.approximate_building_memory()
    }

    fn add_raw_data(&mut self, raw: Vec<u8>) {
        assert!(self.current.key_hash_entries.is_empty());
        self.data.put_u32_le(raw.len() as u32);
        self.data.extend(raw);
        self.block_count += 1;
    }

    fn support_blocked_raw_data(&self) -> bool {
        true
    }

    fn filter_type(&self) -> PbSstableFilterType {
        PbSstableFilterType::SstableFilterXor8
    }
}

// Runtime-only index into the shared fingerprint array. Offsets are not persisted.
#[derive(Clone)]
struct XorFilterBlock {
    seed: u64,
    block_length: u32,
    offset: u32,
}

#[derive(Clone)]
pub struct BlockBasedXorFilter<T> {
    filters: Vec<XorFilterBlock>,
    fingerprints: Vec<T>,
}

pub type BlockBasedXor8Filter = BlockBasedXorFilter<u8>;
pub type BlockBasedXor16Filter = BlockBasedXorFilter<u16>;

impl<T> BlockBasedXorFilter<T> {
    fn decode(
        data: &[u8],
        metas: &[BlockMeta],
        mut append: impl FnMut(&[u8], &mut Vec<T>),
    ) -> Self {
        let footer = data.len() - BLOCKED_XOR_FILTER_FOOTER_LEN;
        let count = (&data[footer..]).get_u32_le() as usize;
        assert_eq!(count, metas.len());
        let mut reader = &data[..footer];
        let mut filters = Vec::with_capacity(count);
        // A normal child has a length prefix, seed, block length and tag. Legacy
        // footer-only empty XOR16 children can underestimate this reservation.
        let payload_len = footer.saturating_sub(
            count * (BLOCKED_XOR_FILTER_ENTRY_LEN_PREFIX + PLAIN_XOR_FILTER_FIXED_LEN),
        );
        let mut fingerprints = Vec::with_capacity(payload_len / size_of::<T>());
        for _ in 0..count {
            let len = reader.get_u32_le() as usize;
            let mut child = &reader[..len];
            reader.advance(len);
            let offset = u32::try_from(fingerprints.len())
                .expect("blocked filter offset exceeds its u32-encoded byte length");
            let (seed, block_length) = if size_of::<T>() == 2 && len == 1 {
                assert_eq!(child[0], FOOTER_XOR16);
                (0, 0)
            } else {
                if size_of::<T>() == 2 {
                    assert_eq!(child.last(), Some(&FOOTER_XOR16));
                }
                let seed = child.get_u64_le();
                let block_length = child.get_u32_le();
                append(&child[..child.len() - 1], &mut fingerprints);
                (seed, block_length)
            };
            filters.push(XorFilterBlock {
                seed,
                block_length,
                offset,
            });
        }
        Self {
            filters,
            fingerprints,
        }
    }

    fn block_fingerprints(&self, index: usize) -> &[T] {
        let start = self.filters[index].offset as usize;
        let end = self
            .filters
            .get(index + 1)
            .map_or(self.fingerprints.len(), |next| next.offset as usize);
        &self.fingerprints[start..end]
    }

    fn encode_block(
        &self,
        index: usize,
        out: &mut Vec<u8>,
        tag: u8,
        write_fingerprints: impl Fn(&[T], &mut Vec<u8>),
    ) {
        let block = &self.filters[index];
        out.put_u64_le(block.seed);
        out.put_u32_le(block.block_length);
        write_fingerprints(self.block_fingerprints(index), out);
        out.put_u8(tag);
    }

    fn encode(
        &self,
        tag: u8,
        blocked_tag: u8,
        write_fingerprints: impl Fn(&[T], &mut Vec<u8>),
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.serialized_len());
        for index in 0..self.filters.len() {
            let len =
                PLAIN_XOR_FILTER_FIXED_LEN + std::mem::size_of_val(self.block_fingerprints(index));
            out.put_u32_le(len as u32);
            self.encode_block(index, &mut out, tag, &write_fingerprints);
        }
        out.put_u32_le(self.filters.len() as u32);
        out.put_u8(blocked_tag);
        out
    }

    fn serialized_len(&self) -> usize {
        BLOCKED_XOR_FILTER_FOOTER_LEN
            + self.filters.len()
                * (BLOCKED_XOR_FILTER_ENTRY_LEN_PREFIX + PLAIN_XOR_FILTER_FIXED_LEN)
            + self.fingerprints.len() * size_of::<T>()
    }

    fn estimated_heap_size(&self) -> usize {
        self.filters.capacity() * size_of::<XorFilterBlock>()
            + self.fingerprints.capacity() * size_of::<T>()
    }

    fn may_exist(
        &self,
        block_metas: &[BlockMeta],
        user_key_range: &UserKeyRangeRef<'_>,
        h: u64,
        contains: impl Fn(&XorFilterBlock, &[T], u64) -> bool,
    ) -> bool {
        debug_assert_eq!(self.filters.len(), block_metas.len());
        let mut block_idx = match user_key_range.0 {
            Bound::Unbounded => 0,
            Bound::Included(left) | Bound::Excluded(left) => block_metas
                .partition_point(|block_meta| {
                    let ord = FullKey::decode(&block_meta.smallest_key)
                        .user_key
                        .cmp(&left);
                    ord == Ordering::Less || ord == Ordering::Equal
                })
                .saturating_sub(1),
        };

        while block_idx < self.filters.len() {
            let read_bound = match user_key_range.1 {
                Bound::Unbounded => false,
                Bound::Included(right) => {
                    let ord = FullKey::decode(&block_metas[block_idx].smallest_key)
                        .user_key
                        .cmp(&right);
                    ord == Ordering::Greater
                }
                Bound::Excluded(right) => {
                    let ord = FullKey::decode(&block_metas[block_idx].smallest_key)
                        .user_key
                        .cmp(&right);
                    ord != Ordering::Less
                }
            };
            if read_bound {
                break;
            }
            if contains(
                &self.filters[block_idx],
                self.block_fingerprints(block_idx),
                h,
            ) {
                return true;
            }
            block_idx += 1;
        }
        false
    }
}

fn xor8_filter_heap_size(filter: &Xor8) -> usize {
    filter.fingerprints.len()
}

fn xor16_filter_heap_size(filter: &Xor16) -> usize {
    filter.fingerprints.len() * std::mem::size_of::<u16>()
}

pub enum XorFilter {
    Xor8(Xor8),
    Xor16(Xor16),
    BlockXor8(BlockBasedXor8Filter),
    BlockXor16(BlockBasedXor16Filter),
}

pub struct XorFilterReader {
    filter: XorFilter,
}

impl XorFilterReader {
    /// Creates an xor filter from a byte slice
    pub fn new(data: &[u8], metas: &[BlockMeta]) -> Self {
        if data.len() <= 1 {
            return Self {
                filter: XorFilter::Xor16(Xor16 {
                    seed: 0,
                    block_length: 0,
                    fingerprints: vec![].into_boxed_slice(),
                }),
            };
        }

        let kind = *data.last().unwrap();
        let filter = if kind == FOOTER_BLOCKED_XOR16 {
            XorFilter::BlockXor16(Self::to_block_xor16(data, metas))
        } else if kind == FOOTER_BLOCKED_XOR8 {
            XorFilter::BlockXor8(Self::to_block_xor8(data, metas))
        } else if kind == FOOTER_XOR16 {
            XorFilter::Xor16(Self::to_xor16(data))
        } else {
            XorFilter::Xor8(Self::to_xor8(data))
        };
        Self { filter }
    }

    fn to_xor8(mut data: &[u8]) -> Xor8 {
        let mbuf = &mut data;
        let xor_filter_seed = mbuf.get_u64_le();
        let xor_filter_block_length = mbuf.get_u32_le();
        // is correct even when there is an extra 0xff byte in the end of buf
        let end_pos = mbuf.len() - 1;
        let xor_filter_fingerprints = mbuf[..end_pos].to_vec().into_boxed_slice();
        Xor8 {
            seed: xor_filter_seed,
            block_length: xor_filter_block_length as usize,
            fingerprints: xor_filter_fingerprints,
        }
    }

    fn to_xor16(mut data: &[u8]) -> Xor16 {
        let kind = *data.last().unwrap();
        assert_eq!(kind, FOOTER_XOR16);
        if data.len() <= 1 {
            return Xor16 {
                seed: 0,
                block_length: 0,
                fingerprints: vec![].into_boxed_slice(),
            };
        }
        let buf = &mut data;
        let xor_filter_seed = buf.get_u64_le();
        let xor_filter_block_length = buf.get_u32_le();
        // is correct even when there is an extra 0xff byte in the end of buf
        let len = buf.len() / 2;
        let xor_filter_fingerprints = (0..len)
            .map(|_| buf.get_u16_le())
            .collect_vec()
            .into_boxed_slice();
        Xor16 {
            seed: xor_filter_seed,
            block_length: xor_filter_block_length as usize,
            fingerprints: xor_filter_fingerprints,
        }
    }

    fn to_block_xor16(data: &[u8], metas: &[BlockMeta]) -> BlockBasedXor16Filter {
        BlockBasedXorFilter::decode(data, metas, |bytes, fingerprints| {
            let (words, _) = bytes.as_chunks::<2>();
            fingerprints.extend(words.iter().copied().map(u16::from_le_bytes));
        })
    }

    fn to_block_xor8(data: &[u8], metas: &[BlockMeta]) -> BlockBasedXor8Filter {
        BlockBasedXorFilter::decode(data, metas, |bytes, fingerprints| {
            fingerprints.extend_from_slice(bytes);
        })
    }

    /// Returns the serialized byte length of the decoded filter reader.
    ///
    /// This intentionally follows RisingWave's filter encoding size, not the exact Rust heap
    /// footprint of the decoded reader.
    #[cfg(test)]
    fn serialized_len(&self) -> usize {
        match &self.filter {
            XorFilter::Xor8(filter) => xor8_filter_serialized_len(filter),
            XorFilter::Xor16(filter) => xor16_filter_serialized_len(filter),
            XorFilter::BlockXor8(reader) => reader.serialized_len(),
            XorFilter::BlockXor16(reader) => reader.serialized_len(),
        }
    }

    /// Estimates heap memory held by decoded filter data.
    ///
    /// Inline fields of `XorFilterReader` are counted by `Sstable::estimated_meta_cache_memory_weight`.
    pub fn estimated_heap_size(&self) -> usize {
        match &self.filter {
            XorFilter::Xor8(filter) => xor8_filter_heap_size(filter),
            XorFilter::Xor16(filter) => xor16_filter_heap_size(filter),
            XorFilter::BlockXor8(reader) => reader.estimated_heap_size(),
            XorFilter::BlockXor16(reader) => reader.estimated_heap_size(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.filter {
            XorFilter::Xor8(filter) => filter.block_length == 0,
            XorFilter::Xor16(filter) => filter.block_length == 0,
            XorFilter::BlockXor8(reader) => reader.filters.is_empty(),
            XorFilter::BlockXor16(reader) => reader.filters.is_empty(),
        }
    }

    /// Judges whether the hash value is in the table with the given false positive rate.
    ///
    /// Note:
    ///   - if the return value is false, then the table surely does not have the user key that has
    ///     the hash;
    ///   - if the return value is true, then the table may or may not have the user key that has
    ///     the hash actually, a.k.a. we don't know the answer.
    pub fn may_match(
        &self,
        block_metas: &[BlockMeta],
        user_key_range: &UserKeyRangeRef<'_>,
        h: u64,
    ) -> bool {
        if self.is_empty() {
            true
        } else {
            match &self.filter {
                XorFilter::Xor8(filter) => filter.contains(&h),
                XorFilter::Xor16(filter) => filter.contains(&h),
                XorFilter::BlockXor8(reader) => {
                    reader.may_exist(block_metas, user_key_range, h, |block, fingerprints, h| {
                        Xor8Ref {
                            seed: block.seed,
                            block_length: block.block_length as usize,
                            fingerprints,
                        }
                        .contains(&h)
                    })
                }
                XorFilter::BlockXor16(reader) => {
                    reader.may_exist(block_metas, user_key_range, h, |block, fingerprints, h| {
                        Xor16Ref {
                            seed: block.seed,
                            block_length: block.block_length as usize,
                            fingerprints,
                        }
                        .contains(&h)
                    })
                }
            }
        }
    }

    pub fn get_block_raw_filter(&self, block_index: usize) -> Vec<u8> {
        match &self.filter {
            XorFilter::BlockXor8(reader) => {
                let mut out = Vec::with_capacity(
                    PLAIN_XOR_FILTER_FIXED_LEN
                        + std::mem::size_of_val(reader.block_fingerprints(block_index)),
                );
                reader.encode_block(block_index, &mut out, FOOTER_XOR8, |fp, out| {
                    out.extend_from_slice(fp);
                });
                out
            }
            XorFilter::BlockXor16(reader) => {
                let mut out = Vec::with_capacity(
                    PLAIN_XOR_FILTER_FIXED_LEN
                        + std::mem::size_of_val(reader.block_fingerprints(block_index)),
                );
                reader.encode_block(block_index, &mut out, FOOTER_XOR16, |fp, out| {
                    fp.iter().for_each(|fp| out.put_u16_le(*fp));
                });
                out
            }
            _ => unreachable!("get_block_raw_filter requires a blocked xor filter"),
        }
    }

    pub fn is_block_based_filter(&self) -> bool {
        matches!(
            self.filter,
            XorFilter::BlockXor8(_) | XorFilter::BlockXor16(_)
        )
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        match &self.filter {
            XorFilter::Xor8(filter) => Xor8FilterBuilder::build_from_xor8(filter),
            XorFilter::Xor16(filter) => Xor16FilterBuilder::build_from_xor16(filter),
            XorFilter::BlockXor8(reader) => {
                reader.encode(FOOTER_XOR8, FOOTER_BLOCKED_XOR8, |fp, out| {
                    out.extend_from_slice(fp);
                })
            }
            XorFilter::BlockXor16(reader) => {
                reader.encode(FOOTER_XOR16, FOOTER_BLOCKED_XOR16, |fp, out| {
                    fp.iter().for_each(|fp| out.put_u16_le(*fp));
                })
            }
        }
    }
}

impl Clone for XorFilterReader {
    fn clone(&self) -> Self {
        match &self.filter {
            XorFilter::Xor8(filter) => Self {
                filter: XorFilter::Xor8(Xor8 {
                    seed: filter.seed,
                    block_length: filter.block_length,
                    fingerprints: filter.fingerprints.clone(),
                }),
            },
            XorFilter::Xor16(filter) => Self {
                filter: XorFilter::Xor16(Xor16 {
                    seed: filter.seed,
                    block_length: filter.block_length,
                    fingerprints: filter.fingerprints.clone(),
                }),
            },
            XorFilter::BlockXor8(reader) => Self {
                filter: XorFilter::BlockXor8(reader.clone()),
            },
            XorFilter::BlockXor16(reader) => Self {
                filter: XorFilter::BlockXor16(reader.clone()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use foyer::Hint;
    use rand::RngCore;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::EpochWithGap;

    use super::*;
    use crate::compaction_catalog_manager::{
        CompactionCatalogAgent, FilterKeyExtractorImpl, FullKeyFilterKeyExtractor,
    };
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::sstable::{SstableBuilder, SstableBuilderOptions};
    use crate::hummock::test_utils::{TEST_KEYS_COUNT, test_user_key_of, test_value_of};
    use crate::hummock::value::HummockValue;
    use crate::hummock::{BlockIterator, CachePolicy, SstableWriterOptions};
    use crate::monitor::StoreLocalStatistic;

    #[test]
    fn test_pooled_blocked_filter_compatibility() {
        for width in [1, 2] {
            let mut encoded = Vec::new();
            let mut children = Vec::new();
            let mut inserted = Vec::new();
            let metas: Vec<_> = (0..4)
                .map(|i| BlockMeta {
                    smallest_key: FullKey::for_test(
                        0.into(),
                        (i as u64).to_be_bytes(),
                        test_epoch(1),
                    )
                    .encode(),
                    ..Default::default()
                })
                .collect();
            for i in 0..metas.len() {
                let keys: Vec<_> = (0..i as u64 * 37)
                    .map(|k| k.wrapping_mul(0x9e37_79b9_7f4a_7c15).wrapping_add(i as u64))
                    .collect();
                let child = if width == 1 {
                    Xor8FilterBuilder::build_from_xor8(&Xor8::from(&keys))
                } else {
                    Xor16FilterBuilder::build_from_xor16(&Xor16::from(&keys))
                };
                encoded.put_u32_le(child.len() as u32);
                encoded.extend_from_slice(&child);
                children.push(child);
                inserted.push(keys);
            }
            encoded.put_u32_le(metas.len() as u32);
            encoded.put_u8(if width == 1 {
                FOOTER_BLOCKED_XOR8
            } else {
                FOOTER_BLOCKED_XOR16
            });
            let reader = {
                let original = XorFilterReader::new(&encoded, &metas);
                let cloned = original.clone();
                drop(original);
                cloned
            };
            assert_eq!(reader.encode_to_bytes(), encoded);
            for (i, child) in children.iter().enumerate() {
                assert_eq!(reader.get_block_raw_filter(i), *child);
                let oracle = XorFilterReader::new(child, &[]);
                let key = FullKey::decode(&metas[i].smallest_key).user_key;
                let range = (Bound::Included(key), Bound::Included(key));
                for hash in inserted[i]
                    .iter()
                    .copied()
                    .chain((0..512_u64).map(|k| k.wrapping_mul(0xd6e8_feb8_6659_fd93)))
                {
                    let expected =
                        oracle.may_match(&[], &(Bound::Unbounded, Bound::Unbounded), hash);
                    assert_eq!(reader.may_match(&metas, &range, hash), expected);
                }
            }
            // The old cache schema stores id and SstableMeta; it never stores
            // the runtime index. Check both reads and writes against that shape.
            for skip in [false, true] {
                let mut meta = super::super::SstableMeta {
                    block_metas: metas.clone(),
                    bloom_filter: encoded.clone(),
                    ..Default::default()
                };
                let sstable = Sstable::new(42.into(), meta.clone(), skip);
                if skip {
                    meta.bloom_filter.clear();
                }
                let old_cache_bytes = bincode::serialize(&(sstable.id, &meta)).unwrap();
                assert_eq!(bincode::serialize(&sstable).unwrap(), old_cache_bytes);
                let restored: Sstable =
                    bincode::deserialize_from(&mut &old_cache_bytes[..]).unwrap();
                if skip {
                    assert!(restored.filter_reader.is_empty());
                } else {
                    assert_eq!(restored.filter_reader.encode_to_bytes(), encoded);
                }
            }
        }
    }

    #[test]
    fn test_pooled_blocked_filter_legacy_empty() {
        for tag in [FOOTER_BLOCKED_XOR8, FOOTER_BLOCKED_XOR16] {
            let data = [0, 0, 0, 0, tag];
            let reader = XorFilterReader::new(&data, &[]);
            assert!(reader.is_empty());
            assert_eq!(reader.encode_to_bytes(), data);
        }
        // The legacy XOR16 decoder also accepts a footer-only empty child.
        let data = [1, 0, 0, 0, FOOTER_XOR16, 1, 0, 0, 0, FOOTER_BLOCKED_XOR16];
        let reader = XorFilterReader::new(&data, &[BlockMeta::default()]);
        let canonical =
            Xor16FilterBuilder::build_from_xor16(&XorFilterReader::to_xor16(&[FOOTER_XOR16]));
        assert_eq!(reader.get_block_raw_filter(0), canonical);

        let next = Xor16FilterBuilder::build_from_xor16(&Xor16::from(&vec![42]));
        let mut mixed = data[..5].to_vec();
        mixed.put_u32_le(next.len() as u32);
        mixed.extend_from_slice(&next);
        mixed.put_u32_le(2);
        mixed.put_u8(FOOTER_BLOCKED_XOR16);
        let reader = XorFilterReader::new(&mixed, &[BlockMeta::default(), BlockMeta::default()]);
        assert_eq!(reader.get_block_raw_filter(0), canonical);
        assert_eq!(reader.get_block_raw_filter(1), next);
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn test_pooled_blocked_filter_keeps_child_bounds() {
        // Three probes at block_length=1 need three fingerprints. A truncated
        // first child must not borrow the first fingerprint of the next child.
        let mut child = Vec::new();
        child.put_u64_le(0);
        child.put_u32_le(1);
        child.put_u16_le(11);
        child.put_u16_le(22);
        child.put_u8(FOOTER_XOR16);
        let mut data = Vec::new();
        data.put_u32_le(child.len() as u32);
        data.extend_from_slice(&child);
        data.put_u32_le(child.len() as u32);
        data.extend_from_slice(&child);
        data.put_u32_le(2);
        data.put_u8(FOOTER_BLOCKED_XOR16);
        let metas: Vec<_> = (0..2_u64)
            .map(|i| BlockMeta {
                smallest_key: FullKey::for_test(0.into(), i.to_be_bytes(), test_epoch(1)).encode(),
                ..Default::default()
            })
            .collect();
        let reader = XorFilterReader::new(&data, &metas);
        let key = FullKey::decode(&metas[0].smallest_key).user_key;
        reader.may_match(&metas, &(Bound::Included(key), Bound::Included(key)), 42);
    }

    #[tokio::test]
    async fn test_blocked_xor_filter() {
        let sstable_store = mock_sstable_store().await;
        let writer_opts = SstableWriterOptions {
            capacity_hint: None,
            tracker: None,
            policy: CachePolicy::Fill(Hint::Normal),
        };
        let opts = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: 0.01,
            ..Default::default()
        };
        let object_id = 1;
        let writer = sstable_store
            .clone()
            .create_sst_writer(object_id, writer_opts);

        let table_id_to_vnode = HashMap::from_iter(vec![(0.into(), VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0.into(), None)]);
        let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::new(
            FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor),
            table_id_to_vnode,
            table_id_to_watermark_serde,
            HashMap::default(),
        ));

        let mut builder = SstableBuilder::new(
            object_id,
            writer,
            BlockedXor16FilterBuilder::create(opts.filter_builder_options()),
            opts,
            compaction_catalog_agent_ref,
            None,
        );
        let mut rng = rand::rng();
        for i in 0..TEST_KEYS_COUNT {
            let epoch_count = rng.next_u64() % 20;
            for j in 0..epoch_count {
                let epoch = test_epoch(20 - j);
                let k = FullKey {
                    user_key: test_user_key_of(i),
                    epoch_with_gap: EpochWithGap::new_from_epoch(epoch),
                };
                let v = HummockValue::put(test_value_of(i));
                builder.add(k.to_ref(), v.as_slice()).await.unwrap();
            }
        }
        let ret = builder.finish().await.unwrap();
        let sst = ret.sst_info.sst_info.clone();
        ret.writer_output.await.unwrap().unwrap();
        let sstable = sstable_store
            .sstable(&sst, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        if let XorFilter::BlockXor16(_) = &sstable.filter_reader.filter {
            for idx in 0..sstable.meta.block_metas.len() {
                let resp = sstable_store
                    .get_block_response(&sstable, idx, CachePolicy::Fill(Hint::Normal))
                    .await
                    .unwrap();
                let block = resp.wait().await.unwrap();
                let mut iter = BlockIterator::new(block);
                iter.seek_to_first();
                while iter.is_valid() {
                    let k = iter.key().user_key.encode();
                    let h = Sstable::hash_for_filter(&k, iter.key().user_key.table_id.as_raw_id());
                    let user_key = iter.key().user_key;
                    let range = (Bound::Included(user_key), Bound::Included(user_key));
                    assert!(
                        sstable
                            .filter_reader
                            .may_match(&sstable.meta.block_metas, &range, h)
                    );
                    iter.next();
                }
            }
        } else {
            panic!();
        }
    }

    fn filter_builder_options(estimated_key_count: usize) -> FilterBuilderOptions {
        FilterBuilderOptions {
            estimated_key_count,
            estimated_block_count: 1,
            hash_prealloc_key_count_cap: DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP,
        }
    }

    fn assert_plain_filter_builder_approx_len_matches_output<F: FilterBuilder>(key_count: usize) {
        let mut builder = F::create(filter_builder_options(key_count));
        for i in 0..key_count {
            builder.add_key(&test_user_key_of(i).encode(), 0);
        }
        let approximate_len = builder.approximate_len();
        let filter = builder.finish(None).unwrap();
        assert_eq!(approximate_len, filter.len(), "key_count={key_count}");
    }

    #[test]
    fn test_plain_filter_builder_approx_len_matches_output() {
        for key_count in [0, 1, 2, 100, 1000] {
            assert_plain_filter_builder_approx_len_matches_output::<Xor8FilterBuilder>(key_count);
            assert_plain_filter_builder_approx_len_matches_output::<Xor16FilterBuilder>(key_count);
        }
    }

    #[test]
    fn test_filter_builder_prealloc_is_bounded_by_output_key_estimate() {
        let estimated_key_count = 128 * 1024 * 1024 / 24 + 1;
        let threshold = 512 * 1024;
        let builder = Xor16FilterBuilder::create(FilterBuilderOptions {
            estimated_key_count,
            estimated_block_count: 1,
            hash_prealloc_key_count_cap: threshold,
        });

        let old_prealloc_bytes = estimated_key_count * size_of::<u64>();
        let new_prealloc_bytes = builder.key_hash_entries.capacity() * size_of::<u64>();
        assert!(old_prealloc_bytes > 40 * 1024 * 1024);
        assert_eq!(builder.key_hash_entries.capacity(), threshold);
        assert_eq!(new_prealloc_bytes, 4 * 1024 * 1024);
    }

    #[test]
    fn test_blocked_filter_builder_prealloc_uses_filter_shape() {
        let estimated_key_count: usize = 128 * 1024 * 1024 / 24 + 1;
        let estimated_block_count: usize = 128 * 1024 * 1024 / 4096 + 1;
        let estimated_key_count_per_block = estimated_key_count.div_ceil(estimated_block_count);
        let old_data_prealloc_bytes = estimated_key_count;
        let builder = BlockedXor16FilterBuilder::create(FilterBuilderOptions {
            estimated_key_count,
            estimated_block_count,
            hash_prealloc_key_count_cap: 512 * 1024,
        });

        assert_eq!(
            builder.current.key_hash_entries.capacity(),
            estimated_key_count_per_block
        );
        assert!(builder.data.capacity() <= MAX_BLOCKED_FILTER_DATA_PREALLOC_BYTES);
        assert!(builder.data.capacity() < old_data_prealloc_bytes);
    }

    fn assert_blocked_filter_builder_approx_len_matches_output<F: FilterBuilder>() {
        let mut builder = F::create(filter_builder_options(0));
        let approximate_len = builder.approximate_len();
        assert_eq!(approximate_len, builder.finish(None).unwrap().len());

        let mut builder = F::create(filter_builder_options(1024));
        for i in 0..50 {
            builder.add_key(&test_user_key_of(i).encode(), 0);
        }
        let approximate_len = builder.approximate_len();
        builder.switch_block(None);
        assert_eq!(approximate_len, builder.finish(None).unwrap().len());

        let mut builder = F::create(filter_builder_options(1024));
        for block_idx in 0..3 {
            for i in 0..50 {
                let key_idx = block_idx * 50 + i;
                builder.add_key(&test_user_key_of(key_idx).encode(), 0);
            }
            builder.switch_block(None);
        }
        let approximate_len = builder.approximate_len();
        assert_eq!(approximate_len, builder.finish(None).unwrap().len());
    }

    #[test]
    fn test_blocked_filter_builder_approx_len_matches_output() {
        assert_blocked_filter_builder_approx_len_matches_output::<BlockedXor8FilterBuilder>();
        assert_blocked_filter_builder_approx_len_matches_output::<BlockedXor16FilterBuilder>();
    }

    // Test to make sure filter key builder finish produce the same result as the filter reader encode_to_bytes
    #[tokio::test]
    async fn test_xor_filter_builder_and_reader() {
        // Test Xor8 filter
        let mut xor8_builder = Xor8FilterBuilder::new(100);
        for i in 0..100 {
            xor8_builder.add_key(&test_user_key_of(i).encode(), 0);
        }
        let xor8_bytes = xor8_builder.finish(None).unwrap();
        let xor8_reader = XorFilterReader::new(&xor8_bytes, &[]);
        let xor8_encoded = xor8_reader.encode_to_bytes();
        assert_eq!(xor8_reader.serialized_len(), xor8_encoded.len());
        assert_eq!(
            xor8_bytes, xor8_encoded,
            "Xor8 builder and reader should produce identical bytes"
        );

        // Test Xor16 filter
        let mut xor16_builder = Xor16FilterBuilder::new(100);
        for i in 0..100 {
            xor16_builder.add_key(&test_user_key_of(i).encode(), 0);
        }
        let xor16_bytes = xor16_builder.finish(None).unwrap();
        let xor16_reader = XorFilterReader::new(&xor16_bytes, &[]);
        let xor16_encoded = xor16_reader.encode_to_bytes();
        assert_eq!(xor16_reader.serialized_len(), xor16_encoded.len());
        assert_eq!(
            xor16_bytes, xor16_encoded,
            "Xor16 builder and reader should produce identical bytes"
        );

        let mut block_metas = Vec::new();
        for block_idx in 0..3 {
            let smallest_key = FullKey {
                user_key: test_user_key_of(block_idx * 50),
                epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
            };

            block_metas.push(BlockMeta {
                smallest_key: smallest_key.encode(),
                len: 0,
                offset: 0,
                uncompressed_size: 0,
                total_key_count: 50,
                stale_key_count: 0,
            });
        }

        let blocked_options = FilterBuilderOptions {
            estimated_key_count: 150,
            estimated_block_count: block_metas.len(),
            hash_prealloc_key_count_cap: DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP,
        };

        // Test BlockedXor8 filter
        let mut blocked_xor8_builder = BlockedXor8FilterBuilder::create(blocked_options);
        for block_idx in 0..3 {
            for i in 0..50 {
                let key_idx = block_idx * 50 + i;
                blocked_xor8_builder.add_key(&test_user_key_of(key_idx).encode(), 0);
            }
            blocked_xor8_builder.switch_block(None);
        }
        let blocked_xor8_bytes = blocked_xor8_builder.finish(None).unwrap();
        let blocked_xor8_reader = XorFilterReader::new(&blocked_xor8_bytes, &block_metas);
        let blocked_xor8_encoded = blocked_xor8_reader.encode_to_bytes();
        assert_eq!(
            blocked_xor8_reader.serialized_len(),
            blocked_xor8_encoded.len()
        );
        assert_eq!(
            blocked_xor8_bytes, blocked_xor8_encoded,
            "BlockedXor8 builder and reader should produce identical bytes"
        );

        // Test BlockedXor16 filter
        let mut blocked_xor16_builder = BlockedXor16FilterBuilder::create(blocked_options);
        for block_idx in 0..3 {
            for i in 0..50 {
                let key_idx = block_idx * 50 + i;
                blocked_xor16_builder.add_key(&test_user_key_of(key_idx).encode(), 0);
            }
            blocked_xor16_builder.switch_block(None);
        }
        let blocked_xor16_bytes = blocked_xor16_builder.finish(None).unwrap();
        let blocked_xor16_reader = XorFilterReader::new(&blocked_xor16_bytes, &block_metas);
        let blocked_xor16_encoded = blocked_xor16_reader.encode_to_bytes();
        assert_eq!(
            blocked_xor16_reader.serialized_len(),
            blocked_xor16_encoded.len()
        );
        assert_eq!(
            blocked_xor16_bytes, blocked_xor16_encoded,
            "BlockedXor16 builder and reader should produce identical bytes"
        );
    }
}
