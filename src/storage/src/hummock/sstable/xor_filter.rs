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
use std::ops::Bound;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use itertools::Itertools;
use risingwave_hummock_sdk::key::{FullKey, UserKeyRangeRef};
use xorf::{Filter, Xor16, Xor8};

use super::{FilterBuilder, Sstable};
use crate::hummock::{BlockMeta, MemoryLimiter};

const FOOTER_XOR8: u8 = 254;
const FOOTER_XOR16: u8 = 255;
const FOOTER_BLOCKED_XOR16: u8 = 253;
const MAX_KV_COUNT_FOR_XOR16: usize = 256 * 1024;

pub struct Xor16FilterBuilder {
    key_hash_entries: Vec<u64>,
}

pub struct Xor8FilterBuilder {
    key_hash_entries: Vec<u64>,
}

impl Xor8FilterBuilder {
    pub fn new(capacity: usize) -> Self {
        let key_hash_entries = if capacity > 0 {
            Vec::with_capacity(capacity)
        } else {
            vec![]
        };
        Self { key_hash_entries }
    }
}

impl Xor16FilterBuilder {
    pub fn new(capacity: usize) -> Self {
        let key_hash_entries = if capacity > 0 {
            Vec::with_capacity(capacity)
        } else {
            vec![]
        };
        Self { key_hash_entries }
    }
}

impl FilterBuilder for Xor16FilterBuilder {
    fn add_key(&mut self, key: &[u8], table_id: u32) {
        self.key_hash_entries
            .push(Sstable::hash_for_bloom_filter(key, table_id));
    }

    fn approximate_len(&self) -> usize {
        self.key_hash_entries.len() * 4
    }

    fn finish(&mut self, memory_limiter: Option<Arc<MemoryLimiter>>) -> Vec<u8> {
        self.key_hash_entries.sort();
        self.key_hash_entries.dedup();

        let _memory_tracker = memory_limiter.as_ref().map(|memory_limit| {
            memory_limit.must_require_memory(self.approximate_building_memory() as u64)
        });

        let xor_filter = Xor16::from(&self.key_hash_entries);
        self.key_hash_entries.clear();
        let mut buf = Vec::with_capacity(8 + 4 + xor_filter.fingerprints.len() * 2 + 1);
        buf.put_u64_le(xor_filter.seed);
        buf.put_u32_le(xor_filter.block_length as u32);
        xor_filter
            .fingerprints
            .iter()
            .for_each(|x| buf.put_u16_le(*x));
        // We add an extra byte so we can distinguish bloom filter and xor filter by the last
        // byte(255 indicates a xor16 filter, 254 indicates a xor8 filter and others indicate a
        // bloom filter).
        buf.put_u8(FOOTER_XOR16);
        buf
    }

    fn create(_fpr: f64, capacity: usize) -> Self {
        Xor16FilterBuilder::new(capacity)
    }

    fn approximate_building_memory(&self) -> usize {
        // related to https://github.com/ayazhafiz/xorf/blob/master/src/xor16.rs
        const XOR_MEMORY_PROPORTION: usize = 123;
        self.key_hash_entries.len() * XOR_MEMORY_PROPORTION
    }
}

impl FilterBuilder for Xor8FilterBuilder {
    fn add_key(&mut self, key: &[u8], table_id: u32) {
        self.key_hash_entries
            .push(Sstable::hash_for_bloom_filter(key, table_id));
    }

    fn finish(&mut self, memory_limiter: Option<Arc<MemoryLimiter>>) -> Vec<u8> {
        self.key_hash_entries.sort();
        self.key_hash_entries.dedup();

        let _memory_tracker = memory_limiter.as_ref().map(|memory_limit| {
            memory_limit.must_require_memory(self.approximate_building_memory() as u64)
        });

        let xor_filter = Xor8::from(&self.key_hash_entries);
        let mut buf = Vec::with_capacity(8 + 4 + xor_filter.fingerprints.len() + 1);
        buf.put_u64_le(xor_filter.seed);
        buf.put_u32_le(xor_filter.block_length as u32);
        buf.put_slice(xor_filter.fingerprints.as_ref());
        // Add footer to tell which kind of filter. 254 indicates a xor8 filter.
        buf.put_u8(FOOTER_XOR8);
        buf
    }

    fn approximate_len(&self) -> usize {
        self.key_hash_entries.len() * 4
    }

    fn create(_fpr: f64, capacity: usize) -> Self {
        Xor8FilterBuilder::new(capacity)
    }

    fn approximate_building_memory(&self) -> usize {
        const XOR_MEMORY_PROPORTION: usize = 123;
        self.key_hash_entries.len() * XOR_MEMORY_PROPORTION
    }
}

pub struct BlockedXor16FilterBuilder {
    current: Xor16FilterBuilder,
    data: Vec<u8>,
    block_count: usize,
}

const BLOCK_FILTER_CAPACITY: usize = 16 * 1024; // 16KB means 2K key count.

impl BlockedXor16FilterBuilder {
    pub fn is_kv_count_too_large(kv_count: usize) -> bool {
        kv_count > MAX_KV_COUNT_FOR_XOR16
    }

    pub fn new(capacity: usize) -> Self {
        Self {
            current: Xor16FilterBuilder::new(BLOCK_FILTER_CAPACITY),
            data: Vec::with_capacity(capacity),
            block_count: 0,
        }
    }
}

impl FilterBuilder for BlockedXor16FilterBuilder {
    fn add_key(&mut self, key: &[u8], table_id: u32) {
        self.current.add_key(key, table_id)
    }

    fn finish(&mut self, _memory_limiter: Option<Arc<MemoryLimiter>>) -> Vec<u8> {
        // Add footer to tell which kind of filter. 254 indicates a xor8 filter.
        self.data.put_u32_le(self.block_count as u32);
        self.data.put_u8(FOOTER_BLOCKED_XOR16);
        std::mem::take(&mut self.data)
    }

    fn approximate_len(&self) -> usize {
        self.current.approximate_len() + self.data.len()
    }

    fn create(_fpr: f64, capacity: usize) -> Self {
        BlockedXor16FilterBuilder::new(capacity)
    }

    fn switch_builder(&mut self) {
        let block = self.current.finish(None);
        self.data.put_u32_le(block.len() as u32);
        self.data.extend(block);
        self.block_count += 1;
    }

    fn approximate_building_memory(&self) -> usize {
        0
    }
}

pub struct BlockBasedXorFilter {
    filters: Vec<(Vec<u8>, Xor16)>,
}

impl Clone for BlockBasedXorFilter {
    fn clone(&self) -> Self {
        let mut filters = Vec::with_capacity(self.filters.len());
        for (key, filter) in &self.filters {
            filters.push((
                key.clone(),
                Xor16 {
                    seed: filter.seed,
                    block_length: filter.block_length,
                    fingerprints: filter.fingerprints.clone(),
                },
            ));
        }
        Self { filters }
    }
}

impl BlockBasedXorFilter {
    pub fn may_exist(&self, user_key_range: &UserKeyRangeRef<'_>, h: u64) -> bool {
        let mut block_idx = match user_key_range.0 {
            Bound::Unbounded => 0,
            Bound::Included(left) | Bound::Excluded(left) => self
                .filters
                .partition_point(|(smallest_key, _)| {
                    let ord = FullKey::decode(smallest_key).user_key.cmp(&left);
                    ord == Ordering::Less || ord == Ordering::Equal
                })
                .saturating_sub(1),
        };

        while block_idx < self.filters.len() {
            let read_bound = match user_key_range.1 {
                Bound::Unbounded => false,
                Bound::Included(right) => {
                    let ord = FullKey::decode(&self.filters[block_idx].0)
                        .user_key
                        .cmp(&right);
                    ord == Ordering::Greater
                }
                Bound::Excluded(right) => {
                    let ord = FullKey::decode(&self.filters[block_idx].0)
                        .user_key
                        .cmp(&right);
                    ord != Ordering::Less
                }
            };
            if read_bound {
                break;
            }
            if self.filters[block_idx].1.contains(&h) {
                return true;
            }
            block_idx += 1;
        }
        false
    }
}

pub enum XorFilter {
    Xor8(Xor8),
    Xor16(Xor16),
    BlockXor16(BlockBasedXorFilter),
}

pub struct XorFilterReader {
    filter: XorFilter,
}

impl XorFilterReader {
    /// Creates an xor filter from a byte slice
    pub fn new(mut data: &[u8], metas: &[BlockMeta]) -> Self {
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
        let l = data.len();
        let filter = if kind == FOOTER_BLOCKED_XOR16 {
            assert!(l >= 5);
            let reader = &mut &data[(l - 5)..];
            let block_count = reader.get_u32_le() as usize;
            assert_eq!(block_count, metas.len());
            let reader = &mut data;
            let mut filters = Vec::with_capacity(block_count);
            for meta in metas {
                let len = reader.get_u32_le() as usize;
                let xor16 = Self::to_xor16(&reader[..len]);
                reader.advance(len);
                filters.push((meta.smallest_key.clone(), xor16));
            }
            XorFilter::BlockXor16(BlockBasedXorFilter { filters })
        } else if kind == FOOTER_XOR16 {
            let xor16 = Self::to_xor16(data);
            XorFilter::Xor16(xor16)
        } else {
            let mbuf = &mut data;
            let xor_filter_seed = mbuf.get_u64_le();
            let xor_filter_block_length = mbuf.get_u32_le();
            // is correct even when there is an extra 0xff byte in the end of buf
            let end_pos = mbuf.len() - 1;
            let xor_filter_fingerprints = mbuf[..end_pos].to_vec().into_boxed_slice();
            XorFilter::Xor8(Xor8 {
                seed: xor_filter_seed,
                block_length: xor_filter_block_length as usize,
                fingerprints: xor_filter_fingerprints,
            })
        };
        Self { filter }
    }

    fn to_xor16(mut data: &[u8]) -> Xor16 {
        let kind = *data.last().unwrap();
        assert_eq!(kind, FOOTER_XOR16);
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

    pub fn estimate_size(&self) -> usize {
        match &self.filter {
            XorFilter::Xor8(filter) => filter.fingerprints.len(),
            XorFilter::Xor16(filter) => filter.fingerprints.len() * std::mem::size_of::<u16>(),
            XorFilter::BlockXor16(reader) => reader
                .filters
                .iter()
                .map(|filter| filter.1.fingerprints.len() * std::mem::size_of::<u16>())
                .sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.filter {
            XorFilter::Xor8(filter) => filter.block_length == 0,
            XorFilter::Xor16(filter) => filter.block_length == 0,
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
    pub fn may_match(&self, user_key_range: &UserKeyRangeRef<'_>, h: u64) -> bool {
        if self.is_empty() {
            true
        } else {
            match &self.filter {
                XorFilter::Xor8(filter) => filter.contains(&h),
                XorFilter::Xor16(filter) => filter.contains(&h),
                XorFilter::BlockXor16(reader) => reader.may_exist(user_key_range, h),
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
            XorFilter::BlockXor16(reader) => Self {
                filter: XorFilter::BlockXor16(reader.clone()),
            },
        }
    }
}
