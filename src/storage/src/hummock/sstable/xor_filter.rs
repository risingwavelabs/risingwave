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

use bytes::{Buf, BufMut};
use itertools::Itertools;
use xorf::{Filter, Xor16, Xor8};

use super::{FilterBuilder, Sstable};

const FOOTER_XOR8: u8 = 254;
const FOOTER_XOR16: u8 = 255;

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

    fn finish(&mut self) -> Vec<u8> {
        self.key_hash_entries.sort();
        self.key_hash_entries.dedup();
        let xor_filter = Xor16::from(&self.key_hash_entries);
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
}

impl FilterBuilder for Xor8FilterBuilder {
    fn add_key(&mut self, key: &[u8], table_id: u32) {
        self.key_hash_entries
            .push(Sstable::hash_for_bloom_filter(key, table_id));
    }

    fn finish(&mut self) -> Vec<u8> {
        self.key_hash_entries.sort();
        self.key_hash_entries.dedup();
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
}

pub enum XorFilter {
    Xor8(Xor8),
    Xor16(Xor16),
}

pub struct XorFilterReader {
    filter: XorFilter,
}

impl XorFilterReader {
    /// Creates an xor filter from a byte slice
    pub fn new(buf: Vec<u8>) -> Self {
        if buf.len() <= 1 {
            return Self {
                filter: XorFilter::Xor16(Xor16 {
                    seed: 0,
                    block_length: 0,
                    fingerprints: vec![].into_boxed_slice(),
                }),
            };
        }

        let kind = *buf.last().unwrap();
        let filter = if kind == FOOTER_XOR16 {
            let buf = &mut &buf[..];
            let xor_filter_seed = buf.get_u64_le();
            let xor_filter_block_length = buf.get_u32_le();
            // is correct even when there is an extra 0xff byte in the end of buf
            let len = buf.len() / 2;
            let xor_filter_fingerprints = (0..len)
                .map(|_| buf.get_u16_le())
                .collect_vec()
                .into_boxed_slice();
            XorFilter::Xor16(Xor16 {
                seed: xor_filter_seed,
                block_length: xor_filter_block_length as usize,
                fingerprints: xor_filter_fingerprints,
            })
        } else {
            let mbuf = &mut &buf[..];
            let xor_filter_seed = mbuf.get_u64_le();
            let xor_filter_block_length = mbuf.get_u32_le();
            // is correct even when there is an extra 0xff byte in the end of buf
            let end_pos = buf.len() - 1;
            let xor_filter_fingerprints = buf[(8 + 4)..end_pos].to_vec().into_boxed_slice();
            XorFilter::Xor8(Xor8 {
                seed: xor_filter_seed,
                block_length: xor_filter_block_length as usize,
                fingerprints: xor_filter_fingerprints,
            })
        };
        Self { filter }
    }

    pub fn estimate_size(&self) -> usize {
        match &self.filter {
            XorFilter::Xor8(filter) => filter.fingerprints.len(),
            XorFilter::Xor16(filter) => filter.fingerprints.len() * std::mem::size_of::<u16>(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.filter {
            XorFilter::Xor8(filter) => filter.block_length == 0,
            XorFilter::Xor16(filter) => filter.block_length == 0,
        }
    }

    /// Judges whether the hash value is in the table with the given false positive rate.
    ///
    /// Note:
    ///   - if the return value is false, then the table surely does not have the user key that has
    ///     the hash;
    ///   - if the return value is true, then the table may or may not have the user key that has
    ///     the hash actually, a.k.a. we don't know the answer.
    pub fn may_match(&self, h: u64) -> bool {
        if self.is_empty() {
            true
        } else {
            match &self.filter {
                XorFilter::Xor8(filter) => filter.contains(&h),
                XorFilter::Xor16(filter) => filter.contains(&h),
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
        }
    }
}
