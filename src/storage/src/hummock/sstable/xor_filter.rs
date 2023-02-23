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

use std::collections::HashSet;

use bytes::{Buf, BufMut};
use itertools::Itertools;
use xorf::{Filter, Xor16};

use super::{FilterBuilder, Sstable};

pub struct XorFilterBuilder {
    key_hash_entries: Vec<u64>,
}

impl XorFilterBuilder {
    pub fn new(capacity: usize) -> Self {
        let key_hash_entries = if capacity > 0 {
            Vec::with_capacity(capacity)
        } else {
            vec![]
        };
        Self { key_hash_entries }
    }
}

impl FilterBuilder for XorFilterBuilder {
    fn add_key(&mut self, key: &[u8], table_id: u32) {
        self.key_hash_entries
            .push(Sstable::hash_for_bloom_filter(key, table_id));
    }

    fn approximate_len(&self) -> usize {
        self.key_hash_entries.len() * 4
    }

    fn finish(&mut self) -> Vec<u8> {
        let xor_filter = Xor16::from(
            &HashSet::<u64>::from_iter(std::mem::take(&mut self.key_hash_entries).into_iter())
                .into_iter()
                .collect_vec(),
        );
        let mut buf = Vec::with_capacity(8 + 4 + xor_filter.fingerprints.len() * 2 + 1);
        buf.put_u64_le(xor_filter.seed);
        buf.put_u32_le(xor_filter.block_length as u32);
        xor_filter
            .fingerprints
            .iter()
            .for_each(|x| buf.put_u16_le(*x));
        // We add an extra byte so we can distinguish bloom filter and xor filter by the last
        // byte(255 indicates a xor filter and others indicate a bloom filter).
        buf.put_u8(255);
        buf
    }

    fn create(_fpr: f64, capacity: usize) -> Self {
        XorFilterBuilder::new(capacity)
    }
}

pub struct XorFilterReader {
    filter: Xor16,
}

impl XorFilterReader {
    /// Creates an xor filter from a byte slice
    pub fn new(buf: Vec<u8>) -> Self {
        if buf.len() <= 1 {
            return Self {
                filter: Xor16 {
                    seed: 0,
                    block_length: 0,
                    fingerprints: vec![].into_boxed_slice(),
                },
            };
        }
        let buf = &mut &buf[..];
        let xor_filter_seed = buf.get_u64_le();
        let xor_filter_block_length = buf.get_u32_le();
        // is correct even when there is an extra 0xff byte in the end of buf
        let len = buf.len() / 2;
        let xor_filter_fingerprints = (0..len)
            .map(|_| buf.get_u16_le())
            .collect_vec()
            .into_boxed_slice();
        Self {
            filter: Xor16 {
                seed: xor_filter_seed,
                block_length: xor_filter_block_length as usize,
                fingerprints: xor_filter_fingerprints,
            },
        }
    }

    pub fn estimate_size(&self) -> usize {
        self.filter.fingerprints.len() * std::mem::size_of::<u16>()
    }

    pub fn is_empty(&self) -> bool {
        self.filter.block_length == 0
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
            self.filter.contains(&h)
        }
    }
}

impl Clone for XorFilterReader {
    fn clone(&self) -> Self {
        Self {
            filter: Xor16 {
                seed: self.filter.seed,
                block_length: self.filter.block_length,
                fingerprints: self.filter.fingerprints.clone(),
            },
        }
    }
}
