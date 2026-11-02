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

//! Independent-Box control from main cbb42b6338. XOR16 uses the already measured
//! bulk decode so that the benchmark isolates pooling from PR27761's getter loop.
use std::cmp::Ordering;
use std::ops::Bound;

use bytes::Buf;
use risingwave_hummock_sdk::key::{FullKey, UserKeyRangeRef};
use risingwave_storage::hummock::BlockMeta;
use xorf::{Filter, Xor8, Xor16};

pub(super) struct Blocked<F> {
    filters: Vec<F>,
}

impl<F: Filter<u64>> Blocked<F> {
    fn new(data: &[u8], metas: &[BlockMeta], decode: impl Fn(&[u8]) -> F) -> Self {
        let count = (&data[data.len() - 5..]).get_u32_le() as usize;
        assert_eq!(count, metas.len());
        let mut reader = data;
        let mut filters = Vec::with_capacity(count);
        for _ in 0..count {
            let len = reader.get_u32_le() as usize;
            filters.push(decode(&reader[..len]));
            reader.advance(len);
        }
        Self { filters }
    }

    // Preserve main's range-selection loop as the query baseline.
    fn may_match(&self, metas: &[BlockMeta], range: &UserKeyRangeRef<'_>, h: u64) -> bool {
        if self.filters.is_empty() {
            return true;
        }
        let mut index = match range.0 {
            Bound::Unbounded => 0,
            Bound::Included(left) | Bound::Excluded(left) => metas
                .partition_point(|meta| {
                    let ord = FullKey::decode(&meta.smallest_key).user_key.cmp(&left);
                    ord == Ordering::Less || ord == Ordering::Equal
                })
                .saturating_sub(1),
        };
        while index < self.filters.len() {
            let read_bound = match range.1 {
                Bound::Unbounded => false,
                Bound::Included(right) => {
                    FullKey::decode(&metas[index].smallest_key)
                        .user_key
                        .cmp(&right)
                        == Ordering::Greater
                }
                Bound::Excluded(right) => {
                    FullKey::decode(&metas[index].smallest_key)
                        .user_key
                        .cmp(&right)
                        != Ordering::Less
                }
            };
            if read_bound {
                break;
            }
            if self.filters[index].contains(&h) {
                return true;
            }
            index += 1;
        }
        false
    }
}

pub(super) enum BoxedReader {
    Eight(Blocked<Xor8>),
    Sixteen(Blocked<Xor16>),
}

impl BoxedReader {
    pub fn new(data: &[u8], metas: &[BlockMeta]) -> Self {
        if data.last() == Some(&252) {
            Self::Eight(Blocked::new(data, metas, |mut child| {
                let seed = child.get_u64_le();
                let block_length = child.get_u32_le() as usize;
                Xor8 {
                    seed,
                    block_length,
                    fingerprints: child[..child.len() - 1].into(),
                }
            }))
        } else {
            assert_eq!(data.last(), Some(&253));
            Self::Sixteen(Blocked::new(data, metas, |mut child| {
                assert_eq!(child.last(), Some(&255));
                let seed = child.get_u64_le();
                let block_length = child.get_u32_le() as usize;
                let (words, _) = child.as_chunks::<2>();
                Xor16 {
                    seed,
                    block_length,
                    fingerprints: words.iter().copied().map(u16::from_le_bytes).collect(),
                }
            }))
        }
    }

    pub fn may_match(&self, metas: &[BlockMeta], range: &UserKeyRangeRef<'_>, h: u64) -> bool {
        match self {
            Self::Eight(reader) => reader.may_match(metas, range, h),
            Self::Sixteen(reader) => reader.may_match(metas, range, h),
        }
    }
}
