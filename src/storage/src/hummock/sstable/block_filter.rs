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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Bound, Range};

use bytes::{Buf, BufMut};
use risingwave_hummock_sdk::key::{FullKey, UserKeyRangeRef};
use xorf::Filter;

use super::BlockMeta;

// Keep each child's query data contiguous, without copying an entire SST filter at once.
pub(super) const BLOCKED_FILTER_CHUNK_SIZE: usize = 1024 * 1024;
pub(super) const BLOCKED_FILTER_ENTRY_LEN_PREFIX: usize = size_of::<u32>();
pub(super) const BLOCKED_FILTER_FOOTER_LEN: usize = size_of::<u32>() + 1;

/// Interprets encoded child bytes without retaining per-child algorithm metadata.
///
/// Query readers borrow the bytes owned by the block container and use the dependency's filter
/// implementation. Encoding owns format-specific legacy normalization.
pub(super) trait BlockFilter: Clone {
    const BLOCKED_TAG: u8;

    fn validate(data: &[u8]);
    fn reader(data: &[u8]) -> impl Filter<u64> + '_;
    /// The number of bytes appended by `encode`, including any canonicalized legacy header.
    fn serialized_len(data: &[u8]) -> usize;
    fn encode(data: &[u8], out: &mut Vec<u8>);
}

#[derive(Clone)]
struct FilterBlock {
    chunk: u32,
    data: Range<u32>,
}

#[derive(Clone)]
pub(super) struct BlockBasedFilter<F> {
    filters: Vec<FilterBlock>,
    chunks: Vec<Vec<u8>>,
    encoded_len: usize,
    _filter: PhantomData<F>,
}

impl<F: BlockFilter> BlockBasedFilter<F> {
    pub(super) fn decode(mut data: Cow<'_, [u8]>, metas: &[BlockMeta]) -> Self {
        assert_eq!(data.last(), Some(&F::BLOCKED_TAG));
        if let Cow::Owned(data) = &mut data {
            // An early-sealed SST can retain the builder's oversized reservation.
            data.shrink_to_fit();
        }
        let footer = data.len() - BLOCKED_FILTER_FOOTER_LEN;
        let count = (&data[footer..]).get_u32_le() as usize;
        assert_eq!(count, metas.len());
        let owned = matches!(&data, Cow::Owned(_));
        let mut chunks = if owned {
            // Reserve chunk zero for the input buffer, moved after parsing.
            vec![Vec::new()]
        } else {
            Vec::with_capacity(data.len().div_ceil(BLOCKED_FILTER_CHUNK_SIZE))
        };
        let mut reader = &data[..footer];
        let mut filters = Vec::with_capacity(count);
        let mut encoded_len = BLOCKED_FILTER_FOOTER_LEN;
        for _ in 0..count {
            let offset = footer - reader.len() + BLOCKED_FILTER_ENTRY_LEN_PREFIX;
            let len = reader.get_u32_le() as usize;
            let child = &reader[..len];
            reader.advance(len);
            F::validate(child);
            let (chunk, start) = if owned {
                (0, offset)
            } else {
                if chunks
                    .last()
                    .is_none_or(|chunk| chunk.capacity() - chunk.len() < child.len())
                {
                    let capacity = child
                        .len()
                        .max(BLOCKED_FILTER_CHUNK_SIZE.min(child.len() + reader.len()));
                    chunks.push(Vec::with_capacity(capacity));
                }
                let index = u32::try_from(chunks.len() - 1).unwrap();
                let chunk = chunks.last_mut().unwrap();
                let start = chunk.len();
                chunk.extend_from_slice(child);
                (index, start)
            };
            let end =
                u32::try_from(start + child.len()).expect("blocked filter data offset exceeds u32");
            let start = u32::try_from(start).expect("blocked filter data offset exceeds u32");
            encoded_len += BLOCKED_FILTER_ENTRY_LEN_PREFIX + F::serialized_len(child);
            filters.push(FilterBlock {
                chunk,
                data: start..end,
            });
        }
        assert!(reader.is_empty(), "trailing blocked filter bytes");
        if let Cow::Owned(data) = data {
            chunks[0] = data;
        }
        Self {
            filters,
            chunks,
            encoded_len,
            _filter: PhantomData,
        }
    }

    #[inline]
    fn block_data(&self, block: &FilterBlock) -> &[u8] {
        &self.chunks[block.chunk as usize][block.data.start as usize..block.data.end as usize]
    }

    pub(super) fn encode_block(&self, index: usize) -> Vec<u8> {
        let block = &self.filters[index];
        let data = self.block_data(block);
        let mut out = Vec::with_capacity(F::serialized_len(data));
        F::encode(data, &mut out);
        out
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.serialized_len());
        for block in &self.filters {
            let data = self.block_data(block);
            out.put_u32_le(F::serialized_len(data) as u32);
            F::encode(data, &mut out);
        }
        out.put_u32_le(self.filters.len() as u32);
        out.put_u8(F::BLOCKED_TAG);
        out
    }

    pub(super) fn serialized_len(&self) -> usize {
        self.encoded_len
    }

    pub(super) fn estimated_heap_size(&self) -> usize {
        self.filters.capacity() * size_of::<FilterBlock>()
            + self.chunks.capacity() * size_of::<Vec<u8>>()
            + self.chunks.iter().map(Vec::capacity).sum::<usize>()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    #[cfg(test)]
    pub(super) fn chunks(&self) -> &[Vec<u8>] {
        &self.chunks
    }

    #[inline]
    pub(super) fn may_exist(
        &self,
        block_metas: &[BlockMeta],
        user_key_range: &UserKeyRangeRef<'_>,
        hash: u64,
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
            let block = &self.filters[block_idx];
            if F::reader(self.block_data(block)).contains(&hash) {
                return true;
            }
            block_idx += 1;
        }
        false
    }
}
