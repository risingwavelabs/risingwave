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
use std::mem::size_of;
use std::ops::{Bound, Range};

use bytes::{Buf, BufMut};
use risingwave_hummock_sdk::key::{FullKey, UserKeyRangeRef};

use super::BlockMeta;

// Keep each child's query data contiguous, without copying an entire SST filter at once.
pub(super) const BLOCKED_FILTER_CHUNK_SIZE: usize = 1024 * 1024;
pub(super) const BLOCKED_FILTER_ENTRY_LEN_PREFIX: usize = size_of::<u32>();
pub(super) const BLOCKED_FILTER_FOOTER_LEN: usize = size_of::<u32>() + 1;

/// Algorithm metadata kept separately from the child filter's query bytes.
///
/// The block container owns the bytes. Implementations define their format, legacy decoding and
/// lookup algorithm. `Self` must contain only inline metadata; the container accounts for its size.
pub(super) trait BlockFilter: Clone {
    const BLOCKED_TAG: u8;

    /// Decodes metadata and returns the range of query bytes relative to this encoded child.
    fn decode(data: &[u8]) -> (Self, Range<usize>);
    fn contains(&self, data: &[u8], hash: u64) -> bool;
    /// The number of bytes appended by `encode`, including any canonicalized legacy header.
    fn serialized_len(&self, data_len: usize) -> usize;
    fn encode(&self, data: &[u8], out: &mut Vec<u8>);
}

#[derive(Clone)]
struct FilterBlock<F> {
    filter: F,
    chunk: u32,
    data: Range<u32>,
}

#[derive(Clone)]
pub(super) struct BlockBasedFilter<F> {
    filters: Vec<FilterBlock<F>>,
    chunks: Vec<Vec<u8>>,
    encoded_len: usize,
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
            let (filter, range) = F::decode(child);
            let payload = &child[range.clone()];
            let (chunk, start) = if owned {
                (0, offset + range.start)
            } else {
                if chunks
                    .last()
                    .is_none_or(|chunk| chunk.capacity() - chunk.len() < payload.len())
                {
                    let capacity = if payload.is_empty() {
                        0
                    } else {
                        payload
                            .len()
                            .max(BLOCKED_FILTER_CHUNK_SIZE.min(payload.len() + reader.len()))
                    };
                    chunks.push(Vec::with_capacity(capacity));
                }
                let index = u32::try_from(chunks.len() - 1).unwrap();
                let chunk = chunks.last_mut().unwrap();
                let start = chunk.len();
                chunk.extend_from_slice(payload);
                (index, start)
            };
            let end = u32::try_from(start + payload.len())
                .expect("blocked filter data offset exceeds u32");
            let start = u32::try_from(start).expect("blocked filter data offset exceeds u32");
            encoded_len += BLOCKED_FILTER_ENTRY_LEN_PREFIX + filter.serialized_len(payload.len());
            filters.push(FilterBlock {
                filter,
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
        }
    }

    #[inline]
    fn block_data(&self, block: &FilterBlock<F>) -> &[u8] {
        &self.chunks[block.chunk as usize][block.data.start as usize..block.data.end as usize]
    }

    pub(super) fn encode_block(&self, index: usize) -> Vec<u8> {
        let block = &self.filters[index];
        let data = self.block_data(block);
        let mut out = Vec::with_capacity(block.filter.serialized_len(data.len()));
        block.filter.encode(data, &mut out);
        out
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.serialized_len());
        for block in &self.filters {
            let data = self.block_data(block);
            out.put_u32_le(block.filter.serialized_len(data.len()) as u32);
            block.filter.encode(data, &mut out);
        }
        out.put_u32_le(self.filters.len() as u32);
        out.put_u8(F::BLOCKED_TAG);
        out
    }

    pub(super) fn serialized_len(&self) -> usize {
        self.encoded_len
    }

    pub(super) fn estimated_heap_size(&self) -> usize {
        self.filters.capacity() * size_of::<FilterBlock<F>>()
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
            if block.filter.contains(self.block_data(block), hash) {
                return true;
            }
            block_idx += 1;
        }
        false
    }
}
