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

// Keep each block's filter contiguous while copying the SST filter in chunks.
pub(super) const BLOCKED_FILTER_CHUNK_SIZE: usize = 1024 * 1024;
pub(super) const BLOCKED_FILTER_ENTRY_LEN_PREFIX: usize = size_of::<u32>();
pub(super) const BLOCKED_FILTER_FOOTER_LEN: usize = size_of::<u32>() + 1;

/// Interprets the encoded filter for one SST data block.
///
/// Query readers borrow the stored bytes and use the dependency's filter
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
struct BlockFilterLocation {
    chunk: u32,
    data: Range<u32>,
}

#[derive(Clone)]
pub(super) struct BlockBasedFilter<F> {
    filters: Vec<BlockFilterLocation>,
    chunks: Vec<Vec<u8>>,
    encoded_len: usize,
    _filter: PhantomData<F>,
}

impl<F: BlockFilter> BlockBasedFilter<F> {
    /// Reads one length-prefixed filter per data block in the order of `metas`.
    ///
    /// The layout repeats `[filter length: u32 LE][filter bytes]`, followed by
    /// `[block count: u32 LE][blocked filter tag: u8]`.
    ///
    /// Owned input is retained in chunk zero. Borrowed input is copied into
    /// chunks that keep each block's filter in a single contiguous slice.
    pub(super) fn decode(data: Cow<'_, [u8]>, metas: &[BlockMeta]) -> Self {
        assert_eq!(data.last(), Some(&F::BLOCKED_TAG));
        let footer = data.len() - BLOCKED_FILTER_FOOTER_LEN;
        let count = (&data[footer..]).get_u32_le() as usize;
        assert_eq!(count, metas.len());
        let owned = matches!(&data, Cow::Owned(_));
        let mut chunks = Vec::new();
        let mut reader = &data[..footer];
        let mut filters = Vec::with_capacity(count);
        let mut encoded_len = BLOCKED_FILTER_FOOTER_LEN;
        for _ in 0..count {
            let len = reader.get_u32_le() as usize;
            let offset = footer - reader.len();
            let (filter_bytes, remaining) = reader.split_at(len);
            reader = remaining;
            F::validate(filter_bytes);
            let (chunk, start) = if owned {
                (0, offset)
            } else {
                append_filter_bytes(&mut chunks, filter_bytes, reader.len())
            };
            let end = u32::try_from(start + filter_bytes.len())
                .expect("blocked filter data offset exceeds u32");
            let start = u32::try_from(start).expect("blocked filter data offset exceeds u32");
            encoded_len += BLOCKED_FILTER_ENTRY_LEN_PREFIX + F::serialized_len(filter_bytes);
            filters.push(BlockFilterLocation {
                chunk,
                data: start..end,
            });
        }
        assert!(reader.is_empty(), "trailing blocked filter bytes");
        if let Cow::Owned(data) = data {
            chunks = vec![data];
        }
        Self {
            filters,
            chunks,
            encoded_len,
            _filter: PhantomData,
        }
    }

    #[inline]
    fn filter_bytes(&self, location: &BlockFilterLocation) -> &[u8] {
        let chunk = &self.chunks[location.chunk as usize];
        &chunk[location.data.start as usize..location.data.end as usize]
    }

    /// Encodes the filter for one data block, as used by fast compaction.
    pub(super) fn encode_block(&self, index: usize) -> Vec<u8> {
        let data = self.filter_bytes(&self.filters[index]);
        let mut out = Vec::with_capacity(F::serialized_len(data));
        F::encode(data, &mut out);
        out
    }

    /// Writes the blocked filter layout described by `decode`.
    ///
    /// Length prefixes count the bytes written by `F::encode`, which may
    /// normalize legacy filters before writing them.
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.serialized_len());
        for location in &self.filters {
            let data = self.filter_bytes(location);
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
        self.filters.capacity() * size_of::<BlockFilterLocation>()
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

    /// Checks the supplied hash against filters for the data blocks that may
    /// overlap the key range. A positive result is a possible match.
    #[inline]
    pub(super) fn may_exist(
        &self,
        block_metas: &[BlockMeta],
        user_key_range: &UserKeyRangeRef<'_>,
        hash: u64,
    ) -> bool {
        debug_assert_eq!(self.filters.len(), block_metas.len());
        // The lower bound can fall inside the block whose start key precedes it.
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
            let location = &self.filters[block_idx];
            if F::reader(self.filter_bytes(location)).contains(&hash) {
                return true;
            }
            block_idx += 1;
        }
        false
    }
}

/// Copies one block's filter and returns its chunk index and starting offset.
#[inline(always)]
fn append_filter_bytes(
    chunks: &mut Vec<Vec<u8>>,
    filter_bytes: &[u8],
    remaining_input_bytes: usize,
) -> (u32, usize) {
    if chunks
        .last()
        .is_none_or(|chunk| chunk.capacity() - chunk.len() < filter_bytes.len())
    {
        // Cap the reservation by the input left to copy and keep large filters contiguous.
        let capacity = BLOCKED_FILTER_CHUNK_SIZE
            .min(filter_bytes.len() + remaining_input_bytes)
            .max(filter_bytes.len());
        chunks.push(Vec::with_capacity(capacity));
    }
    let index = u32::try_from(chunks.len() - 1).unwrap();
    let chunk = chunks.last_mut().unwrap();
    let start = chunk.len();
    chunk.extend_from_slice(filter_bytes);
    (index, start)
}
