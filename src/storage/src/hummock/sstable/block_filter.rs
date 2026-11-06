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
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Bound, Range};

use bytes::{Buf, BufMut};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::key::{FullKey, UserKeyRangeRef};
use xorf::Filter;

use super::BlockMeta;

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
pub(super) struct BlockBasedFilter<F> {
    block_ranges: Vec<Range<u32>>,
    bytes: Vec<u8>,
    encoded_len: usize,
    _filter: PhantomData<F>,
}

impl<F: BlockFilter> BlockBasedFilter<F> {
    /// Reads one length-prefixed filter per data block in the order of `metas`.
    ///
    /// The layout repeats `[filter length: u32 LE][filter bytes]`, followed by
    /// `[block count: u32 LE][blocked filter tag: u8]`.
    ///
    /// Owns only the filter bytes: the object-store path copies the borrowed
    /// filter slice, while Foyer and the builder transfer their existing buffer.
    pub(super) fn decode(data: Cow<'_, [u8]>, metas: &[BlockMeta]) -> Self {
        assert_eq!(data.last(), Some(&F::BLOCKED_TAG));
        let footer_start = data.len() - BLOCKED_FILTER_FOOTER_LEN;
        let block_count = (&data[footer_start..]).get_u32_le() as usize;
        assert_eq!(block_count, metas.len());

        let bytes = data.into_owned();
        let mut entries = &bytes[..footer_start];
        let mut block_ranges = Vec::with_capacity(block_count);
        let mut encoded_len = BLOCKED_FILTER_FOOTER_LEN;
        for _ in 0..block_count {
            let len = entries.get_u32_le();
            let (filter_bytes, remaining) = entries.split_at(len as usize);
            F::validate(filter_bytes);

            let end = u32::try_from(footer_start - remaining.len())
                .expect("blocked filter data offset exceeds u32");
            block_ranges.push(end - len..end);
            encoded_len += BLOCKED_FILTER_ENTRY_LEN_PREFIX + F::serialized_len(filter_bytes);
            entries = remaining;
        }
        assert!(entries.is_empty(), "trailing blocked filter bytes");
        Self {
            block_ranges,
            bytes,
            encoded_len,
            _filter: PhantomData,
        }
    }

    #[inline]
    fn filter_bytes(&self, range: &Range<u32>) -> &[u8] {
        &self.bytes[range.start as usize..range.end as usize]
    }

    /// Encodes the filter for one data block, as used by fast compaction.
    pub(super) fn encode_block(&self, index: usize) -> Vec<u8> {
        let data = self.filter_bytes(&self.block_ranges[index]);
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
        for range in &self.block_ranges {
            let data = self.filter_bytes(range);
            out.put_u32_le(F::serialized_len(data) as u32);
            F::encode(data, &mut out);
        }
        out.put_u32_le(self.block_ranges.len() as u32);
        out.put_u8(F::BLOCKED_TAG);
        out
    }

    pub(super) fn serialized_len(&self) -> usize {
        self.encoded_len
    }

    pub(super) fn estimated_heap_size(&self) -> usize {
        self.block_ranges.capacity() * size_of::<Range<u32>>() + self.bytes.capacity()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.block_ranges.is_empty()
    }

    #[cfg(test)]
    pub(super) fn backing_buffer(&self) -> &Vec<u8> {
        &self.bytes
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
        // The lower bound can fall inside the block whose start key precedes it.
        let first_block = match user_key_range.0 {
            Bound::Unbounded => 0,
            Bound::Included(left) | Bound::Excluded(left) => block_metas
                .partition_point(|meta| FullKey::decode(&meta.smallest_key).user_key <= left)
                .saturating_sub(1),
        };

        for (meta, range) in block_metas
            .iter()
            .zip_eq_fast(&self.block_ranges)
            .skip(first_block)
        {
            let past_end = match user_key_range.1 {
                Bound::Unbounded => false,
                Bound::Included(right) => FullKey::decode(&meta.smallest_key).user_key > right,
                Bound::Excluded(right) => FullKey::decode(&meta.smallest_key).user_key >= right,
            };
            if past_end {
                break;
            }
            if F::reader(self.filter_bytes(range)).contains(&hash) {
                return true;
            }
        }
        false
    }
}
