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

use std::{cmp::Ordering, ops::Deref};
use std::ops::Range;

use bytes::{BytesMut, Buf, BufMut};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{FullKey, UserKey, EPOCH_LEN, TableKey};
use risingwave_hummock_sdk::KeyComparator;

use super::KeyPrefix;
use crate::hummock::BlockHolder;

/// [`BlockIterator`] is used to read kv pairs in a block.
pub struct BlockIterator {
    /// Block that iterates on.
    block: BlockHolder,
    /// Current restart point index.
    restart_point_index: usize,
    /// Current offset.
    offset: usize,
    /// Current key.
    key: BytesMut,
    /// Current value.
    value_range: Range<usize>,
    /// Current entry len.
    entry_len: usize,
}

impl BlockIterator {
    pub fn new(block: BlockHolder) -> Self {
        Self {
            block,
            offset: usize::MAX,
            restart_point_index: usize::MAX,
            key: BytesMut::default(),
            value_range: 0..0,
            entry_len: 0,
        }
    }

    pub fn next(&mut self) {
        assert!(self.is_valid());
        self.next_inner();
    }

    pub fn try_next(&mut self) -> bool {
        assert!(self.is_valid());
        self.try_next_inner()
    }

    pub fn prev(&mut self) {
        assert!(self.is_valid());
        self.prev_inner();
    }

    pub fn try_prev(&mut self) -> bool {
        assert!(self.is_valid());
        self.try_prev_inner()
    }

    pub fn key(&self) -> FullKey<&[u8]> {
        assert!(self.is_valid());
        let table_id = TableId::new(self.block.deref().table_id());
        let epoch_pos = &self.key[..].len() - EPOCH_LEN;
        let epoch = (&self.key[epoch_pos..]).get_u64();
        let user_key = UserKey::new(table_id, TableKey(&self.key[..epoch_pos]));
        let full_key = FullKey::from_user_key(user_key, epoch);

        full_key
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.block.data()[self.value_range.clone()]
    }

    pub fn is_valid(&self) -> bool {
        self.offset < self.block.len()
    }

    pub fn seek_to_first(&mut self) {
        self.seek_restart_point_by_index(0);
    }

    pub fn seek_to_last(&mut self) {
        self.seek_restart_point_by_index(self.block.restart_point_len() - 1);
        self.next_until_prev_offset(self.block.len());
    }

    pub fn seek(&mut self, key: FullKey<&[u8]>) {
        let mut full_key_encoded: BufMut::default() ;
        key.encode_into_without_table_id(&mut full_key_encoded);
        self.seek_restart_point_by_key(&full_key_encoded);
        self.next_until_key(&full_key_encoded);
    }

    pub fn seek_le(&mut self, key: FullKey<&[u8]>) {
        let full_key_encoded = key.encode();
        self.seek_restart_point_by_key(&full_key_encoded);
        self.next_until_key(&full_key_encoded);
        if !self.is_valid() {
            self.seek_to_last();
        }
        self.prev_until_key(&full_key_encoded);
    }
}

impl BlockIterator {
    /// Invalidates current state after reaching a invalid state.
    fn invalidate(&mut self) {
        self.offset = self.block.len();
        self.restart_point_index = self.block.restart_point_len();
        self.key.clear();
        self.value_range = 0..0;
        self.entry_len = 0;
    }

    /// Moving to the next entry
    ///
    /// Note: The current state may be invalid if there is no more data to read
    fn next_inner(&mut self) {
        if !self.try_next_inner() {
            self.invalidate();
        }
    }

    /// Try moving to the next entry.
    ///
    /// The current state will still be valid if there is no more data to read.
    ///
    /// Return: true is the iterator is advanced and false otherwise.
    fn try_next_inner(&mut self) -> bool {
        let offset = self.offset + self.entry_len;
        if offset >= self.block.len() {
            return false;
        }
        let prefix = self.decode_prefix_at(offset);
        self.key.truncate(prefix.overlap_len());
        self.key
            .extend_from_slice(&self.block.data()[prefix.diff_key_range()]);
        self.value_range = prefix.value_range();
        self.offset = offset;
        self.entry_len = prefix.entry_len();
        if self.restart_point_index + 1 < self.block.restart_point_len()
            && self.offset >= self.block.restart_point(self.restart_point_index + 1) as usize
        {
            self.restart_point_index += 1;
        }
        true
    }

    /// Moves forward until reaching the first that equals or larger than the given `key`.
    fn next_until_key(&mut self, key: &[u8]) {
        while self.is_valid()
            && KeyComparator::compare_encoded_full_key(&self.key[..], key) == Ordering::Less
        {
            self.next_inner();
        }
    }

    /// Moves backward until reaching the first key that equals or smaller than the given `key`.
    fn prev_until_key(&mut self, key: &[u8]) {
        while self.is_valid()
            && KeyComparator::compare_encoded_full_key(&self.key[..], key) == Ordering::Greater
        {
            self.prev_inner();
        }
    }

    /// Moves forward until the position reaches the previous position of the given `next_offset` or
    /// the last valid position if exists.
    fn next_until_prev_offset(&mut self, offset: usize) {
        while self.offset + self.entry_len < std::cmp::min(self.block.len(), offset) {
            self.next_inner();
        }
    }

    /// Moving to the previous entry
    ///
    /// Note: The current state may be invalid if there is no more data to read
    fn prev_inner(&mut self) {
        if !self.try_prev_inner() {
            self.invalidate();
        }
    }

    /// Try moving to the previous entry.
    ///
    /// The current state will still be valid if there is no more data to read.
    ///
    /// Return: true is the iterator is advanced and false otherwise.
    fn try_prev_inner(&mut self) -> bool {
        if self.offset == 0 {
            return false;
        }
        if self.block.restart_point(self.restart_point_index) as usize == self.offset {
            self.restart_point_index -= 1;
        }
        let origin_offset = self.offset;
        self.seek_restart_point_by_index(self.restart_point_index);
        self.next_until_prev_offset(origin_offset);
        true
    }

    /// Decodes [`KeyPrefix`] at given offset.
    fn decode_prefix_at(&self, offset: usize) -> KeyPrefix {
        KeyPrefix::decode(&mut &self.block.data()[offset..], offset)
    }

    /// Searches the restart point index that the given `key` belongs to.
    fn search_restart_point_index_by_key(&self, key: &[u8]) -> usize {
        // Find the largest restart point that restart key equals or less than the given key.
        self.block
            .search_restart_partition_point(|&probe| {
                let prefix = self.decode_prefix_at(probe as usize);
                let probe_key = &self.block.data()[prefix.diff_key_range()];
                match KeyComparator::compare_encoded_full_key(probe_key, key) {
                    Ordering::Less | Ordering::Equal => true,
                    Ordering::Greater => false,
                }
            })
            .saturating_sub(1) // Prevent from underflowing when given is smaller than the first.
    }

    /// Seeks to the restart point that the given `key` belongs to.
    fn seek_restart_point_by_key(&mut self, key: &[u8]) {
        let index = self.search_restart_point_index_by_key(key);
        self.seek_restart_point_by_index(index)
    }

    /// Seeks to the restart point by given restart point index.
    fn seek_restart_point_by_index(&mut self, index: usize) {
        let offset = self.block.restart_point(index) as usize;
        let prefix = self.decode_prefix_at(offset);
        self.key = BytesMut::from(&self.block.data()[prefix.diff_key_range()]);
        self.value_range = prefix.value_range();
        self.offset = offset;
        self.entry_len = prefix.entry_len();
        self.restart_point_index = index;
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;

    use super::*;
    use crate::hummock::{Block, BlockBuilder, BlockBuilderOptions};

    fn build_iterator_for_test() -> BlockIterator {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        builder.add(construct_full_key_struct(0, b"k01", 1), b"v01");
        builder.add(construct_full_key_struct(0, b"k02", 2), b"v02");
        builder.add(construct_full_key_struct(0, b"k04", 4), b"v04");
        builder.add(construct_full_key_struct(0, b"k05", 5), b"v05");
        let capacity = builder.uncompressed_block_size();
        let buf = builder.build().to_vec();
        BlockIterator::new(BlockHolder::from_owned_block(Box::new(
            Block::decode(buf.into(), capacity).unwrap(),
        )))
    }

    #[test]
    fn test_seek_first() {
        let mut it = build_iterator_for_test();
        it.seek_to_first();
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k01", 1), it.key());
        assert_eq!(b"v01", it.value());
    }

    #[test]
    fn test_seek_last() {
        let mut it = build_iterator_for_test();
        it.seek_to_last();
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k05", 5), it.key());
        assert_eq!(b"v05", it.value());
    }

    #[test]
    fn test_seek_none_front() {
        let mut it = build_iterator_for_test();
        it.seek(construct_full_key_struct(0, b"k00", 0));
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k01", 1), it.key());
        assert_eq!(b"v01", it.value());

        let mut it = build_iterator_for_test();

        it.seek_le(construct_full_key_struct(0, b"k00", 0));
        assert!(!it.is_valid());
    }

    #[test]
    fn test_seek_none_back() {
        let mut it = build_iterator_for_test();
        it.seek(construct_full_key_struct(0, b"k06", 6));
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test();
        it.seek_le(construct_full_key_struct(0, b"k06", 6));
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k05", 5), it.key());
        assert_eq!(b"v05", it.value());
    }

    #[test]
    fn bi_direction_seek() {
        let mut it = build_iterator_for_test();
        it.seek(construct_full_key_struct(0, b"k03", 3));
        assert_eq!(
            construct_full_key_struct(0, format!("k{:02}", 4).as_bytes(), 4),
            it.key()
        );

        it.seek_le(construct_full_key_struct(0, b"k03", 3));
        assert_eq!(
            construct_full_key_struct(0, format!("k{:02}", 2).as_bytes(), 2),
            it.key()
        );
    }

    #[test]
    fn test_forward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek_to_first();
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k01", 1), it.key());
        assert_eq!(b"v01", it.value());

        it.next();
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k02", 2), it.key());
        assert_eq!(b"v02", it.value());

        it.next();
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k04", 4), it.key());
        assert_eq!(b"v04", it.value());

        it.next();
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k05", 5), it.key());
        assert_eq!(b"v05", it.value());

        it.next();
        assert!(!it.is_valid());
    }

    #[test]
    fn test_backward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek_to_last();
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k05", 5), it.key());
        assert_eq!(b"v05", it.value());

        it.prev();
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k04", 4), it.key());
        assert_eq!(b"v04", it.value());

        it.prev();
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k02", 2), it.key());
        assert_eq!(b"v02", it.value());

        it.prev();
        assert!(it.is_valid());
        assert_eq!(construct_full_key_struct(0, b"k01", 1), it.key());
        assert_eq!(b"v01", it.value());

        it.prev();
        assert!(!it.is_valid());
    }

    #[test]
    fn test_seek_forward_backward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek(construct_full_key_struct(0, b"k03", 3));
        assert_eq!(
            construct_full_key_struct(0, format!("k{:02}", 4).as_bytes(), 4),
            it.key()
        );

        it.prev();
        assert_eq!(
            construct_full_key_struct(0, format!("k{:02}", 2).as_bytes(), 2),
            it.key()
        );

        it.next();
        assert_eq!(
            construct_full_key_struct(0, format!("k{:02}", 4).as_bytes(), 4),
            it.key()
        );
    }

    pub fn construct_full_key_struct(
        table_id: u32,
        table_key: &[u8],
        epoch: u64,
    ) -> FullKey<&[u8]> {
        FullKey::for_test(TableId::new(table_id), table_key, epoch)
    }
}
