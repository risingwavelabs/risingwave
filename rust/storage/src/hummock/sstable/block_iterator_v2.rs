use std::cmp::Ordering;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use super::{BlockV2, KeyPrefix};

/// [`BlockIteratorV2`] is used to read kv pairs in a block.
pub struct BlockIteratorV2 {
    /// Block that iterates on.
    block: Arc<BlockV2>,
    /// Current restart point index.
    restart_point_index: usize,
    /// Current offset.
    offset: usize,
    /// Current key.
    key: BytesMut,
    /// Current value.
    value: Bytes,
    /// Current entry len.
    entry_len: usize,
}

impl BlockIteratorV2 {
    pub fn new(block: Arc<BlockV2>) -> Self {
        Self {
            block,
            offset: usize::MAX,
            restart_point_index: usize::MAX,
            key: BytesMut::default(),
            value: Bytes::default(),
            entry_len: 0,
        }
    }

    pub fn next(&mut self) {
        assert!(self.is_valid());
        self.next_inner();
    }

    pub fn prev(&mut self) {
        assert!(self.is_valid());
        self.prev_inner();
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.key[..]
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.value[..]
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

    pub fn seek(&mut self, key: &[u8]) {
        self.seek_restart_point_by_key(key);
        self.next_until_key(key);
    }

    pub fn seek_le(&mut self, key: &[u8]) {
        self.seek_restart_point_by_key(key);
        self.next_until_key(key);
        if !self.is_valid() {
            self.seek_to_last();
        }
        self.prev_until_key(key);
    }
}

impl BlockIteratorV2 {
    /// Invalidate current state after reaching a invalid state.
    fn invalidate(&mut self) {
        self.offset = self.block.len();
        self.restart_point_index = self.block.restart_point_len();
        self.key.clear();
        self.value.clear();
        self.entry_len = 0;
    }

    /// Move to the next entry.
    ///
    /// Note: Ensure that the current state is valid.
    fn next_inner(&mut self) {
        let offset = self.offset + self.entry_len;
        if offset >= self.block.len() {
            self.invalidate();
            return;
        }
        let prefix = self.decode_prefix_at(offset);
        self.key.truncate(prefix.overlap_len());
        self.key
            .extend_from_slice(&self.block.data()[prefix.diff_key_range()]);
        self.value = self.block.data().slice(prefix.value_range());
        self.offset = offset;
        self.entry_len = prefix.entry_len();
        if self.restart_point_index + 1 < self.block.restart_point_len()
            && self.offset >= self.block.restart_point(self.restart_point_index + 1) as usize
        {
            self.restart_point_index += 1;
        }
    }

    /// Move forward until reach the first that equals or larger than the given `key`.
    fn next_until_key(&mut self, key: &[u8]) {
        while self.is_valid() && (&self.key[..]).cmp(key) == Ordering::Less {
            self.next_inner();
        }
    }

    /// Move backward until reach the first key that equals or smaller than the given `key`.
    fn prev_until_key(&mut self, key: &[u8]) {
        while self.is_valid() && (&self.key[..]).cmp(key) == Ordering::Greater {
            self.prev_inner();
        }
    }

    /// Move forward until the position reaches the previous position of the given `next_offset` or
    /// the last valid position if exists.
    fn next_until_prev_offset(&mut self, offset: usize) {
        while self.offset + self.entry_len < std::cmp::min(self.block.len(), offset) {
            self.next_inner();
        }
    }

    /// Move to the previous entry.
    ///
    /// Note: Ensure that the current state is valid.
    fn prev_inner(&mut self) {
        if self.offset == 0 {
            self.invalidate();
            return;
        }
        if self.block.restart_point(self.restart_point_index) as usize == self.offset {
            self.restart_point_index -= 1;
        }
        let origin_offset = self.offset;
        self.seek_restart_point_by_index(self.restart_point_index);
        self.next_until_prev_offset(origin_offset);
    }

    /// Decode [`KeyPrefix`] at given offset.
    fn decode_prefix_at(&self, offset: usize) -> KeyPrefix {
        KeyPrefix::decode(&mut &self.block.data()[offset..], offset)
    }

    /// Search the restart point index that the given `key` belongs to.
    fn search_restart_point_index_by_key(&self, key: &[u8]) -> usize {
        self.block.search_restart_point_by(|probe| {
            let prefix = self.decode_prefix_at(*probe as usize);
            let probe_key = &self.block.data()[prefix.diff_key_range()];
            (probe_key).cmp(key)
        })
    }

    /// Seek to the restart point that the given `key` belongs to.
    fn seek_restart_point_by_key(&mut self, key: &[u8]) {
        let index = self.search_restart_point_index_by_key(key);
        self.seek_restart_point_by_index(index)
    }

    /// Seek to the restart point by given restart point index.
    fn seek_restart_point_by_index(&mut self, index: usize) {
        let offset = self.block.restart_point(index) as usize;
        let prefix = self.decode_prefix_at(offset);
        self.key = BytesMut::from(&self.block.data()[prefix.diff_key_range()]);
        self.value = self.block.data().slice(prefix.value_range());
        self.offset = offset;
        self.entry_len = prefix.entry_len();
        self.restart_point_index = index;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::sstable::utils::full_key;
    use crate::hummock::{BlockV2Builder, BlockV2BuilderOptions};

    fn build_iterator_for_test() -> BlockIteratorV2 {
        let options = BlockV2BuilderOptions::default();
        let mut builder = BlockV2Builder::new(options);
        builder.add(&full_key(b"k01", 1), b"v01");
        builder.add(&full_key(b"k02", 2), b"v02");
        builder.add(&full_key(b"k04", 4), b"v04");
        builder.add(&full_key(b"k05", 5), b"v05");
        let buf = builder.build();
        BlockIteratorV2::new(Arc::new(BlockV2::decode(buf).unwrap()))
    }

    #[test]
    fn test_seek_first() {
        let mut it = build_iterator_for_test();
        it.seek_to_first();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());
    }

    #[test]
    fn test_seek_last() {
        let mut it = build_iterator_for_test();
        it.seek_to_last();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());
    }

    #[test]
    fn test_seek_none_front() {
        let mut it = build_iterator_for_test();
        it.seek(&full_key(b"k00", 0)[..]);
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());

        let mut it = build_iterator_for_test();

        it.seek_le(&full_key(b"k00", 0)[..]);
        assert!(!it.is_valid());
    }

    #[test]
    fn test_seek_none_back() {
        let mut it = build_iterator_for_test();
        it.seek(&full_key(b"k06", 6)[..]);
        assert!(!it.is_valid());

        let mut it = build_iterator_for_test();
        it.seek_le(&full_key(b"k06", 6)[..]);
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());
    }

    #[test]
    fn bi_direction_seek() {
        let mut it = build_iterator_for_test();
        it.seek(&full_key(b"k03", 3)[..]);
        assert_eq!(&full_key(format!("k{:02}", 4).as_bytes(), 4)[..], it.key());

        it.seek_le(&full_key(b"k03", 3)[..]);
        assert_eq!(&full_key(format!("k{:02}", 2).as_bytes(), 2)[..], it.key());
    }

    #[test]
    fn test_forward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek_to_first();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());

        it.next();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k02", 2)[..], it.key());
        assert_eq!(b"v02", it.value());

        it.next();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k04", 4)[..], it.key());
        assert_eq!(b"v04", it.value());

        it.next();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());

        it.next();
        assert!(!it.is_valid());
    }

    #[test]
    fn test_backward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek_to_last();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k05", 5)[..], it.key());
        assert_eq!(b"v05", it.value());

        it.prev();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k04", 4)[..], it.key());
        assert_eq!(b"v04", it.value());

        it.prev();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k02", 2)[..], it.key());
        assert_eq!(b"v02", it.value());

        it.prev();
        assert!(it.is_valid());
        assert_eq!(&full_key(b"k01", 1)[..], it.key());
        assert_eq!(b"v01", it.value());

        it.prev();
        assert!(!it.is_valid());
    }

    #[test]
    fn test_seek_forward_backward_iterate() {
        let mut it = build_iterator_for_test();

        it.seek(&full_key(b"k03", 3)[..]);
        assert_eq!(&full_key(format!("k{:02}", 4).as_bytes(), 4)[..], it.key());

        it.prev();
        assert_eq!(&full_key(format!("k{:02}", 2).as_bytes(), 2)[..], it.key());

        it.next();
        assert_eq!(&full_key(format!("k{:02}", 4).as_bytes(), 4)[..], it.key());
    }
}
