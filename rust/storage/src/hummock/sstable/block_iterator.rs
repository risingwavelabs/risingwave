use std::cmp::Ordering::*;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use itertools::Itertools;

use super::{Block, Header, HEADER_SIZE};
use crate::hummock::version_cmp::VersionedComparator;

pub enum SeekPos {
    Origin,
    Current,
}

/// Block iterator iterates on an SST block
// TODO: support custom comparator
pub struct BlockIterator {
    /// current index of iterator
    idx: isize,
    /// base key of the block
    base_key: Bytes,
    /// key of current entry
    key: BytesMut,
    /// raw value of current entry
    val: Bytes,
    /// block data in bytes
    data: Bytes,
    /// block struct
    block: Arc<Block>,
    /// previous overlap key, used to construct key of current entry from
    /// previous one faster
    perv_overlap: u16,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        let data = block.data.slice(..block.entries_index_start);
        Self {
            block,
            base_key: Bytes::new(),
            key: BytesMut::new(),
            val: Bytes::new(),
            data,
            perv_overlap: 0,
            idx: 0,
        }
    }

    /// Replace block inside iterator and reset the iterator
    pub fn set_block(&mut self, block: Arc<Block>) {
        self.idx = 0;
        self.base_key.clear();
        self.perv_overlap = 0;
        self.key.clear();
        self.val.clear();
        self.data = block.data.slice(..block.entries_index_start);
        self.block = block;
    }

    #[inline]
    fn entry_offsets(&self) -> &[u32] {
        &self.block.entry_offsets
    }

    /// Update the internal state of the iterator to use the value and key of a given index.
    ///
    /// If the index is not inside the entries, the function will not fetch the value, and will
    /// return false.
    fn set_idx(&mut self, i: isize) -> bool {
        self.idx = i;

        if self.idx < 0 || self.idx >= self.entry_offsets().len() as isize {
            return false;
        }

        let start_offset = self.entry_offsets()[i as usize] as u32;

        if self.base_key.is_empty() {
            let mut base_header = Header::default();
            base_header.decode(&mut self.data.slice(..));
            // TODO: combine this decode with header decode to avoid slice ptr copy
            self.base_key = self
                .data
                .slice(HEADER_SIZE..HEADER_SIZE + base_header.diff as usize);
        }

        let end_offset = if self.idx as usize + 1 == self.entry_offsets().len() {
            self.data.len()
        } else {
            self.entry_offsets()[self.idx as usize + 1] as usize
        };

        let mut entry_data = self.data.slice(start_offset as usize..end_offset as usize);
        let mut header = Header::default();
        header.decode(&mut entry_data);

        // TODO: merge this truncate with the following key truncate
        if header.overlap > self.perv_overlap {
            self.key.truncate(self.perv_overlap as usize);
            self.key.extend_from_slice(
                &self.base_key[self.perv_overlap as usize..header.overlap as usize],
            );
        }
        self.perv_overlap = header.overlap;

        let diff_key = &entry_data[..header.diff as usize];
        self.key.truncate(header.overlap as usize);
        self.key.extend_from_slice(diff_key);
        self.val = entry_data.slice(header.diff as usize..);

        true
    }

    /// Seek to the first entry that is equal or greater than key.
    pub fn seek(&mut self, key: &[u8], whence: SeekPos) {
        let start_index = match whence {
            SeekPos::Origin => 0,
            SeekPos::Current => self.idx as usize,
        };
        let found_entry_idx = (start_index..self.entry_offsets().len())
            // TODO: remove this collect_vec
            .collect_vec()
            .partition_point(|idx| {
                self.set_idx(*idx as isize);

                // compare by version comparator
                VersionedComparator::compare_key(&self.key, key) == Less
            })
            + start_index;

        self.set_idx(found_entry_idx as isize);
    }

    /// Seek to the first entry that is equal or less than key.
    pub fn seek_le(&mut self, key: &[u8], whence: SeekPos) {
        let end_index = match whence {
            SeekPos::Origin => self.entry_offsets().len(),
            SeekPos::Current => self.idx as usize + 1,
        };
        let found_entry_idx = (0..end_index).collect_vec().partition_point(|idx| {
            self.set_idx(*idx as isize);

            let ord = VersionedComparator::compare_key(&self.key, key);
            ord == Less || ord == Equal
        });
        let found_entry_idx = found_entry_idx as isize - 1;

        self.set_idx(found_entry_idx);
    }

    pub fn seek_to_first(&mut self) {
        self.set_idx(0);
    }

    pub fn seek_to_last(&mut self) {
        if self.entry_offsets().is_empty() {
            panic!("invalid block: no entry found inside")
        } else {
            self.set_idx(self.entry_offsets().len() as isize - 1);
        }
    }

    /// Return the key and value of the previous operation
    pub fn data(&self) -> Option<(&[u8], &[u8])> {
        if self.is_valid() {
            Some((&self.key[..], &self.val[..]))
        } else {
            None
        }
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.data().map(|(k, _v)| k)
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.data().map(|(_k, v)| v)
    }

    /// Check whether the iterator is at the last position
    pub fn is_last(&self) -> bool {
        self.idx >= 0 && self.idx == (self.entry_offsets().len() - 1) as isize
    }
    /// Check whether the iterator is at a valid position
    pub fn is_valid(&self) -> bool {
        self.idx >= 0 && self.idx < self.entry_offsets().len() as isize
    }

    /// Move to the next position
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        self.set_idx(self.idx + 1)
    }

    /// Move to the previous position
    pub fn prev(&mut self) -> bool {
        self.set_idx(self.idx - 1)
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use itertools::Itertools;

    use super::super::{SSTableBuilder, SSTableBuilderOptions};
    use super::*;
    use crate::hummock::cloud::gen_remote_sstable;
    use crate::hummock::{HummockValue, REMOTE_DIR};
    use crate::object::{InMemObjectStore, ObjectStore};

    #[tokio::test]
    async fn basic_test() {
        let opt = SSTableBuilderOptions {
            bloom_false_positive: 0.0,
            block_size: 16384,
            table_capacity: 0,
            checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
        };

        let mut b = SSTableBuilder::new(opt);
        for i in 0..10 {
            b.add(
                format!("key_test_{}", i).as_bytes(),
                HummockValue::Put(
                    format!("val_{}", i)
                        .as_str()
                        .as_bytes()
                        .iter()
                        .cloned()
                        .collect_vec(),
                ),
            );
        }
        let (blocks, meta) = b.finish();

        let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
        let table = gen_remote_sstable(obj_client, 0, blocks, meta, REMOTE_DIR)
            .await
            .unwrap();

        let block = table.block(0).await.unwrap();

        let mut blk_iter = BlockIterator::new(block);
        let mut idx = 0;
        blk_iter.seek_to_first();
        loop {
            assert_eq!(blk_iter.idx, idx);
            assert_eq!(
                BytesMut::from(format!("key_test_{}", idx).as_str()),
                blk_iter.key
            );

            let expected = HummockValue::Put(
                format!("val_{}", idx)
                    .as_str()
                    .as_bytes()
                    .iter()
                    .cloned()
                    .collect_vec(),
            );
            let scanned = HummockValue::decode(&mut blk_iter.val).unwrap();

            assert_eq!(scanned, expected);

            blk_iter.next();
            if blk_iter.data().is_none() {
                break;
            } else {
                idx += 1;
            }
        }
        assert_eq!(idx, 9);

        blk_iter.seek_to_first();
        assert_eq!(BytesMut::from("key_test_0"), blk_iter.key);

        blk_iter.seek_to_last();
        assert_eq!(BytesMut::from("key_test_9"), blk_iter.key);

        idx = 9;
        loop {
            assert_eq!(blk_iter.idx, idx);
            assert_eq!(
                BytesMut::from(format!("key_test_{}", idx).as_str()),
                blk_iter.key
            );

            let expected = HummockValue::Put(
                format!("val_{}", idx)
                    .as_str()
                    .as_bytes()
                    .iter()
                    .cloned()
                    .collect_vec(),
            );
            let scanned = HummockValue::decode(&mut blk_iter.val).unwrap();

            assert_eq!(scanned, expected);

            blk_iter.prev();
            if blk_iter.data().is_none() {
                break;
            } else {
                idx -= 1;
            }
        }
        assert_eq!(idx, 0);

        blk_iter.seek(&Bytes::from("key_test_4"), SeekPos::Origin);
        assert_eq!(BytesMut::from("key_test_4"), blk_iter.key);

        blk_iter.seek(&Bytes::from("key_test_0"), SeekPos::Origin);
        assert_eq!(BytesMut::from("key_test_0"), blk_iter.key);

        blk_iter.seek(&Bytes::from("key_test"), SeekPos::Origin);
        assert_eq!(BytesMut::from("key_test_0"), blk_iter.key);

        blk_iter.seek(&Bytes::from("key_test_9"), SeekPos::Origin);
        assert_eq!(BytesMut::from("key_test_9"), blk_iter.key);

        blk_iter.seek(&Bytes::from("key_test_99"), SeekPos::Origin);
        assert!(blk_iter.data().is_none());

        blk_iter.set_idx(3);
        blk_iter.seek(&Bytes::from("key_test_0"), SeekPos::Current);

        assert_eq!(BytesMut::from("key_test_3"), blk_iter.key);
    }
}
