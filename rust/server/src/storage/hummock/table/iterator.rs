use std::cmp::Ordering::*;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use itertools::Itertools;

use crate::storage::hummock::HummockError;

use super::{Block, Header, HEADER_SIZE};

/// Errors that may encounter during iterator operation
#[derive(Clone, Debug, PartialEq)]
pub enum IteratorError {
    Eof,
    // TODO: As we need to clone Error from block iterator to table iterator,
    // we had to save `crate::Error` as String. In the future, we could let all
    // seek-related function to return a `Result<()>` instead of saving an
    // error inside struct.
    Error(String),
}
type IteratorResult<T> = std::result::Result<T, IteratorError>;

impl IteratorError {
    /// Check if iterator has reached its end
    pub fn is_eof(&self) -> bool {
        matches!(self, IteratorError::Eof)
    }

    /// Utility function to check if an Option<IteratorError> is Eof
    pub fn check_eof(err: &Option<IteratorError>) -> bool {
        matches!(err, Some(IteratorError::Eof))
    }
}

impl From<HummockError> for IteratorError {
    fn from(err: HummockError) -> Self {
        IteratorError::Error(err.to_string())
    }
}
enum SeekPos {
    Origin,
    Current,
}

/// Block iterator iterates on an SST block
// TODO: support custom comparator
struct BlockIterator {
    /// current index of iterator
    idx: usize,
    /// base key of the block
    base_key: Bytes,
    /// key of current entry
    key: BytesMut,
    /// raw value of current entry
    val: Bytes,
    /// block data in bytes
    data: Bytes,
    /// block struct
    // TODO: use `&'a Block` if possible
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

    fn set_idx(&mut self, i: usize) -> IteratorResult<()> {
        self.idx = i;
        if i >= self.entry_offsets().len() {
            return Err(IteratorError::Eof);
        }

        let start_offset = self.entry_offsets()[i] as u32;

        if self.base_key.is_empty() {
            let mut base_header = Header::default();
            base_header.decode(&mut self.data.slice(..));
            // TODO: combine this decode with header decode to avoid slice ptr copy
            self.base_key = self
                .data
                .slice(HEADER_SIZE..HEADER_SIZE + base_header.diff as usize);
        }

        let end_offset = if self.idx + 1 == self.entry_offsets().len() {
            self.data.len()
        } else {
            self.entry_offsets()[self.idx + 1] as usize
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
        Ok(())
    }

    /// Seek to the first entry that is equal or greater than key
    pub fn seek(&mut self, key: &Bytes, whence: SeekPos) -> IteratorResult<()> {
        let start_index = match whence {
            SeekPos::Origin => 0,
            SeekPos::Current => self.idx,
        };
        let found_entry_idx = (start_index..self.entry_offsets().len())
            .collect_vec()
            .partition_point(|idx| {
                self.set_idx(*idx).unwrap();
                matches!(&self.key.partial_cmp(key.as_ref()), Some(Less))
            })
            + start_index;

        self.set_idx(found_entry_idx)?;
        Ok(())
    }

    pub fn seek_to_first(&mut self) -> IteratorResult<()> {
        self.set_idx(0)
    }

    pub fn seek_to_last(&mut self) -> IteratorResult<()> {
        if self.entry_offsets().is_empty() {
            self.idx = std::usize::MAX;
            Err(IteratorError::Eof)
        } else {
            self.set_idx(self.entry_offsets().len() - 1)?;
            Ok(())
        }
    }

    pub fn next(&mut self) -> IteratorResult<()> {
        self.set_idx(self.idx + 1)?;
        Ok(())
    }

    pub fn prev(&mut self) -> IteratorResult<()> {
        if self.idx == 0 {
            self.idx = std::usize::MAX;
            Err(IteratorError::Eof)
        } else {
            self.set_idx(self.idx - 1)?;
            Ok(())
        }
    }

    pub fn is_ready(iter: &Option<Self>) -> bool {
        match iter {
            Some(iter) => iter.data.is_empty(),
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use itertools::Itertools;

    use crate::storage::hummock::{
        iterator::SeekPos, HummockValue, Table, TableBuilder, TableBuilderOptions,
    };

    use super::BlockIterator;

    // use super::*;
    #[test]
    fn basic_test() {
        let opt = TableBuilderOptions {
            bloom_false_positive: 0.0,
            block_size: 16384,
            table_size: 0,
        };

        let mut b = TableBuilder::new(opt);
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
        let (blocks, meta) = b.finish_to_blocks_and_meta();
        let table = Table::load(0, blocks, meta).unwrap();
        let block = table.block(0).unwrap();

        let mut blk_iter = BlockIterator::new(block);
        let mut idx = 0;
        blk_iter.seek_to_first().unwrap();
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

            if let Err(e) = blk_iter.next() {
                assert!(e.is_eof());
                break;
            } else {
                idx += 1;
            }
        }
        assert_eq!(idx, 9);

        blk_iter.seek_to_first().unwrap();
        assert_eq!(BytesMut::from("key_test_0"), blk_iter.key);

        blk_iter.seek_to_last().unwrap();
        assert_eq!(BytesMut::from("key_test_9"), blk_iter.key);

        blk_iter
            .seek(&Bytes::from("key_test_4"), SeekPos::Origin)
            .unwrap();
        assert_eq!(BytesMut::from("key_test_4"), blk_iter.key);

        blk_iter
            .seek(&Bytes::from("key_test_0"), SeekPos::Origin)
            .unwrap();
        assert_eq!(BytesMut::from("key_test_0"), blk_iter.key);

        blk_iter
            .seek(&Bytes::from("key_test"), SeekPos::Origin)
            .unwrap();
        assert_eq!(BytesMut::from("key_test_0"), blk_iter.key);

        blk_iter
            .seek(&Bytes::from("key_test_9"), SeekPos::Origin)
            .unwrap();
        assert_eq!(BytesMut::from("key_test_9"), blk_iter.key);

        if let Err(e) = blk_iter.seek(&Bytes::from("key_test_99"), SeekPos::Origin) {
            assert!(e.is_eof());
        } else {
            unreachable!("should return eof");
        }

        blk_iter.set_idx(3).unwrap();
        blk_iter
            .seek(&Bytes::from("key_test_0"), SeekPos::Current)
            .unwrap();

        assert_eq!(BytesMut::from("key_test_3"), blk_iter.key);
    }
}
