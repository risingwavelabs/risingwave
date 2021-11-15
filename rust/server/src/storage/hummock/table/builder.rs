//! Hummock SST builder.
//!
//! The SST format is exactly the same as `AgateDB` (`BadgerDB`), and is very similar to `RocksDB`.

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::utils::{bytes_diff, crc32_checksum};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use risingwave_pb::hummock::{
    checksum::Algorithm as ChecksumAlg, BlockOffset, Checksum, TableIndex,
};

/// Entry header stores the difference between current key and block base key. `overlap` is the
/// common prefix of key and base key, and diff is the length of different part.
#[derive(Default)]
pub struct Header {
    pub overlap: u16,
    pub diff: u16,
}

pub const HEADER_SIZE: usize = std::mem::size_of::<Header>();

impl Header {
    pub fn encode(&self, bytes: &mut impl BufMut) {
        bytes.put_u32_le((self.overlap as u32) << 16 | self.diff as u32);
    }

    pub fn decode(&mut self, bytes: &mut impl Buf) {
        let h = bytes.get_u32_le();
        self.overlap = (h >> 16) as u16;
        self.diff = h as u16;
    }
}

#[derive(Debug, Clone)]
pub struct TableBuilderOptions {
    /// target size of the table
    pub table_size: u64,
    /// size of each block in bytes in SST
    pub block_size: usize,
    /// false positive probability of bloom filter
    pub bloom_false_positive: f64,
}

/// Builder builds an SST.
///
/// The SST format is as follows:
///
/// ```plain
/// | Block | Block | Block | Block | Index |
/// ```
///
/// Inside each block, the block header records the base key of the block. And then, each entry
/// records the difference to the last key, and the value.
///
/// ```plain
/// | Header | DiffKey | Value |
/// ```
pub struct TableBuilder {
    buf: BytesMut,
    base_key: Bytes,
    base_offset: u32,
    entry_offsets: Vec<u32>,
    table_index: TableIndex,
    options: TableBuilderOptions,
}

impl TableBuilder {
    /// Create new builder from options
    pub fn new(options: TableBuilderOptions) -> Self {
        Self {
            // approximately 16MB index + table size
            buf: BytesMut::with_capacity((16 << 20) + options.table_size as usize),
            table_index: TableIndex::default(),
            base_key: Bytes::new(),
            base_offset: 0,
            entry_offsets: vec![],
            options,
        }
    }

    /// Check if the builder is empty
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn key_diff<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        bytes_diff(&self.base_key, key)
    }

    fn add_helper(&mut self, key: &[u8], value: &[u8]) {
        let diff_key = if self.base_key.is_empty() {
            self.base_key = key.to_vec().into();
            key
        } else {
            self.key_diff(key)
        };
        assert!(key.len() - diff_key.len() <= u16::MAX as usize);
        assert!(diff_key.len() <= u16::MAX as usize);
        let h = Header {
            overlap: (key.len() - diff_key.len()) as u16,
            diff: diff_key.len() as u16,
        };
        assert!(self.buf.len() <= u32::MAX as usize);
        self.entry_offsets
            .push(self.buf.len() as u32 - self.base_offset);

        h.encode(&mut self.buf);
        self.buf.put_slice(diff_key);
        self.buf.extend_from_slice(value);

        let sst_size = value.len() + diff_key.len() + 4;
        self.table_index.estimated_size += sst_size as u32;
    }

    fn finish_block(&mut self) {
        if self.entry_offsets.is_empty() {
            return;
        }
        for offset in &self.entry_offsets {
            self.buf.put_u32_le(*offset);
        }
        self.buf.put_u32(self.entry_offsets.len() as u32);

        let cs = self.build_checksum(&self.buf[self.base_offset as usize..]);
        self.write_checksum(cs);

        self.add_block_to_index();
    }

    fn add_block_to_index(&mut self) {
        let block = BlockOffset {
            key: self.base_key.to_vec(),
            offset: self.base_offset,
            len: self.buf.len() as u32 - self.base_offset,
        };
        self.table_index.offsets.push(block);
    }

    fn should_finish_block(&self, key: &[u8], value: &[u8]) -> bool {
        if self.entry_offsets.is_empty() {
            return false;
        }
        let entries_offsets_size = (self.entry_offsets.len() + 1) * 4 +
            4 + // size of list
            8 + // sum64 in checksum proto
            4; // checksum length
        assert!(entries_offsets_size < u32::MAX as usize);
        let estimated_size = (self.buf.len() as u32)
            - self.base_offset + 6 /* header size for entry */
            + key.len() as u32
            + value.len() as u32
            + entries_offsets_size as u32;
        assert!(self.buf.len() + (estimated_size as usize) < u32::MAX as usize);
        estimated_size > self.options.block_size as u32
    }

    /// Add key-value pair to table
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.should_finish_block(key, value) {
            self.finish_block();
            self.base_key.clear();
            assert!(self.buf.len() < u32::MAX as usize);
            self.base_offset = self.buf.len() as u32;
            self.entry_offsets.clear();
        }
        self.add_helper(key, value);
    }

    /// Check if entries reach its capacity
    pub fn reach_capacity(&self) -> bool {
        let block_size = self.buf.len() as u32 + // length of buffer
                                 self.entry_offsets.len() as u32 * 4 + // all entry offsets size
                                 4 + // count of all entry offsets
                                 8 + // checksum bytes
                                 4; // checksum length
        let estimated_size = block_size +
                                  4 + // index length
                                  5 * self.table_index.offsets.len() as u32; // TODO: why 5?
        estimated_size as u64 > self.options.table_size
    }

    /// Finalize the table
    pub fn finish(mut self) -> Bytes {
        self.finish_block();
        if self.buf.is_empty() {
            return Bytes::new();
        }
        let mut bytes = BytesMut::new();
        // TODO: move boundaries and build index if we need to encrypt or compress
        if self.options.bloom_false_positive > 0.0 {
            todo!("bloom filter is not supported yet");
        }
        // append index to buffer
        self.table_index.encode(&mut bytes).unwrap();
        assert!(bytes.len() < u32::MAX as usize);
        self.buf.put_slice(&bytes);
        self.buf.put_u32(bytes.len() as u32);
        // append checksum
        let cs = self.build_checksum(&bytes);
        self.write_checksum(cs);
        self.buf.freeze()
    }

    fn build_checksum(&self, data: &[u8]) -> Checksum {
        Checksum {
            sum: crc32_checksum(data),
            algo: ChecksumAlg::Crc32c as i32,
        }
    }

    fn write_checksum(&mut self, checksum: Checksum) {
        let old_len = self.buf.len();
        checksum.encode(&mut self.buf).unwrap();
        self.buf.put_u32((self.buf.len() - old_len) as u32);
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use itertools::Itertools;

    const TEST_KEYS_COUNT: usize = 100000;

    #[test]
    fn test_empty() {
        let opt = TableBuilderOptions {
            bloom_false_positive: 0.1,
            block_size: 0,
            table_size: 0,
        };

        let b = TableBuilder::new(opt);

        b.finish();
    }

    #[test]
    fn test_header_encode_decode() {
        let mut header = Header {
            overlap: 23333,
            diff: 23334,
        };
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        let mut buf = buf.freeze();
        header.decode(&mut buf);
        assert_eq!(header.overlap, 23333);
        assert_eq!(header.diff, 23334);
    }

    pub fn generate_table() -> Bytes {
        let opt = TableBuilderOptions {
            bloom_false_positive: 0.0,
            block_size: 0,
            table_size: 0,
        };

        let mut b = TableBuilder::new(opt);

        for i in 0..10000 {
            b.add(
                format!("key_test_{}", i).as_bytes(),
                "23332333"
                    .as_bytes()
                    .iter()
                    .cycle()
                    .cloned()
                    .take(i + 1)
                    .collect_vec()
                    .as_slice(),
            );
        }

        b.finish()
    }

    #[test]
    fn test_build() {
        generate_table();
    }
}
