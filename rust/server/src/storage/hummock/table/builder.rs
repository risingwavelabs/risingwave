//! Hummock SST builder.
//!
//! The SST format is exactly the same as `AgateDB` (`BadgerDB`), and is very similar to `RocksDB`.

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::utils::{bytes_diff, crc32_checksum};
use crate::storage::hummock::bloom::Bloom;
use crate::storage::hummock::format::user_key;
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
    pub table_size: u32,
    /// size of each block in bytes in SST
    pub block_size: u32,
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
    meta: TableIndex,
    key_hashes: Vec<u32>,
    options: TableBuilderOptions,
}

impl TableBuilder {
    /// Create new builder from options
    pub fn new(options: TableBuilderOptions) -> Self {
        Self {
            // approximately 16MB index + table size
            buf: BytesMut::with_capacity(options.table_size as usize),
            meta: TableIndex::default(),
            base_key: Bytes::new(),
            base_offset: 0,
            key_hashes: Vec::with_capacity(1024),
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
        self.key_hashes.push(farmhash::fingerprint32(user_key(key)));
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
        self.meta.estimated_size += sst_size as u32;
    }

    /// Append encoded block bytes to the buffer
    fn finish_block(&mut self) {
        assert!(!self.entry_offsets.is_empty());

        for offset in &self.entry_offsets {
            self.buf.put_u32_le(*offset);
        }
        self.buf.put_u32(self.entry_offsets.len() as u32);

        let cs = self.build_checksum(&self.buf[self.base_offset as usize..]);
        write_checksum(cs, &mut self.buf);

        self.add_block_to_meta();
    }

    fn add_block_to_meta(&mut self) {
        let block = BlockOffset {
            key: self.base_key.to_vec(),
            offset: self.base_offset,
            len: self.buf.len() as u32 - self.base_offset,
        };
        self.meta.offsets.push(block);
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
                                  5 * self.meta.offsets.len() as u32; // TODO: why 5?
        estimated_size as u32 > self.options.table_size
    }

    /// Finalize the table
    pub fn finish_to_blocks_and_meta(mut self) -> (Bytes, Bytes) {
        let mut meta = BytesMut::new();

        // append blocks
        self.finish_block();

        // TODO: move boundaries and build index if we need to encrypt or compress

        // initial Bloom filter
        if self.options.bloom_false_positive > 0.0 {
            let bits_per_key =
                Bloom::bloom_bits_per_key(self.key_hashes.len(), self.options.bloom_false_positive);
            let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
            self.meta.bloom_filter = bloom.to_vec();
        }

        // encode index
        let mut raw_index = BytesMut::new();
        self.meta.encode(&mut raw_index).unwrap();
        assert!(raw_index.len() < u32::MAX as usize);

        // append raw index to metadata
        meta.put_slice(&raw_index);

        // append checksum to metadata
        let cs = self.build_checksum(&raw_index);
        write_checksum(cs, &mut meta);

        (self.buf.freeze(), meta.freeze())
    }

    fn build_checksum(&self, data: &[u8]) -> Checksum {
        Checksum {
            sum: crc32_checksum(data),
            algo: ChecksumAlg::Crc32c as i32,
        }
    }
}

fn write_checksum(checksum: Checksum, target: &mut BytesMut) {
    let old_len = target.len();
    let mut cs = BytesMut::new();
    checksum.encode(&mut cs).unwrap();
    target.put(cs);
    target.put_u32((target.len() - old_len) as u32);
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::storage::hummock::Table;
    use itertools::Itertools;

    const TEST_KEYS_COUNT: usize = 100000;

    #[test]
    #[should_panic]
    fn test_empty() {
        let opt = TableBuilderOptions {
            bloom_false_positive: 0.1,
            block_size: 0,
            table_size: 0,
        };

        let b = TableBuilder::new(opt);

        b.finish_to_blocks_and_meta();
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

    pub fn generate_table() -> (Bytes, Bytes) {
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

        b.finish_to_blocks_and_meta()
    }

    fn key(prefix: &[u8], i: usize) -> Bytes {
        Bytes::from([prefix, format!("{:04}", i).as_bytes()].concat())
    }

    fn test_with_bloom_filter(with_blooms: bool) {
        let key_count = 1000;

        let opt = TableBuilderOptions {
            bloom_false_positive: if with_blooms { 0.01 } else { 0.0 },
            block_size: 0,
            table_size: 0,
        };

        let mut b = TableBuilder::new(opt);

        for i in 0..key_count {
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

        let (blocks, meta) = b.finish_to_blocks_and_meta();
        let table = Table::load(0, blocks, meta).unwrap();
        assert_eq!(table.has_bloom_filter, with_blooms);
        for i in 0..key_count {
            let hash = farmhash::fingerprint32(user_key(format!("key_test_{}", i).as_bytes()));
            assert!(!table.surely_not_have(hash));
        }
    }

    #[test]
    fn test_bloom_filter() {
        test_with_bloom_filter(false);
        test_with_bloom_filter(true);
    }

    #[test]
    fn test_build() {
        generate_table();
    }
}
