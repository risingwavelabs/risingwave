// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//! Hummock SST builder.
//!
//! The SST format is exactly the same as `AgateDB` (`BadgerDB`), and is very similar to `RocksDB`.

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_pb::hummock::{BlockMeta, SstableMeta};

use super::bloom::Bloom;
use super::utils::bytes_diff;
use crate::hummock::key::user_key;
use crate::hummock::sstable::utils::checksum;
use crate::hummock::HummockValue;

/// Entry header stores the difference between current key and block base key. `overlap` is the
/// common prefix of key and base key, and diff is the length of different part.
#[derive(Default)]
pub struct Header {
    /// Overlap with base key.
    pub overlap: u16,

    /// Length of the diff.
    pub diff: u16,
}

pub const HEADER_SIZE: usize = std::mem::size_of::<Header>();

impl Header {
    /// Encode encodes the header.
    pub fn encode(&self, bytes: &mut impl BufMut) {
        bytes.put_u32_le((self.overlap as u32) << 16 | self.diff as u32);
    }

    /// Decode decodes the header.
    pub fn decode(bytes: &mut impl Buf) -> Self {
        let h = bytes.get_u32_le();
        Self {
            overlap: (h >> 16) as u16,
            diff: h as u16,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SSTableBuilderOptions {
    /// Target capacity of the table
    pub table_capacity: u32,

    /// Size of each block in bytes in SST
    pub block_size: u32,

    /// False positive probability of Bloom filter
    pub bloom_false_positive: f64,

    /// Checksum algorithm
    pub checksum_algo: ChecksumAlg,
}

/// Builder is used in building a table.
/// Builder builds an SST that consists of two parts:
/// - Table data is simply a sequence of blocks.
/// - Metadata is the prost-encoded `SstableMeta` data and essential information to determine the
/// checksum.
pub struct SSTableBuilder {
    options: SSTableBuilderOptions,

    meta: SstableMeta,

    /// Buffer blocks data
    data_buf: BytesMut,

    /// Used for prefix-encode
    base_key: Bytes,
    base_offset: u32,

    /// Entry offsets in a block
    entry_offsets: Vec<u32>,

    /// Used for building the Bloom filter
    key_hashes: Vec<u32>,
}

impl SSTableBuilder {
    /// Create new builder from options
    pub fn new(options: SSTableBuilderOptions) -> Self {
        Self {
            data_buf: BytesMut::with_capacity(options.table_capacity as usize),
            meta: SstableMeta::default(),
            base_key: Bytes::new(),
            base_offset: 0,
            key_hashes: Vec::with_capacity(1024),
            entry_offsets: vec![],
            options,
        }
    }

    /// Check if the builder is empty
    pub fn is_empty(&self) -> bool {
        self.data_buf.is_empty()
    }

    /// Calculate the difference of two keys
    fn key_diff<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        bytes_diff(&self.base_key, key)
    }

    /// Append encoded block bytes to the buffer
    fn finish_block(&mut self) {
        // try to set smallest key of table
        if self.meta.smallest_key.is_empty() {
            self.meta.smallest_key = self.base_key.to_vec();
        }

        // ---------- encode block ----------
        // different behavior: BadgerDB will just return.
        assert!(!self.entry_offsets.is_empty());

        // encode offsets list and its length
        for offset in &self.entry_offsets {
            self.data_buf.put_u32_le(*offset);
        }
        self.data_buf.put_u32(self.entry_offsets.len() as u32);

        // encode checksum and its length
        let checksum = checksum(
            self.options.checksum_algo,
            &self.data_buf[self.base_offset as usize..],
        );
        let mut cs_bytes = BytesMut::new();
        checksum.encode(&mut cs_bytes).unwrap();
        let ck_len = cs_bytes.len() as u32;
        self.data_buf.put(cs_bytes);
        self.data_buf.put_u32(ck_len);

        // ---------- add block offset to meta ----------
        let block_meta = BlockMeta {
            smallest_key: self.base_key.to_vec(),
            offset: self.base_offset,
            len: self.data_buf.len() as u32 - self.base_offset,
        };
        self.meta.block_metas.push(block_meta);
    }

    fn should_finish_block(&self, key: &[u8], value: HummockValue<&[u8]>) -> bool {
        // If there is no entry till now, we will return false.
        if self.entry_offsets.is_empty() {
            return false;
        }

        // We should include current entry also in size, that's why +1 to len(b.entryOffsets).
        let entries_offsets_size = (self.entry_offsets.len() + 1) * 4 +
        4 + // size of list
        8 + // sum64 in checksum proto
        4; // checksum length

        let estimated_size = self.data_buf.len()
            - self.base_offset as usize + 6 // header size for entry
            + key.len()
            + value.encoded_len()
            + entries_offsets_size;

        // Integer overflow check for table size.
        let _ = u32::try_from(self.data_buf.len() + estimated_size).unwrap();

        estimated_size > self.options.block_size as usize
    }

    /// Table data format:
    /// ```plain
    /// | Block | Block | Block | Block | Block |
    /// ```
    /// Add adds a key-value pair to the block.
    /// Note: the passed key-value pairs should be ordered,
    /// though we do not check that yet.
    pub fn add(&mut self, key: &[u8], value: HummockValue<&[u8]>) {
        if self.should_finish_block(key, value) {
            self.finish_block();
            self.base_key.clear();
            assert!(self.data_buf.len() < u32::MAX as usize);
            self.base_offset = self.data_buf.len() as u32;
            self.entry_offsets.clear();
        }

        // set largest key
        self.meta.largest_key.clear();
        self.meta.largest_key.extend_from_slice(key);

        // remove epoch before calculate hash
        let user_key = user_key(key);
        self.key_hashes.push(farmhash::fingerprint32(user_key));

        // diff_key stores the difference of key with baseKey.
        let diff_key = if self.base_key.is_empty() {
            self.base_key = key.to_vec().into();
            key
        } else {
            self.key_diff(key)
        };
        assert!(key.len() - diff_key.len() <= u16::MAX as usize);
        assert!(diff_key.len() <= u16::MAX as usize);

        // get header
        let header = Header {
            overlap: (key.len() - diff_key.len()) as u16,
            diff: diff_key.len() as u16,
        };
        assert!(self.data_buf.len() <= u32::MAX as usize);

        // store current entry's offset
        self.entry_offsets
            .push(self.data_buf.len() as u32 - self.base_offset);

        // entry layout: header, diffKey, value.
        header.encode(&mut self.data_buf);
        self.data_buf.put_slice(diff_key);
        value.encode(&mut self.data_buf);

        // update estimated size
        let block_size = value.encoded_len() + diff_key.len() + 4;
        self.meta.estimated_size += block_size as u32;
    }

    /// Returns true if we roughly reached capacity
    pub fn reach_capacity(&self) -> bool {
        let block_size = self.data_buf.len() as u32 + // actual length of current buffer
            self.entry_offsets.len() as u32 * 4 + // all entry offsets size
            4 + // count of all entry offsets
            8 + // checksum bytes
            4; // checksum length

        let estimated_size = block_size +
            4 + // index length
            5 * self.meta.block_metas.len() as u32; // TODO: why 5?
        estimated_size as u32 > self.options.table_capacity
    }

    /// Finalize the table to be blocks and metadata
    pub fn finish(mut self) -> (Bytes, SstableMeta) {
        // Append blocks. This will never start a new block.
        self.finish_block();

        // TODO: move boundaries and build index if we need to encrypt or compress

        // initial Bloom filter
        if self.options.bloom_false_positive > 0.0 {
            let bits_per_key =
                Bloom::bloom_bits_per_key(self.key_hashes.len(), self.options.bloom_false_positive);
            let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
            self.meta.bloom_filter = bloom.to_vec();
        }

        (self.data_buf.freeze(), self.meta)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_default_test_sstable, test_key_of, test_value_of,
        TEST_KEYS_COUNT,
    };

    #[test]
    #[should_panic]
    fn test_empty() {
        let opt = SSTableBuilderOptions {
            bloom_false_positive: 0.1,
            block_size: 4096,
            table_capacity: 0,
            checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
        };

        let b = SSTableBuilder::new(opt);

        b.finish();
    }

    #[test]
    fn test_smallest_key_and_largest_key() {
        let mut b = SSTableBuilder::new(default_builder_opt_for_test());

        for i in 0..TEST_KEYS_COUNT {
            b.add(&test_key_of(i), HummockValue::Put(&test_value_of(i)));
        }

        assert_eq!(test_key_of(0), b.meta.smallest_key);
        assert_eq!(test_key_of(TEST_KEYS_COUNT - 1), b.meta.largest_key);
    }

    #[test]
    fn test_header_encode_decode() {
        let header = Header {
            overlap: 23333,
            diff: 23334,
        };

        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        let mut buf = buf.freeze();
        let decoded_header = Header::decode(&mut buf);
        assert_eq!(decoded_header.overlap, 23333);
        assert_eq!(decoded_header.diff, 23334);
    }

    async fn test_with_bloom_filter(with_blooms: bool) {
        let key_count = 1000;

        let opts = SSTableBuilderOptions {
            bloom_false_positive: if with_blooms { 0.01 } else { 0.0 },
            block_size: 4096,
            table_capacity: 0,
            checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
        };

        // build remote table
        let sstable_store = mock_sstable_store();
        let table = gen_default_test_sstable(opts, 0, sstable_store).await;

        assert_eq!(table.has_bloom_filter(), with_blooms);
        for i in 0..key_count {
            let full_key = test_key_of(i);
            assert!(!table.surely_not_have_user_key(user_key(full_key.as_slice())));
        }
    }

    #[tokio::test]
    async fn test_bloom_filter() {
        test_with_bloom_filter(false).await;
        test_with_bloom_filter(true).await;
    }
}
