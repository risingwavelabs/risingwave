//! Hummock state store's SST builder, format and iterator

// TODO: enable checksum
#![allow(dead_code)]

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod block_iterator;
mod bloom;
use bloom::Bloom;
pub mod builder;
pub mod multi_builder;
pub use block_iterator::*;
pub use builder::*;
mod sstable_iterator;
pub use sstable_iterator::*;
mod reverse_sstable_iterator;
pub use reverse_sstable_iterator::*;
mod utils;

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_pb::hummock::{Checksum, SstableMeta};
use utils::verify_checksum;

use super::{HummockError, HummockResult};
use crate::hummock::sstable::utils::checksum;

const DEFAULT_META_BUFFER_CAPACITY: usize = 4096;

/// Block contains several entries. It can be obtained from an SST.
#[derive(Default)]
pub struct Block {
    offset: usize,
    data: Bytes,
    checksum: Checksum,
    entries_index_start: usize,
    entry_offsets: Vec<u32>,
    checksum_len: usize,
}

impl Block {
    fn size(&self) -> u64 {
        3 * std::mem::size_of::<usize>() as u64
            + self.data.len() as u64
            + self.checksum_len as u64
            + self.entry_offsets.len() as u64 * std::mem::size_of::<u32>() as u64
    }

    fn verify_checksum(&self) -> HummockResult<()> {
        // let checksum = prost::Message::decode(self.checksum.clone())?;
        verify_checksum(&self.checksum, &self.data)
    }

    /// Structure of Block:
    /// ```plain
    /// +-------------------+-----------------+--------------------+--------------+------------------+
    /// | Entry1            | Entry2          | Entry3             | Entry4       | Entry5
    /// +-------------------+-----------------+--------------------+--------------+------------------+
    /// | Entry6            | ...             | ...                | ...          | EntryN
    /// +-------------------+-----------------+--------------------+--------------+------------------+
    /// | Offsets list used to perform binary | Offsets list Size  | Block        | Checksum Size
    /// | search in the block                 | (4 Bytes)          | Checksum     | (4 Bytes)
    /// +-------------------------------------+--------------------+--------------+------------------+
    /// ```
    /// Decode block from given bytes
    pub fn decode(data: Bytes, block_offset: usize) -> HummockResult<Arc<Block>> {
        // read checksum length
        let mut read_pos = data.len() - 4;
        let checksum_len = (&data[read_pos..read_pos + 4]).get_u32() as usize;

        if checksum_len > data.len() {
            return Err(HummockError::invalid_block());
        }

        // read checksum
        read_pos -= checksum_len;
        let checksum = Checksum::decode(&data[read_pos..read_pos + checksum_len])?;

        // check raw data
        verify_checksum(&checksum, &data[..read_pos])?;

        // read entries num
        read_pos -= 4;
        let entries_num = (&data[read_pos..read_pos + 4]).get_u32() as usize;

        // read entries position
        let entries_index_start = read_pos - entries_num * 4;
        let entries_index_end = entries_index_start + entries_num * 4;

        // read entries
        let mut entry_offsets_ptr = &data[entries_index_start..entries_index_end];
        let mut entry_offsets = Vec::with_capacity(entries_num);
        for _ in 0..entries_num {
            entry_offsets.push(entry_offsets_ptr.get_u32_le());
        }

        let blk = Arc::new(Block {
            offset: block_offset,
            entries_index_start,
            data: data.clone(),
            entry_offsets,
            checksum_len,
            checksum,
        });

        Ok(blk)
    }

    pub fn encode_meta(meta: &SstableMeta, checksum_algo: ChecksumAlg) -> HummockResult<Bytes> {
        let mut buf = BytesMut::with_capacity(DEFAULT_META_BUFFER_CAPACITY);
        meta.encode(&mut buf).map_err(HummockError::encode_error)?;
        let len = buf.len();
        buf.put_u32_le(0);
        buf.put_u32(len as u32);
        let checksum = checksum(checksum_algo, &buf);
        checksum
            .encode(&mut buf)
            .map_err(HummockError::encode_error)?;
        buf.put_u32(checksum.encoded_len() as u32);
        Ok(buf.freeze())
    }

    pub fn inner_data(&self) -> Bytes {
        self.data.slice(..self.entries_index_start)
    }
}

/// [`Sstable`] is a handle for accessing SST in [`SstableStore`].
pub struct Sstable {
    pub id: u64,
    pub meta: SstableMeta,
}

impl Sstable {
    pub fn new(id: u64, meta: SstableMeta) -> Self {
        Self { id, meta }
    }

    pub fn has_bloom_filter(&self) -> bool {
        !self.meta.bloom_filter.is_empty()
    }

    pub fn surely_not_have_user_key(&self, user_key: &[u8]) -> bool {
        if self.has_bloom_filter() {
            let hash = farmhash::fingerprint32(user_key);
            let bloom = Bloom::new(&self.meta.bloom_filter);
            bloom.surely_not_have_hash(hash)
        } else {
            false
        }
    }

    pub fn block_count(&self) -> usize {
        self.meta.block_metas.len()
    }
}
