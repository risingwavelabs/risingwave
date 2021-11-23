//! Hummock state store's SST builder, format and iterator

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod builder;
pub mod iterator;
use std::sync::Arc;

pub use builder::*;
mod utils;

use super::{HummockError, HummockResult};
use crate::storage::hummock::bloom::Bloom;
use bytes::{Buf, Bytes};
use prost::Message;
use risingwave_pb::hummock::{Checksum, TableIndex};
use utils::verify_checksum;

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
}

/// [`Table`] stores data of an SST. It is immutable once created and initialized. For now, we
/// assume that all content of SST is preloaded into the memory before creating the [`Table`]
/// object. In the future, we should decide on the I/O interface and block cache implementation, and
/// then implement on-demand block loading.
///
/// The table format has already been explained (in a simple way) in [`TableBuilder`]. Here we
/// explain this in more detail:
///
/// Generally, a table is consisted of two parts:
/// - Table data is simply a sequence of blocks.
/// - Metadata is the prost-encoded `TableIndex` data and essential information to determine the
/// checksum.
///
/// ```plain
/// Table data format:
/// | Block | Block | Block | Block | Block |
/// ```
///
/// ```plain
/// Metadata format
/// |       variable      | variable |       4B        |
/// | Prost-encoded Index | Checksum | Checksum Length |
/// ```
///
/// The reader will begin reading the table from the tail of the file. First read checksum length,
/// then decode checksum for the index, and finally load the prost-encoded [`TableIndex`].
///
/// After reading the index, we could know where each block is located, the bloom filter for the
/// table, and the first key of each block. Inside each block, we apply prefix-compression and store
/// key-value pairs.
pub struct Table {
    /// concatenated blocks of an SST
    blocks: Bytes,

    /// SST id
    id: u64,

    /// estimated size, only used on encryption or compression
    estimated_size: u32,

    /// metadata of SST
    meta: TableIndex,

    /// true if there's bloom filter in table
    has_bloom_filter: bool,
}

impl Table {
    /// Open an existing SST from a pre-loaded [`Bytes`].
    pub fn load(id: u64, blocks: Bytes, meta: Bytes) -> HummockResult<Self> {
        let meta = Self::decode_meta(&meta[..])?;
        let has_bloom_filter = !meta.bloom_filter.is_empty();
        let estimated_size = meta.estimated_size;
        Ok(Table {
            id,
            estimated_size,
            meta,
            blocks,
            has_bloom_filter,
        })
    }

    fn decode_meta(content: &[u8]) -> HummockResult<TableIndex> {
        let mut read_pos = content.len();

        // read checksum length from last 4 bytes
        read_pos -= 4;
        let mut buf = &content[read_pos..read_pos + 4];
        let checksum_len = buf.get_u32() as usize;

        // read checksum
        read_pos -= checksum_len;
        let buf = &content[read_pos..read_pos + checksum_len];
        let checksum = Checksum::decode(buf)?;

        // read data
        let data = &content[0..read_pos];
        verify_checksum(&checksum, data)?;

        Ok(TableIndex::decode(data)?)
    }

    fn block(&self, idx: usize) -> HummockResult<Arc<Block>> {
        let block_offset = &self.meta.offsets[idx];

        let offset = block_offset.offset as usize;
        let data = &self.blocks[offset..offset + block_offset.len as usize];

        let mut read_pos = data.len() - 4; // first read checksum length
        let checksum_len = (&data[read_pos..read_pos + 4]).get_u32() as usize;

        if checksum_len > data.len() {
            return Err(HummockError::InvalidBlock);
        }

        // read checksum
        read_pos -= checksum_len;
        let checksum = Checksum::decode(&data[read_pos..read_pos + checksum_len])?;

        // read num entries
        read_pos -= 4;
        let num_entries = (&data[read_pos..read_pos + 4]).get_u32() as usize;

        let entries_index_start = read_pos - num_entries * 4;
        let entries_index_end = entries_index_start + num_entries * 4;

        let mut entry_offsets_ptr = &data[entries_index_start..entries_index_end];
        let mut entry_offsets = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            entry_offsets.push(entry_offsets_ptr.get_u32_le());
        }

        // The checksum is calculated for the blocks
        let data = self.blocks.slice(offset..offset + read_pos + 4);

        let blk = Arc::new(Block {
            offset,
            entries_index_start,
            data,
            entry_offsets,
            checksum_len,
            checksum,
        });

        // verify_checksum(&checksum, &data[..])?;

        Ok(blk)
    }

    fn meta_key(&self) -> u64 {
        self.id
    }

    /// Get number of keys in SST
    pub fn key_count(&self) -> u32 {
        self.meta.key_count
    }

    /// Get size of SST
    pub fn size(&self) -> u64 {
        self.blocks.len() as u64
    }

    /// Get SST id
    pub fn id(&self) -> u64 {
        self.id
    }

    fn fetch_meta(&self) -> &TableIndex {
        &self.meta
        // TODO: encryption
    }

    /// Judge whether the hash is in the table with the given false positive rate.
    /// Note: it means that :
    /// - if the return value is true, then the table surely does not have the value;
    /// - if the return value is false, then the table may or may not have the value actually,
    /// a.k.a. we don't know the answer.
    pub fn surely_not_have(&self, hash: u32) -> bool {
        if self.has_bloom_filter {
            let meta = self.fetch_meta();
            let bloom = Bloom::new(&meta.bloom_filter);
            !bloom.may_contain(hash)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_table_load() {
        let (blocks, meta) = super::builder::tests::generate_table();
        let table = Table::load(0, blocks, meta).unwrap();
        for i in 0..10 {
            table.block(i).unwrap();
        }
    }
}
