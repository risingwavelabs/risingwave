//! Hummock state store's SST builder, format and iterator

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod builder;
pub use builder::*;
mod utils;

use super::{HummockError, HummockResult};
use crate::storage::hummock::bloom::Bloom;
use bytes::{Buf, Bytes};
use prost::Message;
use risingwave_pb::hummock::{Checksum, TableIndex};
use utils::verify_checksum;

pub struct Block;

/// [`Table`] stores data of an SST. It is immutable once created and initialized. For now, we
/// assume that all content of SST is preloaded into the memory before creating the [`Table`]
/// object. In the future, we should decide on the I/O interface and block cache implementation, and
/// then implement on-demand block loading.
///
/// The table format has already been explained (in a simple way) in [`TableBuilder`]. Here we
/// explain this in more detail:
///
/// Generally, a table is consisted of two parts: table data and index content. Table data is simply
/// a sequence of blocks. Index is the prost-encoded `TableIndex` data and essential information to
/// determine the length and checksum. Index is currently located at the end of the SST, and I think
/// we can later split one table into two files: table content `.sst` and index content `.idx`.
///
/// ```plain
/// | Block | Block | Block | Block | Block | Index |
/// ```
///
/// Let's look at the index first.
///
/// ```plain
/// |       variable      |      4B      | variable |       4B        |
/// | Prost-encoded Index | Index Length | Checksum | Checksum Length |
/// ```
///
/// The reader will begin reading the table from the tail of the file. First read checksum length,
/// then decode checksum for the index, and then read index length, and finally load the
/// prost-encoded [`TableIndex`].
///
/// After reading the index, we could know where each block is located, the bloom filter for the
/// table, and the first key of each block. Inside each block, we apply prefix-compression and store
/// key-value pairs.
pub struct Table {
    /// content of an SST
    content: Bytes,

    /// SST id
    id: u64,

    /// estimated size, only used on encryption or compression
    estimated_size: u32,

    /// index of SST
    index: TableIndex,

    /// true if there's bloom filter in table
    has_bloom_filter: bool,
}

impl Table {
    /// Open an existing SST from a pre-loaded [`Bytes`].
    fn load(id: u64, content: Bytes, has_bloom_filter: bool) -> HummockResult<Self> {
        Ok(Table {
            id,
            estimated_size: 0,
            index: Self::decode_index(&content[..])?,
            content,
            has_bloom_filter,
        })
    }

    fn decode_index(content: &[u8]) -> HummockResult<TableIndex> {
        let mut read_pos = content.len();

        // read checksum length from last 4 bytes
        read_pos -= 4;
        let mut buf = &content[read_pos..read_pos + 4];
        let checksum_len = buf.get_u32() as usize;

        // read checksum
        read_pos -= checksum_len;
        let buf = &content[read_pos..read_pos + checksum_len];
        let chksum = Checksum::decode(buf)?;

        // read index size from footer
        read_pos -= 4;
        let mut buf = &content[read_pos..read_pos + 4];
        let index_len = buf.get_u32() as usize;

        // read index
        read_pos -= index_len;
        let data = &content[read_pos..read_pos + index_len];
        verify_checksum(&chksum, data)?;

        Ok(TableIndex::decode(data)?)
    }

    fn block(&self, idx: usize) -> HummockResult<Bytes> {
        let block_offset = &self.index.offsets[idx];

        let offset = block_offset.offset as usize;
        let data = &self.content[offset..offset + block_offset.len as usize];

        let mut read_pos = data.len() - 4; // first read checksum length
        let checksum_len = (&data[read_pos..read_pos + 4]).get_u32() as usize;

        if checksum_len > data.len() {
            return Err(HummockError::InvalidBlock);
        }

        // read checksum
        read_pos -= checksum_len;
        let chksum = Checksum::decode(&data[read_pos..read_pos + checksum_len])?;

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

        // The checksum is calculated for actual data + entry index + index length
        let data = self.content.slice(offset..offset + read_pos + 4);

        verify_checksum(&chksum, &data[..])?;

        Ok(data)
    }

    fn index_key(&self) -> u64 {
        self.id
    }

    /// Get number of keys in SST
    pub fn key_count(&self) -> u32 {
        self.index.key_count
    }

    /// Get size of SST
    pub fn size(&self) -> u64 {
        self.content.len() as u64
    }

    /// Get SST id
    pub fn id(&self) -> u64 {
        self.id
    }

    fn fetch_index(&self) -> &TableIndex {
        &self.index
        // TODO: encryption
    }

    /// Judge whether the hash is in the table with the given false positive rate.
    /// Note: it means that :
    /// - if the return value is true, then the table surely does not have the value;
    /// - if the return value is false, then the table may or may not have the value actually,
    /// a.k.a. we don't know the answer.
    pub fn surely_not_have(&self, hash: u32) -> bool {
        if self.has_bloom_filter {
            let index = self.fetch_index();
            let bloom = Bloom::new(&index.bloom_filter);
            !bloom.may_contain(hash)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_table_load() {
        let table = super::builder::tests::generate_table();
        let table = Table::load(0, table, false).unwrap();
        for i in 0..10 {
            table.block(i).unwrap();
        }
    }
}
