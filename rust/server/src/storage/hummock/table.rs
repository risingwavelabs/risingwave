//! Hummock state store's SST builder, format and iterator

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod block_iterator;
mod builder;
pub use block_iterator::*;
pub use builder::*;
mod table_iterator;
pub use table_iterator::*;
mod utils;
use utils::verify_checksum;

use std::sync::Arc;

use super::{Bloom, HummockError, HummockResult};
use bytes::{Buf, Bytes};
use prost::Message;
use risingwave_pb::hummock::{Checksum, TableMeta};

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
    fn decode(data: Bytes, block_offset: usize) -> HummockResult<Arc<Block>> {
        // read checksum length
        let mut read_pos = data.len() - 4;
        let checksum_len = (&data[read_pos..read_pos + 4]).get_u32() as usize;

        if checksum_len > data.len() {
            return Err(HummockError::InvalidBlock);
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
}

/// [`Table`] represents a loaded SST file with the info we have about it.
pub struct Table {
    /// concatenated blocks of an SST
    pub blocks: Bytes,

    /// SST id
    pub id: u64,

    /// metadata of SST
    pub meta: TableMeta,
}

impl Table {
    /// Open an existing SST from a pre-loaded [`Bytes`].
    pub fn load(id: u64, blocks: Bytes, meta: TableMeta) -> HummockResult<Self> {
        Ok(Table { id, meta, blocks })
    }

    /// Decode bytes to table metadata instance.
    ///
    /// Metadata format:
    /// ```plain
    /// |       variable      | variable |       4B        |
    /// |  Prost-encoded Meta | Checksum | Checksum Length |
    /// ```
    fn decode_meta(content: &[u8]) -> HummockResult<TableMeta> {
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

        Ok(TableMeta::decode(data)?)
    }

    /// Get the required block.
    /// After reading the metadata, we could know where each block is located, the Bloom filter for
    /// the table, and the first key of each block.
    /// Inside each block, we apply prefix-compression and store key-value pairs.
    /// The block header records the base key of the block. And then, each entry
    /// records the difference to the last key, and the value.
    async fn block(&self, idx: usize) -> HummockResult<Arc<Block>> {
        let block_offset = &self.meta.offsets[idx];

        let offset = block_offset.offset as usize;
        let block_data = self
            .blocks
            .slice(offset..offset + block_offset.len as usize);

        Block::decode(block_data, offset)
    }

    /// Return true if the table has a Bloom filter
    pub fn has_bloom_filter(&self) -> bool {
        !self.meta.bloom_filter.is_empty()
    }

    /// Judge whether the hash is in the table with the given false positive rate.
    /// Note: it means that :
    /// - if the return value is true, then the table surely does not have the value;
    /// - if the return value is false, then the table may or may not have the value actually,
    /// a.k.a. we don't know the answer.
    pub fn surely_not_have(&self, hash: u32) -> bool {
        if self.has_bloom_filter() {
            let bloom = Bloom::new(&self.meta.bloom_filter);
            !bloom.may_contain(hash)
        } else {
            false
        }
    }

    /// Number of blocks in the current table
    pub fn block_count(&self) -> usize {
        self.meta.offsets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::builder::tests::*;
    use super::*;

    #[tokio::test]
    async fn test_table_load() {
        let (blocks, meta) = generate_table(default_builder_opt_for_test());
        let table = Table::load(0, blocks, meta).unwrap();
        for i in 0..10 {
            table.block(i).await.unwrap();
        }
    }
}
