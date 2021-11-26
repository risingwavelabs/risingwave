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

use super::{Bloom, HummockError, HummockResult};
use crate::storage::hummock::table::utils::crc32_checksum;
use crate::storage::object::{BlockLocation, ObjectStore};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_pb::hummock::{Checksum, TableMeta};
use std::sync::Arc;

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
    /// SST id
    pub id: u64,

    /// Prost-encoded Metadata of SST
    pub meta: TableMeta,

    /// Client of object store
    pub obj_client: Arc<dyn ObjectStore>,

    // Data path for the data object
    pub data_path: String,
}

impl Table {
    /// Open an existing SST from a pre-loaded [`Bytes`].
    pub async fn load(
        id: u64,
        obj_client: Arc<dyn ObjectStore>,
        data_path: String,
        meta: TableMeta,
    ) -> HummockResult<Self> {
        Ok(Table {
            id,
            obj_client,
            data_path,
            meta,
        })
    }

    /// Decode bytes to table metadata instance.
    ///
    /// Metadata format:
    /// ```plain
    /// |       variable      | variable |       4B        |
    /// |  Prost-encoded Meta | Checksum | Checksum Length |
    /// ```
    pub fn decode_meta(content: &[u8]) -> HummockResult<TableMeta> {
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

    pub fn encode_meta(meta: &TableMeta, buf: &mut BytesMut) {
        // encode index
        let mut raw_meta = BytesMut::new();
        meta.encode(&mut raw_meta).unwrap();
        assert!(raw_meta.len() < u32::MAX as usize);

        // encode checksum and its length
        let checksum = Checksum {
            sum: crc32_checksum(&raw_meta),
            algo: ChecksumAlg::Crc32c as i32,
        };
        let mut cs_bytes = BytesMut::new();
        checksum.encode(&mut cs_bytes).unwrap();
        let cs_len = cs_bytes.len() as u32;

        buf.put(raw_meta);
        buf.put(cs_bytes);
        buf.put_u32(cs_len);
    }

    /// Get the required block.
    pub async fn block(&self, idx: usize) -> HummockResult<Arc<Block>> {
        let block_offset = &self.meta.offsets[idx];
        let offset = block_offset.offset as usize;
        let size = block_offset.len as usize;
        let block_loc = BlockLocation { offset, size };

        let block_data = self
            .obj_client
            .read(self.data_path.as_str(), Some(block_loc))
            .await
            .map_err(|e| HummockError::ObjectIoError(e.to_string()))?;
        let block_data = Bytes::from(block_data);

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

    #[tokio::test]
    async fn test_table_load() {
        // build remote table
        let table = gen_test_table(default_builder_opt_for_test()).await;

        for i in 0..10 {
            table.block(i).await.unwrap();
        }
    }
}
