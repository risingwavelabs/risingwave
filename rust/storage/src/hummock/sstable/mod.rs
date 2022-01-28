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
use crate::object::{BlockLocation, ObjectStore};

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
}

/// [`Table`] represents a loaded SST file with the info we have about it.
pub struct SSTable {
    /// SST id
    pub id: u64,

    /// Prost-encoded Metadata of SST
    pub meta: SstableMeta,

    /// Client of object store
    pub obj_client: Arc<dyn ObjectStore>,

    // Data path for the data object
    pub data_path: String,
}

impl SSTable {
    /// Open an existing SST from a pre-loaded [`Bytes`].
    pub async fn load(
        id: u64,
        obj_client: Arc<dyn ObjectStore>,
        data_path: String,
        meta: SstableMeta,
    ) -> HummockResult<Self> {
        Ok(SSTable {
            id,
            meta,
            obj_client,
            data_path,
        })
    }

    /// Decode bytes to table metadata instance.
    ///
    /// Metadata format:
    /// ```plain
    /// |       variable      | variable |       4B        |
    /// |  Prost-encoded Meta | Checksum | Checksum Length |
    /// ```
    pub fn decode_meta(content: &[u8]) -> HummockResult<SstableMeta> {
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

        Ok(SstableMeta::decode(data)?)
    }

    pub fn encode_meta(meta: &SstableMeta, buf: &mut BytesMut) {
        // encode index
        let mut raw_meta = BytesMut::new();
        meta.encode(&mut raw_meta).unwrap();
        assert!(raw_meta.len() < u32::MAX as usize);

        // encode checksum and its length
        let checksum = checksum(
            ChecksumAlg::XxHash64, // TODO: may add an option for meta checksum algorithm
            &raw_meta,
        );
        let mut cs_bytes = BytesMut::new();
        checksum.encode(&mut cs_bytes).unwrap();
        let cs_len = cs_bytes.len() as u32;

        buf.put(raw_meta);
        buf.put(cs_bytes);
        buf.put_u32(cs_len);
    }

    /// Get the required block.
    pub async fn block(&self, idx: usize) -> HummockResult<Arc<Block>> {
        let block_meta = &self.meta.block_metas[idx];
        let offset = block_meta.offset as usize;
        let size = block_meta.len as usize;
        let block_loc = BlockLocation { offset, size };

        let block_data = self
            .obj_client
            .read(self.data_path.as_str(), Some(block_loc))
            .await
            .map_err(|e| HummockError::ObjectIoError(e.to_string()))?;

        tracing::trace!(table_id = self.id, block_id = idx, block_loc = ?block_loc, "table read block");

        let block_data = Bytes::from(block_data);

        Block::decode(block_data, offset)
    }

    /// Return true if the table has a Bloom filter
    pub fn has_bloom_filter(&self) -> bool {
        !self.meta.bloom_filter.is_empty()
    }

    /// Judge whether the given user key is in the table with the given false positive rate.
    ///
    /// Note of false positive rate:
    ///   - if the return value is true, then the table surely does not have the user key;
    ///   - if the return value is false, then the table may or may not have the user key actually,
    /// a.k.a. we don't know the answer.
    pub fn surely_not_have_user_key(&self, user_key: &[u8]) -> bool {
        if self.has_bloom_filter() {
            let hash = farmhash::fingerprint32(user_key);
            let bloom = Bloom::new(&self.meta.bloom_filter);
            bloom.surely_not_have_hash(hash)
        } else {
            false
        }
    }

    /// Number of blocks in the current table
    pub fn block_count(&self) -> usize {
        self.meta.block_metas.len()
    }
}

#[cfg(test)]
mod tests {
    use super::builder::tests::*;

    #[tokio::test]
    async fn test_sstable_load() {
        // build remote table
        let table = gen_test_sstable(default_builder_opt_for_test()).await;

        for i in 0..10 {
            table.block(i).await.unwrap();
        }
    }
}
