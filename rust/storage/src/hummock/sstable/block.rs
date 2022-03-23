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
use bytes::{Buf, Bytes};
use prost::Message;
use risingwave_pb::hummock::Checksum;

use crate::hummock::sstable::utils::verify_checksum;
use crate::hummock::{Header, HummockError, HummockResult, HEADER_SIZE};

/// [`Block`] represents a memory loaded block from SST.
#[derive(Default)]
pub struct Block {
    /// Entries data only, uncompressed.
    data: Bytes,
    /// Entry indices with a dummy index.
    entry_offsets: Vec<u32>,
    /// Block size in SST, taking compression, checksum, indices into account.
    size: usize,
    /// Base key for prefix compression.
    base_key: Bytes,
}

impl Block {
    /// Decode block from given bytes
    /// Structure of Block:
    /// ```plain
    /// | Entry 0 | ... | Entry N-1 | Entry 0 offset (4B) | ... | Entry N-1 offset (4B) |
    /// | N (4B) | Checksum | Checksum Size (4B) |
    /// ```
    pub fn decode(data: Bytes) -> HummockResult<Block> {
        let size = data.len();

        // verify checksum
        let checksum_len = (&data[size - 4..]).get_u32() as usize;
        let content_len = size - 4 - checksum_len;
        let checksum = Checksum::decode(&data[content_len..content_len + checksum_len])
            .map_err(HummockError::decode_error)?;
        verify_checksum(&checksum, &data[..content_len])?;

        let n_entries = (&data[content_len - 4..content_len]).get_u32() as usize;
        assert!(n_entries > 0);

        // read indices
        let data_len = content_len - 4 - n_entries * 4;
        let mut indices = &data[data_len..content_len - 4];
        let mut entry_offsets = Vec::with_capacity(n_entries + 1);
        for _ in 0..n_entries {
            entry_offsets.push(indices.get_u32_le());
        }
        entry_offsets.push(data_len as u32);

        // base key
        let base_header = Header::decode(&mut &data[..HEADER_SIZE]);
        let base_key = data.slice(HEADER_SIZE..HEADER_SIZE + base_header.diff as usize);

        Ok(Block {
            data: data.slice(..data_len),
            entry_offsets,
            size,
            base_key,
        })
    }

    /// Raw entry data of give entry index.
    /// ```plain
    /// | header | diff key | value |
    /// ```
    pub fn raw_entry(&self, index: usize) -> Bytes {
        assert!(index < self.entry_offsets.len());
        self.data
            .slice(self.entry_offsets[index] as usize..self.entry_offsets[index + 1] as usize)
    }

    /// Block size in SST, taking compression, checksum, indices into account.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Count of entries.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.entry_offsets.len() - 1
    }

    /// Base key for prefix compression.
    pub fn base_key(&self) -> Bytes {
        self.base_key.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::BytesMut;

    use crate::assert_bytes_eq;
    use crate::hummock::key::key_with_epoch;
    use crate::hummock::test_utils::gen_test_sstable_data;
    use crate::hummock::value::HummockValue;
    use crate::hummock::{Block, BlockIterator, SSTableBuilderOptions};

    fn key(index: usize) -> Vec<u8> {
        key_with_epoch(format!("k{:02}", index).into_bytes(), 0)
    }

    fn value(index: usize) -> Vec<u8> {
        format!("v{:02}", index).into_bytes()
    }

    fn put_value_bytes(index: usize) -> Vec<u8> {
        let mut buf = BytesMut::new();
        HummockValue::Put(&value(index)).encode(&mut buf);
        buf.freeze().to_vec()
    }

    #[test]
    fn test_block_enc_dec() {
        let options = SSTableBuilderOptions {
            bloom_false_positive: 0.1,
            block_size: 64,
            table_capacity: 1024,
            checksum_algo: risingwave_pb::hummock::checksum::Algorithm::XxHash64,
        };
        let (data, meta) = gen_test_sstable_data(
            options,
            (0..4).map(|i| (key(i), HummockValue::Put(value(i)))),
        );
        assert_eq!(meta.block_metas.len(), 2);
        let block0 = Block::decode(data.slice(
            meta.block_metas[0].offset as usize
                ..meta.block_metas[0].offset as usize + meta.block_metas[0].len as usize,
        ))
        .unwrap();
        let block1 = Block::decode(data.slice(
            meta.block_metas[1].offset as usize
                ..meta.block_metas[1].offset as usize + meta.block_metas[1].len as usize,
        ))
        .unwrap();
        assert_eq!(block0.len(), 2);
        assert_eq!(block1.len(), 2);

        let mut iter = BlockIterator::new(Arc::new(block0));

        iter.seek_to_first();
        let k0 = iter.key();
        let v0 = iter.value();
        assert_bytes_eq!(k0, key(0));
        assert_bytes_eq!(v0, put_value_bytes(0));

        iter.next();
        let k1 = iter.key();
        let v1 = iter.value();
        assert_bytes_eq!(k1, key(1));
        assert_bytes_eq!(v1, put_value_bytes(1));

        iter.set_block(Arc::new(block1));

        iter.seek_to_first();
        let k2 = iter.key();
        let v2 = iter.value();
        assert_bytes_eq!(k2, key(2));
        assert_bytes_eq!(v2, put_value_bytes(2));

        iter.next();
        let k3 = iter.key();
        let v3 = iter.value();
        assert_bytes_eq!(k3, key(3));
        assert_bytes_eq!(v3, put_value_bytes(3));
    }
}
