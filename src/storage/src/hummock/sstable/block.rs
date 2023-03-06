// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::mem::size_of;
use std::ops::Range;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use risingwave_hummock_sdk::KeyComparator;
use {lz4, zstd};

use super::utils::{bytes_diff_below_max_key_length, xxhash64_verify, CompressionAlgorithm};
use crate::hummock::sstable::utils::xxhash64_checksum;
use crate::hummock::{HummockError, HummockResult};

pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;
pub const DEFAULT_RESTART_INTERVAL: usize = 16;
pub const DEFAULT_ENTRY_SIZE: usize = 24; // table_id(u64) + primary_key(u64) + epoch(u64)

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum LenType {
    U8 = 1,
    U16 = 2,
    U32 = 3,
    U64 = 4,
}

impl From<u8> for LenType {
    fn from(value: u8) -> Self {
        match value {
            1 => LenType::U8,
            2 => LenType::U16,
            3 => LenType::U32,
            4 => LenType::U64,
            _ => {
                println!("error LenType {}", value);
                panic!("unexpected")
            }
        }
    }
}

impl LenType {
    fn new(len: usize) -> Self {
        if len <= u8::MAX as usize {
            LenType::U8
        } else if len <= u16::MAX as usize {
            LenType::U16
        } else if len <= u32::MAX as usize {
            LenType::U32
        } else {
            LenType::U64
        }
    }

    fn put(&self, buf: &mut impl BufMut, value: usize) {
        match *self {
            Self::U8 => {
                buf.put_u8(value as u8);
            }

            Self::U16 => {
                buf.put_u16(value as u16);
            }

            Self::U32 => {
                buf.put_u32(value as u32);
            }

            Self::U64 => {
                buf.put_u64(value as u64);
            }
        }
    }

    fn get(&self, buf: &mut impl Buf) -> usize {
        match *self {
            Self::U8 => buf.get_u8() as usize,

            Self::U16 => buf.get_u16() as usize,

            Self::U32 => buf.get_u32() as usize,

            Self::U64 => buf.get_u64() as usize,
        }
    }

    fn len(&self) -> usize {
        match *self {
            Self::U8 => size_of::<u8>(),

            Self::U16 => size_of::<u16>(),

            Self::U32 => size_of::<u32>(),

            Self::U64 => size_of::<u64>(),
        }
    }
}

#[derive(Clone)]
pub struct Block {
    /// Uncompressed entries data, with restart encoded restart points info.
    data: Bytes,
    /// Uncompressed entried data length.
    data_len: usize,
    /// Restart points.
    restart_points: Vec<u32>,

    restart_points_type_index: BTreeMap<u32, (LenType, LenType)>,
}

impl Block {
    pub fn decode(buf: Bytes, uncompressed_capacity: usize) -> HummockResult<Self> {
        // Verify checksum.
        let xxhash64_checksum = (&buf[buf.len() - 8..]).get_u64_le();
        xxhash64_verify(&buf[..buf.len() - 8], xxhash64_checksum)?;

        // Decompress.
        let compression = CompressionAlgorithm::decode(&mut &buf[buf.len() - 9..buf.len() - 8])?;
        let compressed_data = &buf[..buf.len() - 9];
        let buf = match compression {
            CompressionAlgorithm::None => buf.slice(0..(buf.len() - 9)),
            CompressionAlgorithm::Lz4 => {
                let mut decoder = lz4::Decoder::new(compressed_data.reader())
                    .map_err(HummockError::decode_error)?;
                let mut decoded = Vec::with_capacity(uncompressed_capacity);
                decoder
                    .read_to_end(&mut decoded)
                    .map_err(HummockError::decode_error)?;
                debug_assert_eq!(decoded.capacity(), uncompressed_capacity);
                Bytes::from(decoded)
            }
            CompressionAlgorithm::Zstd => {
                let mut decoder = zstd::Decoder::new(compressed_data.reader())
                    .map_err(HummockError::decode_error)?;
                let mut decoded = Vec::with_capacity(uncompressed_capacity);
                decoder
                    .read_to_end(&mut decoded)
                    .map_err(HummockError::decode_error)?;
                debug_assert_eq!(decoded.capacity(), uncompressed_capacity);
                Bytes::from(decoded)
            }
        };

        Ok(Self::decode_from_raw(buf))
    }

    pub fn decode_from_raw(buf: Bytes) -> Self {
        // decode restart_points_type_index
        let n_index = (&buf[buf.len() - 4..]).get_u32_le();
        let index_data_len =
            size_of::<u32>() + n_index as usize * (size_of::<u32>() + size_of::<u8>());
        let data_len = buf.len() - index_data_len;
        let mut restart_points_type_index_buf = &buf[data_len..buf.len() - 4];

        let mut key_vec = Vec::with_capacity(n_index as usize);
        let mut value_vec = Vec::with_capacity(n_index as usize);
        for _ in 0..n_index {
            let offset = restart_points_type_index_buf.get_u32_le();
            key_vec.push(offset);
        }

        for _ in 0..n_index {
            let value = restart_points_type_index_buf.get_u8();
            let v_type = LenType::from(value & 0x0F);
            let k_type = LenType::from(value >> 4);
            value_vec.push((k_type, v_type));
        }

        let restart_points_type_index: BTreeMap<_, _> = key_vec
            .into_iter()
            .zip(value_vec.into_iter())
            .map(|(k, v)| (k, v))
            .collect();

        // Decode restart points.
        let n_restarts = (&buf[data_len - 4..]).get_u32_le();
        let restart_points_len = size_of::<u32>() + n_restarts as usize * (size_of::<u32>());
        let restarts_end = data_len - 4;
        let data_len = data_len - restart_points_len;
        let mut restart_points = Vec::with_capacity(n_restarts as usize);
        let mut restart_points_buf = &buf[data_len..restarts_end];

        for _ in 0..n_restarts {
            let offset = restart_points_buf.get_u32_le();
            restart_points.push(offset);
        }

        Block {
            data: buf,
            data_len,
            restart_points,
            restart_points_type_index,
        }
    }

    /// Entries data len.
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        assert!(!self.data.is_empty());
        self.data_len
    }

    pub fn capacity(&self) -> usize {
        self.data.len() + self.restart_points.capacity() * std::mem::size_of::<u32>()
    }

    /// Gets restart point by index.
    pub fn restart_point(&self, index: usize) -> u32 {
        self.restart_points[index]
    }

    /// Gets restart point len.
    pub fn restart_point_len(&self) -> usize {
        self.restart_points.len()
    }

    /// Searches the index of the restart point by partition point.
    pub fn search_restart_partition_point<P>(&self, pred: P) -> usize
    where
        P: FnMut(&u32) -> bool,
    {
        self.restart_points.partition_point(pred)
    }

    pub fn data(&self) -> &[u8] {
        &self.data[..self.data_len]
    }

    pub fn raw_data(&self) -> &[u8] {
        &self.data[..]
    }

    pub fn restart_points_type_index(&self, index: u32) -> (LenType, LenType) {
        *self.restart_points_type_index.get(&index).unwrap()
    }
}

/// [`KeyPrefix`] contains info for prefix compression.
#[derive(Debug)]
pub struct KeyPrefix {
    overlap: usize,
    diff: usize,
    value: usize,
    /// Used for calculating range, won't be encoded.
    offset: usize,

    key_len_type: LenType,
    value_len_type: LenType,
}

impl KeyPrefix {
    pub fn encode(&self, buf: &mut impl BufMut, key_len_type: LenType, value_len_type: LenType) {
        key_len_type.put(buf, self.overlap);
        key_len_type.put(buf, self.diff);
        value_len_type.put(buf, self.value);
    }

    pub fn decode(
        buf: &mut impl Buf,
        offset: usize,
        key_len_type: LenType,
        value_len_type: LenType,
    ) -> Self {
        let overlap = key_len_type.get(buf);
        let diff = key_len_type.get(buf);
        let value = value_len_type.get(buf);

        Self {
            overlap,
            diff,
            value,
            offset,
            key_len_type,
            value_len_type,
        }
    }

    /// Encoded length.
    fn len(&self) -> usize {
        // if self.diff >= MAX_KEY_LEN {
        //     12 // 2 + 2 + 4 + 4
        // } else {
        //     8 // 2 + 2 + 4
        // }

        self.key_len_type.len() * 2 + self.value_len_type.len()
    }

    /// Gets overlap len.
    pub fn overlap_len(&self) -> usize {
        self.overlap
    }

    /// Gets diff key range.
    pub fn diff_key_range(&self) -> Range<usize> {
        self.offset + self.len()..self.offset + self.len() + self.diff
    }

    /// Gets value range.
    pub fn value_range(&self) -> Range<usize> {
        self.offset + self.len() + self.diff..self.offset + self.len() + self.diff + self.value
    }

    /// Gets entry len.
    pub fn entry_len(&self) -> usize {
        self.len() + self.diff + self.value
    }
}

pub struct BlockBuilderOptions {
    /// Reserved bytes size when creating buffer to avoid frequent allocating.
    pub capacity: usize,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorithm,
    /// Restart point interval.
    pub restart_interval: usize,
}

impl Default for BlockBuilderOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_BLOCK_SIZE,
            compression_algorithm: CompressionAlgorithm::None,
            restart_interval: DEFAULT_RESTART_INTERVAL,
        }
    }
}

/// [`BlockBuilder`] encodes and appends block to a buffer.
pub struct BlockBuilder {
    /// Write buffer.
    buf: BytesMut,
    /// Entry interval between restart points.
    restart_count: usize,
    /// Restart points.
    restart_points: Vec<u32>,
    /// Last key.
    last_key: Vec<u8>,
    /// Count of entries in current block.
    entry_count: usize,
    /// Compression algorithm.
    compression_algorithm: CompressionAlgorithm,

    // restart_points_type_index: BTreeMap<u32, u8>,
    restart_points_type_index: BTreeMap<u32, (LenType, LenType)>,
}

impl BlockBuilder {
    pub fn new(options: BlockBuilderOptions) -> Self {
        Self {
            // add more space to avoid re-allocate space.
            buf: BytesMut::with_capacity(options.capacity + 256),
            restart_count: options.restart_interval,
            restart_points: Vec::with_capacity(
                options.capacity / DEFAULT_ENTRY_SIZE / options.restart_interval + 1,
            ),
            last_key: vec![],
            entry_count: 0,
            compression_algorithm: options.compression_algorithm,
            restart_points_type_index: BTreeMap::default(),
        }
    }

    /// Appends a kv pair to the block.
    ///
    /// NOTE: Key must be added in ASCEND order.
    ///
    /// # Format
    ///
    /// ```plain
    /// For diff len < MAX_KEY_LEN (65536)
    ///    entry (kv pair): | overlap len (2B) | diff len (2B) | value len(4B) | diff key | value |
    /// For diff len >= MAX_KEY_LEN (65536)
    ///    entry (kv pair): | overlap len (2B) | MAX_KEY_LEN (2B) | diff len (4B) | value len(4B) | diff key | value |
    /// ```
    ///
    /// # Panics
    ///
    /// Panic if key is not added in ASCEND order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.entry_count > 0 {
            debug_assert!(!key.is_empty());
            debug_assert_eq!(
                KeyComparator::compare_encoded_full_key(&self.last_key[..], key),
                Ordering::Less
            );
        }
        // Update restart point if needed and calculate diff key.
        let key_len_type = LenType::new(key.len());
        let value_len_type = LenType::new(value.len());

        let type_mismatch =
            if let Some((_, last_type)) = self.restart_points_type_index.last_key_value() {
                if key_len_type != last_type.0 || value_len_type != last_type.1 {
                    true
                } else {
                    false
                }
            } else {
                false
            };

        let diff_key = if self.entry_count % self.restart_count == 0 || type_mismatch {
            let offset = self.buf.len() as u32;

            self.restart_points.push(offset);
            self.restart_points_type_index
                .insert(offset, (key_len_type, value_len_type));

            key
        } else {
            bytes_diff_below_max_key_length(&self.last_key, key)
        };

        let prefix = KeyPrefix {
            overlap: key.len() - diff_key.len(),
            diff: diff_key.len(),
            value: value.len(),
            offset: self.buf.len(),
            key_len_type,
            value_len_type,
        };

        prefix.encode(&mut self.buf, key_len_type, value_len_type);
        self.buf.put_slice(diff_key);
        self.buf.put_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entry_count += 1;
    }

    pub fn get_last_key(&self) -> &[u8] {
        &self.last_key
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn clear(&mut self) {
        self.buf.clear();
        self.restart_points.clear();
        self.last_key.clear();
        self.entry_count = 0;
    }

    /// Calculate block size without compression.
    pub fn uncompressed_block_size(&mut self) -> usize {
        self.buf.len()
            + (self.restart_points.len() + 1) * std::mem::size_of::<u32>()
            + (4 + 1) * self.restart_points_type_index.len()
            + 4
    }

    /// Finishes building block.
    ///
    /// # Format
    ///
    /// ```plain
    /// compressed: | entries | restart point 0 (4B) | ... | restart point N-1 (4B) | N (4B) |
    /// uncompressed: | compression method (1B) | crc32sum (4B) |
    /// ```
    ///
    /// # Panics
    ///
    /// Panic if there is compression error.
    pub fn build(&mut self) -> &[u8] {
        assert!(self.entry_count > 0);

        for restart_point in &self.restart_points {
            self.buf.put_u32_le(*restart_point);
        }

        self.buf.put_u32_le(self.restart_points.len() as u32);
        for restart_point in self.restart_points_type_index.keys() {
            self.buf.put_u32_le(*restart_point);
        }

        for (k_type, v_type) in self.restart_points_type_index.values() {
            let mut value: u8 = 0;
            value |= *k_type as u8;
            value <<= 4;
            value |= *v_type as u8;
            self.buf.put_u8(value);
        }

        self.buf
            .put_u32_le(self.restart_points_type_index.len() as u32);

        match self.compression_algorithm {
            CompressionAlgorithm::None => (),
            CompressionAlgorithm::Lz4 => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .level(4)
                    .build(BytesMut::with_capacity(self.buf.len()).writer())
                    .map_err(HummockError::encode_error)
                    .unwrap();
                encoder
                    .write_all(&self.buf[..])
                    .map_err(HummockError::encode_error)
                    .unwrap();
                let (writer, result) = encoder.finish();
                result.map_err(HummockError::encode_error).unwrap();
                self.buf = writer.into_inner();
            }
            CompressionAlgorithm::Zstd => {
                let mut encoder =
                    zstd::Encoder::new(BytesMut::with_capacity(self.buf.len()).writer(), 4)
                        .map_err(HummockError::encode_error)
                        .unwrap();
                encoder
                    .write_all(&self.buf[..])
                    .map_err(HummockError::encode_error)
                    .unwrap();
                let writer = encoder
                    .finish()
                    .map_err(HummockError::encode_error)
                    .unwrap();
                self.buf = writer.into_inner();
            }
        };
        self.compression_algorithm.encode(&mut self.buf);
        let checksum = xxhash64_checksum(&self.buf);
        self.buf.put_u64_le(checksum);
        self.buf.as_ref()
    }

    /// Approximate block len (uncompressed).
    pub fn approximate_len(&self) -> usize {
        // block + restart_points + restart_points.len + compression_algorithm + checksum
        self.buf.len()
            + 4 * self.restart_points.len()
            + 4
            + 1
            + 8
            + (4 + 2) * self.restart_points_type_index.len()
    }

    // fn get_len_type(len: usize) -> LenType {
    //     if len <= u8::MAX as usize {
    //         LenType::U8
    //     } else if len <= u16::MAX as usize {
    //         LenType::U16
    //     } else if len <= u32::MAX as usize {
    //         LenType::U32
    //     } else {
    //         LenType::U64
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use risingwave_hummock_sdk::key::MAX_KEY_LEN;

    use super::*;
    use crate::hummock::{BlockHolder, BlockIterator};

    #[test]
    fn test_block_enc_dec() {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        builder.add(&full_key(b"k1", 1), b"v01");
        builder.add(&full_key(b"k2", 2), b"v02");
        builder.add(&full_key(b"k3", 3), b"v03");
        builder.add(&full_key(b"k4", 4), b"v04");
        let capacity = builder.uncompressed_block_size();
        let buf = builder.build().to_vec();
        let block = Box::new(Block::decode(buf.into(), capacity).unwrap());
        let mut bi = BlockIterator::new(BlockHolder::from_owned_block(block));

        bi.seek_to_first();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k1", 1)[..], bi.key());
        assert_eq!(b"v01", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k2", 2)[..], bi.key());
        assert_eq!(b"v02", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k3", 3)[..], bi.key());
        assert_eq!(b"v03", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k4", 4)[..], bi.key());
        assert_eq!(b"v04", bi.value());

        bi.next();
        assert!(!bi.is_valid());
    }

    #[test]
    fn test_compressed_block_enc_dec() {
        inner_test_compressed(CompressionAlgorithm::Lz4);
        inner_test_compressed(CompressionAlgorithm::Zstd);
    }

    fn inner_test_compressed(algo: CompressionAlgorithm) {
        let options = BlockBuilderOptions {
            compression_algorithm: algo,
            ..Default::default()
        };
        let mut builder = BlockBuilder::new(options);
        builder.add(&full_key(b"k1", 1), b"v01");
        builder.add(&full_key(b"k2", 2), b"v02");
        builder.add(&full_key(b"k3", 3), b"v03");
        builder.add(&full_key(b"k4", 4), b"v04");
        let capcitiy = builder.uncompressed_block_size();
        let buf = builder.build().to_vec();
        let block = Box::new(Block::decode(buf.into(), capcitiy).unwrap());
        let mut bi = BlockIterator::new(BlockHolder::from_owned_block(block));

        bi.seek_to_first();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k1", 1)[..], bi.key());
        assert_eq!(b"v01", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k2", 2)[..], bi.key());
        assert_eq!(b"v02", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k3", 3)[..], bi.key());
        assert_eq!(b"v03", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(b"k4", 4)[..], bi.key());
        assert_eq!(b"v04", bi.value());

        bi.next();
        assert!(!bi.is_valid());
    }

    pub fn full_key(user_key: &[u8], epoch: u64) -> Bytes {
        let mut buf = BytesMut::with_capacity(user_key.len() + 8);
        buf.put_slice(user_key);
        buf.put_u64(!epoch);
        buf.freeze()
    }

    #[test]
    fn test_block_enc_large_key() {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        let medium_key = vec![b'a'; MAX_KEY_LEN - 500];
        let large_key = vec![b'b'; MAX_KEY_LEN];
        let xlarge_key = vec![b'c'; MAX_KEY_LEN + 500];

        builder.add(&full_key(&medium_key, 1), b"v1");
        builder.add(&full_key(&large_key, 2), b"v2");
        builder.add(&full_key(&xlarge_key, 3), b"v3");
        let capacity = builder.uncompressed_block_size();
        let buf = builder.build().to_vec();
        let block = Box::new(Block::decode(buf.into(), capacity).unwrap());
        let mut bi = BlockIterator::new(BlockHolder::from_owned_block(block));

        bi.seek_to_first();
        assert!(bi.is_valid());
        assert_eq!(&full_key(&medium_key, 1)[..], bi.key());
        assert_eq!(b"v1", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(&large_key, 2)[..], bi.key());
        assert_eq!(b"v2", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(&full_key(&xlarge_key, 3)[..], bi.key());
        assert_eq!(b"v3", bi.value());

        bi.next();
        assert!(!bi.is_valid());
    }

    #[test]
    fn test_block_restart_point() {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        for index in 0..100 {
            if index < 50 {
                let mut medium_key = vec![b'A'; MAX_KEY_LEN - 500];
                medium_key.push(index);
                builder.add(&full_key(&medium_key, 1), b"v1");
            } else if index < 80 {
                let mut large_key = vec![b'B'; MAX_KEY_LEN];
                large_key.push(index);
                builder.add(&full_key(&large_key, 2), b"v2");
            } else {
                let mut xlarge_key = vec![b'C'; MAX_KEY_LEN + 500];
                xlarge_key.push(index);
                builder.add(&full_key(&xlarge_key, 3), b"v3");
            }
        }

        let capacity = builder.uncompressed_block_size();
        let buf = builder.build().to_vec();
        let block = Box::new(Block::decode(buf.into(), capacity).unwrap());
        let mut bi = BlockIterator::new(BlockHolder::from_owned_block(block));
        bi.seek_to_first();
        assert!(bi.is_valid());

        for index in 0..100 {
            if index < 50 {
                let mut medium_key = vec![b'A'; MAX_KEY_LEN - 500];
                medium_key.push(index);
                assert_eq!(&full_key(&medium_key, 1)[..], bi.key());
            } else if index < 80 {
                let mut large_key = vec![b'B'; MAX_KEY_LEN];
                large_key.push(index);
                assert_eq!(&full_key(&large_key, 2)[..], bi.key());
            } else {
                let mut xlarge_key = vec![b'C'; MAX_KEY_LEN + 500];
                xlarge_key.push(index);
                assert_eq!(&full_key(&xlarge_key, 3)[..], bi.key());
            }
            bi.next();
            assert!(bi.is_valid());
        }

        bi.next();
        assert!(!bi.is_valid());
    }
}
