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
    U8U8 = 1,
    U8U16 = 2,
    U8U32 = 3,
    U16U8 = 4,
    U16U16 = 5,
    U16U32 = 6,
    U32U8 = 7,
    U32U16 = 8,
    U32U32 = 9,
}

impl From<u8> for LenType {
    fn from(value: u8) -> Self {
        match value {
            1 => LenType::U8U8,
            2 => LenType::U8U16,
            3 => LenType::U8U32,
            4 => LenType::U16U8,
            5 => LenType::U16U16,
            6 => LenType::U16U32,
            7 => LenType::U32U8,
            8 => LenType::U32U16,
            9 => LenType::U32U32,

            _ => {
                panic!("unexpected")
            }
        }
    }
}

macro_rules! put_fn {
    ($self_:ident, $buf:ident, $overlap:ident, $diff:ident, $value:ident, $(($ty1:ident => ($put_fn:ident, $as_type: ident, $put_fn2:ident, $as_type2: ident))),*) => {
        match $self_ {
            $(
                LenType::$ty1 => {
                    $buf.$put_fn($overlap as $as_type);
                    $buf.$put_fn($diff as $as_type);
                    $buf.$put_fn2($value as $as_type2);
                }
            )*
        }
    };
}

macro_rules! get_fn {
    ($self_:ident, $buf:ident, $(($ty1:ident => ($get_fn:ident, $get_fn2:ident))),*) => {
        match $self_ {
            $(
                LenType::$ty1 => {
                    (
                    $buf.$get_fn() as usize,
                    $buf.$get_fn() as usize,
                    $buf.$get_fn2() as usize,
                    )
                }
            )*
        }
    };
}

impl LenType {
    fn new(klen: usize, vlen: usize) -> Self {
        const U8_MAX: usize = u8::MAX as usize;
        const U16_MAX: usize = u16::MAX as usize;
        const U32_MAX: usize = u32::MAX as usize;

        match (klen, vlen) {
            (0..=U8_MAX, 0..=U8_MAX) => LenType::U8U8,
            (0..=U8_MAX, 0..=U16_MAX) => LenType::U8U16,
            (0..=U8_MAX, 0..=U32_MAX) => LenType::U8U32,
            (0..=U16_MAX, 0..=U8_MAX) => LenType::U16U8,
            (0..=U16_MAX, 0..=U16_MAX) => LenType::U16U16,
            (0..=U16_MAX, 0..=U32_MAX) => LenType::U16U32,
            (0..=U32_MAX, 0..=U8_MAX) => LenType::U32U8,
            (0..=U32_MAX, 0..=U16_MAX) => LenType::U32U16,
            (0..=U32_MAX, 0..=U32_MAX) => LenType::U32U32,

            _ => unreachable!(),
        }
    }

    fn len(&self) -> (usize, usize) {
        match *self {
            Self::U8U8 => (size_of::<u8>(), size_of::<u8>()),
            Self::U8U16 => (size_of::<u8>(), size_of::<u16>()),
            Self::U8U32 => (size_of::<u8>(), size_of::<u32>()),

            Self::U16U8 => (size_of::<u16>(), size_of::<u8>()),
            Self::U16U16 => (size_of::<u16>(), size_of::<u16>()),
            Self::U16U32 => (size_of::<u16>(), size_of::<u32>()),

            Self::U32U8 => (size_of::<u32>(), size_of::<u8>()),
            Self::U32U16 => (size_of::<u32>(), size_of::<u16>()),
            Self::U32U32 => (size_of::<u32>(), size_of::<u32>()),
        }
    }
}

pub trait BufMutExt: BufMut {
    fn put_key_prefix(&mut self, v1: usize, v2: usize, v3: usize, encoder: LenType) {
        put_fn!(
            encoder,
            self, v1, v2, v3,
            (U8U8 => (put_u8, u8, put_u8, u8)),
            (U8U16 => (put_u8, u8, put_u16, u16)),
            (U8U32 => (put_u8, u8, put_u32, u32)),
            (U16U8 => (put_u16, u16, put_u8, u8)),
            (U16U16 => (put_u16, u16, put_u16, u16)),
            (U16U32 => (put_u16, u16, put_u32, u32)),
            (U32U8 => (put_u32, u32, put_u8, u8)),
            (U32U16 => (put_u32, u32, put_u16, u16)),
            (U32U32 => (put_u32, u32, put_u32, u32))
        );
    }
}

pub trait BufExt: Buf {
    fn get_key_prefix(&mut self, decoder: LenType) -> (usize, usize, usize) {
        get_fn!(
            decoder, self,
            (U8U8 => (get_u8, get_u8)),
            (U8U16 => (get_u8, get_u16)),
            (U8U32 => (get_u8, get_u32)),
            (U16U8 => (get_u16, get_u8)),
            (U16U16 => (get_u16, get_u16)),
            (U16U32 => (get_u16, get_u32)),
            (U32U8 => (get_u32, get_u8)),
            (U32U16 => (get_u32, get_u16)),
            (U32U32 => (get_u32, get_u32))
        )
    }
}

impl<T: BufMut + ?Sized> BufMutExt for &mut T {}

impl<T: Buf + ?Sized> BufExt for &mut T {}

#[derive(Clone)]
pub struct Block {
    /// Uncompressed entries data, with restart encoded restart points info.
    pub data: Bytes,
    /// Uncompressed entried data length.
    data_len: usize,
    /// Restart points. (offset, k-v-type)
    restart_points: Vec<(u32, LenType)>,
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
        let n_index = ((&buf[buf.len() - 4..]).get_u32_le()) as usize;
        let index_data_len = size_of::<u32>() + n_index * (size_of::<u32>() + size_of::<u8>());
        let data_len = buf.len() - index_data_len;
        let mut restart_points_type_index_buf = &buf[data_len..buf.len() - 4];

        let mut index_key_vec = Vec::with_capacity(n_index);
        // let mut index_value_vec = Vec::with_capacity(n_index);
        for _ in 0..n_index {
            let offset = restart_points_type_index_buf.get_u32_le();
            let value = restart_points_type_index_buf.get_u8();

            index_key_vec.push((offset, LenType::from(value)));
        }

        // Decode restart points.
        let n_restarts = ((&buf[data_len - 4..]).get_u32_le()) as usize;
        let restart_points_len = size_of::<u32>() + n_restarts * (size_of::<u32>());
        let restarts_end = data_len - 4;
        let data_len = data_len - restart_points_len;
        let mut restart_points = Vec::with_capacity(n_restarts);
        let mut restart_points_buf = &buf[data_len..restarts_end];

        let mut type_index: usize = 0;
        for _ in 0..n_restarts {
            let offset = restart_points_buf.get_u32_le();
            if type_index < index_key_vec.len() - 1 && offset >= index_key_vec[type_index + 1].0 {
                type_index += 1;
            }

            restart_points.push((offset, index_key_vec[type_index].1));
        }

        Block {
            data: buf,
            data_len,
            restart_points,
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
        self.restart_points[index].0
    }

    /// Gets restart point len.
    pub fn restart_point_len(&self) -> usize {
        self.restart_points.len()
    }

    /// Searches the index of the restart point by partition point.
    pub fn search_restart_partition_point<P>(&self, pred: P) -> usize
    where
        P: FnMut(&(u32, LenType)) -> bool,
    {
        self.restart_points.partition_point(pred)
    }

    pub fn data(&self) -> &[u8] {
        &self.data[..self.data_len]
    }

    pub fn raw_data(&self) -> &[u8] {
        &self.data[..]
    }

    pub fn restart_points_type_index(&self, index: usize) -> LenType {
        self.restart_points[index].1
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

    len: usize,
}

impl KeyPrefix {
    // This function is used in BlockBuilder::add to provide a wrapper for encode since the
    // KeyPrefix len field is only useful in the decode phase
    pub fn new_without_len(overlap: usize, diff: usize, value: usize, offset: usize) -> Self {
        KeyPrefix {
            overlap,
            diff,
            value,
            offset,
            len: 0, // not used when encode
        }
    }
}

impl KeyPrefix {
    pub fn encode(&self, mut buf: &mut impl BufMut, encoder: LenType) {
        buf.put_key_prefix(self.overlap, self.diff, self.value, encoder)
    }

    pub fn decode(mut buf: &mut impl Buf, offset: usize, decoder: LenType) -> Self {
        let (overlap, diff, value) = buf.get_key_prefix(decoder);
        let (k_len, v_len) = decoder.len();
        let len = k_len * 2 + v_len;

        Self {
            overlap,
            diff,
            value,
            offset,
            len,
        }
    }

    /// Encoded length.
    fn len(&self) -> usize {
        self.len
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

    // (offset, LenType)
    restart_points_type_index: Vec<(u32, LenType)>,
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
            restart_points_type_index: Vec::default(),
        }
    }

    /// Appends a kv pair to the block.
    ///
    /// NOTE: Key must be added in ASCEND order.
    ///
    /// # Format
    ///
    /// ```plain
    /// entry (kv pair): | overlap len (len_type) | diff len (len_type) | value len(len_type) | diff key | value |
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
        let len_type = LenType::new(key.len(), value.len());

        let type_mismatch = if let Some((_, last_type)) = self.restart_points_type_index.last() {
            len_type != *last_type
        } else {
            false
        };

        let diff_key = if self.entry_count % self.restart_count == 0 || type_mismatch {
            let offset = self.buf.len() as u32;

            self.restart_points.push(offset);

            if type_mismatch || self.restart_points_type_index.is_empty() {
                self.restart_points_type_index.push((offset, len_type));
            }

            key
        } else {
            bytes_diff_below_max_key_length(&self.last_key, key)
        };

        let prefix = KeyPrefix::new_without_len(
            key.len() - diff_key.len(),
            diff_key.len(),
            value.len(),
            self.buf.len(),
        );

        prefix.encode(&mut self.buf, len_type);
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
            + (std::mem::size_of::<u32>() + std::mem::size_of::<LenType>()) // (offset + len_type(u8)) * len
                * self.restart_points_type_index.len()
            + std::mem::size_of::<u32>() // restart_points_type_index len
    }

    /// Finishes building block.
    ///
    /// # Format
    ///
    /// ```plain
    /// compressed: | entries | restart point 0 (4B) | ... | restart point N-1 (4B) | N (4B) | restart point index 0 (5B)| ... | restart point index N-1 (5B) | N (4B)
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
        for (offset, len_type) in &self.restart_points_type_index {
            self.buf.put_u32_le(*offset);
            self.buf.put_u8(*len_type as u8);
        }

        // for len_type in self.restart_points_type_index.values() {
        //     self.buf.put_u8(*len_type as u8);
        // }

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
        // block + restart_points + restart_points.len + restart_points_type_indexs +
        // restart_points_type_indexs.len compression_algorithm + checksum
        self.buf.len()
            + std::mem::size_of::<u32>() * self.restart_points.len() // restart_points
            + std::mem::size_of::<u32>() // restart_points.len
            + (std::mem::size_of::<u32>() + std::mem::size_of::<LenType>()) * self.restart_points_type_index.len() // restart_points_type_indexs
            + std::mem::size_of::<u32>() // restart_points_type_indexs.len
            + 1 // compression_algorithm
            + 8 // checksum
    }
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
        }

        assert!(!bi.is_valid());
    }
}
