// Copyright 2025 RisingWave Labs
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
use std::fmt::Debug;
use std::io::{Read, Write};
use std::mem::size_of;
use std::ops::Range;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::KeyComparator;
use risingwave_hummock_sdk::key::FullKey;
use serde::{Deserialize, Serialize};

use super::utils::{CompressionAlgorithm, bytes_diff_below_max_key_length, xxhash64_verify};
use crate::hummock::sstable::utils;
use crate::hummock::sstable::utils::xxhash64_checksum;
use crate::hummock::{HummockError, HummockResult};
use crate::monitor::Hitmap;

pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;
pub const DEFAULT_RESTART_INTERVAL: usize = 16;
pub const DEFAULT_ENTRY_SIZE: usize = 24; // table_id(u64) + primary_key(u64) + epoch(u64)

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LenType {
    u8 = 1,
    u16 = 2,
    u32 = 3,
}

macro_rules! put_fn {
    ($name:ident, $($value:ident: $type:ty),*) => {
        fn $name<T: BufMut>(&self, buf: &mut T, $($value: $type),*) {
            match *self {
                LenType::u8 => {
                    $(buf.put_u8($value as u8);)*
                },

                LenType::u16 => {
                    $(buf.put_u16($value as u16);)*
                },

                LenType::u32 => {
                    $(buf.put_u32($value as u32);)*
                },
            }
        }
    };
}

macro_rules! get_fn {
    ($name:ident, $($type:ty),*) => {
        #[allow(unused_parens)]
        fn $name<T: Buf>(&self, buf: &mut T) -> ($($type), *) {
            match *self {
                LenType::u8 => {
                    ($(buf.get_u8() as $type),*)
                }
                LenType::u16 => {
                    ($(buf.get_u16() as $type),*)
                }
                LenType::u32 => {
                    ($(buf.get_u32() as $type),*)
                }
            }
        }
    };
}

impl From<u8> for LenType {
    fn from(value: u8) -> Self {
        match value {
            1 => LenType::u8,
            2 => LenType::u16,
            3 => LenType::u32,
            _ => {
                panic!("unexpected type {}", value)
            }
        }
    }
}

impl LenType {
    put_fn!(put, v1: usize);

    put_fn!(put2, v1: usize, v2: usize);

    get_fn!(get, usize);

    get_fn!(get2, usize, usize);

    fn new(len: usize) -> Self {
        const U8_MAX: usize = u8::MAX as usize + 1;
        const U16_MAX: usize = u16::MAX as usize + 1;
        const U32_MAX: usize = u32::MAX as usize + 1;

        match len {
            0..U8_MAX => LenType::u8,
            U8_MAX..U16_MAX => LenType::u16,
            U16_MAX..U32_MAX => LenType::u32,
            _ => unreachable!("unexpected LenType {}", len),
        }
    }

    fn len(&self) -> usize {
        match *self {
            Self::u8 => size_of::<u8>(),
            Self::u16 => size_of::<u16>(),
            Self::u32 => size_of::<u32>(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestartPoint {
    pub offset: u32,
    pub key_len_type: LenType,
    pub value_len_type: LenType,
}

impl RestartPoint {
    fn size_of() -> usize {
        // store key_len_type and value_len_type in u8 related to `BlockBuilder::build`
        // encoding_value = (key_len_type << 4) | value_len_type
        std::mem::size_of::<u32>() + std::mem::size_of::<LenType>()
    }
}

pub struct Block {
    /// Uncompressed entries data, with restart encoded restart points info.
    data: Bytes,
    /// Uncompressed entries data length.
    data_len: usize,

    /// Table id of this block.
    table_id: TableId,

    /// Restart points.
    restart_points: Vec<RestartPoint>,

    hitmap: Hitmap<{ Self::HITMAP_ELEMS }>,
}

impl Clone for Block {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            data_len: self.data_len,
            table_id: self.table_id,
            restart_points: self.restart_points.clone(),
            hitmap: self.hitmap.clone(),
        }
    }
}

impl Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("data_len", &self.data_len)
            .field("table_id", &self.table_id)
            .field("restart_points", &self.restart_points)
            .finish()
    }
}

impl Serialize for Block {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde_bytes::serialize(&self.data[..], serializer)
    }
}

impl<'de> Deserialize<'de> for Block {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        let data = Bytes::from(data);
        Ok(Block::decode_from_raw(data))
    }
}

impl Block {
    pub const HITMAP_ELEMS: usize = 4;

    pub fn get_algorithm(buf: &Bytes) -> HummockResult<CompressionAlgorithm> {
        let compression = CompressionAlgorithm::decode(&mut &buf[buf.len() - 9..buf.len() - 8])?;
        Ok(compression)
    }

    pub fn decode(buf: Bytes, uncompressed_capacity: usize) -> HummockResult<Self> {
        Self::decode_with_copy(buf, uncompressed_capacity, false)
    }

    pub fn decode_with_copy(
        buf: Bytes,
        uncompressed_capacity: usize,
        copy: bool,
    ) -> HummockResult<Self> {
        // Verify checksum.

        let xxhash64_checksum = (&buf[buf.len() - 8..]).get_u64_le();
        xxhash64_verify(&buf[..buf.len() - 8], xxhash64_checksum)?;

        // Decompress.
        let compression = CompressionAlgorithm::decode(&mut &buf[buf.len() - 9..buf.len() - 8])?;
        let compressed_data = &buf[..buf.len() - 9];

        let buf = match compression {
            CompressionAlgorithm::None => {
                if copy {
                    Bytes::copy_from_slice(&buf[0..(buf.len() - 9)])
                } else {
                    buf.slice(0..(buf.len() - 9))
                }
            }
            CompressionAlgorithm::Lz4 => {
                let mut decoder = lz4::Decoder::new(compressed_data.reader())
                    .map_err(HummockError::decode_error)?;
                let mut decoded = Vec::with_capacity(uncompressed_capacity);
                let read_size = decoder
                    .read_to_end(&mut decoded)
                    .map_err(HummockError::decode_error)?;
                assert_eq!(read_size, uncompressed_capacity);
                Bytes::from(decoded)
            }
            CompressionAlgorithm::Zstd => {
                let mut decoder = zstd::Decoder::new(compressed_data.reader())
                    .map_err(HummockError::decode_error)?;
                let mut decoded = Vec::with_capacity(uncompressed_capacity);
                let read_size = decoder
                    .read_to_end(&mut decoded)
                    .map_err(HummockError::decode_error)?;
                assert_eq!(read_size, uncompressed_capacity);
                Bytes::from(decoded)
            }
        };

        Ok(Self::decode_from_raw(buf))
    }

    pub fn decode_from_raw(buf: Bytes) -> Self {
        let table_id = (&buf[buf.len() - 4..]).get_u32_le();
        // decode restart_points_type_index
        let n_index = ((&buf[buf.len() - 8..buf.len() - 4]).get_u32_le()) as usize;
        let index_data_len = size_of::<u32>() + n_index * RestartPoint::size_of();
        let data_len = buf.len() - 4 - index_data_len;
        let mut restart_points_type_index_buf = &buf[data_len..buf.len() - 8];

        let mut index_key_vec = Vec::with_capacity(n_index);
        for _ in 0..n_index {
            let offset = restart_points_type_index_buf.get_u32_le();
            let value = restart_points_type_index_buf.get_u8();
            let key_len_type = LenType::from(value >> 4);
            let value_len_type = LenType::from(value & 0x0F);

            index_key_vec.push(RestartPoint {
                offset,
                key_len_type,
                value_len_type,
            });
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
            if type_index < index_key_vec.len() - 1
                && offset >= index_key_vec[type_index + 1].offset
            {
                type_index += 1;
            }

            restart_points.push(RestartPoint {
                offset,
                key_len_type: index_key_vec[type_index].key_len_type,
                value_len_type: index_key_vec[type_index].value_len_type,
            });
        }

        Block {
            data: buf,
            data_len,
            restart_points,
            table_id: TableId::new(table_id),
            hitmap: Hitmap::default(),
        }
    }

    /// Entries data len.
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        assert!(!self.data.is_empty());
        self.data_len
    }

    pub fn capacity(&self) -> usize {
        self.data.len()
            + self.restart_points.capacity() * std::mem::size_of::<u32>()
            + std::mem::size_of::<u32>()
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Gets restart point by index.
    pub fn restart_point(&self, index: usize) -> RestartPoint {
        self.restart_points[index]
    }

    /// Gets restart point len.
    pub fn restart_point_len(&self) -> usize {
        self.restart_points.len()
    }

    /// Searches the index of the restart point by partition point.
    pub fn search_restart_partition_point<P>(&self, pred: P) -> usize
    where
        P: FnMut(&RestartPoint) -> bool,
    {
        self.restart_points.partition_point(pred)
    }

    pub fn data(&self) -> &[u8] {
        &self.data[..self.data_len]
    }

    pub fn raw(&self) -> &[u8] {
        &self.data[..]
    }

    pub fn hitmap(&self) -> &Hitmap<{ Self::HITMAP_ELEMS }> {
        &self.hitmap
    }

    pub fn efficiency(&self) -> f64 {
        self.hitmap.ratio()
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
    pub fn encode(&self, buf: &mut impl BufMut, key_len_type: LenType, value_len_type: LenType) {
        key_len_type.put2(buf, self.overlap, self.diff);
        value_len_type.put(buf, self.value);
    }

    pub fn decode(
        buf: &mut impl Buf,
        offset: usize,
        key_len_type: LenType,
        value_len_type: LenType,
    ) -> Self {
        let (overlap, diff) = key_len_type.get2(buf);
        let value = value_len_type.get(buf);

        let len = key_len_type.len() * 2 + value_len_type.len();

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
    /// Compress buffer
    compress_buf: BytesMut,
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

    table_id: Option<u32>,
    // restart_points_type_index stores only the restart_point corresponding to each type change,
    // as an index, in order to reduce space usage
    restart_points_type_index: Vec<RestartPoint>,
}

impl BlockBuilder {
    pub fn new(options: BlockBuilderOptions) -> Self {
        Self {
            // add more space to avoid re-allocate space. (for restart_points and restart_points_type_index)
            buf: BytesMut::with_capacity(Self::buf_reserve_size(&options)),
            compress_buf: BytesMut::default(),
            restart_count: options.restart_interval,
            restart_points: Vec::with_capacity(
                options.capacity / DEFAULT_ENTRY_SIZE / options.restart_interval + 1,
            ),
            last_key: vec![],
            entry_count: 0,
            compression_algorithm: options.compression_algorithm,
            table_id: None,
            restart_points_type_index: Vec::default(),
        }
    }

    pub fn add_for_test(&mut self, full_key: FullKey<&[u8]>, value: &[u8]) {
        self.add(full_key, value);
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
    pub fn add(&mut self, full_key: FullKey<&[u8]>, value: &[u8]) {
        let input_table_id = full_key.user_key.table_id.table_id();
        match self.table_id {
            Some(current_table_id) => assert_eq!(current_table_id, input_table_id),
            None => self.table_id = Some(input_table_id),
        }
        #[cfg(debug_assertions)]
        self.debug_valid();

        let mut key: BytesMut = Default::default();
        full_key.encode_into_without_table_id(&mut key);
        if self.entry_count > 0 {
            debug_assert!(!key.is_empty());
            debug_assert_eq!(
                KeyComparator::compare_encoded_full_key(&self.last_key[..], &key[..]),
                Ordering::Less,
                "epoch: {}, table key: {}",
                full_key.epoch_with_gap.pure_epoch(),
                u64::from_be_bytes(
                    full_key.user_key.table_key.as_ref()[0..8]
                        .try_into()
                        .unwrap()
                ),
            );
        }
        // Update restart point if needed and calculate diff key.
        let k_type = LenType::new(key.len());
        let v_type = LenType::new(value.len());

        let type_mismatch = if let Some(RestartPoint {
            offset: _,
            key_len_type: last_key_len_type,
            value_len_type: last_value_len_type,
        }) = self.restart_points_type_index.last()
        {
            k_type != *last_key_len_type || v_type != *last_value_len_type
        } else {
            true
        };

        let diff_key = if self.entry_count % self.restart_count == 0 || type_mismatch {
            let offset = utils::checked_into_u32(self.buf.len()).unwrap_or_else(|_| {
                panic!(
                    "WARN overflow can't convert buf_len {} into u32 table {:?}",
                    self.buf.len(),
                    self.table_id,
                )
            });

            self.restart_points.push(offset);

            if type_mismatch {
                self.restart_points_type_index.push(RestartPoint {
                    offset,
                    key_len_type: k_type,
                    value_len_type: v_type,
                });
            }

            key.as_ref()
        } else {
            bytes_diff_below_max_key_length(&self.last_key, &key[..])
        };

        let prefix = KeyPrefix::new_without_len(
            key.len() - diff_key.len(),
            diff_key.len(),
            value.len(),
            self.buf.len(),
        );

        prefix.encode(&mut self.buf, k_type, v_type);
        self.buf.put_slice(diff_key);
        self.buf.put_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(&key);
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
        self.table_id = None;
        self.restart_points_type_index.clear();
        self.last_key.clear();
        self.entry_count = 0;
    }

    /// Calculate block size without compression.
    pub fn uncompressed_block_size(&mut self) -> usize {
        self.buf.len()
            + (self.restart_points.len() + 1) * std::mem::size_of::<u32>()
            + (RestartPoint::size_of()) // (offset + len_type(u8)) * len
                * self.restart_points_type_index.len()
            + std::mem::size_of::<u32>() // restart_points_type_index len
            + std::mem::size_of::<u32>() // table_id len
    }

    /// Finishes building block.
    ///
    /// # Format
    ///
    /// ```plain
    /// compressed: | entries | restart point 0 (4B) | ... | restart point N-1 (4B) | N (4B) | restart point index 0 (5B)| ... | restart point index N-1 (5B) | N (4B) | table id (4B)
    /// uncompressed: | compression method (1B) | xxhash64 checksum (8B) |
    /// ```
    ///
    /// # Panics
    ///
    /// Panic if there is compression error.
    pub fn build(&mut self) -> &[u8] {
        assert!(
            self.entry_count > 0,
            "buf_len {} entry_count {} table {:?}",
            self.buf.len(),
            self.entry_count,
            self.table_id
        );

        for restart_point in &self.restart_points {
            self.buf.put_u32_le(*restart_point);
        }

        self.buf.put_u32_le(
            utils::checked_into_u32(self.restart_points.len()).unwrap_or_else(|_| {
                panic!(
                    "WARN overflow can't convert restart_points_len {} into u32 table {:?}",
                    self.restart_points.len(),
                    self.table_id,
                )
            }),
        );
        for RestartPoint {
            offset,
            key_len_type,
            value_len_type,
        } in &self.restart_points_type_index
        {
            self.buf.put_u32_le(*offset);

            let mut value: u8 = 0;
            value |= *key_len_type as u8;
            value <<= 4;
            value |= *value_len_type as u8;

            self.buf.put_u8(value);
        }

        self.buf.put_u32_le(
            utils::checked_into_u32(self.restart_points_type_index.len()).unwrap_or_else(|_| {
                panic!(
                    "WARN overflow can't convert restart_points_type_index_len {} into u32 table {:?}",
                    self.restart_points_type_index.len(),
                    self.table_id,
                )
            }),
        );

        self.buf.put_u32_le(self.table_id.unwrap());
        let result_buf = if self.compression_algorithm != CompressionAlgorithm::None {
            self.compress_buf.clear();
            self.compress_buf = Self::compress(
                &self.buf[..],
                self.compression_algorithm,
                std::mem::take(&mut self.compress_buf),
            );

            &mut self.compress_buf
        } else {
            &mut self.buf
        };

        self.compression_algorithm.encode(result_buf);
        let checksum = xxhash64_checksum(result_buf);
        result_buf.put_u64_le(checksum);
        assert!(
            result_buf.len() < (u32::MAX) as usize,
            "buf_len {} entry_count {} table {:?}",
            result_buf.len(),
            self.entry_count,
            self.table_id
        );

        if self.compression_algorithm != CompressionAlgorithm::None {
            self.compress_buf.as_ref()
        } else {
            self.buf.as_ref()
        }
    }

    pub fn compress_block(
        buf: Bytes,
        target_compression: CompressionAlgorithm,
    ) -> HummockResult<Bytes> {
        // Verify checksum.
        let checksum = (&buf[buf.len() - 8..]).get_u64_le();
        xxhash64_verify(&buf[..buf.len() - 8], checksum)?;
        // Decompress.
        let compression = CompressionAlgorithm::decode(&mut &buf[buf.len() - 9..buf.len() - 8])?;
        let compressed_data = &buf[..buf.len() - 9];
        assert_eq!(compression, CompressionAlgorithm::None);
        let mut compress_writer = Self::compress(
            compressed_data,
            target_compression,
            BytesMut::with_capacity(buf.len()),
        );

        target_compression.encode(&mut compress_writer);
        let checksum = xxhash64_checksum(&compress_writer);
        compress_writer.put_u64_le(checksum);
        Ok(compress_writer.freeze())
    }

    pub fn compress(
        buf: &[u8],
        compression_algorithm: CompressionAlgorithm,
        compress_writer: BytesMut,
    ) -> BytesMut {
        match compression_algorithm {
            CompressionAlgorithm::None => unreachable!(),
            CompressionAlgorithm::Lz4 => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .level(4)
                    .build(compress_writer.writer())
                    .map_err(HummockError::encode_error)
                    .unwrap();
                encoder
                    .write_all(buf)
                    .map_err(HummockError::encode_error)
                    .unwrap();
                let (writer, result) = encoder.finish();
                result.map_err(HummockError::encode_error).unwrap();
                writer.into_inner()
            }
            CompressionAlgorithm::Zstd => {
                let mut encoder = zstd::Encoder::new(compress_writer.writer(), 4)
                    .map_err(HummockError::encode_error)
                    .unwrap();
                encoder
                    .write_all(buf)
                    .map_err(HummockError::encode_error)
                    .unwrap();
                let writer = encoder
                    .finish()
                    .map_err(HummockError::encode_error)
                    .unwrap();
                writer.into_inner()
            }
        }
    }

    /// Approximate block len (uncompressed).
    pub fn approximate_len(&self) -> usize {
        // block + restart_points + restart_points.len + restart_points_type_indices +
        // restart_points_type_indics.len compression_algorithm + checksum
        self.buf.len()
            + std::mem::size_of::<u32>() * self.restart_points.len() // restart_points
            + std::mem::size_of::<u32>() // restart_points.len
            + RestartPoint::size_of() * self.restart_points_type_index.len() // restart_points_type_indics
            + std::mem::size_of::<u32>() // restart_points_type_indics.len
            + std::mem::size_of::<CompressionAlgorithm>() // compression_algorithm
            + std::mem::size_of::<u64>() // checksum
            + std::mem::size_of::<u32>() // table_id
    }

    pub fn debug_valid(&self) {
        if self.entry_count == 0 {
            debug_assert!(self.buf.is_empty());
            debug_assert!(self.restart_points.is_empty());
            debug_assert!(self.restart_points_type_index.is_empty());
            debug_assert!(self.last_key.is_empty());
        }
    }

    pub fn table_id(&self) -> Option<u32> {
        self.table_id
    }

    fn buf_reserve_size(option: &BlockBuilderOptions) -> usize {
        option.capacity + 1024 + 256
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::MAX_KEY_LEN;

    use super::*;
    use crate::hummock::{BlockHolder, BlockIterator};

    #[test]
    fn test_block_enc_dec() {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        builder.add_for_test(construct_full_key_struct_for_test(0, b"k1", 1), b"v01");
        builder.add_for_test(construct_full_key_struct_for_test(0, b"k2", 2), b"v02");
        builder.add_for_test(construct_full_key_struct_for_test(0, b"k3", 3), b"v03");
        builder.add_for_test(construct_full_key_struct_for_test(0, b"k4", 4), b"v04");
        let capacity = builder.uncompressed_block_size();
        assert_eq!(capacity, builder.approximate_len() - 9);
        let buf = builder.build().to_vec();
        let block = Box::new(Block::decode(buf.into(), capacity).unwrap());
        let mut bi = BlockIterator::new(BlockHolder::from_owned_block(block));

        bi.seek_to_first();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct_for_test(0, b"k1", 1), bi.key());
        assert_eq!(b"v01", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct_for_test(0, b"k2", 2), bi.key());
        assert_eq!(b"v02", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct_for_test(0, b"k3", 3), bi.key());
        assert_eq!(b"v03", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct_for_test(0, b"k4", 4), bi.key());
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
        builder.add_for_test(construct_full_key_struct_for_test(0, b"k1", 1), b"v01");
        builder.add_for_test(construct_full_key_struct_for_test(0, b"k2", 2), b"v02");
        builder.add_for_test(construct_full_key_struct_for_test(0, b"k3", 3), b"v03");
        builder.add_for_test(construct_full_key_struct_for_test(0, b"k4", 4), b"v04");
        let capacity = builder.uncompressed_block_size();
        assert_eq!(capacity, builder.approximate_len() - 9);
        let buf = builder.build().to_vec();
        let block = Box::new(Block::decode(buf.into(), capacity).unwrap());
        let mut bi = BlockIterator::new(BlockHolder::from_owned_block(block));

        bi.seek_to_first();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct_for_test(0, b"k1", 1), bi.key());
        assert_eq!(b"v01", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct_for_test(0, b"k2", 2), bi.key());
        assert_eq!(b"v02", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct_for_test(0, b"k3", 3), bi.key());
        assert_eq!(b"v03", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(construct_full_key_struct_for_test(0, b"k4", 4), bi.key());
        assert_eq!(b"v04", bi.value());

        bi.next();
        assert!(!bi.is_valid());
    }

    pub fn construct_full_key_struct_for_test(
        table_id: u32,
        table_key: &[u8],
        epoch: u64,
    ) -> FullKey<&[u8]> {
        FullKey::for_test(TableId::new(table_id), table_key, test_epoch(epoch))
    }

    #[test]
    fn test_block_enc_large_key() {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        let medium_key = vec![b'a'; MAX_KEY_LEN - 500];
        let large_key = vec![b'b'; MAX_KEY_LEN];
        let xlarge_key = vec![b'c'; MAX_KEY_LEN + 500];

        builder.add_for_test(construct_full_key_struct_for_test(0, &medium_key, 1), b"v1");
        builder.add_for_test(construct_full_key_struct_for_test(0, &large_key, 2), b"v2");
        builder.add_for_test(construct_full_key_struct_for_test(0, &xlarge_key, 3), b"v3");
        let capacity = builder.uncompressed_block_size();
        assert_eq!(capacity, builder.approximate_len() - 9);
        let buf = builder.build().to_vec();
        let block = Box::new(Block::decode(buf.into(), capacity).unwrap());
        let mut bi = BlockIterator::new(BlockHolder::from_owned_block(block));

        bi.seek_to_first();
        assert!(bi.is_valid());
        assert_eq!(
            construct_full_key_struct_for_test(0, &medium_key, 1),
            bi.key()
        );
        assert_eq!(b"v1", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(
            construct_full_key_struct_for_test(0, &large_key, 2),
            bi.key()
        );
        assert_eq!(b"v2", bi.value());

        bi.next();
        assert!(bi.is_valid());
        assert_eq!(
            construct_full_key_struct_for_test(0, &xlarge_key, 3),
            bi.key()
        );
        assert_eq!(b"v3", bi.value());

        bi.next();
        assert!(!bi.is_valid());
    }

    #[test]
    fn test_block_restart_point() {
        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);

        const KEY_COUNT: u8 = 100;
        const BUILDER_COUNT: u8 = 5;

        for _ in 0..BUILDER_COUNT {
            for index in 0..KEY_COUNT {
                if index < 50 {
                    let mut medium_key = vec![b'A'; MAX_KEY_LEN - 500];
                    medium_key.push(index);
                    builder
                        .add_for_test(construct_full_key_struct_for_test(0, &medium_key, 1), b"v1");
                } else if index < 80 {
                    let mut large_key = vec![b'B'; MAX_KEY_LEN];
                    large_key.push(index);
                    builder
                        .add_for_test(construct_full_key_struct_for_test(0, &large_key, 2), b"v2");
                } else {
                    let mut xlarge_key = vec![b'C'; MAX_KEY_LEN + 500];
                    xlarge_key.push(index);
                    builder
                        .add_for_test(construct_full_key_struct_for_test(0, &xlarge_key, 3), b"v3");
                }
            }

            let capacity = builder.uncompressed_block_size();
            assert_eq!(capacity, builder.approximate_len() - 9);
            let buf = builder.build().to_vec();
            let block = Box::new(Block::decode(buf.into(), capacity).unwrap());
            let mut bi = BlockIterator::new(BlockHolder::from_owned_block(block));
            bi.seek_to_first();
            assert!(bi.is_valid());

            for index in 0..KEY_COUNT {
                if index < 50 {
                    let mut medium_key = vec![b'A'; MAX_KEY_LEN - 500];
                    medium_key.push(index);
                    assert_eq!(
                        construct_full_key_struct_for_test(0, &medium_key, 1),
                        bi.key()
                    );
                } else if index < 80 {
                    let mut large_key = vec![b'B'; MAX_KEY_LEN];
                    large_key.push(index);
                    assert_eq!(
                        construct_full_key_struct_for_test(0, &large_key, 2),
                        bi.key()
                    );
                } else {
                    let mut xlarge_key = vec![b'C'; MAX_KEY_LEN + 500];
                    xlarge_key.push(index);
                    assert_eq!(
                        construct_full_key_struct_for_test(0, &xlarge_key, 3),
                        bi.key()
                    );
                }
                bi.next();
            }

            assert!(!bi.is_valid());
            builder.clear();
        }
    }

    #[test]
    fn test_block_serde() {
        fn assmut_serde<'de, T: Serialize + Deserialize<'de>>() {}

        assmut_serde::<Block>();
        assmut_serde::<Box<Block>>();

        let options = BlockBuilderOptions::default();
        let mut builder = BlockBuilder::new(options);
        for i in 0..100 {
            builder.add_for_test(
                construct_full_key_struct_for_test(0, format!("k{i:8}").as_bytes(), i),
                format!("v{i:8}").as_bytes(),
            );
        }

        let capacity = builder.uncompressed_block_size();
        assert_eq!(capacity, builder.approximate_len() - 9);
        let buf = builder.build().to_vec();

        let block = Box::new(Block::decode(buf.into(), capacity).unwrap());

        let buffer = bincode::serialize(&block).unwrap();
        let blk: Block = bincode::deserialize(&buffer).unwrap();

        assert_eq!(block.data, blk.data);
        assert_eq!(block.data_len, blk.data_len);
        assert_eq!(block.table_id, blk.table_id,);
        assert_eq!(block.restart_points, blk.restart_points);
    }
}
