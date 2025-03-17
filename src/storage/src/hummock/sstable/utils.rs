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

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Display;
use std::ptr;

use risingwave_hummock_sdk::key::MAX_KEY_LEN;
use xxhash_rust::xxh64;

use super::{HummockError, HummockResult};

unsafe fn read_u64(ptr: *const u8) -> u64 {
    ptr::read_unaligned(ptr as *const u64)
}

unsafe fn read_u32(ptr: *const u8) -> u32 {
    ptr::read_unaligned(ptr as *const u32)
}

#[inline]
pub fn bytes_diff_below_max_key_length<'a>(base: &[u8], target: &'a [u8]) -> &'a [u8] {
    let end = base.len().min(target.len()).min(MAX_KEY_LEN);
    let mut i = 0;
    unsafe {
        while i + 8 <= end {
            if read_u64(base.as_ptr().add(i)) != read_u64(target.as_ptr().add(i)) {
                break;
            }
            i += 8;
        }
        if i + 4 <= end && read_u32(base.as_ptr().add(i)) == read_u32(target.as_ptr().add(i)) {
            i += 4;
        }
        while i < end {
            if base.get_unchecked(i) != target.get_unchecked(i) {
                return target.get_unchecked(i..);
            }
            i += 1;
        }
        target.get_unchecked(end..)
    }
}

/// Calculates the ``XxHash`` of the given data.
pub fn xxhash64_checksum(data: &[u8]) -> u64 {
    xxh64::xxh64(data, 0)
}

/// Verifies the checksum of the data equals the given checksum with xxhash64.
pub fn xxhash64_verify(data: &[u8], checksum: u64) -> HummockResult<()> {
    let data_checksum = xxhash64_checksum(data);
    if data_checksum != checksum {
        return Err(HummockError::checksum_mismatch(checksum, data_checksum));
    }
    Ok(())
}

use bytes::{Buf, BufMut};

pub fn put_length_prefixed_slice(mut buf: impl BufMut, slice: &[u8]) {
    let len = checked_into_u32(slice.len())
        .unwrap_or_else(|_| panic!("WARN overflow can't convert slice {} into u32", slice.len()));
    buf.put_u32_le(len);
    buf.put_slice(slice);
}

pub fn get_length_prefixed_slice(buf: &mut &[u8]) -> Vec<u8> {
    let len = buf.get_u32_le() as usize;
    let v = buf[..len].to_vec();
    buf.advance(len);
    v
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    None,
    Lz4,
    Zstd,
}

impl CompressionAlgorithm {
    pub fn encode(&self, buf: &mut impl BufMut) {
        let v = match self {
            Self::None => 0,
            Self::Lz4 => 1,
            Self::Zstd => 2,
        };
        buf.put_u8(v);
    }

    pub fn decode(buf: &mut impl Buf) -> HummockResult<Self> {
        match buf.get_u8() {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            2 => Ok(Self::Zstd),
            _ => Err(HummockError::decode_error(
                "not valid compression algorithm",
            )),
        }
    }
}

impl From<u32> for CompressionAlgorithm {
    fn from(ca: u32) -> Self {
        match ca {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            _ => CompressionAlgorithm::Zstd,
        }
    }
}

impl From<CompressionAlgorithm> for u8 {
    fn from(ca: CompressionAlgorithm) -> Self {
        match ca {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Lz4 => 1,
            CompressionAlgorithm::Zstd => 2,
        }
    }
}

impl From<CompressionAlgorithm> for u64 {
    fn from(ca: CompressionAlgorithm) -> Self {
        match ca {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Lz4 => 1,
            CompressionAlgorithm::Zstd => 2,
        }
    }
}

impl TryFrom<u8> for CompressionAlgorithm {
    type Error = HummockError;

    fn try_from(v: u8) -> core::result::Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            2 => Ok(Self::Zstd),
            _ => Err(HummockError::decode_error(
                "not valid compression algorithm",
            )),
        }
    }
}

pub fn checked_into_u32<T: TryInto<u32> + Copy + Display>(i: T) -> Result<u32, T::Error> {
    i.try_into()
}
