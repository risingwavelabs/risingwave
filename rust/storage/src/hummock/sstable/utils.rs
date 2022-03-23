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
// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::{self};
use std::hash::Hasher;
use std::ptr;

use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_pb::hummock::Checksum;
use serde::Deserialize;

use super::{HummockError, HummockResult};

unsafe fn u64(ptr: *const u8) -> u64 {
    ptr::read_unaligned(ptr as *const u64)
}

unsafe fn u32(ptr: *const u8) -> u32 {
    ptr::read_unaligned(ptr as *const u32)
}

#[inline]
pub fn bytes_diff<'a, 'b>(base: &'a [u8], target: &'b [u8]) -> &'b [u8] {
    let end = cmp::min(base.len(), target.len());
    let mut i = 0;
    unsafe {
        while i + 8 <= end {
            if u64(base.as_ptr().add(i)) != u64(target.as_ptr().add(i)) {
                break;
            }
            i += 8;
        }
        if i + 4 <= end && u32(base.as_ptr().add(i)) == u32(target.as_ptr().add(i)) {
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

/// Calculate the CRC32 of the given data.
fn crc32_checksum(data: &[u8]) -> u64 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize() as u64
}

/// Calculate the ``XxHash`` of the given data.
pub fn xxhash64_checksum(data: &[u8]) -> u64 {
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    hasher.write(data);
    hasher.finish() as u64
}

/// Verify the checksum of the data equals the given checksum with xxhash64.
pub fn xxhash64_verify(data: &[u8], checksum: u64) -> HummockResult<()> {
    let data_checksum = xxhash64_checksum(data);
    if data_checksum != checksum {
        return Err(HummockError::checksum_mismatch(checksum, data_checksum));
    }
    Ok(())
}

/// Calculate the checksum of the given `data` using `algo`.
pub fn checksum(algo: ChecksumAlg, data: &[u8]) -> Checksum {
    let sum = match algo {
        ChecksumAlg::Crc32c => crc32_checksum(data),
        ChecksumAlg::XxHash64 => xxhash64_checksum(data),
    };
    Checksum {
        sum,
        algo: algo as i32,
    }
}

/// Verify the checksum of the data equals the given checksum.
pub fn verify_checksum(chksum: &Checksum, data: &[u8]) -> HummockResult<()> {
    let data_chksum = match chksum.algo() {
        ChecksumAlg::Crc32c => crc32_checksum(data),
        ChecksumAlg::XxHash64 => xxhash64_checksum(data),
    };
    let expected_result = chksum.get_sum();
    if expected_result != data_chksum {
        return Err(HummockError::checksum_mismatch(
            expected_result,
            data_chksum,
        ));
    }
    Ok(())
}

use bytes::{Buf, BufMut, Bytes, BytesMut};

const MASK: u32 = 128;

pub fn var_u32_len(n: u32) -> usize {
    if n < (1 << 7) {
        1
    } else if n < (1 << 14) {
        2
    } else if n < (1 << 21) {
        3
    } else if n < (1 << 28) {
        4
    } else {
        5
    }
}

pub trait BufMutExt: BufMut {
    fn put_var_u32(&mut self, n: u32) {
        if n < (1 << 7) {
            self.put_u8(n as u8);
        } else if n < (1 << 14) {
            self.put_u8((n | MASK) as u8);
            self.put_u8((n >> 7) as u8);
        } else if n < (1 << 21) {
            self.put_u8((n | MASK) as u8);
            self.put_u8(((n >> 7) | MASK) as u8);
            self.put_u8((n >> 14) as u8);
        } else if n < (1 << 28) {
            self.put_u8((n | MASK) as u8);
            self.put_u8(((n >> 7) | MASK) as u8);
            self.put_u8(((n >> 14) | MASK) as u8);
            self.put_u8((n >> 21) as u8);
        } else {
            self.put_u8((n | MASK) as u8);
            self.put_u8(((n >> 7) | MASK) as u8);
            self.put_u8(((n >> 14) | MASK) as u8);
            self.put_u8(((n >> 21) | MASK) as u8);
            self.put_u8((n >> 28) as u8);
        }
    }
}

pub trait BufExt: Buf {
    fn get_var_u32(&mut self) -> u32 {
        let mut n = 0u32;
        let mut shift = 0;
        loop {
            let v = self.get_u8() as u32;
            if v & MASK != 0 {
                n |= (v & (MASK - 1)) << shift;
            } else {
                n |= v << shift;
                break;
            }
            shift += 7;
        }
        n
    }
}

impl<T: BufMut + ?Sized> BufMutExt for &mut T {}

impl<T: Buf + ?Sized> BufExt for &mut T {}

#[derive(Deserialize, Clone, Copy, Debug)]
pub enum CompressionAlgorithm {
    None,
    Lz4,
}

impl CompressionAlgorithm {
    pub fn encode(&self, buf: &mut impl BufMut) {
        let v = match self {
            Self::None => 0,
            Self::Lz4 => 1,
        };
        buf.put_u8(v);
    }

    pub fn decode(buf: &mut impl Buf) -> HummockResult<Self> {
        match buf.get_u8() {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            _ => {
                Err(HummockError::DecodeError("not valid compression algorithm".to_string()).into())
            }
        }
    }
}

impl From<CompressionAlgorithm> for u8 {
    fn from(ca: CompressionAlgorithm) -> Self {
        match ca {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Lz4 => 1,
        }
    }
}

impl From<CompressionAlgorithm> for u64 {
    fn from(ca: CompressionAlgorithm) -> Self {
        match ca {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Lz4 => 1,
        }
    }
}

impl TryFrom<u8> for CompressionAlgorithm {
    type Error = HummockError;
    fn try_from(v: u8) -> core::result::Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            _ => Err(HummockError::DecodeError(
                "not valid compression algorithm".to_string(),
            )),
        }
    }
}

/// Key categories:
///
/// A full key value pair looks like:
///
/// ```plain
/// | user key | epoch (8B) | value |
///
/// |<------- full key ------->|
/// ```
#[allow(dead_code)]
pub fn full_key(user_key: &[u8], epoch: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(user_key.len() + 8);
    buf.put_slice(user_key);
    buf.put_u64(!epoch);
    buf.freeze()
}

/// Get user key in full key.
#[allow(dead_code)]
pub fn user_key(full_key: &[u8]) -> &[u8] {
    &full_key[..full_key.len() - 8]
}

/// Get epoch in full key.
#[allow(dead_code)]
pub fn epoch(full_key: &[u8]) -> u64 {
    !(&full_key[full_key.len() - 8..]).get_u64()
}
