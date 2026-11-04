// Copyright 2026 RisingWave Labs
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

//! Delete file layout (all integers little-endian):
//!
//! ```text
//! magic: u32, version: u32, epoch: u64, num_keys: u32,
//! then per key: key_len: u32, key,
//! checksum: u64 (xxhash64 of every byte before it)
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use xxhash_rust::xxh64;

use super::FULL_TEXT_FORMAT_VERSION;
use crate::hummock::{HummockError, HummockResult};

const DELETE_FILE_MAGIC: u32 = 0x5257_4644; // "RWFD"
const HEADER_LEN: usize = 4 + 4 + 8 + 4;

/// Encodes a delete file: a sorted, de-duplicated list of keys deleted at one epoch.
pub fn encode_delete_file(epoch: u64, keys: &[Bytes]) -> Bytes {
    debug_assert!(
        keys.is_sorted_by(|a, b| a < b),
        "keys must be sorted and de-duplicated"
    );
    let keys_len: usize = keys.iter().map(|key| 4 + key.len()).sum();
    let mut buf = BytesMut::with_capacity(HEADER_LEN + keys_len + 8);
    buf.put_u32_le(DELETE_FILE_MAGIC);
    buf.put_u32_le(FULL_TEXT_FORMAT_VERSION);
    buf.put_u64_le(epoch);
    buf.put_u32_le(keys.len() as u32);
    for key in keys {
        buf.put_u32_le(key.len() as u32);
        buf.put_slice(key);
    }
    let checksum = xxh64::xxh64(&buf, 0);
    buf.put_u64_le(checksum);
    buf.freeze()
}

/// Decodes a delete file into its epoch and keys.
pub fn decode_delete_file(data: &[u8]) -> HummockResult<(u64, Vec<Bytes>)> {
    let corrupted = || HummockError::decode_error("corrupted full-text delete file");
    if data.len() < HEADER_LEN + 8 {
        return Err(corrupted());
    }
    let (body, mut checksum) = data.split_at(data.len() - 8);
    let mut header = body;
    let magic = header.get_u32_le();
    if magic != DELETE_FILE_MAGIC {
        return Err(HummockError::magic_mismatch(DELETE_FILE_MAGIC, magic));
    }
    let version = header.get_u32_le();
    if version != FULL_TEXT_FORMAT_VERSION {
        return Err(HummockError::invalid_format_version(version));
    }
    let expected_checksum = checksum.get_u64_le();
    let actual_checksum = xxh64::xxh64(body, 0);
    if actual_checksum != expected_checksum {
        return Err(HummockError::checksum_mismatch(
            expected_checksum,
            actual_checksum,
        ));
    }

    let epoch = header.get_u64_le();
    let num_keys = header.get_u32_le() as usize;
    // Keys are sliced out of one shared buffer instead of being copied one by one.
    let buf = Bytes::copy_from_slice(body);
    let mut offset = HEADER_LEN;
    let mut keys = Vec::with_capacity(num_keys.min(buf.len() / 4));
    for _ in 0..num_keys {
        if buf.len() - offset < 4 {
            return Err(corrupted());
        }
        let key_len = (&buf[offset..offset + 4]).get_u32_le() as usize;
        offset += 4;
        if buf.len() - offset < key_len {
            return Err(corrupted());
        }
        keys.push(buf.slice(offset..offset + key_len));
        offset += key_len;
    }
    if offset != buf.len() {
        return Err(corrupted());
    }
    Ok((epoch, keys))
}
