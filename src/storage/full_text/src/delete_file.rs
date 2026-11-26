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

use crate::{FULL_TEXT_FORMAT_VERSION, FullTextError, FullTextResult};

const DELETE_FILE_MAGIC: u32 = 0x5257_4644; // "RWFD"
const DELETE_BLOCK_SIZE: usize = 256 << 10;

struct DeleteFileHeader {
    epoch: u64,
    num_keys: u32,
}

impl DeleteFileHeader {
    const ENCODED_LEN: usize = 4 + 4 + 8 + 4;

    fn encode_into(&self, buf: &mut impl BufMut) {
        buf.put_u32_le(DELETE_FILE_MAGIC);
        buf.put_u32_le(FULL_TEXT_FORMAT_VERSION);
        buf.put_u64_le(self.epoch);
        buf.put_u32_le(self.num_keys);
    }

    fn decode(mut buf: &[u8]) -> FullTextResult<Self> {
        if buf.len() < Self::ENCODED_LEN {
            return Err(corrupted());
        }
        let magic = buf.get_u32_le();
        if magic != DELETE_FILE_MAGIC {
            return Err(FullTextError::MagicMismatch {
                expected: DELETE_FILE_MAGIC,
                found: magic,
            });
        }
        let version = buf.get_u32_le();
        if version != FULL_TEXT_FORMAT_VERSION {
            return Err(FullTextError::InvalidFormatVersion(version));
        }
        Ok(Self {
            epoch: buf.get_u64_le(),
            num_keys: buf.get_u32_le(),
        })
    }
}

fn corrupted() -> FullTextError {
    FullTextError::decode("corrupted full-text delete file")
}

/// Encodes a delete file: sorted, de-duplicated keys deleted between checkpoints, tagged with
/// the closing checkpoint epoch.
pub fn encode_delete_file(epoch: u64, keys: &[Bytes]) -> impl Iterator<Item = Bytes> + '_ {
    debug_assert!(
        keys.is_sorted_by(|a, b| a < b),
        "keys must be sorted and de-duplicated"
    );
    let mut header = BytesMut::with_capacity(DeleteFileHeader::ENCODED_LEN);
    DeleteFileHeader {
        epoch,
        num_keys: keys.len() as u32,
    }
    .encode_into(&mut header);
    let mut header = Some(header);
    let mut keys = keys.iter().peekable();
    let mut large_key: Option<&Bytes> = None;
    let mut hash = xxh64::Xxh64::new(0);
    let mut finished = false;
    std::iter::from_fn(move || {
        if finished {
            return None;
        }
        if let Some(key) = large_key.take() {
            hash.update(key);
            return Some(key.clone());
        }

        let mut block = header.take().unwrap_or_default();
        while let Some(key) = keys.peek() {
            let entry_len = 4 + key.len();
            if entry_len > DELETE_BLOCK_SIZE {
                // Don't copy large keys or grow the block to fit them.
                block.put_u32_le(key.len() as u32);
                large_key = keys.next();
                break;
            }
            if block.len() + entry_len > DELETE_BLOCK_SIZE {
                break;
            }
            let key = keys.next().unwrap();
            block.put_u32_le(key.len() as u32);
            block.put_slice(key);
        }
        hash.update(&block);
        if large_key.is_none() && keys.peek().is_none() {
            block.put_u64_le(hash.digest());
            finished = true;
        }
        Some(block.freeze())
    })
}

/// Decodes a delete file into its epoch and keys.
pub fn decode_delete_file(data: &[u8]) -> FullTextResult<(u64, Vec<Bytes>)> {
    if data.len() < DeleteFileHeader::ENCODED_LEN + 8 {
        return Err(corrupted());
    }
    let (body, mut checksum) = data.split_at(data.len() - 8);
    let header = DeleteFileHeader::decode(body)?;
    let expected_checksum = checksum.get_u64_le();
    let actual_checksum = xxh64::xxh64(body, 0);
    if actual_checksum != expected_checksum {
        return Err(FullTextError::ChecksumMismatch {
            expected: expected_checksum,
            found: actual_checksum,
        });
    }

    let num_keys = header.num_keys as usize;
    // Keys are sliced out of one shared buffer instead of being copied one by one.
    let buf = Bytes::copy_from_slice(body);
    let mut offset = DeleteFileHeader::ENCODED_LEN;
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
    Ok((header.epoch, keys))
}
