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

//! On-object deletion vectors. Dense vectors use the native Tantivy `.del` format.
//! Sparse vectors store only the deleted doc IDs and expand once when a segment opens.

use std::path::Path;

use bytes::{Buf, BufMut, Bytes};
use roaring::RoaringBitmap;
use tantivy::directory::{Directory, ManagedDirectory, RamDirectory, TerminatingWrite};
use tantivy::fastfield::{AliveBitSet, write_alive_bitset};
use tantivy::{BitSet, Index};
use xxhash_rust::xxh64::xxh64;

use crate::{FULL_TEXT_FORMAT_VERSION, FullTextError, FullTextResult, full_text_error};

// The first word of a dense vector is max_doc, which is always below i32::MAX.
const SPARSE_MAGIC: u32 = 0xf257_4442;
const MIN_DENSE_BYTES_FOR_COMPRESSION: usize = 4096;

struct SparseHeader {
    max_doc: u32,
}

impl SparseHeader {
    const ENCODED_LEN: usize = 12;

    fn encode_into(self, buf: &mut impl BufMut) {
        buf.put_u32_le(SPARSE_MAGIC);
        buf.put_u32_le(FULL_TEXT_FORMAT_VERSION);
        buf.put_u32_le(self.max_doc);
    }

    fn decode(mut buf: &[u8]) -> FullTextResult<Self> {
        if buf.len() < Self::ENCODED_LEN || buf.get_u32_le() != SPARSE_MAGIC {
            return Err(corrupted());
        }
        let version = buf.get_u32_le();
        if version != FULL_TEXT_FORMAT_VERSION {
            return Err(FullTextError::InvalidFormatVersion(version));
        }
        Ok(Self {
            max_doc: buf.get_u32_le(),
        })
    }
}

fn corrupted() -> FullTextError {
    FullTextError::decode("corrupted full-text deletion vector")
}

fn dense_len(max_doc: u32) -> usize {
    // Max document count followed by one word per 64 doc IDs, without the footer.
    4 + max_doc.div_ceil(64) as usize * 8
}

pub fn encode_deletion_vector(alive: &BitSet) -> FullTextResult<Bytes> {
    let max_doc = alive.max_value();
    let num_deleted = max_doc as usize - alive.len();
    let dense_bytes = dense_len(max_doc);
    // Sparse expansion costs CPU on cache misses. Only use it for sparse sets with
    // substantial byte savings, even when other shapes could also compress.
    if dense_bytes >= MIN_DENSE_BYTES_FOR_COMPRESSION
        && num_deleted != 0
        && num_deleted <= (max_doc / 64) as usize
    {
        let mut deleted = RoaringBitmap::new();
        for bucket in 0..max_doc.div_ceil(64) {
            let mut missing = !u64::from_le_bytes(alive.tinyset(bucket).into_bytes());
            while missing != 0 {
                let doc = bucket * 64 + missing.trailing_zeros();
                if doc >= max_doc {
                    break; // Padding at the end of the last word.
                }
                deleted.insert(doc);
                missing &= missing - 1;
            }
        }
        let size = SparseHeader::ENCODED_LEN + deleted.serialized_size() + 8;
        if size <= dense_bytes / 2 {
            let mut buf = Vec::with_capacity(size);
            SparseHeader { max_doc }.encode_into(&mut buf);
            deleted.serialize_into(&mut buf).map_err(full_text_error)?;
            let checksum = xxh64(&buf, 0);
            buf.put_u64_le(checksum);
            return Ok(Bytes::from(buf));
        }
    }
    encode_tantivy_delete_file(alive)
}

/// Returns the native `.del` file to put next to the segment files.
pub fn materialize_deletion_vector(data: Bytes, max_doc: u32) -> FullTextResult<Bytes> {
    if !data.starts_with(&SPARSE_MAGIC.to_le_bytes()) {
        return Ok(data);
    }
    if data.len() < SparseHeader::ENCODED_LEN + 8 {
        return Err(corrupted());
    }
    let (body, mut checksum) = data.split_at(data.len() - 8);
    if SparseHeader::decode(body)?.max_doc != max_doc || xxh64(body, 0) != checksum.get_u64_le() {
        return Err(corrupted());
    }
    let mut payload = &body[SparseHeader::ENCODED_LEN..];
    let deleted = RoaringBitmap::deserialize_from(&mut payload).map_err(|_| corrupted())?;
    if !payload.is_empty() || deleted.max().is_none_or(|doc| doc >= max_doc) {
        return Err(corrupted());
    }
    let mut alive = BitSet::with_max_value_and_full(max_doc);
    for doc in deleted {
        alive.remove(doc);
    }
    encode_tantivy_delete_file(&alive)
}

fn encode_tantivy_delete_file(alive: &BitSet) -> FullTextResult<Bytes> {
    let ram = RamDirectory::create();
    let directory = ManagedDirectory::wrap(Box::new(ram.clone())).map_err(full_text_error)?;
    let path = Path::new("alive.del");
    let mut writer = directory.open_write(path).map_err(full_text_error)?;
    write_alive_bitset(alive, &mut writer).map_err(full_text_error)?;
    writer.terminate().map_err(full_text_error)?;
    let data = ram
        .open_read(path)
        .map_err(full_text_error)?
        .read_bytes()
        .map_err(full_text_error)?;
    Ok(Bytes::from_owner(data))
}

pub fn read_alive_bitset(index: &Index, path: &Path, max_doc: u32) -> FullTextResult<AliveBitSet> {
    let directory = index.directory();
    if !directory.validate_checksum(path).map_err(full_text_error)? {
        return Err(corrupted());
    }
    let data = directory
        .open_read(path)
        .map_err(full_text_error)?
        .read_bytes()
        .map_err(full_text_error)?;
    if data.len() != dense_len(max_doc) || data.as_slice()[..4] != max_doc.to_le_bytes() {
        return Err(corrupted());
    }
    Ok(AliveBitSet::open(data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_vector_expands_to_the_native_file() {
        let max_doc = 65_537;
        let mut alive = BitSet::with_max_value_and_full(max_doc);
        for doc in [0, 63, 64, 65_536] {
            alive.remove(doc);
        }
        let native = encode_tantivy_delete_file(&alive).unwrap();
        let sparse = encode_deletion_vector(&alive).unwrap();
        assert!(sparse.starts_with(&SPARSE_MAGIC.to_le_bytes()));
        assert!(sparse.len() < native.len() / 2);
        assert_eq!(
            materialize_deletion_vector(sparse.clone(), max_doc).unwrap(),
            native
        );
        assert!(materialize_deletion_vector(sparse.clone(), max_doc - 1).is_err());
        assert!(materialize_deletion_vector(sparse.slice(..4), max_doc).is_err());
        let mut corrupted = sparse.to_vec();
        corrupted[SparseHeader::ENCODED_LEN] ^= 1;
        assert!(materialize_deletion_vector(Bytes::from(corrupted), max_doc).is_err());
    }

    #[test]
    fn dense_vector_keeps_the_native_file() {
        let max_doc = 65_536;
        let mut alive = BitSet::with_max_value_and_full(max_doc);
        for doc in (0..max_doc).step_by(10) {
            alive.remove(doc);
        }
        let dense = encode_deletion_vector(&alive).unwrap();
        assert_eq!(dense, encode_tantivy_delete_file(&alive).unwrap());
        assert_eq!(
            materialize_deletion_vector(dense.clone(), max_doc).unwrap(),
            dense
        );
    }
}
