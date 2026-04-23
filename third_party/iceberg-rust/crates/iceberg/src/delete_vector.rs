// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Delete vector types and Puffin serialization support.

use std::collections::HashMap;
use std::io::Cursor;
use std::ops::BitOrAssign;

use crc32fast::Hasher;
use roaring::RoaringTreemap;
use roaring::bitmap::Iter;
use roaring::treemap::BitmapIter;

use crate::puffin::{Blob, DELETION_VECTOR_V1};
use crate::{Error, ErrorKind, Result};

const DELETION_VECTOR_MAGIC_BYTES: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
const MIN_SERIALIZED_DELETION_VECTOR_BLOB: usize = 12;

/// Puffin blob property for deletion vector cardinality.
pub(crate) const DELETION_VECTOR_PROPERTY_CARDINALITY: &str = "cardinality";
/// Puffin blob property for referenced data file path.
pub(crate) const DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE: &str = "referenced-data-file";

/// A set of deleted row positions backed by a `RoaringTreemap`.
#[derive(Debug, Default)]
pub struct DeleteVector {
    inner: RoaringTreemap,
}

impl DeleteVector {
    /// Creates a delete vector from an existing roaring treemap.
    pub fn new(roaring_treemap: RoaringTreemap) -> DeleteVector {
        DeleteVector {
            inner: roaring_treemap,
        }
    }

    /// Returns an iterator over deleted row positions in ascending order.
    pub fn iter(&self) -> DeleteVectorIterator<'_> {
        let outer = self.inner.bitmaps();
        DeleteVectorIterator { outer, inner: None }
    }

    /// Marks a single row position as deleted.
    ///
    /// Returns `true` when the position was not already present.
    pub fn insert(&mut self, pos: u64) -> bool {
        self.inner.insert(pos)
    }

    /// Marks the given `positions` as deleted and returns the number of elements appended.
    ///
    /// The input slice must be strictly ordered in ascending order, and every value must be greater than all existing values already in the set.
    ///
    /// # Errors
    ///
    /// Returns an error if the precondition is not met.
    pub fn insert_positions(&mut self, positions: &[u64]) -> Result<usize> {
        if let Err(err) = self.inner.append(positions.iter().copied()) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "failed to marks rows as deleted".to_string(),
            )
            .with_source(err));
        }
        Ok(positions.len())
    }

    /// Returns `true` if there are no deleted positions in this vector.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of deleted row positions in the vector.
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    /// Serialize this delete vector into a Puffin deletion vector blob.
    pub fn to_puffin_blob(&self, properties: HashMap<String, String>) -> Result<Blob> {
        Self::check_properties(&properties)?;

        let serialized_bitmap_size = self.inner.serialized_size();
        let combined_length = (DELETION_VECTOR_MAGIC_BYTES.len() + serialized_bitmap_size) as u32;
        let mut data = Vec::with_capacity(
            std::mem::size_of_val(&combined_length)
                + DELETION_VECTOR_MAGIC_BYTES.len()
                + serialized_bitmap_size
                + 4, // the length of the CRC
        );

        data.extend_from_slice(&combined_length.to_be_bytes());
        data.extend_from_slice(&DELETION_VECTOR_MAGIC_BYTES);

        let bitmap_start = data.len();
        data.resize(bitmap_start + serialized_bitmap_size, 0);
        {
            let mut cursor = std::io::Cursor::new(&mut data[bitmap_start..]);
            self.inner.serialize_into(&mut cursor).map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "failed to serialize deletion vector bitmap".to_string(),
                )
                .with_source(err)
            })?;
        }

        let mut hasher = Hasher::new();
        hasher.update(&data[4..]);
        let crc = hasher.finalize();
        data.extend_from_slice(&crc.to_be_bytes());

        Ok(Blob::builder()
            .r#type(DELETION_VECTOR_V1.to_string())
            .fields(vec![])
            .snapshot_id(-1)
            .sequence_number(-1)
            .data(data)
            .properties(properties)
            .build())
    }

    /// Deserialize a delete vector from a Puffin deletion vector blob.
    pub fn from_puffin_blob(blob: Blob) -> Result<Self> {
        if blob.blob_type() != DELETION_VECTOR_V1 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("unsupported puffin blob type: {}", blob.blob_type()),
            ));
        }

        let data = blob.data();
        if data.len() < MIN_SERIALIZED_DELETION_VECTOR_BLOB {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "serialized deletion vector blob too small".to_string(),
            ));
        }

        let magic = &data[4..8];
        if magic != DELETION_VECTOR_MAGIC_BYTES {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "invalid deletion vector magic bytes".to_string(),
            ));
        }

        let combined_length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let expected_len = std::mem::size_of_val(&combined_length) + combined_length as usize + 4;
        if expected_len != data.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "serialized deletion vector length mismatch: expected {expected_len}, actual {}",
                    data.len()
                ),
            ));
        }

        let bitmap_start = 8;
        let bitmap_end = data.len() - 4;
        let bitmap_data = &data[bitmap_start..bitmap_end];

        let mut hasher = Hasher::new();
        hasher.update(&data[4..bitmap_end]);
        let expected_crc = hasher.finalize();
        let stored_crc = u32::from_be_bytes([
            data[data.len() - 4],
            data[data.len() - 3],
            data[data.len() - 2],
            data[data.len() - 1],
        ]);
        if expected_crc != stored_crc {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("deletion vector crc mismatch: expected {expected_crc}, got {stored_crc}"),
            ));
        }

        let bitmap =
            RoaringTreemap::deserialize_from(&mut Cursor::new(bitmap_data)).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "failed to deserialize deletion vector bitmap".to_string(),
                )
                .with_source(err)
            })?;

        Ok(DeleteVector::new(bitmap))
    }

    fn check_properties(properties: &HashMap<String, String>) -> Result<()> {
        if !properties.contains_key(DELETION_VECTOR_PROPERTY_CARDINALITY) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector properties must include {DELETION_VECTOR_PROPERTY_CARDINALITY}"
                ),
            ));
        }
        if !properties.contains_key(DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector properties must include {DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE}"
                ),
            ));
        }
        Ok(())
    }
}

// Ideally, we'd just wrap `roaring::RoaringTreemap`'s iterator, `roaring::treemap::Iter` here.
// But right now, it does not have a corresponding implementation of `roaring::bitmap::Iter::advance_to`,
// which is very handy in ArrowReader::build_deletes_row_selection.
// There is a PR open on roaring to add this (https://github.com/RoaringBitmap/roaring-rs/pull/314)
// and if that gets merged then we can simplify `DeleteVectorIterator` here, refactoring `advance_to`
// to just a wrapper around the underlying iterator's method.
/// An iterator over deleted row positions.
pub struct DeleteVectorIterator<'a> {
    // NB: `BitMapIter` was only exposed publicly in https://github.com/RoaringBitmap/roaring-rs/pull/316
    // which is not yet released. As a consequence our Cargo.toml temporarily uses a git reference for
    // the roaring dependency.
    outer: BitmapIter<'a>,
    inner: Option<DeleteVectorIteratorInner<'a>>,
}

struct DeleteVectorIteratorInner<'a> {
    high_bits: u32,
    bitmap_iter: Iter<'a>,
}

impl Iterator for DeleteVectorIterator<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = &mut self.inner
            && let Some(inner_next) = inner.bitmap_iter.next()
        {
            return Some(u64::from(inner.high_bits) << 32 | u64::from(inner_next));
        }

        if let Some((high_bits, next_bitmap)) = self.outer.next() {
            self.inner = Some(DeleteVectorIteratorInner {
                high_bits,
                bitmap_iter: next_bitmap.iter(),
            })
        } else {
            return None;
        }

        self.next()
    }
}

impl DeleteVectorIterator<'_> {
    /// Advances the iterator to the first position greater than or equal to `pos`.
    pub fn advance_to(&mut self, pos: u64) {
        let hi = (pos >> 32) as u32;
        let lo = pos as u32;

        let Some(ref mut inner) = self.inner else {
            return;
        };

        while inner.high_bits < hi {
            let Some((next_hi, next_bitmap)) = self.outer.next() else {
                return;
            };

            *inner = DeleteVectorIteratorInner {
                high_bits: next_hi,
                bitmap_iter: next_bitmap.iter(),
            }
        }

        inner.bitmap_iter.advance_to(lo);
    }
}

impl BitOrAssign for DeleteVector {
    fn bitor_assign(&mut self, other: Self) {
        self.inner.bitor_assign(&other.inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insertion_and_iteration() {
        let mut dv = DeleteVector::default();
        assert!(dv.insert(42));
        assert!(dv.insert(100));
        assert!(!dv.insert(42));

        let mut items: Vec<u64> = dv.iter().collect();
        items.sort();
        assert_eq!(items, vec![42, 100]);
        assert_eq!(dv.len(), 2);
    }

    #[test]
    fn test_successful_insert_positions() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 2, 3, 1000, 1 << 33];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 5);

        let mut collected: Vec<u64> = dv.iter().collect();
        collected.sort();
        assert_eq!(collected, positions);
    }

    /// Testing scenario: bulk insertion fails because input positions are not strictly increasing.
    #[test]
    fn test_failed_insertion_unsorted_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 4];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have intersection with existing ones.
    #[test]
    fn test_failed_insertion_with_intersection() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 3);

        let res = dv.insert_positions(&[2, 4]);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have duplicates.
    #[test]
    fn test_failed_insertion_duplicate_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 5];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }
}
