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

use std::fmt::{Display, Write};

use risingwave_pb::data::{ArrayType, PbArray};

use super::bytes_array::{BytesWriter, PartialBytesWriter};
use super::{Array, ArrayBuilder, ArrayMeta, BytesArray, BytesArrayBuilder};
use crate::array::ArrayBuilderImpl;
use crate::buffer::Bitmap;
use crate::collection::estimate_size::EstimateSize;

/// `Utf8Array` is a collection of Rust Utf8 `str`s. It's a wrapper of `BytesArray`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Utf8Array {
    bytes: BytesArray,
}

impl EstimateSize for Utf8Array {
    fn estimated_heap_size(&self) -> usize {
        self.bytes.estimated_heap_size()
    }
}

impl Array for Utf8Array {
    type Builder = Utf8ArrayBuilder;
    type OwnedItem = Box<str>;
    type RefItem<'a> = &'a str;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        let bytes = self.bytes.raw_value_at_unchecked(idx);
        std::str::from_utf8_unchecked(bytes)
    }

    #[inline]
    fn len(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    fn to_protobuf(&self) -> PbArray {
        PbArray {
            array_type: ArrayType::Utf8 as i32,
            ..self.bytes.to_protobuf()
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        self.bytes.null_bitmap()
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.bytes.into_null_bitmap()
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.bytes.set_bitmap(bitmap);
    }

    fn create_builder(&self, capacity: usize) -> ArrayBuilderImpl {
        let array_builder = Utf8ArrayBuilder::new(capacity);
        ArrayBuilderImpl::Utf8(array_builder)
    }
}

impl<'a> FromIterator<Option<&'a str>> for Utf8Array {
    fn from_iter<I: IntoIterator<Item = Option<&'a str>>>(iter: I) -> Self {
        Self {
            bytes: iter.into_iter().map(|s| s.map(|s| s.as_bytes())).collect(),
        }
    }
}

impl<'a> FromIterator<&'a Option<&'a str>> for Utf8Array {
    fn from_iter<I: IntoIterator<Item = &'a Option<&'a str>>>(iter: I) -> Self {
        iter.into_iter().cloned().collect()
    }
}

impl<'a> FromIterator<&'a str> for Utf8Array {
    fn from_iter<I: IntoIterator<Item = &'a str>>(iter: I) -> Self {
        iter.into_iter().map(Some).collect()
    }
}

impl Utf8Array {
    pub fn into_bytes_array(self) -> BytesArray {
        self.bytes
    }

    pub fn from_iter_display(iter: impl IntoIterator<Item = Option<impl Display>>) -> Self {
        let iter = iter.into_iter();
        let mut builder = Utf8ArrayBuilder::new(iter.size_hint().0);
        for e in iter {
            if let Some(s) = e {
                let mut writer = builder.writer().begin();
                write!(writer, "{}", s).unwrap();
                writer.finish();
            } else {
                builder.append_null();
            }
        }
        builder.finish()
    }

    pub(super) fn data(&self) -> &[u8] {
        self.bytes.data()
    }
}

/// `Utf8ArrayBuilder` use `&str` to build an `Utf8Array`.
#[derive(Debug)]
pub struct Utf8ArrayBuilder {
    bytes: BytesArrayBuilder,
}

impl ArrayBuilder for Utf8ArrayBuilder {
    type ArrayType = Utf8Array;

    fn with_meta(capacity: usize, meta: ArrayMeta) -> Self {
        Self {
            bytes: BytesArrayBuilder::with_meta(capacity, meta),
        }
    }

    #[inline]
    fn append_n<'a>(&'a mut self, n: usize, value: Option<&'a str>) {
        self.bytes.append_n(n, value.map(|v| v.as_bytes()));
    }

    #[inline]
    fn append_array(&mut self, other: &Utf8Array) {
        self.bytes.append_array(&other.bytes);
    }

    #[inline]
    fn pop(&mut self) -> Option<()> {
        self.bytes.pop()
    }

    fn finish(self) -> Utf8Array {
        Utf8Array {
            bytes: self.bytes.finish(),
        }
    }
}

impl Utf8ArrayBuilder {
    pub fn writer(&mut self) -> StringWriter<'_> {
        StringWriter {
            bytes: self.bytes.writer(),
        }
    }
}

pub struct StringWriter<'a> {
    bytes: BytesWriter<'a>,
}

impl<'a> StringWriter<'a> {
    /// `begin` will create a `PartialStringWriter`, which allow multiple appendings to create a new
    /// record.
    pub fn begin(self) -> PartialStringWriter<'a> {
        PartialStringWriter {
            bytes: self.bytes.begin(),
        }
    }
}

// Note: dropping an unfinished `PartialStringWriter` will rollback the partial data, which is the
// behavior of the inner `PartialBytesWriter`.
pub struct PartialStringWriter<'a> {
    bytes: PartialBytesWriter<'a>,
}

impl<'a> PartialStringWriter<'a> {
    /// `finish` will be called while the entire record is written.
    /// Exactly one new record was appended and the `builder` can be safely used.
    pub fn finish(self) {
        self.bytes.finish()
    }
}

impl Write for PartialStringWriter<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.bytes.write_ref(s.as_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use itertools::Itertools;

    use super::*;
    use crate::array::NULL_VAL_FOR_HASH;
    use crate::util::iter_util::ZipEqFast;

    #[test]
    fn test_utf8_builder() {
        let mut builder = Utf8ArrayBuilder::new(0);
        for i in 0..100 {
            if i % 2 == 0 {
                builder.append(Some(&format!("{}", i)));
            } else {
                builder.append(None);
            }
        }
        builder.finish();
    }

    #[test]
    fn test_utf8_partial_writer() {
        let mut builder = Utf8ArrayBuilder::new(0);
        {
            let writer = builder.writer();
            let mut partial_writer = writer.begin();
            for _ in 0..2 {
                partial_writer.write_str("ran").unwrap();
            }
            partial_writer.finish()
        };
        let array = builder.finish();
        assert_eq!(array.len(), 1);
        assert_eq!(array.value_at(0), Some("ranran"));
        assert_eq!(unsafe { array.value_at_unchecked(0) }, Some("ranran"));
    }

    #[test]
    fn test_utf8_partial_writer_failed() {
        let mut builder = Utf8ArrayBuilder::new(0);
        // Write a record.
        {
            let writer = builder.writer();
            let mut partial_writer = writer.begin();
            partial_writer.write_str("Dia").unwrap();
            partial_writer.write_str("na").unwrap();
            partial_writer.finish()
        };

        // Write a record failed.
        {
            let writer = builder.writer();
            let mut partial_writer = writer.begin();
            partial_writer.write_str("Ca").unwrap();
            partial_writer.write_str("rol").unwrap();
            // We don't finish here.
        };

        // Write a record.
        {
            let writer = builder.writer();
            let mut partial_writer = writer.begin();
            partial_writer.write_str("Ki").unwrap();
            partial_writer.write_str("ra").unwrap();
            partial_writer.finish()
        };

        // Verify only two valid records.
        let array = builder.finish();
        assert_eq!(array.len(), 2);
        assert_eq!(array.value_at(0), Some("Diana"));
        assert_eq!(array.value_at(1), Some("Kira"));
    }

    #[test]
    fn test_utf8_array() {
        let input = vec![
            Some("1"),
            Some("22"),
            None,
            Some("4444"),
            None,
            Some("666666"),
        ];

        let array = Utf8Array::from_iter(&input);
        assert_eq!(array.len(), input.len());

        assert_eq!(
            array.bytes.data().len(),
            input.iter().map(|s| s.unwrap_or("").len()).sum::<usize>()
        );

        assert_eq!(input, array.iter().collect_vec());
    }

    #[test]
    fn test_utf8_array_to_protobuf() {
        let input = vec![
            Some("1"),
            Some("22"),
            None,
            Some("4444"),
            None,
            Some("666666"),
        ];

        let array = Utf8Array::from_iter(&input);
        let buffers = array.to_protobuf().values;
        assert!(buffers.len() >= 2);
    }

    #[test]
    fn test_utf8_array_hash() {
        use std::hash::BuildHasher;

        use twox_hash::RandomXxHashBuilder64;

        use super::super::test_util::{hash_finish, test_hash};

        const ARR_NUM: usize = 3;
        const ARR_LEN: usize = 90;
        let vecs: [Vec<Option<&str>>; ARR_NUM] = [
            (0..ARR_LEN)
                .map(|x| match x % 2 {
                    0 => Some("1"),
                    1 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 3 {
                    0 => Some("1"),
                    1 => Some("abc"),
                    2 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 5 {
                    0 => Some("1"),
                    1 => Some("abc"),
                    2 => None,
                    3 => Some("ABCDEF"),
                    4 => Some("666666"),
                    _ => unreachable!(),
                })
                .collect_vec(),
        ];

        let arrs = vecs.iter().map(Utf8Array::from_iter).collect_vec();

        let hasher_builder = RandomXxHashBuilder64::default();
        let mut states = vec![hasher_builder.build_hasher(); ARR_LEN];
        vecs.iter().for_each(|v| {
            v.iter()
                .zip_eq_fast(&mut states)
                .for_each(|(x, state)| match x {
                    Some(inner) => inner.hash(state),
                    None => NULL_VAL_FOR_HASH.hash(state),
                })
        });
        let hashes = hash_finish(&mut states[..]);

        let count = hashes.iter().counts().len();
        assert_eq!(count, 30);

        test_hash(arrs, hashes, hasher_builder);
    }
}
