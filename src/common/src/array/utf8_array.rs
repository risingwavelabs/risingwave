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

use std::fmt::{Display, Write};

use risingwave_pb::data::{Array as ProstArray, ArrayType};

use super::bytes_array::{BytesWriter, PartialBytesWriter, WrittenGuard};
use super::iterator::ArrayRawIter;
use super::{Array, ArrayBuilder, ArrayIterator, ArrayMeta, BytesArray, BytesArrayBuilder};
use crate::array::ArrayBuilderImpl;
use crate::buffer::Bitmap;

/// `Utf8Array` is a collection of Rust Utf8 `str`s. It's a wrapper of `BytesArray`.
#[derive(Debug, Clone)]
pub struct Utf8Array {
    bytes: BytesArray,
}

impl Array for Utf8Array {
    type Builder = Utf8ArrayBuilder;
    type Iter<'a> = ArrayIterator<'a, Self>;
    type OwnedItem = Box<str>;
    type RawIter<'a> = ArrayRawIter<'a, Self>;
    type RefItem<'a> = &'a str;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        let bytes = self.bytes.raw_value_at_unchecked(idx);
        std::str::from_utf8_unchecked(bytes)
    }

    fn value_at(&self, idx: usize) -> Option<&str> {
        self.bytes
            .value_at(idx)
            .map(|bytes| unsafe { std::str::from_utf8_unchecked(bytes) })
    }

    #[inline]
    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<&str> {
        self.bytes
            .value_at_unchecked(idx)
            .map(|bytes| unsafe { std::str::from_utf8_unchecked(bytes) })
    }

    #[inline]
    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn iter(&self) -> ArrayIterator<'_, Self> {
        ArrayIterator::new(self)
    }

    fn raw_iter(&self) -> Self::RawIter<'_> {
        ArrayRawIter::new(self)
    }

    #[inline]
    fn to_protobuf(&self) -> ProstArray {
        ProstArray {
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
    /// Retrieve the ownership of the single string value.
    ///
    /// Panics if there're multiple or no values.
    #[inline]
    pub fn into_single_value(self) -> Option<Box<str>> {
        self.bytes
            .into_single_value()
            .map(|bytes| unsafe { std::str::from_boxed_utf8_unchecked(bytes) })
    }

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
    /// `write_ref` will consume `StringWriter` and pass the ownership of `builder` to `BytesGuard`.
    #[inline]
    pub fn write_ref(self, value: &str) -> WrittenGuard {
        self.bytes.write_ref(value.as_bytes())
    }

    /// `write_from_char_iter` will consume `StringWriter` and write the characters from the `iter`.
    ///
    /// Prefer [`StringWriter::begin`] for writing multiple string pieces.
    pub fn write_from_char_iter(self, iter: impl Iterator<Item = char>) -> WrittenGuard {
        let mut writer = self.begin();
        for c in iter {
            writer.write_char(c).unwrap();
        }
        writer.finish()
    }

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
    /// `write_ref` will append partial dirty data to `builder`.
    /// `PartialStringWriter::write_ref` is different from `StringWriter::write_ref`
    /// in that it allows us to call it multiple times.
    #[inline]
    pub fn write_ref(&mut self, value: &str) {
        self.bytes.write_ref(value.as_bytes());
    }

    /// `finish` will be called while the entire record is written.
    /// Exactly one new record was appended and the `builder` can be safely used.
    pub fn finish(self) -> WrittenGuard {
        self.bytes.finish()
    }
}

impl Write for PartialStringWriter<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.write_ref(s);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use itertools::Itertools;

    use super::*;
    use crate::array::NULL_VAL_FOR_HASH;

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
        let _guard: WrittenGuard = {
            let writer = builder.writer();
            let mut partial_writer = writer.begin();
            for _ in 0..2 {
                partial_writer.write_ref("ran");
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
        let _guard: WrittenGuard = {
            let writer = builder.writer();
            let mut partial_writer = writer.begin();
            partial_writer.write_ref("Dia");
            partial_writer.write_ref("na");
            partial_writer.finish()
        };

        // Write a record failed.
        let _maybe_guard: Option<WrittenGuard> = {
            let writer = builder.writer();
            let mut partial_writer = writer.begin();
            partial_writer.write_ref("Ca");
            partial_writer.write_ref("rol");

            // We don't finish here.
            None
        };

        // Write a record.
        let _guard: WrittenGuard = {
            let writer = builder.writer();
            let mut partial_writer = writer.begin();
            partial_writer.write_ref("Ki");
            partial_writer.write_ref("ra");
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
            v.iter().zip_eq(&mut states).for_each(|(x, state)| match x {
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
