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

use std::iter;
use std::mem::size_of;

use itertools::Itertools;
use risingwave_pb::common::buffer::CompressionType;
use risingwave_pb::common::Buffer;
use risingwave_pb::data::{Array as ProstArray, ArrayType};

use super::{Array, ArrayBuilder, ArrayIterator, ArrayMeta};
use crate::array::ArrayBuilderImpl;
use crate::buffer::{Bitmap, BitmapBuilder};

/// `Utf8Array` is a collection of Rust Utf8 `String`s.
#[derive(Debug, Clone)]
pub struct Utf8Array {
    offset: Vec<usize>,
    bitmap: Bitmap,
    data: Vec<u8>,
}

impl Array for Utf8Array {
    type Builder = Utf8ArrayBuilder;
    type Iter<'a> = ArrayIterator<'a, Self>;
    type OwnedItem = Box<str>;
    type RefItem<'a> = &'a str;

    fn value_at(&self, idx: usize) -> Option<&str> {
        if !self.is_null(idx) {
            let data_slice = &self.data[self.offset[idx]..self.offset[idx + 1]];
            Some(unsafe { std::str::from_utf8_unchecked(data_slice) })
        } else {
            None
        }
    }

    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<&str> {
        if !self.is_null_unchecked(idx) {
            let data_slice = &self.data[self.offset[idx]..self.offset[idx + 1]];
            Some(std::str::from_utf8_unchecked(data_slice))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.offset.len() - 1
    }

    fn iter(&self) -> ArrayIterator<'_, Self> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> ProstArray {
        let offset_buffer = self
            .offset
            .iter()
            // length of offset is n + 1 while the length
            // of null_bitmap is n, chain iterator of null_bitmap
            // with one single true here to push the end of offset
            // to offset_buffer
            .zip_eq(self.null_bitmap().iter().chain(iter::once(true)))
            .fold(
                Vec::<u8>::with_capacity(self.offset.len() * size_of::<usize>()),
                |mut buffer, (offset, not_null)| {
                    // TODO: force convert usize to u64, frontend will treat this offset buffer as
                    // u64
                    if not_null {
                        let offset = *offset as u64;
                        buffer.extend_from_slice(&offset.to_be_bytes());
                    }
                    buffer
                },
            );

        let data_buffer = self.data.clone();

        let values = vec![
            Buffer {
                compression: CompressionType::None as i32,
                body: offset_buffer,
            },
            Buffer {
                compression: CompressionType::None as i32,
                body: data_buffer,
            },
        ];
        let null_bitmap = self.null_bitmap().to_protobuf();
        ProstArray {
            null_bitmap: Some(null_bitmap),
            values,
            array_type: ArrayType::Utf8 as i32,
            struct_array_data: None,
            list_array_data: None,
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.bitmap
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.bitmap = bitmap;
    }

    fn create_builder(&self, capacity: usize) -> ArrayBuilderImpl {
        let array_builder = Utf8ArrayBuilder::new(capacity);
        ArrayBuilderImpl::Utf8(array_builder)
    }
}

impl<'a> FromIterator<Option<&'a str>> for Utf8Array {
    fn from_iter<I: IntoIterator<Item = Option<&'a str>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            builder.append(i);
        }
        builder.finish()
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
    /// Retrieve the ownership of the single string value. Panics if there're multiple or no values.
    pub fn into_single_value(self) -> Option<Box<str>> {
        assert_eq!(self.len(), 1);
        if !self.is_null(0) {
            Some(unsafe { String::from_utf8_unchecked(self.data).into_boxed_str() })
        } else {
            None
        }
    }
}

/// `Utf8ArrayBuilder` use `&str` to build an `Utf8Array`.
#[derive(Debug)]
pub struct Utf8ArrayBuilder {
    offset: Vec<usize>,
    bitmap: BitmapBuilder,
    data: Vec<u8>,
}

impl ArrayBuilder for Utf8ArrayBuilder {
    type ArrayType = Utf8Array;

    fn with_meta(capacity: usize, _meta: ArrayMeta) -> Self {
        let mut offset = Vec::with_capacity(capacity + 1);
        offset.push(0);
        Self {
            offset,
            data: Vec::with_capacity(capacity),
            bitmap: BitmapBuilder::with_capacity(capacity),
        }
    }

    fn append<'a>(&'a mut self, value: Option<&'a str>) {
        match value {
            Some(x) => {
                self.bitmap.append(true);
                self.data.extend_from_slice(x.as_bytes());
                self.offset.push(self.data.len())
            }
            None => {
                self.bitmap.append(false);
                self.offset.push(self.data.len())
            }
        }
    }

    fn append_array(&mut self, other: &Utf8Array) {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.data.extend_from_slice(&other.data);
        let start = *self.offset.last().unwrap();
        for other_offset in &other.offset[1..] {
            self.offset.push(*other_offset + start);
        }
    }

    fn pop(&mut self) -> Option<()> {
        if self.bitmap.pop().is_some() {
            self.offset.pop().unwrap();
            let end = self.offset.last().unwrap();
            self.data.truncate(*end);
            Some(())
        } else {
            None
        }
    }

    fn finish(self) -> Utf8Array {
        Utf8Array {
            bitmap: (self.bitmap).finish(),
            data: self.data,
            offset: self.offset,
        }
    }
}

impl Utf8ArrayBuilder {
    pub fn writer(&mut self) -> StringWriter<'_> {
        StringWriter { builder: self }
    }

    /// `append_partial` will add a partial dirty data of the new record.
    /// The partial data will keep untracked until `finish_partial` was called.
    unsafe fn append_partial(&mut self, x: &str) {
        self.data.extend_from_slice(x.as_bytes());
    }

    /// `finish_partial` will create a new record based on the current dirty data.
    /// `finish_partial` was safe even if we don't call `append_partial`, which
    /// is equivalent to appending an empty string.
    fn finish_partial(&mut self) {
        self.offset.push(self.data.len());
        self.bitmap.append(true);
    }

    /// Rollback the partial-written data by [`Self::append_partial`].
    ///
    /// This is a safe method, if no `append_partial` was called, then the call has no effect.
    fn rollback_partial(&mut self) {
        let &last_offset = self.offset.last().unwrap();
        assert!(last_offset <= self.data.len());
        self.data.truncate(last_offset);
    }
}

pub struct StringWriter<'a> {
    builder: &'a mut Utf8ArrayBuilder,
}

pub struct WrittenGuard(());

impl<'a> StringWriter<'a> {
    /// `write_ref` will consume `StringWriter` and pass the ownership of `builder` to `BytesGuard`.
    pub fn write_ref(self, value: &str) -> WrittenGuard {
        self.builder.append(Some(value));
        WrittenGuard(())
    }

    /// `write_from_char_iter` will consume `StringWriter` and write the characters from the `iter`.
    ///
    /// Prefer [`StringWriter::begin`] for writing multiple string pieces.
    pub fn write_from_char_iter(self, iter: impl Iterator<Item = char>) -> WrittenGuard {
        let mut writer = self.begin();
        for c in iter {
            let mut buf = [0; 4];
            let result = c.encode_utf8(&mut buf);
            writer.write_ref(result);
        }
        writer.finish()
    }

    /// `begin` will create a `PartialStringWriter`, which allow multiple appendings to create a new
    /// record.
    pub fn begin(self) -> PartialStringWriter<'a> {
        PartialStringWriter {
            builder: self.builder,
        }
    }
}

pub struct PartialStringWriter<'a> {
    builder: &'a mut Utf8ArrayBuilder,
}

impl<'a> PartialStringWriter<'a> {
    /// `write_ref` will append partial dirty data to `builder`.
    /// `PartialStringWriter::write_ref` is different from `StringWriter::write_ref`
    /// in that it allows us to call it multiple times.
    pub fn write_ref(&mut self, value: &str) {
        // SAFETY: We'll clean the dirty `builder` in the `drop`.
        unsafe { self.builder.append_partial(value) }
    }

    /// `finish` will be called while the entire record is written.
    /// Exactly one new record was appended and the `builder` can be safely used.
    pub fn finish(self) -> WrittenGuard {
        self.builder.finish_partial();

        WrittenGuard(())
    }
}

impl<'a> Drop for PartialStringWriter<'a> {
    fn drop(&mut self) {
        // If `finish` is not called, we should rollback the data.
        self.builder.rollback_partial();
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
            array.data.len(),
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
