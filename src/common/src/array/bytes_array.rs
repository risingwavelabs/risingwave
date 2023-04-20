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

use std::iter;
use std::mem::size_of;

use risingwave_pb::common::buffer::CompressionType;
use risingwave_pb::common::Buffer;
use risingwave_pb::data::{ArrayType, PbArray};

use super::{Array, ArrayBuilder, DataType};
use crate::array::ArrayBuilderImpl;
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::estimate_size::EstimateSize;
use crate::util::iter_util::ZipEqDebug;

/// `BytesArray` is a collection of Rust `[u8]`s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BytesArray {
    offset: Vec<u32>,
    bitmap: Bitmap,
    data: Vec<u8>,
}

impl EstimateSize for BytesArray {
    fn estimated_heap_size(&self) -> usize {
        self.offset.capacity() * size_of::<u32>()
            + self.bitmap.estimated_heap_size()
            + self.data.capacity()
    }
}

impl Array for BytesArray {
    type Builder = BytesArrayBuilder;
    type OwnedItem = Box<[u8]>;
    type RefItem<'a> = &'a [u8];

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> &[u8] {
        let begin = *self.offset.get_unchecked(idx) as usize;
        let end = *self.offset.get_unchecked(idx + 1) as usize;
        self.data.get_unchecked(begin..end)
    }

    fn len(&self) -> usize {
        self.offset.len() - 1
    }

    fn to_protobuf(&self) -> PbArray {
        let offset_buffer = self
            .offset
            .iter()
            // length of offset is n + 1 while the length
            // of null_bitmap is n, chain iterator of null_bitmapƒ
            // with one single true here to push the end of offset
            // to offset_buffer
            .zip_eq_debug(self.null_bitmap().iter().chain(iter::once(true)))
            .fold(
                Vec::<u8>::with_capacity(self.data.len() * size_of::<usize>()),
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
        PbArray {
            null_bitmap: Some(null_bitmap),
            values,
            array_type: ArrayType::Bytea as i32,
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
        let array_builder = BytesArrayBuilder::new(capacity);
        ArrayBuilderImpl::Bytea(array_builder)
    }

    fn data_type(&self) -> DataType {
        DataType::Bytea
    }
}

impl BytesArray {
    pub(super) fn data(&self) -> &[u8] {
        &self.data
    }
}

impl<'a> FromIterator<Option<&'a [u8]>> for BytesArray {
    fn from_iter<I: IntoIterator<Item = Option<&'a [u8]>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            builder.append(i);
        }
        builder.finish()
    }
}

impl<'a> FromIterator<&'a Option<&'a [u8]>> for BytesArray {
    fn from_iter<I: IntoIterator<Item = &'a Option<&'a [u8]>>>(iter: I) -> Self {
        iter.into_iter().cloned().collect()
    }
}

impl<'a> FromIterator<&'a [u8]> for BytesArray {
    fn from_iter<I: IntoIterator<Item = &'a [u8]>>(iter: I) -> Self {
        iter.into_iter().map(Some).collect()
    }
}

/// `BytesArrayBuilder` use `&[u8]` to build an `BytesArray`.
#[derive(Debug)]
pub struct BytesArrayBuilder {
    offset: Vec<u32>,
    bitmap: BitmapBuilder,
    data: Vec<u8>,
}

impl ArrayBuilder for BytesArrayBuilder {
    type ArrayType = BytesArray;

    fn new(capacity: usize) -> Self {
        let mut offset = Vec::with_capacity(capacity + 1);
        offset.push(0);
        Self {
            offset,
            data: Vec::with_capacity(capacity),
            bitmap: BitmapBuilder::with_capacity(capacity),
        }
    }

    fn with_meta(capacity: usize, meta: DataType) -> Self {
        assert_eq!(meta, DataType::Bytea);
        Self::new(capacity)
    }

    fn append_n<'a>(&'a mut self, n: usize, value: Option<&'a [u8]>) {
        match value {
            Some(x) => {
                self.bitmap.append_n(n, true);
                self.data.reserve(x.len() * n);
                self.offset.reserve(n);
                assert!(self.data.capacity() <= u32::MAX as usize);
                for _ in 0..n {
                    self.data.extend_from_slice(x);
                    self.offset.push(self.data.len() as u32);
                }
            }
            None => {
                self.bitmap.append_n(n, false);
                self.offset.reserve(n);
                for _ in 0..n {
                    self.offset.push(self.data.len() as u32);
                }
            }
        }
    }

    fn append_array(&mut self, other: &BytesArray) {
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
            self.data.truncate(*end as usize);
            Some(())
        } else {
            None
        }
    }

    fn finish(self) -> BytesArray {
        BytesArray {
            bitmap: (self.bitmap).finish(),
            data: self.data,
            offset: self.offset,
        }
    }
}

impl BytesArrayBuilder {
    pub fn writer(&mut self) -> BytesWriter<'_> {
        BytesWriter { builder: self }
    }

    /// `append_partial` will add a partial dirty data of the new record.
    /// The partial data will keep untracked until `finish_partial` was called.
    unsafe fn append_partial(&mut self, x: &[u8]) {
        self.data.extend_from_slice(x);
    }

    /// `finish_partial` will create a new record based on the current dirty data.
    /// `finish_partial` was safe even if we don't call `append_partial`, which is equivalent to
    /// appending an empty bytes.
    fn finish_partial(&mut self) {
        self.offset.push(self.data.len() as u32);
        self.bitmap.append(true);
    }

    /// Rollback the partial-written data by [`Self::append_partial`].
    ///
    /// This is a safe method, if no `append_partial` was called, then the call has no effect.
    fn rollback_partial(&mut self) {
        let &last_offset = self.offset.last().unwrap();
        assert!(last_offset <= self.data.len() as u32);
        self.data.truncate(last_offset as usize);
    }
}

pub struct BytesWriter<'a> {
    builder: &'a mut BytesArrayBuilder,
}

impl<'a> BytesWriter<'a> {
    /// `write_ref` will consume `BytesWriter` and pass the ownership of `builder` to `BytesGuard`.
    pub fn write_ref(self, value: &[u8]) {
        self.builder.append(Some(value));
    }

    /// `begin` will create a `PartialBytesWriter`, which allow multiple appendings to create a new
    /// record.
    pub fn begin(self) -> PartialBytesWriter<'a> {
        PartialBytesWriter {
            builder: self.builder,
        }
    }
}

pub struct PartialBytesWriter<'a> {
    builder: &'a mut BytesArrayBuilder,
}

impl<'a> PartialBytesWriter<'a> {
    /// `write_ref` will append partial dirty data to `builder`.
    /// `PartialBytesWriter::write_ref` is different from `BytesWriter::write_ref`
    /// in that it allows us to call it multiple times.
    pub fn write_ref(&mut self, value: &[u8]) {
        // SAFETY: We'll clean the dirty `builder` in the `drop`.
        unsafe { self.builder.append_partial(value) }
    }

    /// `finish` will be called while the entire record is written.
    /// Exactly one new record was appended and the `builder` can be safely used.
    pub fn finish(self) {
        self.builder.finish_partial();
    }
}

impl<'a> Drop for PartialBytesWriter<'a> {
    fn drop(&mut self) {
        // If `finish` is not called, we should rollback the data.
        self.builder.rollback_partial();
    }
}
