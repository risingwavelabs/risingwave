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

#[derive(Debug, Clone)]
/// The layout of `BytesArray` is the same as `Utf8Array`. Now the `BytesGuard` and `BytesWriter` is
/// not added yet, can add it when we need to support corresponding expression.
pub struct BytesArray {
    offset: Vec<usize>,
    bitmap: Bitmap,
    data: Vec<u8>,
}

impl Array for BytesArray {
    type Builder = BytesArrayBuilder;
    type Iter<'a> = ArrayIterator<'a, Self>;
    type OwnedItem = Vec<u8>;
    type RefItem<'a> = &'a [u8];

    fn value_at(&self, idx: usize) -> Option<&[u8]> {
        if !self.is_null(idx) {
            let data_slice = &self.data[self.offset[idx]..self.offset[idx + 1]];
            Some(data_slice)
        } else {
            None
        }
    }

    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<&[u8]> {
        if !self.is_null_unchecked(idx) {
            let data_slice = &self.data[self.offset[idx]..self.offset[idx + 1]];
            Some(data_slice)
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
            // of null_bitmap is n, chain iterator of null_bitmapƒ
            // with one single true here to push the end of offset
            // to offset_buffer
            .zip_eq(self.null_bitmap().iter().chain(iter::once(true)))
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
        ProstArray {
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
}

impl BytesArray {
    pub fn from_slice(data: &[Option<&[u8]>]) -> Self {
        let mut builder = <Self as Array>::Builder::new(data.len());
        for i in data {
            builder.append(*i);
        }
        builder.finish()
    }
}

/// `BytesArrayBuilder` use `&[u8]` to build an `BytesArray`.
#[derive(Debug)]
pub struct BytesArrayBuilder {
    offset: Vec<usize>,
    bitmap: BitmapBuilder,
    data: Vec<u8>,
}

impl ArrayBuilder for BytesArrayBuilder {
    type ArrayType = BytesArray;

    fn with_meta(capacity: usize, _meta: ArrayMeta) -> Self {
        let mut offset = Vec::with_capacity(capacity + 1);
        offset.push(0);
        Self {
            offset,
            data: Vec::with_capacity(capacity),
            bitmap: BitmapBuilder::with_capacity(capacity),
        }
    }

    fn append<'a>(&'a mut self, value: Option<&'a [u8]>) {
        match value {
            Some(x) => {
                self.bitmap.append(true);
                self.data.extend_from_slice(x);
                self.offset.push(self.data.len())
            }
            None => {
                self.bitmap.append(false);
                self.offset.push(self.data.len())
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
            self.data.truncate(*end);
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
