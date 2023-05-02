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

use std::mem::size_of;

use serde_json::Value;

use super::{Array, ArrayBuilder};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::estimate_size::EstimateSize;
use crate::types::{DataType, JsonbRef, JsonbVal};
use crate::util::iter_util::ZipEqFast;

#[derive(Debug)]
pub struct JsonbArrayBuilder {
    bitmap: BitmapBuilder,
    data: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonbArray {
    bitmap: Bitmap,
    data: Vec<Value>,
}

impl ArrayBuilder for JsonbArrayBuilder {
    type ArrayType = JsonbArray;

    fn new(capacity: usize) -> Self {
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        assert_eq!(ty, DataType::Jsonb);
        Self::new(capacity)
    }

    fn append_n(&mut self, n: usize, value: Option<<Self::ArrayType as Array>::RefItem<'_>>) {
        match value {
            Some(x) => {
                self.bitmap.append_n(n, true);
                self.data
                    .extend(std::iter::repeat(x).take(n).map(|x| x.0.clone()));
            }
            None => {
                self.bitmap.append_n(n, false);
                self.data
                    .extend(std::iter::repeat(*JsonbVal::dummy().0).take(n));
            }
        }
    }

    fn append_array(&mut self, other: &Self::ArrayType) {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.data.extend_from_slice(&other.data);
    }

    fn pop(&mut self) -> Option<()> {
        self.data.pop().map(|_| self.bitmap.pop().unwrap())
    }

    fn finish(self) -> Self::ArrayType {
        Self::ArrayType {
            bitmap: self.bitmap.finish(),
            data: self.data,
        }
    }
}

impl JsonbArrayBuilder {
    pub fn append_move(&mut self, value: JsonbVal) {
        self.bitmap.append(true);
        self.data.push(*value.0);
    }
}

impl Array for JsonbArray {
    type Builder = JsonbArrayBuilder;
    type OwnedItem = JsonbVal;
    type RefItem<'a> = JsonbRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        JsonbRef(self.data.get_unchecked(idx))
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn to_protobuf(&self) -> super::PbArray {
        // The memory layout contains `serde_json::Value` trees, but in protobuf we transmit this as
        // variable length bytes in value encoding. That is, one buffer of length n+1 containing
        // start and end offsets into the 2nd buffer containing all value bytes concatenated.

        use risingwave_pb::common::buffer::CompressionType;
        use risingwave_pb::common::Buffer;

        let mut offset_buffer =
            Vec::<u8>::with_capacity((1 + self.data.len()) * std::mem::size_of::<u64>());
        let mut data_buffer = Vec::<u8>::with_capacity(self.data.len());

        let mut offset = 0;
        for (v, not_null) in self.data.iter().zip_eq_fast(self.null_bitmap().iter()) {
            if !not_null {
                continue;
            }
            let d = JsonbRef(v).value_serialize();
            offset_buffer.extend_from_slice(&(offset as u64).to_be_bytes());
            data_buffer.extend_from_slice(&d);
            offset += d.len();
        }
        offset_buffer.extend_from_slice(&(offset as u64).to_be_bytes());

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
        super::PbArray {
            null_bitmap: Some(null_bitmap),
            values,
            array_type: super::PbArrayType::Jsonb as i32,
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

    fn data_type(&self) -> DataType {
        DataType::Jsonb
    }
}

impl FromIterator<Option<JsonbVal>> for JsonbArray {
    fn from_iter<I: IntoIterator<Item = Option<JsonbVal>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            match i {
                Some(x) => builder.append_move(x),
                None => builder.append(None),
            }
        }
        builder.finish()
    }
}

impl FromIterator<JsonbVal> for JsonbArray {
    fn from_iter<I: IntoIterator<Item = JsonbVal>>(iter: I) -> Self {
        iter.into_iter().map(Some).collect()
    }
}

// TODO: We need to fix this later.
impl EstimateSize for JsonbArray {
    fn estimated_heap_size(&self) -> usize {
        self.bitmap.estimated_heap_size() + self.data.capacity() * size_of::<Value>()
    }
}
