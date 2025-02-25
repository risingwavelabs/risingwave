// Copyright 2025 RisingWave Labs
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

use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::{PbArray, PbArrayType};

use super::{Array, ArrayBuilder, ArrayImpl, ArrayResult};
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::types::{DataType, JsonbRef, JsonbVal, Scalar};

#[derive(Debug, Clone, EstimateSize)]
pub struct JsonbArrayBuilder {
    bitmap: BitmapBuilder,
    builder: jsonbb::Builder,
}

#[derive(Debug, Clone, PartialEq, Eq, EstimateSize)]
pub struct JsonbArray {
    bitmap: Bitmap,
    /// Elements are stored as a single JSONB array value.
    data: jsonbb::Value,
}

impl ArrayBuilder for JsonbArrayBuilder {
    type ArrayType = JsonbArray;

    fn new(capacity: usize) -> Self {
        let mut builder = jsonbb::Builder::with_capacity(capacity);
        builder.begin_array();
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            builder,
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
                for _ in 0..n {
                    self.builder.add_value(x.0);
                }
            }
            None => {
                self.bitmap.append_n(n, false);
                for _ in 0..n {
                    self.builder.add_null();
                }
            }
        }
    }

    fn append_array(&mut self, other: &Self::ArrayType) {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        for value in other.data.as_array().unwrap().iter() {
            self.builder.add_value(value);
        }
    }

    fn pop(&mut self) -> Option<()> {
        self.bitmap.pop()?;
        self.builder.pop();
        Some(())
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn finish(mut self) -> Self::ArrayType {
        self.builder.end_array();
        Self::ArrayType {
            bitmap: self.bitmap.finish(),
            data: self.builder.finish(),
        }
    }
}

impl JsonbArray {
    /// Loads a `JsonbArray` from a protobuf array.
    ///
    /// See also `JsonbArray::to_protobuf`.
    pub fn from_protobuf(array: &PbArray) -> ArrayResult<ArrayImpl> {
        ensure!(
            array.values.len() == 1,
            "Must have exactly 1 buffer in a jsonb array"
        );
        let arr = JsonbArray {
            bitmap: array.get_null_bitmap()?.into(),
            data: jsonbb::Value::from_bytes(&array.values[0].body),
        };
        Ok(arr.into())
    }
}

impl Array for JsonbArray {
    type Builder = JsonbArrayBuilder;
    type OwnedItem = JsonbVal;
    type RefItem<'a> = JsonbRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        JsonbRef(self.data.as_array().unwrap().get(idx).unwrap())
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn to_protobuf(&self) -> PbArray {
        use risingwave_pb::common::Buffer;
        use risingwave_pb::common::buffer::CompressionType;

        PbArray {
            null_bitmap: Some(self.null_bitmap().to_protobuf()),
            values: vec![Buffer {
                compression: CompressionType::None as i32,
                body: self.data.as_bytes().to_vec(),
            }],
            array_type: PbArrayType::Jsonb as i32,
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
                Some(x) => builder.append(Some(x.as_scalar_ref())),
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
