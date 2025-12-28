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

use std::fmt::Debug;
use std::mem::size_of;

use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::common::Buffer;
use risingwave_pb::common::buffer::CompressionType;
use risingwave_pb::data::{ArrayType, PbArray};

use super::{Array, ArrayBuilder, ArrayImpl};
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::types::{DataType, Decimal};

macro_rules! impl_array_methods {
    ($scalar_type:ty, $array_type_pb:ident, $array_impl_variant:ident) => {
        const DATA_TYPE: DataType = DataType::$array_impl_variant;

        pub fn erase_array_type(arr: DecimalArray) -> ArrayImpl {
            ArrayImpl::$array_impl_variant(arr)
        }

        pub fn try_into_array(arr: ArrayImpl) -> Option<DecimalArray> {
            match arr {
                ArrayImpl::$array_impl_variant(inner) => Some(inner),
                _ => None,
            }
        }

        pub fn try_into_array_ref(arr: &ArrayImpl) -> Option<&DecimalArray> {
            match arr {
                ArrayImpl::$array_impl_variant(inner) => Some(inner),
                _ => None,
            }
        }

        pub fn array_type() -> ArrayType {
            ArrayType::$array_type_pb
        }
    };
}

impl Decimal {
    impl_array_methods!(Decimal, Decimal, Decimal);

    // fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
    //     Decimal::to_protobuf(self, output)
    // }
    // fn from_protobuf(cur: &mut Cursor<&[u8]>) -> ArrayResult<Self> {
    //     Decimal::from_protobuf(cur)
    // }
}

/// `PrimitiveArray` is a collection of primitive types, such as `i32`, `f32`.
#[derive(Debug, Clone, PartialEq, Eq, EstimateSize)]
pub struct DecimalArray {
    bitmap: Bitmap,
    data: Box<[Decimal]>,
}

impl FromIterator<Option<Decimal>> for DecimalArray {
    fn from_iter<I: IntoIterator<Item = Option<Decimal>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            builder.append(i);
        }
        builder.finish()
    }
}

impl<'a> FromIterator<&'a Option<Decimal>> for DecimalArray {
    fn from_iter<I: IntoIterator<Item = &'a Option<Decimal>>>(iter: I) -> Self {
        iter.into_iter().cloned().collect()
    }
}

impl FromIterator<Decimal> for DecimalArray {
    fn from_iter<I: IntoIterator<Item = Decimal>>(iter: I) -> Self {
        let data: Box<[Decimal]> = iter.into_iter().collect();
        DecimalArray {
            bitmap: Bitmap::ones(data.len()),
            data,
        }
    }
}

impl DecimalArray {
    /// Build a [`DecimalArray`] from iterator and bitmap.
    ///
    /// NOTE: The length of `bitmap` must be equal to the length of `iter`.
    pub fn from_iter_bitmap(iter: impl IntoIterator<Item = Decimal>, bitmap: Bitmap) -> Self {
        let data: Box<[Decimal]> = iter.into_iter().collect();
        assert_eq!(data.len(), bitmap.len());
        DecimalArray { bitmap, data }
    }

    /// Returns a slice containing the entire array.
    pub fn as_slice(&self) -> &[Decimal] {
        &self.data
    }

    /// Returns a mutable slice containing the entire array.
    pub fn as_mut_slice(&mut self) -> &mut [Decimal] {
        &mut self.data
    }
}

impl Array for DecimalArray {
    type Builder = DecimalArrayBuilder;
    type OwnedItem = Decimal;
    type RefItem<'a> = Decimal;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        unsafe { *self.data.get_unchecked(idx) }
    }

    fn raw_iter(&self) -> impl ExactSizeIterator<Item = Self::RefItem<'_>> {
        self.data.iter().cloned()
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn to_protobuf(&self) -> PbArray {
        let mut output_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<Decimal>());

        for v in self.iter() {
            v.map(|node| node.to_protobuf(&mut output_buffer));
        }

        let buffer = Buffer {
            compression: CompressionType::None as i32,
            body: output_buffer,
        };
        let null_bitmap = self.null_bitmap().to_protobuf();
        PbArray {
            null_bitmap: Some(null_bitmap),
            values: vec![buffer],
            array_type: Decimal::array_type() as i32,
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
        Decimal::DATA_TYPE
    }
}

/// `PrimitiveArrayBuilder` constructs a `PrimitiveArray` from `Option<Primitive>`.
#[derive(Debug, Clone, EstimateSize)]
pub struct DecimalArrayBuilder {
    bitmap: BitmapBuilder,
    data: Vec<Decimal>,
}

impl ArrayBuilder for DecimalArrayBuilder {
    type ArrayType = DecimalArray;

    fn new(capacity: usize) -> Self {
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        assert_eq!(ty, Decimal::DATA_TYPE);
        Self::new(capacity)
    }

    fn append_n(&mut self, n: usize, value: Option<Decimal>) {
        match value {
            Some(x) => {
                self.bitmap.append_n(n, true);
                self.data.extend(std::iter::repeat_n(x, n));
            }
            None => {
                self.bitmap.append_n(n, false);
                self.data.extend(std::iter::repeat_n(Decimal::default(), n));
            }
        }
    }

    fn append_array(&mut self, other: &DecimalArray) {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.data.extend_from_slice(&other.data);
    }

    fn pop(&mut self) -> Option<()> {
        self.data.pop().map(|_| self.bitmap.pop().unwrap())
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn finish(self) -> DecimalArray {
        DecimalArray {
            bitmap: self.bitmap.finish(),
            data: self.data.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
