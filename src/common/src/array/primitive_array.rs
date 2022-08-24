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

use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::mem::size_of;

use risingwave_pb::common::buffer::CompressionType;
use risingwave_pb::common::Buffer;
use risingwave_pb::data::{Array as ProstArray, ArrayType};

use super::{Array, ArrayBuilder, ArrayIterator, ArrayResult, NULL_VAL_FOR_HASH};
use crate::array::{ArrayBuilderImpl, ArrayImpl, ArrayMeta};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::for_all_native_types;
use crate::types::interval::IntervalUnit;
use crate::types::{
    NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper, NativeType, Scalar, ScalarRef,
};

/// Physical type of array items which have fixed size.
pub trait PrimitiveArrayItemType
where
    for<'a> Self: Sized
        + Default
        + PartialOrd
        + Scalar<ScalarRefType<'a> = Self>
        + ScalarRef<'a, ScalarType = Self>,
{
    // array methods
    /// A helper to convert a primitive array to `ArrayImpl`.
    fn erase_array_type(arr: PrimitiveArray<Self>) -> ArrayImpl;
    /// A helper to convert `ArrayImpl` to self.
    fn try_into_array(arr: ArrayImpl) -> Option<PrimitiveArray<Self>>;
    /// A helper to convert `ArrayImpl` to self.
    fn try_into_array_ref(arr: &ArrayImpl) -> Option<&PrimitiveArray<Self>>;
    /// Returns array type of the primitive array
    fn array_type() -> ArrayType;
    /// Creates an `ArrayBuilder` for this primitive type
    fn create_array_builder(capacity: usize) -> ArrayResult<ArrayBuilderImpl>;

    // item methods
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize>;
    fn hash_wrapper<H: Hasher>(&self, state: &mut H);
}

macro_rules! impl_array_methods {
    ($scalar_type:ty, $array_type_pb:ident, $array_impl_variant:ident) => {
        fn erase_array_type(arr: PrimitiveArray<Self>) -> ArrayImpl {
            ArrayImpl::$array_impl_variant(arr)
        }

        fn try_into_array(arr: ArrayImpl) -> Option<PrimitiveArray<Self>> {
            match arr {
                ArrayImpl::$array_impl_variant(inner) => Some(inner),
                _ => None,
            }
        }

        fn try_into_array_ref(arr: &ArrayImpl) -> Option<&PrimitiveArray<Self>> {
            match arr {
                ArrayImpl::$array_impl_variant(inner) => Some(inner),
                _ => None,
            }
        }

        fn array_type() -> ArrayType {
            ArrayType::$array_type_pb
        }

        fn create_array_builder(capacity: usize) -> ArrayResult<ArrayBuilderImpl> {
            let array_builder = PrimitiveArrayBuilder::<$scalar_type>::new(capacity);
            Ok(ArrayBuilderImpl::$array_impl_variant(array_builder))
        }
    };
}

macro_rules! impl_primitive_for_native_types {
    ([], $({ $naive_type:ty, $scalar_type:ident } ),*) => {
        $(
            impl PrimitiveArrayItemType for $naive_type {
                impl_array_methods!($naive_type, $scalar_type, $scalar_type);

                fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
                    NativeType::to_protobuf(self, output)
                }

                fn hash_wrapper<H: Hasher>(&self, state: &mut H) {
                    NativeType::hash_wrapper(self, state)
                }
            }
        )*
    }
}

for_all_native_types! { impl_primitive_for_native_types }

/// These types have `to_protobuf` and implement `Hash`.
macro_rules! impl_primitive_for_others {
    ($({ $scalar_type:ty, $array_type_pb:ident, $array_impl_variant:ident } ),*) => {
        $(
            impl PrimitiveArrayItemType for $scalar_type {
                impl_array_methods!($scalar_type, $array_type_pb, $array_impl_variant);

                fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
                    <$scalar_type>::to_protobuf(self, output)
                }

                fn hash_wrapper<H: Hasher>(&self, state: &mut H) {
                    self.hash(state)
                }
            }
        )*
    }
}

impl_primitive_for_others! {
    { IntervalUnit, Interval, Interval },
    { NaiveDateWrapper, Date, NaiveDate },
    { NaiveTimeWrapper, Time, NaiveTime },
    { NaiveDateTimeWrapper, Timestamp, NaiveDateTime }
}

/// `PrimitiveArray` is a collection of primitive types, such as `i32`, `f32`.
#[derive(Debug, Clone)]
pub struct PrimitiveArray<T: PrimitiveArrayItemType> {
    bitmap: Bitmap,
    data: Vec<T>,
}

impl<T: PrimitiveArrayItemType> PrimitiveArray<T> {
    pub fn from_slice(data: &[Option<T>]) -> ArrayResult<Self> {
        let mut builder = <Self as Array>::Builder::new(data.len());
        for i in data {
            builder.append(*i)?;
        }
        builder.finish()
    }
}

impl<T: PrimitiveArrayItemType> Array for PrimitiveArray<T> {
    type Builder = PrimitiveArrayBuilder<T>;
    type Iter<'a> = ArrayIterator<'a, Self>;
    type OwnedItem = T;
    type RefItem<'a> = T;

    fn value_at(&self, idx: usize) -> Option<T> {
        if self.is_null(idx) {
            None
        } else {
            Some(self.data[idx])
        }
    }

    /// # Safety
    ///
    /// This function is unsafe because it does not check whether the index is within the bounds of
    /// the array.
    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<T> {
        if self.is_null_unchecked(idx) {
            None
        } else {
            Some(*self.data.get_unchecked(idx))
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> ProstArray {
        let mut output_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<T>());

        for v in self.iter() {
            v.map(|node| node.to_protobuf(&mut output_buffer));
        }

        let buffer = Buffer {
            compression: CompressionType::None as i32,
            body: output_buffer,
        };
        let null_bitmap = self.null_bitmap().to_protobuf();
        ProstArray {
            null_bitmap: Some(null_bitmap),
            values: vec![buffer],
            array_type: T::array_type() as i32,
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

    #[inline(always)]
    fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            self.data[idx].hash_wrapper(state);
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn create_builder(&self, capacity: usize) -> ArrayResult<ArrayBuilderImpl> {
        T::create_array_builder(capacity)
    }
}

/// `PrimitiveArrayBuilder` constructs a `PrimitiveArray` from `Option<Primitive>`.
#[derive(Debug)]
pub struct PrimitiveArrayBuilder<T: PrimitiveArrayItemType> {
    bitmap: BitmapBuilder,
    data: Vec<T>,
}

impl<T: PrimitiveArrayItemType> ArrayBuilder for PrimitiveArrayBuilder<T> {
    type ArrayType = PrimitiveArray<T>;

    fn with_meta(capacity: usize, _meta: ArrayMeta) -> Self {
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn append(&mut self, value: Option<T>) -> ArrayResult<()> {
        match value {
            Some(x) => {
                self.bitmap.append(true);
                self.data.push(x);
            }
            None => {
                self.bitmap.append(false);
                self.data.push(T::default());
            }
        }
        Ok(())
    }

    fn append_array(&mut self, other: &PrimitiveArray<T>) -> ArrayResult<()> {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn finish(self) -> ArrayResult<PrimitiveArray<T>> {
        Ok(PrimitiveArray {
            bitmap: self.bitmap.finish(),
            data: self.data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OrderedF32, OrderedF64};

    fn helper_test_builder<T: PrimitiveArrayItemType>(
        data: Vec<Option<T>>,
    ) -> ArrayResult<PrimitiveArray<T>> {
        let mut builder = PrimitiveArrayBuilder::<T>::new(data.len());
        for d in data {
            builder.append(d)?;
        }
        builder.finish()
    }

    #[test]
    fn test_i16_builder() {
        let arr = helper_test_builder::<i16>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        )
        .unwrap();
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Int16(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_i32_builder() {
        let arr = helper_test_builder::<i32>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        )
        .unwrap();
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Int32(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_i64_builder() {
        let arr = helper_test_builder::<i64>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        )
        .unwrap();
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Int64(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_f32_builder() {
        let arr = helper_test_builder::<OrderedF32>(
            (0..1000)
                .map(|x| {
                    if x % 2 == 0 {
                        None
                    } else {
                        Some((x as f32).into())
                    }
                })
                .collect(),
        )
        .unwrap();
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Float32(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_f64_builder() {
        let arr = helper_test_builder::<OrderedF64>(
            (0..1000)
                .map(|x| {
                    if x % 2 == 0 {
                        None
                    } else {
                        Some((x as f64).into())
                    }
                })
                .collect(),
        )
        .unwrap();
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Float64(_)) {
            unreachable!()
        }
    }
}
