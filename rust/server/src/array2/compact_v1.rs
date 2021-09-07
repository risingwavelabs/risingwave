//! `compact_v1` module converts `array` back and forth to `array2`.

use super::ArrayImpl;
use super::PrimitiveArray as PrimitiveArrayV2;
use super::PrimitiveArrayBuilder as PrimitiveArrayBuilderV2;
use super::{Array as ArrayV2, ArrayBuilder as ArrayBuilderV2};
use crate::array::ArrayRef;
use crate::array::PrimitiveArray as PrimitiveArrayV1;
use crate::array::PrimitiveArrayBuilder as PrimitiveArrayBuilderV1;
use crate::array::{Array as ArrayV1, ArrayBuilder as ArrayBuilderV1};
use crate::types::*;
use crate::util::downcast_ref;
use std::sync::Arc;

macro_rules! impl_into_v2 {
    ($a:ty, $b:ty) => {
        impl From<&PrimitiveArrayV1<$b>> for PrimitiveArrayV2<$a> {
            fn from(array: &PrimitiveArrayV1<$b>) -> PrimitiveArrayV2<$a> {
                let mut builder = PrimitiveArrayBuilderV2::<$a>::new(array.len()).unwrap();
                for i in array.as_iter().unwrap() {
                    builder.append(i).unwrap();
                }
                builder.finish().unwrap()
            }
        }
    };
}

impl_into_v2! { i16, Int16Type }
impl_into_v2! { i32, Int32Type }
impl_into_v2! { i64, Int64Type }
impl_into_v2! { f32, Float32Type }
impl_into_v2! { f64, Float64Type }

macro_rules! impl_into_v1 {
    ($a:ty, $b:ty) => {
        impl From<PrimitiveArrayV2<$a>> for ArrayRef {
            fn from(array: PrimitiveArrayV2<$a>) -> ArrayRef {
                let mut builder = Box::new(PrimitiveArrayBuilderV1::<$b>::new(
                    Arc::new(<$b>::new(false)),
                    array.len(),
                ));
                for i in array.iter() {
                    builder.append_value(i).unwrap();
                }
                builder.finish().unwrap()
            }
        }
    };
}

macro_rules! impl_into_v1_ref {
    ($a:ty, $b:ty) => {
        impl From<&PrimitiveArrayV2<$a>> for ArrayRef {
            fn from(array: &PrimitiveArrayV2<$a>) -> ArrayRef {
                let mut builder = Box::new(PrimitiveArrayBuilderV1::<$b>::new(
                    Arc::new(<$b>::new(false)),
                    array.len(),
                ));
                for i in array.iter() {
                    builder.append_value(i).unwrap();
                }
                builder.finish().unwrap()
            }
        }
    };
}

impl_into_v1! { i16, Int16Type }
impl_into_v1! { i32, Int32Type }
impl_into_v1! { i64, Int64Type }
impl_into_v1! { f32, Float32Type }
impl_into_v1! { f64, Float64Type }

impl_into_v1_ref! { i16, Int16Type }
impl_into_v1_ref! { i32, Int32Type }
impl_into_v1_ref! { i64, Int64Type }
impl_into_v1_ref! { f32, Float32Type }
impl_into_v1_ref! { f64, Float64Type }

impl From<ArrayRef> for ArrayImpl {
    fn from(data: ArrayRef) -> Self {
        match data.data_type().data_type_kind() {
            DataTypeKind::Int16 => ArrayImpl::Int16(
                (downcast_ref(&*data).unwrap() as &PrimitiveArrayV1<Int16Type>).into(),
            ),
            DataTypeKind::Int32 => ArrayImpl::Int32(
                (downcast_ref(&*data).unwrap() as &PrimitiveArrayV1<Int32Type>).into(),
            ),
            DataTypeKind::Int64 => ArrayImpl::Int64(
                (downcast_ref(&*data).unwrap() as &PrimitiveArrayV1<Int64Type>).into(),
            ),
            DataTypeKind::Float32 => ArrayImpl::Float32(
                (downcast_ref(&*data).unwrap() as &PrimitiveArrayV1<Float32Type>).into(),
            ),
            DataTypeKind::Float64 => ArrayImpl::Float64(
                (downcast_ref(&*data).unwrap() as &PrimitiveArrayV1<Float64Type>).into(),
            ),
            _ => unimplemented!(),
        }
    }
}

impl From<ArrayImpl> for ArrayRef {
    fn from(data: ArrayImpl) -> Self {
        use ArrayImpl::*;
        match data {
            Int16(x) => x.into(),
            Int32(x) => x.into(),
            Int64(x) => x.into(),
            Float32(x) => x.into(),
            Float64(x) => x.into(),
            _ => unimplemented!(),
        }
    }
}

impl From<&ArrayImpl> for ArrayRef {
    fn from(data: &ArrayImpl) -> Self {
        use ArrayImpl::*;
        match data {
            Int16(x) => x.into(),
            Int32(x) => x.into(),
            Int64(x) => x.into(),
            Float32(x) => x.into(),
            Float64(x) => x.into(),
            _ => unimplemented!(),
        }
    }
}
