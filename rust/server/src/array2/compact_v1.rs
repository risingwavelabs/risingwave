//! `compact_v1` module converts `array` back and forth to `array2`.

use super::PrimitiveArray as PrimitiveArrayV2;
use super::PrimitiveArrayBuilder as PrimitiveArrayBuilderV2;
use super::{Array as ArrayV2, ArrayBuilder as ArrayBuilderV2};
use crate::array::ArrayRef;
use crate::array::PrimitiveArray as PrimitiveArrayV1;
use crate::array::PrimitiveArrayBuilder as PrimitiveArrayBuilderV1;
use crate::array::{Array as ArrayV1, ArrayBuilder as ArrayBuilderV1};
use crate::types::*;
use std::sync::Arc;

macro_rules! impl_into_v2 {
    ($a:ty, $b:ty) => {
        impl From<PrimitiveArrayV1<$b>> for PrimitiveArrayV2<$a> {
            fn from(array: PrimitiveArrayV1<$b>) -> PrimitiveArrayV2<$a> {
                let mut builder = PrimitiveArrayBuilderV2::<$a>::new(array.len());
                for i in array.as_iter().unwrap() {
                    builder.append(i);
                }
                builder.finish()
            }
        }
    };
}

impl_into_v2! { i16, Int16Type }
impl_into_v2! { i32, Int32Type }
impl_into_v2! { i64, Int64Type }

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

impl_into_v1! { i16, Int16Type }
impl_into_v1! { i32, Int32Type }
impl_into_v1! { i64, Int64Type }
