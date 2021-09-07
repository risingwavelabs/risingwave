//! `array2` defines all in-memory representations of vectorized execution framework.

mod compact_v1;
mod data_chunk;
mod iterator;
mod primitive_array;
mod utf8_array;

use crate::types::{Scalar, ScalarRef};
pub use data_chunk::{DataChunk, DataChunkRef};
pub use iterator::ArrayIterator;
use paste::paste;
pub use primitive_array::{PrimitiveArray, PrimitiveArrayBuilder};
pub use utf8_array::{UTF8Array, UTF8ArrayBuilder};

pub type I64Array = PrimitiveArray<i64>;
pub type I32Array = PrimitiveArray<i32>;
pub type I16Array = PrimitiveArray<i16>;
pub type F64Array = PrimitiveArray<f64>;
pub type F32Array = PrimitiveArray<f32>;

pub type I64ArrayBuilder = PrimitiveArrayBuilder<i64>;
pub type I32ArrayBuilder = PrimitiveArrayBuilder<i32>;
pub type I16ArrayBuilder = PrimitiveArrayBuilder<i16>;
pub type F64ArrayBuilder = PrimitiveArrayBuilder<f64>;
pub type F32ArrayBuilder = PrimitiveArrayBuilder<f32>;

/// A trait over all array builders.
///
/// `ArrayBuilder` is a trait over all builders. You could build an array with
/// `append` with the help of `ArrayBuilder` trait. The `append` function always
/// accepts reference to an element if it is not primitive. e.g. for `PrimitiveArray`,
/// you could do `builder.append(Some(1))`. For `UTF8Array`, you must do
/// `builder.append(Some("xxx"))`. Note that you don't need to construct a `String`.
///
/// The associated type `ArrayType` is the type of the corresponding array. It is the
/// return type of `finish`.
pub trait ArrayBuilder {
    /// Corresponding `Array` of this builder
    type ArrayType: Array<Builder = Self>;

    /// Create a new builder with `capacity`.
    fn new(capacity: usize) -> Self;

    /// Append a value to builder.
    fn append(&mut self, value: Option<<<Self as ArrayBuilder>::ArrayType as Array>::RefItem<'_>>);

    /// Append an array to builder.
    fn append_array(&mut self, other: &Self::ArrayType);

    /// Finish build and return a new array.
    fn finish(self) -> Self::ArrayType;
}

/// A trait over all array.
///
/// `Array` must be built with an `ArrayBuilder`. The array trait provides several
/// unified interface on an array, like `len`, `value_at` and `iter`.
///
/// The `Builder` associated type is the builder for this array. The `Iter` associated
/// type is the iterator of this array. And the `RefItem` is the item you could
/// retrieve from this array.
///
/// For example, `PrimitiveArray` could return an `Option<u32>`, and `UTF8Array` will
/// return an `Option<&str>`.
///
/// In some cases, we will need to store owned data. For example, when aggregating min
/// and max, we need to store current maximum in the aggregator. In this case, we
/// could use `A::OwnedItem` in aggregator struct.
pub trait Array {
    /// A reference to item in array, as well as return type of `value_at`.
    type RefItem<'a>: ScalarRef<ScalarType = Self::OwnedItem>
    where
        Self: 'a;

    /// Owned type of item in array.
  #[rustfmt::skip]
  // rustfmt will incorrectly remove GAT lifetime
  type OwnedItem: for<'a> Scalar<ScalarRefType<'a> = Self::RefItem<'a>>;

    /// Corresponding builder of this array.
    type Builder: ArrayBuilder<ArrayType = Self>;

    /// Iterator type of this array.
    type Iter<'a>: Iterator<Item = Option<Self::RefItem<'a>>>
    where
        Self: 'a;

    /// Retrieve a reference to value.
    fn value_at(&self, idx: usize) -> Option<Self::RefItem<'_>>;

    /// Number of items of array.
    fn len(&self) -> usize;

    /// Get iterator of current array.
    fn iter(&self) -> Self::Iter<'_>;
}

macro_rules! impl_into {
    ($x:ty, $y:ident) => {
        impl From<$x> for ArrayImpl {
            fn from(array: $x) -> Self {
                Self::$y(array)
            }
        }
    };
}

/// `ArrayCollection` embeds all possible array in `arary2` module.
#[derive(Debug)]
pub enum ArrayImpl {
    Int16(PrimitiveArray<i16>),
    Int32(PrimitiveArray<i32>),
    Int64(PrimitiveArray<i64>),
    Float32(PrimitiveArray<f32>),
    Float64(PrimitiveArray<f64>),
    UTF8(UTF8Array),
}

impl_into! { PrimitiveArray<i16>, Int16 }
impl_into! { PrimitiveArray<i32>, Int32 }
impl_into! { PrimitiveArray<i64>, Int64 }
impl_into! { PrimitiveArray<f32>, Float32 }
impl_into! { PrimitiveArray<f64>, Float64 }
impl_into! { UTF8Array, UTF8 }

macro_rules! impl_as_to {
    ($x:ident, $y:ident, $z:ty) => {
        paste! {
          impl ArrayImpl {
            pub fn [<as_ $y>](&self) -> &$z {
              match self {
                Self::$x(ref array) => array,
                _ => unimplemented!(),
              }
            }

            pub fn [<into_ $y>](self) -> $z {
              match self {
                Self::$x(array) => array,
                _ => unimplemented!(),
              }
            }
          }
        }
    };
}

impl_as_to! { Int16, int16, I16Array }
impl_as_to! { Int32, int32, I32Array }
impl_as_to! { Int64, int64, I64Array }
impl_as_to! { Float32, float32, F32Array }
impl_as_to! { Float64, float64, F64Array }
impl_as_to! { UTF8, utf8, UTF8Array }

pub enum ArrayBuilderImpl {
    Int16(PrimitiveArrayBuilder<i16>),
    Int32(PrimitiveArrayBuilder<i32>),
    Int64(PrimitiveArrayBuilder<i64>),
    Float32(PrimitiveArrayBuilder<f32>),
    Float64(PrimitiveArrayBuilder<f64>),
    UTF8(UTF8ArrayBuilder),
}

impl ArrayBuilderImpl {
    pub fn append_array(&mut self, _other: &ArrayImpl) {
        unimplemented!()
    }

    pub fn finish(self) -> ArrayImpl {
        match self {
            ArrayBuilderImpl::Int16(inner) => inner.finish().into(),
            ArrayBuilderImpl::Int32(inner) => inner.finish().into(),
            ArrayBuilderImpl::Int64(inner) => inner.finish().into(),
            ArrayBuilderImpl::Float32(inner) => inner.finish().into(),
            ArrayBuilderImpl::Float64(inner) => inner.finish().into(),
            ArrayBuilderImpl::UTF8(inner) => inner.finish().into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter<'a, A, F>(data: &'a A, pred: F) -> A
    where
        A: Array + 'a,
        F: Fn(Option<A::RefItem<'a>>) -> bool,
    {
        let mut builder = A::Builder::new(data.len());
        for i in 0..data.len() {
            if pred(data.value_at(i)) {
                builder.append(data.value_at(i));
            }
        }
        builder.finish()
    }

    #[test]
    fn test_filter() {
        let mut builder = PrimitiveArrayBuilder::<i32>::new(0);
        for i in 0..=60 {
            builder.append(Some(i as i32));
        }
        let array = filter(&builder.finish(), |x| x.unwrap_or(0) >= 60);
        assert_eq!(array.iter().collect::<Vec<Option<i32>>>(), vec![Some(60)]);
    }

    use crate::types::NativeType;
    use num_traits::cast::AsPrimitive;
    use num_traits::ops::checked::CheckedAdd;

    fn vec_add<T1, T2, T3>(a: &PrimitiveArray<T1>, b: &PrimitiveArray<T2>) -> PrimitiveArray<T3>
    where
        T1: NativeType + AsPrimitive<T3>,
        T2: NativeType + AsPrimitive<T3>,
        T3: NativeType + CheckedAdd,
    {
        assert_eq!(a.len(), b.len());
        let mut builder = PrimitiveArrayBuilder::<T3>::new(a.len());
        for (a, b) in a.iter().zip(b.iter()) {
            let item = match (a, b) {
                (Some(a), Some(b)) => Some(a.as_() + b.as_()),
                _ => None,
            };
            builder.append(item);
        }
        builder.finish()
    }

    #[test]
    fn test_vectorized_add() {
        let mut builder = PrimitiveArrayBuilder::<i32>::new(0);
        for i in 0..=60 {
            builder.append(Some(i as i32));
        }
        let array1 = builder.finish();

        let mut builder = PrimitiveArrayBuilder::<i16>::new(0);
        for i in 0..=60 {
            builder.append(Some(i as i16));
        }
        let array2 = builder.finish();

        let final_array = vec_add(&array1, &array2) as PrimitiveArray<i64>;

        assert_eq!(final_array.len(), array1.len());
        for (idx, data) in final_array.iter().enumerate() {
            assert_eq!(data, Some(idx as i64 * 2));
        }
    }
}
