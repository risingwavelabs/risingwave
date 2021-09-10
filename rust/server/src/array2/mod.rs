//! `array2` defines all in-memory representations of vectorized execution framework.

mod bool_array;
pub(crate) mod column;
mod compact_v1;
mod data_chunk;
mod decimal_array;
mod iterator;
mod macros;
mod primitive_array;
mod utf8_array;

use crate::buffer::Bitmap;
use crate::error::Result;
use crate::types::{Scalar, ScalarRef};
pub use bool_array::{BoolArray, BoolArrayBuilder};
pub use data_chunk::{DataChunk, DataChunkRef};
pub use decimal_array::{DecimalArray, DecimalArrayBuilder};
pub use iterator::ArrayIterator;
use paste::paste;
pub use primitive_array::{PrimitiveArray, PrimitiveArrayBuilder};
use risingwave_proto::data::Buffer;
use std::sync::Arc;
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
pub trait ArrayBuilder: Sized {
    /// Corresponding `Array` of this builder, which is reciprocal to `ArrayBuilder.
    type ArrayType: Array<Builder = Self>;

    /// Create a new builder with `capacity`.
    fn new(capacity: usize) -> Result<Self>;

    /// Append a value to builder.
    fn append(
        &mut self,
        value: Option<<<Self as ArrayBuilder>::ArrayType as Array>::RefItem<'_>>,
    ) -> Result<()>;

    /// Append an array to builder.
    fn append_array(&mut self, other: &Self::ArrayType) -> Result<()>;

    /// Finish build and return a new array.
    fn finish(self) -> Result<Self::ArrayType>;
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
pub trait Array: Sized + 'static {
    /// A reference to item in array, as well as return type of `value_at`, which is
    /// reciprocal to `Self::OwnedItem`.
    type RefItem<'a>: ScalarRef<'a, ScalarType = Self::OwnedItem>
    where
        Self: 'a;

    /// Owned type of item in array, which is reciprocal to `Self::RefItem`.
  #[rustfmt::skip]
  // rustfmt will incorrectly remove GAT lifetime.
  type OwnedItem: for<'a> Scalar<ScalarRefType<'a> = Self::RefItem<'a>>;

    /// Corresponding builder of this array, which is reciprocal to `Array`.
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

    fn to_protobuf(&self) -> Result<Vec<Buffer>>;

    fn null_bitmap(&self) -> &Bitmap;

    fn is_null(&self, idx: usize) -> bool {
        self.null_bitmap().is_set(idx).map(|v| !v).unwrap()
    }
}

/// Implement `compact` on array, which removes element according to `visibility`.
trait CompactableArray: Array {
    fn compact(&self, visibility: &Bitmap, cardinality: usize) -> Result<Self>;
}

impl<A: Array> CompactableArray for A {
    fn compact(&self, visibility: &Bitmap, cardinality: usize) -> Result<Self> {
        let mut builder = A::Builder::new(cardinality)?;
        for (elem, visible) in self.iter().zip(visibility.iter()) {
            if visible {
                builder.append(elem)?;
            }
        }
        builder.finish()
    }
}

/// `for_all_variants` includes all variants of our array types. If you added a new array
/// type inside the project, be sure to add a variant here.
///
/// Every tuple has four elements, where
/// `{ enum variant name, function suffix name, array type, builder type }`
///
/// There are typically two ways of using this macro, pass token or pass no token.
/// See the following implementations for example.
macro_rules! for_all_variants {
  ($macro:tt $(, $x:tt)*) => {
    $macro! {
      [$($x),*],
      { Int16, int16, I16Array, I16ArrayBuilder },
      { Int32, int32, I32Array, I32ArrayBuilder },
      { Int64, int64, I64Array, I64ArrayBuilder },
      { Float32, float32, F32Array, F32ArrayBuilder },
      { Float64, float64, F64Array, F64ArrayBuilder },
      { UTF8, utf8, UTF8Array, UTF8ArrayBuilder },
      { Bool, bool, BoolArray, BoolArrayBuilder },
      { Decimal, decimal, DecimalArray, DecimalArrayBuilder }
    }
  };
}

/// Define `ArrayImpl` with macro.
macro_rules! array_impl_enum {
  ([], $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
    /// `ArrayImpl` embeds all possible array in `arary2` module.
    #[derive(Debug)]
    pub enum ArrayImpl {
      $( $variant_name($array) ),*
    }
  };
}

for_all_variants! { array_impl_enum }

/// `impl_convert` implements 4 conversions for `Array`.
/// * `ArrayImpl -> &Array` with `impl.as_int16()`.
/// * `ArrayImpl -> Array` with `impl.into_int16()`.
/// * `Array -> ArrayImpl` with `From` trait.
/// * `&ArrayImpl -> &Array` with `From` trait.
macro_rules! impl_convert {
  ([], $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
    $(
      paste! {
        impl ArrayImpl {
          pub fn [<as_ $suffix_name>](&self) -> &$array {
            match self {
              Self::$variant_name(ref array) => array,
              other_array => panic!("cannot covert ArrayImpl::{} to concrete type", other_array.get_ident())
            }
          }

          pub fn [<into_ $suffix_name>](self) -> $array {
            match self {
              Self::$variant_name(array) => array,
              other_array =>  panic!("cannot covert ArrayImpl::{} to concrete type", other_array.get_ident())
            }
          }
        }

        impl From<$array> for ArrayImpl {
          fn from(array: $array) -> Self {
            Self::$variant_name(array)
          }
        }

        impl <'a> From<&'a ArrayImpl> for &'a $array {
          fn from(array: &'a ArrayImpl) -> Self {
            match array {
              ArrayImpl::$variant_name(inner) => inner,
              other_array => panic!("cannot covert ArrayImpl::{} to concrete type", other_array.get_ident())
            }
          }
        }
      }
    )*
  };
}

for_all_variants! { impl_convert }

/// Define `ArrayImplBuilder` with macro.
macro_rules! array_builder_impl_enum {
  ([], $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
    /// `ArrayBuilderImpl` embeds all possible array in `arary2` module.
    pub enum ArrayBuilderImpl {
      $( $variant_name($builder) ),*
    }
  };
}

for_all_variants! { array_builder_impl_enum }

impl ArrayBuilderImpl {
    pub fn append_array(&mut self, other: &ArrayImpl) -> Result<()> {
        macro_rules! impl_all_append_array {
      ([$self:ident, $other:ident], $({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        match $self {
          $( Self::$variant_name(inner) => inner.append_array($other.into()), )*
        }
      };
    }
        for_all_variants! { impl_all_append_array, self, other }
    }

    pub fn finish(self) -> Result<ArrayImpl> {
        macro_rules! impl_all_finish {
      ([$self:ident], $({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        match $self {
          $( Self::$variant_name(inner) => inner.finish()?.into(), )*
        }
      };
    }
        Ok(for_all_variants! { impl_all_finish, self })
    }

    pub fn get_ident(&self) -> &'static str {
        match self {
            ArrayBuilderImpl::Int16(_) => "Int16",
            ArrayBuilderImpl::Int32(_) => "Int32",
            ArrayBuilderImpl::Int64(_) => "Int64",
            ArrayBuilderImpl::Float32(_) => "Float32",
            ArrayBuilderImpl::Float64(_) => "Float64",
            ArrayBuilderImpl::UTF8(_) => "UTF8",
            ArrayBuilderImpl::Bool(_) => "Bool",
            ArrayBuilderImpl::Decimal(_) => "Decimal",
        }
    }
}

impl ArrayImpl {
    pub fn len(&self) -> usize {
        macro_rules! impl_all_len {
      ([$self:ident], $({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        match $self {
          $( Self::$variant_name(inner) => inner.len(), )*
        }
      };
    }
        for_all_variants! { impl_all_len, self }
    }

    pub fn null_bitmap(&self) -> &Bitmap {
        macro_rules! impl_all_null_bitmap {
      ([$self:ident], $({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        match $self {
          $( Self::$variant_name(inner) => inner.null_bitmap(), )*
        }
      };
    }
        for_all_variants! { impl_all_null_bitmap, self }
    }

    pub fn to_protobuf(&self) -> Result<Vec<Buffer>> {
        macro_rules! impl_all_to_protobuf {
      ([$self:ident], $({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        match $self {
          $( Self::$variant_name(inner) => inner.to_protobuf(), )*
        }
      };
    }
        for_all_variants! { impl_all_to_protobuf, self }
    }

    pub fn compact(&self, visibility: &Bitmap, cardinality: usize) -> Result<Self> {
        macro_rules! impl_all_compact {
      ([$self:ident, $visibility:ident, $cardinality:ident], $({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        match $self {
          $( Self::$variant_name(inner) => inner.compact(visibility, cardinality)?.into(), )*
        }
      };
    }
        Ok(for_all_variants! { impl_all_compact, self, visibility, cardinality })
    }

    pub fn get_ident(&self) -> &'static str {
        match self {
            ArrayImpl::Int16(_) => "Int16",
            ArrayImpl::Int32(_) => "Int32",
            ArrayImpl::Int64(_) => "Int64",
            ArrayImpl::Float32(_) => "Float32",
            ArrayImpl::Float64(_) => "Float64",
            ArrayImpl::UTF8(_) => "UTF8",
            ArrayImpl::Bool(_) => "Bool",
            ArrayImpl::Decimal(_) => "Decimal",
        }
    }

    pub fn get_sub_array(&self, indices: &[usize]) -> ArrayImpl {
        let capacity = indices.len();
        macro_rules! impl_all_get_sub_array {
      ([$self:ident], $({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        match $self {
          $( Self::$variant_name(inner) => {
            let mut builder = <$builder>::new(capacity).unwrap();
            indices.iter().for_each(|idx| {
              builder.append(inner.value_at(*idx)).unwrap();
            });
            builder.finish().unwrap().into()
          }, )*
        }
      };
    }
        for_all_variants! { impl_all_get_sub_array, self }
    }
}

macro_rules! impl_into_builders {
  ([], $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
    $(
      impl From<$builder> for ArrayBuilderImpl {
        fn from(builder: $builder) -> Self {
          Self::$variant_name(builder)
        }
      }
    )*
  };
}

for_all_variants! { impl_into_builders }

pub type ArrayRef = Arc<ArrayImpl>;

#[cfg(test)]
mod tests {
    use super::*;

    fn filter<'a, A, F>(data: &'a A, pred: F) -> Result<A>
    where
        A: Array + 'a,
        F: Fn(Option<A::RefItem<'a>>) -> bool,
    {
        let mut builder = A::Builder::new(data.len())?;
        for i in 0..data.len() {
            if pred(data.value_at(i)) {
                builder.append(data.value_at(i))?;
            }
        }
        builder.finish()
    }

    #[test]
    fn test_get_sub_array() {
        let i32vec = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let indices = vec![0, 2, 4, 6, 8];
        let mut builder = PrimitiveArrayBuilder::<i32>::new(0).unwrap();
        for i in &i32vec {
            builder.append(Some(*i)).unwrap();
        }
        let array = builder.finish().unwrap();
        let array_impl = ArrayImpl::Int32(array);
        let sub_array = array_impl.get_sub_array(&indices[..]);
        let inner = sub_array.as_int32();
        for (i, idx) in indices.iter().enumerate() {
            assert_eq!(inner.value_at(i).unwrap(), i32vec[*idx]);
        }
    }

    #[test]
    fn test_filter() {
        let mut builder = PrimitiveArrayBuilder::<i32>::new(0).unwrap();
        for i in 0..=60 {
            builder.append(Some(i as i32)).unwrap();
        }
        let array = filter(&builder.finish().unwrap(), |x| x.unwrap_or(0) >= 60).unwrap();
        assert_eq!(array.iter().collect::<Vec<Option<i32>>>(), vec![Some(60)]);
    }

    use crate::types::NativeType;
    use num_traits::cast::AsPrimitive;
    use num_traits::ops::checked::CheckedAdd;

    fn vec_add<T1, T2, T3>(
        a: &PrimitiveArray<T1>,
        b: &PrimitiveArray<T2>,
    ) -> Result<PrimitiveArray<T3>>
    where
        T1: NativeType + AsPrimitive<T3>,
        T2: NativeType + AsPrimitive<T3>,
        T3: NativeType + CheckedAdd,
    {
        assert_eq!(a.len(), b.len());
        let mut builder = PrimitiveArrayBuilder::<T3>::new(a.len())?;
        for (a, b) in a.iter().zip(b.iter()) {
            let item = match (a, b) {
                (Some(a), Some(b)) => Some(a.as_() + b.as_()),
                _ => None,
            };
            builder.append(item)?;
        }
        builder.finish()
    }

    #[test]
    fn test_vectorized_add() {
        let mut builder = PrimitiveArrayBuilder::<i32>::new(0).unwrap();
        for i in 0..=60 {
            builder.append(Some(i as i32)).unwrap();
        }
        let array1 = builder.finish().unwrap();

        let mut builder = PrimitiveArrayBuilder::<i16>::new(0).unwrap();
        for i in 0..=60 {
            builder.append(Some(i as i16)).unwrap();
        }
        let array2 = builder.finish().unwrap();

        let final_array = vec_add(&array1, &array2).unwrap() as PrimitiveArray<i64>;

        assert_eq!(final_array.len(), array1.len());
        for (idx, data) in final_array.iter().enumerate() {
            assert_eq!(data, Some(idx as i64 * 2));
        }
    }
}
