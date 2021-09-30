//! `array2` defines all in-memory representations of vectorized execution framework.

mod bool_array;
pub(crate) mod column;
mod column_proto_readers;
mod data_chunk;
pub mod data_chunk_iter;
mod decimal_array;
pub(crate) mod interval_array;
mod iterator;
mod macros;
mod primitive_array;
mod utf8_array;
mod value_reader;

use crate::array2::iterator::ArrayImplIterator;
use crate::buffer::Bitmap;
pub use crate::error::ErrorCode::InternalError;
use crate::error::Result;
pub use crate::error::RwError;
use crate::types::ScalarImpl;
use crate::types::{Datum, Scalar, ScalarRef, ScalarRefImpl};
pub use bool_array::{BoolArray, BoolArrayBuilder};
pub use data_chunk::{DataChunk, DataChunkRef};
pub use decimal_array::{DecimalArray, DecimalArrayBuilder};
use interval_array::{IntervalArray, IntervalArrayBuilder};
pub use iterator::ArrayIterator;
use paste::paste;
pub use primitive_array::{PrimitiveArray, PrimitiveArrayBuilder, PrimitiveArrayItemType};
use risingwave_proto::data::Buffer;
use std::convert::From;
use std::hash::Hasher;
use std::sync::Arc;
pub use utf8_array::*;

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

/// The hash source for `None` values when hashing an item.
static NULL_VAL_FOR_HASH: u32 = 0xfffffff0;

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
pub trait ArrayBuilder: Send + Sync + Sized + 'static {
    /// Corresponding `Array` of this builder, which is reciprocal to `ArrayBuilder.
    type ArrayType: Array<Builder = Self>;

    /// Create a new builder with `capacity`.
    fn new(capacity: usize) -> Result<Self>;

    /// Append a value to builder.
    fn append(
        &mut self,
        value: Option<<<Self as ArrayBuilder>::ArrayType as Array>::RefItem<'_>>,
    ) -> Result<()>;

    fn append_null(&mut self) -> Result<()> {
        self.append(None)
    }

    /// Append an array to builder.
    fn append_array(&mut self, other: &Self::ArrayType) -> Result<()>;

    /// Append an element in another array into builder.
    fn append_array_element(&mut self, other: &Self::ArrayType, idx: usize) -> Result<()> {
        self.append(other.value_at(idx))
    }

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
pub trait Array: Send + Sync + Sized + 'static + Into<ArrayImpl> {
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

    /// Get the null `Bitmap` from `Array`.
    fn null_bitmap(&self) -> &Bitmap;

    /// Check if an element is `null` or not.
    fn is_null(&self, idx: usize) -> bool {
        self.null_bitmap().is_set(idx).map(|v| !v).unwrap()
    }

    fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H);

    fn hash_vec<H: Hasher>(&self, hashers: &mut Vec<H>) {
        assert_eq!(hashers.len(), self.len());
        for (idx, state) in hashers.iter_mut().enumerate() {
            self.hash_at(idx, state);
        }
    }
}

/// Implement `compact` on array, which removes element according to `visibility`.
trait CompactableArray: Array {
    /// Select some elements from `Array` based on `visibility` bitmap.
    /// `cardinality` is only used to decide capacity of the new `Array`.
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
#[macro_export]
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
      { Decimal, decimal, DecimalArray, DecimalArrayBuilder },
      { Interval, interval, IntervalArray, IntervalArrayBuilder }
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

impl<T: PrimitiveArrayItemType> From<PrimitiveArray<T>> for ArrayImpl {
    fn from(arr: PrimitiveArray<T>) -> Self {
        T::erase_array_type(arr)
    }
}

impl From<BoolArray> for ArrayImpl {
    fn from(arr: BoolArray) -> Self {
        Self::Bool(arr)
    }
}

impl From<DecimalArray> for ArrayImpl {
    fn from(arr: DecimalArray) -> Self {
        Self::Decimal(arr)
    }
}

impl From<UTF8Array> for ArrayImpl {
    fn from(arr: UTF8Array) -> Self {
        Self::UTF8(arr)
    }
}

impl From<IntervalArray> for ArrayImpl {
    fn from(arr: IntervalArray) -> Self {
        Self::Interval(arr)
    }
}

for_all_variants! { array_impl_enum }

/// `impl_convert` implements several conversions for `Array` and `ArrayBuilder`.
/// * `ArrayImpl -> &Array` with `impl.as_int16()`.
/// * `ArrayImpl -> Array` with `impl.into_int16()`.
/// * `Array -> ArrayImpl` with `From` trait.
/// * `&ArrayImpl -> &Array` with `From` trait.
/// * `ArrayBuilder -> ArrayBuilderImpl` with `From` trait.
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

        impl <'a> From<&'a ArrayImpl> for &'a $array {
          fn from(array: &'a ArrayImpl) -> Self {
            match array {
              ArrayImpl::$variant_name(inner) => inner,
              other_array => panic!("cannot covert ArrayImpl::{} to concrete type", other_array.get_ident())
            }
          }
        }

        impl From<$builder> for ArrayBuilderImpl {
          fn from(builder: $builder) -> Self {
            Self::$variant_name(builder)
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

/// Implements all `ArrayBuilder` functions with `for_all_variant`.
macro_rules! impl_array_builder {
  ([], $({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
    impl ArrayBuilderImpl {
      pub fn append_array(&mut self, other: &ArrayImpl) -> Result<()> {
        match self {
          $( Self::$variant_name(inner) => inner.append_array(other.into()), )*
        }
      }

      pub fn append_null(&mut self) -> Result<()> {
        match self {
          $( Self::$variant_name(inner) => inner.append(None), )*
        }
      }

      /// Append a datum, return error while type not match.
      pub fn append_datum(&mut self, datum: Datum) -> Result<()> {
        match datum {
          None => self.append_null(),
          Some(scalar) => match (self, scalar) {
            $( (Self::$variant_name(inner), ScalarImpl::$variant_name(v)) => inner.append(Some(v.as_scalar_ref())), )*
            _ => Err(RwError::from(InternalError("Invalid datum type".to_string()))),
          },
        }
      }

      pub fn append_array_element(&mut self, other: &ArrayImpl, idx: usize) -> Result<()> {
        match self {
          $( Self::$variant_name(inner) => inner.append_array_element(other.into(), idx), )*
        }
      }

      pub fn finish(self) -> Result<ArrayImpl> {
        match self {
          $( Self::$variant_name(inner) => Ok(inner.finish()?.into()), )*
        }
      }

      pub fn get_ident(&self) -> &'static str {
        match self {
          $( Self::$variant_name(_) => stringify!($variant_name), )*
        }
      }
    }
  }
}

for_all_variants! { impl_array_builder }

/// Implements all `Array` functions with `for_all_variant`.
macro_rules! impl_array {
  ([], $({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
    impl ArrayImpl {
      /// Number of items in array.
      pub fn len(&self) -> usize {
        match self {
          $( Self::$variant_name(inner) => inner.len(), )*
        }
      }

      /// Get the null `Bitmap` of the array.
      pub fn null_bitmap(&self) -> &Bitmap {
        match self {
          $( Self::$variant_name(inner) => inner.null_bitmap(), )*
        }
      }

      pub fn to_protobuf(&self) -> Result<Vec<Buffer>> {
        match self {
          $( Self::$variant_name(inner) => inner.to_protobuf(), )*
        }
      }

      pub fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
        match self {
          $( Self::$variant_name(inner) => inner.hash_at(idx, state), )*
        }
      }

      pub fn hash_vec<H: Hasher>(&self, hashers: &mut Vec<H>) {
        match self {
          $( Self::$variant_name(inner) => inner.hash_vec( hashers), )*
        }
      }

      /// Select some elements from `Array` based on `visibility` bitmap.
      pub fn compact(&self, visibility: &Bitmap, cardinality: usize) -> Result<Self> {
        match self {
          $( Self::$variant_name(inner) => Ok(inner.compact(visibility, cardinality)?.into()), )*
        }
      }

      pub fn get_ident(&self) -> &'static str {
        match self {
          $( Self::$variant_name(_) => stringify!($variant_name), )*
        }
      }

      pub fn get_sub_array(&self, indices: &[usize]) -> ArrayImpl {
        let capacity = indices.len();
        match self {
          $( Self::$variant_name(inner) => {
            let mut builder = <$builder>::new(capacity).unwrap();
            indices.iter().for_each(|idx| {
              builder.append(inner.value_at(*idx)).unwrap();
            });
            builder.finish().unwrap().into()
          }, )*
        }
      }

      pub fn get_continuous_sub_array(&self, start_idx: usize, end_idx: usize) -> ArrayImpl {
        let capacity = end_idx - start_idx + 1;
        match self {
          // TODO: for UTF8Array, we could select start and end offset and directly call copy_to_slice instead of accessing them one index by one index.
          $( Self::$variant_name(inner) => {
            let mut builder = <$builder>::new(capacity).unwrap();
            (start_idx..=end_idx).for_each(|idx| {
              builder.append(inner.value_at(idx)).unwrap();
            });
            builder.finish().unwrap().into()
          }, )*
        }
      }

      /// Get the enum-wrapped `Datum` out of the `Array`.
      pub fn scalar_value_at(&self, idx: usize) -> Datum {
        match self {
          $( Self::$variant_name(inner) => inner
            .value_at(idx)
            .map(|item| item.to_owned_scalar().to_scalar_value()), )*
        }
      }

      pub fn insert_key(&self, keys: &mut Vec<Vec<Datum>>) {
        match self {
          $( Self::$variant_name(inner) => {
            for (val, keys_for_one_row) in inner.iter().zip(keys.iter_mut()) {
              let op: Datum = val.map(|v| v.to_owned_scalar().into());
              keys_for_one_row.push(op);
            }
          }, )*
        }
      }

      /// Get the enum-wrapped `ScalarRefImpl` out of the `Array`.
      pub fn value_at(&self, idx: usize) -> Option<ScalarRefImpl<'_>> {
        match self {
          $( Self::$variant_name(inner) => inner.value_at(idx).map(ScalarRefImpl::$variant_name), )*
        }
      }
    }
  }
}

for_all_variants! { impl_array }

impl ArrayImpl {
    pub fn iter(&self) -> ArrayImplIterator<'_> {
        ArrayImplIterator::new(self)
    }
}

pub type ArrayRef = Arc<ArrayImpl>;

pub fn from_builder<A, F>(f: F) -> Result<A>
where
    A: Array,
    F: FnOnce(&mut A::Builder) -> Result<()>,
{
    let mut builder = A::Builder::new(0)?;
    f(&mut builder)?;
    builder.finish()
}

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

    use num_traits::cast::AsPrimitive;
    use num_traits::ops::checked::CheckedAdd;

    fn vec_add<T1, T2, T3>(
        a: &PrimitiveArray<T1>,
        b: &PrimitiveArray<T2>,
    ) -> Result<PrimitiveArray<T3>>
    where
        T1: PrimitiveArrayItemType + AsPrimitive<T3>,
        T2: PrimitiveArrayItemType + AsPrimitive<T3>,
        T3: PrimitiveArrayItemType + CheckedAdd,
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
#[cfg(test)]
mod test_util {
    use super::Array;
    use itertools::Itertools;
    use std::hash::{BuildHasher, Hasher};

    pub(crate) fn hash_finish<H: Hasher>(hashers: &mut Vec<H>) -> Vec<u64> {
        return hashers
            .iter()
            .map(|hasher| hasher.finish())
            .collect::<Vec<u64>>();
    }

    pub(crate) fn test_hash<H: BuildHasher, A: Array>(
        arrs: Vec<A>,
        expects: Vec<u64>,
        hasher_builder: H,
    ) {
        let len = expects.len();
        let mut states_scalar = Vec::with_capacity(len);
        states_scalar.resize_with(len, || hasher_builder.build_hasher());
        let mut states_vec = Vec::with_capacity(len);
        states_vec.resize_with(len, || hasher_builder.build_hasher());

        arrs.iter().for_each(|arr| {
            for (i, state) in states_scalar.iter_mut().enumerate() {
                arr.hash_at(i, state)
            }
        });
        arrs.iter().for_each(|arr| arr.hash_vec(&mut states_vec));
        itertools::cons_tuples(
            expects
                .iter()
                .zip_eq(hash_finish(&mut states_scalar))
                .zip_eq(hash_finish(&mut states_vec)),
        )
        .all(|(a, b, c)| *a == b && b == c);
    }
}
