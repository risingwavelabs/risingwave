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

//! `Array` defines all in-memory representations of vectorized execution framework.

mod arrow;
mod bool_array;
pub mod bytes_array;
mod chrono_array;
pub mod column;
mod column_proto_readers;
mod data_chunk;
pub mod data_chunk_iter;
mod decimal_array;
pub mod error;
pub mod interval_array;
mod iterator;
mod jsonb_array;
pub mod list_array;
mod macros;
mod num256_array;
mod primitive_array;
pub mod serial_array;
pub mod stream_chunk;
mod stream_chunk_iter;
pub mod stream_record;
pub mod struct_array;
mod utf8_array;
mod value_reader;
mod vis;

use std::convert::From;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub use bool_array::{BoolArray, BoolArrayBuilder};
pub use bytes_array::*;
pub use chrono_array::{
    DateArray, DateArrayBuilder, TimeArray, TimeArrayBuilder, TimestampArray, TimestampArrayBuilder,
};
pub use column_proto_readers::*;
pub use data_chunk::{DataChunk, DataChunkTestExt};
pub use data_chunk_iter::RowRef;
pub use decimal_array::{DecimalArray, DecimalArrayBuilder};
use get_size::GetSize;
pub use interval_array::{IntervalArray, IntervalArrayBuilder};
pub use iterator::ArrayIterator;
pub use jsonb_array::{JsonbArray, JsonbArrayBuilder, JsonbRef, JsonbVal};
pub use list_array::{ListArray, ListArrayBuilder, ListRef, ListValue};
use paste::paste;
pub use primitive_array::{PrimitiveArray, PrimitiveArrayBuilder, PrimitiveArrayItemType};
use risingwave_pb::data::{PbArray, PbArrayType};
pub use serial_array::{Serial, SerialArray, SerialArrayBuilder};
pub use stream_chunk::{Op, StreamChunk, StreamChunkTestExt};
pub use struct_array::{StructArray, StructArrayBuilder, StructRef, StructValue};
pub use utf8_array::*;
pub use vis::{Vis, VisRef};

pub use self::error::ArrayError;
pub use crate::array::num256_array::{
    Int256Array, Int256ArrayBuilder, Uint256Array, Uint256ArrayBuilder,
};
use crate::buffer::Bitmap;
use crate::types::*;
use crate::util::iter_util::ZipEqFast;
pub type ArrayResult<T> = Result<T, ArrayError>;

pub type I64Array = PrimitiveArray<i64>;
pub type I32Array = PrimitiveArray<i32>;
pub type I16Array = PrimitiveArray<i16>;
pub type F64Array = PrimitiveArray<F64>;
pub type F32Array = PrimitiveArray<F32>;

pub type I64ArrayBuilder = PrimitiveArrayBuilder<i64>;
pub type I32ArrayBuilder = PrimitiveArrayBuilder<i32>;
pub type I16ArrayBuilder = PrimitiveArrayBuilder<i16>;
pub type F64ArrayBuilder = PrimitiveArrayBuilder<F64>;
pub type F32ArrayBuilder = PrimitiveArrayBuilder<F32>;

/// The hash source for `None` values when hashing an item.
pub(crate) const NULL_VAL_FOR_HASH: u32 = 0xfffffff0;

/// A trait over all array builders.
///
/// `ArrayBuilder` is a trait over all builders. You could build an array with
/// `append` with the help of `ArrayBuilder` trait. The `append` function always
/// accepts reference to an element if it is not primitive. e.g. for `PrimitiveArray`,
/// you could do `builder.append(Some(1))`. For `Utf8Array`, you must do
/// `builder.append(Some("xxx"))`. Note that you don't need to construct a `String`.
///
/// The associated type `ArrayType` is the type of the corresponding array. It is the
/// return type of `finish`.
pub trait ArrayBuilder: Send + Sync + Sized + 'static {
    /// Corresponding `Array` of this builder, which is reciprocal to `ArrayBuilder`.
    type ArrayType: Array<Builder = Self>;

    /// Create a new builder with `capacity`.
    fn new(capacity: usize) -> Self {
        // No metadata by default.
        Self::with_meta(capacity, ArrayMeta::Simple)
    }

    /// # Panics
    /// Panics if `meta`'s type mismatches with the array type.
    fn with_meta(capacity: usize, meta: ArrayMeta) -> Self;

    /// Append a value multiple times.
    ///
    /// This should be more efficient than calling `append` multiple times.
    fn append_n(&mut self, n: usize, value: Option<<Self::ArrayType as Array>::RefItem<'_>>);

    /// Append a value to builder.
    fn append(&mut self, value: Option<<Self::ArrayType as Array>::RefItem<'_>>) {
        self.append_n(1, value);
    }

    fn append_null(&mut self) {
        self.append(None)
    }

    /// Append an array to builder.
    fn append_array(&mut self, other: &Self::ArrayType);

    /// Pop an element from the builder.
    ///
    /// # Returns
    ///
    /// Returns `None` if there is no elements in the builder.
    fn pop(&mut self) -> Option<()>;

    /// Append an element in another array into builder.
    fn append_array_element(&mut self, other: &Self::ArrayType, idx: usize) {
        self.append(other.value_at(idx));
    }

    /// Finish build and return a new array.
    fn finish(self) -> Self::ArrayType;
}

/// A trait over all array.
///
/// `Array` must be built with an `ArrayBuilder`. The array trait provides several
/// unified interface on an array, like `len`, `value_at` and `iter`.
///
/// The `Builder` associated type is the builder for this array.
///
/// The `Iter` associated type is the iterator of this array. And the `RefItem` is
/// the item you could retrieve from this array.
/// For example, `PrimitiveArray` could return an `Option<u32>`, and `Utf8Array` will
/// return an `Option<&str>`.
///
/// In some cases, we will need to store owned data. For example, when aggregating min
/// and max, we need to store current maximum in the aggregator. In this case, we
/// could use `A::OwnedItem` in aggregator struct.
pub trait Array:
    std::fmt::Debug + Send + Sync + Sized + 'static + Into<ArrayImpl> + GetSize
{
    /// A reference to item in array, as well as return type of `value_at`, which is
    /// reciprocal to `Self::OwnedItem`.
    type RefItem<'a>: ScalarRef<'a, ScalarType = Self::OwnedItem>
    where
        Self: 'a;

    /// Owned type of item in array, which is reciprocal to `Self::RefItem`.
    type OwnedItem: Clone + std::fmt::Debug + for<'a> Scalar<ScalarRefType<'a> = Self::RefItem<'a>>;

    /// Corresponding builder of this array, which is reciprocal to `Array`.
    type Builder: ArrayBuilder<ArrayType = Self>;

    /// Retrieve a reference to value regardless of whether it is null
    /// without checking the index boundary.
    ///
    /// The returned value for NULL values is the default value.
    ///
    /// # Safety
    ///
    /// Index must be within the bounds.
    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_>;

    /// Retrieve a reference to value.
    #[inline]
    fn value_at(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if !self.is_null(idx) {
            // Safety: the above `is_null` check ensures that the index is valid.
            Some(unsafe { self.raw_value_at_unchecked(idx) })
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// Retrieve a reference to value without checking the index boundary.
    #[inline]
    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if !self.is_null_unchecked(idx) {
            Some(self.raw_value_at_unchecked(idx))
        } else {
            None
        }
    }

    /// Number of items of array.
    fn len(&self) -> usize;

    /// Get iterator of current array.
    fn iter(&self) -> ArrayIterator<'_, Self> {
        ArrayIterator::new(self)
    }

    /// Get raw iterator of current array.
    ///
    /// The raw iterator simply iterates values without checking the null bitmap.
    /// The returned value for NULL values is undefined.
    fn raw_iter(&self) -> impl DoubleEndedIterator<Item = Self::RefItem<'_>> {
        (0..self.len()).map(|i| unsafe { self.raw_value_at_unchecked(i) })
    }

    /// Serialize to protobuf
    fn to_protobuf(&self) -> PbArray;

    /// Get the null `Bitmap` from `Array`.
    fn null_bitmap(&self) -> &Bitmap;

    /// Get the owned null `Bitmap` from `Array`.
    fn into_null_bitmap(self) -> Bitmap;

    /// Check if an element is `null` or not.
    fn is_null(&self, idx: usize) -> bool {
        !self.null_bitmap().is_set(idx)
    }

    /// # Safety
    ///
    /// The unchecked version of `is_null`, ignore index out of bound check. It is
    /// the caller's responsibility to ensure the index is valid.
    unsafe fn is_null_unchecked(&self, idx: usize) -> bool {
        !self.null_bitmap().is_set_unchecked(idx)
    }

    fn set_bitmap(&mut self, bitmap: Bitmap);

    /// Feed the value at `idx` into the given [`Hasher`].
    #[inline(always)]
    fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
        // We use a default implementation for all arrays for now, as retrieving the reference
        // should be lightweight.
        if let Some(value) = self.value_at(idx) {
            value.hash_scalar(state);
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn hash_vec<H: Hasher>(&self, hashers: &mut [H]) {
        assert_eq!(hashers.len(), self.len());
        for (idx, state) in hashers.iter_mut().enumerate() {
            self.hash_at(idx, state);
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn create_builder(&self, capacity: usize) -> ArrayBuilderImpl;

    fn array_meta(&self) -> ArrayMeta {
        ArrayMeta::Simple
    }
}

/// The creation of [`Array`] typically does not rely on [`DataType`].
/// For now the exceptions are list and struct, which require type details
/// as they decide the layout of the array.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArrayMeta {
    Simple, // Simple array without given any extra metadata.
    Struct {
        children: Arc<[DataType]>,
        children_names: Arc<[String]>,
    },
    List {
        datatype: Box<DataType>,
    },
}

impl From<&DataType> for ArrayMeta {
    fn from(data_type: &DataType) -> Self {
        match data_type {
            DataType::Struct(struct_type) => ArrayMeta::Struct {
                children: struct_type.fields.clone().into(),
                children_names: struct_type.field_names.clone().into(),
            },
            DataType::List { datatype } => ArrayMeta::List {
                datatype: datatype.clone(),
            },
            _ => ArrayMeta::Simple,
        }
    }
}

/// Implement `compact` on array, which removes element according to `visibility`.
trait CompactableArray: Array {
    /// Select some elements from `Array` based on `visibility` bitmap.
    /// `cardinality` is only used to decide capacity of the new `Array`.
    fn compact(&self, visibility: &Bitmap, cardinality: usize) -> Self;
}

impl<A: Array> CompactableArray for A {
    fn compact(&self, visibility: &Bitmap, cardinality: usize) -> Self {
        let mut builder = A::Builder::with_meta(cardinality, self.array_meta());
        for (elem, visible) in self.iter().zip_eq_fast(visibility.iter()) {
            if visible {
                builder.append(elem);
            }
        }
        builder.finish()
    }
}

/// `for_all_variants` includes all variants of our array types. If you added a new array
/// type inside the project, be sure to add a variant here.
///
/// It is used to simplify the boilerplate code of repeating all array types, while each type
/// has exactly the same code.
///
/// To use it, you need to provide a macro, whose input is `{ enum variant name, function suffix
/// name, array type, builder type }` tuples. Refer to the following implementations as examples.
#[macro_export]
macro_rules! for_all_variants {
    ($macro:ident) => {
        $macro! {
            { Int16, int16, I16Array, I16ArrayBuilder },
            { Int32, int32, I32Array, I32ArrayBuilder },
            { Int64, int64, I64Array, I64ArrayBuilder },
            { Int256, int256, Int256Array, Int256ArrayBuilder },
            { Uint256, uint256, Uint256Array, Uint256ArrayBuilder },
            { Float32, float32, F32Array, F32ArrayBuilder },
            { Float64, float64, F64Array, F64ArrayBuilder },
            { Utf8, utf8, Utf8Array, Utf8ArrayBuilder },
            { Bool, bool, BoolArray, BoolArrayBuilder },
            { Decimal, decimal, DecimalArray, DecimalArrayBuilder },
            { Interval, interval, IntervalArray, IntervalArrayBuilder },
            { Date, date, DateArray, DateArrayBuilder },
            { Timestamp, timestamp, TimestampArray, TimestampArrayBuilder },
            { Time, time, TimeArray, TimeArrayBuilder },
            { Jsonb, jsonb, JsonbArray, JsonbArrayBuilder },
            { Serial, serial, SerialArray, SerialArrayBuilder },
            { Struct, struct, StructArray, StructArrayBuilder },
            { List, list, ListArray, ListArrayBuilder },
            { Bytea, bytea, BytesArray, BytesArrayBuilder}
        }
    };
}

/// Define `ArrayImpl` with macro.
macro_rules! array_impl_enum {
    ( $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        /// `ArrayImpl` embeds all possible array in `array` module.
        #[derive(Debug, Clone, GetSize)]
        pub enum ArrayImpl {
            $( $variant_name($array) ),*
        }
    };
}

for_all_variants! { array_impl_enum }

impl<T: PrimitiveArrayItemType> From<PrimitiveArray<T>> for ArrayImpl {
    fn from(arr: PrimitiveArray<T>) -> Self {
        T::erase_array_type(arr)
    }
}

impl From<Int256Array> for ArrayImpl {
    fn from(arr: Int256Array) -> Self {
        Self::Int256(arr)
    }
}

impl From<Uint256Array> for ArrayImpl {
    fn from(arr: Uint256Array) -> Self {
        Self::Uint256(arr)
    }
}

impl From<BoolArray> for ArrayImpl {
    fn from(arr: BoolArray) -> Self {
        Self::Bool(arr)
    }
}

impl From<Utf8Array> for ArrayImpl {
    fn from(arr: Utf8Array) -> Self {
        Self::Utf8(arr)
    }
}

impl From<JsonbArray> for ArrayImpl {
    fn from(arr: JsonbArray) -> Self {
        Self::Jsonb(arr)
    }
}

impl From<StructArray> for ArrayImpl {
    fn from(arr: StructArray) -> Self {
        Self::Struct(arr)
    }
}

impl From<ListArray> for ArrayImpl {
    fn from(arr: ListArray) -> Self {
        Self::List(arr)
    }
}

impl From<BytesArray> for ArrayImpl {
    fn from(arr: BytesArray) -> Self {
        Self::Bytea(arr)
    }
}

/// `impl_convert` implements several conversions for `Array` and `ArrayBuilder`.
/// * `ArrayImpl -> &Array` with `impl.as_int16()`.
/// * `ArrayImpl -> Array` with `impl.into_int16()`.
/// * `&ArrayImpl -> &Array` with `From` trait.
/// * `ArrayImpl -> Array` with `From` trait.
/// * `ArrayBuilder -> ArrayBuilderImpl` with `From` trait.
macro_rules! impl_convert {
    ($( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        $(
            paste! {
                impl ArrayImpl {
                    pub fn [<as_ $suffix_name>](&self) -> &$array {
                        match self {
                            Self::$variant_name(ref array) => array,
                            other_array => panic!("cannot convert ArrayImpl::{} to concrete type {}", other_array.get_ident(), stringify!($variant_name))
                        }
                    }

                    pub fn [<into_ $suffix_name>](self) -> $array {
                        match self {
                            Self::$variant_name(array) => array,
                            other_array => panic!("cannot convert ArrayImpl::{} to concrete type {}", other_array.get_ident(), stringify!($variant_name))
                        }
                    }
                }

                impl <'a> From<&'a ArrayImpl> for &'a $array {
                    fn from(array: &'a ArrayImpl) -> Self {
                        match array {
                            ArrayImpl::$variant_name(inner) => inner,
                            other_array => panic!("cannot convert ArrayImpl::{} to concrete type {}", other_array.get_ident(), stringify!($variant_name))
                        }
                    }
                }

                impl From<ArrayImpl> for $array {
                    fn from(array: ArrayImpl) -> Self {
                        match array {
                            ArrayImpl::$variant_name(inner) => inner,
                            other_array => panic!("cannot convert ArrayImpl::{} to concrete type {}", other_array.get_ident(), stringify!($variant_name))
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
    ($( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        /// `ArrayBuilderImpl` embeds all possible array in `array` module.
        #[derive(Debug)]
        pub enum ArrayBuilderImpl {
            $( $variant_name($builder) ),*
        }
    };
}

for_all_variants! { array_builder_impl_enum }

/// Implements all `ArrayBuilder` functions with `for_all_variant`.
macro_rules! impl_array_builder {
    ($({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        impl ArrayBuilderImpl {
            pub fn append_array(&mut self, other: &ArrayImpl) {
                match self {
                    $( Self::$variant_name(inner) => inner.append_array(other.into()), )*
                }
            }

            pub fn append_null(&mut self) {
                match self {
                    $( Self::$variant_name(inner) => inner.append(None), )*
                }
            }

            /// Append a [`Datum`] or [`DatumRef`] multiple times,
            /// panicking if the datum's type does not match the array builder's type.
            pub fn append_datum_n(&mut self, n: usize, datum: impl ToDatumRef) {
                match datum.to_datum_ref() {
                    None => match self {
                        $( Self::$variant_name(inner) => inner.append_n(n, None), )*
                    }
                    Some(scalar_ref) => match (self, scalar_ref) {
                        $( (Self::$variant_name(inner), ScalarRefImpl::$variant_name(v)) => inner.append_n(n, Some(v)), )*
                        (this_builder, this_scalar_ref) => panic!(
                            "Failed to append datum, array builder type: {}, scalar type: {}",
                            this_builder.get_ident(),
                            this_scalar_ref.get_ident()
                        ),
                    },
                }
            }

            /// Append a [`Datum`] or [`DatumRef`], return error while type not match.
            pub fn append_datum(&mut self, datum: impl ToDatumRef) {
                self.append_datum_n(1, datum);
            }

            pub fn append_array_element(&mut self, other: &ArrayImpl, idx: usize) {
                match self {
                    $( Self::$variant_name(inner) => inner.append_array_element(other.into(), idx), )*
                };
            }

            pub fn pop(&mut self) -> Option<()> {
                match self {
                    $( Self::$variant_name(inner) => inner.pop(), )*
                }
            }

            pub fn finish(self) -> ArrayImpl {
                match self {
                    $( Self::$variant_name(inner) => inner.finish().into(), )*
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
    ($({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        impl ArrayImpl {
            /// Number of items in array.
            pub fn len(&self) -> usize {
                match self {
                    $( Self::$variant_name(inner) => inner.len(), )*
                }
            }

            pub fn is_empty(&self) -> bool {
                self.len() == 0
            }

            /// Get the null `Bitmap` of the array.
            pub fn null_bitmap(&self) -> &Bitmap {
                match self {
                    $( Self::$variant_name(inner) => inner.null_bitmap(), )*
                }
            }

            pub fn into_null_bitmap(self) -> Bitmap {
                match self {
                    $( Self::$variant_name(inner) => inner.into_null_bitmap(), )*
                }
            }

            pub fn to_protobuf(&self) -> PbArray {
                match self {
                    $( Self::$variant_name(inner) => inner.to_protobuf(), )*
                }
            }

            pub fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
                match self {
                    $( Self::$variant_name(inner) => inner.hash_at(idx, state), )*
                }
            }

            pub fn hash_vec<H: Hasher>(&self, hashers: &mut [H]) {
                match self {
                    $( Self::$variant_name(inner) => inner.hash_vec( hashers), )*
                }
            }

            /// Select some elements from `Array` based on `visibility` bitmap.
            pub fn compact(&self, visibility: &Bitmap, cardinality: usize) -> Self {
                match self {
                    $( Self::$variant_name(inner) => inner.compact(visibility, cardinality).into(), )*
                }
            }

            pub fn get_ident(&self) -> &'static str {
                match self {
                    $( Self::$variant_name(_) => stringify!($variant_name), )*
                }
            }

            /// Get the enum-wrapped `Datum` out of the `Array`.
            pub fn datum_at(&self, idx: usize) -> Datum {
                match self {
                    $( Self::$variant_name(inner) => inner
                        .value_at(idx)
                        .map(|item| item.to_owned_scalar().to_scalar_value()), )*
                }
            }

            /// If the array only have one single element, convert it to `Datum`.
            pub fn to_datum(&self) -> Datum {
                assert_eq!(self.len(), 1);
                self.datum_at(0)
            }

            /// Get the enum-wrapped `ScalarRefImpl` out of the `Array`.
            pub fn value_at(&self, idx: usize) -> DatumRef<'_> {
                match self {
                    $( Self::$variant_name(inner) => inner.value_at(idx).map(ScalarRefImpl::$variant_name), )*
                }
            }

            /// # Safety
            ///
            /// This function is unsafe because it does not check the validity of `idx`. It is caller's
            /// responsibility to ensure the validity of `idx`.
            ///
            /// Unsafe version of getting the enum-wrapped `ScalarRefImpl` out of the `Array`.
            pub unsafe fn value_at_unchecked(&self, idx: usize) -> DatumRef<'_> {
                match self {
                    $( Self::$variant_name(inner) => inner.value_at_unchecked(idx).map(ScalarRefImpl::$variant_name), )*
                }
            }

            pub fn set_bitmap(&mut self, bitmap: Bitmap) {
                match self {
                    $( Self::$variant_name(inner) => inner.set_bitmap(bitmap), )*
                }
            }

            pub fn create_builder(&self, capacity: usize) -> ArrayBuilderImpl {
                match self {
                    $( Self::$variant_name(inner) => inner.create_builder(capacity), )*
                }
            }
        }
    }
}

for_all_variants! { impl_array }

impl ArrayImpl {
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = DatumRef<'_>> + ExactSizeIterator {
        (0..self.len()).map(|i| self.value_at(i))
    }

    pub fn from_protobuf(array: &PbArray, cardinality: usize) -> ArrayResult<Self> {
        use self::column_proto_readers::*;
        use crate::array::value_reader::*;
        let array = match array.array_type() {
            PbArrayType::Int16 => read_numeric_array::<i16, I16ValueReader>(array, cardinality)?,
            PbArrayType::Int32 => read_numeric_array::<i32, I32ValueReader>(array, cardinality)?,
            PbArrayType::Int64 => read_numeric_array::<i64, I64ValueReader>(array, cardinality)?,
            PbArrayType::Serial => {
                read_numeric_array::<Serial, SerialValueReader>(array, cardinality)?
            }
            PbArrayType::Float32 => read_numeric_array::<F32, F32ValueReader>(array, cardinality)?,
            PbArrayType::Float64 => read_numeric_array::<F64, F64ValueReader>(array, cardinality)?,
            PbArrayType::Bool => read_bool_array(array, cardinality)?,
            PbArrayType::Utf8 => {
                read_string_array::<Utf8ArrayBuilder, Utf8ValueReader>(array, cardinality)?
            }
            PbArrayType::Decimal => {
                read_numeric_array::<Decimal, DecimalValueReader>(array, cardinality)?
            }
            PbArrayType::Date => read_date_array(array, cardinality)?,
            PbArrayType::Time => read_time_array(array, cardinality)?,
            PbArrayType::Timestamp => read_timestamp_array(array, cardinality)?,
            PbArrayType::Interval => read_interval_array(array, cardinality)?,
            PbArrayType::Jsonb => {
                read_string_array::<JsonbArrayBuilder, JsonbValueReader>(array, cardinality)?
            }
            PbArrayType::Struct => StructArray::from_protobuf(array)?,
            PbArrayType::List => ListArray::from_protobuf(array)?,
            PbArrayType::Unspecified => unreachable!(),
            PbArrayType::Bytea => {
                read_string_array::<BytesArrayBuilder, BytesValueReader>(array, cardinality)?
            }
            PbArrayType::Int256 => Int256Array::from_protobuf(array, cardinality)?,
            PbArrayType::Uint256 => Uint256Array::from_protobuf(array, cardinality)?,
        };
        Ok(array)
    }
}

impl ArrayBuilderImpl {
    /// Create an array builder from given type.
    pub fn from_type(datatype: &DataType, capacity: usize) -> Self {
        datatype.create_array_builder(capacity)
    }
}

pub type ArrayRef = Arc<ArrayImpl>;

impl PartialEq for ArrayImpl {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn filter<'a, A, F>(data: &'a A, pred: F) -> ArrayResult<A>
    where
        A: Array + 'a,
        F: Fn(Option<A::RefItem<'a>>) -> bool,
    {
        let mut builder = A::Builder::with_meta(data.len(), data.array_meta());
        for i in 0..data.len() {
            if pred(data.value_at(i)) {
                builder.append(data.value_at(i));
            }
        }
        Ok(builder.finish())
    }

    #[test]
    fn test_filter() {
        let mut builder = PrimitiveArrayBuilder::<i32>::new(0);
        for i in 0..=60 {
            builder.append(Some(i));
        }
        let array = filter(&builder.finish(), |x| x.unwrap_or(0) >= 60).unwrap();
        assert_eq!(array.iter().collect::<Vec<Option<i32>>>(), vec![Some(60)]);
    }

    use num_traits::cast::AsPrimitive;
    use num_traits::ops::checked::CheckedAdd;

    fn vec_add<T1, T2, T3>(
        a: &PrimitiveArray<T1>,
        b: &PrimitiveArray<T2>,
    ) -> ArrayResult<PrimitiveArray<T3>>
    where
        T1: PrimitiveArrayItemType + AsPrimitive<T3>,
        T2: PrimitiveArrayItemType + AsPrimitive<T3>,
        T3: PrimitiveArrayItemType + CheckedAdd,
    {
        let mut builder = PrimitiveArrayBuilder::<T3>::new(a.len());
        for (a, b) in a.iter().zip_eq_fast(b.iter()) {
            let item = match (a, b) {
                (Some(a), Some(b)) => Some(a.as_() + b.as_()),
                _ => None,
            };
            builder.append(item);
        }
        Ok(builder.finish())
    }

    #[test]
    fn test_vectorized_add() {
        let mut builder = PrimitiveArrayBuilder::<i32>::new(0);
        for i in 0..=60 {
            builder.append(Some(i));
        }
        let array1 = builder.finish();

        let mut builder = PrimitiveArrayBuilder::<i16>::new(0);
        for i in 0..=60 {
            builder.append(Some(i as i16));
        }
        let array2 = builder.finish();

        let final_array = vec_add(&array1, &array2).unwrap() as PrimitiveArray<i64>;

        assert_eq!(final_array.len(), array1.len());
        for (idx, data) in final_array.iter().enumerate() {
            assert_eq!(data, Some(idx as i64 * 2));
        }
    }
}

#[cfg(test)]
mod test_util {
    use std::hash::{BuildHasher, Hasher};

    use super::Array;
    use crate::util::iter_util::ZipEqFast;

    pub fn hash_finish<H: Hasher>(hashers: &mut [H]) -> Vec<u64> {
        return hashers
            .iter()
            .map(|hasher| hasher.finish())
            .collect::<Vec<u64>>();
    }

    pub fn test_hash<H: BuildHasher, A: Array>(arrs: Vec<A>, expects: Vec<u64>, hasher_builder: H) {
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
        arrs.iter()
            .for_each(|arr| arr.hash_vec(&mut states_vec[..]));
        itertools::cons_tuples(
            expects
                .iter()
                .zip_eq_fast(hash_finish(&mut states_scalar[..]))
                .zip_eq_fast(hash_finish(&mut states_vec[..])),
        )
        .all(|(a, b, c)| *a == b && b == c);
    }
}
