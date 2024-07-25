// Copyright 2024 RisingWave Labs
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

use std::cmp::Ordering;
use std::fmt::{self, Debug, Display};

use itertools::Itertools;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::{PbArray, PbArrayType};

use super::{
    Array, ArrayBuilder, ArrayImpl, ArrayResult, DatumRef, ListArray, ListArrayBuilder, ListRef,
    ListValue, MapType, ScalarRefImpl, StructArray,
};
use crate::bitmap::Bitmap;
use crate::types::{DataType, Scalar, ToText};

#[derive(Debug, Clone, EstimateSize)]
pub struct MapArrayBuilder {
    inner: ListArrayBuilder,
}

impl ArrayBuilder for MapArrayBuilder {
    type ArrayType = MapArray;

    #[cfg(not(test))]
    fn new(_capacity: usize) -> Self {
        panic!("please use `MapArrayBuilder::with_type` instead");
    }

    #[cfg(test)]
    fn new(capacity: usize) -> Self {
        Self::with_type(
            capacity,
            DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Varchar)),
        )
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        let inner = ListArrayBuilder::with_type(capacity, ty.into_map().into_list());
        Self { inner }
    }

    fn append_n(&mut self, n: usize, value: Option<MapRef<'_>>) {
        self.inner.append_n(n, value.map(|v| v.0));
    }

    fn append_array(&mut self, other: &MapArray) {
        self.inner.append_array(&other.inner);
    }

    fn pop(&mut self) -> Option<()> {
        self.inner.pop()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn finish(self) -> MapArray {
        let inner = self.inner.finish();
        MapArray { inner }
    }
}

/// `MapArray` is physically just a `List<Struct<key: K, value: V>>` array, but with some additional restrictions.
///
/// Type:
/// - `key`'s datatype can only be string & integral types. (See [`MapType::assert_key_type_valid`].)
/// - `value` can be any type.
///
/// Value (for each map value in the array):
/// - `key`s are non-null and unique.
/// - `key`s and `value`s must be of the same length.
///   For a `MapArray`, it's sliced by the `ListArray`'s offsets, so it essentially means the
///   `key` and `value` children arrays have the same length.
/// - The lists are not sorted by `key`.
/// - Map values are not comparable.
///   And the map type should not be used as (primary/group/join/order) keys.
///   Such usages should be banned in the frontend, and the implementation of `PartialEq`, `Ord` etc. are `unreachable!()`. (See [`cmp`].)
///   Note that this decision is not definitive. Just be conservative at the beginning.
#[derive(Debug, Clone, Eq)]
pub struct MapArray {
    pub(super) inner: ListArray,
}

impl EstimateSize for MapArray {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_heap_size()
    }
}

impl Array for MapArray {
    type Builder = MapArrayBuilder;
    type OwnedItem = MapValue;
    type RefItem<'a> = MapRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        let list = self.inner.raw_value_at_unchecked(idx);
        MapRef(list)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn to_protobuf(&self) -> PbArray {
        let mut array = self.inner.to_protobuf();
        array.array_type = PbArrayType::Map as i32;
        array
    }

    fn null_bitmap(&self) -> &Bitmap {
        self.inner.null_bitmap()
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.inner.into_null_bitmap()
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.inner.set_bitmap(bitmap)
    }

    fn data_type(&self) -> DataType {
        let list_value_type = self.inner.values().data_type();
        DataType::Map(MapType::from_list_entries(list_value_type))
    }
}

impl MapArray {
    pub fn from_protobuf(array: &PbArray) -> ArrayResult<ArrayImpl> {
        let inner = ListArray::from_protobuf(array)?.into_list();
        Ok(Self { inner }.into())
    }

    /// Return the inner struct array of the list array.
    pub fn as_struct(&self) -> &StructArray {
        self.inner.values().as_struct()
    }

    /// Returns the offsets of this map.
    pub fn offsets(&self) -> &[u32] {
        self.inner.offsets()
    }
}

/// Refer to [`MapArray`] for the invariants of a map value.
#[derive(Clone, Eq, EstimateSize)]
pub struct MapValue(pub(crate) ListValue);

mod cmp {
    use super::*;
    impl PartialEq for MapArray {
        fn eq(&self, _other: &Self) -> bool {
            unreachable!("map is not comparable. Such usage should be banned in frontend.")
        }
    }

    impl PartialEq for MapValue {
        fn eq(&self, _other: &Self) -> bool {
            unreachable!("map is not comparable. Such usage should be banned in frontend.")
        }
    }

    impl PartialOrd for MapValue {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for MapValue {
        fn cmp(&self, _other: &Self) -> Ordering {
            unreachable!("map is not comparable. Such usage should be banned in frontend.")
        }
    }

    impl PartialEq for MapRef<'_> {
        fn eq(&self, _other: &Self) -> bool {
            unreachable!("map is not comparable. Such usage should be banned in frontend.")
        }
    }

    impl Ord for MapRef<'_> {
        fn cmp(&self, _other: &Self) -> Ordering {
            unreachable!("map is not comparable. Such usage should be banned in frontend.")
        }
    }

    impl PartialOrd for MapRef<'_> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }
}

impl Debug for MapValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_scalar_ref().fmt(f)
    }
}

impl Display for MapValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_scalar_ref().write(f)
    }
}

impl MapValue {
    pub fn from_list_entries(list: ListValue) -> Self {
        if cfg!(debug_assertions) {
            // validates list type is valid
            _ = MapType::from_list_entries(list.data_type());
        }
        // TODO: validate the values is valid
        MapValue(list)
    }

    pub fn from_kv(keys: ListValue, values: ListValue) -> Self {
        if cfg!(debug_assertions) {
            assert_eq!(
                keys.len(),
                values.len(),
                "keys: {keys:?}, values: {values:?}"
            );
            let unique_keys = keys.iter().unique().collect_vec();
            assert!(
                unique_keys.len() == keys.len(),
                "non unique keys in map: {keys:?}"
            );
            assert!(!unique_keys.contains(&None), "null key in map: {keys:?}");
        }

        let len = keys.len();
        let key_type = keys.data_type();
        let value_type = values.data_type();
        let struct_array = StructArray::new(
            MapType::struct_type_for_map(key_type, value_type),
            vec![keys.into_array().into_ref(), values.into_array().into_ref()],
            Bitmap::ones(len),
        );
        MapValue(ListValue::new(struct_array.into()))
    }
}

/// A map is just a slice of the underlying struct array.
///
/// Refer to [`MapArray`] for the invariants of a map value.
///
/// XXX: perhaps we can make it `MapRef<'a, 'b>(ListRef<'a>, ListRef<'b>);`.
/// Then we can build a map ref from 2 list refs without copying the data.
/// Currently it's impossible.
#[derive(Copy, Clone, Eq)]
pub struct MapRef<'a>(pub(crate) ListRef<'a>);

impl<'a> MapRef<'a> {
    /// Iterates over the elements of the map.
    pub fn iter(
        self,
    ) -> impl DoubleEndedIterator + ExactSizeIterator<Item = (ScalarRefImpl<'a>, DatumRef<'a>)> + 'a
    {
        self.0.iter().map(|list_elem| {
            let list_elem = list_elem.expect("the list element in map should not be null");
            let struct_ = list_elem.into_struct();
            let (k, v) = struct_
                .iter_fields_ref()
                .next_tuple()
                .expect("the struct in map should have exactly 2 fields");
            (k.expect("map key should not be null"), v)
        })
    }
}

impl Debug for MapRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.0.iter()).finish()
    }
}

impl ToText for MapRef<'_> {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        // Note: This is arbitrarily decided...
        write!(
            f,
            "{{{}}}",
            self.iter().format_with(",", |(key, value), f| {
                let key = key.to_text();
                let value = value.to_text();
                // TODO: consider quote like list and struct
                f(&format_args!("\"{}\":{}", key, value))
            })
        )
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            DataType::Map { .. } => self.write(f),
            _ => unreachable!(),
        }
    }
}
