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

use bytes::{Buf, BufMut};
use itertools::Itertools;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::{PbArray, PbArrayType};
use serde::Serializer;

use super::{
    Array, ArrayBuilder, ArrayImpl, ArrayResult, DatumRef, DefaultOrdered, ListArray,
    ListArrayBuilder, ListRef, ListValue, MapType, ScalarRef, ScalarRefImpl, StructArray,
};
use crate::bitmap::Bitmap;
use crate::types::{DataType, Scalar, ToText};
use crate::util::memcmp_encoding;

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
        self.inner.append_n(n, value.map(|v| v.into_inner()));
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
/// - `key`'s datatype can only be string & integral types. (See [`MapType::check_key_type_valid`].)
/// - `value` can be any type.
///
/// Value (for each map value in the array):
/// - `key`s are non-null and unique.
///
/// - `key`s and `value`s must be of the same length.
///   For a `MapArray`, it's sliced by the `ListArray`'s offsets, so it essentially means the
///   `key` and `value` children arrays have the same length.
///
/// - The lists are NOT sorted by `key`.
///
/// - `Eq` / `Hash` / `Ord` for map:
///
///   It's controversial due to the physicial representation is just an unordered list.
///   In many systems (e.g., `DuckDB` and `ClickHouse`), `{"k1":"v1","k2":"v2"} != {"k2":"v2","k1":"v1"}`.
///   But the reverse definition might be more intuitive, especially when ingesting Avro/Protobuf data.
///
///   To avoid controversy, we wanted to ban all usages and make the implementation `unreachable!()`,
///   but it's hard since these implementations can be used in different places:
///   * Explicit in User-facing functions (e.g., comparison operators). These could be avoided completely.
///   * Implicit in Keys (group by / order by / primary key). These could also be banned, but it's harder.
///   * Some internal usages. One example is `_row_id`. See <https://github.com/risingwavelabs/risingwave/issues/7981#issuecomment-2257661749>.
///     It might be solvable, but we are not sure whether it's depended somewhere else.
///
///   Considering these, it might be better to still choose a _well-defined_ behavior instead
///   of using `unreachable`. We should try to have a consistent definition for these operations to minimize possible surprises.
///   And we could still try our best to ban it to prevent misuse.
///
///   Currently we choose the second behavior. i.e., first sort the map by key, then compare/hash.
///   Note that `Eq` is intuitive, but `Ord` still looks strange. We assume no users really care about
///   which map is larger, but just provide a implementation to prevent undefined behavior.
///
///   See more discussion in <https://github.com/risingwavelabs/risingwave/issues/7981>.
///
///
/// Note that decisions above are not definitive. Just be conservative at the beginning.
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
        MapRef::new_unchecked(list)
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

pub use scalar::{MapRef, MapValue};

/// We can enforce the invariants (see [`MapArray`]) in too many places
/// (both `MapValue`, `MapRef` and `MapArray`).
///
/// So we define the types and constructors in a separated `mod`
/// to prevent direct construction.
/// We only check the invariants in the constructors.
/// After they are constructed, we assume the invariants holds.
mod scalar {
    use super::*;

    /// Refer to [`MapArray`] for the invariants of a map value.
    #[derive(Clone, Eq, EstimateSize)]
    pub struct MapValue(ListValue);

    /// A map is just a slice of the underlying struct array.
    ///
    /// Refer to [`MapArray`] for the invariants of a map value.
    ///
    /// XXX: perhaps we can make it `MapRef<'a, 'b>(ListRef<'a>, ListRef<'b>);`.
    /// Then we can build a map ref from 2 list refs without copying the data.
    /// Currently it's impossible.
    /// <https://github.com/risingwavelabs/risingwave/issues/17863>
    #[derive(Copy, Clone, Eq)]
    pub struct MapRef<'a>(ListRef<'a>);

    impl MapValue {
        pub fn inner(&self) -> &ListValue {
            &self.0
        }

        pub fn into_inner(self) -> ListValue {
            self.0
        }

        /// # Panics
        /// Panics if [map invariants](`super::MapArray`) are violated.
        pub fn from_list_entries(list: ListValue) -> Self {
            // validates list type is valid
            _ = MapType::from_list_entries(list.data_type());
            // TODO: validate the values is valid
            MapValue(list)
        }

        /// # Panics
        /// Panics if [map invariants](`super::MapArray`) are violated.
        pub fn try_from_kv(key: ListValue, value: ListValue) -> Result<Self, String> {
            if key.len() != value.len() {
                return Err("map keys and values have different length".to_string());
            }
            let unique_keys = key.iter().unique().collect_vec();
            if unique_keys.len() != key.len() {
                return Err("map keys must be unique".to_string());
            }
            if unique_keys.contains(&None) {
                return Err("map keys must not be NULL".to_string());
            }

            let len = key.len();
            let key_type = key.data_type();
            let value_type = value.data_type();
            let struct_array = StructArray::new(
                MapType::struct_type_for_map(key_type, value_type),
                vec![key.into_array().into_ref(), value.into_array().into_ref()],
                Bitmap::ones(len),
            );
            Ok(MapValue(ListValue::new(struct_array.into())))
        }
    }

    impl<'a> MapRef<'a> {
        /// # Safety
        /// The caller must ensure the invariants of a map value.
        pub unsafe fn new_unchecked(list: ListRef<'a>) -> Self {
            MapRef(list)
        }

        pub fn inner(&self) -> &ListRef<'a> {
            &self.0
        }

        pub fn into_inner(self) -> ListRef<'a> {
            self.0
        }
    }

    impl Scalar for MapValue {
        type ScalarRefType<'a> = MapRef<'a>;

        fn as_scalar_ref(&self) -> MapRef<'_> {
            // MapValue is assumed to be valid, so we just construct directly without check invariants.
            MapRef(self.0.as_scalar_ref())
        }
    }

    impl<'a> ScalarRef<'a> for MapRef<'a> {
        type ScalarType = MapValue;

        fn to_owned_scalar(&self) -> MapValue {
            // MapRef is assumed to be valid, so we just construct directly without check invariants.
            MapValue(self.0.to_owned_scalar())
        }

        fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H) {
            for (k, v) in self.iter_sorted() {
                super::super::hash_datum(Some(k), state);
                super::super::hash_datum(v, state);
            }
        }
    }
}

/// Refer to [`MapArray`] for the semantics of the comparison.
mod cmp {
    use super::*;
    use crate::array::DefaultOrd;
    impl PartialEq for MapArray {
        fn eq(&self, other: &Self) -> bool {
            self.iter().eq(other.iter())
        }
    }

    impl PartialEq for MapValue {
        fn eq(&self, other: &Self) -> bool {
            self.as_scalar_ref().eq(&other.as_scalar_ref())
        }
    }

    impl PartialOrd for MapValue {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for MapValue {
        fn cmp(&self, other: &Self) -> Ordering {
            self.as_scalar_ref().cmp(&other.as_scalar_ref())
        }
    }

    impl PartialEq for MapRef<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.iter_sorted().eq(other.iter_sorted())
        }
    }

    impl Ord for MapRef<'_> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.iter_sorted()
                .cmp_by(other.iter_sorted(), |(k1, v1), (k2, v2)| {
                    k1.default_cmp(&k2).then_with(|| v1.default_cmp(&v2))
                })
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

impl<'a> MapRef<'a> {
    /// Iterates over the elements of the map.
    pub fn iter(
        self,
    ) -> impl DoubleEndedIterator + ExactSizeIterator<Item = (ScalarRefImpl<'a>, DatumRef<'a>)> + 'a
    {
        self.inner().iter().map(|list_elem| {
            let list_elem = list_elem.expect("the list element in map should not be null");
            let struct_ = list_elem.into_struct();
            let (k, v) = struct_
                .iter_fields_ref()
                .next_tuple()
                .expect("the struct in map should have exactly 2 fields");
            (k.expect("map key should not be null"), v)
        })
    }

    pub fn iter_sorted(
        self,
    ) -> impl DoubleEndedIterator + ExactSizeIterator<Item = (ScalarRefImpl<'a>, DatumRef<'a>)> + 'a
    {
        self.iter().sorted_by_key(|(k, _v)| DefaultOrdered(*k))
    }

    /// Note: Map should not be used as key. But we don't want to panic.
    /// See [`MapArray`] for the semantics. See also the `Ord` implementation.
    /// TODO: ban it in fe <https://github.com/risingwavelabs/risingwave/issues/7981>
    pub fn memcmp_serialize(
        self,
        serializer: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        let mut inner_serializer = memcomparable::Serializer::new(vec![]);
        for (k, v) in self.iter_sorted() {
            memcmp_encoding::serialize_datum_in_composite(Some(k), &mut inner_serializer)?;
            memcmp_encoding::serialize_datum_in_composite(v, &mut inner_serializer)?;
        }
        serializer.serialize_bytes(&inner_serializer.into_inner())
    }
}

impl MapValue {
    /// Note: Map should not be used as key. But we don't want to panic.
    /// See [`MapArray`] for the semantics. See also the `Ord` implementation.
    /// TODO: ban it in fe <https://github.com/risingwavelabs/risingwave/issues/7981>
    pub fn memcmp_deserialize(
        datatype: &MapType,
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        let list = ListValue::memcmp_deserialize(
            &DataType::Struct(datatype.clone().into_struct()),
            deserializer,
        )?;
        Ok(Self::from_list_entries(list))
    }
}

impl Debug for MapRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.inner().iter()).finish()
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
