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

use std::ops::{self, Deref};

use super::Row;
use crate::collection::estimate_size::EstimateSize;
use crate::types::{
    DataType, Datum, DatumRef, Decimal, Interval, Timestamp, Date,
    Time, ScalarImpl, ToDatumRef,
};
use crate::util::iter_util::ZipEqDebug;
use crate::util::value_encoding;
use crate::util::value_encoding::deserialize_datum;

/// An owned row type with a `Vec<Datum>`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct OwnedRow(Vec<Datum>);

/// Do not implement `IndexMut` to make it immutable.
impl ops::Index<usize> for OwnedRow {
    type Output = Datum;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl AsRef<OwnedRow> for OwnedRow {
    fn as_ref(&self) -> &OwnedRow {
        self
    }
}

impl OwnedRow {
    /// Returns an empty row.
    ///
    /// Note: use [`empty`](super::empty) if possible.
    pub const fn empty() -> Self {
        Self(vec![])
    }

    pub const fn new(values: Vec<Datum>) -> Self {
        Self(values)
    }

    /// Retrieve the underlying [`Vec<Datum>`].
    pub fn into_inner(self) -> Vec<Datum> {
        self.0
    }

    pub fn as_inner(&self) -> &[Datum] {
        &self.0
    }

    /// Parse an [`OwnedRow`] from a pretty string, only used in tests.
    pub fn from_pretty_with_tys(tys: &[DataType], s: impl AsRef<str>) -> Self {
        let datums: Vec<_> = tys
            .iter()
            .zip_eq_debug(s.as_ref().split_ascii_whitespace())
            .map(|(ty, x)| {
                let scalar: ScalarImpl = match ty {
                    DataType::Int16 => x.parse::<i16>().unwrap().into(),
                    DataType::Int32 => x.parse::<i32>().unwrap().into(),
                    DataType::Int64 => x.parse::<i64>().unwrap().into(),
                    DataType::Float32 => x.parse::<f32>().unwrap().into(),
                    DataType::Float64 => x.parse::<f64>().unwrap().into(),
                    DataType::Varchar => x.to_string().into(),
                    DataType::Boolean => x.parse::<bool>().unwrap().into(),
                    DataType::Date => x.parse::<Date>().unwrap().into(),
                    DataType::Time => x.parse::<Time>().unwrap().into(),
                    DataType::Timestamp => x.parse::<Timestamp>().unwrap().into(),
                    DataType::Interval => x.parse::<Interval>().unwrap().into(),
                    DataType::Decimal => x.parse::<Decimal>().unwrap().into(),
                    _ => todo!(),
                };
                Some(scalar)
            })
            .collect();
        Self::new(datums)
    }
}

impl OwnedRow {
    pub fn is_null(&self, idx: usize) -> bool {
        self[idx].is_none()
    }

    pub fn get_int16(&self, idx: usize) -> i16 {
        match self[idx].as_ref().unwrap() {
            ScalarImpl::Int16(num) => *num,
            _ => unreachable!("type is not int16 at index: {}", idx),
        }
    }

    pub fn get_int32(&self, idx: usize) -> i32 {
        match self[idx].as_ref().unwrap() {
            ScalarImpl::Int32(num) => *num,
            _ => unreachable!("type is not int32 at index: {}", idx),
        }
    }

    pub fn get_int64(&self, idx: usize) -> i64 {
        match self[idx].as_ref().unwrap() {
            ScalarImpl::Int64(num) => *num,
            _ => unreachable!("type is not int64 at index: {}", idx),
        }
    }

    pub fn get_f32(&self, idx: usize) -> f32 {
        match self[idx].as_ref().unwrap() {
            ScalarImpl::Float32(num) => num.into_inner(),
            _ => unreachable!("type is not float32 at index: {}", idx),
        }
    }

    pub fn get_f64(&self, idx: usize) -> f64 {
        match self[idx].as_ref().unwrap() {
            ScalarImpl::Float64(num) => num.into_inner(),
            _ => unreachable!("type is not float64 at index: {}", idx),
        }
    }

    pub fn get_bool(&self, idx: usize) -> bool {
        match self[idx].as_ref().unwrap() {
            ScalarImpl::Bool(num) => *num,
            _ => unreachable!("type is not boolean at index: {}", idx),
        }
    }

    pub fn get_utf8(&self, idx: usize) -> &str {
        match self[idx].as_ref().unwrap() {
            ScalarImpl::Utf8(s) => s.as_ref(),
            _ => unreachable!("type is not utf8 at index: {}", idx),
        }
    }
}

impl EstimateSize for OwnedRow {
    fn estimated_heap_size(&self) -> usize {
        // FIXME(bugen): this is not accurate now as the heap size of some `Scalar` is not counted.
        self.0.capacity() * std::mem::size_of::<Datum>()
    }
}

impl Row for OwnedRow {
    type Iter<'a> = std::iter::Map<std::slice::Iter<'a, Datum>, fn(&'a Datum) -> DatumRef<'a>>
    where
        Self: 'a;

    #[inline]
    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        self[index].to_datum_ref()
    }

    #[inline]
    unsafe fn datum_at_unchecked(&self, index: usize) -> DatumRef<'_> {
        self.0.get_unchecked(index).to_datum_ref()
    }

    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        self.0.iter().map(ToDatumRef::to_datum_ref)
    }

    #[inline]
    fn to_owned_row(&self) -> OwnedRow {
        self.clone()
    }

    #[inline]
    fn into_owned_row(self) -> OwnedRow {
        self
    }
}

/// Deserializer of the [`OwnedRow`].
#[derive(Clone, Debug)]
pub struct RowDeserializer<D: AsRef<[DataType]> = Vec<DataType>> {
    data_types: D,
}

impl<D: AsRef<[DataType]>> RowDeserializer<D> {
    /// Creates a new `RowDeserializer` with row schema.
    pub fn new(data_types: D) -> Self {
        RowDeserializer { data_types }
    }

    /// Deserialize the row from value encoding bytes.
    pub fn deserialize(&self, mut data: impl bytes::Buf) -> value_encoding::Result<OwnedRow> {
        let mut values = Vec::with_capacity(self.data_types().len());
        for typ in self.data_types() {
            values.push(deserialize_datum(&mut data, typ)?);
        }
        Ok(OwnedRow(values))
    }

    pub fn data_types(&self) -> &[DataType] {
        self.data_types.as_ref()
    }
}

/// A simple wrapper for [`OwnedRow`], which assumes that all fields are defined as `ASC` order.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct AscentOwnedRow(OwnedRow);

impl AscentOwnedRow {
    pub fn into_inner(self) -> OwnedRow {
        self.0
    }
}

impl Deref for AscentOwnedRow {
    type Target = OwnedRow;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Row for AscentOwnedRow {
    type Iter<'a> = <OwnedRow as Row>::Iter<'a>;

    deref_forward_row! {}

    fn into_owned_row(self) -> OwnedRow {
        self.into_inner()
    }
}

impl PartialOrd for AscentOwnedRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.as_inner().partial_cmp(other.0.as_inner())
    }
}

impl Ord for AscentOwnedRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other)
            .unwrap_or_else(|| panic!("cannot compare rows with different types"))
    }
}

impl From<OwnedRow> for AscentOwnedRow {
    fn from(row: OwnedRow) -> Self {
        Self(row)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::row::RowExt;
    use crate::types::{DataType as Ty, Interval, ScalarImpl};
    use crate::util::hash_util::Crc32FastBuilder;

    #[test]
    fn row_value_encode_decode() {
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Utf8("string".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
            Some(ScalarImpl::Interval(Interval::from_month_day_usec(
                7, 8, 9,
            ))),
        ]);
        let value_indices = (0..9).collect_vec();
        let bytes = (&row).project(&value_indices).value_serialize();
        assert_eq!(bytes.len(), 10 + 1 + 2 + 4 + 8 + 4 + 8 + 16 + 16 + 9);
        let de = RowDeserializer::new(vec![
            Ty::Varchar,
            Ty::Boolean,
            Ty::Int16,
            Ty::Int32,
            Ty::Int64,
            Ty::Float32,
            Ty::Float64,
            Ty::Decimal,
            Ty::Interval,
        ]);
        let row1 = de.deserialize(bytes.as_ref()).unwrap();
        assert_eq!(row, row1);
    }

    #[test]
    fn test_hash_row() {
        let hash_builder = Crc32FastBuilder;

        let row1 = OwnedRow::new(vec![
            Some(ScalarImpl::Utf8("string".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
            Some(ScalarImpl::Interval(Interval::from_month_day_usec(
                7, 8, 9,
            ))),
        ]);
        let row2 = OwnedRow::new(vec![
            Some(ScalarImpl::Interval(Interval::from_month_day_usec(
                7, 8, 9,
            ))),
            Some(ScalarImpl::Utf8("string".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
        ]);
        assert_ne!(row1.hash(hash_builder), row2.hash(hash_builder));

        let row_default = OwnedRow::default();
        assert_eq!(row_default.hash(hash_builder).0, 0);
    }
}
