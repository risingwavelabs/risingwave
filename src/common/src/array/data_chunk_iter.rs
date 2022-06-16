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

use std::hash::{BuildHasher, Hash, Hasher};
use std::ops;

use bytes::Buf;
use itertools::Itertools;

use super::column::Column;
use crate::array::DataChunk;
use crate::error::Result as RwResult;
use crate::hash::HashCode;
use crate::types::{
    deserialize_datum_from, deserialize_datum_not_null_from, serialize_datum_into,
    serialize_datum_not_null_into, DataType, Datum, DatumRef, ToOwnedDatum,
};
use crate::util::sort_util::OrderType;
use crate::util::value_encoding::{deserialize_datum, serialize_datum};

impl DataChunk {
    /// Get an iterator for visible rows.
    pub fn rows(&self) -> impl Iterator<Item = RowRef> {
        DataChunkRefIter {
            chunk: self,
            idx: Some(0),
        }
    }
}

struct DataChunkRefIter<'a> {
    chunk: &'a DataChunk,
    /// `None` means finished
    idx: Option<usize>,
}

impl<'a> Iterator for DataChunkRefIter<'a> {
    type Item = RowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.idx {
            None => None,
            Some(idx) => {
                self.idx = self.chunk.next_visible_row_idx(idx);
                match self.idx {
                    None => None,
                    Some(idx) => {
                        self.idx = Some(idx + 1);
                        Some(RowRef {
                            chunk: self.chunk,
                            idx,
                        })
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct RowRef<'a> {
    chunk: &'a DataChunk,

    idx: usize,
}

impl<'a> std::fmt::Debug for RowRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.values()).finish()
    }
}

impl<'a> RowRef<'a> {
    pub fn new(chunk: &'a DataChunk, idx: usize) -> Self {
        debug_assert!(idx < chunk.capacity());
        Self { chunk, idx }
    }

    pub fn value_at(&self, pos: usize) -> DatumRef<'_> {
        debug_assert!(self.idx < self.chunk.capacity());
        // for `RowRef`, the index is always in bound.
        unsafe {
            self.chunk.columns()[pos]
                .array_ref()
                .value_at_unchecked(self.idx)
        }
    }

    pub fn size(&self) -> usize {
        self.chunk.columns().len()
    }

    pub fn values<'b>(&'b self) -> impl Iterator<Item = DatumRef<'a>>
    where
        'a: 'b,
    {
        debug_assert!(self.idx < self.chunk.capacity());
        RowRefIter::<'a> {
            columns: self.chunk.columns().iter(),
            row_idx: self.idx,
        }
    }

    pub fn to_owned_row(&self) -> Row {
        Row(self.values().map(ToOwnedDatum::to_owned_datum).collect())
    }

    /// Get an owned `Row` by the given `indices` from current row ref.
    ///
    /// Use `datum_refs_by_indices` if possible instead to avoid allocating owned datums.
    pub fn row_by_indices(&self, indices: &[usize]) -> Row {
        Row(indices
            .iter()
            .map(|&idx| self.value_at(idx).to_owned_datum())
            .collect_vec())
    }

    /// Get an iterator of datum refs by the given `indices` from current row ref.
    pub fn datum_refs_by_indices<'b, 'c>(
        &'b self,
        indices: &'c [usize],
    ) -> impl Iterator<Item = DatumRef<'c>>
    where
        'a: 'b,
        'b: 'c,
    {
        indices.iter().map(|&idx| self.value_at(idx))
    }

    /// Get the index of this row in the data chunk.
    #[must_use]
    pub(super) fn index(&self) -> usize {
        self.idx
    }
}

impl<'a> PartialEq for RowRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.values()
            .zip_longest(other.values())
            .all(|pair| pair.both().map(|(a, b)| a == b).unwrap_or(false))
    }
}

impl<'a> Eq for RowRef<'a> {}

#[derive(Clone)]
struct RowRefIter<'a> {
    columns: std::slice::Iter<'a, Column>,
    row_idx: usize,
}

impl<'a> Iterator for RowRefIter<'a> {
    type Item = DatumRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: for `RowRef`, the index is always in bound.
        unsafe {
            self.columns
                .next()
                .map(|col| col.array_ref().value_at_unchecked(self.row_idx))
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Row(pub Vec<Datum>);

impl ops::Index<usize> for Row {
    type Output = Datum;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl AsRef<Row> for Row {
    #[inline]
    fn as_ref(&self) -> &Row {
        self
    }
}

// TODO: remove this due to implicit allocation
impl From<RowRef<'_>> for Row {
    fn from(row_ref: RowRef<'_>) -> Self {
        row_ref.to_owned_row()
    }
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.0.len() != other.0.len() {
            return None;
        }
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Row {
    pub fn new(values: Vec<Datum>) -> Self {
        Self(values)
    }

    pub fn empty() -> &'static Self {
        static EMPTY_ROW: Row = Row(Vec::new());
        &EMPTY_ROW
    }

    /// Serialize the row into a memcomparable bytes.
    ///
    /// All values are nullable. Each value will have 1 extra byte to indicate whether it is null.
    pub fn serialize(&self) -> Result<Vec<u8>, memcomparable::Error> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        for v in &self.0 {
            serialize_datum_into(v, &mut serializer)?;
        }
        Ok(serializer.into_inner())
    }

    /// Serialize the row into a memcomparable bytes. All values must not be null.
    pub fn serialize_not_null(&self) -> Result<Vec<u8>, memcomparable::Error> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        for v in &self.0 {
            serialize_datum_not_null_into(v, &mut serializer)?;
        }
        Ok(serializer.into_inner())
    }

    /// Serialize the row into a memcomparable bytes based on the orderings.
    pub fn serialize_with_order(
        &self,
        orders: &[OrderType],
    ) -> Result<Vec<u8>, memcomparable::Error> {
        assert_eq!(self.0.len(), orders.len());
        let mut serializer = memcomparable::Serializer::new(vec![]);
        for (order, datum) in orders.iter().zip_eq(self.0.iter()) {
            serializer.set_reverse(*order == OrderType::Descending);
            serialize_datum_into(datum, &mut serializer)?;
        }
        Ok(serializer.into_inner())
    }

    /// Serialize a datum in the row to a memcomparable bytes. The datum must not be null.
    ///
    /// !Panics
    ///
    /// * Panics when `datum_idx` is out of range.
    pub fn serialize_datum(&self, datum_idx: usize) -> Result<Vec<u8>, memcomparable::Error> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        serialize_datum_into(&self.0[datum_idx], &mut serializer)?;
        Ok(serializer.into_inner())
    }

    /// Serialize the row into a value encode bytes.
    ///
    /// All values are nullable. Each value will have 1 extra byte to indicate whether it is null.
    pub fn value_encode(&self) -> RwResult<Vec<u8>> {
        let mut vec = vec![];
        for v in &self.0 {
            vec.extend(serialize_datum(v)?);
        }
        Ok(vec)
    }

    /// Return number of cells in the row.
    pub fn size(&self) -> usize {
        self.0.len()
    }

    pub fn values(&self) -> impl Iterator<Item = &Datum> {
        self.0.iter()
    }

    /// Hash row data all in one
    pub fn hash_row<H>(&self, hash_builder: &H) -> HashCode
    where
        H: BuildHasher,
    {
        let mut hasher = hash_builder.build_hasher();
        for datum in &self.0 {
            datum.hash(&mut hasher);
        }
        HashCode(hasher.finish())
    }

    /// Compute hash value of a row on corresponding indices.
    pub fn hash_by_indices<H>(
        &self,
        hash_indices: &[usize],
        hash_builder: &H,
    ) -> super::ArrayResult<HashCode>
    where
        H: BuildHasher,
    {
        let mut hasher = hash_builder.build_hasher();
        for idx in hash_indices {
            let datum = self.0.get(*idx);
            match datum {
                Some(datum) => datum.hash(&mut hasher),
                None => bail!("index {} out of row bound", idx),
            }
        }
        Ok(HashCode(hasher.finish()))
    }

    /// Get an owned `Row` by the given `indices` from current row.
    ///
    /// Use `datum_refs_by_indices` if possible instead to avoid allocating owned datums.
    pub fn by_indices(&self, indices: &[usize]) -> Row {
        Row(indices.iter().map(|&idx| self.0[idx].clone()).collect_vec())
    }
}

/// Deserializer of the `Row`.
pub struct RowDeserializer {
    data_types: Vec<DataType>,
}

impl RowDeserializer {
    /// Creates a new `RowDeserializer` with row schema.
    pub fn new(schema: Vec<DataType>) -> Self {
        RowDeserializer { data_types: schema }
    }

    /// Deserialize the row from a memcomparable bytes.
    pub fn deserialize(&self, data: &[u8]) -> Result<Row, memcomparable::Error> {
        let mut values = Vec::with_capacity(self.data_types.len());
        let mut deserializer = memcomparable::Deserializer::new(data);
        for ty in &self.data_types {
            values.push(deserialize_datum_from(ty, &mut deserializer)?);
        }
        Ok(Row(values))
    }

    /// Deserialize the row from a memcomparable bytes. All values are not null.
    pub fn deserialize_not_null(&self, data: &[u8]) -> Result<Row, memcomparable::Error> {
        let mut values = Vec::with_capacity(self.data_types.len());
        let mut deserializer = memcomparable::Deserializer::new(data);
        for ty in &self.data_types {
            values.push(deserialize_datum_not_null_from(
                ty.clone(),
                &mut deserializer,
            )?);
        }
        Ok(Row(values))
    }

    /// Deserialize a datum in the row to a memcomparable bytes. The datum must not be null.
    ///
    /// !Panics
    ///
    /// * Panics when `datum_idx` is out of range.
    pub fn deserialize_datum(
        &self,
        data: &[u8],
        datum_idx: usize,
    ) -> Result<Datum, memcomparable::Error> {
        let mut deserializer = memcomparable::Deserializer::new(data);
        let datum = deserialize_datum_from(&self.data_types[datum_idx], &mut deserializer)?;
        Ok(datum)
    }

    /// Deserialize the row from a value encoding bytes.
    pub fn value_decode(&self, mut data: impl Buf) -> RwResult<Row> {
        let mut values = Vec::with_capacity(self.data_types.len());
        for ty in &self.data_types {
            values.push(deserialize_datum(&mut data, ty)?);
        }
        Ok(Row(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType as Ty, IntervalUnit, ScalarImpl};
    use crate::util::hash_util::CRC32FastBuilder;

    #[test]
    fn row_memcomparable_encode_decode_not_null() {
        let row = Row(vec![
            Some(ScalarImpl::Utf8("string".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
            Some(ScalarImpl::Interval(IntervalUnit::new(7, 8, 9))),
        ]);
        let bytes = row.serialize_not_null().unwrap();
        assert_eq!(bytes.len(), 10 + 1 + 2 + 4 + 8 + 4 + 8 + 5 + 16);

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
        let row1 = de.deserialize_not_null(&bytes).unwrap();
        assert_eq!(row, row1);
    }

    #[test]
    fn row_memcomparable_encode_decode() {
        let row = Row(vec![
            Some(ScalarImpl::Utf8("string".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
            Some(ScalarImpl::Interval(IntervalUnit::new(7, 8, 9))),
        ]);
        let bytes = row.serialize().unwrap();
        assert_eq!(bytes.len(), 10 + 1 + 2 + 4 + 8 + 4 + 8 + 5 + 16 + 9);

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
        let row1 = de.deserialize(&bytes).unwrap();
        assert_eq!(row, row1);
    }

    #[test]
    fn test_hash_row() {
        let hash_builder = CRC32FastBuilder {};

        let row1 = Row(vec![
            Some(ScalarImpl::Utf8("string".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
            Some(ScalarImpl::Interval(IntervalUnit::new(7, 8, 9))),
        ]);
        let row2 = Row(vec![
            Some(ScalarImpl::Interval(IntervalUnit::new(7, 8, 9))),
            Some(ScalarImpl::Utf8("string".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
        ]);
        assert_ne!(row1.hash_row(&hash_builder), row2.hash_row(&hash_builder));

        let row_default = Row::default();
        assert_eq!(row_default.hash_row(&hash_builder).0, 0);
    }
}
