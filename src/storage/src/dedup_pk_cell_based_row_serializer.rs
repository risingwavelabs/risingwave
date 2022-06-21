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

use std::collections::HashSet;
use std::iter::Iterator;

use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::Result;

use crate::cell_based_row_serializer::CellBasedRowSerializer;
use crate::cell_serializer::{CellSerializer, KeyBytes, ValueBytes};

/// `DedupPkCellBasedRowSerializer` is identical to `CellBasedRowSerializer`.
/// Difference is that before serializing a row, pk datums are filtered out.
pub struct DedupPkCellBasedRowSerializer<'a> {
    /// Row indices of datums are already in pk,
    /// or have to be stored regardless (e.g. if memcomparable not equal to value encoding)
    dedup_datum_indices: HashSet<usize>,

    /// Serializing of row after filtering pk datums
    /// should be same as `CellBasedRowSerializer`.
    /// Hence we reuse its functionality.
    inner: CellBasedRowSerializer<'a>,
}

impl DedupPkCellBasedRowSerializer<'_> {
    pub fn new(pk_indices: &[usize], column_descs: &Vec<ColumnDesc>) -> Self {
        let pk_indices = pk_indices.iter().cloned().collect::<HashSet<_>>();
        let dedup_datum_indices = (0..column_descs.len())
            .filter(|i| {
                !pk_indices.contains(i) || !column_descs[*i].data_type.mem_cmp_eq_value_enc()
            })
            .collect();
        let column_ids = remove_dup_pk_column_ids(dedup_datum_indices, column_ids);
        let inner = CellBasedRowSerializer::new(column_ids);
        Self {
            dedup_datum_indices,
            inner,
        }
    }

    fn filter_by_dedup_datum_indices<'a, I>(
        dedup_datum_indices: &HashSet<usize>,
        iter: impl Iterator<Item = I> + 'a,
    ) -> impl Iterator<Item = I> + 'a {
        iter.enumerate()
            .filter(|(i, _)| dedup_datum_indices.contains(i))
            .map(|(_, d)| d)
    }

    fn remove_dup_pk_datums_by_ref(&self, row: &Row) -> Row {
        Row(filter_by_dedup_datum_indices(self.dedup_datum_indices, row.0.iter())
            .cloned()
            .collect())
    }

    fn remove_dup_pk_datums(&self, row: Row) -> Row {
        Row(filter_by_dedup_datum_indices(self.dedup_datum_indices, row.0.into_iter())
            .collect())
    }

    fn remove_dup_pk_column_ids(dedup_datum_indices: &HashSet<usize>, column_ids: &[ColumnId]) -> Vec<ColumnId> {
        self.filter_by_dedup_datum_indices(column_ids.iter())
            .cloned()
            .collect()
    }
}

impl CellSerializer for DedupPkCellBasedRowSerializer<'_> {
    /// Serialize key and value.
    fn serialize(
        &mut self,
        pk: &[u8],
        row: Row,
        column_ids: &[ColumnId],
    ) -> Result<Vec<(KeyBytes, ValueBytes)>> {
        let row = self.remove_dup_pk_datums(row);
        self.inner.serialize(pk, row)
    }

    /// Serialize key and value. Each column id will occupy a position in Vec. For `column_ids` that
    /// doesn't correspond to a cell, the position will be None. Aparts from user-specified
    /// `column_ids`, there will also be a `SENTINEL_CELL_ID` at the end.
    fn serialize_without_filter(
        &mut self,
        pk: &[u8],
        row: Row,
        column_ids: &[ColumnId],
    ) -> Result<Vec<Option<(KeyBytes, ValueBytes)>>> {
        let row = self.remove_dup_pk_datums(row);
        self.inner.serialize_without_filter(pk, row)
    }

    /// Different from [`DedupPkCellBasedRowSerializer::serialize`], only serialize key into cell
    /// key (With column id appended).
    fn serialize_cell_key(
        &mut self,
        pk: &[u8],
        row: &Row,
        column_ids: &[ColumnId],
    ) -> Result<Vec<KeyBytes>> {
        let row = self.remove_dup_pk_datums_by_ref(row);
        self.inner.serialize_cell_key(pk, &row)
    }
}
