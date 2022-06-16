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

use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::Result;
use risingwave_common::types::Datum;
use risingwave_common::util::ordered::{
    serialize_pk_and_column_id, serialize_pk_and_row, SENTINEL_CELL_ID,
};

use crate::cell_serializer::{CellSerializer, KeyBytes, ValueBytes};

pub struct DedupPkCellBasedRowSerializer {
    /// Row indices of datums are already in pk,
    /// or have to be stored regardless (e.g. if memcomparable not equal to value encoding)
    dedup_datum_indices: HashSet<usize>,
}

impl DedupPkCellBasedRowSerializer {
    pub fn new(pk_indices: &[usize], column_descs: &Vec<ColumnDesc>) -> Self {
        let pk_indices = pk_indices.iter().cloned().collect::<HashSet<_>>();
        let dedup_datum_indices = (0..column_descs.len())
            .filter(|i| {
                !pk_indices.contains(i) || !column_descs[*i].data_type.mem_cmp_eq_value_enc()
            })
            .collect();
        Self {
            dedup_datum_indices,
        }
    }

    fn remove_dup_pk_datums_by_ref<'a>(&self, row: &'a Row) -> Vec<&'a Datum> {
        row.0
            .iter()
            .enumerate()
            .filter(|(i, _)| self.dedup_datum_indices.contains(i))
            .map(|(_, d)| d)
            .collect()
    }

    fn remove_dup_pk_datums(&self, row: Row) -> Row {
        Row(row
            .0
            .into_iter()
            .enumerate()
            .filter(|(i, _)| self.dedup_datum_indices.contains(i))
            .map(|(_, d)| d)
            .collect())
    }

    fn remove_dup_pk_column_ids(&self, column_ids: &[ColumnId]) -> Vec<ColumnId> {
        column_ids
            .iter()
            .enumerate()
            .filter(|(i, _)| self.dedup_datum_indices.contains(i))
            .map(|(_, id)| id)
            .cloned()
            .collect()
    }
}

impl CellSerializer for DedupPkCellBasedRowSerializer {
    /// Serialize key and value.
    fn serialize(
        &mut self,
        pk: &[u8],
        row: Row,
        column_ids: &[ColumnId],
    ) -> Result<Vec<(KeyBytes, ValueBytes)>> {
        let row = self.remove_dup_pk_datums(row);
        let column_ids = &self.remove_dup_pk_column_ids(column_ids);
        let res = serialize_pk_and_row(pk, &row, column_ids)?
            .into_iter()
            .flatten()
            .collect_vec();
        Ok(res)
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
        let column_ids = &self.remove_dup_pk_column_ids(column_ids);
        let res = serialize_pk_and_row(pk, &row, column_ids)?;
        Ok(res)
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
        let column_ids = &self.remove_dup_pk_column_ids(column_ids);
        let mut results = Vec::with_capacity(column_ids.len());
        for (index, col_id) in column_ids.iter().enumerate() {
            if row[index].is_none() {
                continue;
            }
            results.push(serialize_pk_and_column_id(pk, col_id)?);
        }
        results.push(serialize_pk_and_column_id(pk, &SENTINEL_CELL_ID)?);
        Ok(results)
    }
}
