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

use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::util::ordered::{
    serialize_pk_and_column_id, serialize_pk_and_row, SENTINEL_CELL_ID,
};

use crate::cell_serializer::{CellSerializer, KeyBytes, ValueBytes};

#[derive(Clone)]
pub struct CellBasedRowSerializer {
    column_ids: Vec<ColumnId>,
}

impl CellBasedRowSerializer {
    pub fn new(column_ids: Vec<ColumnId>) -> Self {
        Self { column_ids }
    }
}

impl CellSerializer for CellBasedRowSerializer {
    /// Serialize key and value. The `row` must be in the same order with the column ids in this
    /// serializer.
    fn serialize(&mut self, pk: &[u8], row: Row) -> Result<Vec<(KeyBytes, ValueBytes)>> {
        let res = serialize_pk_and_row(pk, &row, &self.column_ids)?
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
    ) -> Result<Vec<Option<(KeyBytes, ValueBytes)>>> {
        let res = serialize_pk_and_row(pk, &row, &self.column_ids)?;
        Ok(res)
    }

    /// Different from [`CellBasedRowSerializer::serialize`], only serialize key into cell key (With
    /// column id appended).
    fn serialize_cell_key(&mut self, pk: &[u8], row: &Row) -> Result<Vec<KeyBytes>> {
        let mut results = Vec::with_capacity(self.column_ids.len());
        for (index, col_id) in self.column_ids.iter().enumerate() {
            if row[index].is_none() {
                continue;
            }
            results.push(serialize_pk_and_column_id(pk, col_id)?);
        }
        results.push(serialize_pk_and_column_id(pk, &SENTINEL_CELL_ID)?);
        Ok(results)
    }

    /// Get column ids used by cell serializer to serialize.
    /// TODO: This should probably not be exposed to user.
    fn column_ids(&self) -> &[ColumnId] {
        &self.column_ids
    }
}
