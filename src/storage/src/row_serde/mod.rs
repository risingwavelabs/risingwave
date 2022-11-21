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

use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};

pub mod row_serde_util;

/// Find out the [`ColumnDesc`] by a list of [`ColumnId`].
///
/// # Returns
///
/// A pair of columns and their indexes in input columns
pub fn find_columns_by_ids(
    table_columns: &[ColumnDesc],
    column_ids: &[ColumnId],
) -> (Vec<ColumnDesc>, Vec<usize>) {
    use std::collections::HashMap;
    let mut table_columns = table_columns
        .iter()
        .enumerate()
        .map(|(index, c)| (c.column_id, (c.clone(), index)))
        .collect::<HashMap<_, _>>();
    column_ids
        .iter()
        .map(|id| table_columns.remove(id).unwrap())
        .unzip()
}

#[derive(Clone)]
pub struct ColumnMapping {
    output_indices: Vec<usize>,
}

#[allow(clippy::len_without_is_empty)]
impl ColumnMapping {
    /// Create a mapping with given `table_columns` projected on the `column_ids`.
    pub fn new(output_indices: Vec<usize>) -> Self {
        Self { output_indices }
    }

    /// Project a row with this mapping
    pub fn project(&self, mut origin_row: Row) -> Row {
        let mut output_row = Vec::with_capacity(self.output_indices.len());
        for col_idx in &self.output_indices {
            output_row.push(origin_row.0[*col_idx].take());
        }
        Row(output_row)
    }
}
