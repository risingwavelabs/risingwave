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

use std::collections::HashSet;
use either::Either;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::row::{OwnedRow, Project, RowExt};

pub mod row_serde_util;

pub mod value_serde;

/// Partition non-output columns and output columns.
///
/// # Returns
/// A pair of non-output column_indices and output column_indices
pub fn partition_output_column_indices(
    table_columns: &[ColumnDesc],
    output_column_ids: &[ColumnId],
) -> (Vec<usize>, Vec<usize>) {
    let output_column_ids = output_column_ids.iter().collect::<HashSet<_>>();
    let (pruned_indices, output_indices) = table_columns
        .iter()
        .enumerate()
        .partition_map(|(index, c)| {
            if output_column_ids.contains(&c.column_id) {
                Either::Right(index)
            } else {
                Either::Left(index)
            }
        });
    (pruned_indices, output_indices)
}

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
    pub fn project(&self, origin_row: OwnedRow) -> Project<'_, OwnedRow> {
        origin_row.project(&self.output_indices)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;
    use super::*;

    #[test]
    fn test_partition_output_column_indices(
    ) {
        let output_column_ids = vec![ColumnId::new(1), ColumnId::new(2)];
        let table_columns = vec![
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Varchar),
        ];
        let (non_output_indices, output_indices) =
            partition_output_column_indices(&table_columns, &output_column_ids);
        let expected_non_output_indices = vec![2];
        let expected_output_indices = vec![0, 1];
        assert_eq!(non_output_indices, expected_non_output_indices);
        assert_eq!(output_indices, expected_output_indices);
    }
}