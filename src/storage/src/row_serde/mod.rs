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

use std::collections::HashMap;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::row::{OwnedRow, Project, RowExt};
use risingwave_common::util::column_index_mapping::ColIndexMapping;

pub mod row_serde_util;

pub mod value_serde;

/// Gets pair of i2o column mapping and output column_indices
pub fn get_output_column_indices(
    table_columns: &[ColumnDesc],
    output_column_ids: &[ColumnId],
) -> (ColIndexMapping, Vec<usize>) {
    let output_column_ids_to_input_idx = output_column_ids
        .iter()
        .enumerate()
        .map(|(pos, id) | (*id, pos))
        .collect::<HashMap<_, _>>();
    let mut i2o_mapping = vec![None; table_columns.len()];
    let mut output_column_indices = vec![];
    for (i, table_column) in table_columns.iter().enumerate() {
        if let Some(pos) = output_column_ids_to_input_idx.get(&table_column.column_id) {
            i2o_mapping[i] = Some(*pos);
            output_column_indices.push(i);
        }
    }
    let i2o_mapping = ColIndexMapping::new(i2o_mapping, output_column_indices.len());
    (i2o_mapping, output_column_indices)
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
    use std::fmt::Debug;
    use risingwave_common::types::DataType;
    use expect_test::{expect, Expect};
    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_get_output_column_indices(
    ) {
        let output_column_ids = vec![ColumnId::new(1), ColumnId::new(2)];
        let table_columns = vec![
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Varchar),
        ];
        let (i2o_mapping, output_indices) =
            get_output_column_indices(&table_columns, &output_column_ids);
        check(i2o_mapping, expect!["ColIndexMapping(source_size:3, target_size:2, mapping:0->0,1->1)"]);
        check(output_indices, expect![[r#"
            [
                0,
                1,
            ]"#]]);
    }
}