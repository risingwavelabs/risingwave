// Copyright 2025 RisingWave Labs
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

pub mod row_serde_util;

pub mod value_serde;

/// Find out the [`ColumnDesc`] selected with a list of [`ColumnId`].
///
/// # Returns
///
/// A pair of columns and their indexes in input columns
pub fn find_columns_by_ids(
    table_columns: &[ColumnDesc],
    column_ids: &[ColumnId],
) -> (Vec<ColumnDesc>, Vec<usize>) {
    if column_ids.is_empty() {
        // shortcut
        return (vec![], vec![]);
    }
    let id_to_columns = table_columns
        .iter()
        .enumerate()
        .map(|(index, c)| (c.column_id, (c.clone(), index)))
        .collect::<HashMap<_, _>>();
    column_ids
        .iter()
        .map(|id| id_to_columns.get(id).expect("column id not found").clone())
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
mod test {
    use std::fmt::Debug;

    use expect_test::{Expect, expect};
    use risingwave_common::types::DataType;

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_find_columns_by_ids() {
        let table_columns = vec![
            ColumnDesc::unnamed(1.into(), DataType::Varchar),
            ColumnDesc::unnamed(2.into(), DataType::Int64),
            ColumnDesc::unnamed(3.into(), DataType::Int16),
        ];
        let column_ids = vec![2.into(), 3.into()];
        let result = find_columns_by_ids(&table_columns, &column_ids);
        check(
            result,
            expect![[r#"
                (
                    [
                        ColumnDesc {
                            data_type: Int64,
                            column_id: #2,
                            name: "",
                            generated_or_default_column: None,
                            description: None,
                            additional_column: AdditionalColumn {
                                column_type: None,
                            },
                            version: Pr13707,
                            system_column: None,
                            nullable: true,
                        },
                        ColumnDesc {
                            data_type: Int16,
                            column_id: #3,
                            name: "",
                            generated_or_default_column: None,
                            description: None,
                            additional_column: AdditionalColumn {
                                column_type: None,
                            },
                            version: Pr13707,
                            system_column: None,
                            nullable: true,
                        },
                    ],
                    [
                        1,
                        2,
                    ],
                )"#]],
        );

        let table_columns = vec![
            ColumnDesc::unnamed(2.into(), DataType::Int64),
            ColumnDesc::unnamed(1.into(), DataType::Varchar),
            ColumnDesc::unnamed(3.into(), DataType::Int16),
        ];
        let column_ids = vec![2.into(), 1.into()];
        let result = find_columns_by_ids(&table_columns, &column_ids);
        check(
            result,
            expect![[r#"
                (
                    [
                        ColumnDesc {
                            data_type: Int64,
                            column_id: #2,
                            name: "",
                            generated_or_default_column: None,
                            description: None,
                            additional_column: AdditionalColumn {
                                column_type: None,
                            },
                            version: Pr13707,
                            system_column: None,
                            nullable: true,
                        },
                        ColumnDesc {
                            data_type: Varchar,
                            column_id: #1,
                            name: "",
                            generated_or_default_column: None,
                            description: None,
                            additional_column: AdditionalColumn {
                                column_type: None,
                            },
                            version: Pr13707,
                            system_column: None,
                            nullable: true,
                        },
                    ],
                    [
                        0,
                        1,
                    ],
                )"#]],
        );
    }
}
