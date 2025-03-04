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

use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{FieldLike, INITIAL_TABLE_VERSION_ID, TableVersionId};
use risingwave_common::types::DataType;

use crate::TableCatalog;
use crate::catalog::ColumnId;
use crate::catalog::table_catalog::TableVersion;
use crate::error::Result;

/// Column ID generator for a new table or a new version of an existing table to alter.
#[derive(Debug)]
pub struct ColumnIdGenerator {
    /// Existing column names and their IDs and data types.
    ///
    /// This is used for aligning column IDs between versions (`ALTER`s). If a column already
    /// exists and the data type matches, its ID is reused. Otherwise, a new ID is generated.
    ///
    /// For a new table, this is empty.
    pub existing: HashMap<String, (ColumnId, DataType)>,

    /// The next column ID to generate, used for new columns that do not exist in `existing`.
    pub next_column_id: ColumnId,

    /// The version ID of the table to be created or altered.
    ///
    /// For a new table, this is 0. For altering an existing table, this is the **next** version ID
    /// of the `version_id` field in the original table catalog.
    pub version_id: TableVersionId,
}

impl ColumnIdGenerator {
    /// Creates a new [`ColumnIdGenerator`] for altering an existing table.
    pub fn new_alter(original: &TableCatalog) -> Self {
        let existing = original
            .columns()
            .iter()
            .map(|col| {
                (
                    col.name().to_owned(),
                    (col.column_id(), col.data_type().clone()),
                )
            })
            .collect();

        let version = original.version().expect("version field not set");

        Self {
            existing,
            next_column_id: version.next_column_id,
            version_id: version.version_id + 1,
        }
    }

    /// Creates a new [`ColumnIdGenerator`] for a new table.
    pub fn new_initial() -> Self {
        Self {
            existing: HashMap::new(),
            next_column_id: ColumnId::first_user_column(),
            version_id: INITIAL_TABLE_VERSION_ID,
        }
    }

    /// Generates a new [`ColumnId`] for a column with the given field.
    ///
    /// Returns an error if the data type of the column has been changed.
    pub fn generate(&mut self, field: impl FieldLike) -> Result<ColumnId> {
        if let Some((id, original_type)) = self.existing.get(field.name()) {
            // Intentionally not using `datatype_equals` here because we want nested types to be
            // exactly the same, **NOT** ignoring field names as they may be referenced in expressions
            // of generated columns or downstream jobs.
            // TODO: support compatible changes on types, typically for `STRUCT` types.
            //       https://github.com/risingwavelabs/risingwave/issues/19755
            if original_type == field.data_type() {
                Ok(*id)
            } else {
                bail_not_implemented!(
                    "The data type of column \"{}\" has been changed from \"{}\" to \"{}\". \
                     This is currently not supported, even if it could be a compatible change in external systems.",
                    field.name(),
                    original_type,
                    field.data_type()
                );
            }
        } else {
            let id = self.next_column_id;
            self.next_column_id = self.next_column_id.next();
            Ok(id)
        }
    }

    /// Consume this generator and return a [`TableVersion`] for the table to be created or altered.
    pub fn into_version(self) -> TableVersion {
        TableVersion {
            version_id: self.version_id,
            next_column_id: self.next_column_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BrandNewColumn(&'static str);
    use BrandNewColumn as B;
    use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, Field};
    use risingwave_common::types::StructType;

    impl FieldLike for BrandNewColumn {
        fn name(&self) -> &str {
            self.0
        }

        fn data_type(&self) -> &DataType {
            unreachable!("for brand new columns, data type will not be accessed")
        }
    }

    #[test]
    fn test_col_id_gen_initial() {
        let mut gen = ColumnIdGenerator::new_initial();
        assert_eq!(gen.generate(B("v1")).unwrap(), ColumnId::new(1));
        assert_eq!(gen.generate(B("v2")).unwrap(), ColumnId::new(2));
    }

    #[test]
    fn test_col_id_gen_alter() {
        let mut gen = ColumnIdGenerator::new_alter(&TableCatalog {
            columns: vec![
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field::with_name(DataType::Float32, "f32"),
                        1,
                    ),
                    is_hidden: false,
                },
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field::with_name(DataType::Float64, "f64"),
                        2,
                    ),
                    is_hidden: false,
                },
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field::with_name(
                            StructType::new([("f1", DataType::Int32)]).into(),
                            "nested",
                        ),
                        3,
                    ),
                    is_hidden: false,
                },
            ],
            version: Some(TableVersion::new_initial_for_test(ColumnId::new(3))),
            ..Default::default()
        });

        assert_eq!(gen.generate(B("v1")).unwrap(), ColumnId::new(4));
        assert_eq!(gen.generate(B("v2")).unwrap(), ColumnId::new(5));
        assert_eq!(
            gen.generate(Field::new("f32", DataType::Float32)).unwrap(),
            ColumnId::new(1)
        );

        // mismatched data type
        gen.generate(Field::new("f64", DataType::Float32))
            .unwrap_err();

        // mismatched data type
        // we require the nested data type to be exactly the same
        gen.generate(Field::new(
            "nested",
            StructType::new([("f1", DataType::Int32), ("f2", DataType::Int64)]).into(),
        ))
        .unwrap_err();

        assert_eq!(gen.generate(B("v3")).unwrap(), ColumnId::new(6));
    }
}
