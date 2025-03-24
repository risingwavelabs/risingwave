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

use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnCatalog, INITIAL_TABLE_VERSION_ID, TableVersionId};
use risingwave_common::types::{DataType, MapType, StructType, data_types};
use risingwave_common::util::iter_util::ZipEqFast;

use crate::TableCatalog;
use crate::catalog::ColumnId;
use crate::catalog::table_catalog::TableVersion;
use crate::error::Result;

/// A segment in a [`Path`].
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum Segment {
    Field(String), // for both top-level columns and struct fields
    ListElement,
    MapKey,
    MapValue,
}

impl std::fmt::Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Segment::Field(name) => write!(f, "{}", name),
            Segment::ListElement => write!(f, "element"),
            Segment::MapKey => write!(f, "key"),
            Segment::MapValue => write!(f, "value"),
        }
    }
}

/// The path to a nested field in a column with composite data type.
type Path = Vec<Segment>;

type Existing = HashMap<Path, (ColumnId, DataType)>;

/// Column ID generator for a new table or a new version of an existing table to alter.
#[derive(Debug)]
pub struct ColumnIdGenerator {
    /// Existing fields and their IDs and data types.
    ///
    /// This is used for aligning field IDs between versions (`ALTER`s). If a field already
    /// exists and the data type matches, its ID is reused. Otherwise, a new ID is generated.
    ///
    /// For a new table, this is empty.
    existing: Existing,

    /// The next column ID to generate, used for new columns that do not exist in `existing`.
    next_column_id: ColumnId,

    /// The version ID of the table to be created or altered.
    ///
    /// For a new table, this is 0. For altering an existing table, this is the **next** version ID
    /// of the `version_id` field in the original table catalog.
    version_id: TableVersionId,
}

impl ColumnIdGenerator {
    /// Creates a new [`ColumnIdGenerator`] for altering an existing table.
    pub fn new_alter(original: &TableCatalog) -> Self {
        fn handle(existing: &mut Existing, path: &mut Path, id: ColumnId, data_type: DataType) {
            macro_rules! with_segment {
                ($segment:expr, $block:block) => {
                    path.push($segment);
                    $block
                    path.pop();
                };
            }

            match &data_type {
                DataType::Struct(fields) => {
                    for ((field_name, field_data_type), field_id) in
                        fields.iter().zip_eq_fast(fields.ids_or_placeholder())
                    {
                        with_segment!(Segment::Field(field_name.to_owned()), {
                            handle(existing, path, field_id, field_data_type.clone());
                        });
                    }
                }
                DataType::List(inner) => {
                    // There's no id for the element as list's own structure won't change.
                    with_segment!(Segment::ListElement, {
                        handle(existing, path, ColumnId::placeholder(), *inner.clone());
                    });
                }
                DataType::Map(map) => {
                    // There's no id for the key/value as map's own structure won't change.
                    with_segment!(Segment::MapKey, {
                        handle(existing, path, ColumnId::placeholder(), map.key().clone());
                    });
                    with_segment!(Segment::MapValue, {
                        handle(existing, path, ColumnId::placeholder(), map.value().clone());
                    });
                }

                data_types::simple!() => {}
            }

            existing
                .try_insert(path.clone(), (id, data_type))
                .unwrap_or_else(|_| panic!("duplicate path: {:?}", path));
        }

        let mut existing = Existing::new();

        // Collect all existing fields into `existing`.
        for col in original.columns() {
            let mut path = vec![Segment::Field(col.name().to_owned())];
            handle(
                &mut existing,
                &mut path,
                col.column_id(),
                col.data_type().clone(),
            );
        }

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
            existing: Existing::new(),
            next_column_id: ColumnId::first_user_column(),
            version_id: INITIAL_TABLE_VERSION_ID,
        }
    }

    /// Generate [`ColumnId`]s for the given column and its nested fields (if any) recursively.
    /// The IDs of nested fields will be reflected in the updated [`DataType`] of the column.
    ///
    /// Returns an error if there's incompatible data type change.
    pub fn generate(&mut self, col: &mut ColumnCatalog) -> Result<()> {
        let mut path = vec![Segment::Field(col.name().to_owned())];

        if let Some((original_column_id, original_data_type)) = self.existing.get(&path) {
            if original_data_type == col.data_type() {
                // Only update the top-level column ID, without traversing nested fields.
                col.column_desc.column_id = *original_column_id;
                return Ok(());
            } else {
                // Check if the column can be altered.
                match original_data_type.can_alter() {
                    Some(true) => { /* pass */ }
                    Some(false) => bail!(
                        "column \"{}\" was persisted with legacy encoding thus cannot be altered, \
                         consider dropping and readding the column",
                        col.name()
                    ),
                    None => bail!(
                        "column \"{}\" cannot be altered; only types containing struct can be altered",
                        col.name()
                    ),
                }
            }
        }

        fn handle(
            this: &mut ColumnIdGenerator,
            path: &mut Path,
            data_type: DataType,
        ) -> Result<(ColumnId, DataType)> {
            macro_rules! with_segment {
                ($segment:expr, $block:block) => {{
                    path.push($segment);
                    let ret = $block;
                    path.pop();
                    ret
                }};
            }

            let original_column_id = match this.existing.get(&*path) {
                Some((original_column_id, original_data_type)) => {
                    // Only check the type name (discriminant) for compatibility check here.
                    // For nested fields, we will check them recursively later.
                    if original_data_type.type_name() != data_type.type_name() {
                        let path = path.iter().join(".");
                        bail!(
                            "incompatible data type change from {:?} to {:?} at path \"{}\"",
                            original_data_type.type_name(),
                            data_type.type_name(),
                            path
                        );
                    }
                    Some(*original_column_id)
                }
                None => None,
            };

            // Only top-level column and struct fields need an ID.
            let need_gen_id = matches!(path.last().unwrap(), Segment::Field(_));

            let new_id = if need_gen_id {
                if let Some(id) = original_column_id {
                    assert!(
                        id != ColumnId::placeholder(),
                        "original column id should not be placeholder"
                    );
                    id
                } else {
                    let id = this.next_column_id;
                    this.next_column_id = this.next_column_id.next();
                    id
                }
            } else {
                // Column ID is actually unused by caller (for list and map types), just use a placeholder.
                ColumnId::placeholder()
            };

            let new_data_type = match data_type {
                DataType::Struct(fields) => {
                    let mut new_fields = Vec::new();
                    let mut ids = Vec::new();
                    for (field_name, field_data_type) in fields.iter() {
                        let (id, new_field_data_type) =
                            with_segment!(Segment::Field(field_name.to_owned()), {
                                handle(this, path, field_data_type.clone())?
                            });
                        new_fields.push((field_name.to_owned(), new_field_data_type));
                        ids.push(id);
                    }
                    DataType::Struct(StructType::new(new_fields).with_ids(ids))
                }
                DataType::List(inner) => {
                    let (_, new_inner) =
                        with_segment!(Segment::ListElement, { handle(this, path, *inner)? });
                    DataType::List(Box::new(new_inner))
                }
                DataType::Map(map) => {
                    let (_, new_key) =
                        with_segment!(Segment::MapKey, { handle(this, path, map.key().clone())? });
                    let (_, new_value) = with_segment!(Segment::MapValue, {
                        handle(this, path, map.value().clone())?
                    });
                    DataType::Map(MapType::from_kv(new_key, new_value))
                }

                data_types::simple!() => data_type,
            };

            Ok((new_id, new_data_type))
        }

        let (new_column_id, new_data_type) = handle(self, &mut path, col.data_type().clone())?;

        col.column_desc.column_id = new_column_id;
        col.column_desc.data_type = new_data_type;

        Ok(())
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
    use expect_test::expect;
    use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, Field, FieldLike};
    use risingwave_common::types::StructType;
    use thiserror_ext::AsReport;

    use super::*;

    struct BrandNewColumn(&'static str);
    use BrandNewColumn as B;

    impl FieldLike for BrandNewColumn {
        fn name(&self) -> &str {
            self.0
        }

        fn data_type(&self) -> &DataType {
            &DataType::Boolean
        }
    }

    #[easy_ext::ext(ColumnIdGeneratorTestExt)]
    impl ColumnIdGenerator {
        /// Generate a column ID for the given field.
        ///
        /// This helper function checks that the data type of the field has not changed after
        /// generating the column ID, i.e., it is called on simple types or composite types
        /// with `field_ids` unset and legacy encoding.
        fn generate_simple(&mut self, field: impl FieldLike) -> Result<ColumnId> {
            let original_data_type = field.data_type().clone();

            let field = Field::new(field.name(), original_data_type.clone());
            let mut col = ColumnCatalog {
                column_desc: ColumnDesc::from_field_without_column_id(&field),
                is_hidden: false,
            };
            self.generate(&mut col)?;

            assert_eq!(
                col.column_desc.data_type, original_data_type,
                "data type has changed after generating column id, \
                 are you calling this on a composite type?"
            );

            Ok(col.column_desc.column_id)
        }
    }

    #[test]
    fn test_col_id_gen_initial() {
        let mut r#gen = ColumnIdGenerator::new_initial();
        assert_eq!(r#gen.generate_simple(B("v1")).unwrap(), ColumnId::new(1));
        assert_eq!(r#gen.generate_simple(B("v2")).unwrap(), ColumnId::new(2));
    }

    #[test]
    fn test_col_id_gen_alter() {
        let mut r#gen = ColumnIdGenerator::new_alter(&TableCatalog {
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

        assert_eq!(r#gen.generate_simple(B("v1")).unwrap(), ColumnId::new(4));
        assert_eq!(r#gen.generate_simple(B("v2")).unwrap(), ColumnId::new(5));
        assert_eq!(
            r#gen
                .generate_simple(Field::new("f32", DataType::Float32))
                .unwrap(),
            ColumnId::new(1)
        );

        // mismatched simple data type
        let err = r#gen
            .generate_simple(Field::new("f64", DataType::Float32))
            .unwrap_err();
        expect![[r#"column "f64" cannot be altered; only types containing struct can be altered"#]]
            .assert_eq(&err.to_report_string());

        // mismatched composite data type
        // we require the nested data type to be exactly the same
        let err = r#gen
            .generate_simple(Field::new(
                "nested",
                // NOTE: field ids are unset, legacy encoding, thus cannot be altered
                StructType::new([("f1", DataType::Int32), ("f2", DataType::Int64)]).into(),
            ))
            .unwrap_err();
        expect![[r#"column "nested" was persisted with legacy encoding thus cannot be altered, consider dropping and readding the column"#]].assert_eq(&err.to_report_string());

        // matched composite data type, should work
        let id = r#gen
            .generate_simple(Field::new(
                "nested",
                StructType::new([("f1", DataType::Int32)]).into(),
            ))
            .unwrap();
        assert_eq!(id, ColumnId::new(3));

        assert_eq!(r#gen.generate_simple(B("v3")).unwrap(), ColumnId::new(6));
    }

    #[test]
    fn test_col_id_gen_alter_composite_type() {
        let ori_type = || {
            DataType::from(StructType::new([
                ("f1", DataType::Int32),
                (
                    "map",
                    MapType::from_kv(
                        DataType::Varchar,
                        DataType::List(Box::new(
                            StructType::new([("f2", DataType::Int32), ("f3", DataType::Boolean)])
                                .into(),
                        )),
                    )
                    .into(),
                ),
            ]))
        };

        let new_type = || {
            DataType::from(StructType::new([
                ("f4", DataType::Int32),
                (
                    "map",
                    MapType::from_kv(
                        DataType::Varchar,
                        DataType::List(Box::new(
                            StructType::new([
                                ("f5", DataType::Int32),
                                ("f3", DataType::Boolean),
                                ("f6", DataType::Float32),
                            ])
                            .into(),
                        )),
                    )
                    .into(),
                ),
            ]))
        };

        let incompatible_new_type = || {
            DataType::from(StructType::new([(
                "map",
                MapType::from_kv(
                    DataType::Varchar,
                    DataType::List(Box::new(
                        StructType::new([("f6", DataType::Float64)]).into(),
                    )),
                )
                .into(),
            )]))
        };

        let mut r#gen = ColumnIdGenerator::new_initial();
        let mut col = ColumnCatalog {
            column_desc: ColumnDesc::from_field_without_column_id(&Field::new(
                "nested",
                ori_type(),
            )),
            is_hidden: false,
        };
        r#gen.generate(&mut col).unwrap();
        let version = r#gen.into_version();

        expect![[r#"
            ColumnCatalog {
                column_desc: ColumnDesc {
                    data_type: Struct(
                        StructType {
                            fields: [
                                (
                                    "f1",
                                    Int32,
                                ),
                                (
                                    "map",
                                    Map(
                                        MapType(
                                            (
                                                Varchar,
                                                List(
                                                    Struct(
                                                        StructType {
                                                            fields: [
                                                                (
                                                                    "f2",
                                                                    Int32,
                                                                ),
                                                                (
                                                                    "f3",
                                                                    Boolean,
                                                                ),
                                                            ],
                                                            field_ids: [
                                                                #4,
                                                                #5,
                                                            ],
                                                        },
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ],
                            field_ids: [
                                #2,
                                #3,
                            ],
                        },
                    ),
                    column_id: #1,
                    name: "nested",
                    generated_or_default_column: None,
                    description: None,
                    additional_column: AdditionalColumn {
                        column_type: None,
                    },
                    version: Pr13707,
                    system_column: None,
                    nullable: true,
                },
                is_hidden: false,
            }
        "#]]
        .assert_debug_eq(&col);

        let mut r#gen = ColumnIdGenerator::new_alter(&TableCatalog {
            columns: vec![col],
            version: Some(version),
            ..Default::default()
        });
        let mut new_col = ColumnCatalog {
            column_desc: ColumnDesc::from_field_without_column_id(&Field::new(
                "nested",
                new_type(),
            )),
            is_hidden: false,
        };
        r#gen.generate(&mut new_col).unwrap();
        let version = r#gen.into_version();

        expect![[r#"
            ColumnCatalog {
                column_desc: ColumnDesc {
                    data_type: Struct(
                        StructType {
                            fields: [
                                (
                                    "f4",
                                    Int32,
                                ),
                                (
                                    "map",
                                    Map(
                                        MapType(
                                            (
                                                Varchar,
                                                List(
                                                    Struct(
                                                        StructType {
                                                            fields: [
                                                                (
                                                                    "f5",
                                                                    Int32,
                                                                ),
                                                                (
                                                                    "f3",
                                                                    Boolean,
                                                                ),
                                                                (
                                                                    "f6",
                                                                    Float32,
                                                                ),
                                                            ],
                                                            field_ids: [
                                                                #7,
                                                                #5,
                                                                #8,
                                                            ],
                                                        },
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ],
                            field_ids: [
                                #6,
                                #3,
                            ],
                        },
                    ),
                    column_id: #1,
                    name: "nested",
                    generated_or_default_column: None,
                    description: None,
                    additional_column: AdditionalColumn {
                        column_type: None,
                    },
                    version: Pr13707,
                    system_column: None,
                    nullable: true,
                },
                is_hidden: false,
            }
        "#]]
        .assert_debug_eq(&new_col);

        let mut r#gen = ColumnIdGenerator::new_alter(&TableCatalog {
            columns: vec![new_col],
            version: Some(version),
            ..Default::default()
        });

        let mut new_col = ColumnCatalog {
            column_desc: ColumnDesc::from_field_without_column_id(&Field::new(
                "nested",
                incompatible_new_type(),
            )),
            is_hidden: false,
        };
        let err = r#gen.generate(&mut new_col).unwrap_err();
        expect![[r#"incompatible data type change from Float32 to Float64 at path "nested.map.value.element.f6""#]].assert_eq(&err.to_report_string());
    }
}
