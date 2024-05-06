// Copyright 2024 RisingWave Labs
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
use risingwave_common::array::RowRef;
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarRefImpl};

use crate::source::Column;

pub(crate) fn get_string_from_index_path<'s>(
    path: &[usize],
    default_value: Option<&'s str>,
    row: &'s RowRef<'s>,
) -> Option<&'s str> {
    if let Some(value) = default_value
        && path.is_empty()
    {
        Some(value)
    } else {
        let mut iter = path.iter();
        let scalar = iter
            .next()
            .and_then(|pos| row.datum_at(*pos))
            .and_then(|d| {
                iter.try_fold(d, |d, pos| match d {
                    ScalarRefImpl::Struct(struct_ref) => {
                        struct_ref.iter_fields_ref().nth(*pos).flatten()
                    }
                    _ => None,
                })
            });
        match scalar {
            Some(ScalarRefImpl::Utf8(s)) => Some(s),
            _ => {
                if let Some(value) = default_value {
                    Some(value)
                } else {
                    None
                }
            }
        }
    }
}

pub(crate) fn get_string_field_index_path_from_columns(
    columns: &[Column],
    field: &str,
) -> anyhow::Result<Vec<usize>> {
    let mut iter = field.split('.');
    let mut path = vec![];
    let dt =
        iter.next()
            .and_then(|field| {
                // Extract the field from the schema
                columns
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name == field)
                    .map(|(pos, f)| {
                        path.push(pos);
                        &f.data_type
                    })
            })
            .and_then(|dt| {
                // Iterate over the next fields to extract the fields from the nested structs
                iter.try_fold(dt, |dt, field| match dt {
                    DataType::Struct(st) => {
                        st.iter().enumerate().find(|(_, (s, _))| *s == field).map(
                            |(pos, (_, dt))| {
                                path.push(pos);
                                dt
                            },
                        )
                    }
                    _ => None,
                })
            });

    match dt {
        Some(DataType::Varchar) => Ok(path),
        Some(dt) => Err(anyhow::anyhow!(
            "field `{}` must be of type string but got {:?}",
            field,
            dt
        )),
        None => Err(anyhow::anyhow!("field `{}`  not found", field)),
    }
}

// This function returns the index path to the field in the schema, validating that the field exists and is of type string
// the returnent path can be used to extract the topic field from a row. The path is a list of indexes to be used to navigate the row
// to the field.
pub(crate) fn get_string_field_index_path(
    schema: &Schema,
    field: &str,
) -> anyhow::Result<Vec<usize>> {
    let mut iter = field.split('.');
    let mut path = vec![];
    let dt =
        iter.next()
            .and_then(|field| {
                // Extract the field from the schema
                schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name == field)
                    .map(|(pos, f)| {
                        path.push(pos);
                        &f.data_type
                    })
            })
            .and_then(|dt| {
                // Iterate over the next fields to extract the fields from the nested structs
                iter.try_fold(dt, |dt, field| match dt {
                    DataType::Struct(st) => {
                        st.iter().enumerate().find(|(_, (s, _))| *s == field).map(
                            |(pos, (_, dt))| {
                                path.push(pos);
                                dt
                            },
                        )
                    }
                    _ => None,
                })
            });

    match dt {
        Some(DataType::Varchar) => Ok(path),
        Some(dt) => Err(anyhow::anyhow!(
            "field `{}` must be of type string but got {:?}",
            field,
            dt
        )),
        None => Err(anyhow::anyhow!("field `{}`  not found", field)),
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::array::{DataChunk, DataChunkTestExt, RowRef};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, StructType};

    use super::{get_string_field_index_path, get_string_from_index_path};

    #[test]
    fn test_single_field_extraction() {
        let schema = Schema::new(vec![Field::with_name(DataType::Varchar, "topic")]);
        let path = get_string_field_index_path(&schema, "topic").unwrap();
        assert_eq!(path, vec![0]);

        let chunk = DataChunk::from_pretty(
            "T
            test",
        );

        let row = RowRef::new(&chunk, 0);

        assert_eq!(get_string_from_index_path(&path, None, &row), Some("test"));

        let result = get_string_field_index_path(&schema, "other_field");
        assert!(result.is_err());
    }

    #[test]
    fn test_nested_field_extraction() {
        let schema = Schema::new(vec![Field::with_name(
            DataType::Struct(StructType::new(vec![
                ("field", DataType::Int32),
                ("subtopic", DataType::Varchar),
            ])),
            "topic",
        )]);
        let path = get_string_field_index_path(&schema, "topic.subtopic").unwrap();
        assert_eq!(path, vec![0, 1]);

        let chunk = DataChunk::from_pretty(
            "<i,T>
            (1,test)",
        );

        let row = RowRef::new(&chunk, 0);

        assert_eq!(get_string_from_index_path(&path, None, &row), Some("test"));

        let result = get_string_field_index_path(&schema, "topic.other_field");
        assert!(result.is_err());
    }

    #[test]
    fn test_null_values_extraction() {
        let path = vec![0];
        let chunk = DataChunk::from_pretty(
            "T
            .",
        );
        let row = RowRef::new(&chunk, 0);
        assert_eq!(
            get_string_from_index_path(&path, Some("default"), &row),
            Some("default")
        );
        assert_eq!(get_string_from_index_path(&path, None, &row), None);

        let path = vec![0, 1];
        let chunk = DataChunk::from_pretty(
            "<i,T>
            (1,)",
        );
        let row = RowRef::new(&chunk, 0);
        assert_eq!(
            get_string_from_index_path(&path, Some("default"), &row),
            Some("default")
        );
        assert_eq!(get_string_from_index_path(&path, None, &row), None);
    }

    #[test]
    fn test_multiple_levels() {
        let schema = Schema::new(vec![
            Field::with_name(
                DataType::Struct(StructType::new(vec![
                    ("field", DataType::Int32),
                    (
                        "subtopic",
                        DataType::Struct(StructType::new(vec![
                            ("int_field", DataType::Int32),
                            ("boolean_field", DataType::Boolean),
                            ("string_field", DataType::Varchar),
                        ])),
                    ),
                ])),
                "topic",
            ),
            Field::with_name(DataType::Varchar, "other_field"),
        ]);

        let path = get_string_field_index_path(&schema, "topic.subtopic.string_field").unwrap();
        assert_eq!(path, vec![0, 1, 2]);

        assert!(get_string_field_index_path(&schema, "topic.subtopic.boolean_field").is_err());

        assert!(get_string_field_index_path(&schema, "topic.subtopic.int_field").is_err());

        assert!(get_string_field_index_path(&schema, "topic.field").is_err());

        let path = get_string_field_index_path(&schema, "other_field").unwrap();
        assert_eq!(path, vec![1]);

        let chunk = DataChunk::from_pretty(
            "<i,<T>> T
            (1,(test)) other",
        );

        let row = RowRef::new(&chunk, 0);

        // topic.subtopic.string_field
        assert_eq!(
            get_string_from_index_path(&[0, 1, 0], None, &row),
            Some("test")
        );

        // topic.field
        assert_eq!(get_string_from_index_path(&[0, 0], None, &row), None);

        // other_field
        assert_eq!(get_string_from_index_path(&[1], None, &row), Some("other"));
    }
}
