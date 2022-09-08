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

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use serde_json::Value;

use crate::parser::common::json_parse_value;
use crate::{SourceParser, SourceStreamChunkRowWriter, WriteGuard};

/// Parser for JSON format
#[derive(Debug)]
pub struct JSONParser;

impl SourceParser for JSONParser {
    fn parse(&self, payload: &[u8], writer: SourceStreamChunkRowWriter<'_>) -> Result<WriteGuard> {
        let value: Value = serde_json::from_slice(payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        writer.insert(|desc| {
            json_parse_value(&desc.into(), value.get(&desc.name)).map_err(|e| {
                tracing::error!(
                    "failed to process value ({}): {}",
                    String::from_utf8_lossy(payload),
                    e
                );
                e.into()
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::StructValue;
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_expr::vector_op::cast::{str_to_date, str_to_timestamp};

    use crate::{JSONParser, SourceColumnDesc, SourceParser};

    #[test]
    fn test_json_parser() {
        let parser = JSONParser {};
        let payload = r#"{"i32":1,"bool":true,"i16":1,"i64":12345678,"f32":1.23,"f64":1.2345,"varchar":"varchar","date":"2021-01-01","timestamp":"2021-01-01 16:06:12.269"}"#.as_bytes();
        let descs = vec![
            SourceColumnDesc {
                name: "i32".to_string(),
                data_type: DataType::Int32,
                column_id: ColumnId::from(0),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "bool".to_string(),
                data_type: DataType::Boolean,
                column_id: ColumnId::from(2),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "i16".to_string(),
                data_type: DataType::Int16,
                column_id: ColumnId::from(3),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "i64".to_string(),
                data_type: DataType::Int64,
                column_id: ColumnId::from(4),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "f32".to_string(),
                data_type: DataType::Float32,
                column_id: ColumnId::from(5),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "f64".to_string(),
                data_type: DataType::Float64,
                column_id: ColumnId::from(6),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "varchar".to_string(),
                data_type: DataType::Varchar,
                column_id: ColumnId::from(7),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "date".to_string(),
                data_type: DataType::Date,
                column_id: ColumnId::from(8),
                skip_parse: false,
                fields: vec![],
            },
            SourceColumnDesc {
                name: "timestamp".to_string(),
                data_type: DataType::Timestamp,
                column_id: ColumnId::from(9),
                skip_parse: false,
                fields: vec![],
            },
        ];

        let event = parser.parse(payload, &descs).unwrap();
        let row = event.rows.first().unwrap();
        assert_eq!(row.len(), descs.len());
        assert!(row[0].eq(&Some(ScalarImpl::Int32(1))));
        assert!(row[1].eq(&Some(ScalarImpl::Bool(true))));
        assert!(row[2].eq(&Some(ScalarImpl::Int16(1))));
        assert!(row[3].eq(&Some(ScalarImpl::Int64(12345678))));
        assert!(row[4].eq(&Some(ScalarImpl::Float32(1.23.into()))));
        assert!(row[5].eq(&Some(ScalarImpl::Float64(1.2345.into()))));
        assert!(row[6].eq(&Some(ScalarImpl::Utf8("varchar".to_string()))));
        assert!(row[7].eq(&Some(ScalarImpl::NaiveDate(
            str_to_date("2021-01-01").unwrap()
        ))));
        assert!(row[8].eq(&Some(ScalarImpl::NaiveDateTime(
            str_to_timestamp("2021-01-01 16:06:12.269").unwrap()
        ))));

        let payload = r#"{"i32":1}"#.as_bytes();
        let result = parser.parse(payload, &descs);
        assert!(result.is_ok());
        let event = result.unwrap();
        let row = event.rows.first().unwrap();
        assert_eq!(row.len(), descs.len());
        assert!(row[0].eq(&Some(ScalarImpl::Int32(1))));
        assert!(row[1].eq(&None));

        let payload = r#"{"i32:1}"#.as_bytes();
        let result = parser.parse(payload, &descs);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_parse_struct() {
        let parser = JSONParser {};

        let descs = vec![
            ColumnDesc::new_struct(
                "data",
                0,
                "",
                vec![
                    ColumnDesc::new_atomic(DataType::Timestamp, "created_at", 1),
                    ColumnDesc::new_atomic(DataType::Varchar, "id", 2),
                    ColumnDesc::new_atomic(DataType::Varchar, "text", 3),
                    ColumnDesc::new_atomic(DataType::Varchar, "lang", 4),
                ],
            ),
            ColumnDesc::new_struct(
                "author",
                5,
                "",
                vec![
                    ColumnDesc::new_atomic(DataType::Timestamp, "created_at", 6),
                    ColumnDesc::new_atomic(DataType::Varchar, "id", 7),
                    ColumnDesc::new_atomic(DataType::Varchar, "name", 8),
                    ColumnDesc::new_atomic(DataType::Varchar, "username", 9),
                ],
            ),
        ]
        .iter()
        .map(SourceColumnDesc::from)
        .collect_vec();
        let payload = r#"
        {
            "data": {
              "created_at": "2022-07-13 20:48:37.07",
              "id": "1732524418112319151",
              "text": "Here man favor ourselves mysteriously most her sigh in straightaway for afterwards.",           
              "lang": "English"
            },
            "author": {
              "created_at": "2018-01-29 12:19:11.07",
              "id": "7772634297",
              "name": "Lily Frami yet",
              "username": "Dooley5659"
            }
          }
        "#
        .as_bytes();
        let event = parser.parse(payload, &descs).unwrap();
        let row = event.rows[0].clone();

        let expected = vec![
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::NaiveDateTime(
                    str_to_timestamp("2022-07-13 20:48:37.07").unwrap()
                )),
                Some(ScalarImpl::Utf8("1732524418112319151".to_string())),
                Some(ScalarImpl::Utf8("Here man favor ourselves mysteriously most her sigh in straightaway for afterwards.".to_string())),
                Some(ScalarImpl::Utf8("English".to_string())),
            ]))),
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::NaiveDateTime(
                    str_to_timestamp("2018-01-29 12:19:11.07").unwrap()
                )),
                Some(ScalarImpl::Utf8("7772634297".to_string())),
                Some(ScalarImpl::Utf8("Lily Frami yet".to_string())),
                Some(ScalarImpl::Utf8("Dooley5659".to_string())),
            ]) ))
        ];
        assert_eq!(row, expected);
    }
}
