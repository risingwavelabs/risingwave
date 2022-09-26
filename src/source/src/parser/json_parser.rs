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

use crate::{SourceParser, SourceStreamChunkRowWriter, WriteGuard};

/// Parser for JSON format
#[derive(Debug)]
pub struct JsonParser;

#[cfg(not(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
)))]
impl SourceParser for JsonParser {
    fn parse(&self, payload: &[u8], writer: SourceStreamChunkRowWriter<'_>) -> Result<WriteGuard> {
        use serde_json::Value;

        use crate::parser::common::json_parse_value;
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

#[cfg(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
))]
impl SourceParser for JsonParser {
    fn parse(&self, payload: &[u8], writer: SourceStreamChunkRowWriter<'_>) -> Result<WriteGuard> {
        use simd_json::{BorrowedValue, ValueAccess};

        use crate::parser::common::simd_json_parse_value;
        let mut payload_mut = payload.to_vec();
        let value: BorrowedValue<'_> = simd_json::to_borrowed_value(&mut payload_mut)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        writer.insert(|desc| {
            simd_json_parse_value(&desc.into(), value.get(desc.name.as_str())).map_err(|e| {
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
    use risingwave_common::array::{Op, StructValue};
    use risingwave_common::catalog::ColumnDesc;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_expr::vector_op::cast::str_to_timestamp;

    use crate::{JsonParser, SourceColumnDesc, SourceParser, SourceStreamChunkBuilder};

    #[test]
    fn test_json_parser_failed() {
        let parser = JsonParser;
        let descs = vec![
            SourceColumnDesc::simple("v1", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("v2", DataType::Int16, 1.into()),
            SourceColumnDesc::simple("v3", DataType::Varchar, 2.into()),
        ];
        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 3);

        // Parse a correct record.
        {
            let writer = builder.row_writer();
            let payload = br#"{"v1": 1, "v2": 2, "v3": "3"}"#;
            parser.parse(payload, writer).unwrap();
        }

        // Parse an incorrect record.
        {
            let writer = builder.row_writer();
            // `v2` overflowed.
            let payload = br#"{"v1": 1, "v2": 65536, "v3": "3"}"#;
            parser.parse(payload, writer).unwrap_err();
        }

        // Parse a correct record.
        {
            let writer = builder.row_writer();
            let payload = br#"{"v1": 1, "v2": 2, "v3": "3"}"#;
            parser.parse(payload, writer).unwrap();
        }

        let chunk = builder.finish();
        assert!(chunk.valid());

        assert_eq!(chunk.cardinality(), 2);
    }

    #[test]
    fn test_json_parse_struct() {
        let parser = JsonParser;

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
        let payload = br#"
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
        "#;
        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 1);
        {
            let writer = builder.row_writer();
            parser.parse(payload, writer).unwrap();
        }
        let chunk = builder.finish();
        let (op, row) = chunk.rows().next().unwrap();
        assert_eq!(op, Op::Insert);
        let row = row.to_owned_row().0;

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
