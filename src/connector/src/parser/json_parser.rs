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

use risingwave_common::error::ErrorCode::{self, ProtocolError};
use risingwave_common::error::{Result, RwError};

use super::unified::json::JsonParseOptions;
use super::unified::util::apply_row_accessor_on_stream_chunk_writer;
use super::unified::AccessImpl;
use super::{AccessBuilder, ByteStreamSourceParser};
use crate::parser::unified::json::JsonAccess;
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct JsonAccessBuilder {
    value: Option<Vec<u8>>,
}

impl AccessBuilder for JsonAccessBuilder {
    #[allow(clippy::unused_async)]
    async fn generate_accessor(&mut self, payload: Vec<u8>) -> Result<AccessImpl<'_, '_>> {
        self.value = Some(payload);
        let value = simd_json::to_borrowed_value(self.value.as_mut().unwrap())
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;
        Ok(AccessImpl::Json(JsonAccess::new_with_options(
            value,
            // Debezium and Canal have their special json access builder and will not
            // use this
            &JsonParseOptions::DEFAULT,
        )))
    }
}

impl JsonAccessBuilder {
    pub fn new() -> Result<Self> {
        Ok(Self { value: None })
    }
}

/// Parser for JSON format
#[derive(Debug)]
pub struct JsonParser {
    rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl JsonParser {
    pub fn new(rw_columns: Vec<SourceColumnDesc>, source_ctx: SourceContextRef) -> Result<Self> {
        Ok(Self {
            rw_columns,
            source_ctx,
        })
    }

    pub fn new_for_test(rw_columns: Vec<SourceColumnDesc>) -> Result<Self> {
        Ok(Self {
            rw_columns,
            source_ctx: Default::default(),
        })
    }

    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &self,
        mut payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        if payload.is_none() {
            return Err(RwError::from(ErrorCode::InternalError(
                "Empty payload with nonempty key for non-upsert".into(),
            )));
        }
        let value = simd_json::to_borrowed_value(payload.as_mut().unwrap())
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;
        let values = if let simd_json::BorrowedValue::Array(arr) = value {
            arr
        } else {
            vec![value]
        };
        let mut errors = Vec::new();
        let mut guard = None;
        for value in values {
            let accessor = JsonAccess::new(value);
            match apply_row_accessor_on_stream_chunk_writer(accessor, &mut writer) {
                Ok(this_guard) => guard = Some(this_guard),
                Err(err) => errors.push(err),
            }
        }

        if let Some(guard) = guard {
            if !errors.is_empty() {
                tracing::error!(?errors, "failed to parse some columns");
            }
            Ok(guard)
        } else {
            Err(RwError::from(ErrorCode::InternalError(format!(
                "failed to parse all columns: {:?}",
                errors
            ))))
        }
    }
}

impl ByteStreamSourceParser for JsonParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    async fn parse_one<'a>(
        &'a mut self,
        _key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<WriteGuard> {
        self.parse_inner(payload, writer).await
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::vec;

    use itertools::Itertools;
    use risingwave_common::array::{Op, StructValue};
    use risingwave_common::cast::{str_to_date, str_to_timestamp};
    use risingwave_common::catalog::ColumnDesc;
    use risingwave_common::row::Row;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, Decimal, ScalarImpl, ToOwnedDatum};

    use crate::parser::upsert_parser::UpsertParser;
    use crate::parser::{
        EncodingProperties, JsonParser, JsonProperties, ProtocolProperties, SourceColumnDesc,
        SourceStreamChunkBuilder, SpecificParserConfig,
    };

    fn get_payload() -> Vec<Vec<u8>> {
        vec![
            br#"{"i32":1,"bool":true,"i16":1,"i64":12345678,"f32":1.23,"f64":1.2345,"varchar":"varchar","date":"2021-01-01","timestamp":"2021-01-01 16:06:12.269","decimal":12345.67890}"#.to_vec(),
            br#"{"i32":1,"f32":12345e+10,"f64":12345,"decimal":12345}"#.to_vec(),
        ]
    }

    fn get_array_top_level_payload() -> Vec<Vec<u8>> {
        vec![
            br#"[{"i32":1,"bool":true,"i16":1,"i64":12345678,"f32":1.23,"f64":1.2345,"varchar":"varchar","date":"2021-01-01","timestamp":"2021-01-01 16:06:12.269","decimal":12345.67890}, {"i32":1,"f32":12345e+10,"f64":12345,"decimal":12345}]"#.to_vec()
        ]
    }

    async fn test_json_parser(get_payload: fn() -> Vec<Vec<u8>>) {
        let descs = vec![
            SourceColumnDesc::simple("i32", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("bool", DataType::Boolean, 2.into()),
            SourceColumnDesc::simple("i16", DataType::Int16, 3.into()),
            SourceColumnDesc::simple("i64", DataType::Int64, 4.into()),
            SourceColumnDesc::simple("f32", DataType::Float32, 5.into()),
            SourceColumnDesc::simple("f64", DataType::Float64, 6.into()),
            SourceColumnDesc::simple("varchar", DataType::Varchar, 7.into()),
            SourceColumnDesc::simple("date", DataType::Date, 8.into()),
            SourceColumnDesc::simple("timestamp", DataType::Timestamp, 9.into()),
            SourceColumnDesc::simple("decimal", DataType::Decimal, 10.into()),
        ];

        let parser = JsonParser::new(descs.clone(), Default::default()).unwrap();

        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 2);

        for payload in get_payload() {
            let writer = builder.row_writer();
            parser.parse_inner(Some(payload), writer).await.unwrap();
        }

        let chunk = builder.finish();

        let mut rows = chunk.rows();

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(1)));
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Bool(true)))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int16(1)))
            );
            assert_eq!(
                row.datum_at(3).to_owned_datum(),
                (Some(ScalarImpl::Int64(12345678)))
            );
            assert_eq!(
                row.datum_at(4).to_owned_datum(),
                (Some(ScalarImpl::Float32(1.23.into())))
            );
            assert_eq!(
                row.datum_at(5).to_owned_datum(),
                (Some(ScalarImpl::Float64(1.2345.into())))
            );
            assert_eq!(
                row.datum_at(6).to_owned_datum(),
                (Some(ScalarImpl::Utf8("varchar".into())))
            );
            assert_eq!(
                row.datum_at(7).to_owned_datum(),
                (Some(ScalarImpl::Date(str_to_date("2021-01-01").unwrap())))
            );
            assert_eq!(
                row.datum_at(8).to_owned_datum(),
                (Some(ScalarImpl::Timestamp(
                    str_to_timestamp("2021-01-01 16:06:12.269").unwrap()
                )))
            );
            assert_eq!(
                row.datum_at(9).to_owned_datum(),
                (Some(ScalarImpl::Decimal(
                    Decimal::from_str("12345.67890").unwrap()
                )))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(1)))
            );
            assert_eq!(row.datum_at(1).to_owned_datum(), None);
            assert_eq!(
                row.datum_at(4).to_owned_datum(),
                (Some(ScalarImpl::Float32(12345e+10.into())))
            );
            assert_eq!(
                row.datum_at(5).to_owned_datum(),
                (Some(ScalarImpl::Float64(12345.into())))
            );
            assert_eq!(
                row.datum_at(9).to_owned_datum(),
                (Some(ScalarImpl::Decimal(12345.into())))
            );
        }
    }

    #[tokio::test]
    async fn test_json_parse_object_top_level() {
        test_json_parser(get_payload).await;
    }
    #[ignore]
    #[tokio::test]
    async fn test_json_parse_array_top_level() {
        test_json_parser(get_array_top_level_payload).await;
    }

    #[tokio::test]
    async fn test_json_parser_failed() {
        let descs = vec![
            SourceColumnDesc::simple("v1", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("v2", DataType::Int16, 1.into()),
            SourceColumnDesc::simple("v3", DataType::Varchar, 2.into()),
        ];
        let parser = JsonParser::new(descs.clone(), Default::default()).unwrap();
        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 3);

        // Parse a correct record.
        {
            let writer = builder.row_writer();
            let payload = br#"{"v1": 1, "v2": 2, "v3": "3"}"#.to_vec();
            parser.parse_inner(Some(payload), writer).await.unwrap();
        }

        // Parse an incorrect record.
        {
            let writer = builder.row_writer();
            // `v2` overflowed.
            let payload = br#"{"v1": 1, "v2": 65536, "v3": "3"}"#.to_vec();
            assert!(parser.parse_inner(Some(payload), writer).await.is_err());
        }

        // Parse a correct record.
        {
            let writer = builder.row_writer();
            let payload = br#"{"v1": 1, "v2": 2, "v3": "3"}"#.to_vec();
            parser.parse_inner(Some(payload), writer).await.unwrap();
        }

        let chunk = builder.finish();
        assert!(chunk.valid());

        assert_eq!(chunk.cardinality(), 2);
    }

    #[tokio::test]
    async fn test_json_parse_struct() {
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
            ColumnDesc::new_atomic(DataType::Varchar, "I64CastToVarchar", 10),
            ColumnDesc::new_atomic(DataType::Int64, "VarcharCastToI64", 11),
        ]
        .iter()
        .map(SourceColumnDesc::from)
        .collect_vec();

        let parser = JsonParser::new(descs.clone(), Default::default()).unwrap();
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
            },
            "I64CastToVarchar": 1598197865760800768,
            "VarcharCastToI64": "1598197865760800768"
        }
        "#.to_vec();
        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 1);
        {
            let writer = builder.row_writer();
            parser.parse_inner(Some(payload), writer).await.unwrap();
        }
        let chunk = builder.finish();
        let (op, row) = chunk.rows().next().unwrap();
        assert_eq!(op, Op::Insert);
        let row = row.into_owned_row().into_inner();

        let expected = vec![
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Timestamp(
                    str_to_timestamp("2022-07-13 20:48:37.07").unwrap()
                )),
                Some(ScalarImpl::Utf8("1732524418112319151".into())),
                Some(ScalarImpl::Utf8("Here man favor ourselves mysteriously most her sigh in straightaway for afterwards.".into())),
                Some(ScalarImpl::Utf8("English".into())),
            ]))),
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Timestamp(
                    str_to_timestamp("2018-01-29 12:19:11.07").unwrap()
                )),
                Some(ScalarImpl::Utf8("7772634297".into())),
                Some(ScalarImpl::Utf8("Lily Frami yet".into())),
                Some(ScalarImpl::Utf8("Dooley5659".into())),
            ]) )),
            Some(ScalarImpl::Utf8("1598197865760800768".into())),
            Some(ScalarImpl::Int64(1598197865760800768)),
        ];
        assert_eq!(row, expected.into());
    }
    #[tokio::test]
    async fn test_json_upsert_parser() {
        let items = [
            (r#"{"a":1}"#, r#"{"a":1,"b":2}"#),
            (r#"{"a":1}"#, r#"{"a":1,"b":3}"#),
            (r#"{"a":2}"#, r#"{"a":2,"b":2}"#),
            (r#"{"a":2}"#, r#""#),
        ]
        .to_vec();
        let descs = vec![
            SourceColumnDesc::simple("a", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("b", DataType::Int32, 1.into()),
        ];
        let props = SpecificParserConfig {
            key_encoding_config: None,
            encoding_config: EncodingProperties::Json(JsonProperties {}),
            protocol_config: ProtocolProperties::Upsert,
        };
        let mut parser = UpsertParser::new(props, descs.clone(), Default::default())
            .await
            .unwrap();
        let mut builder = SourceStreamChunkBuilder::with_capacity(descs, 4);
        for item in items {
            let test = parser
                .parse_inner(
                    Some(item.0.as_bytes().to_vec()),
                    if !item.1.is_empty() {
                        Some(item.1.as_bytes().to_vec())
                    } else {
                        None
                    },
                    builder.row_writer(),
                )
                .await;
            println!("{:?}", test);
        }

        let chunk = builder.finish();
        let mut rows = chunk.rows();

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(1)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(1)))
            );
        }
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(2)))
            );
        }
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Delete);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(2)))
            );
        }
    }
}
