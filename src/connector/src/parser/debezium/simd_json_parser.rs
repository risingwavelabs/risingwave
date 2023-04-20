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

use std::fmt::Debug;

use futures_async_stream::try_stream;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use simd_json::{BorrowedValue, StaticNode, ValueAccess};

use super::operators::*;
use crate::impl_common_parser_logic;
use crate::parser::common::{json_object_smart_get_value, simd_json_parse_value};
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};
use crate::source::{SourceColumnDesc, SourceContextRef, SourceFormat};

const BEFORE: &str = "before";
const AFTER: &str = "after";
const OP: &str = "op";

#[inline]
fn ensure_not_null<'a, 'b: 'a>(value: &'a BorrowedValue<'b>) -> Option<&'a BorrowedValue<'b>> {
    if let BorrowedValue::Static(StaticNode::Null) = value {
        None
    } else {
        Some(value)
    }
}

impl_common_parser_logic!(DebeziumJsonParser);

#[derive(Debug)]
pub struct DebeziumJsonParser {
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

impl DebeziumJsonParser {
    pub fn new(rw_columns: Vec<SourceColumnDesc>, source_ctx: SourceContextRef) -> Result<Self> {
        Ok(Self {
            rw_columns,
            source_ctx,
        })
    }

    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &self,
        mut payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let event: BorrowedValue<'_> = simd_json::to_borrowed_value(&mut payload)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        let payload = event
            .get("payload")
            .and_then(ensure_not_null)
            .ok_or_else(|| {
                RwError::from(ProtocolError("no payload in debezium event".to_owned()))
            })?;

        let op = payload.get(OP).and_then(|v| v.as_str()).ok_or_else(|| {
            RwError::from(ProtocolError(
                "op field not found in debezium json".to_owned(),
            ))
        })?;

        let format = SourceFormat::DebeziumJson;
        match op {
            DEBEZIUM_UPDATE_OP => {
                let before = payload.get(BEFORE).and_then(ensure_not_null).ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "before is missing for updating event. If you are using postgres, you may want to try ALTER TABLE $TABLE_NAME REPLICA IDENTITY FULL;".to_string(),
                    ))
                })?;

                let after = payload
                    .get(AFTER)
                    .and_then(ensure_not_null)
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "after is missing for updating event".to_string(),
                        ))
                    })?;

                writer.update(|column| {
                    let before = simd_json_parse_value(
                        &format,
                        &column.data_type,
                        json_object_smart_get_value(before, (&column.name).into()),
                    )?;
                    let after = simd_json_parse_value(
                        &format,
                        &column.data_type,
                        json_object_smart_get_value(after, (&column.name).into()),
                    )?;

                    Ok((before, after))
                })
            }
            DEBEZIUM_CREATE_OP | DEBEZIUM_READ_OP => {
                let after = payload
                    .get(AFTER)
                    .and_then(ensure_not_null)
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "after is missing for creating event".to_string(),
                        ))
                    })?;

                writer.insert(|column| {
                    simd_json_parse_value(
                        &format,
                        &column.data_type,
                        json_object_smart_get_value(after, (&column.name).into()),
                    )
                    .map_err(Into::into)
                })
            }
            DEBEZIUM_DELETE_OP => {
                let before = payload
                    .get(BEFORE)
                    .and_then(ensure_not_null)
                    .ok_or_else(|| {
                        RwError::from(ProtocolError(
                            "before is missing for delete event".to_string(),
                        ))
                    })?;

                writer.delete(|column| {
                    simd_json_parse_value(
                        &format,
                        &column.data_type,
                        json_object_smart_get_value(before, (&column.name).into()),
                    )
                    .map_err(Into::into)
                })
            }
            _ => Err(RwError::from(ProtocolError(format!(
                "unknown debezium op: {}",
                op
            )))),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::convert::TryInto;

    use chrono::{NaiveDate, NaiveTime};
    use risingwave_common::array::Op;
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::{DataType, Date, Scalar, ScalarImpl, Time, Timestamp};
    use serde_json::Value;

    use super::*;
    use crate::parser::{SourceColumnDesc, SourceStreamChunkBuilder};

    fn get_test1_columns() -> Vec<SourceColumnDesc> {
        let descs = vec![
            SourceColumnDesc::simple("id", DataType::Int32, ColumnId::from(0)),
            SourceColumnDesc::simple("name", DataType::Varchar, ColumnId::from(1)),
            SourceColumnDesc::simple("description", DataType::Varchar, ColumnId::from(2)),
            SourceColumnDesc::simple("weight", DataType::Float64, ColumnId::from(3)),
        ];

        descs
    }

    // test2 generated by mysql
    fn get_test2_columns() -> Vec<SourceColumnDesc> {
        let descs = vec![
            SourceColumnDesc::simple("O_KEY", DataType::Int64, ColumnId::from(0)),
            SourceColumnDesc::simple("O_BOOL", DataType::Boolean, ColumnId::from(1)),
            SourceColumnDesc::simple("O_TINY", DataType::Int16, ColumnId::from(2)),
            SourceColumnDesc::simple("O_INT", DataType::Int32, ColumnId::from(3)),
            SourceColumnDesc::simple("O_REAL", DataType::Float32, ColumnId::from(4)),
            SourceColumnDesc::simple("O_DOUBLE", DataType::Float64, ColumnId::from(5)),
            SourceColumnDesc::simple("O_DECIMAL", DataType::Decimal, ColumnId::from(6)),
            SourceColumnDesc::simple("O_CHAR", DataType::Varchar, ColumnId::from(7)),
            SourceColumnDesc::simple("O_DATE", DataType::Date, ColumnId::from(8)),
            SourceColumnDesc::simple("O_TIME", DataType::Time, ColumnId::from(9)),
            SourceColumnDesc::simple("O_DATETIME", DataType::Timestamp, ColumnId::from(10)),
            SourceColumnDesc::simple("O_TIMESTAMP", DataType::Timestamp, ColumnId::from(11)),
            SourceColumnDesc::simple("O_JSON", DataType::Jsonb, ColumnId::from(12)),
        ];

        descs
    }

    fn assert_json_eq(parse_result: &Option<ScalarImpl>, json_str: &str) {
        if let Some(ScalarImpl::Jsonb(json_val)) = parse_result {
            let mut json_string = String::new();
            json_val
                .as_scalar_ref()
                .force_str(&mut json_string)
                .unwrap();
            let val1: Value = serde_json::from_str(json_string.as_str()).unwrap();
            let val2: Value = serde_json::from_str(json_str).unwrap();
            assert_eq!(val1, val2);
        }
    }

    async fn parse_one(
        parser: DebeziumJsonParser,
        columns: Vec<SourceColumnDesc>,
        payload: Vec<u8>,
    ) -> Vec<(Op, OwnedRow)> {
        let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 2);
        {
            let writer = builder.row_writer();
            parser.parse_inner(payload, writer).await.unwrap();
        }
        let chunk = builder.finish();
        chunk
            .rows()
            .map(|(op, row_ref)| (op, row_ref.into_owned_row()))
            .collect::<Vec<_>>()
    }

    #[tokio::test]
    async fn test2_debezium_json_parser_read() {
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"},{"type":"double","optional":true,"field":"O_DECIMAL"},{"type":"string","optional":true,"field":"O_CHAR"},{"type":"string","optional":true,"name":"risingwave.cdc.date.string","field":"O_DATE"},{"type":"int32","optional":true,"name":"org.apache.kafka.connect.data.Time","version":1,"field":"O_TIME"},{"type":"int64","optional":true,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"O_DATETIME"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"default":"1970-01-01T00:00:00Z","field":"O_TIMESTAMP"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"O_JSON"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"},{"type":"double","optional":true,"field":"O_DECIMAL"},{"type":"string","optional":true,"field":"O_CHAR"},{"type":"string","optional":true,"name":"risingwave.cdc.date.string","field":"O_DATE"},{"type":"int32","optional":true,"name":"org.apache.kafka.connect.data.Time","version":1,"field":"O_TIME"},{"type":"int64","optional":true,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"O_DATETIME"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"default":"1970-01-01T00:00:00Z","field":"O_TIMESTAMP"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"O_JSON"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"RW_CDC_test.orders.test.orders.Envelope"},"payload":{"before":null,"after":{"O_KEY":111,"O_BOOL":1,"O_TINY":-1,"O_INT":-1111,"O_REAL":-11.11,"O_DOUBLE":-111.11111,"O_DECIMAL":-111.11,"O_CHAR":"yes please","O_DATE":"1000-01-01","O_TIME":0,"O_DATETIME":0,"O_TIMESTAMP":"1970-01-01T00:00:01Z","O_JSON":"{\"k1\": \"v1\", \"k2\": 11}"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678090651000,"snapshot":"last","db":"test","sequence":null,"table":"orders","server_id":0,"gtid":null,"file":"mysql-bin.000003","pos":951,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1678090651640,"transaction":null}}"#;

        let columns = get_test2_columns();

        let parser = DebeziumJsonParser::new(columns.clone(), Default::default()).unwrap();

        let [(_op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
            .await
            .try_into()
            .unwrap();

        assert!(row[0].eq(&Some(ScalarImpl::Int64(111))));
        assert!(row[1].eq(&Some(ScalarImpl::Bool(true))));
        assert!(row[2].eq(&Some(ScalarImpl::Int16(-1))));
        assert!(row[3].eq(&Some(ScalarImpl::Int32(-1111))));
        assert!(row[4].eq(&Some(ScalarImpl::Float32((-11.11).into()))));
        assert!(row[5].eq(&Some(ScalarImpl::Float64((-111.11111).into()))));
        assert!(row[6].eq(&Some(ScalarImpl::Decimal("-111.11".parse().unwrap()))));
        assert!(row[7].eq(&Some(ScalarImpl::Utf8("yes please".into()))));
        assert!(row[8].eq(&Some(ScalarImpl::Date(Date::new(
            NaiveDate::from_ymd_opt(1000, 1, 1).unwrap()
        )))));
        assert!(row[9].eq(&Some(ScalarImpl::Time(Time::new(
            NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap()
        )))));
        assert!(row[10].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
            "1970-01-01T00:00:00".parse().unwrap()
        )))));
        assert!(row[11].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
            "1970-01-01T00:00:01".parse().unwrap()
        )))));
        assert_json_eq(&row[12], "{\"k1\": \"v1\", \"k2\": 11}");
    }

    #[tokio::test]
    async fn test2_debezium_json_parser_insert() {
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"},{"type":"double","optional":true,"field":"O_DECIMAL"},{"type":"string","optional":true,"field":"O_CHAR"},{"type":"string","optional":true,"name":"risingwave.cdc.date.string","field":"O_DATE"},{"type":"int32","optional":true,"name":"org.apache.kafka.connect.data.Time","version":1,"field":"O_TIME"},{"type":"int64","optional":true,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"O_DATETIME"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"default":"1970-01-01T00:00:00Z","field":"O_TIMESTAMP"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"O_JSON"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"},{"type":"double","optional":true,"field":"O_DECIMAL"},{"type":"string","optional":true,"field":"O_CHAR"},{"type":"string","optional":true,"name":"risingwave.cdc.date.string","field":"O_DATE"},{"type":"int32","optional":true,"name":"org.apache.kafka.connect.data.Time","version":1,"field":"O_TIME"},{"type":"int64","optional":true,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"O_DATETIME"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"default":"1970-01-01T00:00:00Z","field":"O_TIMESTAMP"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"O_JSON"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"RW_CDC_test.orders.test.orders.Envelope"},"payload":{"before":null,"after":{"O_KEY":111,"O_BOOL":1,"O_TINY":-1,"O_INT":-1111,"O_REAL":-11.11,"O_DOUBLE":-111.11111,"O_DECIMAL":-111.11,"O_CHAR":"yes please","O_DATE":"1000-01-01","O_TIME":0,"O_DATETIME":0,"O_TIMESTAMP":"1970-01-01T00:00:01Z","O_JSON":"{\"k1\": \"v1\", \"k2\": 11}"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678088861000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":789,"row":0,"thread":4,"query":null},"op":"c","ts_ms":1678088861249,"transaction":null}}"#;

        let columns = get_test2_columns();
        let parser = DebeziumJsonParser::new(columns.clone(), Default::default()).unwrap();
        let [(op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
            .await
            .try_into()
            .unwrap();
        assert_eq!(op, Op::Insert);

        assert!(row[0].eq(&Some(ScalarImpl::Int64(111))));
        assert!(row[1].eq(&Some(ScalarImpl::Bool(true))));
        assert!(row[2].eq(&Some(ScalarImpl::Int16(-1))));
        assert!(row[3].eq(&Some(ScalarImpl::Int32(-1111))));
        assert!(row[4].eq(&Some(ScalarImpl::Float32((-11.11).into()))));
        assert!(row[5].eq(&Some(ScalarImpl::Float64((-111.11111).into()))));
        assert!(row[6].eq(&Some(ScalarImpl::Decimal("-111.11".parse().unwrap()))));
        assert!(row[7].eq(&Some(ScalarImpl::Utf8("yes please".into()))));
        assert!(row[8].eq(&Some(ScalarImpl::Date(Date::new(
            NaiveDate::from_ymd_opt(1000, 1, 1).unwrap()
        )))));
        assert!(row[9].eq(&Some(ScalarImpl::Time(Time::new(
            NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap()
        )))));
        assert!(row[10].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
            "1970-01-01T00:00:00".parse().unwrap()
        )))));
        assert!(row[11].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
            "1970-01-01T00:00:01".parse().unwrap()
        )))));
        assert_json_eq(&row[12], "{\"k1\": \"v1\", \"k2\": 11}");
    }

    #[tokio::test]
    async fn test2_debezium_json_parser_delete() {
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"},{"type":"double","optional":true,"field":"O_DECIMAL"},{"type":"string","optional":true,"field":"O_CHAR"},{"type":"string","optional":true,"name":"risingwave.cdc.date.string","field":"O_DATE"},{"type":"int32","optional":true,"name":"org.apache.kafka.connect.data.Time","version":1,"field":"O_TIME"},{"type":"int64","optional":true,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"O_DATETIME"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"default":"1970-01-01T00:00:00Z","field":"O_TIMESTAMP"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"O_JSON"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"},{"type":"double","optional":true,"field":"O_DECIMAL"},{"type":"string","optional":true,"field":"O_CHAR"},{"type":"string","optional":true,"name":"risingwave.cdc.date.string","field":"O_DATE"},{"type":"int32","optional":true,"name":"org.apache.kafka.connect.data.Time","version":1,"field":"O_TIME"},{"type":"int64","optional":true,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"O_DATETIME"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"default":"1970-01-01T00:00:00Z","field":"O_TIMESTAMP"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"O_JSON"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"RW_CDC_test.orders.test.orders.Envelope"},"payload":{"before":{"O_KEY":111,"O_BOOL":0,"O_TINY":3,"O_INT":3333,"O_REAL":33.33,"O_DOUBLE":333.33333,"O_DECIMAL":333.33,"O_CHAR":"no thanks","O_DATE":"9999-12-31","O_TIME":86399000,"O_DATETIME":99999999999000,"O_TIMESTAMP":"2038-01-09T03:14:07Z","O_JSON":"{\"k1\":\"v1_updated\",\"k2\":33}"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678090653000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1643,"row":0,"thread":4,"query":null},"op":"d","ts_ms":1678090653611,"transaction":null}}"#;

        let columns = get_test2_columns();
        let parser = DebeziumJsonParser::new(columns.clone(), Default::default()).unwrap();
        let [(op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
            .await
            .try_into()
            .unwrap();

        assert_eq!(op, Op::Delete);

        assert!(row[0].eq(&Some(ScalarImpl::Int64(111))));
        assert!(row[1].eq(&Some(ScalarImpl::Bool(false))));
        assert!(row[2].eq(&Some(ScalarImpl::Int16(3))));
        assert!(row[3].eq(&Some(ScalarImpl::Int32(3333))));
        assert!(row[4].eq(&Some(ScalarImpl::Float32((33.33).into()))));
        assert!(row[5].eq(&Some(ScalarImpl::Float64((333.33333).into()))));
        assert!(row[6].eq(&Some(ScalarImpl::Decimal("333.33".parse().unwrap()))));
        assert!(row[7].eq(&Some(ScalarImpl::Utf8("no thanks".into()))));
        assert!(row[8].eq(&Some(ScalarImpl::Date(Date::new(
            NaiveDate::from_ymd_opt(9999, 12, 31).unwrap()
        )))));
        assert!(row[9].eq(&Some(ScalarImpl::Time(Time::new(
            NaiveTime::from_hms_micro_opt(23, 59, 59, 0).unwrap()
        )))));
        assert!(row[10].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
            "5138-11-16T09:46:39".parse().unwrap()
        )))));
        assert!(row[11].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
            "2038-01-09T03:14:07".parse().unwrap()
        )))));
        assert_json_eq(&row[12], "{\"k1\":\"v1_updated\",\"k2\":33}");
    }

    #[tokio::test]
    async fn test2_debezium_json_parser_update() {
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"},{"type":"double","optional":true,"field":"O_DECIMAL"},{"type":"string","optional":true,"field":"O_CHAR"},{"type":"string","optional":true,"name":"risingwave.cdc.date.string","field":"O_DATE"},{"type":"int32","optional":true,"name":"org.apache.kafka.connect.data.Time","version":1,"field":"O_TIME"},{"type":"int64","optional":true,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"O_DATETIME"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"default":"1970-01-01T00:00:00Z","field":"O_TIMESTAMP"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"O_JSON"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"},{"type":"double","optional":true,"field":"O_DECIMAL"},{"type":"string","optional":true,"field":"O_CHAR"},{"type":"string","optional":true,"name":"risingwave.cdc.date.string","field":"O_DATE"},{"type":"int32","optional":true,"name":"org.apache.kafka.connect.data.Time","version":1,"field":"O_TIME"},{"type":"int64","optional":true,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"O_DATETIME"},{"type":"string","optional":false,"name":"io.debezium.time.ZonedTimestamp","version":1,"default":"1970-01-01T00:00:00Z","field":"O_TIMESTAMP"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"O_JSON"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"RW_CDC_test.orders.test.orders.Envelope"},"payload":{"before":{"O_KEY":111,"O_BOOL":1,"O_TINY":-1,"O_INT":-1111,"O_REAL":-11.11,"O_DOUBLE":-111.11111,"O_DECIMAL":-111.11,"O_CHAR":"yes please","O_DATE":"1000-01-01","O_TIME":0,"O_DATETIME":0,"O_TIMESTAMP":"1970-01-01T00:00:01Z","O_JSON":"{\"k1\": \"v1\", \"k2\": 11}"},"after":{"O_KEY":111,"O_BOOL":0,"O_TINY":3,"O_INT":3333,"O_REAL":33.33,"O_DOUBLE":333.33333,"O_DECIMAL":333.33,"O_CHAR":"no thanks","O_DATE":"9999-12-31","O_TIME":86399000,"O_DATETIME":99999999999000,"O_TIMESTAMP":"2038-01-09T03:14:07Z","O_JSON":"{\"k1\": \"v1_updated\", \"k2\": 33}"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678089331000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1168,"row":0,"thread":4,"query":null},"op":"u","ts_ms":1678089331464,"transaction":null}}"#;

        let columns = get_test2_columns();

        let parser = DebeziumJsonParser::new(columns.clone(), Default::default()).unwrap();
        let [(op1, row1), (op2, row2)]: [_; 2] = parse_one(parser, columns, data.to_vec())
            .await
            .try_into()
            .unwrap();

        assert_eq!(op1, Op::UpdateDelete);
        assert_eq!(op2, Op::UpdateInsert);

        assert!(row1[0].eq(&Some(ScalarImpl::Int64(111))));
        assert!(row1[1].eq(&Some(ScalarImpl::Bool(true))));
        assert!(row1[2].eq(&Some(ScalarImpl::Int16(-1))));
        assert!(row1[3].eq(&Some(ScalarImpl::Int32(-1111))));
        assert!(row1[4].eq(&Some(ScalarImpl::Float32((-11.11).into()))));
        assert!(row1[5].eq(&Some(ScalarImpl::Float64((-111.11111).into()))));
        assert!(row1[6].eq(&Some(ScalarImpl::Decimal("-111.11".parse().unwrap()))));
        assert!(row1[7].eq(&Some(ScalarImpl::Utf8("yes please".into()))));
        assert!(row1[8].eq(&Some(ScalarImpl::Date(Date::new(
            NaiveDate::from_ymd_opt(1000, 1, 1).unwrap()
        )))));
        assert!(row1[9].eq(&Some(ScalarImpl::Time(Time::new(
            NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap()
        )))));
        assert!(row1[10].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
            "1970-01-01T00:00:00".parse().unwrap()
        )))));
        assert!(row1[11].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
            "1970-01-01T00:00:01".parse().unwrap()
        )))));
        assert_json_eq(&row1[12], "{\"k1\": \"v1\", \"k2\": 11}");

        assert!(row2[0].eq(&Some(ScalarImpl::Int64(111))));
        assert!(row2[1].eq(&Some(ScalarImpl::Bool(false))));
        assert!(row2[2].eq(&Some(ScalarImpl::Int16(3))));
        assert!(row2[3].eq(&Some(ScalarImpl::Int32(3333))));
        assert!(row2[4].eq(&Some(ScalarImpl::Float32((33.33).into()))));
        assert!(row2[5].eq(&Some(ScalarImpl::Float64((333.33333).into()))));
        assert!(row2[6].eq(&Some(ScalarImpl::Decimal("333.33".parse().unwrap()))));
        assert!(row2[7].eq(&Some(ScalarImpl::Utf8("no thanks".into()))));
        assert!(row2[8].eq(&Some(ScalarImpl::Date(Date::new(
            NaiveDate::from_ymd_opt(9999, 12, 31).unwrap()
        )))));
        assert!(row2[9].eq(&Some(ScalarImpl::Time(Time::new(
            NaiveTime::from_hms_micro_opt(23, 59, 59, 0).unwrap()
        )))));
        assert!(row2[10].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
            "5138-11-16T09:46:39".parse().unwrap()
        )))));
        assert!(row2[11].eq(&Some(ScalarImpl::Timestamp(Timestamp::new(
            "2038-01-09T03:14:07".parse().unwrap()
        )))));
        assert_json_eq(&row2[12], "{\"k1\": \"v1_updated\", \"k2\": 33}");
    }

    #[tokio::test]
    async fn test2_debezium_json_parser_overflow() {
        let columns = vec![
            SourceColumnDesc::simple("O_KEY", DataType::Int64, ColumnId::from(0)),
            SourceColumnDesc::simple("O_BOOL", DataType::Boolean, ColumnId::from(1)),
            SourceColumnDesc::simple("O_TINY", DataType::Int16, ColumnId::from(2)),
            SourceColumnDesc::simple("O_INT", DataType::Int32, ColumnId::from(3)),
            SourceColumnDesc::simple("O_REAL", DataType::Float32, ColumnId::from(4)),
            SourceColumnDesc::simple("O_DOUBLE", DataType::Float64, ColumnId::from(5)),
        ];
        let parser = DebeziumJsonParser::new(columns.clone(), Default::default()).unwrap();

        let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 2);
        // i64 overflow
        let data0 = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"RW_CDC_test.orders.test.orders.Envelope"},"payload":{"before":null,"after":{"O_KEY":9223372036854775808,"O_BOOL":1,"O_TINY":33,"O_INT":444,"O_REAL":555.0,"O_DOUBLE":666.0},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678158055000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":637,"row":0,"thread":4,"query":null},"op":"c","ts_ms":1678158055464,"transaction":null}}"#;
        if let Err(e) = parser
            .parse_inner(data0.to_vec(), builder.row_writer())
            .await
        {
            println!("{:?}", e.to_string());
        } else {
            panic!("the test case is expected fail");
        }
        // bool incorrect value
        let data1 = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"RW_CDC_test.orders.test.orders.Envelope"},"payload":{"before":null,"after":{"O_KEY":111,"O_BOOL":2,"O_TINY":33,"O_INT":444,"O_REAL":555.0,"O_DOUBLE":666.0},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678158055000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":637,"row":0,"thread":4,"query":null},"op":"c","ts_ms":1678158055464,"transaction":null}}"#;
        if let Err(e) = parser
            .parse_inner(data1.to_vec(), builder.row_writer())
            .await
        {
            println!("{:?}", e.to_string());
        } else {
            panic!("the test case is expected failed");
        }
        // i16 overflow
        let data2 = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"RW_CDC_test.orders.test.orders.Envelope"},"payload":{"before":null,"after":{"O_KEY":111,"O_BOOL":1,"O_TINY":32768,"O_INT":444,"O_REAL":555.0,"O_DOUBLE":666.0},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678158055000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":637,"row":0,"thread":4,"query":null},"op":"c","ts_ms":1678158055464,"transaction":null}}"#;
        if let Err(e) = parser
            .parse_inner(data2.to_vec(), builder.row_writer())
            .await
        {
            println!("{:?}", e.to_string());
        } else {
            panic!("the test case is expected to fail");
        }
        // i32 overflow
        let data3 = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"RW_CDC_test.orders.test.orders.Envelope"},"payload":{"before":null,"after":{"O_KEY":111,"O_BOOL":1,"O_TINY":33,"O_INT":2147483648,"O_REAL":555.0,"O_DOUBLE":666.0},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678158055000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":637,"row":0,"thread":4,"query":null},"op":"c","ts_ms":1678158055464,"transaction":null}}"#;
        if let Err(e) = parser
            .parse_inner(data3.to_vec(), builder.row_writer())
            .await
        {
            println!("{:?}", e.to_string());
        } else {
            panic!("the test case is expected to fail");
        }
        // float32 overflow
        let data4 = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_KEY"},{"type":"int16","optional":true,"field":"O_BOOL"},{"type":"int16","optional":true,"field":"O_TINY"},{"type":"int32","optional":true,"field":"O_INT"},{"type":"double","optional":true,"field":"O_REAL"},{"type":"double","optional":true,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"RW_CDC_test.orders.test.orders.Envelope"},"payload":{"before":null,"after":{"O_KEY":111,"O_BOOL":1,"O_TINY":33,"O_INT":444,"O_REAL":3.80282347E38,"O_DOUBLE":666.0},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678158055000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":637,"row":0,"thread":4,"query":null},"op":"c","ts_ms":1678158055464,"transaction":null}}"#;
        if let Err(e) = parser
            .parse_inner(data4.to_vec(), builder.row_writer())
            .await
        {
            println!("{:?}", e.to_string());
        } else {
            panic!("the test case is expected to fail");
        }
        // float64 will cause debezium simd_json_parser to panic, therefore included in the next
        // test case below
    }

    #[tokio::test]
    #[should_panic]
    async fn test2_debezium_json_parser_overflow_f64() {
        let columns = vec![SourceColumnDesc::simple(
            "O_DOUBLE",
            DataType::Float64,
            ColumnId::from(0),
        )];
        let parser = DebeziumJsonParser::new(columns.clone(), Default::default()).unwrap();
        let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 2);
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"before"},{"type":"struct","fields":[{"type":"int64","optional":false,"field":"O_DOUBLE"}],"optional":true,"name":"RW_CDC_test.orders.test.orders.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"RW_CDC_test.orders.test.orders.Envelope"},"payload":{"before":null,"after":{"O_DOUBLE":1.797695E308},"source":{"version":"1.9.7.Final","connector":"mysql","name":"RW_CDC_test.orders","ts_ms":1678174483000,"snapshot":"false","db":"test","sequence":null,"table":"orders","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":563,"row":0,"thread":3,"query":null},"op":"c","ts_ms":1678174483866,"transaction":null}}"#;
        if let Err(e) = parser
            .parse_inner(data.to_vec(), builder.row_writer())
            .await
        {
            println!("{:?}", e.to_string());
        } else {
            panic!("the test case is expected to fail");
        }
    }

    #[tokio::test]
    async fn test1_debezium_json_parser_read() {
        //     "before": null,
        //     "after": {
        //       "id": 101,
        //       "name": "scooter",
        //       "description": "Small 2-wheel scooter",
        //       "weight": 1.234
        //     },
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"before"},{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.products.Envelope"},"payload":{"before":null,"after":{"id":101,"name":"scooter","description":"Small 2-wheel scooter","weight":1.234},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639547113601,"snapshot":"true","db":"inventory","sequence":null,"table":"products","server_id":0,"gtid":null,"file":"mysql-bin.000003","pos":156,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1639547113602,"transaction":null}}"#;

        let columns = get_test1_columns();

        let parser = DebeziumJsonParser::new(columns.clone(), Default::default()).unwrap();

        let [(_op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
            .await
            .try_into()
            .unwrap();

        assert!(row[0].eq(&Some(ScalarImpl::Int32(101))));
        assert!(row[1].eq(&Some(ScalarImpl::Utf8("scooter".into()))));
        assert!(row[2].eq(&Some(ScalarImpl::Utf8("Small 2-wheel scooter".into()))));
        assert!(row[3].eq(&Some(ScalarImpl::Float64(1.234.into()))));
    }

    #[tokio::test]
    async fn test1_debezium_json_parser_insert() {
        //     "before": null,
        //     "after": {
        //       "id": 102,
        //       "name": "car battery",
        //       "description": "12V car battery",
        //       "weight": 8.1
        //     },
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"before"},{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.products.Envelope"},"payload":{"before":null,"after":{"id":102,"name":"car battery","description":"12V car battery","weight":8.1},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551564000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":717,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1639551564960,"transaction":null}}"#;

        let columns = get_test1_columns();
        let parser = DebeziumJsonParser::new(columns.clone(), Default::default()).unwrap();
        let [(op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
            .await
            .try_into()
            .unwrap();
        assert_eq!(op, Op::Insert);

        assert!(row[0].eq(&Some(ScalarImpl::Int32(102))));
        assert!(row[1].eq(&Some(ScalarImpl::Utf8("car battery".into()))));
        assert!(row[2].eq(&Some(ScalarImpl::Utf8("12V car battery".into()))));
        assert!(row[3].eq(&Some(ScalarImpl::Float64(8.1.into()))));
    }

    #[tokio::test]
    async fn test1_debezium_json_parser_delete() {
        //     "before": {
        //       "id": 101,
        //       "name": "scooter",
        //       "description": "Small 2-wheel scooter",
        //       "weight": 1.234
        //     },
        //     "after": null,
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"before"},{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.products.Envelope"},"payload":{"before":{"id":101,"name":"scooter","description":"Small 2-wheel scooter","weight":1.234},"after":null,"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551767000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1045,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1639551767775,"transaction":null}}"#;

        let columns = get_test1_columns();
        let parser = DebeziumJsonParser::new(columns.clone(), Default::default()).unwrap();
        let [(op, row)]: [_; 1] = parse_one(parser, columns, data.to_vec())
            .await
            .try_into()
            .unwrap();

        assert_eq!(op, Op::Delete);

        assert!(row[0].eq(&Some(ScalarImpl::Int32(101))));
        assert!(row[1].eq(&Some(ScalarImpl::Utf8("scooter".into()))));
        assert!(row[2].eq(&Some(ScalarImpl::Utf8("Small 2-wheel scooter".into()))));
        assert!(row[3].eq(&Some(ScalarImpl::Float64(1.234.into()))));
    }

    #[tokio::test]
    async fn test1_debezium_json_parser_update() {
        //     "before": {
        //       "id": 102,
        //       "name": "car battery",
        //       "description": "12V car battery",
        //       "weight": 8.1
        //     },
        //     "after": {
        //       "id": 102,
        //       "name": "car battery",
        //       "description": "24V car battery",
        //       "weight": 9.1
        //     },
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"before"},{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.products.Envelope"},"payload":{"before":{"id":102,"name":"car battery","description":"12V car battery","weight":8.1},"after":{"id":102,"name":"car battery","description":"24V car battery","weight":9.1},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551901000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":1382,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1639551901165,"transaction":null}}"#;

        let columns = get_test1_columns();

        let parser = DebeziumJsonParser::new(columns.clone(), Default::default()).unwrap();
        let [(op1, row1), (op2, row2)]: [_; 2] = parse_one(parser, columns, data.to_vec())
            .await
            .try_into()
            .unwrap();

        assert_eq!(op1, Op::UpdateDelete);
        assert_eq!(op2, Op::UpdateInsert);

        assert!(row1[0].eq(&Some(ScalarImpl::Int32(102))));
        assert!(row1[1].eq(&Some(ScalarImpl::Utf8("car battery".into()))));
        assert!(row1[2].eq(&Some(ScalarImpl::Utf8("12V car battery".into()))));
        assert!(row1[3].eq(&Some(ScalarImpl::Float64(8.1.into()))));

        assert!(row2[0].eq(&Some(ScalarImpl::Int32(102))));
        assert!(row2[1].eq(&Some(ScalarImpl::Utf8("car battery".into()))));
        assert!(row2[2].eq(&Some(ScalarImpl::Utf8("24V car battery".into()))));
        assert!(row2[3].eq(&Some(ScalarImpl::Float64(9.1.into()))));
    }

    #[tokio::test]
    async fn test1_update_with_before_null() {
        // the test case it identical with test_debezium_json_parser_insert but op is 'u'
        //     "before": null,
        //     "after": {
        //       "id": 102,
        //       "name": "car battery",
        //       "description": "12V car battery",
        //       "weight": 8.1
        //     },
        let data = br#"{"schema":{"type":"struct","fields":[{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"before"},{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"name"},{"type":"string","optional":true,"field":"description"},{"type":"double","optional":true,"field":"weight"}],"optional":true,"name":"dbserver1.inventory.products.Value","field":"after"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":true,"field":"table"},{"type":"int64","optional":false,"field":"server_id"},{"type":"string","optional":true,"field":"gtid"},{"type":"string","optional":false,"field":"file"},{"type":"int64","optional":false,"field":"pos"},{"type":"int32","optional":false,"field":"row"},{"type":"int64","optional":true,"field":"thread"},{"type":"string","optional":true,"field":"query"}],"optional":false,"name":"io.debezium.connector.mysql.Source","field":"source"},{"type":"string","optional":false,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.products.Envelope"},"payload":{"before":null,"after":{"id":102,"name":"car battery","description":"12V car battery","weight":8.1},"source":{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639551564000,"snapshot":"false","db":"inventory","sequence":null,"table":"products","server_id":223344,"gtid":null,"file":"mysql-bin.000003","pos":717,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1639551564960,"transaction":null}}"#;

        let columns = get_test1_columns();
        let parser = DebeziumJsonParser::new(columns.clone(), Default::default()).unwrap();

        let mut builder = SourceStreamChunkBuilder::with_capacity(columns, 2);
        let writer = builder.row_writer();
        if let Err(e) = parser.parse_inner(data.to_vec(), writer).await {
            println!("{:?}", e.to_string());
        } else {
            panic!("the test case is expected to be failed");
        }
    }
}
